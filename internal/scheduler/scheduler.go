// Package scheduler sits between the API's jobs stream and the workers.
//
// The scheduler is the only reader of the jobs stream (through its own
// consumer group). For each job it asks a Policy to pick one of the live
// workers from the registry and moves the job to that worker's private stream.
// The move is a single MULTI/EXEC transaction, so a job is never in both
// streams and never in neither.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dbaltas/mastori/internal/queue"
	"github.com/dbaltas/mastori/internal/registry"
)

var errNoWorker = errors.New("scheduler: no eligible worker")

// Config holds the scheduler's tunables. Zero values select the defaults
// applied by New.
type Config struct {
	Stream             string        // jobs stream written by the API
	Group              string        // consumer group the scheduler reads with
	ConsumerID         string        // consumer name within Group
	WorkerStreamPrefix string        // per-worker stream key prefix
	JobTTL             time.Duration // drop jobs older than this; 0 disables
	BatchSize          int64         // max entries per XREADGROUP
	ReadBlock          time.Duration // XREADGROUP block time
	WorkerRefresh      time.Duration // max age of the cached worker list
	NoWorkerBackoff    time.Duration // wait between attempts when no worker is eligible
	StatsInterval      time.Duration // gauge refresh interval
}

func (c Config) withDefaults() Config {
	if c.Stream == "" {
		c.Stream = "jobs"
	}
	if c.Group == "" {
		c.Group = "mastori-scheduler"
	}
	if c.ConsumerID == "" {
		c.ConsumerID = "scheduler"
	}
	if c.WorkerStreamPrefix == "" {
		c.WorkerStreamPrefix = registry.DefaultStreamPrefix
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 16
	}
	if c.ReadBlock <= 0 {
		c.ReadBlock = time.Second
	}
	if c.WorkerRefresh <= 0 {
		c.WorkerRefresh = 250 * time.Millisecond
	}
	if c.NoWorkerBackoff <= 0 {
		c.NoWorkerBackoff = 100 * time.Millisecond
	}
	if c.StatsInterval <= 0 {
		c.StatsInterval = time.Second
	}
	return c
}

// Scheduler dispatches jobs from the jobs stream to workers.
type Scheduler struct {
	client *redis.Client
	reg    *registry.Registry
	policy Policy
	m      *Metrics
	logger *slog.Logger
	cfg    Config

	// Cached worker list. Only touched from the dispatch goroutine.
	cached   []registry.Worker
	cachedAt time.Time
}

func New(client *redis.Client, reg *registry.Registry, policy Policy, m *Metrics, logger *slog.Logger, cfg Config) *Scheduler {
	return &Scheduler{
		client: client,
		reg:    reg,
		policy: policy,
		m:      m,
		logger: logger,
		cfg:    cfg.withDefaults(),
	}
}

// Run reads and dispatches jobs until ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) error {
	err := s.client.XGroupCreateMkStream(ctx, s.cfg.Stream, s.cfg.Group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("scheduler: create consumer group: %w", err)
	}

	go s.statsLoop(ctx)

	s.logger.Info("scheduler started",
		"policy", s.policy.Name(), "stream", s.cfg.Stream,
		"group", s.cfg.Group, "consumer", s.cfg.ConsumerID)

	for ctx.Err() == nil {
		res, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.cfg.Group,
			Consumer: s.cfg.ConsumerID,
			Streams:  []string{s.cfg.Stream, ">"},
			Count:    s.cfg.BatchSize,
			Block:    s.cfg.ReadBlock,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || ctx.Err() != nil {
				continue
			}
			s.logger.Error("read jobs failed", "error", err)
			s.dispatchError("redis")
			sleep(ctx, s.cfg.NoWorkerBackoff)
			continue
		}
		for _, st := range res {
			for _, msg := range st.Messages {
				if err := s.handle(ctx, msg); err != nil {
					return nil // only returned when ctx is canceled
				}
			}
		}
	}
	return nil
}

func (s *Scheduler) dispatchError(reason string) {
	s.m.DispatchErrors.WithLabelValues(reason).Inc()
}

// handle dispatches one job, retrying until it is placed, dropped, or ctx is
// canceled. The job stays in the jobs stream (pending for this consumer)
// until dispatch commits, so it is never lost on a retryable failure.
func (s *Scheduler) handle(ctx context.Context, msg redis.XMessage) error {
	readAt := time.Now()

	job, ok := queue.JobFromValues(msg.Values)
	if !ok {
		s.logger.Error("dropping malformed job entry", "entry_id", msg.ID)
		s.drop(ctx, msg.ID, "malformed")
		return nil
	}

	for {
		if s.cfg.JobTTL > 0 && time.Since(job.CreatedAt) > s.cfg.JobTTL {
			s.logger.Warn("dropping expired job", "job_id", job.ID, "age", time.Since(job.CreatedAt))
			s.drop(ctx, msg.ID, "expired")
			return nil
		}

		err := s.dispatch(ctx, msg.ID, job, readAt)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errNoWorker) {
			s.dispatchError("no_worker")
		} else {
			s.logger.Error("dispatch failed", "job_id", job.ID, "error", err)
			s.dispatchError("redis")
		}
		if !sleep(ctx, s.cfg.NoWorkerBackoff) {
			return ctx.Err()
		}
	}
}

func (s *Scheduler) dispatch(ctx context.Context, entryID string, job queue.Job, readAt time.Time) error {
	workers, err := s.liveWorkers(ctx)
	if err != nil {
		return err
	}

	candidates := make([]Candidate, 0, len(workers))
	for _, w := range workers {
		if w.Supports(job.Model) {
			candidates = append(candidates, Candidate{Worker: w})
		}
	}
	if len(candidates) == 0 {
		return errNoWorker
	}

	if s.policy.NeedsLoad() {
		if err := s.fillLoads(ctx, candidates); err != nil {
			return err
		}
	}

	idx := s.policy.Select(job, candidates)
	if idx < 0 || idx >= len(candidates) {
		return errNoWorker
	}
	target := candidates[idx].ID

	pipe := s.client.TxPipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: s.cfg.WorkerStreamPrefix + target,
		Values: queue.JobValues(job),
	})
	pipe.XAck(ctx, s.cfg.Stream, s.cfg.Group, entryID)
	pipe.XDel(ctx, s.cfg.Stream, entryID)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	s.m.Dispatched.WithLabelValues(target).Inc()
	s.m.SchedulingSeconds.Observe(time.Since(readAt).Seconds())
	s.m.QueueWaitSeconds.Observe(time.Since(job.CreatedAt).Seconds())
	s.logger.Debug("job dispatched", "job_id", job.ID, "worker", target, "policy", s.policy.Name())
	return nil
}

// drop removes an entry from the jobs stream without dispatching it.
func (s *Scheduler) drop(ctx context.Context, entryID, reason string) {
	pipe := s.client.TxPipeline()
	pipe.XAck(ctx, s.cfg.Stream, s.cfg.Group, entryID)
	pipe.XDel(ctx, s.cfg.Stream, entryID)
	if _, err := pipe.Exec(ctx); err != nil {
		s.logger.Error("drop job failed", "entry_id", entryID, "error", err)
		return
	}
	s.m.JobsDropped.WithLabelValues(reason).Inc()
}

func (s *Scheduler) liveWorkers(ctx context.Context) ([]registry.Worker, error) {
	if s.cached != nil && time.Since(s.cachedAt) < s.cfg.WorkerRefresh {
		return s.cached, nil
	}
	workers, err := s.reg.List(ctx)
	if err != nil {
		return nil, err
	}
	if workers == nil {
		workers = []registry.Worker{}
	}
	s.cached, s.cachedAt = workers, time.Now()
	return workers, nil
}

// fillLoads sets Candidate.Load to the length of each worker's stream. Workers
// delete an entry once the job finishes, so the length is queued plus running.
func (s *Scheduler) fillLoads(ctx context.Context, candidates []Candidate) error {
	pipe := s.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(candidates))
	for i, c := range candidates {
		cmds[i] = pipe.XLen(ctx, s.cfg.WorkerStreamPrefix+c.ID)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	for i := range candidates {
		candidates[i].Load = cmds[i].Val()
	}
	return nil
}

func (s *Scheduler) statsLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.StatsInterval)
	defer t.Stop()
	for {
		s.collect(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// collect refreshes the queue depth and worker gauges.
func (s *Scheduler) collect(ctx context.Context) {
	depth, err := s.client.XLen(ctx, s.cfg.Stream).Result()
	if err != nil {
		s.logger.Warn("queue depth read failed", "error", err)
	} else {
		s.m.QueueDepth.Set(float64(depth))
	}

	workers, err := s.reg.List(ctx)
	if err != nil {
		s.logger.Warn("worker list failed", "error", err)
		return
	}
	s.m.WorkersHealthy.Set(float64(len(workers)))

	pipe := s.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(workers))
	for i, w := range workers {
		cmds[i] = pipe.XLen(ctx, s.cfg.WorkerStreamPrefix+w.ID)
	}
	if len(workers) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			s.logger.Warn("worker queue depth read failed", "error", err)
			return
		}
	}
	s.m.WorkerQueueDepth.Reset() // drop series for workers that are gone
	for i, w := range workers {
		s.m.WorkerQueueDepth.WithLabelValues(w.ID).Set(float64(cmds[i].Val()))
	}
}

// sleep waits for d or until ctx is canceled. It reports whether the full
// duration elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
