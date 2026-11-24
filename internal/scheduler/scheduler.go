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
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dbaltas/mastori/internal/queue"
	"github.com/dbaltas/mastori/internal/registry"
)

var (
	errNoWorker    = errors.New("scheduler: no eligible worker")
	errWorkersFull = errors.New("scheduler: every eligible worker is at the queue limit")
)

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
	ReclaimInterval    time.Duration // dead-worker reclaim pass interval
	InterruptKey       string        // Redis key set when a reclaim invalidates a run
	InterruptTTL       time.Duration // TTL of the interruption flag
	// WorkerQueueLimit is the admission cap: a worker is only dispatched to
	// while its stream holds fewer than this many unfinished jobs (queued
	// plus running). When every worker is at the cap the job waits in the
	// jobs stream, so the platform queue — not the engine — holds the
	// backlog. 0 or negative disables the cap.
	WorkerQueueLimit int64
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
	if c.ReclaimInterval <= 0 {
		c.ReclaimInterval = time.Second
	}
	if c.InterruptKey == "" {
		c.InterruptKey = "run:interrupted"
	}
	if c.InterruptTTL <= 0 {
		c.InterruptTTL = 6 * time.Hour
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

	// Cached worker list, guarded by cacheMu (the dispatch goroutine and
	// the reclaim pass both consult it).
	cacheMu  sync.Mutex
	cached   []registry.Worker
	cachedAt time.Time

	// Workers already warned about for having stranded jobs. Only touched
	// from the reclaim goroutine.
	reclaimWarned map[string]bool
}

func New(client *redis.Client, reg *registry.Registry, policy Policy, m *Metrics, logger *slog.Logger, cfg Config) *Scheduler {
	return &Scheduler{
		client:        client,
		reg:           reg,
		policy:        policy,
		m:             m,
		logger:        logger,
		cfg:           cfg.withDefaults(),
		reclaimWarned: make(map[string]bool),
	}
}

// Run reads and dispatches jobs until ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.ensureGroup(ctx); err != nil {
		return err
	}

	go s.statsLoop(ctx)
	go s.reclaimLoop(ctx)

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
			if redis.HasErrorPrefix(err, "NOGROUP") {
				s.logger.Warn("consumer group missing, recreating", "stream", s.cfg.Stream, "group", s.cfg.Group)
				s.dispatchError("nogroup")
				if gerr := s.ensureGroup(ctx); gerr != nil {
					s.logger.Error("recreate consumer group failed", "error", gerr)
				}
				continue
			}
			s.logger.Error("read jobs failed", "error", err)
			s.dispatchError("redis")
			sleep(ctx, s.cfg.NoWorkerBackoff)
			continue
		}
		for _, st := range res {
			for _, msg := range st.Messages {
				// handle only returns an error when ctx is canceled; every
				// dispatch failure is retried in place.
				if err := s.handle(ctx, msg); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Scheduler) ensureGroup(ctx context.Context) error {
	err := s.client.XGroupCreateMkStream(ctx, s.cfg.Stream, s.cfg.Group, "0").Err()
	if err != nil && !redis.HasErrorPrefix(err, "BUSYGROUP") {
		return fmt.Errorf("scheduler: create consumer group: %w", err)
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

		err := s.place(ctx, s.cfg.Stream, s.cfg.Group, msg.ID, job, readAt)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch {
		case errors.Is(err, errNoWorker):
			s.dispatchError("no_worker")
		case errors.Is(err, errWorkersFull):
			s.dispatchError("workers_full")
		default:
			s.logger.Error("dispatch failed", "job_id", job.ID, "error", err)
			s.dispatchError("redis")
		}
		if !sleep(ctx, s.cfg.NoWorkerBackoff) {
			return ctx.Err()
		}
	}
}

// place picks an eligible worker for the job and atomically moves the entry
// there: XAdd to the worker's stream, XDel from the source stream, and XAck
// when the source is the group-owned jobs stream (group != ""). The whole
// move is one MULTI/EXEC transaction, so the entry is never in both streams
// and never in neither.
func (s *Scheduler) place(ctx context.Context, src, group, entryID string, job queue.Job, readAt time.Time) error {
	target, err := s.pickWorker(ctx, job)
	if err != nil {
		return err
	}

	pipe := s.client.TxPipeline()
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: s.cfg.WorkerStreamPrefix + target,
		Values: queue.JobValues(job),
	})
	if group != "" {
		pipe.XAck(ctx, src, group, entryID)
	}
	pipe.XDel(ctx, src, entryID)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	s.m.Dispatched.WithLabelValues(target).Inc()
	s.m.SchedulingSeconds.Observe(time.Since(readAt).Seconds())
	s.m.QueueWaitSeconds.Observe(time.Since(job.CreatedAt).Seconds())
	s.logger.Debug("job dispatched", "job_id", job.ID, "worker", target, "policy", s.policy.Name())
	return nil
}

// pickWorker asks the policy to select a live worker able to serve the job.
// When WorkerQueueLimit is set, workers at the cap are not eligible and the
// job is left in the jobs stream instead (errWorkersFull).
func (s *Scheduler) pickWorker(ctx context.Context, job queue.Job) (string, error) {
	workers, err := s.liveWorkers(ctx)
	if err != nil {
		return "", err
	}

	candidates := make([]Candidate, 0, len(workers))
	for _, w := range workers {
		if w.Supports(job.Model) {
			candidates = append(candidates, Candidate{Worker: w})
		}
	}
	if len(candidates) == 0 {
		return "", errNoWorker
	}

	if s.cfg.WorkerQueueLimit > 0 || s.policy.NeedsLoad() {
		if err := s.fillLoads(ctx, candidates); err != nil {
			return "", err
		}
	}
	if s.cfg.WorkerQueueLimit > 0 {
		eligible := candidates[:0]
		for _, c := range candidates {
			if c.Load < s.cfg.WorkerQueueLimit {
				eligible = append(eligible, c)
			}
		}
		candidates = eligible
		if len(candidates) == 0 {
			return "", errWorkersFull
		}
	}

	idx := s.policy.Select(job, candidates)
	if idx < 0 || idx >= len(candidates) {
		return "", errNoWorker
	}
	return candidates[idx].ID, nil
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
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
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

// reclaimLoop periodically looks for stranded jobs on dead workers' streams.
// Workers consume with plain XRead and delete each entry after the job
// finishes, so a crashed worker leaves entries on its stream forever; the
// reclaim pass re-dispatches them to live workers and signals that an
// inference slot was lost mid-run.
func (s *Scheduler) reclaimLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.ReclaimInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.reclaimOnce(ctx)
		}
	}
}

func (s *Scheduler) reclaimOnce(ctx context.Context) {
	streams, err := s.workerStreams(ctx)
	if err != nil {
		s.logger.Warn("reclaim scan failed", "error", err)
		return
	}
	for _, key := range streams {
		id := strings.TrimPrefix(key, s.cfg.WorkerStreamPrefix)
		if id == "" {
			continue
		}
		alive, err := s.reg.Alive(ctx, id)
		if err != nil {
			s.logger.Warn("reclaim liveness check failed", "worker", id, "error", err)
			continue
		}
		if alive {
			continue // live worker: its stream holds queued/running work
		}
		if err := s.reclaimStream(ctx, key, id); err != nil {
			s.logger.Warn("reclaim failed", "worker", id, "error", err)
		}
	}
}

// workerStreams lists all per-worker stream keys with SCAN.
func (s *Scheduler) workerStreams(ctx context.Context) ([]string, error) {
	var (
		cursor uint64
		keys   []string
	)
	for {
		batch, next, err := s.client.Scan(ctx, cursor, s.cfg.WorkerStreamPrefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

// reclaimStream re-dispatches the stranded entries of one dead worker's
// stream and deletes the stream key once every entry has been moved or
// dropped. Entries that no live worker can serve stay in place and are
// retried on the next pass.
func (s *Scheduler) reclaimStream(ctx context.Context, key, workerID string) error {
	entries, err := s.client.XRange(ctx, key, "-", "+").Result()
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	var moved, dropped, stranded int
	for _, e := range entries {
		job, ok := queue.JobFromValues(e.Values)
		if !ok {
			if err := s.client.XDel(ctx, key, e.ID).Err(); err != nil {
				return err
			}
			s.m.JobsDropped.WithLabelValues("malformed").Inc()
			dropped++
			continue
		}
		switch err := s.place(ctx, key, "", e.ID, job, time.Now()); {
		case err == nil:
			moved++
			delete(s.reclaimWarned, workerID)
		case errors.Is(err, errNoWorker), errors.Is(err, errWorkersFull):
			stranded++
			if !s.reclaimWarned[workerID] {
				s.reclaimWarned[workerID] = true
				s.logger.Warn("stranded jobs on dead worker, no live worker to take them", "worker", workerID)
			}
		default:
			return err
		}
	}
	if stranded > 0 {
		return nil
	}
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return err
	}
	delete(s.reclaimWarned, workerID)

	// A reclaim means an inference slot was lost mid-run: mark any in
	// progress benchmark run invalid so the traffic driver fails the run
	// instead of reporting numbers spanning a reclaimed slot.
	if err := s.client.Set(ctx, s.cfg.InterruptKey, "1", s.cfg.InterruptTTL).Err(); err != nil {
		s.logger.Warn("set interruption flag failed", "error", err)
	}
	s.m.Reclaims.Inc()
	s.logger.Info("reclaimed dead worker stream", "worker", workerID, "moved", moved, "dropped", dropped)
	return nil
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
