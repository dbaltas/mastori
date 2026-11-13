package worker

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dbaltas/mastori/internal/queue"
	"github.com/dbaltas/mastori/internal/registry"
)

// RunnerConfig configures a Runner.
type RunnerConfig struct {
	ID                 string
	Models             []string // empty: accept any model
	WorkerStreamPrefix string   // per-worker stream key prefix; the worker reads WorkerStreamPrefix+ID
	ChunksPrefix       string
	AbandonPrefix      string
	HeartbeatInterval  time.Duration
	ReadBlock          time.Duration
}

func (c RunnerConfig) withDefaults() RunnerConfig {
	if c.WorkerStreamPrefix == "" {
		c.WorkerStreamPrefix = registry.DefaultStreamPrefix
	}
	if c.ChunksPrefix == "" {
		c.ChunksPrefix = "jobs:chunks:"
	}
	if c.AbandonPrefix == "" {
		c.AbandonPrefix = "jobs:abandon:"
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = time.Second
	}
	if c.ReadBlock <= 0 {
		c.ReadBlock = time.Second
	}
	return c
}

// Runner is a worker that executes jobs the scheduler placed on its private
// stream. It registers itself in the registry and keeps the heartbeat fresh so
// the scheduler keeps routing to it.
type Runner struct {
	client *redis.Client
	reg    *registry.Registry
	gen    Generator
	logger *slog.Logger
	cfg    RunnerConfig

	mu      sync.Mutex
	cancels map[string]context.CancelFunc
}

func NewRunner(client *redis.Client, reg *registry.Registry, gen Generator, logger *slog.Logger, cfg RunnerConfig) *Runner {
	return &Runner{
		client:  client,
		reg:     reg,
		gen:     gen,
		logger:  logger,
		cfg:     cfg.withDefaults(),
		cancels: make(map[string]context.CancelFunc),
	}
}

// Stream returns the key of this worker's private job stream.
func (r *Runner) Stream() string { return r.cfg.WorkerStreamPrefix + r.cfg.ID }

// Run registers the worker and processes jobs until ctx is canceled. In-flight
// jobs are canceled with ctx and Run returns after they have stopped.
func (r *Runner) Run(ctx context.Context) error {
	self := registry.Worker{ID: r.cfg.ID, Models: r.cfg.Models}

	// Register before reading so the scheduler can route to this worker as soon
	// as it can accept work.
	if err := r.reg.Heartbeat(ctx, self); err != nil {
		return err
	}
	go r.heartbeatLoop(ctx, self)

	sub := r.client.PSubscribe(ctx, r.cfg.AbandonPrefix+"*")
	defer func() { _ = sub.Close() }()
	go r.abandonLoop(sub)

	r.logger.Info("worker started", "worker_id", r.cfg.ID, "stream", r.Stream())

	var wg sync.WaitGroup
	lastID := "0" // replay anything dispatched before (re)start
	for ctx.Err() == nil {
		res, err := r.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{r.Stream(), lastID},
			Count:   16,
			Block:   r.cfg.ReadBlock,
		}).Result()
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, redis.Nil) {
				r.logger.Error("read worker stream failed", "error", err)
				select {
				case <-ctx.Done():
				case <-time.After(100 * time.Millisecond):
				}
			}
			continue
		}
		for _, st := range res {
			for _, msg := range st.Messages {
				lastID = msg.ID
				job, ok := queue.JobFromValues(msg.Values)
				if !ok {
					r.logger.Error("skipping malformed job entry", "entry_id", msg.ID)
					r.finish(msg.ID)
					continue
				}
				jobCtx, cancel := context.WithCancel(ctx)
				r.mu.Lock()
				r.cancels[job.ID] = cancel
				r.mu.Unlock()

				wg.Add(1)
				go func() {
					defer wg.Done()
					r.process(jobCtx, job, msg.ID)
				}()
			}
		}
	}

	wg.Wait()

	// Remove the registration so the scheduler stops routing here immediately
	// instead of waiting for the heartbeat TTL.
	deregCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.reg.Deregister(deregCtx, r.cfg.ID); err != nil {
		r.logger.Warn("deregister failed", "error", err)
	}
	r.logger.Info("worker stopped", "worker_id", r.cfg.ID)
	return nil
}

func (r *Runner) heartbeatLoop(ctx context.Context, self registry.Worker) {
	t := time.NewTicker(r.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.reg.Heartbeat(ctx, self); err != nil && ctx.Err() == nil {
				r.logger.Warn("heartbeat failed", "error", err)
			}
		}
	}
}

func (r *Runner) abandonLoop(sub *redis.PubSub) {
	for msg := range sub.Channel() {
		jobID := strings.TrimPrefix(msg.Channel, r.cfg.AbandonPrefix)
		r.mu.Lock()
		cancel, ok := r.cancels[jobID]
		if ok {
			cancel()
			delete(r.cancels, jobID)
		}
		r.mu.Unlock()
		if ok {
			r.logger.Info("job abandoned", "job_id", jobID)
		}
	}
}

func (r *Runner) process(ctx context.Context, job queue.Job, entryID string) {
	defer func() {
		r.mu.Lock()
		delete(r.cancels, job.ID)
		r.mu.Unlock()
	}()

	err := r.gen.Run(ctx, job, func(c queue.Chunk) error {
		values := map[string]any{
			"job_id":     c.JobID,
			"text":       c.Text,
			"done":       strconv.FormatBool(c.Done),
			"tokens_in":  strconv.FormatInt(c.TokensIn, 10),
			"tokens_out": strconv.FormatInt(c.TokensOut, 10),
		}
		if c.Done {
			values["queue_wait_ms"] = strconv.FormatInt(c.QueueWaitMs, 10)
			values["duration_ms"] = strconv.FormatInt(c.DurationMs, 10)
		}
		return r.client.XAdd(ctx, &redis.XAddArgs{
			Stream: r.cfg.ChunksPrefix + job.ID,
			Values: values,
			MaxLen: 10000,
			Approx: true,
		}).Err()
	})
	if err != nil && ctx.Err() == nil {
		r.logger.Error("job generation failed", "job_id", job.ID, "error", err)
	}
	r.finish(entryID)
}

// finish deletes a processed entry from the worker's stream. The scheduler
// reads the stream length as this worker's load, so deletion is what frees the
// slot. It uses a fresh context so a job canceled by shutdown or abandon is
// still released.
func (r *Runner) finish(entryID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.client.XDel(ctx, r.Stream(), entryID).Err(); err != nil {
		r.logger.Warn("release job entry failed", "entry_id", entryID, "error", err)
	}
}
