package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dbaltas/mastori/internal/queue"
	"github.com/dbaltas/mastori/internal/worker"
)

type mockConfig struct {
	redisAddr     string
	stream        string
	chunksPrefix  string
	abandonPrefix string
	group         string
	workerID      string
	minTokens     int
	maxTokens     int
	tokenDelayMs  int
	firstDelayMs  int
}

func loadMockConfig() (*mockConfig, error) {
	cfg := &mockConfig{
		redisAddr:     getenvOr("REDIS_ADDR", "localhost:6379"),
		stream:        getenvOr("REDIS_STREAM", "jobs"),
		chunksPrefix:  getenvOr("REDIS_CHUNKS_PREFIX", "jobs:chunks:"),
		abandonPrefix: getenvOr("REDIS_ABANDON_PREFIX", "jobs:abandon:"),
		group:         getenvOr("WORKER_CONSUMER_GROUP", "mastori-workers"),
		workerID:      getenvOr("WORKER_ID", "mock-"+randomID()),
		minTokens:     16,
		maxTokens:     96,
		tokenDelayMs:  20,
		firstDelayMs:  50,
	}
	for _, f := range []struct {
		key    string
		target *int
	}{
		{"WORKER_MIN_TOKENS", &cfg.minTokens},
		{"WORKER_MAX_TOKENS", &cfg.maxTokens},
		{"WORKER_TOKEN_DELAY_MS", &cfg.tokenDelayMs},
		{"WORKER_FIRST_DELAY_MS", &cfg.firstDelayMs},
	} {
		if v := os.Getenv(f.key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return nil, fmt.Errorf("invalid %s %q: must be a non-negative integer", f.key, v)
			}
			*f.target = n
		}
	}
	return cfg, nil
}

func getenvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func randomID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "0"
	}
	return hex.EncodeToString(b)
}

func main() {
	cfg, err := loadMockConfig()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	client := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer func() { _ = client.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := ensureGroup(ctx, client, cfg.stream, cfg.group); err != nil {
		logger.Error("ensure consumer group failed", "error", err)
		os.Exit(1)
	}

	gen := &worker.MockGenerator{
		MinTokens:  cfg.minTokens,
		MaxTokens:  cfg.maxTokens,
		TokenDelay: time.Duration(cfg.tokenDelayMs) * time.Millisecond,
		FirstDelay: time.Duration(cfg.firstDelayMs) * time.Millisecond,
	}

	var mu sync.Mutex
	cancels := make(map[string]context.CancelFunc)

	sub := client.PSubscribe(ctx, cfg.abandonPrefix+"*")
	go func() {
		for msg := range sub.Channel() {
			jobID := strings.TrimPrefix(msg.Channel, cfg.abandonPrefix)
			mu.Lock()
			if cancel, ok := cancels[jobID]; ok {
				cancel()
				delete(cancels, jobID)
			}
			mu.Unlock()
			logger.Info("job abandoned", "job_id", jobID)
		}
	}()

	var wg sync.WaitGroup
	logger.Info("mock worker started", "worker_id", cfg.workerID, "stream", cfg.stream, "group", cfg.group)

	for {
		res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    cfg.group,
			Consumer: cfg.workerID,
			Streams:  []string{cfg.stream, ">"},
			Count:    1,
			Block:    time.Second,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			continue
		}

		for _, st := range res {
			for _, msg := range st.Messages {
				job, ok := jobFromValues(msg.Values)
				if !ok {
					logger.Error("skipping malformed job entry", "entry_id", msg.ID)
					_ = client.XAck(context.Background(), cfg.stream, cfg.group, msg.ID).Err()
					continue
				}

				jobCtx, cancel := context.WithCancel(ctx)
				mu.Lock()
				cancels[job.ID] = cancel
				mu.Unlock()

				wg.Add(1)
				go func(job queue.Job, entryID string) {
					defer wg.Done()
					defer func() {
						mu.Lock()
						delete(cancels, job.ID)
						mu.Unlock()
					}()

					err := gen.Run(jobCtx, job, func(c queue.Chunk) error {
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
						return client.XAdd(jobCtx, &redis.XAddArgs{
							Stream: cfg.chunksPrefix + job.ID,
							Values: values,
							MaxLen: 10000,
							Approx: true,
						}).Err()
					})

					if err != nil && jobCtx.Err() == nil {
						logger.Error("job generation failed", "job_id", job.ID, "error", err)
					}
					_ = client.XAck(context.Background(), cfg.stream, cfg.group, entryID).Err()
				}(job, msg.ID)
			}
		}
	}

	wg.Wait()
	_ = sub.Close()
	logger.Info("mock worker stopped")
}

func ensureGroup(ctx context.Context, client *redis.Client, stream, group string) error {
	err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func jobFromValues(values map[string]any) (queue.Job, bool) {
	jobID, _ := values["job_id"].(string)
	prompt, _ := values["prompt"].(string)
	model, _ := values["model"].(string)
	traceID, _ := values["trace_id"].(string)
	createdAtStr, _ := values["created_at"].(string)
	if jobID == "" || prompt == "" {
		return queue.Job{}, false
	}
	createdAt, err := time.Parse(time.RFC3339Nano, createdAtStr)
	if err != nil {
		createdAt = time.Now().UTC()
	}
	return queue.Job{
		ID:        jobID,
		TraceID:   traceID,
		Model:     model,
		Prompt:    prompt,
		CreatedAt: createdAt,
	}, true
}
