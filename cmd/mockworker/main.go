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
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dbaltas/mastori/internal/registry"
	"github.com/dbaltas/mastori/internal/tracing"
	"github.com/dbaltas/mastori/internal/worker"
)

type mockConfig struct {
	redisAddr            string
	workerStreamPrefix   string
	workerRegistryPrefix string
	chunksPrefix         string
	abandonPrefix        string
	workerID             string
	models               []string
	minTokens            int
	maxTokens            int
	tokenDelayMs         int
	firstDelayMs         int
	tokenCountJitter     float64
	tokenDelayJitter     float64
	firstDelayJitter     float64
	heartbeatMs          int
}

func loadMockConfig() (*mockConfig, error) {
	cfg := &mockConfig{
		redisAddr:            getenvOr("REDIS_ADDR", "localhost:6379"),
		workerStreamPrefix:   getenvOr("REDIS_WORKER_STREAM_PREFIX", registry.DefaultStreamPrefix),
		workerRegistryPrefix: getenvOr("REDIS_WORKER_REGISTRY_PREFIX", registry.DefaultPrefix),
		chunksPrefix:         getenvOr("REDIS_CHUNKS_PREFIX", "jobs:chunks:"),
		abandonPrefix:        getenvOr("REDIS_ABANDON_PREFIX", "jobs:abandon:"),
		workerID:             getenvOr("WORKER_ID", "mock-"+randomID()),
		minTokens:            16,
		maxTokens:            96,
		tokenDelayMs:         20,
		firstDelayMs:         50,
		tokenCountJitter:     0.25,
		tokenDelayJitter:     0.30,
		firstDelayJitter:     0.30,
		heartbeatMs:          1000,
	}
	if v := os.Getenv("WORKER_MODELS"); v != "" {
		for _, m := range strings.Split(v, ",") {
			if m = strings.TrimSpace(m); m != "" {
				cfg.models = append(cfg.models, m)
			}
		}
	}
	for _, f := range []struct {
		key    string
		target *int
	}{
		{"WORKER_MIN_TOKENS", &cfg.minTokens},
		{"WORKER_MAX_TOKENS", &cfg.maxTokens},
		{"WORKER_TOKEN_DELAY_MS", &cfg.tokenDelayMs},
		{"WORKER_FIRST_DELAY_MS", &cfg.firstDelayMs},
		{"WORKER_HEARTBEAT_MS", &cfg.heartbeatMs},
	} {
		if v := os.Getenv(f.key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return nil, fmt.Errorf("invalid %s %q: must be a non-negative integer", f.key, v)
			}
			*f.target = n
		}
	}
	for _, f := range []struct {
		key    string
		target *float64
	}{
		{"WORKER_TOKEN_COUNT_JITTER", &cfg.tokenCountJitter},
		{"WORKER_TOKEN_DELAY_JITTER", &cfg.tokenDelayJitter},
		{"WORKER_FIRST_DELAY_JITTER", &cfg.firstDelayJitter},
	} {
		if v := os.Getenv(f.key); v != "" {
			n, err := strconv.ParseFloat(v, 64)
			if err != nil || n < 0 {
				return nil, fmt.Errorf("invalid %s %q: must be a non-negative number (fraction of the base value)", f.key, v)
			}
			if n > 1 {
				return nil, fmt.Errorf("invalid %s %q: must be <= 1", f.key, v)
			}
			*f.target = n
		}
	}
	if cfg.heartbeatMs == 0 {
		return nil, fmt.Errorf("invalid WORKER_HEARTBEAT_MS: must be positive")
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

	tracerShutdown, err := tracing.SetupFromEnv(context.Background(), "mastori-worker-mock")
	if err != nil {
		logger.Error("tracing setup failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracerShutdown(shutdownCtx); err != nil {
			logger.Warn("tracer shutdown error", "error", err)
		}
	}()

	client := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer func() { _ = client.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hb := time.Duration(cfg.heartbeatMs) * time.Millisecond
	reg := registry.New(client, cfg.workerRegistryPrefix, 3*hb)

	gen := &worker.MockGenerator{
		MinTokens:        cfg.minTokens,
		MaxTokens:        cfg.maxTokens,
		TokenDelay:       time.Duration(cfg.tokenDelayMs) * time.Millisecond,
		FirstDelay:       time.Duration(cfg.firstDelayMs) * time.Millisecond,
		TokenCountJitter: cfg.tokenCountJitter,
		TokenDelayJitter: cfg.tokenDelayJitter,
		FirstDelayJitter: cfg.firstDelayJitter,
	}

	runner := worker.NewRunner(client, reg, gen, logger, worker.RunnerConfig{
		ID:                 cfg.workerID,
		Models:             cfg.models,
		WorkerStreamPrefix: cfg.workerStreamPrefix,
		ChunksPrefix:       cfg.chunksPrefix,
		AbandonPrefix:      cfg.abandonPrefix,
		HeartbeatInterval:  hb,
	})
	if err := runner.Run(ctx); err != nil {
		logger.Error("worker failed", "error", err)
		os.Exit(1)
	}
}
