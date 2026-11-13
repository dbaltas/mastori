package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dbaltas/mastori/internal/registry"
	"github.com/dbaltas/mastori/internal/worker"
)

type inferConfig struct {
	redisAddr            string
	workerStreamPrefix   string
	workerRegistryPrefix string
	chunksPrefix         string
	abandonPrefix        string
	workerID             string
	models               []string
	baseURL              string
	model                string
	maxTokens            int
	httpTimeout          time.Duration
	heartbeatMs          int
}

func loadInferConfig() (*inferConfig, error) {
	cfg := &inferConfig{
		redisAddr:            getenvOr("REDIS_ADDR", "localhost:6379"),
		workerStreamPrefix:   getenvOr("REDIS_WORKER_STREAM_PREFIX", registry.DefaultStreamPrefix),
		workerRegistryPrefix: getenvOr("REDIS_WORKER_REGISTRY_PREFIX", registry.DefaultPrefix),
		chunksPrefix:         getenvOr("REDIS_CHUNKS_PREFIX", "jobs:chunks:"),
		abandonPrefix:        getenvOr("REDIS_ABANDON_PREFIX", "jobs:abandon:"),
		workerID:             getenvOr("WORKER_ID", "inference-"+randomID()),
		baseURL:              getenvOr("INFERENCE_BASE_URL", "http://localhost:7082"),
		model:                getenvOr("INFERENCE_MODEL", ""),
		maxTokens:            0,
		httpTimeout:          10 * time.Minute,
		heartbeatMs:          1000,
	}
	if v := os.Getenv("WORKER_MODELS"); v != "" {
		for _, m := range strings.Split(v, ",") {
			if m = strings.TrimSpace(m); m != "" {
				cfg.models = append(cfg.models, m)
			}
		}
	}
	if v := os.Getenv("INFERENCE_MAX_TOKENS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid INFERENCE_MAX_TOKENS %q: must be a non-negative integer", v)
		}
		cfg.maxTokens = n
	}
	if v := os.Getenv("INFERENCE_HTTP_TIMEOUT_SECONDS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid INFERENCE_HTTP_TIMEOUT_SECONDS %q: must be a positive integer", v)
		}
		cfg.httpTimeout = time.Duration(n) * time.Second
	}
	if v := os.Getenv("WORKER_HEARTBEAT_MS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid WORKER_HEARTBEAT_MS %q: must be a positive integer", v)
		}
		cfg.heartbeatMs = n
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
	cfg, err := loadInferConfig()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	client := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer func() { _ = client.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	hb := time.Duration(cfg.heartbeatMs) * time.Millisecond
	reg := registry.New(client, cfg.workerRegistryPrefix, 3*hb)

	gen := &worker.OllamaGenerator{
		BaseURL:   cfg.baseURL,
		Model:     cfg.model,
		MaxTokens: cfg.maxTokens,
		Client: &http.Client{
			Timeout: cfg.httpTimeout,
		},
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
