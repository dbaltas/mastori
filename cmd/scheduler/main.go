package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/dbaltas/mastori/internal/registry"
	"github.com/dbaltas/mastori/internal/scheduler"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}
	logger := newLogger(cfg.logLevel)

	policy, err := scheduler.NewPolicy(cfg.policy)
	if err != nil {
		logger.Error("invalid policy", "error", err)
		os.Exit(1)
	}

	client := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer func() { _ = client.Close() }()

	m := scheduler.NewMetrics()
	reg := registry.New(client, cfg.registryPrefix, 0)
	s := scheduler.New(client, reg, policy, m, logger, scheduler.Config{
		Stream:             cfg.stream,
		Group:              cfg.group,
		ConsumerID:         cfg.consumerID,
		WorkerStreamPrefix: cfg.workerStreamPrefix,
		JobTTL:             cfg.jobTTL,
	})

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := client.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: cfg.listenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srvErr := make(chan error, 1)
	go func() {
		logger.Info("scheduler listening", "addr", cfg.listenAddr)
		srvErr <- srv.ListenAndServe()
	}()

	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	select {
	case err := <-runErr:
		if err != nil {
			logger.Error("scheduler failed", "error", err)
			os.Exit(1)
		}
	case err := <-srvErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "error", err)
	}
	logger.Info("scheduler stopped")
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
