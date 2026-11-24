package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dbaltas/mastori/internal/api"
	"github.com/dbaltas/mastori/internal/config"
	"github.com/dbaltas/mastori/internal/metrics"
	"github.com/dbaltas/mastori/internal/queue"
	"github.com/dbaltas/mastori/internal/tracing"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	logger := newLogger(cfg.LogLevel)

	tracerShutdown, err := tracing.Setup(context.Background(), "mastori-api", cfg.OTLPEndpoint, cfg.SamplerRatio)
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

	q := queue.NewRedisQueue(cfg.RedisAddr, cfg.RedisStream, cfg.ChunksPrefix, cfg.AbandonPrefix, cfg.RedisPoolSize)
	defer func() {
		if err := q.Close(); err != nil {
			logger.Warn("redis close error", "error", err)
		}
	}()

	m := metrics.New()

	h := api.NewHandler(cfg, q, m, logger)

	// BaseContext is the root of every request context: canceling it unblocks
	// in-flight SSE/generate handlers when the drain window closes, so Redis
	// is only closed after the handlers have stopped.
	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.NewRouter(cfg, m, logger, q.Ping, h),
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return rootCtx
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("api listening", "addr", cfg.ListenAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal received")
		// Drain window: long enough for in-flight requests to finish within
		// their own RequestTimeout, instead of cutting healthy streams.
		grace := cfg.RequestTimeout + 5*time.Second
		shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("graceful shutdown timed out; canceling in-flight requests", "error", err, "grace", grace)
		}
		// Unblock handlers still waiting so the deferred q.Close() below does
		// not close Redis under live handlers.
		cancelRoot()
	}
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
