package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dbaltas/mastori/internal/fakegpu"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg, err := fakegpu.ConfigFromEnv(os.Getenv)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	client, err := fakegpu.NewInCluster()
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	client.Logger = logger

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("fakegpu starting", "node", cfg.NodeName, "count", cfg.Count, "interval", cfg.Interval)
	if err := client.Run(ctx, cfg.NodeName, cfg.Count, cfg.Interval); err != nil {
		logger.Error("fakegpu stopped", "error", err)
		os.Exit(1)
	}
}
