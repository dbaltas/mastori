package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/dbaltas/mastori/internal/adapter"
)

func getenvOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	srv := &adapter.Server{
		Ollama: &adapter.OllamaClient{
			BaseURL: getenvOr("OLLAMA_BASE_URL", "http://localhost:11434"),
			HTTP:    &http.Client{Timeout: 10 * time.Minute},
		},
		DefaultModel: getenvOr("DEFAULT_MODEL", ""),
		Metrics:      adapter.NewMetrics(),
		Logger:       logger,
	}

	addr := getenvOr("LISTEN_ADDR", ":8080")
	logger.Info("inference adapter started", "addr", addr, "ollama_base_url", srv.Ollama.BaseURL, "default_model", srv.DefaultModel)
	if err := http.ListenAndServe(addr, adapter.NewRouter(srv)); err != nil {
		logger.Error("adapter server failed", "error", err)
		os.Exit(1)
	}
}
