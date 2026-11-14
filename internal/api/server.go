package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/dbaltas/mastori/internal/config"
	"github.com/dbaltas/mastori/internal/metrics"
)

type ReadinessChecker func(ctx context.Context) error

const (
	readyzTimeout   = 2 * time.Second
	serverSpanScope = "mastori-api"
)

func NewRouter(cfg *config.Config, m *metrics.Metrics, logger *slog.Logger, readiness ReadinessChecker, h *Handler) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(otelhttp.NewMiddleware(serverSpanScope, otelhttp.WithSpanNameFormatter(
		func(_ string, r *http.Request) string { return r.Method },
	)))
	r.Use(ServerRouteSpan())
	r.Use(RequestMetrics(m))
	r.Use(RequestLogger(logger))

	r.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyzTimeout)
		defer cancel()
		if err := readiness(ctx); err != nil {
			http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	r.Post("/generate", h.Generate)
	r.Post("/generate/stream", h.GenerateStream)

	return r
}
