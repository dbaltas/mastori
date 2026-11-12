package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"go.opentelemetry.io/otel/trace"

	"github.com/dbaltas/mastori/internal/metrics"
)

func RequestMetrics(m *metrics.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			route := chi.RouteContext(r.Context()).RoutePattern()
			if route == "" {
				route = "unmatched"
			}
			m.RequestsTotal.WithLabelValues(r.Method, route, http.StatusText(ww.Status())).
				Inc()
			m.RequestDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		})
	}
}

func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			attrs := []any{
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
			}
			if sc := trace.SpanContextFromContext(r.Context()); sc.IsValid() {
				attrs = append(attrs, "trace_id", sc.TraceID().String())
			}
			logger.InfoContext(r.Context(), "request handled", attrs...)
		})
	}
}
