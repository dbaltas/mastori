package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/dbaltas/mastori/internal/metrics"
)

// ServerRouteSpan enriches the otelhttp server span with the matched chi
// route pattern: it sets the http.route attribute and names the span
// "METHOD /route" so traces group by endpoint. The pattern is only known
// once the request has passed chi routing, so it is read after the handler.
func ServerRouteSpan() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
			if route := chi.RouteContext(r.Context()).RoutePattern(); route != "" {
				if span := trace.SpanFromContext(r.Context()); span.IsRecording() {
					span.SetName(r.Method + " " + route)
					span.SetAttributes(semconv.HTTPRouteKey.String(route))
				}
			}
		})
	}
}

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
