package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dbaltas/mastori/internal/config"
	"github.com/dbaltas/mastori/internal/metrics"
	"github.com/dbaltas/mastori/internal/queue"
)

const maxBodyBytes = 1 << 20

var tracer = otel.Tracer("mastori/api")

type Handler struct {
	cfg    *config.Config
	queue  queue.Queue
	m      *metrics.Metrics
	logger *slog.Logger
}

func NewHandler(cfg *config.Config, q queue.Queue, m *metrics.Metrics, logger *slog.Logger) *Handler {
	return &Handler{cfg: cfg, queue: q, m: m, logger: logger}
}

type generateRequest struct {
	Prompt string `json:"prompt"`
	Model  string `json:"model"`
}

type generateResponse struct {
	JobID     string `json:"job_id"`
	Text      string `json:"text"`
	TokensIn  int64  `json:"tokens_in"`
	TokensOut int64  `json:"tokens_out"`
	TotalMs   int64  `json:"total_ms"`
}

func (h *Handler) Generate(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "api.generate")
	defer span.End()

	var req generateRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Prompt == "" {
		writeError(w, http.StatusBadRequest, "prompt is required")
		return
	}
	model := req.Model
	if model == "" {
		model = h.cfg.DefaultModel
	}

	span.SetAttributes(
		attribute.String("job.model", model),
		attribute.Int("job.prompt_len", len(req.Prompt)),
	)

	job := queue.Job{
		ID:        newJobID(),
		TraceID:   traceIDFromContext(ctx),
		Model:     model,
		Prompt:    req.Prompt,
		CreatedAt: time.Now().UTC(),
	}
	span.SetAttributes(attribute.String("job.id", job.ID))

	enqStart := time.Now()
	if err := h.queue.Enqueue(ctx, job); err != nil {
		h.m.EnqueueErrors.Inc()
		span.RecordError(err)
		h.logger.ErrorContext(ctx, "enqueue failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "queue unavailable")
		return
	}
	h.m.EnqueueSeconds.Observe(time.Since(enqStart).Seconds())
	h.m.JobsEnqueued.Inc()

	h.m.Inflight.Inc()
	defer h.m.Inflight.Dec()

	waitCtx, cancel := context.WithTimeout(ctx, h.cfg.RequestTimeout)
	defer cancel()

	waitStart := time.Now()
	res, err := h.queue.WaitResult(waitCtx, job.ID)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			span.SetAttributes(attribute.Bool("job.timed_out", true))
			writeError(w, http.StatusGatewayTimeout, "request timed out waiting for result")
			return
		}
		span.RecordError(err)
		h.logger.ErrorContext(ctx, "wait failed", "error", err, "job_id", job.ID)
		writeError(w, http.StatusServiceUnavailable, "failed to retrieve result")
		return
	}

	span.SetAttributes(
		attribute.Int64("job.queue_wait_ms", res.QueueWaitMs),
		attribute.Int64("job.duration_ms", res.DurationMs),
	)

	writeJSON(w, http.StatusOK, generateResponse{
		JobID:     res.JobID,
		Text:      res.Text,
		TokensIn:  res.TokensIn,
		TokensOut: res.TokensOut,
		TotalMs:   time.Since(waitStart).Milliseconds(),
	})
}

func newJobID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return time.Now().UTC().Format("20060102T150405.000000000")
	}
	return hex.EncodeToString(b)
}

func traceIDFromContext(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return sc.TraceID().String()
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
