package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
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
	cfg      *config.Config
	queue    queue.Queue
	m        *metrics.Metrics
	logger   *slog.Logger
	inflight atomic.Int64
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

	release, ok := h.acquire(w)
	if !ok {
		return
	}
	defer release()

	job, ok := h.parseAndEnqueue(w, r, ctx, span)
	if !ok {
		return
	}

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

func (h *Handler) GenerateStream(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "api.generate_stream")
	defer span.End()

	sq, ok := h.queue.(queue.StreamingQueue)
	if !ok {
		writeError(w, http.StatusNotImplemented, "streaming not supported by queue")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported by writer")
		return
	}

	release, ok := h.acquire(w)
	if !ok {
		return
	}
	defer release()

	job, ok := h.parseAndEnqueue(w, r, ctx, span)
	if !ok {
		return
	}

	waitCtx, cancel := context.WithTimeout(ctx, h.cfg.RequestTimeout)
	defer cancel()

	waitStart := time.Now()
	chunks, errCh := sq.ConsumeChunks(waitCtx, job.ID)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	completed := false
	firstToken := true

	failStream := func(err error, msg string) {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			span.SetAttributes(attribute.Bool("job.timed_out", true))
			msg = "request timed out waiting for result"
		} else {
			span.RecordError(err)
			h.logger.ErrorContext(ctx, "stream failed", "error", err, "job_id", job.ID)
		}
		_ = writeSSE(w, "error", map[string]string{"error": msg})
		flusher.Flush()
		_ = sq.Abandon(context.WithoutCancel(waitCtx), job.ID)
	}

	for !completed {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				select {
				case err := <-errCh:
					if err != nil {
						failStream(err, "failed to retrieve result")
						return
					}
				default:
				}
				completed = true
				break
			}
			if firstToken {
				h.m.TTFTSeconds.Observe(time.Since(waitStart).Seconds())
				firstToken = false
			}
			h.m.ChunksStreamed.Inc()
			if !writeSSE(w, "chunk", chunk) {
				_ = sq.Abandon(context.WithoutCancel(waitCtx), job.ID)
				return
			}
			flusher.Flush()
			if chunk.Done {
				span.SetAttributes(
					attribute.Int64("job.tokens_out", chunk.TokensOut),
					attribute.Int64("job.duration_ms", chunk.DurationMs),
				)
				completed = true
			}
		case err := <-errCh:
			failStream(err, "failed to retrieve result")
			return
		case <-waitCtx.Done():
			failStream(waitCtx.Err(), "request timed out waiting for result")
			return
		}
	}

	if _, err := fmt.Fprint(w, "event: done\ndata: [DONE]\n\n"); err != nil {
		return
	}
	flusher.Flush()
}

func (h *Handler) acquire(w http.ResponseWriter) (func(), bool) {
	for {
		cur := h.inflight.Load()
		if cur >= int64(h.cfg.MaxInflight) {
			h.m.BusyRejections.Inc()
			writeError(w, http.StatusTooManyRequests, "server busy: in-flight request cap reached")
			return nil, false
		}
		if h.inflight.CompareAndSwap(cur, cur+1) {
			h.m.Inflight.Inc()
			return func() {
				h.inflight.Add(-1)
				h.m.Inflight.Dec()
			}, true
		}
	}
}

func (h *Handler) parseAndEnqueue(w http.ResponseWriter, r *http.Request, ctx context.Context, span trace.Span) (queue.Job, bool) {
	var req generateRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return queue.Job{}, false
	}
	if req.Prompt == "" {
		writeError(w, http.StatusBadRequest, "prompt is required")
		return queue.Job{}, false
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
		return queue.Job{}, false
	}
	h.m.EnqueueSeconds.Observe(time.Since(enqStart).Seconds())
	h.m.JobsEnqueued.Inc()

	return job, true
}

func writeSSE(w http.ResponseWriter, event string, payload any) bool {
	b, err := json.Marshal(payload)
	if err != nil {
		return false
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b); err != nil {
		return false
	}
	return true
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
