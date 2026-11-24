package queue

import (
	"context"
	"errors"
	"time"
)

// ErrJobFailed reports that a job terminated with an error on the worker
// side (backend failure, malformed backend response, publish failure).
// The underlying message carries the worker's reason.
var ErrJobFailed = errors.New("job failed")

type Job struct {
	ID      string
	TraceID string
	// Traceparent/Tracestate carry the W3C trace context so a consumer in
	// another process (scheduler, worker) can continue the same trace.
	Traceparent string
	Tracestate  string
	Model       string
	Prompt      string
	CreatedAt   time.Time
	// MaxTokens caps output tokens for this job; 0 leaves the backend's
	// default in place (the worker's INFERENCE_MAX_TOKENS, then the
	// backend's own default).
	MaxTokens int64
	// IgnoreEOS keeps the backend generating until MaxTokens is reached,
	// so a benchmark run measures a known output length instead of the
	// model's natural stopping point.
	IgnoreEOS bool
	// Temperature pins sampling when set; nil leaves the backend default.
	Temperature *float64
}

type Result struct {
	JobID       string `json:"job_id"`
	Text        string `json:"text"`
	TokensIn    int64  `json:"tokens_in"`
	TokensOut   int64  `json:"tokens_out"`
	QueueWaitMs int64  `json:"queue_wait_ms"`
	DurationMs  int64  `json:"duration_ms"`
}

type Chunk struct {
	JobID string `json:"job_id"`
	Text  string `json:"text"`
	Done  bool   `json:"done"`
	// Error, when non-empty, marks a terminal error chunk: the job failed on
	// the worker and will never produce output. Done is also true.
	Error     string `json:"error,omitempty"`
	TokensIn  int64  `json:"tokens_in"`
	TokensOut int64  `json:"tokens_out"`
	// TokensSource records where the token counts came from: "usage" when
	// the backend reported them, "estimated" when they were derived (e.g.
	// a word count). Estimated counts are invalid for tok/s claims.
	TokensSource string `json:"tokens_source,omitempty"`
	QueueWaitMs  int64  `json:"queue_wait_ms"`
	DurationMs   int64  `json:"duration_ms"`
}

type Queue interface {
	Enqueue(ctx context.Context, job Job) error
	WaitResult(ctx context.Context, jobID string) (Result, error)
	Ping(ctx context.Context) error
	Close() error
}

type StreamingQueue interface {
	Queue
	ConsumeChunks(ctx context.Context, jobID string) (<-chan Chunk, <-chan error)
	Abandon(ctx context.Context, jobID string) error
}
