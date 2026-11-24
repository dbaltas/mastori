package queue

import (
	"context"
	"time"
)

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
	JobID       string `json:"job_id"`
	Text        string `json:"text"`
	Done        bool   `json:"done"`
	TokensIn    int64  `json:"tokens_in"`
	TokensOut   int64  `json:"tokens_out"`
	QueueWaitMs int64  `json:"queue_wait_ms"`
	DurationMs  int64  `json:"duration_ms"`
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
