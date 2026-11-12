package queue

import (
	"context"
	"time"
)

type Job struct {
	ID        string
	TraceID   string
	Model     string
	Prompt    string
	CreatedAt time.Time
}

type Result struct {
	JobID       string `json:"job_id"`
	Text        string `json:"text"`
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
