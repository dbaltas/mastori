package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxStreamLen = 100000

type RedisQueue struct {
	client       *redis.Client
	stream       string
	resultPrefix string
}

func NewRedisQueue(addr, stream, resultPrefix string) *RedisQueue {
	return &RedisQueue{
		client: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
		stream:       stream,
		resultPrefix: resultPrefix,
	}
}

func (q *RedisQueue) Enqueue(ctx context.Context, job Job) error {
	return q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.stream,
		Values: map[string]any{
			"job_id":     job.ID,
			"trace_id":   job.TraceID,
			"model":      job.Model,
			"prompt":     job.Prompt,
			"created_at": job.CreatedAt.UTC().Format(time.RFC3339Nano),
		},
		MaxLen: maxStreamLen,
		Approx: true,
	}).Err()
}

func (q *RedisQueue) WaitResult(ctx context.Context, jobID string) (Result, error) {
	channel := q.resultPrefix + jobID
	key := channel

	sub := q.client.Subscribe(ctx, channel)
	defer sub.Close()

	if val, err := q.client.Get(ctx, key).Result(); err == nil {
		return parseResult(val)
	}

	select {
	case msg, ok := <-sub.Channel():
		if !ok {
			return Result{}, fmt.Errorf("queue: subscription closed for job %s", jobID)
		}
		return parseResult(msg.Payload)
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func (q *RedisQueue) Ping(ctx context.Context) error {
	return q.client.Ping(ctx).Err()
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}

func parseResult(payload string) (Result, error) {
	var res Result
	if err := json.Unmarshal([]byte(payload), &res); err != nil {
		return Result{}, fmt.Errorf("queue: malformed result payload: %w", err)
	}
	return res, nil
}
