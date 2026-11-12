package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	maxStreamLen     = 100000
	chunkBlockPeriod = 250 * time.Millisecond
)

type RedisQueue struct {
	client        *redis.Client
	stream        string
	resultPrefix  string
	chunksPrefix  string
	abandonPrefix string
}

func NewRedisQueue(addr, stream, resultPrefix, chunksPrefix, abandonPrefix string) *RedisQueue {
	return &RedisQueue{
		client: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
		stream:        stream,
		resultPrefix:  resultPrefix,
		chunksPrefix:  chunksPrefix,
		abandonPrefix: abandonPrefix,
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

func (q *RedisQueue) ConsumeChunks(ctx context.Context, jobID string) (<-chan Chunk, <-chan error) {
	chunks := make(chan Chunk)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunks)

		streamKey := q.chunksPrefix + jobID
		lastID := "0"

		for {
			if ctx.Err() != nil {
				errCh <- ctx.Err()
				return
			}

			res, err := q.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{streamKey, lastID},
				Block:   chunkBlockPeriod,
				Count:   100,
			}).Result()

			if errors.Is(err, redis.Nil) {
				continue
			}
			if err != nil {
				errCh <- err
				return
			}

			for _, st := range res {
				for _, msg := range st.Messages {
					lastID = msg.ID
					chunk, perr := chunkFromValues(msg.Values)
					if perr != nil {
						errCh <- perr
						return
					}
					select {
					case chunks <- chunk:
					case <-ctx.Done():
						errCh <- ctx.Err()
						return
					}
					if chunk.Done {
						return
					}
				}
			}
		}
	}()

	return chunks, errCh
}

func (q *RedisQueue) Abandon(ctx context.Context, jobID string) error {
	return q.client.Publish(ctx, q.abandonPrefix+jobID, jobID).Err()
}

func chunkFromValues(values map[string]any) (Chunk, error) {
	var c Chunk
	c.JobID = stringValue(values["job_id"])
	c.Text = stringValue(values["text"])
	if v := values["done"]; v != nil {
		done, err := strconv.ParseBool(stringValue(v))
		if err != nil {
			return Chunk{}, fmt.Errorf("queue: malformed chunk 'done' field: %w", err)
		}
		c.Done = done
	}
	for _, f := range []struct {
		key    string
		target *int64
	}{
		{"tokens_in", &c.TokensIn},
		{"tokens_out", &c.TokensOut},
		{"queue_wait_ms", &c.QueueWaitMs},
		{"duration_ms", &c.DurationMs},
	} {
		if v := values[f.key]; v != nil {
			n, err := strconv.ParseInt(stringValue(v), 10, 64)
			if err != nil {
				return Chunk{}, fmt.Errorf("queue: malformed chunk %q field: %w", f.key, err)
			}
			*f.target = n
		}
	}
	return c, nil
}

func stringValue(v any) string {
	s, _ := v.(string)
	return s
}

func parseResult(payload string) (Result, error) {
	var res Result
	if err := json.Unmarshal([]byte(payload), &res); err != nil {
		return Result{}, fmt.Errorf("queue: malformed result payload: %w", err)
	}
	return res, nil
}
