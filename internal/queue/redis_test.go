package queue

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestQueue(t *testing.T) (*RedisQueue, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	q := NewRedisQueue(mr.Addr(), "jobs", "jobs:chunks:", "jobs:abandon:")
	t.Cleanup(func() { _ = q.Close() })
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return q, mr, client
}

func TestEnqueueWritesJobFields(t *testing.T) {
	q, _, client := newTestQueue(t)

	job := Job{
		ID:        "job-1",
		TraceID:   "trace-abc",
		Model:     "m1",
		Prompt:    "hello world",
		CreatedAt: time.Now().UTC(),
	}

	if err := q.Enqueue(context.Background(), job); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	msgs, err := client.XRange(context.Background(), "jobs", "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("stream length = %d, want 1", len(msgs))
	}

	vals := msgs[0].Values
	if vals["job_id"] != "job-1" || vals["trace_id"] != "trace-abc" || vals["model"] != "m1" || vals["prompt"] != "hello world" {
		t.Errorf("stream fields = %v", vals)
	}
	if _, ok := vals["created_at"]; !ok {
		t.Error("created_at missing from stream entry")
	}
}

func TestWaitResultAggregatesChunks(t *testing.T) {
	q, _, client := newTestQueue(t)

	resCh := make(chan Result, 1)
	errCh := make(chan error, 1)
	go func() {
		res, err := q.WaitResult(context.Background(), "job-1")
		if err != nil {
			errCh <- err
			return
		}
		resCh <- res
	}()

	time.Sleep(50 * time.Millisecond)

	for _, c := range []struct {
		text string
		done bool
	}{
		{"hel", false},
		{"lo ", false},
		{"world", false},
		{"", true},
	} {
		if err := client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: "jobs:chunks:job-1",
			Values: map[string]any{
				"job_id":        "job-1",
				"text":          c.text,
				"done":          strconv.FormatBool(c.done),
				"tokens_in":     "3",
				"tokens_out":    "10",
				"queue_wait_ms": "5",
				"duration_ms":   "42",
			},
		}).Err(); err != nil {
			t.Fatalf("xadd: %v", err)
		}
	}

	select {
	case res := <-resCh:
		if res.Text != "hello world" || res.TokensIn != 3 || res.TokensOut != 10 || res.DurationMs != 42 {
			t.Errorf("result = %+v", res)
		}
		if res.JobID != "job-1" {
			t.Errorf("job_id = %q, want job-1", res.JobID)
		}
	case err := <-errCh:
		t.Fatalf("wait: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

func TestWaitResultTimeout(t *testing.T) {
	q, _, _ := newTestQueue(t)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := q.WaitResult(ctx, "job-3")
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestWaitResultMalformedChunk(t *testing.T) {
	q, _, client := newTestQueue(t)

	errCh := make(chan error, 1)
	go func() {
		_, err := q.WaitResult(context.Background(), "job-4")
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	if err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "jobs:chunks:job-4",
		Values: map[string]any{"job_id": "job-4", "text": "x", "done": "maybe"},
	}).Err(); err != nil {
		t.Fatalf("xadd: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected malformed payload error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

func xaddChunk(t *testing.T, client *redis.Client, jobID, text string, done bool) {
	t.Helper()
	if err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "jobs:chunks:" + jobID,
		Values: map[string]any{
			"job_id":     jobID,
			"text":       text,
			"done":       strconv.FormatBool(done),
			"tokens_in":  "2",
			"tokens_out": "5",
		},
	}).Err(); err != nil {
		t.Fatalf("xadd chunk: %v", err)
	}
}

func TestConsumeChunksReadsHistoryAndLive(t *testing.T) {
	q, _, client := newTestQueue(t)

	xaddChunk(t, client, "job-s1", "hel", false)

	chunks, errCh := q.ConsumeChunks(context.Background(), "job-s1")

	xaddChunk(t, client, "job-s1", "lo", false)
	xaddChunk(t, client, "job-s1", "", true)

	var got []Chunk
	for chunk := range chunks {
		got = append(got, chunk)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	default:
	}

	if len(got) != 3 {
		t.Fatalf("chunks = %d, want 3", len(got))
	}
	if got[0].Text != "hel" || got[1].Text != "lo" {
		t.Errorf("chunk texts = %q, %q", got[0].Text, got[1].Text)
	}
	if !got[2].Done || got[2].TokensOut != 5 {
		t.Errorf("final chunk = %+v", got[2])
	}
}

func TestConsumeChunksContextCancel(t *testing.T) {
	q, _, _ := newTestQueue(t)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, errCh := q.ConsumeChunks(ctx, "job-s2")

	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error = %v, want deadline exceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no error after cancel")
	}
}

func TestConsumeChunksMalformedField(t *testing.T) {
	q, _, client := newTestQueue(t)

	err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: "jobs:chunks:job-s3",
		Values: map[string]any{"job_id": "job-s3", "text": "x", "done": "maybe"},
	}).Err()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}

	chunks, errCh := q.ConsumeChunks(context.Background(), "job-s3")

	select {
	case <-chunks:
	case <-time.After(2 * time.Second):
		t.Fatal("no chunk delivered")
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected malformed chunk error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no error for malformed chunk")
	}
}

func TestAbandonPublishes(t *testing.T) {
	q, _, client := newTestQueue(t)

	pubCh := client.Subscribe(context.Background(), "jobs:abandon:job-a")
	defer pubCh.Close()

	if err := q.Abandon(context.Background(), "job-a"); err != nil {
		t.Fatalf("abandon: %v", err)
	}

	mctx, mcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer mcancel()
	msg, err := pubCh.ReceiveMessage(mctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if msg.Payload != "job-a" {
		t.Errorf("payload = %q, want job-a", msg.Payload)
	}
}
