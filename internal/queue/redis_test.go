package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestQueue(t *testing.T) (*RedisQueue, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	q := NewRedisQueue(mr.Addr(), "jobs", "jobs:result:")
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

func TestWaitResultReceivesPublishedResult(t *testing.T) {
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

	payload := `{"job_id":"job-1","text":"answer","tokens_in":3,"tokens_out":10,"queue_wait_ms":5,"duration_ms":42}`
	if err := client.Publish(context.Background(), "jobs:result:job-1", payload).Err(); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case res := <-resCh:
		if res.Text != "answer" || res.TokensIn != 3 || res.TokensOut != 10 || res.DurationMs != 42 {
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

func TestWaitResultFallbackKey(t *testing.T) {
	q, _, client := newTestQueue(t)

	if err := client.Set(context.Background(), "jobs:result:job-2",
		`{"job_id":"job-2","text":"stored"}`, time.Minute).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := q.WaitResult(ctx, "job-2")
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if res.Text != "stored" {
		t.Errorf("text = %q, want %q", res.Text, "stored")
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

func TestWaitResultMalformedPayload(t *testing.T) {
	q, _, client := newTestQueue(t)

	errCh := make(chan error, 1)
	go func() {
		_, err := q.WaitResult(context.Background(), "job-4")
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	if err := client.Publish(context.Background(), "jobs:result:job-4", "not-json").Err(); err != nil {
		t.Fatalf("publish: %v", err)
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
