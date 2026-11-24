package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/dbaltas/mastori/internal/queue"
	"github.com/dbaltas/mastori/internal/registry"
)

type runnerHarness struct {
	t      *testing.T
	mr     *miniredis.Miniredis
	client *redis.Client
	reg    *registry.Registry
	q      *queue.RedisQueue
}

func newRunnerHarness(t *testing.T) *runnerHarness {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	q := queue.NewRedisQueue(mr.Addr(), "jobs", "jobs:chunks:", "jobs:abandon:", 16)
	t.Cleanup(func() { _ = q.Close() })
	return &runnerHarness{t: t, mr: mr, client: client, q: q, reg: registry.New(client, "", time.Minute)}
}

func (h *runnerHarness) startGen(id string, gen Generator, models ...string) (stop func()) {
	h.t.Helper()
	r := NewRunner(h.client, h.reg, gen, slog.New(slog.DiscardHandler), RunnerConfig{
		ID: id, Models: models, ReadBlock: 20 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	var stopped bool
	stop = func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	h.t.Cleanup(stop)
	return stop
}

func (h *runnerHarness) start(id string, gen *MockGenerator, models ...string) (stop func()) {
	return h.startGen(id, gen, models...)
}

// dispatch places a job on the worker's stream the way the scheduler does.
func (h *runnerHarness) dispatch(workerID string, job queue.Job) {
	h.t.Helper()
	err := h.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: registry.DefaultStreamPrefix + workerID, Values: queue.JobValues(job),
	}).Err()
	if err != nil {
		h.t.Fatal(err)
	}
}

func fastGen(tokens int) *MockGenerator {
	return &MockGenerator{MinTokens: tokens, MaxTokens: tokens, TokenDelay: time.Millisecond, FirstDelay: time.Millisecond}
}

func chunkTTL(t *testing.T, h *runnerHarness, jobID string) time.Duration {
	t.Helper()
	d, err := h.client.TTL(context.Background(), "jobs:chunks:"+jobID).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	return d
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRunnerRegistersAndDeregisters(t *testing.T) {
	h := newRunnerHarness(t)
	stop := h.start("w1", fastGen(1), "llama")

	waitFor(t, "registration", func() bool {
		ws, _ := h.reg.List(context.Background())
		return len(ws) == 1 && ws[0].ID == "w1" && ws[0].Models[0] == "llama"
	})

	stop()
	ws, _ := h.reg.List(context.Background())
	if len(ws) != 0 {
		t.Errorf("worker still registered after shutdown: %+v", ws)
	}
}

func TestRunnerProcessesDispatchedJob(t *testing.T) {
	h := newRunnerHarness(t)
	h.start("w1", fastGen(3))
	h.dispatch("w1", queue.Job{ID: "job-1", Prompt: "hello world", CreatedAt: time.Now().UTC()})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := h.q.WaitResult(ctx, "job-1")
	if err != nil {
		t.Fatalf("wait result: %v", err)
	}
	if res.TokensOut != 3 || res.TokensIn != 2 {
		t.Errorf("result = %+v", res)
	}

	waitFor(t, "stream entry released", func() bool {
		n, _ := h.client.XLen(context.Background(), registry.DefaultStreamPrefix+"w1").Result()
		return n == 0
	})
}

func TestRunnerReplaysJobsDispatchedBeforeStart(t *testing.T) {
	h := newRunnerHarness(t)
	h.dispatch("w1", queue.Job{ID: "early", Prompt: "p", CreatedAt: time.Now().UTC()})

	h.start("w1", fastGen(2))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := h.q.WaitResult(ctx, "early"); err != nil {
		t.Fatalf("job dispatched before start was not processed: %v", err)
	}
}

func TestRunnerIgnoresOtherWorkersStreams(t *testing.T) {
	h := newRunnerHarness(t)
	h.start("w1", fastGen(1))
	h.dispatch("w2", queue.Job{ID: "not-mine", Prompt: "p", CreatedAt: time.Now().UTC()})

	time.Sleep(150 * time.Millisecond)
	n, _ := h.client.XLen(context.Background(), registry.DefaultStreamPrefix+"w2").Result()
	if n != 1 {
		t.Errorf("w2 stream length = %d, want 1 (untouched)", n)
	}
	if h.mr.Exists("jobs:chunks:not-mine") {
		t.Error("w1 processed a job addressed to w2")
	}
}

func TestRunnerAbandonCancelsJobAndReleasesSlot(t *testing.T) {
	h := newRunnerHarness(t)
	slow := &MockGenerator{MinTokens: 10000, MaxTokens: 10000, TokenDelay: 5 * time.Millisecond, FirstDelay: time.Millisecond}
	h.start("w1", slow)
	h.dispatch("w1", queue.Job{ID: "job-a", Prompt: "p", CreatedAt: time.Now().UTC()})

	waitFor(t, "job started", func() bool { return h.mr.Exists("jobs:chunks:job-a") })
	// Publish repeatedly: the pattern subscription may not be live on the first try.
	waitFor(t, "slot released after abandon", func() bool {
		_ = h.q.Abandon(context.Background(), "job-a")
		n, _ := h.client.XLen(context.Background(), registry.DefaultStreamPrefix+"w1").Result()
		return n == 0
	})
}

func TestRunnerSkipsMalformedEntries(t *testing.T) {
	h := newRunnerHarness(t)
	h.start("w1", fastGen(1))
	h.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: registry.DefaultStreamPrefix + "w1", Values: map[string]any{"junk": "1"},
	})
	h.dispatch("w1", queue.Job{ID: "good", Prompt: "p", CreatedAt: time.Now().UTC()})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := h.q.WaitResult(ctx, "good"); err != nil {
		t.Fatalf("job after malformed entry not processed: %v", err)
	}
	waitFor(t, "malformed entry released", func() bool {
		n, _ := h.client.XLen(context.Background(), registry.DefaultStreamPrefix+"w1").Result()
		return n == 0
	})
}

func TestRunnerSpanJoinsJobTrace(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	h := newRunnerHarness(t)
	h.start("w1", fastGen(1))

	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	job := queue.Job{
		ID:          "tracey",
		Traceparent: fmt.Sprintf("00-%s-%s-01", tid, sid),
		Model:       "m1",
		Prompt:      "p",
		CreatedAt:   time.Now().UTC(),
	}
	h.dispatch("w1", job)

	waitFor(t, "job processed", func() bool {
		n, _ := h.client.XLen(context.Background(), registry.DefaultStreamPrefix+"w1").Result()
		return n == 0
	})

	var proc sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.Name() == "worker.process" {
			proc = s
			break
		}
	}
	if proc == nil {
		t.Fatal("no worker.process span recorded")
	}
	sc := proc.SpanContext()
	if sc.TraceID() != tid {
		t.Errorf("worker.process trace ID = %s, want %s (must join the job's trace)", sc.TraceID(), tid)
	}
	if proc.Parent().SpanID() != sid {
		t.Errorf("worker.process parent span ID = %s, want %s", proc.Parent().SpanID(), sid)
	}
	attrs := map[string]string{}
	for _, kv := range proc.Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs["worker.id"] != "w1" || attrs["job.id"] != "tracey" || attrs["job.model"] != "m1" {
		t.Errorf("worker.process attributes = %v, want worker.id=w1 job.id=tracey job.model=m1", attrs)
	}
	if _, ok := attributeKey(proc, "job.tokens_out"); !ok {
		t.Error("worker.process span missing job.tokens_out attribute")
	}
}

func TestRunnerSetsChunksTTLAfterDoneChunk(t *testing.T) {
	h := newRunnerHarness(t)
	h.start("w1", fastGen(3))
	h.dispatch("w1", queue.Job{ID: "job-ttl", Prompt: "hello world", CreatedAt: time.Now().UTC()})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := h.q.WaitResult(ctx, "job-ttl"); err != nil {
		t.Fatalf("wait result: %v", err)
	}

	waitFor(t, "chunk stream TTL", func() bool { return chunkTTL(t, h, "job-ttl") > 0 })
	ttl := chunkTTL(t, h, "job-ttl")
	if ttl > 10*time.Minute {
		t.Errorf("chunk stream TTL = %v, want <= default 10m", ttl)
	}
}

func TestRunnerChunksTTLOnlyAfterDone(t *testing.T) {
	h := newRunnerHarness(t)
	slow := &MockGenerator{MinTokens: 10000, MaxTokens: 10000, TokenDelay: 5 * time.Millisecond, FirstDelay: time.Millisecond}
	h.start("w1", slow)
	h.dispatch("w1", queue.Job{ID: "job-slow", Prompt: "p", CreatedAt: time.Now().UTC()})

	waitFor(t, "chunk stream created", func() bool { return h.mr.Exists("jobs:chunks:job-slow") })
	if ttl := chunkTTL(t, h, "job-slow"); ttl != -1 {
		t.Fatalf("chunk stream TTL while still running = %v, want -1 (no expiry yet)", ttl)
	}
}

type failingGen struct{ err error }

func (f *failingGen) Run(_ context.Context, _ queue.Job, _ func(queue.Chunk) error) error {
	return f.err
}

func TestRunnerPublishesTerminalErrorChunk(t *testing.T) {
	h := newRunnerHarness(t)
	h.startGen("w1", &failingGen{err: fmt.Errorf("backend exploded")})
	h.dispatch("w1", queue.Job{ID: "job-fail", Prompt: "p", CreatedAt: time.Now().UTC()})

	// WaitResult must fail fast with the worker's reason instead of letting
	// the API wait out the full request timeout.
	_, err := h.q.WaitResult(context.Background(), "job-fail")
	if !errors.Is(err, queue.ErrJobFailed) {
		t.Fatalf("WaitResult error = %v, want queue.ErrJobFailed", err)
	}
	if !strings.Contains(err.Error(), "backend exploded") {
		t.Errorf("error = %v, want worker reason", err)
	}

	// The failed job's chunk stream must still get a TTL, not leak forever.
	if ttl := chunkTTL(t, h, "job-fail"); ttl <= 0 {
		t.Errorf("chunk stream TTL after generator error = %v, want expiry", ttl)
	}
}

func TestRunnerChunksTTLCustomValue(t *testing.T) {
	h := newRunnerHarness(t)
	r := NewRunner(h.client, h.reg, fastGen(1), slog.New(slog.DiscardHandler), RunnerConfig{
		ID: "w1", ReadBlock: 20 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, ChunksTTL: 5 * time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	h.dispatch("w1", queue.Job{ID: "job-custom", Prompt: "p", CreatedAt: time.Now().UTC()})

	waitFor(t, "chunk stream TTL", func() bool { return chunkTTL(t, h, "job-custom") > 0 })
	ttl := chunkTTL(t, h, "job-custom")
	if ttl > 5*time.Minute {
		t.Errorf("chunk stream TTL = %v, want <= 5m", ttl)
	}
}

func TestRunnerChunksTTLEmptyStreamAfterTTL(t *testing.T) {
	h := newRunnerHarness(t)
	mr := h.mr
	mr.SetTime(time.Unix(0, 0))
	r := NewRunner(h.client, h.reg, fastGen(1), slog.New(slog.DiscardHandler), RunnerConfig{
		ID: "w1", ReadBlock: 20 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, ChunksTTL: 10 * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	h.dispatch("w1", queue.Job{ID: "job-expire", Prompt: "p", CreatedAt: time.Now().UTC()})
	waitFor(t, "chunk stream TTL", func() bool { return chunkTTL(t, h, "job-expire") > 0 })

	mr.FastForward(11 * time.Second)
	if mr.Exists("jobs:chunks:job-expire") {
		t.Error("chunk stream still exists after TTL elapsed")
	}
}

func TestRunnerChunksTTLEntryIDAndChunks(t *testing.T) {
	h := newRunnerHarness(t)
	h.start("w1", fastGen(2))
	h.dispatch("w1", queue.Job{ID: "job-chunks", Prompt: "p", CreatedAt: time.Now().UTC()})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := h.q.WaitResult(ctx, "job-chunks"); err != nil {
		t.Fatalf("wait result: %v", err)
	}
	n, _ := h.client.XLen(context.Background(), "jobs:chunks:job-chunks").Result()
	if n == 0 {
		t.Error("no chunks retained in finished stream")
	}
}

func TestRunnerChunksTTLDisabledWithNegativeConfig(t *testing.T) {
	h := newRunnerHarness(t)
	r := NewRunner(h.client, h.reg, fastGen(1), slog.New(slog.DiscardHandler), RunnerConfig{
		ID: "w1", ReadBlock: 20 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, ChunksTTL: -time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	h.dispatch("w1", queue.Job{ID: "job-nottl", Prompt: "p", CreatedAt: time.Now().UTC()})

	ctx, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if _, err := h.q.WaitResult(ctx, "job-nottl"); err != nil {
		t.Fatalf("wait result: %v", err)
	}
	if ttl := chunkTTL(t, h, "job-nottl"); ttl != -1 {
		t.Errorf("chunk stream TTL with disabled config = %v, want -1 (no expiry)", ttl)
	}
}

func attributeKey(s sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}
