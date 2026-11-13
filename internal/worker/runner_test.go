package worker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

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
	q := queue.NewRedisQueue(mr.Addr(), "jobs", "jobs:chunks:", "jobs:abandon:")
	t.Cleanup(func() { _ = q.Close() })
	return &runnerHarness{t: t, mr: mr, client: client, q: q, reg: registry.New(client, "", time.Minute)}
}

func (h *runnerHarness) start(id string, gen *MockGenerator, models ...string) (stop func()) {
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
