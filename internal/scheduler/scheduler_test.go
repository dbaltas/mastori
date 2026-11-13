package scheduler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/dbaltas/mastori/internal/queue"
	"github.com/dbaltas/mastori/internal/registry"
)

type harness struct {
	t      *testing.T
	mr     *miniredis.Miniredis
	client *redis.Client
	reg    *registry.Registry
	q      *queue.RedisQueue
	m      *Metrics
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	q := queue.NewRedisQueue(mr.Addr(), "jobs", "jobs:chunks:", "jobs:abandon:")
	t.Cleanup(func() { _ = q.Close() })
	return &harness{
		t: t, mr: mr, client: client, q: q, m: NewMetrics(),
		reg: registry.New(client, "", time.Minute),
	}
}

func (h *harness) worker(id string, models ...string) {
	h.t.Helper()
	if err := h.reg.Heartbeat(context.Background(), registry.Worker{ID: id, Models: models}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) enqueue(id, model string) {
	h.t.Helper()
	err := h.q.Enqueue(context.Background(), queue.Job{ID: id, Model: model, Prompt: "p " + id, CreatedAt: time.Now().UTC()})
	if err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) start(policy Policy, cfg Config) *Scheduler {
	h.t.Helper()
	cfg.ReadBlock = 20 * time.Millisecond
	cfg.NoWorkerBackoff = 10 * time.Millisecond
	cfg.WorkerRefresh = time.Millisecond
	cfg.StatsInterval = 10 * time.Millisecond
	s := New(h.client, h.reg, policy, h.m, slog.New(slog.DiscardHandler), cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Run(ctx) }()
	h.t.Cleanup(func() { cancel(); <-done })
	return s
}

func (h *harness) streamLen(key string) int64 {
	n, _ := h.client.XLen(context.Background(), key).Result()
	return n
}

func (h *harness) jobsOn(id string) []string {
	msgs, _ := h.client.XRange(context.Background(), "jobs:worker:"+id, "-", "+").Result()
	var ids []string
	for _, m := range msgs {
		ids = append(ids, m.Values["job_id"].(string))
	}
	return ids
}

func eventually(t *testing.T, what string, cond func() bool) {
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

func TestDispatchRoundRobin(t *testing.T) {
	h := newHarness(t)
	h.worker("w1")
	h.worker("w2")
	for _, id := range []string{"j1", "j2", "j3", "j4"} {
		h.enqueue(id, "m")
	}

	h.start(&RoundRobin{}, Config{})

	eventually(t, "all jobs dispatched", func() bool {
		return h.streamLen("jobs:worker:w1")+h.streamLen("jobs:worker:w2") == 4
	})
	if got := h.streamLen("jobs:worker:w1"); got != 2 {
		t.Errorf("w1 has %d jobs, want 2", got)
	}
	if got := h.jobsOn("w1"); got[0] != "j1" || got[1] != "j3" {
		t.Errorf("w1 jobs = %v, want [j1 j3]", got)
	}
}

func TestDispatchRemovesFromJobsStream(t *testing.T) {
	h := newHarness(t)
	h.worker("w1")
	h.enqueue("j1", "m")

	h.start(&RoundRobin{}, Config{})

	eventually(t, "job dispatched", func() bool { return h.streamLen("jobs:worker:w1") == 1 })
	eventually(t, "jobs stream drained", func() bool { return h.streamLen("jobs") == 0 })

	pending, err := h.client.XPending(context.Background(), "jobs", "mastori-scheduler").Result()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Count != 0 {
		t.Errorf("pending = %d, want 0 (dispatch must ack)", pending.Count)
	}
}

func TestDispatchPreservesJobFields(t *testing.T) {
	h := newHarness(t)
	h.worker("w1")
	created := time.Date(2026, 5, 6, 7, 8, 9, 10, time.UTC)
	err := h.q.Enqueue(context.Background(), queue.Job{ID: "j1", TraceID: "tr", Model: "m", Prompt: "hello", CreatedAt: created})
	if err != nil {
		t.Fatal(err)
	}

	h.start(&RoundRobin{}, Config{})
	eventually(t, "dispatched", func() bool { return h.streamLen("jobs:worker:w1") == 1 })

	msgs, _ := h.client.XRange(context.Background(), "jobs:worker:w1", "-", "+").Result()
	job, ok := queue.JobFromValues(msgs[0].Values)
	if !ok || job.ID != "j1" || job.TraceID != "tr" || job.Model != "m" || job.Prompt != "hello" || !job.CreatedAt.Equal(created) {
		t.Errorf("dispatched job = %+v ok=%v", job, ok)
	}
}

func TestLeastLoadedPrefersIdleWorker(t *testing.T) {
	h := newHarness(t)
	h.worker("w1")
	h.worker("w2")
	// w1 already has 3 unfinished jobs.
	for i := range 3 {
		h.client.XAdd(context.Background(), &redis.XAddArgs{Stream: "jobs:worker:w1", Values: queue.JobValues(queue.Job{ID: string(rune('a' + i)), Prompt: "p"})})
	}
	for _, id := range []string{"j1", "j2"} {
		h.enqueue(id, "m")
	}

	h.start(&LeastLoaded{}, Config{})

	eventually(t, "both jobs on w2", func() bool { return h.streamLen("jobs:worker:w2") == 2 })
	if got := h.streamLen("jobs:worker:w1"); got != 3 {
		t.Errorf("w1 length = %d, want 3 (untouched)", got)
	}
}

func TestDispatchRespectsModel(t *testing.T) {
	h := newHarness(t)
	h.worker("w1", "llama")
	h.worker("w2", "mistral")
	h.enqueue("j1", "mistral")
	h.enqueue("j2", "mistral")

	h.start(&RoundRobin{}, Config{})

	eventually(t, "jobs on w2", func() bool { return h.streamLen("jobs:worker:w2") == 2 })
	if h.streamLen("jobs:worker:w1") != 0 {
		t.Error("job routed to worker that does not serve the model")
	}
}

func TestWaitsForWorkerThenDispatches(t *testing.T) {
	h := newHarness(t)
	h.enqueue("j1", "m")

	h.start(&RoundRobin{}, Config{})

	eventually(t, "no_worker error recorded", func() bool {
		return testutil.ToFloat64(h.m.DispatchErrors.WithLabelValues("no_worker")) > 0
	})
	if h.streamLen("jobs") != 1 {
		t.Fatal("job must stay queued while there is no worker")
	}

	h.worker("w1")
	eventually(t, "job dispatched after worker appears", func() bool { return h.streamLen("jobs:worker:w1") == 1 })
}

func TestStopsRoutingToExpiredWorker(t *testing.T) {
	h := newHarness(t)
	h.reg = registry.New(h.client, "", time.Second)
	h.worker("dead")
	h.worker("alive")
	h.mr.FastForward(500 * time.Millisecond)
	h.worker("alive") // refresh
	h.mr.FastForward(700 * time.Millisecond)
	// "dead" is now past its TTL, "alive" is not.

	for _, id := range []string{"j1", "j2", "j3"} {
		h.enqueue(id, "m")
	}
	h.start(&RoundRobin{}, Config{})

	eventually(t, "all jobs on alive", func() bool { return h.streamLen("jobs:worker:alive") == 3 })
	if h.streamLen("jobs:worker:dead") != 0 {
		t.Error("job routed to expired worker")
	}
}

func TestDropsExpiredJobs(t *testing.T) {
	h := newHarness(t)
	h.worker("w1")
	old := time.Now().UTC().Add(-time.Minute)
	if err := h.q.Enqueue(context.Background(), queue.Job{ID: "old", Prompt: "p", CreatedAt: old}); err != nil {
		t.Fatal(err)
	}
	h.enqueue("fresh", "m")

	h.start(&RoundRobin{}, Config{JobTTL: 10 * time.Second})

	eventually(t, "fresh job dispatched", func() bool { return h.streamLen("jobs:worker:w1") == 1 })
	if got := h.jobsOn("w1"); got[0] != "fresh" {
		t.Errorf("dispatched %v, want only fresh", got)
	}
	if got := testutil.ToFloat64(h.m.JobsDropped.WithLabelValues("expired")); got != 1 {
		t.Errorf("expired drops = %v, want 1", got)
	}
}

func TestDropsMalformedEntries(t *testing.T) {
	h := newHarness(t)
	h.worker("w1")
	h.client.XAdd(context.Background(), &redis.XAddArgs{Stream: "jobs", Values: map[string]any{"junk": "x"}})
	h.enqueue("j1", "m")

	h.start(&RoundRobin{}, Config{})

	eventually(t, "valid job dispatched", func() bool { return h.streamLen("jobs:worker:w1") == 1 })
	if got := testutil.ToFloat64(h.m.JobsDropped.WithLabelValues("malformed")); got != 1 {
		t.Errorf("malformed drops = %v, want 1", got)
	}
}

func TestMetricsRecorded(t *testing.T) {
	h := newHarness(t)
	h.worker("w1")
	h.enqueue("j1", "m")
	h.enqueue("j2", "m")

	h.start(&RoundRobin{}, Config{})

	eventually(t, "dispatched", func() bool { return h.streamLen("jobs:worker:w1") == 2 })
	eventually(t, "worker gauges refreshed", func() bool {
		return testutil.ToFloat64(h.m.WorkersHealthy) == 1 &&
			testutil.ToFloat64(h.m.WorkerQueueDepth.WithLabelValues("w1")) == 2 &&
			testutil.ToFloat64(h.m.QueueDepth) == 0
	})
	if got := testutil.ToFloat64(h.m.Dispatched.WithLabelValues("w1")); got != 2 {
		t.Errorf("dispatched_total{w1} = %v, want 2", got)
	}
	if got := testutil.CollectAndCount(h.m.SchedulingSeconds); got == 0 {
		t.Error("scheduling_seconds histogram not collected")
	}
}

func TestResumesPendingEntriesNotLost(t *testing.T) {
	// A second scheduler instance in the same group must not lose or
	// duplicate jobs: every job lands on exactly one worker stream.
	h := newHarness(t)
	h.worker("w1")
	h.worker("w2")
	const n = 20
	for i := range n {
		h.enqueue("j"+string(rune('A'+i)), "m")
	}

	h.start(&RoundRobin{}, Config{ConsumerID: "s1"})
	h.start(&RoundRobin{}, Config{ConsumerID: "s2"})

	eventually(t, "all dispatched", func() bool {
		return h.streamLen("jobs:worker:w1")+h.streamLen("jobs:worker:w2") == n
	})
	seen := map[string]bool{}
	for _, id := range append(h.jobsOn("w1"), h.jobsOn("w2")...) {
		if seen[id] {
			t.Errorf("job %s dispatched twice", id)
		}
		seen[id] = true
	}
}
