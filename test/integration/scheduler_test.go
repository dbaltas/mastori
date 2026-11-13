//go:build integration

// Package integration runs the API, scheduler and mock workers in-process
// against a real Redis. Every test uses its own key prefix, so tests can share
// one Redis instance and run in parallel.
//
// Run with a Redis on INTEGRATION_REDIS_ADDR (default localhost:7079, the
// docker compose port):
//
//	go test -tags integration ./test/integration/...
package integration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/dbaltas/mastori/internal/api"
	"github.com/dbaltas/mastori/internal/config"
	"github.com/dbaltas/mastori/internal/metrics"
	"github.com/dbaltas/mastori/internal/queue"
	"github.com/dbaltas/mastori/internal/registry"
	"github.com/dbaltas/mastori/internal/scheduler"
	"github.com/dbaltas/mastori/internal/worker"
)

type stack struct {
	t      *testing.T
	prefix string
	client *redis.Client
	reg    *registry.Registry
	api    *httptest.Server
	sched  *scheduler.Metrics

	mu      sync.Mutex
	workers map[string]context.CancelFunc
	done    map[string]chan struct{}
	gen     *worker.MockGenerator
}

func redisAddr() string {
	if v := os.Getenv("INTEGRATION_REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:7079"
}

func newStack(t *testing.T, policyName string) *stack {
	t.Helper()
	t.Parallel()

	client := redis.NewClient(&redis.Options{Addr: redisAddr()})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis at %s unreachable: %v", redisAddr(), err)
	}

	b := make([]byte, 4)
	_, _ = rand.Read(b)
	prefix := "it:" + hex.EncodeToString(b) + ":"
	t.Cleanup(func() {
		ctx := context.Background()
		iter := client.Scan(ctx, 0, prefix+"*", 200).Iterator()
		for iter.Next(ctx) {
			client.Del(ctx, iter.Val())
		}
	})

	logger := slog.New(slog.DiscardHandler)
	s := &stack{
		t: t, prefix: prefix, client: client,
		reg:     registry.New(client, prefix+"workers:", 3*time.Second),
		workers: map[string]context.CancelFunc{},
		done:    map[string]chan struct{}{},
		gen:     &worker.MockGenerator{MinTokens: 5, MaxTokens: 5, TokenDelay: time.Millisecond, FirstDelay: time.Millisecond},
	}

	// API
	cfg := &config.Config{
		RedisAddr:      redisAddr(),
		RedisStream:    prefix + "jobs",
		ChunksPrefix:   prefix + "chunks:",
		AbandonPrefix:  prefix + "abandon:",
		RequestTimeout: 10 * time.Second,
		MaxInflight:    100,
		DefaultModel:   "default",
	}
	q := queue.NewRedisQueue(cfg.RedisAddr, cfg.RedisStream, cfg.ChunksPrefix, cfg.AbandonPrefix)
	t.Cleanup(func() { _ = q.Close() })
	am := metrics.New()
	h := api.NewHandler(cfg, q, am, logger)
	s.api = httptest.NewServer(api.NewRouter(cfg, am, logger, q.Ping, h))
	t.Cleanup(s.api.Close)

	// Scheduler
	policy, err := scheduler.NewPolicy(policyName)
	if err != nil {
		t.Fatal(err)
	}
	s.sched = scheduler.NewMetrics()
	sc := scheduler.New(client, s.reg, policy, s.sched, logger, scheduler.Config{
		Stream:             cfg.RedisStream,
		Group:              prefix + "scheduler",
		WorkerStreamPrefix: prefix + "jobs:worker:",
		ReadBlock:          50 * time.Millisecond,
		WorkerRefresh:      10 * time.Millisecond,
		NoWorkerBackoff:    10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	schedDone := make(chan struct{})
	go func() { defer close(schedDone); _ = sc.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-schedDone })

	t.Cleanup(func() {
		s.mu.Lock()
		ids := make([]string, 0, len(s.workers))
		for id := range s.workers {
			ids = append(ids, id)
		}
		s.mu.Unlock()
		for _, id := range ids {
			s.stopWorker(id)
		}
	})
	return s
}

func (s *stack) startWorker(id string) { s.startWorkerWith(id, s.gen) }

func (s *stack) startWorkerWith(id string, gen *worker.MockGenerator) {
	s.t.Helper()
	r := worker.NewRunner(s.client, s.reg, gen, slog.New(slog.DiscardHandler), worker.RunnerConfig{
		ID:                 id,
		WorkerStreamPrefix: s.prefix + "jobs:worker:",
		ChunksPrefix:       s.prefix + "chunks:",
		AbandonPrefix:      s.prefix + "abandon:",
		HeartbeatInterval:  100 * time.Millisecond,
		ReadBlock:          50 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(ctx) }()

	s.mu.Lock()
	s.workers[id], s.done[id] = cancel, done
	s.mu.Unlock()

	// Wait until the worker is visible so tests don't race the registration.
	eventually(s.t, "worker "+id+" registered", func() bool {
		ws, _ := s.reg.List(context.Background())
		for _, w := range ws {
			if w.ID == id {
				return true
			}
		}
		return false
	})
}

func (s *stack) stopWorker(id string) {
	s.mu.Lock()
	cancel, done := s.workers[id], s.done[id]
	delete(s.workers, id)
	delete(s.done, id)
	s.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (s *stack) dispatched(workerID string) float64 {
	return testutil.ToFloat64(s.sched.Dispatched.WithLabelValues(workerID))
}

func (s *stack) generate(prompt string) (int, map[string]any, error) {
	body, _ := json.Marshal(map[string]string{"prompt": prompt})
	resp, err := http.Post(s.api.URL+"/generate", "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, nil
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestRoundRobinSpreadsJobsAcrossWorkers(t *testing.T) {
	s := newStack(t, "round-robin")
	s.startWorker("w1")
	s.startWorker("w2")

	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, out, err := s.generate(fmt.Sprintf("prompt number %d", i))
			switch {
			case err != nil:
				errs <- err
			case code != http.StatusOK:
				errs <- fmt.Errorf("status = %d, body = %v", code, out)
			case out["tokens_out"] != float64(5):
				errs <- fmt.Errorf("tokens_out = %v, want 5", out["tokens_out"])
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	w1, w2 := s.dispatched("w1"), s.dispatched("w2")
	if w1+w2 != n {
		t.Fatalf("dispatched total = %v, want %d", w1+w2, n)
	}
	if w1 != n/2 || w2 != n/2 {
		t.Errorf("round-robin split = %v/%v, want %d/%d", w1, w2, n/2, n/2)
	}

	// Every job and its worker-stream entry is gone once finished.
	eventually(t, "queues drained", func() bool {
		a, _ := s.client.XLen(context.Background(), s.prefix+"jobs").Result()
		b, _ := s.client.XLen(context.Background(), s.prefix+"jobs:worker:w1").Result()
		c, _ := s.client.XLen(context.Background(), s.prefix+"jobs:worker:w2").Result()
		return a+b+c == 0
	})
}

func TestStreamingThroughScheduler(t *testing.T) {
	s := newStack(t, "least-loaded")
	s.startWorker("w1")

	body, _ := json.Marshal(map[string]string{"prompt": "stream me"})
	resp, err := http.Post(s.api.URL+"/generate/stream", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	var chunks int
	var sawDone bool
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "event: chunk") {
			chunks++
		}
		if strings.HasPrefix(line, "data: [DONE]") {
			sawDone = true
		}
	}
	if chunks != 6 { // 5 tokens + final chunk
		t.Errorf("chunks = %d, want 6", chunks)
	}
	if !sawDone {
		t.Error("stream did not end with [DONE]")
	}
}

func TestLeastLoadedAvoidsBusyWorker(t *testing.T) {
	s := newStack(t, "least-loaded")

	// w1 is slow: the jobs below keep running for the whole test, so its
	// stream length (its load) stays at 2.
	slow := &worker.MockGenerator{MinTokens: 5000, MaxTokens: 5000, TokenDelay: 5 * time.Millisecond, FirstDelay: 5 * time.Millisecond}
	s.startWorkerWith("w1", slow)
	s.startWorker("w2")

	for i := range 2 {
		err := s.client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: s.prefix + "jobs:worker:w1",
			Values: queue.JobValues(queue.Job{ID: fmt.Sprintf("busy-%d", i), Prompt: "p", CreatedAt: time.Now().UTC()}),
		}).Err()
		if err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "w1 busy", func() bool {
		n, _ := s.client.XLen(context.Background(), s.prefix+"jobs:worker:w1").Result()
		return n == 2
	})

	for i := range 4 {
		code, out, err := s.generate(fmt.Sprintf("probe %d", i))
		if err != nil || code != http.StatusOK {
			t.Fatalf("generate: code=%d out=%v err=%v", code, out, err)
		}
	}
	if got := s.dispatched("w1"); got != 0 {
		t.Errorf("w1 dispatched = %v, want 0 (w2 was always less loaded)", got)
	}
	if got := s.dispatched("w2"); got != 4 {
		t.Errorf("w2 dispatched = %v, want 4", got)
	}
}

func TestWorkerShutdownStopsRouting(t *testing.T) {
	s := newStack(t, "round-robin")
	s.startWorker("w1")
	s.startWorker("w2")

	s.stopWorker("w2") // graceful: deregisters immediately

	before := s.dispatched("w2")
	for i := range 4 {
		code, out, err := s.generate(fmt.Sprintf("after shutdown %d", i))
		if err != nil || code != http.StatusOK {
			t.Fatalf("generate: code=%d out=%v err=%v", code, out, err)
		}
	}
	if got := s.dispatched("w2"); got != before {
		t.Errorf("jobs routed to stopped worker: %v -> %v", before, got)
	}
	if got := s.dispatched("w1"); got != 4 {
		t.Errorf("w1 dispatched = %v, want 4", got)
	}
}

func TestJobsWaitUntilWorkerRegisters(t *testing.T) {
	s := newStack(t, "round-robin")

	type result struct {
		code int
		err  error
	}
	res := make(chan result, 1)
	go func() {
		code, _, err := s.generate("early bird")
		res <- result{code, err}
	}()

	time.Sleep(200 * time.Millisecond) // job is queued, no worker exists
	select {
	case r := <-res:
		t.Fatalf("request finished with no worker: %+v", r)
	default:
	}

	s.startWorker("late")
	select {
	case r := <-res:
		if r.err != nil || r.code != http.StatusOK {
			t.Errorf("result = %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request did not complete after worker registered")
	}
}
