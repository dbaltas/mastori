package traffic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config describes a full traffic run.
type Config struct {
	RunID       string
	URL         string
	Model       string
	Rows        []Row
	Weights     map[string]float64 // dataset name -> share of traffic
	Seed        int64
	Warmup      time.Duration
	Duration    time.Duration
	Concurrency int // > 0 selects closed-loop mode
	Rate        float64
	Arrival     string // "constant" or "poisson" (open-loop mode)
	RampEndRate float64
	Steps       []StepSeg // overrides rate/arrival/ramp when set
	MaxInflight int
	Stream      bool // use the SSE /generate/stream endpoint
	Timeout     time.Duration
	// GenerationParams are sent on every request so the measured workload's
	// output length and sampling are explicit rather than backend defaults.
	GenerationParams
	// SchedulerURL gates the run against the scheduler's /metrics endpoint
	// when set (plus ExpectedWorkers): before the run, workers_healthy must
	// equal ExpectedWorkers; after the run, per-worker dispatched_total
	// counters are recorded in the summary. Empty disables the gate.
	SchedulerURL string
	// ExpectedWorkers is the healthy-worker count the preflight gate
	// requires; 0 disables the preflight check.
	ExpectedWorkers int
	// Smoke runs one preflight streaming request and asserts chunks > 1,
	// TTFT < e2e, and (for ignore_eos runs) tokens_out == max_tokens; a
	// failure aborts the run. The result is recorded in the summary.
	Smoke bool
	// DrainGrace caps how long the measured phase waits for in-flight
	// warmup requests to finish (default Timeout + 10s).
	DrainGrace time.Duration
	OutputPath string
	// RecordsStdout also streams every record as a JSON line to stdout, so
	// a run executed inside the cluster is recoverable from the pod logs
	// (distroless images have no shell, so results cannot be copied out).
	RecordsStdout bool
	// preflightHealthy stores the preflight workers_healthy observation for
	// the summary (set by Run when the scheduler gate is configured).
	preflightHealthy int
	// smokeCheck stores the preflight smoke result for the summary (set by
	// Run when the smoke gate is configured), including on failure.
	smokeCheck *SmokeCheck
	// ManifestSHA256 identifies the seed data: the SHA-256 of the dataset
	// manifest that produced the cache rows, empty when unknown.
	ManifestSHA256 string
	// RedisAddr enables the interruption-flag check at finalize time
	// (REDIS_ADDR, like the other components); empty disables it.
	RedisAddr string
	// InterruptKey is the Redis key whose presence marks an infra-level
	// interruption that invalidates the run. A single global flag: the
	// scheduler sets it when it reclaims a dead worker's stranded jobs.
	// Concurrent benchmark runs are not a supported scenario.
	InterruptKey string
}

func (c *Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("url is required")
	}
	if len(c.Rows) == 0 {
		return fmt.Errorf("no dataset rows loaded")
	}
	if c.Duration <= 0 {
		return fmt.Errorf("duration must be positive")
	}
	if c.Timeout <= 0 {
		c.Timeout = 65 * time.Second
	}
	if c.DrainGrace < 0 {
		return fmt.Errorf("drain grace must not be negative")
	}
	if c.RunID == "" {
		c.RunID = time.Now().UTC().Format("20060102T150405")
	}
	if c.InterruptKey == "" {
		c.InterruptKey = "run:interrupted"
	}
	if c.Concurrency > 0 {
		return nil
	}
	if c.Rate <= 0 {
		return fmt.Errorf("rate must be positive in open-loop mode (or set concurrency for closed-loop)")
	}
	if c.RampEndRate < 0 {
		return fmt.Errorf("ramp-end-rate must not be negative")
	}
	if len(c.Steps) == 0 && c.RampEndRate == 0 {
		a := c.Arrival
		if a == "" {
			a = "constant"
		}
		if a != "constant" && a != "poisson" {
			return fmt.Errorf("arrival must be \"constant\" or \"poisson\"")
		}
	}
	return nil
}

func (c *Config) inflightLimit() int {
	if c.MaxInflight > 0 {
		return c.MaxInflight
	}
	if c.Concurrency > 0 {
		return c.Concurrency
	}
	return int(math.Max(1, math.Ceil(4*c.Rate)))
}

// Run statuses. The summary file is written with StatusRunning at run start
// and rewritten with the final status at the end, so a driver that dies
// mid-run leaves a summary that is observable as running.
const (
	StatusRunning = "running"
	StatusValid   = "valid"
	StatusInvalid = "invalid"
)

type Summary struct {
	SchemaVersion     string         `json:"schema_version"`
	Status            string         `json:"status"`
	RunID             string         `json:"run_id"`
	StartedAt         time.Time      `json:"started_at"`
	MeasuredStartedAt time.Time      `json:"measured_started_at"`
	WarmupDrainMs     int64          `json:"warmup_drain_ms"`
	FinishedAt        time.Time      `json:"finished_at"`
	WarmupRequests    int            `json:"warmup_requests"`
	Requests          int            `json:"requests"`
	Successes         int            `json:"successes"`
	Failures          int            `json:"failures"`
	ByStatus          map[int]int    `json:"by_status"`
	Errors            map[string]int `json:"errors"`
	RequestedRate     float64        `json:"requested_rate"`
	AchievedSendRate  float64        `json:"achieved_send_rate"`
	// OfferedRequests is the schedule-implied arrival count for the
	// measured window (expected for Poisson); SkippedOffered is how many
	// of those were never sent because the in-flight semaphore held the
	// sender back (open-loop only; closed-loop reports neither).
	OfferedRequests int     `json:"offered_requests,omitempty"`
	SkippedOffered  int     `json:"skipped_offered,omitempty"`
	Throughput      float64 `json:"throughput_rps"`
	// MaxInflight records the in-flight ceiling the run enforced (open
	// loop), so saturated-latency numbers are read together with the cap
	// that produced them (Little's law: the cap sets the latency band).
	MaxInflight    int            `json:"max_inflight,omitempty"`
	LatencyMsP50   float64        `json:"latency_ms_p50"`
	LatencyMsP95   float64        `json:"latency_ms_p95"`
	LatencyMsP99   float64        `json:"latency_ms_p99"`
	LatencyMsMean  float64        `json:"latency_ms_mean"`
	LatencyMsMin   float64        `json:"latency_ms_min"`
	LatencyMsMax   float64        `json:"latency_ms_max"`
	TTFTMsP50      float64        `json:"ttft_ms_p50"`
	TTFTMsP95      float64        `json:"ttft_ms_p95"`
	TTFTMsMean     float64        `json:"ttft_ms_mean"`
	TokensOutTotal int64          `json:"tokens_out_total"`
	TokensInTotal  int64          `json:"tokens_in_total"`
	OutputPath     string         `json:"output_path"`
	Mode           string         `json:"mode"`
	Datasets       map[string]int `json:"datasets"`
	// GenerationParams records the run's output-control parameters, so a
	// summary is self-describing about what shaped its output lengths.
	GenerationParams
	// SmokeCheck records the preflight smoke request's result when the
	// smoke gate is enabled.
	SmokeCheck *SmokeCheck `json:"smoke_check,omitempty"`
	// WorkersHealthyPreflight is the scheduler's mastori_scheduler_workers_healthy
	// gauge observed before the run; ExpectedWorkers is what it was required
	// to be. Both are recorded only when the scheduler gate is configured.
	WorkersHealthyPreflight int `json:"workers_healthy_preflight,omitempty"`
	ExpectedWorkers         int `json:"expected_workers,omitempty"`
	// DispatchedByWorker maps worker id -> mastori_scheduler_dispatched_total
	// observed after the run, so a stray worker (e.g. a mock worker serving
	// GPU models) is visible in the summary itself.
	DispatchedByWorker map[string]int64 `json:"dispatched_by_worker,omitempty"`
	// DatasetManifestSHA256 identifies the seed data the run replayed
	// (SHA-256 of the dataset manifest), empty when unknown.
	DatasetManifestSHA256 string `json:"dataset_manifest_sha256,omitempty"`
}

// accumulator collects counters from concurrent send goroutines.
type accumulator struct {
	mu        sync.Mutex
	warmup    int
	requests  int
	successes int
	failures  int
	byStatus  map[int]int
	errors    map[string]int
	tokensOut int64
	tokensIn  int64
	latency   LatencyStats
	ttft      LatencyStats
	datasets  map[string]int
}

func newAccumulator() *accumulator {
	return &accumulator{
		byStatus: make(map[int]int),
		errors:   make(map[string]int),
		datasets: make(map[string]int),
		latency:  *newLatencyStats(),
		ttft:     *newLatencyStats(),
	}
}

func (a *accumulator) add(r Record, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.Warmup {
		a.warmup++
		return
	}
	a.requests++
	a.datasets[r.Dataset]++
	a.byStatus[r.Status]++
	if ok {
		a.successes++
		a.latency.Add(float64(r.LatencyMs))
		if r.TTFTMs > 0 {
			a.ttft.Add(float64(r.TTFTMs))
		}
		a.tokensOut += r.TokensOut
	} else {
		a.failures++
		if r.Error != "" {
			a.errors[r.Error]++
		}
	}
	// Counted for every record: rejected requests carry 0 prompt tokens
	// (admission happens before prefill), so the sum equals admitted-prompt
	// tokens while keeping the aggregate honest against the JSONL.
	a.tokensIn += r.TokensIn
}

// warmupBarrier separates the warmup phase from the measured phase. Sends
// scheduled before the boundary are registered as in flight; the measured
// phase starts only after every registered request has completed, bounded
// by maxWait so a hung warmup request cannot wedge the run.
type warmupBarrier struct {
	mu        sync.Mutex
	wg        sync.WaitGroup
	boundary  time.Time
	maxWait   time.Duration
	once      sync.Once
	drainDone chan struct{}
	drainWall time.Duration
	drainOK   bool
	start     time.Time // when the measured phase began
}

func newWarmupBarrier(boundary time.Time, maxWait time.Duration) *warmupBarrier {
	return &warmupBarrier{
		boundary:  boundary,
		maxWait:   maxWait,
		drainDone: make(chan struct{}),
	}
}

// mark classifies a send scheduled at t. Warmup sends are registered as
// in flight (the caller must call done after completion); sends at or past
// the boundary are measured and untracked.
func (b *warmupBarrier) mark(t time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !t.Before(b.boundary) {
		return false
	}
	b.wg.Add(1)
	return true
}

func (b *warmupBarrier) done() { b.wg.Done() }

// close ends the warmup phase and waits until every in-flight warmup
// request has completed, bounded by maxWait or ctx. It reports the wait
// duration and whether the drain fully completed. Concurrent callers block
// until the first wait resolves.
func (b *warmupBarrier) close(ctx context.Context) (time.Duration, bool) {
	b.once.Do(func() {
		waitStart := time.Now()
		drained := make(chan struct{})
		go func() {
			b.wg.Wait()
			close(drained)
		}()
		timer := time.NewTimer(b.maxWait)
		defer timer.Stop()
		select {
		case <-drained:
			b.drainOK = true
		case <-timer.C:
			fmt.Fprintf(os.Stderr, "warning: warmup requests still in flight after %v; measured phase may be contaminated\n", b.maxWait)
		case <-ctx.Done():
		}
		b.drainWall = time.Since(waitStart)
		b.start = time.Now()
		close(b.drainDone)
	})
	<-b.drainDone
	return b.drainWall, b.drainOK
}

// Run executes the configured traffic run and returns its summary.
func Run(ctx context.Context, cfg *Config) (*Summary, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Preflight gate: abort before sending traffic when the scheduler does
	// not report the expected number of healthy workers. A run against a
	// half-registered fleet (e.g. a mock worker still serving every model)
	// is not worth executing, let alone publishing.
	if cfg.SchedulerURL != "" && cfg.ExpectedWorkers > 0 {
		healthy, err := CheckWorkersHealthy(ctx, cfg.SchedulerURL, cfg.ExpectedWorkers)
		if err != nil {
			return nil, err
		}
		cfg.preflightHealthy = int(healthy)
	}
	// Smoke gate: one real request must stream before the run is allowed
	// to start (chunks > 1, TTFT < e2e, output length honored).
	if cfg.Smoke {
		sc, err := RunSmokeCheck(ctx, cfg)
		cfg.smokeCheck = sc
		if err != nil {
			return nil, err
		}
	}

	fileWriter, err := NewResultWriter(cfg.OutputPath)
	if err != nil {
		return nil, err
	}
	defer fileWriter.Close()
	var writer *ResultWriter
	if cfg.RecordsStdout {
		writer = NewResultWriterTo(io.MultiWriter(fileWriter.f, os.Stdout))
	} else {
		writer = fileWriter
	}

	sender := NewSender(cfg.URL, cfg.Model, cfg.Stream, cfg.GenerationParams, cfg.Timeout)
	selector := NewWeightedRowSource(cfg.Rows, cfg.Weights, cfg.Seed)
	pattern := buildPattern(cfg)
	total := cfg.Warmup + cfg.Duration
	started := time.Now()
	warmupEnd := started.Add(cfg.Warmup)
	drainGrace := cfg.DrainGrace
	if drainGrace <= 0 {
		drainGrace = cfg.Timeout + 10*time.Second
	}
	barrier := newWarmupBarrier(warmupEnd, drainGrace)
	var offered float64
	if cfg.Concurrency == 0 {
		offered = pattern.Offered(cfg.Duration)
	}

	summaryPath := SummaryPath(cfg.OutputPath)
	startSummary := &Summary{
		SchemaVersion: SchemaVersion,
		Status:        StatusRunning,
		RunID:         cfg.RunID,
		StartedAt:     started,
		Mode:          modeLabel(cfg),
		OutputPath:    cfg.OutputPath,
	}
	if err := WriteSummary(summaryPath, startSummary); err != nil {
		fmt.Fprintf(os.Stderr, "warning: write start summary: %v\n", err)
	}

	var seq atomic.Int64
	acc := newAccumulator()

	var wg sync.WaitGroup
	if cfg.Concurrency > 0 {
		for i := 0; i < cfg.Concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				crossed := false
				deadline := started.Add(total)
				for {
					now := time.Now()
					warmup := now.Before(warmupEnd)
					if !warmup && !crossed {
						// Drain in-flight warmup requests before
						// the first measured send leaves this worker.
						barrier.close(ctx)
						deadline = time.Now().Add(cfg.Duration)
						crossed = true
					}
					if !now.Before(deadline) {
						return
					}
					tracked := barrier.mark(now)
					runSend(ctx, sender, selector, writer, acc, &seq, cfg, warmup, barrier, tracked)
					if ctx.Err() != nil {
						return
					}
				}
			}()
		}
	} else {
		sem := make(chan struct{}, cfg.inflightLimit())
		wg.Add(1)
		go func() {
			defer wg.Done()
			launch := func(delay time.Duration, sendAt time.Time, warmup, tracked bool) bool {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					if tracked {
						barrier.done()
					}
					return false
				case <-timer.C:
				}
				select {
				case <-ctx.Done():
					if tracked {
						barrier.done()
					}
					return false
				case sem <- struct{}{}:
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-sem }()
					runSend(ctx, sender, selector, writer, acc, &seq, cfg, warmup, barrier, tracked)
				}()
				return true
			}

			// Warmup phase: pattern clock anchored at the run start,
			// stopping at the boundary.
			if cfg.Warmup > 0 {
				for {
					now := time.Now()
					if !now.Before(warmupEnd) {
						break
					}
					sendAt := now.Add(pattern.NextDelay(now.Sub(started)))
					if !sendAt.Before(warmupEnd) {
						// The next scheduled send lands past the
						// boundary; cut warmup traffic here.
						break
					}
					if !launch(sendAt.Sub(now), sendAt, true, barrier.mark(sendAt)) {
						return
					}
				}
				barrier.close(ctx)
			}

			// Measured phase: pattern clock anchored at measuredStart so
			// the drain gap does not skip scheduled arrivals, and ramp or
			// step patterns span the measured window.
			measuredStart := time.Now()
			end := measuredStart.Add(cfg.Duration)
			for {
				now := time.Now()
				if !now.Before(end) {
					return
				}
				delay := pattern.NextDelay(now.Sub(measuredStart))
				sendAt := now.Add(delay)
				if !launch(delay, sendAt, false, false) {
					return
				}
			}
		}()
	}

	// Drain in-flight sends, but never wedge past the schedule plus one
	// send timeout of grace (protects against environment freezes that
	// stall connections indefinitely).
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(total + drainGrace + cfg.Timeout + 10*time.Second):
		fmt.Fprintf(os.Stderr, "warning: in-flight requests did not drain within grace; returning partial summary\n")
	}
	finished := time.Now()

	s := acc.snapshot(cfg)
	s.SchemaVersion = SchemaVersion
	s.RunID = cfg.RunID
	s.StartedAt = started
	s.MeasuredStartedAt = barrier.start
	if s.MeasuredStartedAt.IsZero() {
		s.MeasuredStartedAt = started
	}
	s.WarmupDrainMs = barrier.drainWall.Milliseconds()
	s.FinishedAt = finished
	s.OutputPath = cfg.OutputPath
	if cfg.Concurrency == 0 {
		// Open-loop: offered is the schedule-implied arrival count for the
		// measured window, and requested_rate is the true offered average
		// (with steps, cfg.Rate is just the base flag, not what was
		// scheduled).
		s.OfferedRequests = int(math.Round(offered))
		if s.Requests < s.OfferedRequests {
			s.SkippedOffered = s.OfferedRequests - s.Requests
		}
		s.RequestedRate = offered / cfg.Duration.Seconds()
	}
	interrupted := checkInterrupted(cfg)
	if interrupted {
		s.Errors["interrupted"]++
	}
	// Post-run capture: which workers actually received dispatches. A
	// summary is the artifact reviewers trust, so the per-worker split is
	// recorded even when the scheduler is unreachable (with a warning).
	if cfg.SchedulerURL != "" {
		captureCtx, captureCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		dispatched, err := FetchDispatchedByWorker(captureCtx, cfg.SchedulerURL)
		captureCancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: read per-worker dispatched counters: %v\n", err)
		} else {
			s.DispatchedByWorker = dispatched
		}
	}
	if s.Failures > 0 || interrupted {
		s.Status = StatusInvalid
	} else {
		s.Status = StatusValid
	}
	if err := WriteSummary(summaryPath, s); err != nil {
		fmt.Fprintf(os.Stderr, "warning: write final summary: %v\n", err)
	}
	return s, nil
}

// checkInterrupted reports whether the infra-level interruption flag is set
// in Redis. Without a Redis address (or if the check fails) the run is not
// considered interrupted: request failures remain the observable signal.
func checkInterrupted(cfg *Config) bool {
	if cfg.RedisAddr == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = client.Close() }()
	_, err := client.Get(ctx, cfg.InterruptKey).Result()
	if err == nil {
		return true
	}
	if !errors.Is(err, redis.Nil) {
		fmt.Fprintf(os.Stderr, "warning: read interruption flag: %v\n", err)
	}
	return false
}

func runSend(ctx context.Context, sender *Sender, selector *WeightedRowSource, writer *ResultWriter, acc *accumulator, seq *atomic.Int64, cfg *Config, warmup bool, barrier *warmupBarrier, tracked bool) {
	if tracked {
		defer barrier.done()
	}
	sctx, cancel := context.WithTimeout(ctx, cfg.Timeout+5*time.Second)
	defer cancel()

	row := selector.Next()
	sendAt := time.Now()
	res, err := sender.Send(sctx, row)
	latency := time.Since(sendAt)
	ok := err == nil && res.Status == 200

	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	rec := Record{
		SchemaVersion: SchemaVersion,
		RunID:         cfg.RunID,
		Seq:           seq.Add(1),
		Dataset:       row.Dataset,
		RowIndex:      row.Index,
		Warmup:        warmup,
		SendAt:        sendAt,
		Status:        res.Status,
		LatencyMs:     latency.Milliseconds(),
		TTFTMs:        res.TTFT.Milliseconds(),
		Chunks:        res.Chunks,
		TokensIn:      res.APIResp.TokensIn,
		TokensOut:     res.APIResp.TokensOut,
		MaxTokens:     cfg.GenerationParams.MaxTokens,
		APITotalMs:    res.APIResp.TotalMs,
		Error:         errMsg,
	}
	if werr := writer.Write(rec); werr != nil {
		fmt.Fprintf(os.Stderr, "warning: write result: %v\n", werr)
	}
	acc.add(rec, ok)
}

func buildPattern(cfg *Config) Pattern {
	rng := rand.New(rand.NewPCG(uint64(cfg.Seed*17+3), uint64(cfg.Seed)))
	if len(cfg.Steps) > 0 {
		return StepPattern{Segments: cfg.Steps}
	}
	if cfg.RampEndRate > 0 {
		return RampPattern{StartRate: cfg.Rate, EndRate: cfg.RampEndRate, Duration: cfg.Duration}
	}
	if cfg.Arrival == "poisson" {
		return NewPoissonPattern(cfg.Rate, rng)
	}
	return NewConstantPattern(cfg.Rate)
}
func (a *accumulator) snapshot(cfg *Config) *Summary {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := &Summary{
		WarmupRequests: a.warmup,
		Requests:       a.requests,
		Successes:      a.successes,
		Failures:       a.failures,
		ByStatus:       a.byStatus,
		Errors:         a.errors,
		TokensOutTotal: a.tokensOut,
		TokensInTotal:  a.tokensIn,
		Datasets:       a.datasets,
		RequestedRate:  cfg.Rate,
	}
	if cfg.Duration > 0 {
		s.AchievedSendRate = float64(a.requests) / cfg.Duration.Seconds()
		s.Throughput = float64(a.successes) / cfg.Duration.Seconds()
	}
	s.DatasetManifestSHA256 = cfg.ManifestSHA256
	s.Mode = modeLabel(cfg)
	s.GenerationParams = cfg.GenerationParams
	if cfg.Concurrency == 0 {
		s.MaxInflight = cfg.inflightLimit()
	}
	s.WorkersHealthyPreflight = cfg.preflightHealthy
	s.ExpectedWorkers = cfg.ExpectedWorkers
	s.SmokeCheck = cfg.smokeCheck
	if a.latency.Count() > 0 {
		s.LatencyMsP50 = a.latency.Percentile(50)
		s.LatencyMsP95 = a.latency.Percentile(95)
		s.LatencyMsP99 = a.latency.Percentile(99)
		s.LatencyMsMean = a.latency.Mean()
		s.LatencyMsMin = a.latency.Min()
		s.LatencyMsMax = a.latency.Max()
	}
	s.Mode = modeLabel(cfg)
	if cfg.Stream {
		s.Mode += "+stream"
		s.TTFTMsP50 = a.ttft.Percentile(50)
		s.TTFTMsP95 = a.ttft.Percentile(95)
		s.TTFTMsMean = a.ttft.Mean()
	}
	return s
}

func modeLabel(cfg *Config) string {
	if cfg.Concurrency > 0 {
		return fmt.Sprintf("closed-loop:%d", cfg.Concurrency)
	}
	return "open-loop"
}

// RunSignalContext wraps ctx with SIGINT/SIGTERM cancellation for graceful
// shutdown of a run started from the CLI.
func RunSignalContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, os.Interrupt, os.Kill)
}
