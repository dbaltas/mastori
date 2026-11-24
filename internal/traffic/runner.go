package traffic

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	// The startup line and the final summary go to stderr in that mode so
	// stdout stays parseable JSONL.
	RecordsStdout bool
	// VCSRev and VCSModified record the binary's build provenance in the
	// summary (set by the CLI from debug.ReadBuildInfo; empty when unknown).
	VCSRev      string
	VCSModified bool
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

// applyDefaults fills in unset fields with their defaults, mutating cfg.
// Run calls it before Validate; Validate itself never mutates.
func (c *Config) applyDefaults() {
	if c.Timeout <= 0 {
		c.Timeout = 65 * time.Second
	}
	if c.DrainGrace <= 0 {
		c.DrainGrace = c.Timeout + 10*time.Second
	}
	if c.RunID == "" {
		c.RunID = NewRunID()
	}
	if c.InterruptKey == "" {
		c.InterruptKey = "run:interrupted"
	}
	if c.Arrival == "" {
		c.Arrival = "constant"
	}
}

// Validate reports whether the config is well-formed. It does not mutate;
// applyDefaults must run first so defaulted fields validate.
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
	if c.Concurrency < 0 {
		return fmt.Errorf("concurrency must not be negative")
	}
	if c.MaxInflight < 0 {
		return fmt.Errorf("max-inflight must not be negative")
	}
	if c.Concurrency > 0 {
		return nil
	}
	// With steps, the base --rate is never consulted: each segment carries
	// its own rate, so requiring it here would force a meaningless flag.
	if len(c.Steps) == 0 && c.Rate <= 0 {
		return fmt.Errorf("rate must be positive in open-loop mode (or set concurrency for closed-loop)")
	}
	if c.RampEndRate < 0 {
		return fmt.Errorf("ramp-end-rate must not be negative")
	}
	if len(c.Steps) > 0 || c.RampEndRate > 0 {
		if c.Arrival != "" && c.Arrival != "constant" {
			return fmt.Errorf("arrival %q cannot be combined with steps or a ramp: those patterns define their own deterministic schedule", c.Arrival)
		}
		return nil
	}
	a := c.Arrival
	if a == "" {
		a = "constant"
	}
	if a != "constant" && a != "poisson" {
		return fmt.Errorf("arrival must be \"constant\" or \"poisson\"")
	}
	return nil
}

// inflightLimit is the in-flight ceiling the run enforces in open-loop mode.
// It derives from the peak scheduled rate, not the base --rate flag: with
// steps like "10:60s,200:60s" the knee segment needs 4*200 in-flight room,
// and a cap sized from the 10 base rate would clip the knee by default.
func (c *Config) inflightLimit() int {
	if c.MaxInflight > 0 {
		return c.MaxInflight
	}
	if c.Concurrency > 0 {
		return c.Concurrency
	}
	peak := c.Rate
	for _, s := range c.Steps {
		if s.Rate > peak {
			peak = s.Rate
		}
	}
	if c.RampEndRate > peak {
		peak = c.RampEndRate
	}
	return int(math.Max(1, math.Ceil(4*peak)))
}

// NewRunID returns a run identifier: a UTC timestamp plus random hex bytes,
// so runs started within the same second remain distinct. The same format is
// used by the CLI and by the Run fallback.
func NewRunID() string {
	var b [3]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		// The timestamp alone still identifies the run well enough.
		return time.Now().UTC().Format("20060102T150405Z")
	}
	return time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}

// Run statuses. The summary file is written with StatusRunning at run start
// and rewritten with the final status at the end, so a driver that dies
// mid-run leaves a summary that is observable as running.
const (
	StatusRunning = "running"
	StatusValid   = "valid"
	StatusInvalid = "invalid"
)

// RunConfig records the resolved run configuration in the summary, so a
// summary reproduces its run without access to the CLI invocation.
type RunConfig struct {
	URL          string             `json:"url"`
	Model        string             `json:"model,omitempty"`
	Seed         int64              `json:"seed"`
	WarmupMs     int64              `json:"warmup_ms"`
	DurationMs   int64              `json:"duration_ms"`
	Concurrency  int                `json:"concurrency"`
	Rate         float64            `json:"rate"`
	Arrival      string             `json:"arrival,omitempty"`
	RampEndRate  float64            `json:"ramp_end_rate,omitempty"`
	Steps        string             `json:"steps,omitempty"`
	MaxInflight  int                `json:"max_inflight,omitempty"`
	Stream       bool               `json:"stream"`
	TimeoutMs    int64              `json:"timeout_ms"`
	DrainGraceMs int64              `json:"drain_grace_ms,omitempty"`
	Weights      map[string]float64 `json:"weights,omitempty"`
	GenerationParams
	SchedulerURL    string `json:"scheduler_url,omitempty"`
	ExpectedWorkers int    `json:"expected_workers,omitempty"`
	Smoke           bool   `json:"smoke,omitempty"`
	RedisAddr       string `json:"redis_addr,omitempty"`
	InterruptKey    string `json:"interrupt_key,omitempty"`
}

// RunConfig renders the resolved configuration for the summary.
func (c *Config) RunConfig() *RunConfig {
	rc := &RunConfig{
		URL:              c.URL,
		Model:            c.Model,
		Seed:             c.Seed,
		WarmupMs:         c.Warmup.Milliseconds(),
		DurationMs:       c.Duration.Milliseconds(),
		Concurrency:      c.Concurrency,
		Rate:             c.Rate,
		Arrival:          c.Arrival,
		RampEndRate:      c.RampEndRate,
		MaxInflight:      c.MaxInflight,
		Stream:           c.Stream,
		TimeoutMs:        c.Timeout.Milliseconds(),
		DrainGraceMs:     c.DrainGrace.Milliseconds(),
		Weights:          c.Weights,
		GenerationParams: c.GenerationParams,
		SchedulerURL:     c.SchedulerURL,
		ExpectedWorkers:  c.ExpectedWorkers,
		Smoke:            c.Smoke,
		RedisAddr:        c.RedisAddr,
		InterruptKey:     c.InterruptKey,
	}
	if len(c.Steps) > 0 {
		parts := make([]string, len(c.Steps))
		for i, s := range c.Steps {
			parts[i] = fmt.Sprintf("%g:%s", s.Rate, s.Duration)
		}
		rc.Steps = strings.Join(parts, ",")
	}
	return rc
}

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
	// RequestedRate is the schedule-implied offered average rate for the
	// measured window (open-loop only; omitted for closed-loop runs, where
	// the base --rate flag describes nothing).
	RequestedRate    float64 `json:"requested_rate,omitempty"`
	AchievedSendRate float64 `json:"achieved_send_rate"`
	// OfferedRequests is the schedule-implied arrival count for the
	// measured window (expected for Poisson); SkippedOffered is how many
	// of those were neither sent nor rejected by the in-flight semaphore
	// — the residual schedule-vs-clock error (open-loop only; closed-loop
	// reports neither). SkippedBackpressure counts arrivals the semaphore
	// refused before they could be sent.
	OfferedRequests     int     `json:"offered_requests,omitempty"`
	SkippedOffered      int     `json:"skipped_offered,omitempty"`
	SkippedBackpressure int     `json:"skipped_backpressure,omitempty"`
	Throughput          float64 `json:"throughput_rps"`
	// MaxInflight records the in-flight ceiling the run enforced (open
	// loop), so saturated-latency numbers are read together with the cap
	// that produced them (Little's law: the cap sets the latency band).
	MaxInflight    int     `json:"max_inflight,omitempty"`
	LatencyMsP50   float64 `json:"latency_ms_p50"`
	LatencyMsP95   float64 `json:"latency_ms_p95"`
	LatencyMsP99   float64 `json:"latency_ms_p99"`
	LatencyMsMean  float64 `json:"latency_ms_mean"`
	LatencyMsMin   float64 `json:"latency_ms_min"`
	LatencyMsMax   float64 `json:"latency_ms_max"`
	TTFTMsP50      float64 `json:"ttft_ms_p50"`
	TTFTMsP95      float64 `json:"ttft_ms_p95"`
	TTFTMsP99      float64 `json:"ttft_ms_p99"`
	TTFTMsMean     float64 `json:"ttft_ms_mean"`
	TokensOutTotal int64   `json:"tokens_out_total"`
	TokensInTotal  int64   `json:"tokens_in_total"`
	// TokensOutEstimatedResponses counts successful measured responses
	// whose tokens_out was estimated rather than reported by the backend;
	// tok/s claims are only trustworthy when it is zero.
	TokensOutEstimatedResponses int            `json:"tokens_out_estimated_responses,omitempty"`
	OutputPath                  string         `json:"output_path"`
	Mode                        string         `json:"mode"`
	Datasets                    map[string]int `json:"datasets"`
	// GenerationParams records the run's output-control parameters, so a
	// summary is self-describing about what shaped its output lengths.
	GenerationParams
	// Config is the resolved run configuration (seed, arrival, steps,
	// concurrency, timeouts, weights, generation params), so a summary
	// reproduces its run.
	Config *RunConfig `json:"config,omitempty"`
	// VCSRev/VCSModified record the traffic binary's build provenance
	// (debug.ReadBuildInfo); empty when built without VCS stamping.
	VCSRev      string `json:"vcs_rev,omitempty"`
	VCSModified bool   `json:"vcs_modified,omitempty"`
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
	// InterruptedBeforeRun records whether the infra-level interruption
	// flag was already set when the run started (it is cleared afterwards,
	// so the finalize check reflects only interruptions during this run).
	InterruptedBeforeRun bool `json:"interrupted_before_run,omitempty"`
}

// accumulator collects counters from concurrent send goroutines.
type accumulator struct {
	mu        sync.Mutex
	warmup    int
	requests  int
	successes int
	failures  int
	estimated int
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
		a.latency.AddUS(r.LatencyUs)
		if r.HasTTFT {
			a.ttft.AddUS(r.TTFTUs)
		}
		a.tokensOut += r.TokensOut
		if r.TokensSource == "estimated" {
			a.estimated++
		}
	} else {
		a.failures++
		if r.ErrorKind != "" {
			a.errors[r.ErrorKind]++
		} else if r.Error != "" {
			a.errors["transport"]++
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

// pendingSend is one scheduled request, drawn by the scheduler goroutine so
// the arrival->prompt mapping is deterministic for a seed: the row choice
// and the send sequence number are assigned when the arrival is scheduled,
// not when its response completes.
type pendingSend struct {
	row Row
	seq int64
}

// runner owns the shared state of one run. Methods serialize their own
// access; send goroutines only touch the accumulator and writer, which are
// safe for concurrent use.
type runner struct {
	cfg      *Config
	sender   *Sender
	selector *WeightedRowSource
	writer   *ResultWriter
	acc      *accumulator
	barrier  *warmupBarrier

	sem                 chan struct{}
	seq                 atomic.Int64
	skippedBackpressure atomic.Int64
}

// nextPending draws the row and the send sequence number. It must run in the
// scheduling goroutine (closed-loop worker or the open-loop scheduler), so
// two runs with the same seed replay the same send->prompt sequence.
func (r *runner) nextPending() pendingSend {
	return pendingSend{row: r.selector.Next(), seq: r.seq.Add(1)}
}

// send executes one request and records it. It returns the classification
// of the failure ("" on success) so closed-loop workers can back off.
func (r *runner) send(ctx context.Context, pending pendingSend, warmup, tracked bool) (bool, string) {
	if tracked {
		defer r.barrier.done()
	}
	sctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout+5*time.Second)
	defer cancel()

	sendAt := time.Now()
	res, err := r.sender.Send(sctx, pending.row)
	latency := time.Since(sendAt)
	ok := err == nil && res.Status == 200

	rec := Record{
		SchemaVersion: SchemaVersion,
		RunID:         r.cfg.RunID,
		Seq:           pending.seq,
		Dataset:       pending.row.Dataset,
		RowIndex:      pending.row.Index,
		Warmup:        warmup,
		SendAt:        sendAt,
		Status:        res.Status,
		LatencyUs:     latency.Microseconds(),
		TPOTUs:        res.TPOT.Microseconds(),
		Chunks:        res.Chunks,
		TokensIn:      res.APIResp.TokensIn,
		TokensOut:     res.APIResp.TokensOut,
		TokensSource:  res.TokensSource,
		MaxTokens:     r.cfg.GenerationParams.MaxTokens,
		APITotalMs:    res.APIResp.TotalMs,
	}
	if res.TTFT > 0 {
		rec.HasTTFT = true
		rec.TTFTUs = res.TTFT.Microseconds()
		rec.FirstChunkAt = &res.FirstChunkAt
		rec.LastChunkAt = &res.LastChunkAt
	}
	rec.ErrorKind = classifySendError(err, res)
	if err != nil {
		rec.Error = err.Error()
	}
	if werr := r.writer.Write(rec); werr != nil {
		fmt.Fprintf(os.Stderr, "warning: write result: %v\n", werr)
	}
	r.acc.add(rec, ok)
	return ok, rec.ErrorKind
}

// Run executes the configured traffic run and returns its summary.
func Run(ctx context.Context, cfg *Config) (*Summary, error) {
	cfg.applyDefaults()
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

	// Interruption flag lifecycle: snapshot and clear a stale flag at run
	// start, so the finalize check reflects only interruptions that happen
	// during this run (a previous incident must not invalidate every run
	// for the flag's remaining TTL).
	var interruptedBeforeRun bool
	if cfg.RedisAddr != "" {
		interruptedBeforeRun = checkInterrupted(cfg)
		if interruptedBeforeRun {
			clearInterrupted(cfg)
		}
	}

	fileWriter, err := NewResultWriter(cfg.OutputPath)
	if err != nil {
		return nil, err
	}
	defer fileWriter.Close()
	if cfg.RecordsStdout {
		fileWriter.Tee(os.Stdout)
	}

	// sendCtx is derived so a hung drain can be cancelled without
	// cancelling the caller's context: after the drain timeout the run
	// still finalizes a summary, and in-flight sends must stop writing to
	// (soon closed) results files.
	sendCtx, sendCancel := context.WithCancel(ctx)
	defer sendCancel()

	r := &runner{
		cfg:      cfg,
		sender:   NewSender(cfg.URL, cfg.Model, cfg.Stream, cfg.GenerationParams, cfg.Timeout),
		selector: NewWeightedRowSource(cfg.Rows, cfg.Weights, cfg.Seed),
		writer:   fileWriter,
		acc:      newAccumulator(),
		sem:      make(chan struct{}, cfg.inflightLimit()),
	}
	pattern := buildPattern(cfg)
	total := cfg.Warmup + cfg.Duration
	started := time.Now()
	warmupEnd := started.Add(cfg.Warmup)
	r.barrier = newWarmupBarrier(warmupEnd, cfg.DrainGrace)
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
		Config:        cfg.RunConfig(),
		VCSRev:        cfg.VCSRev,
		VCSModified:   cfg.VCSModified,
	}
	if err := WriteSummary(summaryPath, startSummary); err != nil {
		fmt.Fprintf(os.Stderr, "warning: write start summary: %v\n", err)
	}

	var wg sync.WaitGroup
	if cfg.Concurrency > 0 {
		// Closed loop: each worker is its own scheduler; it draws the row
		// and send_seq itself right before sending, so the send order is
		// the arrival order and the mapping stays deterministic.
		for i := 0; i < cfg.Concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				crossed := false
				deadline := started.Add(total)
				backoff := time.Duration(0)
				for {
					now := time.Now()
					warmup := now.Before(warmupEnd)
					if !warmup && !crossed {
						// Drain in-flight warmup requests before
						// the first measured send leaves this worker.
						r.barrier.close(sendCtx)
						deadline = time.Now().Add(cfg.Duration)
						crossed = true
					}
					if !now.Before(deadline) {
						return
					}
					tracked := r.barrier.mark(now)
					_, kind := r.send(sendCtx, r.nextPending(), warmup, tracked)
					if sendCtx.Err() != nil {
						return
					}
					// Back off on connection-level failures so a dead
					// API cannot turn a worker into a failure-record
					// generator; status-code failures are real server
					// answers and keep the loop tight.
					switch kind {
					case "", "http_429", "http_5xx":
						if kind == "" {
							backoff = 0
						}
					default:
						backoff = maxBackoff(backoff)
						select {
						case <-time.After(backoff):
						case <-sendCtx.Done():
							return
						}
					}
				}
			}()
		}
	} else {
		// Open loop: one scheduler goroutine owns the pattern clock. Each
		// send time is computed from the absolute schedule (anchored at the
		// phase start), not from the wall clock after the previous launch,
		// so timer and spawn overhead cannot drift the arrivals; running
		// late only collapses the next delay, never shifts the schedule.
		wg.Add(1)
		go func() {
			defer wg.Done()
			launch := func(delay time.Duration, warmup, tracked bool) bool {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-sendCtx.Done():
					return false
				case <-timer.C:
				}
				select {
				case <-sendCtx.Done():
					return false
				case r.sem <- struct{}{}:
				default:
					// Non-blocking try: the scheduled arrival was offered
					// but the in-flight cap prevents it from being sent at
					// all. Counted separately from the offered-sent gap.
					r.skippedBackpressure.Add(1)
					if tracked {
						r.barrier.done()
					}
					return true
				}
				pending := r.nextPending()
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-r.sem }()
					r.send(sendCtx, pending, warmup, tracked)
				}()
				return true
			}

			// Warmup phase: pattern clock anchored at the run start,
			// stopping at the boundary. Warmup sends register with the
			// barrier at their scheduled time.
			if cfg.Warmup > 0 {
				next := started
				for {
					next = next.Add(pattern.NextDelay(next.Sub(started)))
					if !next.Before(warmupEnd) {
						// The next scheduled send lands past the
						// boundary; cut warmup traffic here.
						break
					}
					tracked := r.barrier.mark(next)
					if !launch(time.Until(next), true, tracked) {
						if tracked {
							r.barrier.done()
						}
						return
					}
				}
				r.barrier.close(sendCtx)
			}

			// Measured phase: pattern clock anchored at measuredStart so
			// the drain gap does not skip scheduled arrivals, and ramp or
			// step patterns span the measured window.
			measuredStart := time.Now()
			end := measuredStart.Add(cfg.Duration)
			next := measuredStart
			for {
				next = next.Add(pattern.NextDelay(next.Sub(measuredStart)))
				if !next.Before(end) {
					return
				}
				if !launch(time.Until(next), false, false) {
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
	case <-time.After(total + cfg.DrainGrace + cfg.Timeout + 10*time.Second):
		// Cancel the in-flight sends and wait briefly: after Run returns
		// the results file closes, and sends must not outlive it.
		fmt.Fprintf(os.Stderr, "warning: in-flight requests did not drain within grace; cancelling and returning a partial summary\n")
		sendCancel()
		select {
		case <-done:
		case <-time.After(cfg.DrainGrace):
			fmt.Fprintf(os.Stderr, "warning: sends still in flight after cancel; their records are missing from the summary\n")
		}
	}
	finished := time.Now()

	s := r.acc.snapshot(cfg)
	s.SchemaVersion = SchemaVersion
	s.RunID = cfg.RunID
	s.StartedAt = started
	s.MeasuredStartedAt = r.barrier.start
	if s.MeasuredStartedAt.IsZero() {
		s.MeasuredStartedAt = started
	}
	s.WarmupDrainMs = r.barrier.drainWall.Milliseconds()
	s.FinishedAt = finished
	s.OutputPath = cfg.OutputPath
	s.Config = cfg.RunConfig()
	s.VCSRev = cfg.VCSRev
	s.VCSModified = cfg.VCSModified
	s.InterruptedBeforeRun = interruptedBeforeRun
	if cfg.Concurrency == 0 {
		// Open-loop: offered is the schedule-implied arrival count for the
		// measured window, and requested_rate is the true offered average
		// (with steps, cfg.Rate is just the base flag, not what was
		// scheduled). skipped_backpressure is reported separately; the
		// residual gap is the schedule-vs-clock error.
		s.OfferedRequests = int(math.Round(offered))
		s.SkippedBackpressure = int(r.skippedBackpressure.Load())
		if residual := s.OfferedRequests - s.Requests - s.SkippedBackpressure; residual > 0 {
			s.SkippedOffered = residual
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

// maxBackoff doubles the previous backoff up to one second; the first
// failure starts at 10ms.
func maxBackoff(prev time.Duration) time.Duration {
	if prev == 0 {
		return 10 * time.Millisecond
	}
	if next := 2 * prev; next < time.Second {
		return next
	}
	return time.Second
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

// clearInterrupted deletes the interruption flag, so a stale flag from a
// previous incident does not invalidate this run. Failures are warnings:
// a leftover flag still shows up in the finalize check.
func clearInterrupted(cfg *Config) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer func() { _ = client.Close() }()
	if err := client.Del(ctx, cfg.InterruptKey).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: clear interruption flag: %v\n", err)
	}
}

func buildPattern(cfg *Config) Pattern {
	rng := mathrand.New(mathrand.NewPCG(mixSeed(uint64(cfg.Seed), 1), mixSeed(uint64(cfg.Seed), 2)))
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
	if cfg.Stream {
		s.Mode += "+stream"
		if a.ttft.Count() > 0 {
			s.TTFTMsP50 = a.ttft.Percentile(50)
			s.TTFTMsP95 = a.ttft.Percentile(95)
			s.TTFTMsP99 = a.ttft.Percentile(99)
			s.TTFTMsMean = a.ttft.Mean()
		}
	}
	s.TokensOutEstimatedResponses = a.estimated
	return s
}

func modeLabel(cfg *Config) string {
	if cfg.Concurrency > 0 {
		return fmt.Sprintf("closed-loop:%d", cfg.Concurrency)
	}
	return "open-loop"
}

// RunSignalContext wraps ctx with SIGINT/SIGTERM cancellation for graceful
// shutdown of a run started from the CLI. SIGKILL cannot be caught by any
// process; the signal list covers what a driver (and Kubernetes eviction,
// which sends SIGTERM) can deliver.
func RunSignalContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
}
