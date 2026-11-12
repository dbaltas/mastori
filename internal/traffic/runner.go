package traffic

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
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
	OutputPath  string
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
	if c.RunID == "" {
		c.RunID = time.Now().UTC().Format("20060102T150405")
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

type Summary struct {
	RunID            string         `json:"run_id"`
	StartedAt        time.Time      `json:"started_at"`
	FinishedAt       time.Time      `json:"finished_at"`
	WarmupRequests   int            `json:"warmup_requests"`
	Requests         int            `json:"requests"`
	Successes        int            `json:"successes"`
	Failures         int            `json:"failures"`
	ByStatus         map[int]int    `json:"by_status"`
	Errors           map[string]int `json:"errors"`
	RequestedRate    float64        `json:"requested_rate"`
	AchievedSendRate float64        `json:"achieved_send_rate"`
	Throughput       float64        `json:"throughput_rps"`
	LatencyMsP50     float64        `json:"latency_ms_p50"`
	LatencyMsP95     float64        `json:"latency_ms_p95"`
	LatencyMsP99     float64        `json:"latency_ms_p99"`
	LatencyMsMean    float64        `json:"latency_ms_mean"`
	LatencyMsMin     float64        `json:"latency_ms_min"`
	LatencyMsMax     float64        `json:"latency_ms_max"`
	TTFTMsP50        float64        `json:"ttft_ms_p50"`
	TTFTMsP95        float64        `json:"ttft_ms_p95"`
	TTFTMsMean       float64        `json:"ttft_ms_mean"`
	TokensOutTotal   int64          `json:"tokens_out_total"`
	OutputPath       string         `json:"output_path"`
	Mode             string         `json:"mode"`
	Datasets         map[string]int `json:"datasets"`
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
	latency   LatencyStats
	ttft      LatencyStats
	datasets  map[string]int
}

func newAccumulator() *accumulator {
	return &accumulator{
		byStatus: make(map[int]int),
		errors:   make(map[string]int),
		datasets: make(map[string]int),
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
}

// Run executes the configured traffic run and returns its summary.
func Run(ctx context.Context, cfg *Config) (*Summary, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	writer, err := NewResultWriter(cfg.OutputPath)
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	sender := NewSender(cfg.URL, cfg.Model, cfg.Stream, cfg.Timeout)
	selector := NewWeightedRowSource(cfg.Rows, cfg.Weights, cfg.Seed)
	pattern := buildPattern(cfg)
	total := cfg.Warmup + cfg.Duration
	started := time.Now()
	warmupEnd := started.Add(cfg.Warmup)

	var seq atomic.Int64
	acc := newAccumulator()

	var wg sync.WaitGroup
	if cfg.Concurrency > 0 {
		deadline := started.Add(total)
		for i := 0; i < cfg.Concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					runSend(ctx, sender, selector, writer, acc, &seq, warmupEnd, cfg)
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
			end := started.Add(total)
			for {
				elapsed := time.Since(started)
				if !time.Now().Before(end) {
					return
				}
				delay := pattern.NextDelay(elapsed)
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				select {
				case <-ctx.Done():
					return
				case sem <- struct{}{}:
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() { <-sem }()
					runSend(ctx, sender, selector, writer, acc, &seq, warmupEnd, cfg)
				}()
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
	case <-time.After(total + cfg.Timeout + 10*time.Second):
		fmt.Fprintf(os.Stderr, "warning: in-flight requests did not drain within grace; returning partial summary\n")
	}
	finished := time.Now()

	s := acc.snapshot(cfg)
	s.RunID = cfg.RunID
	s.StartedAt = started
	s.FinishedAt = finished
	s.OutputPath = cfg.OutputPath
	return s, nil
}

func runSend(ctx context.Context, sender *Sender, selector *WeightedRowSource, writer *ResultWriter, acc *accumulator, seq *atomic.Int64, warmupEnd time.Time, cfg *Config) {
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
		RunID:      cfg.RunID,
		Seq:        seq.Add(1),
		Dataset:    row.Dataset,
		RowIndex:   row.Index,
		Warmup:     sendAt.Before(warmupEnd),
		SendAt:     sendAt,
		Status:     res.Status,
		LatencyMs:  latency.Milliseconds(),
		TTFTMs:     res.TTFT.Milliseconds(),
		Chunks:     res.Chunks,
		TokensIn:   res.APIResp.TokensIn,
		TokensOut:  res.APIResp.TokensOut,
		APITotalMs: res.APIResp.TotalMs,
		Error:      errMsg,
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
		Datasets:       a.datasets,
		RequestedRate:  cfg.Rate,
	}
	if cfg.Duration > 0 {
		s.AchievedSendRate = float64(a.requests) / cfg.Duration.Seconds()
		s.Throughput = float64(a.successes) / cfg.Duration.Seconds()
	}
	if a.latency.Count() > 0 {
		s.LatencyMsP50 = a.latency.Percentile(50)
		s.LatencyMsP95 = a.latency.Percentile(95)
		s.LatencyMsP99 = a.latency.Percentile(99)
		s.LatencyMsMean = a.latency.Mean()
		s.LatencyMsMin = a.latency.Min()
		s.LatencyMsMax = a.latency.Max()
	}
	if cfg.Concurrency > 0 {
		s.Mode = fmt.Sprintf("closed-loop:%d", cfg.Concurrency)
	} else {
		s.Mode = "open-loop"
	}
	if cfg.Stream {
		s.Mode += "+stream"
		s.TTFTMsP50 = a.ttft.Percentile(50)
		s.TTFTMsP95 = a.ttft.Percentile(95)
		s.TTFTMsMean = a.ttft.Mean()
	}
	return s
}

// RunSignalContext wraps ctx with SIGINT/SIGTERM cancellation for graceful
// shutdown of a run started from the CLI.
func RunSignalContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, os.Interrupt, os.Kill)
}
