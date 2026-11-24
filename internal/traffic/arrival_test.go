package traffic

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func TestRunSendRowDeterminism(t *testing.T) {
	// Two runs with the same seed must replay the same send_seq → row
	// mapping: the scheduler goroutine draws the row (and the sequence
	// number) when the arrival is scheduled, not when the response
	// completes.
	rows := make([]Row, 20)
	for i := range rows {
		rows[i] = Row{Dataset: "d", Index: i, Text: "p"}
	}
	srv := fakeAPI(t, time.Millisecond, 200)
	out := t.TempDir() + "/results.jsonl"
	cfg := &Config{
		URL:        srv.URL,
		Rows:       rows,
		Weights:    map[string]float64{"d": 1},
		Seed:       42,
		Duration:   150 * time.Millisecond,
		Rate:       100,
		Timeout:    5 * time.Second,
		OutputPath: out,
	}
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	first := seqToRow(t, out)

	out2 := t.TempDir() + "/results.jsonl"
	cfg.OutputPath = out2
	if _, err := Run(context.Background(), cfg); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	second := seqToRow(t, out2)

	if len(first) == 0 {
		t.Fatal("no records")
	}
	for seq, row := range first {
		if second[seq] != row {
			t.Fatalf("mapping diverges at seq %d: %d vs %d", seq, row, second[seq])
		}
	}
	if len(first) != len(second) {
		t.Fatalf("record counts differ: %d vs %d", len(first), len(second))
	}
}

func seqToRow(t *testing.T, path string) map[int64]int {
	t.Helper()
	m := make(map[int64]int)
	for _, rec := range readRecords(t, path) {
		m[rec.Seq] = rec.RowIndex
	}
	return m
}

func TestInflightLimitUsesPeakStepRate(t *testing.T) {
	// With steps, the cap must size for the knee, not the base flag.
	cfg := &Config{Rate: 10, Steps: []StepSeg{{Rate: 10, Duration: 60 * time.Second}, {Rate: 200, Duration: 60 * time.Second}}}
	if got := cfg.inflightLimit(); got != 800 {
		t.Errorf("inflight with steps = %d, want 800 (4 * peak 200)", got)
	}
	cfg = &Config{Rate: 10, RampEndRate: 90}
	if got := cfg.inflightLimit(); got != 360 {
		t.Errorf("inflight with ramp = %d, want 360 (4 * max(10, 90))", got)
	}
}

func TestRunSignalContextHandlesSIGTERM(t *testing.T) {
	// Kubernetes sends SIGTERM on eviction; SIGKILL cannot be caught by
	// any process. RunSignalContext must cancel on SIGTERM.
	if runtime.GOOS == "windows" {
		t.Skip("no SIGTERM on windows")
	}
	ctx, cancel := RunSignalContext(context.Background())
	defer cancel()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM to self: %v", err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("context was not cancelled by SIGTERM")
	}
}

func TestClassifySendError(t *testing.T) {
	_, portErr := net.Dial("tcp", "127.0.0.1:1")
	cases := []struct {
		name string
		err  error
		res  SendResult
		want string
	}{
		{"nil", nil, SendResult{}, ""},
		{"status 429", errors.New("unexpected status: too many"), SendResult{Status: 429}, "http_429"},
		{"status 500", errors.New("unexpected status: oops"), SendResult{Status: 500}, "http_5xx"},
		{"status 404", errors.New("unexpected status: nope"), SendResult{Status: 404}, "http_404"},
		{"timeout", &netError{timeout: true}, SendResult{}, "timeout"},
		{"conn refused", portErr, SendResult{}, "conn_refused"},
		{"conn reset", errors.New("request failed: read tcp: connection reset by peer"), SendResult{}, "conn_reset"},
		{"stream error event", errors.New("stream error event: timed out"), SendResult{}, "stream_error"},
		{"truncated", errors.New("stream ended without chunks"), SendResult{}, "stream_truncated"},
		{"decode", errors.New("decode chunk: bad json"), SendResult{}, "decode"},
		{"transport", errors.New("request failed: something else"), SendResult{}, "transport"},
	}
	for _, tc := range cases {
		if got := classifySendError(tc.err, tc.res); got != tc.want {
			t.Errorf("%s: classifySendError = %q, want %q", tc.name, got, tc.want)
		}
	}
}

type netError struct{ timeout bool }

func (e *netError) Error() string   { return "i/o timeout" }
func (e *netError) Timeout() bool   { return e.timeout }
func (e *netError) Temporary() bool { return false }

func TestParseStepsRejectsNonPositiveRates(t *testing.T) {
	// A zero or negative rate makes NextDelay return an hour and the run
	// hang until the drain timeout; it must be rejected at parse time.
	for _, spec := range []string{"0:30s", "-5:30s", "10:30s,0:10s", "10:30s,-1:10s"} {
		if _, err := ParseSteps(spec); err == nil {
			t.Errorf("ParseSteps(%q) accepted a non-positive rate", spec)
		}
	}
	if _, err := ParseSteps("10:30s"); err != nil {
		t.Errorf("ParseSteps(10:30s): %v", err)
	}
}

func TestPoissonDistributionShape(t *testing.T) {
	// Beyond the mean, the delays must actually be exponential: the
	// coefficient of variation of an exponential distribution is 1.
	p := NewPoissonPattern(100, rand.New(rand.NewPCG(7, 9)))
	n := 5000
	var sum, sumSq float64
	for i := 0; i < n; i++ {
		d := p.NextDelay(0).Seconds()
		sum += d
		sumSq += d * d
	}
	mean := sum / float64(n)
	wantMean := 10 * time.Millisecond // 1/100s
	if mean < wantMean.Seconds()*0.85 || mean > wantMean.Seconds()*1.15 {
		t.Errorf("mean delay = %.1fms, want ~%.1fms", mean*1000, wantMean.Seconds()*1000)
	}
	variance := sumSq/float64(n) - mean*mean
	cv := variance / (mean * mean) // squared CV
	if cv < 0.6 || cv > 1.4 {
		t.Errorf("squared CV = %.2f, want ~1 (exponential shape)", cv)
	}
}

func TestRampOfferedConsistentWithNextDelayPastEnd(t *testing.T) {
	// Past the ramp duration the pattern holds the end rate (NextDelay
	// clamps), so Offered must keep integrating that rate instead of
	// truncating: the two patterns must behave consistently.
	const rate = 10.0
	p := RampPattern{StartRate: rate, EndRate: 2 * rate, Duration: time.Second}
	got := p.Offered(2 * time.Second)
	want := (rate+2*rate)/2*1 + 2*rate*1 // trapezoid over the ramp + end rate held for another second
	if got != want {
		t.Errorf("Offered(2s) = %v, want %v", got, want)
	}
}

func TestNewRunIDUniqueWithinSameSecond(t *testing.T) {
	ids := make(map[string]bool, 100)
	for i := 0; i < 100; i++ {
		id := NewRunID()
		if ids[id] {
			t.Fatalf("duplicate run id %q within one second", id)
		}
		ids[id] = true
	}
}

func TestValidateRejectsPoissonWithStepsOrRamp(t *testing.T) {
	// --arrival poisson with steps or a ramp is silently ignored by the
	// pattern selection; the combination must be rejected instead (M12).
	rows := []Row{{Dataset: "d", Index: 0, Text: "p"}}
	cfg := &Config{URL: "http://x", Rows: rows, Duration: time.Second, Rate: 10,
		Arrival: "poisson", Steps: []StepSeg{{Rate: 10, Duration: time.Second}}}
	if err := cfg.Validate(); err == nil {
		t.Error("poisson + steps accepted")
	}
	cfg = &Config{URL: "http://x", Rows: rows, Duration: time.Second, Rate: 10,
		Arrival: "poisson", RampEndRate: 20}
	if err := cfg.Validate(); err == nil {
		t.Error("poisson + ramp accepted")
	}
}

func TestSummaryRecordsResolvedConfig(t *testing.T) {
	// The summary must reproduce its run: resolved config (seed, steps,
	// timeout, weights) embedded, requested_rate present in open loop and
	// omitted in closed loop (M7).
	srv := fakeAPI(t, time.Millisecond, 200)
	out := t.TempDir() + "/results.jsonl"
	cfg := &Config{
		URL:        srv.URL,
		Rows:       []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights:    map[string]float64{"d": 1},
		Seed:       9,
		Duration:   100 * time.Millisecond,
		Rate:       50,
		Steps:      []StepSeg{{Rate: 50, Duration: 100 * time.Millisecond}},
		Timeout:    5 * time.Second,
		VCSRev:     "abc123",
		OutputPath: out,
	}
	summary, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	rc := summary.Config
	if rc == nil {
		t.Fatal("summary has no resolved config")
	}
	if rc.Seed != 9 || rc.Steps != "50:100ms" || rc.TimeoutMs != 5000 || len(rc.Weights) != 1 {
		t.Errorf("resolved config incomplete: %+v", rc)
	}
	if summary.RequestedRate != 50 {
		t.Errorf("requested_rate = %v, want the steps-implied 50", summary.RequestedRate)
	}
	if summary.VCSRev != "abc123" {
		t.Errorf("vcs_rev = %q, want abc123", summary.VCSRev)
	}

	out2 := t.TempDir() + "/results.jsonl"
	closed := &Config{
		URL: srv.URL, Rows: []Row{{Dataset: "d", Index: 0, Text: "p"}},
		Weights: map[string]float64{"d": 1}, Seed: 9,
		Duration: 100 * time.Millisecond, Concurrency: 2,
		Timeout: 5 * time.Second, OutputPath: out2,
	}
	summary, err = Run(context.Background(), closed)
	if err != nil {
		t.Fatalf("closed-loop Run: %v", err)
	}
	if summary.RequestedRate != 0 {
		t.Errorf("closed-loop requested_rate = %v, want 0 (omitted: the flag describes nothing there)", summary.RequestedRate)
	}
	if summary.Config == nil || summary.Config.Concurrency != 2 {
		t.Errorf("closed-loop config not recorded: %+v", summary.Config)
	}
}
