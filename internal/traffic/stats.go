package traffic

import (
	"math"

	"github.com/HdrHistogram/hdrhistogram-go"
)

const (
	// Histogram values are recorded at microsecond resolution so that
	// sub-millisecond latencies (e.g. TTFT) keep meaningful precision.
	histUnitMs      = 1000.0 // µs per ms
	trackableMaxMs  = 10 * 60 * 1000
	histSigFigs     = 3
	histRelErrorPct = 1.0
)

// LatencyStats accumulates latencies of successful measured requests and
// computes the standard benchmark percentiles.
//
// Values are accumulated in an HDR histogram: memory is bounded regardless
// of run length or request length, and at 3 significant figures the
// relative percentile error is about ±0.1% (HDR histogram buckets cover
// 1/1000th of the value at full precision — the older "±1% at 3 sig figs"
// reading confused decimal digits with binary bucket counts). Values above
// the 10-minute trackable maximum are clamped to it silently by design:
// such requests are counted (so success rates stay honest), and their raw
// values remain available in the results JSONL for exact offline
// percentiles. Tests tolerate 1% because the recorded value and the
// histogram bucket that quantile falls in can straddle a bucket edge.
type LatencyStats struct {
	h *hdrhistogram.Histogram
}

func newLatencyStats() *LatencyStats {
	return &LatencyStats{h: hdrhistogram.New(1, trackableMaxMs*1000, histSigFigs)}
}

// Add records one latency in milliseconds. Callers must serialize Add calls
// (the traffic accumulator does); the zero value is usable.
func (s *LatencyStats) Add(ms float64) {
	s.AddUS(int64(math.Round(ms * histUnitMs)))
}

// AddUS records one latency in microseconds. Callers must serialize AddUS
// calls (the traffic accumulator does); the zero value is usable.
func (s *LatencyStats) AddUS(us int64) {
	if s.h == nil {
		s.h = hdrhistogram.New(1, trackableMaxMs*1000, histSigFigs)
	}
	if us < 1 {
		us = 1
	}
	if err := s.h.RecordValue(us); err != nil {
		_ = s.h.RecordValue(trackableMaxMs * 1000)
	}
}

func (s *LatencyStats) Count() int {
	if s.h == nil {
		return 0
	}
	return int(s.h.TotalCount())
}

// Percentile returns the p'th percentile (0..100) in milliseconds, accurate
// to the histogram's configured significant figures.
func (s *LatencyStats) Percentile(p float64) float64 {
	if s.h == nil || s.h.TotalCount() == 0 {
		return 0
	}
	return float64(s.h.ValueAtQuantile(p)) / histUnitMs
}

func (s *LatencyStats) Min() float64 {
	if s.h == nil || s.h.TotalCount() == 0 {
		return 0
	}
	return float64(s.h.Min()) / histUnitMs
}

func (s *LatencyStats) Max() float64 {
	if s.h == nil || s.h.TotalCount() == 0 {
		return 0
	}
	return float64(s.h.Max()) / histUnitMs
}

func (s *LatencyStats) Mean() float64 {
	if s.h == nil || s.h.TotalCount() == 0 {
		return 0
	}
	return s.h.Mean() / histUnitMs
}
