package traffic

import (
	"context"
	"fmt"
	"time"
)

// SmokeCheck verifies, with one real request, that the streaming path
// actually streams. It is the run-time half of the review's H2 fix: with the
// worker misparsing vLLM's completions stream, every request looked like a
// single-chunk stream with TTFT == e2e, and nobody noticed for two GPU
// sessions. The check runs before the measured phase and aborts the run when
// the stream is degenerate.
type SmokeCheck struct {
	Chunks    int     `json:"chunks"`
	TTFTMs    float64 `json:"ttft_ms"`
	E2EMs     float64 `json:"e2e_ms"`
	TokensIn  int64   `json:"tokens_in"`
	TokensOut int64   `json:"tokens_out"`
}

// RunSmokeCheck sends one smoke request through the streaming endpoint and
// asserts:
//   - chunks > 1: the response arrived as a multi-chunk stream, so TTFT and
//     ITL metrics are real;
//   - TTFT < e2e: the first chunk arrived strictly before completion;
//   - tokens_out == MaxTokens when the run pins output length with
//     ignore_eos: the backend honored the requested cap exactly.
func RunSmokeCheck(ctx context.Context, cfg *Config) (*SmokeCheck, error) {
	if !cfg.Stream {
		return nil, fmt.Errorf("smoke check requires stream mode")
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout+5*time.Second)
	defer cancel()

	sender := NewSender(cfg.URL, cfg.Model, true, cfg.GenerationParams, cfg.Timeout)
	start := time.Now()
	res, err := sender.Send(ctx, Row{Dataset: "smoke", Index: 0, Text: "Respond with a short sentence about the weather."})
	if err != nil {
		return nil, fmt.Errorf("smoke check request failed: %w", err)
	}
	e2e := time.Since(start)

	sc := &SmokeCheck{
		Chunks:    res.Chunks,
		TTFTMs:    float64(res.TTFT.Milliseconds()),
		E2EMs:     float64(e2e.Milliseconds()),
		TokensIn:  res.APIResp.TokensIn,
		TokensOut: res.APIResp.TokensOut,
	}
	if res.Chunks <= 1 {
		return sc, fmt.Errorf("smoke check: stream carried %d chunk(s), want > 1; the streaming path is degenerate (TTFT would equal e2e)", res.Chunks)
	}
	if res.TTFT <= 0 || res.TTFT >= e2e {
		return sc, fmt.Errorf("smoke check: TTFT %v not < e2e %v; first-token timing is not real streaming", res.TTFT, e2e)
	}
	if cfg.MaxTokens > 0 && cfg.IgnoreEOS && res.APIResp.TokensOut != cfg.MaxTokens {
		return sc, fmt.Errorf("smoke check: tokens_out = %d, want exactly max_tokens = %d (ignore_eos run must produce its requested output length)", res.APIResp.TokensOut, cfg.MaxTokens)
	}
	return sc, nil
}
