package traffic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// APIResponse mirrors the /generate response body of the API service.
type APIResponse struct {
	JobID     string `json:"job_id"`
	Text      string `json:"text"`
	TokensIn  int64  `json:"tokens_in"`
	TokensOut int64  `json:"tokens_out"`
	TotalMs   int64  `json:"total_ms"`
}

// SendResult carries one completed request's observations.
type SendResult struct {
	Status  int
	APIResp APIResponse
	// TTFT is first-chunk-at minus the request send time (streaming mode
	// only). It covers the full client path — TCP connect, upload, API
	// admission, queueing, and one RTT before the response headers — so it
	// can never understate the API-side TTFT (design doc §12).
	TTFT time.Duration
	// TPOT is the mean inter-chunk gap: (last-first)/(tokens_out-1),
	// measured from the first to the last token chunk (streaming mode
	// only; 0 when tokens_out <= 1).
	TPOT time.Duration
	// FirstChunkAt and LastChunkAt are the arrival times of the first and
	// last token chunk (streaming mode only).
	FirstChunkAt time.Time
	LastChunkAt  time.Time
	// Chunks counts the token chunk events received, excluding the final
	// Done chunk (streaming mode only).
	Chunks int
	// TokensSource records where tokens_out came from: "usage" when the
	// backend reported the token counts, "estimated" when they were
	// derived some other way (e.g. a word count). Estimated counts are
	// invalid for tok/s claims.
	TokensSource string
}

// Sender posts single requests to the API. It uses the SSE streaming
// endpoint when Stream is set, otherwise the blocking /generate endpoint.
//
// GenerationParams carries the run's output-control parameters; they are sent
// on every request so the backend's defaults (vLLM completions defaults to
// max_tokens=16) cannot silently shape the measured workload.
type Sender struct {
	URL    string
	Model  string
	Stream bool
	GenerationParams
	HTTP *http.Client
}

// GenerationParams controls output length and sampling per request. The zero
// value omits every parameter from the request body, leaving backend defaults
// in place.
type GenerationParams struct {
	// MaxTokens caps output tokens; 0 omits the parameter.
	MaxTokens int64 `json:"max_tokens,omitempty"`
	// IgnoreEOS makes the backend generate until MaxTokens, so output
	// length follows the workload instead of the model's stopping point.
	IgnoreEOS bool `json:"ignore_eos,omitempty"`
	// Temperature pins sampling when non-nil; nil omits the parameter.
	Temperature *float64 `json:"temperature,omitempty"`
}

func (p GenerationParams) body(prompt, model string) map[string]any {
	body := map[string]any{"prompt": prompt}
	if model != "" {
		body["model"] = model
	}
	if p.MaxTokens > 0 {
		body["max_tokens"] = p.MaxTokens
	}
	if p.IgnoreEOS {
		body["ignore_eos"] = true
	}
	if p.Temperature != nil {
		body["temperature"] = *p.Temperature
	}
	return body
}

// NewSender builds a sender. Note that HTTP.Timeout covers the entire
// request including the full SSE body, so a streaming generation that runs
// longer than the timeout counts as a failure; set the timeout above the
// longest generation the workload can produce.
func NewSender(url, model string, stream bool, params GenerationParams, timeout time.Duration) *Sender {
	return &Sender{
		URL:              url,
		Model:            model,
		Stream:           stream,
		GenerationParams: params,
		HTTP: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        1024,
				MaxIdleConnsPerHost: 1024,
				// Idle connections outlive the gap between sends at low
				// Poisson rates (intervals can exceed 30s), so 30s would
				// redial exactly when latency is being measured.
				IdleConnTimeout: 90 * time.Second,
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
			},
		},
	}
}

// Send posts one request and returns the result. A non-2xx status, a stream
// error event, and transport failures are all surfaced as errors; the status
// is always recorded for backpressure analysis.
func (s *Sender) Send(ctx context.Context, row Row) (SendResult, error) {
	body, err := json.Marshal(s.body(row.Text, s.Model))
	if err != nil {
		return SendResult{}, fmt.Errorf("encode request: %w", err)
	}

	path := "/generate"
	if s.Stream {
		path = "/generate/stream"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+path, bytes.NewReader(body))
	if err != nil {
		return SendResult{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	sendAt := time.Now()
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return SendResult{}, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		// Drain the rest so the connection returns to the idle pool; a
		// redial storm during 429 backpressure is exactly when latency
		// measurements matter most.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return SendResult{Status: resp.StatusCode}, fmt.Errorf("unexpected status: %s", string(b))
	}

	if s.Stream {
		return s.readStream(resp, sendAt)
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return SendResult{Status: resp.StatusCode}, fmt.Errorf("decode response: %w", err)
	}
	return SendResult{Status: resp.StatusCode, APIResp: apiResp, TokensSource: "usage"}, nil
}

// sseChunk mirrors the API's chunk event payload.
type sseChunk struct {
	JobID        string `json:"job_id"`
	Text         string `json:"text"`
	Done         bool   `json:"done"`
	TokensIn     int64  `json:"tokens_in"`
	TokensOut    int64  `json:"tokens_out"`
	TokensSource string `json:"tokens_source,omitempty"`
	QueueWaitMs  int64  `json:"queue_wait_ms"`
	DurationMs   int64  `json:"duration_ms"`
}

func (s *Sender) readStream(resp *http.Response, sendAt time.Time) (SendResult, error) {
	out := SendResult{Status: resp.StatusCode}

	fail := func(err error) (SendResult, error) {
		// Drain the remainder so the connection can be reused even when
		// the stream ends early (error events, truncation).
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return out, err
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	event := ""
	done := false
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data := strings.TrimPrefix(line, "data: ")
			switch event {
			case "chunk":
				var ch sseChunk
				if err := json.Unmarshal([]byte(data), &ch); err != nil {
					return fail(fmt.Errorf("decode chunk: %w", err))
				}
				if ch.Text == "" && !ch.Done {
					continue
				}
				if ch.Done {
					out.APIResp.TokensIn = ch.TokensIn
					out.APIResp.TokensOut = ch.TokensOut
					out.APIResp.TotalMs = ch.DurationMs
					out.TokensSource = firstNonEmpty(ch.TokensSource, tokensSourceFallback(ch))
					done = true
					continue
				}
				// A token chunk: the timestamp basis for TTFT and TPOT.
				now := time.Now()
				if out.FirstChunkAt.IsZero() {
					out.FirstChunkAt = now
				}
				out.LastChunkAt = now
				out.Chunks++
			case "error":
				return fail(fmt.Errorf("stream error event: %s", data))
			case "done":
				// The [DONE] sentinel follows the final chunk event.
			}
		case line == "":
			event = ""
		}
	}
	if err := scanner.Err(); err != nil {
		return fail(fmt.Errorf("read stream: %w", err))
	}
	if !done || out.Chunks == 0 {
		return fail(fmt.Errorf("stream ended without chunks"))
	}
	out.TTFT = out.FirstChunkAt.Sub(sendAt)
	if out.APIResp.TokensOut > 1 {
		out.TPOT = out.LastChunkAt.Sub(out.FirstChunkAt) / time.Duration(out.APIResp.TokensOut-1)
	}
	return out, nil
}

// tokensSourceFallback classifies the token counts when the API did not send
// an explicit source: a done chunk that reports usage is "usage", one with
// no token counts at all cannot be trusted for tok/s claims.
func tokensSourceFallback(ch sseChunk) string {
	if ch.TokensOut > 0 {
		return "usage"
	}
	return "estimated"
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// classifySendError maps a send failure to a low-cardinality classification
// key for the summary's errors map. Raw error messages (which embed ports,
// URLs, and response bodies) stay in the JSONL records only.
func classifySendError(err error, res SendResult) string {
	if err == nil {
		return ""
	}
	if res.Status != 0 {
		switch {
		case res.Status == http.StatusTooManyRequests:
			return "http_429"
		case res.Status >= 500:
			return "http_5xx"
		default:
			return fmt.Sprintf("http_%d", res.Status)
		}
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return "timeout"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return "conn_refused"
	case strings.Contains(msg, "connection reset"):
		return "conn_reset"
	case strings.Contains(msg, "stream error event"):
		return "stream_error"
	case strings.Contains(msg, "stream ended without chunks"):
		return "stream_truncated"
	case strings.Contains(msg, "decode"), strings.Contains(msg, "malformed"):
		return "decode"
	}
	return "transport"
}
