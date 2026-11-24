package traffic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
	TTFT    time.Duration // time to first chunk (streaming mode only)
	Chunks  int           // number of SSE chunk events received (streaming mode only)
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
				IdleConnTimeout:     30 * time.Second,
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

	resp, err := s.HTTP.Do(req)
	if err != nil {
		return SendResult{}, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return SendResult{Status: resp.StatusCode}, fmt.Errorf("unexpected status: %s", string(b))
	}

	if s.Stream {
		return s.readStream(resp)
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return SendResult{Status: resp.StatusCode}, fmt.Errorf("decode response: %w", err)
	}
	return SendResult{Status: resp.StatusCode, APIResp: apiResp}, nil
}

// sseChunk mirrors the API's chunk event payload.
type sseChunk struct {
	JobID       string `json:"job_id"`
	Text        string `json:"text"`
	Done        bool   `json:"done"`
	TokensIn    int64  `json:"tokens_in"`
	TokensOut   int64  `json:"tokens_out"`
	QueueWaitMs int64  `json:"queue_wait_ms"`
	DurationMs  int64  `json:"duration_ms"`
}

func (s *Sender) readStream(resp *http.Response) (SendResult, error) {
	start := time.Now()
	out := SendResult{Status: resp.StatusCode}

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
					return out, fmt.Errorf("decode chunk: %w", err)
				}
				out.Chunks++
				if out.TTFT == 0 {
					out.TTFT = time.Since(start)
				}
				out.APIResp.Text += ch.Text
				if ch.Done {
					out.APIResp.TokensIn = ch.TokensIn
					out.APIResp.TokensOut = ch.TokensOut
					out.APIResp.TotalMs = ch.DurationMs
				}
			case "error":
				return out, fmt.Errorf("stream error event: %s", data)
			case "done":
				done = true
			}
		case line == "":
			event = ""
		}
	}
	if err := scanner.Err(); err != nil {
		return out, fmt.Errorf("read stream: %w", err)
	}
	if !done || out.Chunks == 0 {
		return out, fmt.Errorf("stream ended without chunks")
	}
	return out, nil
}
