package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dbaltas/mastori/internal/queue"
)

// OllamaGenerator generates completions by calling an inference backend that
// exposes the vLLM OpenAI-compatible API (locally: the Ollama adapter in
// internal/adapter; in the cloud: a real vLLM server). The backend URL is the
// same in both cases, so the worker needs no changes when moving to vLLM.
type OllamaGenerator struct {
	// BaseURL is the vLLM-compatible server root, e.g. http://adapter:8080.
	BaseURL string
	// Model is used when the job does not name a concrete model.
	Model string
	// MaxTokens caps output tokens; 0 means unbounded.
	MaxTokens int
	// Client is the HTTP client used for generation calls.
	Client *http.Client
}

type sseEvent struct {
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

func (g *OllamaGenerator) client() *http.Client {
	if g.Client != nil {
		return g.Client
	}
	return http.DefaultClient
}

func (g *OllamaGenerator) model(job queue.Job) string {
	if job.Model != "" && job.Model != "default" {
		return job.Model
	}
	return g.Model
}

func (g *OllamaGenerator) estimateTokens(text string) int64 {
	n := len(strings.Fields(text))
	if n == 0 {
		n = 1
	}
	return int64(n)
}

// Run streams the completion for the job from the vLLM-compatible backend,
// emitting one Chunk per streamed delta and a final Done chunk with usage.
func (g *OllamaGenerator) Run(ctx context.Context, job queue.Job, onChunk func(queue.Chunk) error) error {
	start := time.Now()
	queueWaitMs := time.Since(job.CreatedAt).Milliseconds()

	body := map[string]any{
		"model":  g.model(job),
		"prompt": job.Prompt,
		"stream": true,
		"stream_options": map[string]any{
			"include_usage": true,
		},
	}
	if g.MaxTokens > 0 {
		body["max_tokens"] = g.MaxTokens
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("inference: encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(g.BaseURL, "/")+"/v1/completions", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("inference: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := g.client().Do(req)
	if err != nil {
		return fmt.Errorf("inference: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("inference: backend returned %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var tokensIn, tokensOut int64
	sawUsage := false
	var text strings.Builder

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}

		var ev sseEvent
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			return fmt.Errorf("inference: malformed SSE payload: %w", err)
		}
		if ev.Error != nil {
			return fmt.Errorf("inference: backend error: %s", ev.Error.Message)
		}
		for _, c := range ev.Choices {
			if c.Delta.Content == "" {
				continue
			}
			chunk := queue.Chunk{JobID: job.ID, Text: c.Delta.Content}
			if err := onChunk(chunk); err != nil {
				return err
			}
			tokensOut++
			text.WriteString(c.Delta.Content)
		}
		if ev.Usage != nil {
			tokensIn = ev.Usage.PromptTokens
			tokensOut = ev.Usage.CompletionTokens
			sawUsage = true
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("inference: stream read failed: %w", err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !sawUsage {
		tokensOut = g.estimateTokens(text.String())
	}

	done := queue.Chunk{
		JobID:       job.ID,
		Done:        true,
		TokensIn:    tokensIn,
		TokensOut:   tokensOut,
		QueueWaitMs: queueWaitMs,
		DurationMs:  time.Since(start).Milliseconds(),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return onChunk(done)
}
