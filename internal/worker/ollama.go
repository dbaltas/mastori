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

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

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
	// Tracer creates the inference spans; nil uses the worker tracer.
	Tracer oteltrace.Tracer
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
	c := g.Client
	if c == nil {
		c = http.DefaultClient
	}
	transport := c.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	// Wrap the transport so every inference call becomes a client span under
	// worker.process and carries the trace context into the backend server.
	return &http.Client{Transport: otelhttp.NewTransport(transport), Timeout: c.Timeout}
}

func (g *OllamaGenerator) model(job queue.Job) string {
	if job.Model != "" && job.Model != "default" {
		return job.Model
	}
	return g.Model
}

// spanTracer returns the tracer used for inference spans.
func (g *OllamaGenerator) spanTracer() oteltrace.Tracer {
	if g.Tracer != nil {
		return g.Tracer
	}
	return tracer
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

	// Child of worker.process: covers the backend call and the whole SSE body
	// read, so the time spent inside the generator becomes visible in the
	// trace instead of hiding inside worker.process.
	reqCtx, reqSpan := g.spanTracer().Start(ctx, "inference.request",
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
		oteltrace.WithAttributes(
			attribute.String("job.id", job.ID),
			attribute.String("job.model", g.model(job)),
			attribute.Int64("job.prompt_len", int64(len(job.Prompt))),
		))
	defer reqSpan.End()
	fail := func(err error) error {
		reqSpan.RecordError(err)
		reqSpan.SetStatus(codes.Error, err.Error())
		return err
	}

	// Child of inference.request: ends the moment the first content token
	// arrives, so its duration is the true time-to-first-token as seen by the
	// worker (not the chunk-consume time the API records today).
	_, firstSpan := g.spanTracer().Start(reqCtx, "inference.first_token")
	defer firstSpan.End()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, strings.TrimSuffix(g.BaseURL, "/")+"/v1/completions", bytes.NewReader(payload))
	if err != nil {
		return fail(fmt.Errorf("inference: build request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := g.client().Do(req)
	if err != nil {
		return fail(fmt.Errorf("inference: request failed: %w", err))
	}
	defer func() { _ = resp.Body.Close() }()
	reqSpan.SetAttributes(attribute.Int("inference.status_code", resp.StatusCode))

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fail(fmt.Errorf("inference: backend returned %d: %s", resp.StatusCode, strings.TrimSpace(string(b))))
	}

	var tokensIn, tokensOut int64
	sawUsage := false
	sawFirstToken := false
	var text strings.Builder

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return fail(ctx.Err())
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
			return fail(fmt.Errorf("inference: malformed SSE payload: %w", err))
		}
		if ev.Error != nil {
			return fail(fmt.Errorf("inference: backend error: %s", ev.Error.Message))
		}
		for _, c := range ev.Choices {
			if c.Delta.Content == "" {
				continue
			}
			if !sawFirstToken {
				sawFirstToken = true
				ttft := time.Since(start)
				firstSpan.SetAttributes(
					attribute.Int64("stream.ttft_ms", ttft.Milliseconds()),
				)
				firstSpan.End()
				reqSpan.AddEvent("inference.first_token", oteltrace.WithAttributes(
					attribute.Int64("stream.ttft_ms", ttft.Milliseconds()),
				))
			}
			chunk := queue.Chunk{JobID: job.ID, Text: c.Delta.Content}
			if err := onChunk(chunk); err != nil {
				return fail(err)
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
		return fail(fmt.Errorf("inference: stream read failed: %w", err))
	}
	if ctx.Err() != nil {
		return fail(ctx.Err())
	}
	if !sawUsage {
		tokensOut = g.estimateTokens(text.String())
	}

	reqSpan.SetAttributes(
		attribute.Int64("job.tokens_in", tokensIn),
		attribute.Int64("job.tokens_out", tokensOut),
		attribute.Int64("job.duration_ms", time.Since(start).Milliseconds()),
	)
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
		return fail(ctx.Err())
	default:
	}
	return onChunk(done)
}
