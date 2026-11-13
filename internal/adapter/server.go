package adapter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server serves the vLLM OpenAI-compatible API surface backed by Ollama:
// /v1/completions, /v1/chat/completions, /v1/models, /health, and /metrics
// with synthesized vllm:* metrics.
type Server struct {
	Ollama       *OllamaClient
	DefaultModel string
	Metrics      *Metrics
	Logger       *slog.Logger
}

type completionRequest struct {
	Model         string   `json:"model"`
	Prompt        any      `json:"prompt"`
	MaxTokens     *int     `json:"max_tokens"`
	Temperature   *float64 `json:"temperature"`
	Stream        bool     `json:"stream"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
}

type chatCompletionRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	MaxTokens           *int     `json:"max_tokens"`
	MaxCompletionTokens *int     `json:"max_completion_tokens"`
	Temperature         *float64 `json:"temperature"`
	Stream              bool     `json:"stream"`
	StreamOptions       *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
}

type openAIUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

func (u openAIUsage) total() int64 { return u.PromptTokens + u.CompletionTokens }

// streamSpec unifies what a streaming generation needs, regardless of
// whether it came from /v1/completions or /v1/chat/completions.
type streamSpec struct {
	model        string
	prompt       string
	messages     []ChatMessage
	promptTokens int64
	maxTokens    *int
	temperature  *float64
	includeUsage bool
	chat         bool
}

func NewRouter(s *Server) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	r.Handle("/metrics", promhttp.HandlerFor(s.Metrics.Registry, promhttp.HandlerOpts{}))
	r.Get("/health", s.health)
	r.Get("/v1/models", s.models)
	r.Post("/v1/completions", s.completions)
	r.Post("/v1/chat/completions", s.chatCompletions)

	return r
}

func (s *Server) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := s.Ollama.Tags(ctx); err != nil {
		http.Error(w, "ollama unreachable: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	names, err := s.Ollama.Tags(r.Context())
	if err != nil {
		writeOpenAIError(w, err)
		return
	}
	type modelEntry struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		OwnedBy string `json:"owned_by"`
	}
	data := make([]modelEntry, 0, len(names))
	for _, n := range names {
		data = append(data, modelEntry{ID: n, Object: "model", OwnedBy: "ollama"})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   data,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeOpenAIError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	var oerr *OllamaError
	if errors.As(err, &oerr) && oerr.Status == http.StatusNotFound {
		status = http.StatusNotFound
	}
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"message": err.Error(),
			"type":    "internal_error",
		},
	})
}

func openAIErrorBody(msg string) map[string]any {
	return map[string]any{
		"error": map[string]any{"message": msg, "type": "invalid_request_error"},
	}
}

func (s *Server) completions(w http.ResponseWriter, r *http.Request) {
	var req completionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, openAIErrorBody("invalid request body: "+err.Error()))
		return
	}
	prompt, err := promptString(req.Prompt)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, openAIErrorBody(err.Error()))
		return
	}
	model := s.modelOrDefault(req.Model)

	if req.Stream {
		defer s.trackRunning(model)()
		s.streamCompletion(w, r, streamSpec{
			model:        model,
			prompt:       prompt,
			promptTokens: wordCount(prompt),
			maxTokens:    req.MaxTokens,
			temperature:  req.Temperature,
			includeUsage: req.StreamOptions != nil && req.StreamOptions.IncludeUsage,
		})
		return
	}

	defer s.trackRunning(model)()
	var text strings.Builder
	result, err := s.Ollama.StreamGeneration(r.Context(), generateRequest{
		Model:   model,
		Prompt:  prompt,
		Options: ollamaOptions(req.MaxTokens, req.Temperature),
	}, func(ch GenerateChunk) error {
		text.WriteString(ch.Response)
		return nil
	})
	if err != nil {
		writeOpenAIError(w, err)
		return
	}
	s.record(model, result)
	writeJSON(w, http.StatusOK, map[string]any{
		"id":      completionID("cmpl"),
		"object":  "text_completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{
			{
				"index":         0,
				"text":          text.String(),
				"finish_reason": "stop",
			},
		},
		"usage": usageFromFinal(result, prompt, result.Response),
	})
}

func (s *Server) chatCompletions(w http.ResponseWriter, r *http.Request) {
	var req chatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, openAIErrorBody("invalid request body: "+err.Error()))
		return
	}
	model := s.modelOrDefault(req.Model)
	maxTokens := req.MaxTokens
	if maxTokens == nil {
		maxTokens = req.MaxCompletionTokens
	}
	messages := make([]ChatMessage, 0, len(req.Messages))
	var promptTokens int64
	for _, m := range req.Messages {
		messages = append(messages, ChatMessage{Role: m.Role, Content: m.Content})
		promptTokens += wordCount(m.Content)
	}
	if len(messages) == 0 {
		writeJSON(w, http.StatusBadRequest, openAIErrorBody("messages must not be empty"))
		return
	}
	defer s.trackRunning(model)()

	if req.Stream {
		s.streamCompletion(w, r, streamSpec{
			model:        model,
			messages:     messages,
			promptTokens: promptTokens,
			maxTokens:    maxTokens,
			temperature:  req.Temperature,
			includeUsage: req.StreamOptions != nil && req.StreamOptions.IncludeUsage,
			chat:         true,
		})
		return
	}

	var text strings.Builder
	result, err := s.Ollama.StreamGeneration(r.Context(), generateRequest{
		Model:    model,
		Messages: messages,
		Options:  ollamaOptions(maxTokens, req.Temperature),
	}, func(ch GenerateChunk) error {
		if ch.Message != nil {
			text.WriteString(ch.Message.Content)
		}
		return nil
	})
	if err != nil {
		writeOpenAIError(w, err)
		return
	}
	s.record(model, result)
	writeJSON(w, http.StatusOK, map[string]any{
		"id":      completionID("chatcmpl"),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []map[string]any{
			{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": text.String(),
				},
				"finish_reason": "stop",
			},
		},
		"usage": usageFromFinal(result, "", text.String()),
	})
}

// streamCompletion translates the Ollama NDJSON stream into OpenAI SSE
// chunks, recording vllm:* latency metrics as events arrive.
func (s *Server) streamCompletion(w http.ResponseWriter, r *http.Request, spec streamSpec) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, openAIErrorBody("streaming unsupported"))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	defer s.trackRunning(spec.model)()

	object := "text_completion"
	if spec.chat {
		object = "chat.completion.chunk"
	}
	id := completionID("cmpl")

	emit := func(payload map[string]any) bool {
		if _, err := fmt.Fprintf(w, "data: %s\n\n", mustJSON(payload)); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	greq := generateRequest{
		Model:    spec.model,
		Prompt:   spec.prompt,
		Messages: spec.messages,
		Options:  ollamaOptions(spec.maxTokens, spec.temperature),
	}

	var (
		text      strings.Builder
		tokensOut int64
		start     = time.Now()
		firstAt   time.Time
		lastAt    time.Time
	)

	result, err := s.Ollama.StreamGeneration(r.Context(), greq, func(ch GenerateChunk) error {
		now := time.Now()
		if firstAt.IsZero() {
			firstAt = now
			s.Metrics.TTFTSeconds.WithLabelValues(spec.model).Observe(firstAt.Sub(start).Seconds())
			if !emit(map[string]any{
				"id": id, "object": object, "created": start.Unix(), "model": spec.model,
				"choices": []map[string]any{{"index": 0, "delta": map[string]any{"role": "assistant"}, "finish_reason": nil}},
			}) {
				return errClientGone
			}
		} else if !lastAt.IsZero() {
			s.Metrics.TimePerOutputToken.WithLabelValues(spec.model).Observe(now.Sub(lastAt).Seconds())
		}
		lastAt = now

		content := ch.Response
		if ch.Message != nil {
			content = ch.Message.Content
		}
		if content != "" {
			text.WriteString(content)
			tokensOut++
			if !emit(map[string]any{
				"id": id, "object": object, "created": start.Unix(), "model": spec.model,
				"choices": []map[string]any{{"index": 0, "delta": map[string]any{"content": content}, "finish_reason": nil}},
			}) {
				return errClientGone
			}
		}
		return nil
	})

	if err != nil && !errors.Is(err, errClientGone) {
		emit(map[string]any{"error": map[string]any{"message": err.Error(), "type": "internal_error"}})
		_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
		return
	}

	_ = emit(map[string]any{
		"id": id, "object": object, "created": start.Unix(), "model": spec.model,
		"choices": []map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}},
	})
	if spec.includeUsage {
		usage := openAIUsage{
			PromptTokens:     spec.promptTokens,
			CompletionTokens: tokensOut,
		}
		if result != nil {
			usage = usageFromFinal(result, spec.prompt, text.String())
			if usage.PromptTokens == 0 {
				usage.PromptTokens = spec.promptTokens
			}
		}
		usage.TotalTokens = usage.total()
		_ = emit(map[string]any{
			"id": id, "object": object, "created": start.Unix(), "model": spec.model,
			"choices": []any{},
			"usage":   usage,
		})
	}
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()

	if err == nil {
		s.record(spec.model, result)
		s.Metrics.E2ERequestLatency.WithLabelValues(spec.model).Observe(time.Since(start).Seconds())
	}
}

var errClientGone = errors.New("client closed stream")

func (s *Server) trackRunning(model string) func() {
	s.Metrics.NumRequestsRunning.WithLabelValues(model).Inc()
	return func() {
		s.Metrics.NumRequestsRunning.WithLabelValues(model).Dec()
	}
}

func ollamaOptions(maxTokens *int, temperature *float64) OllamaOptions {
	opts := OllamaOptions{}
	if maxTokens != nil && *maxTokens > 0 {
		opts.NumPredict = *maxTokens
	}
	if temperature != nil {
		t := *temperature
		opts.Temperature = &t
	}
	return opts
}

func usageFromFinal(final *GenerateChunk, prompt, text string) openAIUsage {
	u := openAIUsage{}
	if final != nil {
		u.PromptTokens = final.PromptEvalCount
		u.CompletionTokens = final.EvalCount
	}
	if u.PromptTokens == 0 {
		u.PromptTokens = wordCount(prompt)
	}
	if u.CompletionTokens == 0 {
		u.CompletionTokens = wordCount(text)
	}
	u.TotalTokens = u.total()
	return u
}

func wordCount(s string) int64 {
	n := len(strings.Fields(s))
	if n == 0 {
		return 1
	}
	return int64(n)
}

func (s *Server) record(model string, final *GenerateChunk) {
	if final == nil {
		return
	}
	if final.PromptEvalCount > 0 {
		s.Metrics.PromptTokensTotal.WithLabelValues(model).Add(float64(final.PromptEvalCount))
	}
	if final.EvalCount > 0 {
		s.Metrics.GenerationTokensTotal.WithLabelValues(model).Add(float64(final.EvalCount))
	}
	s.Metrics.RequestSuccessTotal.WithLabelValues(model).Inc()
}

func promptString(v any) (string, error) {
	switch p := v.(type) {
	case string:
		if p == "" {
			return "", errors.New("prompt must not be empty")
		}
		return p, nil
	case []any:
		if len(p) == 1 {
			s, ok := p[0].(string)
			if !ok {
				return "", errors.New("prompt array elements must be strings")
			}
			return s, nil
		}
		return "", errors.New("prompt arrays with more than one element are not supported")
	default:
		return "", errors.New("prompt must be a string")
	}
}

func (s *Server) modelOrDefault(reqModel string) string {
	if reqModel != "" {
		return reqModel
	}
	return s.DefaultModel
}

func completionID(prefix string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return prefix + "-" + hex.EncodeToString(b)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
