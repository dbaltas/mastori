package adapter

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newFakeOllama(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	s := &Server{
		Ollama:       &OllamaClient{BaseURL: srv.URL, HTTP: srv.Client()},
		DefaultModel: "tiny",
		Metrics:      NewMetrics(),
	}
	return srv, s
}

func ollamaStreamBody(text string, promptCount, evalCount int64, chat bool) string {
	var b strings.Builder
	for _, w := range strings.Fields(text) {
		line := map[string]any{"model": "tiny", "response": w + " ", "done": false}
		if chat {
			line["response"] = ""
			line["message"] = map[string]any{"role": "assistant", "content": w + " "}
		}
		j, _ := json.Marshal(line)
		b.WriteString(string(j) + "\n")
	}
	final := map[string]any{
		"model": "tiny", "done": true,
		"prompt_eval_count": promptCount, "eval_count": evalCount,
	}
	if chat {
		final["response"] = ""
		final["message"] = map[string]any{"role": "assistant", "content": ""}
	} else {
		final["response"] = ""
	}
	j, _ := json.Marshal(final)
	b.WriteString(string(j) + "\n")
	return b.String()
}

func TestCompletionsNonStream(t *testing.T) {
	_, s := newFakeOllama(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/generate" {
			t.Errorf("path = %q", r.URL.Path)
		}
		var req generateRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Model != "tiny" || req.Prompt != "hello world" {
			t.Errorf("request = %+v", req)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(ollamaStreamBody("a b", 5, 2, false)))
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"prompt":"hello world"}`))
	NewRouter(s).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["object"] != "text_completion" {
		t.Errorf("object = %v", out["object"])
	}
	choices := out["choices"].([]any)
	text := choices[0].(map[string]any)["text"].(string)
	if text != "a b " {
		t.Errorf("text = %q", text)
	}
	usage := out["usage"].(map[string]any)
	if usage["prompt_tokens"].(float64) != 5 || usage["completion_tokens"].(float64) != 2 {
		t.Errorf("usage = %v", usage)
	}
}

func TestCompletionsStream(t *testing.T) {
	_, s := newFakeOllama(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(ollamaStreamBody("one two", 3, 2, false)))
	})

	w := httptest.NewRecorder()
	body := `{"prompt":"hi","stream":true,"stream_options":{"include_usage":true}}`
	NewRouter(s).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(body)))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type = %q", ct)
	}

	var events []map[string]any
	scanner := bufio.NewScanner(w.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			events = append(events, nil)
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			t.Fatalf("malformed event %q: %v", data, err)
		}
		events = append(events, ev)
	}

	if len(events) != 6 { // role delta + 2 content + finish + usage + [DONE]
		t.Fatalf("events = %d, want 6", len(events))
	}
	if events[0] == nil || events[len(events)-1] != nil {
		t.Fatalf("missing role delta or [DONE] sentinel: %v", events)
	}
	second := events[1]["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)
	if second["content"] != "one " {
		t.Errorf("first content delta = %v", second)
	}
	finish := events[3]["choices"].([]any)[0].(map[string]any)["finish_reason"].(string)
	if finish != "stop" {
		t.Errorf("finish_reason = %q", finish)
	}
	usage := events[4]["usage"].(map[string]any)
	if usage["completion_tokens"].(float64) != 2 || usage["prompt_tokens"].(float64) != 3 {
		t.Errorf("usage = %v", usage)
	}
}

func TestChatCompletionsStreamAndNonStream(t *testing.T) {
	_, s := newFakeOllama(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			t.Errorf("path = %q", r.URL.Path)
		}
		var req generateRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if len(req.Messages) == 0 || len(req.Messages) > 2 {
			t.Errorf("messages = %+v", req.Messages)
		}
		isChat := len(req.Messages) > 0
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(ollamaStreamBody("hi there", 9, 2, isChat)))
	})

	w := httptest.NewRecorder()
	body := `{"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"}],"stream":true,"stream_options":{"include_usage":true}}`
	NewRouter(s).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	text := strings.Join(extractContents(t, w.Body.String()), "")
	if text != "hi there " {
		t.Errorf("streamed text = %q", text)
	}

	w2 := httptest.NewRecorder()
	nonStream := `{"messages":[{"role":"user","content":"hi"}]}`
	NewRouter(s).ServeHTTP(w2, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(nonStream)))
	if w2.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w2.Code, w2.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w2.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["object"] != "chat.completion" {
		t.Errorf("object = %v", out["object"])
	}
	msg := out["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)
	if msg["role"] != "assistant" {
		t.Errorf("role = %v", msg["role"])
	}
}

func extractContents(t *testing.T, body string) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") || strings.Contains(line, "[DONE]") {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
			continue
		}
		choices, ok := ev["choices"].([]any)
		if !ok || len(choices) == 0 {
			continue
		}
		delta := choices[0].(map[string]any)["delta"].(map[string]any)
		if c, ok := delta["content"].(string); ok {
			out = append(out, c)
		}
	}
	return out
}

func TestCompletionsInvalidPrompt(t *testing.T) {
	_, s := newFakeOllama(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("ollama should not be called for invalid prompt")
	})
	w := httptest.NewRecorder()
	NewRouter(s).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"prompt":""}`)))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestCompletionsOllamaNotFoundMapsTo404(t *testing.T) {
	_, s := newFakeOllama(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"model 'x' not found"}`))
	})
	w := httptest.NewRecorder()
	NewRouter(s).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"prompt":"hi"}`)))
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["error"].(map[string]any); !ok {
		t.Errorf("error body = %v", out)
	}
}

func TestHealth(t *testing.T) {
	_, s := newFakeOllama(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tags" {
			t.Errorf("path = %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"models":[{"name":"tiny"}]}`))
	})
	w := httptest.NewRecorder()
	NewRouter(s).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	unreachable := &Server{Ollama: &OllamaClient{BaseURL: "http://127.0.0.1:1", HTTP: &http.Client{}}, Metrics: NewMetrics()}
	w2 := httptest.NewRecorder()
	NewRouter(unreachable).ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/health", nil))
	if w2.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w2.Code)
	}
}

func TestModels(t *testing.T) {
	_, s := newFakeOllama(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"models":[{"name":"tiny"},{"name":"qwen2.5:0.5b"}]}`))
	})
	w := httptest.NewRecorder()
	NewRouter(s).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["object"] != "list" {
		t.Errorf("object = %v", out["object"])
	}
	if got := len(out["data"].([]any)); got != 2 {
		t.Errorf("models = %d, want 2", got)
	}
}

func TestMetricsExposed(t *testing.T) {
	_, s := newFakeOllama(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(ollamaStreamBody("hello", 2, 1, false)))
	})
	NewRouter(s).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(`{"prompt":"hi","stream":true,"stream_options":{"include_usage":true}}`)))

	w := httptest.NewRecorder()
	NewRouter(s).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	metricsBody := w.Body.String()
	for _, name := range []string{
		"vllm_time_to_first_token_seconds",
		"vllm_time_per_output_token_seconds",
		"vllm_e2e_request_latency_seconds",
		"vllm_prompt_tokens_total",
		"vllm_generation_tokens_total",
		"vllm_request_success_total",
		"vllm_num_requests_running",
		"vllm_num_requests_waiting",
	} {
		if !strings.Contains(metricsBody, name) {
			t.Errorf("metrics missing %s", name)
		}
	}
}

func TestOllamaOptions(t *testing.T) {
	mt := 64
	temp := 0.5
	opts := ollamaOptions(&mt, &temp)
	if opts.NumPredict != 64 {
		t.Errorf("NumPredict = %d", opts.NumPredict)
	}
	if opts.Temperature == nil || *opts.Temperature != 0.5 {
		t.Errorf("Temperature = %v", opts.Temperature)
	}
	if got := ollamaOptions(nil, nil); got.NumPredict != 0 || got.Temperature != nil {
		t.Errorf("default options = %+v", got)
	}
}

func TestPromptString(t *testing.T) {
	if _, err := promptString("ok"); err != nil {
		t.Errorf("string prompt: %v", err)
	}
	if _, err := promptString([]any{"only"}); err != nil {
		t.Errorf("single-element array: %v", err)
	}
	if _, err := promptString([]any{"a", "b"}); err == nil {
		t.Error("multi-element array should error")
	}
	if _, err := promptString(42); err == nil {
		t.Error("non-string should error")
	}
}
