package traffic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// streamAPI serves the SSE contract of the API's /generate/stream: chunk
// events, an optional error event, and the final done/[DONE] sentinel.
func streamAPI(t *testing.T, delay, tokenGap time.Duration, chunks int, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(status)
		if status != http.StatusOK {
			fmt.Fprint(w, `{"error":"boom"}`)
			return
		}
		flusher := w.(http.Flusher)
		for i := 0; i < chunks; i++ {
			done := i == chunks-1
			var usage string
			if done {
				usage = `,"tokens_in":10,"tokens_out":20,"duration_ms":123`
			}
			fmt.Fprintf(w, "event: chunk\ndata: {\"job_id\":\"j1\",\"text\":\"t%d\",\"done\":%t%s}\n\n", i, done, usage)
			flusher.Flush()
			if !done {
				time.Sleep(tokenGap)
			}
		}
		fmt.Fprint(w, "event: done\ndata: [DONE]\n\n")
		flusher.Flush()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestSendStream(t *testing.T) {
	srv := streamAPI(t, 5*time.Millisecond, 5*time.Millisecond, 5, http.StatusOK)
	s := NewSender(srv.URL, "", true, GenerationParams{}, 5*time.Second)

	res, err := s.Send(context.Background(), Row{Dataset: "d", Index: 0, Text: "p"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if res.Status != 200 {
		t.Errorf("status = %d", res.Status)
	}
	if res.Chunks != 5 {
		t.Errorf("chunks = %d, want 5", res.Chunks)
	}
	if res.TTFT <= 0 {
		t.Error("TTFT not recorded")
	}
	if res.APIResp.TokensIn != 10 || res.APIResp.TokensOut != 20 || res.APIResp.TotalMs != 123 {
		t.Errorf("usage not captured from done chunk: %+v", res.APIResp)
	}
	if res.APIResp.Text != "t0t1t2t3t4" {
		t.Errorf("text = %q", res.APIResp.Text)
	}
}

func TestSendStreamZeroChunks(t *testing.T) {
	// A stream that ends without any chunk event is an error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: done\ndata: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	s := NewSender(srv.URL, "", true, GenerationParams{}, 5*time.Second)

	if _, err := s.Send(context.Background(), Row{Dataset: "d", Index: 0, Text: "p"}); err == nil {
		t.Error("stream without chunks should error")
	} else if !strings.Contains(err.Error(), "without chunks") {
		t.Errorf("wrong error: %v", err)
	}
}

func TestSendStreamErrorEvent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: error\ndata: {\"error\":\"timed out\"}\n\n")
	}))
	t.Cleanup(srv.Close)
	s := NewSender(srv.URL, "", true, GenerationParams{}, 5*time.Second)

	_, err := s.Send(context.Background(), Row{Dataset: "d", Index: 0, Text: "p"})
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Errorf("expected stream error event, got %v", err)
	}
}

func TestSendStreamNon200(t *testing.T) {
	srv := streamAPI(t, 0, 0, 5, http.StatusServiceUnavailable)
	s := NewSender(srv.URL, "", true, GenerationParams{}, 5*time.Second)

	res, err := s.Send(context.Background(), Row{Dataset: "d", Index: 0, Text: "p"})
	if err == nil {
		t.Fatal("expected error for 503")
	}
	if res.Status != 503 {
		t.Errorf("status = %d, want 503", res.Status)
	}
}

func TestSendBlockingUnchanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/generate" {
			t.Errorf("blocking mode hit %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		fmt.Fprint(w, `{"job_id":"j1","text":"ok","tokens_in":3,"tokens_out":7,"total_ms":9}`)
	}))
	t.Cleanup(srv.Close)
	s := NewSender(srv.URL, "", false, GenerationParams{}, 5*time.Second)

	res, err := s.Send(context.Background(), Row{Dataset: "d", Index: 0, Text: "p"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if res.APIResp.TokensIn != 3 || res.APIResp.TokensOut != 7 {
		t.Errorf("wrong response: %+v", res.APIResp)
	}
	if res.Chunks != 0 || res.TTFT != 0 {
		t.Errorf("blocking mode should not report stream fields: %+v", res)
	}
}

func TestSendStreamPath(t *testing.T) {
	// Assert the streaming sender posts to /generate/stream.
	var gotPath string
	rec := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: chunk\ndata: {\"text\":\"x\",\"done\":true,\"tokens_in\":1,\"tokens_out\":1}\n\n")
		fmt.Fprint(w, "event: done\ndata: [DONE]\n\n")
	}))
	t.Cleanup(rec.Close)

	s := NewSender(rec.URL, "", true, GenerationParams{}, 5*time.Second)
	if _, err := s.Send(context.Background(), Row{Dataset: "d", Index: 0, Text: "p"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if gotPath != "/generate/stream" {
		t.Errorf("stream mode hit %s", gotPath)
	}
}

func TestSendGenerationParams(t *testing.T) {
	// Output-control parameters must reach the API request body: without
	// max_tokens the backend default (vLLM completions: 16) shapes the
	// measured workload silently (2026-11-24 review, H3).
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &got); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"job_id":"j1","text":"ok","tokens_in":1,"tokens_out":1,"total_ms":1}`)
	}))
	t.Cleanup(srv.Close)

	temp := 0.0
	s := NewSender(srv.URL, "m", false, GenerationParams{
		MaxTokens:   256,
		IgnoreEOS:   true,
		Temperature: &temp,
	}, 5*time.Second)
	if _, err := s.Send(context.Background(), Row{Dataset: "d", Index: 0, Text: "p"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got["max_tokens"] != float64(256) || got["ignore_eos"] != true || got["temperature"] != float64(0) {
		t.Errorf("generation params = %v, want max_tokens 256, ignore_eos true, temperature 0", got)
	}
	if got["prompt"] != "p" || got["model"] != "m" {
		t.Errorf("prompt/model lost: %v", got)
	}
}

func TestSendOmitsUnsetGenerationParams(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &got); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"job_id":"j1","text":"ok","tokens_in":1,"tokens_out":1,"total_ms":1}`)
	}))
	t.Cleanup(srv.Close)

	s := NewSender(srv.URL, "", false, GenerationParams{}, 5*time.Second)
	if _, err := s.Send(context.Background(), Row{Dataset: "d", Index: 0, Text: "p"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	for _, key := range []string{"max_tokens", "ignore_eos", "temperature", "model"} {
		if _, present := got[key]; present {
			t.Errorf("%s present in request, want omitted", key)
		}
	}
}
