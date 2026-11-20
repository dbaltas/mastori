package fakegpu

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func nodeJSON(allocatable string) string {
	if allocatable == "" {
		return `{"status":{"allocatable":{"cpu":"8"}}}`
	}
	return `{"status":{"allocatable":{"cpu":"8","` + ResourceName + `":"` + allocatable + `"}}}`
}

func TestPatchBytes(t *testing.T) {
	got := string(PatchBytes(2))
	want := `[{"op":"add","path":"/status/allocatable/nvidia.com~1gpu","value":"2"}]`
	if got != want {
		t.Fatalf("PatchBytes = %s, want %s", got, want)
	}
}

func TestParseAllocatable(t *testing.T) {
	count, present, err := ParseAllocatable([]byte(nodeJSON("2")))
	if err != nil {
		t.Fatalf("ParseAllocatable: %v", err)
	}
	if !present || count != 2 {
		t.Fatalf("got count=%d present=%v, want 2 true", count, present)
	}

	count, present, err = ParseAllocatable([]byte(nodeJSON("")))
	if err != nil {
		t.Fatalf("ParseAllocatable missing key: %v", err)
	}
	if present {
		t.Fatal("got present=true for node without gpu key")
	}
	if count != 0 {
		t.Fatalf("got count=%d, want 0", count)
	}

	_, _, err = ParseAllocatable([]byte("not json"))
	if err == nil {
		t.Fatal("expected error for malformed node body")
	}

	_, _, err = ParseAllocatable([]byte(`{"status":{"allocatable":{"` + ResourceName + `":"abc"}}}`))
	if err == nil {
		t.Fatal("expected error for non-numeric allocatable")
	}
}

func TestConfigFromEnv(t *testing.T) {
	cfg, err := ConfigFromEnv(func(key string) string {
		return map[string]string{"NODE_NAME": "gpu-node-1", "FAKE_GPU_COUNT": "3", "FAKE_GPU_INTERVAL": "5s"}[key]
	})
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.NodeName != "gpu-node-1" || cfg.Count != 3 || cfg.Interval != 5*time.Second {
		t.Fatalf("got %+v", cfg)
	}

	cfg, err = ConfigFromEnv(func(key string) string { return map[string]string{"NODE_NAME": "n"}[key] })
	if err != nil {
		t.Fatalf("ConfigFromEnv defaults: %v", err)
	}
	if cfg.Count != 1 || cfg.Interval != DefaultInterval {
		t.Fatalf("defaults not applied: %+v", cfg)
	}

	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{"missing node name", map[string]string{}},
		{"bad count", map[string]string{"NODE_NAME": "n", "FAKE_GPU_COUNT": "zero"}},
		{"zero count", map[string]string{"NODE_NAME": "n", "FAKE_GPU_COUNT": "0"}},
		{"negative count", map[string]string{"NODE_NAME": "n", "FAKE_GPU_COUNT": "-1"}},
		{"bad interval", map[string]string{"NODE_NAME": "n", "FAKE_GPU_INTERVAL": "soon"}},
		{"zero interval", map[string]string{"NODE_NAME": "n", "FAKE_GPU_INTERVAL": "0s"}},
	} {
		_, err := ConfigFromEnv(func(key string) string { return tc.env[key] })
		if err == nil {
			t.Errorf("%s: expected error", tc.name)
		}
	}
}

func TestAdvertiseSkipsWhenCurrent(t *testing.T) {
	var patches atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, nodeJSON("2"))
		case http.MethodPatch:
			patches.Add(1)
			http.Error(w, "patch should not happen", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client := &Client{Host: server.URL, Token: "test-token", HTTP: server.Client(), Logger: slog.Default()}
	patched, err := client.Advertise(context.Background(), "gpu-node-1", 2)
	if err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	if patched {
		t.Fatal("patched although allocatable already current")
	}
	if patches.Load() != 0 {
		t.Fatalf("unexpected %d patches", patches.Load())
	}
}

func TestAdvertisePatchesWhenMissing(t *testing.T) {
	var patches atomic.Int64
	var gotPatch []byte
	var gotAuth string
	var patchPath string
	var getPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, nodeJSON(""))
		case http.MethodPatch:
			if ct := r.Header.Get("Content-Type"); ct != PatchContentType {
				t.Errorf("patch Content-Type = %q, want %q", ct, PatchContentType)
			}
			if auth := r.Header.Get("Authorization"); auth != "Bearer test-token" {
				t.Errorf("Authorization = %q", auth)
			}
			body, _ := io.ReadAll(r.Body)
			gotPatch = body
			gotAuth = r.Header.Get("Authorization")
			patchPath = r.URL.Path
			patches.Add(1)
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, nodeJSON("1"))
		}
	}))
	defer server.Close()

	client := &Client{Host: server.URL, Token: "test-token", HTTP: server.Client(), Logger: slog.Default()}
	patched, err := client.Advertise(context.Background(), "gpu-node-1", 1)
	if err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	if !patched {
		t.Fatal("expected a patch")
	}
	if patches.Load() != 1 {
		t.Fatalf("got %d patches, want 1", patches.Load())
	}
	if string(gotPatch) != string(PatchBytes(1)) {
		t.Fatalf("patch body = %s, want %s", gotPatch, PatchBytes(1))
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if patchPath != "/api/v1/nodes/gpu-node-1/status" {
		t.Fatalf("patch path = %q, want the status subresource", patchPath)
	}
	if getPath != "/api/v1/nodes/gpu-node-1" {
		t.Fatalf("get path = %q", getPath)
	}
}

func TestAdvertisePatchesWhenCountChanged(t *testing.T) {
	var patches atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			io.WriteString(w, nodeJSON("1"))
		case http.MethodPatch:
			patches.Add(1)
			io.WriteString(w, nodeJSON("4"))
		}
	}))
	defer server.Close()

	client := &Client{Host: server.URL, Token: "t", HTTP: server.Client(), Logger: slog.Default()}
	patched, err := client.Advertise(context.Background(), "gpu-node-1", 4)
	if err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	if !patched || patches.Load() != 1 {
		t.Fatalf("patched=%v patches=%d, want true/1", patched, patches.Load())
	}
}

func TestAdvertiseValidation(t *testing.T) {
	client := &Client{Host: "http://unused", HTTP: http.DefaultClient, Logger: slog.Default()}
	if _, err := client.Advertise(context.Background(), "", 1); err == nil {
		t.Fatal("expected error for empty node name")
	}
	if _, err := client.Advertise(context.Background(), "node", 0); err == nil {
		t.Fatal("expected error for count 0")
	}
}

func TestAdvertiseAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer server.Close()

	client := &Client{Host: server.URL, HTTP: server.Client(), Logger: slog.Default()}
	_, err := client.Advertise(context.Background(), "gpu-node-1", 1)
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected 403 error, got %v", err)
	}
}

func TestRunReassertsPeriodically(t *testing.T) {
	var patches atomic.Int64
	var ready = make(chan struct{})
	var once atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			io.WriteString(w, nodeJSON(""))
		case http.MethodPatch:
			patches.Add(1)
			if once.CompareAndSwap(false, true) {
				close(ready)
			}
			io.WriteString(w, nodeJSON("1"))
		}
	}))
	defer server.Close()

	client := &Client{Host: server.URL, HTTP: server.Client(), Logger: slog.Default()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx, "gpu-node-1", 1, 10*time.Millisecond) }()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("no patch within 2s")
	}

	afterFirst := patches.Load()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if patches.Load() > afterFirst {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if patches.Load() <= afterFirst {
		t.Fatal("Run did not re-assert after the interval")
	}

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected context error from Run")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRunLogsErrorsAndKeepsGoing(t *testing.T) {
	var gets atomic.Int64
	var ready = make(chan struct{})
	var once atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if gets.Add(1) <= 2 {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			io.WriteString(w, nodeJSON(""))
		case http.MethodPatch:
			io.WriteString(w, nodeJSON("1"))
			if once.CompareAndSwap(false, true) {
				close(ready)
			}
		}
	}))
	defer server.Close()

	client := &Client{Host: server.URL, HTTP: server.Client(), Logger: slog.Default()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx, "gpu-node-1", 1, 5*time.Millisecond) }()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("Run gave up after initial failures")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
