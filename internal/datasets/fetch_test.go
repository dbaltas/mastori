package datasets

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func fetchTestServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func testDataset() Dataset {
	return Dataset{
		Name:     "test",
		Source:   "org/ds",
		Config:   "default",
		Split:    "train",
		Revision: "abc",
		Extract:  "mmlu",
		Selection: Selection{
			Start: 0,
			Count: 2,
		},
	}
}

func rowsBody(n, offset int) string {
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, map[string]any{
			"subject":  "s",
			"question": fmt.Sprintf("q%d", offset+i),
			"choices":  []string{"a", "b"},
			"answer":   0,
		})
	}
	b, _ := json.Marshal(map[string]any{"rows": wrap(rows)})
	return string(b)
}

func wrap(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		out[i] = map[string]any{"row": r}
	}
	return out
}

func TestFetchWritesJSONL(t *testing.T) {
	srv := fetchTestServer(t, http.StatusOK, rowsBody(2, 0))
	f := NewFetcher()
	f.BaseURL = srv.URL
	dir := t.TempDir()

	path, err := f.Fetch(testDataset(), dir)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var first extracted
	if err := json.Unmarshal([]byte(firstLine(data)), &first); err != nil {
		t.Fatalf("decode first line: %v", err)
	}
	if first.Index != 0 {
		t.Errorf("Index = %d, want 0", first.Index)
	}
	if first.Data["question"] != "q0" {
		t.Errorf("question = %v, want q0", first.Data["question"])
	}
}

func firstLine(data []byte) string {
	for i, b := range data {
		if b == '\n' {
			return string(data[:i])
		}
	}
	return string(data)
}

func TestFetchRetriesOn5xx(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, rowsBody(2, 0))
	}))
	t.Cleanup(srv.Close)

	f := NewFetcher()
	f.BaseURL = srv.URL
	f.Backoff = func(int) {}

	if _, err := f.Fetch(testDataset(), t.TempDir()); err != nil {
		t.Fatalf("Fetch after retries: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestFetchFailsOn4xx(t *testing.T) {
	srv := fetchTestServer(t, http.StatusNotFound, "not found")
	f := NewFetcher()
	f.BaseURL = srv.URL
	f.Backoff = func(int) {}

	if _, err := f.Fetch(testDataset(), t.TempDir()); err == nil {
		t.Fatal("expected error on 404")
	}
}

func TestFetchExhaustedDataset(t *testing.T) {
	srv := fetchTestServer(t, http.StatusOK, rowsBody(0, 0))
	f := NewFetcher()
	f.BaseURL = srv.URL
	f.Backoff = func(int) {}

	if _, err := f.Fetch(testDataset(), t.TempDir()); err == nil {
		t.Fatal("expected error when dataset returns no rows")
	}
}

func TestExtractRowMMLU(t *testing.T) {
	row := map[string]any{
		"subject":  "math",
		"question": "q",
		"choices":  []any{"a", "b", "c", "d"},
		"answer":   2,
	}
	got, err := extractRow(testDataset(), 0, row)
	if err != nil {
		t.Fatalf("extractRow: %v", err)
	}
	if got["question"] != "q" || got["subject"] != "math" {
		t.Errorf("unexpected extraction: %v", got)
	}
}

func TestExtractRowFirstHumanTurn(t *testing.T) {
	d := testDataset()
	d.Extract = "first_human_turn"
	row := map[string]any{
		"conversations": []any{
			map[string]any{"from": "human", "value": "hello"},
			map[string]any{"from": "gpt", "value": "hi there"},
			map[string]any{"from": "human", "value": "bye"},
		},
	}
	got, err := extractRow(d, 0, row)
	if err != nil {
		t.Fatalf("extractRow: %v", err)
	}
	if got["prompt"] != "hello" {
		t.Errorf("prompt = %v, want hello", got["prompt"])
	}
	if got["reference_output"] != "hi there" {
		t.Errorf("reference_output = %v, want 'hi there'", got["reference_output"])
	}
}

func TestExtractRowFirstHumanTurnNoHuman(t *testing.T) {
	d := testDataset()
	d.Extract = "first_human_turn"
	row := map[string]any{
		"conversations": []any{
			map[string]any{"from": "gpt", "value": "only gpt"},
		},
	}
	if _, err := extractRow(d, 3, row); err == nil {
		t.Fatal("expected error when no human turn exists")
	}
}

func TestExtractRowUnknownMode(t *testing.T) {
	d := testDataset()
	d.Extract = "bogus"
	if _, err := extractRow(d, 0, map[string]any{}); err == nil {
		t.Fatal("expected error for unknown extract mode")
	}
}

func TestFetchRespectsSelectionStart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if got := q.Get("offset"); got != "7" {
			t.Errorf("offset = %q, want 7", got)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, rowsBody(2, 7))
	}))
	t.Cleanup(srv.Close)

	f := NewFetcher()
	f.BaseURL = srv.URL

	d := testDataset()
	d.Selection.Start = 7
	if _, err := f.Fetch(d, t.TempDir()); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
}

func TestFetchWritesToCacheDir(t *testing.T) {
	srv := fetchTestServer(t, http.StatusOK, rowsBody(2, 0))
	f := NewFetcher()
	f.BaseURL = srv.URL

	cache := filepath.Join(t.TempDir(), "nested", "cache")
	path, err := f.Fetch(testDataset(), cache)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if filepath.Base(path) != "test.jsonl" {
		t.Errorf("output = %s, want test.jsonl", filepath.Base(path))
	}
}
