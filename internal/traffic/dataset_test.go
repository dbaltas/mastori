package traffic

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCache(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".jsonl"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadRowsShareGPT(t *testing.T) {
	dir := t.TempDir()
	writeCache(t, dir, "ds", `{"index":0,"data":{"prompt":"hello world","reference_output":"x"}}
{"index":1,"data":{"prompt":"second"}}
`)

	rows, err := LoadRows(dir, []string{"ds"})
	if err != nil {
		t.Fatalf("LoadRows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0].Dataset != "ds" || rows[0].Index != 0 || rows[0].Text != "hello world" {
		t.Errorf("row 0 wrong: %+v", rows[0])
	}
	if rows[1].Index != 1 || rows[1].Text != "second" {
		t.Errorf("row 1 wrong: %+v", rows[1])
	}
}

func TestLoadRowsMMLUComposition(t *testing.T) {
	dir := t.TempDir()
	writeCache(t, dir, "mmlu", `{"index":0,"data":{"question":"What is 2+2?","choices":["3","4","5","6"],"answer":1,"subject":"math"}}
`)

	rows, err := LoadRows(dir, []string{"mmlu"})
	if err != nil {
		t.Fatalf("LoadRows: %v", err)
	}
	want := "What is 2+2?\nA) 3\nB) 4\nC) 5\nD) 6"
	if rows[0].Text != want {
		t.Errorf("prompt = %q, want %q", rows[0].Text, want)
	}
}

func TestLoadRowsErrors(t *testing.T) {
	dir := t.TempDir()
	writeCache(t, dir, "bad", `{"index":0,"data":{"subject":"x"}}
`)

	if _, err := LoadRows(dir, []string{"missing"}); err == nil {
		t.Error("missing file should error")
	} else if !strings.Contains(err.Error(), "fetch") {
		t.Errorf("error should hint at fetch: %v", err)
	}

	if _, err := LoadRows(dir, []string{"bad"}); err == nil {
		t.Error("row without prompt fields should error")
	}

	writeCache(t, dir, "invalid", "not json\n")
	if _, err := LoadRows(dir, []string{"invalid"}); err == nil {
		t.Error("invalid JSON should error")
	}
}
