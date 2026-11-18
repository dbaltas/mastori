package traffic

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SchemaVersion is the version of the exported record and summary format.
const SchemaVersion = "0.1"

// Record is one per-request observation written to the results JSONL file.
type Record struct {
	SchemaVersion string    `json:"schema_version"`
	RunID         string    `json:"run_id"`
	Seq           int64     `json:"seq"`
	Dataset       string    `json:"dataset"`
	RowIndex      int       `json:"row_index"`
	Warmup        bool      `json:"warmup"`
	SendAt        time.Time `json:"send_at"`
	Status        int       `json:"status"`
	LatencyMs     int64     `json:"latency_ms"`
	TTFTMs        int64     `json:"ttft_ms,omitempty"`
	Chunks        int       `json:"chunks,omitempty"`
	TokensIn      int64     `json:"tokens_in"`
	TokensOut     int64     `json:"tokens_out"`
	APITotalMs    int64     `json:"api_total_ms"`
	Error         string    `json:"error,omitempty"`
}

// ResultWriter appends records as JSON lines, creating parent directories.
// Safe for concurrent use.
type ResultWriter struct {
	mu  sync.Mutex
	f   *os.File
	enc *json.Encoder
}

func NewResultWriter(path string) (*ResultWriter, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create results dir: %w", err)
		}
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	enc := json.NewEncoder(f)
	return &ResultWriter{f: f, enc: enc}, nil
}

func (w *ResultWriter) Write(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(r)
}

func (w *ResultWriter) Close() error { return w.f.Close() }

// SummaryPath returns the summary file path for a results path: the results
// path with its extension replaced by .summary.json.
func SummaryPath(path string) string {
	if ext := filepath.Ext(path); ext != "" {
		return strings.TrimSuffix(path, ext) + ".summary.json"
	}
	return path + ".summary.json"
}

// WriteSummary writes a run summary as indented JSON, creating parent
// directories. Rewriting the same path replaces the file, which is how the
// driver publishes a summary as running and later the final status.
func WriteSummary(path string, s *Summary) error {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create summary dir: %w", err)
		}
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("encode summary: %w", err)
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}
