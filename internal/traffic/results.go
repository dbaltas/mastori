package traffic

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record is one per-request observation written to the results JSONL file.
type Record struct {
	RunID      string    `json:"run_id"`
	Seq        int64     `json:"seq"`
	Dataset    string    `json:"dataset"`
	RowIndex   int       `json:"row_index"`
	Warmup     bool      `json:"warmup"`
	SendAt     time.Time `json:"send_at"`
	Status     int       `json:"status"`
	LatencyMs  int64     `json:"latency_ms"`
	TTFTMs     int64     `json:"ttft_ms,omitempty"`
	Chunks     int       `json:"chunks,omitempty"`
	TokensIn   int64     `json:"tokens_in"`
	TokensOut  int64     `json:"tokens_out"`
	APITotalMs int64     `json:"api_total_ms"`
	Error      string    `json:"error,omitempty"`
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
