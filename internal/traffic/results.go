package traffic

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// SchemaVersion is the version of the exported record and summary format.
// 0.2: latency_us/ttft_us replaced the truncated *_ms fields, ttft gained
// the pre-header path and a has_ttft flag, and tpot/first/last chunk
// timestamps plus tokens_out_source were added.
const SchemaVersion = "0.2"

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
	// LatencyUs is the full client e2e latency in microseconds (not
	// truncated to whole milliseconds, which would bias every aggregate
	// down by ~0.5ms).
	LatencyUs int64 `json:"latency_us"`
	// TTFTUs is time-to-first-token measured from the request send, so it
	// covers TCP connect, upload, API admission, queueing, and the RTT
	// before the response headers. HasTTFT is false in blocking mode.
	TTFTUs  int64 `json:"ttft_us,omitempty"`
	HasTTFT bool  `json:"has_ttft,omitempty"`
	// TPOTUs is the mean inter-chunk gap in microseconds
	// ((last-first)/(tokens_out-1); 0 when tokens_out <= 1).
	TPOTUs int64 `json:"tpot_us,omitempty"`
	// FirstChunkAt/LastChunkAt are the absolute arrival times of the first
	// and last token chunk (streaming mode only).
	FirstChunkAt *time.Time `json:"first_chunk_at,omitempty"`
	LastChunkAt  *time.Time `json:"last_chunk_at,omitempty"`
	Chunks       int        `json:"chunks,omitempty"`
	TokensIn     int64      `json:"tokens_in"`
	TokensOut    int64      `json:"tokens_out"`
	// TokensSource is "usage" or "estimated": where tokens_out came from.
	// Estimated counts are invalid for tok/s claims.
	TokensSource string `json:"tokens_out_source,omitempty"`
	// MaxTokens is the output-token cap requested for this request, so
	// requested vs actual completion_tokens (TokensOut) is visible per row
	// instead of hidden in a backend default.
	MaxTokens  int64 `json:"max_tokens,omitempty"`
	APITotalMs int64 `json:"api_total_ms"`
	// Error keeps the raw error message (ports, URLs, bodies included);
	// ErrorKind is its low-cardinality classification used for the
	// summary's errors map.
	Error     string `json:"error,omitempty"`
	ErrorKind string `json:"error_kind,omitempty"`
}

// ResultWriter appends records as JSON lines, creating parent directories.
// Safe for concurrent use.
type ResultWriter struct {
	mu     sync.Mutex
	f      *os.File
	enc    *json.Encoder
	teeEnc *json.Encoder
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

// NewResultWriterTo writes records to an arbitrary writer.
func NewResultWriterTo(w io.Writer) *ResultWriter {
	return &ResultWriter{enc: json.NewEncoder(w)}
}

// Tee mirrors every record to an additional writer (e.g. stdout) alongside
// the results file, so a run executed inside the cluster is recoverable
// from the pod logs. Call it before the first Write.
func (w *ResultWriter) Tee(extra io.Writer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.teeEnc = json.NewEncoder(extra)
}

func (w *ResultWriter) Write(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.enc.Encode(r); err != nil {
		return err
	}
	if w.teeEnc != nil {
		if err := w.teeEnc.Encode(r); err != nil {
			return fmt.Errorf("tee write: %w", err)
		}
	}
	return nil
}

func (w *ResultWriter) Close() error {
	if w.f == nil {
		return nil
	}
	return w.f.Close()
}

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
