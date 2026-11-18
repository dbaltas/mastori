package traffic

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"
)

func TestNewResultWriterTo(t *testing.T) {
	var buf bytes.Buffer
	w := NewResultWriterTo(&buf)

	rec := Record{SchemaVersion: SchemaVersion, RunID: "r1", Seq: 1}
	if err := w.Write(rec); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var got Record
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal %q: %v", buf.String(), err)
	}
	if got.RunID != "r1" || got.Seq != 1 {
		t.Errorf("record = %+v, want run_id r1 seq 1", got)
	}
}

func TestResultWriterMultiWriter(t *testing.T) {
	var file bytes.Buffer
	var stdout bytes.Buffer
	w := NewResultWriterTo(io.MultiWriter(&file, &stdout))

	if err := w.Write(Record{RunID: "r2"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if file.String() != stdout.String() {
		t.Errorf("file %q != stdout %q", file.String(), stdout.String())
	}
}

func TestSnapshotRecordsManifestHash(t *testing.T) {
	acc := newAccumulator()
	cfg := &Config{Duration: time.Second, ManifestSHA256: "abc123"}
	s := acc.snapshot(cfg)
	if s.DatasetManifestSHA256 != "abc123" {
		t.Errorf("DatasetManifestSHA256 = %q, want abc123", s.DatasetManifestSHA256)
	}
}
