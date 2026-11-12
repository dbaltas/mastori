package datasets

import (
	"os"
	"path/filepath"
	"testing"
)

func writeManifest(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "datasets.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const validManifest = `
datasets:
  ds1:
    source: org/ds1
    config: default
    split: train
    revision: abc123
    extract: mmlu
    selection:
      start: 0
      count: 10
    validate:
      min_rows: 10
      required_fields: [question]
      spot_checks:
        - index: 0
          contains: "hello"
  ds2:
    source: org/ds2
    config: default
    split: train
    extract: first_human_turn
    selection:
      start: 5
      count: 2
`

func TestLoadManifestValid(t *testing.T) {
	m, err := LoadManifest(writeManifest(t, validManifest))
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if len(m.Datasets) != 2 {
		t.Fatalf("expected 2 datasets, got %d", len(m.Datasets))
	}
	ds1, err := m.Get("ds1")
	if err != nil {
		t.Fatalf("Get ds1: %v", err)
	}
	if ds1.Name != "ds1" {
		t.Errorf("Name = %q, want ds1", ds1.Name)
	}
	if ds1.Source != "org/ds1" || ds1.Revision != "abc123" {
		t.Errorf("unexpected fields: %+v", ds1)
	}
	if ds1.Selection.Count != 10 || ds1.Validate.MinRows != 10 {
		t.Errorf("selection/validate not parsed: %+v", ds1)
	}
	if len(ds1.Validate.SpotChecks) != 1 || ds1.Validate.SpotChecks[0].Contains != "hello" {
		t.Errorf("spot checks not parsed: %+v", ds1.Validate.SpotChecks)
	}
}

func TestLoadManifestMissingFile(t *testing.T) {
	if _, err := LoadManifest("/nonexistent/datasets.yaml"); err == nil {
		t.Fatal("expected error for missing manifest")
	}
}

func TestLoadManifestInvalidYAML(t *testing.T) {
	if _, err := LoadManifest(writeManifest(t, "datasets: [broken")); err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoadManifestEmptyDatasets(t *testing.T) {
	if _, err := LoadManifest(writeManifest(t, "datasets: {}")); err == nil {
		t.Fatal("expected error for empty datasets")
	}
}

func TestManifestGetUnknown(t *testing.T) {
	m, err := LoadManifest(writeManifest(t, validManifest))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Get("nope"); err == nil {
		t.Fatal("expected error for unknown dataset name")
	}
}
