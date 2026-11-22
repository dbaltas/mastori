package main

import (
	"context"
	"strings"
	"testing"
)

func TestRunFetchValidateUnknownDataset(t *testing.T) {
	err := runFetchValidate("fetch", "testdata/nonexistent.yaml", t.TempDir(), "nope")
	if err == nil || !strings.Contains(err.Error(), "read manifest") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunCachePrepareWithoutBucket(t *testing.T) {
	t.Setenv("DATASETS_S3_BUCKET", "")
	// No bucket: prepare must take the plain-fetch path; a nonexistent
	// manifest surfaces that path's error.
	err := runCache(context.Background(), "prepare", "testdata/nonexistent.yaml", t.TempDir())
	if err == nil {
		t.Fatal("expected an error from the fetch path")
	}
	if !strings.Contains(err.Error(), "read manifest") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunCacheRestoreMissWithoutBucket(t *testing.T) {
	t.Setenv("DATASETS_S3_BUCKET", "")
	err := runCache(context.Background(), "restore", "testdata/nonexistent.yaml", t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "no remote cache bucket configured") {
		t.Errorf("unexpected error: %v", err)
	}
}
