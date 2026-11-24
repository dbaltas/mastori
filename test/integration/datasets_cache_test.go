//go:build integration

// Dataset cache integration test: exercises the real S3 client (aws-sdk-go-v2
// through internal/datasets) against a throwaway LocalStack S3 container —
// the same code path the traffic Job's datasets initContainer runs on EKS
// (restore on hit, cache miss, checksum sidecar verification).
//
// Run with an S3-compatible endpoint on INTEGRATION_S3_ADDR (default
// localhost:7098, the throwaway LocalStack that `make test-integration`
// starts):
//
//	go test -tags integration ./test/integration/...
package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dbaltas/mastori/internal/datasets"
)

const s3DefaultAddr = "localhost:7098"

func s3Endpoint(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("INTEGRATION_S3_ADDR")
	if addr == "" {
		addr = s3DefaultAddr
	}
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		// Locally a missing endpoint is a legitimate skip; in CI a silent
		// skip can hide a broken provisioning step for months.
		if os.Getenv("CI") == "true" {
			t.Fatalf("S3 endpoint not reachable on %s in CI: %v", addr, err)
		}
		t.Skipf("S3 endpoint not reachable on %s: %v", addr, err)
	}
	conn.Close()
	return "http://" + addr
}

func TestDatasetCacheS3Roundtrip(t *testing.T) {
	endpoint := s3Endpoint(t)
	t.Setenv("DATASETS_S3_ENDPOINT", endpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Unique bucket per run so repeated runs never collide.
	bucket := fmt.Sprintf("it-datasets-%d", time.Now().UnixNano())
	if err := createBucket(ctx, endpoint, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	store, err := datasets.NewS3Store(ctx, bucket)
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}

	manifest := writeManifest(t)
	srcDir := writeCache(t)
	opts := datasets.CacheOptions{ManifestPath: manifest, CacheDir: srcDir, Bucket: bucket, Store: store}

	// Miss on an empty bucket.
	found, err := datasets.Restore(ctx, opts)
	if err != nil {
		t.Fatalf("restore on empty bucket: %v", err)
	}
	if found {
		t.Fatal("restore reported a hit on an empty bucket")
	}

	// Seed: pack the cache and push it under the manifest-SHA keys, the same
	// layout Seed uses after a successful fetch.
	if err := seedPack(ctx, t, store, manifest, srcDir); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Restore into a fresh directory and compare content.
	dstDir := t.TempDir()
	opts.CacheDir = dstDir
	found, err = datasets.Restore(ctx, opts)
	if err != nil {
		t.Fatalf("restore after seed: %v", err)
	}
	if !found {
		t.Fatal("restore reported a miss after seeding")
	}

	want, err := os.ReadFile(filepath.Join(srcDir, "mmlu.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dstDir, "mmlu.jsonl"))
	if err != nil {
		t.Fatalf("restored cache missing mmlu.jsonl: %v", err)
	}
	if string(want) != string(got) {
		t.Error("restored mmlu.jsonl content mismatch")
	}
}

func createBucket(ctx context.Context, endpoint, bucket string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	_, err = client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	return err
}

func seedPack(ctx context.Context, t *testing.T, store datasets.ObjectStore, manifest, cacheDir string) error {
	t.Helper()
	sha, err := datasets.SHA256File(manifest)
	if err != nil {
		return err
	}
	packPath := filepath.Join(t.TempDir(), "cache.tar.gz")
	packSHA, err := datasets.Pack(cacheDir, packPath)
	if err != nil {
		return err
	}
	checkPath := filepath.Join(t.TempDir(), "cache.sha256")
	if err := os.WriteFile(checkPath, []byte(packSHA+"\n"), 0o644); err != nil {
		return err
	}
	if err := store.Put(ctx, "datasets/"+sha+"/cache.tar.gz", packPath); err != nil {
		return err
	}
	return store.Put(ctx, "datasets/"+sha+"/cache.sha256", checkPath)
}

func writeManifest(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "datasets.yaml")
	body := "datasets:\n  mmlu:\n    source: org/ds\n    config: default\n    split: train\n    extract: mmlu\n    selection:\n      start: 0\n      count: 2\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeCache(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	content := "{\"index\":0,\"data\":{\"question\":\"q0\"}}\n{\"index\":1,\"data\":{\"question\":\"q1\"}}\n"
	if err := os.WriteFile(filepath.Join(dir, "mmlu.jsonl"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}
