package datasets

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeStore is an in-memory ObjectStore for the unit tests.
type fakeStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	getErr  map[string]error
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: map[string][]byte{}, getErr: map[string]error{}}
}

func (s *fakeStore) Exists(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[key]
	return ok, nil
}

func (s *fakeStore) Get(_ context.Context, key, dstPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err, ok := s.getErr[key]; ok {
		return err
	}
	data, ok := s.objects[key]
	if !ok {
		return fmt.Errorf("NoSuchKey: %s", key)
	}
	return os.WriteFile(dstPath, data, 0o644)
}

func (s *fakeStore) Put(_ context.Context, key, srcPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(srcPath)
	if err != nil {
		return err
	}
	s.objects[key] = data
	return nil
}

// testCacheDir writes a minimal cache (two JSONL files) and returns its path.
func testCacheDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"mmlu.jsonl":     "{\"index\":0,\"data\":{\"question\":\"q0\"}}\n{\"index\":1,\"data\":{\"question\":\"q1\"}}\n",
		"sharegpt.jsonl": "{\"index\":0,\"data\":{\"prompt\":\"p\"}}\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// testManifest writes a datasets.yaml and returns its path.
func testManifest(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "datasets.yaml")
	body := "datasets:\n  mmlu:\n    source: org/ds\n    config: default\n    split: train\n    extract: mmlu\n    selection:\n      start: 0\n      count: 2\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPackUnpackRoundtrip(t *testing.T) {
	cacheDir := testCacheDir(t)
	pack := filepath.Join(t.TempDir(), "cache.tar.gz")

	sha, err := Pack(cacheDir, pack)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if sha == "" || len(sha) != 64 {
		t.Fatalf("pack sha256 %q not a 64-char hex digest", sha)
	}

	outDir := filepath.Join(t.TempDir(), "restored")
	if err := Unpack(pack, outDir); err != nil {
		t.Fatalf("unpack: %v", err)
	}

	for _, name := range []string{"mmlu.jsonl", "sharegpt.jsonl"} {
		want, err := os.ReadFile(filepath.Join(cacheDir, name))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("restored cache missing %s: %v", name, err)
		}
		if string(want) != string(got) {
			t.Errorf("%s content mismatch after roundtrip", name)
		}
	}
}

func TestPackDeterministic(t *testing.T) {
	cacheDir := testCacheDir(t)
	pack1 := filepath.Join(t.TempDir(), "a.tar.gz")
	pack2 := filepath.Join(t.TempDir(), "b.tar.gz")

	sha1, err := Pack(cacheDir, pack1)
	if err != nil {
		t.Fatal(err)
	}
	sha2, err := Pack(cacheDir, pack2)
	if err != nil {
		t.Fatal(err)
	}
	if sha1 != sha2 {
		t.Errorf("pack of the same cache produced different digests: %s vs %s", sha1, sha2)
	}
}

func TestUnpackRejectsTraversal(t *testing.T) {
	// Hand-craft an archive with a ../ entry.
	evil := filepath.Join(t.TempDir(), "evil.tar.gz")
	if err := writeTraversalArchive(evil); err != nil {
		t.Fatal(err)
	}

	outDir := filepath.Join(t.TempDir(), "out")
	err := Unpack(evil, outDir)
	if err == nil {
		t.Fatal("unpack accepted an archive with a ../ entry")
	}
	if !strings.Contains(err.Error(), "unsafe entry") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRestoreCacheMiss(t *testing.T) {
	store := newFakeStore()
	o := CacheOptions{ManifestPath: testManifest(t), CacheDir: testCacheDir(t), Bucket: "bkt", Store: store}

	got, err := Restore(context.Background(), o)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got {
		t.Fatal("restore reported a hit on an empty bucket")
	}
}

func TestSeedRestoreRoundtrip(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	manifest := testManifest(t)

	srcDir := testCacheDir(t)
	o := CacheOptions{ManifestPath: manifest, CacheDir: srcDir, Bucket: "bkt", Store: store}
	if err := uploadPack(ctx, o); err != nil {
		t.Fatalf("upload: %v", err)
	}

	// The keys must be derived from the manifest SHA.
	sha, err := SHA256File(manifest)
	if err != nil {
		t.Fatal(err)
	}
	packKey := "datasets/" + sha + "/" + packObject
	if _, ok := store.objects[packKey]; !ok {
		t.Errorf("expected object at %s, bucket holds %v", packKey, keys(store.objects))
	}
	if _, ok := store.objects["datasets/"+sha+"/"+checkObject]; !ok {
		t.Errorf("expected checksum sidecar at datasets/%s/%s", sha, checkObject)
	}

	dstDir := filepath.Join(t.TempDir(), "fresh")
	o.CacheDir = dstDir
	restored, err := Restore(ctx, o)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !restored {
		t.Fatal("restore reported a miss after seeding")
	}

	want, _ := os.ReadFile(filepath.Join(srcDir, "mmlu.jsonl"))
	got, err := os.ReadFile(filepath.Join(dstDir, "mmlu.jsonl"))
	if err != nil {
		t.Fatalf("restored cache missing mmlu.jsonl: %v", err)
	}
	if string(want) != string(got) {
		t.Error("restored mmlu.jsonl content mismatch")
	}
}

func TestRestoreChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	manifest := testManifest(t)

	o := CacheOptions{ManifestPath: manifest, CacheDir: testCacheDir(t), Bucket: "bkt", Store: store}
	if err := uploadPack(ctx, o); err != nil {
		t.Fatal(err)
	}

	// Corrupt the sidecar so the downloaded pack no longer verifies.
	sha, _ := SHA256File(manifest)
	store.objects["datasets/"+sha+"/"+checkObject] = []byte(strings.Repeat("0", 64) + "\n")

	o.CacheDir = filepath.Join(t.TempDir(), "fresh")
	_, err := Restore(ctx, o)
	if err == nil {
		t.Fatal("restore accepted a pack with a mismatching checksum")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPrepareWithoutBucketFetches(t *testing.T) {
	// Bucket empty and Store nil: Prepare must fall back to the plain fetch
	// path (the local kind behavior); a nonexistent manifest surfaces the
	// fetch-path error instead of a cache error.
	o := CacheOptions{ManifestPath: "testdata/nonexistent.yaml", CacheDir: t.TempDir()}
	err := Prepare(context.Background(), o, nil)
	if err == nil {
		t.Fatal("expected an error from the manifest-SHA path, not a silent skip")
	}
	if !strings.Contains(err.Error(), "no such file") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPrepareWithBucketSeedsOnMiss(t *testing.T) {
	// A miss must enter the seed path: a valid manifest plus a fetcher whose
	// HF calls all fail proves the fetch ran (instead of silently skipping).
	o := CacheOptions{ManifestPath: testManifest(t), CacheDir: t.TempDir(), Bucket: "bkt", Store: newFakeStore()}
	srv := fetchTestServer(t, 500, "boom")
	f := &Fetcher{Client: srv.Client(), BaseURL: srv.URL, Backoff: func(int) {}}
	err := Prepare(context.Background(), o, f)
	if err == nil {
		t.Fatal("expected a fetch error on cache miss")
	}
	if !strings.Contains(err.Error(), "HF API returned 500") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRestoreWithoutStoreFails(t *testing.T) {
	o := CacheOptions{ManifestPath: testManifest(t), CacheDir: t.TempDir(), Bucket: "bkt"}
	_, err := Restore(context.Background(), o)
	if err == nil || !strings.Contains(err.Error(), "no object store configured") {
		t.Errorf("unexpected error: %v", err)
	}
}

func keys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// writeTraversalArchive creates a tar.gz whose single entry escapes the
// extraction directory (tar path traversal).
func writeTraversalArchive(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	hdr := &tar.Header{Name: "../evil.txt", Mode: 0o644, Typeflag: tar.TypeReg, Size: 4}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	if _, err := tw.Write([]byte("evil")); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}
