package datasets

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ObjectStore is the remote cache the datasets initContainer exchanges pack
// files with. One in-memory fake backs the unit tests; the S3 implementation
// (s3store.go) is the production store.
type ObjectStore interface {
	// Exists reports whether the object at key is present. Transport errors
	// (other than "not found") are returned as errors.
	Exists(ctx context.Context, key string) (bool, error)
	// Get streams the object at key into the file at dstPath.
	Get(ctx context.Context, key, dstPath string) error
	// Put uploads the file at srcPath as key.
	Put(ctx context.Context, key, srcPath string) error
}

// Cache layout under the bucket: <prefix>/<manifestSHA>/cache.tar.gz plus a
// <prefix>/<manifestSHA>/cache.sha256 sidecar. Keying by the manifest SHA
// makes a datasets.yaml change a new prefix (automatic invalidation), makes
// concurrent seeders harmless (last writer wins, same bytes), and matches the
// dataset-manifest SHA-256 recorded in the run summary and by
// make cluster-env-capture.
const (
	packObject    = "cache.tar.gz"
	checkObject   = "cache.sha256"
	defaultPrefix = "datasets"
)

// CacheOptions configures the remote cache exchange.
type CacheOptions struct {
	// ManifestPath is the datasets.yaml whose SHA-256 keys the cache.
	ManifestPath string
	// CacheDir is the local (pod emptyDir) cache directory.
	CacheDir string
	// Bucket is the S3 bucket; empty disables the remote cache entirely.
	Bucket string
	// Prefix is the key prefix inside the bucket (default "datasets").
	Prefix string
	// Store performs the remote object exchange; required when Bucket != "".
	Store ObjectStore
}

func (o *CacheOptions) prefix() string {
	if o.Prefix != "" {
		return o.Prefix
	}
	return defaultPrefix
}

func (o *CacheOptions) manifestSHA() (string, error) {
	return SHA256File(o.ManifestPath)
}

func (o *CacheOptions) validate() error {
	if o.Bucket == "" {
		return fmt.Errorf("no remote cache bucket configured")
	}
	if o.Store == nil {
		return fmt.Errorf("no object store configured")
	}
	if o.CacheDir == "" {
		return fmt.Errorf("no cache directory configured")
	}
	return nil
}

func (o *CacheOptions) packKey(sha string) string {
	return fmt.Sprintf("%s/%s/%s", o.prefix(), sha, packObject)
}

func (o *CacheOptions) checkKey(sha string) string {
	return fmt.Sprintf("%s/%s/%s", o.prefix(), sha, checkObject)
}

// Prepare restores the cache from the remote store, seeding it from the
// dataset manifest on a cache miss (the object keys are SHA-keyed, so a race
// between two seeders is harmless: last writer wins with identical bytes).
// It reports what happened on stdout.
func Prepare(ctx context.Context, o CacheOptions, f *Fetcher) error {
	if o.Bucket == "" {
		// No remote cache configured (local kind runs): fetch from the
		// source service directly, the pre-S3 behavior.
		return fetchAll(f, o.ManifestPath, o.CacheDir)
	}
	if err := o.validate(); err != nil {
		return err
	}

	restored, err := Restore(ctx, o)
	if err != nil {
		return err
	}
	if restored {
		fmt.Printf("cache restored from s3://%s (manifest %s)\n", o.Bucket, o.ManifestPath)
		return nil
	}

	fmt.Printf("cache miss in s3://%s; seeding from the dataset manifest\n", o.Bucket)
	if err := Seed(ctx, o, f); err != nil {
		return err
	}
	fmt.Printf("cache seeded into s3://%s\n", o.Bucket)
	return nil
}

// Seed fetches every dataset in the manifest from the source service into
// CacheDir, then packs and uploads the cache (pack object + sha256 sidecar).
func Seed(ctx context.Context, o CacheOptions, f *Fetcher) error {
	if err := o.validate(); err != nil {
		return err
	}
	if err := fetchAll(f, o.ManifestPath, o.CacheDir); err != nil {
		return err
	}
	return uploadPack(ctx, o)
}

// Restore downloads the pack for the current manifest SHA into CacheDir.
// It returns false (a cache miss) without touching CacheDir when the pack is
// absent, and verifies the sha256 sidecar after the download.
func Restore(ctx context.Context, o CacheOptions) (bool, error) {
	if err := o.validate(); err != nil {
		return false, err
	}
	sha, err := o.manifestSHA()
	if err != nil {
		return false, err
	}
	packKey, checkKey := o.packKey(sha), o.checkKey(sha)

	present, err := o.Store.Exists(ctx, packKey)
	if err != nil {
		return false, fmt.Errorf("check %s: %w", packKey, err)
	}
	if !present {
		return false, nil
	}

	tmp, err := os.MkdirTemp("", "datasets-restore-")
	if err != nil {
		return false, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmp)

	packPath := filepath.Join(tmp, packObject)
	checkPath := filepath.Join(tmp, checkObject)
	if err := o.Store.Get(ctx, packKey, packPath); err != nil {
		return false, fmt.Errorf("get %s: %w", packKey, err)
	}
	if err := o.Store.Get(ctx, checkKey, checkPath); err != nil {
		return false, fmt.Errorf("get %s: %w", checkKey, err)
	}

	want, err := firstLineOfFile(checkPath)
	if err != nil {
		return false, err
	}
	got, err := SHA256File(packPath)
	if err != nil {
		return false, err
	}
	if want != got {
		return false, fmt.Errorf("pack checksum mismatch for %s: want %s, got %s", packKey, want, got)
	}

	if err := Unpack(packPath, o.CacheDir); err != nil {
		return false, fmt.Errorf("unpack %s: %w", packKey, err)
	}
	return true, nil
}

func uploadPack(ctx context.Context, o CacheOptions) error {
	sha, err := o.manifestSHA()
	if err != nil {
		return err
	}
	packKey, checkKey := o.packKey(sha), o.checkKey(sha)

	tmp, err := os.MkdirTemp("", "datasets-pack-")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmp)

	packPath := filepath.Join(tmp, packObject)
	packSHA, err := Pack(o.CacheDir, packPath)
	if err != nil {
		return err
	}
	checkPath := filepath.Join(tmp, checkObject)
	if err := os.WriteFile(checkPath, []byte(packSHA+"\n"), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", checkPath, err)
	}

	if err := o.Store.Put(ctx, packKey, packPath); err != nil {
		return fmt.Errorf("put %s: %w", packKey, err)
	}
	if err := o.Store.Put(ctx, checkKey, checkPath); err != nil {
		return fmt.Errorf("put %s: %w", checkKey, err)
	}
	fmt.Printf("uploaded s3://%s/%s (sha256 %s)\n", o.Bucket, packKey, packSHA)
	return nil
}

// fetchAll fetches every dataset in the manifest into cacheDir — the same
// traversal as the plain fetch command without --dataset.
func fetchAll(f *Fetcher, manifestPath, cacheDir string) error {
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	for name, d := range manifest.Datasets {
		path, err := f.Fetch(d, cacheDir)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		fmt.Printf("fetched %s: %d rows -> %s\n", name, d.Selection.Count, path)
	}
	return nil
}

// Pack writes a gzipped tar of cacheDir to dstPath and returns the archive's
// hex sha256. Only regular files are archived; empty directories are skipped.
func Pack(cacheDir, dstPath string) (string, error) {
	out, err := os.Create(dstPath)
	if err != nil {
		return "", fmt.Errorf("create %s: %w", dstPath, err)
	}
	defer out.Close()

	hash := sha256.New()
	gz := gzip.NewWriter(io.MultiWriter(out, hash))
	tw := tar.NewWriter(gz)

	err = filepath.WalkDir(cacheDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%s: not a regular file", path)
		}
		rel, err := filepath.Rel(cacheDir, path)
		if err != nil {
			return err
		}
		src, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		defer src.Close()
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		// Use forward slashes and no absolute paths in the archive; the
		// extraction is rooted at the cache dir.
		hdr.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := io.Copy(tw, src); err != nil {
			return fmt.Errorf("add %s: %w", rel, err)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("pack %s: %w", cacheDir, err)
	}
	if err := tw.Close(); err != nil {
		return "", err
	}
	if err := gz.Close(); err != nil {
		return "", err
	}
	// out and the deferred Close both flush; flush explicitly here so the
	// digest covers every byte on disk before upload.
	if err := out.Sync(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// Unpack extracts a gzipped tar into cacheDir, rejecting entries that escape
// the directory (tar path traversal) and pre-existing symlinks.
func Unpack(packPath, cacheDir string) error {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	f, err := os.Open(packPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", packPath, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gunzip %s: %w", packPath, err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := hdr.Name
		if name == "" || strings.HasPrefix(name, "/") || strings.Contains(name, "..") || strings.Contains(name, "\\") {
			return fmt.Errorf("unsafe entry in archive: %q", name)
		}
		switch hdr.Typeflag {
		case tar.TypeReg:
		case tar.TypeDir:
			// Directories are not shipped by Pack; create them on demand.
			if err := os.MkdirAll(filepath.Join(cacheDir, filepath.FromSlash(name)), 0o755); err != nil {
				return err
			}
			continue
		default:
			return fmt.Errorf("unsupported entry type %q in archive: %q", string(rune(hdr.Typeflag)), name)
		}
		dst := filepath.Join(cacheDir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
		if err != nil {
			return fmt.Errorf("create %s: %w", dst, err)
		}
		if _, err := io.Copy(out, tr); err != nil {
			out.Close()
			return fmt.Errorf("write %s: %w", dst, err)
		}
		if err := out.Close(); err != nil {
			return err
		}
	}
}

// SHA256File returns the hex sha256 digest of the file's contents.
func SHA256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func firstLineOfFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	line := strings.TrimSpace(string(data))
	if line == "" {
		return "", fmt.Errorf("empty checksum file %s", path)
	}
	return line, nil
}
