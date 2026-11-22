package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/dbaltas/mastori/internal/datasets"
)

const usage = `mastori-datasets — fetch and validate benchmark datasets

Usage:
  mastori-datasets fetch    [--manifest <path>] [--cache <dir>] [--dataset <name>]
  mastori-datasets validate [--manifest <path>] [--cache <dir>] [--dataset <name>]
  mastori-datasets prepare  [--manifest <path>] [--cache <dir>]
  mastori-datasets seed     [--manifest <path>] [--cache <dir>]
  mastori-datasets restore  [--manifest <path>] [--cache <dir>]

fetch    downloads the selection for each dataset into the local cache as JSONL.
validate checks a cache file: row count, required fields, and content spot checks.
prepare  restores the cache from the S3 remote cache; on a miss, seeds it:
         fetches from the dataset manifest, packs, and uploads. Without
         DATASETS_S3_BUCKET set, prepare behaves like fetch (local kind runs).
seed     fetches every dataset in the manifest and (re)uploads the pack to S3.
restore  downloads the pack for the current manifest SHA from S3 (fails on a
         cache miss; use prepare for the miss-tolerant flow).

S3 environment (prepare/seed/restore):
  DATASETS_S3_BUCKET    bucket holding the remote cache (empty = disabled)
  DATASETS_S3_ENDPOINT  optional endpoint override (S3-compatible, e.g. MinIO)
  AWS_REGION            region; credentials via the standard AWS chain (IRSA
                        on EKS injects them automatically)
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd := os.Args[1]

	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	manifestPath := fs.String("manifest", "benchmark/workloads/datasets.yaml", "path to dataset manifest")
	cacheDir := fs.String("cache", "benchmark/workloads/cache", "directory for downloaded dataset caches")
	only := fs.String("dataset", "", "operate on a single dataset by name (fetch/validate only)")
	if err := fs.Parse(os.Args[2:]); err != nil {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	ctx := context.Background()

	switch cmd {
	case "fetch", "validate":
		if err := runFetchValidate(cmd, *manifestPath, *cacheDir, *only); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "prepare", "seed", "restore":
		if err := runCache(ctx, cmd, *manifestPath, *cacheDir); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
}

func runFetchValidate(cmd, manifestPath, cacheDir, only string) error {
	manifest, err := datasets.LoadManifest(manifestPath)
	if err != nil {
		return err
	}

	names := make([]string, 0, len(manifest.Datasets))
	for name := range manifest.Datasets {
		names = append(names, name)
	}
	if only != "" {
		if _, err := manifest.Get(only); err != nil {
			return err
		}
		names = []string{only}
	}

	fetcher := datasets.NewFetcher()
	validator := datasets.NewValidator()

	for _, name := range names {
		d, err := manifest.Get(name)
		if err != nil {
			return err
		}
		switch cmd {
		case "fetch":
			path, err := fetcher.Fetch(d, cacheDir)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			fmt.Printf("fetched %s: %d rows -> %s\n", name, d.Selection.Count, path)
		case "validate":
			if err := validator.Validate(d, cacheDir); err != nil {
				return fmt.Errorf("FAIL %s: %w", name, err)
			}
		}
	}
	return nil
}

func runCache(ctx context.Context, cmd, manifestPath, cacheDir string) error {
	opts := datasets.CacheOptions{
		ManifestPath: manifestPath,
		CacheDir:     cacheDir,
		Bucket:       os.Getenv("DATASETS_S3_BUCKET"),
	}
	if opts.Bucket != "" {
		store, err := datasets.NewS3Store(ctx, opts.Bucket)
		if err != nil {
			return err
		}
		opts.Store = store
	}

	switch cmd {
	case "prepare":
		return datasets.Prepare(ctx, opts, datasets.NewFetcher())
	case "seed":
		return datasets.Seed(ctx, opts, datasets.NewFetcher())
	case "restore":
		found, err := datasets.Restore(ctx, opts)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("cache miss for %s in s3://%s: run 'mastori-datasets seed' or 'prepare'", manifestPath, opts.Bucket)
		}
		fmt.Printf("cache restored from s3://%s (manifest %s)\n", opts.Bucket, manifestPath)
		return nil
	}
	return nil
}
