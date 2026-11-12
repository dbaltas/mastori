package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dbaltas/mastori/internal/datasets"
)

const usage = `mastori-datasets — fetch and validate benchmark datasets

Usage:
  mastori-datasets fetch    [--manifest <path>] [--cache <dir>] [--dataset <name>]
  mastori-datasets validate [--manifest <path>] [--cache <dir>] [--dataset <name>]

fetch    downloads the selection for each dataset into the local cache as JSONL.
validate checks a cache file: row count, required fields, and content spot checks.
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
	only := fs.String("dataset", "", "operate on a single dataset by name")
	if err := fs.Parse(os.Args[2:]); err != nil {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	manifest, err := datasets.LoadManifest(*manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	names := make([]string, 0, len(manifest.Datasets))
	for name := range manifest.Datasets {
		names = append(names, name)
	}
	if *only != "" {
		if _, err := manifest.Get(*only); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		names = []string{*only}
	}

	fetcher := datasets.NewFetcher()
	validator := datasets.NewValidator()

	for _, name := range names {
		d, err := manifest.Get(name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		switch cmd {
		case "fetch":
			path, err := fetcher.Fetch(d, *cacheDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %s: %v\n", name, err)
				os.Exit(1)
			}
			fmt.Printf("fetched %s: %d rows -> %s\n", name, d.Selection.Count, path)
		case "validate":
			if err := validator.Validate(d, *cacheDir); err != nil {
				fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", name, err)
				os.Exit(1)
			}
		default:
			fmt.Fprint(os.Stderr, usage)
			os.Exit(2)
		}
	}
}
