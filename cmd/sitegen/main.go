// sitegen builds the static GitHub Pages site from the benchmark submodule.
//
// It reads experiment configs (benchmark/experiments/*.yaml) and result
// stories (benchmark/results/*/story.md), renders each story to HTML via
// pandoc, and writes an index page linking every story to outDir.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/dbaltas/mastori/internal/sitegen"
)

func main() {
	benchmarkRoot := flag.String("benchmark", "benchmark", "path to the benchmark submodule root")
	outDir := flag.String("out", "site", "output directory for the static site")
	flag.Parse()

	if err := run(*benchmarkRoot, *outDir); err != nil {
		log.Fatalf("sitegen: %v", err)
	}
}

func run(benchmarkRoot, outDir string) error {
	configExperiments, err := sitegen.LoadExperiments(
		filepath.Join(benchmarkRoot, "experiments"), benchmarkRoot)
	if err != nil {
		return err
	}

	known := make(map[string]bool, len(configExperiments))
	for _, exp := range configExperiments {
		known[exp.Dir] = true
	}

	discovered, err := sitegen.DiscoverStories(benchmarkRoot, known)
	if err != nil {
		return err
	}

	experiments := sitegen.Merge(configExperiments, discovered)
	if len(experiments) == 0 {
		return fmt.Errorf("no experiment stories found under %s", benchmarkRoot)
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", outDir, err)
	}

	if err := sitegen.WriteOutput(outDir, experiments, benchmarkRoot); err != nil {
		return err
	}

	for _, exp := range experiments {
		if exp.StoryPath == "" {
			return fmt.Errorf("experiment %s has no story path", exp.Dir)
		}
		storyMD := filepath.Join(benchmarkRoot, filepath.FromSlash(exp.StoryPath))
		bodyHTML, err := renderMarkdown(storyMD)
		if err != nil {
			return err
		}
		storyPage := sitegen.RenderStoryPage(exp.Title, bodyHTML)
		storyOut := filepath.Join(outDir, exp.Dir, "story.html")
		if err := os.MkdirAll(filepath.Dir(storyOut), 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", filepath.Dir(storyOut), err)
		}
		if err := os.WriteFile(storyOut, []byte(storyPage), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", storyOut, err)
		}
	}

	indexPath := filepath.Join(outDir, "index.html")
	if err := os.WriteFile(indexPath, []byte(sitegen.RenderIndex(experiments)), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", indexPath, err)
	}

	log.Printf("sitegen: wrote %d experiment stories to %s", len(experiments), outDir)
	return nil
}

// renderMarkdown converts a markdown file to an HTML fragment using pandoc.
func renderMarkdown(path string) (string, error) {
	cmd := exec.Command("pandoc", "-f", "gfm", "-t", "html5", "--wrap=none", path)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("pandoc %s: %w", path, err)
	}
	return string(out), nil
}
