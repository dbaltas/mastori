package sitegen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLoadExperimentsHonorsStoryField(t *testing.T) {
	// The YAML `story:` field must decide which file the experiment
	// publishes — not the results/<Dir>/story.md convention (B9).
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "experiments", "demo.yaml"), `
experiment:
  name: Demo
  date: "2026-11-20"
  objective: Check the honors-story path.
  story: results/20261120-demo/custom-story.md
`)
	writeFile(t, filepath.Join(root, "results", "20261120-demo", "custom-story.md"), "# Custom title\n")

	exps, err := LoadExperiments(filepath.Join(root, "experiments"), root)
	if err != nil {
		t.Fatalf("LoadExperiments: %v", err)
	}
	if len(exps) != 1 {
		t.Fatalf("experiments = %d, want 1", len(exps))
	}
	exp := exps[0]
	if exp.StoryPath != "results/20261120-demo/custom-story.md" {
		t.Errorf("StoryPath = %q, want the YAML story field", exp.StoryPath)
	}
	if exp.Title != "Custom title" {
		t.Errorf("title = %q, want the story's first heading", exp.Title)
	}
	if exp.Date != "2026-11-20" || exp.ConfigName != "demo.yaml" {
		t.Errorf("metadata = %q / %q", exp.Date, exp.ConfigName)
	}
}

func TestLoadExperimentsSkipsUnpublishedStories(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "experiments", "pending.yaml"), `
experiment:
  name: Pending
  story: results/20261121-pending/story.md
`)
	exps, err := LoadExperiments(filepath.Join(root, "experiments"), root)
	if err != nil {
		t.Fatalf("LoadExperiments: %v", err)
	}
	if len(exps) != 0 {
		t.Errorf("experiments = %d, want 0 for a story that does not exist", len(exps))
	}
}

func TestDiscoverStories(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "results", "20261115-knee", "story.md"), "# Knee\n")
	writeFile(t, filepath.Join(root, "results", "no-date-run", "story.md"), "# No date\n")
	writeFile(t, filepath.Join(root, "results", "not-a-story", "other.md"), "# Other\n")

	got, err := DiscoverStories(root, nil)
	if err != nil {
		t.Fatalf("DiscoverStories: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("discovered = %d, want 2", len(got))
	}
	var dated, undated bool
	for _, exp := range got {
		switch exp.Dir {
		case "20261115-knee":
			dated = exp.Date == "2026-11-15" && exp.StoryPath == "results/20261115-knee/story.md"
		case "no-date-run":
			undated = exp.Date == "" && exp.StoryPath == "results/no-date-run/story.md"
		}
	}
	if !dated || !undated {
		t.Errorf("discovery metadata incomplete: %+v", got)
	}
}

func TestWriteOutputUsesStoryPath(t *testing.T) {
	root := t.TempDir()
	storyDir := filepath.Join(root, "results", "20261120-demo")
	writeFile(t, filepath.Join(storyDir, "custom-story.md"), "# Body\n")

	exps := []Experiment{{
		Dir:       "20261120-demo",
		Title:     "Body",
		StoryPath: "results/20261120-demo/custom-story.md",
	}}
	out := t.TempDir()
	if err := WriteOutput(out, exps, root); err != nil {
		t.Fatalf("WriteOutput: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(out, "20261120-demo", "story.md"))
	if err != nil {
		t.Fatalf("published story missing: %v", err)
	}
	if string(data) != "# Body\n" {
		t.Errorf("published story = %q", data)
	}
}

func TestRenderIndexConfigLinkWithoutDate(t *testing.T) {
	// The config link must render even when the experiment has no date
	// (B9): provenance is worth publishing regardless of the date field.
	html := RenderIndex([]Experiment{{Dir: "d", Title: "T", ConfigName: "x.yaml"}})
	if !strings.Contains(html, `experiments/x.yaml`) {
		t.Error("config link missing for an experiment without a date")
	}
	if idx := RenderIndex([]Experiment{{Dir: "d", Title: "T", Date: "2026-01-01", ConfigName: "x.yaml"}}); !strings.Contains(idx, `2026-01-01 · <a href="experiments/x.yaml"`) {
		t.Error("date and config link not joined on one meta line")
	}
}

func TestMergeDedupesByDirAndSortsNewestFirst(t *testing.T) {
	configs := []Experiment{{Dir: "20261120-a", Title: "A", Date: "2026-11-20"}}
	discovered := []Experiment{
		{Dir: "20261120-a", Title: "dup"},
		{Dir: "20261125-b", Title: "B", Date: "2026-11-25"},
	}
	got := Merge(configs, discovered)
	if len(got) != 2 {
		t.Fatalf("merged = %d, want 2 (dup dropped)", len(got))
	}
	if got[0].Dir != "20261125-b" || got[1].Dir != "20261120-a" {
		t.Errorf("order = %s, %s; want newest first", got[0].Dir, got[1].Dir)
	}
}
