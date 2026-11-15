package sitegen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTree creates files under a temp benchmark root shaped like the
// benchmark submodule: experiments/*.yaml and results/<dir>/story.md.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestLoadExperiments(t *testing.T) {
	root := writeTree(t, map[string]string{
		"experiments/20261115-mockworker-scaling.yaml": `
experiment:
  name: mockworker-scaling-knee
  date: "2026-11-15"
  objective: >
    Characterize mock worker scaling and find the smooth-throughput knee.
  story: results/20261115-mockworker-scaling-knee/story.md
`,
		"results/20261115-mockworker-scaling-knee/story.md": "# Benchmark story — mockworker scaling\n\nBody text.\n",
		// Experiment whose story has not been published yet: must be skipped.
		"experiments/20261116-unpublished.yaml": `
experiment:
  name: unpublished
  date: "2026-11-16"
  story: results/20261116-unpublished/story.md
`,
	})

	experiments, err := LoadExperiments(filepath.Join(root, "experiments"), root)
	if err != nil {
		t.Fatalf("LoadExperiments: %v", err)
	}
	if len(experiments) != 1 {
		t.Fatalf("got %d experiments, want 1 (unpublished story must be skipped): %+v", len(experiments), experiments)
	}

	exp := experiments[0]
	if exp.Dir != "20261115-mockworker-scaling-knee" {
		t.Errorf("Dir = %q", exp.Dir)
	}
	if exp.Title != "Benchmark story — mockworker scaling" {
		t.Errorf("Title = %q", exp.Title)
	}
	if exp.Date != "2026-11-15" {
		t.Errorf("Date = %q", exp.Date)
	}
	if !strings.Contains(exp.Objective, "smooth-throughput knee") {
		t.Errorf("Objective = %q", exp.Objective)
	}
	if exp.ConfigName != "20261115-mockworker-scaling.yaml" {
		t.Errorf("ConfigName = %q", exp.ConfigName)
	}
}

func TestLoadExperimentsTitleFallsBackToName(t *testing.T) {
	root := writeTree(t, map[string]string{
		"experiments/20261115-mockworker-scaling.yaml": `
experiment:
  name: fallback-name
  story: results/20261115-mockworker-scaling-knee/story.md
`,
		"results/20261115-mockworker-scaling-knee/story.md": "No heading here.\n",
	})

	experiments, err := LoadExperiments(filepath.Join(root, "experiments"), root)
	if err != nil {
		t.Fatalf("LoadExperiments: %v", err)
	}
	if len(experiments) != 1 || experiments[0].Title != "fallback-name" {
		t.Fatalf("got %+v, want single experiment titled fallback-name", experiments)
	}
}

func TestLoadExperimentsIgnoresNonYAML(t *testing.T) {
	root := writeTree(t, map[string]string{
		"experiments/README.md": "not yaml",
		"experiments/notes.txt": "still not yaml",
	})

	experiments, err := LoadExperiments(filepath.Join(root, "experiments"), root)
	if err != nil {
		t.Fatalf("LoadExperiments: %v", err)
	}
	if len(experiments) != 0 {
		t.Fatalf("got %+v, want none", experiments)
	}
}

func TestDiscoverStories(t *testing.T) {
	root := writeTree(t, map[string]string{
		"results/20261115-mockworker-scaling-knee/story.md": "# Story A\n",
		"results/20261114-legacy/story.md":                  "# Story B\n",
		"results/_legacy/README.md":                         "no story.md here\n",
		"results/20261113-raw/other.txt":                    "no story.md here\n",
	})

	discovered, err := DiscoverStories(root, map[string]bool{"20261115-mockworker-scaling-knee": true})
	if err != nil {
		t.Fatalf("DiscoverStories: %v", err)
	}
	if len(discovered) != 1 {
		t.Fatalf("got %+v, want only 20261114-legacy (known dir skipped, no-story dirs skipped)", discovered)
	}
	if discovered[0].Dir != "20261114-legacy" || discovered[0].Title != "Story B" {
		t.Fatalf("unexpected entry %+v", discovered[0])
	}
	if discovered[0].Date != "2026-11-14" {
		t.Errorf("Date = %q, want 2026-11-14", discovered[0].Date)
	}
}

func TestDateFromDirName(t *testing.T) {
	cases := map[string]string{
		"20261115-mockworker-scaling-knee": "2026-11-15",
		"20260101-x":                       "2026-01-01",
		"not-a-date":                       "",
		"2026":                             "",
		"20261301-x":                       "", // month 13
		"20260232-x":                       "", // day 32
	}
	for dirName, want := range cases {
		if got := dateFromDirName(dirName); got != want {
			t.Errorf("dateFromDirName(%q) = %q, want %q", dirName, got, want)
		}
	}
}

func TestMergeDedupesAndSortsNewestFirst(t *testing.T) {
	configExperiments := []Experiment{
		{Dir: "20261115-knee", Title: "Knee", Date: "2026-11-15"},
	}
	discovered := []Experiment{
		{Dir: "20261115-knee", Title: "Knee (bare)", Date: "2026-11-15"},
		{Dir: "20261110-early", Title: "Early", Date: "2026-11-10"},
	}

	merged := Merge(configExperiments, discovered)
	if len(merged) != 2 {
		t.Fatalf("got %d experiments, want 2: %+v", len(merged), merged)
	}
	if merged[0].Dir != "20261115-knee" || merged[0].Title != "Knee" {
		t.Errorf("first = %+v, want config entry 20261115-knee", merged[0])
	}
	if merged[1].Dir != "20261110-early" {
		t.Errorf("second = %+v, want 20261110-early", merged[1])
	}
}

func TestTitleFromMarkdownBytes(t *testing.T) {
	cases := []struct {
		md   string
		want string
	}{
		{"# Title\nbody", "Title"},
		{"intro\n\n# Later heading\n", "Later heading"},
		{"## Not h1\n", ""},
		{"", ""},
		{"#NoSpace\n", ""},
		{"#   Padded  \n", "Padded"},
	}
	for _, tc := range cases {
		if got := titleFromMarkdownBytes([]byte(tc.md)); got != tc.want {
			t.Errorf("titleFromMarkdownBytes(%q) = %q, want %q", tc.md, got, tc.want)
		}
	}
}

func TestRenderIndexListsAndEscapes(t *testing.T) {
	experiments := []Experiment{
		{Dir: "20261115-knee", Title: "A <title> & \"quote\"", Date: "2026-11-15",
			Objective: "Find <knee>", ConfigName: "20261115-knee.yaml"},
		{Dir: "20261110-early", Title: "Early", Date: "2026-11-10"},
	}

	out := RenderIndex(experiments)

	for _, want := range []string{
		`<a href="20261115-knee/story.html">`,
		`experiments/20261115-knee.yaml`,
		`A &lt;title&gt; &amp; &#34;quote&#34;`,
		`Find &lt;knee&gt;`,
		`<a href="20261110-early/story.html">Early</a>`,
		`https://huggingface.co/datasets/dbaltas/inference-api-benchmark-mastori`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("index missing %q", want)
		}
	}
	// Newest first.
	if strings.Index(out, "20261115-knee") > strings.Index(out, "20261110-early") {
		t.Error("index not sorted newest-first")
	}
}

func TestRenderStoryPage(t *testing.T) {
	out := RenderStoryPage("Story & <title>", "<h1>Body</h1>")
	for _, want := range []string{
		`<title>Story &amp; &lt;title&gt;</title>`,
		`<h1>Body</h1>`,
		`<a href="../index.html">`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("story page missing %q", want)
		}
	}
}

func TestWriteOutputCopiesStoryAndConfig(t *testing.T) {
	root := writeTree(t, map[string]string{
		"experiments/20261115-knee.yaml":                       "experiment:\n  name: knee\n  story: results/20261115-knee/story.md\n",
		"results/20261115-knee/story.md":                       "# Knee\n",
		"results/20261110-early/story.md":                      "# Early\n",
		"results/20261110-early/runs/20261110T000000Z-0.jsonl": "{}\n",
	})

	experiments := []Experiment{
		{Dir: "20261115-knee", Title: "Knee", ConfigName: "20261115-knee.yaml"},
		{Dir: "20261110-early", Title: "Early"},
	}
	out := filepath.Join(t.TempDir(), "site")
	if err := WriteOutput(out, experiments, root); err != nil {
		t.Fatalf("WriteOutput: %v", err)
	}

	for _, path := range []string{
		filepath.Join(out, "20261115-knee", "story.md"),
		filepath.Join(out, "experiments", "20261115-knee.yaml"),
		filepath.Join(out, "20261110-early", "story.md"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected %s to exist: %v", path, err)
		}
	}
	// Run JSONL files must not be copied.
	if _, err := os.Stat(filepath.Join(out, "20261110-early", "runs")); !os.IsNotExist(err) {
		t.Error("run data should not be copied to the site")
	}
}

func TestSortNewestFirst(t *testing.T) {
	experiments := []Experiment{
		{Dir: "a", Date: "2026-11-10"},
		{Dir: "b", Date: "2026-11-15"},
		{Dir: "c", Date: "2026-11-15"},
	}
	Sort(experiments)
	if experiments[0].Dir != "b" || experiments[1].Dir != "c" || experiments[2].Dir != "a" {
		t.Fatalf("sort order wrong: %+v", experiments)
	}
}
