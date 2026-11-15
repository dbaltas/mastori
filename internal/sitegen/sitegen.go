// Package sitegen discovers experiment stories in the benchmark submodule and
// renders a small static site: an index page linking every experiment story,
// plus metadata (objective, date, config path) pulled from the experiment YAML.
package sitegen

import (
	"fmt"
	"html"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Experiment describes one published experiment story.
type Experiment struct {
	// Dir is the results directory name, e.g. "20261115-mockworker-scaling-knee".
	// It doubles as the story's URL segment on the site.
	Dir string
	// Title is the story page title, taken from the story's first heading.
	Title string
	// Date is the experiment date in YYYY-MM-DD form ("" if unknown).
	Date string
	// Objective is the experiment objective from the experiment YAML ("" for
	// stories without a matching experiment file).
	Objective string
	// ConfigName is the published experiment config file name ("" if none).
	ConfigName string
}

// experimentYAML mirrors the header block of benchmark/experiments/*.yaml.
type experimentYAML struct {
	Experiment struct {
		Name      string `yaml:"name"`
		Date      string `yaml:"date"`
		Objective string `yaml:"objective"`
		Story     string `yaml:"story"`
	} `yaml:"experiment"`
}

// LoadExperiments parses every *.yaml file in experimentsDir and returns one
// Experiment per file whose story file exists relative to benchmarkRoot.
// A story discovered here takes priority over a bare discovered story with
// the same results directory.
func LoadExperiments(experimentsDir, benchmarkRoot string) ([]Experiment, error) {
	entries, err := os.ReadDir(experimentsDir)
	if err != nil {
		return nil, fmt.Errorf("read experiments dir: %w", err)
	}

	var experiments []Experiment
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		exp, err := loadExperiment(filepath.Join(experimentsDir, entry.Name()), benchmarkRoot)
		if err != nil {
			return nil, err
		}
		if exp != nil {
			experiments = append(experiments, *exp)
		}
	}
	return experiments, nil
}

func loadExperiment(configPath, benchmarkRoot string) (*Experiment, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read experiment config %s: %w", configPath, err)
	}

	var parsed experimentYAML
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("parse experiment config %s: %w", configPath, err)
	}

	meta := parsed.Experiment
	if meta.Story == "" {
		return nil, nil
	}

	storyPath := filepath.Join(benchmarkRoot, filepath.FromSlash(meta.Story))
	if _, err := os.Stat(storyPath); err != nil {
		// The experiment references a story that has not been published yet.
		return nil, nil
	}

	return &Experiment{
		Dir:        filepath.Base(filepath.Dir(storyPath)),
		Title:      firstNonEmpty(titleFromMarkdown(storyPath), meta.Name),
		Date:       strings.TrimSpace(meta.Date),
		Objective:  strings.TrimSpace(meta.Objective),
		ConfigName: filepath.Base(configPath),
	}, nil
}

// DiscoverStories scans benchmarkRoot/results for story directories
// (directories containing a story.md) and returns one Experiment per
// directory. Entries whose directory already appears in known are skipped so
// experiment-config metadata wins over bare discovery.
func DiscoverStories(benchmarkRoot string, known map[string]bool) ([]Experiment, error) {
	resultsDir := filepath.Join(benchmarkRoot, "results")
	entries, err := os.ReadDir(resultsDir)
	if err != nil {
		return nil, fmt.Errorf("read results dir: %w", err)
	}

	var stories []Experiment
	for _, entry := range entries {
		if !entry.IsDir() || known[entry.Name()] {
			continue
		}
		storyPath := filepath.Join(resultsDir, entry.Name(), "story.md")
		if _, err := os.Stat(storyPath); err != nil {
			continue
		}
		stories = append(stories, Experiment{
			Dir:   entry.Name(),
			Title: firstNonEmpty(titleFromMarkdown(storyPath), entry.Name()),
			Date:  dateFromDirName(entry.Name()),
		})
	}
	return stories, nil
}

// titleFromMarkdown extracts the title from a markdown file's first
// "# heading" line. Returns "" when the file is missing, unreadable, or has
// no level-1 heading.
func titleFromMarkdown(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return titleFromMarkdownBytes(data)
}

func titleFromMarkdownBytes(data []byte) string {
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "# ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "# "))
		}
	}
	return ""
}

// dateFromDirName parses the leading YYYYMMDD of a results directory name
// (e.g. "20261115-mockworker-scaling-knee") into YYYY-MM-DD, returning ""
// when the name does not start with a valid date.
func dateFromDirName(dirName string) string {
	if len(dirName) < 8 {
		return ""
	}
	var year, month, day int
	if _, err := fmt.Sscanf(dirName[:8], "%4d%2d%2d", &year, &month, &day); err != nil {
		return ""
	}
	if year < 1000 || month < 1 || month > 12 || day < 1 || day > 31 {
		return ""
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

// Sort orders experiments newest-first, then by title.
func Sort(experiments []Experiment) {
	sort.Slice(experiments, func(i, j int) bool {
		if experiments[i].Date != experiments[j].Date {
			return experiments[i].Date > experiments[j].Date
		}
		return experiments[i].Title < experiments[j].Title
	})
}

// Merge combines experiment-config entries and bare discovered stories,
// deduplicating by results directory (config entries win) and sorting
// newest-first.
func Merge(configExperiments, discovered []Experiment) []Experiment {
	seen := make(map[string]bool, len(configExperiments))
	for _, exp := range configExperiments {
		seen[exp.Dir] = true
	}
	merged := make([]Experiment, 0, len(configExperiments)+len(discovered))
	merged = append(merged, configExperiments...)
	for _, exp := range discovered {
		if !seen[exp.Dir] {
			merged = append(merged, exp)
		}
	}
	Sort(merged)
	return merged
}

// CopyFile copies src to dst, creating dst's parent directory.
func CopyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", dst, err)
	}
	return nil
}

// WriteOutput writes story markdown and experiment config copies into outDir.
// Each experiment gets outDir/<Dir>/story.md (when available) and, when the
// experiment has a config, outDir/experiments/<ConfigName>.
func WriteOutput(outDir string, experiments []Experiment, benchmarkRoot string) error {
	for _, exp := range experiments {
		storySrc := filepath.Join(benchmarkRoot, "results", exp.Dir, "story.md")
		if _, err := os.Stat(storySrc); err == nil {
			if err := CopyFile(storySrc, filepath.Join(outDir, exp.Dir, "story.md")); err != nil {
				return err
			}
		}
		if exp.ConfigName != "" {
			src := filepath.Join(benchmarkRoot, "experiments", exp.ConfigName)
			if _, err := os.Stat(src); err == nil {
				if err := CopyFile(src, filepath.Join(outDir, "experiments", exp.ConfigName)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// WalkExperiments walks every regular file under root (used by main for
// ancillary files). Exported for tests.
func WalkExperiments(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	return paths, err
}

const pageCSS = `body{max-width:820px;margin:2rem auto;padding:0 1rem;font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;color:#222}a{color:#0366d6;text-decoration:none}a:hover{text-decoration:underline}code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.9em}pre{background:#f6f8fa;padding:.75rem;border-radius:6px;overflow-x:auto}code{background:#f6f8fa;padding:.1em .3em;border-radius:3px}table{border-collapse:collapse;margin:1rem 0}th,td{border:1px solid #d0d7de;padding:.35rem .6rem;text-align:left}h1,h2{line-height:1.25}.meta{color:#57606a;font-size:.9rem}ul.experiments{list-style:none;padding:0}ul.experiments li{margin:1.5rem 0;padding-bottom:1rem;border-bottom:1px solid #d8dee4}footer{margin-top:3rem;color:#57606a;font-size:.85rem}`

// RenderIndex builds the site index page listing every experiment story.
func RenderIndex(experiments []Experiment) string {
	var b strings.Builder
	b.WriteString(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>mastori — benchmark stories</title>
<style>` + pageCSS + `</style>
</head>
<body>
<h1>mastori</h1>
<p class="meta">Benchmark experiment stories for the mastori GPU-inference benchmarking platform. Raw data lives in the <a href="https://huggingface.co/datasets/dbaltas/inference-api-benchmark-mastori/tree/main/results">benchmark dataset</a>; configuration and runbook are in the <a href="https://github.com/dbaltas/mastori">mastori repo</a>.</p>
<ul class="experiments">
`)
	for _, exp := range experiments {
		b.WriteString(`<li>`)
		b.WriteString(`<h2><a href="` + html.EscapeString(exp.Dir) + `/story.html">` + html.EscapeString(exp.Title) + `</a></h2>`)
		if exp.Date != "" {
			b.WriteString(`<p class="meta">` + html.EscapeString(exp.Date))
			if exp.ConfigName != "" {
				b.WriteString(` · <a href="experiments/` + html.EscapeString(exp.ConfigName) + `">experiment config</a>`)
			}
			b.WriteString(`</p>`)
		}
		if exp.Objective != "" {
			b.WriteString(`<p>` + html.EscapeString(exp.Objective) + `</p>`)
		}
		b.WriteString(`</li>
`)
	}
	b.WriteString(`</ul>
<footer>mastori benchmark stories · generated from the benchmark submodule</footer>
</body>
</html>
`)
	return b.String()
}

// RenderStoryPage wraps pandoc-rendered story HTML in the site shell.
func RenderStoryPage(title, bodyHTML string) string {
	escapedTitle := html.EscapeString(title)
	return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>` + escapedTitle + `</title>
<style>` + pageCSS + `</style>
</head>
<body>
<p><a href="../index.html">← All benchmark stories</a></p>
` + bodyHTML + `
<footer>mastori benchmark stories · <a href="https://huggingface.co/datasets/dbaltas/inference-api-benchmark-mastori/tree/main/results">raw dataset</a></footer>
</body>
</html>
`
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
