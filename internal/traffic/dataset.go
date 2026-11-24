package traffic

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Row is a single ready-to-send prompt loaded from a dataset cache file.
type Row struct {
	Dataset string
	Index   int
	Text    string
}

type cacheLine struct {
	Index int            `json:"index"`
	Data  map[string]any `json:"data"`
}

// LoadRows reads the JSONL cache files for the given dataset names from
// cacheDir and returns the extracted prompts in file order.
func LoadRows(cacheDir string, names []string) ([]Row, error) {
	var rows []Row
	for _, name := range names {
		path := filepath.Join(cacheDir, name+".jsonl")
		rowsFor, err := loadRowsFile(path, name)
		if err != nil {
			return nil, err
		}
		rows = append(rows, rowsFor...)
	}
	return rows, nil
}

func loadRowsFile(path, name string) ([]Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w (run `go run ./cmd/datasets fetch` first)", path, err)
	}
	defer f.Close()

	var rows []Row
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var cl cacheLine
		if err := json.Unmarshal([]byte(line), &cl); err != nil {
			return nil, fmt.Errorf("%s: row %d: invalid JSON: %w", path, len(rows), err)
		}
		text := extractPrompt(cl.Data)
		if text == "" {
			return nil, fmt.Errorf("%s: row %d: no prompt fields found", path, len(rows))
		}
		rows = append(rows, Row{Dataset: name, Index: cl.Index, Text: text})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", path, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%s: no rows", path)
	}
	return rows, nil
}

// extractPrompt builds the prompt text from a cache row. ShareGPT-style rows
// carry a "prompt" field; MMLU-style rows carry "question" plus "choices".
func extractPrompt(data map[string]any) string {
	if p, ok := data["prompt"].(string); ok && strings.TrimSpace(p) != "" {
		return p
	}
	q, ok := data["question"].(string)
	if !ok || strings.TrimSpace(q) == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(q)
	b.WriteString("\n")
	if choices, ok := data["choices"].([]any); ok {
		letters := "ABCDEFGH"
		for i, c := range choices {
			if s, ok := c.(string); ok && i < len(letters) {
				fmt.Fprintf(&b, "%c) %s\n", letters[i], s)
			}
		}
	}
	return strings.TrimRight(b.String(), "\n")
}
