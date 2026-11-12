package datasets

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const rowsAPI = "https://datasets-server.huggingface.co/rows"

// The datasets-server API is a shared free service and throttles bursts with
// 5xx responses, so fetches retry with exponential backoff.
const maxRetries = 6

type Fetcher struct {
	Client  *http.Client
	HFToken string
}

func NewFetcher() *Fetcher {
	return &Fetcher{
		Client:  &http.Client{Timeout: 60 * time.Second},
		HFToken: os.Getenv("HF_TOKEN"),
	}
}

func (f *Fetcher) fetchPage(d Dataset, offset, length int) ([]map[string]any, error) {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		rows, retryable, err := f.fetchPageOnce(d, offset, length)
		if err == nil {
			return rows, nil
		}
		lastErr = err
		if !retryable || attempt == maxRetries {
			break
		}
		backoff := time.Duration(1<<uint(attempt)) * 2 * time.Second
		time.Sleep(backoff)
	}
	return nil, lastErr
}

func (f *Fetcher) fetchPageOnce(d Dataset, offset, length int) ([]map[string]any, bool, error) {
	url := fmt.Sprintf("%s?dataset=%s&config=%s&split=%s&offset=%d&length=%d",
		rowsAPI, d.Source, d.Config, d.Split, offset, length)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("User-Agent", "mastori-datasets/0.1")
	if f.HFToken != "" {
		req.Header.Set("Authorization", "Bearer "+f.HFToken)
	}
	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("request %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}
	if resp.StatusCode != http.StatusOK {
		retryable := resp.StatusCode >= 500
		return nil, retryable, fmt.Errorf("HF API returned %d: %s", resp.StatusCode, truncate(body, 300))
	}
	var parsed struct {
		Rows []struct {
			Row map[string]any `json:"row"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, false, fmt.Errorf("decode response: %w", err)
	}
	out := make([]map[string]any, 0, len(parsed.Rows))
	for _, r := range parsed.Rows {
		out = append(out, r.Row)
	}
	return out, false, nil
}

type extracted struct {
	Index int            `json:"index"`
	Data  map[string]any `json:"data"`
}

func extractRow(d Dataset, index int, row map[string]any) (map[string]any, error) {
	switch d.Extract {
	case "mmlu":
		return map[string]any{
			"subject":  row["subject"],
			"question": row["question"],
			"choices":  row["choices"],
			"answer":   row["answer"],
		}, nil
	case "first_human_turn":
		convs, ok := row["conversations"].([]any)
		if !ok {
			return nil, fmt.Errorf("row %d: missing conversations", index)
		}
		var prompt, reference string
		for _, c := range convs {
			msg, ok := c.(map[string]any)
			if !ok {
				continue
			}
			from, _ := msg["from"].(string)
			val, _ := msg["value"].(string)
			if from == "human" && prompt == "" {
				prompt = val
			}
			if from == "gpt" && reference == "" {
				reference = val
			}
			if prompt != "" && reference != "" {
				break
			}
		}
		if prompt == "" {
			return nil, fmt.Errorf("row %d: no human turn found", index)
		}
		return map[string]any{
			"prompt":           prompt,
			"reference_output": reference,
		}, nil
	default:
		return nil, fmt.Errorf("unknown extract mode %q", d.Extract)
	}
}

// Fetch downloads the selection for a dataset and writes it as JSONL to
// <cacheDir>/<name>.jsonl. The rows API serves the current revision of the
// dataset; the pinned revision in the manifest documents the state the sample
// was generated from.
func (f *Fetcher) Fetch(d Dataset, cacheDir string) (string, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", fmt.Errorf("create cache dir: %w", err)
	}

	rows := make([]extracted, 0, d.Selection.Count)
	offset := d.Selection.Start
	for len(rows) < d.Selection.Count {
		pageSize := 100
		remaining := d.Selection.Count - len(rows)
		if remaining < pageSize {
			pageSize = remaining
		}
		page, err := f.fetchPage(d, offset, pageSize)
		if err != nil {
			return "", err
		}
		if len(page) == 0 {
			return "", fmt.Errorf("dataset exhausted at offset %d: got %d/%d rows", offset, len(rows), d.Selection.Count)
		}
		for _, row := range page {
			ex, err := extractRow(d, d.Selection.Start+len(rows), row)
			if err != nil {
				return "", err
			}
			rows = append(rows, extracted{Index: len(rows), Data: ex})
		}
		offset += len(page)
	}

	outPath := filepath.Join(cacheDir, d.Name+".jsonl")
	out, err := os.Create(outPath)
	if err != nil {
		return "", fmt.Errorf("create %s: %w", outPath, err)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return "", fmt.Errorf("encode row %d: %w", r.Index, err)
		}
	}
	return outPath, nil
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}
