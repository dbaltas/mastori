package datasets

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Validator struct{}

func NewValidator() *Validator { return &Validator{} }

type checkResult struct {
	OK      bool
	Details string
}

func (v *Validator) Validate(d Dataset, cacheDir string) error {
	path := filepath.Join(cacheDir, d.Name+".jsonl")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("cache file missing: %s (run the fetch command first)", path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")

	if res := checkCount(lines, d); !res.OK {
		return fmt.Errorf("%s", res.Details)
	}
	fmt.Printf("PASS %s: row count = %d (min %d, expected %d)\n", d.Name, len(lines), d.Validate.MinRows, d.Selection.Count)

	rows := make([]extracted, 0, len(lines))
	for i, line := range lines {
		var r extracted
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return fmt.Errorf("%s: row %d: invalid JSON: %w", d.Name, i, err)
		}
		rows = append(rows, r)
	}

	if res := checkFields(rows, d); !res.OK {
		return fmt.Errorf("%s", res.Details)
	}
	fmt.Printf("PASS %s: required fields present (%s)\n", d.Name, strings.Join(d.Validate.RequiredFields, ", "))

	if res := checkSpotMatches(rows, d); !res.OK {
		return fmt.Errorf("%s", res.Details)
	}
	fmt.Printf("PASS %s: all %d spot checks matched\n", d.Name, len(d.Validate.SpotChecks))

	return nil
}

func checkCount(lines []string, d Dataset) checkResult {
	n := len(lines)
	if n < d.Validate.MinRows {
		return checkResult{false, fmt.Sprintf("%s: only %d rows, minimum is %d", d.Name, n, d.Validate.MinRows)}
	}
	if d.Selection.Count > 0 && n != d.Selection.Count {
		return checkResult{false, fmt.Sprintf("%s: %d rows, expected exactly %d — cache is stale or partial, re-run fetch", d.Name, n, d.Selection.Count)}
	}
	return checkResult{true, ""}
}

func checkFields(rows []extracted, d Dataset) checkResult {
	bad := make(map[string]int)
	for _, r := range rows {
		for _, field := range d.Validate.RequiredFields {
			val, ok := r.Data[field]
			if !ok || val == nil {
				bad[field]++
				continue
			}
			if s, isStr := val.(string); isStr && strings.TrimSpace(s) == "" {
				bad[field]++
			}
			if arr, isArr := val.([]any); isArr && len(arr) == 0 {
				bad[field]++
			}
		}
	}
	if len(bad) > 0 {
		var parts []string
		for f, c := range bad {
			parts = append(parts, fmt.Sprintf("%s missing/empty in %d rows", f, c))
		}
		return checkResult{false, fmt.Sprintf("%s: %s", d.Name, strings.Join(parts, "; "))}
	}
	return checkResult{true, ""}
}

func rowString(r extracted) string {
	var sb strings.Builder
	for _, val := range r.Data {
		switch v := val.(type) {
		case string:
			sb.WriteString(v)
			sb.WriteString("\n")
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok {
					sb.WriteString(s)
					sb.WriteString("\n")
				}
			}
		}
	}
	return sb.String()
}

func checkSpotMatches(rows []extracted, d Dataset) checkResult {
	var failures []string
	for _, sc := range d.Validate.SpotChecks {
		if sc.Index < 0 || sc.Index >= len(rows) {
			failures = append(failures, fmt.Sprintf("index %d out of range (%d rows)", sc.Index, len(rows)))
			continue
		}
		if !strings.Contains(rowString(rows[sc.Index]), sc.Contains) {
			failures = append(failures, fmt.Sprintf("row %d does not contain %q", sc.Index, sc.Contains))
		}
	}
	if len(failures) > 0 {
		return checkResult{false, fmt.Sprintf("%s: %s", d.Name, strings.Join(failures, "; "))}
	}
	return checkResult{true, ""}
}
