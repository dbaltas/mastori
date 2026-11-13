package adapter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OllamaClient talks to Ollama's native API. The adapter translates Ollama's
// NDJSON streaming into the vLLM/OpenAI-compatible surface it serves.
type OllamaClient struct {
	BaseURL string
	HTTP    *http.Client
}

// GenerateChunk is one NDJSON line from Ollama's /api/generate or /api/chat.
// Both endpoints share the done/usage fields; generate fills Response and
// chat fills Message.
type GenerateChunk struct {
	Model     string    `json:"model"`
	CreatedAt time.Time `json:"created_at"`
	Response  string    `json:"response"`
	Message   *struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"message"`
	Done            bool   `json:"done"`
	Error           string `json:"error"`
	PromptEvalCount int64  `json:"prompt_eval_count"`
	EvalCount       int64  `json:"eval_count"`
}

type OllamaOptions struct {
	NumPredict  int      `json:"num_predict,omitempty"`
	Temperature *float64 `json:"temperature,omitempty"`
}

type generateRequest struct {
	Model    string        `json:"model"`
	Prompt   string        `json:"prompt,omitempty"`
	Messages []ChatMessage `json:"messages,omitempty"`
	Stream   bool          `json:"stream"`
	Options  OllamaOptions `json:"options"`
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (c *OllamaClient) client() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *OllamaClient) post(ctx context.Context, path string, payload any) (*http.Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(c.BaseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.client().Do(req)
}

// StreamGeneration posts to /api/generate or /api/chat (when Messages is set)
// and calls onChunk for every NDJSON event until done. It returns the final
// chunk, which carries the token counts.
func (c *OllamaClient) StreamGeneration(ctx context.Context, req generateRequest, onChunk func(GenerateChunk) error) (*GenerateChunk, error) {
	path := "/api/generate"
	if len(req.Messages) > 0 {
		path = "/api/chat"
	}

	resp, err := c.post(ctx, path, req)
	if err != nil {
		return nil, fmt.Errorf("ollama: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, &OllamaError{Status: resp.StatusCode, Message: strings.TrimSpace(string(b))}
	}

	var final GenerateChunk
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ch GenerateChunk
		if err := json.Unmarshal([]byte(line), &ch); err != nil {
			return nil, fmt.Errorf("ollama: malformed stream line: %w", err)
		}
		if ch.Error != "" {
			return nil, &OllamaError{Status: http.StatusInternalServerError, Message: ch.Error}
		}
		if err := onChunk(ch); err != nil {
			return nil, err
		}
		if ch.Done {
			final = ch
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("ollama: stream read failed: %w", err)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if !final.Done {
		return nil, fmt.Errorf("ollama: stream ended without done event")
	}
	return &final, nil
}

// Tags lists installed models from /api/tags.
func (c *OllamaClient) Tags(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(c.BaseURL, "/")+"/api/tags", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &OllamaError{Status: resp.StatusCode, Message: "tags request failed"}
	}
	var out struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("ollama: malformed tags response: %w", err)
	}
	names := make([]string, 0, len(out.Models))
	for _, m := range out.Models {
		names = append(names, m.Name)
	}
	return names, nil
}

// OllamaError carries the status Ollama returned plus its error message.
type OllamaError struct {
	Status  int
	Message string
}

func (e *OllamaError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("ollama: request failed with status %d", e.Status)
	}
	return fmt.Sprintf("ollama: %d: %s", e.Status, e.Message)
}
