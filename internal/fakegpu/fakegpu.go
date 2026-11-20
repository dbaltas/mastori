// Package fakegpu advertises a fake nvidia.com/gpu extended resource on the
// Kubernetes node it runs on, by patching the node status allocatable. It
// exists for the local kind GPU pool, whose nodes have no NVIDIA hardware;
// on cloud GPU nodes the real GPU Operator or device plugin supersedes it.
package fakegpu

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ResourceName     = "nvidia.com/gpu"
	PatchContentType = "application/json-patch+json"
	// Kubelet rewrites node status every ~10s and drops extended resources
	// it does not manage, so re-assert well inside that window to keep the
	// device schedulable almost continuously.
	DefaultInterval = 2 * time.Second

	tokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caPath    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

type Client struct {
	Host   string
	Token  string
	HTTP   *http.Client
	Logger *slog.Logger
}

func NewInCluster() (*Client, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("fakegpu: KUBERNETES_SERVICE_HOST/PORT not set; not running in cluster")
	}
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("fakegpu: read service account token: %w", err)
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("fakegpu: read service account CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("fakegpu: no valid certificates in service account CA bundle")
	}
	return &Client{
		Host:   "https://" + net.JoinHostPort(host, port),
		Token:  strings.TrimSpace(string(token)),
		HTTP:   &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}},
		Logger: slog.Default(),
	}, nil
}

func PatchBytes(count int64) []byte {
	path := strings.ReplaceAll(ResourceName, "/", "~1")
	return []byte(fmt.Sprintf(`[{"op":"add","path":"/status/allocatable/%s","value":"%d"}]`, path, count))
}

type nodeStatus struct {
	Status struct {
		Allocatable map[string]string `json:"allocatable"`
	} `json:"status"`
}

func ParseAllocatable(body []byte) (int64, bool, error) {
	var node nodeStatus
	if err := json.Unmarshal(body, &node); err != nil {
		return 0, false, fmt.Errorf("fakegpu: parse node: %w", err)
	}
	raw, ok := node.Status.Allocatable[ResourceName]
	if !ok {
		return 0, false, nil
	}
	count, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, true, fmt.Errorf("fakegpu: parse allocatable %s=%q: %w", ResourceName, raw, err)
	}
	return count, true, nil
}

func (c *Client) nodeURL(nodeName string) string {
	return c.Host + "/api/v1/nodes/" + nodeName
}

func (c *Client) nodeStatusURL(nodeName string) string {
	return c.nodeURL(nodeName) + "/status"
}

func (c *Client) Current(ctx context.Context, nodeName string) (int64, bool, error) {
	body, err := c.request(ctx, http.MethodGet, nodeName, nil, "", false)
	if err != nil {
		return 0, false, err
	}
	return ParseAllocatable(body)
}

func (c *Client) Advertise(ctx context.Context, nodeName string, count int64) (bool, error) {
	if nodeName == "" {
		return false, errors.New("fakegpu: empty node name")
	}
	if count < 1 {
		return false, fmt.Errorf("fakegpu: count %d < 1", count)
	}
	current, present, err := c.Current(ctx, nodeName)
	if err != nil {
		return false, err
	}
	if present && current == count {
		return false, nil
	}
	_, err = c.request(ctx, http.MethodPatch, nodeName, PatchBytes(count), PatchContentType, true)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *Client) request(ctx context.Context, method, nodeName string, body []byte, contentType string, status bool) ([]byte, error) {
	url := c.nodeURL(nodeName)
	if status {
		url = c.nodeStatusURL(nodeName)
	}
	var reader io.Reader
	if body != nil {
		reader = strings.NewReader(string(body))
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, fmt.Errorf("fakegpu: build request: %w", err)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fakegpu: %s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("fakegpu: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fakegpu: %s %s: status %d: %s", method, nodeName, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}

func (c *Client) Run(ctx context.Context, nodeName string, count int64, interval time.Duration) error {
	if interval <= 0 {
		interval = DefaultInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := c.Advertise(ctx, nodeName, count); err != nil {
			c.Logger.Error("fakegpu advertise failed", "node", nodeName, "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
