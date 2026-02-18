package depclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client calls the dependency simulator service.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// NewClient creates a dep client pointing at the given base URL.
// LAB: STEP1 TODO - The http.Client here has no timeout configured.
// Participants should add transport-level timeouts and ensure requests
// use the caller's context for deadline propagation.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
		// LAB: STEP1 TODO - add Timeout and/or a custom Transport with
		// TLSHandshakeTimeout, ResponseHeaderTimeout, etc.
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				TLSHandshakeTimeout:   3 * time.Second,
				ResponseHeaderTimeout: 5 * time.Second,
				IdleConnTimeout:       10 * time.Second,
			},
		},
	}
}

// Call invokes the /work endpoint on the dep service.
// LAB: STEP1 TODO - This function ignores the context. Participants should:
//  1. Use context.WithTimeout to enforce a deadline
//  2. Use http.NewRequestWithContext so the HTTP call respects cancellation
func Call(ctx context.Context, c *Client, sleep string, failRate string) (string, error) {
	url := fmt.Sprintf("%s/work?sleep=%s&fail=%s", c.BaseURL, sleep, failRate)
	// LAB: STEP1 TODO - replace http.Get with http.NewRequestWithContext(ctx, ...)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("dep call failed: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading dep response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("dep returned %d: %s", resp.StatusCode, string(body))
	}
	return string(body), nil
}
