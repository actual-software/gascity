package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// connError wraps transport-level errors (connection refused, timeout, etc.)
// to distinguish them from API-level error responses.
type connError struct {
	err error
}

func (e *connError) Error() string { return e.err.Error() }
func (e *connError) Unwrap() error { return e.err }

// IsConnError reports whether err is a transport-level connection failure
// (e.g., connection refused, timeout) rather than an API-level error response.
func IsConnError(err error) bool {
	var ce *connError
	return errors.As(err, &ce)
}

// readOnlyError indicates the API server rejected a mutation because it's
// running in read-only mode (non-localhost bind).
type readOnlyError struct {
	msg string
}

func (e *readOnlyError) Error() string { return e.msg }

// ShouldFallback reports whether err indicates the CLI should fall back to
// direct file mutation. This is true for transport-level failures (connection
// refused, timeout) and for read-only API rejections (server bound to
// non-localhost, mutations disabled).
func ShouldFallback(err error) bool {
	if IsConnError(err) {
		return true
	}
	var ro *readOnlyError
	return errors.As(err, &ro)
}

// Client is an HTTP client for the Gas City API server.
// It wraps mutation endpoints so CLI commands can route writes
// through the API when a controller is running.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new API client targeting the given base URL
// (e.g., "http://127.0.0.1:8080").
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// SuspendCity suspends the city via PATCH /v0/city.
func (c *Client) SuspendCity() error {
	return c.patchCity(true)
}

// ResumeCity resumes the city via PATCH /v0/city.
func (c *Client) ResumeCity() error {
	return c.patchCity(false)
}

func (c *Client) patchCity(suspend bool) error {
	body := map[string]any{"suspended": suspend}
	return c.doMutation("PATCH", "/v0/city", body)
}

// SuspendAgent suspends an agent via POST /v0/agent/{name}/suspend.
// Name can be qualified (e.g., "myrig/worker") — the server route uses
// {name...} wildcard which captures slashes.
func (c *Client) SuspendAgent(name string) error {
	return c.doMutation("POST", "/v0/agent/"+escapeName(name)+"/suspend", nil)
}

// ResumeAgent resumes an agent via POST /v0/agent/{name}/resume.
func (c *Client) ResumeAgent(name string) error {
	return c.doMutation("POST", "/v0/agent/"+escapeName(name)+"/resume", nil)
}

// SuspendRig suspends a rig via POST /v0/rig/{name}/suspend.
func (c *Client) SuspendRig(name string) error {
	return c.doMutation("POST", "/v0/rig/"+escapeName(name)+"/suspend", nil)
}

// ResumeRig resumes a rig via POST /v0/rig/{name}/resume.
func (c *Client) ResumeRig(name string) error {
	return c.doMutation("POST", "/v0/rig/"+escapeName(name)+"/resume", nil)
}

// RestartRig restarts a rig via POST /v0/rig/{name}/restart.
// Kills all agents in the rig; the reconciler restarts them.
func (c *Client) RestartRig(name string) error {
	return c.doMutation("POST", "/v0/rig/"+escapeName(name)+"/restart", nil)
}

// RestartAgent restarts an agent via POST /v0/agent/{name}/restart.
// Kills the agent's session; the reconciler restarts it.
func (c *Client) RestartAgent(name string) error {
	return c.doMutation("POST", "/v0/agent/"+escapeName(name)+"/restart", nil)
}

// escapeName escapes each segment of a potentially qualified name (e.g.,
// "myrig/worker") for use in URL paths. Slashes are preserved as path
// separators; other URL metacharacters (#, ?, etc.) are percent-encoded.
func escapeName(name string) string {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// doMutation sends a mutation request and checks for errors.
func (c *Client) doMutation(method, path string, body any) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("X-GC-Request", "true")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return &connError{err: fmt.Errorf("request failed: %w", err)}
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	// Parse error response.
	var apiErr struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		return fmt.Errorf("API returned %d", resp.StatusCode)
	}

	// Read-only rejection: server is bound non-localhost and rejects mutations.
	// CLI should fall back to direct file mutation.
	if apiErr.Error == "read_only" {
		msg := apiErr.Message
		if msg == "" {
			msg = "mutations disabled (read-only server)"
		}
		return &readOnlyError{msg: msg}
	}

	if apiErr.Message != "" {
		return fmt.Errorf("API error: %s", apiErr.Message)
	}
	if apiErr.Error != "" {
		return fmt.Errorf("API error: %s", apiErr.Error)
	}
	return fmt.Errorf("API returned %d", resp.StatusCode)
}
