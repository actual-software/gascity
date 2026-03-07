package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientSuspendCity(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody map[string]any

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		json.NewDecoder(r.Body).Decode(&gotBody) //nolint:errcheck
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	if err := c.SuspendCity(); err != nil {
		t.Fatalf("SuspendCity: %v", err)
	}
	if gotMethod != "PATCH" {
		t.Errorf("method = %q, want PATCH", gotMethod)
	}
	if gotPath != "/v0/city" {
		t.Errorf("path = %q, want /v0/city", gotPath)
	}
	if gotBody["suspended"] != true {
		t.Errorf("body suspended = %v, want true", gotBody["suspended"])
	}
}

func TestClientResumeCity(t *testing.T) {
	var gotBody map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody) //nolint:errcheck
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	if err := c.ResumeCity(); err != nil {
		t.Fatalf("ResumeCity: %v", err)
	}
	if gotBody["suspended"] != false {
		t.Errorf("body suspended = %v, want false", gotBody["suspended"])
	}
}

func TestClientSuspendAgent(t *testing.T) {
	var gotMethod, gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	if err := c.SuspendAgent("worker"); err != nil {
		t.Fatalf("SuspendAgent: %v", err)
	}
	if gotMethod != "POST" {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/v0/agent/worker/suspend" {
		t.Errorf("path = %q, want /v0/agent/worker/suspend", gotPath)
	}
}

func TestClientResumeAgent(t *testing.T) {
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	if err := c.ResumeAgent("worker"); err != nil {
		t.Fatalf("ResumeAgent: %v", err)
	}
	if gotPath != "/v0/agent/worker/resume" {
		t.Errorf("path = %q, want /v0/agent/worker/resume", gotPath)
	}
}

func TestClientSuspendRig(t *testing.T) {
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	if err := c.SuspendRig("myrig"); err != nil {
		t.Fatalf("SuspendRig: %v", err)
	}
	if gotPath != "/v0/rig/myrig/suspend" {
		t.Errorf("path = %q, want /v0/rig/myrig/suspend", gotPath)
	}
}

func TestClientResumeRig(t *testing.T) {
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	if err := c.ResumeRig("myrig"); err != nil {
		t.Fatalf("ResumeRig: %v", err)
	}
	if gotPath != "/v0/rig/myrig/resume" {
		t.Errorf("path = %q, want /v0/rig/myrig/resume", gotPath)
	}
}

func TestClientErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
			"error":   "not_found",
			"message": "agent 'nope' not found",
		})
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	err := c.SuspendAgent("nope")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got != "API error: agent 'nope' not found" {
		t.Errorf("error = %q", got)
	}
}

func TestClientQualifiedAgentName(t *testing.T) {
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	if err := c.SuspendAgent("myrig/worker"); err != nil {
		t.Fatalf("SuspendAgent: %v", err)
	}
	// The server uses {name...} wildcard, so the raw slash must arrive unescaped.
	if gotPath != "/v0/agent/myrig/worker/suspend" {
		t.Errorf("path = %q, want /v0/agent/myrig/worker/suspend", gotPath)
	}
}

func TestClientConnError(t *testing.T) {
	// Client targeting a port with nothing listening → connection refused.
	c := NewClient("http://127.0.0.1:1") // port 1 is never listening
	err := c.SuspendCity()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !IsConnError(err) {
		t.Errorf("IsConnError = false for connection refused error: %v", err)
	}
}

func TestClientAPIErrorNotConnError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "bad_request"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	err := c.SuspendCity()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if IsConnError(err) {
		t.Errorf("IsConnError = true for API error response: %v", err)
	}
}

func TestClientReadOnlyFallback(t *testing.T) {
	// Server returns 403 with read_only error code — should trigger ShouldFallback.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
			"error":   "read_only",
			"message": "mutations disabled: server bound to non-localhost address",
		})
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	err := c.SuspendCity()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !ShouldFallback(err) {
		t.Errorf("ShouldFallback = false for read-only rejection: %v", err)
	}
	if IsConnError(err) {
		t.Errorf("IsConnError = true for read-only rejection (should be false)")
	}
}

func TestClientConnErrorShouldFallback(t *testing.T) {
	c := NewClient("http://127.0.0.1:1")
	err := c.SuspendCity()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !ShouldFallback(err) {
		t.Errorf("ShouldFallback = false for connection error: %v", err)
	}
}

func TestClientBusinessErrorNoFallback(t *testing.T) {
	// A 404 not_found is a business error — should NOT trigger fallback.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
			"error":   "not_found",
			"message": "agent 'nope' not found",
		})
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	err := c.SuspendAgent("nope")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if ShouldFallback(err) {
		t.Errorf("ShouldFallback = true for business error: %v", err)
	}
}

func TestClientRestartRig(t *testing.T) {
	var gotMethod, gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	if err := c.RestartRig("myrig"); err != nil {
		t.Fatalf("RestartRig: %v", err)
	}
	if gotMethod != "POST" {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/v0/rig/myrig/restart" {
		t.Errorf("path = %q, want /v0/rig/myrig/restart", gotPath)
	}
}

func TestClientRestartAgent(t *testing.T) {
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	if err := c.RestartAgent("myrig/worker"); err != nil {
		t.Fatalf("RestartAgent: %v", err)
	}
	if gotPath != "/v0/agent/myrig/worker/restart" {
		t.Errorf("path = %q, want /v0/agent/myrig/worker/restart", gotPath)
	}
}

func TestClientCSRFHeader(t *testing.T) {
	var gotHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-GC-Request")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	}))
	defer ts.Close()

	c := NewClient(ts.URL)
	c.SuspendAgent("worker") //nolint:errcheck
	if gotHeader != "true" {
		t.Errorf("X-GC-Request = %q, want %q", gotHeader, "true")
	}
}
