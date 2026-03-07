package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gastownhall/gascity/internal/automations"
)

func TestHandleAutomationList_Empty(t *testing.T) {
	fs := newFakeState(t)
	srv := New(fs)

	req := httptest.NewRequest("GET", "/v0/automations", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp struct {
		Automations []automationResponse `json:"automations"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Automations) != 0 {
		t.Errorf("len(automations) = %d, want 0", len(resp.Automations))
	}
}

func TestHandleAutomationList(t *testing.T) {
	fs := newFakeState(t)
	enabled := true
	fs.autos = []automations.Automation{
		{
			Name:        "dolt-health",
			Description: "Check dolt status",
			Exec:        "dolt status",
			Gate:        "cooldown",
			Interval:    "5m",
			Enabled:     &enabled,
		},
		{
			Name:    "deploy",
			Formula: "deploy-steps",
			Gate:    "manual",
			Pool:    "workers",
			Rig:     "myrig",
		},
	}
	srv := New(fs)

	req := httptest.NewRequest("GET", "/v0/automations", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp struct {
		Automations []automationResponse `json:"automations"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Automations) != 2 {
		t.Fatalf("len(automations) = %d, want 2", len(resp.Automations))
	}

	a0 := resp.Automations[0]
	if a0.Name != "dolt-health" {
		t.Errorf("name = %q, want %q", a0.Name, "dolt-health")
	}
	if a0.Type != "exec" {
		t.Errorf("type = %q, want %q", a0.Type, "exec")
	}
	if a0.Gate != "cooldown" {
		t.Errorf("gate = %q, want %q", a0.Gate, "cooldown")
	}
	if a0.Interval != "5m" {
		t.Errorf("interval = %q, want %q", a0.Interval, "5m")
	}
	if !a0.Enabled {
		t.Error("expected enabled=true")
	}

	a1 := resp.Automations[1]
	if a1.Name != "deploy" {
		t.Errorf("name = %q, want %q", a1.Name, "deploy")
	}
	if a1.Type != "formula" {
		t.Errorf("type = %q, want %q", a1.Type, "formula")
	}
	if a1.Rig != "myrig" {
		t.Errorf("rig = %q, want %q", a1.Rig, "myrig")
	}
	if a1.Pool != "workers" {
		t.Errorf("pool = %q, want %q", a1.Pool, "workers")
	}
}

func TestHandleAutomationGet(t *testing.T) {
	fs := newFakeState(t)
	fs.autos = []automations.Automation{
		{
			Name:        "dolt-health",
			Description: "Check dolt status",
			Exec:        "dolt status",
			Gate:        "cooldown",
			Interval:    "5m",
		},
	}
	srv := New(fs)

	req := httptest.NewRequest("GET", "/v0/automation/dolt-health", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp automationResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Name != "dolt-health" {
		t.Errorf("name = %q, want %q", resp.Name, "dolt-health")
	}
	if resp.Type != "exec" {
		t.Errorf("type = %q, want %q", resp.Type, "exec")
	}
}

func TestHandleAutomationGet_ScopedName(t *testing.T) {
	fs := newFakeState(t)
	fs.autos = []automations.Automation{
		{
			Name: "health",
			Exec: "echo ok",
			Gate: "cooldown",
			Rig:  "myrig",
		},
	}
	srv := New(fs)

	// Match by scoped name: health:rig:myrig
	req := httptest.NewRequest("GET", "/v0/automation/health:rig:myrig", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, http.StatusOK, w.Body.String())
	}

	var resp automationResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Name != "health" {
		t.Errorf("name = %q, want %q", resp.Name, "health")
	}
	if resp.Rig != "myrig" {
		t.Errorf("rig = %q, want %q", resp.Rig, "myrig")
	}
}

func TestHandleAutomationGet_NotFound(t *testing.T) {
	fs := newFakeState(t)
	srv := New(fs)

	req := httptest.NewRequest("GET", "/v0/automation/nonexistent", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestHandleAutomationDisable(t *testing.T) {
	fs := newFakeMutatorState(t)
	fs.autos = []automations.Automation{
		{Name: "health", Exec: "echo ok", Gate: "cooldown"},
	}
	srv := New(fs)

	req := newPostRequest("/v0/automation/health/disable", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, http.StatusOK, w.Body.String())
	}

	// Verify override was written.
	if len(fs.cfg.Automations.Overrides) != 1 {
		t.Fatalf("expected 1 override, got %d", len(fs.cfg.Automations.Overrides))
	}
	ov := fs.cfg.Automations.Overrides[0]
	if ov.Name != "health" {
		t.Errorf("override name = %q, want %q", ov.Name, "health")
	}
	if ov.Enabled == nil || *ov.Enabled {
		t.Error("expected enabled=false")
	}
}

func TestHandleAutomationEnable(t *testing.T) {
	fs := newFakeMutatorState(t)
	fs.autos = []automations.Automation{
		{Name: "health", Exec: "echo ok", Gate: "cooldown"},
	}
	srv := New(fs)

	req := newPostRequest("/v0/automation/health/enable", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, http.StatusOK, w.Body.String())
	}

	if len(fs.cfg.Automations.Overrides) != 1 {
		t.Fatalf("expected 1 override, got %d", len(fs.cfg.Automations.Overrides))
	}
	ov := fs.cfg.Automations.Overrides[0]
	if ov.Enabled == nil || !*ov.Enabled {
		t.Error("expected enabled=true")
	}
}

func TestHandleAutomationGet_Ambiguous(t *testing.T) {
	fs := newFakeState(t)
	fs.autos = []automations.Automation{
		{Name: "health", Exec: "echo ok", Gate: "cooldown", Rig: "rig-a"},
		{Name: "health", Exec: "echo ok", Gate: "cooldown", Rig: "rig-b"},
	}
	srv := New(fs)

	// Bare name should return 409 when ambiguous.
	req := httptest.NewRequest("GET", "/v0/automation/health", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, http.StatusConflict, w.Body.String())
	}

	// Scoped name should resolve unambiguously.
	req = httptest.NewRequest("GET", "/v0/automation/health:rig:rig-a", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, http.StatusOK, w.Body.String())
	}
	var resp automationResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Rig != "rig-a" {
		t.Errorf("rig = %q, want %q", resp.Rig, "rig-a")
	}
}

func TestHandleAutomationDisable_Ambiguous(t *testing.T) {
	fs := newFakeMutatorState(t)
	fs.autos = []automations.Automation{
		{Name: "health", Exec: "echo ok", Gate: "cooldown", Rig: "rig-a"},
		{Name: "health", Exec: "echo ok", Gate: "cooldown", Rig: "rig-b"},
	}
	srv := New(fs)

	req := newPostRequest("/v0/automation/health/disable", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, http.StatusConflict, w.Body.String())
	}
}

func TestHandleAutomationDisable_NotFound(t *testing.T) {
	fs := newFakeMutatorState(t)
	srv := New(fs)

	req := newPostRequest("/v0/automation/nonexistent/disable", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}
