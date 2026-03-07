package auto

import (
	"context"
	"testing"

	"github.com/gastownhall/gascity/internal/runtime"
)

var _ runtime.Provider = (*Provider)(nil)

func TestRouteDefaultAndACP(t *testing.T) {
	defaultSP := runtime.NewFake()
	acpSP := runtime.NewFake()
	p := New(defaultSP, acpSP)

	// Unregistered session routes to default.
	if got := p.route("agent-a"); got != defaultSP {
		t.Fatal("unregistered session should route to default")
	}

	// Register as ACP.
	p.RouteACP("agent-b")
	if got := p.route("agent-b"); got != acpSP {
		t.Fatal("registered session should route to ACP")
	}
	if got := p.route("agent-a"); got != defaultSP {
		t.Fatal("other session should still route to default")
	}
}

func TestUnroute(t *testing.T) {
	defaultSP := runtime.NewFake()
	acpSP := runtime.NewFake()
	p := New(defaultSP, acpSP)

	p.RouteACP("agent-x")
	if got := p.route("agent-x"); got != acpSP {
		t.Fatal("should route to ACP after registration")
	}

	p.Unroute("agent-x")
	if got := p.route("agent-x"); got != defaultSP {
		t.Fatal("should route to default after unroute")
	}
}

func TestAttachReturnsErrorForACP(t *testing.T) {
	defaultSP := runtime.NewFake()
	acpSP := runtime.NewFake()
	p := New(defaultSP, acpSP)

	p.RouteACP("headless-agent")
	err := p.Attach("headless-agent")
	if err == nil {
		t.Fatal("Attach on ACP session should return error")
	}
	if want := `agent "headless-agent" uses ACP transport (no terminal to attach to)`; err.Error() != want {
		t.Errorf("Attach error = %q, want %q", err.Error(), want)
	}

	// Default sessions with an existing session should not error.
	_ = defaultSP.Start(context.Background(), "normal-agent", runtime.Config{})
	if err := p.Attach("normal-agent"); err != nil {
		t.Errorf("Attach on default session should not error: %v", err)
	}
}

func TestListRunningMergesBothBackends(t *testing.T) {
	defaultSP := runtime.NewFake()
	acpSP := runtime.NewFake()
	p := New(defaultSP, acpSP)

	// Start sessions on each backend.
	_ = defaultSP.Start(context.Background(), "default-1", runtime.Config{})
	_ = acpSP.Start(context.Background(), "acp-1", runtime.Config{})

	names, err := p.ListRunning("")
	if err != nil {
		t.Fatalf("ListRunning: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("ListRunning returned %d names, want 2: %v", len(names), names)
	}
	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	if !found["default-1"] || !found["acp-1"] {
		t.Errorf("ListRunning = %v, want default-1 and acp-1", names)
	}
}

func TestStopPreservesRouteOnBothFail(t *testing.T) {
	defaultSP := runtime.NewFailFake() // both backends fail
	acpSP := runtime.NewFailFake()
	p := New(defaultSP, acpSP)

	p.RouteACP("agent-fail")
	err := p.Stop("agent-fail")
	if err == nil {
		t.Fatal("Stop should return error when both backends fail")
	}

	// Route should be preserved since Stop failed on both.
	if got := p.route("agent-fail"); got != acpSP {
		t.Fatal("route should be preserved when Stop fails on both backends")
	}
}

func TestListRunningPartialError(t *testing.T) {
	defaultSP := runtime.NewFake()
	acpSP := runtime.NewFailFake() // ListRunning returns error
	p := New(defaultSP, acpSP)

	_ = defaultSP.Start(context.Background(), "default-1", runtime.Config{})

	names, err := p.ListRunning("")
	if err == nil {
		t.Fatal("ListRunning should return error when one backend fails")
	}
	// Should still return partial results from the working backend.
	if len(names) != 1 || names[0] != "default-1" {
		t.Errorf("ListRunning partial = %v, want [default-1]", names)
	}
}

func TestListRunningBothFail(t *testing.T) {
	defaultSP := runtime.NewFailFake()
	acpSP := runtime.NewFailFake()
	p := New(defaultSP, acpSP)

	names, err := p.ListRunning("")
	if err == nil {
		t.Fatal("ListRunning should return error when both backends fail")
	}
	if names != nil {
		t.Errorf("ListRunning both fail = %v, want nil", names)
	}
}

func TestIsRunningFallsThrough(t *testing.T) {
	defaultSP := runtime.NewFake()
	acpSP := runtime.NewFake()
	p := New(defaultSP, acpSP)

	// Start on default backend but register route as ACP (simulates stale route).
	_ = defaultSP.Start(context.Background(), "stale-agent", runtime.Config{})
	p.RouteACP("stale-agent")

	// ACP says not running → should fall through to default → true.
	if !p.IsRunning("stale-agent") {
		t.Fatal("IsRunning should fall through to default when ACP reports not running")
	}

	// Reverse: start on ACP, don't register route (simulates lost route).
	_ = acpSP.Start(context.Background(), "lost-route", runtime.Config{})
	if !p.IsRunning("lost-route") {
		t.Fatal("IsRunning should fall through to ACP when default reports not running")
	}
}

func TestStopFallsThrough(t *testing.T) {
	defaultSP := runtime.NewFailFake() // Stop always fails (simulates "not found")
	acpSP := runtime.NewFake()
	p := New(defaultSP, acpSP)

	// Start on ACP but don't register route (simulates lost route after restart).
	_ = acpSP.Start(context.Background(), "orphan", runtime.Config{})

	// Stop routes to default (no route entry), which fails → falls through to ACP.
	if err := p.Stop("orphan"); err != nil {
		t.Fatalf("Stop should fall through to ACP backend: %v", err)
	}
	if acpSP.IsRunning("orphan") {
		t.Fatal("session should be stopped on ACP backend after fallthrough")
	}
}

func TestStopCleansUpRoute(t *testing.T) {
	defaultSP := runtime.NewFake()
	acpSP := runtime.NewFake()
	p := New(defaultSP, acpSP)

	p.RouteACP("agent-z")
	_ = acpSP.Start(context.Background(), "agent-z", runtime.Config{})

	if err := p.Stop("agent-z"); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// After stop, route entry should be cleaned up.
	if got := p.route("agent-z"); got != defaultSP {
		t.Fatal("route should fall back to default after Stop")
	}
}
