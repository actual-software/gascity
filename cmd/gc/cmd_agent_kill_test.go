package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/runtime"
)

// ---------------------------------------------------------------------------
// doAgentKill tests
// ---------------------------------------------------------------------------

func TestDoAgentKill(t *testing.T) {
	sp := runtime.NewFake()
	if err := sp.Start(context.Background(), "worker", runtime.Config{Command: "echo"}); err != nil {
		t.Fatal(err)
	}

	rec := events.NewFake()
	var stdout, stderr bytes.Buffer
	code := doAgentKill(sp, rec, "worker", "worker", &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr: %s", code, stderr.String())
	}
	// Session should be stopped.
	if sp.IsRunning("worker") {
		t.Error("session still running after kill")
	}
	// Event recorded.
	if len(rec.Events) != 1 || rec.Events[0].Type != events.AgentStopped {
		t.Errorf("events = %v, want one AgentStopped event", rec.Events)
	}
	if rec.Events[0].Subject != "worker" {
		t.Errorf("event subject = %q, want %q", rec.Events[0].Subject, "worker")
	}
	// stdout message.
	if got := stdout.String(); got != "Killed agent 'worker'\n" {
		t.Errorf("stdout = %q, want %q", got, "Killed agent 'worker'\n")
	}
}

func TestDoAgentKillNotRunning(t *testing.T) {
	sp := runtime.NewFake() // no sessions started

	var stdout, stderr bytes.Buffer
	code := doAgentKill(sp, events.Discard, "worker", "worker", &stdout, &stderr)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if got := stderr.String(); got != "gc agent kill: agent \"worker\" is not running\n" {
		t.Errorf("stderr = %q", got)
	}
}

func TestDoAgentKillStopError(t *testing.T) {
	sp := runtime.NewFailFake()
	// FailFake returns false for IsRunning, so we need a custom approach.
	// Use a regular fake and inject a stop error via a wrapper.
	sp2 := runtime.NewFake()
	if err := sp2.Start(context.Background(), "worker", runtime.Config{Command: "echo"}); err != nil {
		t.Fatal(err)
	}
	_ = sp // unused

	// Use a stopErrorProvider that wraps Fake but fails on Stop.
	wrapper := &stopErrorProvider{Fake: sp2, stopErr: errors.New("tmux borked")}

	var stdout, stderr bytes.Buffer
	code := doAgentKill(wrapper, events.Discard, "worker", "worker", &stdout, &stderr)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if got := stderr.String(); got != "gc agent kill: tmux borked\n" {
		t.Errorf("stderr = %q", got)
	}
}

// stopErrorProvider wraps runtime.Fake but returns an error on Stop.
type stopErrorProvider struct {
	*runtime.Fake
	stopErr error
}

func (s *stopErrorProvider) Stop(_ string) error {
	return s.stopErr
}
