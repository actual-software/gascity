package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

func TestAutoSuspendChatSessions(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	mgr := session.NewManager(store, sp)

	// Create two sessions.
	s1, err := mgr.Create(context.Background(), "default", "S1", "echo s1", "/tmp", "test", nil, session.ProviderResume{}, runtime.Config{})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := mgr.Create(context.Background(), "default", "S2", "echo s2", "/tmp", "test", nil, session.ProviderResume{}, runtime.Config{})
	if err != nil {
		t.Fatal(err)
	}

	// Set activity times: s1 was active 2 hours ago, s2 was active 1 minute ago.
	sp.SetActivity(s1.SessionName, time.Now().Add(-2*time.Hour))
	sp.SetActivity(s2.SessionName, time.Now().Add(-1*time.Minute))

	// Neither is attached.
	sp.SetAttached(s1.SessionName, false)
	sp.SetAttached(s2.SessionName, false)

	var stdout, stderr bytes.Buffer
	autoSuspendChatSessions(store, sp, 30*time.Minute, &stdout, &stderr)

	// s1 should be suspended (idle 2h > 30m timeout).
	got1, err := mgr.Get(s1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got1.State != session.StateSuspended {
		t.Errorf("s1 state = %q, want suspended", got1.State)
	}

	// s2 should still be active (idle 1m < 30m timeout).
	got2, err := mgr.Get(s2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got2.State != session.StateActive {
		t.Errorf("s2 state = %q, want active", got2.State)
	}

	// Verify stdout mentions the suspended session.
	if !strings.Contains(stdout.String(), s1.ID) {
		t.Errorf("stdout should mention suspended session ID %s, got: %s", s1.ID, stdout.String())
	}
	if stderr.Len() != 0 {
		t.Errorf("unexpected stderr: %s", stderr.String())
	}
}

func TestAutoSuspendSkipsAttachedSessions(t *testing.T) {
	store := beads.NewMemStore()
	sp := runtime.NewFake()
	mgr := session.NewManager(store, sp)

	s1, err := mgr.Create(context.Background(), "default", "Attached", "echo a", "/tmp", "test", nil, session.ProviderResume{}, runtime.Config{})
	if err != nil {
		t.Fatal(err)
	}

	// Old activity but attached — should NOT be suspended.
	sp.SetActivity(s1.SessionName, time.Now().Add(-2*time.Hour))
	sp.SetAttached(s1.SessionName, true)

	var stdout, stderr bytes.Buffer
	autoSuspendChatSessions(store, sp, 30*time.Minute, &stdout, &stderr)

	got, err := mgr.Get(s1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != session.StateActive {
		t.Errorf("attached session state = %q, want active", got.State)
	}
}

func TestAutoSuspendNilStore(t *testing.T) {
	t.Helper() // uses t for test name
	sp := runtime.NewFake()
	var stdout, stderr bytes.Buffer
	// Should not panic with nil store.
	autoSuspendChatSessions(nil, sp, 30*time.Minute, &stdout, &stderr)
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Errorf("unexpected output with nil store: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}
