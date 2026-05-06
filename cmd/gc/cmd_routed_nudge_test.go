package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
)

// TestDispatchReadyRoutedNudgesEnqueuesForLiveSession is the happy path: a
// step bead is ready and routed to a live session, no nudge has been queued
// for it yet, so the dispatcher enqueues one and starts the poller.
func TestDispatchReadyRoutedNudgesEnqueuesForLiveSession(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	cityPath := t.TempDir()
	prepareCityNudgeStore(t, cityPath)

	rigStore := beads.NewMemStore()
	created, err := rigStore.Create(beads.Bead{
		Type:   "task",
		Status: "open",
		Title:  "Write the feature plan",
		Metadata: map[string]string{
			"gc.routed_to": "fired-up-pizza/factory.planner",
			"gc.step_ref":  "mol-release-delivery.plan",
		},
	})
	if err != nil {
		t.Fatalf("Create step bead: %v", err)
	}
	stepBeadID := created.ID

	sessionBead := beads.Bead{
		Type:   sessionBeadType,
		Status: "open",
		Metadata: map[string]string{
			"template":     "fired-up-pizza/factory.planner",
			"session_name": "factory__planner-rdt-1fa",
			"state":        "awake",
		},
	}
	sessionCreated, err := rigStore.Create(sessionBead)
	if err != nil {
		t.Fatalf("Create session bead: %v", err)
	}
	sessionBead.ID = sessionCreated.ID
	snapshot := newSessionBeadSnapshot([]beads.Bead{sessionBead})

	prevStart := startNudgePoller
	pollerCalls := 0
	startNudgePoller = func(_, _, _ string) error {
		pollerCalls++
		return nil
	}
	t.Cleanup(func() { startNudgePoller = prevStart })

	rigStores := map[string]beads.Store{"fired-up-pizza": rigStore}
	if err := dispatchReadyRoutedNudges(cityPath, rigStores, snapshot, time.Now()); err != nil {
		t.Fatalf("dispatchReadyRoutedNudges: %v", err)
	}
	if pollerCalls != 1 {
		t.Errorf("startNudgePoller calls = %d, want 1", pollerCalls)
	}

	cityStore := openNudgeBeadStore(cityPath)
	if cityStore == nil {
		t.Fatal("openNudgeBeadStore returned nil")
	}
	nudgeID := routedNudgeID(stepBeadID, sessionBead.ID)
	if _, found, err := findQueuedNudgeBead(cityStore, nudgeID); err != nil {
		t.Fatalf("findQueuedNudgeBead: %v", err)
	} else if !found {
		t.Errorf("expected queued nudge with ID %q to exist after dispatch", nudgeID)
	}
}

// TestDispatchReadyRoutedNudgesIsIdempotent verifies that running the sweep
// twice does not enqueue duplicate nudges or restart the poller a second
// time for the same (bead, session) pair.
func TestDispatchReadyRoutedNudgesIsIdempotent(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	cityPath := t.TempDir()
	prepareCityNudgeStore(t, cityPath)

	rigStore := beads.NewMemStore()
	if _, err := rigStore.Create(beads.Bead{
		Type:   "task",
		Status: "open",
		Metadata: map[string]string{
			"gc.routed_to": "fired-up-pizza/factory.planner",
			"gc.step_ref":  "mol-release-delivery.plan",
		},
	}); err != nil {
		t.Fatalf("Create step bead: %v", err)
	}
	sessionCreated, err := rigStore.Create(beads.Bead{
		Type:   sessionBeadType,
		Status: "open",
		Metadata: map[string]string{
			"template":     "fired-up-pizza/factory.planner",
			"session_name": "factory__planner-rdt-1fa",
			"state":        "awake",
		},
	})
	if err != nil {
		t.Fatalf("Create session bead: %v", err)
	}
	sessionCreated.Metadata = map[string]string{
		"template":     "fired-up-pizza/factory.planner",
		"session_name": "factory__planner-rdt-1fa",
		"state":        "awake",
	}
	snapshot := newSessionBeadSnapshot([]beads.Bead{sessionCreated})

	prevStart := startNudgePoller
	pollerCalls := 0
	startNudgePoller = func(_, _, _ string) error {
		pollerCalls++
		return nil
	}
	t.Cleanup(func() { startNudgePoller = prevStart })

	rigStores := map[string]beads.Store{"fired-up-pizza": rigStore}
	now := time.Now()
	for i := 0; i < 3; i++ {
		if err := dispatchReadyRoutedNudges(cityPath, rigStores, snapshot, now); err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
	}

	if pollerCalls != 1 {
		t.Errorf("startNudgePoller calls across 3 sweeps = %d, want 1 (deduped by nudge ID)", pollerCalls)
	}
}

// TestDispatchReadyRoutedNudgesSkipsBeadsWithoutLiveSession verifies the
// sweep is a no-op when the routed agent has no active session bead — that
// path is handled by fresh-session creation, not by the routed-nudge sweep.
func TestDispatchReadyRoutedNudgesSkipsBeadsWithoutLiveSession(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	cityPath := t.TempDir()
	prepareCityNudgeStore(t, cityPath)

	rigStore := beads.NewMemStore()
	if _, err := rigStore.Create(beads.Bead{
		Type:   "task",
		Status: "open",
		Metadata: map[string]string{
			"gc.routed_to": "fired-up-pizza/factory.planner",
			"gc.step_ref":  "mol-release-delivery.plan",
		},
	}); err != nil {
		t.Fatalf("Create step bead: %v", err)
	}

	// No matching session bead.
	snapshot := newSessionBeadSnapshot(nil)

	prevStart := startNudgePoller
	pollerCalls := 0
	startNudgePoller = func(_, _, _ string) error {
		pollerCalls++
		return nil
	}
	t.Cleanup(func() { startNudgePoller = prevStart })

	rigStores := map[string]beads.Store{"fired-up-pizza": rigStore}
	if err := dispatchReadyRoutedNudges(cityPath, rigStores, snapshot, time.Now()); err != nil {
		t.Fatalf("dispatchReadyRoutedNudges: %v", err)
	}
	if pollerCalls != 0 {
		t.Errorf("startNudgePoller called %d times for missing session, want 0", pollerCalls)
	}
}

// TestDispatchReadyRoutedNudgesSkipsRawWorkBeads verifies that beads without
// gc.step_ref (i.e. raw work beads, not graph.v2 steps) are left alone — the
// existing assignee-driven dispatch path owns those.
func TestDispatchReadyRoutedNudgesSkipsRawWorkBeads(t *testing.T) {
	t.Setenv("GC_BEADS", "file")
	cityPath := t.TempDir()
	prepareCityNudgeStore(t, cityPath)

	rigStore := beads.NewMemStore()
	if _, err := rigStore.Create(beads.Bead{
		Type:   "task",
		Status: "open",
		Metadata: map[string]string{
			"gc.routed_to": "fired-up-pizza/factory.planner",
			// no gc.step_ref
		},
	}); err != nil {
		t.Fatalf("Create raw bead: %v", err)
	}
	sessionCreated, err := rigStore.Create(beads.Bead{
		Type:   sessionBeadType,
		Status: "open",
		Metadata: map[string]string{
			"template":     "fired-up-pizza/factory.planner",
			"session_name": "factory__planner-rdt-1fa",
			"state":        "awake",
		},
	})
	if err != nil {
		t.Fatalf("Create session: %v", err)
	}
	sessionCreated.Metadata = map[string]string{
		"template":     "fired-up-pizza/factory.planner",
		"session_name": "factory__planner-rdt-1fa",
		"state":        "awake",
	}
	snapshot := newSessionBeadSnapshot([]beads.Bead{sessionCreated})

	prevStart := startNudgePoller
	pollerCalls := 0
	startNudgePoller = func(_, _, _ string) error {
		pollerCalls++
		return nil
	}
	t.Cleanup(func() { startNudgePoller = prevStart })

	rigStores := map[string]beads.Store{"fired-up-pizza": rigStore}
	if err := dispatchReadyRoutedNudges(cityPath, rigStores, snapshot, time.Now()); err != nil {
		t.Fatalf("dispatchReadyRoutedNudges: %v", err)
	}
	if pollerCalls != 0 {
		t.Errorf("startNudgePoller called %d times for raw bead, want 0", pollerCalls)
	}
}

// prepareCityNudgeStore initializes a minimal city HQ bead store at the
// given path so openNudgeBeadStore returns a usable store inside tests.
func prepareCityNudgeStore(t *testing.T, cityPath string) {
	t.Helper()
	if !filepath.IsAbs(cityPath) {
		t.Fatalf("cityPath must be absolute: %s", cityPath)
	}
	if store := openNudgeBeadStore(cityPath); store == nil {
		t.Skip("city HQ bead store unavailable in test environment; skipping")
	}
	if !strings.HasPrefix(routedNudgeID("a", "b"), "routed-") {
		t.Fatalf("routedNudgeID format invariant broken")
	}
}
