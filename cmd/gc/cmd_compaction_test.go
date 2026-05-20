package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
)

// seedTestSessionBead inserts a session bead with the metadata fields
// resolveCompactionContext / resolveSessionID need to round-trip through.
//
//nolint:unparam // helper keeps the explicit sessionName at call sites for parity with resolveSessionID/resolveCompactionContext, even though every current caller passes "mayor".
func seedTestSessionBead(t *testing.T, store beads.Store, sessionName, template string) beads.Bead {
	t.Helper()
	b, err := store.Create(beads.Bead{
		Type:   sessionBeadType,
		Labels: []string{"gc:session"},
	})
	if err != nil {
		t.Fatalf("seeding session bead: %v", err)
	}
	if err := store.SetMetadata(b.ID, "session_name", sessionName); err != nil {
		t.Fatalf("set session_name: %v", err)
	}
	if err := store.SetMetadata(b.ID, "template", template); err != nil {
		t.Fatalf("set template: %v", err)
	}
	got, err := store.Get(b.ID)
	if err != nil {
		t.Fatalf("rehydrating session bead: %v", err)
	}
	return got
}

func newTestCompactionConfig(threshold int, policy, message string) *config.City {
	return &config.City{
		Agents: []config.Agent{{
			Name: "mayor",
			Compaction: config.Compaction{
				ThresholdTurns: threshold,
				Policy:         policy,
				Message:        message,
			},
		}},
	}
}

func TestCompactionTick_DisabledAgentIsNoOp(t *testing.T) {
	store := beads.NewMemStore()
	cfg := newTestCompactionConfig(0, "", "")
	rec := events.NewFake()
	dops := newFakeDrainOps()
	seedTestSessionBead(t, store, "mayor", "mayor")

	var stdout, stderr bytes.Buffer
	code := doCompactionTick(store, cfg, rec, dops, nil,
		sessionRuntimeTarget{cityPath: t.TempDir(), display: "mayor", sessionName: "mayor"},
		&stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr=%q", code, stderr.String())
	}
	all, _ := store.ListOpen()
	for _, b := range all {
		if got := b.Metadata[compactionTurnsMetaKey]; got != "" {
			t.Errorf("disabled agent should not touch counter; got %q on bead %s", got, b.ID)
		}
	}
	if len(rec.Events) != 0 {
		t.Errorf("disabled agent should record no events; got %+v", rec.Events)
	}
	if dops.restartRequested["mayor"] {
		t.Error("disabled agent should not request restart")
	}
}

func TestCompactionTick_BelowThresholdJustBumps(t *testing.T) {
	store := beads.NewMemStore()
	cfg := newTestCompactionConfig(5, "handoff", "cycle $GC_AGENT")
	rec := events.NewFake()
	dops := newFakeDrainOps()
	bead := seedTestSessionBead(t, store, "mayor", "mayor")

	var stdout, stderr bytes.Buffer
	code := doCompactionTick(store, cfg, rec, dops, nil,
		sessionRuntimeTarget{cityPath: t.TempDir(), display: "mayor", sessionName: "mayor"},
		&stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr=%q", code, stderr.String())
	}
	got, err := store.Get(bead.ID)
	if err != nil {
		t.Fatalf("rehydrating: %v", err)
	}
	if got.Metadata[compactionTurnsMetaKey] != "1" {
		t.Errorf("counter = %q, want %q", got.Metadata[compactionTurnsMetaKey], "1")
	}
	if got.Metadata[compactionLastFiredAtMetaKey] != "" {
		t.Errorf("last_fired_at should be empty below threshold; got %q", got.Metadata[compactionLastFiredAtMetaKey])
	}
	if len(rec.Events) != 0 {
		t.Errorf("no events expected below threshold; got %+v", rec.Events)
	}
	if dops.restartRequested["mayor"] {
		t.Error("no restart expected below threshold")
	}
}

func TestCompactionTick_ThresholdFiresHandoffAndResets(t *testing.T) {
	store := beads.NewMemStore()
	cfg := newTestCompactionConfig(3, "handoff", "Cycle $GC_AGENT at $TURNS turns; resume the convoy.")
	rec := events.NewFake()
	dops := newFakeDrainOps()
	bead := seedTestSessionBead(t, store, "mayor", "mayor")
	// Pre-seed the counter at threshold-1 so the next tick trips.
	if err := store.SetMetadata(bead.ID, compactionTurnsMetaKey, "2"); err != nil {
		t.Fatalf("seeding counter: %v", err)
	}
	persistCalled := false
	persist := func() error {
		persistCalled = true
		return nil
	}

	var stdout, stderr bytes.Buffer
	code := doCompactionTick(store, cfg, rec, dops, persist,
		sessionRuntimeTarget{cityPath: t.TempDir(), display: "mayor", sessionName: "mayor"},
		&stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr=%q", code, stderr.String())
	}

	// Counter reset.
	got, err := store.Get(bead.ID)
	if err != nil {
		t.Fatalf("rehydrating: %v", err)
	}
	if got.Metadata[compactionTurnsMetaKey] != "0" {
		t.Errorf("counter = %q, want %q after firing", got.Metadata[compactionTurnsMetaKey], "0")
	}
	if got.Metadata[compactionLastFiredAtMetaKey] == "" {
		t.Error("compaction_last_fired_at not stamped after firing")
	}

	// Handoff mail created with rendered body.
	all, _ := store.ListOpen()
	var mail beads.Bead
	for _, b := range all {
		if b.Type == "message" && strings.HasPrefix(b.Title, "Compaction handoff:") {
			mail = b
			break
		}
	}
	if mail.ID == "" {
		t.Fatalf("no compaction-handoff mail created; beads=%+v", all)
	}
	if !strings.Contains(mail.Description, "Cycle mayor at 3 turns") {
		t.Errorf("mail body = %q, want rendered $GC_AGENT and $TURNS", mail.Description)
	}

	if !dops.restartRequested["mayor"] {
		t.Error("expected restart-requested flag to be set on handoff")
	}
	if !persistCalled {
		t.Error("expected persistRestart to be called")
	}
	if !strings.Contains(stdout.String(), "restart requested") {
		t.Errorf("stdout = %q, want restart confirmation", stdout.String())
	}
}

func TestCompactionTick_WarnPolicyWritesStderrAndResets(t *testing.T) {
	store := beads.NewMemStore()
	cfg := newTestCompactionConfig(2, "warn", "Budget alert: $GC_AGENT at $TURNS turns")
	rec := events.NewFake()
	dops := newFakeDrainOps()
	bead := seedTestSessionBead(t, store, "mayor", "mayor")
	if err := store.SetMetadata(bead.ID, compactionTurnsMetaKey, "1"); err != nil {
		t.Fatalf("seeding counter: %v", err)
	}

	var stdout, stderr bytes.Buffer
	code := doCompactionTick(store, cfg, rec, dops, nil,
		sessionRuntimeTarget{cityPath: t.TempDir(), display: "mayor", sessionName: "mayor"},
		&stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Budget alert: mayor at 2 turns") {
		t.Errorf("stderr = %q, want rendered warn message", stderr.String())
	}
	if len(rec.Events) != 0 {
		t.Errorf("warn must not send mail; events=%+v", rec.Events)
	}
	if dops.restartRequested["mayor"] {
		t.Error("warn must not request restart")
	}
	got, _ := store.Get(bead.ID)
	if got.Metadata[compactionTurnsMetaKey] != "0" {
		t.Errorf("counter = %q, want 0 after warn", got.Metadata[compactionTurnsMetaKey])
	}
}

func TestCompactionTick_ResetPolicyJustClearsCounter(t *testing.T) {
	store := beads.NewMemStore()
	cfg := newTestCompactionConfig(2, "reset", "irrelevant")
	rec := events.NewFake()
	dops := newFakeDrainOps()
	bead := seedTestSessionBead(t, store, "mayor", "mayor")
	if err := store.SetMetadata(bead.ID, compactionTurnsMetaKey, "1"); err != nil {
		t.Fatalf("seeding counter: %v", err)
	}

	var stdout, stderr bytes.Buffer
	code := doCompactionTick(store, cfg, rec, dops, nil,
		sessionRuntimeTarget{cityPath: t.TempDir(), display: "mayor", sessionName: "mayor"},
		&stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr=%q", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Errorf("reset policy must be silent; stderr=%q", stderr.String())
	}
	if len(rec.Events) != 0 {
		t.Errorf("reset policy must not record events; events=%+v", rec.Events)
	}
	got, _ := store.Get(bead.ID)
	if got.Metadata[compactionTurnsMetaKey] != "0" {
		t.Errorf("counter = %q, want 0 after reset", got.Metadata[compactionTurnsMetaKey])
	}
}

func TestCompactionTick_UnknownTemplateIsNoOp(t *testing.T) {
	store := beads.NewMemStore()
	cfg := newTestCompactionConfig(2, "handoff", "msg")
	rec := events.NewFake()
	dops := newFakeDrainOps()
	// template "ghost" doesn't match any configured agent.
	seedTestSessionBead(t, store, "mayor", "ghost")

	var stdout, stderr bytes.Buffer
	code := doCompactionTick(store, cfg, rec, dops, nil,
		sessionRuntimeTarget{cityPath: t.TempDir(), display: "mayor", sessionName: "mayor"},
		&stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr=%q", code, stderr.String())
	}
	if len(rec.Events) != 0 {
		t.Error("unknown template should not fire policy")
	}
}

func TestRenderCompactionMessage_DefaultOnEmpty(t *testing.T) {
	got := renderCompactionMessage("", "mayor", "mayor-1", 7, 0)
	if !strings.Contains(got, "Auto handoff") {
		t.Errorf("default message missing 'Auto handoff' opener: %q", got)
	}
	if !strings.Contains(got, "mayor") {
		t.Errorf("default message should expand $GC_AGENT: %q", got)
	}
	if !strings.Contains(got, "7 turns") {
		t.Errorf("default message should expand $TURNS: %q", got)
	}
}

func TestRenderCompactionMessage_UnknownPlaceholderPassesThrough(t *testing.T) {
	// $UNKNOWN_VAR is not in the env map; the renderer should leave the
	// literal "$UNKNOWN_VAR" in place rather than dropping it. This lets
	// operators include literal "$" characters without escaping every dollar.
	got := renderCompactionMessage("see $UNKNOWN_VAR and $GC_AGENT", "mayor", "mayor", 1, 0)
	if !strings.Contains(got, "$UNKNOWN_VAR") {
		t.Errorf("unknown var should pass through; got %q", got)
	}
	if !strings.Contains(got, "mayor") {
		t.Errorf("known var should still expand; got %q", got)
	}
}

func TestApplyAgentCompactionMessageToAutoArgs(t *testing.T) {
	store := beads.NewMemStore()
	cfg := newTestCompactionConfig(5, "handoff", "Resume for $GC_AGENT after $TURNS turns")
	bead := seedTestSessionBead(t, store, "mayor", "mayor")
	if err := store.SetMetadata(bead.ID, compactionTurnsMetaKey, "4"); err != nil {
		t.Fatalf("seeding counter: %v", err)
	}
	current := sessionRuntimeTarget{cityPath: t.TempDir(), display: "mayor", sessionName: "mayor"}
	got := applyAgentCompactionMessageToAutoArgs(store, cfg, current, nil)
	if len(got) != 2 {
		t.Fatalf("expected [subject, body], got %v", got)
	}
	if !strings.HasPrefix(got[0], "Compaction handoff: ") {
		t.Errorf("subject = %q, want compaction-prefixed", got[0])
	}
	if !strings.Contains(got[1], "Resume for mayor after 4 turns") {
		t.Errorf("body = %q, want rendered message", got[1])
	}
}

func TestApplyAgentCompactionMessageToAutoArgs_NoConfigPassthrough(t *testing.T) {
	store := beads.NewMemStore()
	cfg := newTestCompactionConfig(0, "", "")
	seedTestSessionBead(t, store, "mayor", "mayor")
	current := sessionRuntimeTarget{cityPath: t.TempDir(), display: "mayor", sessionName: "mayor"}

	// args passed in untouched when no Message is configured.
	in := []string{"context cycle"}
	got := applyAgentCompactionMessageToAutoArgs(store, cfg, current, in)
	if len(got) != 1 || got[0] != "context cycle" {
		t.Errorf("expected passthrough, got %v", got)
	}
}

// Ensure the helper returns args unchanged when the agent has a non-empty
// Message but the store/cfg lookup fails (defense-in-depth: a corrupt city
// store on PreCompact should not crash the hook).
func TestApplyAgentCompactionMessageToAutoArgs_NilGuards(t *testing.T) {
	current := sessionRuntimeTarget{cityPath: "", display: "mayor", sessionName: "mayor"}
	if got := applyAgentCompactionMessageToAutoArgs(nil, nil, current, []string{"ctx"}); !errors.Is(nil, nil) || len(got) != 1 {
		t.Errorf("nil store: got %v, want passthrough", got)
	}
}
