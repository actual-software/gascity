package main

import (
	"context"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/orders"
)

// messagesOfType returns the recorded messages for one event type, so a test
// can assert on what an order.failed event actually says rather than only that
// one was emitted.
func (r *memRecorder) messagesOfType(typ string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, e := range r.events {
		if e.Type == typ {
			out = append(out, e.Message)
		}
	}
	return out
}

// dispatchExecForDetailTest runs one exec order end to end through the real
// shell runner and returns its tracking bead plus the recorder. Using the real
// runner is deliberate: the exit-status plumbing under test only exists on a
// genuine *exec.ExitError.
func dispatchExecForDetailTest(t *testing.T, order orders.Order) (beads.Bead, *memRecorder) {
	t.Helper()
	store := beads.NewMemStore()
	label := "order-run:" + order.Name
	tracking, err := store.Create(beads.Bead{
		Title:  "order:" + order.Name,
		Labels: []string{label, labelOrderTracking},
	})
	if err != nil {
		t.Fatalf("creating tracking bead: %v", err)
	}

	var rec memRecorder
	ad := buildOrderDispatcherFromListExec([]orders.Order{order}, store, events.NewFake(), shellExecRunner, &rec)
	if ad == nil {
		t.Fatal("expected non-nil dispatcher")
	}
	mad := ad.(*memoryOrderDispatcher)

	captureCmdOrderLogs(t, func() {
		mad.dispatchExec(context.Background(), orders.NewStore(beads.OrdersStore{Store: store}),
			execStoreTarget{ScopeRoot: t.TempDir()}, mad.aa[0], t.TempDir(), tracking.ID, nil)
	})

	all := trackingBeads(t, store, label)
	if len(all) != 1 {
		t.Fatalf("tracking beads = %d, want 1", len(all))
	}
	return all[0], &rec
}

// The defect: a failed order recorded THAT it failed and discarded WHY, so
// diagnosing one meant catching the next failure live.
func TestOrderDispatchExecFailureRecordsDiagnosticDetail(t *testing.T) {
	bead, rec := dispatchExecForDetailTest(t, orders.Order{
		Name:     "preflight",
		Trigger:  "cooldown",
		Interval: "1h",
		Exec:     `echo "gc: unknown command \"doctor\"" >&2; exit 12`,
	})

	if !slicesContain(bead.Labels, "exec-failed") {
		t.Fatalf("tracking bead labels = %v, want exec-failed", bead.Labels)
	}
	if !strings.Contains(bead.Description, "unknown command") {
		t.Fatalf("tracking bead description = %q, want the command's stderr", bead.Description)
	}
	if !strings.Contains(bead.Description, "exit status: 12") {
		t.Fatalf("tracking bead description = %q, want the exit status", bead.Description)
	}
	if !strings.Contains(bead.Description, "exec: echo") {
		t.Fatalf("tracking bead description = %q, want the resolved exec string", bead.Description)
	}

	msgs := rec.messagesOfType(events.OrderFailed)
	if len(msgs) != 1 {
		t.Fatalf("order.failed messages = %d, want 1", len(msgs))
	}
	// The short reason stays on the first line; readers and greps have always
	// found it there.
	if first, _, _ := strings.Cut(msgs[0], "\n"); first != "exit status 12" {
		t.Fatalf("order.failed first line = %q, want the bare exit status", first)
	}
	if !strings.Contains(msgs[0], "unknown command") {
		t.Fatalf("order.failed message = %q, want the output tail appended", msgs[0])
	}
}

// branch_protection.py's documented contract reserves exit 1 for "drift
// detected, already reported". Without success_exit_codes gc logs that as a
// failure, and an order with a 100% failure rate becomes indistinguishable
// from a healthy one.
func TestOrderDispatchExecDeclaredInformationalExitCompletes(t *testing.T) {
	bead, rec := dispatchExecForDetailTest(t, orders.Order{
		Name:             "branch-protection",
		Trigger:          "cron",
		Schedule:         "0 10 * * *",
		Exec:             `echo "drift detected on 2 repos"; exit 1`,
		SuccessExitCodes: []int{1},
	})

	if slicesContain(bead.Labels, "exec-failed") {
		t.Fatalf("tracking bead labels = %v, want no exec-failed for a declared informational exit", bead.Labels)
	}
	if !slicesContain(bead.Labels, "exec") {
		t.Fatalf("tracking bead labels = %v, want exec", bead.Labels)
	}
	if rec.hasType(events.OrderFailed) {
		t.Fatal("recorded order.failed for an exit code the order declares informational")
	}
	if !rec.hasType(events.OrderCompleted) {
		t.Fatal("missing order.completed for a declared informational exit")
	}
	// The findings are the whole point of the informational exit, so they must
	// survive on the run even though it is not a failure.
	if !strings.Contains(bead.Description, "drift detected on 2 repos") {
		t.Fatalf("tracking bead description = %q, want the command's findings", bead.Description)
	}
	if !strings.Contains(bead.Description, "exit status: 1") {
		t.Fatalf("tracking bead description = %q, want the informational exit status", bead.Description)
	}
}

// Negative control. An undeclared non-zero exit must still fail — otherwise the
// success_exit_codes plumbing would be swallowing real failures rather than
// classifying declared ones.
func TestOrderDispatchExecUndeclaredExitStillFails(t *testing.T) {
	bead, rec := dispatchExecForDetailTest(t, orders.Order{
		Name:             "branch-protection-strict",
		Trigger:          "cron",
		Schedule:         "0 10 * * *",
		Exec:             `echo "gh: not authenticated" >&2; exit 11`,
		SuccessExitCodes: []int{1},
	})

	if !slicesContain(bead.Labels, "exec-failed") {
		t.Fatalf("tracking bead labels = %v, want exec-failed for an undeclared exit code", bead.Labels)
	}
	if !rec.hasType(events.OrderFailed) {
		t.Fatal("missing order.failed for an undeclared exit code")
	}
	if !strings.Contains(bead.Description, "not authenticated") {
		t.Fatalf("tracking bead description = %q, want the command's stderr", bead.Description)
	}
}

// A city carries tens of thousands of order-tracking beads. A routine success
// has nothing to diagnose, so it must not write a description on every run.
func TestOrderDispatchExecSuccessWritesNoDetail(t *testing.T) {
	bead, rec := dispatchExecForDetailTest(t, orders.Order{
		Name:     "worktree-cleanup",
		Trigger:  "cooldown",
		Interval: "1h",
		Exec:     `echo "nothing to clean"`,
	})

	if bead.Description != "" {
		t.Fatalf("tracking bead description = %q, want empty for a routine exit-0 run", bead.Description)
	}
	if !rec.hasType(events.OrderCompleted) {
		t.Fatal("missing order.completed for a successful run")
	}
	if rec.hasType(events.OrderFailed) {
		t.Fatal("recorded order.failed for a successful run")
	}
}
