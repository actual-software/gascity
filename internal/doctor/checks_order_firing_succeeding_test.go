package doctor

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/citylayout"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/orders"
)

func orderFiringTestEventPath(cityPath string) string {
	return filepath.Join(cityPath, citylayout.RuntimeRoot, "events.jsonl")
}

// A green "last fired" says nothing about whether the scheduled work actually
// happened. An order firing exactly on schedule and failing every time is the
// case this check exists to separate from a healthy one.
func TestOrderFiringCurrent_FiringButNeverSucceeding(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	cityPath, cfg := orderFiringTestCity(t)
	writeOrderFiringTestOrder(t, cityPath, "branch-protection", "cron", "0 10 * * *")
	writeOrderFiringTestEvents(t, cityPath,
		events.Event{Type: events.ControllerStarted, Ts: now.Add(-72 * time.Hour)},
		events.Event{Type: events.OrderFired, Subject: "branch-protection", Ts: now.Add(-2 * time.Hour)},
		events.Event{Type: events.OrderFailed, Subject: "branch-protection", Ts: now.Add(-2 * time.Hour)},
	)

	result := runOrderFiringCurrentTest(t, cfg, cityPath, now)
	if result.Status != StatusError {
		t.Fatalf("status = %v, want error; msg = %s; details = %v", result.Status, result.Message, result.Details)
	}
	if result.Message != "scheduled orders are firing but not succeeding" {
		t.Fatalf("Message = %q, want the firing-but-not-succeeding summary", result.Message)
	}
	details := strings.Join(result.Details, "\n")
	if !strings.Contains(details, "has never succeeded") {
		t.Fatalf("details = %v, want a never-succeeded entry", result.Details)
	}
	// The order IS firing on schedule, so the firing entry must stay green.
	// Losing that distinction is the defect, not the fix.
	if !strings.Contains(details, "last fired 2h ago") {
		t.Fatalf("details = %v, want the firing entry to still report a recent fire", result.Details)
	}
	if result.Severity != SeverityAdvisory {
		t.Fatalf("Severity = %v, want SeverityAdvisory: a city whose orders have been failing for weeks must not be red-gated the moment this check ships", result.Severity)
	}
}

func TestOrderFiringCurrent_SucceedingOrderReportsBothSignals(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	cityPath, cfg := orderFiringTestCity(t)
	writeOrderFiringTestOrder(t, cityPath, "worktree-cleanup", "cooldown", "1h")
	writeOrderFiringTestEvents(t, cityPath,
		events.Event{Type: events.ControllerStarted, Ts: now.Add(-72 * time.Hour)},
		events.Event{Type: events.OrderFired, Subject: "worktree-cleanup", Ts: now.Add(-20 * time.Minute)},
		events.Event{Type: events.OrderCompleted, Subject: "worktree-cleanup", Ts: now.Add(-20 * time.Minute)},
	)

	result := runOrderFiringCurrentTest(t, cfg, cityPath, now)
	if result.Status != StatusOK {
		t.Fatalf("status = %v, want ok; msg = %s; details = %v", result.Status, result.Message, result.Details)
	}
	details := strings.Join(result.Details, "\n")
	if !strings.Contains(details, "last fired 20m ago") {
		t.Fatalf("details = %v, want a last-fired entry", result.Details)
	}
	if !strings.Contains(details, "last succeeded 20m ago") {
		t.Fatalf("details = %v, want a last-succeeded entry alongside last-fired", result.Details)
	}
}

// Negative control for the new signal. A city whose event log predates outcome
// recording has no order.completed or order.failed rows at all; the success
// state is unknown, not bad, and the check must stay silent about it rather
// than reporting every legacy order as never having succeeded.
func TestOrderFiringCurrent_NoOutcomeHistoryStaysSilent(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	cityPath, cfg := orderFiringTestCity(t)
	writeOrderFiringTestOrder(t, cityPath, "legacy-order", "cooldown", "1h")
	writeOrderFiringTestEvents(t, cityPath,
		events.Event{Type: events.ControllerStarted, Ts: now.Add(-72 * time.Hour)},
		events.Event{Type: events.OrderFired, Subject: "legacy-order", Ts: now.Add(-20 * time.Minute)},
	)

	result := runOrderFiringCurrentTest(t, cfg, cityPath, now)
	if result.Status != StatusOK {
		t.Fatalf("status = %v, want ok; msg = %s; details = %v", result.Status, result.Message, result.Details)
	}
	for _, detail := range result.Details {
		if strings.Contains(detail, "succeeded") {
			t.Fatalf("detail %q mentions success, but the order has no outcome history to judge", detail)
		}
	}
}

func TestClassifyOrderSucceeding(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	expected := time.Hour
	order := orders.Order{Name: "freshness-check", Trigger: "cooldown"}

	tests := []struct {
		name          string
		lastSucceeded time.Time
		lastFailed    time.Time
		wantStatus    CheckStatus
		wantDetail    string
	}{
		{
			name:       "no outcome history is unknown, not bad",
			wantStatus: StatusOK,
			wantDetail: "",
		},
		{
			name:       "failures with no success ever",
			lastFailed: now.Add(-30 * time.Minute),
			wantStatus: StatusError,
			wantDetail: "freshness-check: has never succeeded (last failed 30m ago)",
		},
		{
			name:          "recent success",
			lastSucceeded: now.Add(-10 * time.Minute),
			wantStatus:    StatusOK,
			wantDetail:    "freshness-check: last succeeded 10m ago",
		},
		{
			name:          "success overdue at 1.5x the interval",
			lastSucceeded: now.Add(-2 * time.Hour),
			lastFailed:    now.Add(-10 * time.Minute),
			wantStatus:    StatusWarning,
			wantDetail:    "freshness-check: last succeeded 2h ago, expected every 1h (overdue)",
		},
		{
			name:          "success critical at 3x the interval",
			lastSucceeded: now.Add(-5 * time.Hour),
			lastFailed:    now.Add(-10 * time.Minute),
			wantStatus:    StatusError,
			wantDetail:    "freshness-check: last succeeded 5h ago, expected every 1h (CRITICAL: firing but not succeeding)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, detail := classifyOrderSucceeding(order, now, expected, tc.lastSucceeded, tc.lastFailed)
			if status != tc.wantStatus {
				t.Fatalf("status = %v, want %v (detail %q)", status, tc.wantStatus, detail)
			}
			if detail != tc.wantDetail {
				t.Fatalf("detail = %q, want %q", detail, tc.wantDetail)
			}
		})
	}
}

func TestLatestOrderOutcomes(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	cityPath, _ := orderFiringTestCity(t)
	writeOrderFiringTestEvents(t, cityPath,
		events.Event{Type: events.OrderCompleted, Subject: "alpha", Ts: now.Add(-3 * time.Hour)},
		events.Event{Type: events.OrderCompleted, Subject: "alpha", Ts: now.Add(-1 * time.Hour)},
		events.Event{Type: events.OrderFailed, Subject: "beta", Ts: now.Add(-2 * time.Hour)},
		// A subject-less outcome cannot be attributed to an order and is skipped.
		events.Event{Type: events.OrderFailed, Ts: now},
	)

	completed, failed, err := latestOrderOutcomes(orderFiringTestEventPath(cityPath))
	if err != nil {
		t.Fatalf("latestOrderOutcomes: %v", err)
	}
	if got := completed["alpha"]; !got.Equal(now.Add(-1 * time.Hour)) {
		t.Fatalf("completed[alpha] = %v, want the newest of the two completions", got)
	}
	if _, ok := completed["beta"]; ok {
		t.Fatalf("completed[beta] present, want absent: beta only ever failed")
	}
	if got := failed["beta"]; !got.Equal(now.Add(-2 * time.Hour)) {
		t.Fatalf("failed[beta] = %v, want the recorded failure time", got)
	}
	if _, ok := failed[""]; ok {
		t.Fatalf("failed[\"\"] present, want the subject-less event skipped")
	}
}
