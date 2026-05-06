package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
)

// dispatchReadyRoutedNudges queues a nudge for each ready, routed work bead
// whose target session is alive but unaware of the routing. This addresses
// the autonomy gap where a graph.v2 step bead becomes unblocked, gets routed
// to a live session via gc.routed_to, but never triggers a turn — because
// the agent's TUI is at the prompt with no event source to wake it.
//
// Background: gc has three explicit nudge entry points (gc sling --nudge,
// gc session nudge, gc mail send) plus a wait-satisfaction nudge sweep in
// dispatchReadyWaitNudges. None of them fire when a step in an existing
// formula transitions from blocked to ready and is auto-routed by the graph.
// Bug gastownhall/gascity#1543 documents the visible symptom: queued nudges
// reach the pane but the agent never starts a turn. The narrower issue here
// is the missing nudge-enqueue side: nothing puts work into the queue when
// step routing happens server-side.
//
// Idempotent: a deterministic nudge ID per (bead-id, session-id) prevents
// duplicates across reconciler ticks. Skips beads whose target session
// cannot currently receive a nudge (asleep, draining, etc.).
//
// Errors are returned to the caller for logging but do not abort the
// reconciler tick — a failure on one rig should not stall others.
func dispatchReadyRoutedNudges(cityPath string, rigStores map[string]beads.Store, sessionBeads *sessionBeadSnapshot, now time.Time) error {
	if cityPath == "" || sessionBeads == nil || len(rigStores) == 0 {
		return nil
	}
	cityStore := openNudgeBeadStore(cityPath)
	if cityStore == nil {
		return fmt.Errorf("opening city nudge store at %q", cityPath)
	}
	var firstErr error
	for rigName, rigStore := range rigStores {
		if err := dispatchRigReadyRoutedNudges(cityPath, cityStore, rigName, rigStore, sessionBeads, now); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// dispatchRigReadyRoutedNudges scans one rig's bead store for ready-and-routed
// work beads and queues a nudge for each one whose target session is live.
func dispatchRigReadyRoutedNudges(cityPath string, cityStore beads.Store, rigName string, rigStore beads.Store, sessionBeads *sessionBeadSnapshot, now time.Time) error {
	if rigStore == nil {
		return nil
	}
	const readyScanLimit = 200
	ready, err := rigStore.Ready(beads.ReadyQuery{Limit: readyScanLimit})
	if err != nil {
		return fmt.Errorf("rig %q: querying ready work: %w", rigName, err)
	}
	for _, b := range ready {
		routedTo := strings.TrimSpace(b.Metadata["gc.routed_to"])
		if routedTo == "" {
			continue
		}
		// Only step beads carry gc.step_ref; skip raw work beads to avoid
		// stepping on the existing assignee-driven dispatch path.
		if strings.TrimSpace(b.Metadata["gc.step_ref"]) == "" {
			continue
		}
		sessionBead, ok := findSessionBeadByTemplate(sessionBeads, routedTo)
		if !ok {
			// No live session yet — fresh-session creation will deliver
			// the agent's startup nudge with Enter, so we don't need to.
			continue
		}
		if !cachedSessionCanReceiveWaitNudge(sessionBead) {
			continue
		}
		nudgeID := routedNudgeID(b.ID, sessionBead.ID)
		if _, found, err := findQueuedNudgeBead(cityStore, nudgeID); err == nil && found {
			continue
		}
		msg := fmt.Sprintf("Step %s ready (%s). Run gc hook to claim and process.", b.ID, b.Metadata["gc.step_ref"])
		item := newQueuedNudgeWithOptions(routedTo, msg, "routed", now, queuedNudgeOptions{
			ID:                nudgeID,
			SessionID:         sessionBead.ID,
			ContinuationEpoch: sessionBead.Metadata["continuation_epoch"],
			Reference:         &nudgeReference{Kind: "bead", ID: b.ID},
		})
		if err := enqueueQueuedNudgeWithStore(cityPath, cityStore, item); err != nil {
			return fmt.Errorf("rig %q: enqueuing routed nudge for %s: %w", rigName, b.ID, err)
		}
		sessionName := strings.TrimSpace(sessionBead.Metadata["session_name"])
		if sessionName == "" {
			continue
		}
		if err := startNudgePoller(cityPath, routedTo, sessionName); err != nil {
			return fmt.Errorf("rig %q: starting routed nudge poller for %s: %w", rigName, b.ID, err)
		}
	}
	return nil
}

// findSessionBeadByTemplate returns the open session bead whose template
// metadata matches the routed-to target, if one exists.
func findSessionBeadByTemplate(sessionBeads *sessionBeadSnapshot, template string) (beads.Bead, bool) {
	if sessionBeads == nil || strings.TrimSpace(template) == "" {
		return beads.Bead{}, false
	}
	for _, b := range sessionBeads.Open() {
		if strings.TrimSpace(b.Metadata["template"]) == template {
			return b, true
		}
	}
	return beads.Bead{}, false
}

// routedNudgeID builds a deterministic nudge ID for the (work-bead,
// session-bead) pair so reconciler retries do not enqueue duplicates.
func routedNudgeID(workBeadID, sessionBeadID string) string {
	return fmt.Sprintf("routed-%s-%s", workBeadID, sessionBeadID)
}
