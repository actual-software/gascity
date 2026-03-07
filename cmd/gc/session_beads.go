package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
)

// sessionBeadLabel is the label applied to all session beads for config agents.
const sessionBeadLabel = "gc:agent_session"

// sessionBeadType is the bead type for config agent session beads.
const sessionBeadType = "agent_session"

// syncSessionBeads ensures every config agent has a corresponding session bead.
// This is an additive side-effect — it creates beads for agents that don't have
// them and updates metadata for those that do. It does NOT change agent behavior;
// the existing reconciler continues to manage agent lifecycle.
//
// configuredNames is the set of ALL configured agent session names (including
// suspended agents). Beads for names not in this set are marked "orphaned".
// Beads for names in configuredNames but not in the agents slice are marked
// "suspended" (the agent exists in config but isn't currently runnable).
//
// This is Phase 1 of the unified session model: beads record reality alongside
// the existing reconciler. Phase 2 switches to a bead-driven reconciler.
func syncSessionBeads(
	store beads.Store,
	agents []agent.Agent,
	configuredNames map[string]bool,
	clk clock.Clock,
	stderr io.Writer,
) {
	if store == nil {
		return
	}

	// Load existing session beads.
	existing, err := store.ListByLabel(sessionBeadLabel, 0)
	if err != nil {
		fmt.Fprintf(stderr, "session beads: listing existing: %v\n", err) //nolint:errcheck
		return
	}

	// Index by session_name for O(1) lookup. Skip closed beads — a closed
	// bead is a completed lifecycle record, not a live session. If an agent
	// restarts after its bead was closed, we create a fresh bead.
	bySessionName := make(map[string]beads.Bead, len(existing))
	for _, b := range existing {
		if b.Status == "closed" {
			continue
		}
		if sn := b.Metadata["session_name"]; sn != "" {
			bySessionName[sn] = b
		}
	}

	// Build a set of desired session names for orphan detection.
	desired := make(map[string]bool, len(agents))

	now := clk.Now().UTC()

	for _, a := range agents {
		sn := a.SessionName()
		desired[sn] = true

		agentCfg := a.SessionConfig()
		coreHash := runtime.CoreFingerprint(agentCfg)
		liveHash := runtime.LiveFingerprint(agentCfg)

		// Use agent-level IsRunning which checks process liveness,
		// not just session existence.
		state := "stopped"
		if a.IsRunning() {
			state = "active"
		}

		b, exists := bySessionName[sn]
		if !exists {
			// Create a new session bead.
			newBead, createErr := store.Create(beads.Bead{
				Title:  a.Name(),
				Type:   sessionBeadType,
				Labels: []string{sessionBeadLabel, "agent:" + a.Name()},
				Metadata: map[string]string{
					"session_name":   sn,
					"agent_name":     a.Name(),
					"config_hash":    coreHash,
					"live_hash":      liveHash,
					"generation":     "1",
					"instance_token": generateToken(),
					"state":          state,
					"synced_at":      now.Format("2006-01-02T15:04:05Z07:00"),
				},
			})
			if createErr != nil {
				fmt.Fprintf(stderr, "session beads: creating bead for %s: %v\n", a.Name(), createErr) //nolint:errcheck
			} else {
				_ = newBead // created successfully
			}
			continue
		}

		// Update existing bead — check for drift.
		// Write config_hash LAST so it serves as the "commit" signal.
		// If an earlier write fails, the stale config_hash ensures the
		// next tick retries the full update.
		changed := false

		if b.Metadata["config_hash"] != coreHash {
			// Core config changed — bump generation and token first,
			// then write config_hash last as the "commit" signal.
			// If any preceding write fails, skip config_hash so the
			// stale hash triggers a retry on the next tick.
			gen, _ := strconv.Atoi(b.Metadata["generation"])
			gen++
			ok := true
			if setMeta(store, b.ID, "generation", strconv.Itoa(gen), stderr) != nil {
				ok = false
			}
			if setMeta(store, b.ID, "instance_token", generateToken(), stderr) != nil {
				ok = false
			}
			if ok {
				if setMeta(store, b.ID, "config_hash", coreHash, stderr) == nil {
					changed = true
				}
			}
		}

		if b.Metadata["live_hash"] != liveHash {
			setMeta(store, b.ID, "live_hash", liveHash, stderr) //nolint:errcheck
			changed = true
		}

		// Update state.
		if b.Metadata["state"] != state {
			setMeta(store, b.ID, "state", state, stderr) //nolint:errcheck
			changed = true
		}

		// Only update synced_at when something actually changed,
		// to avoid disk thrashing on every tick.
		if changed {
			setMeta(store, b.ID, "synced_at", now.Format("2006-01-02T15:04:05Z07:00"), stderr) //nolint:errcheck
		}
	}

	// Classify beads with no matching runnable agent.
	// - If the session name is in configuredNames but not in desired (runnable),
	//   the agent is suspended/disabled — mark "suspended".
	// - If the session name is not in configuredNames at all, the agent was
	//   removed from config — mark "orphaned". This includes pool/multi
	//   instances: they are ephemeral (not user-configured) and correctly
	//   become orphaned when their template is suspended or removed.
	// Do NOT close beads (that's Phase 2).
	for _, b := range existing {
		sn := b.Metadata["session_name"]
		if sn == "" || desired[sn] {
			continue
		}
		if b.Status == "closed" {
			continue
		}
		if configuredNames[sn] {
			// Still in config but not runnable (suspended/disabled).
			if b.Metadata["state"] != "suspended" {
				setMeta(store, b.ID, "state", "suspended", stderr) //nolint:errcheck
			}
		} else {
			// Not in config at all — orphaned.
			if b.Metadata["state"] != "orphaned" {
				setMeta(store, b.ID, "state", "orphaned", stderr) //nolint:errcheck
			}
		}
	}
}

// configuredSessionNames builds the set of ALL configured agent session names
// from the config, including suspended agents. Used to distinguish "orphaned"
// (removed from config) from "suspended" (still in config, not runnable).
func configuredSessionNames(cfg *config.City, cityName string) map[string]bool {
	st := cfg.Workspace.SessionTemplate
	names := make(map[string]bool, len(cfg.Agents))
	for _, a := range cfg.Agents {
		names[agent.SessionNameFor(cityName, a.QualifiedName(), st)] = true
	}
	return names
}

// setMeta wraps store.SetMetadata with error logging. Returns the error
// so callers can abort dependent writes (e.g., skip config_hash on failure).
func setMeta(store beads.Store, id, key, value string, stderr io.Writer) error {
	if err := store.SetMetadata(id, key, value); err != nil {
		fmt.Fprintf(stderr, "session beads: setting %s on %s: %v\n", key, id, err) //nolint:errcheck
		return err
	}
	return nil
}

// generateToken returns a cryptographically random hex token.
func generateToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
