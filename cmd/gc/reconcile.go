package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/telemetry"
)

// reconcileOps provides session-level operations needed by declarative
// reconciliation. Separate from runtime.Provider to avoid bloating the
// core 4-method interface with operations only reconciliation needs.
type reconcileOps interface {
	// listRunning returns all session names that match the given prefix.
	listRunning(prefix string) ([]string, error)

	// storeConfigHash persists a config hash in the session's environment
	// so future reconciliation can detect drift.
	storeConfigHash(name, hash string) error

	// configHash retrieves the previously stored config hash for a session.
	// Returns ("", nil) if no hash was stored (e.g., session predates hashing).
	configHash(name string) (string, error)

	// storeLiveHash persists the session_live hash separately.
	storeLiveHash(name, hash string) error

	// liveHash retrieves the previously stored session_live hash.
	// Returns ("", nil) if no hash was stored.
	liveHash(name string) (string, error)

	// runLive re-applies session_live commands to a running session.
	runLive(name string, cfg runtime.Config) error
}

// providerReconcileOps implements reconcileOps using runtime.Provider metadata.
type providerReconcileOps struct {
	sp runtime.Provider
}

func (o *providerReconcileOps) listRunning(prefix string) ([]string, error) {
	return o.sp.ListRunning(prefix)
}

func (o *providerReconcileOps) storeConfigHash(name, hash string) error {
	return o.sp.SetMeta(name, "GC_CONFIG_HASH", hash)
}

func (o *providerReconcileOps) configHash(name string) (string, error) {
	val, err := o.sp.GetMeta(name, "GC_CONFIG_HASH")
	if err != nil {
		// No hash stored yet — not an error for reconciliation.
		return "", nil
	}
	return val, nil
}

func (o *providerReconcileOps) storeLiveHash(name, hash string) error {
	return o.sp.SetMeta(name, "GC_LIVE_HASH", hash)
}

func (o *providerReconcileOps) liveHash(name string) (string, error) {
	val, err := o.sp.GetMeta(name, "GC_LIVE_HASH")
	if err != nil {
		return "", nil
	}
	return val, nil
}

func (o *providerReconcileOps) runLive(name string, cfg runtime.Config) error {
	return o.sp.RunLive(name, cfg)
}

// newReconcileOps creates a reconcileOps from a runtime.Provider.
func newReconcileOps(sp runtime.Provider) reconcileOps {
	return &providerReconcileOps{sp: sp}
}

// doReconcileAgents performs declarative reconciliation: make reality match
// the desired agent list. It handles four rows:
//
//  1. Not running + in config → Start
//  2. Running + healthy (same hash) → Skip
//  3. Running + NOT in config → Stop (orphan cleanup)
//  4. Running + wrong config (hash differs) → Stop + Start
//
// If rops is nil, reconciliation degrades gracefully to the simpler
// start-if-not-running behavior (no drift detection, no orphan cleanup).
func doReconcileAgents(agents []agent.Agent,
	sp runtime.Provider, rops reconcileOps, dops drainOps,
	ct crashTracker, it idleTracker,
	rec events.Recorder,
	poolSessions map[string]time.Duration,
	suspendedNames map[string]bool,
	driftDrainTimeout time.Duration,
	startupTimeout time.Duration,
	stdout, stderr io.Writer,
	ctxOpts ...context.Context,
) int {
	// Optional parent context for cancellation propagation. When called from
	// the controller loop, the controller's context is passed so that shutdown
	// cancels in-flight agent starts. When omitted (one-shot or tests),
	// context.Background() is used as a safe default.
	parentCtx := context.Background()
	if len(ctxOpts) > 0 && ctxOpts[0] != nil {
		parentCtx = ctxOpts[0]
	}
	// Build desired session name set for orphan detection.
	desired := make(map[string]bool, len(agents))

	// Pre-fetch running sessions once for batch lookup. This replaces N
	// individual IsRunning() calls (each a subprocess spawn for exec
	// providers) with a single ListRunning() call + O(N) map lookups.
	// The result is also reused for Phase 2 orphan cleanup.
	var allRunning []string
	var runningSet map[string]bool
	if rops != nil {
		if names, err := rops.listRunning(""); err == nil {
			allRunning = names
			runningSet = make(map[string]bool, len(names))
			for _, name := range names {
				runningSet[name] = true
			}
		}
	}

	// Phase 1a (sequential): Triage each agent — collect those that need
	// starting, handle running agents inline (drift, restart, idle are fast
	// and touch more shared state).
	type startCandidate struct {
		agent  agent.Agent
		reason string
	}
	var toStart []startCandidate

	for _, a := range agents {
		desired[a.SessionName()] = true

		// Fast path: if we have a pre-fetched running set and this
		// session isn't in it, skip per-agent IsRunning+ProcessAlive
		// calls entirely. No zombie capture needed (session doesn't
		// exist). This is the main scaling win for exec providers.
		if runningSet != nil && !runningSet[a.SessionName()] {
			if ct != nil && ct.isQuarantined(a.SessionName(), time.Now()) {
				continue
			}
			reason := "initial start"
			if _, isPool := poolSessions[a.SessionName()]; isPool {
				reason = "pool scale-up"
			}
			toStart = append(toStart, startCandidate{agent: a, reason: reason})
			continue
		}

		if !a.IsRunning() {
			// Row 1: not running → candidate for parallel start.

			// Zombie capture: session exists but agent process dead.
			// Grab pane output for crash forensics before Start() kills the zombie.
			zombieCaptured := false
			if sp.IsRunning(a.SessionName()) {
				zombieCaptured = true
				output, err := a.Peek(50)
				if err == nil && output != "" {
					rec.Record(events.Event{
						Type:    events.AgentCrashed,
						Actor:   "gc",
						Subject: a.Name(),
						Message: output,
					})
					telemetry.RecordAgentCrash(context.Background(), a.Name(), output)
				}
			}

			// Check crash loop quarantine.
			if ct != nil && ct.isQuarantined(a.SessionName(), time.Now()) {
				continue // skip silently — event was emitted when quarantine started
			}

			reason := "initial start"
			if zombieCaptured {
				reason = "crash recovery"
			} else if _, isPool := poolSessions[a.SessionName()]; isPool {
				reason = "pool scale-up"
			}
			toStart = append(toStart, startCandidate{agent: a, reason: reason})
			continue
		}

		// Running — clear drain if this desired agent was previously being drained
		// (handles scale-back-up: agent returns to desired set while draining).
		// Skip clearing if this is a drift-restart drain (we initiated it).
		if dops != nil {
			if draining, _ := dops.isDraining(a.SessionName()); draining {
				if isDrift, _ := dops.isDriftRestart(a.SessionName()); !isDrift {
					_ = dops.clearDrain(a.SessionName())
				}
			}
		}

		// Running — check if agent requested a restart (context exhaustion, etc.).
		if dops != nil {
			if restart, _ := dops.isRestartRequested(a.SessionName()); restart {
				_ = dops.clearRestartRequested(a.SessionName())                                // clear before stop to prevent re-fire
				fmt.Fprintf(stdout, "Agent '%s' requested restart, restarting...\n", a.Name()) //nolint:errcheck // best-effort stdout
				rec.Record(events.Event{
					Type:    events.AgentStopped,
					Actor:   "gc",
					Subject: a.Name(),
					Message: "restart requested by agent",
				})
				if err := a.Stop(); err != nil {
					fmt.Fprintf(stderr, "gc start: stopping %s for restart: %v\n", a.Name(), err) //nolint:errcheck // best-effort stderr
					continue
				}
				if err := a.Start(parentCtx); err != nil {
					fmt.Fprintf(stderr, "gc start: restarting %s: %v\n", a.Name(), err) //nolint:errcheck // best-effort stderr
					continue
				}
				_ = a.ClearScrollback()                                 // best-effort: clean slate after restart
				fmt.Fprintf(stdout, "Restarted agent '%s'\n", a.Name()) //nolint:errcheck // best-effort stdout
				rec.Record(events.Event{
					Type:    events.AgentStarted,
					Actor:   "gc",
					Subject: a.Name(),
				})
				if ct != nil {
					ct.recordStart(a.SessionName(), time.Now())
				}
				if rops != nil {
					cfg := a.SessionConfig()
					_ = rops.storeConfigHash(a.SessionName(), runtime.CoreFingerprint(cfg))
					_ = rops.storeLiveHash(a.SessionName(), runtime.LiveFingerprint(cfg))
				}
				continue
			}
		}

		// Running — check idle timeout (opt-in per agent).
		if it != nil && it.checkIdle(a, time.Now()) {
			fmt.Fprintf(stdout, "Agent '%s' idle too long, restarting...\n", a.Name()) //nolint:errcheck // best-effort stdout
			rec.Record(events.Event{
				Type:    events.AgentIdleKilled,
				Actor:   "gc",
				Subject: a.Name(),
			})
			telemetry.RecordAgentIdleKill(context.Background(), a.Name())
			if err := a.Stop(); err != nil {
				fmt.Fprintf(stderr, "gc start: stopping idle %s: %v\n", a.Name(), err) //nolint:errcheck // best-effort stderr
				continue
			}
			if err := a.Start(parentCtx); err != nil {
				fmt.Fprintf(stderr, "gc start: restarting idle %s: %v\n", a.Name(), err) //nolint:errcheck // best-effort stderr
				continue
			}
			_ = a.ClearScrollback()                                 // best-effort: clean slate after restart
			fmt.Fprintf(stdout, "Restarted agent '%s'\n", a.Name()) //nolint:errcheck // best-effort stdout
			rec.Record(events.Event{
				Type:    events.AgentStarted,
				Actor:   "gc",
				Subject: a.Name(),
			})
			// Record for crash tracking (idle kills count as restarts).
			if ct != nil {
				ct.recordStart(a.SessionName(), time.Now())
			}
			if rops != nil {
				cfg := a.SessionConfig()
				_ = rops.storeConfigHash(a.SessionName(), runtime.CoreFingerprint(cfg)) // best-effort
				_ = rops.storeLiveHash(a.SessionName(), runtime.LiveFingerprint(cfg))
			}
			continue
		}

		// Running — check pending drift restart (drain-then-restart in progress).
		if dops != nil {
			if isDrift, _ := dops.isDriftRestart(a.SessionName()); isDrift {
				acked, _ := dops.isDrainAcked(a.SessionName())
				timedOut := false
				if !acked && driftDrainTimeout > 0 {
					if started, err := dops.drainStartTime(a.SessionName()); err == nil {
						timedOut = time.Since(started) > driftDrainTimeout
					}
				}
				if acked || timedOut {
					// Drain complete → stop + start with new config.
					_ = dops.clearDriftRestart(a.SessionName())
					_ = dops.clearDrain(a.SessionName())
					if err := a.Stop(); err != nil {
						fmt.Fprintf(stderr, "gc start: stopping %s for drift restart: %v\n", a.Name(), err) //nolint:errcheck // best-effort stderr
						continue
					}
					if err := a.Start(parentCtx); err != nil {
						fmt.Fprintf(stderr, "gc start: restarting %s after drift drain: %v\n", a.Name(), err) //nolint:errcheck // best-effort stderr
						continue
					}
					_ = a.ClearScrollback()
					fmt.Fprintf(stdout, "Restarted agent '%s'\n", a.Name()) //nolint:errcheck // best-effort stdout
					rec.Record(events.Event{
						Type:    events.AgentStarted,
						Actor:   "gc",
						Subject: a.Name(),
					})
					if ct != nil {
						ct.recordStart(a.SessionName(), time.Now())
					}
					if rops != nil {
						cfg := a.SessionConfig()
						_ = rops.storeConfigHash(a.SessionName(), runtime.CoreFingerprint(cfg))
						_ = rops.storeLiveHash(a.SessionName(), runtime.LiveFingerprint(cfg))
					}
				}
				continue // skip normal drift check — already handling it
			}
		}

		// Running — check for drift if reconcile ops available.
		if rops == nil {
			continue // Row 2: no reconcile ops, skip.
		}

		storedCore, err := rops.configHash(a.SessionName())
		if err != nil || storedCore == "" {
			// No stored hash — graceful upgrade, don't restart.
			continue
		}

		cfg := a.SessionConfig()
		currentCore := runtime.CoreFingerprint(cfg)
		if storedCore != currentCore {
			// Core drift → drain + restart (existing behavior).
			if dops != nil {
				_ = dops.setDrain(a.SessionName())
				_ = dops.setDriftRestart(a.SessionName())
				fmt.Fprintf(stdout, "Config changed for '%s', draining for restart...\n", a.Name()) //nolint:errcheck // best-effort stdout
				rec.Record(events.Event{
					Type:    events.AgentDraining,
					Actor:   "gc",
					Subject: a.Name(),
					Message: "config drift detected",
				})
			} else {
				// No drain ops — fall back to hard restart (backward compat).
				fmt.Fprintf(stdout, "Config changed for '%s', restarting...\n", a.Name()) //nolint:errcheck // best-effort stdout
				if err := a.Stop(); err != nil {
					fmt.Fprintf(stderr, "gc start: stopping %s for restart: %v\n", a.Name(), err) //nolint:errcheck // best-effort stderr
					continue
				}
				if err := a.Start(parentCtx); err != nil {
					fmt.Fprintf(stderr, "gc start: restarting %s: %v\n", a.Name(), err) //nolint:errcheck // best-effort stderr
					continue
				}
				_ = a.ClearScrollback()
				fmt.Fprintf(stdout, "Restarted agent '%s'\n", a.Name()) //nolint:errcheck // best-effort stdout
				rec.Record(events.Event{
					Type:    events.AgentStarted,
					Actor:   "gc",
					Subject: a.Name(),
				})
				_ = rops.storeConfigHash(a.SessionName(), currentCore)
				_ = rops.storeLiveHash(a.SessionName(), runtime.LiveFingerprint(cfg))
			}
			continue
		}

		// Core matches — check live-only drift.
		storedLive, _ := rops.liveHash(a.SessionName())
		if storedLive == "" {
			continue // No stored live hash — graceful upgrade, don't re-apply.
		}
		currentLive := runtime.LiveFingerprint(cfg)
		if storedLive != currentLive {
			// Live-only drift → re-apply session_live without restart.
			fmt.Fprintf(stdout, "Live config changed for '%s', re-applying...\n", a.Name()) //nolint:errcheck // best-effort stdout
			_ = rops.runLive(a.SessionName(), cfg)
			_ = rops.storeLiveHash(a.SessionName(), currentLive)
			rec.Record(events.Event{
				Type:    events.AgentUpdated,
				Actor:   "gc",
				Subject: a.Name(),
				Message: "session_live re-applied",
			})
		}
	}

	// Phase 1b (parallel): Start all pending agents concurrently.
	// Each goroutine writes to its own slot — no shared writes.
	// Context carries the startup timeout so cancellation propagates
	// cleanly to the session provider (no goroutine leak).
	type startResult struct {
		agent   agent.Agent
		reason  string
		err     error
		elapsed time.Duration
	}
	results := make([]startResult, len(toStart))
	var wg sync.WaitGroup
	for i, c := range toStart {
		wg.Add(1)
		go func(idx int, a agent.Agent, reason string) {
			defer wg.Done()
			t0 := time.Now()
			ctx := parentCtx
			if startupTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(parentCtx, startupTimeout)
				defer cancel()
			}
			err := a.Start(ctx)
			results[idx] = startResult{agent: a, reason: reason, err: err, elapsed: time.Since(t0)}
		}(i, c.agent, c.reason)
	}
	wg.Wait()

	// Phase 1c (sequential): Process start results — logging, crash tracking,
	// event recording, config hash storage.
	for _, r := range results {
		if r.err != nil {
			fmt.Fprintf(stderr, "gc start: starting %s: %v\n", r.agent.Name(), r.err) //nolint:errcheck // best-effort stderr
			continue
		}

		// Record the start for crash tracking.
		if ct != nil {
			ct.recordStart(r.agent.SessionName(), time.Now())
			// Check if this start just tripped the threshold.
			if ct.isQuarantined(r.agent.SessionName(), time.Now()) {
				rec.Record(events.Event{
					Type:    events.AgentQuarantined,
					Actor:   "gc",
					Subject: r.agent.Name(),
					Message: "crash loop detected",
				})
				telemetry.RecordAgentQuarantine(context.Background(), r.agent.Name())
				fmt.Fprintf(stderr, "gc start: agent '%s' quarantined (crash loop: restarted too many times within window)\n", r.agent.Name()) //nolint:errcheck // best-effort stderr
			}
		}

		fmt.Fprintf(stdout, "Started agent '%s' (%s, %s)\n", r.agent.Name(), r.reason, formatElapsed(r.elapsed)) //nolint:errcheck // best-effort stdout
		rec.Record(events.Event{
			Type:    events.AgentStarted,
			Actor:   "gc",
			Subject: r.agent.Name(),
		})
		telemetry.RecordAgentStart(context.Background(), r.agent.SessionName(), r.agent.Name(), nil)
		// Store config hashes after successful start.
		if rops != nil {
			cfg := r.agent.SessionConfig()
			_ = rops.storeConfigHash(r.agent.SessionName(), runtime.CoreFingerprint(cfg)) // best-effort
			_ = rops.storeLiveHash(r.agent.SessionName(), runtime.LiveFingerprint(cfg))
		}
	}

	// Phase 2: Orphan cleanup — stop sessions with the city prefix that
	// are not in the desired set. Excess pool members are drained
	// gracefully (if drain ops available); true orphans are killed.
	// Reuses the pre-fetched running set when available.
	if rops != nil {
		running := allRunning
		if running == nil {
			var err error
			running, err = rops.listRunning("")
			if err != nil {
				fmt.Fprintf(stderr, "gc start: listing sessions: %v\n", err) //nolint:errcheck // best-effort stderr
			}
		}
		for _, name := range running {
			if desired[name] {
				continue
			}
			// Excess pool member → drain gracefully.
			drainTimeout, isPoolSession := poolSessions[name]
			if dops != nil && isPoolSession {
				draining, _ := dops.isDraining(name)
				if !draining {
					_ = dops.setDrain(name)
					fmt.Fprintf(stdout, "Draining '%s' (pool scaling down)\n", name) //nolint:errcheck // best-effort stdout
					continue
				}
				// Already draining — check if agent acknowledged.
				acked, _ := dops.isDrainAcked(name)
				if acked {
					// Agent ack'd drain → stop the session.
					if err := sp.Stop(name); err != nil {
						fmt.Fprintf(stderr, "gc start: stopping drained %s: %v\n", name, err) //nolint:errcheck // best-effort stderr
					} else {
						fmt.Fprintf(stdout, "Stopped drained session '%s'\n", name) //nolint:errcheck // best-effort stdout
					}
					continue
				}
				// Check drain timeout.
				if drainTimeout > 0 {
					started, err := dops.drainStartTime(name)
					if err == nil && time.Since(started) > drainTimeout {
						// Force-kill: drain timed out.
						if err := sp.Stop(name); err != nil {
							fmt.Fprintf(stderr, "gc start: stopping timed-out %s: %v\n", name, err) //nolint:errcheck // best-effort stderr
						} else {
							fmt.Fprintf(stdout, "Killed drained session '%s' (timeout after %s)\n", name, drainTimeout) //nolint:errcheck // best-effort stdout
						}
						continue
					}
				}
				continue // still winding down
			}
			// Suspended agent → stop with distinct messaging.
			if suspendedNames[name] {
				if err := sp.Stop(name); err != nil {
					fmt.Fprintf(stderr, "gc start: stopping suspended %s: %v\n", name, err) //nolint:errcheck // best-effort stderr
				} else {
					fmt.Fprintf(stdout, "Stopped suspended agent '%s'\n", name) //nolint:errcheck // best-effort stdout
					rec.Record(events.Event{
						Type:    events.AgentSuspended,
						Actor:   "gc",
						Subject: name,
					})
				}
				continue
			}
			// True orphan → kill.
			if err := sp.Stop(name); err != nil {
				fmt.Fprintf(stderr, "gc start: stopping orphan %s: %v\n", name, err) //nolint:errcheck // best-effort stderr
			} else {
				fmt.Fprintf(stdout, "Stopped orphan session '%s'\n", name) //nolint:errcheck // best-effort stdout
				telemetry.RecordAgentStop(context.Background(), name, "orphan", nil)
			}
		}
	}

	return 0
}

// doStopOrphans stops sessions that are not in the desired set. Used by gc stop
// to clean up orphans after stopping config agents. With per-city socket
// isolation, all sessions on the socket belong to this city.
// Uses gracefulStopAll for two-pass shutdown.
func doStopOrphans(sp runtime.Provider, rops reconcileOps, desired map[string]bool,
	timeout time.Duration, rec events.Recorder, stdout, stderr io.Writer,
) {
	if rops == nil {
		return
	}
	running, err := rops.listRunning("")
	if err != nil {
		fmt.Fprintf(stderr, "gc stop: listing sessions: %v\n", err) //nolint:errcheck // best-effort stderr
		return
	}
	var orphans []string
	for _, name := range running {
		if desired[name] {
			continue
		}
		orphans = append(orphans, name)
	}
	gracefulStopAll(orphans, sp, timeout, rec, stdout, stderr)
}

// formatElapsed returns a human-friendly duration string.
// Sub-second durations show milliseconds (e.g., "120ms"),
// longer durations show seconds with one decimal (e.g., "1.2s").
func formatElapsed(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}
