package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// restartDoltHealthDefaultTimeout is the default budget for verifying
// managed Dolt reachability after `gc restart`. The supervisor's own
// post-start tick treats the same health probe as non-fatal (see
// prepareCityForSupervisor → "checking_bead_store_health"), so the
// post-condition lives at the operator command level instead. The
// budget is roughly the upper bound for managed Dolt to come up on a
// developer laptop (cold start + first query); operators on slower
// disks can extend it via the env var below.
const restartDoltHealthDefaultTimeout = 30 * time.Second

// envRestartDoltHealthTimeout overrides restartDoltHealthDefaultTimeout
// per invocation. Parsed with time.ParseDuration (e.g. "45s", "2m").
const envRestartDoltHealthTimeout = "GC_RESTART_DOLT_HEALTH_TIMEOUT"

// restartDoltHealthRetryInterval is the gap between consecutive health
// probes while waiting for managed Dolt to settle. healthBeadsProvider
// already has its own internal recovery path, so a short gap is enough
// to surface a port that came up moments after the start step returned.
const restartDoltHealthRetryInterval = 500 * time.Millisecond

// verifyDoltHealthyAfterRestartHook is the seam tests use to substitute
// the real probe. Production callers go through cmdRestartJSON, which
// invokes this hook (never the underlying function directly) so tests
// can drive the post-restart code path deterministically without
// spinning a real Dolt process.
var verifyDoltHealthyAfterRestartHook = verifyDoltHealthyAfterRestart

// verifyDoltHealthyAfterRestart polls healthBeadsProvider until managed
// Dolt is reachable, or until the configured budget expires. The error
// it returns names the cause and the recovery path (`gc start`), which
// the caller writes to stderr verbatim. A single one-line "verifying"
// message is written to stderr on entry so operators watching
// `gc restart` see progress before the full budget elapses.
//
// No-ops on cities that don't use the bd store contract (file
// providers) and on cities whose Dolt lifecycle is owned by something
// other than gc (postgres backend, external Dolt). Those configurations
// either have no managed process to verify, or the operator manages
// the database lifecycle themselves and a gc-side failure is not an
// honest signal.
func verifyDoltHealthyAfterRestart(cityPath string, stderr io.Writer) error {
	if !cityUsesBdStoreContract(cityPath) {
		return nil
	}
	owned, err := managedDoltLifecycleOwned(cityPath)
	if err != nil {
		return fmt.Errorf("checking managed Dolt ownership: %w", err)
	}
	if !owned {
		return nil
	}

	timeout := restartDoltHealthTimeoutFromEnv()
	if stderr != nil {
		fmt.Fprintf(stderr, "Verifying managed Dolt is healthy (budget %s)...\n", timeout) //nolint:errcheck // best-effort progress message
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		lastErr = healthBeadsProviderHook(cityPath)
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(restartDoltHealthRetryInterval)
	}

	cityRef := strings.TrimSpace(cityPath)
	if cityRef == "" {
		cityRef = "<city>"
	}
	return fmt.Errorf(
		"managed Dolt did not become healthy within %s after restart: %w\n"+
			"  The supervisor came back up, but the bead-store backend never reached a queryable state.\n"+
			"  Recover with: gc start %s\n"+
			"  (Override the budget with %s=<duration>, e.g. 45s.)",
		timeout, lastErr, cityRef, envRestartDoltHealthTimeout,
	)
}

// healthBeadsProviderHook is a test seam: production calls flow through
// healthBeadsProvider, but tests substitute a deterministic probe so
// they can exercise the success / timeout branches without a real Dolt.
var healthBeadsProviderHook = healthBeadsProvider

func restartDoltHealthTimeoutFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv(envRestartDoltHealthTimeout))
	if raw == "" {
		return restartDoltHealthDefaultTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return restartDoltHealthDefaultTimeout
	}
	return d
}
