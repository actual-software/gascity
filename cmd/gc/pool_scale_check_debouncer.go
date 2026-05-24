package main

import "sync"

// defaultPoolScaleCheckConfirmWindow is the number of consecutive scale_check
// samples a pool template's count must clear before the reconciler acts on a
// non-zero value as new demand. The pathology this prevents: a single tick
// where the scale_check shell returns 1 (because the bead briefly appeared
// unassigned via some upstream race) is enough to spawn a fresh pool session,
// which then runs work_query, finds nothing actually claimable, and drains —
// repeating every ~tick interval. Confirming over a window collapses that
// loop without changing steady-state demand semantics.
//
// 3 is the smallest window that suppresses the dominant single-tick flap
// while still tolerating a single dropped sample inside an otherwise
// sustained run. The window applies per template, so a slow reconciler tick
// cadence (multi-second) yields a confirm latency on the order of N ticks
// before legitimate new demand is acted on — acceptable for pool spawn
// because in-flight session beads from a prior tick continue to count as
// "spent" new demand via poolInFlightNewRequests during the wait.
const defaultPoolScaleCheckConfirmWindow = 3

// poolScaleCheckDebouncer holds the per-template history of recent
// scale_check samples used to confirm new pool demand. Methods are safe
// for concurrent use.
type poolScaleCheckDebouncer struct {
	mu      sync.Mutex
	windowN int
	history map[string][]int
}

// defaultPoolScaleCheckDebouncerSingleton is the process-scoped debouncer
// used by the reconciler. One gascity process supervises one city, so a
// package-level singleton keyed only by agent template name is sufficient
// to isolate history within the process.
//
// Tests should reset this via resetPoolScaleCheckDebouncerForTesting (and
// optionally setPoolScaleCheckDebouncerWindowForTesting to shrink the
// window) to avoid cross-test bleed from accumulated history.
var defaultPoolScaleCheckDebouncerSingleton = newPoolScaleCheckDebouncer(defaultPoolScaleCheckConfirmWindow)

func newPoolScaleCheckDebouncer(windowN int) *poolScaleCheckDebouncer {
	if windowN < 1 {
		windowN = 1
	}
	return &poolScaleCheckDebouncer{
		windowN: windowN,
		history: make(map[string][]int),
	}
}

// debounce records this tick's raw scale_check counts and returns the
// confirmed counts the reconciler should act on. For each template:
//   - If the template is in partialTemplates, the raw count passes through
//     and history is left untouched: a probe failure isn't a real
//     observation of demand, and admitting it would poison the window.
//   - Otherwise the sample is appended to the per-template ring. While the
//     ring has fewer than windowN samples, the raw count passes through —
//     so a cold-start supervisor still acts on demand without first
//     accumulating windowN ticks of history. Once the ring is full, the
//     returned count is the minimum across the window: a single tick at 0
//     in an otherwise positive window forces 0, killing the 1-tick flap.
func (d *poolScaleCheckDebouncer) debounce(raw map[string]int, partialTemplates map[string]bool) map[string]int {
	d.mu.Lock()
	defer d.mu.Unlock()

	out := make(map[string]int, len(raw))
	for template, count := range raw {
		if partialTemplates[template] {
			out[template] = count
			continue
		}
		h := append(d.history[template], count)
		if len(h) > d.windowN {
			h = h[len(h)-d.windowN:]
		}
		d.history[template] = h

		if len(h) < d.windowN {
			out[template] = count
			continue
		}

		minCount := h[0]
		for _, v := range h[1:] {
			if v < minCount {
				minCount = v
			}
		}
		out[template] = minCount
	}
	return out
}

// debouncePoolScaleCheckCounts is the package-level entry point the
// reconciler uses to apply the singleton's confirm window to a fresh
// scale_check sample.
func debouncePoolScaleCheckCounts(raw map[string]int, partialTemplates map[string]bool) map[string]int {
	return defaultPoolScaleCheckDebouncerSingleton.debounce(raw, partialTemplates)
}

// resetPoolScaleCheckDebouncerForTesting clears the singleton's per-template
// history. Tests that exercise the reconciler across multiple buildDesiredState
// or ComputePoolDesiredStatesDebounced calls should reset to keep the window
// deterministic.
func resetPoolScaleCheckDebouncerForTesting() {
	defaultPoolScaleCheckDebouncerSingleton.mu.Lock()
	defer defaultPoolScaleCheckDebouncerSingleton.mu.Unlock()
	defaultPoolScaleCheckDebouncerSingleton.history = make(map[string][]int)
}

// setPoolScaleCheckDebouncerWindowForTesting overrides the singleton's
// confirm window. windowN<=1 makes the debouncer a no-op (it returns the
// current sample unchanged), which is the default for cmd/gc tests so
// existing assertions about single-call spawn behavior remain valid.
// Returns the previous window for restoration.
func setPoolScaleCheckDebouncerWindowForTesting(windowN int) int {
	if windowN < 1 {
		windowN = 1
	}
	defaultPoolScaleCheckDebouncerSingleton.mu.Lock()
	defer defaultPoolScaleCheckDebouncerSingleton.mu.Unlock()
	prev := defaultPoolScaleCheckDebouncerSingleton.windowN
	defaultPoolScaleCheckDebouncerSingleton.windowN = windowN
	defaultPoolScaleCheckDebouncerSingleton.history = make(map[string][]int)
	return prev
}
