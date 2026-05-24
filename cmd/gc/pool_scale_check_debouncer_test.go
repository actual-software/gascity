package main

import (
	"reflect"
	"testing"
)

// withPoolScaleCheckDebouncerWindow installs a fresh debouncer at the given
// window for one test and restores the prior window on cleanup. Use this
// inside debouncer-specific tests because main_test.go's TestMain pins the
// singleton at window=1 so unrelated tests don't get debouncing applied.
func withPoolScaleCheckDebouncerWindow(t *testing.T, windowN int) {
	t.Helper()
	prev := setPoolScaleCheckDebouncerWindowForTesting(windowN)
	t.Cleanup(func() {
		setPoolScaleCheckDebouncerWindowForTesting(prev)
	})
}

func TestPoolScaleCheckDebouncer_ColdStartPassesThroughBelowWindow(t *testing.T) {
	withPoolScaleCheckDebouncerWindow(t, 3)

	got := debouncePoolScaleCheckCounts(map[string]int{"local-core.builder": 1}, nil)
	if want := map[string]int{"local-core.builder": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first sample: got %v, want %v (history smaller than window must pass raw count)", got, want)
	}
	got = debouncePoolScaleCheckCounts(map[string]int{"local-core.builder": 1}, nil)
	if want := map[string]int{"local-core.builder": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("second sample: got %v, want %v (still below window)", got, want)
	}
}

func TestPoolScaleCheckDebouncer_SteadyDemandConfirmsAfterWindow(t *testing.T) {
	withPoolScaleCheckDebouncerWindow(t, 3)

	for i := 0; i < 3; i++ {
		got := debouncePoolScaleCheckCounts(map[string]int{"local-core.builder": 1}, nil)
		if got["local-core.builder"] != 1 {
			t.Fatalf("tick %d: got %d, want 1 (every sample is 1, min over window must be 1)", i, got["local-core.builder"])
		}
	}
	// Tenth tick still 1 — window slides forward, but min stays 1.
	for i := 0; i < 10; i++ {
		got := debouncePoolScaleCheckCounts(map[string]int{"local-core.builder": 1}, nil)
		if got["local-core.builder"] != 1 {
			t.Fatalf("sustained tick %d: got %d, want 1", i, got["local-core.builder"])
		}
	}
}

func TestPoolScaleCheckDebouncer_SuppressesSingleTickFlap(t *testing.T) {
	withPoolScaleCheckDebouncerWindow(t, 3)

	// Mirror the fm-0ubml4 pathology: scale_check flaps 1, 0, 1, 0, 1, 0...
	// Once the window is full (after 3 samples), the minimum over any window
	// containing at least one 0 must be 0, which kills the spawn loop.
	samples := []int{1, 0, 1, 0, 1, 0}
	results := make([]int, 0, len(samples))
	for _, s := range samples {
		got := debouncePoolScaleCheckCounts(map[string]int{"local-core.builder": s}, nil)
		results = append(results, got["local-core.builder"])
	}
	// First two samples pass through (window not yet full).
	// Samples 3+ are min over [1,0,1]=0, [0,1,0]=0, [1,0,1]=0, [0,1,0]=0.
	want := []int{1, 0, 0, 0, 0, 0}
	if !reflect.DeepEqual(results, want) {
		t.Fatalf("flap sequence: got %v, want %v", results, want)
	}
}

func TestPoolScaleCheckDebouncer_PartialTemplatesPassThroughAndDoNotPoisonWindow(t *testing.T) {
	withPoolScaleCheckDebouncerWindow(t, 3)

	// A partial sample must not be recorded into the per-template history —
	// otherwise a probe failure (count defaults to 0) would force min=0 for
	// the next two ticks even after the probe recovers and reports steady 1.
	for i := 0; i < 3; i++ {
		got := debouncePoolScaleCheckCounts(
			map[string]int{"local-core.builder": 0},
			map[string]bool{"local-core.builder": true},
		)
		if got["local-core.builder"] != 0 {
			t.Fatalf("partial tick %d: got %d, want 0 (partial passes raw through)", i, got["local-core.builder"])
		}
	}
	// Three real samples of 1 should now confirm — partial ticks weren't
	// recorded, so the window fills cleanly from real samples.
	for i := 0; i < 3; i++ {
		got := debouncePoolScaleCheckCounts(map[string]int{"local-core.builder": 1}, nil)
		if got["local-core.builder"] != 1 {
			t.Fatalf("post-partial real tick %d: got %d, want 1", i, got["local-core.builder"])
		}
	}
}

func TestPoolScaleCheckDebouncer_WindowOfOneIsNoOp(t *testing.T) {
	withPoolScaleCheckDebouncerWindow(t, 1)

	// Window=1 is what TestMain installs so existing tests behave as before.
	// Whatever the raw count is, it must be returned unchanged on every tick.
	for _, s := range []int{1, 0, 5, 0, 1} {
		got := debouncePoolScaleCheckCounts(map[string]int{"local-core.builder": s}, nil)
		if got["local-core.builder"] != s {
			t.Fatalf("window=1 sample %d: got %d, want %d (debouncer must be a no-op)", s, got["local-core.builder"], s)
		}
	}
}

func TestPoolScaleCheckDebouncer_PerTemplateIsolation(t *testing.T) {
	withPoolScaleCheckDebouncerWindow(t, 3)

	// Builder flaps; planner is steady. Each template's history must be
	// independent — the planner's confirmed count must not collapse to 0
	// just because the builder's window saw a 0 sample.
	type tick struct {
		builder int
		planner int
	}
	ticks := []tick{
		{builder: 1, planner: 1},
		{builder: 0, planner: 1},
		{builder: 1, planner: 1},
		{builder: 0, planner: 1},
	}
	for i, tk := range ticks {
		got := debouncePoolScaleCheckCounts(
			map[string]int{"local-core.builder": tk.builder, "local-core.planner": tk.planner},
			nil,
		)
		if got["local-core.planner"] != 1 {
			t.Fatalf("tick %d planner: got %d, want 1 (steady demand must not be debounced by sibling template's flap)", i, got["local-core.planner"])
		}
	}
}

func TestPoolScaleCheckDebouncer_ResetClearsHistory(t *testing.T) {
	withPoolScaleCheckDebouncerWindow(t, 3)

	// Fill the window with 0s — without reset, the next real sample of 1
	// would be suppressed to 0 by the min over [0,0,1].
	for i := 0; i < 3; i++ {
		debouncePoolScaleCheckCounts(map[string]int{"local-core.builder": 0}, nil)
	}
	got := debouncePoolScaleCheckCounts(map[string]int{"local-core.builder": 1}, nil)
	if got["local-core.builder"] != 0 {
		t.Fatalf("pre-reset: got %d, want 0 (history [0,0,1] → min=0 confirms suppression)", got["local-core.builder"])
	}

	resetPoolScaleCheckDebouncerForTesting()

	// After reset, the next sample is a cold start again — passes through.
	got = debouncePoolScaleCheckCounts(map[string]int{"local-core.builder": 1}, nil)
	if got["local-core.builder"] != 1 {
		t.Fatalf("post-reset: got %d, want 1 (history is empty, raw count must pass through)", got["local-core.builder"])
	}
}
