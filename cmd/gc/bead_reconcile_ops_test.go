package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/runtime"
)

// setupBeadReconcileOps creates a beadReconcileOps backed by a MemStore,
// syncs beads for the given agents, and returns the ops with its index populated.
func setupBeadReconcileOps(t *testing.T, agents []agent.Agent) (*beadReconcileOps, beads.Store) {
	t.Helper()
	store := beads.NewMemStore()
	clk := &clock.Fake{Time: time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)}

	var stderr bytes.Buffer
	idx := syncSessionBeads(store, agents, allConfigured(agents), nil, clk, &stderr, false)
	if stderr.Len() > 0 {
		t.Fatalf("unexpected stderr from syncSessionBeads: %s", stderr.String())
	}

	provider := newFakeReconcileOps()
	for _, a := range agents {
		if a.IsRunning() {
			provider.running[a.SessionName()] = true
		}
	}

	bro := newBeadReconcileOps(provider, func() beads.Store { return store })
	bro.updateIndex(idx)
	return bro, store
}

func TestBeadReconcileOps_StoreAndRetrieveConfigHash(t *testing.T) {
	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	bro, _ := setupBeadReconcileOps(t, agents)

	if err := bro.storeConfigHash("mayor", "abc123"); err != nil {
		t.Fatalf("storeConfigHash: %v", err)
	}

	hash, err := bro.configHash("mayor")
	if err != nil {
		t.Fatalf("configHash: %v", err)
	}
	if hash != "abc123" {
		t.Errorf("configHash = %q, want %q", hash, "abc123")
	}
}

func TestBeadReconcileOps_StoreAndRetrieveLiveHash(t *testing.T) {
	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	bro, _ := setupBeadReconcileOps(t, agents)

	if err := bro.storeLiveHash("mayor", "live456"); err != nil {
		t.Fatalf("storeLiveHash: %v", err)
	}

	hash, err := bro.liveHash("mayor")
	if err != nil {
		t.Fatalf("liveHash: %v", err)
	}
	if hash != "live456" {
		t.Errorf("liveHash = %q, want %q", hash, "live456")
	}
}

func TestBeadReconcileOps_MissingBeadFallsBackToProvider(t *testing.T) {
	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	bro, _ := setupBeadReconcileOps(t, agents)

	// Store hash for unknown session — should fall back to provider.
	if err := bro.storeConfigHash("unknown", "fallback"); err != nil {
		t.Fatalf("storeConfigHash for unknown: %v", err)
	}

	// Verify provider received the hash.
	provider := bro.provider.(*fakeReconcileOps)
	if provider.hashes["unknown"] != "fallback" {
		t.Errorf("provider configHash = %q, want %q", provider.hashes["unknown"], "fallback")
	}

	// Reading from unknown session should also fall back to provider.
	hash, err := bro.configHash("unknown")
	if err != nil {
		t.Fatalf("configHash for unknown: %v", err)
	}
	if hash != "fallback" {
		t.Errorf("configHash for unknown = %q, want %q (should fall back to provider)", hash, "fallback")
	}
}

func TestBeadReconcileOps_HashesSeparateFromSyncHashes(t *testing.T) {
	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	bro, store := setupBeadReconcileOps(t, agents)

	// Store reconciler hashes.
	if err := bro.storeConfigHash("mayor", "started-hash"); err != nil {
		t.Fatalf("storeConfigHash: %v", err)
	}
	if err := bro.storeLiveHash("mayor", "started-live"); err != nil {
		t.Fatalf("storeLiveHash: %v", err)
	}

	// Verify the bead has BOTH reconciler hashes and sync hashes.
	all, _ := store.ListByLabel(sessionBeadLabel, 0)
	if len(all) != 1 {
		t.Fatalf("expected 1 bead, got %d", len(all))
	}
	b := all[0]

	// Sync hashes (written by syncSessionBeads).
	if b.Metadata["config_hash"] == "" {
		t.Error("sync config_hash is empty")
	}
	if b.Metadata["live_hash"] == "" {
		t.Error("sync live_hash is empty")
	}

	// Reconciler hashes (written by beadReconcileOps).
	if b.Metadata["started_config_hash"] != "started-hash" {
		t.Errorf("started_config_hash = %q, want %q", b.Metadata["started_config_hash"], "started-hash")
	}
	if b.Metadata["started_live_hash"] != "started-live" {
		t.Errorf("started_live_hash = %q, want %q", b.Metadata["started_live_hash"], "started-live")
	}
}

func TestBeadReconcileOps_DriftDetection(t *testing.T) {
	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	bro, _ := setupBeadReconcileOps(t, agents)

	// Agent started with config hash "v1".
	if err := bro.storeConfigHash("mayor", "v1"); err != nil {
		t.Fatalf("storeConfigHash: %v", err)
	}

	// Reconciler reads: stored "v1" != current "v2" → drift.
	stored, _ := bro.configHash("mayor")
	if stored != "v1" {
		t.Errorf("stored = %q, want %q", stored, "v1")
	}

	// After restart with new config:
	if err := bro.storeConfigHash("mayor", "v2"); err != nil {
		t.Fatalf("storeConfigHash v2: %v", err)
	}
	stored, _ = bro.configHash("mayor")
	if stored != "v2" {
		t.Errorf("after update = %q, want %q", stored, "v2")
	}
}

func TestBeadReconcileOps_DelegatesListRunning(t *testing.T) {
	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	bro, _ := setupBeadReconcileOps(t, agents)

	names, err := bro.listRunning("")
	if err != nil {
		t.Fatalf("listRunning: %v", err)
	}
	if len(names) != 1 || names[0] != "mayor" {
		t.Errorf("listRunning = %v, want [mayor]", names)
	}
}

func TestBeadReconcileOps_DelegatesRunLive(t *testing.T) {
	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	bro, _ := setupBeadReconcileOps(t, agents)

	cfg := runtime.Config{Command: "claude"}
	if err := bro.runLive("mayor", cfg); err != nil {
		t.Fatalf("runLive: %v", err)
	}

	provider := bro.provider.(*fakeReconcileOps)
	if len(provider.liveCalls) != 1 || provider.liveCalls[0] != "mayor" {
		t.Errorf("runLive calls = %v, want [mayor]", provider.liveCalls)
	}
}

func TestBeadReconcileOps_IndexUpdateAfterResync(t *testing.T) {
	store := beads.NewMemStore()
	clk := &clock.Fake{Time: time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC)}

	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	var stderr bytes.Buffer
	idx1 := syncSessionBeads(store, agents, allConfigured(agents), nil, clk, &stderr, false)

	provider := newFakeReconcileOps()
	provider.running["mayor"] = true
	bro := newBeadReconcileOps(provider, func() beads.Store { return store })
	bro.updateIndex(idx1)

	// Store a hash.
	if err := bro.storeConfigHash("mayor", "v1"); err != nil {
		t.Fatalf("storeConfigHash: %v", err)
	}

	// Suspend the agent (bead gets closed).
	clk.Advance(5 * time.Second)
	configuredNames := map[string]bool{"mayor": true}
	idx2 := syncSessionBeads(store, nil, configuredNames, nil, clk, &stderr, false)
	bro.updateIndex(idx2)

	// Mayor bead is now closed — no index entry, falls back to provider
	// which has the mirrored "v1" from the earlier write. This is correct:
	// the provider mirror ensures hash continuity across suspend/resume.
	hash, _ := bro.configHash("mayor")
	if hash != "v1" {
		t.Errorf("after suspension, configHash = %q, want %q (provider mirror)", hash, "v1")
	}

	// Resume — new bead created.
	clk.Advance(5 * time.Second)
	idx3 := syncSessionBeads(store, agents, allConfigured(agents), nil, clk, &stderr, false)
	bro.updateIndex(idx3)

	// New bead has no started_config_hash yet — falls back to provider
	// which still has the mirrored "v1".
	hash, _ = bro.configHash("mayor")
	if hash != "v1" {
		t.Errorf("after resume, configHash = %q, want %q (provider mirror)", hash, "v1")
	}

	// Store hash on the new bead — updates both bead and provider.
	if err := bro.storeConfigHash("mayor", "v2"); err != nil {
		t.Fatalf("storeConfigHash after resume: %v", err)
	}
	hash, _ = bro.configHash("mayor")
	if hash != "v2" {
		t.Errorf("after resume + store, configHash = %q, want %q", hash, "v2")
	}
}

func TestBeadReconcileOps_StoreDegradedFallsBackToProvider(t *testing.T) {
	// When store.Get fails, configHash/liveHash should fall back to
	// provider. This is safe because storeConfigHash/storeLiveHash
	// mirror every write to the provider, so it always has current data.
	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	bro, _ := setupBeadReconcileOps(t, agents)

	// Store hashes — these mirror to provider automatically.
	if err := bro.storeConfigHash("mayor", "v1"); err != nil {
		t.Fatalf("storeConfigHash: %v", err)
	}
	if err := bro.storeLiveHash("mayor", "live1"); err != nil {
		t.Fatalf("storeLiveHash: %v", err)
	}

	// Verify provider received mirrored writes.
	provider := bro.provider.(*fakeReconcileOps)
	if provider.hashes["mayor"] != "v1" {
		t.Fatalf("provider config hash = %q, want %q (mirror write)", provider.hashes["mayor"], "v1")
	}
	if provider.liveHashes["mayor"] != "live1" {
		t.Fatalf("provider live hash = %q, want %q (mirror write)", provider.liveHashes["mayor"], "live1")
	}

	// Simulate store degradation: swap storeFunc to a fresh empty store
	// so Get(id) returns "not found" for the existing bead ID.
	bro.storeFunc = func() beads.Store { return beads.NewMemStore() }

	// configHash should fall back to provider's mirrored hash.
	hash, err := bro.configHash("mayor")
	if err != nil {
		t.Fatalf("configHash during degradation: %v", err)
	}
	if hash != "v1" {
		t.Errorf("configHash = %q, want %q (provider fallback)", hash, "v1")
	}

	// liveHash should also fall back to provider's mirrored hash.
	lhash, err := bro.liveHash("mayor")
	if err != nil {
		t.Fatalf("liveHash during degradation: %v", err)
	}
	if lhash != "live1" {
		t.Errorf("liveHash = %q, want %q (provider fallback)", lhash, "live1")
	}
}

func TestBeadReconcileOps_UpgradePathFallsBackToProvider(t *testing.T) {
	// Simulates upgrade: bead exists but has no started_config_hash.
	// configHash should fall back to provider's stored hash.
	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	bro, _ := setupBeadReconcileOps(t, agents)

	// Simulate pre-upgrade state: provider has the hash, bead doesn't.
	provider := bro.provider.(*fakeReconcileOps)
	provider.hashes["mayor"] = "pre-upgrade-hash"

	// configHash should fall back to provider since started_config_hash is empty.
	hash, err := bro.configHash("mayor")
	if err != nil {
		t.Fatalf("configHash: %v", err)
	}
	if hash != "pre-upgrade-hash" {
		t.Errorf("configHash = %q, want %q (should fall back to provider on upgrade)", hash, "pre-upgrade-hash")
	}

	// liveHash should also fall back.
	provider.liveHashes["mayor"] = "pre-upgrade-live"
	lhash, err := bro.liveHash("mayor")
	if err != nil {
		t.Fatalf("liveHash: %v", err)
	}
	if lhash != "pre-upgrade-live" {
		t.Errorf("liveHash = %q, want %q (should fall back to provider on upgrade)", lhash, "pre-upgrade-live")
	}

	// After storing a hash in the bead, it should take precedence.
	if err := bro.storeConfigHash("mayor", "v1"); err != nil {
		t.Fatalf("storeConfigHash: %v", err)
	}
	hash, _ = bro.configHash("mayor")
	if hash != "v1" {
		t.Errorf("after store, configHash = %q, want %q", hash, "v1")
	}
}

func TestBeadReconcileOps_NilStoreFallsBackToProvider(t *testing.T) {
	// When storeFunc returns nil, all operations should fall back to
	// provider without panicking.
	agents := []agent.Agent{
		&agent.Fake{
			FakeName:          "mayor",
			FakeSessionName:   "mayor",
			Running:           true,
			FakeSessionConfig: runtime.Config{Command: "claude"},
		},
	}

	bro, _ := setupBeadReconcileOps(t, agents)

	// Set storeFunc to return nil.
	bro.storeFunc = func() beads.Store { return nil }

	// Writes should not panic and should mirror to provider.
	if err := bro.storeConfigHash("mayor", "v1"); err != nil {
		t.Fatalf("storeConfigHash with nil store: %v", err)
	}
	if err := bro.storeLiveHash("mayor", "live1"); err != nil {
		t.Fatalf("storeLiveHash with nil store: %v", err)
	}

	// Provider should have the mirrored values.
	provider := bro.provider.(*fakeReconcileOps)
	if provider.hashes["mayor"] != "v1" {
		t.Errorf("provider config hash = %q, want %q", provider.hashes["mayor"], "v1")
	}
	if provider.liveHashes["mayor"] != "live1" {
		t.Errorf("provider live hash = %q, want %q", provider.liveHashes["mayor"], "live1")
	}

	// Reads should fall back to provider.
	hash, err := bro.configHash("mayor")
	if err != nil {
		t.Fatalf("configHash with nil store: %v", err)
	}
	if hash != "v1" {
		t.Errorf("configHash = %q, want %q (provider fallback)", hash, "v1")
	}

	lhash, err := bro.liveHash("mayor")
	if err != nil {
		t.Fatalf("liveHash with nil store: %v", err)
	}
	if lhash != "live1" {
		t.Errorf("liveHash = %q, want %q (provider fallback)", lhash, "live1")
	}
}
