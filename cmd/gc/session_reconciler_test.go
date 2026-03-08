package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/runtime"
)

// wasStarted checks if a Fake agent had Start() called.
func wasStarted(a *agent.Fake) bool {
	for _, c := range a.Calls {
		if c.Method == "Start" {
			return true
		}
	}
	return false
}

// reconcilerTestEnv holds common test infrastructure.
type reconcilerTestEnv struct {
	store      beads.Store
	sp         *runtime.Fake
	dt         *drainTracker
	clk        *clock.Fake
	rec        events.Recorder
	stdout     bytes.Buffer
	stderr     bytes.Buffer
	cfg        *config.City
	agentIndex map[string]agent.Agent
}

func newReconcilerTestEnv() *reconcilerTestEnv {
	return &reconcilerTestEnv{
		store:      beads.NewMemStore(),
		sp:         runtime.NewFake(),
		dt:         newDrainTracker(),
		clk:        &clock.Fake{Time: time.Date(2026, 3, 8, 12, 0, 0, 0, time.UTC)},
		rec:        events.Discard,
		cfg:        &config.City{},
		agentIndex: make(map[string]agent.Agent),
	}
}

func (e *reconcilerTestEnv) addAgent(name string, running bool) *agent.Fake {
	a := &agent.Fake{
		FakeName:          name,
		FakeSessionName:   name,
		Running:           running,
		FakeSessionConfig: runtime.Config{Command: "test-cmd"},
	}
	e.agentIndex[name] = a
	// Register session in provider so sp.IsRunning matches agent state.
	if running {
		_ = e.sp.Start(context.Background(), name, runtime.Config{Command: "test-cmd"})
	}
	return a
}

func (e *reconcilerTestEnv) createSessionBead(name, template string) beads.Bead {
	meta := map[string]string{
		"session_name":   name,
		"agent_name":     name,
		"template":       template,
		"config_hash":    runtime.CoreFingerprint(runtime.Config{Command: "test-cmd"}),
		"live_hash":      runtime.LiveFingerprint(runtime.Config{Command: "test-cmd"}),
		"generation":     "1",
		"instance_token": "test-token",
		"state":          "asleep",
	}
	b, err := e.store.Create(beads.Bead{
		Title:    name,
		Type:     sessionBeadType,
		Labels:   []string{sessionBeadLabel},
		Metadata: meta,
	})
	if err != nil {
		panic("creating test bead: " + err.Error())
	}
	return b
}

func (e *reconcilerTestEnv) reconcile(sessions []beads.Bead) int {
	cfgNames := configuredSessionNames(e.cfg, "")
	return reconcileSessionBeads(
		context.Background(), sessions, e.agentIndex, cfgNames, e.cfg, e.sp,
		e.store, e.dt, map[string]int{}, "",
		e.clk, e.rec, 0, 0, &e.stdout, &e.stderr,
	)
}

// --- buildDepsMap tests ---

func TestBuildDepsMap_NilConfig(t *testing.T) {
	deps := buildDepsMap(nil)
	if deps != nil {
		t.Errorf("expected nil, got %v", deps)
	}
}

func TestBuildDepsMap_NoDeps(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "a"},
			{Name: "b"},
		},
	}
	deps := buildDepsMap(cfg)
	if len(deps) != 0 {
		t.Errorf("expected empty map, got %v", deps)
	}
}

func TestBuildDepsMap_WithDeps(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "worker", DependsOn: []string{"db"}},
			{Name: "db"},
		},
	}
	deps := buildDepsMap(cfg)
	if len(deps) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(deps))
	}
	if len(deps["worker"]) != 1 || deps["worker"][0] != "db" {
		t.Errorf("expected worker -> [db], got %v", deps["worker"])
	}
}

// --- derivePoolDesired tests ---

func TestDerivePoolDesired_NilConfig(t *testing.T) {
	result := derivePoolDesired(nil, nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestDerivePoolDesired_CountsPoolInstances(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "worker", Pool: &config.PoolConfig{Min: 1, Max: 5}},
			{Name: "overseer"},
		},
	}
	agents := []agent.Agent{
		&agent.Fake{FakeName: "worker-1", FakeSessionName: "worker-1"},
		&agent.Fake{FakeName: "worker-2", FakeSessionName: "worker-2"},
		&agent.Fake{FakeName: "worker-3", FakeSessionName: "worker-3"},
		&agent.Fake{FakeName: "overseer", FakeSessionName: "overseer"},
	}
	result := derivePoolDesired(agents, cfg)
	if result["worker"] != 3 {
		t.Errorf("expected worker desired=3, got %d", result["worker"])
	}
	// Non-pool agents should not appear.
	if _, ok := result["overseer"]; ok {
		t.Error("non-pool agent should not be in poolDesired")
	}
}

// --- allDependenciesAlive tests ---

func TestAllDependenciesAlive_NoDeps(t *testing.T) {
	session := beads.Bead{Metadata: map[string]string{"template": "worker"}}
	cfg := &config.City{Agents: []config.Agent{{Name: "worker"}}}
	if !allDependenciesAlive(session, cfg, nil, "test") {
		t.Error("no deps should return true")
	}
}

func TestAllDependenciesAlive_DepAlive(t *testing.T) {
	session := beads.Bead{Metadata: map[string]string{"template": "worker"}}
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "worker", DependsOn: []string{"db"}},
			{Name: "db"},
		},
	}
	agentIndex := map[string]agent.Agent{
		"db": &agent.Fake{FakeName: "db", FakeSessionName: "db", Running: true},
	}
	if !allDependenciesAlive(session, cfg, agentIndex, "test") {
		t.Error("dep is alive, should return true")
	}
}

func TestAllDependenciesAlive_DepDead(t *testing.T) {
	session := beads.Bead{Metadata: map[string]string{"template": "worker"}}
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "worker", DependsOn: []string{"db"}},
			{Name: "db"},
		},
	}
	agentIndex := map[string]agent.Agent{
		"db": &agent.Fake{FakeName: "db", FakeSessionName: "db", Running: false},
	}
	if allDependenciesAlive(session, cfg, agentIndex, "test") {
		t.Error("dep is dead, should return false")
	}
}

// --- reconcileSessionBeads tests ---

func TestReconcileSessionBeads_WakesDeadSession(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	a := env.addAgent("worker", false)
	session := env.createSessionBead("worker", "worker")

	woken := env.reconcile([]beads.Bead{session})

	if woken != 1 {
		t.Errorf("expected 1 woken, got %d", woken)
	}
	if !wasStarted(a) {
		t.Error("agent should have been started")
	}
}

func TestReconcileSessionBeads_SkipsAliveSession(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	a := env.addAgent("worker", true)
	session := env.createSessionBead("worker", "worker")

	woken := env.reconcile([]beads.Bead{session})

	if woken != 0 {
		t.Errorf("expected 0 woken, got %d", woken)
	}
	if wasStarted(a) {
		t.Error("agent should NOT have been started (already alive)")
	}
}

func TestReconcileSessionBeads_SkipsQuarantinedSession(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	env.addAgent("worker", false)
	session := env.createSessionBead("worker", "worker")
	// Set quarantine in the future.
	_ = env.store.SetMetadata(session.ID, "quarantined_until",
		env.clk.Now().Add(10*time.Minute).UTC().Format(time.RFC3339))
	session.Metadata["quarantined_until"] = env.clk.Now().Add(10 * time.Minute).UTC().Format(time.RFC3339)

	woken := env.reconcile([]beads.Bead{session})

	if woken != 0 {
		t.Errorf("expected 0 woken (quarantined), got %d", woken)
	}
}

func TestReconcileSessionBeads_RespectsWakeBudget(t *testing.T) {
	env := newReconcilerTestEnv()
	// Create more agents than the wake budget.
	var cfgAgents []config.Agent
	var sessions []beads.Bead
	for i := 0; i < defaultMaxWakesPerTick+3; i++ {
		name := fmt.Sprintf("worker-%d", i)
		cfgAgents = append(cfgAgents, config.Agent{Name: name})
		env.addAgent(name, false)
		sessions = append(sessions, env.createSessionBead(name, name))
	}
	env.cfg = &config.City{Agents: cfgAgents}

	woken := env.reconcile(sessions)

	if woken != defaultMaxWakesPerTick {
		t.Errorf("expected %d woken (budget), got %d", defaultMaxWakesPerTick, woken)
	}
}

func TestReconcileSessionBeads_ConfigDriftInitiatesDrain(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	// Agent is alive with a DIFFERENT config than what's in the bead.
	a := env.addAgent("worker", true)
	a.FakeSessionConfig = runtime.Config{Command: "new-cmd"}

	session := env.createSessionBead("worker", "worker")

	// Verify hashes differ.
	storedHash := session.Metadata["config_hash"]
	currentHash := runtime.CoreFingerprint(runtime.Config{Command: "new-cmd"})
	if storedHash == currentHash {
		t.Fatalf("test setup error: stored hash %q should differ from current %q", storedHash, currentHash)
	}

	env.reconcile([]beads.Bead{session})

	// Should have initiated a drain.
	ds := env.dt.get(session.ID)
	if ds == nil {
		t.Fatalf("expected drain to be initiated for config drift (session.ID=%q, stderr=%s)", session.ID, env.stderr.String())
	}
	if ds.reason != "config-drift" {
		t.Errorf("drain reason = %q, want %q", ds.reason, "config-drift")
	}
}

func TestReconcileSessionBeads_NoDriftWhenHashMatches(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	env.addAgent("worker", true) // same config as bead
	session := env.createSessionBead("worker", "worker")

	env.reconcile([]beads.Bead{session})

	// No drain should be initiated.
	if ds := env.dt.get(session.ID); ds != nil {
		t.Errorf("expected no drain, got %+v", ds)
	}
}

func TestReconcileSessionBeads_DependencyOrdering_DepDeadBlocksWake(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{
		Agents: []config.Agent{
			{Name: "worker", DependsOn: []string{"db"}},
			{Name: "db"},
		},
	}
	env.addAgent("worker", false)
	// db agent exists but will fail to start — simulates dep not becoming alive.
	dbAgent := env.addAgent("db", false)
	dbAgent.StartErr = fmt.Errorf("db failed to start")

	dbBead := env.createSessionBead("db", "db")
	workerBead := env.createSessionBead("worker", "worker")

	env.reconcile([]beads.Bead{workerBead, dbBead})

	// db attempted to start but failed.
	if !wasStarted(dbAgent) {
		t.Error("db should have attempted to start")
	}
	// worker should NOT be started because db is still dead.
	workerAgent := env.agentIndex["worker"].(*agent.Fake)
	if wasStarted(workerAgent) {
		t.Error("worker should NOT have been started (dep not alive)")
	}
}

func TestReconcileSessionBeads_DependencyOrdering_TopoOrder(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{
		Agents: []config.Agent{
			{Name: "worker", DependsOn: []string{"db"}},
			{Name: "db"},
		},
	}
	env.addAgent("worker", false)
	env.addAgent("db", false)

	dbBead := env.createSessionBead("db", "db")
	workerBead := env.createSessionBead("worker", "worker")

	// Even though worker bead is listed first, topo ordering ensures
	// db is processed first. Since the Fake sets Running=true on Start,
	// worker can wake in the same tick after db succeeds.
	woken := env.reconcile([]beads.Bead{workerBead, dbBead})

	if woken != 2 {
		t.Errorf("expected 2 woken (both), got %d", woken)
	}
	dbAgent := env.agentIndex["db"].(*agent.Fake)
	workerAgent := env.agentIndex["worker"].(*agent.Fake)
	if !wasStarted(dbAgent) {
		t.Error("db should have been started")
	}
	if !wasStarted(workerAgent) {
		t.Error("worker should have been started (dep is alive after db.Start)")
	}
}

func TestReconcileSessionBeads_PoolDependencyBlocksWake(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{
		Agents: []config.Agent{
			{Name: "worker", DependsOn: []string{"db"}},
			{Name: "db", Pool: &config.PoolConfig{Min: 2, Max: 2}},
		},
	}
	// Worker depends on pool "db". No db instances alive → worker blocked.
	env.addAgent("worker", false)
	workerBead := env.createSessionBead("worker", "worker")

	woken := env.reconcile([]beads.Bead{workerBead})

	if woken != 0 {
		t.Errorf("expected 0 woken (pool dep dead), got %d", woken)
	}
	workerAgent := env.agentIndex["worker"].(*agent.Fake)
	if wasStarted(workerAgent) {
		t.Error("worker should NOT start (pool dependency db has no alive instances)")
	}
}

func TestReconcileSessionBeads_PoolDependencyUnblocksWake(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{
		Agents: []config.Agent{
			{Name: "worker", DependsOn: []string{"db"}},
			{Name: "db", Pool: &config.PoolConfig{Min: 2, Max: 2}},
		},
	}
	// One db pool instance is alive → unblocks worker.
	env.addAgent("worker", false)
	dbAgent := env.addAgent("db-1", true)
	dbAgent.FakeName = "db-1"
	dbAgent.FakeSessionName = "db-1"
	workerBead := env.createSessionBead("worker", "worker")

	woken := env.reconcile([]beads.Bead{workerBead})

	if woken != 1 {
		t.Errorf("expected 1 woken (pool dep alive), got %d", woken)
	}
}

func TestReconcileSessionBeads_OrphanSessionDrained(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "other"}}}
	// Session bead for "orphan" with no matching agent, but running in provider.
	_ = env.sp.Start(context.Background(), "orphan", runtime.Config{})
	session := env.createSessionBead("orphan", "orphan")

	env.reconcile([]beads.Bead{session})

	// Should have initiated a drain for the orphan.
	ds := env.dt.get(session.ID)
	if ds == nil {
		t.Fatal("expected drain for orphan session")
	}
	if ds.reason != "orphaned" {
		t.Errorf("drain reason = %q, want %q", ds.reason, "orphaned")
	}
}

func TestReconcileSessionBeads_OrphanNotRunningClosed(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "other"}}}
	// Session bead for "orphan" with no matching agent and NOT running.
	session := env.createSessionBead("orphan", "orphan")

	env.reconcile([]beads.Bead{session})

	// Bead should be closed with "orphaned" reason.
	b, _ := env.store.Get(session.ID)
	if b.Status != "closed" {
		t.Errorf("orphan bead status = %q, want closed", b.Status)
	}
	if b.Metadata["close_reason"] != "orphaned" {
		t.Errorf("close_reason = %q, want %q", b.Metadata["close_reason"], "orphaned")
	}
}

func TestReconcileSessionBeads_SuspendedSessionDrained(t *testing.T) {
	env := newReconcilerTestEnv()
	// "worker" is in config (configuredNames) but NOT in agentIndex (suspended).
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	// Session is running in provider but no agent in desired set.
	_ = env.sp.Start(context.Background(), "worker", runtime.Config{})
	session := env.createSessionBead("worker", "worker")

	env.reconcile([]beads.Bead{session})

	// Should drain with "suspended" reason (in config, not in desired set).
	ds := env.dt.get(session.ID)
	if ds == nil {
		t.Fatal("expected drain for suspended session")
	}
	if ds.reason != "suspended" {
		t.Errorf("drain reason = %q, want %q", ds.reason, "suspended")
	}
}

func TestReconcileSessionBeads_SuspendedNotRunningClosed(t *testing.T) {
	env := newReconcilerTestEnv()
	// "worker" is in config but NOT in agentIndex and NOT running.
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	session := env.createSessionBead("worker", "worker")

	env.reconcile([]beads.Bead{session})

	// Bead should be closed with "suspended" reason.
	b, _ := env.store.Get(session.ID)
	if b.Status != "closed" {
		t.Errorf("suspended bead status = %q, want closed", b.Status)
	}
	if b.Metadata["close_reason"] != "suspended" {
		t.Errorf("close_reason = %q, want %q", b.Metadata["close_reason"], "suspended")
	}
}

func TestReconcileSessionBeads_HealsExpiredTimers(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	env.addAgent("worker", false)
	session := env.createSessionBead("worker", "worker")
	// Set an expired held_until.
	past := env.clk.Now().Add(-5 * time.Minute).UTC().Format(time.RFC3339)
	_ = env.store.SetMetadata(session.ID, "held_until", past)
	_ = env.store.SetMetadata(session.ID, "sleep_reason", "user-hold")
	session.Metadata["held_until"] = past
	session.Metadata["sleep_reason"] = "user-hold"

	env.reconcile([]beads.Bead{session})

	// held_until should be cleared.
	b, _ := env.store.Get(session.ID)
	if b.Metadata["held_until"] != "" {
		t.Error("expired held_until should be cleared")
	}
	if b.Metadata["sleep_reason"] != "" {
		t.Error("sleep_reason should be cleared with expired hold")
	}
}

func TestReconcileSessionBeads_CrashDetection(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	env.addAgent("worker", false)
	session := env.createSessionBead("worker", "worker")
	// Simulate: woke 5 seconds ago, now dead (rapid exit).
	recentWake := env.clk.Now().Add(-5 * time.Second).UTC().Format(time.RFC3339)
	_ = env.store.SetMetadata(session.ID, "last_woke_at", recentWake)
	session.Metadata["last_woke_at"] = recentWake

	env.reconcile([]beads.Bead{session})

	// Should have recorded a wake failure.
	b, _ := env.store.Get(session.ID)
	if b.Metadata["wake_attempts"] != "1" {
		t.Errorf("wake_attempts = %q, want %q", b.Metadata["wake_attempts"], "1")
	}
}

func TestReconcileSessionBeads_StableClearsFailures(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	env.addAgent("worker", true)
	session := env.createSessionBead("worker", "worker")
	// Set wake_attempts and a last_woke_at that's old enough to be stable.
	stableWake := env.clk.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339)
	_ = env.store.SetMetadata(session.ID, "wake_attempts", "3")
	_ = env.store.SetMetadata(session.ID, "last_woke_at", stableWake)
	session.Metadata["wake_attempts"] = "3"
	session.Metadata["last_woke_at"] = stableWake

	env.reconcile([]beads.Bead{session})

	// wake_attempts should be cleared.
	b, _ := env.store.Get(session.ID)
	if b.Metadata["wake_attempts"] != "0" {
		t.Errorf("wake_attempts = %q, want %q", b.Metadata["wake_attempts"], "0")
	}
}

func TestReconcileSessionBeads_NoAgentNotWoken(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{}
	// Session bead exists but no matching agent in the index (orphaned).
	// Without an agent, alive=false and shouldWake=false, so nothing happens.
	// In practice, syncSessionBeads would close the bead for orphaned sessions.
	session := env.createSessionBead("orphan", "orphan")

	woken := env.reconcile([]beads.Bead{session})
	if woken != 0 {
		t.Errorf("expected 0 woken for orphan, got %d", woken)
	}
}

func TestReconcileSessionBeads_PreWakeCommitWritesMetadata(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	env.addAgent("worker", false)
	session := env.createSessionBead("worker", "worker")

	env.reconcile([]beads.Bead{session})

	// Verify preWakeCommit wrote metadata.
	b, _ := env.store.Get(session.ID)
	if b.Metadata["generation"] != "2" {
		t.Errorf("generation = %q, want %q (incremented by preWakeCommit)", b.Metadata["generation"], "2")
	}
	if b.Metadata["instance_token"] == "test-token" {
		t.Error("instance_token should have been regenerated by preWakeCommit")
	}
	if b.Metadata["last_woke_at"] == "" {
		t.Error("last_woke_at should be set by preWakeCommit")
	}
}

func TestReconcileSessionBeads_CancelsDrainOnWakeReason(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	env.addAgent("worker", true)
	session := env.createSessionBead("worker", "worker")

	// Pre-set a non-drift drain.
	gen := 1
	env.dt.set(session.ID, &drainState{
		startedAt:  env.clk.Now(),
		deadline:   env.clk.Now().Add(5 * time.Minute),
		reason:     "pool-excess",
		generation: gen,
	})

	env.reconcile([]beads.Bead{session})

	// Drain should be canceled because the agent is in the desired set.
	if ds := env.dt.get(session.ID); ds != nil {
		t.Errorf("drain should be canceled, got %+v", ds)
	}
}

func TestAllDependenciesAlive_WithSessionTemplate(t *testing.T) {
	// Verify that SessionTemplate is used consistently in session name
	// computation for dependency lookups.
	session := beads.Bead{Metadata: map[string]string{"template": "worker"}}
	cfg := &config.City{
		Workspace: config.Workspace{SessionTemplate: "{{.City}}-{{.Agent}}"},
		Agents: []config.Agent{
			{Name: "worker", DependsOn: []string{"db"}},
			{Name: "db"},
		},
	}
	// Agent indexed by the templated session name.
	sn := agent.SessionNameFor("myCity", "db", "{{.City}}-{{.Agent}}")
	agentIndex := map[string]agent.Agent{
		sn: &agent.Fake{FakeName: "db", FakeSessionName: sn, Running: true},
	}
	if !allDependenciesAlive(session, cfg, agentIndex, "myCity") {
		t.Errorf("dep should be alive (session name: %q)", sn)
	}
}

func TestReconcileSessionBeads_DriftDrainUsesConfigTimeout(t *testing.T) {
	env := newReconcilerTestEnv()
	env.cfg = &config.City{
		Agents: []config.Agent{{Name: "worker"}},
		Daemon: config.DaemonConfig{DriftDrainTimeout: "7m"},
	}
	a := env.addAgent("worker", true)
	a.FakeSessionConfig = runtime.Config{Command: "new-cmd"}
	session := env.createSessionBead("worker", "worker")

	// Pass drift drain timeout from config.
	cfgNames := configuredSessionNames(env.cfg, "")
	reconcileSessionBeads(
		context.Background(), []beads.Bead{session}, env.agentIndex, cfgNames,
		env.cfg, env.sp, env.store, env.dt, map[string]int{}, "",
		env.clk, env.rec, 0, env.cfg.Daemon.DriftDrainTimeoutDuration(),
		&env.stdout, &env.stderr,
	)

	ds := env.dt.get(session.ID)
	if ds == nil {
		t.Fatal("expected drain for config drift")
	}
	// Drain deadline should be ~7 minutes from now, not the default.
	expected := env.clk.Now().Add(7 * time.Minute)
	if ds.deadline != expected {
		t.Errorf("drain deadline = %v, want %v (7m from now)", ds.deadline, expected)
	}
}

// --- resolveAgentTemplate tests ---

func TestResolveAgentTemplate_DirectMatch(t *testing.T) {
	cfg := &config.City{Agents: []config.Agent{{Name: "overseer"}}}
	if got := resolveAgentTemplate("overseer", cfg); got != "overseer" {
		t.Errorf("got %q, want %q", got, "overseer")
	}
}

func TestResolveAgentTemplate_PoolInstance(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "worker", Pool: &config.PoolConfig{Min: 1, Max: 5}},
		},
	}
	if got := resolveAgentTemplate("worker-3", cfg); got != "worker" {
		t.Errorf("got %q, want %q", got, "worker")
	}
}

func TestResolveAgentTemplate_Fallback(t *testing.T) {
	cfg := &config.City{}
	if got := resolveAgentTemplate("unknown", cfg); got != "unknown" {
		t.Errorf("got %q, want %q", got, "unknown")
	}
}

func TestResolveAgentTemplate_NilConfig(t *testing.T) {
	if got := resolveAgentTemplate("test", nil); got != "test" {
		t.Errorf("got %q, want %q", got, "test")
	}
}

// --- resolvePoolSlot tests ---

func TestResolvePoolSlot_PoolInstance(t *testing.T) {
	if got := resolvePoolSlot("worker-3", "worker"); got != 3 {
		t.Errorf("got %d, want 3", got)
	}
}

func TestResolvePoolSlot_NonPool(t *testing.T) {
	if got := resolvePoolSlot("overseer", "overseer"); got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

func TestResolvePoolSlot_NonNumericSuffix(t *testing.T) {
	if got := resolvePoolSlot("worker-abc", "worker"); got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}
