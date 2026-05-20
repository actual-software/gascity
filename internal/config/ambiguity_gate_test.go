package config

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/gastownhall/gascity/internal/fsys"
)

// TestAmbiguityGateEnabled covers the three helper booleans that the
// controller uses to decide which gate to run. Each helper is false only
// when its specific threshold is zero — the empty struct disables every
// gate, preserving back-compat with configs that pre-date the spec.
func TestAmbiguityGateEnabled(t *testing.T) {
	cases := []struct {
		name             string
		g                AmbiguityGate
		intake, planning bool
		any              bool
	}{
		{"empty disabled", AmbiguityGate{}, false, false, false},
		{"intake only", AmbiguityGate{IntakeThreshold: 5}, true, false, true},
		{"planning only", AmbiguityGate{PlanningAssumptionCap: 3}, false, true, true},
		{"both", AmbiguityGate{IntakeThreshold: 5, PlanningAssumptionCap: 3}, true, true, true},
		{"high-stakes without thresholds is disabled", AmbiguityGate{PlanningHighStakes: []string{"data shape"}}, false, false, false},
	}
	for _, tc := range cases {
		if got := tc.g.IntakeEnabled(); got != tc.intake {
			t.Errorf("%s: IntakeEnabled() = %v, want %v", tc.name, got, tc.intake)
		}
		if got := tc.g.PlanningEnabled(); got != tc.planning {
			t.Errorf("%s: PlanningEnabled() = %v, want %v", tc.name, got, tc.planning)
		}
		if got := tc.g.Enabled(); got != tc.any {
			t.Errorf("%s: Enabled() = %v, want %v", tc.name, got, tc.any)
		}
	}
}

// TestMergeAmbiguityGate exercises every kind of merge collision in
// isolation so the four pack-loading tests below don't have to re-prove
// the same field-by-field semantics.
func TestMergeAmbiguityGate(t *testing.T) {
	pack := &AmbiguityGate{
		IntakeThreshold:       5,
		PlanningAssumptionCap: 3,
		PlanningHighStakes:    []string{"data shape", "security model"},
		KnownAreas:            []string{"manager", "builder"},
		BounceLogPath:         ".runtime/builder/bounces.jsonl",
	}

	t.Run("both nil -> nil", func(t *testing.T) {
		if got := MergeAmbiguityGate(nil, nil); got != nil {
			t.Errorf("nil+nil should return nil, got %+v", got)
		}
	})

	t.Run("pack nil, agent set -> agent copy", func(t *testing.T) {
		agent := &AmbiguityGate{IntakeThreshold: 7}
		out := MergeAmbiguityGate(nil, agent)
		if out == nil {
			t.Fatal("merge returned nil")
		}
		if out.IntakeThreshold != 7 {
			t.Errorf("got %d, want 7", out.IntakeThreshold)
		}
	})

	t.Run("agent nil, pack set -> pack copy", func(t *testing.T) {
		out := MergeAmbiguityGate(pack, nil)
		if out == nil {
			t.Fatal("merge returned nil")
		}
		if !reflect.DeepEqual(*out, *pack) {
			t.Errorf("merge = %+v, want pack copy %+v", *out, *pack)
		}
	})

	t.Run("agent non-zero scalars win", func(t *testing.T) {
		agent := &AmbiguityGate{IntakeThreshold: 99, PlanningAssumptionCap: 7, BounceLogPath: "/tmp/foo.jsonl"}
		out := MergeAmbiguityGate(pack, agent)
		if out.IntakeThreshold != 99 {
			t.Errorf("IntakeThreshold = %d, want 99 (agent wins)", out.IntakeThreshold)
		}
		if out.PlanningAssumptionCap != 7 {
			t.Errorf("PlanningAssumptionCap = %d, want 7 (agent wins)", out.PlanningAssumptionCap)
		}
		if out.BounceLogPath != "/tmp/foo.jsonl" {
			t.Errorf("BounceLogPath = %q, want /tmp/foo.jsonl (agent wins)", out.BounceLogPath)
		}
	})

	t.Run("agent zero scalars inherit from pack", func(t *testing.T) {
		agent := &AmbiguityGate{IntakeThreshold: 0, PlanningAssumptionCap: 7}
		out := MergeAmbiguityGate(pack, agent)
		if out.IntakeThreshold != 5 {
			t.Errorf("IntakeThreshold = %d, want 5 (pack default)", out.IntakeThreshold)
		}
		if out.PlanningAssumptionCap != 7 {
			t.Errorf("PlanningAssumptionCap = %d, want 7 (agent wins)", out.PlanningAssumptionCap)
		}
		if out.BounceLogPath != pack.BounceLogPath {
			t.Errorf("BounceLogPath = %q, want pack default %q", out.BounceLogPath, pack.BounceLogPath)
		}
	})

	t.Run("agent non-empty slices win", func(t *testing.T) {
		agent := &AmbiguityGate{
			PlanningHighStakes: []string{"auth boundary"},
			KnownAreas:         []string{"slack.py"},
		}
		out := MergeAmbiguityGate(pack, agent)
		if !reflect.DeepEqual(out.PlanningHighStakes, []string{"auth boundary"}) {
			t.Errorf("PlanningHighStakes = %v, want [auth boundary] (agent wins)", out.PlanningHighStakes)
		}
		if !reflect.DeepEqual(out.KnownAreas, []string{"slack.py"}) {
			t.Errorf("KnownAreas = %v, want [slack.py] (agent wins)", out.KnownAreas)
		}
	})

	t.Run("agent empty slices inherit from pack", func(t *testing.T) {
		out := MergeAmbiguityGate(pack, &AmbiguityGate{IntakeThreshold: 1})
		if !reflect.DeepEqual(out.PlanningHighStakes, pack.PlanningHighStakes) {
			t.Errorf("PlanningHighStakes = %v, want pack default %v", out.PlanningHighStakes, pack.PlanningHighStakes)
		}
		if !reflect.DeepEqual(out.KnownAreas, pack.KnownAreas) {
			t.Errorf("KnownAreas = %v, want pack default %v", out.KnownAreas, pack.KnownAreas)
		}
	})

	t.Run("slices are deep-copied", func(t *testing.T) {
		out := MergeAmbiguityGate(pack, nil)
		// Mutate output and verify the source pack is unchanged.
		out.PlanningHighStakes[0] = "MUTATED"
		out.KnownAreas[0] = "MUTATED"
		if pack.PlanningHighStakes[0] == "MUTATED" {
			t.Errorf("merge output shares PlanningHighStakes backing array with pack input")
		}
		if pack.KnownAreas[0] == "MUTATED" {
			t.Errorf("merge output shares KnownAreas backing array with pack input")
		}

		// And again for the case where agent provides the slice — its
		// backing array must not leak through either.
		agent := &AmbiguityGate{PlanningHighStakes: []string{"x"}, KnownAreas: []string{"y"}}
		out2 := MergeAmbiguityGate(pack, agent)
		out2.PlanningHighStakes[0] = "MUTATED"
		out2.KnownAreas[0] = "MUTATED"
		if agent.PlanningHighStakes[0] == "MUTATED" {
			t.Errorf("merge output shares PlanningHighStakes backing array with agent input")
		}
		if agent.KnownAreas[0] == "MUTATED" {
			t.Errorf("merge output shares KnownAreas backing array with agent input")
		}
	})
}

// TestPackAmbiguityGateMissing covers back-compat — a pack with no
// ambiguity_gate block at either scope leaves every agent's gate nil
// (disabled). This is the case every existing pack hits today.
func TestPackAmbiguityGateMissing(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/local-core/pack.toml", `
[pack]
name = "local-core"
schema = 2

[[agent]]
name = "builder"
`)

	agents, _, _, _, _, _, _, err := loadPack(
		fsys.OSFS{},
		filepath.Join(dir, "packs/local-core/pack.toml"),
		filepath.Join(dir, "packs/local-core"),
		dir, "", nil)
	if err != nil {
		t.Fatalf("loadPack: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("got %d agents, want 1", len(agents))
	}
	if agents[0].AmbiguityGate != nil {
		t.Errorf("missing block should leave AmbiguityGate nil, got %+v", *agents[0].AmbiguityGate)
	}
}

// TestPackAmbiguityGateAgentBlockOnly covers a pack that declares
// [agent.ambiguity_gate] on a single agent with no pack-level defaults.
// All five fields should round-trip unchanged.
func TestPackAmbiguityGateAgentBlockOnly(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/local-core/pack.toml", `
[pack]
name = "local-core"
schema = 2

[[agent]]
name = "builder"

  [agent.ambiguity_gate]
  intake_threshold = 5
  planning_assumption_cap = 3
  planning_high_stakes = ["data shape", "security model", "public API surface", "auth boundary"]
  known_areas = ["manager", "builder", "slack.py", "pack.toml", "factory-packs", "local-core"]
  bounce_log_path = ".runtime/builder/bounces.jsonl"
`)

	agents, _, _, _, _, _, _, err := loadPack(
		fsys.OSFS{},
		filepath.Join(dir, "packs/local-core/pack.toml"),
		filepath.Join(dir, "packs/local-core"),
		dir, "", nil)
	if err != nil {
		t.Fatalf("loadPack: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("got %d agents, want 1", len(agents))
	}
	if agents[0].AmbiguityGate == nil {
		t.Fatalf("AmbiguityGate is nil but block was declared")
	}
	want := AmbiguityGate{
		IntakeThreshold:       5,
		PlanningAssumptionCap: 3,
		PlanningHighStakes:    []string{"data shape", "security model", "public API surface", "auth boundary"},
		KnownAreas:            []string{"manager", "builder", "slack.py", "pack.toml", "factory-packs", "local-core"},
		BounceLogPath:         ".runtime/builder/bounces.jsonl",
	}
	if !reflect.DeepEqual(*agents[0].AmbiguityGate, want) {
		t.Errorf("AmbiguityGate mismatch:\n got %+v\nwant %+v", *agents[0].AmbiguityGate, want)
	}
}

// TestPackAmbiguityGatePackLevelOnly covers the inheritance case:
// [pack.ambiguity_gate] supplies defaults that every agent in the pack
// receives because they declare no agent-level block of their own.
func TestPackAmbiguityGatePackLevelOnly(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/local-core/pack.toml", `
[pack]
name = "local-core"
schema = 2

  [pack.ambiguity_gate]
  intake_threshold = 5
  planning_assumption_cap = 3
  planning_high_stakes = ["data shape", "security model"]
  known_areas = ["manager", "builder"]
  bounce_log_path = ".runtime/<agent>/bounces.jsonl"

[[agent]]
name = "builder"

[[agent]]
name = "architect"
`)

	agents, _, _, _, _, _, _, err := loadPack(
		fsys.OSFS{},
		filepath.Join(dir, "packs/local-core/pack.toml"),
		filepath.Join(dir, "packs/local-core"),
		dir, "", nil)
	if err != nil {
		t.Fatalf("loadPack: %v", err)
	}
	if len(agents) != 2 {
		t.Fatalf("got %d agents, want 2", len(agents))
	}
	want := AmbiguityGate{
		IntakeThreshold:       5,
		PlanningAssumptionCap: 3,
		PlanningHighStakes:    []string{"data shape", "security model"},
		KnownAreas:            []string{"manager", "builder"},
		BounceLogPath:         ".runtime/<agent>/bounces.jsonl",
	}
	for _, a := range agents {
		if a.AmbiguityGate == nil {
			t.Errorf("agent %q AmbiguityGate is nil but pack-level block was declared", a.Name)
			continue
		}
		if !reflect.DeepEqual(*a.AmbiguityGate, want) {
			t.Errorf("agent %q AmbiguityGate mismatch:\n got %+v\nwant %+v", a.Name, *a.AmbiguityGate, want)
		}
	}
}

// TestPackAmbiguityGateAgentOverridesPack covers the collision case from
// the design doc: agent-level values win on per-field collision; the
// pack supplies anything the agent left at the zero value.
func TestPackAmbiguityGateAgentOverridesPack(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/local-core/pack.toml", `
[pack]
name = "local-core"
schema = 2

  [pack.ambiguity_gate]
  intake_threshold = 5
  planning_assumption_cap = 3
  planning_high_stakes = ["data shape", "security model"]
  known_areas = ["manager", "builder"]
  bounce_log_path = ".runtime/<agent>/bounces.jsonl"

[[agent]]
name = "builder"

  # Override the intake threshold and the high-stakes set; leave
  # planning_assumption_cap, known_areas, and bounce_log_path zero so
  # the pack defaults flow through.
  [agent.ambiguity_gate]
  intake_threshold = 10
  planning_high_stakes = ["auth boundary"]

[[agent]]
name = "architect"

  # Override known_areas to a smaller dictionary; everything else
  # inherits from the pack default.
  [agent.ambiguity_gate]
  known_areas = ["docs/adr"]
`)

	agents, _, _, _, _, _, _, err := loadPack(
		fsys.OSFS{},
		filepath.Join(dir, "packs/local-core/pack.toml"),
		filepath.Join(dir, "packs/local-core"),
		dir, "", nil)
	if err != nil {
		t.Fatalf("loadPack: %v", err)
	}
	if len(agents) != 2 {
		t.Fatalf("got %d agents, want 2", len(agents))
	}
	byName := map[string]Agent{}
	for _, a := range agents {
		byName[a.Name] = a
	}

	wantBuilder := AmbiguityGate{
		IntakeThreshold:       10, // agent override
		PlanningAssumptionCap: 3,  // pack default
		PlanningHighStakes:    []string{"auth boundary"},
		KnownAreas:            []string{"manager", "builder"},
		BounceLogPath:         ".runtime/<agent>/bounces.jsonl",
	}
	if byName["builder"].AmbiguityGate == nil {
		t.Fatal("builder.AmbiguityGate is nil")
	}
	if !reflect.DeepEqual(*byName["builder"].AmbiguityGate, wantBuilder) {
		t.Errorf("builder AmbiguityGate mismatch:\n got %+v\nwant %+v", *byName["builder"].AmbiguityGate, wantBuilder)
	}

	wantArchitect := AmbiguityGate{
		IntakeThreshold:       5, // pack default
		PlanningAssumptionCap: 3, // pack default
		PlanningHighStakes:    []string{"data shape", "security model"},
		KnownAreas:            []string{"docs/adr"}, // agent override
		BounceLogPath:         ".runtime/<agent>/bounces.jsonl",
	}
	if byName["architect"].AmbiguityGate == nil {
		t.Fatal("architect.AmbiguityGate is nil")
	}
	if !reflect.DeepEqual(*byName["architect"].AmbiguityGate, wantArchitect) {
		t.Errorf("architect AmbiguityGate mismatch:\n got %+v\nwant %+v", *byName["architect"].AmbiguityGate, wantArchitect)
	}
}

// TestPackAmbiguityGatePackOverrideDoesNotLeakAcrossIncludes guards the
// merge boundary: when pack A includes pack B, A's [pack.ambiguity_gate]
// must not retroactively apply to B's agents (they already passed through
// B's own merge during recursive loading). Otherwise an outer pack could
// silently change the behavior of agents it inherits.
func TestPackAmbiguityGatePackOverrideDoesNotLeakAcrossIncludes(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "packs/inner/pack.toml", `
[pack]
name = "inner"
schema = 1

[[agent]]
name = "dog"
`)
	writeFile(t, dir, "packs/outer/pack.toml", `
[pack]
name = "outer"
schema = 1
includes = ["../inner"]

  [pack.ambiguity_gate]
  intake_threshold = 7

[[agent]]
name = "mayor"
`)

	agents, _, _, _, _, _, _, err := loadPack(
		fsys.OSFS{},
		filepath.Join(dir, "packs/outer/pack.toml"),
		filepath.Join(dir, "packs/outer"),
		dir, "", nil)
	if err != nil {
		t.Fatalf("loadPack: %v", err)
	}
	if len(agents) != 2 {
		t.Fatalf("got %d agents, want 2", len(agents))
	}
	byName := map[string]Agent{}
	for _, a := range agents {
		byName[a.Name] = a
	}
	// dog came from inner (no pack-level config) — outer's pack-level
	// must not have flowed through to it.
	if byName["dog"].AmbiguityGate != nil {
		t.Errorf("dog inherited outer's pack-level config across include boundary: got %+v", *byName["dog"].AmbiguityGate)
	}
	// mayor came from outer — it should have the pack-level default.
	if byName["mayor"].AmbiguityGate == nil || byName["mayor"].AmbiguityGate.IntakeThreshold != 7 {
		got := "nil"
		if byName["mayor"].AmbiguityGate != nil {
			got = "{intake=" + intToString(byName["mayor"].AmbiguityGate.IntakeThreshold) + "}"
		}
		t.Errorf("mayor missing outer's pack-level config: got %s, want intake=7", got)
	}
}

func intToString(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
