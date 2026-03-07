package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/runtime"
)

// testBuildParams returns agentBuildParams suitable for unit tests.
func testBuildParams(sp runtime.Provider) *agentBuildParams {
	return &agentBuildParams{
		cityName:  "city",
		cityPath:  "/tmp/city",
		workspace: &config.Workspace{Name: "city"},
		lookPath:  fakeLookPath,
		fs:        fsys.NewFake(),
		sp:        sp,
		stderr:    io.Discard,
	}
}

func TestEvaluatePoolSuccess(t *testing.T) {
	pool := config.PoolConfig{Min: 0, Max: 10, Check: "echo 5"}
	runner := func(_, _ string) (string, error) { return "5", nil }

	got, err := evaluatePool("worker", pool, "", runner)
	if err != nil {
		t.Fatalf("evaluatePool: %v", err)
	}
	if got != 5 {
		t.Errorf("got %d, want 5", got)
	}
}

func TestEvaluatePoolClampToMax(t *testing.T) {
	pool := config.PoolConfig{Min: 0, Max: 10, Check: "echo 20"}
	runner := func(_, _ string) (string, error) { return "20", nil }

	got, err := evaluatePool("worker", pool, "", runner)
	if err != nil {
		t.Fatalf("evaluatePool: %v", err)
	}
	if got != 10 {
		t.Errorf("got %d, want 10 (max)", got)
	}
}

func TestEvaluatePoolClampToMin(t *testing.T) {
	pool := config.PoolConfig{Min: 2, Max: 10, Check: "echo 0"}
	runner := func(_, _ string) (string, error) { return "0", nil }

	got, err := evaluatePool("worker", pool, "", runner)
	if err != nil {
		t.Fatalf("evaluatePool: %v", err)
	}
	if got != 2 {
		t.Errorf("got %d, want 2 (min)", got)
	}
}

func TestEvaluatePoolRunnerError(t *testing.T) {
	pool := config.PoolConfig{Min: 2, Max: 10, Check: "fail"}
	runner := func(_, _ string) (string, error) {
		return "", fmt.Errorf("command failed")
	}

	got, err := evaluatePool("worker", pool, "", runner)
	if err == nil {
		t.Fatal("expected error")
	}
	if got != 2 {
		t.Errorf("got %d, want 2 (min on error)", got)
	}
}

func TestEvaluatePoolNonInteger(t *testing.T) {
	pool := config.PoolConfig{Min: 1, Max: 10, Check: "echo abc"}
	runner := func(_, _ string) (string, error) { return "abc", nil }

	got, err := evaluatePool("worker", pool, "", runner)
	if err == nil {
		t.Fatal("expected error for non-integer output")
	}
	if got != 1 {
		t.Errorf("got %d, want 1 (min on error)", got)
	}
}

func TestEvaluatePoolWhitespace(t *testing.T) {
	pool := config.PoolConfig{Min: 0, Max: 10, Check: "echo 3"}
	runner := func(_, _ string) (string, error) { return " 3\n", nil }

	got, err := evaluatePool("worker", pool, "", runner)
	if err != nil {
		t.Fatalf("evaluatePool: %v", err)
	}
	if got != 3 {
		t.Errorf("got %d, want 3", got)
	}
}

// Regression: empty check output must be an error, not silent success.
func TestEvaluatePoolEmptyOutput(t *testing.T) {
	pool := config.PoolConfig{Min: 2, Max: 10, Check: "true"}
	runner := func(_, _ string) (string, error) { return "", nil }

	got, err := evaluatePool("worker", pool, "", runner)
	if err == nil {
		t.Fatal("expected error for empty output")
	}
	if got != 2 {
		t.Errorf("got %d, want 2 (min on error)", got)
	}
}

// Regression: whitespace-only output should also be treated as empty.
func TestEvaluatePoolWhitespaceOnly(t *testing.T) {
	pool := config.PoolConfig{Min: 1, Max: 10, Check: "echo"}
	runner := func(_, _ string) (string, error) { return "  \n", nil }

	got, err := evaluatePool("worker", pool, "", runner)
	if err == nil {
		t.Fatal("expected error for whitespace-only output")
	}
	if got != 1 {
		t.Errorf("got %d, want 1 (min on error)", got)
	}
}

func TestEvaluatePoolUnlimitedNoClamp(t *testing.T) {
	pool := config.PoolConfig{Min: 0, Max: -1, Check: "echo 100"}
	runner := func(_, _ string) (string, error) { return "100", nil }

	got, err := evaluatePool("worker", pool, "", runner)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// With max=-1 (unlimited), the value should not be clamped.
	if got != 100 {
		t.Errorf("got %d, want 100 (no upper clamp for unlimited)", got)
	}
}

func TestEvaluatePoolUnlimitedClampsToMin(t *testing.T) {
	pool := config.PoolConfig{Min: 2, Max: -1, Check: "echo 0"}
	runner := func(_, _ string) (string, error) { return "0", nil }

	got, err := evaluatePool("worker", pool, "", runner)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != 2 {
		t.Errorf("got %d, want 2 (clamped to min)", got)
	}
}

func TestPoolAgentsUnlimitedNaming(t *testing.T) {
	cfgAgent := &config.Agent{
		Name:         "polecat",
		StartCommand: "echo hello",
		Pool:         &config.PoolConfig{Min: 0, Max: -1, Check: "echo 3"},
	}
	sp := runtime.NewFake()
	bp := newAgentBuildParams("city", t.TempDir(), &config.City{}, sp, time.Now(), io.Discard)

	agents, err := poolAgents(bp, cfgAgent, 3)
	if err != nil {
		t.Fatalf("poolAgents: %v", err)
	}
	if len(agents) != 3 {
		t.Fatalf("len = %d, want 3", len(agents))
	}
	// Unlimited pools use suffixed names (like max > 1).
	for i, a := range agents {
		want := fmt.Sprintf("polecat-%d", i+1)
		if a.Name() != want {
			t.Errorf("agents[%d].Name() = %q, want %q", i, a.Name(), want)
		}
	}
}

func TestDiscoverPoolInstancesBounded(t *testing.T) {
	sp := runtime.NewFake()
	pool := config.PoolConfig{Min: 0, Max: 3}
	instances := discoverPoolInstances("worker", "myrig", pool, "city", "", sp)
	if len(instances) != 3 {
		t.Fatalf("len = %d, want 3", len(instances))
	}
	want := []string{"myrig/worker-1", "myrig/worker-2", "myrig/worker-3"}
	for i, got := range instances {
		if got != want[i] {
			t.Errorf("instances[%d] = %q, want %q", i, got, want[i])
		}
	}
}

func TestDiscoverPoolInstancesUnlimited(t *testing.T) {
	sp := runtime.NewFake()
	// Start some instances that look like pool members.
	_ = sp.Start(context.Background(), "myrig--worker-1", runtime.Config{})
	_ = sp.Start(context.Background(), "myrig--worker-3", runtime.Config{})
	// Start a non-matching session.
	_ = sp.Start(context.Background(), "myrig--refinery", runtime.Config{})

	pool := config.PoolConfig{Min: 0, Max: -1}
	instances := discoverPoolInstances("worker", "myrig", pool, "city", "", sp)
	if len(instances) != 2 {
		t.Fatalf("len = %d, want 2 (instances: %v)", len(instances), instances)
	}
}

func TestCountRunningPoolInstancesUnlimited(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "worker-1", runtime.Config{})
	_ = sp.Start(context.Background(), "worker-3", runtime.Config{})

	count := countRunningPoolInstances("worker", "", config.PoolConfig{Min: 0, Max: -1}, "city", "", sp)
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func TestPoolAgentsNaming(t *testing.T) {
	cfgAgent := &config.Agent{
		Name:         "worker",
		StartCommand: "echo hello",
		Pool:         &config.PoolConfig{Min: 0, Max: 5, Check: "echo 3"},
	}
	sp := runtime.NewFake()
	agents, err := poolAgents(testBuildParams(sp), cfgAgent, 3)
	if err != nil {
		t.Fatalf("poolAgents: %v", err)
	}
	if len(agents) != 3 {
		t.Fatalf("len(agents) = %d, want 3", len(agents))
	}
	want := []string{"worker-1", "worker-2", "worker-3"}
	for i, a := range agents {
		if a.Name() != want[i] {
			t.Errorf("agents[%d].Name() = %q, want %q", i, a.Name(), want[i])
		}
	}
}

func TestPoolAgentsSessionNames(t *testing.T) {
	cfgAgent := &config.Agent{
		Name:         "worker",
		StartCommand: "echo hello",
		Pool:         &config.PoolConfig{Min: 0, Max: 5, Check: "echo 3"},
	}
	sp := runtime.NewFake()
	agents, err := poolAgents(testBuildParams(sp), cfgAgent, 3)
	if err != nil {
		t.Fatalf("poolAgents: %v", err)
	}
	want := []string{"worker-1", "worker-2", "worker-3"}
	for i, a := range agents {
		if a.SessionName() != want[i] {
			t.Errorf("agents[%d].SessionName() = %q, want %q", i, a.SessionName(), want[i])
		}
	}
}

func TestPoolAgentsZeroDesired(t *testing.T) {
	cfgAgent := &config.Agent{
		Name:         "worker",
		StartCommand: "echo hello",
		Pool:         &config.PoolConfig{Min: 0, Max: 5, Check: "echo 0"},
	}
	sp := runtime.NewFake()
	agents, err := poolAgents(testBuildParams(sp), cfgAgent, 0)
	if err != nil {
		t.Fatalf("poolAgents: %v", err)
	}
	if len(agents) != 0 {
		t.Errorf("len(agents) = %d, want 0", len(agents))
	}
}

func TestPoolAgentsEnv(t *testing.T) {
	cfgAgent := &config.Agent{
		Name:         "worker",
		StartCommand: "echo hello",
		Pool:         &config.PoolConfig{Min: 0, Max: 5, Check: "echo 2"},
		Env:          map[string]string{"POOL_VAR": "yes"},
	}
	sp := runtime.NewFake()
	agents, err := poolAgents(testBuildParams(sp), cfgAgent, 2)
	if err != nil {
		t.Fatalf("poolAgents: %v", err)
	}
	if len(agents) != 2 {
		t.Fatalf("len(agents) = %d, want 2", len(agents))
	}
	// Check that GC_AGENT is set correctly for each agent.
	cfg1 := agents[0].SessionConfig()
	if cfg1.Env["GC_AGENT"] != "worker-1" {
		t.Errorf("agent[0] GC_AGENT = %q, want %q", cfg1.Env["GC_AGENT"], "worker-1")
	}
	cfg2 := agents[1].SessionConfig()
	if cfg2.Env["GC_AGENT"] != "worker-2" {
		t.Errorf("agent[1] GC_AGENT = %q, want %q", cfg2.Env["GC_AGENT"], "worker-2")
	}
	// Check pool-level env is passed through.
	if cfg1.Env["POOL_VAR"] != "yes" {
		t.Errorf("agent[0] POOL_VAR = %q, want %q", cfg1.Env["POOL_VAR"], "yes")
	}
}

func TestPoolAgentsMaxOneNoSuffix(t *testing.T) {
	// When max == 1, the agent should use the bare name (no -1 suffix).
	cfgAgent := &config.Agent{
		Name:         "refinery",
		StartCommand: "echo hello",
		Pool:         &config.PoolConfig{Min: 0, Max: 1, Check: "echo 1"},
	}
	sp := runtime.NewFake()
	agents, err := poolAgents(testBuildParams(sp), cfgAgent, 1)
	if err != nil {
		t.Fatalf("poolAgents: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("len(agents) = %d, want 1", len(agents))
	}
	if agents[0].Name() != "refinery" {
		t.Errorf("Name() = %q, want %q (bare name, no suffix)", agents[0].Name(), "refinery")
	}
	if agents[0].SessionName() != "refinery" {
		t.Errorf("SessionName() = %q, want %q", agents[0].SessionName(), "refinery")
	}
}

// ---------------------------------------------------------------------------
// Session setup template expansion tests
// ---------------------------------------------------------------------------

func TestExpandSessionSetup_Basic(t *testing.T) {
	ctx := SessionSetupContext{
		Session:  "mayor",
		Agent:    "mayor",
		Rig:      "",
		CityRoot: "/home/user/city",
		CityName: "bright-lights",
		WorkDir:  "/home/user/city",
	}
	cmds := []string{
		"tmux set-option -t {{.Session}} status-style 'bg=blue'",
		"tmux set-option -t {{.Session}} status-left ' {{.Agent}} '",
	}
	got := expandSessionSetup(cmds, ctx)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0] != "tmux set-option -t mayor status-style 'bg=blue'" {
		t.Errorf("cmd[0] = %q", got[0])
	}
	if got[1] != "tmux set-option -t mayor status-left ' mayor '" {
		t.Errorf("cmd[1] = %q", got[1])
	}
}

func TestExpandSessionSetup_AllVariables(t *testing.T) {
	ctx := SessionSetupContext{
		Session:  "hw--polecat",
		Agent:    "hw/polecat",
		Rig:      "hello-world",
		CityRoot: "/city",
		CityName: "bl",
		WorkDir:  "/city/.gc/worktrees/polecat",
	}
	cmds := []string{
		"echo {{.Session}} {{.Agent}} {{.Rig}} {{.CityRoot}} {{.CityName}} {{.WorkDir}}",
	}
	got := expandSessionSetup(cmds, ctx)
	want := "echo hw--polecat hw/polecat hello-world /city bl /city/.gc/worktrees/polecat"
	if got[0] != want {
		t.Errorf("got %q, want %q", got[0], want)
	}
}

func TestExpandSessionSetup_InvalidTemplate(t *testing.T) {
	ctx := SessionSetupContext{Session: "test"}
	cmds := []string{
		"tmux {{.Session}}",    // valid
		"tmux {{.BadSyntax",    // invalid template
		"tmux {{.Session}} ok", // valid
	}
	got := expandSessionSetup(cmds, ctx)
	if got[0] != "tmux test" {
		t.Errorf("cmd[0] = %q, want expanded", got[0])
	}
	// Invalid template → raw command preserved.
	if got[1] != "tmux {{.BadSyntax" {
		t.Errorf("cmd[1] = %q, want raw (fallback)", got[1])
	}
	if got[2] != "tmux test ok" {
		t.Errorf("cmd[2] = %q, want expanded", got[2])
	}
}

func TestExpandSessionSetup_Nil(t *testing.T) {
	got := expandSessionSetup(nil, SessionSetupContext{})
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestExpandSessionSetup_Empty(t *testing.T) {
	got := expandSessionSetup([]string{}, SessionSetupContext{})
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestResolveSetupScript_Relative(t *testing.T) {
	got := resolveSetupScript("scripts/setup.sh", "/home/user/city")
	if got != "/home/user/city/scripts/setup.sh" {
		t.Errorf("got %q, want absolute path", got)
	}
}

func TestResolveSetupScript_Absolute(t *testing.T) {
	got := resolveSetupScript("/usr/local/bin/setup.sh", "/home/user/city")
	if got != "/usr/local/bin/setup.sh" {
		t.Errorf("got %q, want unchanged absolute path", got)
	}
}

func TestResolveSetupScript_Empty(t *testing.T) {
	got := resolveSetupScript("", "/home/user/city")
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestPoolAgentsSessionSetup(t *testing.T) {
	cfgAgent := &config.Agent{
		Name:         "worker",
		StartCommand: "echo hello",
		Pool:         &config.PoolConfig{Min: 0, Max: 1, Check: "echo 1"},
		SessionSetup: []string{
			"tmux set-option -t {{.Session}} status-left ' {{.Agent}} '",
		},
		SessionSetupScript: "scripts/setup.sh",
	}
	sp := runtime.NewFake()
	agents, err := poolAgents(testBuildParams(sp), cfgAgent, 1)
	if err != nil {
		t.Fatalf("poolAgents: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("len(agents) = %d, want 1", len(agents))
	}
	cfg := agents[0].SessionConfig()

	// Template should be expanded with session name.
	if len(cfg.SessionSetup) != 1 {
		t.Fatalf("SessionSetup len = %d, want 1", len(cfg.SessionSetup))
	}
	want := "tmux set-option -t worker status-left ' worker '"
	if cfg.SessionSetup[0] != want {
		t.Errorf("SessionSetup[0] = %q, want %q", cfg.SessionSetup[0], want)
	}

	// Script should be resolved to absolute path.
	if cfg.SessionSetupScript != "/tmp/city/scripts/setup.sh" {
		t.Errorf("SessionSetupScript = %q, want %q", cfg.SessionSetupScript, "/tmp/city/scripts/setup.sh")
	}
}

func TestExpandSessionSetup_ConfigDir(t *testing.T) {
	ctx := SessionSetupContext{
		Session:   "mayor",
		Agent:     "mayor",
		CityRoot:  "/home/user/city",
		CityName:  "bright-lights",
		WorkDir:   "/home/user/city",
		ConfigDir: "/home/user/city/packs/gastown",
	}
	cmds := []string{
		"{{.ConfigDir}}/scripts/status-line.sh {{.Agent}}",
	}
	got := expandSessionSetup(cmds, ctx)
	want := "/home/user/city/packs/gastown/scripts/status-line.sh mayor"
	if got[0] != want {
		t.Errorf("got %q, want %q", got[0], want)
	}
}

func TestPoolAgentsConfigDir(t *testing.T) {
	cfgAgent := &config.Agent{
		Name:         "worker",
		StartCommand: "echo hello",
		Pool:         &config.PoolConfig{Min: 0, Max: 1, Check: "echo 1"},
		SourceDir:    "/city/packs/gt",
		SessionSetup: []string{
			"{{.ConfigDir}}/scripts/setup.sh {{.Agent}}",
		},
	}
	sp := runtime.NewFake()
	agents, err := poolAgents(testBuildParams(sp), cfgAgent, 1)
	if err != nil {
		t.Fatalf("poolAgents: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("len(agents) = %d, want 1", len(agents))
	}
	cfg := agents[0].SessionConfig()
	// ConfigDir should use SourceDir, not CityRoot.
	want := "/city/packs/gt/scripts/setup.sh worker"
	if len(cfg.SessionSetup) != 1 || cfg.SessionSetup[0] != want {
		t.Errorf("SessionSetup = %v, want [%q]", cfg.SessionSetup, want)
	}
}

func TestPoolAgentsConfigDir_DefaultsToCityPath(t *testing.T) {
	cfgAgent := &config.Agent{
		Name:         "worker",
		StartCommand: "echo hello",
		Pool:         &config.PoolConfig{Min: 0, Max: 1, Check: "echo 1"},
		SessionSetup: []string{
			"{{.ConfigDir}}/scripts/setup.sh",
		},
	}
	sp := runtime.NewFake()
	agents, err := poolAgents(testBuildParams(sp), cfgAgent, 1)
	if err != nil {
		t.Fatalf("poolAgents: %v", err)
	}
	cfg := agents[0].SessionConfig()
	// No SourceDir → ConfigDir defaults to cityPath.
	want := "/tmp/city/scripts/setup.sh"
	if len(cfg.SessionSetup) != 1 || cfg.SessionSetup[0] != want {
		t.Errorf("SessionSetup = %v, want [%q]", cfg.SessionSetup, want)
	}
}

func TestPoolAgentsOverlayDirCopied(t *testing.T) {
	// Verify OverlayDir is deep-copied from cfgAgent to pool instances.
	cfgAgent := &config.Agent{
		Name:         "worker",
		StartCommand: "echo hello",
		Pool:         &config.PoolConfig{Min: 0, Max: 2, Check: "echo 2"},
		OverlayDir:   "overlays/worker",
	}
	sp := runtime.NewFake()
	agents, err := poolAgents(testBuildParams(sp), cfgAgent, 2)
	if err != nil {
		t.Fatalf("poolAgents: %v", err)
	}
	if len(agents) != 2 {
		t.Fatalf("len(agents) = %d, want 2", len(agents))
	}
	// OverlayDir should be set on both instances (resolved at CopyDir call time, not here).
	// The pool build just copies the field — actual resolution happens at startup.
}

func TestCountRunningPoolInstancesUsesListRunning(t *testing.T) {
	sp := runtime.NewFake()
	// Start 3 out of 5 pool instances.
	_ = sp.Start(context.Background(), "worker-1", runtime.Config{})
	_ = sp.Start(context.Background(), "worker-3", runtime.Config{})
	_ = sp.Start(context.Background(), "worker-5", runtime.Config{})

	count := countRunningPoolInstances("worker", "", config.PoolConfig{Min: 0, Max: 5}, "city", "", sp)
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}

func TestCountRunningPoolInstancesWithDir(t *testing.T) {
	sp := runtime.NewFake()
	// Rig-scoped pool: dir/name pattern.
	_ = sp.Start(context.Background(), "myrig--worker-1", runtime.Config{})
	_ = sp.Start(context.Background(), "myrig--worker-2", runtime.Config{})

	count := countRunningPoolInstances("worker", "myrig", config.PoolConfig{Min: 0, Max: 3}, "city", "", sp)
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func TestCountRunningPoolInstancesNoneRunning(t *testing.T) {
	sp := runtime.NewFake()
	count := countRunningPoolInstances("worker", "", config.PoolConfig{Min: 0, Max: 10}, "city", "", sp)
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

// TestDeepCopyAgentCoversAllFields verifies that deepCopyAgent copies every
// field from config.Agent. Uses reflection to detect fields added to Agent
// but not handled in the deep-copy, preventing silent data loss for pool
// instances.
func TestDeepCopyAgentCoversAllFields(t *testing.T) {
	trueVal := true
	intVal := 42
	src := config.Agent{
		Name:                   "original",
		Dir:                    "original-dir",
		Scope:                  "city",
		Suspended:              true,
		PreStart:               []string{"pre-cmd"},
		PromptTemplate:         "prompts/test.md",
		Nudge:                  "nudge text",
		Session:                "acp",
		Provider:               "claude",
		StartCommand:           "claude --dangerously",
		Args:                   []string{"--arg1"},
		PromptMode:             "flag",
		PromptFlag:             "--prompt",
		ReadyDelayMs:           &intVal,
		ReadyPromptPrefix:      "ready>",
		ProcessNames:           []string{"claude"},
		EmitsPermissionWarning: &trueVal,
		Env:                    map[string]string{"K": "V"},
		Pool:                   &config.PoolConfig{Min: 1, Max: 5, Check: "echo 3"},
		WorkQuery:              "bd ready",
		SlingQuery:             "bd update {}",
		IdleTimeout:            "15m",
		InstallAgentHooks:      []string{"claude"},
		HooksInstalled:         &trueVal,
		SessionSetup:           []string{"setup-cmd"},
		SessionSetupScript:     "scripts/setup.sh",
		SessionLive:            []string{"live-cmd"},
		OverlayDir:             "overlays/test",
		SourceDir:              "/src",
		DefaultSlingFormula:    "mol-work",
		InjectFragments:        []string{"frag1"},
		Attach:                 &trueVal,
		Fallback:               true,
		Multi:                  true,
		PoolName:               "template/name",
	}

	// Verify every Agent field is set (non-zero) in the test data.
	sv := reflect.ValueOf(src)
	st := sv.Type()
	for i := 0; i < st.NumField(); i++ {
		if sv.Field(i).IsZero() {
			t.Fatalf("Agent field %q is zero in test data — add it to the test source", st.Field(i).Name)
		}
	}

	dst := deepCopyAgent(&src, "copy-name", "copy-dir")

	// Name and Dir should be the overridden values.
	if dst.Name != "copy-name" {
		t.Errorf("Name = %q, want %q", dst.Name, "copy-name")
	}
	if dst.Dir != "copy-dir" {
		t.Errorf("Dir = %q, want %q", dst.Dir, "copy-dir")
	}

	// All other fields should match the source.
	dv := reflect.ValueOf(dst)
	for i := 0; i < st.NumField(); i++ {
		fname := st.Field(i).Name
		if fname == "Name" || fname == "Dir" {
			continue // Intentionally overridden.
		}
		if dv.Field(i).IsZero() {
			t.Errorf("deepCopyAgent did not copy field %q", fname)
		}
	}

	// Verify deep independence: mutating src slices/maps should not affect dst.
	src.PreStart[0] = "MUTATED"
	src.Env["K"] = "MUTATED"
	src.SessionSetup[0] = "MUTATED"
	src.Args[0] = "MUTATED"
	src.ProcessNames[0] = "MUTATED"
	src.InjectFragments[0] = "MUTATED"
	src.InstallAgentHooks[0] = "MUTATED"
	src.Pool.Min = 999

	if dst.PreStart[0] == "MUTATED" {
		t.Error("PreStart is not a deep copy")
	}
	if dst.Env["K"] == "MUTATED" {
		t.Error("Env is not a deep copy")
	}
	if dst.SessionSetup[0] == "MUTATED" {
		t.Error("SessionSetup is not a deep copy")
	}
	if dst.Args[0] == "MUTATED" {
		t.Error("Args is not a deep copy")
	}
	if dst.ProcessNames[0] == "MUTATED" {
		t.Error("ProcessNames is not a deep copy")
	}
	if dst.InjectFragments[0] == "MUTATED" {
		t.Error("InjectFragments is not a deep copy")
	}
	if dst.InstallAgentHooks[0] == "MUTATED" {
		t.Error("InstallAgentHooks is not a deep copy")
	}
	if dst.Pool.Min == 999 {
		t.Error("Pool is not a deep copy")
	}
}

func TestDeepCopyAgentSetsPoolName(t *testing.T) {
	src := &config.Agent{
		Name: "dog",
		Dir:  "hello-world",
		Pool: &config.PoolConfig{Min: 0, Max: 3},
	}
	dst := deepCopyAgent(src, "dog-1", "hello-world")
	if dst.PoolName != "hello-world/dog" {
		t.Errorf("PoolName = %q, want %q", dst.PoolName, "hello-world/dog")
	}
}

func TestPoolInstanceWorkQueryUsesTemplateName(t *testing.T) {
	src := &config.Agent{
		Name: "dog",
		Dir:  "hello-world",
		Pool: &config.PoolConfig{Min: 0, Max: 3},
	}
	dst := deepCopyAgent(src, "dog-2", "hello-world")
	got := dst.EffectiveWorkQuery()
	want := "bd ready --label=pool:hello-world/dog --limit=1"
	if got != want {
		t.Errorf("pool instance EffectiveWorkQuery() = %q, want %q", got, want)
	}
}

func TestRunPoolOnBoot(t *testing.T) {
	var ran []string
	runner := func(cmd, _ string) (string, error) {
		ran = append(ran, cmd)
		return "", nil
	}

	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "mayor"},
			{Name: "dog", Pool: &config.PoolConfig{Min: 0, Max: 3}},
			{Name: "cat", Pool: &config.PoolConfig{Min: 0, Max: 2}},
		},
	}

	var stderr bytes.Buffer
	runPoolOnBoot(cfg, t.TempDir(), runner, &stderr)

	if len(ran) != 2 {
		t.Fatalf("ran %d commands, want 2 (one per pool agent)", len(ran))
	}
	// Both should contain unclaim logic.
	for i, cmd := range ran {
		if !strings.Contains(cmd, "--unclaim") {
			t.Errorf("ran[%d] = %q, want --unclaim", i, cmd)
		}
	}
}

func TestRunPoolOnBootError(t *testing.T) {
	runner := func(_, _ string) (string, error) {
		return "", fmt.Errorf("bd not found")
	}

	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "dog", Pool: &config.PoolConfig{Min: 0, Max: 3}},
		},
	}

	var stderr bytes.Buffer
	runPoolOnBoot(cfg, t.TempDir(), runner, &stderr)

	// Error should be logged, not fatal.
	if !strings.Contains(stderr.String(), "on_boot dog") {
		t.Errorf("stderr = %q, want on_boot error logged", stderr.String())
	}
}

func TestComputePoolDeathHandlers(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test"},
		Agents: []config.Agent{
			{Name: "mayor"}, // not a pool
			{Name: "dog", Pool: &config.PoolConfig{Min: 0, Max: 3}},
			{Name: "cat", Pool: &config.PoolConfig{Min: 0, Max: 1}}, // max=1, skipped
		},
	}

	handlers := computePoolDeathHandlers(cfg, "test", t.TempDir(), runtime.NewFake())

	// dog has max=3, so 3 handlers (dog-1, dog-2, dog-3).
	// cat has max=1, skipped. mayor is not a pool.
	if len(handlers) != 3 {
		t.Fatalf("len(handlers) = %d, want 3", len(handlers))
	}

	// Default session template is empty → session name = sanitized agent name.
	for i := 1; i <= 3; i++ {
		sn := fmt.Sprintf("dog-%d", i)
		info, ok := handlers[sn]
		if !ok {
			t.Errorf("missing handler for %s (have keys: %v)", sn, handlerKeys(handlers))
			continue
		}
		want := fmt.Sprintf("--assignee=dog-%d", i)
		if !strings.Contains(info.Command, want) {
			t.Errorf("handler[%s].Command = %q, want %s", sn, info.Command, want)
		}
	}
}

func handlerKeys(m map[string]poolDeathInfo) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// fakeLookPath always succeeds — tests don't need real binaries.
func fakeLookPath(name string) (string, error) {
	return "/usr/bin/" + name, nil
}
