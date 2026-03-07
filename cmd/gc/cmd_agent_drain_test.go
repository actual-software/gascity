package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/runtime"
)

// fakeDrainOps is a test double for drainOps.
type fakeDrainOps struct {
	draining         map[string]bool
	drainTimes       map[string]time.Time // when drain was set
	acked            map[string]bool
	restartRequested map[string]bool
	driftRestart     map[string]bool
	err              error // injected error for all ops
	setDrainCalls    []string
	clearDrainCalls  []string
}

func newFakeDrainOps() *fakeDrainOps {
	return &fakeDrainOps{
		draining:         make(map[string]bool),
		drainTimes:       make(map[string]time.Time),
		acked:            make(map[string]bool),
		restartRequested: make(map[string]bool),
		driftRestart:     make(map[string]bool),
	}
}

func (f *fakeDrainOps) setDrain(sessionName string) error {
	f.setDrainCalls = append(f.setDrainCalls, sessionName)
	if f.err != nil {
		return f.err
	}
	f.draining[sessionName] = true
	f.drainTimes[sessionName] = time.Now()
	return nil
}

func (f *fakeDrainOps) clearDrain(sessionName string) error {
	f.clearDrainCalls = append(f.clearDrainCalls, sessionName)
	if f.err != nil {
		return f.err
	}
	delete(f.draining, sessionName)
	delete(f.drainTimes, sessionName)
	return nil
}

func (f *fakeDrainOps) isDraining(sessionName string) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return f.draining[sessionName], nil
}

func (f *fakeDrainOps) drainStartTime(sessionName string) (time.Time, error) {
	if f.err != nil {
		return time.Time{}, f.err
	}
	t, ok := f.drainTimes[sessionName]
	if !ok {
		return time.Time{}, fmt.Errorf("no drain time for %s", sessionName)
	}
	return t, nil
}

func (f *fakeDrainOps) setDrainAck(sessionName string) error {
	if f.err != nil {
		return f.err
	}
	f.acked[sessionName] = true
	return nil
}

func (f *fakeDrainOps) isDrainAcked(sessionName string) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return f.acked[sessionName], nil
}

func (f *fakeDrainOps) setRestartRequested(sessionName string) error {
	if f.err != nil {
		return f.err
	}
	f.restartRequested[sessionName] = true
	return nil
}

func (f *fakeDrainOps) isRestartRequested(sessionName string) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return f.restartRequested[sessionName], nil
}

func (f *fakeDrainOps) clearRestartRequested(sessionName string) error {
	if f.err != nil {
		return f.err
	}
	delete(f.restartRequested, sessionName)
	return nil
}

func (f *fakeDrainOps) setDriftRestart(sessionName string) error {
	if f.err != nil {
		return f.err
	}
	f.driftRestart[sessionName] = true
	return nil
}

func (f *fakeDrainOps) isDriftRestart(sessionName string) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return f.driftRestart[sessionName], nil
}

func (f *fakeDrainOps) clearDriftRestart(sessionName string) error {
	if f.err != nil {
		return f.err
	}
	delete(f.driftRestart, sessionName)
	return nil
}

// ---------------------------------------------------------------------------
// doAgentDrain tests
// ---------------------------------------------------------------------------

func TestDoAgentDrain(t *testing.T) {
	dops := newFakeDrainOps()
	sp := runtime.NewFake()
	if err := sp.Start(context.Background(), "worker", runtime.Config{Command: "echo"}); err != nil {
		t.Fatal(err)
	}

	rec := events.NewFake()
	var stdout, stderr bytes.Buffer
	code := doAgentDrain(dops, sp, rec, "worker", "worker", &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr: %s", code, stderr.String())
	}
	if !dops.draining["worker"] {
		t.Error("drain flag not set")
	}
	if got := stdout.String(); got != "Draining agent 'worker'\n" {
		t.Errorf("stdout = %q, want %q", got, "Draining agent 'worker'\n")
	}
	if len(rec.Events) != 1 || rec.Events[0].Type != events.AgentDraining {
		t.Errorf("events = %v, want one AgentDraining event", rec.Events)
	}
	if rec.Events[0].Subject != "worker" {
		t.Errorf("event subject = %q, want %q", rec.Events[0].Subject, "worker")
	}
}

func TestDoAgentDrainNotRunning(t *testing.T) {
	dops := newFakeDrainOps()
	sp := runtime.NewFake() // no sessions started

	var stdout, stderr bytes.Buffer
	code := doAgentDrain(dops, sp, events.Discard, "worker", "worker", &stdout, &stderr)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if got := stderr.String(); got != "gc agent drain: agent \"worker\" is not running\n" {
		t.Errorf("stderr = %q", got)
	}
}

func TestDoAgentDrainSetError(t *testing.T) {
	dops := newFakeDrainOps()
	dops.err = errors.New("tmux borked")
	sp := runtime.NewFake()
	if err := sp.Start(context.Background(), "worker", runtime.Config{Command: "echo"}); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := doAgentDrain(dops, sp, events.Discard, "worker", "worker", &stdout, &stderr)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if got := stderr.String(); got != "gc agent drain: tmux borked\n" {
		t.Errorf("stderr = %q", got)
	}
}

// ---------------------------------------------------------------------------
// doAgentUndrain tests
// ---------------------------------------------------------------------------

func TestDoAgentUndrain(t *testing.T) {
	dops := newFakeDrainOps()
	dops.draining["worker"] = true
	sp := runtime.NewFake()
	if err := sp.Start(context.Background(), "worker", runtime.Config{Command: "echo"}); err != nil {
		t.Fatal(err)
	}

	rec := events.NewFake()
	var stdout, stderr bytes.Buffer
	code := doAgentUndrain(dops, sp, rec, "worker", "worker", &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr: %s", code, stderr.String())
	}
	if dops.draining["worker"] {
		t.Error("drain flag still set after undrain")
	}
	if got := stdout.String(); got != "Undrained agent 'worker'\n" {
		t.Errorf("stdout = %q, want %q", got, "Undrained agent 'worker'\n")
	}
	if len(rec.Events) != 1 || rec.Events[0].Type != events.AgentUndrained {
		t.Errorf("events = %v, want one AgentUndrained event", rec.Events)
	}
}

func TestDoAgentUndrainNotRunning(t *testing.T) {
	dops := newFakeDrainOps()
	sp := runtime.NewFake()

	var stdout, stderr bytes.Buffer
	code := doAgentUndrain(dops, sp, events.Discard, "worker", "worker", &stdout, &stderr)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if got := stderr.String(); got != "gc agent undrain: agent \"worker\" is not running\n" {
		t.Errorf("stderr = %q", got)
	}
}

// ---------------------------------------------------------------------------
// doAgentDrainCheck tests
// ---------------------------------------------------------------------------

func TestDoAgentDrainCheck(t *testing.T) {
	dops := newFakeDrainOps()
	dops.draining["worker"] = true

	code := doAgentDrainCheck(dops, "worker")
	if code != 0 {
		t.Errorf("code = %d, want 0 (draining)", code)
	}
}

func TestDoAgentDrainCheckNotDraining(t *testing.T) {
	dops := newFakeDrainOps()

	code := doAgentDrainCheck(dops, "worker")
	if code != 1 {
		t.Errorf("code = %d, want 1 (not draining)", code)
	}
}

func TestDoAgentDrainCheckError(t *testing.T) {
	dops := newFakeDrainOps()
	dops.err = errors.New("tmux gone")

	code := doAgentDrainCheck(dops, "worker")
	if code != 1 {
		t.Errorf("code = %d, want 1 (error → not draining)", code)
	}
}

// ---------------------------------------------------------------------------
// doAgentDrainAck tests
// ---------------------------------------------------------------------------

func TestDoAgentDrainAck(t *testing.T) {
	dops := newFakeDrainOps()
	var stdout, stderr bytes.Buffer
	code := doAgentDrainAck(dops, "worker", &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, want 0; stderr: %s", code, stderr.String())
	}
	if !dops.acked["worker"] {
		t.Error("drain ack flag not set")
	}
	if got := stdout.String(); got != "Drain acknowledged. Controller will stop this session.\n" {
		t.Errorf("stdout = %q", got)
	}
}

func TestDoAgentDrainAckError(t *testing.T) {
	dops := newFakeDrainOps()
	dops.err = errors.New("tmux borked")
	var stdout, stderr bytes.Buffer
	code := doAgentDrainAck(dops, "worker", &stdout, &stderr)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if got := stderr.String(); got != "gc agent drain-ack: tmux borked\n" {
		t.Errorf("stderr = %q", got)
	}
}

// ---------------------------------------------------------------------------
// newDrainOps factory tests
// ---------------------------------------------------------------------------

func TestNewDrainOpsAlwaysReturnsNonNil(t *testing.T) {
	// newDrainOps works with any Provider — no type assertions.
	fp := runtime.NewFake()
	dops := newDrainOps(fp)
	if dops == nil {
		t.Fatal("newDrainOps(Fake) = nil, want non-nil")
	}
	if _, ok := dops.(*providerDrainOps); !ok {
		t.Errorf("newDrainOps returned %T, want *providerDrainOps", dops)
	}
}

func TestProviderDrainOpsRoundTrip(t *testing.T) {
	// Verify drain ops work through Provider meta interface.
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "worker", runtime.Config{})
	dops := newDrainOps(sp)

	// Not draining initially.
	draining, _ := dops.isDraining("worker")
	if draining {
		t.Error("should not be draining initially")
	}

	// Set drain.
	if err := dops.setDrain("worker"); err != nil {
		t.Fatalf("setDrain: %v", err)
	}
	draining, _ = dops.isDraining("worker")
	if !draining {
		t.Error("should be draining after setDrain")
	}

	// Drain start time should be parseable.
	ts, err := dops.drainStartTime("worker")
	if err != nil {
		t.Fatalf("drainStartTime: %v", err)
	}
	if ts.IsZero() {
		t.Error("drain start time should not be zero")
	}

	// Set and check ack.
	if err := dops.setDrainAck("worker"); err != nil {
		t.Fatalf("setDrainAck: %v", err)
	}
	acked, _ := dops.isDrainAcked("worker")
	if !acked {
		t.Error("should be acked after setDrainAck")
	}

	// Clear drain (also clears ack).
	if err := dops.clearDrain("worker"); err != nil {
		t.Fatalf("clearDrain: %v", err)
	}
	draining, _ = dops.isDraining("worker")
	if draining {
		t.Error("should not be draining after clearDrain")
	}
	acked, _ = dops.isDrainAcked("worker")
	if acked {
		t.Error("ack should be cleared after clearDrain")
	}
}

// ---------------------------------------------------------------------------
// doAgentRequestRestart tests
// ---------------------------------------------------------------------------

func TestDoAgentRequestRestartError(t *testing.T) {
	dops := newFakeDrainOps()
	dops.err = errors.New("tmux borked")
	var stdout, stderr bytes.Buffer
	code := doAgentRequestRestart(dops, events.Discard, "worker", "worker", &stdout, &stderr)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if got := stderr.String(); got != "gc agent request-restart: tmux borked\n" {
		t.Errorf("stderr = %q", got)
	}
}

func TestRequestRestartAcceptsNoArgs(t *testing.T) {
	// Verify the cobra command accepts no args.
	var stdout, stderr bytes.Buffer
	cmd := newAgentRequestRestartCmd(&stdout, &stderr)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	t.Setenv("GC_AGENT", "")
	t.Setenv("GC_CITY", "")
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Error("request-restart with no env should return non-zero")
	}
	if !strings.Contains(stderr.String(), "not in agent context") {
		t.Errorf("stderr = %q, want 'not in agent context' error", stderr.String())
	}
}

func TestProviderDrainOpsRestartRequestedRoundTrip(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "worker", runtime.Config{})
	dops := newDrainOps(sp)

	// Not requested initially.
	requested, _ := dops.isRestartRequested("worker")
	if requested {
		t.Error("should not be restart-requested initially")
	}

	// Set restart requested.
	if err := dops.setRestartRequested("worker"); err != nil {
		t.Fatalf("setRestartRequested: %v", err)
	}
	requested, _ = dops.isRestartRequested("worker")
	if !requested {
		t.Error("should be restart-requested after set")
	}

	// Clear restart requested.
	if err := dops.clearRestartRequested("worker"); err != nil {
		t.Fatalf("clearRestartRequested: %v", err)
	}
	requested, _ = dops.isRestartRequested("worker")
	if requested {
		t.Error("should not be restart-requested after clear")
	}
}

func TestProviderDrainOpsDriftRestartRoundTrip(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "worker", runtime.Config{})
	dops := newDrainOps(sp)

	// Not drift-restart initially.
	isDrift, _ := dops.isDriftRestart("worker")
	if isDrift {
		t.Error("should not be drift-restart initially")
	}

	// Set drift restart.
	if err := dops.setDriftRestart("worker"); err != nil {
		t.Fatalf("setDriftRestart: %v", err)
	}
	isDrift, _ = dops.isDriftRestart("worker")
	if !isDrift {
		t.Error("should be drift-restart after set")
	}

	// Clear drift restart.
	if err := dops.clearDriftRestart("worker"); err != nil {
		t.Fatalf("clearDriftRestart: %v", err)
	}
	isDrift, _ = dops.isDriftRestart("worker")
	if isDrift {
		t.Error("should not be drift-restart after clear")
	}
}

// ---------------------------------------------------------------------------
// newAgentDrainCheckCmd / newAgentDrainAckCmd arg acceptance tests
// ---------------------------------------------------------------------------

func TestDrainCheckAcceptsPositionalArg(t *testing.T) {
	// Verify cobra allows 0 or 1 positional arg (no longer NoArgs).
	// The command will fail at runtime (no city), but it should NOT fail
	// with "unknown command" or "accepts 0 arg(s)" errors.
	var stdout, stderr bytes.Buffer
	cmd := newAgentDrainCheckCmd(&stdout, &stderr)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"polecat"})
	err := cmd.Execute()
	// Expect a runtime error (no city dir), NOT an arg-count error.
	if err != nil {
		// errExit is expected — the command runs but fails to find a city.
		// What we're testing is that cobra didn't reject the arg.
		if stderr.String() != "" && strings.Contains(stderr.String(), "accepts 0 arg") {
			t.Errorf("drain-check should accept positional arg, got: %s", stderr.String())
		}
	}
}

func TestDrainCheckNoArgsStillWorks(t *testing.T) {
	// Without args and without env vars, drain-check returns exit 1 silently.
	var stdout, stderr bytes.Buffer
	cmd := newAgentDrainCheckCmd(&stdout, &stderr)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	// Ensure env vars are not set (in case test environment has them).
	t.Setenv("GC_AGENT", "")
	t.Setenv("GC_CITY", "")
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Error("drain-check with no args and no env should return non-zero")
	}
	// Should be silent — no error message on stderr.
	if stderr.Len() > 0 {
		t.Errorf("drain-check should be silent without env vars, got stderr: %q", stderr.String())
	}
}

func TestDrainAckAcceptsPositionalArg(t *testing.T) {
	// Same pattern as drain-check: verify cobra allows positional arg.
	var stdout, stderr bytes.Buffer
	cmd := newAgentDrainAckCmd(&stdout, &stderr)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"polecat"})
	err := cmd.Execute()
	if err != nil {
		if stderr.String() != "" && strings.Contains(stderr.String(), "accepts 0 arg") {
			t.Errorf("drain-ack should accept positional arg, got: %s", stderr.String())
		}
	}
}

func TestDrainAckNoArgsErrorMessage(t *testing.T) {
	// Without args and without env vars, drain-ack prints an error message.
	var stdout, stderr bytes.Buffer
	cmd := newAgentDrainAckCmd(&stdout, &stderr)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	t.Setenv("GC_AGENT", "")
	t.Setenv("GC_CITY", "")
	cmd.SetArgs([]string{})
	err := cmd.Execute()
	if err == nil {
		t.Error("drain-ack with no args and no env should return non-zero")
	}
	if !strings.Contains(stderr.String(), "not in agent context") {
		t.Errorf("stderr = %q, want 'not in agent context' error", stderr.String())
	}
}

// ---------------------------------------------------------------------------
// resolveAgentIdentity / findAgentByQualified unit tests
// ---------------------------------------------------------------------------

func TestResolveAgentIdentity(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "mayor"},
			{Name: "worker", Pool: &config.PoolConfig{Min: 0, Max: 5, Check: "echo 3"}},
			{Name: "singleton", Pool: &config.PoolConfig{Min: 0, Max: 1, Check: "echo 1"}},
		},
	}

	tests := []struct {
		name      string
		lookup    string
		wantFound bool
		wantName  string
		wantPool  bool // whether returned agent should have Pool set
	}{
		// Exact match on non-pool agent.
		{"exact match", "mayor", true, "mayor", false},

		// Exact match on pool agent (base name).
		{"pool base name", "worker", true, "worker", true},

		// Pool instance matches.
		{"pool instance worker-1", "worker-1", true, "worker-1", false},
		{"pool instance worker-5", "worker-5", true, "worker-5", false},

		// Pool instance out of range (too high).
		{"pool instance worker-6", "worker-6", false, "", false},

		// Pool instance out of range (zero).
		{"pool instance worker-0", "worker-0", false, "", false},

		// Pool instance non-numeric suffix.
		{"pool instance worker-abc", "worker-abc", false, "", false},

		// Pool instance negative (parsed as non-numeric due to dash).
		{"pool instance worker--1", "worker--1", false, "", false},

		// Max=1 pool: the guard requires Max > 1, so {name}-1 does NOT match.
		{"singleton-1 no match", "singleton-1", false, "", false},

		// Nonexistent agent.
		{"nonexistent", "nobody", false, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := resolveAgentIdentity(cfg, tt.lookup, "")
			if found != tt.wantFound {
				t.Fatalf("resolveAgentIdentity(%q) found = %v, want %v", tt.lookup, found, tt.wantFound)
			}
			if !found {
				return
			}
			if got.Name != tt.wantName {
				t.Errorf("agent.Name = %q, want %q", got.Name, tt.wantName)
			}
			if (got.Pool != nil) != tt.wantPool {
				t.Errorf("agent.Pool != nil = %v, want %v", got.Pool != nil, tt.wantPool)
			}
		})
	}
}

func TestResolveAgentIdentityQualified(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "mayor"},
			{Name: "polecat", Dir: "frontend"},
			{Name: "polecat", Dir: "backend"},
			{Name: "worker", Dir: "frontend", Pool: &config.PoolConfig{Min: 0, Max: 3, Check: "echo 2"}},
			{Name: "coder", Dir: "backend", Pool: &config.PoolConfig{Min: 0, Max: -1}},
		},
	}

	tests := []struct {
		name       string
		input      string
		rigContext string
		wantFound  bool
		wantDir    string
		wantName   string
	}{
		// City-wide literal.
		{"city-wide literal", "mayor", "", true, "", "mayor"},
		// Qualified literal.
		{"qualified frontend/polecat", "frontend/polecat", "", true, "frontend", "polecat"},
		{"qualified backend/polecat", "backend/polecat", "", true, "backend", "polecat"},
		// Bare name with rig context.
		{"bare polecat + frontend context", "polecat", "frontend", true, "frontend", "polecat"},
		{"bare polecat + backend context", "polecat", "backend", true, "backend", "polecat"},
		// Bare name with no matching context — ambiguous (2 polecats), not found.
		{"bare polecat no context", "polecat", "", false, "", ""},
		// Pool instance with qualified name.
		{"qualified pool frontend/worker-2", "frontend/worker-2", "", true, "frontend", "worker-2"},
		// Pool instance with rig context.
		{"bare pool worker-1 + frontend context", "worker-1", "frontend", true, "frontend", "worker-1"},
		// Step 3: unambiguous bare name — worker is unique across all agents.
		{"unambiguous bare worker", "worker", "", true, "frontend", "worker"},
		// Step 3: unambiguous pool instance via bare name.
		{"unambiguous bare worker-2", "worker-2", "", true, "frontend", "worker-2"},
		// Unlimited pool: qualified lookup.
		{"qualified unlimited backend/coder-5", "backend/coder-5", "", true, "backend", "coder-5"},
		// Unlimited pool: unambiguous bare name.
		{"unambiguous bare coder-42", "coder-42", "", true, "backend", "coder-42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := resolveAgentIdentity(cfg, tt.input, tt.rigContext)
			if found != tt.wantFound {
				t.Fatalf("resolveAgentIdentity(%q, %q) found = %v, want %v", tt.input, tt.rigContext, found, tt.wantFound)
			}
			if !found {
				return
			}
			if got.Dir != tt.wantDir {
				t.Errorf("Dir = %q, want %q", got.Dir, tt.wantDir)
			}
			if got.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", got.Name, tt.wantName)
			}
		})
	}
}

func TestResolveAgentIdentityUnambiguous(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "mayor"},
			{Name: "polecat", Dir: "myrig"},
			{Name: "builder", Dir: "frontend", Pool: &config.PoolConfig{Min: 0, Max: 3, Check: "echo 2"}},
			{Name: "coder", Dir: "backend", Pool: &config.PoolConfig{Min: 0, Max: -1}},
		},
	}

	tests := []struct {
		name      string
		input     string
		wantFound bool
		wantDir   string
		wantName  string
	}{
		// Unique bare name resolves via step 3.
		{"unambiguous polecat", "polecat", true, "myrig", "polecat"},
		// Unique pool template resolves via step 3.
		{"unambiguous builder", "builder", true, "frontend", "builder"},
		// Pool instance, unambiguous.
		{"unambiguous builder-2", "builder-2", true, "frontend", "builder-2"},
		// Pool instance out of range.
		{"builder-4 out of range", "builder-4", false, "", ""},
		// City-wide agent resolves via step 1, before step 3.
		{"mayor step 1", "mayor", true, "", "mayor"},
		// Unlimited pool: bare name resolves.
		{"unlimited pool bare", "coder", true, "backend", "coder"},
		// Unlimited pool: any instance number resolves.
		{"unlimited pool instance", "coder-99", true, "backend", "coder-99"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := resolveAgentIdentity(cfg, tt.input, "")
			if found != tt.wantFound {
				t.Fatalf("resolveAgentIdentity(%q) found = %v, want %v", tt.input, found, tt.wantFound)
			}
			if !found {
				return
			}
			if got.Dir != tt.wantDir {
				t.Errorf("Dir = %q, want %q", got.Dir, tt.wantDir)
			}
			if got.Name != tt.wantName {
				t.Errorf("Name = %q, want %q", got.Name, tt.wantName)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// findAgentByName unit tests (pool suffix stripping for gc prime)
// ---------------------------------------------------------------------------

func TestFindAgentByNameExact(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "mayor"},
			{Name: "polecat", Dir: "frontend"},
		},
	}
	a, ok := findAgentByName(cfg, "mayor")
	if !ok {
		t.Fatal("expected to find mayor")
	}
	if a.Name != "mayor" {
		t.Errorf("Name = %q, want %q", a.Name, "mayor")
	}
}

func TestFindAgentByNamePoolInstance(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "polecat", Dir: "frontend", Pool: &config.PoolConfig{Min: 0, Max: 5, Check: "echo 3"}},
		},
	}
	// "polecat-3" should strip suffix and match "polecat" pool.
	a, ok := findAgentByName(cfg, "polecat-3")
	if !ok {
		t.Fatal("expected to find polecat via pool suffix stripping")
	}
	if a.Name != "polecat" {
		t.Errorf("Name = %q, want %q", a.Name, "polecat")
	}
}

func TestFindAgentByNamePoolOutOfRange(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "polecat", Pool: &config.PoolConfig{Min: 0, Max: 3, Check: "echo 2"}},
		},
	}
	// "polecat-4" is out of range (max=3).
	_, ok := findAgentByName(cfg, "polecat-4")
	if ok {
		t.Error("polecat-4 should not match pool with max=3")
	}
}

func TestFindAgentByNameSingletonPoolNoMatch(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "singleton", Pool: &config.PoolConfig{Min: 0, Max: 1, Check: "echo 1"}},
		},
	}
	// Max=1 pools don't get instance suffixes.
	_, ok := findAgentByName(cfg, "singleton-1")
	if ok {
		t.Error("singleton-1 should not match pool with max=1")
	}
}

func TestFindAgentByNameUnlimitedPool(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "polecat", Dir: "myrig", Pool: &config.PoolConfig{Min: 0, Max: -1}},
		},
	}
	// Any instance number should match an unlimited pool.
	a, ok := findAgentByName(cfg, "polecat-99")
	if !ok {
		t.Fatal("expected to find polecat-99 in unlimited pool")
	}
	if a.Name != "polecat" {
		t.Errorf("Name = %q, want %q", a.Name, "polecat")
	}
}

func TestFindAgentByNameNoMatch(t *testing.T) {
	cfg := &config.City{
		Agents: []config.Agent{
			{Name: "mayor"},
		},
	}
	_, ok := findAgentByName(cfg, "nobody")
	if ok {
		t.Error("expected no match for nonexistent agent")
	}
}
