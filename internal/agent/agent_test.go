package agent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
)

func TestSessionNameFor(t *testing.T) {
	tests := []struct {
		city  string
		agent string
		want  string
	}{
		{"bright-lights", "mayor", "mayor"},
		{"bright-lights", "hello-world/polecat", "hello-world--polecat"},
		{"bright-lights", "backend/worker-1", "backend--worker-1"},
		{"city", "worker-3", "worker-3"},
	}
	for _, tt := range tests {
		got := SessionNameFor(tt.city, tt.agent, "")
		if got != tt.want {
			t.Errorf("SessionNameFor(%q, %q, \"\") = %q, want %q", tt.city, tt.agent, got, tt.want)
		}
	}
}

func TestSessionNameForCustomTemplate(t *testing.T) {
	tests := []struct {
		name     string
		city     string
		agent    string
		template string
		want     string
	}{
		{
			name:     "no gc prefix",
			city:     "bright-lights",
			agent:    "mayor",
			template: "{{.City}}-{{.Agent}}",
			want:     "bright-lights-mayor",
		},
		{
			name:     "name only",
			city:     "bright-lights",
			agent:    "mayor",
			template: "{{.Name}}",
			want:     "mayor",
		},
		{
			name:     "dir and name",
			city:     "bright-lights",
			agent:    "hello-world/polecat",
			template: "{{.Dir}}--{{.Name}}",
			want:     "hello-world--polecat",
		},
		{
			name:     "rig-scoped agent sanitized",
			city:     "city",
			agent:    "hello-world/polecat",
			template: "{{.City}}-{{.Agent}}",
			want:     "city-hello-world--polecat",
		},
		{
			name:     "singleton dir is empty",
			city:     "city",
			agent:    "mayor",
			template: "x-{{.Dir}}-{{.Name}}",
			want:     "x--mayor",
		},
		{
			name:     "custom prefix",
			city:     "bright-lights",
			agent:    "worker-3",
			template: "agent-{{.Name}}",
			want:     "agent-worker-3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SessionNameFor(tt.city, tt.agent, tt.template)
			if got != tt.want {
				t.Errorf("SessionNameFor(%q, %q, %q) = %q, want %q",
					tt.city, tt.agent, tt.template, got, tt.want)
			}
		})
	}
}

func TestSessionNameForInvalidTemplate(t *testing.T) {
	// Invalid template syntax → falls back to default (just sanitized name).
	got := SessionNameFor("city", "mayor", "{{.Unclosed")
	want := "mayor"
	if got != want {
		t.Errorf("SessionNameFor with bad template = %q, want %q", got, want)
	}
}

func TestSessionNameForExecutionError(t *testing.T) {
	// Template calls a missing method → falls back to default.
	got := SessionNameFor("city", "mayor", "{{.City | len | printf}}")
	// This should either work or fall back to default; either way shouldn't panic.
	if got == "" {
		t.Error("SessionNameFor with tricky template returned empty")
	}
}

func TestHandleFor(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})

	h := HandleFor("mayor", "city", "", sp)

	if got := h.Name(); got != "mayor" {
		t.Errorf("Name() = %q, want %q", got, "mayor")
	}
	if got := h.SessionName(); got != "mayor" {
		t.Errorf("SessionName() = %q, want %q", got, "mayor")
	}
	if !h.IsRunning() {
		t.Error("IsRunning() = false, want true")
	}
	if err := h.Nudge("hello"); err != nil {
		t.Errorf("Nudge() = %v, want nil", err)
	}
	if _, err := h.Peek(10); err != nil {
		t.Errorf("Peek() error = %v, want nil", err)
	}
	if err := h.Stop(); err != nil {
		t.Errorf("Stop() = %v, want nil", err)
	}
	if h.IsRunning() {
		t.Error("IsRunning() = true after Stop, want false")
	}
}

func TestHandleForCustomTemplate(t *testing.T) {
	sp := runtime.NewFake()
	h := HandleFor("mayor", "city", "{{.City}}-{{.Name}}", sp)

	if got := h.SessionName(); got != "city-mayor" {
		t.Errorf("SessionName() = %q, want %q", got, "city-mayor")
	}
}

func TestManagedName(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "claude", "", nil, StartupHints{}, "", "", nil, sp)
	if got := a.Name(); got != "mayor" {
		t.Errorf("Name() = %q, want %q", got, "mayor")
	}
}

func TestManagedSessionName(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "claude", "", nil, StartupHints{}, "", "", nil, sp)
	if got := a.SessionName(); got != "mayor" {
		t.Errorf("SessionName() = %q, want %q", got, "mayor")
	}
}

func TestManagedSessionNameCustomTemplate(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "claude", "", nil, StartupHints{}, "", "{{.City}}-{{.Name}}", nil, sp)
	if got := a.SessionName(); got != "city-mayor" {
		t.Errorf("SessionName() = %q, want %q", got, "city-mayor")
	}
}

func TestManagedStart(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "claude --skip", "", nil, StartupHints{}, "", "", nil, sp)

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	// Verify delegation: sp.Start was called with session name + command.
	if len(sp.Calls) != 1 {
		t.Fatalf("got %d calls, want 1: %+v", len(sp.Calls), sp.Calls)
	}
	c := sp.Calls[0]
	if c.Method != "Start" {
		t.Errorf("Method = %q, want %q", c.Method, "Start")
	}
	if c.Name != "mayor" {
		t.Errorf("Name = %q, want %q", c.Name, "mayor")
	}
	if c.Config.Command != "claude --skip" {
		t.Errorf("Config.Command = %q, want %q", c.Config.Command, "claude --skip")
	}
}

func TestManagedStartWithPrompt(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "claude --skip", "You are a mayor", nil, StartupHints{}, "", "", nil, sp)

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	c := sp.Calls[0]
	want := "claude --skip 'You are a mayor'"
	if c.Config.Command != want {
		t.Errorf("Config.Command = %q, want %q", c.Config.Command, want)
	}
}

func TestManagedStartWithEnv(t *testing.T) {
	sp := runtime.NewFake()
	env := map[string]string{"GC_AGENT": "mayor"}
	a := New("mayor", "city", "claude", "", env, StartupHints{}, "", "", nil, sp)

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	c := sp.Calls[0]
	if c.Config.Env["GC_AGENT"] != "mayor" {
		t.Errorf("Config.Env[GC_AGENT] = %q, want %q", c.Config.Env["GC_AGENT"], "mayor")
	}
}

func TestManagedStartWithHints(t *testing.T) {
	sp := runtime.NewFake()
	hints := StartupHints{
		ReadyPromptPrefix:      "> ",
		ReadyDelayMs:           5000,
		ProcessNames:           []string{"claude", "node"},
		EmitsPermissionWarning: true,
	}
	a := New("mayor", "city", "claude", "", nil, hints, "", "", nil, sp)

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	c := sp.Calls[0]
	if c.Config.ReadyPromptPrefix != "> " {
		t.Errorf("Config.ReadyPromptPrefix = %q, want %q", c.Config.ReadyPromptPrefix, "> ")
	}
	if c.Config.ReadyDelayMs != 5000 {
		t.Errorf("Config.ReadyDelayMs = %d, want %d", c.Config.ReadyDelayMs, 5000)
	}
	if len(c.Config.ProcessNames) != 2 || c.Config.ProcessNames[0] != "claude" {
		t.Errorf("Config.ProcessNames = %v, want [claude node]", c.Config.ProcessNames)
	}
	if !c.Config.EmitsPermissionWarning {
		t.Error("Config.EmitsPermissionWarning = false, want true")
	}
}

func TestManagedStartWithZeroHints(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "claude", "", nil, StartupHints{}, "", "", nil, sp)

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	c := sp.Calls[0]
	if c.Config.ReadyPromptPrefix != "" {
		t.Errorf("Config.ReadyPromptPrefix = %q, want empty", c.Config.ReadyPromptPrefix)
	}
	if c.Config.ReadyDelayMs != 0 {
		t.Errorf("Config.ReadyDelayMs = %d, want 0", c.Config.ReadyDelayMs)
	}
	if len(c.Config.ProcessNames) != 0 {
		t.Errorf("Config.ProcessNames = %v, want nil", c.Config.ProcessNames)
	}
	if c.Config.EmitsPermissionWarning {
		t.Error("Config.EmitsPermissionWarning = true, want false")
	}
}

func TestManagedStartAllParamsCombined(t *testing.T) {
	sp := runtime.NewFake()
	env := map[string]string{"GC_AGENT": "mayor"}
	hints := StartupHints{
		ReadyPromptPrefix:      "❯ ",
		ReadyDelayMs:           10000,
		ProcessNames:           []string{"claude", "node"},
		EmitsPermissionWarning: true,
	}
	a := New("mayor", "city", "claude --skip", "You are mayor", env, hints, "", "", nil, sp)

	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}

	c := sp.Calls[0]
	// Command includes shell-quoted prompt.
	want := "claude --skip 'You are mayor'"
	if c.Config.Command != want {
		t.Errorf("Config.Command = %q, want %q", c.Config.Command, want)
	}
	if c.Config.Env["GC_AGENT"] != "mayor" {
		t.Errorf("Config.Env[GC_AGENT] = %q, want %q", c.Config.Env["GC_AGENT"], "mayor")
	}
	if c.Config.ReadyPromptPrefix != "❯ " {
		t.Errorf("Config.ReadyPromptPrefix = %q, want %q", c.Config.ReadyPromptPrefix, "❯ ")
	}
	if c.Config.ReadyDelayMs != 10000 {
		t.Errorf("Config.ReadyDelayMs = %d, want %d", c.Config.ReadyDelayMs, 10000)
	}
	if len(c.Config.ProcessNames) != 2 || c.Config.ProcessNames[0] != "claude" {
		t.Errorf("Config.ProcessNames = %v, want [claude node]", c.Config.ProcessNames)
	}
	if !c.Config.EmitsPermissionWarning {
		t.Error("Config.EmitsPermissionWarning = false, want true")
	}
}

func TestManagedStartError(t *testing.T) {
	sp := runtime.NewFailFake()
	a := New("mayor", "city", "claude", "", nil, StartupHints{}, "", "", nil, sp)

	err := a.Start(context.Background())
	if err == nil {
		t.Fatal("Start() = nil, want error from broken provider")
	}
}

func TestManagedStopError(t *testing.T) {
	sp := runtime.NewFailFake()
	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)

	err := a.Stop()
	if err == nil {
		t.Fatal("Stop() = nil, want error from broken provider")
	}
}

func TestManagedAttachError(t *testing.T) {
	sp := runtime.NewFailFake()
	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)

	err := a.Attach()
	if err == nil {
		t.Fatal("Attach() = nil, want error from broken provider")
	}
}

func TestManagedSessionConfig(t *testing.T) {
	sp := runtime.NewFake()
	env := map[string]string{"GC_AGENT": "mayor"}
	hints := StartupHints{
		ReadyPromptPrefix:      "> ",
		ReadyDelayMs:           5000,
		ProcessNames:           []string{"claude"},
		EmitsPermissionWarning: true,
	}
	a := New("mayor", "city", "claude --skip", "You are mayor", env, hints, "", "", nil, sp)

	cfg := a.SessionConfig()

	// Command includes shell-quoted prompt.
	wantCmd := "claude --skip 'You are mayor'"
	if cfg.Command != wantCmd {
		t.Errorf("Command = %q, want %q", cfg.Command, wantCmd)
	}
	if cfg.Env["GC_AGENT"] != "mayor" {
		t.Errorf("Env[GC_AGENT] = %q, want %q", cfg.Env["GC_AGENT"], "mayor")
	}
	if cfg.ReadyPromptPrefix != "> " {
		t.Errorf("ReadyPromptPrefix = %q, want %q", cfg.ReadyPromptPrefix, "> ")
	}
	if cfg.ReadyDelayMs != 5000 {
		t.Errorf("ReadyDelayMs = %d, want %d", cfg.ReadyDelayMs, 5000)
	}
	if len(cfg.ProcessNames) != 1 || cfg.ProcessNames[0] != "claude" {
		t.Errorf("ProcessNames = %v, want [claude]", cfg.ProcessNames)
	}
	if !cfg.EmitsPermissionWarning {
		t.Error("EmitsPermissionWarning = false, want true")
	}

	// SessionConfig should not call the provider.
	if len(sp.Calls) != 0 {
		t.Errorf("provider received %d calls, want 0", len(sp.Calls))
	}
}

func TestManagedSessionConfigWorkDir(t *testing.T) {
	sp := runtime.NewFake()
	a := New("worker", "city", "claude", "", nil, StartupHints{}, "/tmp/project", "", nil, sp)

	cfg := a.SessionConfig()
	if cfg.WorkDir != "/tmp/project" {
		t.Errorf("WorkDir = %q, want %q", cfg.WorkDir, "/tmp/project")
	}
}

func TestManagedSessionConfigEmptyWorkDir(t *testing.T) {
	sp := runtime.NewFake()
	a := New("worker", "city", "claude", "", nil, StartupHints{}, "", "", nil, sp)

	cfg := a.SessionConfig()
	if cfg.WorkDir != "" {
		t.Errorf("WorkDir = %q, want empty", cfg.WorkDir)
	}
}

func TestManagedSessionConfigOverlayDir(t *testing.T) {
	sp := runtime.NewFake()
	hints := StartupHints{OverlayDir: "/tmp/overlay"}
	a := New("worker", "city", "claude", "", nil, hints, "", "", nil, sp)

	cfg := a.SessionConfig()
	if cfg.OverlayDir != "/tmp/overlay" {
		t.Errorf("OverlayDir = %q, want %q", cfg.OverlayDir, "/tmp/overlay")
	}
}

func TestManagedSessionConfigNoPrompt(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "claude", "", nil, StartupHints{}, "", "", nil, sp)

	cfg := a.SessionConfig()
	if cfg.Command != "claude" {
		t.Errorf("Command = %q, want %q", cfg.Command, "claude")
	}
}

// TestPromptModeNone verifies that when prompt_mode="none" (the caller passes
// prompt="" to agent.New), the command is not modified — no beacon or prompt
// is shell-quoted and appended. This is critical for agents using
// start_command = "bash" where extra arguments would be misinterpreted.
func TestPromptModeNone(t *testing.T) {
	sp := runtime.NewFake()
	// When prompt_mode="none", cmd_start.go and pool.go pass prompt="" to agent.New().
	// The command should remain exactly as configured.
	a := New("worker", "city", "bash", "", nil, StartupHints{}, "/tmp/work", "", nil, sp)

	cfg := a.SessionConfig()
	if cfg.Command != "bash" {
		t.Errorf("Command = %q, want %q (prompt_mode=none should not append anything)", cfg.Command, "bash")
	}

	// Start should pass the bare command to the provider.
	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	c := sp.Calls[0]
	if c.Config.Command != "bash" {
		t.Errorf("Start command = %q, want %q", c.Config.Command, "bash")
	}
}

func TestShellQuote(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"hello", "'hello'"},
		{"it's here", `'it'\''s here'`},
		{"", "''"},
		{"line1\nline2", "'line1\nline2'"},
	}
	for _, tt := range tests {
		got := shellQuote(tt.in)
		if got != tt.want {
			t.Errorf("shellQuote(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestManagedStop(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Calls = nil

	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)
	if err := a.Stop(); err != nil {
		t.Fatalf("Stop() = %v, want nil", err)
	}

	if len(sp.Calls) != 1 {
		t.Fatalf("got %d calls, want 1: %+v", len(sp.Calls), sp.Calls)
	}
	if sp.Calls[0].Method != "Stop" {
		t.Errorf("Method = %q, want %q", sp.Calls[0].Method, "Stop")
	}
	if sp.Calls[0].Name != "mayor" {
		t.Errorf("Name = %q, want %q", sp.Calls[0].Name, "mayor")
	}
}

func TestManagedIsRunning(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)

	if a.IsRunning() {
		t.Error("IsRunning() = true before Start, want false")
	}

	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Calls = nil

	if !a.IsRunning() {
		t.Error("IsRunning() = false after Start, want true")
	}

	// With no process names, only IsRunning is called (ProcessAlive
	// is called but returns true for empty names).
	if len(sp.Calls) < 1 {
		t.Fatalf("got %d calls, want at least 1: %+v", len(sp.Calls), sp.Calls)
	}
	if sp.Calls[0].Method != "IsRunning" {
		t.Errorf("Method = %q, want %q", sp.Calls[0].Method, "IsRunning")
	}
}

func TestManagedIsRunningZombie(t *testing.T) {
	sp := runtime.NewFake()
	hints := StartupHints{ProcessNames: []string{"claude", "node"}}
	a := New("mayor", "city", "claude", "", nil, hints, "", "", nil, sp)

	// Start the session, then mark it as zombie.
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Zombies["mayor"] = true

	if a.IsRunning() {
		t.Error("IsRunning() = true for zombie session, want false")
	}
}

func TestManagedIsRunningHealthy(t *testing.T) {
	sp := runtime.NewFake()
	hints := StartupHints{ProcessNames: []string{"claude", "node"}}
	a := New("mayor", "city", "claude", "", nil, hints, "", "", nil, sp)

	_ = sp.Start(context.Background(), "mayor", runtime.Config{})

	if !a.IsRunning() {
		t.Error("IsRunning() = false for healthy session, want true")
	}
}

func TestManagedIsRunningNoProcessNames(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "claude", "", nil, StartupHints{}, "", "", nil, sp)

	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Zombies["mayor"] = true // zombie, but no process names configured

	if !a.IsRunning() {
		t.Error("IsRunning() = false with no process names, want true (can't check deeper)")
	}
}

func TestManagedNudge(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Calls = nil

	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)
	if err := a.Nudge("wake up"); err != nil {
		t.Fatalf("Nudge() = %v, want nil", err)
	}

	if len(sp.Calls) != 1 {
		t.Fatalf("got %d calls, want 1: %+v", len(sp.Calls), sp.Calls)
	}
	c := sp.Calls[0]
	if c.Method != "Nudge" {
		t.Errorf("Method = %q, want %q", c.Method, "Nudge")
	}
	if c.Name != "mayor" {
		t.Errorf("Name = %q, want %q", c.Name, "mayor")
	}
	if c.Message != "wake up" {
		t.Errorf("Message = %q, want %q", c.Message, "wake up")
	}
}

func TestManagedNudgeError(t *testing.T) {
	sp := runtime.NewFailFake()
	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)

	err := a.Nudge("wake up")
	if err == nil {
		t.Fatal("Nudge() = nil, want error from broken provider")
	}
}

func TestManagedAttach(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Calls = nil

	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)
	if err := a.Attach(); err != nil {
		t.Fatalf("Attach() = %v, want nil", err)
	}

	if len(sp.Calls) != 1 {
		t.Fatalf("got %d calls, want 1: %+v", len(sp.Calls), sp.Calls)
	}
	if sp.Calls[0].Method != "Attach" {
		t.Errorf("Method = %q, want %q", sp.Calls[0].Method, "Attach")
	}
	if sp.Calls[0].Name != "mayor" {
		t.Errorf("Name = %q, want %q", sp.Calls[0].Name, "mayor")
	}
}

func TestManagedInterrupt(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Calls = nil

	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)
	if err := a.Interrupt(); err != nil {
		t.Fatalf("Interrupt() = %v, want nil", err)
	}

	if len(sp.Calls) != 1 || sp.Calls[0].Method != "Interrupt" {
		t.Errorf("got calls %+v, want single Interrupt call", sp.Calls)
	}
	if sp.Calls[0].Name != "mayor" {
		t.Errorf("Name = %q, want %q", sp.Calls[0].Name, "mayor")
	}
}

func TestManagedProcessAlive(t *testing.T) {
	sp := runtime.NewFake()
	hints := StartupHints{ProcessNames: []string{"claude"}}
	a := New("mayor", "city", "claude", "", nil, hints, "", "", nil, sp)

	_ = sp.Start(context.Background(), "mayor", runtime.Config{})

	if !a.ProcessAlive() {
		t.Error("ProcessAlive() = false for healthy session, want true")
	}

	sp.Zombies["mayor"] = true
	if a.ProcessAlive() {
		t.Error("ProcessAlive() = true for zombie session, want false")
	}
}

func TestManagedClearScrollback(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)
	sp.Calls = nil

	if err := a.ClearScrollback(); err != nil {
		t.Fatalf("ClearScrollback() = %v, want nil", err)
	}

	if len(sp.Calls) != 1 || sp.Calls[0].Method != "ClearScrollback" {
		t.Errorf("got calls %+v, want single ClearScrollback call", sp.Calls)
	}
}

func TestManagedGetLastActivity(t *testing.T) {
	sp := runtime.NewFake()
	now := time.Now()
	sp.SetActivity("mayor", now)

	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)
	sp.Calls = nil

	got, err := a.GetLastActivity()
	if err != nil {
		t.Fatalf("GetLastActivity() error = %v", err)
	}
	if !got.Equal(now) {
		t.Errorf("GetLastActivity() = %v, want %v", got, now)
	}

	if len(sp.Calls) != 1 || sp.Calls[0].Method != "GetLastActivity" {
		t.Errorf("got calls %+v, want single GetLastActivity call", sp.Calls)
	}
}

func TestManagedSendKeys(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)
	sp.Calls = nil

	if err := a.SendKeys("Enter", "Down"); err != nil {
		t.Fatalf("SendKeys() = %v, want nil", err)
	}

	if len(sp.Calls) != 1 || sp.Calls[0].Method != "SendKeys" {
		t.Errorf("got calls %+v, want single SendKeys call", sp.Calls)
	}
}

func TestManagedRunLive(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)
	sp.Calls = nil

	cfg := runtime.Config{SessionLive: []string{"echo hello"}}
	if err := a.RunLive(cfg); err != nil {
		t.Fatalf("RunLive() = %v, want nil", err)
	}

	if len(sp.Calls) != 1 || sp.Calls[0].Method != "RunLive" {
		t.Errorf("got calls %+v, want single RunLive call", sp.Calls)
	}
}

func TestManagedSetGetRemoveMeta(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)
	sp.Calls = nil

	// SetMeta
	if err := a.SetMeta("GC_DRAIN", "12345"); err != nil {
		t.Fatalf("SetMeta() = %v, want nil", err)
	}

	// GetMeta
	val, err := a.GetMeta("GC_DRAIN")
	if err != nil {
		t.Fatalf("GetMeta() error = %v", err)
	}
	if val != "12345" {
		t.Errorf("GetMeta() = %q, want %q", val, "12345")
	}

	// RemoveMeta
	if err := a.RemoveMeta("GC_DRAIN"); err != nil {
		t.Fatalf("RemoveMeta() = %v, want nil", err)
	}

	// Verify removed
	val, err = a.GetMeta("GC_DRAIN")
	if err != nil {
		t.Fatalf("GetMeta() after remove error = %v", err)
	}
	if val != "" {
		t.Errorf("GetMeta() after remove = %q, want empty", val)
	}

	// Verify correct session name passed through.
	for _, c := range sp.Calls {
		if c.Name != "mayor" {
			t.Errorf("call %q: Name = %q, want %q", c.Method, c.Name, "mayor")
		}
	}
}

func TestManagedEventsNilWithoutObserver(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "echo", "", nil, StartupHints{}, "", "", nil, sp)

	if ch := a.Events(); ch != nil {
		t.Error("Events() should return nil when no observer is set")
	}
}

func TestManagedSetObserverAndEvents(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "echo", "", nil, StartupHints{}, "", "", nil, sp)

	// Create a simple mock observer.
	ch := make(chan Event, 1)
	obs := &fakeObserver{ch: ch}
	a.SetObserver(obs)

	// Events() should return the observer's channel.
	got := a.Events()
	if got == nil {
		t.Fatal("Events() returned nil after SetObserver")
	}

	// Send an event and verify we receive it.
	want := Event{Type: EventToolCall, Agent: "mayor", Data: "Bash"}
	ch <- want

	select {
	case ev := <-got:
		if ev.Type != want.Type || ev.Agent != want.Agent || ev.Data != want.Data {
			t.Errorf("got %+v, want %+v", ev, want)
		}
	default:
		t.Fatal("expected event on channel")
	}
}

func TestManagedSetObserverClosesOld(t *testing.T) {
	sp := runtime.NewFake()
	a := New("mayor", "city", "echo", "", nil, StartupHints{}, "", "", nil, sp)

	old := &fakeObserver{ch: make(chan Event)}
	a.SetObserver(old)

	// Replace with new observer — old should be closed.
	newObs := &fakeObserver{ch: make(chan Event)}
	a.SetObserver(newObs)

	if !old.closed {
		t.Error("old observer should have been closed when replaced")
	}
}

func TestManagedStopClosesObserver(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)

	obs := &fakeObserver{ch: make(chan Event)}
	a.SetObserver(obs)

	if err := a.Stop(); err != nil {
		t.Fatalf("Stop() = %v, want nil", err)
	}

	if !obs.closed {
		t.Error("observer should be closed after Stop()")
	}
	if a.Events() != nil {
		t.Error("Events() should return nil after Stop()")
	}
}

func TestManagedDoubleStopSafe(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	a := New("mayor", "city", "", "", nil, StartupHints{}, "", "", nil, sp)

	obs := &fakeObserver{ch: make(chan Event)}
	a.SetObserver(obs)

	if err := a.Stop(); err != nil {
		t.Fatalf("first Stop() = %v, want nil", err)
	}
	// Second stop should not panic (observer already nil).
	if err := a.Stop(); err != nil {
		t.Fatalf("second Stop() = %v, want nil", err)
	}
}

func TestManagedStopRunsOnStopCallbacks(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "worker", runtime.Config{})

	var called1, called2 bool
	cb1 := func() error { called1 = true; return nil }
	cb2 := func() error { called2 = true; return nil }

	a := New("worker", "city", "", "", nil, StartupHints{}, "", "", nil, sp, cb1, cb2)
	if err := a.Stop(); err != nil {
		t.Fatalf("Stop() = %v, want nil", err)
	}
	if !called1 || !called2 {
		t.Errorf("onStop callbacks: called1=%v called2=%v, want both true", called1, called2)
	}
}

func TestManagedStopSkipsCallbacksOnError(t *testing.T) {
	sp := runtime.NewFailFake()

	var called bool
	cb := func() error { called = true; return nil }

	a := New("worker", "city", "", "", nil, StartupHints{}, "", "", nil, sp, cb)
	if err := a.Stop(); err == nil {
		t.Fatal("Stop() = nil, want error")
	}
	if called {
		t.Error("onStop callback should NOT run when sp.Stop fails")
	}
}

func TestHandleForWithOnStop(t *testing.T) {
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "worker", runtime.Config{})

	var called bool
	cb := func() error { called = true; return nil }

	h := HandleFor("worker", "city", "", sp, cb)
	if err := h.Stop(); err != nil {
		t.Fatalf("Stop() = %v, want nil", err)
	}
	if !called {
		t.Error("onStop callback should run after HandleFor().Stop()")
	}
}

func TestFakeStopRunsOnStopCallbacks(t *testing.T) {
	var called bool
	f := NewFake("worker", "worker")
	f.Running = true
	f.OnStop = []func() error{func() error { called = true; return nil }}

	if err := f.Stop(); err != nil {
		t.Fatalf("Stop() = %v, want nil", err)
	}
	if !called {
		t.Error("Fake.OnStop callback should run on successful Stop")
	}
}

func TestFakeStopSkipsOnStopOnError(t *testing.T) {
	var called bool
	f := NewFake("worker", "worker")
	f.Running = true
	f.StopErr = fmt.Errorf("injected")
	f.OnStop = []func() error{func() error { called = true; return nil }}

	if err := f.Stop(); err == nil {
		t.Fatal("Stop() = nil, want error")
	}
	if called {
		t.Error("Fake.OnStop should NOT run when StopErr is set")
	}
}

// fakeObserver is a test double for ObservationStrategy.
type fakeObserver struct {
	ch     chan Event
	closed bool
}

func (f *fakeObserver) Events() <-chan Event { return f.ch }
func (f *fakeObserver) Close() error {
	f.closed = true
	return nil
}
