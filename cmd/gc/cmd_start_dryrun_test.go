package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
)

func TestPrintDryRunPreview(t *testing.T) {
	sp := runtime.NewFake()

	agents := []agent.Agent{
		agent.New("mayor", "test", "echo hello", "", nil, agent.StartupHints{}, "", "", nil, sp),
		agent.New("hw/polecat-1", "test", "echo hello", "", nil, agent.StartupHints{}, "", "", nil, sp),
		agent.New("hw/polecat-2", "test", "echo hello", "", nil, agent.StartupHints{}, "", "", nil, sp),
	}

	cfg := &config.City{
		Workspace: config.Workspace{Name: "test"},
		Agents: []config.Agent{
			{Name: "mayor"},
			{Name: "polecat", Dir: "hw"},
			{Name: "worker", Suspended: true},
		},
	}

	var stdout bytes.Buffer
	printDryRunPreview(agents, cfg, "test", &stdout)
	out := stdout.String()

	if !strings.Contains(out, "3 agent(s) would start") {
		t.Errorf("should report 3 agents, got:\n%s", out)
	}
	if !strings.Contains(out, "mayor") {
		t.Errorf("should list mayor, got:\n%s", out)
	}
	if !strings.Contains(out, "hw/polecat-1") {
		t.Errorf("should list hw/polecat-1, got:\n%s", out)
	}
	if !strings.Contains(out, "1 agent(s) suspended") {
		t.Errorf("should mention 1 suspended, got:\n%s", out)
	}
	if !strings.Contains(out, "No side effects executed (--dry-run).") {
		t.Errorf("should show dry-run footer, got:\n%s", out)
	}
}

func TestPrintDryRunPreviewEmpty(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "empty"},
	}

	var stdout bytes.Buffer
	printDryRunPreview(nil, cfg, "empty", &stdout)
	out := stdout.String()

	if !strings.Contains(out, "0 agent(s) would start") {
		t.Errorf("should report 0 agents, got:\n%s", out)
	}
	if !strings.Contains(out, "(no agents to start)") {
		t.Errorf("should show empty message, got:\n%s", out)
	}
}

func TestStartDryRunFlagExists(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cmd := newStartCmd(&stdout, &stderr)
	f := cmd.Flags().Lookup("dry-run")
	if f == nil {
		t.Fatal("missing --dry-run flag")
	}
	if f.Shorthand != "n" {
		t.Errorf("--dry-run shorthand = %q, want %q", f.Shorthand, "n")
	}
}
