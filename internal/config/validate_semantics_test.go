package config

import (
	"strings"
	"testing"
)

func TestValidateSemanticsNoWarnings(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "claude"},
		Agents: []Agent{
			{Name: "mayor", Provider: "claude"},
			{Name: "worker", Provider: "codex"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got: %v", warnings)
	}
}

func TestValidateSemanticsUnknownAgentProvider(t *testing.T) {
	cfg := &City{
		Agents: []Agent{
			{Name: "mayor", Provider: "cloude"}, // typo
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "cloude") {
		t.Errorf("warning should mention bad provider: %s", warnings[0])
	}
	if !strings.Contains(warnings[0], "mayor") {
		t.Errorf("warning should mention agent: %s", warnings[0])
	}
}

func TestValidateSemanticsCustomProviderOK(t *testing.T) {
	cfg := &City{
		Providers: map[string]ProviderSpec{
			"my-agent": {Command: "my-agent-cli"},
		},
		Agents: []Agent{
			{Name: "worker", Provider: "my-agent"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Errorf("expected no warnings for custom provider, got: %v", warnings)
	}
}

func TestValidateSemanticsUnknownWorkspaceProvider(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "bogus"},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "[workspace]") {
		t.Errorf("warning should mention workspace: %s", warnings[0])
	}
}

func TestValidateSemanticsStartCommandSkipsProviderCheck(t *testing.T) {
	cfg := &City{
		Agents: []Agent{
			{Name: "custom", Provider: "nonexistent", StartCommand: "my-binary"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Errorf("start_command should skip provider check, got: %v", warnings)
	}
}

func TestValidateSemanticsAgentSessionTransportAllowsTmux(t *testing.T) {
	cfg := &City{
		Agents: []Agent{
			{Name: "worker", Provider: "claude", Session: "tmux"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings for tmux session transport, got: %v", warnings)
	}
}

func TestValidateSemanticsAgentSessionTransportRejectsUnknown(t *testing.T) {
	cfg := &City{
		Agents: []Agent{
			{Name: "worker", Provider: "claude", Session: "stdio"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "stdio") || !strings.Contains(warnings[0], "tmux") {
		t.Fatalf("warning should mention bad value and allowed transports: %s", warnings[0])
	}
}

func TestValidateSemanticsProviderPromptModeBad(t *testing.T) {
	cfg := &City{
		Providers: map[string]ProviderSpec{
			"bad": {PromptMode: "pipe"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "pipe") {
		t.Errorf("warning should mention bad value: %s", warnings[0])
	}
}

func TestValidateSemanticsProviderPromptFlagRequired(t *testing.T) {
	cfg := &City{
		Providers: map[string]ProviderSpec{
			"needsflag": {PromptMode: "flag"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "prompt_flag") {
		t.Errorf("warning should mention prompt_flag: %s", warnings[0])
	}
}

func TestValidateSemanticsProviderPromptFlagOK(t *testing.T) {
	cfg := &City{
		Providers: map[string]ProviderSpec{
			"ok": {PromptMode: "flag", PromptFlag: "--prompt"},
		},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got: %v", warnings)
	}
}

func TestValidateSemanticsMultipleIssues(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "nope"},
		Providers: map[string]ProviderSpec{
			"bad": {PromptMode: "pipe"},
		},
		Agents: []Agent{
			{Name: "a1", Provider: "missing1"},
			{Name: "a2", Provider: "missing2"},
		},
	}
	warnings := ValidateSemantics(cfg, "test.toml")
	// 1 workspace + 2 agents + 1 provider = 4
	if len(warnings) != 4 {
		t.Fatalf("expected 4 warnings, got %d: %v", len(warnings), warnings)
	}
}

func TestValidateSemanticsIncludesSource(t *testing.T) {
	cfg := &City{
		Agents: []Agent{
			{Name: "bad", Provider: "missing"},
		},
	}
	warnings := ValidateSemantics(cfg, "/path/to/city.toml")
	if len(warnings) == 0 {
		t.Fatal("expected warning")
	}
	if !strings.Contains(warnings[0], "/path/to/city.toml") {
		t.Errorf("warning should include source path: %s", warnings[0])
	}
}

func TestValidateAgentsScopeBadEnum(t *testing.T) {
	agents := []Agent{
		{Name: "bad", Scope: "global"},
	}
	err := ValidateAgents(agents)
	if err == nil {
		t.Fatal("expected error for bad scope")
	}
	if !strings.Contains(err.Error(), "global") {
		t.Errorf("error should mention bad value: %v", err)
	}
}

func TestValidateAgentsScopeValidValues(t *testing.T) {
	for _, scope := range []string{"", "city", "rig"} {
		agents := []Agent{
			{Name: "ok", Scope: scope},
		}
		if err := ValidateAgents(agents); err != nil {
			t.Errorf("scope %q should be valid, got: %v", scope, err)
		}
	}
}

func TestValidateAgentsPromptModeBadEnum(t *testing.T) {
	agents := []Agent{
		{Name: "bad", PromptMode: "pipe"},
	}
	err := ValidateAgents(agents)
	if err == nil {
		t.Fatal("expected error for bad prompt_mode")
	}
	if !strings.Contains(err.Error(), "pipe") {
		t.Errorf("error should mention bad value: %v", err)
	}
}

func TestValidateAgentsPromptModeValidValues(t *testing.T) {
	for _, mode := range []string{"", "arg", "flag", "none"} {
		agents := []Agent{
			{Name: "ok", PromptMode: mode, PromptFlag: "--p"},
		}
		if err := ValidateAgents(agents); err != nil {
			t.Errorf("prompt_mode %q should be valid, got: %v", mode, err)
		}
	}
}

func TestValidateAgentsLifecycleValues(t *testing.T) {
	for _, lifecycle := range []string{"", AgentLifecycleOneShot} {
		if err := ValidateAgents([]Agent{{Name: "ok", Lifecycle: lifecycle}}); err != nil {
			t.Errorf("lifecycle %q should be valid, got: %v", lifecycle, err)
		}
	}
	err := ValidateAgents([]Agent{{Name: "bad", Lifecycle: "short_lived"}})
	if err == nil {
		t.Fatal("expected error for bad lifecycle")
	}
	if !strings.Contains(err.Error(), "short_lived") {
		t.Errorf("error should mention bad value: %v", err)
	}
}

func TestValidateAgentsPromptFlagRequiredForFlagMode(t *testing.T) {
	agents := []Agent{
		{Name: "bad", PromptMode: "flag"},
	}
	err := ValidateAgents(agents)
	if err == nil {
		t.Fatal("expected error for missing prompt_flag")
	}
	if !strings.Contains(err.Error(), "prompt_flag") {
		t.Errorf("error should mention prompt_flag: %v", err)
	}
}

func TestValidateAgentsPromptFlagWithFlagModeOK(t *testing.T) {
	agents := []Agent{
		{Name: "ok", PromptMode: "flag", PromptFlag: "--prompt"},
	}
	if err := ValidateAgents(agents); err != nil {
		t.Errorf("should be valid: %v", err)
	}
}

func TestValidateSemanticsCompactionUnknownPolicy(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "claude"},
		Agents: []Agent{{
			Name:       "mayor",
			Provider:   "claude",
			Compaction: Compaction{Policy: "explode", ThresholdTurns: 10},
		}},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "compaction.policy") {
		t.Errorf("warning should mention compaction.policy: %s", warnings[0])
	}
	if !strings.Contains(warnings[0], "explode") {
		t.Errorf("warning should echo bad policy value: %s", warnings[0])
	}
}

func TestValidateSemanticsCompactionNegativeThresholdsWarn(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "claude"},
		Agents: []Agent{{
			Name:       "mayor",
			Provider:   "claude",
			Compaction: Compaction{ThresholdTurns: -1, ThresholdTokens: -5, Policy: "handoff"},
		}},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	// Expect two warnings: one for each negative threshold. The policy-set-
	// but-no-threshold warning should not fire because Enabled() is computed
	// from positive thresholds; both negatives evaluate as disabled, so the
	// "policy set but no threshold" rule applies.
	turnWarn, tokenWarn := false, false
	for _, w := range warnings {
		if strings.Contains(w, "threshold_turns must be >= 0") {
			turnWarn = true
		}
		if strings.Contains(w, "threshold_tokens must be >= 0") {
			tokenWarn = true
		}
	}
	if !turnWarn {
		t.Errorf("missing turns-negative warning; got: %v", warnings)
	}
	if !tokenWarn {
		t.Errorf("missing tokens-negative warning; got: %v", warnings)
	}
}

func TestValidateSemanticsCompactionTokensWithoutTurnsIsInert(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "claude"},
		Agents: []Agent{{
			Name:       "mayor",
			Provider:   "claude",
			Compaction: Compaction{ThresholdTokens: 100000},
		}},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	// One warning: tokens-without-turns is inert in v1. Plus the policy-
	// without-threshold warning would NOT fire because policy is empty.
	got := false
	for _, w := range warnings {
		if strings.Contains(w, "currently inert") {
			got = true
			break
		}
	}
	if !got {
		t.Errorf("expected inert-tokens warning; got: %v", warnings)
	}
}

func TestValidateSemanticsCompactionPolicyWithoutThresholdWarns(t *testing.T) {
	cfg := &City{
		Workspace: Workspace{Provider: "claude"},
		Agents: []Agent{{
			Name:       "mayor",
			Provider:   "claude",
			Compaction: Compaction{Policy: "warn"}, // no thresholds set
		}},
	}
	warnings := ValidateSemantics(cfg, "city.toml")
	got := false
	for _, w := range warnings {
		if strings.Contains(w, "policy will not fire until threshold_turns is set") {
			got = true
			break
		}
	}
	if !got {
		t.Errorf("expected policy-without-threshold warning; got: %v", warnings)
	}
}

func TestCompactionEnabled(t *testing.T) {
	cases := []struct {
		name string
		c    Compaction
		want bool
	}{
		{"empty disabled", Compaction{}, false},
		{"zero policy only disabled", Compaction{Policy: "handoff"}, false},
		{"turns positive enabled", Compaction{ThresholdTurns: 1}, true},
		{"tokens positive enabled", Compaction{ThresholdTokens: 100}, true},
	}
	for _, tc := range cases {
		if got := tc.c.Enabled(); got != tc.want {
			t.Errorf("%s: Enabled() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestCompactionEffectivePolicy(t *testing.T) {
	// Disabled -> empty.
	if got := (Compaction{}).EffectivePolicy(); got != "" {
		t.Errorf("disabled EffectivePolicy = %q, want empty", got)
	}
	// Enabled + empty policy -> "handoff" default.
	c := Compaction{ThresholdTurns: 5}
	if got := c.EffectivePolicy(); got != "handoff" {
		t.Errorf("default policy = %q, want handoff", got)
	}
	// Explicit policy preserved.
	c2 := Compaction{ThresholdTurns: 5, Policy: "warn"}
	if got := c2.EffectivePolicy(); got != "warn" {
		t.Errorf("explicit policy = %q, want warn", got)
	}
}

func TestIsValidCompactionPolicy(t *testing.T) {
	for _, p := range []string{"", "handoff", "warn", "reset"} {
		if !IsValidCompactionPolicy(p) {
			t.Errorf("%q should be valid", p)
		}
	}
	if IsValidCompactionPolicy("explode") {
		t.Error("'explode' should not be valid")
	}
}
