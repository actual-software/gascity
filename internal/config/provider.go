package config

import "strings"

// ProviderSpec defines a named provider's startup parameters.
// Built-in presets are returned by BuiltinProviders(). Users can override
// or define new providers via [providers.xxx] in city.toml.
type ProviderSpec struct {
	// DisplayName is the human-readable name shown in UI and logs.
	DisplayName string `toml:"display_name,omitempty"`
	// Command is the executable to run for this provider.
	Command string `toml:"command,omitempty"`
	// Args are default command-line arguments passed to the provider.
	Args []string `toml:"args,omitempty"`
	// PromptMode controls how prompts are delivered: "arg", "flag", or "none".
	PromptMode string `toml:"prompt_mode,omitempty" jsonschema:"enum=arg,enum=flag,enum=none,default=arg"`
	// PromptFlag is the CLI flag used when prompt_mode is "flag" (e.g. "--prompt").
	PromptFlag string `toml:"prompt_flag,omitempty"`
	// ReadyDelayMs is milliseconds to wait after launch before the provider is considered ready.
	ReadyDelayMs int `toml:"ready_delay_ms,omitempty" jsonschema:"minimum=0"`
	// ReadyPromptPrefix is the string prefix that indicates the provider is ready for input.
	ReadyPromptPrefix string `toml:"ready_prompt_prefix,omitempty"`
	// ProcessNames lists process names to look for when checking if the provider is running.
	ProcessNames []string `toml:"process_names,omitempty"`
	// EmitsPermissionWarning indicates whether the provider emits permission prompts.
	EmitsPermissionWarning bool `toml:"emits_permission_warning,omitempty"`
	// Env sets additional environment variables for the provider process.
	Env map[string]string `toml:"env,omitempty"`
	// PathCheck overrides the binary name used for PATH detection.
	// When set, lookupProvider and detectProviderName use this instead
	// of Command for exec.LookPath checks. Useful when Command is a
	// shell wrapper (e.g. sh -c '...') but we need to verify the real
	// binary is installed.
	PathCheck string `toml:"path_check,omitempty"`
	// SupportsACP indicates the binary speaks the Agent Client Protocol
	// (JSON-RPC 2.0 over stdio). When an agent sets session = "acp",
	// its resolved provider must have SupportsACP = true.
	SupportsACP bool `toml:"supports_acp,omitempty"`
	// SupportsHooks indicates the provider has an executable hook mechanism
	// (settings.json, plugins, etc.) for lifecycle events.
	SupportsHooks bool `toml:"supports_hooks,omitempty"`
	// InstructionsFile is the filename the provider reads for project instructions
	// (e.g., "CLAUDE.md", "AGENTS.md"). Empty defaults to "AGENTS.md".
	InstructionsFile string `toml:"instructions_file,omitempty"`
	// ResumeFlag is the CLI flag for resuming a session by ID.
	// Empty means the provider does not support resume.
	// Examples: "--resume" (claude), "resume" (codex)
	ResumeFlag string `toml:"resume_flag,omitempty"`
	// ResumeStyle controls how ResumeFlag is applied:
	//   "flag"       → command --resume <key>              (default)
	//   "subcommand" → command resume <key>
	ResumeStyle string `toml:"resume_style,omitempty"`
	// SessionIDFlag is the CLI flag for creating a session with a specific ID.
	// Enables the Generate & Pass strategy for session key management.
	// Example: "--session-id" (claude)
	SessionIDFlag string `toml:"session_id_flag,omitempty"`
}

// ResolvedProvider is the fully-merged, ready-to-use provider config.
// All fields are populated after resolution (built-in + city override + agent override).
type ResolvedProvider struct {
	Name                   string
	Command                string
	Args                   []string
	PromptMode             string
	PromptFlag             string
	ReadyDelayMs           int
	ReadyPromptPrefix      string
	ProcessNames           []string
	EmitsPermissionWarning bool
	Env                    map[string]string
	SupportsACP            bool
	SupportsHooks          bool
	InstructionsFile       string
	ResumeFlag             string
	ResumeStyle            string
	SessionIDFlag          string
}

// CommandString returns the full command line: command followed by args.
func (rp *ResolvedProvider) CommandString() string {
	if len(rp.Args) == 0 {
		return rp.Command
	}
	return rp.Command + " " + strings.Join(rp.Args, " ")
}

// pathCheckBinary returns the binary name to use for PATH detection.
// If PathCheck is set, it is used; otherwise Command is used directly.
func (ps *ProviderSpec) pathCheckBinary() string {
	if ps.PathCheck != "" {
		return ps.PathCheck
	}
	return ps.Command
}

// builtinProviderOrder is the priority order for provider detection and
// wizard display. Claude is first (default), followed by major providers
// in rough popularity order.
var builtinProviderOrder = []string{
	"claude", "codex", "gemini", "cursor", "copilot",
	"amp", "opencode", "auggie", "pi", "omp",
}

// BuiltinProviderOrder returns the provider names in their canonical order.
// Used by the wizard for display and by auto-detection for priority.
func BuiltinProviderOrder() []string {
	out := make([]string, len(builtinProviderOrder))
	copy(out, builtinProviderOrder)
	return out
}

// BuiltinProviders returns the built-in provider presets.
// These are available without any [providers] section in city.toml.
// Lifted from gastown's AgentPresetInfo table — only startup-relevant
// fields are included (session resume, hooks, etc. are future work).
func BuiltinProviders() map[string]ProviderSpec {
	return map[string]ProviderSpec{
		"claude": {
			DisplayName:            "Claude Code",
			Command:                `sh -c 'EXTRA=""; if command -v bd >/dev/null 2>&1 && [ -n "$GC_AGENT" ]; then WD=$(bd list --json --assignee="$GC_AGENT" --status=in-progress 2>/dev/null | jq -r ".[0].metadata.worktree_dir // empty" 2>/dev/null); [ -n "$WD" ] && [ -d "$WD" ] && EXTRA="--cwd $WD --continue"; fi; exec ${GC_CLI:-claude} --dangerously-skip-permissions $EXTRA "$@"' --`,
			PathCheck:              "claude",
			PromptMode:             "arg",
			ReadyDelayMs:           10000,
			ReadyPromptPrefix:      "\u276f ", // ❯
			ProcessNames:           []string{"node", "claude"},
			EmitsPermissionWarning: true,
			SupportsACP:            true,
			SupportsHooks:          true,
			InstructionsFile:       "CLAUDE.md",
			ResumeFlag:             "--resume",
			ResumeStyle:            "flag",
			SessionIDFlag:          "--session-id",
		},
		"codex": {
			DisplayName:      "Codex CLI",
			Command:          "codex",
			Args:             []string{"--dangerously-bypass-approvals-and-sandbox"},
			PromptMode:       "none",
			ReadyDelayMs:     3000,
			ProcessNames:     []string{"codex"},
			InstructionsFile: "AGENTS.md",
		},
		"gemini": {
			DisplayName:      "Gemini CLI",
			Command:          "gemini",
			Args:             []string{"--approval-mode", "yolo"},
			PromptMode:       "arg",
			ReadyDelayMs:     5000,
			ProcessNames:     []string{"gemini"},
			SupportsHooks:    true,
			InstructionsFile: "AGENTS.md",
		},
		"cursor": {
			DisplayName:      "Cursor Agent",
			Command:          "cursor-agent",
			Args:             []string{"-f"},
			PromptMode:       "arg",
			ProcessNames:     []string{"cursor-agent"},
			SupportsHooks:    true,
			InstructionsFile: "AGENTS.md",
		},
		"copilot": {
			DisplayName:       "GitHub Copilot",
			Command:           "copilot",
			Args:              []string{"--yolo"},
			PromptMode:        "arg",
			ReadyPromptPrefix: "\u276f ", // ❯
			ReadyDelayMs:      5000,
			ProcessNames:      []string{"copilot"},
			SupportsHooks:     true,
			InstructionsFile:  "AGENTS.md",
		},
		"amp": {
			DisplayName:      "Sourcegraph AMP",
			Command:          "amp",
			Args:             []string{"--dangerously-allow-all", "--no-ide"},
			PromptMode:       "arg",
			ProcessNames:     []string{"amp"},
			InstructionsFile: "AGENTS.md",
		},
		"opencode": {
			DisplayName:      "OpenCode",
			Command:          "opencode",
			Args:             []string{},
			PromptMode:       "arg",
			ReadyDelayMs:     8000,
			ProcessNames:     []string{"opencode", "node", "bun"},
			Env:              map[string]string{"OPENCODE_PERMISSION": `{"*":"allow"}`},
			SupportsACP:      true,
			SupportsHooks:    true,
			InstructionsFile: "AGENTS.md",
		},
		"auggie": {
			DisplayName:      "Auggie CLI",
			Command:          "auggie",
			Args:             []string{"--allow-indexing"},
			PromptMode:       "arg",
			ProcessNames:     []string{"auggie"},
			InstructionsFile: "AGENTS.md",
		},
		"pi": {
			DisplayName:      "Pi Coding Agent",
			Command:          "pi",
			Args:             []string{"-e", ".pi/extensions/gc-hooks.js"},
			PromptMode:       "arg",
			ReadyDelayMs:     8000,
			ProcessNames:     []string{"pi", "node", "bun"},
			SupportsHooks:    true,
			InstructionsFile: "AGENTS.md",
		},
		"omp": {
			DisplayName:      "Oh My Pi (OMP)",
			Command:          "omp",
			Args:             []string{"--hook", ".omp/hooks/gc-hook.ts"},
			PromptMode:       "arg",
			ProcessNames:     []string{"omp", "node", "bun"},
			SupportsHooks:    true,
			InstructionsFile: "AGENTS.md",
		},
	}
}
