// Package agent provides the Agent interface for managed agent lifecycle.
//
// An Agent encapsulates identity (name, session name) and lifecycle
// operations (start, stop, attach) backed by a [runtime.Provider].
// The CLI layer builds agents from config; the do* functions operate
// on them without knowing how sessions are implemented.
package agent

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
)

// Handle is the narrow interface for per-agent operations that don't
// require lifecycle management or session configuration. Use HandleFor()
// to construct lightweight handles for CLI commands that only need to
// query, nudge, peek, or stop an agent.
type Handle interface {
	// Name returns the agent's configured name.
	Name() string

	// SessionName returns the session identifier for this agent.
	SessionName() string

	// IsRunning reports whether the agent's session is active.
	IsRunning() bool

	// IsAttached reports whether a user terminal is connected to this
	// agent's session.
	IsAttached() bool

	// Stop destroys the agent's session.
	Stop() error

	// Interrupt sends a soft interrupt signal (e.g., Ctrl-C) to the agent.
	// Best-effort: returns nil if the session doesn't exist.
	Interrupt() error

	// Peek captures the last N lines of the agent's session output.
	Peek(lines int) (string, error)

	// Nudge sends a message to wake or redirect the agent.
	Nudge(message string) error

	// SetMeta stores a key-value pair associated with the agent's session.
	SetMeta(key, value string) error

	// GetMeta retrieves a previously stored metadata value.
	GetMeta(key string) (string, error)

	// RemoveMeta removes a metadata key from the agent's session.
	RemoveMeta(key string) error

	// Events returns a channel of structured observation events from the
	// agent's session, or nil if no observer is attached.
	Events() <-chan Event
}

// Agent is the full interface — Handle plus lifecycle management and
// session configuration. Use New() to construct full agents for the
// controller and reconciler.
type Agent interface {
	Handle

	// Start creates the agent's session. The context controls the startup
	// deadline — the call returns early with ctx.Err() on cancellation.
	Start(ctx context.Context) error

	// Attach connects the user's terminal to the agent's session.
	Attach() error

	// SessionConfig returns the runtime.Config this agent would use
	// when starting. Used by reconciliation to compute config fingerprints
	// without actually starting the agent.
	SessionConfig() runtime.Config

	// ClearScrollback clears the scrollback history of the agent's session.
	// Best-effort.
	ClearScrollback() error

	// GetLastActivity returns the time of the last I/O activity in the
	// agent's session. Returns zero time if unknown or unsupported.
	GetLastActivity() (time.Time, error)

	// SendKeys sends bare keystrokes to the agent's session.
	// Unlike Nudge, does not append Enter.
	SendKeys(keys ...string) error

	// RunLive re-applies session_live commands to the running session.
	RunLive(cfg runtime.Config) error

	// ProcessAlive reports whether the agent's session has a live process
	// matching its configured process names.
	ProcessAlive() bool

	// SetObserver attaches an ObservationStrategy to the agent. The observer
	// is independent of the execution runtime — it can read JSONL files,
	// scrape terminal output, or use any other observation mechanism.
	// Replaces any previously set observer (closing the old one).
	SetObserver(obs ObservationStrategy)
}

// StartupHints carries provider startup behavior from config resolution
// through to runtime.Config. All fields are optional — zero values mean
// no special startup handling (fire-and-forget).
type StartupHints struct {
	ReadyPromptPrefix      string
	ReadyDelayMs           int
	ProcessNames           []string
	EmitsPermissionWarning bool
	// Nudge is text typed into the session after the agent is ready.
	// Used for CLI agents that don't accept command-line prompts.
	Nudge string
	// PreStart is a list of shell commands run before session creation.
	// Already template-expanded by the caller.
	PreStart []string
	// SessionSetup is a list of shell commands run after session creation.
	// Already template-expanded by the caller.
	SessionSetup []string
	// SessionSetupScript is a script path run after session_setup commands.
	SessionSetupScript string
	// SessionLive is a list of idempotent commands run after session_setup
	// and re-applied on config change without restart.
	SessionLive []string
	// PackOverlayDirs lists overlay directories from packs. Copied to
	// the session workdir before the agent's own OverlayDir.
	PackOverlayDirs []string
	// OverlayDir is the resolved overlay directory path on the host.
	// Passed through to the exec session provider for remote copy.
	OverlayDir string
	// CopyFiles lists files/directories to stage in the session's working
	// directory before the agent command starts.
	CopyFiles []runtime.CopyEntry
}

// sessionData holds template variables for custom session naming.
type sessionData struct {
	City  string // workspace name
	Agent string // tmux-safe qualified name (/ → --)
	Dir   string // rig/dir component (empty for singletons)
	Name  string // bare agent name
}

// SessionNameFor returns the session name for a city agent.
// This is the single source of truth for the naming convention.
// sessionTemplate is a Go text/template string; empty means use the
// default pattern "{agent}" (the sanitized agent name). With per-city
// tmux socket isolation as the default, the city prefix is unnecessary.
//
// For rig-scoped agents (name contains "/"), the dir and name
// components are joined with "--" to avoid tmux naming issues:
//
//	"mayor"               → "mayor"
//	"hello-world/polecat" → "hello-world--polecat"
func SessionNameFor(cityName, agentName, sessionTemplate string) string {
	// Pre-sanitize: replace "/" with "--" for tmux safety.
	sanitized := strings.ReplaceAll(agentName, "/", "--")

	if sessionTemplate == "" {
		// Default: just the sanitized agent name. Per-city tmux socket
		// isolation makes a city prefix redundant.
		return sanitized
	}

	// Parse dir/name components for template variables.
	var dir, name string
	if i := strings.LastIndex(agentName, "/"); i >= 0 {
		dir = agentName[:i]
		name = agentName[i+1:]
	} else {
		name = agentName
	}

	tmpl, err := template.New("session").Parse(sessionTemplate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gc: session_template parse error: %v (using default)\n", err)
		return sanitized
	}

	var buf bytes.Buffer
	data := sessionData{
		City:  cityName,
		Agent: sanitized,
		Dir:   dir,
		Name:  name,
	}
	if err := tmpl.Execute(&buf, data); err != nil {
		fmt.Fprintf(os.Stderr, "gc: session_template execute error: %v (using default)\n", err)
		return sanitized
	}
	return buf.String()
}

// New creates an Agent backed by the given session provider.
// name is the agent's configured name (from TOML). cityName is the city's
// workspace name — used to derive the session name. prompt is the agent's
// initial prompt content (appended to command via shell quoting). env is
// additional environment variables for the session. hints carries provider
// startup behavior for session readiness detection. workDir is the working
// directory for the agent's session (empty means provider default).
// sessionTemplate is a Go text/template for session naming (empty = default).
// fpExtra carries additional data for config fingerprinting (e.g.
// pool config) that isn't part of the session command.
func New(name, cityName, command, prompt string,
	env map[string]string, hints StartupHints, workDir string,
	sessionTemplate string,
	fpExtra map[string]string,
	sp runtime.Provider,
	onStop ...func() error,
) Agent {
	return &managed{
		name:        name,
		sessionName: SessionNameFor(cityName, name, sessionTemplate),
		command:     command,
		prompt:      prompt,
		env:         env,
		hints:       hints,
		workDir:     workDir,
		fpExtra:     fpExtra,
		sp:          sp,
		onStop:      onStop,
	}
}

// HandleFor creates a lightweight Handle for an agent. Use this when you
// only need to query, nudge, peek, or stop an agent — not manage its
// full lifecycle. Takes 4 params vs New()'s 10.
func HandleFor(name, cityName, sessionTemplate string, sp runtime.Provider, onStop ...func() error) Handle {
	return &managed{
		name:        name,
		sessionName: SessionNameFor(cityName, name, sessionTemplate),
		sp:          sp,
		onStop:      onStop,
	}
}

// managed is the concrete Agent implementation that delegates to a
// runtime.Provider using the agent's session name.
type managed struct {
	name        string
	sessionName string
	command     string
	prompt      string
	env         map[string]string
	hints       StartupHints
	workDir     string
	fpExtra     map[string]string
	sp          runtime.Provider
	observer    ObservationStrategy // nil = no structured observation
	onStop      []func() error      // cleanup callbacks run after session stop
}

func (a *managed) Name() string        { return a.name }
func (a *managed) SessionName() string { return a.sessionName }
func (a *managed) IsRunning() bool {
	if !a.sp.IsRunning(a.sessionName) {
		return false
	}
	return a.sp.ProcessAlive(a.sessionName, a.hints.ProcessNames)
}

func (a *managed) IsAttached() bool { return a.sp.IsAttached(a.sessionName) }

func (a *managed) Stop() error {
	if a.observer != nil {
		a.observer.Close() //nolint:errcheck // best-effort cleanup
		a.observer = nil
	}
	if err := a.sp.Stop(a.sessionName); err != nil {
		return err
	}
	// Best-effort cleanup callbacks (session is dead).
	for _, fn := range a.onStop {
		_ = fn()
	}
	return nil
}
func (a *managed) Attach() error              { return a.sp.Attach(a.sessionName) }
func (a *managed) Nudge(message string) error { return a.sp.Nudge(a.sessionName, message) }
func (a *managed) Peek(lines int) (string, error) {
	return a.sp.Peek(a.sessionName, lines)
}

// SessionConfig returns the runtime.Config this agent would use when starting.
func (a *managed) SessionConfig() runtime.Config {
	cmd := a.command
	if a.prompt != "" {
		cmd = cmd + " " + shellQuote(a.prompt)
	}
	return runtime.Config{
		Command:                cmd,
		Env:                    a.env,
		WorkDir:                a.workDir,
		ReadyPromptPrefix:      a.hints.ReadyPromptPrefix,
		ReadyDelayMs:           a.hints.ReadyDelayMs,
		ProcessNames:           a.hints.ProcessNames,
		EmitsPermissionWarning: a.hints.EmitsPermissionWarning,
		Nudge:                  a.hints.Nudge,
		PreStart:               a.hints.PreStart,
		SessionSetup:           a.hints.SessionSetup,
		SessionSetupScript:     a.hints.SessionSetupScript,
		SessionLive:            a.hints.SessionLive,
		PackOverlayDirs:        a.hints.PackOverlayDirs,
		OverlayDir:             a.hints.OverlayDir,
		CopyFiles:              a.hints.CopyFiles,
		FingerprintExtra:       a.fpExtra,
	}
}

func (a *managed) Interrupt() error                    { return a.sp.Interrupt(a.sessionName) }
func (a *managed) ProcessAlive() bool                  { return a.sp.ProcessAlive(a.sessionName, a.hints.ProcessNames) }
func (a *managed) ClearScrollback() error              { return a.sp.ClearScrollback(a.sessionName) }
func (a *managed) GetLastActivity() (time.Time, error) { return a.sp.GetLastActivity(a.sessionName) }
func (a *managed) SendKeys(keys ...string) error       { return a.sp.SendKeys(a.sessionName, keys...) }
func (a *managed) RunLive(cfg runtime.Config) error    { return a.sp.RunLive(a.sessionName, cfg) }
func (a *managed) SetMeta(key, value string) error     { return a.sp.SetMeta(a.sessionName, key, value) }
func (a *managed) GetMeta(key string) (string, error)  { return a.sp.GetMeta(a.sessionName, key) }
func (a *managed) RemoveMeta(key string) error         { return a.sp.RemoveMeta(a.sessionName, key) }

func (a *managed) Events() <-chan Event {
	if a.observer == nil {
		return nil
	}
	return a.observer.Events()
}

func (a *managed) SetObserver(obs ObservationStrategy) {
	if a.observer != nil {
		a.observer.Close() //nolint:errcheck // best-effort cleanup
	}
	a.observer = obs
}

func (a *managed) Start(ctx context.Context) error {
	return a.sp.Start(ctx, a.sessionName, a.SessionConfig())
}

// shellQuote wraps s in single quotes, escaping any embedded single quotes
// using the standard shell idiom: replace ' with '\”.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
