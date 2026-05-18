package main

// Per-agent context-window management.
//
// Pre-investigation findings (recorded at v1 to avoid re-investigation):
//
//   - UserPromptSubmit is the only per-turn signal that fires reliably across
//     every supported provider's hook surface (see internal/hooks/config/README.md
//     for the event-vocabulary matrix). Wiring `gc compaction tick` here piggy-
//     backs on the same hook trigger the mail/nudge injectors already use, so a
//     city that has those hooks installed gets compaction observability "for
//     free" with no extra provider plumbing.
//
//   - Per-turn token usage is NOT surfaced into hook stdin by Claude Code,
//     codex, cursor, gemini, opencode, omp, or pi in their current shapes
//     (audit at v1: none of these write per-turn usage into the hook stdin
//     payload, and the on-disk session logs each provider produces use a
//     different schema). Pulling cumulative token counts would require either
//     (a) reading provider session logs and decoding per-provider shapes, or
//     (b) talking to the provider's own usage API, both of which are out of
//     scope for v1. ThresholdTokens is therefore parsed and validated but not
//     yet acted on; only ThresholdTurns triggers the policy today.
//
//   - PreCompact and equivalents already exist and fire when the provider
//     itself decides to compact. They are wired to `gc handoff --auto` via
//     internal/hooks/config/claude.json (and its per-provider siblings). The
//     operator's preemptive lever sits *before* PreCompact in the sequence:
//     `gc compaction tick` trips on UserPromptSubmit, cycles the session, and
//     PreCompact remains the fallback if the provider compacts first.
//
//   - The turn counter is stored in the session bead's metadata under
//     "compaction_turns" — same store the reconciler already reads (it
//     looks at "creation_complete_at", "template", "state", etc.). Following
//     that pattern keeps the counter durable across crashes and observable
//     via `gc session inspect`, without introducing a new on-disk format.

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/spf13/cobra"
)

const (
	// compactionTurnsMetaKey holds the running UserPromptSubmit count for a
	// session, persisted in the session bead's metadata so it survives
	// transient process restarts. Reset to 0 every time a configured
	// threshold trips.
	compactionTurnsMetaKey = "compaction_turns"
	// compactionLastFiredAtMetaKey records the RFC3339 timestamp when the
	// policy last fired. Informational — surfaced for `gc session inspect`
	// debugging; not consumed by the reconciler.
	compactionLastFiredAtMetaKey = "compaction_last_fired_at"
)

func newCompactionCmd(stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compaction",
		Short: "Per-agent context-window management",
		Long: `Manage operator-controlled context-window thresholds.

Today this exposes the "tick" hook command, intended to be wired into
the provider's UserPromptSubmit hook so the controller can preemptively
cycle a long-lived session before token costs balloon or budgets hit.
See "gc compaction tick --help" for details.`,
	}
	cmd.AddCommand(newCompactionTickCmd(stdout, stderr))
	return cmd
}

func newCompactionTickCmd(stdout, stderr io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "tick",
		Short: "Increment the current session's turn counter and apply the configured compaction policy",
		Long: `Increment the current session's UserPromptSubmit counter.

When the counter reaches the agent's [agent.compaction] threshold_turns,
the configured policy fires:

  policy = "handoff" (default when threshold_turns > 0):
    A self-mail is queued with the operator-authored message as its body,
    and a controller restart is requested. The reconciler stops the
    session on its next tick; the fresh session starts with the mail
    queued so the rendered message becomes the first thing it sees
    (delivered via the standard "gc mail check --inject" UserPromptSubmit
    hook). Non-blocking: the tick returns immediately so the user's next
    prompt is not delayed by waiting on the controller.

  policy = "warn":
    The rendered message is written to stderr (visible in the session's
    scrollback / supervisor log). The counter resets so the warning
    re-fires each window.

  policy = "reset":
    The counter resets silently. Intended for tests and dry-run rollouts.

The tick is designed to be safe to call when no compaction is configured
or when no session context is available: it exits 0 silently in those
cases so wiring it into a global UserPromptSubmit hook never breaks the
user prompt path.

Message rendering: shell-style $VAR placeholders in the operator-authored
message are expanded against the current environment ($GC_AGENT,
$GC_SESSION_ID, $GC_ALIAS, $BEAD_ID) plus two synthesized values,
$TURNS (the counter value at the moment the threshold tripped) and
$TOKENS (currently always 0 in v1 — see the package-level investigation
notes). Unknown placeholders pass through unchanged, so "$$5" and similar
literal-dollar uses survive.`,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if cmdCompactionTick(stdout, stderr) != 0 {
				return errExit
			}
			return nil
		},
	}
}

func cmdCompactionTick(stdout, stderr io.Writer) int {
	// All branches that return early do so silently: this command runs on
	// every UserPromptSubmit, so any noisy failure mode would attach to
	// every single user turn. The compaction feature is purely additive —
	// silent no-op is the correct behavior when prerequisites are missing.
	current, err := currentSessionRuntimeTarget()
	if err != nil {
		return 0
	}
	store, err := openCityStoreAt(current.cityPath)
	if err != nil || store == nil {
		return 0
	}
	cfg, _ := loadCityConfig(current.cityPath, stderr)
	if cfg == nil {
		return 0
	}
	rec := openCityRecorderAt(current.cityPath, stderr)
	sp := newSessionProvider()
	dops := newDrainOps(sp)
	persistRestart := sessionRestartPersister(current.cityPath, store, sp, cfg, current.sessionName)
	return doCompactionTick(store, cfg, rec, dops, persistRestart, current, stdout, stderr)
}

// doCompactionTick is the injectable core of `gc compaction tick`. Returns
// 0 in every success and silent-no-op path; only an explicit programmer
// error (which today never returns non-zero) would bubble up.
func doCompactionTick(
	store beads.Store,
	cfg *config.City,
	rec events.Recorder,
	dops drainOps,
	persistRestart func() error,
	current sessionRuntimeTarget,
	stdout, stderr io.Writer,
) int {
	agent, sessionBead, ok := resolveCompactionContext(store, cfg, current.sessionName)
	if !ok || !agent.Compaction.Enabled() {
		return 0
	}
	turns := readCompactionTurns(sessionBead) + 1
	if agent.Compaction.ThresholdTurns <= 0 || turns < agent.Compaction.ThresholdTurns {
		// Below threshold — just bump the counter. Errors here are best-
		// effort: a transient store write failure means we may double-bump
		// next turn, which is fine.
		_ = store.SetMetadataBatch(sessionBead.ID, map[string]string{
			compactionTurnsMetaKey: strconv.Itoa(turns),
		})
		return 0
	}
	// Threshold tripped.
	policy := agent.Compaction.EffectivePolicy()
	rendered := renderCompactionMessage(agent.Compaction.Message, current.display, current.sessionName, turns, 0)
	switch policy {
	case "handoff":
		applyCompactionHandoff(store, rec, dops, persistRestart, current, rendered, sessionBead.ID, stdout, stderr)
	case "warn":
		fmt.Fprintln(stderr, rendered) //nolint:errcheck // best-effort stderr
	case "reset":
		// nothing to do beyond resetting the counter below
	default:
		// EffectivePolicy normalizes empty to "handoff" when enabled and
		// rejects unknown values via validation, but be defensive: ignore.
		return 0
	}
	resetCompactionCounter(store, sessionBead.ID)
	return 0
}

// resolveCompactionContext returns the agent config and session bead for the
// running session, or ok=false when the session bead is not resolvable or its
// template does not match any configured agent.
func resolveCompactionContext(store beads.Store, cfg *config.City, sessionName string) (*config.Agent, beads.Bead, bool) {
	id, err := resolveSessionID(store, sessionName)
	if err != nil {
		return nil, beads.Bead{}, false
	}
	b, err := store.Get(id)
	if err != nil {
		return nil, beads.Bead{}, false
	}
	template := strings.TrimSpace(b.Metadata["template"])
	if template == "" {
		return nil, beads.Bead{}, false
	}
	agent := findAgentByTemplate(cfg, template)
	if agent == nil {
		return nil, beads.Bead{}, false
	}
	return agent, b, true
}

func readCompactionTurns(b beads.Bead) int {
	raw := strings.TrimSpace(b.Metadata[compactionTurnsMetaKey])
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func resetCompactionCounter(store beads.Store, sessionID string) {
	_ = store.SetMetadataBatch(sessionID, map[string]string{
		compactionTurnsMetaKey:       "0",
		compactionLastFiredAtMetaKey: time.Now().UTC().Format(time.RFC3339),
	})
}

// applyCompactionHandoff queues self-mail with the rendered message and
// requests a controller restart. Non-blocking: this runs from a
// UserPromptSubmit hook and must not block the user's next prompt waiting
// for the reconciler.
func applyCompactionHandoff(
	store beads.Store,
	rec events.Recorder,
	dops drainOps,
	persistRestart func() error,
	current sessionRuntimeTarget,
	rendered, sessionBeadID string,
	stdout, stderr io.Writer,
) {
	subject := fmt.Sprintf("Compaction handoff: %s", current.display)
	args := []string{subject, rendered}
	mail, ok := createHandoffMail(store, rec, current.display, current.display, args, subject, stderr)
	if !ok {
		// createHandoffMail already wrote a diagnostic to stderr.
		return
	}
	if dops != nil {
		if err := dops.setRestartRequested(current.sessionName); err != nil {
			fmt.Fprintf(stderr, "gc compaction tick: requesting restart: %v\n", err) //nolint:errcheck // best-effort stderr
		}
	}
	// Persist the restart flag through the worker boundary so it survives
	// tmux session death, mirroring `gc runtime request-restart` and
	// `gc handoff`. Non-fatal: the runtime flag above is primary.
	if persistRestart != nil {
		if err := persistRestart(); err != nil {
			fmt.Fprintf(stderr, "gc compaction tick: persisting restart: %v\n", err) //nolint:errcheck // best-effort stderr
		}
	}

	fmt.Fprintf(stdout, "Compaction: sent mail %s, restart requested (session bead %s).\n", mail.ID, sessionBeadID) //nolint:errcheck // best-effort stdout
}

// renderCompactionMessage expands a small, fixed set of shell-style $VAR
// placeholders in the operator-authored message. Unknown placeholders are
// left as-is so the operator can safely include literal "$" characters
// without escaping. Empty input returns a small canned default so the
// handoff mail body is never empty.
func renderCompactionMessage(template, agentDisplay, sessionName string, turns, tokens int) string {
	if strings.TrimSpace(template) == "" {
		// Operator did not configure a message. Use a self-documenting
		// default so the new session has a useful opener.
		template = "Auto handoff: $GC_AGENT crossed $TURNS turns; summarize state and continue."
	}
	env := compactionEnv(agentDisplay, sessionName, turns, tokens)
	return os.Expand(template, func(key string) string {
		// os.Expand passes $$ through as a single "$" character via key "$"
		// only when the literal sequence is $$ — but to be safe, treat
		// unknown keys as "leave the placeholder intact" so operators can
		// embed literal $-strings without escaping every dollar.
		if v, ok := env[key]; ok {
			return v
		}
		return "$" + key
	})
}

// applyAgentCompactionMessageToAutoArgs replaces the default subject/message
// passed to `gc handoff --auto` with the agent's configured compaction
// message when one is set. Operator intent: provider-initiated compaction
// (PreCompact) and gc-initiated compaction (the tick path above) should
// queue the same operator-authored opener for the next session.
//
// When the lookup fails — no session bead, unknown agent template, no
// configured Compaction.Message — args is returned unchanged so PreCompact
// keeps its historical "context cycle" behavior.
func applyAgentCompactionMessageToAutoArgs(store beads.Store, cfg *config.City, current sessionRuntimeTarget, args []string) []string {
	if store == nil || cfg == nil {
		return args
	}
	agent, sessionBead, ok := resolveCompactionContext(store, cfg, current.sessionName)
	if !ok {
		return args
	}
	if strings.TrimSpace(agent.Compaction.Message) == "" {
		return args
	}
	turns := readCompactionTurns(sessionBead)
	rendered := renderCompactionMessage(agent.Compaction.Message, current.display, current.sessionName, turns, 0)
	subject := fmt.Sprintf("Compaction handoff: %s", current.display)
	// Preserve any operator-passed override that arrived as args[0]; only
	// substitute when the hook called us with the default zero-arg shape.
	if len(args) == 0 {
		return []string{subject, rendered}
	}
	if len(args) == 1 {
		return []string{args[0], rendered}
	}
	return args
}

// compactionEnv builds the variable map used by renderCompactionMessage.
// Pulls from process env first, falling back to the session-context values
// so the hook works even if a provider strips parts of the parent env.
func compactionEnv(agentDisplay, sessionName string, turns, tokens int) map[string]string {
	env := map[string]string{
		"GC_AGENT":      firstNonEmpty(os.Getenv("GC_AGENT"), agentDisplay),
		"GC_SESSION_ID": firstNonEmpty(os.Getenv("GC_SESSION_ID"), os.Getenv("GC_ALIAS"), sessionName),
		"GC_ALIAS":      firstNonEmpty(os.Getenv("GC_ALIAS"), agentDisplay),
		"BEAD_ID":       os.Getenv("BEAD_ID"),
		"TURNS":         strconv.Itoa(turns),
		"TOKENS":        strconv.Itoa(tokens),
	}
	return env
}

