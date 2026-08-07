package core

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/orders"
)

// readOrder parses an order TOML from the embedded pack FS and restores the
// Name the scanner would normally derive from the filename (Parse leaves it
// blank because Name is not a TOML field).
func readOrder(t *testing.T, file string) orders.Order {
	t.Helper()
	data, err := fs.ReadFile(PackFS, "orders/"+file)
	if err != nil {
		t.Fatalf("reading orders/%s: %v", file, err)
	}
	o, err := orders.Parse(data)
	if err != nil {
		t.Fatalf("parsing orders/%s: %v", file, err)
	}
	o.Name = strings.TrimSuffix(file, ".toml")
	return o
}

// TestCoreOrdersValidate asserts every embedded order TOML parses and
// passes structural validation, so a malformed order can never ship in the gc
// binary's bundled core pack.
func TestCoreOrdersValidate(t *testing.T) {
	entries, err := fs.ReadDir(PackFS, "orders")
	if err != nil {
		t.Fatalf("reading orders dir: %v", err)
	}
	saw := false
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".toml") {
			continue
		}
		saw = true
		o := readOrder(t, e.Name())
		if err := orders.Validate(o); err != nil {
			t.Errorf("order %s failed validation: %v", e.Name(), err)
		}
	}
	if !saw {
		t.Fatal("no order TOML files found in embedded pack")
	}
}

// assertEventExecOrder checks an event-triggered exec order: it must validate,
// listen for the expected event type, dispatch via exec (not a formula/pool),
// and point at a script that is actually embedded in the pack.
func assertEventExecOrder(t *testing.T, orderFile, eventType, scriptBase string) {
	t.Helper()
	o := readOrder(t, orderFile)
	if err := orders.Validate(o); err != nil {
		t.Fatalf("%s failed validation: %v", orderFile, err)
	}
	if o.Trigger != "event" {
		t.Errorf("%s: trigger = %q, want %q", orderFile, o.Trigger, "event")
	}
	if o.On != eventType {
		t.Errorf("%s: on = %q, want %q", orderFile, o.On, eventType)
	}
	if !o.IsExec() {
		t.Errorf("%s: want exec dispatch, got formula %q", orderFile, o.Formula)
	}
	if o.Pool != "" {
		t.Errorf("%s: exec orders must not set a pool, got %q", orderFile, o.Pool)
	}
	wantSuffix := "assets/scripts/" + scriptBase
	if !strings.HasSuffix(o.Exec, wantSuffix) {
		t.Errorf("%s: exec = %q, want suffix %q", orderFile, o.Exec, wantSuffix)
	}
	if _, err := fs.ReadFile(PackFS, "assets/scripts/"+scriptBase); err != nil {
		t.Errorf("%s: referenced script not embedded: %v", orderFile, err)
	}
}

// TestNudgeOnRouteOrder pins the nudge-on-route order's event contract: it wakes
// on bead.updated and runs the nudge-on-route script.
func TestNudgeOnRouteOrder(t *testing.T) {
	assertEventExecOrder(t, "nudge-on-route.toml", "bead.updated", "nudge-on-route.sh")
}

// TestCascadeNudgeOnBlockerCloseOrder pins the cascade-nudge order's event
// contract: it wakes on bead.closed — the event the close transition actually
// emits — and runs the cascade-nudge script.
func TestCascadeNudgeOnBlockerCloseOrder(t *testing.T) {
	assertEventExecOrder(t, "cascade-nudge-on-blocker-close.toml", "bead.closed", "cascade-nudge-on-blocker-close.sh")
}

// TestCascadeNudgeRoutesCrossRig guards the cascade order's cross-rig
// routing. Two properties must hold or cross-rig cascades break silently
// (failures are soft-skipped via `|| continue`, so a regression is invisible
// at runtime): (1) the dependent lookup runs through the `gc bd` wrapper, not
// bare `bd` — `--rig` is a gc flag, not a bd flag, and the wrapper runs bd in
// the owning rig's directory; (2) the prefix->rig lookup excludes the HQ entry
// (`gc rig list` reports the city root as an hq=true pseudo-rig that
// `gc --rig <cityName>` cannot resolve), matching orphan-sweep.sh's
// `select(.hq == false)` convention.
func TestCascadeNudgeRoutesCrossRig(t *testing.T) {
	data, err := fs.ReadFile(PackFS, "assets/scripts/cascade-nudge-on-blocker-close.sh")
	if err != nil {
		t.Fatalf("reading cascade-nudge-on-blocker-close.sh: %v", err)
	}
	body := string(data)
	if !strings.Contains(body, "gc bd dep list") {
		t.Error("cascade-nudge script must route the dep lookup through `gc bd dep list`; missing")
	}
	if strings.Contains(body, "$(bd dep list") {
		t.Error("cascade-nudge script must not run bare `bd dep list` (--rig is a gc flag, not a bd flag)")
	}
	if !strings.Contains(body, ".hq != true") {
		t.Error("cascade-nudge script must exclude the HQ entry from the prefix->rig lookup; missing `.hq != true`")
	}
}

// TestNudgeOnRouteResolvesPoolMembers guards the pool-base fan-out: a
// multi-session pool routes to the pool BASE (sling's NormalizePoolRouteTarget
// collapses slot -> base), which is the members' template, not a session name
// `gc session nudge` can resolve. The script must therefore enumerate pool
// members by template before nudging — a naive `gc session nudge "$routed_to"`
// silently no-ops for exactly the warm-idle pool workers this order targets.
func TestNudgeOnRouteResolvesPoolMembers(t *testing.T) {
	data, err := fs.ReadFile(PackFS, "assets/scripts/nudge-on-route.sh")
	if err != nil {
		t.Fatalf("reading nudge-on-route.sh: %v", err)
	}
	body := string(data)
	for _, want := range []string{"gc session list", "--template"} {
		if !strings.Contains(body, want) {
			t.Errorf("nudge-on-route.sh must resolve pool members; missing %q", want)
		}
	}
}

// TestNotifyOnHumanGateCreationOrder pins the notify-on-human-gate-creation
// order's event contract: it wakes on bead.created — the event synthesized for
// any newly-appeared bead — and runs the notify-on-human-gate-creation script.
func TestNotifyOnHumanGateCreationOrder(t *testing.T) {
	assertEventExecOrder(t, "notify-on-human-gate-creation.toml", "bead.created", "notify-on-human-gate-creation.sh")
}

// TestNotifyOnHumanGateCreationScriptContract guards the load-bearing behaviors
// of the notify script. Each property, if it regresses, breaks the order
// silently (failures are best-effort and swallowed at runtime), so they are
// pinned here:
//
//   - The bead.created payload does NOT carry await_type, so a human gate is
//     indistinguishable from a timer/gh gate at the event alone. The script
//     must re-fetch the bead via `gc bd show` and gate on await_type == "human"
//     AND status == "open" — otherwise it would notify on every gate creation
//     (or none).
//   - Addressee resolution must consult gc.deferred_assignee: formula/molecule
//     gates strip the assignee to that metadata key at create time, so a naive
//     `.assignee`-only lookup finds an empty addressee and misroutes to the
//     human fallback for exactly the automated gates that name a real one.
//   - Notification must ride `gc mail send --notify`, the one primitive that
//     mails AND nudges a real session while natively skipping the tmux-nudge
//     for the sessionless "human" recipient (cmd_mail.go `to != "human"`). A
//     hand-rolled `gc session nudge` would fail on the human channel.
//   - The prefix->rig lookup must exclude the HQ entry (`gc rig list` reports
//     the city root as an hq=true pseudo-rig `gc --rig <cityName>` cannot
//     resolve), matching the cross-rig convention in the sibling scripts.
//   - Event-shape robustness: the API envelope wraps the bead under
//     .payload.bead, but the `gc events` local fallback (API down) emits the
//     bead fields directly under .payload. The filter must read both via
//     `(.payload.bead // .payload)` or it silently finds no gates in fallback
//     mode — exactly when notifications matter most.
//   - Loud-fail: an undeliverable send must surface and NOT be recorded as
//     done. Surfacing requires a NON-ZERO exit — the controller logs an exec
//     order's captured output only on a non-zero exit — so the script must
//     exit non-zero when any send failed (gastownhall/gascity#4543).
func TestNotifyOnHumanGateCreationScriptContract(t *testing.T) {
	data, err := fs.ReadFile(PackFS, "assets/scripts/notify-on-human-gate-creation.sh")
	if err != nil {
		t.Fatalf("reading notify-on-human-gate-creation.sh: %v", err)
	}
	body := string(data)

	for _, want := range []string{
		"(.payload.bead // .payload)", // normalize API-envelope vs local-fallback event shape
		`$b.issue_type == "gate"`,     // filter events to gate creations
		"gc bd show",                  // re-fetch (event lacks await_type)
		`"$AWAIT_TYPE" = "human"`,     // human gates only
		`"$STATUS" = "open"`,          // skip already-resolved gates
		`gc.deferred_assignee`,        // formula/molecule addressee
		"--notify",                    // mail + nudge, human-safe primitive
		".hq != true",                 // exclude HQ from prefix->rig lookup
	} {
		if !strings.Contains(body, want) {
			t.Errorf("notify-on-human-gate-creation.sh missing load-bearing element %q", want)
		}
	}

	// Loud-fail: the send must be conditional (retry on failure), and the
	// failure path must surface to stderr rather than silently record the gate
	// as notified. The dedup record must live on the SUCCESS branch only.
	if !strings.Contains(body, "if gc mail send") {
		t.Error("notify-on-human-gate-creation.sh must branch on the mail-send result (loud-fail retry), not fire-and-forget")
	}
	if !strings.Contains(body, "will retry next sweep") {
		t.Error("notify-on-human-gate-creation.sh must surface an undeliverable send to stderr (loud-fail #4543)")
	}
	// The controller captures an exec order's combined output but logs it only
	// on a NON-ZERO exit (order_dispatch.go), so a fire-and-forget exit 0 would
	// swallow the failure lines above. The script must exit non-zero when any
	// send failed — after writing state, so recorded successes are not lost.
	if !strings.Contains(body, `"$FAILED" -gt 0`) {
		t.Error("notify-on-human-gate-creation.sh must exit non-zero when a send failed, or the loud-fail message is never logged (#4543)")
	}
}

// assertCooldownExecOrder checks a cooldown-triggered exec order: it must
// validate, run on a cooldown trigger with a parseable interval, dispatch via
// exec (not a formula/pool), and point at a script embedded in the pack.
func assertCooldownExecOrder(t *testing.T, orderFile, scriptBase string) {
	t.Helper()
	o := readOrder(t, orderFile)
	if err := orders.Validate(o); err != nil {
		t.Fatalf("%s failed validation: %v", orderFile, err)
	}
	if o.Trigger != "cooldown" {
		t.Errorf("%s: trigger = %q, want %q", orderFile, o.Trigger, "cooldown")
	}
	if _, err := time.ParseDuration(o.Interval); err != nil {
		t.Errorf("%s: interval %q is not a valid duration: %v", orderFile, o.Interval, err)
	}
	if !o.IsExec() {
		t.Errorf("%s: want exec dispatch, got formula %q", orderFile, o.Formula)
	}
	if o.Pool != "" {
		t.Errorf("%s: exec orders must not set a pool, got %q", orderFile, o.Pool)
	}
	wantSuffix := "assets/scripts/" + scriptBase
	if !strings.HasSuffix(o.Exec, wantSuffix) {
		t.Errorf("%s: exec = %q, want suffix %q", orderFile, o.Exec, wantSuffix)
	}
	if _, err := fs.ReadFile(PackFS, "assets/scripts/"+scriptBase); err != nil {
		t.Errorf("%s: referenced script not embedded: %v", orderFile, err)
	}
}

// shellCodeOnly strips the shebang and whole-line `#` comments from a shell
// script so a "must not contain" assertion tests what the script DOES rather
// than what its header says about what it used to do.
func shellCodeOnly(body string) string {
	var code []string
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		code = append(code, line)
	}
	return strings.Join(code, "\n")
}

// TestWispCompactOrder pins the wisp-compact order's contract: a
// cooldown-triggered exec order running the wisp-compact script.
func TestWispCompactOrder(t *testing.T) {
	assertCooldownExecOrder(t, "wisp-compact.toml", "wisp-compact.sh")
}

// TestWispCompactScriptContract guards the enumeration and observability
// contract of the wisp sweep. The regression it exists to prevent ran silently
// for two days in a live city: the script enumerated with `gc bd list --json
// --all -n 0` and kept only `select(.ephemeral == true)`, but `bd list`
// excludes ephemeral beads outright AND omits the `ephemeral` field from its
// projection, so the filter matched zero records of 33,322 live wisps. The
// sweep reaped nothing and exited 0, which is indistinguishable from a healthy
// run — the controller logs an exec order's output only on a NON-ZERO exit
// (order_dispatch.go), so the summary line never reached the log either.
//
//   - Enumeration MUST go through `gc bd query ephemeral=true`, the only
//     listing that returns wisps, with `--limit 0` so a large backlog is not
//     silently truncated to the default page.
//   - `bd list` MUST NOT be the enumeration source, and the
//     `select(.ephemeral == true)` filter it fed must not come back.
//   - Loud-fail (#4543): an enumeration failure, an empty result, and a failed
//     action must each exit non-zero, or the failure is swallowed exactly the
//     way the original was.
//   - Deletion MUST be batched via `--from-file`: each `gc bd` invocation costs
//     about a second of process startup, so one call per bead put the sweep
//     past its 300s deadline once the backlog grew.
//   - The run MUST be bounded by a wall-clock budget, so a backlog larger than
//     one run drains across sweeps instead of dying at the deadline.
//   - Deletion scope stays closed-only: a non-closed wisp past TTL is promoted
//     for stuck detection, never deleted.
func TestWispCompactScriptContract(t *testing.T) {
	data, err := fs.ReadFile(PackFS, "assets/scripts/wisp-compact.sh")
	if err != nil {
		t.Fatalf("reading wisp-compact.sh: %v", err)
	}
	// Every assertion runs against executable lines only. The script's header
	// documents the defect and names each knob in prose, so testing the raw
	// body would let a comment satisfy a requirement the code no longer meets —
	// which is exactly how the `--from-file` assertion first shipped vacuous.
	code := shellCodeOnly(string(data))

	// Each element pins a load-bearing CONSTRUCT, not merely a name. Stripping
	// comments is not sufficient on its own: a knob's name also appears inside
	// the operator-facing error message that tells the reader how to set it, so
	// asserting the bare name lets that message satisfy the requirement after
	// the guard it describes has been deleted. Assert the expansion form, which
	// only the guard can produce.
	for _, want := range []string{
		"gc bd query --json 'ephemeral=true'", // the only enumeration that returns wisps
		"--limit 0",                           // no silent truncation of the backlog
		"--from-file",                         // batched delete, not one gc bd call per bead
		"${GC_WISP_COMPACT_BUDGET:-",          // per-run wall-clock bound, read not just named
		"${GC_WISP_COMPACT_ALLOW_EMPTY:-",     // the opt-out guard itself, not its error text
	} {
		if !strings.Contains(code, want) {
			t.Errorf("wisp-compact.sh missing load-bearing element %q", want)
		}
	}

	// The exact enumeration that made the sweep a silent no-op. `bd list` does
	// not return ephemeral beads and does not carry the field the filter tests,
	// so either half coming back reintroduces the regression. Checked against
	// executable lines only: the script's own header explains the defect in
	// prose, and documenting it must not read as committing it.
	if strings.Contains(code, "bd list") {
		t.Error("wisp-compact.sh must not enumerate via `bd list`: it excludes ephemeral beads and omits the ephemeral field, so the sweep silently matches nothing")
	}
	if strings.Contains(code, "select(.ephemeral == true)") {
		t.Error("wisp-compact.sh must not filter a list projection on .ephemeral: the field is absent from `bd list` output, so the filter always matches zero beads")
	}

	// Loud-fail: the controller logs an exec order's output only on a non-zero
	// exit, so a swallowed enumeration failure is invisible. `|| exit 0` after
	// the enumeration is the specific construct that hid the original.
	//
	// Pin the message AND the non-zero exit beneath it, not the message alone.
	// The exit is the load-bearing half: the message is only ever read because
	// a non-zero status made the controller log it, so a message with no exit
	// behind it is written to a stream nobody reads. Asserting the text on its
	// own is vacuous in the same way the opt-out and deletion-scope checks
	// were before they were pinned to their expansion and routing forms —
	// flipping `exit 1` to `exit 0` is the single likeliest edit if this order
	// is ever called noisy, and it restores the silent sweep this whole change
	// exists to remove while leaving every message in place.
	//
	// The exit must be the next non-blank line after the message. That is
	// deliberately strict: inserting a line between them turns this red, which
	// is the correct failure mode for an assertion whose defect would
	// otherwise be silence.
	for _, want := range []struct {
		re     *regexp.Regexp
		reason string
	}{
		{
			re:     regexp.MustCompile(`enumerating ephemeral beads failed[^\n]*\n\s*exit\s+[1-9]`),
			reason: "must surface an enumeration failure to stderr AND exit non-zero (loud-fail #4543)",
		},
		{
			re:     regexp.MustCompile(`could not be actioned[^\n]*\n\s*exit\s+[1-9]`),
			reason: "must exit non-zero when a wisp could not be actioned, or the loud-fail message is never logged (#4543)",
		},
	} {
		if !want.re.MatchString(code) {
			t.Errorf("wisp-compact.sh %s", want.reason)
		}
	}
	// Retained alongside the exit pin above: this one guards that the failure
	// counter is still what gates the loud exit, which the regexp does not say.
	if !strings.Contains(code, `"$FAILED" -gt 0`) {
		t.Error("wisp-compact.sh must gate its non-zero exit on the failed-action counter (#4543)")
	}

	// Deletion scope: a non-closed wisp past TTL is promoted for stuck
	// detection, never deleted. Widening this would delete live work.
	//
	// Pin the PROMOTE-branch disjunct specifically. `$b.status != "closed"`
	// on its own also occurs in the reason ternary immediately below the
	// guard, which merely selects the wording of the promotion comment — so
	// the bare substring stays present after the guard is removed, and the
	// assertion survives its own defect. Deleting the disjunct flips every
	// open-past-TTL wisp from promoted to deleted, which is the one scope
	// widening this order must never make.
	if !strings.Contains(code, `or ($b.status != "closed") then`) {
		t.Error("wisp-compact.sh must promote non-closed wisps past TTL rather than delete them (stuck detection)")
	}
}

// TestRenudgeStaleHumanGatesOrder pins the staleness-sweep order's contract: it
// is a cooldown-triggered exec order running the renudge-stale-human-gates
// script. It is the repeating companion to notify-on-human-gate-creation (which
// fires once, on bead.created); this one re-fires on a cooldown for gates left
// open.
func TestRenudgeStaleHumanGatesOrder(t *testing.T) {
	assertCooldownExecOrder(t, "renudge-stale-human-gates.toml", "renudge-stale-human-gates.sh")
}

// TestRenudgeStaleHumanGatesScriptContract guards the load-bearing behaviors of
// the staleness re-nudge script. Like the creation-notify script its failures
// are best-effort and swallowed at runtime, so the contract is pinned here:
//
//   - Enumeration is over OPEN gates (`gc bd gate list`, open-only by default)
//     with `--limit 0` so a rig past the default 50-gate page is not silently
//     truncated — a truncated page would drop stale gates from the sweep.
//   - It re-nudges ONLY open human gates: await_type == "human" AND
//     status == "open". The live town carries dozens of legacy await_type=null
//     workflow gates that must never be mailed about.
//   - Both the staleness threshold and the repeat interval are configurable
//     (GC_STALE_GATE_THRESHOLD / GC_STALE_GATE_RENUDGE_INTERVAL) — the order's
//     purpose is "open past a configurable threshold, repeating on the
//     interval".
//   - Addressee resolution consults gc.deferred_assignee (formula/molecule
//     gates strip the assignee there), matching the creation notify so a gate
//     is re-nudged at the same address it was first notified.
//   - The list projection omits assignee/metadata, so the script must re-fetch
//     via `gc bd show` to resolve the addressee.
//   - Notification rides `gc mail send --notify`, the one primitive that mails
//     AND nudges a real session while natively skipping the tmux-nudge for the
//     sessionless "human" recipient (cmd_mail.go `to != "human"`).
//   - The prefix->rig enumeration excludes the HQ pseudo-rig (`.hq != true`),
//     matching the sibling scripts' cross-rig convention.
//   - Timestamp parsing is portable: GNU-only `date -d` returns empty on
//     BSD/macOS, skipping every gate and silently disabling the sweep, so the
//     BSD `date -ju -f` fallback (matching wisp-compact.sh) is required.
//   - Loud-fail: an undeliverable send must surface and NOT be recorded. As
//     with the creation notify, surfacing requires a NON-ZERO exit (the
//     controller logs an exec order's output only on a non-zero exit), so the
//     script must exit non-zero when any re-nudge failed (#4543).
func TestRenudgeStaleHumanGatesScriptContract(t *testing.T) {
	data, err := fs.ReadFile(PackFS, "assets/scripts/renudge-stale-human-gates.sh")
	if err != nil {
		t.Fatalf("reading renudge-stale-human-gates.sh: %v", err)
	}
	body := string(data)

	for _, want := range []string{
		"gc bd gate list",                // enumerate OPEN gates (not events)
		"--limit 0",                      // no silent 50-gate truncation
		`.await_type == "human"`,         // human gates only
		`.status == "open"`,              // skip already-resolved gates
		"GC_STALE_GATE_THRESHOLD",        // configurable staleness threshold
		"GC_STALE_GATE_RENUDGE_INTERVAL", // configurable repeat interval
		"gc bd show",                     // re-fetch (list omits assignee)
		"gc.deferred_assignee",           // formula/molecule addressee
		"--notify",                       // mail + nudge, human-safe primitive
		".hq != true",                    // exclude HQ from prefix->rig lookup
	} {
		if !strings.Contains(body, want) {
			t.Errorf("renudge-stale-human-gates.sh missing load-bearing element %q", want)
		}
	}

	// Loud-fail: the send must be conditional (retry on failure), and the
	// failure path must surface to stderr rather than silently record the gate
	// as re-nudged. The dedup record must live on the SUCCESS branch only.
	if !strings.Contains(body, "if gc mail send") {
		t.Error("renudge-stale-human-gates.sh must branch on the mail-send result (loud-fail retry), not fire-and-forget")
	}
	if !strings.Contains(body, "will retry next sweep") {
		t.Error("renudge-stale-human-gates.sh must surface an undeliverable send to stderr (loud-fail #4543)")
	}
	// Timestamp parsing must be portable: GNU-only `date -d` returns empty on
	// BSD/macOS, which skips every gate at the age check and silently disables
	// the whole sweep. The BSD `date -ju -f` fallback (matching wisp-compact.sh)
	// is load-bearing.
	if !strings.Contains(body, "date -ju -f") {
		t.Error("renudge-stale-human-gates.sh must parse timestamps portably via the BSD `date -ju -f` fallback; GNU-only `date -d` disables the sweep on macOS")
	}
	// Same loud-fail exit contract as the creation notify: the controller logs
	// an exec order's output only on a non-zero exit.
	if !strings.Contains(body, `"$FAILED" -gt 0`) {
		t.Error("renudge-stale-human-gates.sh must exit non-zero when a re-nudge failed, or the loud-fail message is never logged (#4543)")
	}
}
