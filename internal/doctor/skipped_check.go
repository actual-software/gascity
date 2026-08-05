package doctor

import "fmt"

// SkippedCheck returns a Check that reports a group of checks gc could not
// run at all. It exists so a check group that is dropped before it registers
// still leaves a visible result behind.
//
// The motivating failure: `gc doctor` gates seven groups of checks on the city
// config loading cleanly, and when that load fails every pack, pack-source,
// rig, data, session and Dolt-ops check silently disappears. The usual cause is
// the pack import cache picking up a stray worktree change. The run then prints
// a shorter summary that also reads healthier, because the checks that were
// failing left along with the rest, so an operator reading it has no way to
// tell the factory went largely uninspected.
//
// stillBroken decides the severity, and is evaluated when the check runs
// rather than when it is registered. That distinction matters under --fix: the
// check list is built once, up front, from a config that failed to load, but a
// fix earlier in the same run may repair the very config that caused the skip.
// Reporting a hard failure on that repaired run would be stale, and would make
// a successful `gc doctor --fix` exit non-zero.
//
//   - stillBroken() true — the config is genuinely unusable, nothing inspected
//     these groups, and nothing repaired them. StatusError with
//     SeverityBlocking, so the run cannot exit 0 on an uninspected factory.
//     That is the whole point of the check.
//   - stillBroken() false — something fixed the config mid-run. These groups
//     still did not run, so the result is not OK, but the honest report is a
//     warning telling the operator to rerun rather than a failure blaming a
//     problem that no longer exists.
//
// A nil stillBroken means "assume still broken", the conservative default.
//
// Any details are shown under --verbose. Callers should put the underlying
// cause there rather than in the reason when several groups share one cause:
// repeating a long config-load error on every skipped line buries the names
// of the groups themselves, which is the part the operator needs to see.
func SkippedCheck(name, reason, fixHint string, stillBroken func() bool, details ...string) Check {
	return &skippedCheck{name: name, reason: reason, fixHint: fixHint, stillBroken: stillBroken, details: details}
}

type skippedCheck struct {
	name        string
	reason      string
	fixHint     string
	stillBroken func() bool
	details     []string
}

func (c *skippedCheck) Name() string { return c.name }

func (c *skippedCheck) CanFix() bool { return false }

func (c *skippedCheck) WarmupEligible() bool { return false }

func (c *skippedCheck) Fix(_ *CheckContext) error { return nil }

func (c *skippedCheck) Run(_ *CheckContext) *CheckResult {
	if c.stillBroken != nil && !c.stillBroken() {
		return &CheckResult{
			Name:    c.name,
			Status:  StatusWarning,
			Skipped: true,
			Message: fmt.Sprintf("not run this pass — %s; the config loads now, so rerun gc doctor to inspect them", c.reason),
			Details: c.details,
		}
	}
	return &CheckResult{
		Name:     c.name,
		Status:   StatusError,
		Severity: SeverityBlocking,
		Skipped:  true,
		Message:  fmt.Sprintf("not run — %s", c.reason),
		Details:  c.details,
		FixHint:  c.fixHint,
	}
}
