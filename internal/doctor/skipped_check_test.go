package doctor

import (
	"bytes"
	"strings"
	"testing"
)

func TestSkippedCheckIsBlockingFailure(t *testing.T) {
	c := SkippedCheck("pack-doctor-checks", "the city config did not load", "fix the config", nil, "config load error: boom")
	got := c.Run(&CheckContext{})

	if got.Status != StatusError {
		t.Errorf("status = %v, want StatusError; a skipped group must not read as a pass", got.Status)
	}
	if got.Severity != SeverityBlocking {
		t.Errorf("severity = %v, want SeverityBlocking; an advisory skip would still exit 0", got.Severity)
	}
	if !got.Skipped {
		t.Error("Skipped = false, want true")
	}
	if !strings.Contains(got.Message, "not run") {
		t.Errorf("message = %q, want it to say the group did not run", got.Message)
	}
	if !strings.Contains(got.Message, "the city config did not load") {
		t.Errorf("message = %q, want the reason preserved", got.Message)
	}
	if len(got.Details) != 1 || !strings.Contains(got.Details[0], "boom") {
		t.Errorf("details = %q, want the underlying cause carried as a detail", got.Details)
	}
	if got.FixHint != "fix the config" {
		t.Errorf("fix hint = %q, want it carried through", got.FixHint)
	}
	if c.CanFix() {
		t.Error("CanFix = true, want false; a skipped group has nothing to remediate")
	}
}

// TestSkippedCheckWarnsWhenCauseRepairedMidRun covers the --fix interaction.
// The check list is built once, up front, from a config that failed to load;
// a fix earlier in the same run can repair that config. The skipped group
// still did not run, so this must not read as a pass — but blaming a config
// error that no longer exists would be stale, and would make a successful
// `gc doctor --fix` exit non-zero.
func TestSkippedCheckWarnsWhenCauseRepairedMidRun(t *testing.T) {
	repaired := func() bool { return false } // no longer broken
	c := SkippedCheck("pack-doctor-checks", "the city config did not load", "fix the config", repaired)

	got := c.Run(&CheckContext{})

	if got.Status != StatusWarning {
		t.Errorf("status = %v, want StatusWarning once the cause is repaired", got.Status)
	}
	if got.Severity == SeverityBlocking && got.Status == StatusError {
		t.Error("a repaired-cause skip must not gate the exit code")
	}
	if !got.Skipped {
		t.Error("Skipped = false, want true; the group still did not run")
	}
	if !strings.Contains(got.Message, "rerun") {
		t.Errorf("message = %q, want it to tell the operator to rerun", got.Message)
	}
}

// TestSkippedCheckStillBlocksWhenCausePersists is the paired positive control:
// the run-time recheck must not soften a skip whose cause is unresolved,
// which is the dirty-import-cache case this whole check exists for.
func TestSkippedCheckStillBlocksWhenCausePersists(t *testing.T) {
	broken := func() bool { return true }
	c := SkippedCheck("pack-doctor-checks", "the city config did not load", "fix the config", broken)

	got := c.Run(&CheckContext{})

	if got.Status != StatusError || got.Severity != SeverityBlocking {
		t.Errorf("status/severity = %v/%v, want error/blocking while the cause persists", got.Status, got.Severity)
	}
}

// TestReportCountsSkippedGroupsWhenWarning pins that the summary still counts
// a skipped group that downgraded to a warning — it did not run either way.
func TestReportCountsSkippedGroupsWhenWarning(t *testing.T) {
	repaired := func() bool { return false }
	d := &Doctor{}
	d.Register(SkippedCheck("pack-doctor-checks", "config did not load", "", repaired))

	r := d.RunCollect(&CheckContext{}, false)

	if r.Skipped != 1 {
		t.Errorf("Skipped = %d, want 1", r.Skipped)
	}
	if r.Warned != 1 {
		t.Errorf("Warned = %d, want 1", r.Warned)
	}
	if r.BlockingFailed != 0 {
		t.Errorf("BlockingFailed = %d, want 0; a repaired cause must not gate the exit code", r.BlockingFailed)
	}
}

// TestReportCountsSkippedGroups pins the accounting that makes a dropped
// group visible: it is counted as skipped AND as a blocking failure, so the
// summary names it and the exit code reflects it.
func TestReportCountsSkippedGroups(t *testing.T) {
	d := &Doctor{}
	d.Register(&mockCheck{name: "ran-fine", status: StatusOK, msg: "ok"})
	d.Register(SkippedCheck("pack-doctor-checks", "config did not load", "", nil))
	d.Register(SkippedCheck("rig-checks", "config did not load", "", nil))

	r := d.RunCollect(&CheckContext{}, false)

	if r.Skipped != 2 {
		t.Errorf("Skipped = %d, want 2", r.Skipped)
	}
	if r.BlockingFailed != 2 {
		t.Errorf("BlockingFailed = %d, want 2; skipped groups must gate the exit code", r.BlockingFailed)
	}
	if r.Passed != 1 {
		t.Errorf("Passed = %d, want 1", r.Passed)
	}
}

// TestReportSkippedZeroWhenNothingSkipped is the negative control for the
// counter: an ordinary failing check must not inflate the skipped count, or
// every red run would claim the factory went uninspected.
func TestReportSkippedZeroWhenNothingSkipped(t *testing.T) {
	d := &Doctor{}
	d.Register(&mockCheck{name: "ran-fine", status: StatusOK, msg: "ok"})
	d.Register(&mockCheck{name: "ran-and-failed", status: StatusError, msg: "broken"})

	r := d.RunCollect(&CheckContext{}, false)

	if r.Skipped != 0 {
		t.Errorf("Skipped = %d, want 0; a check that ran and failed is not a skipped group", r.Skipped)
	}
	if r.Failed != 1 {
		t.Errorf("Failed = %d, want 1", r.Failed)
	}
}

func TestPrintSummaryReportsSkippedGroups(t *testing.T) {
	var buf bytes.Buffer
	PrintSummary(&buf, &Report{Passed: 32, Failed: 6, BlockingFailed: 6, Skipped: 6})
	out := buf.String()

	if !strings.Contains(out, "6 check groups skipped") {
		t.Errorf("summary = %q, want it to state how many groups were skipped", out)
	}
	if !strings.Contains(out, "not fully inspected") {
		t.Errorf("summary = %q, want an explicit warning that the run was partial", out)
	}
}

func TestPrintSummarySkippedSingular(t *testing.T) {
	var buf bytes.Buffer
	PrintSummary(&buf, &Report{Passed: 1, Failed: 1, BlockingFailed: 1, Skipped: 1})
	if out := buf.String(); !strings.Contains(out, "1 check group skipped") {
		t.Errorf("summary = %q, want singular phrasing", out)
	}
}

// TestPrintSummaryQuietWhenNothingSkipped is the negative control for the
// summary: a healthy run must not carry the partial-inspection warning.
func TestPrintSummaryQuietWhenNothingSkipped(t *testing.T) {
	var buf bytes.Buffer
	PrintSummary(&buf, &Report{Passed: 62, Warned: 1, Failed: 3, BlockingFailed: 3})
	out := buf.String()

	if strings.Contains(out, "skipped") {
		t.Errorf("summary = %q, want no skipped clause when nothing was skipped", out)
	}
	if strings.Contains(out, "not fully inspected") {
		t.Errorf("summary = %q, want no partial-inspection warning on a complete run", out)
	}
}
