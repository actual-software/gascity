package ralph

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

func TestAppendRetryCopiesBlockingDeps(t *testing.T) {
	store := beads.NewMemStore()

	root, err := store.Create(beads.Bead{Title: "root", Type: "task"})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	design, err := store.Create(beads.Bead{Title: "design", Type: "task", ParentID: root.ID})
	if err != nil {
		t.Fatalf("create design: %v", err)
	}
	logical, err := store.Create(beads.Bead{
		Title:    "implement",
		Type:     "task",
		ParentID: root.ID,
		Metadata: map[string]string{"gc.kind": "ralph", "gc.step_id": "implement"},
	})
	if err != nil {
		t.Fatalf("create logical: %v", err)
	}
	run, err := store.Create(beads.Bead{
		Title:       "implement",
		Description: "make changes",
		Type:        "task",
		ParentID:    root.ID,
		Metadata: map[string]string{
			"gc.kind":      "run",
			"gc.step_id":   "implement",
			"gc.attempt":   "1",
			"gc.routed_to": "worker",
		},
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	check, err := store.Create(beads.Bead{
		Title:    "check implement",
		Type:     "task",
		ParentID: root.ID,
		Metadata: map[string]string{
			"gc.kind":         "check",
			"gc.step_id":      "implement",
			"gc.attempt":      "1",
			"gc.outcome":      "fail",
			"gc.check_mode":   "exec",
			"gc.check_path":   "./check.sh",
			"gc.retry_from":   "old-check",
			"gc.max_attempts": "3",
		},
	})
	if err != nil {
		t.Fatalf("create check: %v", err)
	}

	if err := store.DepAdd(run.ID, design.ID, "blocks"); err != nil {
		t.Fatalf("dep add run->design: %v", err)
	}
	if err := store.DepAdd(check.ID, run.ID, "blocks"); err != nil {
		t.Fatalf("dep add check->run: %v", err)
	}
	if err := store.DepAdd(logical.ID, check.ID, "blocks"); err != nil {
		t.Fatalf("dep add logical->check: %v", err)
	}

	newRunID, newCheckID, err := appendRetry(store, logical.ID, run, check, 2)
	if err != nil {
		t.Fatalf("appendRetry: %v", err)
	}

	newRun, err := store.Get(newRunID)
	if err != nil {
		t.Fatalf("get new run: %v", err)
	}
	if newRun.Metadata["gc.attempt"] != "2" {
		t.Errorf("new run attempt = %q, want 2", newRun.Metadata["gc.attempt"])
	}
	if newRun.Metadata["gc.routed_to"] != "" {
		t.Errorf("new run routed_to = %q, want empty", newRun.Metadata["gc.routed_to"])
	}

	newCheck, err := store.Get(newCheckID)
	if err != nil {
		t.Fatalf("get new check: %v", err)
	}
	if newCheck.Metadata["gc.attempt"] != "2" {
		t.Errorf("new check attempt = %q, want 2", newCheck.Metadata["gc.attempt"])
	}
	if newCheck.Metadata["gc.outcome"] != "" {
		t.Errorf("new check outcome = %q, want empty", newCheck.Metadata["gc.outcome"])
	}

	runDeps, err := store.DepList(newRunID, "down")
	if err != nil {
		t.Fatalf("dep list new run: %v", err)
	}
	foundDesign := false
	for _, dep := range runDeps {
		if dep.Type == "blocks" && dep.DependsOnID == design.ID {
			foundDesign = true
		}
	}
	if !foundDesign {
		t.Fatalf("new run does not block on original design dep; deps=%v", runDeps)
	}

	checkDeps, err := store.DepList(newCheckID, "down")
	if err != nil {
		t.Fatalf("dep list new check: %v", err)
	}
	foundRun := false
	for _, dep := range checkDeps {
		if dep.Type == "blocks" && dep.DependsOnID == newRunID {
			foundRun = true
		}
	}
	if !foundRun {
		t.Fatalf("new check does not block on new run; deps=%v", checkDeps)
	}

	logicalDeps, err := store.DepList(logical.ID, "down")
	if err != nil {
		t.Fatalf("dep list logical: %v", err)
	}
	foundNewCheck := false
	for _, dep := range logicalDeps {
		if dep.Type == "blocks" && dep.DependsOnID == newCheckID {
			foundNewCheck = true
		}
	}
	if !foundNewCheck {
		t.Fatalf("logical does not block on new check; deps=%v", logicalDeps)
	}
}

func TestResolveLogicalBeadIDFromBlockingDep(t *testing.T) {
	store := beads.NewMemStore()

	root, err := store.Create(beads.Bead{Title: "root", Type: "task"})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	logical, err := store.Create(beads.Bead{
		Title:    "implement",
		Type:     "task",
		ParentID: root.ID,
		Metadata: map[string]string{"gc.kind": "ralph", "gc.step_id": "implement"},
	})
	if err != nil {
		t.Fatalf("create logical: %v", err)
	}
	check, err := store.Create(beads.Bead{
		Title:    "check implement",
		Type:     "task",
		ParentID: root.ID,
		Metadata: map[string]string{
			"gc.kind":         "check",
			"gc.step_id":      "implement",
			"gc.attempt":      "1",
			"gc.check_mode":   "exec",
			"gc.check_path":   "./check.sh",
			"gc.max_attempts": "3",
		},
	})
	if err != nil {
		t.Fatalf("create check: %v", err)
	}
	if err := store.DepAdd(logical.ID, check.ID, "blocks"); err != nil {
		t.Fatalf("dep add logical->check: %v", err)
	}

	got := resolveLogicalBeadID(store, check)
	if got != logical.ID {
		t.Fatalf("resolveLogicalBeadID() = %q, want %q", got, logical.ID)
	}
}

func TestCloseReadyWorkflowHeadsClosesPassAndFail(t *testing.T) {
	store := beads.NewMemStore()

	passWorkflow, err := store.Create(beads.Bead{
		Title:    "workflow-pass",
		Type:     "task",
		Metadata: map[string]string{"gc.kind": "workflow"},
	})
	if err != nil {
		t.Fatalf("create pass workflow: %v", err)
	}
	passBlocker, err := store.Create(beads.Bead{
		Title:    "logical-pass",
		Type:     "task",
		Metadata: map[string]string{"gc.kind": "ralph", "gc.outcome": "pass"},
	})
	if err != nil {
		t.Fatalf("create pass blocker: %v", err)
	}
	if err := store.DepAdd(passWorkflow.ID, passBlocker.ID, "blocks"); err != nil {
		t.Fatalf("dep add pass workflow: %v", err)
	}
	if err := store.Close(passBlocker.ID); err != nil {
		t.Fatalf("close pass blocker: %v", err)
	}

	failWorkflow, err := store.Create(beads.Bead{
		Title:    "workflow-fail",
		Type:     "task",
		Metadata: map[string]string{"gc.kind": "workflow"},
	})
	if err != nil {
		t.Fatalf("create fail workflow: %v", err)
	}
	failBlocker, err := store.Create(beads.Bead{
		Title:    "logical-fail",
		Type:     "task",
		Metadata: map[string]string{"gc.kind": "ralph", "gc.outcome": "fail"},
	})
	if err != nil {
		t.Fatalf("create fail blocker: %v", err)
	}
	if err := store.DepAdd(failWorkflow.ID, failBlocker.ID, "blocks"); err != nil {
		t.Fatalf("dep add fail workflow: %v", err)
	}
	if err := store.Close(failBlocker.ID); err != nil {
		t.Fatalf("close fail blocker: %v", err)
	}

	closed, err := CloseReadyWorkflowHeads(store)
	if err != nil {
		t.Fatalf("CloseReadyWorkflowHeads: %v", err)
	}
	if closed != 2 {
		t.Fatalf("closed = %d, want 2", closed)
	}

	gotPassWorkflow, err := store.Get(passWorkflow.ID)
	if err != nil {
		t.Fatalf("get pass workflow: %v", err)
	}
	if gotPassWorkflow.Status != "closed" {
		t.Fatalf("pass workflow status = %q, want closed", gotPassWorkflow.Status)
	}
	if gotPassWorkflow.Metadata["gc.outcome"] != "pass" {
		t.Fatalf("pass workflow outcome = %q, want pass", gotPassWorkflow.Metadata["gc.outcome"])
	}

	gotFailWorkflow, err := store.Get(failWorkflow.ID)
	if err != nil {
		t.Fatalf("get fail workflow: %v", err)
	}
	if gotFailWorkflow.Status != "closed" {
		t.Fatalf("fail workflow status = %q, want closed", gotFailWorkflow.Status)
	}
	if gotFailWorkflow.Metadata["gc.outcome"] != "fail" {
		t.Fatalf("fail workflow outcome = %q, want fail", gotFailWorkflow.Metadata["gc.outcome"])
	}
}

func TestCloseReadyWorkflowHeadsSkipsBlockedWorkflow(t *testing.T) {
	store := beads.NewMemStore()

	workflow, err := store.Create(beads.Bead{
		Title:    "workflow",
		Type:     "task",
		Metadata: map[string]string{"gc.kind": "workflow"},
	})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	blocker, err := store.Create(beads.Bead{
		Title: "logical",
		Type:  "task",
	})
	if err != nil {
		t.Fatalf("create blocker: %v", err)
	}
	if err := store.DepAdd(workflow.ID, blocker.ID, "blocks"); err != nil {
		t.Fatalf("dep add workflow: %v", err)
	}

	closed, err := CloseReadyWorkflowHeads(store)
	if err != nil {
		t.Fatalf("CloseReadyWorkflowHeads: %v", err)
	}
	if closed != 0 {
		t.Fatalf("closed = %d, want 0", closed)
	}

	got, err := store.Get(workflow.ID)
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if got.Status != "open" {
		t.Fatalf("workflow status = %q, want open", got.Status)
	}
	if got.Metadata["gc.outcome"] != "" {
		t.Fatalf("workflow outcome = %q, want empty", got.Metadata["gc.outcome"])
	}
}

func TestProcessCheckRetriesThenClosesWorkflowOnPass(t *testing.T) {
	cityPath := t.TempDir()
	scriptDir := filepath.Join(cityPath, ".gc", "scripts")
	if err := os.MkdirAll(scriptDir, 0o755); err != nil {
		t.Fatalf("mkdir script dir: %v", err)
	}
	scriptPath := filepath.Join(scriptDir, "retry-check.sh")
	script := "#!/bin/bash\nset -euo pipefail\nTARGET=\"$GC_CITY_ROOT/retry-demo.txt\"\n[ -f \"$TARGET\" ]\ngrep -qx \"pass\" \"$TARGET\"\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write check script: %v", err)
	}

	store, workflow, logical, run1, check1 := newTestLoop(t, "implement", ".gc/scripts/retry-check.sh", 2)

	if err := os.WriteFile(filepath.Join(cityPath, "retry-demo.txt"), []byte("fail\n"), 0o644); err != nil {
		t.Fatalf("write failing artifact: %v", err)
	}
	if err := store.Close(run1.ID); err != nil {
		t.Fatalf("close run1: %v", err)
	}

	result1, err := ProcessCheck(check1, cityPath, store)
	if err != nil {
		t.Fatalf("ProcessCheck(first): %v", err)
	}
	if !result1.Processed || result1.Action != "retry" {
		t.Fatalf("first result = %+v, want processed retry", result1)
	}

	check1State, err := store.Get(check1.ID)
	if err != nil {
		t.Fatalf("get check1: %v", err)
	}
	if check1State.Status != "closed" {
		t.Fatalf("check1 status = %q, want closed", check1State.Status)
	}
	if check1State.Metadata["gc.outcome"] != "fail" {
		t.Fatalf("check1 outcome = %q, want fail", check1State.Metadata["gc.outcome"])
	}

	run2, check2 := nextAttempt(t, store, logical.ID)

	if err := os.WriteFile(filepath.Join(cityPath, "retry-demo.txt"), []byte("pass\n"), 0o644); err != nil {
		t.Fatalf("write passing artifact: %v", err)
	}
	if err := store.Close(run2.ID); err != nil {
		t.Fatalf("close run2: %v", err)
	}

	result2, err := ProcessCheck(check2, cityPath, store)
	if err != nil {
		t.Fatalf("ProcessCheck(second): %v", err)
	}
	if !result2.Processed || result2.Action != "pass" {
		t.Fatalf("second result = %+v, want processed pass", result2)
	}

	closed, err := CloseReadyWorkflowHeads(store)
	if err != nil {
		t.Fatalf("CloseReadyWorkflowHeads: %v", err)
	}
	if closed != 1 {
		t.Fatalf("closed workflows = %d, want 1", closed)
	}

	logicalState, err := store.Get(logical.ID)
	if err != nil {
		t.Fatalf("get logical: %v", err)
	}
	if logicalState.Status != "closed" || logicalState.Metadata["gc.outcome"] != "pass" {
		t.Fatalf("logical state = status %q outcome %q, want closed/pass", logicalState.Status, logicalState.Metadata["gc.outcome"])
	}

	workflowState, err := store.Get(workflow.ID)
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if workflowState.Status != "closed" || workflowState.Metadata["gc.outcome"] != "pass" {
		t.Fatalf("workflow state = status %q outcome %q, want closed/pass", workflowState.Status, workflowState.Metadata["gc.outcome"])
	}
}

func TestProcessCheckExhaustsRetriesAndClosesWorkflowOnFail(t *testing.T) {
	cityPath := t.TempDir()
	scriptDir := filepath.Join(cityPath, ".gc", "scripts")
	if err := os.MkdirAll(scriptDir, 0o755); err != nil {
		t.Fatalf("mkdir script dir: %v", err)
	}
	scriptPath := filepath.Join(scriptDir, "always-fail-check.sh")
	script := "#!/bin/bash\nset -euo pipefail\nTARGET=\"$GC_CITY_ROOT/retry-demo.txt\"\n[ -f \"$TARGET\" ]\ngrep -qx \"pass\" \"$TARGET\"\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write check script: %v", err)
	}

	store, workflow, logical, run1, check1 := newTestLoop(t, "implement", ".gc/scripts/always-fail-check.sh", 2)

	if err := os.WriteFile(filepath.Join(cityPath, "retry-demo.txt"), []byte("fail\n"), 0o644); err != nil {
		t.Fatalf("write failing artifact: %v", err)
	}
	if err := store.Close(run1.ID); err != nil {
		t.Fatalf("close run1: %v", err)
	}

	result1, err := ProcessCheck(check1, cityPath, store)
	if err != nil {
		t.Fatalf("ProcessCheck(first): %v", err)
	}
	if !result1.Processed || result1.Action != "retry" {
		t.Fatalf("first result = %+v, want processed retry", result1)
	}

	run2, check2 := nextAttempt(t, store, logical.ID)
	if err := os.WriteFile(filepath.Join(cityPath, "retry-demo.txt"), []byte("fail\n"), 0o644); err != nil {
		t.Fatalf("rewrite failing artifact: %v", err)
	}
	if err := store.Close(run2.ID); err != nil {
		t.Fatalf("close run2: %v", err)
	}

	result2, err := ProcessCheck(check2, cityPath, store)
	if err != nil {
		t.Fatalf("ProcessCheck(second): %v", err)
	}
	if !result2.Processed || result2.Action != "fail" {
		t.Fatalf("second result = %+v, want processed fail", result2)
	}

	check2State, err := store.Get(check2.ID)
	if err != nil {
		t.Fatalf("get check2: %v", err)
	}
	if check2State.Status != "closed" || check2State.Metadata["gc.outcome"] != "fail" {
		t.Fatalf("check2 state = status %q outcome %q, want closed/fail", check2State.Status, check2State.Metadata["gc.outcome"])
	}

	closed, err := CloseReadyWorkflowHeads(store)
	if err != nil {
		t.Fatalf("CloseReadyWorkflowHeads: %v", err)
	}
	if closed != 1 {
		t.Fatalf("closed workflows = %d, want 1", closed)
	}

	logicalState, err := store.Get(logical.ID)
	if err != nil {
		t.Fatalf("get logical: %v", err)
	}
	if logicalState.Status != "closed" || logicalState.Metadata["gc.outcome"] != "fail" {
		t.Fatalf("logical state = status %q outcome %q, want closed/fail", logicalState.Status, logicalState.Metadata["gc.outcome"])
	}
	if logicalState.Metadata["gc.failed_attempt"] != "2" {
		t.Fatalf("logical failed attempt = %q, want 2", logicalState.Metadata["gc.failed_attempt"])
	}

	workflowState, err := store.Get(workflow.ID)
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if workflowState.Status != "closed" || workflowState.Metadata["gc.outcome"] != "fail" {
		t.Fatalf("workflow state = status %q outcome %q, want closed/fail", workflowState.Status, workflowState.Metadata["gc.outcome"])
	}

	ready, err := store.Ready()
	if err != nil {
		t.Fatalf("ready after fail: %v", err)
	}
	if len(ready) != 0 {
		t.Fatalf("ready count after fail = %d, want 0", len(ready))
	}
}

func newTestLoop(t *testing.T, stepID, checkPath string, maxAttempts int) (*beads.MemStore, beads.Bead, beads.Bead, beads.Bead, beads.Bead) {
	t.Helper()

	store := beads.NewMemStore()
	workflow, err := store.Create(beads.Bead{
		Title:    "workflow",
		Type:     "task",
		Metadata: map[string]string{"gc.kind": "workflow"},
	})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	logical, err := store.Create(beads.Bead{
		Title: "implement",
		Type:  "task",
		Metadata: map[string]string{
			"gc.kind":         "ralph",
			"gc.step_id":      stepID,
			"gc.max_attempts": strconv.Itoa(maxAttempts),
			"gc.root_bead_id": workflow.ID,
		},
	})
	if err != nil {
		t.Fatalf("create logical: %v", err)
	}
	run1, err := store.Create(beads.Bead{
		Title: "run 1",
		Type:  "task",
		Metadata: map[string]string{
			"gc.kind":            "run",
			"gc.step_id":         stepID,
			"gc.attempt":         "1",
			"gc.root_bead_id":    workflow.ID,
			"gc.logical_bead_id": logical.ID,
		},
	})
	if err != nil {
		t.Fatalf("create run1: %v", err)
	}
	check1, err := store.Create(beads.Bead{
		Title: "check 1",
		Type:  "task",
		Metadata: map[string]string{
			"gc.kind":            "check",
			"gc.step_id":         stepID,
			"gc.attempt":         "1",
			"gc.check_mode":      "exec",
			"gc.check_path":      checkPath,
			"gc.check_timeout":   "30s",
			"gc.max_attempts":    strconv.Itoa(maxAttempts),
			"gc.root_bead_id":    workflow.ID,
			"gc.logical_bead_id": logical.ID,
		},
	})
	if err != nil {
		t.Fatalf("create check1: %v", err)
	}
	if err := store.DepAdd(check1.ID, run1.ID, "blocks"); err != nil {
		t.Fatalf("dep add check1->run1: %v", err)
	}
	if err := store.DepAdd(logical.ID, check1.ID, "blocks"); err != nil {
		t.Fatalf("dep add logical->check1: %v", err)
	}
	if err := store.DepAdd(workflow.ID, logical.ID, "blocks"); err != nil {
		t.Fatalf("dep add workflow->logical: %v", err)
	}
	return store, workflow, logical, run1, check1
}

func nextAttempt(t *testing.T, store *beads.MemStore, logicalID string) (beads.Bead, beads.Bead) {
	t.Helper()

	logicalDeps, err := store.DepList(logicalID, "down")
	if err != nil {
		t.Fatalf("dep list logical: %v", err)
	}
	if len(logicalDeps) != 1 {
		t.Fatalf("logical deps = %+v, want exactly one current blocker", logicalDeps)
	}
	check2ID := logicalDeps[0].DependsOnID
	check2, err := store.Get(check2ID)
	if err != nil {
		t.Fatalf("get check2: %v", err)
	}
	if check2.Metadata["gc.kind"] != "check" || check2.Metadata["gc.attempt"] != "2" {
		t.Fatalf("check2 metadata = %+v, want check attempt 2", check2.Metadata)
	}

	ready, err := store.Ready()
	if err != nil {
		t.Fatalf("ready after retry append: %v", err)
	}
	if len(ready) != 1 {
		t.Fatalf("ready count after retry append = %d, want 1", len(ready))
	}
	run2 := ready[0]
	if run2.Metadata["gc.kind"] != "run" || run2.Metadata["gc.attempt"] != "2" {
		t.Fatalf("ready bead = %+v, want run attempt 2", run2.Metadata)
	}

	return run2, check2
}
