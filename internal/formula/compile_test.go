package formula

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCompileSimpleFormula(t *testing.T) {
	dir := t.TempDir()
	formulaContent := `
formula = "pancakes"
description = "Make pancakes"
version = 1

[[steps]]
id = "dry"
title = "Mix dry ingredients"
type = "task"

[[steps]]
id = "wet"
title = "Mix wet ingredients"
type = "task"

[[steps]]
id = "cook"
title = "Cook pancakes"
needs = ["dry", "wet"]
`
	if err := os.WriteFile(filepath.Join(dir, "pancakes.formula.toml"), []byte(formulaContent), 0o644); err != nil {
		t.Fatal(err)
	}

	recipe, err := Compile(context.Background(), "pancakes", []string{dir}, nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	if recipe.Name != "pancakes" {
		t.Errorf("Name = %q, want %q", recipe.Name, "pancakes")
	}

	// Root + 3 steps = 4 total
	if len(recipe.Steps) != 4 {
		t.Errorf("len(Steps) = %d, want 4", len(recipe.Steps))
	}

	// Root should be first and marked
	if !recipe.Steps[0].IsRoot {
		t.Error("Steps[0] should be root")
	}
	if recipe.Steps[0].ID != "pancakes" {
		t.Errorf("root ID = %q, want %q", recipe.Steps[0].ID, "pancakes")
	}

	// Check step IDs are namespaced
	if recipe.Steps[1].ID != "pancakes.dry" {
		t.Errorf("step 1 ID = %q, want %q", recipe.Steps[1].ID, "pancakes.dry")
	}

	// Check deps include the needs -> blocks edge
	foundNeedsDep := false
	for _, dep := range recipe.Deps {
		if dep.StepID == "pancakes.cook" && dep.DependsOnID == "pancakes.dry" && dep.Type == "blocks" {
			foundNeedsDep = true
		}
	}
	if !foundNeedsDep {
		t.Error("missing blocks dependency from cook -> dry")
	}
}

func TestCompileWithVarsAndConditions(t *testing.T) {
	dir := t.TempDir()
	formulaContent := `
formula = "conditional"
version = 1

[vars.mode]
description = "Execution mode"
default = "fast"

[[steps]]
id = "always"
title = "Always runs"

[[steps]]
id = "slow-only"
title = "Only in slow mode"
condition = "{{mode}} == slow"
`
	if err := os.WriteFile(filepath.Join(dir, "conditional.formula.toml"), []byte(formulaContent), 0o644); err != nil {
		t.Fatal(err)
	}

	// With default vars (mode=fast), slow-only should be filtered out
	recipe, err := Compile(context.Background(), "conditional", []string{dir}, map[string]string{})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Root + always = 2 (slow-only filtered out by condition)
	if len(recipe.Steps) != 2 {
		t.Errorf("len(Steps) = %d, want 2 (slow-only filtered)", len(recipe.Steps))
	}

	// With mode=slow, both should be present
	recipe2, err := Compile(context.Background(), "conditional", []string{dir}, map[string]string{"mode": "slow"})
	if err != nil {
		t.Fatalf("Compile with mode=slow: %v", err)
	}

	if len(recipe2.Steps) != 3 {
		t.Errorf("len(Steps) = %d, want 3 (all included)", len(recipe2.Steps))
	}
}

func TestCompileWithChildren(t *testing.T) {
	dir := t.TempDir()
	formulaContent := `
formula = "nested"
version = 1

[[steps]]
id = "parent"
title = "Parent step"

[[steps.children]]
id = "child-a"
title = "Child A"

[[steps.children]]
id = "child-b"
title = "Child B"
needs = ["child-a"]
`
	if err := os.WriteFile(filepath.Join(dir, "nested.formula.toml"), []byte(formulaContent), 0o644); err != nil {
		t.Fatal(err)
	}

	recipe, err := Compile(context.Background(), "nested", []string{dir}, nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Root + parent (promoted to epic) + child-a + child-b = 4
	if len(recipe.Steps) != 4 {
		t.Errorf("len(Steps) = %d, want 4", len(recipe.Steps))
	}

	// Parent should be promoted to epic
	parentStep := recipe.StepByID("nested.parent")
	if parentStep == nil {
		t.Fatal("parent step not found")
	}
	if parentStep.Type != "epic" {
		t.Errorf("parent.Type = %q, want %q", parentStep.Type, "epic")
	}

	// Child IDs should be nested
	childA := recipe.StepByID("nested.parent.child-a")
	if childA == nil {
		t.Fatal("child-a not found at nested.parent.child-a")
	}
}

func TestCompileNotFound(t *testing.T) {
	_, err := Compile(context.Background(), "nonexistent", nil, nil)
	if err == nil {
		t.Fatal("expected error for nonexistent formula")
	}
}

func TestCompileVaporPhase(t *testing.T) {
	dir := t.TempDir()
	formulaContent := `
formula = "patrol"
version = 1
phase = "vapor"

[[steps]]
id = "scan"
title = "Scan"
`
	if err := os.WriteFile(filepath.Join(dir, "patrol.formula.toml"), []byte(formulaContent), 0o644); err != nil {
		t.Fatal(err)
	}

	recipe, err := Compile(context.Background(), "patrol", []string{dir}, nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	if recipe.Phase != "vapor" {
		t.Errorf("Phase = %q, want %q", recipe.Phase, "vapor")
	}
	if !recipe.RootOnly {
		t.Error("vapor formula should be RootOnly by default")
	}
}

func TestCompileRalphMarksWorkflowRootAndBlocksOnTopLevelSteps(t *testing.T) {
	dir := t.TempDir()
	formulaContent := `
formula = "ralph-demo"
version = 1

[[steps]]
id = "design"
title = "Design"

[[steps]]
id = "implement"
title = "Implement"
needs = ["design"]

[steps.ralph]
max_attempts = 2

[steps.ralph.check]
mode = "exec"
path = ".gascity/checks/widget.sh"
timeout = "30s"
`
	if err := os.WriteFile(filepath.Join(dir, "ralph-demo.formula.toml"), []byte(formulaContent), 0o644); err != nil {
		t.Fatal(err)
	}

	recipe, err := Compile(context.Background(), "ralph-demo", []string{dir}, nil)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	root := recipe.RootStep()
	if root == nil {
		t.Fatal("root step missing")
	}
	if got := root.Metadata["gc.kind"]; got != "workflow" {
		t.Fatalf("root gc.kind = %q, want workflow", got)
	}
	if root.Type != "task" {
		t.Fatalf("root type = %q, want task", root.Type)
	}

	assertHasDep := func(stepID, dependsOnID, depType string) {
		t.Helper()
		for _, dep := range recipe.Deps {
			if dep.StepID == stepID && dep.DependsOnID == dependsOnID && dep.Type == depType {
				return
			}
		}
		t.Fatalf("missing dep %s --%s--> %s", stepID, depType, dependsOnID)
	}
	assertLacksDep := func(stepID, dependsOnID, depType string) {
		t.Helper()
		for _, dep := range recipe.Deps {
			if dep.StepID == stepID && dep.DependsOnID == dependsOnID && dep.Type == depType {
				t.Fatalf("unexpected dep %s --%s--> %s", stepID, depType, dependsOnID)
			}
		}
	}

	assertHasDep("ralph-demo", "ralph-demo.design", "blocks")
	assertHasDep("ralph-demo", "ralph-demo.implement", "blocks")
	assertLacksDep("ralph-demo", "ralph-demo.implement.run.1", "blocks")
	assertLacksDep("ralph-demo", "ralph-demo.implement.check.1", "blocks")
}
