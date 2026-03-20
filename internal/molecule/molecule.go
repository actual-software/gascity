// Package molecule instantiates compiled formula recipes as bead molecules
// in a Store. It composes the formula compilation layer (Layer 2) with the
// bead store (Layer 1) to implement Gas City's mechanism #7.
//
// The primary entry points are Cook (compile + instantiate) and Instantiate
// (instantiate a pre-compiled Recipe).
package molecule

import (
	"context"
	"fmt"
	"strings"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/formula"
)

// Options configures molecule instantiation.
type Options struct {
	// Title overrides the root bead's title. If empty, the formula's
	// default title (or {{title}} placeholder after substitution) is used.
	Title string

	// Vars provides variable values for {{placeholder}} substitution in
	// titles, descriptions, and notes. Formula defaults are applied first;
	// these values take precedence.
	Vars map[string]string

	// ParentID attaches the molecule to an existing bead. When set, the
	// root bead's ParentID is set to this value.
	ParentID string

	// IdempotencyKey is set as metadata on the root bead atomically with
	// creation. Used by the convergence loop to prevent duplicate wisps
	// on crash-retry.
	IdempotencyKey string
}

// Result holds the outcome of molecule instantiation.
type Result struct {
	// RootID is the store-assigned ID of the root bead.
	RootID string

	// IDMapping maps recipe step IDs to store-assigned bead IDs.
	IDMapping map[string]string

	// Created is the total number of beads created.
	Created int
}

// Cook compiles a formula by name and instantiates it as a molecule.
// This is the convenience wrapper that most callers should use.
func Cook(ctx context.Context, store beads.Store, formulaName string, searchPaths []string, opts Options) (*Result, error) {
	recipe, err := formula.Compile(ctx, formulaName, searchPaths, opts.Vars)
	if err != nil {
		return nil, fmt.Errorf("compiling formula %q: %w", formulaName, err)
	}
	return Instantiate(ctx, store, recipe, opts)
}

// CookOn compiles a formula and attaches it to an existing bead.
// Shorthand for Cook with opts.ParentID set.
func CookOn(ctx context.Context, store beads.Store, formulaName string, searchPaths []string, opts Options) (*Result, error) {
	if opts.ParentID == "" {
		return nil, fmt.Errorf("CookOn requires Options.ParentID")
	}
	return Cook(ctx, store, formulaName, searchPaths, opts)
}

// Instantiate creates beads from a pre-compiled Recipe. Use this when
// you need to inspect or modify the Recipe before instantiation.
//
// Steps are created in order (root first, then children depth-first).
// Dependencies are wired after all beads exist. On partial failure,
// already-created beads are marked with "molecule_failed" metadata
// for cleanup.
func Instantiate(ctx context.Context, store beads.Store, recipe *formula.Recipe, opts Options) (*Result, error) {
	_ = ctx // reserved for future cancellation support

	if recipe == nil {
		return nil, fmt.Errorf("recipe is nil")
	}
	if len(recipe.Steps) == 0 {
		return nil, fmt.Errorf("recipe %q has no steps", recipe.Name)
	}

	// Merge variable defaults from recipe with caller-provided vars.
	vars := applyVarDefaults(opts.Vars, recipe.Vars)

	// Build the list of beads to create.
	idMapping := make(map[string]string, len(recipe.Steps))
	var createdIDs []string

	for i, step := range recipe.Steps {
		// For RootOnly recipes, only create the root bead.
		if recipe.RootOnly && i > 0 {
			break
		}

		b := stepToBead(step, vars)

		// Root bead overrides.
		if step.IsRoot {
			if step.Metadata["gc.kind"] != "workflow" {
				b.Type = "molecule"
			}
			b.Ref = recipe.Name
			if opts.Title != "" {
				b.Title = opts.Title
			}
			if opts.ParentID != "" {
				b.ParentID = opts.ParentID
			}
			if opts.IdempotencyKey != "" {
				if b.Metadata == nil {
					b.Metadata = make(map[string]string, 1)
				}
				b.Metadata["idempotency_key"] = opts.IdempotencyKey
			}
		} else {
			// Non-root beads: resolve ParentID from the parent-child deps.
			for _, dep := range recipe.Deps {
				if dep.StepID == step.ID && dep.Type == "parent-child" {
					if parentBeadID, ok := idMapping[dep.DependsOnID]; ok {
						b.ParentID = parentBeadID
					}
					break
				}
			}
			// Set Ref to the step ID suffix (after the formula name prefix).
			b.Ref = step.ID

			if step.Metadata["gc.kind"] != "" {
				if rootBeadID, ok := idMapping[recipe.Steps[0].ID]; ok && rootBeadID != "" {
					if b.Metadata == nil {
						b.Metadata = make(map[string]string, 1)
					}
					b.Metadata["gc.root_bead_id"] = rootBeadID
				}
			}

			// Inline Ralph attempt beads need the actual logical bead ID at runtime.
			// Stamp it during instantiation while the recipe-step -> bead mapping is live.
			if logicalStepID, ok := logicalRecipeStepID(step); ok {
				if logicalBeadID, exists := idMapping[logicalStepID]; exists {
					if b.Metadata == nil {
						b.Metadata = make(map[string]string, 1)
					}
					b.Metadata["gc.logical_bead_id"] = logicalBeadID
				}
			}
		}

		created, err := store.Create(b)
		if err != nil {
			// Best-effort cleanup: mark already-created beads as failed.
			markFailed(store, createdIDs)
			return nil, fmt.Errorf("creating bead for step %q: %w", step.ID, err)
		}

		idMapping[step.ID] = created.ID
		createdIDs = append(createdIDs, created.ID)
	}

	// Wire dependencies using the IDMapping.
	if !recipe.RootOnly {
		for _, dep := range recipe.Deps {
			fromID, fromOK := idMapping[dep.StepID]
			toID, toOK := idMapping[dep.DependsOnID]
			if !fromOK || !toOK {
				continue // step was filtered out (RootOnly or condition)
			}
			// Skip parent-child deps — already handled via ParentID field.
			if dep.Type == "parent-child" {
				continue
			}
			if err := store.DepAdd(fromID, toID, dep.Type); err != nil {
				markFailed(store, createdIDs)
				return nil, fmt.Errorf("wiring dep %s->%s: %w", dep.StepID, dep.DependsOnID, err)
			}
		}
	}

	rootID := ""
	if len(createdIDs) > 0 {
		rootID = createdIDs[0]
	}

	return &Result{
		RootID:    rootID,
		IDMapping: idMapping,
		Created:   len(createdIDs),
	}, nil
}

// stepToBead converts a RecipeStep to a Bead with variable substitution.
func stepToBead(step formula.RecipeStep, vars map[string]string) beads.Bead {
	stepType := step.Type
	if stepType == "" {
		stepType = "task"
	}

	b := beads.Bead{
		Title:       formula.Substitute(step.Title, vars),
		Description: formula.Substitute(step.Description, vars),
		Type:        stepType,
		Labels:      step.Labels,
		Assignee:    step.Assignee,
	}

	// Merge step metadata + notes into bead metadata.
	if len(step.Metadata) > 0 || step.Notes != "" {
		b.Metadata = make(map[string]string, len(step.Metadata)+1)
		for k, v := range step.Metadata {
			b.Metadata[k] = formula.Substitute(v, vars)
		}
		if step.Notes != "" {
			b.Metadata["notes"] = formula.Substitute(step.Notes, vars)
		}
	}

	return b
}

// applyVarDefaults merges formula variable defaults with caller-provided
// vars. Caller values take precedence over defaults.
func applyVarDefaults(vars map[string]string, defs map[string]*formula.VarDef) map[string]string {
	result := make(map[string]string, len(vars)+len(defs))
	for name, def := range defs {
		if def != nil && def.Default != nil {
			result[name] = *def.Default
		}
	}
	for k, v := range vars {
		result[k] = v
	}
	return result
}

// markFailed sets "molecule_failed" metadata on all created beads.
// Best-effort: errors are silently ignored since we're already in an
// error path.
func markFailed(store beads.Store, ids []string) {
	for _, id := range ids {
		_ = store.SetMetadata(id, "molecule_failed", "true")
	}
}

func logicalRecipeStepID(step formula.RecipeStep) (string, bool) {
	kind := step.Metadata["gc.kind"]
	if kind != "run" && kind != "check" {
		return "", false
	}
	if attempt := step.Metadata["gc.attempt"]; attempt != "" {
		if trimmed, ok := trimAttemptSuffix(step.ID, "."+kind+"."+attempt); ok {
			return trimmed, true
		}
	}
	for _, prefix := range []string{".run.", ".check."} {
		if idx := strings.LastIndex(step.ID, prefix); idx > 0 {
			return step.ID[:idx], true
		}
	}
	return "", false
}

func trimAttemptSuffix(id, suffix string) (string, bool) {
	if suffix == "" || !strings.HasSuffix(id, suffix) {
		return "", false
	}
	return strings.TrimSuffix(id, suffix), true
}
