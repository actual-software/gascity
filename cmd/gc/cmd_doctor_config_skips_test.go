package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/doctor"
)

// TestConfigDependentCheckGroupsAreDeclared pins the group list against an
// independent copy of the names.
//
// The other two tests in this file both iterate configDependentCheckGroups, so
// the list is simultaneously the thing under test and the oracle. Deleting an
// entry makes them test less while still passing, which is the failure mode a
// list-as-its-own-oracle always has. This test is the second source of truth:
// remove or rename a group and it fails here.
//
// What it deliberately does NOT cover: a config-gated register block added to
// buildDoctorChecks with no matching entry here. That block is still dropped
// silently, and catching it needs a tripwire over the gated blocks themselves
// rather than over this list. See the maintenance contract on
// configDependentCheckGroups.
func TestConfigDependentCheckGroupsAreDeclared(t *testing.T) {
	t.Parallel()

	want := []string{
		"pack-doctor-checks",
		"pack-source-checks",
		"config-validation-checks",
		"rig-checks",
		"data-checks",
		"session-checks",
		"dolt-ops-checks",
	}
	got := make([]string, 0, len(configDependentCheckGroups))
	for _, group := range configDependentCheckGroups {
		got = append(got, group.name)
		if strings.TrimSpace(group.covers) == "" {
			t.Errorf("group %q has an empty covers string; the skip message names it to the operator", group.name)
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("configDependentCheckGroups changed\n got: %v\nwant: %v\nIf a config-gated group was added or removed, update this list too, and check the skip still names every gated block.", got, want)
	}
}

// TestBuildDoctorChecksRegistersSkipsWhenConfigFails is the positive control
// for the silent-drop fix. A dirty pack import cache fails config load, which
// used to drop every pack, rig, and data check with no trace: the run printed
// roughly half as many checks and read healthier, because the failing ones
// left with the rest. Each dropped group must now leave a blocking result.
func TestBuildDoctorChecksRegistersSkipsWhenConfigFails(t *testing.T) {
	// The config must be genuinely unloadable, not merely reported as such by
	// the cfgErr argument: the skip re-reads the config when it runs, so that
	// a cause repaired mid-run downgrades to a warning. Broken TOML keeps the
	// cause unresolved, which is the dirty-cache case being pinned here.
	cityDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte("[workspace\nname = \"demo\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}

	// The Dolt skips travel through opts, not the process environment:
	// buildDoctorChecks never reads GC_DOLT (only runDoctor does, to populate
	// these fields). So neither test here mutates the environment, which keeps
	// them out of the cmd/gc environment debt ratchet in test/test-resources.toml.
	cfgErr := errors.New(`city import "local-core": cached import file:///packs/local-core has local worktree changes; run "gc import install"`)
	checks := buildDoctorChecks(cityDir, nil, cfgErr, buildDoctorChecksOpts{
		SkipCityDoltCheck:    true,
		SkipManagedDoltCheck: true,
	})
	names := doctorCheckNames(checks)

	for _, group := range configDependentCheckGroups {
		if doctorCheckIndex(names, group.name) < 0 {
			t.Errorf("%s not registered; a dropped group must be named, not silently omitted. names=%v", group.name, names)
		}
	}

	// The result must gate the exit code and carry the underlying cause,
	// otherwise `gc doctor` still exits 0 on an uninspected factory.
	var found bool
	for _, c := range checks {
		if c.Name() != "pack-doctor-checks" {
			continue
		}
		found = true
		r := c.Run(&doctor.CheckContext{})
		if r.Status != doctor.StatusError || r.Severity != doctor.SeverityBlocking {
			t.Errorf("pack-doctor-checks status/severity = %v/%v, want error/blocking", r.Status, r.Severity)
		}
		if !r.Skipped {
			t.Error("pack-doctor-checks Skipped = false, want true")
		}
		if !strings.Contains(r.Message, "did not load") {
			t.Errorf("message = %q, want it to name the config load as the cause", r.Message)
		}
		if !strings.Contains(strings.Join(r.Details, "\n"), "local worktree changes") {
			t.Errorf("details = %q, want the underlying config-load error preserved", r.Details)
		}
		if !strings.Contains(r.FixHint, "gc import install") {
			t.Errorf("fix hint = %q, want it to name the cache repair command", r.FixHint)
		}
	}
	if !found {
		t.Fatal("pack-doctor-checks check not found")
	}
}

// TestBuildDoctorChecksNoSkipsWhenConfigLoads is the negative control: on a
// healthy city no skip result may appear, or every clean run would claim the
// factory went uninspected and `gc doctor` would never exit 0 again.
func TestBuildDoctorChecksNoSkipsWhenConfigLoads(t *testing.T) {
	cityDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte("[workspace]\nname = \"demo\"\n"), 0o644); err != nil {
		t.Fatalf("write city.toml: %v", err)
	}

	cfg := &config.City{Workspace: config.Workspace{Name: "demo"}}
	names := doctorCheckNames(buildDoctorChecks(cityDir, cfg, nil, buildDoctorChecksOpts{
		SkipCityDoltCheck:    true,
		SkipManagedDoltCheck: true,
	}))

	for _, group := range configDependentCheckGroups {
		if idx := doctorCheckIndex(names, group.name); idx >= 0 {
			t.Errorf("%s registered at %d on a healthy config, want absent", group.name, idx)
		}
	}
}
