package config

import (
	"slices"
	"testing"
)

// validateLockedRemoteCache decides whether a cached import counts as locally
// modified. It deliberately runs `git status --porcelain` WITHOUT --ignored:
// --ignored prints a `!! <path>` line for every gitignored file, so a pack's own
// gitignored build artifacts (__pycache__/*.pyc, .DS_Store, a runtime state
// directory) would fail the check and no .gitignore could prevent it. The city
// then wedges behind a perpetual "run gc import install" gate with every
// pack-provided subcommand dropped.
//
// This test pins the argument list, because the argument list is the defect.

func TestValidateLockedRemoteCacheStatusOmitsIgnored(t *testing.T) {
	const commit = "abc123def456"

	var statusArgs []string
	prev := runRepoCacheGit
	runRepoCacheGit = func(_ string, args ...string) (string, error) {
		if len(args) > 0 && args[0] == "status" {
			statusArgs = append([]string(nil), args...)
			return "", nil
		}
		return commit, nil
	}
	t.Cleanup(func() { runRepoCacheGit = prev })

	if err := validateLockedRemoteCache("https://example.com/tools.git", t.TempDir(), commit); err != nil {
		t.Fatalf("validateLockedRemoteCache on a clean cache: %v", err)
	}

	if statusArgs == nil {
		t.Fatal("validateLockedRemoteCache never ran git status, so this test asserts nothing")
	}
	if slices.Contains(statusArgs, "--ignored") {
		t.Errorf("git status ran with --ignored (%v); gitignored artifacts must not mark a cached import dirty", statusArgs)
	}
}

// The companion assertion: omitting --ignored must not stop a genuinely modified
// cache from failing validation.
func TestValidateLockedRemoteCacheRejectsModifiedWorktree(t *testing.T) {
	const commit = "abc123def456"

	prev := runRepoCacheGit
	runRepoCacheGit = func(_ string, args ...string) (string, error) {
		if len(args) > 0 && args[0] == "status" {
			return " M packs/local-core/pack.toml\n", nil
		}
		return commit, nil
	}
	t.Cleanup(func() { runRepoCacheGit = prev })

	err := validateLockedRemoteCache("https://example.com/tools.git", t.TempDir(), commit)
	if err == nil {
		t.Fatal("validateLockedRemoteCache accepted a cache with a modified tracked file")
	}
}
