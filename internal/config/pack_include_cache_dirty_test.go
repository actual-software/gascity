package config

import (
	"slices"
	"strings"
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

// statusShowsIgnored reports whether a `git status` argument list would print
// `!! <path>` lines. git accepts the flag in several spellings and they are not
// interchangeable to a naive equality check: bare --ignored means
// --ignored=traditional, and --ignored=matching prints the same `!!` lines
// (verified against git 2.50.1). Only --ignored=no suppresses them. Matching on
// the exact string "--ignored" alone would let --ignored=matching reintroduce
// the defect with this test still green, so unrecognized modes fail closed.
func statusShowsIgnored(args []string) bool {
	return slices.ContainsFunc(args, func(arg string) bool {
		return strings.HasPrefix(arg, "--ignored") && arg != "--ignored=no"
	})
}

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
	if statusShowsIgnored(statusArgs) {
		t.Errorf("git status ran with --ignored (%v); gitignored artifacts must not mark a cached import dirty", statusArgs)
	}
}

// The companion assertion: omitting --ignored must not stop a genuinely modified
// cache from failing validation.
//
// validateLockedRemoteCache fails earlier if rev-parse HEAD disagrees with the
// locked commit, so asserting only err != nil would keep this test green if a
// reorder made the status check stop rejecting anything. Assert that status ran
// and that the error is the worktree one.
func TestValidateLockedRemoteCacheRejectsModifiedWorktree(t *testing.T) {
	const commit = "abc123def456"

	statusRan := false
	prev := runRepoCacheGit
	runRepoCacheGit = func(_ string, args ...string) (string, error) {
		if len(args) > 0 && args[0] == "status" {
			statusRan = true
			return " M packs/local-core/pack.toml\n", nil
		}
		return commit, nil
	}
	t.Cleanup(func() { runRepoCacheGit = prev })

	err := validateLockedRemoteCache("https://example.com/tools.git", t.TempDir(), commit)
	if !statusRan {
		t.Fatal("validateLockedRemoteCache never ran git status, so this test asserts nothing")
	}
	if err != nil && !strings.Contains(err.Error(), "local worktree changes") {
		t.Fatalf("rejected for the wrong reason: %v", err)
	}
	if err == nil {
		t.Fatal("validateLockedRemoteCache accepted a cache with a modified tracked file")
	}
}
