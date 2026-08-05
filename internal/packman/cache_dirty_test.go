package packman

import (
	"slices"
	"strings"
	"testing"
)

// cachedRepoDirty deliberately runs `git status --porcelain` WITHOUT --ignored,
// so gitignored build artifacts that land in a cache clone in place (Python
// __pycache__/*.pyc from running a cached pack's scripts, a stray .DS_Store, a
// pack's own gitignored runtime state directory) do not count as local edits.
//
// Re-adding the flag wedges the city behind a perpetual "run gc import install"
// gate that no .gitignore can escape: --ignored prints a `!! <path>` line for
// exactly the files an ignore rule was meant to neutralize, so ignoring the path
// turns a `??` line into a `!!` line and changes nothing the check sees. The
// city then loses every pack-provided subcommand until the next install, and the
// artifact reappears.
//
// The argument list is the defect, so that is what these tests pin.

// statusShowsIgnored reports whether a `git status` argument list would print
// `!! <path>` lines. git accepts the flag in several spellings and they are not
// interchangeable to a naive equality check: bare --ignored means
// --ignored=traditional, and --ignored=matching prints the same `!!` lines
// (verified against git 2.50.1). Only --ignored=no suppresses them. Matching on
// the exact string "--ignored" alone would let --ignored=matching reintroduce
// the defect with every test in this file still green, so unrecognized modes
// fail closed.
func statusShowsIgnored(args []string) bool {
	return slices.ContainsFunc(args, func(arg string) bool {
		return strings.HasPrefix(arg, "--ignored") && arg != "--ignored=no"
	})
}

// gitStatusStub reports the status arguments cachedRepoDirty passed, and models
// git's own behavior: --ignored adds a `!! <path>` line for an ignored artifact,
// and a plain --porcelain run does not report it at all.
// The returned accessor reports the status arguments seen so far.
func gitStatusStub(t *testing.T, ignoredArtifact string) func() []string {
	t.Helper()
	var seen []string
	prev := runGit
	runGit = func(_ string, args ...string) (string, error) {
		if len(args) > 0 && args[0] == "status" {
			seen = append([]string(nil), args...)
			if statusShowsIgnored(args) {
				return "!! " + ignoredArtifact + "\n", nil
			}
			return "", nil
		}
		return "", nil
	}
	t.Cleanup(func() { runGit = prev })
	return func() []string { return seen }
}

func TestCachedRepoDirtyIgnoresGitignoredArtifacts(t *testing.T) {
	const artifact = "packs/local-core/.runtime/"
	statusArgs := gitStatusStub(t, artifact)

	dirty, err := cachedRepoDirty(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if statusArgs() == nil {
		t.Fatal("cachedRepoDirty never ran git status, so this test asserts nothing")
	}
	if statusShowsIgnored(statusArgs()) {
		t.Errorf("git status ran with --ignored (%v); a gitignored artifact must not mark a cache clone dirty", statusArgs())
	}
	if dirty {
		t.Errorf("gitignored artifact %q reported the cache dirty", artifact)
	}
}

// The companion to the test above. Dropping --ignored must not blind the check to
// changes that really are local edits to the pack's content.
func TestCachedRepoDirtyStillCatchesRealChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
	}{
		{name: "untracked file", status: "?? packs/local-core/stray.txt\n"},
		{name: "tracked file edited", status: " M packs/local-core/pack.toml\n"},
		{name: "tracked file deleted", status: " D packs/local-core/pack.toml\n"},
		{name: "staged addition", status: "A  packs/local-core/new.toml\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prev := runGit
			runGit = func(_ string, args ...string) (string, error) {
				if len(args) > 0 && args[0] == "status" {
					return tc.status, nil
				}
				return "", nil
			}
			t.Cleanup(func() { runGit = prev })

			dirty, err := cachedRepoDirty(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if !dirty {
				t.Errorf("status %q did not report the cache dirty", strings.TrimSpace(tc.status))
			}
		})
	}
}
