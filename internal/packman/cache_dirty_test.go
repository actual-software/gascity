package packman

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// cachedRepoDirty deliberately runs `git status --porcelain` WITHOUT --ignored,
// so gitignored build artifacts that land in a cache clone in place (Python
// __pycache__/*.pyc from running a cached pack's scripts, a stray .DS_Store, a
// pack's own gitignored .runtime/ state directory) do not count as local
// modifications. Re-adding --ignored wedges the city behind a perpetual "run gc
// import install" gate that no .gitignore can escape, because --ignored prints a
// `!! <path>` line for exactly the files the ignore rule was meant to neutralize.
//
// These tests pin that behavior. They use a real git repo rather than a stub so
// they assert the observable outcome, not just the argument list.

func dirtyTestGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

// newDirtyTestRepo returns a committed repo that gitignores .runtime/ and holds
// one tracked file, so callers can perturb it in a controlled way.
func newDirtyTestRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	dirtyTestGit(t, repo, "init", "--quiet")
	dirtyTestGit(t, repo, "config", "user.email", "test@example.invalid")
	dirtyTestGit(t, repo, "config", "user.name", "packman test")
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte(".runtime/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("v1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	dirtyTestGit(t, repo, "add", ".")
	dirtyTestGit(t, repo, "commit", "--quiet", "-m", "base")

	dirty, err := newDirtyTestRepoBaseline(repo)
	if err != nil {
		t.Fatal(err)
	}
	if dirty {
		t.Fatalf("fixture is dirty before any perturbation")
	}
	return repo
}

func newDirtyTestRepoBaseline(repo string) (bool, error) { return cachedRepoDirty(repo) }

func TestCachedRepoDirtyIgnoresGitignoredArtifacts(t *testing.T) {
	repo := newDirtyTestRepo(t)

	// The artifact observed in the field: a pack's gitignored runtime state
	// directory recreated inside the cache clone after `gc import install`.
	if err := os.MkdirAll(filepath.Join(repo, ".runtime", "reminders"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".runtime", "reminders", ".lock"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	// Positive control. If --ignored sees nothing here, the fixture does not
	// model the regression and a pass below would be meaningless.
	withIgnored := dirtyTestGit(t, repo, "status", "--porcelain", "--ignored")
	if strings.TrimSpace(withIgnored) == "" {
		t.Fatal("fixture does not model the regression: `git status --porcelain --ignored` reported nothing")
	}

	dirty, err := cachedRepoDirty(repo)
	if err != nil {
		t.Fatal(err)
	}
	if dirty {
		t.Errorf("gitignored artifact reported the cache dirty; `git status --porcelain --ignored` saw %q, and cachedRepoDirty must not", strings.TrimSpace(withIgnored))
	}
}

// The companion to the test above. Dropping --ignored must not blind the gate to
// changes that really are local edits to the pack's content.
func TestCachedRepoDirtyStillCatchesRealChanges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		perturb func(t *testing.T, repo string)
	}{
		{
			name: "untracked file",
			perturb: func(t *testing.T, repo string) {
				if err := os.WriteFile(filepath.Join(repo, "stray.txt"), []byte("x\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "tracked file edited",
			perturb: func(t *testing.T, repo string) {
				if err := os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("v2\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "tracked file deleted",
			perturb: func(t *testing.T, repo string) {
				if err := os.Remove(filepath.Join(repo, "tracked.txt")); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := newDirtyTestRepo(t)
			tc.perturb(t, repo)

			dirty, err := cachedRepoDirty(repo)
			if err != nil {
				t.Fatal(err)
			}
			if !dirty {
				t.Error("a real local change did not report the cache dirty")
			}
		})
	}
}
