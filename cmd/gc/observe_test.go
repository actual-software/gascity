package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestClaudeProjectSlug(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/data/projects/gascity", "-data-projects-gascity"},
		{"/home/user/my.project", "-home-user-my-project"},
		{"/tmp/a/b/c", "-tmp-a-b-c"},
		{"/", "-"},
	}
	for _, tt := range tests {
		got := claudeProjectSlug(tt.path)
		if got != tt.want {
			t.Errorf("claudeProjectSlug(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

func TestFindJSONLSessionFile(t *testing.T) {
	base := t.TempDir()
	slug := "-data-projects-gascity"
	dir := filepath.Join(base, slug)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Write two JSONL files with different mod times.
	older := filepath.Join(dir, "old-session.jsonl")
	newer := filepath.Join(dir, "new-session.jsonl")
	if err := os.WriteFile(older, []byte(`{"type":"assistant"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Ensure different mod time.
	oldTime := time.Now().Add(-10 * time.Second)
	if err := os.Chtimes(older, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newer, []byte(`{"type":"tool_use"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	got := findJSONLSessionFile([]string{base}, "/data/projects/gascity")
	if got != newer {
		t.Errorf("findJSONLSessionFile() = %q, want %q", got, newer)
	}
}

func TestFindJSONLSessionFileNoMatch(t *testing.T) {
	base := t.TempDir()
	got := findJSONLSessionFile([]string{base}, "/no/such/project")
	if got != "" {
		t.Errorf("findJSONLSessionFile() = %q, want empty", got)
	}
}

func TestObserveSearchPathsDedup(t *testing.T) {
	paths := observeSearchPaths([]string{
		"/extra/path",
		"/extra/path", // duplicate
		"/another/path",
	})

	// Should include default + deduplicated extras.
	seen := make(map[string]int)
	for _, p := range paths {
		seen[p]++
	}
	for p, count := range seen {
		if count > 1 {
			t.Errorf("path %q appears %d times, want 1", p, count)
		}
	}
	// Extra paths should be present.
	found := false
	for _, p := range paths {
		if p == "/extra/path" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected /extra/path in results: %v", paths)
	}
}
