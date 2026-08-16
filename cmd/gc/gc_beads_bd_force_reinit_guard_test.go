package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

// Exit codes of bd_runtime_store_holds_bd_tables. Named here so the table below
// reads as the three-way answer it is rather than as bare integers.
const (
	bdTablesPresent = 0
	bdTablesAbsent  = 1
	bdTablesUnknown = 2
)

// TestStoreHoldsBdTablesDistinguishesEmptyFromUndetermined pins the guard that
// decides whether op_init may answer a negative bd_runtime_schema_ready probe
// with a DESTRUCTIVE `bd init --force`.
//
// The probe swallows every error, so "the bd schema is absent" and "the server
// hiccuped mid-query" arrive at the call site as the same false. server_reachable
// narrows that only to the case where the whole server is down: it runs earlier,
// on a different connection, and without a database context, so a blip during
// the schema probe itself still reads as a missing schema. Forcing a reinit
// there re-runs bd's migrations over a working set that already holds
// uncommitted rows, beads refuses to migrate a dirty table
// (gastownhall/beads#4566), and city init dies with a bare "bd init failed"
// naming neither the database nor the reason.
//
// The three-way answer is the point. "Absent" authorizes the reinit and is the
// ordinary fresh-init path, "present" forbids it, and "undetermined" is neither.
// Folding undetermined into present would refuse to initialize a fresh city
// whenever the count query is unavailable; folding it into absent would restore
// the destructive guess this guard exists to stop.
func TestStoreHoldsBdTablesDistinguishesEmptyFromUndetermined(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available; skipping shell-function test")
	}

	src := readGCBeadsBdScript(t)
	validSQLName := extractShellFunction(t, src, "valid_sql_name")
	tableCount := extractShellFunction(t, src, "bd_runtime_bd_table_count")
	holdsTables := extractShellFunction(t, src, "bd_runtime_store_holds_bd_tables")

	cases := []struct {
		name     string
		stdout   string
		exitCode int
		want     int
		why      string
	}{
		{
			name:     "empty_database_authorizes_reinit",
			stdout:   "cnt\n0\n",
			exitCode: 0,
			want:     bdTablesAbsent,
			why:      "no bd tables is the fresh store this branch serves, so reinit only creates schema and must not be delayed",
		},
		{
			name:     "populated_database_forbids_reinit",
			stdout:   "cnt\n3\n",
			exitCode: 0,
			want:     bdTablesPresent,
			why:      "bd tables exist, so the negative schema probe contradicts the database and a forced reinit would migrate over live rows",
		},
		{
			name:     "unanswered_query_is_undetermined",
			stdout:   "",
			exitCode: 1,
			want:     bdTablesUnknown,
			why:      "a query that did not answer is evidence of nothing, and must read as neither an empty nor a populated store",
		},
		{
			name:     "empty_output_with_success_is_undetermined",
			stdout:   "",
			exitCode: 0,
			want:     bdTablesUnknown,
			why:      "a server or stub that exits 0 without printing a count has still told us nothing",
		},
		{
			name:     "unparseable_count_is_undetermined",
			stdout:   "cnt\nnot-a-number\n",
			exitCode: 0,
			want:     bdTablesUnknown,
			why:      "a count that is not a number says nothing about what the reinit would destroy",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			binDir := t.TempDir()
			writeFakeCountDolt(t, binDir, tc.stdout, tc.exitCode)

			// connect_host is overridden so the test exercises only the
			// safety decision, not host resolution.
			script := "connect_host() { printf '127.0.0.1'; }\n" +
				validSQLName + "\n" +
				tableCount + "\n" +
				holdsTables + "\n" +
				"bd_runtime_store_holds_bd_tables hq\n"

			got := exitCodeOf(t, runGCBeadsBdSnippet(t, script, binDir))
			if got != tc.want {
				t.Fatalf("bd_runtime_store_holds_bd_tables = %d, want %d (fake dolt stdout=%q exit=%d): %s",
					got, tc.want, tc.stdout, tc.exitCode, tc.why)
			}
		})
	}
}

// TestBdRuntimeBdTableCountRejectsUnsafeDatabaseNames keeps the count query's
// database argument on the same allowlist the rest of the script interpolates
// under. The name reaches a SQL string directly, so one that valid_sql_name
// would reject must never get that far.
func TestBdRuntimeBdTableCountRejectsUnsafeDatabaseNames(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available; skipping shell-function test")
	}

	src := readGCBeadsBdScript(t)
	validSQLName := extractShellFunction(t, src, "valid_sql_name")
	tableCount := extractShellFunction(t, src, "bd_runtime_bd_table_count")

	cases := []struct {
		name string
		db   string
	}{
		{"empty", ""},
		{"statement_separator", "hq; DROP DATABASE hq"},
		{"single_quote", "hq'"},
		{"backtick", "hq`"},
		{"whitespace", "hq hq"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			binDir := t.TempDir()
			// A clean count and exit 0: were the name guard missing, the call
			// would succeed and this test would catch it.
			writeFakeCountDolt(t, binDir, "cnt\n0\n", 0)

			script := "connect_host() { printf '127.0.0.1'; }\n" +
				validSQLName + "\n" +
				tableCount + "\n" +
				"bd_runtime_bd_table_count \"$1\"\n"

			if err := runGCBeadsBdSnippet(t, script, binDir, tc.db); err == nil {
				t.Fatalf("bd_runtime_bd_table_count accepted unsafe database name %q", tc.db)
			}
		})
	}
}

// readGCBeadsBdScript returns the provider script's source.
func readGCBeadsBdScript(t *testing.T) string {
	t.Helper()
	scriptPath := filepath.Join(repoRootForLint(t), "examples", "bd", "assets", "scripts", "gc-beads-bd.sh")
	scriptBytes, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read script: %v", err)
	}
	return string(scriptBytes)
}

// runGCBeadsBdSnippet runs extracted shell functions with binDir first on PATH,
// passing args as $1, $2, … and returning the snippet's exit status as an error.
func runGCBeadsBdSnippet(t *testing.T, script, binDir string, args ...string) error {
	t.Helper()
	cmd := exec.Command("bash", append([]string{"-c", script, "bash"}, args...)...)
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"DOLT_PORT=42188",
		"DOLT_USER=root",
		"DOLT_PASSWORD=",
	)
	return cmd.Run()
}

// exitCodeOf turns runGCBeadsBdSnippet's error back into the shell exit status,
// failing the test on an error that carries no status (the snippet never ran).
func exitCodeOf(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	t.Fatalf("running shell snippet: %v", err)
	return -1
}

// writeFakeCountDolt installs a dolt stub that prints the given CSV on stdout
// and exits with the given code, standing in for the count query's server. The
// payload goes through a file so no shell quoting of the CSV is needed.
func writeFakeCountDolt(t *testing.T, dir, stdout string, exitCode int) {
	t.Helper()
	payload := filepath.Join(dir, "count.csv")
	if err := os.WriteFile(payload, []byte(stdout), 0o600); err != nil {
		t.Fatalf("write fake dolt payload: %v", err)
	}
	body := "#!/bin/sh\ncat '" + payload + "'\nexit " + strconv.Itoa(exitCode) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "dolt"), []byte(body), 0o755); err != nil {
		t.Fatalf("write fake dolt: %v", err)
	}
}
