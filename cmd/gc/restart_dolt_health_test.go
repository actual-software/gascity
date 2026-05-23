package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/config"
)

// newManagedBdCity scaffolds a bd-contract city whose Dolt lifecycle
// is gc-owned (the path under test for verifyDoltHealthyAfterRestart's
// happy / failure branches). The on-disk minimum is a city.toml that
// selects the managed bd provider; no rig metadata so
// resolveConfiguredCityDoltTarget returns ok=false (owned by gc).
func newManagedBdCity(t *testing.T) string {
	t.Helper()
	cityPath := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cityPath, ".gc"), 0o755); err != nil {
		t.Fatal(err)
	}
	cityToml := `[workspace]
name = "test-city"

[beads]
provider = "bd"
`
	if err := os.WriteFile(filepath.Join(cityPath, "city.toml"), []byte(cityToml), 0o644); err != nil {
		t.Fatal(err)
	}
	// Clear the in-memory dolt-config registry for this city so prior tests
	// can't leak an external-Dolt registration into ours.
	clearCityDoltConfig(cityPath)
	t.Cleanup(func() { clearCityDoltConfig(cityPath) })
	return cityPath
}

func TestVerifyDoltHealthyAfterRestart_NoOpForFileProvider(t *testing.T) {
	t.Setenv("GC_BEADS", "")
	t.Setenv("GC_BEADS_SCOPE_ROOT", "")

	cityPath := t.TempDir()
	if err := os.WriteFile(filepath.Join(cityPath, "city.toml"), []byte(`[workspace]
name = "test-city"

[beads]
provider = "file"
`), 0o644); err != nil {
		t.Fatal(err)
	}

	called := int32(0)
	restoreHook := overrideHealthBeadsProviderHook(func(string) error {
		atomic.AddInt32(&called, 1)
		return errors.New("should not be called for file provider")
	})
	defer restoreHook()

	if err := verifyDoltHealthyAfterRestart(cityPath, io.Discard); err != nil {
		t.Fatalf("verifyDoltHealthyAfterRestart() error = %v, want nil for file provider", err)
	}
	if got := atomic.LoadInt32(&called); got != 0 {
		t.Fatalf("healthBeadsProvider called %d times, want 0 (skipped for file provider)", got)
	}
}

func TestVerifyDoltHealthyAfterRestart_NoOpWhenManagedDoltNotOwned(t *testing.T) {
	// External-Dolt city: bd-contract, but the operator pinned host/port
	// to an external server. resolveConfiguredCityDoltTarget reports the
	// endpoint as operator-configured (ok=true), so the lifecycle is not
	// owned by gc and the post-restart check has nothing to verify.
	cityPath := newManagedBdCity(t)
	registerCityDoltConfig(cityPath, config.DoltConfig{
		Host: "db.example.com",
		Port: 4406,
	})
	t.Cleanup(func() { clearCityDoltConfig(cityPath) })

	called := int32(0)
	restoreHook := overrideHealthBeadsProviderHook(func(string) error {
		atomic.AddInt32(&called, 1)
		return errors.New("should not be called for external Dolt")
	})
	defer restoreHook()

	if err := verifyDoltHealthyAfterRestart(cityPath, io.Discard); err != nil {
		t.Fatalf("verifyDoltHealthyAfterRestart() error = %v, want nil for external Dolt", err)
	}
	if got := atomic.LoadInt32(&called); got != 0 {
		t.Fatalf("healthBeadsProvider called %d times, want 0 (skipped when gc does not own Dolt)", got)
	}
}

func TestVerifyDoltHealthyAfterRestart_SuccessOnFirstProbe(t *testing.T) {
	cityPath := newManagedBdCity(t)

	restoreHook := overrideHealthBeadsProviderHook(func(string) error {
		return nil
	})
	defer restoreHook()

	if err := verifyDoltHealthyAfterRestart(cityPath, io.Discard); err != nil {
		t.Fatalf("verifyDoltHealthyAfterRestart() error = %v, want nil", err)
	}
}

func TestVerifyDoltHealthyAfterRestart_SuccessAfterRetry(t *testing.T) {
	cityPath := newManagedBdCity(t)

	calls := int32(0)
	restoreHook := overrideHealthBeadsProviderHook(func(string) error {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return errors.New("dial tcp 127.0.0.1:0: connect: can't assign requested address")
		}
		return nil
	})
	defer restoreHook()
	// Give the loop room to retry.
	t.Setenv(envRestartDoltHealthTimeout, "5s")

	if err := verifyDoltHealthyAfterRestart(cityPath, io.Discard); err != nil {
		t.Fatalf("verifyDoltHealthyAfterRestart() error = %v, want nil after retry", err)
	}
	if got := atomic.LoadInt32(&calls); got < 3 {
		t.Fatalf("healthBeadsProvider called %d times, want >= 3 (retried until healthy)", got)
	}
}

// TestVerifyDoltHealthyAfterRestart_TimeoutReturnsLoudError is the
// regression-test the bead asks for: simulate a `gc restart` whose
// Dolt is in a sleeping/closed state (healthBeadsProvider keeps
// returning unreachable), and assert the command exits with a clear
// failure that names the cause.
func TestVerifyDoltHealthyAfterRestart_TimeoutReturnsLoudError(t *testing.T) {
	cityPath := newManagedBdCity(t)

	probeErr := errors.New("dial tcp 127.0.0.1:0: connect: can't assign requested address")
	restoreHook := overrideHealthBeadsProviderHook(func(string) error {
		return probeErr
	})
	defer restoreHook()
	// Short timeout keeps the test fast.
	t.Setenv(envRestartDoltHealthTimeout, "100ms")

	start := time.Now()
	err := verifyDoltHealthyAfterRestart(cityPath, io.Discard)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("verifyDoltHealthyAfterRestart() error = nil, want non-nil for persistent unreachability")
	}
	msg := err.Error()
	wantSubstrings := []string{
		"managed Dolt did not become healthy",
		"100ms",
		probeErr.Error(),
		"gc start",
		envRestartDoltHealthTimeout,
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q\nfull message: %s", want, msg)
		}
	}
	// The loop must respect the deadline; allow some slack but reject
	// runaway loops.
	if elapsed > 2*time.Second {
		t.Errorf("verifyDoltHealthyAfterRestart took %s, want under 2s with 100ms budget", elapsed)
	}
}

func TestRestartDoltHealthTimeoutFromEnv(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want time.Duration
	}{
		{"unset uses default", "", restartDoltHealthDefaultTimeout},
		{"valid duration", "45s", 45 * time.Second},
		{"valid minute duration", "2m", 2 * time.Minute},
		{"invalid syntax falls back to default", "garbage", restartDoltHealthDefaultTimeout},
		{"zero falls back to default", "0s", restartDoltHealthDefaultTimeout},
		{"negative falls back to default", "-5s", restartDoltHealthDefaultTimeout},
		{"whitespace-only falls back to default", "   ", restartDoltHealthDefaultTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envRestartDoltHealthTimeout, tc.env)
			got := restartDoltHealthTimeoutFromEnv()
			if got != tc.want {
				t.Fatalf("restartDoltHealthTimeoutFromEnv() = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestCmdRestartJSON_HealthcheckFailureExitsNonZero is the integration
// view of the same scenario: cmdRestartJSON itself returns 1 and writes
// the healthcheck error to stderr when the post-restart probe times out,
// even though the stop+start steps succeeded.
func TestCmdRestartJSON_HealthcheckFailureExitsNonZero(t *testing.T) {
	cityPath := newManagedBdCity(t)

	// Replace the healthcheck hook with a deterministic failure so we don't
	// need a real Dolt process to drive the failure branch.
	probeErr := errors.New("dial tcp 127.0.0.1:0: connect: can't assign requested address")
	restoreVerifyHook := overrideVerifyDoltHealthyAfterRestartHook(func(_ string, _ io.Writer) error {
		return fmt.Errorf("managed Dolt did not become healthy within 100ms after restart: %v\n  Recover with: gc start %s", probeErr, cityPath)
	})
	defer restoreVerifyHook()

	// Skip the stop+start network/supervisor work by stubbing them out.
	restoreStop, restoreStart := stubRestartLifecycleForTest(t)
	defer restoreStop()
	defer restoreStart()

	// Make registration-name resolution work without a real supervisor.
	restoreName := overrideRestartRegistrationNameHook(func([]string) (string, error) {
		return "test-city", nil
	})
	defer restoreName()

	var stdout, stderr bytes.Buffer
	code := cmdRestartJSON([]string{cityPath}, &stdout, &stderr, false /*jsonOut*/)

	if code == 0 {
		t.Fatalf("cmdRestartJSON returned 0, want non-zero on healthcheck failure\nstdout: %s\nstderr: %s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "gc restart:") {
		t.Errorf("stderr missing 'gc restart:' prefix, got: %s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "managed Dolt did not become healthy") {
		t.Errorf("stderr missing healthcheck-failure body, got: %s", stderr.String())
	}
}

// overrideHealthBeadsProviderHook swaps the test seam atomically.
func overrideHealthBeadsProviderHook(fn func(string) error) func() {
	prev := healthBeadsProviderHook
	healthBeadsProviderHook = fn
	return func() { healthBeadsProviderHook = prev }
}

// overrideVerifyDoltHealthyAfterRestartHook swaps the seam used by
// cmdRestartJSON. Tests that drive the integration path use this so
// they can simulate timeouts without spinning a real Dolt.
func overrideVerifyDoltHealthyAfterRestartHook(fn func(string, io.Writer) error) func() {
	prev := verifyDoltHealthyAfterRestartHook
	verifyDoltHealthyAfterRestartHook = fn
	return func() { verifyDoltHealthyAfterRestartHook = prev }
}

// overrideRestartRegistrationNameHook substitutes the city-name
// resolver in cmdRestartJSON so the test does not need a real
// supervisor registry.
func overrideRestartRegistrationNameHook(fn func([]string) (string, error)) func() {
	prev := restartRegistrationNameHook
	restartRegistrationNameHook = fn
	return func() { restartRegistrationNameHook = prev }
}

// stubRestartLifecycleForTest replaces the stop+start steps in
// cmdRestartJSON with no-ops that report success, so tests can drive
// the post-start healthcheck branch deterministically. Returns two
// restore funcs (one per hook) for symmetric defer cleanup.
func stubRestartLifecycleForTest(t *testing.T) (func(), func()) {
	t.Helper()
	prevStop := restartCmdStopHook
	restartCmdStopHook = func(_ []string, _ io.Writer, _ io.Writer, _ time.Duration, _ bool) int {
		return 0
	}
	prevStart := restartDoStartWithNameOverrideHook
	restartDoStartWithNameOverrideHook = func(_ []string, _ bool, _ io.Writer, _ io.Writer, _ string) int {
		return 0
	}
	return func() { restartCmdStopHook = prevStop },
		func() { restartDoStartWithNameOverrideHook = prevStart }
}
