package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTranscript(t *testing.T, lines ...string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "transcript.jsonl")
	if err := os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	return p
}

func usageLine(model string, input, cacheRead, cacheCreate int) string {
	return fmt.Sprintf(
		`{"type":"assistant","message":{"model":%q,"usage":{"input_tokens":%d,"cache_read_input_tokens":%d,"cache_creation_input_tokens":%d}}}`,
		model, input, cacheRead, cacheCreate)
}

func hookInputFor(path string) []byte {
	return []byte(fmt.Sprintf(`{"transcript_path":%q,"hook_event_name":"UserPromptSubmit"}`, path))
}

func TestContextInjectSilentBelowAdvisory(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	// 100k of 1M = 10% — well below the 60% advisory threshold.
	p := writeTranscript(t, usageLine("claude-fable-5", 1_000, 98_000, 1_000))
	if got := contextInjectLine(hookInputFor(p)); got != "" {
		t.Errorf("below advisory should be silent, got %q", got)
	}
}

func TestContextInjectAdvisoryBand(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	// 700k of 1M = 70% — advisory band.
	p := writeTranscript(t, usageLine("claude-fable-5", 10_000, 680_000, 10_000))
	got := contextInjectLine(hookInputFor(p))
	if !strings.Contains(got, "700k/1000k") || !strings.Contains(got, "~70%") {
		t.Errorf("advisory line wrong: %q", got)
	}
	if !strings.Contains(got, "clean seam") || !strings.Contains(got, "reset") {
		t.Errorf("advisory must point toward a clean seam + planned reset, got %q", got)
	}
	if strings.Contains(got, "HIGH") {
		t.Errorf("advisory band must not be marked HIGH: %q", got)
	}
}

func TestContextInjectUrgentBand(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	// 900k of 1M = 90% — urgent band.
	p := writeTranscript(t, usageLine("claude-opus-4-8[1m]", 50_000, 800_000, 50_000))
	got := contextInjectLine(hookInputFor(p))
	if !strings.Contains(got, "HIGH") || !strings.Contains(got, "gc session reset") {
		t.Errorf("urgent line must direct to handoff + self gc session reset: %q", got)
	}
	if !strings.Contains(got, "operator") {
		t.Errorf("urgent line must preserve the operator-stay-up override: %q", got)
	}
}

func TestContextInjectLastUsageEntryWins(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	// Older 90% entry followed by a newer 10% one (post-compaction shape):
	// the LAST entry is the live context size, so this must be silent.
	p := writeTranscript(t,
		usageLine("claude-fable-5", 50_000, 800_000, 50_000),
		usageLine("claude-fable-5", 5_000, 90_000, 5_000),
	)
	if got := contextInjectLine(hookInputFor(p)); got != "" {
		t.Errorf("last entry (10%%) should win and be silent, got %q", got)
	}
}

// TestContextInjectWindowResolution covers every way the denominator gets
// chosen: the conservative default, family recognition, and promotion past an
// observed footprint. The cases share one process-environment mutation on
// purpose. cmd/gc carries a debt ratchet on untagged environment calls (see
// TESTING.md), so new coverage here is added as table rows rather than as new
// test functions that would each need their own environment mutation.
func TestContextInjectWindowResolution(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	for _, tc := range []struct {
		name  string
		model string
		// The three usage fields sum to the observed footprint.
		input, cacheRead, cacheCreate int
		// wants are substrings the rendered line must contain. An empty
		// wants means the line must be silent.
		wants []string
		why   string
	}{
		{
			name:  "unrecognized model falls back to the 200k default",
			model: "some-other-model", input: 10_000, cacheRead: 130_000, cacheCreate: 10_000,
			wants: []string{"150k/200k", "~75%"},
			why:   "150k against the conservative default is 75%",
		},
		{
			name:  "bare claude-opus-5 resolves to the 1M window",
			model: "claude-opus-5", input: 11_440, cacheRead: 150_000, cacheCreate: 10_000,
			why: "Claude Code records the resolved API model and drops the launch-time 1M selector, so a session launched as claude-opus-5[1m] logs a bare claude-opus-5; 171k of 1M is 17% and must be silent, where the old 200k default read 86% and ordered a recycle every turn",
		},
		{
			name:  "bare claude-sonnet-5 resolves to the 1M window",
			model: "claude-sonnet-5", input: 10_000, cacheRead: 680_000, cacheCreate: 10_000,
			wants: []string{"700k/1000k"},
			why:   "700k of 1M is 70%, the advisory band, which proves the family resolved to 1M",
		},
		{
			name:  "footprint past the resolved window promotes it",
			model: "some-future-model", input: 20_000, cacheRead: 660_000, cacheCreate: 20_000,
			wants: []string{"700k/1000k"},
			why:   "a model the table has never seen, whose footprint already exceeds the default, is provably on a bigger window, so promote rather than report 350% and fire the urgent tier every turn",
		},
		{
			name:  "promotion stops at the smallest tier that holds the footprint",
			model: "gpt-5.6-sol", input: 10_000, cacheRead: 200_000, cacheCreate: 10_000,
			wants: []string{"220k/258k"},
			why:   "220k lands on the 258k tier rather than 1M; over-promoting would hide real context pressure",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := writeTranscript(t, usageLine(tc.model, tc.input, tc.cacheRead, tc.cacheCreate))
			got := contextInjectLine(hookInputFor(p))
			if len(tc.wants) == 0 {
				if got != "" {
					t.Errorf("expected a silent line (%s), got %q", tc.why, got)
				}
				return
			}
			for _, want := range tc.wants {
				if !strings.Contains(got, want) {
					t.Errorf("expected %q in the line (%s), got %q", want, tc.why, got)
				}
			}
		})
	}
}

func TestContextInjectWindowOverride(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	t.Setenv("GC_CONTEXT_WINDOW_TOKENS", "500000")
	p := writeTranscript(t, usageLine("some-other-model", 10_000, 380_000, 10_000))
	got := contextInjectLine(hookInputFor(p))
	if !strings.Contains(got, "400k/500k") {
		t.Errorf("window override not applied: %q", got)
	}
}

func TestContextInjectThresholdOverrides(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	t.Setenv("GC_CONTEXT_ADVISORY_PCT", "30")
	t.Setenv("GC_CONTEXT_URGENT_PCT", "40")
	// 50% of 1M: above the overridden urgent threshold.
	p := writeTranscript(t, usageLine("claude-fable-5", 10_000, 480_000, 10_000))
	if got := contextInjectLine(hookInputFor(p)); !strings.Contains(got, "HIGH") {
		t.Errorf("threshold overrides not applied: %q", got)
	}
}

func TestContextInjectDisabled(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "0")
	p := writeTranscript(t, usageLine("claude-fable-5", 50_000, 800_000, 50_000))
	if got := contextInjectLine(hookInputFor(p)); got != "" {
		t.Errorf("disabled should be silent, got %q", got)
	}
}

func TestContextInjectFailSafeSilent(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	for name, input := range map[string][]byte{
		"nil stdin":          nil,
		"garbage stdin":      []byte("not json"),
		"no transcript path": []byte(`{"hook_event_name":"UserPromptSubmit"}`),
		"missing file":       hookInputFor("/nonexistent/transcript.jsonl"),
	} {
		if got := contextInjectLine(input); got != "" {
			t.Errorf("%s: want silent, got %q", name, got)
		}
	}
	// Transcript with no usage entries.
	p := writeTranscript(t, `{"type":"user","message":{"content":"hi"}}`)
	if got := contextInjectLine(hookInputFor(p)); got != "" {
		t.Errorf("no-usage transcript: want silent, got %q", got)
	}
}

// Regression: the newest usage entry lacking a model string must not flip a
// 1M session to the 200k default (would fire the urgent tier far too early).
func TestContextInjectLastNonEmptyModelWins(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	// First entry names the 1M model; the newest usage entry omits model.
	// 700k must read as 70% of 1M (advisory), not 350% of 200k.
	p := writeTranscript(t,
		usageLine("claude-fable-5", 10_000, 680_000, 10_000),
		`{"type":"assistant","message":{"usage":{"input_tokens":10000,"cache_read_input_tokens":680000,"cache_creation_input_tokens":10000}}}`,
	)
	got := contextInjectLine(hookInputFor(p))
	if !strings.Contains(got, "700k/1000k") {
		t.Errorf("empty-model newest entry must retain the 1M window: %q", got)
	}
	if strings.Contains(got, "HIGH") {
		t.Errorf("70%% of 1M is advisory, not urgent: %q", got)
	}
}

// Bare claude-opus-4-8 is a 1M-context model (no [1m] suffix in the transcript).
func TestContextInjectBareOpus48Is1M(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	p := writeTranscript(t, usageLine("claude-opus-4-8", 10_000, 680_000, 10_000))
	got := contextInjectLine(hookInputFor(p))
	if !strings.Contains(got, "700k/1000k") {
		t.Errorf("bare opus-4-8 must resolve to the 1M window: %q", got)
	}
}

// Sidecar/compaction call on a smaller-window model must not shrink the
// main-loop session's window: max-over-models wins. (The observed 782k/200k
// bug: a Fable session with bare-opus sidecar entries, newest entry opus.)
func TestContextInjectSidecarDoesNotShrinkWindow(t *testing.T) {
	t.Setenv("GC_INJECT_CONTEXT", "")
	// Newest entry classifies 200k but carries the live (high) token count; an
	// earlier entry is the 1M main-loop model. Window must be 1M (max), so 700k
	// reads as ~70% (advisory), not ~350% of 200k.
	p := writeTranscript(t,
		usageLine("claude-fable-5", 10_000, 680_000, 10_000),   // main loop, 1M
		usageLine("claude-haiku-4-5", 10_000, 680_000, 10_000), // 200k-classified, newest, high tokens
	)
	got := contextInjectLine(hookInputFor(p))
	if !strings.Contains(got, "700k/1000k") {
		t.Errorf("a 200k-classified newest entry must not shrink the 1M session window: %q", got)
	}
}
