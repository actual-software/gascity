package orders

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestExecRunDetailString(t *testing.T) {
	tests := []struct {
		name   string
		detail ExecRunDetail
		want   string
	}{
		{
			name: "failed run carries command, status, error, and output",
			detail: ExecRunDetail{
				Command:  "gc doctor --json",
				ExitCode: 1,
				Err:      "exit status 1",
				Output:   []byte("gc: unknown command \"doctor\"\n"),
			},
			want: "exec: gc doctor --json\n" +
				"exit status: 1\n" +
				"error: exit status 1\n" +
				"output (29 bytes):\ngc: unknown command \"doctor\"\n",
		},
		{
			name: "silent failure still names what ran and how it exited",
			detail: ExecRunDetail{
				Command:  "scripts/preflight.sh",
				ExitCode: 2,
				Err:      "exit status 2",
			},
			want: "exec: scripts/preflight.sh\n" +
				"exit status: 2\n" +
				"error: exit status 2\n" +
				"output: (empty)",
		},
		{
			name: "no resolvable exit code says so rather than implying 0",
			detail: ExecRunDetail{
				Command:  "scripts/slow.sh",
				ExitCode: -1,
				Err:      "context deadline exceeded",
			},
			want: "exec: scripts/slow.sh\n" +
				"exit status: unknown (no exit code; canceled, signaled, or never started)\n" +
				"error: context deadline exceeded\n" +
				"output: (empty)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.detail.String(); got != tc.want {
				t.Fatalf("String() =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

// A chatty command must not be able to write an unbounded description onto its
// tracking bead, and the reader has to be told the transcript was cut.
func TestExecRunDetailStringTruncatesOutputTail(t *testing.T) {
	output := append(bytes.Repeat([]byte("a"), ExecOutputTailLimit+500), []byte("TAIL-MARKER")...)
	detail := ExecRunDetail{Command: "scripts/chatty.sh", ExitCode: 1, Err: "exit status 1", Output: output}

	got := detail.String()
	header := fmt.Sprintf("output (last %d of %d bytes):", ExecOutputTailLimit, len(output))
	if !strings.Contains(got, header) {
		t.Fatalf("String() missing truncation header %q; got:\n%s", header, got)
	}
	if !strings.HasSuffix(got, "TAIL-MARKER") {
		t.Fatal("String() dropped the tail of the output; the last lines are the ones that explain a failure")
	}
	if strings.Contains(got, string(bytes.Repeat([]byte("a"), ExecOutputTailLimit+1))) {
		t.Fatal("String() retained more than the tail limit of output")
	}
}

// The genuine *exec.ExitError paths are covered end to end by the dispatch
// tests in cmd/gc, which run real commands through shellExecRunner and assert
// on the resolved status (exit 12 reported as a failure, exit 1 honored as
// declared informational, exit 11 still failing). This package's tests cover
// the non-ExitError contract without spawning a process of their own: the
// repo's resource census ratchets subprocess call sites, and a unit test is
// not worth a new one.
func TestExitCodeFromError(t *testing.T) {
	t.Run("nil error is exit 0", func(t *testing.T) {
		code, ok := ExitCodeFromError(nil)
		if !ok || code != 0 {
			t.Fatalf("ExitCodeFromError(nil) = (%d, %v), want (0, true)", code, ok)
		}
	})

	t.Run("context cancellation has no exit code", func(t *testing.T) {
		if code, ok := ExitCodeFromError(context.Canceled); ok {
			t.Fatalf("ExitCodeFromError(context.Canceled) = (%d, true), want unresolvable", code)
		}
	})

	t.Run("a plain error has no exit code", func(t *testing.T) {
		if code, ok := ExitCodeFromError(errors.New("fork/exec: no such file or directory")); ok {
			t.Fatalf("ExitCodeFromError(plain) = (%d, true), want unresolvable", code)
		}
	})

	t.Run("wrapping does not invent an exit code", func(t *testing.T) {
		if code, ok := ExitCodeFromError(fmt.Errorf("running order: %w", context.DeadlineExceeded)); ok {
			t.Fatalf("ExitCodeFromError(wrapped deadline) = (%d, true), want unresolvable", code)
		}
	})

	// A joined error means a second failure rode along with any exit status,
	// process-group cleanup most often. Reducing it to the exit code alone
	// would let a declared-informational code swallow that failure whole.
	t.Run("joined error refuses to reduce to an exit code", func(t *testing.T) {
		joined := errors.Join(context.Canceled, errors.New("terminating process group: operation not permitted"))
		if code, ok := ExitCodeFromError(joined); ok {
			t.Fatalf("ExitCodeFromError(joined) = (%d, true), want unresolvable", code)
		}
	})
}
