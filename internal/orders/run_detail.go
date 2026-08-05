package orders

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// ExecOutputTailLimit bounds how many bytes of an exec order's combined
// stdout/stderr are retained in the run's diagnostic detail. A failing order
// is usually explained by its last few lines, and the detail is stored on a
// tracking bead and echoed into an event message, so the whole transcript is
// neither needed nor affordable.
const ExecOutputTailLimit = 2048

// ExecRunDetail describes one exec order run in enough detail to diagnose it
// after the fact. It is the antidote to a tracking bead that records THAT an
// order failed and discards WHY: rendered through String and stored on the
// run, it answers "what ran, how did it exit, and what did it say" without
// having to catch the failure live.
type ExecRunDetail struct {
	// Command is the resolved shell command the controller ran.
	Command string
	// ExitCode is the child's exit status. It is negative when no exit code
	// could be resolved — the command never started, the context was
	// canceled, or the process was killed by a signal.
	ExitCode int
	// Err is the dispatch error string, e.g. "exit status 1". Empty on a
	// successful run.
	Err string
	// Output is the child's combined stdout and stderr.
	Output []byte
}

// String renders the detail as the plain-text block stored on the run's
// tracking bead and echoed into the order.failed event message. Callers are
// responsible for redacting the result before it is persisted; every field can
// carry values interpolated from the order's environment.
func (d ExecRunDetail) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "exec: %s\n", strings.TrimSpace(d.Command))
	if d.ExitCode >= 0 {
		fmt.Fprintf(&b, "exit status: %d\n", d.ExitCode)
	} else {
		b.WriteString("exit status: unknown (no exit code; canceled, signaled, or never started)\n")
	}
	if d.Err != "" {
		fmt.Fprintf(&b, "error: %s\n", d.Err)
	}
	total := len(d.Output)
	switch {
	case total == 0:
		b.WriteString("output: (empty)")
	case total > ExecOutputTailLimit:
		fmt.Fprintf(&b, "output (last %d of %d bytes):\n%s", ExecOutputTailLimit, total, d.Output[total-ExecOutputTailLimit:])
	default:
		fmt.Fprintf(&b, "output (%d bytes):\n%s", total, d.Output)
	}
	return b.String()
}

// ExitCodeFromError resolves the child process exit code carried by an
// ExecRunner error. The second return reports whether a code was resolvable:
// a nil error is exit 0, an *exec.ExitError carries its own status, and
// everything else — a context cancellation, a start failure, or an error
// joined with a second failure such as process-group cleanup — is
// unresolvable and must not be reduced to a code.
func ExitCodeFromError(err error) (int, bool) {
	if err == nil {
		return 0, true
	}
	// A joined error carries a second, independent failure alongside any exit
	// status. Reducing it to the exit code alone would silently drop that
	// failure, so refuse it here and let the caller treat the run as failed.
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		return 0, false
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), true
	}
	return 0, false
}
