package tmux

import (
	"testing"
)

// fakeExecutor captures tmux command arguments for unit testing.
type fakeExecutor struct {
	calls [][]string // each call's full args
	out   string
	err   error
}

func (f *fakeExecutor) execute(args []string) (string, error) {
	// Copy args to avoid aliasing with the caller's slice.
	cp := make([]string, len(args))
	copy(cp, args)
	f.calls = append(f.calls, cp)
	return f.out, f.err
}

func TestRunInjectsSocketFlag(t *testing.T) {
	fe := &fakeExecutor{}
	tm := &Tmux{cfg: Config{SocketName: "bright-lights"}, exec: fe}
	_, _ = tm.run("list-sessions")

	if len(fe.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(fe.calls))
	}
	got := fe.calls[0]
	want := []string{"-u", "-L", "bright-lights", "list-sessions"}
	if len(got) != len(want) {
		t.Fatalf("args = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("args[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRunNoSocketFlagWhenEmpty(t *testing.T) {
	fe := &fakeExecutor{}
	tm := &Tmux{cfg: DefaultConfig(), exec: fe}
	_, _ = tm.run("list-sessions")

	if len(fe.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(fe.calls))
	}
	got := fe.calls[0]
	want := []string{"-u", "list-sessions"}
	if len(got) != len(want) {
		t.Fatalf("args = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("args[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRunAlwaysPrependsUTF8Flag(t *testing.T) {
	fe := &fakeExecutor{}
	tm := &Tmux{cfg: Config{SocketName: "x"}, exec: fe}
	_, _ = tm.run("new-session", "-s", "test")

	if len(fe.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(fe.calls))
	}
	got := fe.calls[0]
	if got[0] != "-u" {
		t.Errorf("args[0] = %q, want %q", got[0], "-u")
	}
	// Verify full arg list: -u -L x new-session -s test
	want := []string{"-u", "-L", "x", "new-session", "-s", "test"}
	if len(got) != len(want) {
		t.Fatalf("args = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("args[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
