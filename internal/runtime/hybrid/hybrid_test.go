package hybrid

import (
	"context"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/runtime"
)

func isRemote(name string) bool { return strings.Contains(name, "polecat") }

func TestStart_RoutesToLocal(t *testing.T) {
	local, remote := runtime.NewFake(), runtime.NewFake()
	h := New(local, remote, isRemote)

	if err := h.Start(context.Background(), "refinery", runtime.Config{}); err != nil {
		t.Fatal(err)
	}
	if !local.IsRunning("refinery") {
		t.Error("expected local to have session")
	}
	if remote.IsRunning("refinery") {
		t.Error("remote should not have session")
	}
}

func TestStart_RoutesToRemote(t *testing.T) {
	local, remote := runtime.NewFake(), runtime.NewFake()
	h := New(local, remote, isRemote)

	if err := h.Start(context.Background(), "polecat-1", runtime.Config{}); err != nil {
		t.Fatal(err)
	}
	if local.IsRunning("polecat-1") {
		t.Error("local should not have session")
	}
	if !remote.IsRunning("polecat-1") {
		t.Error("expected remote to have session")
	}
}

func TestListRunning_MergesBothBackends(t *testing.T) {
	local, remote := runtime.NewFake(), runtime.NewFake()
	h := New(local, remote, isRemote)

	_ = h.Start(context.Background(), "gc-demo--refinery", runtime.Config{})
	_ = h.Start(context.Background(), "gc-demo--polecat-1", runtime.Config{})
	_ = h.Start(context.Background(), "gc-demo--polecat-2", runtime.Config{})

	names, err := h.ListRunning("gc-demo-")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 3 {
		t.Fatalf("expected 3 sessions, got %d: %v", len(names), names)
	}
}

func TestListRunning_PartialFailure(t *testing.T) {
	local := runtime.NewFake()
	remote := runtime.NewFailFake()
	h := New(local, remote, isRemote)

	_ = local.Start(context.Background(), "gc-demo--refinery", runtime.Config{})

	names, err := h.ListRunning("gc-demo-")
	if err != nil {
		t.Fatalf("expected nil error on partial failure, got %v", err)
	}
	if len(names) != 1 {
		t.Fatalf("expected 1 session from healthy backend, got %d", len(names))
	}
}

func TestListRunning_BothFail(t *testing.T) {
	local := runtime.NewFailFake()
	remote := runtime.NewFailFake()
	h := New(local, remote, isRemote)

	_, err := h.ListRunning("gc-demo-")
	if err == nil {
		t.Fatal("expected error when both backends fail")
	}
}

func TestAttach_RoutesCorrectly(t *testing.T) {
	local, remote := runtime.NewFake(), runtime.NewFake()
	h := New(local, remote, isRemote)

	_ = h.Start(context.Background(), "refinery", runtime.Config{})
	_ = h.Start(context.Background(), "polecat-1", runtime.Config{})

	if err := h.Attach("refinery"); err != nil {
		t.Errorf("attach local: %v", err)
	}
	if err := h.Attach("polecat-1"); err != nil {
		t.Errorf("attach remote: %v", err)
	}

	// Verify calls went to correct backends.
	var localAttach, remoteAttach int
	for _, c := range local.Calls {
		if c.Method == "Attach" {
			localAttach++
		}
	}
	for _, c := range remote.Calls {
		if c.Method == "Attach" {
			remoteAttach++
		}
	}
	if localAttach != 1 {
		t.Errorf("expected 1 local attach, got %d", localAttach)
	}
	if remoteAttach != 1 {
		t.Errorf("expected 1 remote attach, got %d", remoteAttach)
	}
}

func TestStop_RoutesCorrectly(t *testing.T) {
	local, remote := runtime.NewFake(), runtime.NewFake()
	h := New(local, remote, isRemote)

	_ = h.Start(context.Background(), "refinery", runtime.Config{})
	_ = h.Start(context.Background(), "polecat-1", runtime.Config{})

	if err := h.Stop("refinery"); err != nil {
		t.Fatal(err)
	}
	if err := h.Stop("polecat-1"); err != nil {
		t.Fatal(err)
	}

	if local.IsRunning("refinery") {
		t.Error("refinery should be stopped")
	}
	if remote.IsRunning("polecat-1") {
		t.Error("polecat-1 should be stopped")
	}
}
