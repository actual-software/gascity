package main

import (
	"sync"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/runtime"
)

func TestControllerStateReadAccess(t *testing.T) {
	sp := runtime.NewFake()
	ep := events.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "rig1", Path: t.TempDir()},
		},
	}

	cs := newControllerState(cfg, sp, ep, "test-city", t.TempDir())

	if got := cs.CityName(); got != "test-city" {
		t.Errorf("CityName() = %q, want %q", got, "test-city")
	}
	if cs.Config() != cfg {
		t.Error("Config() returned wrong config")
	}
	if cs.SessionProvider() != sp {
		t.Error("SessionProvider() returned wrong provider")
	}
	if cs.EventProvider() != ep {
		t.Error("EventProvider() returned wrong provider")
	}

	stores := cs.BeadStores()
	if len(stores) != 1 {
		t.Errorf("BeadStores() len = %d, want 1", len(stores))
	}
	if cs.BeadStore("rig1") == nil {
		t.Error("BeadStore(rig1) = nil")
	}
	if cs.BeadStore("nonexistent") != nil {
		t.Error("BeadStore(nonexistent) should be nil")
	}

	provs := cs.MailProviders()
	if len(provs) != 1 {
		t.Errorf("MailProviders() len = %d, want 1", len(provs))
	}
	if cs.MailProvider("rig1") == nil {
		t.Error("MailProvider(rig1) = nil")
	}
}

func TestControllerStateConcurrentAccess(t *testing.T) {
	sp := runtime.NewFake()
	ep := events.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "rig1", Path: t.TempDir()},
		},
	}

	cs := newControllerState(cfg, sp, ep, "test-city", t.TempDir())

	// Concurrent readers should not race.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cs.Config()
			_ = cs.SessionProvider()
			_ = cs.BeadStores()
			_ = cs.MailProviders()
			_ = cs.EventProvider()
			_ = cs.CityName()
			_ = cs.CityPath()
		}()
	}
	wg.Wait()
}

func TestControllerStateUpdate(t *testing.T) {
	sp := runtime.NewFake()
	ep := events.NewFake()
	cfg1 := &config.City{
		Workspace: config.Workspace{Name: "city1"},
		Rigs: []config.Rig{
			{Name: "rig1", Path: t.TempDir()},
		},
	}

	cs := newControllerState(cfg1, sp, ep, "city1", t.TempDir())

	if len(cs.BeadStores()) != 1 {
		t.Fatalf("initial stores = %d, want 1", len(cs.BeadStores()))
	}

	// Update with new config adding a rig.
	cfg2 := &config.City{
		Workspace: config.Workspace{Name: "city1"},
		Rigs: []config.Rig{
			{Name: "rig1", Path: t.TempDir()},
			{Name: "rig2", Path: t.TempDir()},
		},
	}

	sp2 := runtime.NewFake()
	cs.update(cfg2, sp2)

	if len(cs.BeadStores()) != 2 {
		t.Errorf("updated stores = %d, want 2", len(cs.BeadStores()))
	}
	if cs.SessionProvider() != sp2 {
		t.Error("SessionProvider() not updated")
	}
	if cs.Config() != cfg2 {
		t.Error("Config() not updated")
	}
}

func TestControllerStateNilEventProvider(t *testing.T) {
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
	}

	cs := newControllerState(cfg, sp, nil, "test-city", t.TempDir())

	if cs.EventProvider() != nil {
		t.Error("EventProvider() should be nil when events disabled")
	}
}

// Verify controllerState satisfies the api.State interface at compile time.
// This uses a blank import check, not an explicit runtime assertion.
var _ interface {
	Config() *config.City
	SessionProvider() runtime.Provider
	BeadStore(string) beads.Store
	BeadStores() map[string]beads.Store
	EventProvider() events.Provider
	CityName() string
	CityPath() string
} = (*controllerState)(nil)

// Verify controllerState satisfies StateMutator at compile time.
var _ interface {
	SuspendAgent(string) error
	ResumeAgent(string) error
	KillAgent(string) error
	DrainAgent(string) error
	UndrainAgent(string) error
	NudgeAgent(string, string) error
	SuspendRig(string) error
	ResumeRig(string) error
} = (*controllerState)(nil)
