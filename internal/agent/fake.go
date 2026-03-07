package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
)

var _ Handle = (*Fake)(nil) // Fake satisfies Handle (already satisfies Agent)

// Call records a method invocation on [Fake].
type Call struct {
	Method  string // method name (e.g. "Start", "Stop", "SetMeta")
	Name    string // agent name at time of call
	Message string // only set for Nudge/SendKeys calls
	Lines   int    // only set for Peek calls
	Key     string // only set for meta calls
	Value   string // only set for SetMeta calls
}

// Fake is a test double for [Agent] with spy and configurable errors.
// Set the exported error fields to inject failures per-test.
// Safe for concurrent use.
type Fake struct {
	mu              sync.Mutex
	FakeName        string
	FakeSessionName string
	Running         bool
	Calls           []Call

	// FakeSessionConfig is returned by SessionConfig(). Set it per-test
	// to control the config fingerprint for reconciliation tests.
	FakeSessionConfig runtime.Config

	// FakePeekOutput is returned by Peek(). Set it per-test.
	FakePeekOutput string

	// FakeLastActivity is returned by GetLastActivity(). Set it per-test.
	FakeLastActivity time.Time

	// FakeIsAttached is returned by IsAttached(). Defaults to false.
	FakeIsAttached bool

	// FakeProcessAlive is returned by ProcessAlive(). Defaults to true.
	FakeProcessAlive *bool

	// Meta stores key-value metadata. Populated by SetMeta, queried by GetMeta.
	Meta map[string]string

	// StartDelay adds a sleep before Start returns, simulating slow startup
	// (e.g., Docker container readiness). Used to test parallel startup.
	StartDelay time.Duration

	// FakeEvents is returned by Events(). Set it per-test.
	FakeEvents chan Event

	// OnStop callbacks run after a successful Stop (mirrors managed.onStop).
	OnStop []func() error

	// Set these to inject errors per-test.
	StartErr     error
	StopErr      error
	AttachErr    error
	NudgeErr     error
	PeekErr      error
	InterruptErr error
	MetaErr      error
}

// NewFake returns a ready-to-use [Fake] with the given identity.
func NewFake(name, sessionName string) *Fake {
	return &Fake{FakeName: name, FakeSessionName: sessionName}
}

// NewFakeHandle returns a Fake typed as Handle. Convenience for tests
// that accept Handle parameters.
func NewFakeHandle(name, sessionName string) Handle {
	return NewFake(name, sessionName)
}

// Name records the call and returns FakeName.
func (f *Fake) Name() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "Name", Name: f.FakeName})
	return f.FakeName
}

// SessionName records the call and returns FakeSessionName.
func (f *Fake) SessionName() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "SessionName", Name: f.FakeName})
	return f.FakeSessionName
}

// IsRunning records the call and returns the Running field.
func (f *Fake) IsRunning() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "IsRunning", Name: f.FakeName})
	return f.Running
}

// IsAttached records the call and returns FakeIsAttached.
func (f *Fake) IsAttached() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "IsAttached", Name: f.FakeName})
	return f.FakeIsAttached
}

// Start records the call. Sleeps for StartDelay if set, respecting
// context cancellation. Returns StartErr if set; otherwise sets Running=true.
func (f *Fake) Start(ctx context.Context) error {
	f.mu.Lock()
	delay := f.StartDelay
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "Start", Name: f.FakeName})
	if f.StartErr != nil {
		return f.StartErr
	}
	f.Running = true
	return nil
}

// Stop records the call. Returns StopErr if set; otherwise sets Running=false
// and runs OnStop callbacks (best-effort).
func (f *Fake) Stop() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "Stop", Name: f.FakeName})
	if f.StopErr != nil {
		return f.StopErr
	}
	f.Running = false
	for _, fn := range f.OnStop {
		_ = fn()
	}
	return nil
}

// Attach records the call and returns AttachErr (nil if not set).
func (f *Fake) Attach() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "Attach", Name: f.FakeName})
	return f.AttachErr
}

// Nudge records the call and returns NudgeErr (nil if not set).
func (f *Fake) Nudge(message string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "Nudge", Name: f.FakeName, Message: message})
	return f.NudgeErr
}

// Peek records the call and returns FakePeekOutput or PeekErr.
func (f *Fake) Peek(lines int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "Peek", Name: f.FakeName, Lines: lines})
	if f.PeekErr != nil {
		return "", f.PeekErr
	}
	return f.FakePeekOutput, nil
}

// SessionConfig records the call and returns FakeSessionConfig.
func (f *Fake) SessionConfig() runtime.Config {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "SessionConfig", Name: f.FakeName})
	return f.FakeSessionConfig
}

// Interrupt records the call and returns InterruptErr (nil if not set).
func (f *Fake) Interrupt() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "Interrupt", Name: f.FakeName})
	return f.InterruptErr
}

// ProcessAlive records the call and returns FakeProcessAlive (true by default).
func (f *Fake) ProcessAlive() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "ProcessAlive", Name: f.FakeName})
	if f.FakeProcessAlive != nil {
		return *f.FakeProcessAlive
	}
	return true
}

// ClearScrollback records the call and returns nil.
func (f *Fake) ClearScrollback() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "ClearScrollback", Name: f.FakeName})
	return nil
}

// GetLastActivity records the call and returns FakeLastActivity.
func (f *Fake) GetLastActivity() (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "GetLastActivity", Name: f.FakeName})
	return f.FakeLastActivity, nil
}

// SendKeys records the call and returns nil.
func (f *Fake) SendKeys(keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "SendKeys", Name: f.FakeName, Message: fmt.Sprintf("%v", keys)})
	return nil
}

// RunLive records the call and returns nil.
func (f *Fake) RunLive(_ runtime.Config) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "RunLive", Name: f.FakeName})
	return nil
}

// SetMeta records the call and stores the key-value pair.
func (f *Fake) SetMeta(key, value string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "SetMeta", Name: f.FakeName, Key: key, Value: value})
	if f.MetaErr != nil {
		return f.MetaErr
	}
	if f.Meta == nil {
		f.Meta = make(map[string]string)
	}
	f.Meta[key] = value
	return nil
}

// GetMeta records the call and returns the stored value.
func (f *Fake) GetMeta(key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "GetMeta", Name: f.FakeName, Key: key})
	if f.MetaErr != nil {
		return "", f.MetaErr
	}
	return f.Meta[key], nil
}

// RemoveMeta records the call and removes the key.
func (f *Fake) RemoveMeta(key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "RemoveMeta", Name: f.FakeName, Key: key})
	if f.MetaErr != nil {
		return f.MetaErr
	}
	delete(f.Meta, key)
	return nil
}

// Events returns FakeEvents (nil by default).
func (f *Fake) Events() <-chan Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.FakeEvents
}

// SetObserver records the call. The observer is not stored — use
// FakeEvents directly to control event delivery in tests.
func (f *Fake) SetObserver(_ ObservationStrategy) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Method: "SetObserver", Name: f.FakeName})
}
