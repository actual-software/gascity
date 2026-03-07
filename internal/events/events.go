// Package events provides tier-0 observability for Gas City.
//
// Events are infrastructure records of what happened (agent lifecycle,
// bead operations, controller state). The recorder writes JSON lines to
// .gc/events.jsonl; the reader scans them back. Recording is best-effort:
// errors are logged to stderr but never returned to callers.
//
// Agent observation data (messages, tool calls, thinking) is read directly
// from provider session logs via the sessionlog package, not the event bus.
package events

import (
	"context"
	"encoding/json"
	"time"
)

// Event type constants. Only types we actually emit today.
const (
	AgentStarted        = "agent.started"
	AgentStopped        = "agent.stopped"
	AgentCrashed        = "agent.crashed"
	BeadCreated         = "bead.created"
	BeadClosed          = "bead.closed"
	BeadUpdated         = "bead.updated"
	MailSent            = "mail.sent"
	MailRead            = "mail.read"
	MailArchived        = "mail.archived"
	MailMarkedRead      = "mail.marked_read"
	MailMarkedUnread    = "mail.marked_unread"
	MailReplied         = "mail.replied"
	MailDeleted         = "mail.deleted"
	AgentDraining       = "agent.draining"
	AgentUndrained      = "agent.undrained"
	AgentQuarantined    = "agent.quarantined"
	AgentIdleKilled     = "agent.idle_killed"
	AgentSuspended      = "agent.suspended"
	AgentUpdated        = "agent.updated"
	ConvoyCreated       = "convoy.created"
	ConvoyClosed        = "convoy.closed"
	ControllerStarted   = "controller.started"
	ControllerStopped   = "controller.stopped"
	CitySuspended       = "city.suspended"
	CityResumed         = "city.resumed"
	AutomationFired     = "automation.fired"
	AutomationCompleted = "automation.completed"
	AutomationFailed    = "automation.failed"
	ProviderSwapped     = "provider.swapped"
)

// Event is a single recorded occurrence in the system.
type Event struct {
	Seq     uint64          `json:"seq"`
	Type    string          `json:"type"`
	Ts      time.Time       `json:"ts"`
	Actor   string          `json:"actor"`
	Subject string          `json:"subject,omitempty"`
	Message string          `json:"message,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Recorder records events. Safe for concurrent use. Best-effort.
// This sub-interface is used by callers that only need to write events.
type Recorder interface {
	Record(e Event)
}

// Provider is the full interface for event backends. It embeds Recorder
// for writing and adds reading, querying, and watching. Implementations
// include FileRecorder (built-in JSONL file) and exec (user-supplied
// script via fork/exec).
type Provider interface {
	Recorder

	// List returns events matching the filter.
	List(filter Filter) ([]Event, error)

	// LatestSeq returns the highest sequence number, or 0 if empty.
	LatestSeq() (uint64, error)

	// Watch returns a Watcher that yields events with Seq > afterSeq.
	// The watcher blocks on Next() until an event arrives or ctx is
	// canceled. Callers must call Close() when done.
	Watch(ctx context.Context, afterSeq uint64) (Watcher, error)

	// Close releases any resources held by the provider.
	Close() error
}

// Watcher yields events one at a time. Created by [Provider.Watch].
// Callers must call Close() when done watching.
type Watcher interface {
	// Next blocks until the next event is available, the context is
	// canceled, or the watcher is closed. Returns the event or an error.
	// Implementations must unblock any in-flight Next call when Close
	// is called or the parent context is canceled.
	Next() (Event, error)

	// Close stops the watcher, unblocks any pending Next call, and
	// releases resources. Safe to call concurrently with Next.
	Close() error
}

// Discard silently drops all events.
var Discard Recorder = discardRecorder{}

type discardRecorder struct{}

func (discardRecorder) Record(Event) {}
