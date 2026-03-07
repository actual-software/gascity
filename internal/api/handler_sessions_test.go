package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

func newSessionFakeState(t *testing.T) *fakeState {
	t.Helper()
	fs := newFakeState(t)
	fs.cityBeadStore = beads.NewMemStore()
	return fs
}

func createTestSession(t *testing.T, store beads.Store, sp *runtime.Fake, title string) session.Info {
	t.Helper()
	mgr := session.NewManager(store, sp)
	info, err := mgr.Create(context.Background(), "default", title, "echo test", "/tmp", "test", nil, session.ProviderResume{}, runtime.Config{})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	return info
}

func TestHandleSessionList(t *testing.T) {
	fs := newSessionFakeState(t)
	srv := New(fs)

	// Create two sessions.
	createTestSession(t, fs.cityBeadStore, fs.sp, "Session A")
	createTestSession(t, fs.cityBeadStore, fs.sp, "Session B")

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v0/sessions", nil)
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d", w.Code, http.StatusOK)
	}

	var resp listResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 {
		t.Errorf("got total %d, want 2", resp.Total)
	}
}

func TestHandleSessionListFilterByState(t *testing.T) {
	fs := newSessionFakeState(t)
	srv := New(fs)

	info := createTestSession(t, fs.cityBeadStore, fs.sp, "To Suspend")
	createTestSession(t, fs.cityBeadStore, fs.sp, "Stay Active")

	// Suspend one.
	mgr := session.NewManager(fs.cityBeadStore, fs.sp)
	if err := mgr.Suspend(info.ID); err != nil {
		t.Fatalf("suspend: %v", err)
	}

	// List only active.
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v0/sessions?state=active", nil)
	srv.ServeHTTP(w, r)

	var resp listResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 {
		t.Errorf("got total %d, want 1 (only active)", resp.Total)
	}
}

func TestHandleSessionGet(t *testing.T) {
	fs := newSessionFakeState(t)
	srv := New(fs)

	info := createTestSession(t, fs.cityBeadStore, fs.sp, "My Session")

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v0/session/"+info.ID, nil)
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d", w.Code, http.StatusOK)
	}

	var resp sessionResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ID != info.ID {
		t.Errorf("got ID %q, want %q", resp.ID, info.ID)
	}
	if resp.Title != "My Session" {
		t.Errorf("got title %q, want %q", resp.Title, "My Session")
	}
	if resp.State != "active" {
		t.Errorf("got state %q, want %q", resp.State, "active")
	}
}

func TestHandleSessionGetNotFound(t *testing.T) {
	fs := newSessionFakeState(t)
	srv := New(fs)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v0/session/nonexistent", nil)
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("got status %d, want %d", w.Code, http.StatusNotFound)
	}
}

func TestHandleSessionSuspend(t *testing.T) {
	fs := newSessionFakeState(t)
	srv := New(fs)

	info := createTestSession(t, fs.cityBeadStore, fs.sp, "To Suspend")

	w := httptest.NewRecorder()
	r := newPostRequest("/v0/session/"+info.ID+"/suspend", nil)
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	// Verify the session is now suspended.
	mgr := session.NewManager(fs.cityBeadStore, fs.sp)
	got, err := mgr.Get(info.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.State != session.StateSuspended {
		t.Errorf("got state %q, want %q", got.State, session.StateSuspended)
	}
}

func TestHandleSessionClose(t *testing.T) {
	fs := newSessionFakeState(t)
	srv := New(fs)

	info := createTestSession(t, fs.cityBeadStore, fs.sp, "To Close")

	w := httptest.NewRecorder()
	r := newPostRequest("/v0/session/"+info.ID+"/close", nil)
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d; body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	// Session should no longer appear in default listing (excludes closed).
	mgr := session.NewManager(fs.cityBeadStore, fs.sp)
	sessions, err := mgr.List("", "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(sessions) != 0 {
		t.Errorf("got %d sessions after close, want 0", len(sessions))
	}
}

func TestHandleSessionNoCityStore(t *testing.T) {
	fs := newFakeState(t) // no cityBeadStore set
	srv := New(fs)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v0/sessions", nil)
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got status %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}
