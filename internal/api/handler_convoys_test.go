package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

func TestConvoyCreateAndGet(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	// Create a bead to link as convoy item.
	store := state.stores["myrig"]
	item, err := store.Create(beads.Bead{Title: "task-1"})
	if err != nil {
		t.Fatalf("create item: %v", err)
	}

	// Create convoy with the item.
	body := `{"rig":"myrig","title":"test convoy","items":["` + item.ID + `"]}`
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, newPostRequest("/v0/convoys", strings.NewReader(body)))

	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d, want 201; body = %s", rec.Code, rec.Body.String())
	}

	var convoy beads.Bead
	if err := json.NewDecoder(rec.Body).Decode(&convoy); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if convoy.Type != "convoy" {
		t.Fatalf("type = %q, want %q", convoy.Type, "convoy")
	}

	// Get convoy.
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v0/convoy/"+convoy.ID, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("get: status = %d, want 200", rec.Code)
	}
}

func TestConvoyCreateInvalidItem(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	body := `{"rig":"myrig","title":"test","items":["nonexistent"]}`
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, newPostRequest("/v0/convoys", strings.NewReader(body)))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body = %s", rec.Code, rec.Body.String())
	}
}

func TestConvoyAddItems(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	store := state.stores["myrig"]
	convoy, _ := store.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	item, _ := store.Create(beads.Bead{Title: "task"})

	body := `{"items":["` + item.ID + `"]}`
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, newPostRequest("/v0/convoy/"+convoy.ID+"/add", strings.NewReader(body)))

	if rec.Code != http.StatusOK {
		t.Fatalf("add: status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
}

func TestConvoyClose(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	store := state.stores["myrig"]
	convoy, _ := store.Create(beads.Bead{Title: "convoy", Type: "convoy"})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, newPostRequest("/v0/convoy/"+convoy.ID+"/close", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("close: status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
}

func TestConvoyNotFound(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v0/convoy/nonexistent", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestConvoyRemoveItems(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	store := state.stores["myrig"]
	convoy, _ := store.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	item, _ := store.Create(beads.Bead{Title: "task"})

	// Add item to convoy.
	pid := convoy.ID
	store.Update(item.ID, beads.UpdateOpts{ParentID: &pid}) //nolint:errcheck

	// Remove item from convoy.
	body := `{"items":["` + item.ID + `"]}`
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, newPostRequest("/v0/convoy/"+convoy.ID+"/remove", strings.NewReader(body)))

	if rec.Code != http.StatusOK {
		t.Fatalf("remove: status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	// Verify item is unlinked.
	got, _ := store.Get(item.ID)
	if got.ParentID != "" {
		t.Errorf("ParentID = %q, want empty", got.ParentID)
	}
}

func TestConvoyRemoveNonMember(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	store := state.stores["myrig"]
	convoy, _ := store.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	item, _ := store.Create(beads.Bead{Title: "unrelated task"})

	// Item is not linked to this convoy — remove should fail.
	body := `{"items":["` + item.ID + `"]}`
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, newPostRequest("/v0/convoy/"+convoy.ID+"/remove", strings.NewReader(body)))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("remove non-member: status = %d, want 400; body = %s", rec.Code, rec.Body.String())
	}
}

func TestConvoyCheck(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	store := state.stores["myrig"]
	convoy, _ := store.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	item1, _ := store.Create(beads.Bead{Title: "task1"})
	item2, _ := store.Create(beads.Bead{Title: "task2"})

	pid := convoy.ID
	store.Update(item1.ID, beads.UpdateOpts{ParentID: &pid}) //nolint:errcheck
	store.Update(item2.ID, beads.UpdateOpts{ParentID: &pid}) //nolint:errcheck
	store.Close(item1.ID)                                    //nolint:errcheck

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v0/convoy/"+convoy.ID+"/check", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("check: status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	json.NewDecoder(rec.Body).Decode(&resp) //nolint:errcheck
	if resp["total"] != float64(2) {
		t.Errorf("total = %v, want 2", resp["total"])
	}
	if resp["closed"] != float64(1) {
		t.Errorf("closed = %v, want 1", resp["closed"])
	}
	if resp["complete"] != false {
		t.Errorf("complete = %v, want false", resp["complete"])
	}
}

func TestConvoyCheckComplete(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	store := state.stores["myrig"]
	convoy, _ := store.Create(beads.Bead{Title: "convoy", Type: "convoy"})
	item, _ := store.Create(beads.Bead{Title: "task"})

	pid := convoy.ID
	store.Update(item.ID, beads.UpdateOpts{ParentID: &pid}) //nolint:errcheck
	store.Close(item.ID)                                    //nolint:errcheck

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v0/convoy/"+convoy.ID+"/check", nil))

	var resp map[string]any
	json.NewDecoder(rec.Body).Decode(&resp) //nolint:errcheck
	if resp["complete"] != true {
		t.Errorf("complete = %v, want true", resp["complete"])
	}
}

func TestConvoyDelete(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	store := state.stores["myrig"]
	convoy, _ := store.Create(beads.Bead{Title: "convoy", Type: "convoy"})

	req := httptest.NewRequest("DELETE", "/v0/convoy/"+convoy.ID, nil)
	req.Header.Set("X-GC-Request", "true")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("delete: status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}

	// Verify closed.
	got, _ := store.Get(convoy.ID)
	if got.Status != "closed" {
		t.Errorf("Status = %q, want %q", got.Status, "closed")
	}
}

func TestConvoyDeleteNotConvoy(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	store := state.stores["myrig"]
	task, _ := store.Create(beads.Bead{Title: "task", Type: "task"})

	req := httptest.NewRequest("DELETE", "/v0/convoy/"+task.ID, nil)
	req.Header.Set("X-GC-Request", "true")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestConvoyList(t *testing.T) {
	state := newFakeMutatorState(t)
	srv := New(state)

	store := state.stores["myrig"]
	if _, err := store.Create(beads.Bead{Title: "convoy", Type: "convoy"}); err != nil {
		t.Fatalf("create convoy: %v", err)
	}
	if _, err := store.Create(beads.Bead{Title: "task", Type: "task"}); err != nil {
		t.Fatalf("create task: %v", err)
	}

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/v0/convoys", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp listResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 {
		t.Fatalf("total = %d, want 1 (only convoys)", resp.Total)
	}
}
