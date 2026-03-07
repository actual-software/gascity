package api

import (
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIdempotency_MissOnFirstRequest(t *testing.T) {
	c := newIdempotencyCache(30 * time.Minute)
	w := httptest.NewRecorder()
	handled := c.handleIdempotent(w, "key-1", "hash-abc")
	if handled {
		t.Error("handleIdempotent should return false on cache miss (key reserved)")
	}
}

func TestIdempotency_HitOnReplay(t *testing.T) {
	c := newIdempotencyCache(30 * time.Minute)

	// Reserve and complete.
	w := httptest.NewRecorder()
	c.handleIdempotent(w, "key-1", "hash-abc")
	c.storeResponse("key-1", "hash-abc", 201, map[string]string{"id": "bead-42"})

	// Replay with same key and hash.
	w2 := httptest.NewRecorder()
	handled := c.handleIdempotent(w2, "key-1", "hash-abc")
	if !handled {
		t.Fatal("handleIdempotent should return true on cache hit")
	}
	if w2.Code != 201 {
		t.Errorf("status = %d, want 201", w2.Code)
	}
	body := w2.Body.String()
	if !containsStr(body, "bead-42") {
		t.Errorf("body should contain cached response: %s", body)
	}
}

func TestIdempotency_MismatchReturns422(t *testing.T) {
	c := newIdempotencyCache(30 * time.Minute)
	w := httptest.NewRecorder()
	c.handleIdempotent(w, "key-1", "hash-abc")
	c.storeResponse("key-1", "hash-abc", 201, map[string]string{"id": "bead-42"})

	// Replay with same key but different hash.
	w2 := httptest.NewRecorder()
	handled := c.handleIdempotent(w2, "key-1", "hash-xyz")
	if !handled {
		t.Fatal("handleIdempotent should return true on mismatch")
	}
	if w2.Code != 422 {
		t.Errorf("status = %d, want 422", w2.Code)
	}
	body := w2.Body.String()
	if !containsStr(body, "idempotency_mismatch") {
		t.Errorf("body should contain idempotency_mismatch: %s", body)
	}
}

func TestIdempotency_PendingReturns409(t *testing.T) {
	c := newIdempotencyCache(30 * time.Minute)

	// First request reserves the key.
	w := httptest.NewRecorder()
	handled := c.handleIdempotent(w, "key-1", "hash-abc")
	if handled {
		t.Fatal("first request should reserve, not handle")
	}

	// Second request with same key while first is still in-flight.
	w2 := httptest.NewRecorder()
	handled = c.handleIdempotent(w2, "key-1", "hash-abc")
	if !handled {
		t.Fatal("second request should be handled (409)")
	}
	if w2.Code != 409 {
		t.Errorf("status = %d, want 409", w2.Code)
	}
	body := w2.Body.String()
	if !containsStr(body, "in_flight") {
		t.Errorf("body should contain in_flight: %s", body)
	}
}

func TestIdempotency_UnreserveAllowsRetry(t *testing.T) {
	c := newIdempotencyCache(30 * time.Minute)

	// Reserve.
	w := httptest.NewRecorder()
	c.handleIdempotent(w, "key-1", "hash-abc")

	// Unreserve (simulating a failed create).
	c.unreserve("key-1")

	// Retry should succeed (not get 409).
	w2 := httptest.NewRecorder()
	handled := c.handleIdempotent(w2, "key-1", "hash-abc")
	if handled {
		t.Error("after unreserve, key should be available for retry")
	}
}

func TestIdempotency_ExpiredEntryMisses(t *testing.T) {
	c := newIdempotencyCache(1 * time.Millisecond)
	w := httptest.NewRecorder()
	c.handleIdempotent(w, "key-1", "hash-abc")
	c.storeResponse("key-1", "hash-abc", 201, map[string]string{"id": "bead-42"})

	time.Sleep(5 * time.Millisecond)

	w2 := httptest.NewRecorder()
	handled := c.handleIdempotent(w2, "key-1", "hash-abc")
	if handled {
		t.Error("handleIdempotent should return false for expired entry")
	}
}

func TestIdempotency_EmptyKeySkips(t *testing.T) {
	c := newIdempotencyCache(30 * time.Minute)
	w := httptest.NewRecorder()
	handled := c.handleIdempotent(w, "", "hash-abc")
	if handled {
		t.Error("handleIdempotent should return false for empty key")
	}
}

func TestIdempotency_StoreResponseNoKey(t *testing.T) {
	c := newIdempotencyCache(30 * time.Minute)
	// Should be a no-op.
	c.storeResponse("", "hash-abc", 201, map[string]string{"id": "bead-42"})
	if len(c.entries) != 0 {
		t.Errorf("cache should be empty after storeResponse with empty key, got %d entries", len(c.entries))
	}
}

func TestIdempotency_ConcurrentSameKey(t *testing.T) {
	c := newIdempotencyCache(30 * time.Minute)
	const goroutines = 10
	var reserved atomic.Int32
	var wg sync.WaitGroup

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			if !c.handleIdempotent(w, "race-key", "hash-same") {
				reserved.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := reserved.Load(); got != 1 {
		t.Errorf("exactly 1 goroutine should reserve the key, got %d", got)
	}
}

func TestHashBody_Deterministic(t *testing.T) {
	body := map[string]string{"title": "test", "rig": "myrig"}
	h1 := hashBody(body)
	h2 := hashBody(body)
	if h1 != h2 {
		t.Errorf("hashBody should be deterministic: %q != %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Errorf("hashBody should return 64-char hex string, got %d chars", len(h1))
	}
}

func TestHashBody_DifferentInputs(t *testing.T) {
	h1 := hashBody(map[string]string{"title": "a"})
	h2 := hashBody(map[string]string{"title": "b"})
	if h1 == h2 {
		t.Error("hashBody should produce different hashes for different inputs")
	}
}

func TestScopedIdemKey_NamespacedByEndpoint(t *testing.T) {
	r1 := httptest.NewRequest("POST", "/v0/beads", nil)
	r2 := httptest.NewRequest("POST", "/v0/mail", nil)

	k1 := scopedIdemKey(r1, "abc")
	k2 := scopedIdemKey(r2, "abc")
	if k1 == k2 {
		t.Errorf("same Idempotency-Key on different endpoints should produce different scoped keys: %q == %q", k1, k2)
	}
	if k1 == "" {
		t.Error("scopedIdemKey should return non-empty for non-empty key")
	}
}

func TestScopedIdemKey_EmptyKey(t *testing.T) {
	r := httptest.NewRequest("POST", "/v0/beads", nil)
	if got := scopedIdemKey(r, ""); got != "" {
		t.Errorf("scopedIdemKey with empty key should return empty, got %q", got)
	}
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
