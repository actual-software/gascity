package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// idempotencyCache stores responses keyed by Idempotency-Key header values.
// Used on create endpoints so clients can safely retry after network failures.
//
// The cache uses a two-phase protocol to prevent TOCTOU races:
//  1. reserve(key, hash) atomically inserts a pending entry if absent
//  2. complete(key, ...) fills in the response body once the create succeeds
//
// Concurrent requests with the same key: the first reserves, others see the
// pending entry and get a 409 Conflict response.
type idempotencyCache struct {
	mu      sync.Mutex
	entries map[string]cachedEntry
	ttl     time.Duration
}

type cachedEntry struct {
	pending    bool // true while the create is in-flight
	statusCode int
	body       []byte
	bodyHash   string
	expiresAt  time.Time
}

func newIdempotencyCache(ttl time.Duration) *idempotencyCache {
	return &idempotencyCache{
		entries: make(map[string]cachedEntry),
		ttl:     ttl,
	}
}

// reserve atomically reserves a key for processing. Returns:
//   - (entry, true) if the key already exists (completed or pending)
//   - (zero, false) if the key was successfully reserved for this caller
func (c *idempotencyCache) reserve(key, bodyHash string) (cachedEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if ok {
		if time.Now().After(entry.expiresAt) {
			delete(c.entries, key)
			// Fall through to reserve.
		} else {
			return entry, true
		}
	}
	// Reserve the key with a pending entry.
	c.entries[key] = cachedEntry{
		pending:   true,
		bodyHash:  bodyHash,
		expiresAt: time.Now().Add(c.ttl),
	}
	// Lazy cleanup when cache grows large.
	if len(c.entries) > 1000 {
		now := time.Now()
		for k, v := range c.entries {
			if now.After(v.expiresAt) {
				delete(c.entries, k)
			}
		}
	}
	return cachedEntry{}, false
}

// complete fills in the response for a previously reserved key.
func (c *idempotencyCache) complete(key string, statusCode int, body []byte, bodyHash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cachedEntry{
		pending:    false,
		statusCode: statusCode,
		body:       body,
		bodyHash:   bodyHash,
		expiresAt:  time.Now().Add(c.ttl),
	}
}

// unreserve removes a pending reservation on failure (so the key can be retried).
func (c *idempotencyCache) unreserve(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok && entry.pending {
		delete(c.entries, key)
	}
}

// handleIdempotent checks for a cached or in-flight entry matching the given
// Idempotency-Key and body hash. Returns true if it handled the response
// (replayed cached, wrote 409 for in-flight, or wrote 422 for mismatch).
// Returns false if the caller should proceed with normal processing (key
// was atomically reserved for this caller).
func (c *idempotencyCache) handleIdempotent(w http.ResponseWriter, key, bodyHash string) bool {
	if key == "" {
		return false
	}
	existing, found := c.reserve(key, bodyHash)
	if !found {
		// Key reserved for us — proceed with create.
		return false
	}
	// Key already exists.
	if existing.bodyHash != bodyHash {
		writeError(w, http.StatusUnprocessableEntity, "idempotency_mismatch",
			"Idempotency-Key reused with different request body")
		return true
	}
	if existing.pending {
		// Another request is still processing this key.
		writeError(w, http.StatusConflict, "in_flight",
			"request with this Idempotency-Key is already in progress")
		return true
	}
	// Replay cached response.
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(existing.statusCode)
	w.Write(existing.body) //nolint:errcheck // best-effort
	return true
}

// storeResponse caches the JSON-serialized response for later replay.
//
//nolint:unparam // statusCode is 201 today but the cache is status-agnostic by design
func (c *idempotencyCache) storeResponse(key, bodyHash string, statusCode int, v any) {
	if key == "" {
		return
	}
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	// Append newline to match json.Encoder.Encode behavior.
	data = append(data, '\n')
	c.complete(key, statusCode, data, bodyHash)
}

// scopedIdemKey returns an idempotency cache key namespaced by HTTP method
// and path, preventing cross-endpoint collisions when clients reuse the same
// Idempotency-Key value across different endpoints.
func scopedIdemKey(r *http.Request, key string) string {
	if key == "" {
		return ""
	}
	return r.Method + ":" + r.URL.Path + ":" + key
}

// hashBody returns a hex-encoded SHA-256 hash of the JSON-marshaled body.
func hashBody(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
