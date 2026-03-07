package api

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"runtime/debug"
	"time"
)

// withLogging wraps a handler with request logging.
func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		log.Printf("api: %s %s %d %s", r.Method, r.URL.Path, rw.status, time.Since(start).Round(time.Microsecond))
	})
}

// withRecovery catches panics and returns 500.
func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("api: panic: %v\n%s", err, debug.Stack())
				writeError(w, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// withCORS adds restricted CORS headers for localhost dashboard access.
// Only allows localhost origins to prevent browser-origin attacks on mutation endpoints.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if isLocalhostOrigin(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Last-Event-ID, X-GC-Request")
			w.Header().Set("Access-Control-Expose-Headers", "X-GC-Index, X-GC-Request-Id")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isMutationMethod returns true for HTTP methods that modify state.
func isMutationMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// withReadOnly rejects all mutation requests. Used when the API server binds
// to a non-localhost address where mutations would be unauthenticated.
func withReadOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isMutationMethod(r.Method) {
			writeError(w, http.StatusForbidden, "read_only", "mutations disabled: server bound to non-localhost address")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// withCSRFCheck requires a custom X-GC-Request header on mutation requests.
// Custom headers trigger CORS preflight, preventing simple cross-origin form submissions.
func withCSRFCheck(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isMutationMethod(r.Method) && r.Header.Get("X-GC-Request") == "" {
			writeError(w, http.StatusForbidden, "csrf", "X-GC-Request header required on mutation endpoints")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isLocalhostOrigin checks if an origin is from localhost/127.0.0.1.
// Rejects origins like http://localhost.evil.com by requiring the host
// to be exactly localhost, 127.0.0.1, or [::1] with an optional port.
func isLocalhostOrigin(origin string) bool {
	if origin == "" {
		return false
	}
	// Match http://localhost, http://localhost:PORT
	for _, base := range []string{
		"http://localhost",
		"http://127.0.0.1",
		"http://[::1]",
		"https://localhost",
		"https://127.0.0.1",
		"https://[::1]",
	} {
		if origin == base {
			return true
		}
		// Must be base + ":" + port (no other suffixes like ".evil.com")
		if len(origin) > len(base) && origin[:len(base)] == base && origin[len(base)] == ':' {
			return true
		}
	}
	return false
}

// withRequestID adds a unique X-GC-Request-Id header to every response.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf [8]byte
		rand.Read(buf[:]) //nolint:errcheck
		w.Header().Set("X-GC-Request-Id", hex.EncodeToString(buf[:]))
		next.ServeHTTP(w, r)
	})
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// Unwrap supports http.ResponseController and http.Flusher detection.
func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}
