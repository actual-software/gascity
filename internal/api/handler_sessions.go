package api

import (
	"net/http"
	"time"

	"github.com/gastownhall/gascity/internal/session"
)

// sessionResponse is the JSON representation of a chat session.
type sessionResponse struct {
	ID          string `json:"id"`
	Template    string `json:"template"`
	State       string `json:"state"`
	Title       string `json:"title"`
	Provider    string `json:"provider"`
	SessionName string `json:"session_name"`
	CreatedAt   string `json:"created_at"`
	LastActive  string `json:"last_active,omitempty"`
	Attached    bool   `json:"attached"`
}

func sessionToResponse(info session.Info) sessionResponse {
	r := sessionResponse{
		ID:          info.ID,
		Template:    info.Template,
		State:       string(info.State),
		Title:       info.Title,
		Provider:    info.Provider,
		SessionName: info.SessionName,
		CreatedAt:   info.CreatedAt.Format(time.RFC3339),
		Attached:    info.Attached,
	}
	if !info.LastActive.IsZero() {
		r.LastActive = info.LastActive.Format(time.RFC3339)
	}
	return r
}

func (s *Server) handleSessionList(w http.ResponseWriter, r *http.Request) {
	store := s.state.CityBeadStore()
	if store == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "no bead store configured")
		return
	}
	sp := s.state.SessionProvider()
	mgr := session.NewManager(store, sp)

	q := r.URL.Query()
	stateFilter := q.Get("state")
	templateFilter := q.Get("template")

	sessions, err := mgr.List(stateFilter, templateFilter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}

	items := make([]sessionResponse, len(sessions))
	for i, sess := range sessions {
		items[i] = sessionToResponse(sess)
	}
	writeJSON(w, http.StatusOK, listResponse{Items: items, Total: len(items)})
}

func (s *Server) handleSessionGet(w http.ResponseWriter, r *http.Request) {
	store := s.state.CityBeadStore()
	if store == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "no bead store configured")
		return
	}
	sp := s.state.SessionProvider()
	mgr := session.NewManager(store, sp)

	id := r.PathValue("id")
	info, err := mgr.Get(id)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sessionToResponse(info))
}

func (s *Server) handleSessionSuspend(w http.ResponseWriter, r *http.Request) {
	store := s.state.CityBeadStore()
	if store == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "no bead store configured")
		return
	}
	sp := s.state.SessionProvider()
	mgr := session.NewManager(store, sp)

	id := r.PathValue("id")
	if err := mgr.Suspend(id); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleSessionClose(w http.ResponseWriter, r *http.Request) {
	store := s.state.CityBeadStore()
	if store == nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "no bead store configured")
		return
	}
	sp := s.state.SessionProvider()
	mgr := session.NewManager(store, sp)

	id := r.PathValue("id")
	if err := mgr.Close(id); err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
