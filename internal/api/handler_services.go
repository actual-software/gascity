package api

import "net/http"

func (s *Server) handleServiceList(w http.ResponseWriter, _ *http.Request) {
	reg := s.state.ServiceRegistry()
	if reg == nil {
		writeListJSON(w, s.latestIndex(), []any{}, 0)
		return
	}
	items := reg.List()
	writeListJSON(w, s.latestIndex(), items, len(items))
}

func (s *Server) handleServiceGet(w http.ResponseWriter, r *http.Request) {
	reg := s.state.ServiceRegistry()
	if reg == nil {
		writeError(w, http.StatusNotFound, "not_found", "service "+r.PathValue("name")+" not found")
		return
	}
	item, ok := reg.Get(r.PathValue("name"))
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "service "+r.PathValue("name")+" not found")
		return
	}
	writeIndexJSON(w, s.latestIndex(), item)
}

func (s *Server) handleServiceProxy(w http.ResponseWriter, r *http.Request) {
	reg := s.state.ServiceRegistry()
	if reg == nil || !reg.ServeHTTP(w, r) {
		writeError(w, http.StatusNotFound, "not_found", "service route not found")
	}
}
