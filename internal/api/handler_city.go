package api

import (
	"net/http"
	"strings"
	"time"
)

type cityGetResponse struct {
	Name            string `json:"name"`
	Path            string `json:"path"`
	Version         string `json:"version,omitempty"`
	Suspended       bool   `json:"suspended"`
	Provider        string `json:"provider,omitempty"`
	SessionTemplate string `json:"session_template,omitempty"`
	UptimeSec       int    `json:"uptime_sec"`
	AgentCount      int    `json:"agent_count"`
	RigCount        int    `json:"rig_count"`
}

func (s *Server) handleCityGet(w http.ResponseWriter, _ *http.Request) {
	cfg := s.state.Config()
	resp := cityGetResponse{
		Name:            s.state.CityName(),
		Path:            s.state.CityPath(),
		Version:         s.state.Version(),
		Suspended:       cfg.Workspace.Suspended,
		Provider:        cfg.Workspace.Provider,
		SessionTemplate: cfg.Workspace.SessionTemplate,
		UptimeSec:       int(time.Since(s.state.StartedAt()).Seconds()),
		AgentCount:      len(cfg.Agents),
		RigCount:        len(cfg.Rigs),
	}
	writeJSON(w, http.StatusOK, resp)
}

// cityPatchRequest is the JSON body for PATCH /v0/city.
type cityPatchRequest struct {
	Suspended *bool `json:"suspended,omitempty"`
}

func (s *Server) handleCityPatch(w http.ResponseWriter, r *http.Request) {
	sm, ok := s.state.(StateMutator)
	if !ok {
		writeError(w, http.StatusNotImplemented, "internal", "mutations not supported")
		return
	}

	var body cityPatchRequest
	if err := decodeBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}

	if body.Suspended == nil {
		writeError(w, http.StatusBadRequest, "invalid", "no fields to update")
		return
	}

	var err error
	if *body.Suspended {
		err = sm.SuspendCity()
	} else {
		err = sm.ResumeCity()
	}
	if err != nil {
		if strings.Contains(err.Error(), "validating") {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
