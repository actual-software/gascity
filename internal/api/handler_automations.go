package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gastownhall/gascity/internal/automations"
)

type automationResponse struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Type        string `json:"type"`
	Gate        string `json:"gate"`
	Interval    string `json:"interval,omitempty"`
	Schedule    string `json:"schedule,omitempty"`
	Check       string `json:"check,omitempty"`
	On          string `json:"on,omitempty"`
	Formula     string `json:"formula,omitempty"`
	Exec        string `json:"exec,omitempty"`
	Pool        string `json:"pool,omitempty"`
	Timeout     string `json:"timeout,omitempty"`
	Enabled     bool   `json:"enabled"`
	Rig         string `json:"rig,omitempty"`
}

func (s *Server) handleAutomationList(w http.ResponseWriter, _ *http.Request) {
	aa := s.state.Automations()
	resp := make([]automationResponse, len(aa))
	for i, a := range aa {
		resp[i] = toAutomationResponse(a)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"automations": resp,
	})
}

func (s *Server) handleAutomationGet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	a, err := resolveAutomation(s.state.Automations(), name)
	if err != nil {
		if strings.Contains(err.Error(), "ambiguous") {
			writeError(w, http.StatusConflict, "ambiguous", err.Error())
		} else {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, toAutomationResponse(*a))
}

func (s *Server) handleAutomationEnable(w http.ResponseWriter, r *http.Request) {
	s.setAutomationEnabled(w, r, true)
}

func (s *Server) handleAutomationDisable(w http.ResponseWriter, r *http.Request) {
	s.setAutomationEnabled(w, r, false)
}

func (s *Server) setAutomationEnabled(w http.ResponseWriter, r *http.Request, enabled bool) {
	sm, ok := s.state.(StateMutator)
	if !ok {
		writeError(w, http.StatusNotImplemented, "internal", "mutations not supported")
		return
	}

	name := r.PathValue("name")

	// Resolve name and rig from the automation list.
	a, err := resolveAutomation(s.state.Automations(), name)
	if err != nil {
		if strings.Contains(err.Error(), "ambiguous") {
			writeError(w, http.StatusConflict, "ambiguous", err.Error())
		} else {
			writeError(w, http.StatusNotFound, "not_found", err.Error())
		}
		return
	}
	autoName := a.Name
	autoRig := a.Rig

	if enabled {
		err = sm.EnableAutomation(autoName, autoRig)
	} else {
		err = sm.DisableAutomation(autoName, autoRig)
	}
	if err != nil {
		if strings.Contains(err.Error(), "validating") {
			writeError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}

	action := "enabled"
	if !enabled {
		action = "disabled"
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": action, "automation": autoName})
}

// resolveAutomation finds an automation by name or scoped name. If a bare
// name matches multiple automations across rigs, it returns an error
// requiring the caller to use the scoped name instead.
func resolveAutomation(aa []automations.Automation, name string) (*automations.Automation, error) {
	// Scoped name is always unambiguous — try it first.
	for i, a := range aa {
		if a.ScopedName() == name {
			return &aa[i], nil
		}
	}
	// Bare name match — collect all matches to detect ambiguity.
	var matches []int
	for i, a := range aa {
		if a.Name == name {
			matches = append(matches, i)
		}
	}
	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("automation %s not found", name)
	case 1:
		return &aa[matches[0]], nil
	default:
		var scoped []string
		for _, idx := range matches {
			scoped = append(scoped, aa[idx].ScopedName())
		}
		return nil, fmt.Errorf("ambiguous automation name %q; use scoped name: %s", name, strings.Join(scoped, ", "))
	}
}

func toAutomationResponse(a automations.Automation) automationResponse {
	typ := "formula"
	if a.IsExec() {
		typ = "exec"
	}
	return automationResponse{
		Name:        a.Name,
		Description: a.Description,
		Type:        typ,
		Gate:        a.Gate,
		Interval:    a.Interval,
		Schedule:    a.Schedule,
		Check:       a.Check,
		On:          a.On,
		Formula:     a.Formula,
		Exec:        a.Exec,
		Pool:        a.Pool,
		Timeout:     a.Timeout,
		Enabled:     a.IsEnabled(),
		Rig:         a.Rig,
	}
}
