package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/config"
)

type agentResponse struct {
	Name       string       `json:"name"`
	Running    bool         `json:"running"`
	Suspended  bool         `json:"suspended"`
	Rig        string       `json:"rig,omitempty"`
	Pool       string       `json:"pool,omitempty"`
	Session    *sessionInfo `json:"session,omitempty"`
	ActiveBead string       `json:"active_bead,omitempty"`
}

type sessionInfo struct {
	Name         string     `json:"name"`
	LastActivity *time.Time `json:"last_activity,omitempty"`
	Attached     bool       `json:"attached"`
}

func (s *Server) handleAgentList(w http.ResponseWriter, r *http.Request) {
	bp := parseBlockingParams(r)
	if bp.isBlocking() {
		waitForChange(r.Context(), s.state.EventProvider(), bp)
	}

	cfg := s.state.Config()
	sp := s.state.SessionProvider()
	cityName := s.state.CityName()
	sessTmpl := cfg.Workspace.SessionTemplate

	// Query filters.
	qPool := r.URL.Query().Get("pool")
	qRig := r.URL.Query().Get("rig")
	qRunning := r.URL.Query().Get("running")

	var agents []agentResponse
	for _, a := range cfg.Agents {
		expanded := expandAgent(a, cityName, sessTmpl, sp)
		for _, ea := range expanded {
			// Apply filters.
			if qRig != "" && ea.rig != qRig {
				continue
			}
			if qPool != "" && ea.pool != qPool {
				continue
			}

			sessionName := agentSessionName(cityName, ea.qualifiedName, sessTmpl)
			running := sp.IsRunning(sessionName)

			if qRunning == "true" && !running {
				continue
			}
			if qRunning == "false" && running {
				continue
			}

			// Merge config + runtime suspended state.
			suspended := ea.suspended
			if v, err := sp.GetMeta(sessionName, "suspended"); err == nil && v == "true" {
				suspended = true
			}

			resp := agentResponse{
				Name:      ea.qualifiedName,
				Running:   running,
				Suspended: suspended,
				Rig:       ea.rig,
				Pool:      ea.pool,
			}

			if running {
				si := &sessionInfo{Name: sessionName}
				if t, err := sp.GetLastActivity(sessionName); err == nil && !t.IsZero() {
					si.LastActivity = &t
				}
				si.Attached = sp.IsAttached(sessionName)
				resp.Session = si
			}

			// Find active bead by querying bead stores.
			resp.ActiveBead = s.findActiveBead(ea.qualifiedName, ea.rig)

			agents = append(agents, resp)
		}
	}

	if agents == nil {
		agents = []agentResponse{}
	}
	writeListJSON(w, s.latestIndex(), agents, len(agents))
}

func (s *Server) handleAgent(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "invalid", "agent name required")
		return
	}

	cfg := s.state.Config()
	sp := s.state.SessionProvider()
	cityName := s.state.CityName()

	// Try exact agent match first, then check for sub-resource suffix.
	// This prevents agent names ending in "/peek" from being misrouted.
	agentCfg, ok := findAgent(cfg, name)
	if !ok {
		// Not found as exact agent — check for sub-resource suffixes.
		if after, found := strings.CutSuffix(name, "/peek"); found {
			s.handleAgentPeek(w, r, after)
			return
		}
		if after, found := strings.CutSuffix(name, "/logs"); found {
			s.handleAgentLogs(w, r, after)
			return
		}
		writeError(w, http.StatusNotFound, "not_found", "agent "+name+" not found")
		return
	}

	sessionName := agentSessionName(cityName, name, cfg.Workspace.SessionTemplate)
	running := sp.IsRunning(sessionName)

	// Merge config + runtime suspended state.
	suspended := agentCfg.Suspended
	if v, err := sp.GetMeta(sessionName, "suspended"); err == nil && v == "true" {
		suspended = true
	}

	resp := agentResponse{
		Name:      name,
		Running:   running,
		Suspended: suspended,
		Rig:       agentCfg.Dir,
	}
	if agentCfg.IsPool() {
		resp.Pool = agentCfg.QualifiedName()
	}

	if running {
		si := &sessionInfo{Name: sessionName}
		if t, err := sp.GetLastActivity(sessionName); err == nil && !t.IsZero() {
			si.LastActivity = &t
		}
		si.Attached = sp.IsAttached(sessionName)
		resp.Session = si
	}

	// Find active bead by querying bead stores.
	resp.ActiveBead = s.findActiveBead(name, agentCfg.Dir)

	writeIndexJSON(w, s.latestIndex(), resp)
}

func (s *Server) handleAgentPeek(w http.ResponseWriter, _ *http.Request, name string) {
	sp := s.state.SessionProvider()
	cfg := s.state.Config()
	sessionName := agentSessionName(s.state.CityName(), name, cfg.Workspace.SessionTemplate)

	if !sp.IsRunning(sessionName) {
		writeError(w, http.StatusNotFound, "not_found", "agent "+name+" not running")
		return
	}

	output, err := sp.Peek(sessionName, 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"output": output})
}

func (s *Server) handleAgentAction(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	sm, ok := s.state.(StateMutator)
	if !ok {
		writeError(w, http.StatusNotImplemented, "internal", "mutations not supported")
		return
	}

	// Parse action suffix before validating agent name.
	var action string
	if after, found := strings.CutSuffix(name, "/suspend"); found {
		name = after
		action = "suspend"
	} else if after, found := strings.CutSuffix(name, "/resume"); found {
		name = after
		action = "resume"
	} else if after, found := strings.CutSuffix(name, "/kill"); found {
		name = after
		action = "kill"
	} else if after, found := strings.CutSuffix(name, "/drain"); found {
		name = after
		action = "drain"
	} else if after, found := strings.CutSuffix(name, "/undrain"); found {
		name = after
		action = "undrain"
	} else if after, found := strings.CutSuffix(name, "/nudge"); found {
		name = after
		action = "nudge"
	} else {
		writeError(w, http.StatusNotFound, "not_found", "unknown agent action")
		return
	}

	// Validate agent exists in config.
	cfg := s.state.Config()
	agentCfg, ok := findAgent(cfg, name)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "agent "+name+" not found")
		return
	}

	// Reject mutations on the pool parent when max > 1.
	// Runtime sessions are pool-1, pool-2, etc. — mutating the parent is a no-op.
	if agentCfg.IsPool() {
		pool := agentCfg.EffectivePool()
		if pool.Max > 1 && name == agentCfg.QualifiedName() {
			writeError(w, http.StatusBadRequest, "invalid",
				"cannot mutate pool parent "+name+"; target a specific member (e.g. "+name+"-1)")
			return
		}
	}

	var err error
	switch action {
	case "suspend":
		err = sm.SuspendAgent(name)
	case "resume":
		err = sm.ResumeAgent(name)
	case "kill":
		err = sm.KillAgent(name)
	case "drain":
		err = sm.DrainAgent(name)
	case "undrain":
		err = sm.UndrainAgent(name)
	case "nudge":
		var body struct {
			Message string `json:"message"`
		}
		if decErr := decodeBody(r, &body); decErr != nil {
			writeError(w, http.StatusBadRequest, "invalid", decErr.Error())
			return
		}
		err = sm.NudgeAgent(name, body.Message)
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// expandedAgent holds a single (possibly pool-expanded) agent identity.
type expandedAgent struct {
	qualifiedName string
	rig           string
	pool          string
	suspended     bool
}

// expandAgent expands a config.Agent into its effective runtime agents.
// For bounded pool agents, this generates pool-1..pool-max members.
// For unlimited pools (max < 0), it discovers running instances via session
// provider prefix matching — the same approach as discoverPoolInstances.
func expandAgent(a config.Agent, cityName, sessTmpl string, sp sessionLister) []expandedAgent {
	if !a.IsPool() {
		return []expandedAgent{{
			qualifiedName: a.QualifiedName(),
			rig:           a.Dir,
			suspended:     a.Suspended,
		}}
	}

	pool := a.EffectivePool()
	poolName := a.QualifiedName()

	// Unlimited pool: discover running instances via session prefix.
	if pool.IsUnlimited() && sp != nil {
		return discoverUnlimitedPool(a, poolName, cityName, sessTmpl, sp)
	}

	// Bounded pool: static enumeration.
	poolMax := pool.Max
	if poolMax <= 0 {
		poolMax = 1
	}

	var result []expandedAgent
	for i := 1; i <= poolMax; i++ {
		memberName := a.Name
		if poolMax > 1 {
			memberName = a.Name + "-" + strconv.Itoa(i)
		}
		qn := memberName
		if a.Dir != "" {
			qn = a.Dir + "/" + memberName
		}
		result = append(result, expandedAgent{
			qualifiedName: qn,
			rig:           a.Dir,
			pool:          poolName,
			suspended:     a.Suspended,
		})
	}
	return result
}

// sessionLister is the subset of session.Provider needed for pool discovery.
type sessionLister interface {
	ListRunning(prefix string) ([]string, error)
}

// discoverUnlimitedPool finds running instances of an unlimited pool by
// listing sessions with a matching prefix, then reverse-mapping session
// names back to qualified agent names.
func discoverUnlimitedPool(a config.Agent, poolName, cityName, sessTmpl string, sp sessionLister) []expandedAgent {
	// Build session name prefix: e.g. "city--myrig--polecat-"
	qnPrefix := a.Name + "-"
	if a.Dir != "" {
		qnPrefix = a.Dir + "/" + a.Name + "-"
	}
	snPrefix := agent.SessionNameFor(cityName, qnPrefix, sessTmpl)

	running, err := sp.ListRunning(snPrefix)
	if err != nil || len(running) == 0 {
		return nil
	}

	// Reverse session names back to qualified agent names.
	templatePrefix := agent.SessionNameFor(cityName, "", sessTmpl)
	var result []expandedAgent
	for _, sn := range running {
		qnSanitized := sn
		if templatePrefix != "" && strings.HasPrefix(qnSanitized, templatePrefix) {
			qnSanitized = qnSanitized[len(templatePrefix):]
		}
		qn := strings.ReplaceAll(qnSanitized, "--", "/")
		result = append(result, expandedAgent{
			qualifiedName: qn,
			rig:           a.Dir,
			pool:          poolName,
			suspended:     a.Suspended,
		})
	}
	return result
}

// agentSessionName converts a qualified agent name to a tmux session name
// using the canonical naming contract from agent.SessionNameFor.
func agentSessionName(cityName, qualifiedName, sessionTemplate string) string {
	return agent.SessionNameFor(cityName, qualifiedName, sessionTemplate)
}

// findAgent looks up an agent by qualified name in the config.
// For pool members, it matches the pool definition.
func findAgent(cfg *config.City, name string) (config.Agent, bool) {
	dir, baseName := config.ParseQualifiedName(name)
	for _, a := range cfg.Agents {
		if a.Dir == dir && a.Name == baseName {
			return a, true
		}
		// Check pool members.
		if a.IsPool() && a.Dir == dir {
			pool := a.EffectivePool()
			if pool.IsUnlimited() {
				// Unlimited pool: match "{name}-{N}" where N >= 1.
				prefix := a.Name + "-"
				if strings.HasPrefix(baseName, prefix) {
					suffix := baseName[len(prefix):]
					if n, err := strconv.Atoi(suffix); err == nil && n >= 1 {
						return a, true
					}
				}
				continue
			}
			// Bounded pool: enumerate.
			poolMax := pool.Max
			if poolMax <= 0 {
				poolMax = 1
			}
			for i := 1; i <= poolMax; i++ {
				memberName := a.Name
				if poolMax > 1 {
					memberName = a.Name + "-" + strconv.Itoa(i)
				}
				if memberName == baseName {
					return a, true
				}
			}
		}
	}
	return config.Agent{}, false
}

// findActiveBead returns the ID of the first in_progress bead assigned to the
// given agent. If rig is non-empty, only that rig's store is searched;
// otherwise all stores are searched. Returns "" if no match.
func (s *Server) findActiveBead(agentName, rig string) string {
	stores := s.state.BeadStores()
	var rigNames []string
	if rig != "" {
		if _, ok := stores[rig]; ok {
			rigNames = []string{rig}
		}
	}
	if rigNames == nil {
		rigNames = sortedRigNames(stores)
	}
	for _, rn := range rigNames {
		list, err := stores[rn].List()
		if err != nil {
			continue
		}
		for _, b := range list {
			if b.Status == "in_progress" && b.Assignee == agentName {
				return b.ID
			}
		}
	}
	return ""
}
