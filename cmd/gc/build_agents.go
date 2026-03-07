package main

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
)

// buildAgentsFromConfig builds the desired agent list from config and session
// provider. This is the shared core used by both standalone (gc start) and
// supervisor paths. It handles suspension checks, multi-instance template
// expansion, pool evaluation via scale_check, and single-agent building.
//
// Parameters:
//   - cityName, cityPath: city identifiers
//   - beaconTime: stable timestamp captured once (must NOT be time.Now() per call)
//   - c: city config (may be freshly reloaded)
//   - sp: session provider
//   - multiReg: multi-instance registry, or nil if multi agents are not supported
//   - logPrefix: error message prefix (e.g. "gc start" or "gc supervisor")
//   - stderr: writer for diagnostic output
func buildAgentsFromConfig(
	cityName, cityPath string,
	beaconTime time.Time,
	c *config.City,
	sp runtime.Provider,
	multiReg *multiRegistry,
	logPrefix string,
	stderr io.Writer,
) []agent.Agent {
	// City-level suspension: no agents should start.
	if c.Workspace.Suspended {
		return nil
	}

	bp := newAgentBuildParams(cityName, cityPath, c, sp, beaconTime, stderr)

	// Pre-compute suspended rig paths so we can skip agents in suspended rigs.
	suspendedRigPaths := make(map[string]bool)
	for _, r := range c.Rigs {
		if r.Suspended {
			suspendedRigPaths[filepath.Clean(r.Path)] = true
		}
	}

	// poolEvalWork collects pool agents for parallel scale_check evaluation.
	type poolEvalWork struct {
		agentIdx int
		pool     config.PoolConfig
		poolDir  string
	}

	var agents []agent.Agent
	var pendingPools []poolEvalWork
	for i := range c.Agents {
		if c.Agents[i].Suspended {
			continue // Suspended agent — skip until resumed.
		}

		// Multi-instance template: build an agent for each running instance.
		if c.Agents[i].IsMulti() {
			if multiReg == nil {
				fmt.Fprintf(stderr, "%s: multi agent %q skipped (not supported in this mode)\n", logPrefix, c.Agents[i].QualifiedName()) //nolint:errcheck
			} else {
				instances, mErr := multiReg.instancesForTemplate(c.Agents[i].QualifiedName())
				if mErr != nil {
					fmt.Fprintf(stderr, "%s: multi %q: %v (skipping)\n", logPrefix, c.Agents[i].QualifiedName(), mErr) //nolint:errcheck
					continue
				}
				for _, mi := range instances {
					if mi.State != "running" {
						continue
					}
					instanceAgent := deepCopyAgent(&c.Agents[i], mi.Name, c.Agents[i].Dir)
					instanceAgent.Multi = false
					instanceAgent.PoolName = c.Agents[i].QualifiedName()
					instanceQN := c.Agents[i].QualifiedName() + "/" + mi.Name
					fpExtra := buildFingerprintExtra(&instanceAgent)
					// Capture loop variables for closure.
					templateQN := c.Agents[i].QualifiedName()
					instName := mi.Name
					onStop := func() error {
						return multiReg.stop(templateQN, instName)
					}
					a, bErr := buildOneAgent(bp, &instanceAgent, instanceQN, fpExtra, onStop)
					if bErr != nil {
						fmt.Fprintf(stderr, "%s: multi instance %q: %v (skipping)\n", logPrefix, instanceQN, bErr) //nolint:errcheck
						continue
					}
					agents = append(agents, a)
				}
			}
			continue // Template itself never runs.
		}

		pool := c.Agents[i].EffectivePool()

		if pool.Max == 0 {
			continue // Disabled agent.
		}

		if pool.Max == 1 && !c.Agents[i].IsPool() {
			// Fixed agent: check rig suspension, then build via shared path.
			expandedDir := expandDirTemplate(c.Agents[i].Dir, SessionSetupContext{
				Agent:    c.Agents[i].QualifiedName(),
				Rig:      c.Agents[i].Dir,
				CityRoot: cityPath,
				CityName: cityName,
			})
			workDir, err := resolveAgentDir(cityPath, expandedDir)
			if err != nil {
				fmt.Fprintf(stderr, "%s: agent %q: %v (skipping)\n", logPrefix, c.Agents[i].QualifiedName(), err) //nolint:errcheck
				continue
			}
			if suspendedRigPaths[filepath.Clean(workDir)] {
				continue // Agent's rig is suspended — skip.
			}

			fpExtra := buildFingerprintExtra(&c.Agents[i])
			a, err := buildOneAgent(bp, &c.Agents[i], c.Agents[i].QualifiedName(), fpExtra)
			if err != nil {
				fmt.Fprintf(stderr, "%s: %v (skipping)\n", logPrefix, err) //nolint:errcheck
				continue
			}
			agents = append(agents, a)
			continue
		}

		// Pool agent (explicit [agents.pool] or implicit singleton with pool config).
		// Collect for parallel scale_check evaluation below.
		if c.Agents[i].Dir != "" {
			poolDir, pdErr := resolveAgentDir(cityPath, c.Agents[i].Dir)
			if pdErr == nil && suspendedRigPaths[filepath.Clean(poolDir)] {
				continue // Agent's rig is suspended — skip.
			}
		}
		// Resolve pool working directory for scale_check context.
		poolDir := cityPath
		if c.Agents[i].Dir != "" {
			if pd, pdErr := resolveAgentDir(cityPath, c.Agents[i].Dir); pdErr == nil {
				poolDir = pd
			}
		}
		pendingPools = append(pendingPools, poolEvalWork{agentIdx: i, pool: pool, poolDir: poolDir})
	}

	// Run pool scale_check commands in parallel. Each check is an
	// independent shell command; running them concurrently reduces
	// wall-clock time from sum(check_durations) to max(check_duration).
	type poolEvalResult struct {
		desired int
		err     error
	}
	evalResults := make([]poolEvalResult, len(pendingPools))
	var wg sync.WaitGroup
	for j, pw := range pendingPools {
		wg.Add(1)
		go func(idx int, name string, pool config.PoolConfig, dir string) {
			defer wg.Done()
			desired, err := evaluatePool(name, pool, dir, shellScaleCheck)
			evalResults[idx] = poolEvalResult{desired: desired, err: err}
		}(j, c.Agents[pw.agentIdx].Name, pw.pool, pw.poolDir)
	}
	wg.Wait()

	// Process results sequentially (logging, counting, agent building).
	for j, pw := range pendingPools {
		pr := evalResults[j]
		if pr.err != nil {
			fmt.Fprintf(stderr, "%s: %v (using min=%d)\n", logPrefix, pr.err, pw.pool.Min) //nolint:errcheck
		}
		running := countRunningPoolInstances(c.Agents[pw.agentIdx].Name, c.Agents[pw.agentIdx].Dir, pw.pool, cityName, c.Workspace.SessionTemplate, sp)
		if pr.desired != running {
			fmt.Fprintf(stderr, "%s: pool '%s': check returned %d, %d running → scaling %s\n", //nolint:errcheck
				logPrefix, c.Agents[pw.agentIdx].Name, pr.desired, running, scaleDirection(running, pr.desired))
		}
		pa, err := poolAgents(bp, &c.Agents[pw.agentIdx], pr.desired)
		if err != nil {
			fmt.Fprintf(stderr, "%s: pool %q: %v (skipping)\n", logPrefix, c.Agents[pw.agentIdx].QualifiedName(), err) //nolint:errcheck
			continue
		}
		agents = append(agents, pa...)
	}
	return agents
}
