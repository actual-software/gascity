package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/events"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/spf13/cobra"
)

func newStopCmd(stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop [path]",
		Short: "Stop all agent sessions in the city",
		Long: `Stop all agent sessions in the city with graceful shutdown.

Sends interrupt signals to running agents, waits for the configured
shutdown timeout, then force-kills any remaining sessions. Also stops
the Dolt server and cleans up orphan sessions. If a controller is
running, delegates shutdown to it.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if cmdStop(args, stdout, stderr) != 0 {
				return errExit
			}
			return nil
		},
	}
	return cmd
}

// cmdStop stops the city by terminating all configured agent sessions.
// If a path is given, operates there; otherwise uses cwd.
func cmdStop(args []string, stdout, stderr io.Writer) int {
	var dir string
	var err error
	switch {
	case len(args) > 0:
		dir, err = filepath.Abs(args[0])
	case cityFlag != "":
		dir, err = filepath.Abs(cityFlag)
	default:
		dir, err = os.Getwd()
	}
	if err != nil {
		fmt.Fprintf(stderr, "gc stop: %v\n", err) //nolint:errcheck // best-effort stderr
		return 1
	}
	cityPath, err := findCity(dir)
	if err != nil {
		fmt.Fprintf(stderr, "gc stop: %v\n", err) //nolint:errcheck // best-effort stderr
		return 1
	}
	cfg, err := loadCityConfig(cityPath)
	if err != nil {
		fmt.Fprintf(stderr, "gc stop: %v\n", err) //nolint:errcheck // best-effort stderr
		return 1
	}
	cityName := cfg.Workspace.Name
	if cityName == "" {
		cityName = filepath.Base(cityPath)
	}

	// If a controller is running, ask it to shut down (it stops agents).
	if tryStopController(cityPath, stdout) {
		// Controller handled the shutdown — still stop bead store below.
		if err := shutdownBeadsProvider(cityPath); err != nil {
			fmt.Fprintf(stderr, "gc stop: bead store: %v\n", err) //nolint:errcheck // best-effort stderr
		}
		return 0
	}

	sp := newSessionProvider()
	st := cfg.Workspace.SessionTemplate
	var agents []agent.Handle
	desired := make(map[string]bool, len(cfg.Agents))
	for _, a := range cfg.Agents {
		pool := a.EffectivePool()
		qn := a.QualifiedName()
		if !pool.IsMultiInstance() {
			// Single agent.
			agents = append(agents, agent.HandleFor(qn, cityName, st, sp))
			desired[agent.SessionNameFor(cityName, qn, st)] = true
		} else {
			// Pool agent: discover instances (static for bounded, live for unlimited).
			for _, qualifiedInstance := range discoverPoolInstances(a.Name, a.Dir, pool, cityName, st, sp) {
				agents = append(agents, agent.HandleFor(qualifiedInstance, cityName, st, sp))
				desired[agent.SessionNameFor(cityName, qualifiedInstance, st)] = true
			}
		}
	}
	recorder := events.Discard
	if fr, err := events.NewFileRecorder(
		filepath.Join(cityPath, ".gc", "events.jsonl"), stderr); err == nil {
		recorder = fr
	}

	code := doStop(agents, sp, cfg.Daemon.ShutdownTimeoutDuration(), recorder, stdout, stderr)

	// Clean up orphan sessions (sessions with the city prefix that are
	// not in the current config).
	rops := newReconcileOps(sp)
	doStopOrphans(sp, rops, desired, cfg.Daemon.ShutdownTimeoutDuration(), recorder, stdout, stderr)

	// Stop bead store's backing service after agents.
	if err := shutdownBeadsProvider(cityPath); err != nil {
		fmt.Fprintf(stderr, "gc stop: bead store: %v\n", err) //nolint:errcheck // best-effort stderr
		// Non-fatal warning.
	}

	return code
}

// tryStopController connects to .gc/controller.sock and sends "stop".
// Returns true if a controller acknowledged the shutdown. If no controller
// is running (socket doesn't exist or connection refused), returns false.
func tryStopController(cityPath string, stdout io.Writer) bool {
	sockPath := filepath.Join(cityPath, ".gc", "controller.sock")
	conn, err := net.DialTimeout("unix", sockPath, 2*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()                                     //nolint:errcheck // best-effort cleanup
	conn.Write([]byte("stop\n"))                           //nolint:errcheck // best-effort
	conn.SetReadDeadline(time.Now().Add(10 * time.Second)) //nolint:errcheck // best-effort
	buf := make([]byte, 64)
	n, readErr := conn.Read(buf)
	if readErr != nil || !strings.Contains(string(buf[:n]), "ok") {
		return false // controller did not acknowledge — fall through to direct cleanup
	}
	fmt.Fprintln(stdout, "Controller stopping...") //nolint:errcheck // best-effort stdout
	return true
}

// doStop is the pure logic for "gc stop". It collects running agents and
// performs graceful shutdown (interrupt → wait → kill). Accepts pre-built
// agents, provider, timeout, and recorder for testability.
func doStop(agents []agent.Handle, sp runtime.Provider, timeout time.Duration,
	rec events.Recorder, stdout, stderr io.Writer,
) int {
	var names []string
	for _, a := range agents {
		if a.IsRunning() {
			names = append(names, a.SessionName())
		}
	}
	gracefulStopAll(names, sp, timeout, rec, stdout, stderr)
	fmt.Fprintln(stdout, "City stopped.") //nolint:errcheck // best-effort stdout
	return 0
}
