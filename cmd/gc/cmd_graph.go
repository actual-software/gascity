package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/spf13/cobra"
)

func newGraphCmd(stdout, stderr io.Writer) *cobra.Command {
	var mermaid bool
	cmd := &cobra.Command{
		Use:   "graph <bead-ids|convoy-id|epic-id...>",
		Short: "Show dependency graph for beads",
		Long: `Show the dependency graph for a set of beads, a convoy, or an epic.

Resolves dependencies via the bead store and prints each bead with its
status and what blocks it. Convoys and epics are expanded to their
children automatically. Readiness is computed within the displayed set.

By default prints a table. Use --mermaid for a Mermaid.js flowchart
you can paste into Markdown.`,
		Example: `  gc graph gc-42               # expand convoy or epic children
  gc graph gc-1 gc-2 gc-3     # arbitrary beads
  gc graph gc-42 --mermaid     # Mermaid.js diagram`,
		Args: cobra.ArbitraryArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			opts := graphOpts{Mermaid: mermaid}
			if cmdGraph(args, opts, stdout, stderr) != 0 {
				return errExit
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&mermaid, "mermaid", false, "output Mermaid.js flowchart")
	return cmd
}

// graphOpts controls graph output format.
type graphOpts struct {
	Mermaid bool
}

// cmdGraph is the CLI entry point.
func cmdGraph(args []string, opts graphOpts, stdout, stderr io.Writer) int {
	store, code := openCityStore(stderr, "gc graph")
	if store == nil {
		return code
	}
	return doGraph(store, args, opts, stdout, stderr)
}

// graphNode holds a bead and its resolved dependency edges.
type graphNode struct {
	bead        beads.Bead
	blockedBy   []string // IDs of beads in the set that block this one (all edges)
	openBlocker []string // IDs of open beads in the set that block this one
}

// doGraph resolves beads and their dependencies, then prints the graph.
func doGraph(store beads.Store, args []string, opts graphOpts, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprintln(stderr, "gc graph: missing bead IDs") //nolint:errcheck // best-effort stderr
		return 1
	}

	// Resolve input — expand containers.
	beadIDs, err := resolveGraphInput(store, args)
	if err != nil {
		fmt.Fprintf(stderr, "gc graph: %v\n", err) //nolint:errcheck // best-effort stderr
		return 1
	}
	if len(beadIDs) == 0 {
		fmt.Fprintln(stdout, "No beads to graph") //nolint:errcheck // best-effort stdout
		return 0
	}

	// Build set for filtering edges to within-set only.
	inSet := make(map[string]bool, len(beadIDs))
	for _, id := range beadIDs {
		inSet[id] = true
	}

	// Fetch beads and their dependencies.
	nodes := make([]graphNode, 0, len(beadIDs))
	for _, id := range beadIDs {
		b, err := store.Get(id)
		if err != nil {
			fmt.Fprintf(stderr, "gc graph: %v\n", err) //nolint:errcheck // best-effort stderr
			return 1
		}
		deps, err := store.DepList(id, "down")
		if err != nil {
			fmt.Fprintf(stderr, "gc graph: listing deps for %s: %v\n", id, err) //nolint:errcheck // best-effort stderr
			return 1
		}
		var blockedBy []string
		for _, d := range deps {
			if inSet[d.DependsOnID] {
				blockedBy = append(blockedBy, d.DependsOnID)
			}
		}
		sort.Strings(blockedBy)
		nodes = append(nodes, graphNode{bead: b, blockedBy: blockedBy})
	}

	// Second pass: compute open blockers by cross-referencing status.
	closedIDs := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		if n.bead.Status == "closed" {
			closedIDs[n.bead.ID] = true
		}
	}
	for i, n := range nodes {
		for _, dep := range n.blockedBy {
			if !closedIDs[dep] {
				nodes[i].openBlocker = append(nodes[i].openBlocker, dep)
			}
		}
	}

	if opts.Mermaid {
		printMermaid(nodes, stdout)
	} else {
		printTable(nodes, stdout)
	}
	return 0
}

// resolveGraphInput expands container types (convoy, epic) to their children.
// Non-containers are passed through. Multiple args are resolved individually.
// Duplicate IDs are removed.
func resolveGraphInput(store beads.Store, args []string) ([]string, error) {
	seen := make(map[string]bool)
	var ids []string
	add := func(id string) {
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	for _, arg := range args {
		b, err := store.Get(arg)
		if err != nil {
			return nil, err
		}
		if beads.IsContainerType(b.Type) {
			children, err := store.Children(b.ID)
			if err != nil {
				return nil, fmt.Errorf("expanding %s %s: %w", b.Type, b.ID, err)
			}
			for _, ch := range children {
				add(ch.ID)
			}
		} else {
			add(b.ID)
		}
	}
	return ids, nil
}

// printTable prints the graph as a table with blocked-by and ready columns.
func printTable(nodes []graphNode, stdout io.Writer) {
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "BEAD\tTITLE\tSTATUS\tBLOCKED BY\tREADY") //nolint:errcheck // best-effort stdout

	ready := 0
	for _, n := range nodes {
		blockedBy := "-"
		if len(n.blockedBy) > 0 {
			blockedBy = strings.Join(n.blockedBy, ", ")
		}

		isReady := isBeadReady(n)
		var readyStr string
		switch {
		case n.bead.Status == "closed":
			readyStr = "done"
		case isReady:
			readyStr = "yes"
			ready++
		default:
			readyStr = "blocked"
		}

		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", //nolint:errcheck // best-effort stdout
			n.bead.ID, n.bead.Title, n.bead.Status, blockedBy, readyStr)
	}
	tw.Flush() //nolint:errcheck // best-effort stdout

	total := len(nodes)
	closed := 0
	for _, n := range nodes {
		if n.bead.Status == "closed" {
			closed++
		}
	}
	fmt.Fprintf(stdout, "\n%d bead(s): %d closed, %d ready, %d blocked\n", //nolint:errcheck // best-effort stdout
		total, closed, ready, total-closed-ready)
}

// printMermaid outputs a Mermaid.js flowchart.
func printMermaid(nodes []graphNode, stdout io.Writer) {
	fmt.Fprintln(stdout, "graph TD") //nolint:errcheck // best-effort stdout

	for _, n := range nodes {
		label := mermaidLabel(n)
		fmt.Fprintf(stdout, "  %s[\"%s\"]\n", n.bead.ID, label) //nolint:errcheck // best-effort stdout
	}

	// Print edges.
	for _, n := range nodes {
		for _, dep := range n.blockedBy {
			fmt.Fprintf(stdout, "  %s --> %s\n", dep, n.bead.ID) //nolint:errcheck // best-effort stdout
		}
	}

	// Style closed nodes.
	for _, n := range nodes {
		if n.bead.Status == "closed" {
			fmt.Fprintf(stdout, "  style %s fill:#90EE90\n", n.bead.ID) //nolint:errcheck // best-effort stdout
		} else if isBeadReady(n) {
			fmt.Fprintf(stdout, "  style %s fill:#FFD700\n", n.bead.ID) //nolint:errcheck // best-effort stdout
		}
	}
}

// mermaidLabel creates a display label for a mermaid node.
func mermaidLabel(n graphNode) string {
	status := ""
	switch n.bead.Status {
	case "closed":
		status = " done"
	case "in_progress":
		status = " ..."
	}
	// Escape quotes in titles for mermaid safety.
	title := strings.ReplaceAll(n.bead.Title, "\"", "'")
	return fmt.Sprintf("%s%s", title, status)
}

// isBeadReady reports whether a bead has no open blockers.
func isBeadReady(n graphNode) bool {
	return n.bead.Status != "closed" && len(n.openBlocker) == 0
}
