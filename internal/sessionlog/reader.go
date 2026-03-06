package sessionlog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Session is the resolved view of a Claude JSONL session file.
type Session struct {
	// ID is the session identifier (from the filename).
	ID string

	// Messages is the active branch in conversation order (root → tip).
	// Entries that aren't relevant for display (file-history-snapshot,
	// progress hooks) are filtered out.
	Messages []*Entry

	// OrphanedToolUseIDs contains tool_use IDs with no matching result.
	OrphanedToolUseIDs map[string]bool

	// HasBranches is true if the session has conversation forks.
	HasBranches bool

	// Pagination metadata.
	Pagination *PaginationInfo
}

// PaginationInfo describes the pagination state of a session response.
type PaginationInfo struct {
	HasOlderMessages       bool   `json:"has_older_messages"`
	TotalMessageCount      int    `json:"total_message_count"`
	ReturnedMessageCount   int    `json:"returned_message_count"`
	TruncatedBeforeMessage string `json:"truncated_before_message,omitempty"`
	TotalCompactions       int    `json:"total_compactions"`
}

// displayTypes are entry types included in the display output.
var displayTypes = map[string]bool{
	"user":      true,
	"assistant": true,
	"system":    true,
	"result":    true,
}

// ReadFile reads a Claude JSONL session file and resolves it into a
// Session. The file is parsed, DAG-resolved, and filtered to display
// entries. Returns the most recent tailCompactions worth of messages
// (0 = all messages).
func ReadFile(path string, tailCompactions int) (*Session, error) {
	entries, err := parseFile(path)
	if err != nil {
		return nil, err
	}

	dag := BuildDag(entries)

	// Filter to display types.
	var messages []*Entry
	for _, e := range dag.ActiveBranch {
		if displayTypes[e.Type] {
			messages = append(messages, e)
		}
	}

	// Extract session ID from filename.
	base := filepath.Base(path)
	sessionID := strings.TrimSuffix(base, filepath.Ext(base))

	sess := &Session{
		ID:                 sessionID,
		Messages:           messages,
		OrphanedToolUseIDs: dag.OrphanedToolUseIDs,
		HasBranches:        dag.HasBranches,
	}

	// Apply compact-boundary pagination.
	if tailCompactions > 0 {
		paginated, info := sliceAtCompactBoundaries(messages, tailCompactions, "")
		sess.Messages = paginated
		sess.Pagination = info
	}

	return sess, nil
}

// ReadFileOlder loads older messages before a cursor, returning the
// previous tailCompactions segment.
func ReadFileOlder(path string, tailCompactions int, beforeMessageID string) (*Session, error) {
	entries, err := parseFile(path)
	if err != nil {
		return nil, err
	}

	dag := BuildDag(entries)

	var messages []*Entry
	for _, e := range dag.ActiveBranch {
		if displayTypes[e.Type] {
			messages = append(messages, e)
		}
	}

	base := filepath.Base(path)
	sessionID := strings.TrimSuffix(base, filepath.Ext(base))

	paginated, info := sliceAtCompactBoundaries(messages, tailCompactions, beforeMessageID)

	return &Session{
		ID:                 sessionID,
		Messages:           paginated,
		OrphanedToolUseIDs: dag.OrphanedToolUseIDs,
		HasBranches:        dag.HasBranches,
		Pagination:         info,
	}, nil
}

// parseFile reads all JSONL lines from a file into entries.
func parseFile(path string) ([]*Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening session file: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-only file

	var entries []*Entry
	scanner := bufio.NewScanner(f)
	// Default scanner buffer is 64KB; Claude entries can be large
	// (tool results with full file contents, base64 images, etc.).
	// Use 50MB max to handle very large entries without aborting the whole file.
	scanner.Buffer(make([]byte, 0, 256*1024), 50*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			continue // skip malformed lines
		}
		// Preserve the raw JSON for API pass-through.
		raw := make([]byte, len(line))
		copy(raw, line)
		e.Raw = raw
		entries = append(entries, &e)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning session file: %w", err)
	}

	return entries, nil
}

// sliceAtCompactBoundaries returns the tail portion of messages starting
// from the Nth-from-last compact boundary. The boundary itself is
// included so consumers can render a "Context compacted" divider.
func sliceAtCompactBoundaries(messages []*Entry, tailCompactions int, beforeMessageID string) ([]*Entry, *PaginationInfo) {
	totalCount := len(messages)

	// For "load older" requests: truncate at cursor first.
	working := messages
	if beforeMessageID != "" {
		for i, m := range messages {
			if m.UUID == beforeMessageID {
				working = messages[:i]
				break
			}
		}
	}

	// Guard: tailCompactions <= 0 means "return the working set as-is".
	if tailCompactions <= 0 {
		return working, &PaginationInfo{
			HasOlderMessages:     false,
			TotalMessageCount:    totalCount,
			ReturnedMessageCount: len(working),
		}
	}

	// Find all compact_boundary indices.
	var compactIndices []int
	for i, m := range working {
		if m.IsCompactBoundary() {
			compactIndices = append(compactIndices, i)
		}
	}

	totalCompactions := len(compactIndices)

	// Fewer boundaries than requested — return everything.
	if len(compactIndices) <= tailCompactions {
		return working, &PaginationInfo{
			HasOlderMessages:     false,
			TotalMessageCount:    totalCount,
			ReturnedMessageCount: len(working),
			TotalCompactions:     totalCompactions,
		}
	}

	// Slice from the Nth-from-last boundary (inclusive).
	sliceFrom := compactIndices[len(compactIndices)-tailCompactions]
	sliced := working[sliceFrom:]

	var truncatedBefore string
	if len(sliced) > 0 {
		truncatedBefore = sliced[0].UUID
	}

	return sliced, &PaginationInfo{
		HasOlderMessages:       true,
		TotalMessageCount:      totalCount,
		ReturnedMessageCount:   len(sliced),
		TruncatedBeforeMessage: truncatedBefore,
		TotalCompactions:       totalCompactions,
	}
}

// FindSessionFile searches for the most recently modified JSONL session
// file for the given working directory. Returns "" if not found.
//
// Claude stores sessions at ~/.claude/projects/{slug}/{sessionID}.jsonl
// where slug is the absolute working directory path with "/" and "."
// replaced by "-".
func FindSessionFile(searchPaths []string, workDir string) string {
	slug := projectSlug(workDir)
	for _, base := range searchPaths {
		dir := filepath.Join(base, slug)
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		var bestPath string
		var bestTime int64
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			mt := info.ModTime().UnixNano()
			if mt > bestTime {
				bestTime = mt
				bestPath = filepath.Join(dir, e.Name())
			}
		}
		if bestPath != "" {
			return bestPath
		}
	}
	return ""
}

// DefaultSearchPaths returns the default search paths for Claude JSONL
// session files.
func DefaultSearchPaths() []string {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil
	}
	return []string{filepath.Join(home, ".claude", "projects")}
}

// projectSlug converts an absolute path to the Claude project directory
// slug convention: all "/" and "." are replaced with "-".
func projectSlug(absPath string) string {
	s := strings.ReplaceAll(absPath, "/", "-")
	s = strings.ReplaceAll(s, ".", "-")
	return s
}
