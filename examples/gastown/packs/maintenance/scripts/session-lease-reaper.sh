#!/usr/bin/env bash
# session-lease-reaper — TTL-based cleanup of expired session leases.
#
# Problem: When sessions die unexpectedly (crash, OOM, network drop),
# the bead store may still show them as "active" or "creating", and
# their tmux panes may linger. This blocks new sessions from starting
# because max_active_sessions=1 slots are occupied by ghosts.
#
# Solution: Each session has an implicit lease based on its last
# activity timestamp. If a session exceeds its TTL without activity,
# this script:
#   1. Kills the orphaned tmux session (frees the slot)
#   2. Closes the gc session bead
#   3. Unassigns any beads stuck on the dead session
#
# TTL policy:
#   - "active" sessions with no activity for SESSION_ACTIVE_TTL → expired
#   - "creating" sessions older than SESSION_CREATING_TTL → stuck
#   - tmux sessions with no matching gc session → orphaned (5m grace)
#
# Runs as an exec order (no LLM, no agent, no wisp).
set -euo pipefail

CITY="${GC_CITY:-.}"
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
. "$SCRIPT_DIR/lib-resilience.sh"
PACK_STATE_DIR="${GC_PACK_STATE_DIR:-${GC_CITY_RUNTIME_DIR:-$CITY/.gc/runtime}/packs/maintenance}"

# Configurable TTLs (seconds).
SESSION_ACTIVE_TTL="${SESSION_ACTIVE_TTL:-7200}"      # 2h: active session with no activity
SESSION_CREATING_TTL="${SESSION_CREATING_TTL:-120}"    # 2m: session stuck in creating
SESSION_ASLEEP_TTL="${SESSION_ASLEEP_TTL:-86400}"      # 24h: asleep session lingering

# City name for tmux socket.
CITY_NAME=$(basename "$CITY")
TMUX_SOCKET="${GC_TMUX_SOCKET:-$CITY_NAME}"

NOW=$(date +%s)
REAPED=0
TMUX_KILLED=0
BEADS_RESET=0

# Temp files for set lookups (avoids bash 4 associative arrays).
LIVE_GC_FILE=$(mktemp)
REAPED_FILE=$(mktemp)
trap 'rm -f "$LIVE_GC_FILE" "$REAPED_FILE"' EXIT

# --- Phase 1: Reap expired gc sessions ---

SESSIONS=$(gc session list --json 2>/dev/null) || exit 0
if [ -z "$SESSIONS" ] || [ "$SESSIONS" = "null" ]; then
    exit 0
fi

# Track all non-closed session names for Phase 2.
echo "$SESSIONS" | jq -r '.[] | select(.State != "closed") | .SessionName' 2>/dev/null > "$LIVE_GC_FILE"

echo "$SESSIONS" | jq -c '.[]' 2>/dev/null | while IFS= read -r session; do
    id=$(echo "$session" | jq -r '.ID')
    state=$(echo "$session" | jq -r '.State')
    session_name=$(echo "$session" | jq -r '.SessionName')
    created_at=$(echo "$session" | jq -r '.CreatedAt')
    last_active=$(echo "$session" | jq -r '.LastActive')

    # Parse timestamps.
    # macOS date: -j -f format; Linux date: -d string. Try both.
    CREATED_TS=$(date -j -f "%Y-%m-%dT%H:%M:%S" "${created_at%%.*}" +%s 2>/dev/null || \
                 date -d "$created_at" +%s 2>/dev/null) || continue

    ACTIVE_TS=0
    if [ "$last_active" != "0001-01-01T00:00:00Z" ] && [ -n "$last_active" ]; then
        # Strip timezone offset for macOS parsing.
        clean_ts="${last_active%%.*}"
        clean_ts="${clean_ts%%-07:00}"
        ACTIVE_TS=$(date -j -f "%Y-%m-%dT%H:%M:%S" "$clean_ts" +%s 2>/dev/null || \
                    date -d "$last_active" +%s 2>/dev/null) || ACTIVE_TS=0
    fi

    # Use the most recent timestamp.
    LAST_TS=$CREATED_TS
    if [ "$ACTIVE_TS" -gt "$LAST_TS" ] 2>/dev/null; then
        LAST_TS=$ACTIVE_TS
    fi
    AGE=$((NOW - LAST_TS))

    EXPIRED=false
    REASON=""

    case "$state" in
        active)
            if [ "$AGE" -gt "$SESSION_ACTIVE_TTL" ]; then
                EXPIRED=true
                REASON="active session idle for ${AGE}s (TTL: ${SESSION_ACTIVE_TTL}s)"
            fi
            ;;
        creating)
            if [ "$AGE" -gt "$SESSION_CREATING_TTL" ]; then
                EXPIRED=true
                REASON="stuck in creating for ${AGE}s (TTL: ${SESSION_CREATING_TTL}s)"
            fi
            ;;
        asleep)
            if [ "$AGE" -gt "$SESSION_ASLEEP_TTL" ]; then
                EXPIRED=true
                REASON="asleep for ${AGE}s (TTL: ${SESSION_ASLEEP_TTL}s)"
            fi
            ;;
    esac

    if [ "$EXPIRED" = true ]; then
        echo "session-lease-reaper: expiring $id ($session_name): $REASON"

        # Kill tmux session if it exists.
        if tmux -L "$TMUX_SOCKET" kill-session -t "$session_name" 2>/dev/null; then
            TMUX_KILLED=$((TMUX_KILLED + 1))
        fi

        # Close the gc session.
        gc session close "$id" 2>/dev/null || \
            gc session kill "$id" 2>/dev/null || true

        # Track reaped session names for Phase 3.
        echo "$session_name" >> "$REAPED_FILE"

        # Remove from live set.
        grep -v "^${session_name}$" "$LIVE_GC_FILE" > "${LIVE_GC_FILE}.tmp" 2>/dev/null || true
        mv "${LIVE_GC_FILE}.tmp" "$LIVE_GC_FILE"

        REAPED=$((REAPED + 1))
    fi
done

# --- Phase 2: Kill orphaned tmux sessions ---
# tmux sessions with s-* prefix that have no matching gc session.

TMUX_SESSIONS=$(tmux -L "$TMUX_SOCKET" list-sessions -F "#{session_name}|#{session_activity}" 2>/dev/null) || true

if [ -n "$TMUX_SESSIONS" ]; then
    echo "$TMUX_SESSIONS" | while IFS='|' read -r tmux_name tmux_activity; do
        # Only check agent sessions (s-* prefix), skip dog sessions.
        case "$tmux_name" in s-*) ;; *) continue ;; esac

        # Check if this tmux session has a matching live gc session.
        if ! grep -qx "$tmux_name" "$LIVE_GC_FILE" 2>/dev/null; then
            # Orphaned tmux session — check age before killing.
            TMUX_AGE=$((NOW - tmux_activity))
            if [ "$TMUX_AGE" -gt 300 ]; then  # 5 min grace period
                echo "session-lease-reaper: killing orphaned tmux session $tmux_name (no matching gc session, idle ${TMUX_AGE}s)"
                tmux -L "$TMUX_SOCKET" kill-session -t "$tmux_name" 2>/dev/null || true
                TMUX_KILLED=$((TMUX_KILLED + 1))
            fi
        fi
    done
fi

# --- Phase 3: Reset beads assigned to reaped/dead sessions ---

IN_PROGRESS=$(bd_retry list --status=in_progress --json --limit=0 2>/dev/null) || true
if [ -n "$IN_PROGRESS" ] && [ "$IN_PROGRESS" != "[]" ]; then
    # Get current live session names after reaping.
    CURRENT_LIVE=$(gc session list --json 2>/dev/null | \
        jq -r '.[] | select(.State == "active" or .State == "creating") | .SessionName' 2>/dev/null) || true

    CURRENT_LIVE_FILE=$(mktemp)
    trap 'rm -f "$LIVE_GC_FILE" "$REAPED_FILE" "$CURRENT_LIVE_FILE"' EXIT
    echo "$CURRENT_LIVE" > "$CURRENT_LIVE_FILE"

    echo "$IN_PROGRESS" | jq -r '.[] | select(.assignee != null and .assignee != "") | "\(.id)\t\(.assignee)"' 2>/dev/null | while IFS=$'\t' read -r bead_id assignee; do
        # Session-ID assignees start with s-.
        case "$assignee" in s-*) ;; *) continue ;; esac

        if ! grep -qx "$assignee" "$CURRENT_LIVE_FILE" 2>/dev/null; then
            echo "session-lease-reaper: resetting bead $bead_id (was assigned to dead session $assignee)"
            bd_retry update "$bead_id" --status=open 2>/dev/null || true
            bd_retry assign "$bead_id" "" 2>/dev/null || true
            BEADS_RESET=$((BEADS_RESET + 1))
        fi
    done
fi

# --- Summary ---
TOTAL=$((REAPED + TMUX_KILLED + BEADS_RESET))
if [ "$TOTAL" -gt 0 ]; then
    echo "session-lease-reaper: reaped=$REAPED tmux_killed=$TMUX_KILLED beads_reset=$BEADS_RESET"
fi
