#!/usr/bin/env bash
# lib-resilience.sh — Retry and resilience primitives for bd/gc scripts.
#
# Source this file from any maintenance script to get durable bd/gc calls
# that survive transient Dolt failures (connection refused, server restart,
# read-after-write inconsistency).
#
# Usage:
#   . "$(dirname "$0")/lib-resilience.sh"
#   bd_retry show 2hwv-abc
#   bd_retry update 2hwv-abc --status=open
#   bd_retry_critical update 2hwv-abc --add-label=foo   # fails script on exhaustion
#   gc_retry session list --json
#
# Configuration (env vars):
#   BD_RETRY_MAX=3         Max retry attempts (default: 3)
#   BD_RETRY_DELAY=1       Initial delay in seconds (default: 1)
#   BD_RETRY_BACKOFF=2     Backoff multiplier (default: 2, so: 1s, 2s, 4s)
#   BD_RETRY_JITTER=1      Add 0-N seconds of jitter (default: 1)
#   BD_RETRY_VERBOSE=0     Log retry attempts to stderr (default: 0)
#
# Error classification:
#   TRANSIENT — retried automatically:
#     - "connection refused" (Dolt restarting)
#     - "no such host" / "dial tcp" (network blip)
#     - "server unreachable" (Dolt not ready)
#     - "database is locked" (Dolt GC in progress)
#     - "nothing to commit" (benign Dolt race)
#     - "no issue found matching" after a recent create (read-after-write lag)
#
#   PERMANENT — fail immediately:
#     - "unknown flag" (programming error)
#     - "unknown column" (schema mismatch)
#     - "invalid escape" (TOML syntax error)
#     - "permission denied"
#     - All other errors (unknown = assume permanent)

: "${BD_RETRY_MAX:=3}"
: "${BD_RETRY_DELAY:=1}"
: "${BD_RETRY_BACKOFF:=2}"
: "${BD_RETRY_JITTER:=1}"
: "${BD_RETRY_VERBOSE:=0}"

# --- Error classification ---

_is_transient_error() {
    local output="$1"
    case "$output" in
        *"connection refused"*)     return 0 ;;
        *"server unreachable"*)     return 0 ;;
        *"no such host"*)           return 0 ;;
        *"dial tcp"*)               return 0 ;;
        *"database is locked"*)     return 0 ;;
        *"nothing to commit"*)      return 0 ;;
        *"no issue found matching"*)return 0 ;;
        *"i/o timeout"*)            return 0 ;;
        *"broken pipe"*)            return 0 ;;
        *"connection reset"*)       return 0 ;;
        *"EOF"*)                    return 0 ;;
        *"try again"*)              return 0 ;;
        *"temporarily unavailable"*)return 0 ;;
    esac
    return 1
}

_is_permanent_error() {
    local output="$1"
    case "$output" in
        *"unknown flag"*)           return 0 ;;
        *"unknown column"*)         return 0 ;;
        *"invalid escape"*)         return 0 ;;
        *"permission denied"*)      return 0 ;;
        *"schema mismatch"*)        return 0 ;;
        *"accepts "[0-9]*" arg"*)   return 0 ;;
        *"invalid"*"syntax"*)       return 0 ;;
    esac
    return 1
}

# --- Retry engine ---

# _retry_cmd <max> <delay> <backoff> <jitter> <cmd...>
# Returns: exit code of last attempt. Stdout/stderr from last attempt.
_retry_cmd() {
    local max="$1" delay="$2" backoff="$3" jitter="$4"
    shift 4

    local attempt=1
    local output=""
    local rc=0

    while [ "$attempt" -le "$max" ]; do
        # Capture both stdout and stderr, preserve exit code.
        output=$("$@" 2>&1) && rc=0 || rc=$?

        if [ "$rc" -eq 0 ]; then
            # Success — emit output and return.
            [ -n "$output" ] && echo "$output"
            return 0
        fi

        # Check error classification.
        if _is_permanent_error "$output"; then
            [ "$BD_RETRY_VERBOSE" -eq 1 ] && \
                echo "resilience: permanent error on attempt $attempt/$max: $*" >&2
            [ -n "$output" ] && echo "$output"
            return "$rc"
        fi

        if ! _is_transient_error "$output"; then
            # Unknown error — treat as permanent (safe default).
            [ "$BD_RETRY_VERBOSE" -eq 1 ] && \
                echo "resilience: unknown error on attempt $attempt/$max (not retrying): $*" >&2
            [ -n "$output" ] && echo "$output"
            return "$rc"
        fi

        # Transient error — retry with backoff.
        if [ "$attempt" -lt "$max" ]; then
            # Calculate sleep with jitter.
            local sleep_time="$delay"
            if [ "$jitter" -gt 0 ]; then
                # RANDOM not available in all shells; fall back to no jitter.
                local j=0
                j=$(( $(od -An -N2 -tu2 /dev/urandom 2>/dev/null | tr -d ' ') % (jitter + 1) )) 2>/dev/null || j=0
                sleep_time=$((delay + j))
            fi

            [ "$BD_RETRY_VERBOSE" -eq 1 ] && \
                echo "resilience: transient error on attempt $attempt/$max, retrying in ${sleep_time}s: $*" >&2

            sleep "$sleep_time"
            delay=$((delay * backoff))
        else
            [ "$BD_RETRY_VERBOSE" -eq 1 ] && \
                echo "resilience: exhausted $max attempts: $*" >&2
        fi

        attempt=$((attempt + 1))
    done

    # All retries exhausted — emit last output and return last exit code.
    [ -n "$output" ] && echo "$output"
    return "$rc"
}

# --- Public API ---

# bd_retry <bd-args...>
# Retry a bd command on transient errors. Returns output on success or last failure.
bd_retry() {
    _retry_cmd "$BD_RETRY_MAX" "$BD_RETRY_DELAY" "$BD_RETRY_BACKOFF" "$BD_RETRY_JITTER" bd "$@"
}

# bd_retry_critical <bd-args...>
# Same as bd_retry but exits the calling script (exit 1) if all retries fail.
bd_retry_critical() {
    local output=""
    output=$(bd_retry "$@") || {
        local rc=$?
        echo "resilience: CRITICAL bd command failed after $BD_RETRY_MAX attempts: bd $*" >&2
        [ -n "$output" ] && echo "$output" >&2
        exit "$rc"
    }
    [ -n "$output" ] && echo "$output"
}

# gc_retry <gc-args...>
# Retry a gc command on transient errors.
gc_retry() {
    _retry_cmd "$BD_RETRY_MAX" "$BD_RETRY_DELAY" "$BD_RETRY_BACKOFF" "$BD_RETRY_JITTER" gc "$@"
}

# bd_wait_for <bead-id> [timeout_seconds]
# Wait for a bead to become visible in the store (handles read-after-write lag).
# Returns 0 when found, 1 on timeout.
bd_wait_for() {
    local bead_id="$1"
    local timeout="${2:-10}"
    local elapsed=0
    local delay=1

    while [ "$elapsed" -lt "$timeout" ]; do
        if bd show "$bead_id" >/dev/null 2>&1; then
            return 0
        fi
        sleep "$delay"
        elapsed=$((elapsed + delay))
        # Cap delay at 3s.
        [ "$delay" -lt 3 ] && delay=$((delay + 1))
    done
    return 1
}

# bd_create_and_update <create-args...> -- <update-args...>
# Atomically create a bead and update it, handling read-after-write lag.
# The separator "--" splits create args from update args.
# The created bead ID is substituted for "{ID}" in update args.
bd_create_and_update() {
    local create_args=()
    local update_args=()
    local in_update=false

    for arg in "$@"; do
        if [ "$arg" = "--" ]; then
            in_update=true
            continue
        fi
        if [ "$in_update" = true ]; then
            update_args+=("$arg")
        else
            create_args+=("$arg")
        fi
    done

    # Create the bead.
    local create_output=""
    create_output=$(bd_retry_critical create "${create_args[@]}" --json)
    local bead_id=""
    bead_id=$(echo "$create_output" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())['id'])" 2>/dev/null) || {
        echo "resilience: failed to parse bead ID from create output" >&2
        echo "$create_output"
        return 1
    }

    # Wait for read-after-write consistency.
    if ! bd_wait_for "$bead_id" 10; then
        echo "resilience: bead $bead_id not visible after 10s (read-after-write timeout)" >&2
        return 1
    fi

    # Substitute {ID} in update args.
    local final_update_args=()
    for arg in "${update_args[@]}"; do
        final_update_args+=("${arg//\{ID\}/$bead_id}")
    done

    # Update the bead.
    if [ "${#final_update_args[@]}" -gt 0 ]; then
        bd_retry_critical update "$bead_id" "${final_update_args[@]}"
    fi

    echo "$bead_id"
}

# dolt_is_healthy
# Quick probe: returns 0 if Dolt server responds, 1 otherwise.
dolt_is_healthy() {
    gc dolt status >/dev/null 2>&1
}
