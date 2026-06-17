#!/usr/bin/env bash
# escalation — shared helper for ESCALATION mails from maintenance scripts.
#
# Wraps `gc mail send` with three layers:
#   (1) per-(subject,body) dedupe state — sha256(subject + body) is the key,
#       a configurable cooldown (default 6h, GC_ESCALATION_COOLDOWN_SECONDS
#       env override) suppresses repeats. Suppressed sends increment a counter
#       that the next fresh send appends as a footer so the operator sees the
#       cadence.
#   (2) best-effort bd labelling — after a successful send, parse the new
#       bead id from `gc mail send`'s "Sent message <id> to <to>" stdout
#       line and apply wisp_type:escalation so wisp-compact treats the bead
#       as long-lived (7d) instead of the default (24h).
#   (3) auto-clear hook — callers signal "underlying condition cleared" by
#       invoking clear_escalation_state, which drops dedupe entries so the
#       next firing escalates fresh.
#
# This file is sourced by reaper.sh and jsonl-export.sh; do not run it
# directly.

# Wisp-compact (packs/maintenance/assets/scripts/wisp-compact.sh) treats
# beads carrying this label as the 7d "escalation" class. Without it,
# escalation mails fall into the 24h default bucket and get reaped early.
ESCALATION_LABEL="wisp_type:escalation"

# Default cooldown is 6 hours; operators can tune per-factory via
# GC_ESCALATION_COOLDOWN_SECONDS without editing the scripts.
_escalation_cooldown_seconds() {
    local raw="${GC_ESCALATION_COOLDOWN_SECONDS:-21600}"
    case "$raw" in
        ''|*[!0-9]*)
            printf '%s\n' "21600"
            ;;
        *)
            printf '%s\n' "$raw"
            ;;
    esac
}

# sha256 over the subject and body. Output is just the hex digest, no
# trailing filename. Available on macOS (shasum) and Linux (sha256sum);
# falls back to openssl if neither is on PATH.
_escalation_key() {
    local subject="$1"
    local body="$2"
    local payload
    payload=$(printf '%s\n%s' "$subject" "$body")
    if command -v sha256sum >/dev/null 2>&1; then
        printf '%s' "$payload" | sha256sum | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        printf '%s' "$payload" | shasum -a 256 | awk '{print $1}'
    elif command -v openssl >/dev/null 2>&1; then
        printf '%s' "$payload" | openssl dgst -sha256 | awk '{print $NF}'
    else
        # Last-resort: hash via cksum — collision-prone but better than
        # disabling dedupe entirely. We log to stderr so a smoke-test env
        # missing all of {sha256sum,shasum,openssl} is visible.
        echo "escalation: sha256 unavailable, falling back to cksum (dedupe weakened)" >&2
        printf '%s' "$payload" | cksum | awk '{print $1 "-" $2}'
    fi
}

# Read the state JSON object from a path, returning '{}' if the file is
# missing or unparseable. Kept independent of jsonl-export's read_state_json
# so reaper.sh (which has no state-IO helpers of its own) can use it too.
_escalation_read_state() {
    local path="$1"
    if [ -f "$path" ] && command -v jq >/dev/null 2>&1; then
        if jq -e 'type == "object"' "$path" >/dev/null 2>&1; then
            cat "$path"
            return
        fi
    fi
    printf '%s\n' '{}'
}

# Atomic JSON state write: tmp file in the same dir, then mv. Mirrors the
# pattern jsonl-export.sh uses so concurrent ticks can't tear the file.
_escalation_write_state() {
    local path="$1"
    local content="$2"
    local dir
    local tmpfile
    dir=$(dirname "$path")
    mkdir -p "$dir" 2>/dev/null || true
    if ! tmpfile=$(mktemp "${path}.tmp.XXXXXX" 2>/dev/null); then
        echo "escalation: failed to create tmp state file under $dir" >&2
        return 1
    fi
    if ! printf '%s\n' "$content" > "$tmpfile"; then
        rm -f "$tmpfile"
        return 1
    fi
    if ! mv -f "$tmpfile" "$path"; then
        rm -f "$tmpfile"
        return 1
    fi
}

# Convert an RFC3339 UTC timestamp to a unix-epoch second count. Tolerates
# the macOS and GNU date dialects; on failure echoes 0 so the caller sees
# "very old, cooldown elapsed".
_escalation_epoch() {
    local ts="$1"
    [ -z "$ts" ] && { printf '%s\n' "0"; return; }
    date -d "$ts" +%s 2>/dev/null \
        || date -ju -f "%Y-%m-%dT%H:%M:%SZ" "$ts" +%s 2>/dev/null \
        || printf '%s\n' "0"
}

# Send an ESCALATION mail to mayor/, deduped against a script-local state
# JSON. The caller chooses the state file so two scripts (reaper.sh and
# jsonl-export.sh) cannot cross-suppress each other's escalations.
#
# Args:
#   $1 — state file path (will be created with mode-default perms if absent)
#   $2 — subject (used both as the mail subject and as part of the dedupe key)
#   $3 — body
#
# Returns:
#   0 if a mail was sent (fresh or released-after-suppression).
#   1 if the mail was suppressed by the cooldown.
#   2 on a tooling failure (jq missing, gc unreachable, etc.). In this case
#     the dedupe state is not updated, so the next tick will retry.
send_escalation_mail() {
    local state_file="$1"
    local subject="$2"
    local body="$3"
    local cooldown
    local key
    local now_ts
    local now_iso
    local state_json
    local last_ts
    local last_epoch
    local suppressed_count
    local final_body
    local sent_line
    local bead_id

    if ! command -v jq >/dev/null 2>&1; then
        echo "escalation: jq required but missing; sending without dedupe" >&2
        gc mail send mayor/ -s "$subject" -m "$body" 2>/dev/null || return 2
        return 0
    fi

    cooldown=$(_escalation_cooldown_seconds)
    key=$(_escalation_key "$subject" "$body")
    now_ts=$(date -u +%s)
    now_iso=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    state_json=$(_escalation_read_state "$state_file")

    last_ts=$(printf '%s' "$state_json" | jq -r --arg k "$key" '.escalations[$k].last_sent_at // ""')
    suppressed_count=$(printf '%s' "$state_json" | jq -r --arg k "$key" '.escalations[$k].suppressed_count // 0')

    if [ -n "$last_ts" ]; then
        last_epoch=$(_escalation_epoch "$last_ts")
        if [ "$((now_ts - last_epoch))" -lt "$cooldown" ]; then
            # Still inside the cooldown window — bump the suppressed counter
            # and return without sending. The next caller outside the window
            # will report the cadence in the released-after-suppression footer.
            state_json=$(
                printf '%s' "$state_json" \
                    | jq -c \
                        --arg k "$key" \
                        --arg subject "$subject" \
                        --argjson n "$((suppressed_count + 1))" \
                        '.escalations[$k] = (
                            (.escalations[$k] // {})
                            + { subject: $subject, suppressed_count: $n }
                        )'
            ) || return 2
            _escalation_write_state "$state_file" "$state_json" || return 2
            return 1
        fi
    fi

    # Cooldown elapsed (or first send). Build the final body: if we had any
    # suppressed sends, prepend a one-line footer naming the cadence.
    final_body="$body"
    if [ "$suppressed_count" -gt 0 ] && [ -n "$last_ts" ]; then
        final_body="$body

[Suppressed $suppressed_count time(s) since $last_ts; cooldown ${cooldown}s.]"
    fi

    if ! sent_line=$(gc mail send mayor/ -s "$subject" -m "$final_body" 2>/dev/null); then
        # Mail send itself failed. Leave dedupe state alone so the next tick
        # retries fresh; do not increment suppressed_count (that would lie
        # about cadence). Return 2 so the caller can distinguish "tooling
        # broke" from "intentionally suppressed".
        return 2
    fi

    # Reset state for this key: counter back to zero, last_sent_at now.
    state_json=$(
        printf '%s' "$state_json" \
            | jq -c \
                --arg k "$key" \
                --arg subject "$subject" \
                --arg at "$now_iso" \
                '.escalations[$k] = { subject: $subject, last_sent_at: $at, suppressed_count: 0 }'
    ) || return 2
    _escalation_write_state "$state_file" "$state_json" || return 2

    # Best-effort labelling. `gc mail send` writes "Sent message <id> to <to>"
    # to stdout; parse the id and apply wisp_type:escalation so wisp-compact
    # treats this bead as the 7d retention class.
    bead_id=$(printf '%s\n' "$sent_line" | awk '/^Sent message / {print $3; exit}')
    if [ -n "$bead_id" ]; then
        bd label add "$bead_id" "$ESCALATION_LABEL" >/dev/null 2>&1 || true
    fi
    return 0
}

# Drop dedupe state for a script. If a subject is provided, only entries
# whose stored subject matches are removed; without a subject, all dedupe
# entries are cleared. Callers invoke this when they know the underlying
# condition is no longer active (reaper sees empty anomalies; jsonl-export
# pushes successfully or clears a pending spike).
clear_escalation_state() {
    local state_file="$1"
    local subject_filter="${2:-}"
    local state_json
    local updated

    if [ ! -f "$state_file" ] || ! command -v jq >/dev/null 2>&1; then
        return 0
    fi
    state_json=$(_escalation_read_state "$state_file")
    if [ -z "$subject_filter" ]; then
        updated=$(printf '%s' "$state_json" | jq -c 'del(.escalations)')
    else
        updated=$(
            printf '%s' "$state_json" \
                | jq -c \
                    --arg subject "$subject_filter" \
                    '
                    if (.escalations // {}) | length == 0 then
                        .
                    else
                        .escalations |= with_entries(select(.value.subject != $subject))
                        | if (.escalations // {}) == {} then
                              del(.escalations)
                          else
                              .
                          end
                    end
                    '
        )
    fi
    [ -z "$updated" ] && return 0
    _escalation_write_state "$state_file" "$updated" || return 1
}
