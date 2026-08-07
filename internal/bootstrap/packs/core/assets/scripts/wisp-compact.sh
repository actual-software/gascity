#!/usr/bin/env bash
# wisp-compact — TTL-based cleanup of expired ephemeral beads.
#
# Wisps are short-lived work items (heartbeats, pings, patrols) that
# accumulate and bloat the database. This script applies retention policy:
# - Closed wisps past TTL → deleted (Dolt AS OF preserves history)
# - Non-closed wisps past TTL → promoted to permanent (stuck detection)
# - Wisps with comments or "keep" label → promoted (proven value)
#
# TTL by wisp_type label:
#   heartbeat, ping: 6h
#   patrol, gc_report: 24h
#   recovery, error, escalation: 7d
#   default (untyped): 24h
#
# Enumeration runs through `gc bd query ephemeral=true`, which is the only
# listing that returns wisps. `gc bd list` excludes ephemeral beads outright
# and omits the `ephemeral` field from its projection, so the older
# `list | select(.ephemeral == true)` pipeline matched zero records on every
# run and reaped nothing while still exiting 0.
#
# Each run is bounded by a wall-clock budget (GC_WISP_COMPACT_BUDGET, default
# 240s) that fits inside the 300s exec-order deadline. A backlog larger than
# one budget drains across successive hourly runs instead of blowing the
# deadline mid-sweep.
#
# Loud-fail (#4543): the controller logs an exec order's output only on a
# NON-ZERO exit, so anything this script reports on a clean exit is discarded.
# Enumeration failures, an empty ephemeral set, and per-bead action failures
# therefore exit non-zero — otherwise a sweep that silently stops reaping is
# indistinguishable from a healthy one.
#
# Environment knobs:
#   GC_WISP_COMPACT_BUDGET        per-run wall-clock budget, seconds (default 240)
#   GC_WISP_COMPACT_DELETE_CHUNK  ids per batched delete call (default 500)
#   GC_WISP_COMPACT_ALLOW_EMPTY   set to 1 to accept an empty ephemeral set
#
# Runs as an exec order (no LLM, no agent, no wisp).
set -euo pipefail

# Trace bd invocations to $GC_BD_TRACE when set (no-op otherwise).
__SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
. "$__SCRIPT_DIR/_bd_trace.sh" "wisp-compact"

CITY="${GC_CITY:-.}"

BUDGET_SECONDS="${GC_WISP_COMPACT_BUDGET:-240}"
DELETE_CHUNK="${GC_WISP_COMPACT_DELETE_CHUNK:-500}"

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

# Enumerate ephemeral beads. A failure here is loud: the sweep reaping nothing
# because it could not see the wisps is the exact regression this guards.
if ! EPHEMERALS=$(gc bd query --json 'ephemeral=true' --all --limit 0 2>&1); then
    echo "wisp-compact: enumerating ephemeral beads failed: $EPHEMERALS" >&2
    exit 1
fi

NOW=$(date -u +%s)

# Classify every bead in ONE jq pass. The previous per-bead loop spawned five
# jq processes and up to three `date` processes per bead, which is what put the
# sweep past its deadline once the backlog grew.
#
# TTL resolution reduces over the label list so the LAST matching label wins,
# matching the original `for label in $labels; do case ... esac; done`. A bead
# whose timestamp will not parse is dropped rather than aborting the run, which
# is what the original's `|| continue` did.
#
# Output is one record per actionable bead, oldest first, plus a trailing count
# of beads still inside their TTL:
#   DELETE<TAB><id>
#   PROMOTE<TAB><id><TAB><reason>
#   SKIPPED<TAB><n>
if ! printf '%s' "$EPHEMERALS" | jq -r --argjson now "$NOW" '
    def ttl($labels):
      reduce $labels[] as $l (24 * 3600;
        if   $l == "wisp_type:heartbeat"  or $l == "wisp_type:ping"       then 6 * 3600
        elif $l == "wisp_type:patrol"     or $l == "wisp_type:gc_report"  then 24 * 3600
        elif $l == "wisp_type:recovery"   or $l == "wisp_type:error"
          or $l == "wisp_type:escalation"                                 then 7 * 24 * 3600
        elif $l == "keep"                                                 then 0
        else . end);

    def epoch($s):
      try ($s | if test("Z$") then . else . + "Z" end | fromdateiso8601) catch null;

    [ .[]
      | . as $b
      # Defensive, and deliberately NOT `== true`. The query already filters
      # server-side, so this only guards a stray non-ephemeral row. Testing
      # `!= false` keeps a bead whose `ephemeral` field is absent, where
      # `== true` would silently drop every row the moment a projection stops
      # emitting that field, which is the exact failure this change undoes.
      | select($b.ephemeral != false)
      | (if ($b.labels | type) == "array" then $b.labels else [] end) as $labels
      | (ttl($labels)) as $ttl
      | (epoch($b.updated_at // $b.created_at)) as $ts
      | select($ts != null)
      | ($now - $ts) as $age
      | if $ttl > 0 and $age < $ttl then
          { verdict: "SKIP", age: $age }
        elif (($b.comment_count // 0) > 0) or ($labels | index("keep")) or ($b.status != "closed") then
          { verdict: "PROMOTE", id: $b.id, age: $age,
            reason: (if $b.status != "closed"
                     then "open past TTL (stuck detection)"
                     else "proven value" end) }
        else
          { verdict: "DELETE", id: $b.id, age: $age }
        end
    ] as $all
    | ( [$all[] | select(.verdict == "DELETE")]  | sort_by(-.age) ) as $del
    | ( [$all[] | select(.verdict == "PROMOTE")] | sort_by(-.age) ) as $prom
    | ( [$all[] | select(.verdict == "SKIP")]    | length )         as $skipped
    | ( $del[]  | "DELETE\t\(.id)" ),
      ( $prom[] | "PROMOTE\t\(.id)\t\(.reason)" ),
      "SKIPPED\t\($skipped)"
' > "$WORKDIR/plan" 2>"$WORKDIR/plan.err"; then
    echo "wisp-compact: classifying ephemeral beads failed: $(cat "$WORKDIR/plan.err")" >&2
    exit 1
fi

awk -F'\t' '$1 == "DELETE"  { print $2 }'          "$WORKDIR/plan" > "$WORKDIR/delete-ids"
awk -F'\t' '$1 == "PROMOTE" { print $2 "\t" $3 }'  "$WORKDIR/plan" > "$WORKDIR/promote"
SKIPPED=$(awk -F'\t' '$1 == "SKIPPED" { print $2 }' "$WORKDIR/plan")
DELETE_TOTAL=$(wc -l < "$WORKDIR/delete-ids" | tr -d ' ')
PROMOTE_TOTAL=$(wc -l < "$WORKDIR/promote" | tr -d ' ')

# An empty ENUMERATION is loud unless the operator opts out. A city running
# exec orders always holds at least the order-tracking wisp for the dispatch
# that invoked this script, so zero rows means the query stopped seeing wisps,
# the failure that masked itself as a healthy sweep for two days.
#
# The test is on the raw row count, NOT on how many beads turned out to be
# actionable. A sweep that legitimately finds every wisp still inside its TTL
# has nothing to do and should stay quiet; a sweep that cannot see wisps at all
# is broken. Conflating the two would make the quiet-and-correct case shout.
RAW_COUNT=$(printf '%s' "$EPHEMERALS" | jq 'length' 2>/dev/null || echo 0)
if [ "${RAW_COUNT:-0}" -eq 0 ]; then
    if [ "${GC_WISP_COMPACT_ALLOW_EMPTY:-}" = "1" ]; then
        exit 0
    fi
    echo "wisp-compact: gc bd query ephemeral=true returned no beads; the sweep can see no wisps to retain, promote, or delete (set GC_WISP_COMPACT_ALLOW_EMPTY=1 if this city genuinely has none)" >&2
    exit 1
fi

DEADLINE=$((NOW + BUDGET_SECONDS))
# Promotions run first against half the budget, deletions against all of it.
# Splitting this way keeps either phase from starving the other: a large
# delete backlog cannot consume the whole run before a stuck wisp is promoted,
# and promotions finishing early hand the remainder straight back to deletion.
PROMOTE_DEADLINE=$((NOW + BUDGET_SECONDS / 2))
PROMOTED=0
DELETED=0
FAILED=0

# Promotions stay per-bead: each needs its own --persistent flip and its own
# comment, and they are rare next to the deletions.
while IFS=$'\t' read -r id reason; do
    [ -z "$id" ] && continue
    [ "$(date -u +%s)" -ge "$PROMOTE_DEADLINE" ] && break
    if gc bd update "$id" --persistent >/dev/null 2>&1; then
        gc bd comment "$id" "Promoted from wisp: $reason" >/dev/null 2>&1 || true
        PROMOTED=$((PROMOTED + 1))
    else
        FAILED=$((FAILED + 1))
        echo "wisp-compact: promoting $id failed" >&2
    fi
done < "$WORKDIR/promote"

# Deleted in batches. `gc bd delete --from-file` takes the whole chunk in one
# call, which matters because each `gc bd` invocation costs roughly a second of
# process startup regardless of how much work it does.
CHUNK_START=1
while [ "$CHUNK_START" -le "$DELETE_TOTAL" ]; do
    [ "$(date -u +%s)" -ge "$DEADLINE" ] && break
    CHUNK_END=$((CHUNK_START + DELETE_CHUNK - 1))
    sed -n "${CHUNK_START},${CHUNK_END}p" "$WORKDIR/delete-ids" > "$WORKDIR/chunk"
    CHUNK_COUNT=$(wc -l < "$WORKDIR/chunk" | tr -d ' ')
    [ "$CHUNK_COUNT" -eq 0 ] && break
    if gc bd delete --from-file "$WORKDIR/chunk" --force >/dev/null 2>"$WORKDIR/delete.err"; then
        DELETED=$((DELETED + CHUNK_COUNT))
    else
        # Stop on the first failed chunk. A batch delete that fails once
        # almost always fails identically on the next chunk, and retrying all
        # of them turns one fault into one log line per chunk.
        FAILED=$((FAILED + (DELETE_TOTAL - DELETED)))
        echo "wisp-compact: batch delete failed after $DELETED deletions, abandoning $((DELETE_TOTAL - DELETED)) this sweep: $(cat "$WORKDIR/delete.err")" >&2
        break
    fi
    CHUNK_START=$((CHUNK_END + 1))
done

REMAINING=$(( (DELETE_TOTAL - DELETED) + (PROMOTE_TOTAL - PROMOTED) - FAILED ))
[ "$REMAINING" -lt 0 ] && REMAINING=0

echo "wisp-compact: promoted=$PROMOTED deleted=$DELETED skipped=$SKIPPED remaining=$REMAINING"

if [ "$FAILED" -gt 0 ]; then
    echo "wisp-compact: $FAILED wisps could not be actioned; will retry next sweep" >&2
    exit 1
fi
