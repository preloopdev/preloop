#!/usr/bin/env bash
# run-timing.sh — per-phase and per-step timing breakdown for one preloop run.
#
# Pulls the native run record (the same surface the conformance poller used)
# and prints the phases we optimize against:
#
#   submit → accepted       (server-side submit handling, includes snapshot)
#   accepted → first start  (queue wait)
#   first start → complete  (execution, incl. runner lifecycle steps)
#   snapshot                (capture duration + repository size, when local)
#   per job / per step      (server-stamped start/finish deltas)
#
# Usage: scripts/run-timing.sh [run_id]
#   run_id        default: the most recently created run
#
# Environment:
#   PRELOOP_URL          server base URL (default http://127.0.0.1:9090)
#   PRELOOP_SYSTEM_TOKEN native bearer token (default preloop-system-token)
#
# Emits the same JSON keys the poller captured, plus `snapshot_timing` and
# per-step `started_at`/`finished_at`, so jq consumers can reuse it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PRELOOP_URL="${PRELOOP_URL:-http://127.0.0.1:9090}"
if [ -n "${PRELOOP_SYSTEM_TOKEN:-}" ]; then
    TOKEN="$PRELOOP_SYSTEM_TOKEN"
elif [ -f "${PRELOOP_HOME:-$HOME/.preloop}/engine.token" ]; then
    TOKEN="$(tr -d '[:space:]' < "${PRELOOP_HOME:-$HOME/.preloop}/engine.token")"
else
    TOKEN="preloop-system-token"
fi
RUN_ID="${1:-}"

fetch() {
    curl -sf -H "Authorization: Bearer $TOKEN" "$PRELOOP_URL$1"
}

if [ -z "$RUN_ID" ]; then
    RUN_ID=$(fetch /api/v1/runs | jq -r '.runs[0].run_id // empty' 2>/dev/null \
        || fetch /api/v1/runs | jq -r '.[0].run_id // empty')
    [ -n "$RUN_ID" ] || { echo "ERROR: no runs found at $PRELOOP_URL/api/v1/runs" >&2; exit 1; }
fi

RUN=$(fetch "/api/v1/runs/$RUN_ID") || { echo "ERROR: run $RUN_ID not found" >&2; exit 1; }

fmt_iso() { jq -r "$1 // \"-\"" <<<"$RUN"; }

created=$(fmt_iso '.created_at')
started=$(fmt_iso '.started_at')
completed=$(fmt_iso '.completed_at')

ms_between() {
    python3 -c "
import sys, datetime
def p(v):
    try: return datetime.datetime.fromisoformat(v.replace('Z', '+00:00'))
    except Exception: return None
a, b = p('$1'), p('$2')
print(int((b - a).total_seconds() * 1000) if a and b else '-')
"
}

echo "=== run $RUN_ID ==="
echo "status:      $(jq -r '.status // "-"' <<<"$RUN")"
echo "created:     $created"
echo "started:     $started"
echo "completed:   $completed"
echo "submit→accept:  - (server stamps created_at at acceptance)"
echo "queue wait:  $(ms_between "$created" "$started") ms (accepted → first job start)"
echo "execution:   $(ms_between "$started" "$completed") ms (first start → completion)"

if jq -e '.snapshot_timing' <<<"$RUN" >/dev/null 2>&1; then
    jq -r '"snapshot:    \(.snapshot_timing.duration_ms) ms, \(.snapshot_timing.object_count) objects, \(.snapshot_timing.pack_bytes) bytes pack" ' <<<"$RUN"
else
    echo "snapshot:    not captured (no local workspace or pre-timing server)"
fi

echo
echo "=== jobs ==="
jq -r '.jobs_list[] | [.name, .conclusion, ([.steps[].started_at // empty] | first // "-"), ([.steps[].finished_at // empty] | last // "-")] | @tsv' <<<"$RUN" \
    | while IFS=$'\t' read -r name conclusion first last; do
        span=$(ms_between "$first" "$last")
        printf '%-40s %-10s span=%s ms\n' "$name" "$conclusion" "$span"
    done

echo
echo "=== steps ==="
jq -r '.jobs_list[] | .steps[] | [.name, .conclusion, .started_at, .finished_at] | @tsv' <<<"$RUN" \
    | while IFS=$'\t' read -r name conclusion first last; do
        dur=$(ms_between "$first" "$last")
        printf '%-42s %-10s %8s ms\n' "$name" "$conclusion" "$dur"
    done

echo
echo "METRIC run_id=$RUN_ID status=$(jq -r '.status' <<<"$RUN")"
