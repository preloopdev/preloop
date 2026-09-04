#!/usr/bin/env bash
# autoresearch.sh — benchmark preloop E2E protocol latency.
#
# Starts preloop on 127.0.0.1:9090 and uses the official runner's development
# service override so the explicit port is preserved.
#
# Submits fixtures/workflows/dogfood.yml and measures wall-clock time from
# submission to the runner's JobCompletedEvent.
#
# Emits:
#   METRIC e2e_latency_ms=<integer>   — submission → runner exit
#   METRIC job_succeeded=<0|1>        — 1 = Succeeded, 0 = other
#
# Exits 0 on success, non-zero on any setup failure.

set -euo pipefail

unset all_proxy ALL_PROXY http_proxy https_proxy HTTP_PROXY HTTPS_PROXY \
      no_proxy NO_PROXY 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PRELOOP_BIN="$REPO_ROOT/target/release/preloop-server"
RUNNER_DIR="${RUNNER_DIR:-$HOME/.cache/actions-runner/current}"
PRELOOP_PORT="${PRELOOP_PORT:-9090}"
CLIENT_URL="${CLIENT_URL:-http://127.0.0.1:$PRELOOP_PORT}"
SYSTEM_TOKEN="${PRELOOP_SYSTEM_TOKEN:-preloop-system-token}"
STATE_DIR="$(mktemp -d /tmp/preloop-bench-XXXXXX)"
LOG="$STATE_DIR/preloop.log"
PRELOOP_PID=""
RUNNER_PID=""

cleanup() {
    local status=$?
    [ -n "$PRELOOP_PID" ]   && kill "$PRELOOP_PID"   2>/dev/null || true
    [ -n "$RUNNER_PID" ] && kill "$RUNNER_PID" 2>/dev/null || true
    wait "$PRELOOP_PID"   2>/dev/null || true
    wait "$RUNNER_PID" 2>/dev/null || true
    if [ "$status" -eq 0 ]; then
        rm -rf "$STATE_DIR"
    else
        echo "Preserved debug state: $STATE_DIR" >&2
    fi
}
trap cleanup EXIT

die() { echo "ERROR: $*" >&2; exit 1; }

json_field() {
    python3 -c "
import json, sys
try:
    print(json.load(sys.stdin)['$1'])
except Exception:
    sys.exit(1)"
}

# ── preflight ─────────────────────────────────────────────────────────────────

[ -f "$PRELOOP_BIN" ]           || die "server binary not found: $PRELOOP_BIN — run: cargo build --release -p preloop-runner-server"
[ -f "$RUNNER_DIR/run.sh" ]  || die "runner not found: $RUNNER_DIR"

lsof -i :"$PRELOOP_PORT" -sTCP:LISTEN >/dev/null 2>&1 \
    && die "port $PRELOOP_PORT already in use; run: lsof -ti:$PRELOOP_PORT | xargs kill"

# ── start preloop ───────────────────────────────────────────────────────────────

PRELOOP_PUBLIC_URL="$CLIENT_URL" PRELOOP_RUNNER_URL="$CLIENT_URL" \
    RUST_LOG=info "$PRELOOP_BIN" serve \
    --listen "127.0.0.1:${PRELOOP_PORT}" \
    --state-dir "$STATE_DIR/state" \
    >> "$LOG" 2>&1 &
PRELOOP_PID=$!

# Wait until preloop logs "listening" (more reliable than nc + sleep)
retries=0
until grep -q "listening" "$LOG" 2>/dev/null; do
    retries=$((retries + 1))
    [ $retries -gt 50 ] && { echo "preloop startup timeout" >&2; cat "$LOG" >&2; exit 1; }
    sleep 0.2
done
# Probe the same origin the runner will use.
python3 -c "
import urllib.request
try:
    urllib.request.urlopen('${CLIENT_URL}/_apis/connectionData?connectOptions=0&lastChangeId=0&lastChangeId64=0', timeout=3)
except Exception as e:
    import sys; print('ERROR: runner origin not reachable:', e, file=sys.stderr); sys.exit(1)
" || die "runner origin unavailable: $CLIENT_URL"

# ── configure runner ─────────────────────────────────────────────────────────

cd "$RUNNER_DIR"
RUNNER_URL="$CLIENT_URL/runner/server"
resp=$(python3 -c "
import urllib.request, json
req = urllib.request.Request(
    '${CLIENT_URL}/api/v3/repos/owner/repo/actions/runners/registration-token',
    data=b'{}',
    headers={
        'Content-Type': 'application/json',
        'Authorization': 'RemoteAuth $SYSTEM_TOKEN',
    },
    method='POST'
)
with urllib.request.urlopen(req, timeout=5) as r:
    print(r.read().decode())
") || resp=""
token=$(printf '%s' "$resp" | json_field token) \
    || die "failed to get registration token: $resp"

# The server uses a fresh state directory for each benchmark, so an existing
# .runner file can refer to a client id the new server does not know.
rm -f .runner .credentials .credentials_rsaparams

# The official runner also caches the Actions service location (access
# mappings keyed by the server's instance GUID) in the SDK client cache.
# Every preloop server shares one stable instance GUID, so a cache written
# against an earlier server on a different port makes the next config resolve
# service locations to that stale origin, and config.sh never reaches this
# server. Wipe it like the other runner state. The root depends on the
# platform's LocalApplicationData mapping.
for cache_root in \
    "$HOME/Library/Application Support/GitHub/ActionsService" \
    "$HOME/.config/GitHub/ActionsService" \
    "$HOME/.local/share/GitHub/ActionsService"; do
    rm -rf "$cache_root"/*/Cache 2>/dev/null || true
done

# Run config.sh directly and capture output in log
USE_DEV_ACTIONS_SERVICE_URL=1 ./config.sh --unattended \
    --url "$RUNNER_URL" \
    --token "$token" \
    --name "preloop-bench" \
    --labels "self-hosted,mitm" \
    --work _work \
    --replace >> "$LOG" 2>&1 \
    || die "runner config failed"

[ -f .runner ] || die "runner config failed: .runner not created"

# ── start runner ─────────────────────────────────────────────────────────────

USE_DEV_ACTIONS_SERVICE_URL=1 ./run.sh >> "$LOG" 2>&1 &
RUNNER_PID=$!
sleep 1   # let runner connect and start long-polling

# ── submit workflow + start clock ────────────────────────────────────────────

T_START=$(python3 -c "import time; print(int(time.time() * 1000))")

resp=$(python3 - <<PYEOF
import json
import pathlib
import urllib.request

repo = pathlib.Path("$REPO_ROOT").resolve()
workflow = repo.joinpath("fixtures", "workflows", "dogfood.yml").read_text()
payload = json.dumps({
    "workflow_yaml": workflow,
    "event": "push",
    "repository": "owner/repo",
    "vars": {
        "PRELOOP_REPO_ROOT": str(repo),
    },
}).encode()
req = urllib.request.Request(
    "$CLIENT_URL/api/v1/runs",
    data=payload,
    headers={
        "Content-Type": "application/json",
        "Authorization": "Bearer $SYSTEM_TOKEN",
    },
    method="POST",
)
with urllib.request.urlopen(req, timeout=10) as r:
    print(r.read().decode())
PYEOF
) || resp=""
RUN_ID=$(printf '%s' "$resp" | json_field run_id) \
    || die "workflow submission failed: $resp"

# ── wait for job completion ──────────────────────────────────────────────────
# completion from the runner terminal line. The server also logs structured
# completion, but the terminal line is stable across failed and succeeded jobs.

deadline=$(python3 -c "import time; print(int(time.time()) + 600)")
while ! grep -Eq "Job .* completed with result:" "$LOG" 2>/dev/null; do
    now=$(python3 -c "import time; print(int(time.time()))")
    [ "$now" -gt "$deadline" ] && { echo "runner timeout after 600s" >&2; exit 1; }
    sleep 0.2
done
# The real official runner is the integration probe for listener-token
# lifecycle fencing. A warning here means the runner used its machine
# credential for renew/complete instead of the job runtime token.
if grep -q "job lifecycle call used the bare listener token" "$LOG" 2>/dev/null; then
    die "official runner used the bare listener token for job lifecycle"
fi


T_END=$(python3 -c "import time; print(int(time.time() * 1000))")
LATENCY_MS=$(( T_END - T_START ))

# Infer success from either the runner terminal line or preloop's structured log.
JOB_SUCCEEDED=0
grep -Eq "completed with result: Succeeded|result=\"succeeded\"" "$LOG" 2>/dev/null && JOB_SUCCEEDED=1
echo "METRIC e2e_latency_ms=${LATENCY_MS}"
echo "METRIC job_succeeded=${JOB_SUCCEEDED}"
[ "$JOB_SUCCEEDED" -eq 1 ] || die "dogfood workflow failed; see $LOG"
