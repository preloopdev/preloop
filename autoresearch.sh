#!/usr/bin/env bash
# autoresearch.sh — benchmark aksh E2E protocol latency.
#
# Prerequisites (one-time sudo setup):
#   ./scripts/e2e-setup.sh         # sets up pfctl redirect 80→9090
#
# Starts aksh on 127.0.0.1:9090, uses the pfctl 80→9090 redirect so the
# runner can reach it on port 80 (the runner strips non-default HTTP ports
# from URLs; the redirect makes port 80 work without root on aksh itself).
#
# Submits a 3-step echo workflow and measures wall-clock time from submission
# to runner exit (job completed).
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
AKSH_BIN="$HOME/rust-runner-server/target/release/aksh-runner-server"
RUNNER_DIR="$HOME/mitm-proxy/experiments/mitm/.cache/runner-official"
AKSH_PORT=9090
# Clients use port 80 via pfctl redirect (runner strips non-default HTTP ports)
CLIENT_URL="http://127.0.0.1:80"
STATE_DIR="$(mktemp -d /tmp/aksh-bench-XXXXXX)"
LOG="$STATE_DIR/aksh.log"
AKSH_PID=""
RUNNER_PID=""

cleanup() {
    [ -n "$AKSH_PID" ]   && kill "$AKSH_PID"   2>/dev/null || true
    [ -n "$RUNNER_PID" ] && kill "$RUNNER_PID" 2>/dev/null || true
    wait "$AKSH_PID"   2>/dev/null || true
    wait "$RUNNER_PID" 2>/dev/null || true
    rm -rf "$STATE_DIR"
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

[ -f "$AKSH_BIN" ]           || die "aksh binary not found: $AKSH_BIN — build: cd ~/rust-runner-server && cargo build --release"
[ -f "$RUNNER_DIR/run.sh" ]  || die "runner not found: $RUNNER_DIR"

lsof -i :"$AKSH_PORT" -sTCP:LISTEN >/dev/null 2>&1 \
    && die "port $AKSH_PORT already in use; run: lsof -ti:$AKSH_PORT | xargs kill"

# Verify port-80 redirect is active by probing it after aksh starts
# (checked after aksh is ready, below)

# ── start aksh ───────────────────────────────────────────────────────────────

RUST_LOG=info "$AKSH_BIN" serve \
    --listen "127.0.0.1:${AKSH_PORT}" \
    --state-dir "$STATE_DIR/state" \
    >> "$LOG" 2>&1 &
AKSH_PID=$!

# Wait until aksh logs "listening" (more reliable than nc + sleep)
retries=0
until grep -q "listening" "$LOG" 2>/dev/null; do
    retries=$((retries + 1))
    [ $retries -gt 50 ] && { echo "aksh startup timeout" >&2; cat "$LOG" >&2; exit 1; }
    sleep 0.2
done
# Probe redirect: port 80 must reach aksh
python3 -c "
import urllib.request
try:
    urllib.request.urlopen('http://127.0.0.1:80/_apis/connectionData?connectOptions=0&lastChangeId=0&lastChangeId64=0', timeout=3)
except Exception as e:
    import sys; print('ERROR: port-80 redirect not reachable:', e, file=sys.stderr); sys.exit(1)
" || die "pfctl redirect not active; run: sudo ./scripts/e2e-setup.sh"

# ── configure runner (skip if already configured for this URL) ───────────────

cd "$RUNNER_DIR"
RUNNER_URL="$CLIENT_URL/runner/server"
EXISTING_URL=$(python3 -c "import json; d=json.load(open('.runner', encoding='utf-8-sig')); print(d.get('serverUrl',''))" 2>/dev/null || echo "")

if [ "$EXISTING_URL" != "$RUNNER_URL" ]; then
    resp=$(python3 -c "
import urllib.request, json
req = urllib.request.Request(
    '${CLIENT_URL}/api/v3/repos/owner/repo/actions/runners/registration-token',
    data=b'{}',
    headers={'Content-Type':'application/json','Authorization':'RemoteAuth test'},
    method='POST'
)
with urllib.request.urlopen(req, timeout=5) as r:
    print(r.read().decode())
") || resp=""
    token=$(printf '%s' "$resp" | json_field token) \
        || die "failed to get registration token: $resp"

    # config.sh needs a TTY to avoid registration timeout; script(1) provides one
    script -q /dev/null ./config.sh --unattended \
        --url "$RUNNER_URL" \
        --token "$token" \
        --name "aksh-bench" \
        --labels "self-hosted,mitm" \
        --work _work \
        --replace >/dev/null 2>&1 \
        || die "runner config failed"

    [ -f .runner ] || die "runner config failed: .runner not created"
fi

# ── start runner ─────────────────────────────────────────────────────────────

./run.sh >> "$LOG" 2>&1 &
RUNNER_PID=$!
sleep 1   # let runner connect and start long-polling

# ── submit workflow + start clock ────────────────────────────────────────────

T_START=$(python3 -c "import time; print(int(time.time() * 1000))")

resp=$(python3 - <<'PYEOF'
import urllib.request, json
payload = json.dumps({
    "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hello from aksh\n      - run: whoami\n      - run: date\n",
    "event": "push",
    "repository": "owner/repo"
}).encode()
req = urllib.request.Request(
    "http://127.0.0.1:80/api/v1/runs",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST"
)
with urllib.request.urlopen(req, timeout=10) as r:
    print(r.read().decode())
PYEOF
) || resp=""
RUN_ID=$(printf '%s' "$resp" | json_field run_id) \
    || die "workflow submission failed: $resp"

# ── wait for job completion ──────────────────────────────────────────────────
# run.status polling is unreliable (job_uuid_to_name lookup bug); detect
# completion from the aksh log line "job completed" instead.

deadline=$(python3 -c "import time; print(int(time.time()) + 90)")
while ! grep -q "job completed" "$LOG" 2>/dev/null; do
    now=$(python3 -c "import time; print(int(time.time()))")
    [ "$now" -gt "$deadline" ] && { echo "runner timeout after 90s" >&2; exit 1; }
    sleep 0.2
done

T_END=$(python3 -c "import time; print(int(time.time() * 1000))")
LATENCY_MS=$(( T_END - T_START ))

# Infer success: "job completed" with result=Succeeded
JOB_SUCCEEDED=0
grep -q "result=Succeeded" "$LOG" 2>/dev/null && JOB_SUCCEEDED=1
echo "METRIC e2e_latency_ms=${LATENCY_MS}"
echo "METRIC job_succeeded=${JOB_SUCCEEDED}"
