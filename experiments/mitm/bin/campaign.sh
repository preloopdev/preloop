#!/usr/bin/env bash
# campaign.sh — capture one conformance scenario for one golden cell.
#
#   cells:   gh-official | gh-preloop | pl-official | pl-preloop
#   usage:   campaign.sh --cell <cell> --scenario <name> [options]
#
# gh-* cells: official runner binary or preloop-runner registers to GitHub
#   (GH_REPO) through mitmdump on the host; capture = runner-side flows.jsonl.
# pl-* cells: preloop-server on the host records its own wire
#   (--record-flows); runners reach it on <LAN_IP>:80 (relay → :9191) for the
#   official runner (which strips non-default ports) or <LAN_IP>:9191 for
#   preloop-runner.
#
# Runner work happens inside a smolvm VM ($VM). Host orchestrates dispatch,
# wait, artifact writes. Mirrors the v2.337.0 golden contract:
#   <OUT>/<cell>/<scenario>/{flows.jsonl, summary.json, run-result.json}
set -euo pipefail

CELL="" SCENARIO=""
VM="${VM:-camp-gh}"
GH_REPO="${GH_REPO:-Bnjoroge1/conformance-v2337}"
GH_REF="${GH_REF:-main}"
RUNNER_VERSION="${RUNNER_VERSION:-2.338.0}"
MITM_PORT="${MITM_PORT:-18080}"
PRELOOP_PORT="${PRELOOP_PORT:-9191}"
RELAY_PORT="${RELAY_PORT:-80}"
OUT_ROOT="${OUT_ROOT:-$PWD/golden/v$RUNNER_VERSION}"
SCEN_ROOT="${SCEN_ROOT:-$PWD/experiments/mitm/scenarios}"
SERVER_BIN="${SERVER_BIN:-$PWD/target/release/preloop-server}"
STATE_ROOT="${STATE_ROOT:-$PWD/campaign/state}"
RUN_LOG_ROOT="${RUN_LOG_ROOT:-$PWD/campaign/logs}"
MITM_CONFDIR="${MITM_CONFDIR:-$PWD/.cache/mitmproxy}"
PRELOOP_SYSTEM_TOKEN="${PRELOOP_SYSTEM_TOKEN:-campaign-$(date +%s)}"
WATCH_DEADLINE="${WATCH_DEADLINE:-420}"

while [ $# -gt 0 ]; do
  case "$1" in
    --cell) CELL="$2"; shift 2 ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
    --vm) VM="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
case "$CELL" in
  gh-official|gh-preloop|pl-official|pl-preloop) ;;
  *) echo "usage: $0 --cell {gh-official|gh-preloop|pl-official|pl-preloop} --scenario <name>" >&2; exit 2 ;;
esac
SCEN_DIR="$SCEN_ROOT/$SCENARIO"
[ -f "$SCEN_DIR/scenario.toml" ] || { echo "no scenario.toml in $SCEN_DIR" >&2; exit 1; }

# backend/runner-kind mapping
case "$CELL" in
  gh-official)  BACKEND=official; KIND=official; KINDVER="$RUNNER_VERSION" ;;
  gh-preloop)   BACKEND=official; KIND=preloop;  KINDVER="preloop-runner" ;;
  pl-official)  BACKEND=preloop;  KIND=official; KINDVER="$RUNNER_VERSION" ;;
  pl-preloop)   BACKEND=preloop;  KIND=preloop;  KINDVER="preloop-runner" ;;
esac


STARTED_AT="$(date -u +%Y%m%dT%H%M%SZ)"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$OUT_ROOT/$CELL/$SCENARIO"
RUN_DIR="$RUN_LOG_ROOT/$CELL/$SCENARIO-$TS"
mkdir -p "$OUT" "$RUN_DIR" "$STATE_ROOT"
export MITM_CAPTURE_DIR="$RUN_DIR/mitm"
LAN_IP="${LAN_IP:-$(ipconfig getifaddr en1 2>/dev/null || ipconfig getifaddr en0 2>/dev/null || ifconfig 2>/dev/null | awk '/inet / && $2 !~ /^127\./ {print $2; exit}')}"
[ -n "$LAN_IP" ] || { echo "cannot determine host LAN IP (set LAN_IP)" >&2; exit 1; }
STATUS="ok" RUNNER_EXIT=0 WATCH_EXIT=0
FLOWS_COUNT=0 SERVER_FLOWS=0
RUN_ID=""

log() { echo "[$(date +%H:%M:%S)] $*"; }
vexec() { smolvm machine exec --name "$VM" -- bash -lc "$*"; }

cleanup_gh() {
  # Cancel queued/in-progress runs and delete offline self-hosted runners so
  # the next capture cannot acquire a stale job (quarantine.toml history).
  gh run list -R "$GH_REPO" -L 30 --json databaseId,status \
    -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null \
    | while read -r rid; do gh run cancel "$rid" -R "$GH_REPO" >/dev/null 2>&1 || true; done
  gh api "repos/$GH_REPO/actions/runners" --paginate \
    --jq '.runners[] | select(.status == "offline") | .id' 2>/dev/null \
    | while read -r rid; do gh api -X DELETE "repos/$GH_REPO/actions/runners/$rid" >/dev/null 2>&1 || true; done
}

start_mitm() {
  mkdir -p "$MITM_CONFDIR"
  mitmdump --quiet --listen-host 0.0.0.0 --listen-port "$MITM_PORT" \
    --set confdir="$MITM_CONFDIR" \
    -s "$PWD/experiments/mitm/addons/capture.py" \
    --save-stream-file "$RUN_DIR/flows.mitm" > "$RUN_DIR/mitmdump.log" 2>&1 &
  MITM_PID=$!
  echo "$MITM_PID" > "$RUN_DIR/mitmdump.pid"
  for i in $(seq 1 40); do
    curl -fsS --connect-timeout 1 --max-time 2 --proxy "http://127.0.0.1:$MITM_PORT" http://mitm.it/ >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "mitmproxy did not start" >&2; return 4
}

stop_mitm() {
  [ -f "$RUN_DIR/mitmdump.pid" ] && kill -INT "$(cat "$RUN_DIR/mitmdump.pid")" 2>/dev/null || true
  sleep 1
}

start_preloop() {
  local runner_url="$1"
  rm -f "$STATE_ROOT/current/flows.jsonl"
  # Isolate from the operator's real ~/.preloop config (sealed secrets under a
  # different key fail the load) and from the OS keychain.
  : > "$STATE_ROOT/empty-config.toml"
  mkdir -p "$STATE_ROOT/current"
  PRELOOP_CONFIG="$STATE_ROOT/empty-config.toml" \
    preloop store migrate --store "sqlite://$STATE_ROOT/current/preloop.db" >/dev/null 2>&1 || true
  PRELOOP_CONFIG="$STATE_ROOT/empty-config.toml" \
  PRELOOP_CREDENTIAL_STORE=memory \
  PRELOOP_SYSTEM_TOKEN="$PRELOOP_SYSTEM_TOKEN" \
  PRELOOP_REGISTRATION_POLICY=permissive \
  PRELOOP_PUBLIC_URL="http://$LAN_IP:$PRELOOP_PORT" \
  PRELOOP_RUNNER_URL="$runner_url" \
    "$SERVER_BIN" serve --listen "127.0.0.1:$PRELOOP_PORT" \
      --state-dir "$STATE_ROOT/current" \
      --record-flows "$STATE_ROOT/current/flows.jsonl" \
      > "$RUN_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  echo "$SERVER_PID" > "$RUN_DIR/server.pid"
  # Loopback-only permissive policy: relay LAN :9191 → loopback so the VMs
  # can reach the API (and the official runner can reach :80 → :9191).
  socat TCP-LISTEN:$PRELOOP_PORT,fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:$PRELOOP_PORT \
    > "$RUN_DIR/socat-relay.log" 2>&1 &
  RELAY_PID=$!
  echo "$RELAY_PID" > "$RUN_DIR/relay.pid"
  for i in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:$PRELOOP_PORT/healthz" >/dev/null 2>&1 && return 0
    kill -0 "$SERVER_PID" 2>/dev/null || { tail -20 "$RUN_DIR/server.log" >&2; return 1; }
    sleep 0.5
  done
  echo "preloop-server did not become ready" >&2; return 1
}

stop_preloop() {
  [ -f "$RUN_DIR/server.pid" ] && kill -INT "$(cat "$RUN_DIR/server.pid")" 2>/dev/null || true
  [ "${RELAY_PID:-}" ] && kill "$RELAY_PID" 2>/dev/null || true
  if [ "${USE_RELAY:-}" = 1 ]; then sudo -n kill "$SUDO_SOCAT_PID" 2>/dev/null || true; fi
  sleep 1
}

vm_runner_env() {
  cat <<ENVEOF
https_proxy=http://$LAN_IP:$MITM_PORT
HTTPS_PROXY=http://$LAN_IP:$MITM_PORT
http_proxy=http://$LAN_IP:$MITM_PORT
HTTP_PROXY=http://$LAN_IP:$MITM_PORT
no_proxy=
NO_PROXY=
GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1
GIT_SSL_NO_VERIFY=true
NODE_EXTRA_CA_CERTS=/etc/mitmproxy/mitmproxy-ca-cert.pem
SSL_CERT_FILE=/etc/mitmproxy/mitmproxy-ca-cert.pem
ENVEOF
}

gh_regtoken() {
  gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token
}

find_run_id() {
  grep -oE "run id: [A-Za-z0-9_-]+" "$RUN_DIR/scenario.log" | tail -1 | awk '{print $3}' || true
}

project_gh_run_result() {
  local rid="$1"
  gh run view "$rid" -R "$GH_REPO" --json conclusion,jobs -q '.' 2>/dev/null \
    | python3 -c '
import json,sys
d = json.load(sys.stdin)
out = {"conclusion": d.get("conclusion")}
out["jobs"] = [
  {"name": j.get("name"), "conclusion": j.get("conclusion"),
   "steps": [{"name": s.get("name"), "conclusion": s.get("conclusion"), "number": s.get("number")} for s in j.get("steps", [])]}
  for j in d.get("jobs", [])]
print(json.dumps(out))'
}

wait_pl_terminal() {
  local rid="$1" deadline=$(( $(date +%s) + WATCH_DEADLINE ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    local body
    body=$(curl -fsS -H "Authorization: Bearer $PRELOOP_SYSTEM_TOKEN" \
      "http://127.0.0.1:$PRELOOP_PORT/api/v1/runs/$rid" 2>/dev/null || echo '{}')
    local status
    status=$(python3 -c 'import json,sys; d=json.loads(sys.stdin.read() or "{}"); print(d.get("status") or d.get("conclusion") or "")' <<<"$body" 2>/dev/null || echo "")
    case "$status" in
      completed|success|failure|cancelled|canceled|failed|neutral|skipped|timed_out|action_required|stale)
        printf '%s' "$body"; return 0 ;;
    esac
    sleep 3
  done
  return 1
}

RUNNER_NAME="conf-$CELL-$SCENARIO-$(date +%s | tail -c 7)"

run_gh_cell() {
  cleanup_gh
  start_mitm
  local token; token=$(gh_regtoken)

  # Generated guest script — runs as the `runner` user (official config.sh
  # refuses root/sudo; preloop-runner doesn't care).
  cat > "$RUN_DIR/setup-runner.sh" <<SCRIPT
#!/bin/bash
set -uo pipefail
$(vm_runner_env | sed 's/^/export /')
export HOME=/home/runner
SCRIPT
  if [ "$KIND" = official ]; then
    cat >> "$RUN_DIR/setup-runner.sh" <<SCRIPT
cd /opt/runner/actions-runner
rm -f .runner .credentials .credentials_rsaparams
cat > .env <<'ENVEOF'
$(vm_runner_env)
ENVEOF
./config.sh --unattended --replace \\
  --url https://github.com/$GH_REPO --token '$token' \\
  --name '$RUNNER_NAME' --labels self-hosted,mitm --work _work \\
  > \$HOME/config.log 2>&1 || { cat \$HOME/config.log; exit 1; }
nohup ./run.sh > \$HOME/runner.log 2>&1 &
SCRIPT
  else
    cat >> "$RUN_DIR/setup-runner.sh" <<SCRIPT
cd /opt/plrunner
rm -f .runner .credentials .runner.json
./preloop-runner configure \\
  --url https://github.com/$GH_REPO --token '$token' \\
  --name '$RUNNER_NAME' --labels self-hosted,mitm --work _work \\
  --unattended --replace \\
  > \$HOME/config.log 2>&1 || { cat \$HOME/config.log; exit 1; }
nohup ./preloop-runner run --via broker > \$HOME/runner.log 2>&1 &
SCRIPT
  fi
  echo 'echo "runner started pid=$!" >> $HOME/runner.log' >> "$RUN_DIR/setup-runner.sh"
  smolvm machine cp "$RUN_DIR/setup-runner.sh" "$VM:/tmp/setup-runner.sh"
  vexec "chmod +x /tmp/setup-runner.sh && su runner -s /bin/bash -c /tmp/setup-runner.sh" \
    > "$RUN_DIR/runner-config.log" 2>&1 || STATUS=config_failed

  if [ "$STATUS" = ok ]; then
    GITHUB_OWNER="${GH_REPO%%/*}" GITHUB_REPO="${GH_REPO##*/}" GITHUB_REF="$GH_REF" \
      timeout "$WATCH_DEADLINE" \
      "$PWD/experiments/mitm/bin/_run_scenario.py" \
        --backend official --scenario "$SCEN_DIR/scenario.toml" \
        --capture-dir "$MITM_CAPTURE_DIR" --mitm-dir "$PWD/experiments/mitm" \
        --run true > "$RUN_DIR/scenario.log" 2>&1 || STATUS=scenario_failed
    RUN_ID=$(find_run_id)
    if [ -n "$RUN_ID" ]; then
      gh run watch "$RUN_ID" -R "$GH_REPO" --exit-status --interval 10 \
        > "$RUN_DIR/watch.log" 2>&1 || WATCH_EXIT=$?
      project_gh_run_result "$RUN_ID" > "$OUT/run-result.json" 2>/dev/null || true
    fi
  fi

  vexec "pkill -INT -f 'Runner.Listener|preloop-runner run' 2>/dev/null; sleep 1; pkill -KILL -f 'Runner.Listener|Runner.Worker|preloop-runner' 2>/dev/null; exit 0" || true
  if [ "$KIND" = official ]; then
    local rtok; rtok=$(gh_regtoken 2>/dev/null || true)
    [ -n "$rtok" ] && vexec "su runner -s /bin/bash -c 'cd /opt/runner/actions-runner && ./config.sh remove --unattended --token $rtok' >/dev/null 2>&1 || true" || true
  else
    vexec "su runner -s /bin/bash -c 'cd /opt/plrunner && ./preloop-runner remove --token $token' >/dev/null 2>&1 || true" 2>/dev/null || true
  fi
  cleanup_gh
  stop_mitm
  [ -d "$MITM_CAPTURE_DIR" ] && cp "$MITM_CAPTURE_DIR/flows.jsonl" "$OUT/flows.jsonl" 2>/dev/null || true
  RUNNER_EXIT=$(vexec "test -f /home/runner/runner.log; echo \$?" 2>/dev/null || echo 0)
}

# ---------------------------------------------------------------- pl cells
run_pl_cell() {
  rm -rf "$STATE_ROOT/current"; mkdir -p "$STATE_ROOT/current"
  local runner_url
  if [ "$KIND" = official ]; then
    runner_url="http://$LAN_IP"          # port stripped by config.sh → :80 relay
    USE_RELAY=1
    sudo -n socat TCP-LISTEN:$RELAY_PORT,fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:$PRELOOP_PORT \
      > "$RUN_DIR/socat.log" 2>&1 &
    SUDO_SOCAT_PID=$!
  else
    runner_url="http://$LAN_IP:$PRELOOP_PORT"
  fi
  start_preloop "$runner_url"

  cat > "$RUN_DIR/setup-runner.sh" <<SCRIPT
#!/bin/bash
set -uo pipefail
export HOME=/home/runner
SCRIPT
  if [ "$KIND" = official ]; then
    cat >> "$RUN_DIR/setup-runner.sh" <<SCRIPT
cd /opt/runner/actions-runner
rm -f .runner .credentials .credentials_rsaparams .env
./config.sh --unattended --replace \\
  --url http://$LAN_IP --token '$PRELOOP_SYSTEM_TOKEN' \\
  --name '$RUNNER_NAME' --labels self-hosted,mitm --work _work \\
  > \$HOME/config.log 2>&1 || { cat \$HOME/config.log; exit 1; }
nohup ./run.sh > \$HOME/runner.log 2>&1 &
SCRIPT
  else
    cat >> "$RUN_DIR/setup-runner.sh" <<SCRIPT
cd /opt/plrunner
rm -f .runner .credentials .runner.json
./preloop-runner configure \\
  --url http://$LAN_IP:$PRELOOP_PORT --token '$PRELOOP_SYSTEM_TOKEN' \\
  --name '$RUNNER_NAME' --labels self-hosted,mitm --work _work \\
  --unattended --replace \\
  > \$HOME/config.log 2>&1 || { cat \$HOME/config.log; exit 1; }
nohup ./preloop-runner run --via broker > \$HOME/runner.log 2>&1 &
SCRIPT
  fi
  smolvm machine cp "$RUN_DIR/setup-runner.sh" "$VM:/tmp/setup-runner.sh"
  vexec "chmod +x /tmp/setup-runner.sh && su runner -s /bin/bash -c /tmp/setup-runner.sh" \
    > "$RUN_DIR/runner-config.log" 2>&1 || STATUS=config_failed

  if [ "$STATUS" = ok ]; then
    sleep 2   # let the session register before the job lands
    PRELOOP_API_URL="http://127.0.0.1:$PRELOOP_PORT" \
    PRELOOP_SYSTEM_TOKEN="$PRELOOP_SYSTEM_TOKEN" \
      timeout "$WATCH_DEADLINE" \
      "$PWD/experiments/mitm/bin/_run_scenario.py" \
        --backend preloop --scenario "$SCEN_DIR/scenario.toml" \
        --capture-dir "$STATE_ROOT/current" --mitm-dir "$PWD/experiments/mitm" \
        --run true > "$RUN_DIR/scenario.log" 2>&1 || STATUS=scenario_failed
    RUN_ID=$(find_run_id)
    if [ -n "$RUN_ID" ]; then
      if body=$(wait_pl_terminal "$RUN_ID"); then
        WATCH_EXIT=0
        printf '%s' "$body" | python3 -m json.tool > "$OUT/run-result.json" 2>/dev/null \
          || printf '%s' "$body" > "$OUT/run-result.json"
      else
        WATCH_EXIT=1
        STATUS=watch_timeout
      fi
    else
      WATCH_EXIT=1
    fi
  fi

  vexec "pkill -INT -f 'Runner.Listener|preloop-runner run' 2>/dev/null; sleep 1; pkill -KILL -f 'Runner.Listener|Runner.Worker|preloop-runner' 2>/dev/null; exit 0" || true
  stop_preloop
  cp "$STATE_ROOT/current/flows.jsonl" "$OUT/flows.jsonl" 2>/dev/null || true
}

# ------------------------------------------------------------------ drive
case "$CELL" in
  gh-*) run_gh_cell ;;
  pl-*) run_pl_cell ;;
esac

FLOWS_COUNT=$([ -f "$OUT/flows.jsonl" ] && wc -l < "$OUT/flows.jsonl" | tr -d ' ' || echo 0)
case "$CELL" in pl-*) SERVER_FLOWS=$FLOWS_COUNT; FLOWS_COUNT=0 ;; esac
ENDED_AT="$(date -u +%Y%m%dT%H%M%SZ)"

cat > "$OUT/summary.json" <<JSON
{
  "cell": "$CELL",
  "scenario": "$SCENARIO",
  "backend": "$BACKEND",
  "runner_kind": "$KIND",
  "runner_version": "$KINDVER",
  "started_at": "$STARTED_AT",
  "status": "$STATUS",
  "runner_exit_code": $RUNNER_EXIT,
  "watch_exit_code": $WATCH_EXIT,
  "flows_count": $FLOWS_COUNT,
  "server_flows_count": $SERVER_FLOWS
}
JSON

log "done $CELL/$SCENARIO status=$STATUS flows=$FLOWS_COUNT server=$SERVER_FLOWS watch=$WATCH_EXIT"
[ "$STATUS" = ok ]
