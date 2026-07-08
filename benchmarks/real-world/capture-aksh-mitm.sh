#!/usr/bin/env bash
# capture-aksh-mitm.sh — Capture aksh-runner MITM flows against GitHub
# Usage: ./capture-aksh-mitm.sh <scenario> [vm=bench-aksh-4]
#   scenario: 07-step-failure, 10-uses-checkout, etc.
set -euo pipefail

SCENARIO="${1:?Usage: $0 <scenario-name>}"
VM="${2:-bench-aksh-4}"
GH_REPO="preloopdev/aksh-conformance-sample"
WF_NAME="$SCENARIO.yml"
RESULTS_DIR="$(cd "$(dirname "$0")/results/mitm" && pwd)"
MITM_PORT=8080

log() { echo "[$(date +%T.%3N)] $*"; }

# ── Resolve workflow file ────────────────────────────────────────────
# Map scenario names like "07-step-failure" to "07-step-failure.yml"
WF="$WF_NAME"

# ── Clean slate ──────────────────────────────────────────────────────
log "Cleaning up previous runs on $VM..."
smolvm machine exec --name "$VM" -- bash -c "
  pkill -f mitmdump 2>/dev/null || true
  killall aksh-runner 2>/dev/null || true
  sleep 1
" 2>/dev/null || true

# Cancel stale GitHub runs
log "Cancelling stale runs..."
for rid in $(gh run list -R "$GH_REPO" -L 20 --json databaseId,status \
  -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null); do
  gh run cancel "$rid" -R "$GH_REPO" 2>/dev/null || true
done
sleep 2

# Delete stale offline runners
log "Cleaning stale runners..."
gh api "repos/$GH_REPO/actions/runners" \
  --jq '.runners[] | select(.status == "offline") | .id' 2>/dev/null | \
  while read -r rid; do
    gh api -X DELETE "repos/$GH_REPO/actions/runners/$rid" 2>/dev/null || true
  done

# ── Setup CA cert ────────────────────────────────────────────────────
log "Setting up mitmproxy CA cert..."
smolvm machine exec --name "$VM" -- bash -c "
  # Ensure mitmproxy CA exists
  CONFDIR=/tmp/mitm-capture-certs
  mkdir -p \$CONFDIR
  if [ ! -f \$CONFDIR/mitmproxy-ca-cert.pem ]; then
    timeout 8 mitmdump --set confdir=\$CONFDIR --listen-port 0 >/dev/null 2>&1 || true
    sleep 1
  fi
  if [ -f \$CONFDIR/mitmproxy-ca-cert.pem ]; then
    echo 'CA cert ok'
  elif [ -f ~/.mitmproxy/mitmproxy-ca-cert.pem ]; then
    cp ~/.mitmproxy/mitmproxy-ca-cert.pem \$CONFDIR/
    echo 'CA cert copied'
  else
    echo 'FAIL: no CA cert'
    exit 1
  fi
"

# ── Create capture dir ───────────────────────────────────────────────
TIMESTAMP=$(date -u +%Y-%m-%dT%H-%M-%SZ)
CAPTURE_DIR="$RESULTS_DIR/$SCENARIO/$TIMESTAMP"
mkdir -p "$CAPTURE_DIR"
log "Capture dir: $CAPTURE_DIR"

# ── Start mitmdump in VM ─────────────────────────────────────────────
log "Starting mitmdump on $VM:$MITM_PORT..."
smolvm machine exec --name "$VM" -- bash -c "
  CONFDIR=/tmp/mitm-capture-certs
  CAPTURE_DIR=/tmp/mitm-capture
  rm -rf \$CAPTURE_DIR && mkdir -p \$CAPTURE_DIR
  export MITM_CAPTURE_DIR=\$CAPTURE_DIR
  nohup mitmdump \
    --listen-host 127.0.0.1 \
    --listen-port $MITM_PORT \
    --set confdir=\$CONFDIR \
    -s /workspace/benchmarks/real-world/capture-addon.py \
    > /tmp/mitmdump.log 2>&1 &
  echo \$! > /tmp/mitmdump.pid
  # Wait for mitmdump to be ready
  for i in \$(seq 1 20); do
    if nc -z 127.0.0.1 $MITM_PORT 2>/dev/null; then
      echo 'mitmdump ready'
      exit 0
    fi
    sleep 0.5
  done
  echo 'TIMEOUT: mitmdump did not start'
  cat /tmp/mitmdump.log
  exit 1
"

# ── Register + run aksh-runner ───────────────────────────────────────
REG_TOKEN=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)
RUNNER_NAME="aksh-capture-${SCENARIO}-$(date +%s)"
RUNNER_ROOT="/tmp/aksh-capture-$$"
log "Token: ${REG_TOKEN:0:10}..."

smolvm machine exec --name "$VM" -- bash -c "
set -euo pipefail
export https_proxy=http://127.0.0.1:$MITM_PORT
export HTTPS_PROXY=http://127.0.0.1:$MITM_PORT
export http_proxy=http://127.0.0.1:$MITM_PORT
export HTTP_PROXY=http://127.0.0.1:$MITM_PORT
export no_proxy=
export NO_PROXY=
export SSL_CERT_FILE=/tmp/mitm-capture-certs/mitmproxy-ca-cert.pem
export RUST_LOG=info

killall aksh-runner 2>/dev/null || true
sleep 0.5

BIN=/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner
rm -rf '$RUNNER_ROOT' && mkdir -p '$RUNNER_ROOT'

\$BIN --runner-root '$RUNNER_ROOT' configure \
  --url https://github.com/$GH_REPO \
  --token '$REG_TOKEN' \
  --name '$RUNNER_NAME' \
  --unattended --replace --ephemeral \
  --labels self-hosted,linux,x64,mitm 2>&1 | tail -1

\$BIN --runner-root '$RUNNER_ROOT' run --once > /tmp/aksh-capture-runner.log 2>&1 &
echo \"RUNNER_VM_PID=\$!\"
" 2>&1

# ── Wait for runner to appear online ─────────────────────────────────
log "Waiting for runner to appear online..."
for attempt in $(seq 1 30); do
  ONLINE=$(gh api "repos/$GH_REPO/actions/runners" \
    --jq "[.runners[] | select(.status == \"online\")] | length" 2>/dev/null || echo 0)
  [ "$ONLINE" -ge 1 ] && { log "Runner online"; break; }
  [ "$attempt" -eq 30 ] && { log "TIMEOUT: only $ONLINE online"; exit 1; }
  sleep 3
done
# ── Dispatch workflow ────────────────────────────────────────────────
log "Dispatching $WF..."
gh workflow run "$WF" -R "$GH_REPO" --ref main 2>&1
sleep 5

RUN_ID=$(gh run list -R "$GH_REPO" -w "$WF" --json databaseId,status \
  -q '.[0].databaseId' 2>/dev/null)
log "Run ID: $RUN_ID"

# ── Wait for completion ──────────────────────────────────────────────
log "Watching run..."
gh run watch "$RUN_ID" -R "$GH_REPO" --exit-status 2>&1 || true

# ── Wait for runner to finish ───────────────────────────────────────
# The gh run watch already waited for the run to complete,
# so the runner should have exited by now. Give it a moment.
log "Waiting for runner to exit..."
sleep 3

# ── Collect runner log from VM ──────────────────────────────────────
smolvm machine exec --name "$VM" -- cat /tmp/aksh-capture-runner.log > "$CAPTURE_DIR/runner.log" 2>/dev/null || log "WARN: no runner log"

# ── Stop mitmdump ───────────────────────────────────────────────────
log "Stopping mitmdump..."
smolvm machine exec --name "$VM" -- bash -c "
  if [ -f /tmp/mitmdump.pid ]; then
    kill \$(cat /tmp/mitmdump.pid) 2>/dev/null || true
  fi
  sleep 1
  echo 'mitmdump stopped'
  if [ -f /tmp/mitm-capture/flows.jsonl ]; then
    echo 'HAS_FLOWS'
  fi
" 2>/dev/null

# ── Collect flows from VM ──────────────────────────────────────────
log "Collecting MITM flows..."
smolvm machine exec --name "$VM" -- cat /tmp/mitm-capture/flows.jsonl > "$CAPTURE_DIR/flows.jsonl" 2>/dev/null
if [ ! -s "$CAPTURE_DIR/flows.jsonl" ]; then
  log "WARN: no flows.jsonl captured"
fi
# Bin files (large binary bodies) are not collected — flows.jsonl contains
# all HTTP metadata; bin files are only needed for exact byte-level replay.
log "Collecting runner log..."
smolvm machine exec --name "$VM" -- cat /tmp/aksh-capture-runner.log > "$CAPTURE_DIR/runner.log" 2>/dev/null || true
# ── Get run result ───────────────────────────────────────────────────
CONCLUSION=$(gh run view "$RUN_ID" -R "$GH_REPO" --json conclusion -q '.conclusion' 2>/dev/null || echo "unknown")
FLOW_COUNT=$(wc -l < "$CAPTURE_DIR/flows.jsonl" 2>/dev/null || echo 0)

# ── Write summary ────────────────────────────────────────────────────
cat > "$CAPTURE_DIR/summary.json" <<JSONEND
{
  "backend": "aksh",
  "scenario": "$SCENARIO",
  "run_id": "$RUN_ID",
  "conclusion": "$CONCLUSION",
  "flows_count": $FLOW_COUNT,
  "timestamp": "$TIMESTAMP"
}
JSONEND

log "Done: $SCENARIO  conclusion=$CONCLUSION  flows=$FLOW_COUNT"
log "Results: $CAPTURE_DIR"
