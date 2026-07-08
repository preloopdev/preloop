#!/usr/bin/env bash
# run-one-scenario.sh — Dispatch one workflow and run a single aksh-runner in a VM for it
# Usage: ./run-one-scenario.sh <workflow-file> [vm-name]
set -euo pipefail

WF="${1:?Usage: $0 <workflow-file> [vm-name]}"
VM="${2:-bench-golden}"
GH_REPO="preloopdev/aksh-conformance-sample"
RESULTS_DIR="$(cd "$(dirname "$0")/results" && pwd)"
WORKSPACE="/Users/bnjoroge/macos-runners"

log() { echo "[$(date +%T.%3N)] $*"; }

# ── Cancel any queued runs for this workflow ────────────────────────
WFBASE=$(basename "$WF" .yml)
log "Cancelling stale runs for $WFBASE..."
gh run list -R "$GH_REPO" -w "$WF" --json databaseId,status \
  -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null | \
  while read -r rid; do
    gh run cancel "$rid" -R "$GH_REPO" 2>/dev/null || true
  done

# ── Kill any running runners in VM ──────────────────────────────────
log "Killing stale runners in $VM..."
smolvm machine exec --name "$VM" -- bash -c 'pkill -f aksh-runner 2>/dev/null || true' 2>/dev/null || true
sleep 2

# ── Get registration token ──────────────────────────────────────────
log "Getting registration token..."
REG_TOKEN=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)
log "Token: ${REG_TOKEN:0:10}..."

# ── Configure and run aksh-runner in VM ─────────────────────────────
JOB=$(date +%s)
RUNNER_NAME="aksh-${WFBASE}-${JOB}"
RUNNER_ROOT="/tmp/aksh-${JOB}"

log "Starting runner: $RUNNER_NAME in $VM..."
smolvm machine exec --name "$VM" -- bash -c "
set -euo pipefail
export AKSH_RUNNER=/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner

# Configure
rm -rf '$RUNNER_ROOT'
mkdir -p '$RUNNER_ROOT'
\$AKSH_RUNNER --runner-root '$RUNNER_ROOT' configure \
  --url 'https://github.com/$GH_REPO' \
  --token '$REG_TOKEN' \
  --name '$RUNNER_NAME' \
  --unattended --replace --ephemeral \
  --labels 'self-hosted,linux,x64' 2>&1 | tail -3

# Run once
RUST_LOG=info \$AKSH_RUNNER --runner-root '$RUNNER_ROOT' run --once 2>&1
echo 'RUNNER_EXIT='\$?
" > "/tmp/runner-${WFBASE}.log" 2>&1 &
RUNNER_PID=$!

# Wait for runner to register
log "Waiting for runner to register..."
sleep 8

# ── Check runner registered ─────────────────────────────────────────
for i in $(seq 1 20); do
  if gh api "repos/$GH_REPO/actions/runners" --jq '.runners[] | select(.status == "online") | .name' 2>/dev/null | grep -q "$RUNNER_NAME"; then
    log "Runner $RUNNER_NAME registered"
    break
  fi
  sleep 3
done

# ── Dispatch workflow ───────────────────────────────────────────────
log "Dispatching $WF..."
gh workflow run "$WF" -R "$GH_REPO" --ref main 2>&1
sleep 5

# Get run ID
RUN_ID=$(gh run list -R "$GH_REPO" -w "$WF" --json databaseId,status -q '.[0].databaseId' 2>/dev/null)
log "Run ID: $RUN_ID"

# ── Wait for run to complete ────────────────────────────────────────
log "Waiting for run $RUN_ID to complete..."
gh run watch "$RUN_ID" -R "$GH_REPO" --exit-status 2>&1 || true

# ── Collect results ─────────────────────────────────────────────────
CONCLUSION=$(gh run view "$RUN_ID" -R "$GH_REPO" --json conclusion -q '.conclusion' 2>/dev/null || echo "unknown")
RESULT=$(gh run view "$RUN_ID" -R "$GH_REPO" --json conclusion,jobs --jq '{
  conclusion: .conclusion,
  jobs: [.jobs[] | {
    name: .name,
    conclusion: .conclusion,
    steps: [.steps[] | {name: .name, conclusion: .conclusion, number: .number}]
  }]
}' 2>/dev/null || echo '{"conclusion":"unknown","jobs":[]}')

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")

echo "{\"runner\":\"aksh\",\"workflow\":\"$WF\",\"run_id\":\"$RUN_ID\",\"conclusion\":\"$CONCLUSION\",\"result\":$RESULT,\"timestamp\":\"$TIMESTAMP\"}" \
  >> "$RESULTS_DIR/conformance/conformance-aksh.jsonl"

log "Done: $WF => $CONCLUSION"

# ── Cleanup ─────────────────────────────────────────────────────────
wait "$RUNNER_PID" 2>/dev/null || true
