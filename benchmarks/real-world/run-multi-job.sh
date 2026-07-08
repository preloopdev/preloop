#!/usr/bin/env bash
# run-multi-job.sh — Dispatch a multi-job workflow with multiple runners
# Usage: ./run-multi-job.sh <workflow-file> [num-runners=2]
set -euo pipefail

WF="${1:?Usage: $0 <workflow-file> [num-runners]}"
NUM_RUNNERS="${2:-2}"
GH_REPO="preloopdev/aksh-conformance-sample"
RESULTS_DIR="$(cd "$(dirname "$0")/results" && pwd)"
WFBASE=$(basename "$WF" .yml)

log() { echo "[$(date +%T.%3N)] $*"; }

# ── Cancel ALL stale runs (not just this workflow) ──────────────────
log "Cancelling ALL queued/in-progress runs..."
for rid in $(gh run list -R "$GH_REPO" -L 30 --json databaseId,status \
  -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null); do
  gh run cancel "$rid" -R "$GH_REPO" 2>/dev/null || true
done
sleep 3

# ── Delete stale offline runners on GitHub ──────────────────────────
log "Deleting stale offline runners..."
gh api "repos/$GH_REPO/actions/runners" --jq '.runners[] | select(.status == "offline") | .id' 2>/dev/null | \
  while read -r rid; do
    gh api -X DELETE "repos/$GH_REPO/actions/runners/$rid" 2>/dev/null || true
  done
sleep 2

# ── Kill stale runner processes inside VMs ─────────────────────────
for i in $(seq 1 "$NUM_RUNNERS"); do
  vm="bench-aksh-$i"
  smolvm machine exec --name "$vm" -- bash -c 'pkill -f aksh-runner 2>/dev/null; true' 2>/dev/null || true
done
sleep 2

# ── Get registration token ──────────────────────────────────────────
REG_TOKEN=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)
log "Token: ${REG_TOKEN:0:10}..."

# ── Start runners in all VMs ────────────────────────────────────────
JOB_TS=$(date +%s)
for i in $(seq 1 "$NUM_RUNNERS"); do
  vm="bench-aksh-$i"
  name="aksh-${WFBASE}-${JOB_TS}-${i}"
  root="/tmp/aksh-${JOB_TS}-${i}"
  log "Starting runner $i/$NUM_RUNNERS: $name on $vm"

  smolvm machine exec --name "$vm" -- bash -c "
set -euo pipefail
rm -rf '$root'
mkdir -p '$root'
/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner \
  --runner-root '$root' configure \
  --url 'https://github.com/$GH_REPO' \
  --token '$REG_TOKEN' \
  --name '$name' \
  --unattended --replace --ephemeral \
  --labels 'self-hosted,linux,x64,mitm' >&2
RUST_LOG=info /workspace/target/aarch64-unknown-linux-musl/release/aksh-runner \
  --runner-root '$root' run --once 2>&1
echo 'EXIT='\$?
" > "/tmp/runner-${WFBASE}-${i}.log" 2>&1 &
done

# ── Poll until all runners are online on GitHub ────────────────────
log "Waiting for $NUM_RUNNERS runners to appear online..."
for attempt in $(seq 1 30); do
  ONLINE=$(gh api "repos/$GH_REPO/actions/runners" --jq '[.runners[] | select(.status == "online")] | length' 2>/dev/null || echo 0)
  [ "$ONLINE" -ge "$NUM_RUNNERS" ] && { log "All $NUM_RUNNERS runners online"; break; }
  [ "$attempt" -eq 30 ] && { log "TIMEOUT: only $ONLINE/$NUM_RUNNERS runners online"; exit 1; }
  sleep 10
done
# ── Dispatch ────────────────────────────────────────────────────────
log "Dispatching $WF..."
gh workflow run "$WF" -R "$GH_REPO" --ref main 2>&1
sleep 5

RUN_ID=$(gh run list -R "$GH_REPO" -w "$WF" --json databaseId,status -q '.[0].databaseId' 2>/dev/null)
log "Run ID: $RUN_ID"

# ── Wait for run to complete ────────────────────────────────────────
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

# Wait for background runner processes with 30s timeout
timeout 30 wait 2>/dev/null || true
# Delete our ephemeral runners from GitHub
log "Cleaning up runners..."
for i in $(seq 1 "$NUM_RUNNERS"); do
  gh api "repos/$GH_REPO/actions/runners" --jq ".runners[] | select(.name | startswith(\"aksh-${WFBASE}-\")) | .id" 2>/dev/null | \
    while read -r rid; do
      gh api -X DELETE "repos/$GH_REPO/actions/runners/$rid" 2>/dev/null || true
    done
done

