#!/usr/bin/env bash
# run-matrix-compare.sh — Run 09-matrix-fan-out with official vs aksh runners
# and compare step-level conclusions
set -euo pipefail

WF="09-matrix-fan-out.yml"
NUM_RUNNERS=3
VM_OFFSET="${2:-4}"  # which bench-aksh-N to start from (4,5,6 have clean overlay)
GH_REPO="preloopdev/aksh-conformance-sample"
RESULTS_DIR="$(cd "$(dirname "$0")/results" && pwd)"
WFBASE="09-matrix-fan-out"
RUNNER="${1:-official}"  # official | aksh

log() { echo "[$(date +%T.%3N)] $*"; }

# ── Cancel stale runs ───────────────────────────────────────────────
log "Cancelling stale runs..."
for rid in $(gh run list -R "$GH_REPO" -L 30 --json databaseId,status \
  -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null); do
  gh run cancel "$rid" -R "$GH_REPO" 2>/dev/null || true
done
sleep 3

# ── Clean stale offline runners ─────────────────────────────────────
log "Cleaning stale runners..."
gh api "repos/$GH_REPO/actions/runners" \
  --jq '.runners[] | select(.status == "offline") | .id' 2>/dev/null | \
  while read -r rid; do
    gh api -X DELETE "repos/$GH_REPO/actions/runners/$rid" 2>/dev/null || true
  done

# ── Kill stale processes in VMs ─────────────────────────────────────
for i in $(seq 1 $NUM_RUNNERS); do
  smolvm machine exec --name bench-aksh-$i -- bash -c \
    'pkill -f "aksh-runner|Runner.Worker|Runner.Listener" 2>/dev/null; true' 2>/dev/null || true
done
sleep 2

# ── Registration token ──────────────────────────────────────────────
REG_TOKEN=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)
log "Token: ${REG_TOKEN:0:10}..."

JOB_TS=$(date +%s)
LOGDIR="/tmp/matrix-compare-$JOB_TS"
mkdir -p "$LOGDIR"

# ── Start runners ────────────────────────────────────────────────────
for i in $(seq 1 $NUM_RUNNERS); do
  vm="bench-aksh-$((VM_OFFSET + i - 1))"
  name="${RUNNER}-matrix-${JOB_TS}-${i}"
  root="/tmp/runner-matrix-${JOB_TS}-${i}"
  log "Starting $RUNNER runner $i/$NUM_RUNNERS on $vm..."

  if [ "$RUNNER" = "official" ]; then
    smolvm machine exec --name "$vm" -- bash -c "
set -euo pipefail
export RUNNER_ALLOW_RUNASROOT=1
rm -rf '$root' && mkdir -p '$root'
cd /home/bnjoroge/actions-runner
./config.sh \
  --url 'https://github.com/$GH_REPO' \
  --token '$REG_TOKEN' \
  --name '$name' \
  --labels 'self-hosted,linux,x64,mitm' \
  --work '$root/_work' \
  --unattended --replace --ephemeral 2>&1 | tail -2
RUNNER_ALLOW_RUNASROOT=1 ./run.sh 2>&1
echo 'EXIT='\$?
" > "$LOGDIR/runner-$i.log" 2>&1 &
  else
    smolvm machine exec --name "$vm" -- bash -c "
set -euo pipefail
rm -rf '$root' && mkdir -p '$root'
/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner \
  --runner-root '$root' configure \
  --url 'https://github.com/$GH_REPO' \
  --token '$REG_TOKEN' \
  --name '$name' \
  --unattended --replace --ephemeral \
  --labels 'self-hosted,linux,x64,mitm' 2>&1 | tail -2
RUST_LOG=info /workspace/target/aarch64-unknown-linux-musl/release/aksh-runner \
  --runner-root '$root' run --once 2>&1
echo 'EXIT='\$?
" > "$LOGDIR/runner-$i.log" 2>&1 &
  fi
done

# ── Wait for all runners online ──────────────────────────────────────
log "Waiting for $NUM_RUNNERS runners online..."
for attempt in $(seq 1 30); do
  ONLINE=$(gh api "repos/$GH_REPO/actions/runners" \
    --jq "[.runners[] | select(.status == \"online\")] | length" 2>/dev/null || echo 0)
  [ "$ONLINE" -ge "$NUM_RUNNERS" ] && { log "All $NUM_RUNNERS online"; break; }
  [ "$attempt" -eq 30 ] && { log "TIMEOUT: only $ONLINE/$NUM_RUNNERS online"; exit 1; }
  sleep 3
done

# ── Dispatch ─────────────────────────────────────────────────────────
log "Dispatching $WF..."
gh workflow run "$WF" -R "$GH_REPO" --ref main
sleep 5

RUN_ID=$(gh run list -R "$GH_REPO" -w "$WF" --json databaseId,status \
  -q '.[0].databaseId' 2>/dev/null)
log "Run ID: $RUN_ID  (watching...)"

gh run watch "$RUN_ID" -R "$GH_REPO" --exit-status 2>&1 || true

# ── Collect results ───────────────────────────────────────────────────
log "Collecting results..."
gh run view "$RUN_ID" -R "$GH_REPO" \
  --json conclusion,jobs \
  --jq '{
    runner: "'"$RUNNER"'",
    run_id: "'"$RUN_ID"'",
    conclusion: .conclusion,
    jobs: [.jobs[] | {
      name: .name,
      conclusion: .conclusion,
      steps: [.steps[] | {name: .name, conclusion: .conclusion}]
    }]
  }' | tee "$LOGDIR/result-$RUNNER.json"

echo ""
log "Runner logs in $LOGDIR/"
timeout 30 wait 2>/dev/null || true
