#!/usr/bin/env bash
# batch-conformance.sh — Run conformance workflows in batches of 4
# Each workflow dispatched on GitHub, picked up by a pre-packed smolvm runner
#
# Usage: ./batch-conformance.sh <aksh|official|both> [workflow-glob]
# Examples:
#   ./batch-conformance.sh aksh "8*"          # run 80-89 with aksh-runner
#   ./batch-conformance.sh both               # run all new scenarios with both runners
#   ./batch-conformance.sh official "98-*"    # run specific workflow
set -euo pipefail

RUNNER_TYPE="${1:?Usage: $0 <aksh|official|both> [workflow-glob]}"
WF_GLOB="${2:-}"

GH_REPO="preloopdev/aksh-conformance-sample"
HOST_WORKSPACE="/Users/bnjoroge/macos-runners"
RESULTS_DIR="$HOST_WORKSPACE/benchmarks/compatibility/runner/behavior"
TMP_DIR="/tmp/batch-conformance-$$"
BATCH_SIZE=4
VM_PREFIX="bench-aksh"

mkdir -p "$RESULTS_DIR" "$TMP_DIR"

# ── Helpers ─────────────────────────────────────────────────────────
ms() { python3 -c "import time; print(int(time.time()*1000))"; }
now_iso() { date -u +"%Y-%m-%dT%H:%M:%S.%3NZ"; }
log() { echo "[$(date +%T.%3N)] $*" | tee -a "$TMP_DIR/batch.log"; }

# ── Discover workflows ──────────────────────────────────────────────
log "Fetching workflow list from $GH_REPO..."
ALL_WFS=$(gh api "repos/$GH_REPO/contents/.github/workflows" --jq '.[].name' | sort)

# Filter to new conformance gap workflows (80-100)
if [ -n "$WF_GLOB" ]; then
  WORKFLOWS=()
  while IFS= read -r wf; do
    # shellcheck disable=SC2254
    case "$wf" in $WF_GLOB) WORKFLOWS+=("$wf") ;; esac
  done <<< "$ALL_WFS"
else
  WORKFLOWS=()
  while IFS= read -r wf; do
    case "$wf" in
      82-reusable-callee*) ;; # workflow_call only, not dispatchable
      8[0-9]-*|9[0-9]-*|100-*) WORKFLOWS+=("$wf") ;;
    esac
  done <<< "$ALL_WFS"
fi

if [ ${#WORKFLOWS[@]} -eq 0 ]; then
  echo "No workflows matched. Available:"
  echo "$ALL_WFS" | grep -E '^[89][0-9]-|^100-'
  exit 1
fi

log "Will run ${#WORKFLOWS[@]} workflows: ${WORKFLOWS[*]}"

# ── Clean stale runners ────────────────────────────────────────────
log "Cleaning stale offline runners..."
gh api "repos/$GH_REPO/actions/runners" --jq '.runners[] | select(.status == "offline") | .id' 2>/dev/null | \
  while read -r rid; do
    gh api -X DELETE "repos/$GH_REPO/actions/runners/$rid" 2>/dev/null || true
  done

# ── Cancel any queued runs ─────────────────────────────────────────
log "Cancelling any queued/in_progress runs..."
gh run list -R "$GH_REPO" --json databaseId,status -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null | \
  while read -r rid; do
    gh run cancel "$rid" -R "$GH_REPO" 2>/dev/null || true
    log "  Cancelled run $rid"
  done
sleep 2

# ── Run one batch of up to 4 workflows ─────────────────────────────
run_batch() {
  local runner_mode="$1"  # "aksh" or "official"
  shift
  local wfs=("$@")
  local batch_count=${#wfs[@]}

  log "━━ Batch: ${wfs[*]} (runner=$runner_mode) ━━"

  # Get a fresh registration token
  local reg_token
  reg_token=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)

  # Stop and restart VMs
  for i in $(seq 1 "$batch_count"); do
    local vm="${VM_PREFIX}-${i}"
    smolvm machine stop --name "$vm" 2>/dev/null || true
  done
  sleep 1
  for i in $(seq 1 "$batch_count"); do
    local vm="${VM_PREFIX}-${i}"
    smolvm machine start --name "$vm" > /dev/null 2>&1
    log "  VM $vm started"
  done

  # Start runners in VMs (they'll register and wait for jobs)
  local vm_pids=()
  local vm_logs=()
  local runner_script
  if [ "$runner_mode" = "official" ]; then
    runner_script="vm-run-official.sh"
  else
    runner_script="vm-run-aksh.sh"
  fi

  for i in $(seq 1 "$batch_count"); do
    local vm="${VM_PREFIX}-${i}"
    local vm_log="$TMP_DIR/vm-${runner_mode}-batch-${i}.log"
    vm_logs+=("$vm_log")

    if [ "$runner_mode" = "official" ]; then
      smolvm machine exec --name "$vm" -- bash -c "
        export GH_REG_TOKEN='$reg_token'
        export RUNNER_TIMING_LOG='/tmp/runner-j${i}.log'
        bash /workspace/benchmarks/real-world/vm-run-official.sh $i '$GH_REPO' 'self-hosted,linux,x64'
      " > "$vm_log" 2>&1 &
    else
      smolvm machine exec --name "$vm" -- bash -c "
        export GH_REG_TOKEN='$reg_token'
        export RUNNER_TIMING_LOG='/tmp/runner-j${i}.log'
        bash /workspace/benchmarks/real-world/vm-run-aksh.sh $i 'https://github.com/$GH_REPO' 'self-hosted,linux,x64'
      " > "$vm_log" 2>&1 &
    fi
    vm_pids+=($!)
    sleep 1
  done

  # Wait for runners to register
  log "  Waiting for ${batch_count} runners to register..."
  sleep 15

  # Dispatch all workflows in this batch
  local run_ids=()
  for wf in "${wfs[@]}"; do
    log "  Dispatching: $wf"
    gh workflow run "$wf" -R "$GH_REPO" --ref main 2>&1 || {
      log "  WARNING: dispatch failed for $wf"
      run_ids+=("FAILED")
      continue
    }
    sleep 2
    local rid
    rid=$(gh run list -R "$GH_REPO" -w "$wf" --json databaseId,status -q '.[0].databaseId' 2>/dev/null || echo "UNKNOWN")
    run_ids+=("$rid")
    log "  Run ID: $rid"
  done

  # Wait for all GitHub runs to complete
  log "  Waiting for all runs to complete..."
  for idx in "${!run_ids[@]}"; do
    local rid="${run_ids[$idx]}"
    local wf="${wfs[$idx]}"
    if [ "$rid" = "FAILED" ] || [ "$rid" = "UNKNOWN" ]; then
      log "  $wf: SKIPPED (dispatch failed)"
      continue
    fi
    gh run watch "$rid" -R "$GH_REPO" --exit-status 2>&1 &
    local watch_pid=$!
    ( sleep 300 && kill "$watch_pid" 2>/dev/null ) &
    local timer_pid=$!
    wait "$watch_pid" 2>/dev/null || true
    kill "$timer_pid" 2>/dev/null || true
  done

  # Wait for VM runners to finish
  for idx in "${!vm_pids[@]}"; do
    local pid="${vm_pids[$idx]}"
    wait "$pid" 2>/dev/null || true
  done

  # Collect results
  for idx in "${!wfs[@]}"; do
    local wf="${wfs[$idx]}"
    local rid="${run_ids[$idx]}"
    if [ "$rid" = "FAILED" ] || [ "$rid" = "UNKNOWN" ]; then
      echo "{\"runner\":\"$runner_mode\",\"workflow\":\"$wf\",\"status\":\"dispatch_failed\",\"timestamp\":\"$(now_iso)\"}" \
        >> "$RESULTS_DIR/conformance-${runner_mode}.jsonl"
      continue
    fi

    # Get detailed results from GitHub
    local result
    result=$(gh run view "$rid" -R "$GH_REPO" --json conclusion,jobs \
      --jq '{
        conclusion: .conclusion,
        jobs: [.jobs[] | {name: .name, conclusion: .conclusion, steps: [.steps[] | {name: .name, conclusion: .conclusion, number: .number}]}]
      }' 2>/dev/null || echo '{"conclusion":"unknown","jobs":[]}')

    local conclusion
    conclusion=$(echo "$result" | python3 -c "import json,sys; print(json.load(sys.stdin).get('conclusion','unknown'))" 2>/dev/null || echo "unknown")

    echo "{\"runner\":\"$runner_mode\",\"workflow\":\"$wf\",\"run_id\":\"$rid\",\"conclusion\":\"$conclusion\",\"result\":$result,\"timestamp\":\"$(now_iso)\"}" \
      >> "$RESULTS_DIR/conformance-${runner_mode}.jsonl"

    log "  $wf: $conclusion (run $rid)"
  done

  # Save VM logs
  for idx in "${!vm_logs[@]}"; do
    local src="${vm_logs[$idx]}"
    local j=$((idx + 1))
    if [ -f "$src" ]; then
      cp "$src" "$RESULTS_DIR/vm-${runner_mode}-batch-${j}-$(date +%s).log"
    fi
  done
}

# ── Main loop: batch workflows ──────────────────────────────────────
run_all() {
  local runner_mode="$1"
  local total=${#WORKFLOWS[@]}
  local batch_num=0

  log "══════════════════════════════════════════════════════════════"
  log "  Runner: $runner_mode | Workflows: $total | Batch size: $BATCH_SIZE"
  log "  $(date)"
  log "══════════════════════════════════════════════════════════════"

  local t0=$(ms)

  for ((batch_idx=0; batch_idx<total; batch_idx+=BATCH_SIZE)); do
    batch_num=$((batch_num + 1))
    local batch=("${WORKFLOWS[@]:batch_idx:BATCH_SIZE}")
    log ""
    log "── Batch $batch_num (${#batch[@]} workflows) ──"
    run_batch "$runner_mode" "${batch[@]}"

    # Cool down between batches
    if [ $((batch_idx + BATCH_SIZE)) -lt "$total" ]; then
      log "  Cooldown: 5s before next batch..."
      sleep 5
    fi
  done

  local t1=$(ms)
  local total_ms=$((t1 - t0))
  log ""
  log "══════════════════════════════════════════════════════════════"
  log "  $runner_mode COMPLETE: ${total} workflows in ${total_ms}ms"
  log "══════════════════════════════════════════════════════════════"
}

# ── Execute ─────────────────────────────────────────────────────────
case "$RUNNER_TYPE" in
  aksh)
    run_all "aksh"
    ;;
  official)
    run_all "official"
    ;;
  both)
    run_all "aksh"
    log ""
    log "Switching to official runner..."
    sleep 5
    run_all "official"
    ;;
  *)
    echo "Unknown runner type: $RUNNER_TYPE (expected: aksh|official|both)"
    exit 1
    ;;
esac

# ── Summary ─────────────────────────────────────────────────────────
log ""
log "══════════════════════════════════════════════════════════════"
log "  RESULTS"
log "══════════════════════════════════════════════════════════════"
for f in "$RESULTS_DIR"/conformance-*.jsonl; do
  [ -f "$f" ] || continue
  log ""
  log "  $(basename "$f"):"
  while IFS= read -r line; do
    local_wf=$(echo "$line" | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'  {d[\"workflow\"]:40s} {d[\"conclusion\"]}')" 2>/dev/null || echo "  (parse error)")
    log "$local_wf"
  done < "$f"
done

log ""
log "Full results: $RESULTS_DIR/"
log "Batch log: $TMP_DIR/batch.log"
