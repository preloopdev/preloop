#!/usr/bin/env bash
# run-official-parallel.sh — Run conformance workflows with official C# runner, 4 at a time
set -euo pipefail

GH_REPO="preloopdev/aksh-conformance-sample"
RESULTS_DIR="$HOME/macos-runners/benchmarks/real-world/results/conformance"
mkdir -p "$RESULTS_DIR"

log() { echo "[$(date +%T.%3N)] $*"; }
ms() { python3 -c "import time; print(int(time.time()*1000))"; }

# All single-job conformance workflows (skip multi-job: 80=3jobs, 84=2jobs, 97=2jobs)
SINGLE_JOB_WFS=(
  87-multiline-output.yml
  88-state-and-post.yml
  89-workflow-inputs.yml
  90-shell-exit-behavior.yml
  91-large-output.yml
  92-unicode-special-chars.yml
  93-empty-null-values.yml
  94-action-pinning.yml
  95-nested-composite-outputs.yml
  96-env-inheritance.yml
  98-outcome-vs-conclusion.yml
  99-workspace-defaults.yml
  100-tool-cache.yml
  82-reusable-workflow.yml
  83-local-node-action.yml
  85-permissions-scoping.yml
  86-environment-deployments.yml
)

# Multi-job workflows need special handling (1 runner per job)
MULTI_JOB_WFS=(
  "80-custom-shells.yml:3"
  "81-step-timeout.yml:1"
  "84-concurrency-groups.yml:2"
  "97-artifact-cross-job.yml:2"
)

# Clean stale runners
log "Cleaning stale offline runners..."
gh api "repos/$GH_REPO/actions/runners" --jq '.runners[] | select(.status == "offline") | .id' 2>/dev/null | \
  while read -r rid; do gh api -X DELETE "repos/$GH_REPO/actions/runners/$rid" 2>/dev/null || true; done

# Cancel queued runs
log "Cancelling queued runs..."
gh run list -R "$GH_REPO" -L 50 --json databaseId,status -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null | \
  while read -r rid; do gh run cancel "$rid" -R "$GH_REPO" 2>/dev/null || true; done
sleep 2

start_official_runner() {
  local vm="$1" runner_name="$2" reg_token="$3"
  smolvm machine exec --name "$vm" -- bash -c "
    export PATH='/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/root/.cargo/bin'
    rm -rf /tmp/off-work-$runner_name /tmp/off-bin-$runner_name
    mkdir -p /tmp/off-work-$runner_name
    cp -a /opt/runners/actions-runner /tmp/off-bin-$runner_name
    id runner 2>/dev/null || useradd -m -s /bin/bash runner
    chown -R runner:runner /tmp/off-work-$runner_name /tmp/off-bin-$runner_name
    cd /tmp/off-bin-$runner_name
    su runner -c \"export PATH='/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'; ./config.sh --url https://github.com/$GH_REPO --token $reg_token --name $runner_name --work /tmp/off-work-$runner_name --unattended --replace --ephemeral --labels self-hosted,linux,x64\" 2>&1 | tail -2
    su runner -c \"export PATH='/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'; timeout 300 ./run.sh --once\" 2>&1
  "
}

run_batch() {
  local wfs=("$@")
  local count=${#wfs[@]}
  local vms=(bench-aksh-1 bench-aksh-2 bench-aksh-3 bench-aksh-4)

  log "── Batch: ${wfs[*]} ──"

  # Get reg token
  local reg_token
  reg_token=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)

  # Restart VMs
  for i in $(seq 0 $((count - 1))); do
    smolvm machine stop --name "${vms[$i]}" 2>/dev/null || true
  done
  sleep 1
  for i in $(seq 0 $((count - 1))); do
    smolvm machine start --name "${vms[$i]}" > /dev/null 2>&1
  done

  # Start runners in background
  local pids=()
  for i in $(seq 0 $((count - 1))); do
    local rname="off-$(date +%s)-$i"
    start_official_runner "${vms[$i]}" "$rname" "$reg_token" > "/tmp/off-vm-$i.log" 2>&1 &
    pids+=($!)
    sleep 1
  done

  # Wait for runners to register
  sleep 12

  # Dispatch all workflows
  local run_ids=()
  for wf in "${wfs[@]}"; do
    gh workflow run "$wf" -R "$GH_REPO" --ref main 2>&1 || true
    sleep 2
    local rid
    rid=$(gh run list -R "$GH_REPO" -w "$wf" --json databaseId,status -q '.[0].databaseId' 2>/dev/null || echo "UNKNOWN")
    run_ids+=("$rid")
    log "  Dispatched $wf -> $rid"
  done

  # Wait for all runs (with timeout)
  for idx in "${!run_ids[@]}"; do
    local rid="${run_ids[$idx]}"
    local wf="${wfs[$idx]}"
    [ "$rid" = "UNKNOWN" ] && continue
    
    # Poll until done (max 5 min)
    local deadline=$(($(date +%s) + 300))
    while [ "$(date +%s)" -lt "$deadline" ]; do
      local status
      status=$(gh run view "$rid" -R "$GH_REPO" --json status -q '.status' 2>/dev/null || echo "unknown")
      [ "$status" = "completed" ] && break
      sleep 5
    done
  done

  # Wait for VM runners
  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done

  # Collect results
  for idx in "${!run_ids[@]}"; do
    local rid="${run_ids[$idx]}"
    local wf="${wfs[$idx]}"
    [ "$rid" = "UNKNOWN" ] && { log "  $wf: DISPATCH_FAILED"; continue; }

    local conclusion
    conclusion=$(gh run view "$rid" -R "$GH_REPO" --json conclusion -q '.conclusion' 2>/dev/null || echo "unknown")
    
    local result
    result=$(gh run view "$rid" -R "$GH_REPO" --json conclusion,jobs \
      --jq '{conclusion: .conclusion, jobs: [.jobs[] | {name: .name, conclusion: .conclusion, steps: [.steps[] | {name: .name, conclusion: .conclusion}]}]}' 2>/dev/null || echo '{}')

    echo "{\"runner\":\"official\",\"workflow\":\"$wf\",\"run_id\":\"$rid\",\"conclusion\":\"$conclusion\",\"result\":$result,\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" \
      >> "$RESULTS_DIR/conformance-official.jsonl"

    log "  $wf: $conclusion (run $rid)"
  done
}

# ── Main ──
T0=$(ms)
log "═══════════════════════════════════════════════════════════"
log "  Official Runner Conformance — ${#SINGLE_JOB_WFS[@]} single-job + ${#MULTI_JOB_WFS[@]} multi-job"
log "═══════════════════════════════════════════════════════════"

# Clear old results
> "$RESULTS_DIR/conformance-official.jsonl"

# Run single-job workflows in batches of 4
for ((i=0; i<${#SINGLE_JOB_WFS[@]}; i+=4)); do
  batch=("${SINGLE_JOB_WFS[@]:i:4}")
  run_batch "${batch[@]}"
  log "  Cooldown 3s..."
  sleep 3
done

# Run multi-job workflows: allocate enough runners per workflow
for entry in "${MULTI_JOB_WFS[@]}"; do
  wf="${entry%%:*}"
  job_count="${entry##*:}"
  log "── Multi-job: $wf ($job_count jobs) ──"
  
  reg_token=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)
  
  # Restart VMs
  for j in $(seq 1 "$job_count"); do
    smolvm machine stop --name "bench-aksh-$j" 2>/dev/null || true
  done
  sleep 1
  for j in $(seq 1 "$job_count"); do
    smolvm machine start --name "bench-aksh-$j" > /dev/null 2>&1
  done

  # Start one runner per job
  local_pids=()
  for j in $(seq 1 "$job_count"); do
    rname="off-multi-$(date +%s)-$j"
    start_official_runner "bench-aksh-$j" "$rname" "$reg_token" > "/tmp/off-multi-$j.log" 2>&1 &
    local_pids+=($!)
    sleep 1
  done

  sleep 12

  gh workflow run "$wf" -R "$GH_REPO" --ref main 2>&1 || true
  sleep 2
  rid=$(gh run list -R "$GH_REPO" -w "$wf" --json databaseId -q '.[0].databaseId' 2>/dev/null || echo "UNKNOWN")
  log "  Dispatched $wf -> $rid"

  # Wait for completion
  deadline=$(($(date +%s) + 300))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    status=$(gh run view "$rid" -R "$GH_REPO" --json status -q '.status' 2>/dev/null || echo "unknown")
    [ "$status" = "completed" ] && break
    sleep 5
  done

  for pid in "${local_pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done

  conclusion=$(gh run view "$rid" -R "$GH_REPO" --json conclusion -q '.conclusion' 2>/dev/null || echo "unknown")
  result=$(gh run view "$rid" -R "$GH_REPO" --json conclusion,jobs \
    --jq '{conclusion: .conclusion, jobs: [.jobs[] | {name: .name, conclusion: .conclusion, steps: [.steps[] | {name: .name, conclusion: .conclusion}]}]}' 2>/dev/null || echo '{}')
  echo "{\"runner\":\"official\",\"workflow\":\"$wf\",\"run_id\":\"$rid\",\"conclusion\":\"$conclusion\",\"result\":$result,\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" \
    >> "$RESULTS_DIR/conformance-official.jsonl"
  log "  $wf: $conclusion"
  sleep 3
done

T1=$(ms)
log ""
log "═══════════════════════════════════════════════════════════"
log "  DONE: $((T1-T0))ms total"
log "═══════════════════════════════════════════════════════════"
log ""
log "Results:"
cat "$RESULTS_DIR/conformance-official.jsonl" | python3 -c "
import json,sys
for line in sys.stdin:
    d=json.loads(line.strip())
    print(f\"  {d['workflow']}: {d['conclusion']}\")
"
