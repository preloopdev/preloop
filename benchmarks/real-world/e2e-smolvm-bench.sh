#!/usr/bin/env bash
# e2e-smolvm-bench.sh — Run E2E workflows with per-job SmolVM instances
# Usage: ./e2e-smolvm-bench.sh <github|preloop> <serde|axum|bat|all>
#
# github mode: trigger workflow_dispatch on GitHub, runners connect to GitHub
# preloop mode:   start preloop-server on host, submit workflow, runners connect to preloop-server
set -euo pipefail

MODE="${1:?Usage: $0 <github|preloop> <serde|axum|bat|all>}"
REPO="${2:-all}"
VM_NAME="build-runner"  # existing warm VM with Rust + runners
PRELOOP_BIN="$HOME/preloop-runner"  # preloop binaries inside VM (via ssh)
HOST_SERVER="$HOME/cachingv4/target/release/preloop-server"
HOST_CLIENT="$HOME/cachingv4/target/release/preloop-runner-client"
GH_REPO="preloopdev/preloop-conformance-sample"
RESULTS_DIR="$HOME/cachingv4/benchmarks/real-world/results"
mkdir -p "$RESULTS_DIR"
if [[ "$MODE" == "preloop" ]]; then
  if [[ -z "${PRELOOP_SYSTEM_TOKEN:-}" ]]; then
    export PRELOOP_SYSTEM_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
  fi
fi


ms() { python3 -c "import time; print(int(time.time()*1000))"; }

log() { echo "[$(date +%T.%3N)] $*"; }

# --- GitHub Mode ---
run_github() {
  local wf="$1" label="$2"
  log "Triggering $wf on GitHub..."
  local t0=$(ms)
  
  # Clean up stale offline runners first
  log "Cleaning stale runners..."
  
  # Register fresh runners — one per job
  # Get job count from workflow
  local job_count
  job_count=$(grep -c "runs-on:" "$HOME/cachingv4/benchmarks/real-world/$wf" || echo 1)
  log "Workflow has $job_count jobs"
  
  # For each job, the VM's runner will pick it up via --once
  # We use the existing build-runner VM and run the runner multiple times
  local run_id
  log "Dispatching workflow..."
  gh workflow run "$wf" -R "$GH_REPO" 2>&1
  sleep 3
  
  # Get the run ID
  run_id=$(gh run list -R "$GH_REPO" -w "$wf" --json databaseId,status -q '.[0].databaseId')
  log "Run ID: $run_id"
  
  # Start runners in the VM (one per job, sequentially via --once)
  for i in $(seq 1 "$job_count"); do
    log "Starting preloop-runner instance $i/$job_count..."
    local runner_name="e2e-${label}-$(date +%s)-${i}"
    ssh vm103 "
      rm -rf /tmp/e2e-root-$i; mkdir -p /tmp/e2e-root-$i
      /home/bnjoroge/preloop-runner/preloop-runner --runner-root /tmp/e2e-root-$i configure \
        --url https://github.com/$GH_REPO --token \$(gh api repos/$GH_REPO/actions/runners/registration-token --jq .token) \
        --name $runner_name --unattended --replace --ephemeral \
        --labels self-hosted,linux,x64 2>&1 | tail -2
      RUST_LOG=info /home/bnjoroge/preloop-runner/preloop-runner --runner-root /tmp/e2e-root-$i run --once > /tmp/e2e-runner-$i.log 2>&1
      echo 'RUNNER_EXIT='\$?
    " &
  done
  
  # Wait for the GitHub run to complete
  log "Waiting for run $run_id..."
  gh run watch "$run_id" -R "$GH_REPO" --exit-status 2>&1 || true
  local t1=$(ms)
  
  wait  # wait for all SSH sessions
  
  local total=$((t1 - t0))
  log "$wf completed in ${total}ms"
  
  # Get per-job timings from GitHub
  gh run view "$run_id" -R "$GH_REPO" --json jobs --jq '.jobs[] | "\(.name)\t\(.conclusion)\t\(.startedAt)\t\(.completedAt)"' | while IFS=$'\t' read -r name conclusion started completed; do
    echo "  $name: $conclusion ($started → $completed)"
  done
  
  echo "{\"mode\":\"github\",\"workflow\":\"$wf\",\"total_ms\":$total,\"run_id\":$run_id}" >> "$RESULTS_DIR/${label}-github.jsonl"
}

# --- preloop-server Mode ---
run_preloop_server() {
  local wf="$1" label="$2"
  log "Running $wf against preloop-server..."
  
  # Start server on host Mac
  pkill -f preloop-server 2>/dev/null || true; sleep 0.3
  # Server listens on all interfaces so VM can reach it via host IP
  local host_ip
  host_ip=$(ipconfig getifaddr en0 2>/dev/null || echo "127.0.0.1")
  export PRELOOP_PUBLIC_URL="http://${host_ip}:9191"
  RUST_LOG=info PRELOOP_SYSTEM_TOKEN="$PRELOOP_SYSTEM_TOKEN" "$HOST_SERVER" serve --listen "0.0.0.0:9191" --state-dir "/tmp/preloop-e2e-state-$$" > /tmp/preloop-e2e-server.log 2>&1 &
  local server_pid=$!
  sleep 1
  curl -sf "http://127.0.0.1:9191/healthz" >/dev/null || { log "Server failed"; cat /tmp/preloop-e2e-server.log; exit 1; }
  log "preloop-server running on $host_ip:9191 (pid=$server_pid)"
  
  # Get job count
  local job_count
  job_count=$(grep -c "runs-on:" "$HOME/cachingv4/benchmarks/real-world/$wf" || echo 1)
  log "Workflow has $job_count jobs"
  
  # Submit workflow
  local wf_path=".github/workflows/$wf"
  local t0=$(ms)
  log "Submitting workflow..."
  local submit_out
  submit_out=$("$HOST_CLIENT" --server "http://127.0.0.1:9191" submit \
    -W "$HOME/cachingv4/benchmarks/real-world/$wf" \
    --workspace-root /tmp/preloop-conformance-push \
    --git-ref refs/heads/main 2>&1)
  log "Submit: $submit_out"
  
  # Start runners in the VM
  for i in $(seq 1 "$job_count"); do
    log "Starting preloop-runner $i/$job_count in VM..."
    ssh vm103 "
      rm -rf /tmp/e2e-preloop-$i; mkdir -p /tmp/e2e-preloop-$i
      /home/bnjoroge/preloop-runner/preloop-runner --runner-root /tmp/e2e-preloop-$i configure \
        --url http://${host_ip}:9191 --token t --name preloop-e2e-$i \
        --unattended --replace --ephemeral \
        --labels self-hosted,linux,x64 --no-externals 2>&1 | tail -1
      RUST_LOG=info /home/bnjoroge/preloop-runner/preloop-runner --runner-root /tmp/e2e-preloop-$i run --once > /tmp/e2e-preloop-runner-$i.log 2>&1
      echo 'EXIT='\$?
      grep -E 'Running step|completed:' /tmp/e2e-preloop-runner-$i.log
    " &
  done
  
  wait
  local t1=$(ms)
  local total=$((t1 - t0))
  log "$wf against preloop-server completed in ${total}ms"
  
  echo "{\"mode\":\"preloop\",\"workflow\":\"$wf\",\"total_ms\":$total}" >> "$RESULTS_DIR/${label}-preloop.jsonl"
  
  kill $server_pid 2>/dev/null || true
}

# --- Main ---
WORKFLOWS=()
case "$REPO" in
  serde) WORKFLOWS=("e2e-serde.yml") ;;
  axum)  WORKFLOWS=("e2e-axum.yml") ;;
  bat)   WORKFLOWS=("e2e-bat.yml") ;;
  all)   WORKFLOWS=("e2e-serde.yml" "e2e-axum.yml" "e2e-bat.yml") ;;
  *) echo "Unknown repo: $REPO"; exit 1 ;;
esac

echo "================================================================"
echo "  E2E Benchmark: mode=$MODE repos=${REPO}"
echo "  $(date)"
echo "================================================================"

for wf in "${WORKFLOWS[@]}"; do
  label="${wf%.yml}"
  case "$MODE" in
    github) run_github "$wf" "$label" ;;
    preloop)   run_preloop_server "$wf" "$label" ;;
    *) echo "Unknown mode: $MODE"; exit 1 ;;
  esac
done

echo ""
echo "Results in: $RESULTS_DIR/"
