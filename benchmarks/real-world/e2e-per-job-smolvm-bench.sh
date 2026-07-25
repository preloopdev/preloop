#!/usr/bin/env bash
# e2e-per-job-smolvm-bench.sh — per-job smolvm isolation benchmark
#
# Each job runs in its OWN smolvm VM (NOT as a process in one VM).
# Warm cache via shared workspace mounts.
#
# Usage:
#   ./e2e-per-job-smolvm-bench.sh <mode> <workflow>
#
# Modes:
#   github-official  — Official C# runner → GitHub control plane
#   github-aksh      — aksh Rust runner → GitHub control plane
#   aksh-server      — aksh Rust runner → local aksh-server control plane
#
# Workflows: serde | axum | bat | all
set -euo pipefail

# ── Config ──────────────────────────────────────────────────────────
MODE="${1:?Usage: $0 <github-official|github-aksh|aksh-server> <serde|axum|bat|all>}"
WF_SELECTOR="${2:?}"

GH_REPO="preloopdev/aksh-conformance-sample"
HOST_WORKSPACE="/Users/bnjoroge/macos-runners"
WARM_CACHE="/Users/bnjoroge/cachingv4"
WARM_RUSTUP="$WARM_CACHE/.rustup"
WARM_CARGO="$WARM_CACHE/.cargo"
WARM_OFFICIAL="$WARM_CACHE/actions-runner"
RESULTS_DIR="$HOST_WORKSPACE/benchmarks/real-world/results"
TMP_DIR="/tmp/e2e-per-job-bench-$$"

VM_CPUS=4
VM_MEM=8192
VM_STORAGE=20
VM_IMAGE="ubuntu:24.04"  # fallback for ephemeral mode
BENCH_VM_PREFIX="bench-aksh"  # persistent pre-packed VMs: bench-aksh-{1..4}

mkdir -p "$RESULTS_DIR" "$TMP_DIR"

# ── Helpers ─────────────────────────────────────────────────────────
ms() { python3 -c "import time; print(int(time.time()*1000))"; }
now_iso() { date -u +"%Y-%m-%dT%H:%M:%S.%3NZ"; }
log() { echo "[$(date +%T.%3N)] $*" | tee -a "$TMP_DIR/orchestrator.log"; }

# ── Determine workflows to run ──────────────────────────────────────
WORKFLOWS=()
case "$WF_SELECTOR" in
  serde) WORKFLOWS=("e2e-serde.yml") ;;
  axum)  WORKFLOWS=("e2e-axum.yml") ;;
  bat)   WORKFLOWS=("e2e-bat.yml") ;;
  all)   WORKFLOWS=("e2e-serde.yml" "e2e-axum.yml" "e2e-bat.yml") ;;
  *) echo "Unknown workflow: $WF_SELECTOR"; exit 1 ;;
esac

# ── Count jobs in a workflow YAML ───────────────────────────────────
count_jobs() {
  local wf_file="$1"
  grep -c "runs-on:" "$wf_file" 2>/dev/null || echo 1
}

# ── GitHub modes ────────────────────────────────────────────────────
run_github_mode() {
  local runner_type="$1"  # "official" or "aksh"
  local wf="$2"
  local label="${wf%.yml}"
  local run_label="${label}-github-${runner_type}"

  log "════════════════════════════════════════════════════════════"
  log "  MODE: GitHub + ${runner_type} runner | WF: $wf"
  log "════════════════════════════════════════════════════════════"

  local wf_path="$HOST_WORKSPACE/benchmarks/real-world/$wf"
  local job_count
  job_count=$(count_jobs "$wf_path")
  log "Workflow has $job_count jobs → need $job_count VMs"

  # Get registration token
  log "Getting GitHub registration token..."
  local reg_token
  reg_token=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)
  log "Token acquired (expires in 1h)"

  # Choose runner script
  local runner_script
  if [ "$runner_type" = "official" ]; then
    runner_script="/workspace/benchmarks/real-world/vm-run-official.sh"
  else
    runner_script="/workspace/benchmarks/real-world/vm-run-aksh.sh"
  fi

  # Start VM runners using pre-packed persistent VMs
  local t_overall_start=$(ms)
  local vm_pids=()
  local vm_logs=()

  for i in $(seq 1 "$job_count"); do
    local vm_log="$TMP_DIR/vm-${label}-${runner_type}-j${i}.log"
    local vm_name="${BENCH_VM_PREFIX}-${i}"
    vm_logs+=("$vm_log")

    log "Starting VM $vm_name for job $i/$job_count (runner=$runner_type)..."

    # Stop/start the persistent VM (fast: ~200ms)
    smolvm machine stop --name "$vm_name" 2>/dev/null || true
    smolvm machine start --name "$vm_name" > /dev/null 2>&1

    # Run the benchmark script inside the persistent VM
    smolvm machine exec --name "$vm_name" -- bash -c "
      export GH_REG_TOKEN='$reg_token'
      export RUNNER_TIMING_LOG='/tmp/runner-j${i}.log'
      bash /workspace/benchmarks/real-world/$( [ \"$runner_type\" = \"official\" ] && echo vm-run-official.sh || echo vm-run-aksh.sh ) \
        $i \
        $( [ \"$runner_type\" = \"official\" ] && echo \"$GH_REPO\" || echo \"https://github.com/$GH_REPO\" ) \
        'self-hosted,linux,x64'
    " > "$vm_log" 2>&1 &
    vm_pids+=($!)

    # Small stagger to avoid thundering herd on GitHub registration
    sleep 1
  done

  log "All $job_count VMs started. Waiting for runners to register..."
  sleep 10  # Pre-packed VMs: only need configure + register (~5s)

  # Dispatch workflow (use full filename)
  log "Dispatching workflow: $wf on $GH_REPO..."
  gh workflow run "$wf" -R "$GH_REPO" --ref main 2>&1
  sleep 5

  # Get run ID
  local run_id
  run_id=$(gh run list -R "$GH_REPO" -w "$wf" --json databaseId,status -q '.[0].databaseId')
  log "GitHub Run ID: $run_id"

  # Wait for GitHub run to complete
  log "Waiting for GitHub run $run_id to complete..."
  gh run watch "$run_id" -R "$GH_REPO" --exit-status 2>&1 || true

  # Wait for all VMs to finish
  log "Waiting for VM runners to exit..."
  local vm_failures=0
  for i in "${!vm_pids[@]}"; do
    local pid="${vm_pids[$i]}"
    local j=$((i + 1))
    if wait "$pid" 2>/dev/null; then
      log "VM job $j: OK"
    else
      log "VM job $j: FAILED (exit=$?)"
      vm_failures=$((vm_failures + 1))
    fi
  done

  local t_overall_end=$(ms)
  local total_ms=$((t_overall_end - t_overall_start))

  # Collect per-job timings from GitHub
  log "────────────────────────────────────────────────────────────"
  log "  Job results from GitHub:"
  gh run view "$run_id" -R "$GH_REPO" --json jobs --jq '.jobs[] | "\(.name)\t\(.conclusion)\t\(.startedAt)\t\(.completedAt)"' \
    | while IFS=$'\t' read -r name conclusion started completed; do
    log "  $name: $conclusion ($started → $completed)"
  done

  log "  Total wall time: ${total_ms}ms"
  log "  VM failures: $vm_failures"

  # Record result
  local result_json
  result_json=$(jq -n \
    --arg mode "github-$runner_type" \
    --arg workflow "$wf" \
    --arg run_id "$run_id" \
    --arg total_ms "$total_ms" \
    --arg vm_failures "$vm_failures" \
    --arg timestamp "$(now_iso)" \
    '{mode: $mode, workflow: $workflow, run_id: $run_id, total_ms: ($total_ms|tonumber), vm_failures: ($vm_failures|tonumber), timestamp: $timestamp}')

  echo "$result_json" >> "$RESULTS_DIR/${run_label}.jsonl"
  log "Result: $result_json"
}

# ── aksh-server mode ────────────────────────────────────────────────
run_aksh_server_mode() {
  local wf="$1"
  local label="${wf%.yml}"
  local run_label="${label}-aksh-server"

  log "════════════════════════════════════════════════════════════"
  log "  MODE: aksh-server + aksh-runner | WF: $wf"
  log "════════════════════════════════════════════════════════════"

  local wf_path="$HOST_WORKSPACE/benchmarks/real-world/$wf"
  local job_count
  job_count=$(count_jobs "$wf_path")
  log "Workflow has $job_count jobs → need $job_count VMs"

  # Start aksh-server on localhost, with Python proxy for VM access
  # (macOS blocks Rust binary on external interfaces, but Python works)
  local server_internal_port=9192
  local server_external_port=9191
  local host_ip
  host_ip=$(ifconfig en1 2>/dev/null | grep 'inet ' | awk '{print $2}') || true
  if [ -z "$host_ip" ]; then
    host_ip=$(ifconfig 2>/dev/null | grep 'inet ' | grep -v 127.0.0.1 | head -1 | awk '{print $2}') || true
  fi
  host_ip="${host_ip:-127.0.0.1}"
  local server_url="http://${host_ip}:${server_external_port}"

  # Use host-native binaries (macOS), not the linux-musl cross-compiled ones
  local server_bin="$HOST_WORKSPACE/target/release/preloop-server"
  local client_bin="$HOST_WORKSPACE/target/release/aksh-runner-client"

  # Kill stale processes
  pkill -f preloop-server 2>/dev/null || true
  pkill -f "python3.*tcp-proxy" 2>/dev/null || true
  sleep 0.5

  # Start aksh-server on localhost only
  log "Starting aksh-server on 127.0.0.1:${server_internal_port}..."
  AKSH_PUBLIC_URL="$server_url" RUST_LOG=info "$server_bin" serve \
    --listen "127.0.0.1:${server_internal_port}" \
    --state-dir "$TMP_DIR/aksh-state" \
    > "$TMP_DIR/aksh-server.log" 2>&1 &
  local server_pid=$!

  # Wait for server
  sleep 1
  local server_ok=0
  for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${server_internal_port}/healthz" >/dev/null 2>&1; then
      log "aksh-server ready (pid=$server_pid)"
      server_ok=1
      break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
      log "Server died!"
      cat "$TMP_DIR/aksh-server.log"
      return 1
    fi
    sleep 0.2
  done
  [ "$server_ok" = 1 ] || { log "Server failed to start"; return 1; }

  # Start Python TCP proxy: 0.0.0.0:9191 → 127.0.0.1:9192
  log "Starting Python TCP proxy ${host_ip}:${server_external_port} → 127.0.0.1:${server_internal_port}..."
  python3 -c "
import socket, threading
def proxy(client, target):
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.connect(target)
        def fwd(src, dst):
            while True:
                data = src.recv(8192)
                if not data: break
                dst.sendall(data)
        t1 = threading.Thread(target=fwd, args=(client, server), daemon=True)
        t2 = threading.Thread(target=fwd, args=(server, client), daemon=True)
        t1.start(); t2.start(); t1.join(); t2.join()
    except: pass
    finally: client.close()
def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', ${server_external_port}))
    s.listen(128)
    while True:
        c, a = s.accept()
        threading.Thread(target=proxy, args=(c, ('127.0.0.1', ${server_internal_port})), daemon=True).start()
main()
" > "$TMP_DIR/tcp-proxy.log" 2>&1 &
  local proxy_pid=$!
  sleep 0.5
  log "TCP proxy ready (pid=$proxy_pid)"

  # Start VM runners FIRST (they register and wait for jobs)
  log "Starting $job_count VMs (runners will register and poll)..."
  local vm_pids=()
  local vm_logs=()
  local runner_script="/workspace/benchmarks/real-world/vm-run-aksh.sh"
  local t_overall_start=$(ms)

  for i in $(seq 1 "$job_count"); do
    local vm_log="$TMP_DIR/vm-${label}-aksh-server-j${i}.log"
    vm_logs+=("$vm_log")

    log "Starting VM for job $i/$job_count..."

    smolvm machine run --net \
      --image "$VM_IMAGE" \
      --cpus "$VM_CPUS" --mem "$VM_MEM" --storage "$VM_STORAGE" \
      -v "$HOST_WORKSPACE:/workspace" \
      -v "$WARM_RUSTUP:/workspace/.rustup" \
      -v "$WARM_CARGO:/workspace/.cargo" \
      --env "RUNNER_TIMING_LOG=/tmp/runner-j${i}.log" \
      -- bash "$runner_script" "$i" "$server_url" "self-hosted,linux,x64" \
      > "$vm_log" 2>&1 &
    vm_pids+=($!)

    sleep 2
  done

  # Wait for all VMs to boot, install packages, configure, and register
  log "Waiting for runners to register (30s)..."
  sleep 30

  # NOW submit the workflow — all runners are connected
  log "Submitting workflow..."
  local submit_out
  submit_out=$("$client_bin" --server "http://127.0.0.1:${server_internal_port}" submit \
    -W "$wf_path" \
    --event workflow_dispatch \
    --workspace-root /tmp/aksh-conformance-push \
    --git-ref refs/heads/main 2>&1) || true
  log "Submit: $submit_out"

  # Wait for all VMs
  log "Waiting for $job_count VMs to complete..."
  local vm_failures=0
  for i in "${!vm_pids[@]}"; do
    local pid="${vm_pids[$i]}"
    local j=$((i + 1))
    if wait "$pid" 2>/dev/null; then
      log "VM job $j: OK"
    else
      log "VM job $j: FAILED (exit=$?)"
      vm_failures=$((vm_failures + 1))
    fi
  done

  local t_overall_end=$(ms)
  local total_ms=$((t_overall_end - t_overall_start))

  # Collect step timings from VM logs
  log "────────────────────────────────────────────────────────────"
  log "  Per-VM step timings:"
  for i in $(seq 1 "$job_count"); do
    local vm_log="$TMP_DIR/vm-${label}-aksh-server-j${i}.log"
    log "  --- VM job $i ---"
    grep -E "Running step:|Job .* completed:" "$vm_log" 2>/dev/null | head -20 || log "  (no step data)"
  done

  log "  Total wall time: ${total_ms}ms"
  log "  VM failures: $vm_failures"

  # Record result
  local result_json
  result_json=$(jq -n \
    --arg mode "aksh-server" \
    --arg workflow "$wf" \
    --arg total_ms "$total_ms" \
    --arg vm_failures "$vm_failures" \
    --arg timestamp "$(now_iso)" \
    '{mode: $mode, workflow: $workflow, total_ms: ($total_ms|tonumber), vm_failures: ($vm_failures|tonumber), timestamp: $timestamp}')

  echo "$result_json" >> "$RESULTS_DIR/${run_label}.jsonl"
  log "Result: $result_json"

  # Stop proxy and server
  log "Stopping TCP proxy (pid=$proxy_pid)..."
  kill "$proxy_pid" 2>/dev/null || true
  log "Stopping aksh-server (pid=$server_pid)..."
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
}

# ── Main ────────────────────────────────────────────────────────────
log "================================================================"
log "  e2e-per-job-smolvm-bench.sh"
log "  Mode: $MODE | Workflows: ${WORKFLOWS[*]}"
log "  Host: $(hostname) | $(date)"
log "  VM: $VM_CPUS vCPU, ${VM_MEM}MB RAM, $VM_IMAGE"
log "================================================================"

# Verify prerequisites
for prereq in smolvm gh jq; do
  command -v "$prereq" >/dev/null 2>&1 || { log "ERROR: $prereq not found"; exit 1; }
done

# Verify image is cached
if ! smolvm machine ls --json 2>/dev/null | jq -e '.[] | select(.image == "ubuntu:24.04")' >/dev/null 2>&1; then
  log "Pulling ubuntu:24.04 image once..."
  smolvm machine run --image ubuntu:24.04 -- echo "image ready" 2>&1 | tail -1
fi

for wf in "${WORKFLOWS[@]}"; do
  case "$MODE" in
    github-official) run_github_mode "official" "$wf" ;;
    github-aksh)     run_github_mode "aksh" "$wf" ;;
    aksh-server)     run_aksh_server_mode "$wf" ;;
    *) log "Unknown mode: $MODE"; exit 1 ;;
  esac

  # Cooldown between workflows
  log "Cooldown: 10s before next workflow..."
  sleep 10
done

log ""
log "================================================================"
log "  DONE — Results in: $RESULTS_DIR/"
log "  Orchestrator log: $TMP_DIR/orchestrator.log"
log "================================================================"
