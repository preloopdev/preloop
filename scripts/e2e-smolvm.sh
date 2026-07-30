#!/usr/bin/env bash
# e2e-smolvm.sh — Run E2E workflow tests using independent smolVMs per job.
#
# Each job gets its own ephemeral Alpine smolvm with the Rust runner binary.
# The VM is created, runs one job, and is destroyed.
#
# Usage: ./scripts/e2e-smolvm.sh [--jobs N] [workflow.yml]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVER_PORT="${AKSH_PORT:-9191}"
SERVER_URL="http://127.0.0.1:${SERVER_PORT}"
SERVER_BIN="$REPO_ROOT/target/release/preloop-server"
RUNNER_BIN="$REPO_ROOT/target/aarch64-unknown-linux-musl/release/preloop-runner"
MAX_JOBS="${MAX_JOBS:-3}"

red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
info()  { printf '\033[1;34m▸ %s\033[0m\n' "$*"; }

# Parse args
WORKFLOW_FILE="${1:-}"
JOBS_FLAG=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jobs) MAX_JOBS="$2"; shift 2 ;;
        *) WORKFLOW_FILE="$1"; shift ;;
    esac
done

cleanup() {
    pkill -f "preloop-server.*${SERVER_PORT}" 2>/dev/null || true
    # Clean up any test VMs
    for vm in $(smolvm machine ls --json 2>/dev/null | python3 -c 'import sys,json; [print(m["name"]) for m in json.load(sys.stdin) if m["name"].startswith("e2e-job-")]' 2>/dev/null); do
        smolvm machine stop --name "$vm" 2>/dev/null || true
        smolvm machine delete --name "$vm" -f 2>/dev/null || true
    done
}
trap cleanup EXIT

# Start server
info "Starting aksh server on port $SERVER_PORT..."
pkill -f "preloop-server.*${SERVER_PORT}" 2>/dev/null || true
sleep 1
RUST_LOG=info AKSH_PUBLIC_URL="$SERVER_URL" "$SERVER_BIN" serve --listen "0.0.0.0:${SERVER_PORT}" > /tmp/aksh-e2e-server.log 2>&1 &
sleep 2
curl -sf "$SERVER_URL/healthz" > /dev/null || { red "Server failed to start"; exit 1; }
info "Server running"

# Create a runner VM template
run_job_in_vm() {
    local vm_name="e2e-job-$RANDOM"
    local runner_name="$vm_name"

    info "Creating VM $vm_name..."
    smolvm machine create --name "$vm_name" --net --image alpine > /dev/null 2>&1
    smolvm machine start --name "$vm_name" > /dev/null 2>&1

    # Install deps and copy runner
    smolvm machine exec --name "$vm_name" -- sh -c 'apk add --no-cache bash curl git > /dev/null 2>&1'
    smolvm machine cp "$RUNNER_BIN" "$vm_name:/workspace/aksh-runner"
    smolvm machine exec --name "$vm_name" -- chmod +x /workspace/aksh-runner

    # Configure runner
    smolvm machine exec --name "$vm_name" -- /workspace/aksh-runner configure \
        --url "$SERVER_URL" \
        --token aksh-system-token \
        --name "$runner_name" \
        --labels self-hosted,Linux,ARM64 \
        --work /workspace/_work \
        --ephemeral 2>&1 | tail -1

    # Run the runner (picks up one job)
    info "Running job in VM $vm_name..."
    smolvm machine exec --name "$vm_name" -- \
        /workspace/aksh-runner run --once 2>&1 | tail -3

    # Cleanup
    smolvm machine stop --name "$vm_name" > /dev/null 2>&1
    smolvm machine delete --name "$vm_name" -f > /dev/null 2>&1
    info "VM $vm_name cleaned up"
}

# Default workflow
if [ -z "$WORKFLOW_FILE" ]; then
    WORKFLOW_YAML='name: smolvm-e2e
on: push
jobs:
  job1:
    runs-on: self-hosted
    steps:
      - name: Hello from VM 1
        run: |
          echo "Job 1 running in $(hostname)"
          uname -a
          echo "PID: $$"
  job2:
    runs-on: self-hosted
    needs: job1
    steps:
      - name: Hello from VM 2
        run: |
          echo "Job 2 running in $(hostname)"
          echo "Job 1 completed, I can run!"'
else
    WORKFLOW_YAML=$(cat "$WORKFLOW_FILE")
fi

# Submit workflow
info "Submitting workflow..."
PAYLOAD=$(python3 -c "
import json
print(json.dumps({
    'workflow_yaml': '''$WORKFLOW_YAML''',
    'event': 'push',
    'repository': 'test/repo',
    'git_ref': 'refs/heads/main'
}))
")
RESULT=$(curl -s -X POST "$SERVER_URL/api/v1/runs" \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Bearer aksh-system-token' \
    -d "$PAYLOAD")
RUN_ID=$(echo "$RESULT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["run_id"])')
QUEUED=$(echo "$RESULT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["queued_jobs"])')
info "Run submitted: $RUN_ID ($QUEUED jobs queued)"

# Run each job in its own VM
for i in $(seq 1 "$QUEUED"); do
    run_job_in_vm
done

# Check final status
FINAL=$(curl -s "$SERVER_URL/api/v1/runs/$RUN_ID" \
    -H 'Authorization: Bearer aksh-system-token')
STATUS=$(echo "$FINAL" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')

echo ""
if [ "$STATUS" = "success" ]; then
    green "✅ Run $RUN_ID: $STATUS"
else
    red "❌ Run $RUN_ID: $STATUS"
    echo "$FINAL" | python3 -m json.tool
fi

cleanup
