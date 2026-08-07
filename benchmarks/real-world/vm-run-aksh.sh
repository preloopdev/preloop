#!/usr/bin/env bash
# vm-run-preloop.sh — configure + run preloop Rust runner inside per-job smolvm
# Usage: vm-run-preloop.sh <job_index> <server_url> <runner_labels>
# server_url: https://github.com/<repo> or http://<host>:<port>
set -euo pipefail

JOB_INDEX="${1:?Usage: $0 <job_index> <server_url> <runner_labels>}"
SERVER_URL="${2:?}"
LABELS="${3:-self-hosted,linux,x64}"

RUNNER_ROOT="/tmp/preloop-j${JOB_INDEX}"
RUNNER_NAME="e2e-preloop-${JOB_INDEX}-$(date +%s)"
TIMING_LOG="${RUNNER_TIMING_LOG:-/tmp/runner-j${JOB_INDEX}.log}"
# Find preloop-runner: prefer mounted /opt/runners, then workspace build
if [ -x /opt/runners/preloop-runner ]; then
  PRELOOP_RUNNER="/opt/runners/preloop-runner"
elif [ -x /opt/preloop/preloop-runner ]; then
  PRELOOP_RUNNER="/opt/preloop/preloop-runner"
else
  PRELOOP_RUNNER="/workspace/target/aarch64-unknown-linux-musl/release/preloop-runner"
fi

log() { echo "[preloop-runner-j${JOB_INDEX} $(date +%T.%3N)] $*"; }

# Setup — ensure cargo is in PATH
export PATH="/root/.cargo/bin:$PATH"
bash /workspace/benchmarks/real-world/vm-setup-common.sh

log "Configuring preloop-runner at $RUNNER_ROOT..."
rm -rf "$RUNNER_ROOT"
mkdir -p "$RUNNER_ROOT"

# Get registration token if connecting to GitHub
REG_TOKEN="t"
if [[ "$SERVER_URL" == https://github.com/* ]]; then
  REG_TOKEN="${GH_REG_TOKEN:-}"
  if [ -z "$REG_TOKEN" ]; then
    log "ERROR: GH_REG_TOKEN required for GitHub mode"
    exit 1
  fi
fi

RUST_LOG=info "$PRELOOP_RUNNER" --runner-root "$RUNNER_ROOT" configure \
  --url "$SERVER_URL" \
  --token "$REG_TOKEN" \
  --name "$RUNNER_NAME" \
  --unattended \
  --replace \
  --ephemeral \
  --labels "$LABELS" 2>&1 | tail -3

log "Configuration complete. Running --once..."

# Record start time
echo "RUNNER_START_MS=$(date +%s%3N)" >> "$TIMING_LOG"

RUST_LOG=info "$PRELOOP_RUNNER" --runner-root "$RUNNER_ROOT" run --once 2>&1 | while IFS= read -r line; do
  echo "[$(date +%T.%3N)] $line"
done

EXIT_CODE=$?
echo "RUNNER_EXIT_CODE=$EXIT_CODE" >> "$TIMING_LOG"
echo "RUNNER_END_MS=$(date +%s%3N)" >> "$TIMING_LOG"

log "Runner exited with code $EXIT_CODE"

# Extract step timings
log "Step summary:"
grep -E "Running step:|Job .* completed:" "$RUNNER_ROOT/_diag"/*.log 2>/dev/null || true

exit $EXIT_CODE
