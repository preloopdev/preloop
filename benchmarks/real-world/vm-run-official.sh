#!/usr/bin/env bash
# vm-run-official.sh — configure + run official C# runner inside per-job smolvm
# Usage: vm-run-official.sh <job_index> <gh_repo> <runner_labels>
# Env: GH_REG_TOKEN (registration token), RUNNER_TIMING_LOG
set -euo pipefail

JOB_INDEX="${1:?Usage: $0 <job_index> <gh_repo> <runner_labels>}"
GH_REPO="${2:?}"
LABELS="${3:-self-hosted,linux,x64}"

: "${GH_REG_TOKEN:?GH_REG_TOKEN env required}"

RUNNER_ROOT="/tmp/official-j${JOB_INDEX}"
RUNNER_NAME="e2e-off-${JOB_INDEX}-$(date +%s)"
TIMING_LOG="${RUNNER_TIMING_LOG:-/tmp/runner-j${JOB_INDEX}.log}"
# Find official runner: prefer mounted /opt/runners, then /opt/actions-runner
if [ -d /opt/runners/actions-runner ]; then
  OFFICIAL_SRC="/opt/runners/actions-runner"
else
  OFFICIAL_SRC="/opt/actions-runner"
fi
OFFICIAL_DIR="/tmp/runner-bin-j${JOB_INDEX}"

log() { echo "[off-runner-j${JOB_INDEX} $(date +%T.%3N)] $*"; }

# Setup — ensure cargo is in PATH
export PATH="/root/.cargo/bin:$PATH"
bash /workspace/benchmarks/real-world/vm-setup-common.sh

# Official runner refuses root — create a runner user
if [ "$(id -u)" = "0" ]; then
  useradd -m -s /bin/bash runner 2>/dev/null || true
  # Give runner user access to workspace and toolchains
  chown -R runner:runner /tmp
fi

log "Copying official runner to $OFFICIAL_DIR..."
cp -a "$OFFICIAL_SRC" "$OFFICIAL_DIR"
rm -rf "$RUNNER_ROOT"
mkdir -p "$RUNNER_ROOT"
chown -R runner:runner "$RUNNER_ROOT" "$OFFICIAL_DIR"

cd "$OFFICIAL_DIR"
su runner -c "./config.sh \
  --url 'https://github.com/${GH_REPO}' \
  --token '${GH_REG_TOKEN}' \
  --name '${RUNNER_NAME}' \
  --work '${RUNNER_ROOT}' \
  --unattended \
  --replace \
  --ephemeral \
  --labels '${LABELS}'" 2>&1 | tail -3

log "Configuration complete. Running --once..."
cd "$OFFICIAL_DIR"

# Record start time
echo "RUNNER_START_MS=$(date +%s%3N)" >> "$TIMING_LOG"

su runner -c "timeout 900 ./run.sh --once" 2>&1 | while IFS= read -r line; do
  echo "[$(date +%T.%3N)] $line"
done

EXIT_CODE=$?
echo "RUNNER_EXIT_CODE=$EXIT_CODE" >> "$TIMING_LOG"
echo "RUNNER_END_MS=$(date +%s%3N)" >> "$TIMING_LOG"

log "Runner exited with code $EXIT_CODE"

# Extract step timings from diag
if [ -d "$RUNNER_ROOT/_diag" ]; then
  log "Diagnostic logs:"
  find "$RUNNER_ROOT/_diag" -name "Worker_*.log" -exec tail -50 {} \; 2>/dev/null || true
fi

exit $EXIT_CODE
