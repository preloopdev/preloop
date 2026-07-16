#!/usr/bin/env bash
# Batch capture remaining scenarios that have no MITM flow data yet.
# Runs sequentially per scenario, using the same VM (bench-aksh-1).
# Scenarios that already have captures are skipped.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS="$SCRIPT_DIR/../compatibility/runner/protocol"

# All remaining scenarios from CAPTURE-INVENTORY.md that have "—" for both sides
SCENARIOS=(
  # Already running in parallel: 19, 21, 22, 23, 24, 50
  51-action-contexts.yml
  52-expression-features.yml
  53-secret-masking.yml
  54-job-annotations.yml
  55-proxy-injection.yml
  56-problem-matcher-frompath.yml
  57-runner-settings.yml
  58-auth-and-diag.yml
  60-hashfiles-and-fips.yml
  61-cache-stress.yml
  62-artifact-stress.yml
  63-mega-runner-stress.yml
  70-defaults-run.yml
  71-composite-advanced.yml
  72-label-matching.yml
  73-path-env.yml
  74-broker-poll-timing.yml
)

log() { echo "[$(date -u +%H:%M:%S.%3NZ)] $*"; }

already_captured() {
  local sc="${1%.yml}"
  [ -d "$RESULTS/$sc/official" ] && [ -d "$RESULTS/$sc/aksh" ]
}

cd "$SCRIPT_DIR/.."

for wf in "${SCENARIOS[@]}"; do
  sc="${wf%.yml}"
  if already_captured "$wf"; then
    log "SKIP $sc — already captured"
    continue
  fi
  log "CAPTURING $sc"
  bash "$SCRIPT_DIR/runner-flow-capture.sh" "$wf" both 1 || log "WARN: capture failed for $sc"
  log "DONE $sc"
  sleep 5
done

log "=== Batch capture complete ==="
