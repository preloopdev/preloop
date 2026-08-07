#!/usr/bin/env bash
# Recapture scenarios 15, 19, 21, 22, 23, 24 with both runners.
# Official runs on bench-preloop-2; preloop runs in parallel on bench-preloop-{3,4,5,6}.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCENARIOS=(15-oidc-id-token 19-step-summary 21-job-timeout 22-cancel-semantics 23-context-fields 24-problem-matcher)
PRELOOP_VMS=(bench-preloop-3 bench-preloop-4 bench-preloop-5 bench-preloop-6)
OFFICIAL_VM="bench-preloop-2"

log() { echo "[$(date -u +%H:%M:%S.%3NZ)] $*"; }

# --- Phase 1: Official captures (sequential on bench-preloop-2) ---
log "=== Phase 1: Official runner captures on $OFFICIAL_VM ==="
for sc in "${SCENARIOS[@]}"; do
  log "Capturing official: $sc"
  "$SCRIPT_DIR/direct-capture.sh" "$sc.yml" "$OFFICIAL_VM" official 2>&1 | tail -3
  log "Done official: $sc"
  sleep 5
done

# --- Phase 2: Preloop captures (parallel across 4 VMs) ---
log "=== Phase 2: Preloop runner captures (parallel) ==="
pids=()
vm_idx=0
for sc in "${SCENARIOS[@]}"; do
  vm="${PRELOOP_VMS[$((vm_idx % ${#PRELOOP_VMS[@]}))]}"
  log "Capturing preloop: $sc on $vm"
  "$SCRIPT_DIR/direct-capture.sh" "$sc.yml" "$vm" preloop 2>&1 | tail -3 &
  pids+=($!)
  vm_idx=$((vm_idx + 1))
  sleep 2
done

log "Waiting for ${#pids[@]} preloop captures..."
for pid in "${pids[@]}"; do
  wait "$pid" || log "WARN: capture pid $pid failed"
done

log "=== All captures complete ==="
