#!/usr/bin/env bash
# Recapture scenarios 15, 19, 21, 22, 23, 24 with both runners.
# Official runs on bench-aksh-2; aksh runs in parallel on bench-aksh-{3,4,5,6}.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCENARIOS=(15-oidc-id-token 19-step-summary 21-job-timeout 22-cancel-semantics 23-context-fields 24-problem-matcher)
AKSH_VMS=(bench-aksh-3 bench-aksh-4 bench-aksh-5 bench-aksh-6)
OFFICIAL_VM="bench-aksh-2"

log() { echo "[$(date -u +%H:%M:%S.%3NZ)] $*"; }

# --- Phase 1: Official captures (sequential on bench-aksh-2) ---
log "=== Phase 1: Official runner captures on $OFFICIAL_VM ==="
for sc in "${SCENARIOS[@]}"; do
  log "Capturing official: $sc"
  "$SCRIPT_DIR/direct-capture.sh" "$sc.yml" "$OFFICIAL_VM" official 2>&1 | tail -3
  log "Done official: $sc"
  sleep 5
done

# --- Phase 2: Aksh captures (parallel across 4 VMs) ---
log "=== Phase 2: Aksh runner captures (parallel) ==="
pids=()
vm_idx=0
for sc in "${SCENARIOS[@]}"; do
  vm="${AKSH_VMS[$((vm_idx % ${#AKSH_VMS[@]}))]}"
  log "Capturing aksh: $sc on $vm"
  "$SCRIPT_DIR/direct-capture.sh" "$sc.yml" "$vm" aksh 2>&1 | tail -3 &
  pids+=($!)
  vm_idx=$((vm_idx + 1))
  sleep 2
done

log "Waiting for ${#pids[@]} aksh captures..."
for pid in "${pids[@]}"; do
  wait "$pid" || log "WARN: capture pid $pid failed"
done

log "=== All captures complete ==="
