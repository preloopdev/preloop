#!/usr/bin/env bash
# campaign-cell.sh — run all 2xx scenarios for one cell, in order.
# usage: campaign-cell.sh <cell> [--vm <name>] [--scenarios "201-... 210-..."]
set -euo pipefail
CELL="${1:?cell required}"
shift || true
VM_ARGS=()
SCENS="${SCENARIOS:-}"
while [ $# -gt 0 ]; do
  case "$1" in
    --vm) VM_ARGS=(--vm "$2"); shift 2 ;;
    --scenarios) SCENS="$2"; shift 2 ;;
    *) shift ;;
  esac
done
case "$CELL" in gh-*) VM="${VM:-camp-gh}" ;; pl-*) VM="${VM:-camp-pl}" ;; esac
[ ${#VM_ARGS[@]} -eq 0 ] && VM_ARGS=(--vm "$VM")

if [ -z "$SCENS" ]; then
  SCENS=$(ls "$PWD/experiments/mitm/scenarios" | grep -E '^2[0-9][0-9]-' | sort)
fi

PASS=0; FAIL=0; FAILED=()
for sc in $SCENS; do
  echo "===== $CELL / $sc ====="
  if "$PWD/experiments/mitm/bin/campaign.sh" --cell "$CELL" --scenario "$sc" "${VM_ARGS[@]}"; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1)); FAILED+=("$sc")
  fi
done
echo "===== cell $CELL done: pass=$PASS fail=$FAIL ${FAILED[*]:-} ====="
