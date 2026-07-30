#!/usr/bin/env bash
# Run official runner -> GitHub and official runner -> aksh comparisons.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_ROOT="$ROOT/benchmarks/compatibility/server/behavior"
REPORT="$RESULTS_ROOT/SERVER-CONFORMANCE-REPORT.md"
SCENARIO_LIST="${SERVER_CONFORMANCE_SCENARIOS:-200-v2336-combined.yml 201-v2336-background-cancel.yml 202-v2336-file-commands.yml}"
read -r -a SCENARIOS <<< "$SCENARIO_LIST"
FAILED=0

for scenario in "${SCENARIOS[@]}"; do
  echo "=== server deep: $scenario ==="
  if ! bash "$ROOT/scripts/compare-servers.sh" "$scenario"; then
    FAILED=1
  fi
done

scenario_stems=()
for scenario in "${SCENARIOS[@]}"; do
  scenario_stems+=("${scenario%.yml}")
done
if ! python3 "$ROOT/scripts/check-server-conformance.py" \
  --root "$RESULTS_ROOT" --output "$REPORT" "${scenario_stems[@]}"; then
  FAILED=1
fi

exit "$FAILED"
