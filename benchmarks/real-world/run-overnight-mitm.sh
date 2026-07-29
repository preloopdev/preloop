#!/usr/bin/env bash
# Capture official and Aksh runner traffic for the overnight workflow corpus.
# The repository defaults to the personal conformance repo; override GH_REPO
# when running against another fork.
set -euo pipefail

MODE="${1:-both}"
JOB_COUNT="${JOB_COUNT:-8}"
START_AT="${START_AT:-101}"
SCENARIO_GLOB="${SCENARIO_GLOB:-10[1-9]|110}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

case "$MODE" in
  official|aksh|both) ;;
  *) echo "usage: $0 <official|aksh|both>" >&2; exit 2 ;;
esac

scenarios=()
while IFS= read -r scenario; do
  [ -n "$scenario" ] && scenarios+=("$scenario")
done < <(
  find "$SCRIPT_DIR/overnight-workflows" -maxdepth 1 -type f -name '*.yml' -print \
    | sort \
    | while read -r path; do
        name="$(basename "$path")"
        if [[ "$name" =~ ^10[1-9]- || "$name" =~ ^110- ]]; then
          echo "$name"
        fi
      done
)
for scenario in "${scenarios[@]}"; do
  number="${scenario%%-*}"
  [ "$number" -lt "$START_AT" ] && continue
  echo "=== MITM capture: $scenario ($MODE) repo=${GH_REPO:-preloopdev/aksh-conformance} ==="
  GH_REPO="${GH_REPO:-preloopdev/aksh-conformance}" \
    "$SCRIPT_DIR/runner-flow-capture.sh" "$scenario" "$MODE" "$JOB_COUNT"
done
