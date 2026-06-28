#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Golden captures ==="
echo ""

GOLDEN_ROOT="$MITM_DIR/golden"
if [ ! -d "$GOLDEN_ROOT" ]; then
    echo "No golden captures found. Run bin/record-golden.sh first."
    exit 0
fi

for version_dir in "$GOLDEN_ROOT"/v*/; do
    [ -d "$version_dir" ] || continue
    version=$(basename "$version_dir")
    echo "--- $version ---"
    for scenario_dir in "$version_dir"/*/; do
        [ -d "$scenario_dir" ] || continue
        scenario=$(basename "$scenario_dir")
        flow_count=$(wc -l < "$scenario_dir/flows.jsonl" 2>/dev/null || echo 0)
        has_mitm="no"
        [ -f "$scenario_dir/flows.mitm" ] && has_mitm="yes"
        echo "  $scenario: $flow_count flows, mitm=$has_mitm"
    done
    echo ""
done

# Also show latest captures per backend.
echo "=== Latest captures ==="
echo ""
CAPTURES_ROOT="$MITM_DIR/captures"
if [ -d "$CAPTURES_ROOT" ]; then
    for backend_dir in "$CAPTURES_ROOT"/*/; do
        [ -d "$backend_dir" ] || continue
        backend=$(basename "$backend_dir")
        for scenario_dir in "$backend_dir"/*/; do
            [ -d "$scenario_dir" ] || continue
            scenario=$(basename "$scenario_dir")
            latest="$scenario_dir/latest"
            if [ -L "$latest" ] && [ -d "$latest" ]; then
                flow_count=$(wc -l < "$latest/flows.jsonl" 2>/dev/null || echo 0)
                echo "  $backend/$scenario: $flow_count flows"
            fi
        done
    done
fi
