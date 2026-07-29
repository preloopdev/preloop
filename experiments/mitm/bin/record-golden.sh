#!/usr/bin/env bash
set -euo pipefail
# Record golden captures for all scenarios against the official GitHub backend.
# Golden captures are stored in golden/v<runner-version>/ per scenario.
unset GITHUB_TOKEN 2>/dev/null || true
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE="$MITM_DIR/.cache"

usage() {
    echo "Usage: $0 [--scenario <name>] [--non-interactive]" >&2
    echo "  Without --scenario, records all scenarios." >&2
    exit 1
}

SCENARIO=""
NON_INTERACTIVE=false
while [ $# -gt 0 ]; do
    case "$1" in
        --scenario) SCENARIO="$2"; shift 2 ;;
        --non-interactive) NON_INTERACTIVE=true; shift ;;
        *) usage ;;
    esac
done

RUNNER_VERSION=$(grep runner_version "$MITM_DIR/versions.toml" | cut -d'"' -f2)
GOLDEN_DIR="$MITM_DIR/golden/v$RUNNER_VERSION"
mkdir -p "$GOLDEN_DIR"

# Collect scenarios.
if [ -n "$SCENARIO" ]; then
    SCENARIOS="$SCENARIO"
else
    SCENARIOS=""
    for d in "$MITM_DIR"/scenarios/*/; do
        name=$(basename "$d")
        if [ -f "$d/scenario.toml" ]; then
            SCENARIOS="$SCENARIOS $name"
        fi
    done
fi

echo "Recording golden captures for runner v$RUNNER_VERSION"
echo "Scenarios:$SCENARIOS"
echo "Output: $GOLDEN_DIR"
echo ""

PASS=0
FAIL=0
for sc in $SCENARIOS; do
    echo "=== Recording $sc ==="
    if "$SCRIPT_DIR/record.sh" --backend official --scenario "$sc" --non-interactive; then
        # Copy the latest capture to golden.
        LATEST="$MITM_DIR/captures/official/$sc/latest"
        STATUS="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("status",""))' "$LATEST/summary.json" 2>/dev/null || true)"
        FLOWS_COUNT="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("flows_count",0))' "$LATEST/summary.json" 2>/dev/null || echo 0)"
        if [ -d "$LATEST" ] && [ "$STATUS" = "ok" ] && [ "$FLOWS_COUNT" -gt 0 ]; then
            DEST="$GOLDEN_DIR/$sc"
            rm -rf "$DEST"
            cp -rL "$LATEST" "$DEST"
            echo "golden saved: $DEST"
            PASS=$((PASS + 1))
        else
            echo "WARNING: capture for $sc was not usable (status=$STATUS, flows=$FLOWS_COUNT)" >&2
            FAIL=$((FAIL + 1))
        fi
    else
        echo "FAILED: $sc" >&2
        FAIL=$((FAIL + 1))
    fi
    echo ""
done

echo "=== Golden recording complete ==="
echo "passed: $PASS, failed: $FAIL"
echo "golden dir: $GOLDEN_DIR"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
