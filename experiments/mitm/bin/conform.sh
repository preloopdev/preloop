#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    echo "Usage: $0 --golden <capture-dir|scenario-name> --target <backend> --scenario <name>" >&2
    echo "" >&2
    echo "Run a conformance test: replay golden capture against target backend, then compare." >&2
    echo "" >&2
    echo "  --golden    Path to golden capture dir, or scenario name (uses latest official capture)" >&2
    echo "  --target    Backend to test (e.g. aksh, runner-server)" >&2
    echo "  --scenario  Scenario name for the comparison report" >&2
    echo "  --timeout   Max seconds for replay (default: 120)" >&2
    exit 1
}

GOLDEN=""
TARGET=""
SCENARIO=""
TIMEOUT=120
while [ $# -gt 0 ]; do
    case "$1" in
        --golden) GOLDEN="$2"; shift 2 ;;
        --target) TARGET="$2"; shift 2 ;;
        --scenario) SCENARIO="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        *) usage ;;
    esac
done
[ -z "$GOLDEN" ] && usage
[ -z "$TARGET" ] && usage
[ -z "$SCENARIO" ] && usage

echo "=== Conformance test: $SCENARIO ==="
echo "golden: $GOLDEN"
echo "target: $TARGET"
echo ""

# Step 1: Run replay to generate target traffic.
echo "--- Step 1: Replay golden traffic against $TARGET ---"
"$SCRIPT_DIR/replay.sh" \
    --golden "$GOLDEN" \
    --target "$TARGET" \
    --timeout "$TIMEOUT"

# Step 2: Compare golden vs replayed.
echo ""
echo "--- Step 2: Compare captures ---"

# Resolve golden dir for comparison.
if [ -d "$GOLDEN" ]; then
    GOLDEN_DIR="$GOLDEN"
else
    GOLDEN_DIR="$MITM_DIR/captures/official/$GOLDEN/latest"
fi

# Find the latest replay capture.
REPLAY_DIR=$(find "$MITM_DIR/captures/replay-$TARGET" -maxdepth 1 -type d | sort | tail -1)
if [ -z "$REPLAY_DIR" ] || [ ! -d "$REPLAY_DIR" ]; then
    echo "no replay capture found for $TARGET" >&2
    exit 4
fi

TIMESTAMP=$(date -u +%Y-%m-%dT%H-%M-%SZ)
OUTPUT="$MITM_DIR/reports/conformance-$SCENARIO/$TIMESTAMP.md"

"$SCRIPT_DIR/_compare.py" \
    --scenario "$SCENARIO" \
    --left-dir "$GOLDEN_DIR" \
    --right-dir "$REPLAY_DIR" \
    --left-label "golden" \
    --right-label "$TARGET" \
    --output "$OUTPUT"

echo ""
echo "=== Conformance report: $OUTPUT ==="
