#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    echo "Usage: $0 --scenario <name> [--left <backend>] [--right <backend>]" >&2
    echo "  Defaults: --left official --right runner-server" >&2
    exit 1
}

SCENARIO=""
LEFT="official"
RIGHT="runner-server"
while [ $# -gt 0 ]; do
    case "$1" in
        --scenario) SCENARIO="$2"; shift 2 ;;
        --left) LEFT="$2"; shift 2 ;;
        --right) RIGHT="$2"; shift 2 ;;
        *) usage ;;
    esac
done
[ -z "$SCENARIO" ] && usage

LEFT_DIR="$MITM_DIR/captures/$LEFT/$SCENARIO/latest"
RIGHT_DIR="$MITM_DIR/captures/$RIGHT/$SCENARIO/latest"
TIMESTAMP=$(date -u +%Y-%m-%dT%H-%M-%SZ)
OUTPUT="$MITM_DIR/reports/$SCENARIO/$TIMESTAMP.md"

if [ ! -d "$LEFT_DIR" ]; then
    echo "$LEFT capture not found at $LEFT_DIR (run recording first)" >&2
    exit 4
fi
if [ ! -d "$RIGHT_DIR" ]; then
    echo "$RIGHT capture not found at $RIGHT_DIR (run recording first)" >&2
    exit 4
fi

"$SCRIPT_DIR/_compare.py" \
    --scenario "$SCENARIO" \
    --left-dir "$LEFT_DIR" \
    --right-dir "$RIGHT_DIR" \
    --left-label "$LEFT" \
    --right-label "$RIGHT" \
    --output "$OUTPUT"

echo "report: $OUTPUT"
