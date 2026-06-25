#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    echo "Usage: $0 --scenario <name>" >&2
    exit 1
}

SCENARIO=""
while [ $# -gt 0 ]; do
    case "$1" in
        --scenario) SCENARIO="$2"; shift 2 ;;
        *) usage ;;
    esac
done
[ -z "$SCENARIO" ] && usage

OFFICIAL_DIR="$MITM_DIR/captures/official/$SCENARIO/latest"
RS_DIR="$MITM_DIR/captures/runner-server/$SCENARIO/latest"
TIMESTAMP=$(date -u +%Y-%m-%dT%H-%M-%SZ)
OUTPUT="$MITM_DIR/reports/$SCENARIO/$TIMESTAMP.md"

if [ ! -d "$OFFICIAL_DIR" ]; then
    echo "official capture not found at $OFFICIAL_DIR (run recording first)" >&2
    exit 4
fi
if [ ! -d "$RS_DIR" ]; then
    echo "runner-server capture not found at $RS_DIR (run recording first)" >&2
    exit 4
fi

"$SCRIPT_DIR/_compare.py" \
    --scenario "$SCENARIO" \
    --official-dir "$OFFICIAL_DIR" \
    --runner-server-dir "$RS_DIR" \
    --output "$OUTPUT"

echo "report: $OUTPUT"
