#!/usr/bin/env bash
set -euo pipefail
# Record all scenarios for a given backend.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    echo "Usage: $0 --backend {official|runner-server|aksh} [--non-interactive]" >&2
    exit 1
}

BACKEND=""
NON_INTERACTIVE=""
while [ $# -gt 0 ]; do
    case "$1" in
        --backend) BACKEND="$2"; shift 2 ;;
        --non-interactive) NON_INTERACTIVE="--non-interactive"; shift ;;
        *) usage ;;
    esac
done
[ -z "$BACKEND" ] && usage

SCENARIOS=""
for d in "$MITM_DIR"/scenarios/*/; do
    name=$(basename "$d")
    if [ -f "$d/scenario.toml" ]; then
        SCENARIOS="$SCENARIOS $name"
    fi
done

echo "Recording all scenarios for backend=$BACKEND"
echo "Scenarios:$SCENARIOS"
echo ""

PASS=0
FAIL=0
for sc in $SCENARIOS; do
    echo "=== Recording $sc ==="
    if "$SCRIPT_DIR/record.sh" --backend "$BACKEND" --scenario "$sc" $NON_INTERACTIVE; then
        PASS=$((PASS + 1))
    else
        echo "FAILED: $sc" >&2
        FAIL=$((FAIL + 1))
    fi
    echo ""
done

echo "=== Recording complete ==="
echo "passed: $PASS, failed: $FAIL"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
