#!/usr/bin/env bash
# run-comparison.sh — Run all comparison tools and produce a unified report
#
# Usage:
#   ./run-comparison.sh                    # Compare existing data
#   ./run-comparison.sh --capture aksh     # Re-run all scenarios with aksh, then compare
#   ./run-comparison.sh --capture both     # Re-run with both runners, then compare
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$BENCH_DIR/../compatibility/runner/behavior"
CAPTURE_MODE="${1:-}"
CAPTURE_RUNNER="${2:-}"

log() { echo "[$(date +%T.%3N)] $*"; }

# ── Step 1: Optionally re-run conformance scenarios ─────────────────
if [ "$CAPTURE_MODE" = "--capture" ]; then
    RUNNER="${CAPTURE_RUNNER:?Usage: $0 --capture <aksh|official|both>}"
    log "Running batch conformance with runner=$RUNNER..."
    bash "$BENCH_DIR/batch-conformance.sh" "$RUNNER"
    log "Batch conformance complete."
fi

# ── Step 2: Step-level + outcome comparison ─────────────────────────
log "Running step-level conformance diff..."
python3 "$BENCH_DIR/conformance-diff.py" \
    --official "$RESULTS_DIR/conformance-official.jsonl" \
    --aksh "$RESULTS_DIR/conformance-aksh.jsonl" \
    --output "$RESULTS_DIR/CONFORMANCE-REPORT.md"

# ── Step 3: Log content comparison ──────────────────────────────────
log "Running log content diff..."
python3 "$BENCH_DIR/log-content-diff.py" \
    --batch \
    --flows-root "$BENCH_DIR/../compatibility/runner/protocol" \
    --output "$RESULTS_DIR/LOG-CONTENT-REPORT.md"

# ── Step 4: Flow-level diffs for captured scenarios ─────────────────
log "Running flow diffs for captured scenarios..."
FLOW_REPORT="$RESULTS_DIR/FLOW-DIFF-REPORT.md"
echo "# MITM Flow Comparison Report" > "$FLOW_REPORT"
echo "" >> "$FLOW_REPORT"

flow_count=0
for scenario_dir in "$BENCH_DIR/../compatibility/runner/protocol"/*/; do
    scenario=$(basename "$scenario_dir")
    off_latest="$scenario_dir/official/latest"
    aksh_latest="$scenario_dir/aksh/latest"

    if [ -d "$off_latest" ] && [ -d "$aksh_latest" ]; then
        # Check if both have flows.jsonl
        off_flows="$off_latest/flows.jsonl"
        aksh_flows="$aksh_latest/flows.jsonl"
        [ -f "$off_flows" ] || off_flows="$off_latest/vm-mitm/flows.jsonl"
        [ -f "$aksh_flows" ] || aksh_flows="$aksh_latest/vm-mitm/flows.jsonl"

        if [ -f "$off_flows" ] && [ -f "$aksh_flows" ]; then
            log "  Flow diff: $scenario"
            diff_out="$scenario_dir/diff.md"
            python3 "$BENCH_DIR/runner-flow-diff.py" \
                --scenario "$scenario" \
                --official-dir "$off_latest" \
                --aksh-dir "$aksh_latest" \
                --output "$diff_out" 2>/dev/null || true

            if [ -f "$diff_out" ]; then
                # Extract verdict
                verdict=$(grep "^FAIL\|^PASS" "$diff_out" | head -1 || echo "unknown")
                echo "## $scenario: $verdict" >> "$FLOW_REPORT"
                echo "" >> "$FLOW_REPORT"
                # Include endpoint counts table
                sed -n '/^## Endpoint counts$/,/^## /p' "$diff_out" | head -40 >> "$FLOW_REPORT"
                echo "" >> "$FLOW_REPORT"
                flow_count=$((flow_count + 1))
            fi
        fi
    fi
done

echo "---" >> "$FLOW_REPORT"
echo "**Total**: $flow_count scenarios with flow captures compared" >> "$FLOW_REPORT"

# ── Step 5: Unified summary ─────────────────────────────────────────
log "Generating unified summary..."
UNIFIED="$RESULTS_DIR/UNIFIED-COMPARISON.md"
{
    echo "# Unified Runner Comparison Report"
    echo ""
    echo "Generated: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo ""
    echo "## Reports"
    echo ""
    echo "1. **[Conformance Report](CONFORMANCE-REPORT.md)** — Step-level outcome comparison across all scenarios"
    echo "2. **[Log Content Report](LOG-CONTENT-REPORT.md)** — Log formatting, timestamps, groups, annotations"
    echo "3. **[Flow Diff Report](FLOW-DIFF-REPORT.md)** — HTTP-level protocol comparison from MITM captures"
    echo ""
    echo "## Quick Summary"
    echo ""

    # Extract key metrics from conformance report
    if [ -f "$RESULTS_DIR/CONFORMANCE-REPORT.md" ]; then
        grep "^\*\*Totals\*\*" "$RESULTS_DIR/CONFORMANCE-REPORT.md" || true
    fi
    echo ""

    # Extract key metrics from log report
    if [ -f "$RESULTS_DIR/LOG-CONTENT-REPORT.md" ]; then
        grep "^\*\*Total\*\*" "$RESULTS_DIR/LOG-CONTENT-REPORT.md" || true
    fi
    echo ""

    # Extract key metrics from flow report
    if [ -f "$FLOW_REPORT" ]; then
        grep "^\*\*Total\*\*" "$FLOW_REPORT" || true
    fi
    echo ""
} > "$UNIFIED"

log "════════════════════════════════════════════════════"
log "  Reports written to: $RESULTS_DIR/"
log "  - UNIFIED-COMPARISON.md"
log "  - CONFORMANCE-REPORT.md"
log "  - LOG-CONTENT-REPORT.md"
log "  - FLOW-DIFF-REPORT.md"
log "════════════════════════════════════════════════════"
