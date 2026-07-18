#!/usr/bin/env bash
# Overnight conformance + property test runner.
# Usage: bash scripts/overnight-conformance.sh
#
# Runs:
#   1. Release build of aksh-runner-server and runner-watch
#   2. All 12 conformance scenarios via runner-watch conform
#   3. Property tests with PROPTEST_CASES=10000
#   4. Full unit test suite (excluding aksh-dap which has a pre-existing compile issue)
#
# Results are collected in /tmp/overnight-conformance-results/

set -euo pipefail

RESULTS_DIR="/tmp/overnight-conformance-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG="$RESULTS_DIR/run-$TIMESTAMP.log"
SERVER_PID=""

mkdir -p "$RESULTS_DIR"

log() {
    echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"
}

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        log "Stopping server (pid=$SERVER_PID)"
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ── Phase 1: Build ──────────────────────────────────────────────────────────
log "Phase 1: Building release binaries"
cargo build --release -p aksh-runner-server -p runner-watch 2>&1 | tee -a "$LOG"
log "Build complete"

# ── Phase 2: Conformance replay ─────────────────────────────────────────────
log "Phase 2: Conformance replay (all scenarios)"

SCENARIOS=(
    "01-register-and-idle"
    "06-multi-step"
    "07-step-failure"
    "08-job-outputs-needs"
    "09-matrix-fan-out"
    "10-uses-checkout"
    "11-cache-roundtrip"
    "12-artifact"
    "13-composite-action"
    "14-annotations"
    "15-oidc-id-token"
)

CONFORM_PASS=0
CONFORM_FAIL=0
CONFORM_RESULTS="$RESULTS_DIR/conformance-$TIMESTAMP.txt"
echo "# Conformance Results — $TIMESTAMP" > "$CONFORM_RESULTS"
echo "" >> "$CONFORM_RESULTS"

for scenario in "${SCENARIOS[@]}"; do
    log "  Running scenario: $scenario"

    # Start fresh server for each scenario to avoid state leakage
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi

    cargo run --release -p aksh-runner-server -- serve --listen 127.0.0.1:9090 \
        &>"$RESULTS_DIR/server-$scenario.log" &
    SERVER_PID=$!

    # Wait for server to be ready
    for i in $(seq 1 30); do
        if curl -s http://127.0.0.1:9090/ready >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done

    if ! curl -s http://127.0.0.1:9090/ready >/dev/null 2>&1; then
        log "  FAIL: Server did not start for $scenario"
        echo "| $scenario | FAIL (server start) |" >> "$CONFORM_RESULTS"
        ((CONFORM_FAIL++)) || true
        continue
    fi

    # Run conformance
    SCENARIO_LOG="$RESULTS_DIR/conform-$scenario.log"
    if cargo run --release -p runner-watch -- conform \
        --runner v2.335.1 \
        --aksh-url http://127.0.0.1:9090 \
        --scenario "$scenario" \
        --skip-cargo-test \
        >"$SCENARIO_LOG" 2>&1; then
        log "  PASS: $scenario"
        echo "| $scenario | PASS |" >> "$CONFORM_RESULTS"
        ((CONFORM_PASS++)) || true
    else
        log "  FAIL: $scenario (exit=$?)"
        echo "| $scenario | FAIL |" >> "$CONFORM_RESULTS"
        ((CONFORM_FAIL++)) || true
    fi

    # Copy the conformance report
    if [[ -f ".runner-watch/conformance/v2.335.1/$scenario.md" ]]; then
        cp ".runner-watch/conformance/v2.335.1/$scenario.md" \
            "$RESULTS_DIR/report-$scenario.md"
    fi
done

# Kill server after conformance
if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=""

log "Conformance: $CONFORM_PASS passed, $CONFORM_FAIL failed out of ${#SCENARIOS[@]} scenarios"
echo "" >> "$CONFORM_RESULTS"
echo "**Total: $CONFORM_PASS passed, $CONFORM_FAIL failed**" >> "$CONFORM_RESULTS"

# ── Phase 3: Property tests (high case count) ──────────────────────────────
log "Phase 3: Property tests (PROPTEST_CASES=10000)"

PROP_RESULTS="$RESULTS_DIR/property-tests-$TIMESTAMP.txt"
echo "# Property Test Results — $TIMESTAMP" > "$PROP_RESULTS"

# Concurrency pure properties
log "  Concurrency pure properties"
if PROPTEST_CASES=10000 cargo test --release -p aksh-runner-server \
    'concurrency::properties' -- --test-threads=1 \
    >>"$PROP_RESULTS" 2>&1; then
    log "  PASS: concurrency pure properties"
else
    log "  FAIL: concurrency pure properties"
fi

# Concurrency state machine
log "  Concurrency state machine"
if PROPTEST_CASES=10000 cargo test --release -p aksh-runner-server \
    'concurrency_properties' -- --test-threads=1 \
    >>"$PROP_RESULTS" 2>&1; then
    log "  PASS: concurrency state machine"
else
    log "  FAIL: concurrency state machine"
fi

# HTTP sequence properties (lower case count — these are slow)
log "  HTTP sequence properties"
if PROPTEST_CASES=256 cargo test --release -p aksh-runner-server \
    'concurrency_http_properties' -- --test-threads=1 \
    >>"$PROP_RESULTS" 2>&1; then
    log "  PASS: HTTP sequence properties"
else
    log "  FAIL: HTTP sequence properties"
fi

# Expression properties
log "  Expression properties"
if PROPTEST_CASES=10000 cargo test --release -p aksh-gha-expressions \
    -- --test-threads=1 \
    >>"$PROP_RESULTS" 2>&1; then
    log "  PASS: expression properties"
else
    log "  FAIL: expression properties"
fi

# Parser concurrency properties
log "  Parser concurrency properties"
if cargo test --release -p aksh-gha-parser 'concurrency_' \
    -- --test-threads=1 \
    >>"$PROP_RESULTS" 2>&1; then
    log "  PASS: parser concurrency properties"
else
    log "  FAIL: parser concurrency properties"
fi

# Runner timespan properties
log "  Runner timespan properties"
if PROPTEST_CASES=10000 cargo test --release -p aksh-runner 'timespan_tests' \
    -- --test-threads=1 \
    >>"$PROP_RESULTS" 2>&1; then
    log "  PASS: runner timespan properties"
else
    log "  FAIL: runner timespan properties"
fi

# ── Phase 4: Full unit test suite ──────────────────────────────────────────
log "Phase 4: Full unit test suite"

UNIT_RESULTS="$RESULTS_DIR/unit-tests-$TIMESTAMP.txt"
echo "# Unit Test Results — $TIMESTAMP" > "$UNIT_RESULTS"

for crate in aksh-runner-server aksh-gha-parser aksh-gha-expressions aksh-gha-protocol aksh-runner runner-watch; do
    log "  Testing $crate"
    if cargo test --release -p "$crate" --lib \
        >>"$UNIT_RESULTS" 2>&1; then
        log "  PASS: $crate"
    else
        log "  FAIL: $crate"
    fi
done

# ── Summary ────────────────────────────────────────────────────────────────
log "========================================="
log "Overnight conformance run complete"
log "Results: $RESULTS_DIR"
log "Conformance: $CONFORM_PASS/${#SCENARIOS[@]} scenarios passed"
log "========================================="

# Generate summary
SUMMARY="$RESULTS_DIR/SUMMARY-$TIMESTAMP.md"
cat > "$SUMMARY" <<EOF
# Overnight Conformance Summary — $TIMESTAMP

## Conformance Replay: $CONFORM_PASS/${#SCENARIOS[@]} scenarios passed

$(cat "$CONFORM_RESULTS")

## Property Tests

See \`property-tests-$TIMESTAMP.txt\` for detailed output.

## Unit Tests

See \`unit-tests-$TIMESTAMP.txt\` for detailed output.

## Per-Scenario Reports

$(ls -la "$RESULTS_DIR"/report-*.md 2>/dev/null || echo "No reports generated")
EOF

log "Summary written to $SUMMARY"
