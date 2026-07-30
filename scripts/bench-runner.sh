#!/usr/bin/env bash
# bench-runner.sh — Performance and size benchmarking for aksh-runner
#
# Measures binary size, cold start time, and memory footprint.
# Emits METRIC lines to stdout (matching autoresearch.sh convention).
# Use --json to write results to bench-results.json.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUST_RUNNER="$REPO_ROOT/target/release/preloop-runner"
OFFICIAL_RUNNER_DIR="${OFFICIAL_RUNNER_DIR:-$HOME/mitm-proxy/experiments/mitm/.cache/runner-official}"
JSON_OUTPUT=""

for arg in "$@"; do
    case "$arg" in
        --json) JSON_OUTPUT="$REPO_ROOT/bench-results.json" ;;
        --json=*) JSON_OUTPUT="${arg#--json=}" ;;
    esac
done

echo "=== aksh-runner benchmark ==="
echo ""

# Build if needed
if [ ! -f "$RUST_RUNNER" ]; then
    echo "Building aksh-runner..."
    cargo build --release -p aksh-runner --manifest-path "$REPO_ROOT/Cargo.toml"
fi

# ── Binary size ──────────────────────────────────────────────────────────

rust_size_bytes=$(stat -f%z "$RUST_RUNNER" 2>/dev/null || stat -c%s "$RUST_RUNNER" 2>/dev/null || echo 0)
rust_size_human=$(du -h "$RUST_RUNNER" | cut -f1)

echo "METRIC rust_binary_size_bytes=$rust_size_bytes"
echo "METRIC rust_binary_size_human=$rust_size_human"

# Official runner size (if available)
official_size_human="N/A"
official_size_bytes="0"
if [ -d "$OFFICIAL_RUNNER_DIR" ]; then
    official_size_human=$(du -sh "$OFFICIAL_RUNNER_DIR" | cut -f1)
    official_size_bytes=$(du -s "$OFFICIAL_RUNNER_DIR" | cut -f1)
    echo "METRIC official_runner_dir_size_human=$official_size_human"
    echo "METRIC official_runner_dir_size_bytes=$official_size_bytes"
else
    echo "METRIC official_runner_dir_size_human=N/A (not found at $OFFICIAL_RUNNER_DIR)"
fi

# ── Cold start (--version) ───────────────────────────────────────────────

start_ns=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
"$RUST_RUNNER" --version > /dev/null 2>&1
end_ns=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')

cold_start_ms=$(( (end_ns - start_ns) / 1000000 ))
echo "METRIC rust_cold_start_ms=$cold_start_ms"

# ── Version ──────────────────────────────────────────────────────────────

version=$("$RUST_RUNNER" --version)
echo "METRIC rust_runner_version=$version"

# ── Memory (idle RSS) ────────────────────────────────────────────────────

# Start runner briefly to measure RSS (it will fail without config, but we can time it)
echo "METRIC rust_idle_rss_kb=N/A (requires configured runner)"

# ── Summary ──────────────────────────────────────────────────────────────

echo ""
echo "=== Summary ==="
echo "Rust binary:     $rust_size_human ($rust_size_bytes bytes)"
echo "Official runner: $official_size_human"
echo "Cold start:      ${cold_start_ms}ms"
echo "Version:         $version"

# ── JSON output ──────────────────────────────────────────────────────────

if [ -n "$JSON_OUTPUT" ]; then
    cat > "$JSON_OUTPUT" << ENDJSON
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "rust_runner": {
    "binary_size_bytes": $rust_size_bytes,
    "binary_size_human": "$rust_size_human",
    "cold_start_ms": $cold_start_ms,
    "version": "$version"
  },
  "official_runner": {
    "dir_size_human": "$official_size_human",
    "dir_size_bytes": $official_size_bytes
  }
}
ENDJSON
    echo ""
    echo "Results written to $JSON_OUTPUT"
fi
