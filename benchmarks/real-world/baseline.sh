#!/usr/bin/env bash
# Direct cargo execution baseline — no runner overhead
# Usage: ./baseline.sh <serde|axum|bat>
set -euo pipefail

REPO="${1:?Usage: $0 <serde|axum|bat>}"
REPO_DIR="/tmp/bench-repos/$REPO"

# Serde needs 1.86.0, axum/bat need stable
case "$REPO" in
  serde) TC="1.86.0-x86_64-unknown-linux-gnu" ;;
  *)     TC="stable-x86_64-unknown-linux-gnu" ;;
esac

export PATH="/home/bnjoroge/.rustup/toolchains/$TC/bin:/home/bnjoroge/.cargo/bin:$PATH"
export CARGO_HOME="/home/bnjoroge/.cargo"
export RUSTUP_HOME="/home/bnjoroge/.rustup"

cd "$REPO_DIR"

ms() { date +%s%3N; }

time_step() {
  local name="$1"; shift
  local t0=$(ms)
  "$@" 2>&1 | tail -3
  local t1=$(ms)
  local dur=$((t1 - t0))
  printf "  %-35s %8dms\n" "$name" "$dur"
}

echo "================================================================"
echo "  Direct Baseline: $REPO (no runner)"
echo "  Toolchain: $TC"
echo "  $(date)"
echo "================================================================"
echo ""

T0=$(ms)

case "$REPO" in
  serde)
    time_step "Rust version" rustc --version
    time_step "Rustfmt" cargo fmt --all --check
    time_step "Build serde" bash -c "cd serde && cargo build --features rc"
    time_step "Build serde (no default)" bash -c "cd serde && cargo build --no-default-features"
    time_step "Clippy serde" bash -c "cd serde && cargo clippy --features rc"
    time_step "Clippy serde_derive" bash -c "cd serde_derive && cargo clippy"
    time_step "Test serde_core" bash -c "cd serde_core && cargo test --features rc"
    time_step "Test serde_derive" bash -c "cd serde_derive && cargo test"
    ;;
  axum)
    time_step "Rust version" rustc --version
    time_step "Rustfmt" cargo fmt --all --check
    time_step "Clippy" cargo clippy --workspace --all-targets --all-features -- -D warnings
    time_step "Test" cargo test --workspace --all-features
    time_step "Doc" bash -c "RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --all-features --no-deps"
    ;;
  bat)
    time_step "Rust version" rustc --version
    time_step "Rustfmt" cargo fmt --all --check
    time_step "Build" cargo build --locked
    time_step "Clippy" cargo clippy --locked --all-targets --all-features -- -D warnings
    time_step "Test" cargo test --locked
    ;;
esac

T1=$(ms)
echo ""
echo "TOTAL: $((T1 - T0))ms"
