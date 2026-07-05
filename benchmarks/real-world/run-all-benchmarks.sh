#!/usr/bin/env bash
# run-all-benchmarks.sh — runs baseline + aksh for a given repo
set -euo pipefail

REPO="${1:?Usage: $0 <serde|axum|bat>}"
BENCH_DIR="$HOME/cachingv4/benchmarks/real-world"

case "$REPO" in
  serde) TC="1.86.0-x86_64-unknown-linux-gnu" ;;
  *)     TC="stable-x86_64-unknown-linux-gnu" ;;
esac

export PATH="$HOME/.rustup/toolchains/$TC/bin:$HOME/.cargo/bin:$PATH"
export CARGO_HOME="$HOME/.cargo"
export RUSTUP_HOME="$HOME/.rustup"

# Copy workflow
mkdir -p /tmp/bench-repos/$REPO/.github/workflows
cp "$BENCH_DIR/${REPO}-bench.yml" "/tmp/bench-repos/$REPO/.github/workflows/${REPO}-bench.yml"
cd /tmp/bench-repos/$REPO
git rev-parse HEAD >/dev/null 2>&1 || { git init -b main; git add -A; git commit -m bench; }

echo "================================================================"
echo "  BENCHMARK: $REPO ($(date))"
echo "  Toolchain: $(rustc --version)"
echo "================================================================"

# 1. Baseline
echo ""
echo "--- BASELINE ---"
"$BENCH_DIR/baseline.sh" "$REPO"

# 2. aksh-runner
echo ""
echo "--- AKSH-RUNNER ---"
sudo kill $(pgrep -f aksh-runner-server) 2>/dev/null || true; sleep 0.3
sudo AKSH_PUBLIC_URL=http://127.0.0.1 RUST_LOG=info \
  $HOME/aksh-runner/aksh-runner-server serve --listen 127.0.0.1:80 --state-dir /tmp/bs-$REPO \
  > /tmp/bs-$REPO.log 2>&1 &
SPID=$!; sleep 1
curl -sf http://127.0.0.1/healthz >/dev/null || { echo "Server failed"; cat /tmp/bs-$REPO.log; exit 1; }

rm -rf /tmp/br-$REPO; mkdir -p /tmp/br-$REPO
$HOME/aksh-runner/aksh-runner --runner-root /tmp/br-$REPO configure \
  --url http://127.0.0.1 --token t --name $REPO-bench \
  --unattended --replace --ephemeral --labels "self-hosted,Linux,X64" --no-externals 2>&1 | tail -1

$HOME/aksh-runner/aksh-runner-client --server http://127.0.0.1 \
  submit -W ".github/workflows/${REPO}-bench.yml" --workspace-root . --git-ref refs/heads/main 2>&1

T_START=$(date +%s%3N)
RUST_LOG=info $HOME/aksh-runner/aksh-runner --runner-root /tmp/br-$REPO run --once > /tmp/${REPO}-aksh.log 2>&1
T_END=$(date +%s%3N)
echo "  aksh runner wall time: $((T_END - T_START))ms (includes ~50s broker timeout)"

echo ""
echo "  Step timings:"
grep -E "Running step|completed:" /tmp/${REPO}-aksh.log

# 3. Official runner
echo ""
echo "--- OFFICIAL RUNNER ---"
sudo kill $SPID 2>/dev/null; sleep 0.3
sudo AKSH_PUBLIC_URL=http://127.0.0.1 RUST_LOG=info \
  $HOME/aksh-runner/aksh-runner-server serve --listen 127.0.0.1:80 --state-dir /tmp/bs-${REPO}-off \
  > /tmp/bs-${REPO}-off.log 2>&1 &
SPID=$!; sleep 1
curl -sf http://127.0.0.1/healthz >/dev/null || { echo "Server failed"; exit 1; }

cd $HOME/actions-runner
./config.sh remove --token t 2>/dev/null || true
./config.sh --url http://127.0.0.1 --token t --name ${REPO}-off \
  --work /tmp/off-${REPO} --unattended --replace --ephemeral \
  --labels "self-hosted,Linux,X64" 2>&1 | tail -2

cd /tmp/bench-repos/$REPO
$HOME/aksh-runner/aksh-runner-client --server http://127.0.0.1 \
  submit -W ".github/workflows/${REPO}-bench.yml" --workspace-root . --git-ref refs/heads/main 2>&1

cd $HOME/actions-runner
T_START=$(date +%s%3N)
timeout 600 ./run.sh --once > /tmp/${REPO}-official.log 2>&1
T_END=$(date +%s%3N)
echo "  Official runner wall time: $((T_END - T_START))ms"
cat /tmp/${REPO}-official.log

sudo kill $SPID 2>/dev/null || true
echo ""
echo "=== $REPO COMPLETE ==="
