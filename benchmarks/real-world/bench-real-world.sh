#!/usr/bin/env bash
# bench-real-world.sh — Benchmark real-world repos with aksh/official runner
# Usage: ./bench-real-world.sh <serde|axum|bat> [aksh|official|both] [runs]
set -euo pipefail

REPO="${1:?Usage: $0 <serde|axum|bat> [aksh|official|both] [runs]}"
MODE="${2:-aksh}"
RUNS="${3:-1}"

PORT=9191
REPO_DIR="/tmp/bench-repos/$REPO"
BENCH_DIR="$(dirname "$(readlink -f "$0")")"
RESULTS="/tmp/bench-results"
AKSH="${AKSH:-/usr/local/bin}"
OFFICIAL="$HOME/actions-runner"

mkdir -p "$RESULTS"

WFNAME="${REPO}-bench.yml"
[ -f "$BENCH_DIR/$WFNAME" ] || { echo "Missing $BENCH_DIR/$WFNAME"; exit 1; }
if [ ! -d "$REPO_DIR" ] && [ -d "/workspace/repos/$REPO" ]; then
  mkdir -p "$(dirname "$REPO_DIR")"
  cp -r "/workspace/repos/$REPO" "$REPO_DIR"
fi

mkdir -p "$REPO_DIR/.github/workflows"
cp "$BENCH_DIR/$WFNAME" "$REPO_DIR/.github/workflows/$WFNAME"

cd "$REPO_DIR"
git rev-parse HEAD >/dev/null 2>&1 || { git init -b main; git add -A; git commit -m "bench"; }

ms() { date +%s%3N; }

SERVER_PID=""
cleanup() { [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; true; }
trap cleanup EXIT

start_fresh_server() {
  [ -n "$SERVER_PID" ] && { kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; true; }
  sleep 0.3
  export AKSH_PUBLIC_URL="http://127.0.0.1:$PORT"
  local t0=$(ms)
  RUST_LOG=info "$AKSH/preloop-server" serve --listen "127.0.0.1:$PORT" \
    --state-dir "/tmp/bench-state-$$" > /tmp/bench-server.log 2>&1 &
  SERVER_PID=$!
  local ok=0
  for i in $(seq 1 50); do
    curl -sf "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 && { ok=1; break; }
    kill -0 "$SERVER_PID" 2>/dev/null || { echo "Server died"; cat /tmp/bench-server.log; exit 1; }
    sleep 0.1
  done
  [ "$ok" = 1 ] || { echo "Server timeout"; exit 1; }
  local t1=$(ms)
  echo "  server_start: $((t1-t0))ms"
}

configure_aksh() {
  local root="$1" name="$2"
  rm -rf "$root"; mkdir -p "$root"
  local t0=$(ms)
  "$AKSH/aksh-runner" --runner-root "$root" configure \
    --url "http://127.0.0.1:$PORT" --token t --name "$name" \
    --unattended --replace --ephemeral \
    --labels "self-hosted,Linux,X64" 2>&1 | tail -3
  local t1=$(ms)
  echo "  configure: $((t1-t0))ms"
}

submit_workflow() {
  local t0=$(ms)
  local out
  out=$("$AKSH/aksh-runner-client" --server "http://127.0.0.1:$PORT" \
    submit -W ".github/workflows/$WFNAME" \
    --workspace-root "$REPO_DIR" \
    --git-ref refs/heads/main 2>&1)
  local t1=$(ms)
  echo "  submit: $((t1-t0))ms → $out"
}

run_aksh_runner() {
  local root="$1" log="$2"
  local t0=$(ms)
  RUST_LOG=info "$AKSH/aksh-runner" --runner-root "$root" run --once > "$log" 2>&1 || true
  local t1=$(ms)
  echo $((t1-t0))
}

run_official_runner() {
  local log="$1"
  local t0=$(ms)
  cd "$OFFICIAL"
  timeout 900 ./run.sh --once > "$log" 2>&1 || true
  cd "$REPO_DIR"
  local t1=$(ms)
  echo $((t1-t0))
}

extract_step_timings() {
  local log="$1" runner="$2"
  if [ "$runner" = "aksh" ]; then
    # Parse aksh runner logs: timestamps + step names
    grep -E "Running step:|Job .* completed:" "$log" 2>/dev/null | while IFS= read -r line; do
      local ts=$(echo "$line" | grep -oP '^\S+' | head -1)
      local step=$(echo "$line" | sed 's/.*Running step: //' | sed 's/.*Job /Job /' )
      echo "    $ts  $step"
    done
  else
    grep -iE "Running step|Completing step" "$log" 2>/dev/null | head -30
  fi
}

do_aksh_run() {
  local n=$1
  echo ""
  echo "--- aksh run $n/$RUNS ---"
  start_fresh_server

  local root="/tmp/bench-aksh-$n"
  configure_aksh "$root" "bench-aksh-$n"
  submit_workflow

  local log="/tmp/bench-aksh-$REPO-$n.log"
  echo "  running..."
  local run_ms=$(run_aksh_runner "$root" "$log")
  echo "  runner: ${run_ms}ms"

  # Check result
  local result=$(grep "Job .* completed:" "$log" 2>/dev/null | tail -1 | grep -oP '(Succeeded|Failed)' || echo "Unknown")
  echo "  result: $result"

  # Step timings
  echo "  steps:"
  extract_step_timings "$log" "aksh"

  echo "{\"runner\":\"aksh\",\"repo\":\"$REPO\",\"n\":$n,\"runner_ms\":$run_ms,\"result\":\"$result\"}" \
    >> "$RESULTS/${REPO}-bench.jsonl"
}

do_official_run() {
  local n=$1
  echo ""
  echo "--- official run $n/$RUNS ---"
  start_fresh_server

  # Configure official
  local work="/tmp/bench-official-work-$n"
  rm -rf "$work"; mkdir -p "$work"
  local t0=$(ms)
  cd "$OFFICIAL"
  ./config.sh remove --token t 2>/dev/null || true
  ./config.sh --url "http://127.0.0.1:$PORT" --token t \
    --name "bench-off-$n" --work "$work" \
    --unattended --replace --ephemeral \
    --labels "self-hosted,Linux,X64" 2>&1 | tail -3
  local t1=$(ms)
  cd "$REPO_DIR"
  echo "  configure: $((t1-t0))ms"

  submit_workflow

  local log="/tmp/bench-official-$REPO-$n.log"
  echo "  running..."
  local run_ms=$(run_official_runner "$log")
  echo "  runner: ${run_ms}ms"
  echo "  steps:"
  extract_step_timings "$log" "official"

  echo "{\"runner\":\"official\",\"repo\":\"$REPO\",\"n\":$n,\"runner_ms\":$run_ms}" \
    >> "$RESULTS/${REPO}-bench.jsonl"
}

echo "================================================================"
echo "  Real-World Benchmark: $REPO"
echo "  Mode: $MODE | Runs: $RUNS | $(date)"
echo "================================================================"

> "$RESULTS/${REPO}-bench.jsonl"

for i in $(seq 1 "$RUNS"); do
  case "$MODE" in
    aksh)     do_aksh_run "$i" ;;
    official) do_official_run "$i" ;;
    both)     do_aksh_run "$i"; do_official_run "$i" ;;
  esac
done

echo ""
echo "================================================================"
echo "  RESULTS: $REPO ($MODE)"
echo "================================================================"
cat "$RESULTS/${REPO}-bench.jsonl"
echo ""
echo "Logs: /tmp/bench-{aksh,official}-${REPO}-*.log"
