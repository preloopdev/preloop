#!/usr/bin/env bash
# bench-runner-compare.sh — preloop-runner vs official actions/runner, same server.
#
# Phase A (no server): install footprint, binary size, cold start.
# Phase B: both runners register against a local preloop-server, then we
# measure time-to-ready, idle RSS, job pickup latency, and peak RSS during a
# real job. Runners are exercised one at a time so they never contend.
#
# Usage: scripts/bench-runner-compare.sh [--skip-live]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUST_RUNNER="$REPO_ROOT/target/release/preloop-runner"
SERVER_BIN="$REPO_ROOT/target/debug/preloop-server"
CLIENT_BIN="$REPO_ROOT/target/debug/preloop-runner-client"
OFFICIAL_DIR="${OFFICIAL_RUNNER_DIR:-$HOME/.cache/actions-runner/current}"

# USE_DEV_ACTIONS_SERVICE_URL makes the official runner treat --url as the
# server URL verbatim (legacy VSS path) — without it the runner rebuilds the
# API base from the host and silently drops the port.
PORT="${BENCH_PORT:-19099}"
BASE="http://127.0.0.1:$PORT"
TOKEN="${PRELOOP_SYSTEM_TOKEN:-$(python3 -c 'import secrets;print(secrets.token_hex(32))')}"
export PRELOOP_SYSTEM_TOKEN="$TOKEN"
# Isolate from ~/.preloop/config.toml (real GitHub App creds → api.github.com).
export PRELOOP_CONFIG=/tmp/preloop-bench-empty-config.toml
export PRELOOP_PUBLIC_URL="$BASE"

BENCH_DIR="$(mktemp -d /tmp/runner-bench.XXXXXX)"
STATE_DIR="$BENCH_DIR/state"
OFFICIAL_ROOT="$BENCH_DIR/official"
RUST_ROOT="$BENCH_DIR/rust-runner"
SERVER_LOG="$BENCH_DIR/server.log"

SKIP_LIVE=0
[ "${1:-}" = "--skip-live" ] && SKIP_LIVE=1

metric() { echo "METRIC $1=$2"; }

cleanup() {
  kill_matching 'Runner.Listener|Runner.Worker'
  kill_matching 'preloop-runner run'
  [ -n "${SERVER_PID:-}" ] && kill "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

# Sum RSS (KB) of every process whose command line matches ERE $1.
rss_of() {
  ps -eo rss=,command= | awk -v pat="$1" '$0 ~ pat && $0 !~ /awk/ {s+=$1} END {print s+0}'
}

# Kill every process whose command line matches ERE $1.
kill_matching() { pkill -f "$1" 2>/dev/null || true; }

# ── Phase A: static ──────────────────────────────────────────────────────────

echo "=== Phase A: footprint & cold start ==="

# As shipped: the official tarball bundles ~350MB of node20+node24 externals;
# preloop-runner is a single binary that fetches the same runtimes on demand
# at configure time (skipped here with --no-externals).
official_bytes=$(du -sk "$OFFICIAL_DIR" | cut -f1)
official_files=$(find "$OFFICIAL_DIR" -type f | wc -l | tr -d ' ')
metric official_install_kb "$official_bytes"
metric official_file_count "$official_files"

rust_bytes=$(stat -f%z "$RUST_RUNNER")
metric rust_binary_bytes "$rust_bytes"

# Cold start: --version, best of 5.
cold() { # $1 = label, rest = cmd
  local label="$1"; shift
  local best=999999
  for _ in 1 2 3 4 5; do
    local t0 t1
    t0=$(now_ms); "$@" >/dev/null 2>&1 || true; t1=$(now_ms)
    local d=$((t1 - t0)); [ "$d" -lt "$best" ] && best=$d
  done
  metric "${label}_cold_start_ms" "$best"
}
cold official "$OFFICIAL_DIR/bin/Runner.Listener" --version
cold rust "$RUST_RUNNER" --version

if [ "$SKIP_LIVE" = 1 ]; then exit 0; fi

# ── Phase B: live against local server ───────────────────────────────────────

echo "=== Phase B: live runner comparison (server on :$PORT) ==="

"$SERVER_BIN" serve --listen "127.0.0.1:$PORT" --state-dir "$STATE_DIR" \
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!



for _ in $(seq 1 100); do
  curl -sf "$BASE/healthz" >/dev/null 2>&1 && break
  curl -sf "$BASE/api/v1/status" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -sf "$BASE/healthz" >/dev/null 2>&1 || curl -sf "$BASE/api/v1/status" >/dev/null 2>&1 \
  || { echo "server did not come up"; tail -20 "$SERVER_LOG"; exit 1; }

# Mint a real registration token (GitHub-compatible endpoint; needs a body).
REG_TOKEN=$(curl -sf -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d '{}' \
  "$BASE/api/v3/repos/local/preloop/actions/runners/registration-token" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])' 2>/dev/null || true)
[ -z "$REG_TOKEN" ] && REG_TOKEN="$TOKEN"   # fall back to system token

# Minimal workflow: enough to spawn a worker and stream logs.
cat > "$BENCH_DIR/bench.yml" <<'YAML'
name: bench
on: [push]
jobs:
  bench:
    runs-on: self-hosted
    steps:
      - run: echo "bench step one"; sleep 1
      - run: echo "bench step two"; dd if=/dev/zero bs=1m count=20 2>/dev/null | shasum
YAML

wait_ready() { # $1 = ready-marker ERE, $2 = log file, $3 = timeout s → ms-to-ready
  local t0; t0=$(now_ms)
  for _ in $(seq 1 $(( $3 * 5 ))); do
    if grep -qiE "$1" "$2" 2>/dev/null; then
      echo $(( $(now_ms) - t0 )); return 0
    fi
    sleep 0.2
  done
  echo -1
}

# Submit one job, then sample RSS at 10Hz until the runner's log goes quiet.
# Prints "pickup_ms peak_kb" — pickup is submit → job announcement.
run_one_job() { # $1 = rss pattern, $2 = runner log file
  local t0 pickup=-1 peak=0 done_at=0
  t0=$(now_ms)
  "$CLIENT_BIN" --server "$BASE" submit -W "$BENCH_DIR/bench.yml" \
    --repository local/preloop >/dev/null 2>&1
  for _ in $(seq 1 400); do
    if [ "$pickup" -lt 0 ] && grep -qiE "running job|received job|job request|received broker message" "$2" 2>/dev/null; then
      pickup=$(( $(now_ms) - t0 ))
    fi
    if grep -qiE "job.*completed|completed job|worker.*completed|finished" "$2" 2>/dev/null; then
      done_at=$(now_ms); break
    fi
    local r; r=$(rss_of "$1")
    [ "$r" -gt "$peak" ] && peak=$r
    sleep 0.1
  done
  # keep sampling briefly past completion for teardown spikes
  local until=$(( ${done_at:-$(now_ms)} + 2000 ))
  while [ "$(now_ms)" -lt "$until" ]; do
    local r; r=$(rss_of "$1")
    [ "$r" -gt "$peak" ] && peak=$r
    sleep 0.1
  done
  echo "$pickup $peak"
}

median() { printf '%s\n' "$@" | sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}'; }

RUNS="${BENCH_RUNS:-3}"
bench_runner() { # $1 = label, $2 = rss pattern, $3 = configure cmd, $4 = run cmd, $5 = log prefix, $6 = ready-marker ERE
  local label="$1" pat="$2" cfg="$3" run="$4" logp="$5" ready_pat="$6"

  local configures=() readys=() idles=() pickups=() peaks=()
  for i in $(seq 1 "$RUNS"); do
    local t0 ready idle res pickup peak
    t0=$(now_ms)
    eval "$cfg" >"$BENCH_DIR/${logp}-config-$i.log" 2>&1 || true
    configures+=($(( $(now_ms) - t0 )))
    eval "$run" >"$BENCH_DIR/${logp}-run-$i.log" 2>&1 &
    ready=$(wait_ready "$ready_pat" "$BENCH_DIR/${logp}-run-$i.log" 60)

    readys+=("$ready")
    sleep 5
    idle=$(rss_of "$pat"); idles+=("$idle")

    res=$(run_one_job "$pat" "$BENCH_DIR/${logp}-run-$i.log")
    pickup=${res%% *}; peak=${res##* }
    pickups+=("$pickup"); peaks+=("$peak")

    kill_matching "$pat"; sleep 1
  done
  metric "${label}_configure_ms" "$(median "${configures[@]}")"
  metric "${label}_ready_ms" "$(median "${readys[@]}")"
  metric "${label}_idle_rss_kb" "$(median "${idles[@]}")"
  metric "${label}_pickup_ms" "$(median "${pickups[@]}")"
  metric "${label}_peak_rss_kb" "$(median "${peaks[@]}")"
  echo "  ${label} raw: configure=[${configures[*]}] ready=[${readys[*]}] idle=[${idles[*]}] pickup=[${pickups[*]}] peak=[${peaks[*]}]" >&2
}

# ── official runner ──
# Copy without externals — matches the footprint metric; bash steps don't need node.
rsync -a --exclude externals "$OFFICIAL_DIR/" "$OFFICIAL_ROOT/" 2>/dev/null \
  || { mkdir -p "$OFFICIAL_ROOT"; tar cf - --exclude=externals -C "$OFFICIAL_DIR" . | tar xf - -C "$OFFICIAL_ROOT"; }
cd "$OFFICIAL_ROOT"
bench_runner official \
  'Runner.Listener|Runner.Worker' \
  "USE_DEV_ACTIONS_SERVICE_URL=1 ./config.sh --unattended --url '$BASE' --token '$REG_TOKEN' --name bench-official --labels self-hosted --work _work --replace" \
  "./run.sh" \
  official \
  'Listening for Jobs'

# ── preloop-runner ──
mkdir -p "$RUST_ROOT"
cd "$REPO_ROOT"
bench_runner rust \
  'preloop-runner run' \
  "'$RUST_RUNNER' configure --url '$BASE' --token '$TOKEN' --name bench-rust --labels self-hosted --runner-root '$RUST_ROOT' --unattended --replace --no-externals" \
  "RUST_LOG=info '$RUST_RUNNER' run --runner-root '$RUST_ROOT'" \
  rust \
  'Broker session created'
echo ""
echo "=== logs in $BENCH_DIR ==="
