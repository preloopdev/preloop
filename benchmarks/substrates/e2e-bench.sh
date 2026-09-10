#!/usr/bin/env bash
# e2e-bench.sh — end-to-end CI comparison: preloop on AgentENV vs preloop on SmolVM.
#
# Fairness rules enforced here:
#   * one substrate at a time, never concurrently, on an otherwise idle host;
#   * identical guest resources (CPU/RAM) and identical runner bundle;
#   * identical control-plane transport (TCP to the host LAN address) so the
#     comparison is the substrate, not the socket relay;
#   * each substrate runs in fork mode, which is its own fast path;
#   * identical workflow set and repetition count, same order;
#   * a fresh PRELOOP_HOME per substrate, and all VMs/sandboxes purged before
#     each run, so no run inherits the other's warm state;
#   * the OCI base image is pre-pulled into both substrates before timing.
#
# Usage: e2e-bench.sh <substrate: aenv|smolvm> <reps> <outdir>
set -uo pipefail

SUBSTRATE="${1:?substrate}"
REPS="${2:-3}"
OUTDIR="${3:-/tmp/e2e-$SUBSTRATE}"
REPO=~/preloop-bench
BIN="$REPO/target/release/preloop"
# A bundle holding only the guest runner. `target/release` would work too, but
# it is 1.5 GiB of build artifacts: SmolVM would mount it for free while
# AgentENV has to stream every byte into each golden, which would measure the
# bundle instead of the substrate.
BUNDLE="$HOME/bench-bundle"
WORK=~/bench-workspace
PORT=9490
HOSTIP=$(ip -4 -o addr show scope global | awk '{print $4}' | cut -d/ -f1 | head -1)
export PRELOOP_SYSTEM_TOKEN=bench-token
export PRELOOP_RUNNER_NAME_PREFIX="bench-$SUBSTRATE"
# The CLI talks to the native API on PRELOOP_URL; without it the client would
# reach whatever engine owns the default 127.0.0.1:9090 and be rejected with a
# 401 for the wrong token.
export PRELOOP_URL="http://127.0.0.1:$PORT"
export PRELOOP_HOME="$HOME/.preloop-bench-$SUBSTRATE"
export PRELOOP_RUNNER_BUNDLE="$BUNDLE"
export PRELOOP_RUNNER_URL="http://$HOSTIP:$PORT"
export PRELOOP_RUNNER_CPUS=4
export PRELOOP_RUNNER_MEMORY_MB=4096
export PRELOOP_RUNNER_STORAGE_GB=20
export PRELOOP_RUNNER_POOL_SIZE=1
# `/tmp` is tmpfs here, a different device from $PRELOOP_HOME. The engine
# builds a workspace snapshot in TMPDIR and publishes it into the state dir by
# rename, which fails with EXDEV across devices — the run then falls back to a
# real `actions/checkout` clone against a repo that does not exist on GitHub,
# and the job fails for reasons that have nothing to do with the substrate.
export TMPDIR="$PRELOOP_HOME/tmp"
export PRELOOP_USE_FORK=1
export PRELOOP_LOG_FORMAT=json
export RUST_LOG=info
export PRELOOP_AENV_TTL_SECS=7200

mkdir -p "$OUTDIR"
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$OUTDIR/driver.log" >&2; }

case "$SUBSTRATE" in
  aenv)   export PRELOOP_VM_BACKEND=agentenv ;;
  smolvm) export PRELOOP_VM_BACKEND=smolvm; export PRELOOP_USE_PACKED_GOLDEN=1 ;;
  *) echo "unknown substrate $SUBSTRATE" >&2; exit 2 ;;
esac

# Purge only this benchmark's own state: a blanket sweep would delete the
# machines and sandboxes of any other engine on the host. SmolVM machines are
# name-filtered; AgentENV assigns sandbox ids server-side and accepts no
# caller-supplied name, so this benchmark's own sandboxes are read from its
# registry — $PRELOOP_HOME is benchmark-scoped — before the directory is
# wiped. A stale id (sandbox already gone) is ignored.
purge() {
  log "purging substrate state for bench-$SUBSTRATE"
  local registry="$PRELOOP_HOME/agentenv-machines.json"
  if [ -f "$registry" ]; then
    for id in $(jq -r '.machines[]?.sandbox // empty' "$registry" 2>/dev/null); do
      [ -n "$id" ] && aenv delete "$id" >/dev/null 2>&1 || true
    done
  fi
  for m in $(smolvm machine ls --json 2>/dev/null | jq -r '.[].name' | grep -E '^bench-' || true); do
    smolvm machine stop --name "$m" >/dev/null 2>&1; smolvm machine delete --name "$m" -f >/dev/null 2>&1
  done
  rm -rf "$PRELOOP_HOME"
  mkdir -p "$PRELOOP_HOME" "$TMPDIR"
}

start_server() {
  # A leftover engine on this port would make the new one exit with
  # "Address already in use" and every run would then fail against a server
  # that is not the one under test.
  for i in $(seq 1 30); do
    ss -ltn "sport = :$PORT" 2>/dev/null | grep -q LISTEN || break
    log "port $PORT still busy; waiting"
    sleep 2
  done
  if ss -ltn "sport = :$PORT" 2>/dev/null | grep -q LISTEN; then
    log "port $PORT is still in use; aborting"; return 1
  fi
  log "starting preloop serve ($SUBSTRATE) on :$PORT"
  ( cd "$WORK" && nohup "$BIN" serve --listen "0.0.0.0:$PORT" > "$OUTDIR/server.log" 2>&1 & echo $! > "$OUTDIR/server.pid" )
  for i in $(seq 1 120); do
    if curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then log "server healthy after ${i}s"; return 0; fi
    if ! kill -0 "$(cat "$OUTDIR/server.pid")" 2>/dev/null; then
      log "server exited during startup: $(tail -2 "$OUTDIR/server.log")"; return 1
    fi
    sleep 1
  done
  log "server never became healthy"; return 1
}

# Block until the pool has finished preparing its golden.
#
# Submitting into a preparing pool is not a substrate measurement: the
# control plane fails a job that waits more than 600s for a matching runner,
# and a golden bake (rust + go + docker tooling) legitimately exceeds that on
# either substrate. Cold start is therefore measured here, from process start
# to a pool that can serve work, and the job timings below all start warm.
pool_ready() {
  local deadline=$((SECONDS + 3600))
  # `preparing` is false before the pool has even begun, so poll the
  # orchestrator's own readiness line instead. It must be one emitted AFTER
  # the golden bake: "warm runner pool disabled…" is printed at startup and
  # would match a pool that has not built anything yet.
  while [ $SECONDS -lt $deadline ]; do
    grep -qE '"message":"(on-demand runner pool|runner pool ready)' "$OUTDIR/server.log" && return 0
    sleep 2
  done
  return 1
}

stop_server() {
  local pid; pid=$(cat "$OUTDIR/server.pid" 2>/dev/null || true)
  [ -n "$pid" ] && kill "$pid" 2>/dev/null
  for i in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  kill -9 "$pid" 2>/dev/null
  log "server stopped"
}

# One workflow run, timed end to end from submit to terminal state.
run_one() { # workflow rep
  local wf="$1" rep="$2" s e rc
  s=$(date +%s.%N)
  ( cd "$WORK" && timeout 1800 "$BIN" run -f "$wf" --no-debug > "$OUTDIR/$wf.$rep.out" 2>&1 )
  rc=$?
  e=$(date +%s.%N)
  local run_id conclusion
  run_id=$(grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' "$OUTDIR/$wf.$rep.out" | head -1)
  conclusion=success
  [ $rc != 0 ] && conclusion=failure
  printf '{"substrate":"%s","workflow":"%s","rep":%s,"seconds":%s,"exit":%s,"conclusion":"%s","run_id":"%s"}\n' \
    "$SUBSTRATE" "$wf" "$rep" "$(echo "$e-$s" | bc)" "$rc" "$conclusion" "${run_id:-}" >> "$OUTDIR/results.jsonl"
  if [ -n "$run_id" ]; then
    ( cd "$WORK" && "$BIN" status "$run_id" > "$OUTDIR/$wf.$rep.status.json" 2>&1 )
  fi
  log "  $wf rep$rep: $(printf '%.1f' "$(echo "$e-$s" | bc)")s rc=$rc"
}

WORKFLOWS=(bench-hello.yml bench-io.yml bench-checkout-rust.yml bench-node.yml bench-matrix8.yml)

purge
: > "$OUTDIR/results.jsonl"

# Cold start: process start to a pool that can serve work (golden prepared).
COLD_START=$(date +%s.%N)
start_server || exit 1
if pool_ready; then
  COLD_END=$(date +%s.%N)
  log "cold start to pool ready: $(printf '%.1f' "$(echo "$COLD_END-$COLD_START" | bc)")s"
  printf '{"substrate":"%s","workflow":"__cold_start_to_pool_ready","rep":0,"seconds":%s,"exit":0,"conclusion":"success","run_id":""}\n' \
    "$SUBSTRATE" "$(echo "$COLD_END-$COLD_START" | bc)" >> "$OUTDIR/results.jsonl"
else
  log "pool never finished preparing; aborting"
  stop_server; exit 1
fi

# Steady state: every workflow, REPS times, same order for both substrates.
for rep in $(seq 1 "$REPS"); do
  for wf in "${WORKFLOWS[@]}"; do
    run_one "$wf" "$rep"
  done
done

stop_server
log "e2e complete: $OUTDIR/results.jsonl"
