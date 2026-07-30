#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
RUNNER_VERSION="$(
  python3 -c 'import tomllib; print(tomllib.load(open("versions.toml", "rb"))["runner_version"])'
)"
STATE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/preloop-conform.XXXXXX")"
SERVER_LOG="$STATE_DIR/server.log"
SERVER_PID=""
REPLAY_PORT="$(
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
)"
REPLAY_URL="http://127.0.0.1:$REPLAY_PORT"

cleanup() {
  local status=$?
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  if [[ $status -eq 0 ]]; then
    rm -rf "$STATE_DIR"
  else
    echo "conform: preserved failure artifacts at $STATE_DIR" >&2
  fi
  return "$status"
}
trap cleanup EXIT

python3 benchmarks/conformance/check_corpus.py

# The CI recipe already runs the workspace tests. Standalone conformance builds
# only the server it executes; runner-watch is told not to repeat the suite.
cargo build --quiet -p aksh-runner-server
AKSH_PUBLIC_URL="$REPLAY_URL" \
  ./target/debug/preloop-server serve --listen "127.0.0.1:$REPLAY_PORT" \
  --state-dir "$STATE_DIR/server-state" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do
  kill -0 "$SERVER_PID" >/dev/null 2>&1 ||
    { cat "$SERVER_LOG" >&2; echo "conform: replay server exited" >&2; exit 1; }
  curl -fsS "$REPLAY_URL/healthz" >/dev/null 2>&1 && break
  sleep .1
done
curl -fsS "$REPLAY_URL/healthz" >/dev/null ||
  { cat "$SERVER_LOG" >&2; echo "conform: replay server not ready" >&2; exit 1; }

# Official runner -> GitHub is the committed golden. Replaying every request
# into the current server establishes official runner -> Preloop and performs
# runner-watch's strict normalized endpoint/status/header/body-schema diff.
cargo run --quiet -p runner-watch -- conform --runner "v$RUNNER_VERSION" \
  --aksh-url "$REPLAY_URL" --skip-cargo-test
kill "$SERVER_PID" >/dev/null 2>&1 || true
wait "$SERVER_PID" >/dev/null 2>&1 || true
SERVER_PID=""

echo "conform: PASS"
