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
cargo build --quiet -p preloop-runner-server
# Pin the engine config into the throwaway state dir. Without this the replay
# server reads the developer's ~/.preloop/config.toml, so conformance would
# depend on host credentials — and a stale or malformed App key there aborts
# startup, failing the run for reasons that have nothing to do with protocol
# fidelity. The path intentionally does not exist: a missing file loads the
# default (unconfigured) engine config.
# The goldens carry real GitHub-issued registration tokens this control plane
# cannot verify, so the replay server opts into the permissive registration
# policy — the same sanctioned exception `preloop-conformance` uses. Without
# it every scenario 401s on /api/v3/actions/runner-registration.
PRELOOP_PUBLIC_URL="$REPLAY_URL" \
  PRELOOP_CONFIG="$STATE_DIR/config.toml" \
  PRELOOP_REGISTRATION_POLICY="permissive" \
  ./target/debug/preloop-server serve --listen "127.0.0.1:$REPLAY_PORT" \
  --state-dir "$STATE_DIR/server-state" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
# Fresh state generates both the runner session and OIDC RSA keypairs. Prime
# generation is nondeterministic and can exceed ten seconds even on otherwise
# idle hosts, so give startup the same minute-scale allowance as other local
# service probes instead of making conformance timing-dependent.
for _ in $(seq 1 600); do
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
  --preloop-url "$REPLAY_URL" --skip-cargo-test

if [[ -d ".runner-watch/golden/v2.337.0/gh-official" ]]; then
  cargo run --quiet -p runner-watch -- conform --runner "2.337.0/gh-official" \
    --preloop-url "$REPLAY_URL" --skip-cargo-test
fi

kill "$SERVER_PID" >/dev/null 2>&1 || true
wait "$SERVER_PID" >/dev/null 2>&1 || true
SERVER_PID=""

echo "conform: PASS"
