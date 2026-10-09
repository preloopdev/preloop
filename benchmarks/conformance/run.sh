#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
STATE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/preloop-conform.XXXXXX")"
SERVER_LOG="$STATE_DIR/server.log"
SERVER_PID=""
REPLAY_PORT="$(
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
)"
REPLAY_URL="http://127.0.0.1:$REPLAY_PORT"
if [[ -z "${PRELOOP_SYSTEM_TOKEN:-}" ]]; then
  export PRELOOP_SYSTEM_TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
fi


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

TARGETS="$(
  python3 -c '
import tomllib
from pathlib import Path

config = tomllib.loads(Path("benchmarks/conformance/targets.toml").read_text())
for target in config["targets"]:
    print("|".join((
        target["runner_version"],
        target.get("cell", ""),
        target.get("scenario_prefix", ""),
        "1" if target.get("validate_ownership", False) else "0",
        ",".join(target.get("exclude_prefix", [])),
    )))
'
)"

while IFS='|' read -r runner_version cell scenario_prefix validate_ownership exclude_prefix; do
  [[ -z "$runner_version" ]] && continue
  check_args=(
    --version "$runner_version"
    --golden-root ".runner-watch/golden/v$runner_version"
  )
  [[ -n "$cell" ]] && check_args+=(--cell "$cell")
  [[ -n "$scenario_prefix" ]] && check_args+=(--scenario-prefix "$scenario_prefix")
  if [[ -n "$exclude_prefix" ]]; then
    IFS=',' read -ra excluded <<< "$exclude_prefix"
    for prefix in "${excluded[@]}"; do
      check_args+=(--exclude-prefix "$prefix")
    done
  fi
  [[ "$validate_ownership" == "1" ]] && check_args+=(--validate-ownership)
  python3 benchmarks/conformance/check_corpus.py "${check_args[@]}"
done <<< "$TARGETS"

# The CI recipe already runs the workspace tests. Standalone conformance builds
# only the binaries it executes; runner-watch is told not to repeat the suite.
cargo build --quiet -p preloop-runner-server -p preloop-cli
# Pin the engine config into the throwaway state dir. Without this the replay
# server reads the developer's ~/.preloop/config.toml, so conformance would
# depend on host credentials — and a stale or malformed App key there aborts
# startup, failing the run for reasons that have nothing to do with protocol
# fidelity. The file carries no settings on purpose: scenario 110's job
# declares `environment: staging`, and protection rules now come from GitHub's
# environments API — the replay has no credential to reach it, so no
# `environment:` name needs registering (and a `[environments]` registry is
# rejected by the loader).
cat > "$STATE_DIR/config.toml" <<'EOF'
# Intentionally empty; see the comment in run.sh.
EOF
# The control store is migration-ledgered, and `preloop serve` refuses a
# database with no ledger instead of creating one: a fresh conformance state
# dir initializes its store the way an install does (`preloop store migrate`)
# before the replay server starts.
./target/debug/preloop store migrate --store "$STATE_DIR/server-state/preloop.db"
# The goldens carry real GitHub-issued registration tokens this control plane
# cannot verify, so the replay server opts into the permissive registration
# policy — the same sanctioned exception `preloop-conformance` uses. Without
# it every scenario 401s on /api/v3/actions/runner-registration.
PRELOOP_PUBLIC_URL="$REPLAY_URL" \
  PRELOOP_SYSTEM_TOKEN="$PRELOOP_SYSTEM_TOKEN" \
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

# Each target uses the same throwaway server; reports remain isolated under
# .runner-watch/conformance/v<version>.
while IFS='|' read -r runner_version cell scenario_prefix validate_ownership exclude_prefix; do
  [[ -z "$runner_version" ]] && continue
  echo "conform: replaying runner v$runner_version${cell:+ ($cell)}"
  conform_args=(
    cargo run --quiet -p runner-watch -- conform
    --runner "v$runner_version"
    --preloop-url "$REPLAY_URL"
    --skip-cargo-test
  )
  [[ -n "$cell" ]] && conform_args+=(--cell "$cell")
  "${conform_args[@]}"
done <<< "$TARGETS"
kill "$SERVER_PID" >/dev/null 2>&1 || true
wait "$SERVER_PID" >/dev/null 2>&1 || true
SERVER_PID=""

echo "conform: PASS"
