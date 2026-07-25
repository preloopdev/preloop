#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE="$MITM_DIR/.cache"

AKSH_PORT=9090
AKSH_URL="http://127.0.0.1:$AKSH_PORT"
WORKTREE_ROOT="$(cd "$MITM_DIR/../.." && pwd)"
STATE_DIR="$CACHE/aksh-state"

# Check prerequisites.
if ! command -v cargo &>/dev/null; then
    echo "install rust toolchain (https://rustup.rs)" >&2
    exit 3
fi

# Check port.
if lsof -ti:$AKSH_PORT &>/dev/null; then
    echo "port $AKSH_PORT is already in use" >&2
    exit 2
fi

# Build aksh-runner-server from the main worktree.
echo "building aksh-runner-server..."
mkdir -p "$STATE_DIR"
cargo build --release -p aksh-runner-server --manifest-path "$WORKTREE_ROOT/Cargo.toml" 2>&1 | tail -5

AKSH_BIN="$WORKTREE_ROOT/target/release/aksh-server"
if [ ! -x "$AKSH_BIN" ]; then
    # Fallback: cargo build puts binaries in target/release with the crate's binary name.
    AKSH_BIN="$WORKTREE_ROOT/target/release/preloop-server"
fi
if [ ! -x "$AKSH_BIN" ]; then
    echo "aksh binary not found after build" >&2
    exit 1
fi

echo "starting aksh on $AKSH_URL..."
"$AKSH_BIN" serve --listen "127.0.0.1:$AKSH_PORT" --state-dir "$STATE_DIR" &
PID=$!
echo "$PID" > "$CACHE/aksh.pid"

ready=0
echo "waiting for aksh..."
for i in $(seq 1 120); do
    if curl -fsS "$AKSH_URL/healthz" >/dev/null 2>&1; then
        ready=1
        echo "aksh is ready"
        break
    fi
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "aksh died during startup" >&2
        exit 1
    fi
    sleep 1
done

if [ "$ready" -eq 0 ]; then
    echo "aksh did not become ready within 120s" >&2
    kill -INT "$PID" 2>/dev/null || true
    exit 1
fi

# Write artefacts. The runner connects via the runner.server-compat URL prefix.
echo "http://aksh.local:9090/runner/server" > "$CACHE/aksh.url"
echo "ThisIsIgnored" > "$CACHE/aksh.token"

echo "aksh running on $AKSH_URL (pid $PID)"
