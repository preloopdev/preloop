#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE="$MITM_DIR/.cache"

REF=$(grep runner_server_ref "$MITM_DIR/versions.toml" | cut -d'"' -f2)
CLONE_DIR="$CACHE/runner.server"
URL="http://127.0.0.1:5000"

# Check prerequisites.
if ! command -v dotnet &>/dev/null; then
    echo "install dotnet sdk 8.0 or newer (brew install --cask dotnet-sdk)" >&2
    exit 3
fi

# Check port.
if lsof -ti:5000 &>/dev/null; then
    echo "port 5000 is already in use" >&2
    exit 2
fi

# Clone or update runner.server.
if [ ! -d "$CLONE_DIR" ]; then
    echo "cloning runner.server..."
    mkdir -p "$CLONE_DIR"
    git clone --depth 1 https://github.com/ChristopherHX/runner.server "$CLONE_DIR"
fi

cd "$CLONE_DIR"
git fetch --depth 1 origin "$REF" 2>/dev/null || git fetch --depth 1 origin
git checkout -q "$REF" 2>/dev/null || git checkout -q "origin/$REF" 2>/dev/null || git checkout -q "$REF"

echo "building runner.server..."
dotnet run --project src/Runner.Server -- --urls "$URL" &
PID=$!
echo "$PID" > "$CACHE/runner-server.pid"

ready=0
echo "waiting for runner.server..."
for i in $(seq 1 120); do
    if curl -fsS "$URL/_apis/connectionData" >/dev/null 2>&1; then
        ready=1
        echo "runner.server is ready"
        break
    fi
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "runner.server died during startup" >&2
        exit 1
    fi
    sleep 1
done

if [ "$ready" -eq 0 ]; then
    echo "runner.server did not become ready within 120s" >&2
    kill -INT "$PID" 2>/dev/null || true
    exit 1
fi
# Write artefacts.
echo "$URL/runner/server" > "$CACHE/runner-server.url"
echo "ThisIsIgnored" > "$CACHE/runner-server.token"

echo "runner.server running on $URL (pid $PID)"
