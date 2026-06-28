#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE="$MITM_DIR/.cache"

usage() {
    echo "Usage: $0 --golden <capture-dir> --target <backend> [--port <port>] [--timeout <seconds>]" >&2
    echo "" >&2
    echo "Replays recorded golden traffic against a target backend using mitmdump --server-replay." >&2
    echo "The golden capture's flows.mitm file is replayed; the target backend answers as if live." >&2
    echo "" >&2
    echo "  --golden   Path to a golden capture directory (must contain flows.mitm)" >&2
    echo "  --target   Backend to replay against (e.g. aksh, runner-server)" >&2
    echo "  --port     Port for mitmdump to listen on (default: 8081)" >&2
    echo "  --timeout  Max seconds to run replay (default: 120)" >&2
    exit 1
}

GOLDEN=""
TARGET=""
PORT=8081
TIMEOUT=120
while [ $# -gt 0 ]; do
    case "$1" in
        --golden) GOLDEN="$2"; shift 2 ;;
        --target) TARGET="$2"; shift 2 ;;
        --port) PORT="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        *) usage ;;
    esac
done
[ -z "$GOLDEN" ] && usage
[ -z "$TARGET" ] && usage

# Resolve golden capture dir (support 'latest' symlink).
if [ -d "$GOLDEN" ]; then
    GOLDEN_DIR="$GOLDEN"
elif [ -d "$MITM_DIR/captures/official/$GOLDEN/latest" ]; then
    GOLDEN_DIR="$MITM_DIR/captures/official/$GOLDEN/latest"
else
    echo "golden capture not found: $GOLDEN" >&2
    exit 4
fi

FLOW_FILE="$GOLDEN_DIR/flows.mitm"
if [ ! -f "$FLOW_FILE" ]; then
    echo "flows.mitm not found in $GOLDEN_DIR — record a golden capture first" >&2
    exit 4
fi

# Determine target URL.
case "$TARGET" in
    aksh)
        if [ ! -f "$CACHE/aksh.url" ]; then
            echo "aksh not running — run bin/up-aksh.sh first" >&2
            exit 3
        fi
        TARGET_URL=$(cat "$CACHE/aksh.url")
        ;;
    runner-server)
        if [ ! -f "$CACHE/runner-server.url" ]; then
            echo "runner-server not running — run bin/up-runner-server.sh first" >&2
            exit 3
        fi
        TARGET_URL=$(cat "$CACHE/runner-server.url")
        ;;
    *)
        echo "unknown target: $TARGET (expected: aksh, runner-server)" >&2
        exit 1
        ;;
esac

echo "replaying golden capture against $TARGET ($TARGET_URL)..."
echo "golden: $GOLDEN_DIR"
echo "listen port: $PORT"
echo "timeout: ${TIMEOUT}s"

# Prepare replay capture dir.
TIMESTAMP=$(date -u +%Y-%m-%dT%H-%M-%SZ)
REPLAY_DIR="$MITM_DIR/captures/replay-$TARGET/$TIMESTAMP"
mkdir -p "$REPLAY_DIR"
export MITM_CAPTURE_DIR="$REPLAY_DIR"

CONFDIR="$CACHE/mitmproxy"
mkdir -p "$CONFDIR"

# Start mitmdump in server-replay mode.
# --server-replay-nopop: don't remove flows after replay (allows re-use).
# --set upstream_cert=false: don't validate upstream TLS certs.
mitmdump \
    --listen-host 127.0.0.1 \
    --listen-port "$PORT" \
    --set confdir="$CONFDIR" \
    --set upstream_cert=false \
    --server-replay "$FLOW_FILE" \
    --server-replay-nopop \
    -s "$MITM_DIR/addons/capture.py" \
    --save-stream-file "$REPLAY_DIR/flows.mitm" &
MITM_PID=$!
echo "$MITM_PID" > "$REPLAY_DIR/mitmdump.pid"

# Wait for mitmproxy.
echo "waiting for mitmproxy on port $PORT..."
ready=0
for i in $(seq 1 30); do
    if curl -fsS "http://127.0.0.1:$PORT" >/dev/null 2>&1 || \
       curl -fsS -o /dev/null -w '' "http://127.0.0.1:$PORT" 2>/dev/null; then
        ready=1
        break
    fi
    sleep 1
done

if [ "$ready" -eq 0 ]; then
    # mitmdump might not respond to plain GET but is still running.
    if ! kill -0 "$MITM_PID" 2>/dev/null; then
        echo "mitmdump died during startup" >&2
        exit 1
    fi
fi

echo "replay proxy running (pid $MITM_PID)"

# Generate traffic by running the runner through the replay proxy.
# The runner will send requests to the proxy, which replays golden responses.
RUNNER_DIR="$CACHE/runner-replay"
RUNNER_VERSION=$(grep runner_version "$MITM_DIR/versions.toml" | cut -d'"' -f2)

if [ -f "$RUNNER_DIR/run.sh" ]; then
    echo "configuring runner for replay..."
    cd "$RUNNER_DIR"

    cat > .env <<ENVEOF
https_proxy=http://127.0.0.1:$PORT
HTTPS_PROXY=http://127.0.0.1:$PORT
http_proxy=http://127.0.0.1:$PORT
HTTP_PROXY=http://127.0.0.1:$PORT
no_proxy=
NO_PROXY=
GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1
NODE_EXTRA_CA_CERTS=$CACHE/mitmproxy/mitmproxy-ca-cert.pem
SSL_CERT_FILE=$CACHE/mitmproxy/mitmproxy-ca-cert.pem
ENVEOF

    export https_proxy="http://127.0.0.1:$PORT" HTTPS_PROXY="http://127.0.0.1:$PORT"
    export http_proxy="http://127.0.0.1:$PORT" HTTP_PROXY="http://127.0.0.1:$PORT"
    export no_proxy= NO_PROXY=
    export GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1

    echo "starting runner for replay..."
    ./run.sh > "$REPLAY_DIR/runner.log" 2>&1 &
    RUNNER_PID=$!

    echo "waiting up to ${TIMEOUT}s for replay..."
    sleep "$TIMEOUT"

    if kill -0 "$RUNNER_PID" 2>/dev/null; then
        echo "stopping runner..."
        kill -INT "$RUNNER_PID" 2>/dev/null || true
        wait "$RUNNER_PID" 2>/dev/null || true
    fi
else
    echo "runner not cached at $RUNNER_DIR — skipping runner-driven replay" >&2
    echo "replay proxy is still running; send traffic manually via:" >&2
    echo "  curl -x http://127.0.0.1:$PORT http://..." >&2
    echo "press Ctrl+C to stop." >&2
    wait "$MITM_PID" 2>/dev/null || true
fi

# Teardown.
echo "stopping mitmdump..."
kill -INT "$MITM_PID" 2>/dev/null || true
wait "$MITM_PID" 2>/dev/null || true

FLOW_COUNT=$(wc -l < "$REPLAY_DIR/flows.jsonl" 2>/dev/null || echo 0)
echo "done. replayed flows=$FLOW_COUNT"
echo "capture dir: $REPLAY_DIR"
