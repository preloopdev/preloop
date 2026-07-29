#!/usr/bin/env bash
set -euo pipefail
# Strip stale OAuth token so it never leaks into scripts or captures.
unset GITHUB_TOKEN 2>/dev/null || true
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE="$MITM_DIR/.cache"
MITM_PORT="${MITM_PORT:-8080}"
MITM_HOST="${MITM_HOST:-127.0.0.1}"
MITM_LISTEN_HOST="${MITM_LISTEN_HOST:-$MITM_HOST}"
MITM_URL="http://$MITM_HOST:$MITM_PORT"

usage() {
    echo "Usage: $0 --backend {official|runner-server|aksh} --scenario <name> [--non-interactive]" >&2
    exit 1
}

BACKEND=""
SCENARIO=""
NON_INTERACTIVE=false
while [ $# -gt 0 ]; do
    case "$1" in
        --backend) BACKEND="$2"; shift 2 ;;
        --scenario) SCENARIO="$2"; shift 2 ;;
        --non-interactive) NON_INTERACTIVE=true; shift ;;
        *) usage ;;
    esac
done
[ -z "$BACKEND" ] && usage
[ -z "$SCENARIO" ] && usage

SCENARIO_DIR="$MITM_DIR/scenarios/$SCENARIO"
[ -d "$SCENARIO_DIR" ] || { echo "scenario not found: $SCENARIO_DIR" >&2; exit 1; }
[ -f "$SCENARIO_DIR/scenario.toml" ] || { echo "scenario.toml not found in $SCENARIO_DIR" >&2; exit 1; }

TIMESTAMP=$(date -u +%Y-%m-%dT%H-%M-%SZ)
CAPTURE_DIR="$MITM_DIR/captures/$BACKEND/$SCENARIO/$TIMESTAMP"
SHORT_TS=$(date -u +%s)
mkdir -p "$CAPTURE_DIR"
export MITM_CAPTURE_DIR="$CAPTURE_DIR"
echo "capture dir: $CAPTURE_DIR"


# Port conflict detection. lsof can block on remote filesystems and is not
# installed in minimal capture hosts. Probe loopback because the advertised
# proxy host may be a Docker bridge that drops packets while nothing listens.
if curl -fsS --connect-timeout 1 --max-time 2 --proxy "http://127.0.0.1:$MITM_PORT" http://mitm.it/ >/dev/null 2>&1; then
    echo "port $MITM_PORT (mitmproxy) is already in use — stop it first or choose another MITM_PORT" >&2
    exit 2
fi
if [ "$BACKEND" = "aksh" ]; then
    AKSH_BASE_URL="${AKSH_URL:-http://127.0.0.1:9090}"
    if ! curl -fsS "$AKSH_BASE_URL/healthz" >/dev/null 2>&1; then
        echo "aksh backend requires a running aksh on $AKSH_BASE_URL — run bin/up-aksh.sh first or set AKSH_URL" >&2
        exit 2
    fi
    echo "http://aksh.local:9090/runner/server" > "$CACHE/aksh.url"
    echo "ThisIsIgnored" > "$CACHE/aksh.token"
fi
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
RUNNER_NAME="m-$BACKEND-$SCENARIO-$SHORT_TS"

# Determine backend port for mitm proxy port-80 forwarding.
if [ "$BACKEND" = "runner-server" ]; then
    export BACKEND_PORT=5000
elif [ "$BACKEND" = "aksh" ]; then
    export BACKEND_PORT=9090
fi

# Start mitmdump.
echo "starting mitmdump..."
CONFDIR="$CACHE/mitmproxy"
mkdir -p "$CONFDIR"
mitmdump \
    --quiet \
    --listen-host "$MITM_LISTEN_HOST" \
    --listen-port "$MITM_PORT" \
    --set confdir="$CONFDIR" \
    -s "$MITM_DIR/addons/capture.py" \
    --save-stream-file "$CAPTURE_DIR/flows.mitm" &
MITM_PID=$!
echo "$MITM_PID" > "$CAPTURE_DIR/mitmdump.pid"

# Wait for mitmproxy.
echo "waiting for mitmproxy..."
for i in $(seq 1 30); do
    if curl -fsS --connect-timeout 1 --max-time 2 --proxy "http://127.0.0.1:$MITM_PORT" http://mitm.it/ >/dev/null 2>&1; then
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "mitmproxy did not start" >&2
        kill "$MITM_PID" 2>/dev/null || true
        exit 4
    fi
    sleep 0.5
done

# Prepare runner dir.
RUNNER_DIR="$CACHE/runner-$BACKEND-$(uname -s)-$(uname -m)"
RUNNER_VERSION=$(grep runner_version "$MITM_DIR/versions.toml" | cut -d'"' -f2)

stop_cached_runner_processes() {
    # run.sh may exit before its Listener/Worker children. Those children can
    # rewrite credentials while the next ephemeral runner is being configured.
    for process in Runner.Listener Runner.Worker; do
        pkill -TERM -f "$RUNNER_DIR/bin/$process" 2>/dev/null || true
    done
    sleep 1
    for process in Runner.Listener Runner.Worker; do
        pkill -KILL -f "$RUNNER_DIR/bin/$process" 2>/dev/null || true
    done
}

setup_runner() {
    local config_url="$1"
    local config_token="$2"
    local config_name="$3"

    if [ ! -f "$RUNNER_DIR/run.sh" ]; then
        echo "downloading actions/runner v$RUNNER_VERSION..."
        mkdir -p "$RUNNER_DIR"
        case "$(uname -s)/$(uname -m)" in
            Darwin/arm64) RUNNER_PLATFORM="osx-arm64" ;;
            Darwin/x86_64) RUNNER_PLATFORM="osx-x64" ;;
            Linux/aarch64|Linux/arm64) RUNNER_PLATFORM="linux-arm64" ;;
            Linux/x86_64) RUNNER_PLATFORM="linux-x64" ;;
            *) echo "unsupported runner host: $(uname -s)/$(uname -m)" >&2; return 1 ;;
        esac
        TARBALL="actions-runner-${RUNNER_PLATFORM}-${RUNNER_VERSION}.tar.gz"
        curl -fsSL "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${TARBALL}" -o "$RUNNER_DIR/runner.tar.gz"
        tar xzf "$RUNNER_DIR/runner.tar.gz" -C "$RUNNER_DIR"
        rm "$RUNNER_DIR/runner.tar.gz"
        if [ -x "$RUNNER_DIR/bin/installdependencies.sh" ]; then
            "$RUNNER_DIR/bin/installdependencies.sh"
        fi
    fi

    stop_cached_runner_processes
    cd "$RUNNER_DIR"

    if [ -f .runner ]; then
        echo "removing existing runner configuration..."
        ./config.sh remove --unattended --token "$config_token" >/dev/null 2>&1 || {
            rm -f .runner .credentials .credentials_rsaparams
        }
    fi

    # Write .env for run.sh (sourced by env.sh at runner startup).
    cat > .env <<ENVEOF
https_proxy=$MITM_URL
HTTPS_PROXY=$MITM_URL
http_proxy=$MITM_URL
HTTP_PROXY=$MITM_URL
no_proxy=
NO_PROXY=
GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1
GIT_SSL_NO_VERIFY=true
NODE_EXTRA_CA_CERTS=$MITM_DIR/.cache/mitmproxy/mitmproxy-ca-cert.pem
SSL_CERT_FILE=$MITM_DIR/.cache/mitmproxy/mitmproxy-ca-cert.pem
ENVEOF

    export https_proxy="$MITM_URL" HTTPS_PROXY="$MITM_URL"
    export http_proxy="$MITM_URL"  HTTP_PROXY="$MITM_URL"
    export no_proxy= NO_PROXY=
    export GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1
    export GIT_SSL_NO_VERIFY=true

    echo "configuring runner..."
    ./config.sh --unattended \
        --url "$config_url" \
        --token "$config_token" \
        --name "$config_name" \
        --labels "${RUNNER_LABELS:-self-hosted,mitm}" \
        --work _work \
        --replace || return 1
}

RUNNER_PID=""
RUNNER_EXIT=0
STATUS="ok"

if [ "$BACKEND" = "official" ]; then
    : "${GITHUB_OWNER:?must set GITHUB_OWNER}"
    : "${GITHUB_REPO:?must set GITHUB_REPO}"
    : "${GITHUB_REF:?must set GITHUB_REF}"
    : "${GITHUB_RUNNER_TOKEN:?must set GITHUB_RUNNER_TOKEN}"
    if ! setup_runner "https://github.com/$GITHUB_OWNER/$GITHUB_REPO" "$GITHUB_RUNNER_TOKEN" "$RUNNER_NAME"; then
        STATUS="config_failed"
    fi
elif [ "$BACKEND" = "runner-server" ]; then
    RS_URL=$(cat "$CACHE/runner-server.url")
    RS_TOKEN=$(cat "$CACHE/runner-server.token")
    if ! setup_runner "$RS_URL" "$RS_TOKEN" "$RUNNER_NAME"; then
        STATUS="config_failed"
    fi
elif [ "$BACKEND" = "aksh" ]; then
    AKSH_URL=$(cat "$CACHE/aksh.url")
    AKSH_TOKEN=$(cat "$CACHE/aksh.token")
    if ! setup_runner "$AKSH_URL" "$AKSH_TOKEN" "$RUNNER_NAME"; then
        STATUS="config_failed"
    fi
else
    echo "unknown backend: $BACKEND (expected: official, runner-server, aksh)" >&2
    exit 1
fi

# Start runner and run scenario (skip if config failed).
if [ "$STATUS" != "config_failed" ]; then
    echo "starting runner..."
    cd "$RUNNER_DIR"
    ./run.sh > "$CAPTURE_DIR/runner.log" 2>&1 &
    RUNNER_PID=$!
    echo "$RUNNER_PID" > "$CAPTURE_DIR/runner.pid"

    echo "running scenario $SCENARIO..."
    RUNNER_EXIT=0
    if ! "$MITM_DIR/bin/_run_scenario.py" \
        --backend "$BACKEND" \
        --scenario "$SCENARIO_DIR/scenario.toml" \
        --capture-dir "$CAPTURE_DIR" \
        --mitm-dir "$MITM_DIR" \
        --run "true" 2>&1 | tee -a "$CAPTURE_DIR/runner.log"; then
        STATUS="scenario_failed"
    fi
else
    echo "runner configuration failed — check output above" >&2
    if [ -f "$CAPTURE_DIR/runner.log" ]; then
        echo "--- last 20 lines of runner.log ---" >&2
        tail -20 "$CAPTURE_DIR/runner.log" >&2
    fi
fi

# Teardown.
echo "cleaning up..."
if [ "${RUNNER_PID:-}" != "" ] && kill -0 "$RUNNER_PID" 2>/dev/null; then
    echo "stopping runner..."
    kill -INT "$RUNNER_PID" 2>/dev/null || true
    stop_cached_runner_processes
    for i in $(seq 1 5); do
        if ! kill -0 "$RUNNER_PID" 2>/dev/null; then break; fi
        sleep 1
    done
    if kill -0 "$RUNNER_PID" 2>/dev/null; then
        kill -TERM "$RUNNER_PID" 2>/dev/null || true
        sleep 3
        kill -KILL "$RUNNER_PID" 2>/dev/null || true
    fi
    wait "$RUNNER_PID" 2>/dev/null || RUNNER_EXIT=$?
fi
stop_cached_runner_processes

echo "stopping mitmdump..."
kill -INT "$MITM_PID" 2>/dev/null || true
wait "$MITM_PID" 2>/dev/null || true

ENDED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
if [ -f "$CAPTURE_DIR/flows.jsonl" ]; then
    FLOW_COUNT=$(wc -l < "$CAPTURE_DIR/flows.jsonl")
else
    FLOW_COUNT=0
fi

cat > "$CAPTURE_DIR/summary.json" <<JSONEND
{
  "backend": "$BACKEND",
  "scenario": "$SCENARIO",
  "started_at": "$STARTED_AT",
  "ended_at": "$ENDED_AT",
  "status": "$STATUS",
  "runner_exit_code": $RUNNER_EXIT,
  "flows_count": $FLOW_COUNT,
  "runner_version": "$RUNNER_VERSION",
  "runner_server_ref": "$(grep runner_server_ref "$MITM_DIR/versions.toml" | cut -d'"' -f2)",
  "mitmproxy_version": "$(mitmdump --version 2>&1 | head -1)"
}
JSONEND

LATEST="$MITM_DIR/captures/$BACKEND/$SCENARIO/latest"
rm -f "$LATEST"
ln -s "$CAPTURE_DIR" "$LATEST"

echo "done. status=$STATUS, flows=$FLOW_COUNT, runner_exit=$RUNNER_EXIT"
