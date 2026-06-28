#!/usr/bin/env bash
set -euo pipefail
# Strip stale OAuth token so it never leaks into scripts or captures.
unset GITHUB_TOKEN 2>/dev/null || true
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE="$MITM_DIR/.cache"

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
mkdir -p "$CAPTURE_DIR"
export MITM_CAPTURE_DIR="$CAPTURE_DIR"
echo "capture dir: $CAPTURE_DIR"


# Port conflict detection.
if lsof -ti:8080 &>/dev/null; then
    echo "port 8080 (mitmproxy) is already in use — stop it first or kill the process" >&2
    exit 2
fi
if [ "$BACKEND" = "aksh" ] && lsof -ti:9090 &>/dev/null; then
    echo "port 9090 (aksh) is already in use — run bin/down-aksh.sh first" >&2
    exit 2
fi
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Start mitmdump.
echo "starting mitmdump..."
CONFDIR="$CACHE/mitmproxy"
mkdir -p "$CONFDIR"
mitmdump \
    --listen-host 127.0.0.1 \
    --listen-port 8080 \
    --set confdir="$CONFDIR" \
    -s "$MITM_DIR/addons/capture.py" \
    --save-stream-file "$CAPTURE_DIR/flows.mitm" &
MITM_PID=$!
echo "$MITM_PID" > "$CAPTURE_DIR/mitmdump.pid"

# Wait for mitmproxy.
echo "waiting for mitmproxy..."
for i in $(seq 1 30); do
    if nc -z 127.0.0.1 8080 2>/dev/null; then
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
RUNNER_DIR="$CACHE/runner-$BACKEND"
RUNNER_VERSION=$(grep runner_version "$MITM_DIR/versions.toml" | cut -d'"' -f2)

setup_runner() {
    local config_url="$1"
    local config_token="$2"
    local config_name="$3"

    if [ ! -f "$RUNNER_DIR/run.sh" ]; then
        echo "downloading actions/runner v$RUNNER_VERSION..."
        mkdir -p "$RUNNER_DIR"
        TARBALL="actions-runner-osx-arm64-${RUNNER_VERSION}.tar.gz"
        curl -fsSL "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${TARBALL}" -o "$RUNNER_DIR/runner.tar.gz"
        tar xzf "$RUNNER_DIR/runner.tar.gz" -C "$RUNNER_DIR"
        rm "$RUNNER_DIR/runner.tar.gz"
    fi

    cd "$RUNNER_DIR"

    # Write .env for run.sh (sourced by env.sh at runner startup).
    cat > .env <<ENVEOF
https_proxy=http://127.0.0.1:8080
HTTPS_PROXY=http://127.0.0.1:8080
http_proxy=http://127.0.0.1:8080
HTTP_PROXY=http://127.0.0.1:8080
no_proxy=
NO_PROXY=
GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1
NODE_EXTRA_CA_CERTS=$MITM_DIR/.cache/mitmproxy/mitmproxy-ca-cert.pem
SSL_CERT_FILE=$MITM_DIR/.cache/mitmproxy/mitmproxy-ca-cert.pem
ENVEOF

    export https_proxy=http://127.0.0.1:8080 HTTPS_PROXY=http://127.0.0.1:8080
    export http_proxy=http://127.0.0.1:8080  HTTP_PROXY=http://127.0.0.1:8080
    export no_proxy= NO_PROXY=
    export GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1

    echo "configuring runner..."
    ./config.sh --unattended \
        --url "$config_url" \
        --token "$config_token" \
        --name "$config_name" \
        --labels mitm \
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
    if ! setup_runner "https://github.com/$GITHUB_OWNER/$GITHUB_REPO" "$GITHUB_RUNNER_TOKEN" "mitm-official"; then
        STATUS="config_failed"
    fi
elif [ "$BACKEND" = "runner-server" ]; then
    RS_URL=$(cat "$CACHE/runner-server.url")
    RS_TOKEN=$(cat "$CACHE/runner-server.token")
    if ! setup_runner "$RS_URL" "$RS_TOKEN" "mitm-runner-server"; then
        STATUS="config_failed"
    fi
elif [ "$BACKEND" = "aksh" ]; then
    AKSH_URL=$(cat "$CACHE/aksh.url")
    AKSH_TOKEN=$(cat "$CACHE/aksh.token")
    if ! setup_runner "$AKSH_URL" "$AKSH_TOKEN" "mitm-aksh"; then
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
    for i in $(seq 1 30); do
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

echo "stopping mitmdump..."
kill -INT "$MITM_PID" 2>/dev/null || true
wait "$MITM_PID" 2>/dev/null || true

ENDED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
FLOW_COUNT=$(wc -l < "$CAPTURE_DIR/flows.jsonl" 2>/dev/null || echo 0)

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
