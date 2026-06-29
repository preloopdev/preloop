#!/usr/bin/env bash
#
# e2e-start.sh — start aksh for E2E testing with the real runner.
#
# Verifies port redirect is active, starts aksh on 9090, submits a workflow,
# and runs the official runner against it. Captures all output for debugging.
#
# Usage:
#   ./scripts/e2e-start.sh                    # full E2E run
#   ./scripts/e2e-start.sh --skip-runner      # start aksh only (for manual testing)
#   ./scripts/e2e-start.sh --log              # show last E2E log

set -euo pipefail

# The runner's mitm work sets proxy env vars; they hijack our curls to aksh.
unset all_proxy ALL_PROXY http_proxy https_proxy HTTP_PROXY HTTPS_PROXY no_proxy NO_PROXY 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
AKSH_STATE="${AKSH_STATE:-$HOME/mitm-proxy/experiments/mitm/.cache/aksh-state}"
RUNNER_DIR="${RUNNER_DIR:-$HOME/mitm-proxy/experiments/mitm/.cache/runner-official}"
AKSH_PORT="${AKSH_PORT:-9090}"
# aksh binds 9090, but clients (and the runner) reach it via the port-80 redirect.
# Direct connections to 9090 are broken by the pf rdr rule's reverse NAT on lo0;
# only the redirected 80→9090 path has correct bidirectional pf state. So every
# client request below MUST go through port 80, exactly like the real runner.
CLIENT="${CLIENT:-http://127.0.0.1:80}"
LOG_DIR="$REPO_ROOT/logs/e2e"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/e2e-$TIMESTAMP.log"

mkdir -p "$LOG_DIR"

# ── helpers ──────────────────────────────────────────────────────────────────

red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
dim()   { printf '\033[2m%s\033[0m\n' "$*"; }
info()  { printf '\033[1;34m▸ %s\033[0m\n' "$*"; }

# Extract a JSON field from stdin; empty output + exit 1 on parse failure.
json_field() {
    python3 -c "import json,sys
try:
    print(json.load(sys.stdin)['$1'])
except Exception:
    sys.exit(1)"
}

cleanup() {
    # Kill processes started by this script. Avoid broad `pkill -f` patterns
    # that could match unrelated runners/servers on the same host.
    if [[ -n "${AKSH_PID:-}" ]]; then
        kill "$AKSH_PID" 2>/dev/null || true
    fi
    # Reap any remaining direct children (e.g. perl/run.sh) of this shell.
    pkill -P $$ 2>/dev/null || true
}
trap cleanup EXIT

# ── preflight ────────────────────────────────────────────────────────────────

preflight() {
    info "Preflight checks..."

    # Check aksh binary
    local aksh_bin="$HOME/rust-runner-server/target/release/aksh-runner-server"
    if [ ! -f "$aksh_bin" ]; then
        red "aksh binary not found. Build it: cd ~/rust-runner-server && cargo build --release"
        exit 1
    fi
    dim "  aksh binary: $aksh_bin"

    # Check runner binary
    if [ ! -f "$RUNNER_DIR/run.sh" ]; then
        red "Runner not found at $RUNNER_DIR"
        exit 1
    fi
    dim "  runner: $RUNNER_DIR"

    # Check port redirect
    if "$SCRIPT_DIR/e2e-setup.sh" --status 2>/dev/null; then
        green "  port redirect: active"
    else
        red "  port redirect: not active"
        info "Run: $SCRIPT_DIR/e2e-setup.sh"
        exit 1
    fi

    # Check port 9090 is free
    if lsof -i :"$AKSH_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
        red "  port $AKSH_PORT is already in use"
        lsof -i :"$AKSH_PORT" -sTCP:LISTEN 2>/dev/null | head -3
        exit 1
    fi
    dim "  port $AKSH_PORT: free"

    green "Preflight OK"
}

# ── start aksh ───────────────────────────────────────────────────────────────

start_aksh() {
    info "Starting aksh on 127.0.0.1:$AKSH_PORT..."
    mkdir -p "$AKSH_STATE"

    "$HOME/rust-runner-server/target/release/aksh-runner-server" serve \
        --listen "127.0.0.1:$AKSH_PORT" \
        --state-dir "$AKSH_STATE" \
        >> "$LOG_FILE" 2>&1 &
    AKSH_PID=$!
    dim "  aksh PID: $AKSH_PID"

    # Wait for aksh to be ready — probe via the port-80 redirect (the working path)
    local retries=0
    while ! curl -sf --max-time 1 "$CLIENT/_apis/connectionData?connectOptions=0&lastChangeId=0&lastChangeId64=0" >/dev/null 2>&1; do
        retries=$((retries + 1))
        if [ $retries -gt 20 ]; then
            red "aksh failed to start (timeout)"
            cat "$LOG_FILE" | tail -20
            exit 1
        fi
        sleep 0.5
    done
    green "aksh ready"
}

# ── configure runner ─────────────────────────────────────────────────────────

configure_runner() {
    info "Configuring runner..."

    cd "$RUNNER_DIR"

    # Clear any prior local config (don't call `config.sh remove` — it contacts
    # aksh's unregister endpoint, which we don't need and which can hang).
    rm -f .runner .credentials .credentials_rsaparams 2>/dev/null || true

    # Get registration token. `|| resp=""` stops set -e from killing us silently
    # on a connection error; json_field then reports the empty/bad response.
    local resp token
    resp=$(curl -s --max-time 5 \
        -X POST "$CLIENT/api/v3/repos/owner/repo/actions/runners/registration-token" \
        -H "Content-Type: application/json" \
        -H "Authorization: RemoteAuth test" \
        -d '{}') || resp=""
    if ! token=$(printf '%s' "$resp" | json_field token); then
        red "Failed to get registration token. Server response:"
        printf '  %s\n' "$resp"
        exit 1
    fi

    # Configure with pfctl redirect (port 80 goes to 9090)
    ./config.sh --unattended \
        --url "http://127.0.0.1/runner/server" \
        --token "$token" \
        --name "e2e-$(date +%s)" \
        --labels "mitm,self-hosted" \
        --work _work \
        --replace >> "$LOG_FILE" 2>&1

    if [ ! -f .runner ]; then
        red "Runner configuration failed"
        cat "$LOG_FILE" | tail -10
        exit 1
    fi
    green "Runner configured"
}

# ── submit workflow ──────────────────────────────────────────────────────────

submit_workflow() {
    info "Submitting workflow..."
    local response
    response=$(curl -s --max-time 10 \
        -X POST "$CLIENT/api/v1/runs" \
        -H "Content-Type: application/json" \
        -d '{
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello from aksh\n      - run: whoami\n      - run: date\n",
            "event": "push",
            "repository": "owner/repo"
        }') || response=""
    if ! RUN_ID=$(printf '%s' "$response" | json_field run_id); then
        red "Failed to submit workflow. Server response:"
        printf '  %s\n' "$response"
        exit 1
    fi
    green "Run submitted: $RUN_ID"
}

# ── run the runner ───────────────────────────────────────────────────────────

run_runner() {
    info "Running runner against aksh..."
    cd "$RUNNER_DIR"

    # `timeout` is not available on macOS without coreutils; use perl as a portable substitute.
    perl -e 'alarm 90; exec @ARGV' -- ./run.sh 2>&1 | tee -a "$LOG_FILE" | grep -E \
        "Listening|Job|Step|completed|error|Error|Failed|Succeeded|Worker" | head -50 || true

    # Check result
    if [ -n "${RUN_ID:-}" ]; then
        local status
        status=$(curl -sf --max-time 5 "$CLIENT/api/v1/runs/$RUN_ID" 2>/dev/null \
            | python3 -c "import json,sys;d=json.load(sys.stdin);print(d.get('status','unknown'))" 2>/dev/null || echo "unknown")
        info "Run status: $status"
    fi
}

# ── show last log ────────────────────────────────────────────────────────────

show_log() {
    local latest
    latest=$(ls -t "$LOG_DIR"/e2e-*.log 2>/dev/null | head -1)
    if [ -n "$latest" ]; then
        info "Latest log: $latest"
        cat "$latest"
    else
        red "No E2E logs found"
    fi
}

# ── dispatch ─────────────────────────────────────────────────────────────────

case "${1:-}" in
    -h|--help)
        cat <<EOF
Usage: $0 [OPTION]

Options:
  (none)            Full E2E: start aksh, configure runner, submit workflow, run
  --skip-runner     Start aksh only (for manual testing)
  --log             Show the latest E2E log
  -h, --help        Show this help

Environment:
  AKSH_STATE=~/mitm-proxy/experiments/mitm/.cache/aksh-state
  RUNNER_DIR=~/mitm-proxy/experiments/mitm/.cache/runner-official
  AKSH_PORT=9090
EOF
        exit 0
        ;;
    --log)
        show_log
        exit 0
        ;;
    --skip-runner)
        preflight
        start_aksh
        info "aksh running. Press Ctrl+C to stop."
        wait $AKSH_PID
        ;;
    *)
        preflight
        start_aksh
        configure_runner
        submit_workflow
        run_runner
        green "E2E complete. Log: $LOG_FILE"
        ;;
esac
