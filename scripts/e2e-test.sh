#!/usr/bin/env bash
# e2e-test.sh — run a workflow YAML against aksh server + runner
# Usage: ./scripts/e2e-test.sh <workflow.yml> [--verbose]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVER_PORT="${AKSH_PORT:-9191}"
SERVER_URL="http://127.0.0.1:${SERVER_PORT}"
RUNNER_DIR="/tmp/aksh-e2e-runner"
RUNNER_BIN="$REPO_ROOT/target/release/aksh-runner"
SERVER_BIN="$REPO_ROOT/target/release/aksh-runner-server"
VERBOSE="${2:-}"

red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
info()  { printf '\033[1;34m▸ %s\033[0m\n' "$*"; }

cleanup() {
    pkill -f "aksh-runner-server.*${SERVER_PORT}" 2>/dev/null || true
}

# Read workflow file
WORKFLOW_FILE="${1:?Usage: $0 <workflow.yml>}"
if [ ! -f "$WORKFLOW_FILE" ]; then
    red "Workflow file not found: $WORKFLOW_FILE"
    exit 1
fi
WORKFLOW_YAML=$(cat "$WORKFLOW_FILE")

# Start server
info "Starting server on port $SERVER_PORT..."
pkill -f "aksh-runner-server.*${SERVER_PORT}" 2>/dev/null || true
sleep 1
RUST_LOG=info AKSH_PUBLIC_URL="$SERVER_URL" "$SERVER_BIN" serve --listen "0.0.0.0:${SERVER_PORT}" > /tmp/aksh-e2e-server.log 2>&1 &
SERVER_PID=$!
sleep 2

if ! curl -s "$SERVER_URL/healthz" > /dev/null 2>&1; then
    red "Server failed to start"
    exit 1
fi
info "Server running (PID $SERVER_PID)"

# Configure runner
rm -rf "$RUNNER_DIR"
mkdir -p "$RUNNER_DIR"
cd "$RUNNER_DIR"

info "Configuring runner..."
"$RUNNER_BIN" configure \
  --url "$SERVER_URL" \
  --token aksh-system-token \
  --name e2e-runner \
  --labels self-hosted,macOS,ARM64 \
  --work _work \
  --ephemeral 2>&1 | grep -E "INFO|ERROR"

# Submit workflow
info "Submitting workflow..."
PAYLOAD=$(python3 -c "
import json, sys
yaml_content = open('$WORKFLOW_FILE').read()
print(json.dumps({
    'workflow_yaml': yaml_content,
    'event': 'push',
    'repository': 'test/repo',
    'git_ref': 'refs/heads/main',
    'vars': {'AKSH_REPO_ROOT': '$REPO_ROOT'}
}))
")

RESULT=$(curl -s -X POST "$SERVER_URL/api/v1/runs" \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer aksh-system-token' \
  -d "$PAYLOAD")
RUN_ID=$(echo "$RESULT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["run_id"])')
info "Run submitted: $RUN_ID"

# Run runner
info "Running runner..."
LOG_LEVEL="info"
if [ "$VERBOSE" = "--verbose" ]; then
    LOG_LEVEL="debug"
fi
RUST_LOG=$LOG_LEVEL timeout 120 "$RUNNER_BIN" run --once 2>&1

# Check result
FINAL=$(curl -s "$SERVER_URL/api/v1/runs/$RUN_ID" \
  -H 'Authorization: Bearer aksh-system-token')
STATUS=$(echo "$FINAL" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')

echo ""
if [ "$STATUS" = "success" ]; then
    green "✅ Run $RUN_ID: $STATUS"
else
    red "❌ Run $RUN_ID: $STATUS"
    echo "$FINAL" | python3 -m json.tool
fi

# Cleanup
cleanup
exit 0
