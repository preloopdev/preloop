#!/usr/bin/env bash
# conformance-test.sh — Run local webhook conformance against aksh server.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR; kill %1 2>/dev/null || true" EXIT

echo "=== aksh Webhook Conformance Test ==="
echo "Temp dir: $TEMP_DIR"
echo ""

# 1. Set up workspace with workflow fixtures
mkdir -p "$TEMP_DIR/workspace/.github/workflows"
cp "$PROJECT_ROOT/fixtures/webhook-conformance/"*.yml "$TEMP_DIR/workspace/.github/workflows/"
git -C "$TEMP_DIR/workspace" init --initial-branch=main --quiet
git -C "$TEMP_DIR/workspace" config user.email conformance@aksh.local
git -C "$TEMP_DIR/workspace" config user.name aksh-conformance
git -C "$TEMP_DIR/workspace" add .
git -C "$TEMP_DIR/workspace" commit --quiet -m "webhook conformance fixtures"
git -C "$TEMP_DIR/workspace" tag v1.0.0
echo "Copied $(ls "$TEMP_DIR/workspace/.github/workflows/" | wc -l) workflow fixtures"
echo ""

# 2. Start aksh server with the same signed-webhook configuration used below.
echo "Starting aksh server..."
WEBHOOK_SECRET="conformance-test-secret"
TEST_API_TOKEN="conformance-test-api-token"
AKSH_LOCAL_WORKSPACE="$TEMP_DIR/workspace" \
AKSH_WEBHOOK_SECRET="$WEBHOOK_SECRET" \
"$PROJECT_ROOT/target/debug/aksh-runner-server" serve \
  --listen 127.0.0.1:9199 \
  --state-dir "$TEMP_DIR/server-state" \
  --enable-test-api \
  --test-api-token "$TEST_API_TOKEN" \
  > "$TEMP_DIR/server.log" 2>&1 &
SERVER_PID=$!

# Wait for server to be ready
for i in $(seq 1 30); do
  if curl -s http://127.0.0.1:9199/healthz > /dev/null 2>&1; then
    echo "Server ready (PID $SERVER_PID)"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "ERROR: Server failed to start"
    cat "$TEMP_DIR/server.log"
    exit 1
  fi
  sleep 0.5
done
echo ""

# 3. Run conformance for all 26 events
PASS=0
FAIL=0
RESULTS=""

payload_for() {
  case "$1" in
    push) echo '{"ref":"refs/heads/main","after":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","repository":{"full_name":"test/ci","default_branch":"main"},"commits":[{"message":"test commit"}]}' ;;
    pull_request) echo '{"action":"opened","number":1,"pull_request":{"number":1,"base":{"ref":"main","sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},"head":{"ref":"feature/x","sha":"cccccccccccccccccccccccccccccccccccccccc","repo":{"fork":false}},"merge_commit_sha":"dddddddddddddddddddddddddddddddddddddddd"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    pull_request_target) echo '{"action":"opened","number":2,"pull_request":{"number":2,"base":{"ref":"main","sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},"head":{"ref":"fork/x","sha":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","repo":{"fork":true}},"merge_commit_sha":"ffffffffffffffffffffffffffffffffffffffff"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    pull_request_review) echo '{"action":"submitted","pull_request":{"number":1,"head":{"sha":"cccccccccccccccccccccccccccccccccccccccc"},"merge_commit_sha":"dddddddddddddddddddddddddddddddddddddddd"},"review":{"state":"approved"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    workflow_dispatch) echo '{"inputs":{"name":"conformance-test"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    workflow_run) echo '{"action":"requested","workflow_run":{"head_branch":"main","head_sha":"cccccccccccccccccccccccccccccccccccccccc","event":"push","path":".github/workflows/webhook-push.yml","name":"Webhook Push"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    repository_dispatch) echo '{"action":"test-trigger","client_payload":{"key":"value"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    issues) echo '{"action":"opened","issue":{"number":1,"title":"Test issue"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    issue_comment) echo '{"action":"created","issue":{"number":1},"comment":{"body":"test comment"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    discussion) echo '{"action":"created","discussion":{"number":1,"title":"Test discussion"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    discussion_comment) echo '{"action":"created","discussion":{"number":1},"comment":{"body":"test comment"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    label) echo '{"action":"created","label":{"name":"bug"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    milestone) echo '{"action":"created","milestone":{"title":"v1.0"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    create) echo '{"ref_type":"branch","ref":"main","repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    watch) echo '{"action":"started","repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    fork) echo '{"action":"created","forkee":{"full_name":"other/ci"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    member) echo '{"action":"added","member":{"login":"testuser"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    public) echo '{"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    release) echo '{"action":"published","release":{"tag_name":"v1.0.0","name":"Version 1.0"},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    delete) echo '{"ref_type":"branch","ref":"feature/old-feat","repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    deployment) echo '{"action":"created","deployment":{"id":1},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    deployment_status) echo '{"deployment_status":{"state":"success","id":1},"deployment":{"id":1},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    gollum) echo '{"pages":[{"page_name":"Home","action":"edited"}],"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    page_build) echo '{"action":"built","build":{"id":1},"repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    schedule) echo '{"schedule":"0 0 * * *","repository":{"full_name":"test/ci","default_branch":"main"}}' ;;
    *) return 1 ;;
  esac
}

# Compute HMAC for webhook payloads.
compute_sig() {
  echo -n "$1" | python3 -c "
import sys, hmac, hashlib
body = sys.stdin.buffer.read()
sig = hmac.new(b'$WEBHOOK_SECRET', body, hashlib.sha256).hexdigest()
print(f'sha256={sig}')
"
}

# The server needs the webhook secret configured. For test_api mode,
# we POST directly to /api/v1/runs instead of the webhook endpoint.
# This bypasses signature verification.

for event in push pull_request pull_request_target pull_request_review \
             workflow_dispatch workflow_run repository_dispatch \
             issues issue_comment discussion discussion_comment \
             label milestone watch fork member public \
             release create delete deployment deployment_status \
             gollum page_build schedule; do
  
  payload=$(payload_for "$event")
  
  response=$(curl -s -w "\n%{http_code}" \
    -X POST \
    -H "Authorization: Bearer $TEST_API_TOKEN" \
    -H "Content-Type: application/json" \
    -H "X-GitHub-Event: $event" \
    -d "$payload" \
    "http://127.0.0.1:9199/runner/server/_apis/pipelines/workflows?api-version=6.0-preview" 2>/dev/null || echo -e "\n000")
  
  http_code=$(echo "$response" | tail -1)
  
  # Also try the native webhook endpoint
  sig=$(compute_sig "$payload")
  wh_response=$(curl -s -w "\n%{http_code}" \
    -X POST \
    -H "Content-Type: application/json" \
    -H "X-GitHub-Event: $event" \
    -H "X-Hub-Signature-256: $sig" \
    -d "$payload" \
    "http://127.0.0.1:9199/api/v1/github/webhooks" 2>/dev/null || echo -e "\n000")
  
  wh_code=$(echo "$wh_response" | tail -1)
  wh_body=$(echo "$wh_response" | sed '$d')
  
  # Check if runs were triggered
  runs_count=$(echo "$wh_body" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo "0")
  
  if [ "$wh_code" = "200" ] && [ "$runs_count" != "0" ]; then
    echo "  PASS $event: $runs_count run(s) triggered"
    PASS=$((PASS + 1))
  elif [ "$wh_code" = "200" ]; then
    echo "  FAIL $event: expected at least one matching fixture run, got 0"
    FAIL=$((FAIL + 1))
  else
    echo "  FAIL $event: HTTP $wh_code, runs=$runs_count"
    FAIL=$((FAIL + 1))
  fi
  
done

echo ""
echo "=== Results ==="
echo "Passed: $PASS"
echo "Failed: $FAIL"
echo ""

# Each signed webhook response above is the authoritative run-count assertion.
# The native API exposes individual runs by ID rather than a global run-list endpoint.

echo ""
echo "Server log (last 20 lines):"
tail -20 "$TEMP_DIR/server.log" 2>/dev/null || echo "(no log)"

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi

echo ""
echo "Done. Killing server..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true
