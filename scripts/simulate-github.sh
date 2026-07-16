#!/usr/bin/env bash
# simulate-github.sh — Simulate GitHub webhook events against a local aksh server.
#
# Usage:
#   ./scripts/simulate-github.sh <event> [workflow-file] [payload-json]
#
# Examples:
#   ./scripts/simulate-github.sh push fixtures/workflows/dogfood.yml
#   ./scripts/simulate-github.sh pull_request fixtures/pr-opened.json
#   ./scripts/simulate-github.sh issues
#
# The script creates a minimal valid payload for each event and POSTs it
# to the local aksh server webhook endpoint.

set -euo pipefail

AKSH_URL="${AKSH_URL:-http://127.0.0.1:8080}"
WEBHOOK_SECRET="${AKSH_WEBHOOK_SECRET:-dev-secret}"
PAYLOADS_DIR="${PAYLOADS_DIR:-experiments/mitm/scenarios}"

event="${1:-}"
workflow_file="${2:-}"
payload_json="${3:-}"

if [ -z "$event" ]; then
    echo "Usage: $0 <event> [workflow-file] [payload-json]"
    echo ""
    echo "Supported events:"
    echo "  push pull_request pull_request_target pull_request_review"
    echo "  workflow_dispatch workflow_run repository_dispatch"
    echo "  create delete release"
    echo "  issues issue_comment discussion discussion_comment"
    echo "  label milestone watch fork"
    echo "  deployment deployment_status member public"
    echo "  gollum page_build schedule"
    echo ""
    echo "Environment:"
    echo "  AKSH_URL           server URL (default: http://127.0.0.1:8080)"
    echo "  AKSH_WEBHOOK_SECRET  webhook secret (default: dev-secret)"
    echo "  PAYLOADS_DIR       captured payloads dir (default: experiments/mitm/scenarios)"
    exit 1
fi

# --- Payload resolution ---

# If a specific JSON file was given, use it
if [ -n "$payload_json" ] && [ -f "$payload_json" ]; then
    body=$(cat "$payload_json")
# Otherwise, check for a captured payload for this event
elif [ -f "$PAYLOADS_DIR/event-${event}/payload.json" ]; then
    echo "Using captured payload: $PAYLOADS_DIR/event-${event}/payload.json"
    body=$(cat "$PAYLOADS_DIR/event-${event}/payload.json")
else
    # Generate a minimal payload based on event type
    echo "No payload found, generating minimal $event payload..."
    case "$event" in
    push)
        body=$(cat <<'PAYLOAD'
{
  "ref": "refs/heads/main",
  "after": "0000000000000000000000000000000000000000",
  "repository": {
    "full_name": "local/repo",
    "default_branch": "main"
  },
  "commits": [],
  "head_commit": {"message": "simulated push"}
}
PAYLOAD
)
        ;;
    pull_request)
        body=$(cat <<'PAYLOAD'
{
  "action": "opened",
  "number": 1,
  "pull_request": {
    "number": 1,
    "base": {"ref": "main", "sha": "base-sha"},
    "head": {"ref": "feature/x", "sha": "head-sha-head-sha-head-sha-head0000", "repo": {"fork": false}},
    "merge_commit_sha": "merge-merge-merge-merge-merge-merge0000"
  },
  "repository": {
    "full_name": "local/repo",
    "default_branch": "main"
  }
}
PAYLOAD
)
        ;;
    pull_request_target)
        body=$(cat <<'PAYLOAD'
{
  "action": "opened",
  "number": 1,
  "pull_request": {
    "number": 1,
    "base": {"ref": "main", "sha": "base-sha"},
    "head": {"ref": "feature/x", "sha": "head-sha-head-sha-head-sha-head0000", "repo": {"fork": false}}
  },
  "repository": {
    "full_name": "local/repo",
    "default_branch": "main"
  }
}
PAYLOAD
)
        ;;
    issues|issue_comment|discussion|discussion_comment|label|milestone|fork|watch|member|public|page_build|repository_dispatch)
        body="{\"action\":\"created\",\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    pull_request_review)
        body="{\"action\":\"submitted\",\"pull_request\":{\"number\":1,\"head\":{\"sha\":\"head-sha-head-sha-head-sha-head0000\"},\"merge_commit_sha\":\"merge-merge-merge-merge-merge-merge0000\"},\"review\":{\"state\":\"approved\"},\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    workflow_dispatch)
        body="{\"action\":\"\",\"inputs\":{},\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    workflow_run)
        body="{\"action\":\"requested\",\"workflow_run\":{\"head_branch\":\"main\",\"head_sha\":\"head-sha-head-sha-head-sha-head0000\",\"event\":\"push\",\"path\":\".github/workflows/ci.yml\"},\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    create)
        body="{\"ref_type\":\"branch\",\"ref\":\"feature/new-branch\",\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    delete)
        body="{\"ref_type\":\"branch\",\"ref\":\"feature/old-branch\",\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    release)
        body="{\"action\":\"published\",\"release\":{\"tag_name\":\"v1.0.0\"},\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    deployment)
        body="{\"action\":\"created\",\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    deployment_status)
        body="{\"deployment_status\":{\"state\":\"success\"},\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    gollum)
        body="{\"pages\":[{\"page_name\":\"Home\",\"action\":\"edited\"}],\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    schedule)
        body="{\"schedule\":\"* * * * *\",\"repository\":{\"full_name\":\"local/repo\",\"default_branch\":\"main\"}}"
        ;;
    *)
        echo "Unknown event: $event"
        echo "See usage for supported events."
        exit 1
        ;;
    esac
fi

# --- Compute HMAC-SHA256 signature ---
# Using Python for HMAC since it's universally available
sig_hex=$(echo -n "$body" | python3 -c "
import sys, hmac, hashlib
body = sys.stdin.buffer.read()
secret = sys.argv[1].encode()
sig = hmac.new(secret, body, hashlib.sha256).hexdigest()
print(f'sha256={sig}')
" "$WEBHOOK_SECRET")

# --- POST to aksh webhook endpoint ---
echo "POSTing $event to $AKSH_URL/api/v1/github/webhooks ..."
response=$(curl -s -w "\n%{http_code}" \
    -X POST \
    -H "Content-Type: application/json" \
    -H "X-GitHub-Event: $event" \
    -H "X-Hub-Signature-256: $sig_hex" \
    -d "$body" \
    "$AKSH_URL/api/v1/github/webhooks")

# Split response body and status code
http_code=$(echo "$response" | tail -1)
response_body=$(echo "$response" | sed '$d')

echo ""
echo "HTTP Status: $http_code"
echo "Response:"
echo "$response_body" | python3 -m json.tool 2>/dev/null || echo "$response_body"

if [ "$http_code" = "200" ]; then
    echo ""
    echo "Success! Check the server logs for workflow execution details."
else
    echo ""
    echo "Request failed with status $http_code"
    exit 1
fi
