#!/usr/bin/env bash
# oidc-conformance.sh — Run OIDC conformance workflows against aksh server.
# Validates 4 scenarios: push, tag, PR, deployment-environment.
set -euo pipefail

RED='\033[1;31m'; GREEN='\033[1;32m'; BLUE='\033[1;34m'; NC='\033[0m'
PASS=0; FAIL=0
pass() { echo -e "${GREEN}PASS${NC} $1"; PASS=$((PASS+1)); }
fail() { echo -e "${RED}FAIL${NC} $1 — $2"; FAIL=$((FAIL+1)); }

REPO="$(cd "$(dirname "$0")/.." && pwd)"
SERVER="$REPO/target/release/aksh-runner-server"
STATE_DIR=$(mktemp -d /tmp/aksh-oidc.XXXXXX)
PORT=9191
BASE="http://127.0.0.1:$PORT"

cleanup() { kill "${SERVER_PID:-}" 2>/dev/null; rm -rf "$STATE_DIR"; }
trap cleanup EXIT

# ── Start server ────────────────────────────────────────────────────
echo -e "${BLUE}▸${NC} Starting aksh on $PORT..."
"$SERVER" serve --listen "127.0.0.1:$PORT" --state-dir "$STATE_DIR" > "$STATE_DIR/server.log" 2>&1 &
SERVER_PID=$!
for i in $(seq 1 30); do
    curl -sf --max-time 1 "$BASE/_apis/connectionData?connectOptions=0&lastChangeId=0&lastChangeId64=0" >/dev/null 2>&1 && break
    sleep 0.5
done
echo -e "${GREEN}✓${NC} Server ready"

# ── Helper: json-encode a workflow YAML body ─────────────────────────
submit_workflow() {
    local name="$1" yaml_file="$2" event="$3" ref="$4" repo="$5"
    python3 -c "
import json, sys
yaml = open('$yaml_file').read()
print(json.dumps({
    'workflow_yaml': yaml,
    'event': '$event',
    'git_ref': '$ref',
    'repository': '$repo'
}))
"
}

# ── Helper: get plan_id from queued job ──────────────────────────────
get_plan_and_job() {
    # We'll poll the broker acquire path. Register a runner first.
    local rid=$1
    # acquirejob returns the job message with plan.planId
    curl -sf --max-time 10 \
        -H "Authorization: Bearer aksh-system-token" \
        -H "Content-Type: application/json" \
        -X POST "$BASE/broker/$rid/acquirejob" \
        -d '{"jobMessageType":"PipelineAgentJobRequest","jobId":"00000000-0000-0000-0000-000000000000"}' 2>/dev/null
}

# ── Run one OIDC test ────────────────────────────────────────────────
run_test() {
    local test_name="$1" yaml_file="$2" event="$3" ref="$4" repo="$5" aud="$6"
    local extra_check="$7"

    echo -e "\n${BLUE}▸${NC} $test_name"

    # Build workflow YAML file content
    local wf_json
    wf_json=$(python3 -c "
import json
y = open('$yaml_file').read()
print(json.dumps({'workflow_yaml':y,'event':'$event','git_ref':'$ref','repository':'$repo'}))
")

    # Submit
    local submit_resp
    submit_resp=$(curl -sf --max-time 10 -X POST "$BASE/api/v1/runs" \
        -H "Content-Type: application/json" -d "$wf_json")
    local run_id
    run_id=$(echo "$submit_resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])")
    echo "  run_id=$run_id"

    # Register a runner and acquire the job to get plan_id
    local runner_reg
    runner_reg=$(curl -sf --max-time 5 -X POST "$BASE/api/v1/runners" \
        -H "Content-Type: application/json" -H "Authorization: Bearer aksh-system-token" \
        -d '{"name":"oidc-r","labels":["ubuntu-latest"]}')
    local runner_id
    runner_id=$(echo "$runner_reg" | python3 -c "import sys,json; print(json.load(sys.stdin)['runner_id'])")
    echo "  runner_id=$runner_id"

    # Wait briefly for job dispatch
    sleep 1

    # Acquire job — this returns the AgentJobRequestMessage
    local job_msg
    job_msg=$(curl -sf --max-time 10 \
        -H "Authorization: Bearer aksh-system-token" -H "Content-Type: application/json" \
        -X POST "$BASE/broker/$runner_id/acquirejob" \
        -d '{"jobMessageType":"PipelineAgentJobRequest","jobId":"00000000-0000-0000-0000-000000000000"}' 2>&1) || {
        fail "$test_name" "acquirejob failed: $job_msg"
        return
    }

    local plan_id job_id
    plan_id=$(echo "$job_msg" | python3 -c "import sys,json; print(json.load(sys.stdin)['plan']['planId'])")
    job_id=$(echo "$job_msg" | python3 -c "import sys,json; print(json.load(sys.stdin)['jobId'])")
    echo "  plan_id=$plan_id"

    # Request OIDC token
    local tok_resp
    tok_resp=$(curl -sf --max-time 10 \
        -H "Authorization: Bearer aksh-system-token" \
        "$BASE/runner/server/_apis/distributedtask/hubs/actions/plans/$plan_id/jobs/$job_id/oidctoken?audience=$aud" 2>&1) || {
        fail "$test_name" "oidctoken request failed: $tok_resp"
        return
    }

    # Decode JWT and validate
    local result
    result=$(echo "$tok_resp" | python3 -c "
import sys, json, base64
t = json.load(sys.stdin)['value']
parts = t.split('.')
assert len(parts)==3, 'JWT must have 3 parts'
h = json.loads(base64.urlsafe_b64decode(parts[0]+'=='))
c = json.loads(base64.urlsafe_b64decode(parts[1]+'=='))
assert h['alg']=='RS256', f'alg: {h[\"alg\"]}'
assert len(h['kid'])>0, 'missing kid'
assert c['iss']=='https://token.actions.githubusercontent.com', f'iss: {c[\"iss\"]}'
assert c['aud']=='$aud', f'aud: {c[\"aud\"]} wanted $aud'
assert c['exp']>c['iat'], 'exp <= iat'
assert c['exp']-c['iat']<=3600, 'TTL > 3600'
assert len(c['jti'])>0, 'missing jti'
$extra_check
print(f'OK  alg=RS256  kid={h[\"kid\"][:16]}...  sub={c[\"sub\"]}  aud={c[\"aud\"]}')
" 2>&1) || {
        fail "$test_name" "$result"
        return
    }
    echo "  $result"
    pass "$test_name"
}

# ══════════════════════════════════════════════════════════════════════

# Test 1: Basic push → branch ref, id-token: write at workflow level
cat > "$STATE_DIR/wf1.yml" << 'YAML'
name: oidc-push
on: push
permissions:
  id-token: write
  contents: read
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ok
YAML
run_test "push-branch" "$STATE_DIR/wf1.yml" "push" "refs/heads/main" "acme/app" "sts.amazonaws.com" \
    "assert c['ref_type']=='branch'; assert c['ref']=='refs/heads/main'; assert 'repo:acme/app:ref:refs/heads/main'==c['sub']"

# Test 2: Tag push → ref_type=tag, sub contains refs/tags/
cat > "$STATE_DIR/wf2.yml" << 'YAML'
name: oidc-tag
on:
  push:
    tags: ['v*']
permissions:
  id-token: write
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - run: echo release
YAML
run_test "tag-release" "$STATE_DIR/wf2.yml" "push" "refs/tags/v2.0.1" "acme/app" "api://vault" \
    "assert c['ref_type']=='tag'; assert ':ref:refs/tags/' in c['sub']; assert c['ref']=='refs/tags/v2.0.1'"

# Test 3: Pull request → sub ends with :pull_request, no :ref:
cat > "$STATE_DIR/wf3.yml" << 'YAML'
name: oidc-pr
on:
  pull_request:
    branches: [main]
permissions:
  id-token: write
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: echo pr
YAML
run_test "pull-request" "$STATE_DIR/wf3.yml" "pull_request" "refs/pull/42/merge" "acme/app" "pr-aud" \
    "assert c['event_name']=='pull_request'; assert c['sub'].endswith(':pull_request'); assert ':ref:' not in c['sub']"

# Test 4: Deployment environment → sub uses :environment:
cat > "$STATE_DIR/wf4.yml" << 'YAML'
name: oidc-deploy
on: deployment
jobs:
  deploy:
    runs-on: ubuntu-latest
    permissions:
      id-token: write
    steps:
      - run: echo deploy
YAML
run_test "deploy-env" "$STATE_DIR/wf4.yml" "deployment" "refs/heads/main" "acme/app" "vault/hcp" \
    "assert ':environment:' in c['sub']; assert c['event_name']=='deployment'"

# ══════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════"
echo -e "${GREEN}$PASS passed${NC}  ${RED}$FAIL failed${NC}"
echo "Server log: $STATE_DIR/server.log"
echo "OIDC key: $(cat "$STATE_DIR/oidc-key.json" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('kid','no-key'))" 2>/dev/null || echo 'no-key')"
exit $FAIL
