#!/usr/bin/env bash
# oidc-conformance-run.sh — 4 OIDC conformance tests through aksh-runner.
# Each test: submit workflow → runner picks up job → step curls OIDC URL →
# step runs a Python verifier → server captures step log with PASS/FAIL.
set -uo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
SERVER="$REPO/target/release/aksh-runner-server"
RUNNER="$REPO/target/release/aksh-runner"
PORT=9192
BASE="http://127.0.0.1:$PORT"
NOW=$(date -u +%Y-%m-%dT%H-%M-%SZ)
STATE=$(mktemp -d /tmp/aksh-oidc.XXXXXX)
RESULTS="$REPO/benchmarks/real-world/results/runner-flow"

RED='\033[1;31m'; GREEN='\033[1;32m'; BLUE='\033[1;34m'; NC='\033[0m'
PASS=0; FAIL=0

cleanup() { kill "$SPID" 2>/dev/null; wait "$SPID" 2>/dev/null; rm -rf "$STATE"; }
trap cleanup EXIT

# ── Python verifiers ─────────────────────────────────────────────────
cat > /tmp/verify-push.py << 'PYEOF'
import sys,json,base64
t=json.load(sys.stdin)['value'];c=json.loads(base64.urlsafe_b64decode(t.split('.')[1]+'=='))
assert c['ref_type']=='branch',f'ref_type={c["ref_type"]}'
assert c['ref']=='refs/heads/main',f'ref={c["ref"]}'
assert 'repo:acme/app:ref:refs/heads/main'==c['sub'],f'sub={c["sub"]}'
assert c['iss']=='https://token.actions.githubusercontent.com'
assert c['aud']=='sts.amazonaws.com',f'aud={c["aud"]}'
print(f'PASS: sub={c["sub"]} aud={c["aud"]} ref_type={c["ref_type"]}')
PYEOF

cat > /tmp/verify-tag.py << 'PYEOF'
import sys,json,base64
t=json.load(sys.stdin)['value'];c=json.loads(base64.urlsafe_b64decode(t.split('.')[1]+'=='))
assert c['ref_type']=='tag',f'ref_type={c["ref_type"]}'
assert ':ref:refs/tags/' in c['sub'],f'sub={c["sub"]}'
assert c['ref']=='refs/tags/v2.0.1',f'ref={c["ref"]}'
print(f'PASS: sub={c["sub"]} ref_type={c["ref_type"]} ref={c["ref"]}')
PYEOF

cat > /tmp/verify-pr.py << 'PYEOF'
import sys,json,base64
t=json.load(sys.stdin)['value'];c=json.loads(base64.urlsafe_b64decode(t.split('.')[1]+'=='))
assert c['event_name']=='pull_request',f'event={c["event_name"]}'
assert c['sub'].endswith(':pull_request'),f'sub={c["sub"]}'
assert ':ref:' not in c['sub'],f'sub={c["sub"]}'
print(f'PASS: sub={c["sub"]} event={c["event_name"]}')
PYEOF

cat > /tmp/verify-deploy.py << 'PYEOF'
import sys,json,base64
t=json.load(sys.stdin)['value'];c=json.loads(base64.urlsafe_b64decode(t.split('.')[1]+'=='))
assert ':environment:' in c['sub'],f'sub={c["sub"]}'
assert c['event_name']=='deployment',f'event={c["event_name"]}'
print(f'PASS: sub={c["sub"]} event={c["event_name"]}')
PYEOF

# ── Start server ────────────────────────────────────────────────────
echo -e "${BLUE}▸${NC} Starting server on $PORT..."
AKSH_PUBLIC_URL="$BASE" "$SERVER" serve --listen "127.0.0.1:$PORT" --state-dir "$STATE" \
    > "$STATE/server.log" 2>&1 &
SPID=$!
for i in $(seq 1 30); do curl -sf --max-time 1 "$BASE/healthz" >/dev/null 2>&1 && break; sleep 0.5; done
KID=$(curl -sf "$BASE/.well-known/jwks.json" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['keys'][0]['kid'][:16])" 2>/dev/null || echo '?')
echo -e "${GREEN}✓${NC} Server ready  JWKS kid=$KID"
sleep 1

# ── Run one test ────────────────────────────────────────────────────
run_test() {
    local name="$1" verifier="$2" event="$3" ref="$4" repo="$5" aud="$6" yaml_body="$7"
    echo -e "\n${BLUE}══════════${NC} $name ${BLUE}══════════${NC}"

    # Build and submit workflow
    local body_file="$STATE/body-$name.json"
    python3 -c "
import json
wf = '''$yaml_body'''
json.dump({'workflow_yaml':wf,'event':'$event','git_ref':'$ref','repository':'$repo'}, open('$body_file','w'))
"

    local before_ts resp run_id
    before_ts=$(date +%s)
    resp=$(curl -sf -X POST "$BASE/api/v1/runs" -H "Content-Type: application/json" -d "@$body_file")
    run_id=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])" 2>/dev/null) || {
        echo -e "  ${RED}✗${NC} submit failed: $resp"; FAIL=$((FAIL+1)); return
    }
    echo "  run_id=$run_id"

    # Configure + run
    local rd=$(mktemp -d /tmp/aksh-runner.XXXXXX)
    "$RUNNER" --runner-root "$rd" configure \
        --url "$BASE" --token t --name "$name" \
        --unattended --replace --ephemeral --labels "self-hosted" --no-externals \
        > /dev/null 2>&1

    local log="$RESULTS/$name/aksh/$NOW/runner.log"
    mkdir -p "$(dirname "$log")"
    { echo "=== $name ==="; echo "run_id=$run_id  event=$event  ref=$ref  repo=$repo"; echo ""; } > "$log"
    "$RUNNER" --runner-root "$rd" run --once >> "$log" 2>&1 || true

    # Find step log
    local status step_log oidc
    status=$(curl -sf "$BASE/api/v1/runs/$run_id" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo '?')
    step_log=$(find "$STATE/replay/results" -name "step-*.txt" -newermt "@$before_ts" 2>/dev/null | sort | tail -1)

    if [[ -z "$step_log" ]]; then
        echo -e "  ${RED}✗${NC} No step log   status=$status"
        FAIL=$((FAIL+1)); rm -rf "$rd"; return
    fi

    oidc=$(grep "PASS:" "$step_log" 2>/dev/null || true)
    if [[ -n "$oidc" ]]; then
        echo "  $oidc"
        echo -e "  ${GREEN}PASS${NC}  status=$status  step=$(basename "$step_log")"
        PASS=$((PASS+1))
    else
        echo -e "  ${RED}FAIL${NC}  status=$status"
        cat "$step_log" | tail -5 | sed 's/^/  /'
        FAIL=$((FAIL+1))
    fi
    rm -rf "$rd"
}

# ══════════════════════════════════════════════════════════════════════
# Test 1: Push → branch ref
# ══════════════════════════════════════════════════════════════════════
run_test "oidc-push" "/tmp/verify-push.py" "push" "refs/heads/main" "acme/app" "sts.amazonaws.com" \
'name: oidc-push
on: push
permissions:
  id-token: write
  contents: read
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: |
          URL="${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=sts.amazonaws.com"
          python3 /tmp/verify-push.py <<< "$(curl -sf "$URL" -H "Authorization: Bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN")"'

# Test 2: Tag push
run_test "oidc-tag" "/tmp/verify-tag.py" "push" "refs/tags/v2.0.1" "acme/app" "api://vault" \
'name: oidc-tag
on:
  push:
    tags: ["v*"]
permissions:
  id-token: write
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - run: |
          URL="${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=api://vault"
          python3 /tmp/verify-tag.py <<< "$(curl -sf "$URL" -H "Authorization: Bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN")"'

# Test 3: Pull request
run_test "oidc-pr" "/tmp/verify-pr.py" "pull_request" "refs/pull/42/merge" "acme/app" "pr-aud" \
'name: oidc-pr
on:
  pull_request:
    branches: [main]
permissions:
  id-token: write
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: |
          URL="${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=pr-aud"
          python3 /tmp/verify-pr.py <<< "$(curl -sf "$URL" -H "Authorization: Bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN")"'

# Test 4: Deployment environment
run_test "oidc-deploy" "/tmp/verify-deploy.py" "deployment" "refs/heads/main" "acme/app" "vault/hcp" \
'name: oidc-deploy
on: deployment
jobs:
  deploy-staging:
    runs-on: ubuntu-latest
    permissions:
      id-token: write
    steps:
      - run: |
          URL="${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=vault/hcp"
          python3 /tmp/verify-deploy.py <<< "$(curl -sf "$URL" -H "Authorization: Bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN")"'

# ══════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════"
echo -e "${GREEN}$PASS passed${NC}  ${RED}$FAIL failed${NC}"
echo "Results: $RESULTS/*/aksh/$NOW/"
exit $FAIL
