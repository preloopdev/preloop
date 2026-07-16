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
SYSTEM_TOKEN="${AKSH_SYSTEM_TOKEN:-aksh-system-token}"
NOW=$(date -u +%Y-%m-%dT%H-%M-%SZ)
STATE=$(mktemp -d /tmp/aksh-oidc.XXXXXX)
RESULTS="$REPO/benchmarks/real-world/results/runner-flow"

RED='\033[1;31m'; GREEN='\033[1;32m'; BLUE='\033[1;34m'; NC='\033[0m'
PASS=0; FAIL=0

cleanup() { kill "$SPID" 2>/dev/null; wait "$SPID" 2>/dev/null; rm -rf "$STATE"; }
trap cleanup EXIT

# ── Python verifiers ─────────────────────────────────────────────────
cat > "$STATE/aksh_oidc_verify.py" << PYEOF
import base64, hashlib, json, urllib.request

BASE = "$BASE"

def _decode(part):
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))

def verify_token(token):
    parts = token.split(".")
    assert len(parts) == 3, "JWT must have three parts"
    header = json.loads(_decode(parts[0]))
    claims = json.loads(_decode(parts[1]))
    assert header.get("alg") == "RS256", f"alg={header.get('alg')}"
    jwks = json.load(urllib.request.urlopen(BASE + "/oidc/.well-known/jwks.json"))
    key = next(k for k in jwks["keys"] if k.get("kid") == header.get("kid"))
    n = int.from_bytes(_decode(key["n"]), "big")
    e = int.from_bytes(_decode(key["e"]), "big")
    signature = int.from_bytes(_decode(parts[2]), "big")
    encoded = (parts[0] + "." + parts[1]).encode()
    digest_info = bytes.fromhex("3031300d060960864801650304020105000420") + hashlib.sha256(encoded).digest()
    recovered = pow(signature, e, n).to_bytes((n.bit_length() + 7) // 8, "big")
    assert recovered[:2] == b"\x00\x01", "invalid RS256 padding"
    separator = recovered.find(b"\x00", 2)
    assert separator >= 10 and all(byte == 0xff for byte in recovered[2:separator]), "invalid RS256 padding"
    assert recovered[separator + 1:] == digest_info, "invalid RS256 signature"
    assert claims["iss"] == BASE + "/oidc", f"iss={claims.get('iss')}"
    assert claims["exp"] > claims["iat"] >= claims["nbf"], "invalid token lifetime"
    return claims
PYEOF
cat > "$STATE/verify-push.py" << PYEOF
import sys,json,os; sys.path.insert(0,os.path.dirname(os.path.abspath(__file__))); from aksh_oidc_verify import verify_token
t=json.load(sys.stdin)['value'];c=verify_token(t)
assert c['ref_type']=='branch',f'ref_type={c["ref_type"]}'
assert c['ref']=='refs/heads/main',f'ref={c["ref"]}'
assert 'repo:acme/app:ref:refs/heads/main'==c['sub'],f'sub={c["sub"]}'
assert c['iss']=='http://127.0.0.1:9192/oidc'
assert c['aud']=='sts.amazonaws.com',f'aud={c["aud"]}'
print(f'PASS: sub={c["sub"]} aud={c["aud"]} ref_type={c["ref_type"]}')
PYEOF

cat > "$STATE/verify-tag.py" << PYEOF
import sys,json,os; sys.path.insert(0,os.path.dirname(os.path.abspath(__file__))); from aksh_oidc_verify import verify_token
t=json.load(sys.stdin)['value'];c=verify_token(t)
assert c['ref_type']=='tag',f'ref_type={c["ref_type"]}'
assert ':ref:refs/tags/' in c['sub'],f'sub={c["sub"]}'
assert c['ref']=='refs/tags/v2.0.1',f'ref={c["ref"]}'
assert c['aud']=='api://vault',f'aud={c["aud"]}'
print(f'PASS: sub={c["sub"]} ref_type={c["ref_type"]} ref={c["ref"]} aud={c["aud"]}')
PYEOF

cat > "$STATE/verify-pr.py" << PYEOF
import sys,json,os; sys.path.insert(0,os.path.dirname(os.path.abspath(__file__))); from aksh_oidc_verify import verify_token
t=json.load(sys.stdin)['value'];c=verify_token(t)
assert c['event_name']=='pull_request',f'event={c["event_name"]}'
assert c['sub'].endswith(':pull_request'),f'sub={c["sub"]}'
assert ':ref:' not in c['sub'],f'sub={c["sub"]}'
assert c['aud']=='pr-aud',f'aud={c["aud"]}'
print(f'PASS: sub={c["sub"]} event={c["event_name"]} aud={c["aud"]}')
PYEOF

cat > "$STATE/verify-deploy.py" << PYEOF
import sys,json,os; sys.path.insert(0,os.path.dirname(os.path.abspath(__file__))); from aksh_oidc_verify import verify_token
t=json.load(sys.stdin)['value'];c=verify_token(t)
assert ':environment:staging:' in c['sub'] or c['sub'].endswith(':environment:staging'),f'sub must contain :environment:staging: got {c["sub"]}'
assert c['event_name']=='deployment',f'event={c["event_name"]}'
assert c['aud']=='vault/hcp',f'aud={c["aud"]}'
print(f'PASS: sub={c["sub"]} event={c["event_name"]} aud={c["aud"]}')
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

    local resp run_id
    resp=$(curl -sf -X POST "$BASE/api/v1/runs" -H "Authorization: Bearer $SYSTEM_TOKEN" -H "Content-Type: application/json" -d "@$body_file")
    run_id=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])" 2>/dev/null) || {
        echo -e "  ${RED}✗${NC} submit failed: $resp"; FAIL=$((FAIL+1)); return
    }
    echo "  run_id=$run_id"

    # Configure + run
    local log_marker="$STATE/log-marker-$name"
    touch "$log_marker"
    local rd=$(mktemp -d /tmp/aksh-runner.XXXXXX)
    "$RUNNER" --runner-root "$rd" configure \
        --url "$BASE" --token t --name "$name" \
        --unattended --replace --ephemeral --labels "self-hosted" --no-externals \
        > /dev/null 2>&1

    local log="$RESULTS/$name/aksh/$NOW/runner.log"
    mkdir -p "$(dirname "$log")"
    { echo "=== $name ==="; echo "run_id=$run_id  event=$event  ref=$ref  repo=$repo"; echo ""; } > "$log"
    local runner_exit=0
    if "$RUNNER" --runner-root "$rd" run --once >> "$log" 2>&1; then
        :
    else
        runner_exit=$?
    fi

    # The run status and verifier output must both belong to this invocation.
    local status step_log oidc
    status=$(curl -sf "$BASE/api/v1/runs/$run_id" -H "Authorization: Bearer $SYSTEM_TOKEN" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo '?')
    step_log="$STATE/step-log-$name.txt"
    find "$STATE/replay/results" -type f -name "step-*.txt" -newer "$log_marker" -exec cat {} + > "$step_log" 2>/dev/null || :

    oidc=$(grep "PASS:" "$step_log" 2>/dev/null || true)
    if [[ $runner_exit -eq 0 && ( "$status" == "success" || "$status" == "completed" ) && -n "$oidc" ]]; then
        echo "  $oidc"
        echo -e "  ${GREEN}PASS${NC}  status=$status  step=run-log"
        PASS=$((PASS+1))
    else
        echo -e "  ${RED}FAIL${NC}  runner_exit=$runner_exit status=$status"
        [[ -s "$step_log" ]] && tail -5 "$step_log" | sed 's/^/  /'
        FAIL=$((FAIL+1))
    fi
    rm -rf "$rd"
}

# ══════════════════════════════════════════════════════════════════════
# Test 1: Push → branch ref
# ══════════════════════════════════════════════════════════════════════
run_test "oidc-push" "$STATE/verify-push.py" "push" "refs/heads/main" "acme/app" "sts.amazonaws.com" \
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
          python3 '"$STATE"'/verify-push.py <<< "$(curl -sf "$URL" -H "Authorization: Bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN")"'

# Test 2: Tag push
run_test "oidc-tag" "$STATE/verify-tag.py" "push" "refs/tags/v2.0.1" "acme/app" "api://vault" \
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
          python3 '"$STATE"'/verify-tag.py <<< "$(curl -sf "$URL" -H "Authorization: Bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN")"'

# Test 3: Pull request
run_test "oidc-pr" "$STATE/verify-pr.py" "pull_request" "refs/pull/42/merge" "acme/app" "pr-aud" \
'name: oidc-pr
on: pull_request
permissions:
  id-token: write
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: |
          URL="${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=pr-aud"
          python3 '"$STATE"'/verify-pr.py <<< "$(curl -sf "$URL" -H "Authorization: Bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN")"'

# Test 4: Deployment environment
run_test "oidc-deploy" "$STATE/verify-deploy.py" "deployment" "refs/heads/main" "acme/app" "vault/hcp" \
'name: oidc-deploy
on: deployment
jobs:
  deploy-staging:
    runs-on: ubuntu-latest
    environment: staging
    permissions:
      id-token: write
    steps:
      - run: |
          URL="${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=vault/hcp"
          python3 '"$STATE"'/verify-deploy.py <<< "$(curl -sf "$URL" -H "Authorization: Bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN")"'

# ══════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════"
echo -e "${GREEN}$PASS passed${NC}  ${RED}$FAIL failed${NC}"
echo "Results: $RESULTS/*/aksh/$NOW/"
exit $FAIL
