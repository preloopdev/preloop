#!/usr/bin/env bash
# batch-compare.sh — Run official runner against aksh server for multiple scenarios
# Runs each scenario inside its own smolVM with the aksh server + official runner.
# Then compares with GitHub conclusions from pre-captured data or live runs.
#
# Usage: ./scripts/batch-compare.sh [scenario1.yml scenario2.yml ...]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEMPLATE="${TEMPLATE:-/private/tmp/bench-runner.smolmachine}"
OFFICIAL_RUNNER_HOST="${OFFICIAL_RUNNER_HOST:-$HOME/cachingv4}"
AKSH_SERVER_BIN="$REPO_ROOT/target/aarch64-unknown-linux-musl/release/preloop-server"
GH_REPO="${GH_REPO:-preloopdev/aksh-conformance-sample}"
RESULTS_DIR="$REPO_ROOT/benchmarks/compatibility/server/behavior"

red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
info()  { printf '\033[1;34m▸ %s\033[0m\n' "$*"; }

# Default scenarios if none specified
if [ $# -eq 0 ]; then
    set -- 07-step-failure 14-annotations 23-context-fields 52-expression-features 53-secret-masking 87-multiline-output 90-shell-exit-behavior 98-outcome-vs-conclusion
fi

PASS=0
FAIL=0
RESULTS=()

run_scenario() {
    local scenario="$1"
    local payload_file="$RESULTS_DIR/payload-${scenario}.json"
    local result_dir="$RESULTS_DIR/${scenario}/aksh-server"
    mkdir -p "$result_dir"

    if [ ! -f "$payload_file" ]; then
        red "  Missing payload: $payload_file"
        return 1
    fi

    local vm="batch-${scenario}-$$"
    smolvm machine create --name "$vm" --from "$TEMPLATE" \
        --net -v "${OFFICIAL_RUNNER_HOST}:/opt/runners:ro" -v "$REPO_ROOT:/workspace" >/dev/null 2>&1
    smolvm machine update --name "$vm" --rosetta >/dev/null 2>&1
    smolvm machine start --name "$vm" >/dev/null 2>&1
    smolvm machine exec --name "$vm" -- bash -lc 'mount -t binfmt_misc binfmt_misc /proc/sys/fs/binfmt_misc 2>/dev/null || true; if [ -x /usr/bin/rosetta-wrapper ] && [ -x /mnt/rosetta/rosetta ]; then echo ":rosetta:M::\\x7fELF\\x02\\x01\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\\x00\\x3e\\x00:\\xff\\xff\\xff\\xff\\xff\\xfe\\xfe\\x00\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xfe\\xff\\xff\\xff:/usr/bin/rosetta-wrapper:F" > /proc/sys/fs/binfmt_misc/register 2>/dev/null || true; fi' >/dev/null 2>&1 || true
    smolvm machine cp "$AKSH_SERVER_BIN" "$vm:/usr/local/bin/preloop-server" >/dev/null 2>&1

    local output
    output=$(smolvm machine exec --name "$vm" -- bash -lc "
        set -u
        chmod +x /usr/local/bin/preloop-server
        RUST_LOG=info AKSH_PUBLIC_URL=http://127.0.0.1 preloop-server serve --listen 0.0.0.0:80 > /tmp/server.log 2>&1 &
        server_pid=\$!
        sleep 2
        if ! wget -qO- http://127.0.0.1/healthz >/dev/null; then
            echo 'SUBMISSION_ERROR: healthz failed'
            kill \$server_pid 2>/dev/null || true
            exit 1
        fi

        RESULT=\$(wget -qO- --post-file=/workspace/benchmarks/compatibility/server/behavior/payload-${scenario}.json \\
            --header='Content-Type: application/json' \
            --header='Authorization: Bearer aksh-system-token' \
            http://127.0.0.1/api/v1/runs 2>/dev/null)
        echo "SUBMISSION: \$RESULT"
        RUN_ID=\$(echo "\$RESULT" | python3 -c 'import sys,json; print(json.load(sys.stdin)["run_id"])')

        export RUNNER_ALLOW_RUNASROOT=1
        cp -a /opt/runners/actions-runner /tmp/runner
        rm -f /tmp/runner/.runner /tmp/runner/.credentials /tmp/runner/.credentials_rsaparams
        rm -rf /tmp/runner/_work; mkdir -p /tmp/runner/_work
        cd /tmp/runner
        ./config.sh --unattended --url 'http://127.0.0.1' --token 'aksh-system-token' \
            --name 'batch-test' --labels 'self-hosted,linux,x64' --work _work --replace --ephemeral > /tmp/config.log 2>&1
        rm -rf _work; mkdir -p _work
        timeout 180 ./run.sh > /tmp/runner.log 2>&1
        runner_rc=\$?
        echo "RUNNER_EXIT: \$runner_rc"
        cat /tmp/runner.log

        echo '---RESULT---'
        wget -qO /tmp/status.json --header='Authorization: Bearer aksh-system-token' \
            "http://127.0.0.1/api/v1/runs/\$RUN_ID" 2>/dev/null
        cat /tmp/status.json | python3 -c '
import sys, json
d = json.load(sys.stdin)
print(d["status"])
for k,v in d["jobs"].items():
    print(f"  {k}: {v}")
'
        echo '---ENDRESULT---'
        cp /tmp/server.log /workspace/benchmarks/compatibility/server/behavior/${scenario}/aksh-server/server.log 2>/dev/null || true
        cp /tmp/runner.log /workspace/benchmarks/compatibility/server/behavior/${scenario}/aksh-server/official-runner.log 2>/dev/null || true
        cp /tmp/config.log /workspace/benchmarks/compatibility/server/behavior/${scenario}/aksh-server/config.log 2>/dev/null || true
        cp /tmp/status.json /workspace/benchmarks/compatibility/server/behavior/${scenario}/aksh-server/status.json 2>/dev/null || true
        exit \$runner_rc
    " 2>&1 || true)

    smolvm machine stop --name "$vm" >/dev/null 2>&1 || true
    smolvm machine delete --name "$vm" -f >/dev/null 2>&1 || true

    local status
    status=$(echo "$output" | sed -n '/---RESULT---/,$ p' | sed '1d' | sed '/---ENDRESULT---/,$d' | head -1)
    [ -n "$status" ] || status=error
    local jobs
    jobs=$(echo "$output" | sed -n '/---RESULT---/,$ p' | sed '1d' | sed '/---ENDRESULT---/,$d' | tail -n +2)

    echo "$status" > "$result_dir/conclusion.txt"
    printf '%s\n' "$jobs" > "$result_dir/jobs.txt"
    printf '%s\n' "$output" > "$result_dir/runner.log"
    echo "$status"
}

info "Batch server comparison: ${#@} scenarios"
info "Official runner v2.335.1 → aksh server (in smolVM)"
echo ""

for scenario in "$@"; do
    printf "  %-35s " "$scenario"
    aksh_result=$(run_scenario "$scenario" 2>/dev/null || echo "ERROR")
    
    # Get GitHub comparison
    gh_conclusion="?"
    # Check from existing runner-flow captures
    if [ -f "$REPO_ROOT/benchmarks/compatibility/runner/protocol/${scenario}/official/latest/summary.json" ]; then
        gh_conclusion=$(python3 -c "import json; print(json.load(open('$REPO_ROOT/benchmarks/compatibility/runner/protocol/${scenario}/official/latest/summary.json'))['conclusion'])" 2>/dev/null || echo "?")
    fi
    # Check from server-compare captures
    if [ -f "$RESULTS_DIR/${scenario}/github/summary.json" ]; then
        gh_conclusion=$(python3 -c "import json; print(json.load(open('$RESULTS_DIR/${scenario}/github/summary.json'))['conclusion'])" 2>/dev/null || echo "?")
    fi

    if [ "$aksh_result" = "$gh_conclusion" ]; then
        green "aksh=$aksh_result  github=$gh_conclusion  ✅"
        PASS=$((PASS + 1))
    elif [ "$gh_conclusion" = "?" ]; then
        printf "aksh=%s  github=?  ⚠ (no github capture)\n" "$aksh_result"
    else
        red "aksh=$aksh_result  github=$gh_conclusion  ❌ MISMATCH"
        FAIL=$((FAIL + 1))
    fi
done

echo ""
echo "══════════════════════════════════════════════════"
echo "  Passed: $PASS  Failed: $FAIL  Total: $(($PASS + $FAIL))"
if [ "$FAIL" -eq 0 ]; then
    green "  All scenarios match!"
else
    red "  $FAIL mismatches found"
fi
echo "══════════════════════════════════════════════════"
