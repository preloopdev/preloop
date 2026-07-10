#!/usr/bin/env bash
# batch-aksh-compare.sh — Run all scenarios against aksh server in a persistent VM
# Reuses one VM for all scenarios to avoid spin-up overhead.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GH_REPO="${GH_REPO:-preloopdev/aksh-conformance-sample}"
TEMPLATE="${TEMPLATE:-/private/tmp/bench-runner.smolmachine}"
OFFICIAL_RUNNER_HOST="${OFFICIAL_RUNNER_HOST:-$HOME/cachingv4}"
AKSH_SERVER_BIN="$REPO_ROOT/target/aarch64-unknown-linux-musl/release/aksh-runner-server"
RESULTS_BASE="$REPO_ROOT/benchmarks/real-world/results/server-compare"

red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
log()   { echo "[$(date -u +%H:%M:%S)] $*"; }

VM="cmp-batch-$$"
cleanup() {
    smolvm machine stop --name "$VM" 2>/dev/null || true
    smolvm machine delete --name "$VM" -f 2>/dev/null || true
}
trap cleanup EXIT

# Create persistent VM
log "Creating persistent VM: $VM"
smolvm machine create --name "$VM" --from "$TEMPLATE" \
    --net -v "${OFFICIAL_RUNNER_HOST}:/opt/runners:ro" -v "$REPO_ROOT:/workspace" >/dev/null 2>&1
smolvm machine start --name "$VM" >/dev/null 2>&1
smolvm machine cp "$AKSH_SERVER_BIN" "$VM:/usr/local/bin/aksh-runner-server" 2>&1 | tail -1
log "VM ready"

SCENARIOS="${@:-07-step-failure 52-expression-features 53-secret-masking 80-custom-shells 08-job-outputs-needs 09-matrix-fan-out 10-uses-checkout 14-annotations 87-multiline-output 88-state-and-post 90-shell-exit-behavior 98-outcome-vs-conclusion}"

for scenario in $SCENARIOS; do
    log "═══ $scenario ═══"
    RESULT_DIR="$RESULTS_BASE/$scenario/aksh-server"
    rm -rf "$RESULT_DIR"
    mkdir -p "$RESULT_DIR/diag"

    # Get workflow YAML
    WF_YAML=$(gh api "repos/$GH_REPO/contents/.github/workflows/${scenario}.yml" --jq .content 2>/dev/null | base64 -d)
    JOB_COUNT=$(echo "$WF_YAML" | grep -c "runs-on:" || echo 1)
    
    # Prepare modified workflow (change runs-on to just self-hosted)
    MODIFIED_YAML=$(echo "$WF_YAML" | sed 's/runs-on:.*$/runs-on: self-hosted/')

    # Prepare submission JSON
    PAYLOAD_FILE="$RESULTS_BASE/payload-${scenario}.json"
    python3 -c "
import json, sys
print(json.dumps({
    'workflow_yaml': sys.stdin.read(),
    'event': 'workflow_dispatch',
    'repository': '$GH_REPO',
    'git_ref': 'refs/heads/main'
}))
" <<< "$MODIFIED_YAML" > "$PAYLOAD_FILE"

    RESULT_BASE_VM="/workspace/benchmarks/real-world/results/server-compare/$scenario/aksh-server"

    # Run scenario in the persistent VM (each run: start server, run runners, stop server)
    smolvm machine exec --name "$VM" -- bash -lc "
        set -u
        chmod +x /usr/local/bin/aksh-runner-server
        
        # Start server
        RUST_LOG=info AKSH_PUBLIC_URL=http://127.0.0.1 aksh-runner-server serve --listen 0.0.0.0:80 > /tmp/server.log 2>&1 &
        server_pid=\$!
        sleep 2
        wget -qO- http://127.0.0.1/healthz >/dev/null || { echo 'healthz failed'; kill \$server_pid 2>/dev/null; exit 1; }
        
        # Submit workflow
        RESULT=\$(wget -qO- --post-file=/workspace/benchmarks/real-world/results/server-compare/payload-${scenario}.json \
            --header='Content-Type: application/json' \
            --header='Authorization: Bearer aksh-system-token' \
            http://127.0.0.1/api/v1/runs 2>/dev/null)
        RUN_ID=\$(echo \"\$RESULT\" | python3 -c 'import sys,json; print(next(iter(json.load(sys.stdin).values())))' 2>/dev/null)
        echo \"RUN_ID=\$RUN_ID\"

        export RUNNER_ALLOW_RUNASROOT=1 ACTIONS_RUNNER_DEBUG=true RUNNER_DEBUG=1
        JOB_COUNT=$JOB_COUNT

        # Configure all runners
        for i in \$(seq 1 \$JOB_COUNT); do
            RUNNER_DIR=/tmp/runner-\$i
            cp -a /opt/runners/actions-runner \$RUNNER_DIR
            rm -f \$RUNNER_DIR/.runner \$RUNNER_DIR/.credentials \$RUNNER_DIR/.credentials_rsaparams
            rm -rf \$RUNNER_DIR/_work \$RUNNER_DIR/_diag; mkdir -p \$RUNNER_DIR/_work
            cd \$RUNNER_DIR
            ./config.sh --unattended --url 'http://127.0.0.1' --token 'aksh-system-token' \
                --name \"cmp-\$i\" --labels 'self-hosted,linux,x64' --work _work --replace --ephemeral > /tmp/config-\$i.log 2>&1
            rm -rf _work; mkdir -p _work
        done

        # Launch all runners concurrently, track PIDs
        RUNNER_PIDS=""
        for i in \$(seq 1 \$JOB_COUNT); do
            RUNNER_DIR=/tmp/runner-\$i
            (cd \$RUNNER_DIR && timeout 120 ./run.sh > /tmp/runner-\$i.log 2>&1; echo \$? > /tmp/runner-\$i.rc) &
            RUNNER_PIDS=\"\$RUNNER_PIDS \$!\"
        done
        for pid in \$RUNNER_PIDS; do wait \$pid 2>/dev/null || true; done

        # Collect results
        sleep 1
        wget -qO /tmp/status.json --header='Authorization: Bearer aksh-system-token' \
            \"http://127.0.0.1/api/v1/runs/\$RUN_ID\" 2>/dev/null || true

        # Copy artifacts
        for i in \$(seq 1 \$JOB_COUNT); do
            cp -a /tmp/runner-\$i/_diag/. $RESULT_BASE_VM/diag/ 2>/dev/null || true
            cp /tmp/runner-\$i.log $RESULT_BASE_VM/runner-\$i.log 2>/dev/null || true
        done
        cp /tmp/runner-1.log $RESULT_BASE_VM/official-runner.log 2>/dev/null || true
        cp /tmp/server.log $RESULT_BASE_VM/server.log || true
        cp /tmp/status.json $RESULT_BASE_VM/status.json || true

        # Stop server
        kill \$server_pid 2>/dev/null; wait \$server_pid 2>/dev/null || true
        
        # Print status
        cat /tmp/status.json 2>/dev/null || echo '{}'
    " > "$RESULT_DIR/runner.log" 2>&1

    # Extract and display results
    aksh_status=$(python3 -c "import json; print(json.load(open('$RESULT_DIR/status.json'))['status'])" 2>/dev/null || echo unknown)
    job_details=$(python3 -c "import json; d=json.load(open('$RESULT_DIR/status.json')); print('\n'.join(f'  {k}: {v}' for k,v in d.get('jobs',{}).items()))" 2>/dev/null || true)
    echo "{\"server\":\"aksh\",\"conclusion\":\"$aksh_status\"}" > "$RESULT_DIR/summary.json"

    # Compare with GitHub baseline if available
    gh_c=$(python3 -c "import json; print(json.load(open('$RESULTS_BASE/$scenario/github/summary.json')).get('conclusion','?'))" 2>/dev/null || echo "missing")
    
    if [ "$gh_c" = "missing" ] || [ "$gh_c" = "cancelled" ]; then
        log "aksh=$aksh_status (no valid GitHub baseline)"
        echo "$job_details"
    elif [ "$gh_c" = "$aksh_status" ]; then
        green "  ✅ $scenario: MATCH (both=$aksh_status)"
        echo "$job_details"
    else
        red "  ❌ $scenario: MISMATCH (github=$gh_c aksh=$aksh_status)"
        echo "$job_details"
    fi
    echo ""
done

log "Batch complete"
