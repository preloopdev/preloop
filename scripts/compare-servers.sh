#!/usr/bin/env bash
# compare-servers.sh — Official runner against GitHub vs official runner against aksh
# Usage: ./scripts/compare-servers.sh <scenario.yml> [--github-only|--aksh-only]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SCENARIO="${1:?Usage: $0 <scenario.yml>}"
MODE="${2:-both}"
GH_REPO="${GH_REPO:-Bnjoroge1/aksh-conformance}"
TEMPLATE="${TEMPLATE:-/private/tmp/bench-runner.smolmachine}"
OFFICIAL_RUNNER_HOST="${OFFICIAL_RUNNER_HOST:-$HOME/cachingv4}"
AKSH_SERVER_BIN="$REPO_ROOT/target/aarch64-unknown-linux-musl/release/preloop-server"
RESULTS_DIR="$REPO_ROOT/benchmarks/compatibility/server/behavior/${SCENARIO%.yml}"
PROTOCOL_DIR="$REPO_ROOT/benchmarks/compatibility/server/protocol/captures/${SCENARIO%.yml}"
MITM_PORT=18081
GITHUB_ACTIONS_TOKEN="${AKSH_GITHUB_TOKEN:-}"

red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
info()  { printf '\033[1;34m▸ %s\033[0m\n' "$*"; }
log()   { echo "[$(date -u +%H:%M:%S)] $*"; }

cleanup() {
    for vm in $(smolvm machine ls --json 2>/dev/null | python3 -c 'import sys,json; [print(m["name"]) for m in json.load(sys.stdin) if m["name"].startswith("cmp-")]' 2>/dev/null || true); do
        smolvm machine stop --name "$vm" 2>/dev/null || true
        smolvm machine delete --name "$vm" -f 2>/dev/null || true
    done
}
trap cleanup EXIT

enable_rosetta() {
    local vm="$1"
    smolvm machine exec --name "$vm" -- bash -lc '
        if [ -x /usr/bin/rosetta-wrapper ] && [ -x /mnt/rosetta/rosetta ]; then
            mount -t binfmt_misc binfmt_misc /proc/sys/fs/binfmt_misc 2>/dev/null || true
            echo ":rosetta:M::\\x7fELF\\x02\\x01\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\\x00\\x3e\\x00:\\xff\\xff\\xff\\xff\\xff\\xfe\\xfe\\x00\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xfe\\xff\\xff\\xff:/usr/bin/rosetta-wrapper:F" > /proc/sys/fs/binfmt_misc/register 2>/dev/null || true
        fi
    ' >/dev/null 2>&1 || true
}

mkdir -p "$RESULTS_DIR"

# Get workflow YAML and save to temp file
WF_YAML_FILE=$(mktemp)
gh api "repos/$GH_REPO/contents/.github/workflows/$SCENARIO" --jq .content 2>/dev/null | base64 -d > "$WF_YAML_FILE"
JOB_COUNT="${RUNNER_COUNT:-$(grep -c "runs-on:" "$WF_YAML_FILE" || echo 1)}"
WF_LABEL=$(grep "runs-on:" "$WF_YAML_FILE" | head -1 | sed "s/.*\[//;s/\].*//" | tr "," "\n" | grep -v self-hosted | head -1 | tr -d " ")
[ -z "$WF_LABEL" ] && WF_LABEL=mitm

# ─── GitHub side ──────────────────────────────────────────────
run_github() {
    log "═══ Official runner → GitHub ═══"
    local cap_dir="$RESULTS_DIR/github"
    rm -rf "$cap_dir"
    mkdir -p "$cap_dir"

    gh run list -R "$GH_REPO" -L 10 --json databaseId,status \
        -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null |
        while read -r rid; do [ -n "$rid" ] && gh run cancel "$rid" -R "$GH_REPO" >/dev/null 2>&1 || true; done

    local token
    token=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)

    local vm="cmp-gh-$$"
    local template_arg="--from $TEMPLATE"
    if [ ! -f "$TEMPLATE" ]; then
        template_arg="--image ubuntu:24.04"
    fi
    smolvm machine create --name "$vm" $template_arg \
        --net -v "${OFFICIAL_RUNNER_HOST}:/opt/runners:ro" -v "$REPO_ROOT:/workspace:ro" >/dev/null 2>&1
    smolvm machine update --name "$vm" --rosetta >/dev/null 2>&1
    smolvm machine start --name "$vm" >/dev/null 2>&1
    enable_rosetta "$vm"
    log "VM $vm started"

    # Start N non-ephemeral runners (for multi-job), or 1 ephemeral (single-job)
    smolvm machine exec --name "$vm" -- bash -lc "
        set -euo pipefail

       echo 'Waiting for internet access...'
       for n in \$(seq 1 30); do
          getent hosts github.com >/dev/null && break; sleep 1
       done

       echo 'Installing runner dependencies (git, curl, wget, nodejs, libicu, mitmproxy)...'
       apt-get update -qq && apt-get install -y -qq --no-install-recommends git curl wget ca-certificates nodejs npm libicu-dev mitmproxy >/dev/null 2>&1

        mkdir -p /tmp/cap/vm-mitm /tmp/cap/vm-mitm-conf
        nohup env MITM_CAPTURE_DIR=/tmp/cap/vm-mitm mitmdump \
            --listen-host 127.0.0.1 --listen-port $MITM_PORT \
            --set confdir=/tmp/cap/vm-mitm-conf \
            -s /workspace/experiments/mitm/addons/capture.py \
            > /tmp/cap/vm-mitm.log 2>&1 < /dev/null &
        for n in \$(seq 1 40); do
            bash -c '</dev/tcp/127.0.0.1/$MITM_PORT' 2>/dev/null && break; sleep .25
        done
        export HTTP_PROXY='http://127.0.0.1:$MITM_PORT' HTTPS_PROXY='http://127.0.0.1:$MITM_PORT'
        export http_proxy='http://127.0.0.1:$MITM_PORT' https_proxy='http://127.0.0.1:$MITM_PORT'
        export NODE_EXTRA_CA_CERTS=/tmp/cap/vm-mitm-conf/mitmproxy-ca-cert.pem
        export SSL_CERT_FILE=/tmp/cap/vm-mitm-conf/mitmproxy-ca-cert.pem
        export GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1 RUNNER_ALLOW_RUNASROOT=1

        case "$SCENARIO" in
            *container*|*services*|*docker*)
                echo 'Installing and starting Docker inside guest VM...'
                apt-get update -qq && apt-get install -y -qq docker.io >/dev/null 2>&1
                mkdir -p /storage/docker
                (nohup dockerd --data-root /storage/docker > /tmp/dockerd.log 2>&1 &)
                for n in \$(seq 1 40); do
                    docker info >/dev/null 2>&1 && break; sleep 1
                done
                ;;
        esac

        JOB_COUNT=$JOB_COUNT
        for i in \$(seq 1 \$JOB_COUNT); do
            RUNNER_DIR=/tmp/runner-\$i
            cp -a /opt/runners/actions-runner \$RUNNER_DIR
            rm -f \$RUNNER_DIR/.runner \$RUNNER_DIR/.credentials \$RUNNER_DIR/.credentials_rsaparams
            rm -rf \$RUNNER_DIR/_work \$RUNNER_DIR/_diag; mkdir -p \$RUNNER_DIR/_work
            cd \$RUNNER_DIR
            if [ \$JOB_COUNT -eq 1 ]; then
                ./config.sh --unattended --url 'https://github.com/$GH_REPO' --token '$token' \\
                    --name \"cmp-gh-\$i-$$\" --labels 'self-hosted,linux,x64,$WF_LABEL' --work _work --replace --ephemeral 2>&1
            else
                ./config.sh --unattended --url 'https://github.com/$GH_REPO' --token '$token' \\
                    --name \"cmp-gh-\$i-$$\" --labels 'self-hosted,linux,x64,$WF_LABEL' --work _work --replace 2>&1
            fi
            rm -rf _work; mkdir -p _work
            nohup sh -c \"cd \$RUNNER_DIR && timeout 300 ./run.sh\" > /tmp/runner-\$i.log 2>&1 &
        done
        # Wait for all runners to either finish or time out
        wait
    " > "$cap_dir/runner.log" 2>&1 &
    local rpid=$!

    sleep 50
    gh workflow run "$SCENARIO" -R "$GH_REPO" --ref main >/dev/null
    sleep 3
    local run_id
    run_id=$(gh run list -R "$GH_REPO" -w "$SCENARIO" --json databaseId -q '.[0].databaseId')
    log "GitHub dispatched as run $run_id"

    local deadline=$(($(date +%s) + 300))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local st
        st=$(gh run view "$run_id" -R "$GH_REPO" --json status -q '.status' 2>/dev/null || echo unknown)
        [ "$st" = completed ] && break; sleep 5
    done
    # The GitHub workflow can complete while the runner listener remains in a
    # long-poll. Export protocol traffic before waiting for that listener.
    sleep 5
    local protocol_cap_dir="$PROTOCOL_DIR/github"
    mkdir -p "$protocol_cap_dir"
    smolvm machine exec --name "$vm" -- cat /tmp/cap/vm-mitm/flows.jsonl > "$protocol_cap_dir/flows.jsonl" 2>/dev/null || true
    smolvm machine exec --name "$vm" -- cat /tmp/cap/vm-mitm.log > "$protocol_cap_dir/mitm.log" 2>/dev/null || true
    if [ -f "$protocol_cap_dir/flows.jsonl" ]; then
        wc -l < "$protocol_cap_dir/flows.jsonl" > "$protocol_cap_dir/flow-count"
    fi
    kill "$rpid" 2>/dev/null || true
    wait "$rpid" 2>/dev/null || true

    # Deregister non-ephemeral runners
    if [ "$JOB_COUNT" -gt 1 ]; then
        for i in $(seq 1 "$JOB_COUNT"); do
            gh api "repos/$GH_REPO/actions/runners" --jq ".runners[] | select(.name == \"cmp-gh-${i}-$$\") | .id" 2>/dev/null |
                while read -r rid; do gh api -X DELETE "repos/$GH_REPO/actions/runners/$rid" 2>/dev/null || true; done
        done
    fi

    local conclusion
    conclusion=$(gh run view "$run_id" -R "$GH_REPO" --json conclusion -q '.conclusion' 2>/dev/null || echo unknown)
    local job_details
    job_details=$(gh run view "$run_id" -R "$GH_REPO" --json jobs --jq '.jobs[] | "\(.name): \(.conclusion)"' 2>/dev/null || echo "")
    gh run view "$run_id" -R "$GH_REPO" --log > "$cap_dir/steps.log" 2>/dev/null || true
    gh run view "$run_id" -R "$GH_REPO" --json jobs > "$cap_dir/jobs.json" 2>/dev/null || true
    echo "{\"server\":\"github\",\"conclusion\":\"$conclusion\",\"run_id\":\"$run_id\"}" > "$cap_dir/summary.json"
    echo "$job_details" > "$cap_dir/jobs.txt"
    log "GitHub done: conclusion=$conclusion"
    echo "$job_details"

    smolvm machine stop --name "$vm" 2>/dev/null || true
    smolvm machine delete --name "$vm" -f 2>/dev/null || true
}

# ─── aksh server side ────────────────────────────────────────
run_aksh() {
    log "═══ Official runner → aksh server ═══"
    local cap_dir="$RESULTS_DIR/aksh-server"
    rm -rf "$cap_dir"
    mkdir -p "$cap_dir"

    local vm="cmp-aksh-$$"
    local template_arg="--from $TEMPLATE"
    if [ ! -f "$TEMPLATE" ]; then
        template_arg="--image ubuntu:24.04"
    fi
    smolvm machine create --name "$vm" $template_arg \
        --net -v "${OFFICIAL_RUNNER_HOST}:/opt/runners:ro" -v "$REPO_ROOT:/workspace" >/dev/null 2>&1
    smolvm machine update --name "$vm" --rosetta >/dev/null 2>&1
    smolvm machine start --name "$vm" >/dev/null 2>&1
    enable_rosetta "$vm"
    log "VM $vm started"

    smolvm machine cp "$AKSH_SERVER_BIN" "$vm:/usr/local/bin/preloop-server" 2>&1 | tail -1

    # Prepare modified workflow YAML (change runs-on to just self-hosted)
    local modified_yaml
    modified_yaml=$(mktemp)
    sed 's/runs-on:.*$/runs-on: self-hosted/' "$WF_YAML_FILE" > "$modified_yaml"

    # Prepare submission JSON on the mounted workspace; /tmp is not persistent across smolVM exec calls.
    local payload_file="$REPO_ROOT/benchmarks/compatibility/server/behavior/payload-${SCENARIO%.yml}.json"
    python3 -c "
import json
yaml_content = open('$modified_yaml').read()
print(json.dumps({
    'workflow_yaml': yaml_content,
    'event': 'workflow_dispatch',
    'repository': '$GH_REPO',
    'git_ref': 'refs/heads/main'
}))
" > "$payload_file"
    local vm_result_dir="$REPO_ROOT/benchmarks/compatibility/server/behavior/${SCENARIO%.yml}/aksh-server"
    mkdir -p "$vm_result_dir"

    local result_base="/workspace/benchmarks/compatibility/server/behavior/${SCENARIO%.yml}/aksh-server"

    smolvm machine exec --name "$vm" -- bash -lc "
        set -u

       echo 'Waiting for internet access...'
       for n in \$(seq 1 30); do
          getent hosts github.com >/dev/null && break; sleep 1
       done

       echo 'Installing runner dependencies (git, curl, wget, nodejs, libicu, mitmproxy)...'
       apt-get update -qq && apt-get install -y -qq --no-install-recommends git curl wget ca-certificates nodejs npm libicu-dev mitmproxy >/dev/null 2>&1

        mkdir -p /tmp/cap/vm-mitm /tmp/cap/vm-mitm-conf
        nohup env MITM_CAPTURE_DIR=/tmp/cap/vm-mitm BACKEND_PORT=80 mitmdump \\
            --listen-host 127.0.0.1 --listen-port $MITM_PORT \\
            --set confdir=/tmp/cap/vm-mitm-conf \\
            -s /workspace/experiments/mitm/addons/capture.py \\
            > /tmp/cap/vm-mitm.log 2>&1 < /dev/null &
        for n in \$(seq 1 40); do
            bash -c '</dev/tcp/127.0.0.1/$MITM_PORT' 2>/dev/null && break; sleep .25
        done
        chmod +x /usr/local/bin/preloop-server
        RUST_LOG=info AKSH_PUBLIC_URL=http://127.0.0.1 AKSH_GITHUB_TOKEN='$GITHUB_ACTIONS_TOKEN' preloop-server serve --listen 127.0.0.1:80 --state-dir /tmp/aksh-state > /tmp/server.log 2>&1 &
        server_pid=\$!
        sleep 2
        wget -qO- http://127.0.0.1/healthz >/dev/null || { echo 'healthz failed'; cat /tmp/server.log; cp /tmp/server.log $result_base/server.log || true; exit 1; }
        case "$SCENARIO" in
            *container*|*services*|*docker*)
                echo 'Installing and starting Docker inside guest VM...'
                apt-get update -qq && apt-get install -y -qq docker.io >/dev/null 2>&1
                mkdir -p /storage/docker
                (nohup dockerd --data-root /storage/docker > /tmp/dockerd.log 2>&1 &)
                for n in \$(seq 1 40); do
                    docker info >/dev/null 2>&1 && break; sleep 1
                done
                ;;
        esac
        RESULT=\$(wget -qO- --post-file=/workspace/benchmarks/compatibility/server/behavior/payload-${SCENARIO%.yml}.json \\
            --header='Content-Type: application/json' \\
            --header='Authorization: Bearer aksh-system-token' \\
            http://127.0.0.1/api/v1/runs 2>/dev/null)
        echo \"SUBMISSION: \$RESULT\"
        RUN_ID=\$(echo \"\$RESULT\" | python3 -c 'import sys,json; print(next(iter(json.load(sys.stdin).values())))')

        export RUNNER_ALLOW_RUNASROOT=1 ACTIONS_RUNNER_DEBUG=true RUNNER_DEBUG=1
        export HTTP_PROXY='http://127.0.0.1:$MITM_PORT' HTTPS_PROXY='http://127.0.0.1:$MITM_PORT'
        export http_proxy='http://127.0.0.1:$MITM_PORT' https_proxy='http://127.0.0.1:$MITM_PORT'
        export NO_PROXY='' no_proxy='' NODE_EXTRA_CA_CERTS=/tmp/cap/vm-mitm-conf/mitmproxy-ca-cert.pem
        export SSL_CERT_FILE=/tmp/cap/vm-mitm-conf/mitmproxy-ca-cert.pem
        git config --global http.sslCAInfo /tmp/cap/vm-mitm-conf/mitmproxy-ca-cert.pem
        JOB_COUNT=$JOB_COUNT
        last_rc=0

        # Configure all runners first (sequential - each needs registration)
        for i in \$(seq 1 \$JOB_COUNT); do
            RUNNER_DIR=/tmp/runner-\$i
            cp -a /opt/runners/actions-runner \$RUNNER_DIR
            rm -f \$RUNNER_DIR/.runner \$RUNNER_DIR/.credentials \$RUNNER_DIR/.credentials_rsaparams
            rm -rf \$RUNNER_DIR/_work \$RUNNER_DIR/_diag; mkdir -p \$RUNNER_DIR/_work
            cd \$RUNNER_DIR
            ./config.sh --unattended --url 'http://127.0.0.1' --token 'aksh-system-token' \\
                --name \"cmp-aksh-\$i-$$\" --labels 'self-hosted,linux,x64' --work _work --replace --ephemeral > /tmp/config-\$i.log 2>&1
            rm -rf _work; mkdir -p _work
        done

        # Launch all runners concurrently, track PIDs
        RUNNER_PIDS=""
        for i in \$(seq 1 \$JOB_COUNT); do
            RUNNER_DIR=/tmp/runner-\$i
            (cd \$RUNNER_DIR && timeout 240 ./run.sh > /tmp/runner-\$i.log 2>&1; echo \$? > /tmp/runner-\$i.rc) &
            RUNNER_PIDS=\"\$RUNNER_PIDS \$!\"
        done

        # GitHub's cancellation scenario cancels the in-flight workflow through
        # GitHub's control plane. Mirror that control-plane event for aksh.
        if [ "$SCENARIO" = "22-cancel-semantics.yml" ]; then
            (
                sleep 5
                wget -qO /dev/null --post-data='' \
                    --header='Authorization: Bearer aksh-system-token' \
                    "http://127.0.0.1/api/v1/runs/\$RUN_ID/cancel"
            ) &
        fi
        for pid in \$RUNNER_PIDS; do wait \$pid 2>/dev/null || true; done

        for i in \$(seq 1 \$JOB_COUNT); do
            rc=\$(cat /tmp/runner-\$i.rc 2>/dev/null || echo 1)
            echo \"RUNNER_\$i EXIT=\$rc\"
            if [ \$rc -ne 0 ]; then last_rc=\$rc; fi
        done

        echo \"RUN_ID=\$RUN_ID\"
        sleep 2
        wget -qO /tmp/status.json --header='Authorization: Bearer aksh-system-token' \\
            \"http://127.0.0.1/api/v1/runs/\$RUN_ID\" 2>/dev/null || true

        # Collect artifacts from all runners
        mkdir -p $result_base/diag
        for i in \$(seq 1 \$JOB_COUNT); do
            cp -a /tmp/runner-\$i/_diag/. $result_base/diag/ 2>/dev/null || true
            cp -a /tmp/runner-\$i/_work/. $result_base/work 2>/dev/null || true
            cp /tmp/runner-\$i.log $result_base/runner-\$i.log 2>/dev/null || true
            cp /tmp/config-\$i.log $result_base/config-\$i.log 2>/dev/null || true
        done
        # For backwards compat, also copy runner-1 as the default log
        cp /tmp/runner-1.log $result_base/official-runner.log 2>/dev/null || true
        cp /tmp/server.log $result_base/server.log || true
        cp /tmp/status.json $result_base/status.json || true
        cp /tmp/status.json $result_base/run.json || true
        mkdir -p $result_base/replay
        cp -a /tmp/aksh-state/replay/results $result_base/replay/ 2>/dev/null || true
        echo '---STATUS---'
        cat /tmp/status.json 2>/dev/null || true
        echo '---ENDSTATUS---'
        exit \$last_rc
    " > "$cap_dir/runner.log" 2>&1 || true
    local protocol_cap_dir="$PROTOCOL_DIR/aksh-server"
    mkdir -p "$protocol_cap_dir"
    smolvm machine exec --name "$vm" -- cat /tmp/cap/vm-mitm/flows.jsonl > "$protocol_cap_dir/flows.jsonl" 2>/dev/null || true
    smolvm machine exec --name "$vm" -- cat /tmp/cap/vm-mitm.log > "$protocol_cap_dir/mitm.log" 2>/dev/null || true
    if [ -f "$protocol_cap_dir/flows.jsonl" ]; then
        wc -l < "$protocol_cap_dir/flows.jsonl" > "$protocol_cap_dir/flow-count"
    fi

    # Extract results from the persisted API response, not human runner output.
    local status_file="$vm_result_dir/status.json"
    local aksh_status
    aksh_status=$(python3 -c "import json; print(json.load(open('$status_file'))['status'])" 2>/dev/null || echo unknown)
    local job_details
    job_details=$(python3 -c "import json; d=json.load(open('$status_file')); print('\\n'.join(f'{k}: {v}' for k,v in d.get('jobs',{}).items()))" 2>/dev/null || true)
    echo "{\"server\":\"aksh\",\"conclusion\":\"$aksh_status\"}" > "$cap_dir/summary.json"
    printf '%s\n' "$job_details" > "$cap_dir/jobs.txt"
    log "aksh done: status=$aksh_status"
    echo "$job_details"

    smolvm machine stop --name "$vm" >/dev/null 2>&1 || true
    smolvm machine delete --name "$vm" -f >/dev/null 2>&1 || true
}

# ─── Main ─────────────────────────────────────────────────────
info "Server comparison: $SCENARIO ($JOB_COUNT jobs)"
info "  Official runner → GitHub  vs  Official runner → aksh server"

if [ "$MODE" != "--aksh-only" ]; then
    run_github
fi
if [ "$MODE" != "--github-only" ]; then
    run_aksh
fi

rm -f "$WF_YAML_FILE"

# Compare
if [ -f "$RESULTS_DIR/github/summary.json" ] && [ -f "$RESULTS_DIR/aksh-server/summary.json" ]; then
    gh_c=$(python3 -c "import json; print(json.load(open('$RESULTS_DIR/github/summary.json'))['conclusion'])")
    aksh_c=$(python3 -c "import json; print(json.load(open('$RESULTS_DIR/aksh-server/summary.json'))['conclusion'])")

    echo ""
    echo "══════════════════════════════════════════════════"
    printf "  %-30s %s\n" "Scenario:" "$SCENARIO"
    printf "  %-30s %s\n" "GitHub conclusion:" "$gh_c"
    printf "  %-30s %s\n" "aksh server conclusion:" "$aksh_c"
    if [ "$gh_c" = "$aksh_c" ]; then
        green "  ✅ MATCH — aksh server matches GitHub"
    else
        red "  ❌ MISMATCH — aksh=$aksh_c github=$gh_c"
    fi
    echo "══════════════════════════════════════════════════"
    echo ""
    echo "  GitHub jobs:"
    sed 's/^/    /' "$RESULTS_DIR/github/jobs.txt" 2>/dev/null || true
    echo "  aksh jobs:"
    sed 's/^/    /' "$RESULTS_DIR/aksh-server/jobs.txt" 2>/dev/null || true
fi
