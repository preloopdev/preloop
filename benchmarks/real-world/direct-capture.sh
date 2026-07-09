#!/usr/bin/env bash
# Direct capture on an already-running VM. Bypasses ensure_vms/prepare_vm.
# Usage: ./direct-capture.sh <workflow-yml> <vm-name> [official|aksh|both]
set -euo pipefail

SCENARIO="${1:?Usage: $0 <workflow> <vm> [kind]}"
VM="${2:?Need VM name}"
RUNNER_KIND="${3:-both}"

GH_REPO="${GH_REPO:-preloopdev/aksh-conformance-sample}"
GH_REF="${GH_REF:-main}"
MITM_PORT="${MITM_PORT:-18081}"
HOST_WORKSPACE="$PWD"
VM_WORKSPACE="/workspace"
RESULTS_ROOT="${RESULTS_ROOT:-$PWD/benchmarks/real-world/results/runner-flow}"
AKSH_RUNNER="/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner"
OFFICIAL_SRC="/opt/runners/actions-runner"
MITM_ADDON="/workspace/experiments/mitm/addons/capture.py"

log() { echo "[$(date -u +%H:%M:%S.%3NZ)] $*"; }

cancel_stale_runs() {
  gh run list -R "$GH_REPO" -L 30 --json databaseId,status \
    -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null \
    | while read -r rid; do [ -n "$rid" ] && gh run cancel "$rid" -R "$GH_REPO" >/dev/null 2>&1 || true; done
}

wait_for_run() {
  local run_id="$1" deadline status
  deadline=$(($(date +%s) + 900))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    status=$(gh run view "$run_id" -R "$GH_REPO" --json status -q '.status' 2>/dev/null || echo unknown)
    [ "$status" = "completed" ] && return 0
    sleep 5
  done
  return 1
}

start_mitm() {
  local capture_dir="$1"
  local vm_capture_dir="${capture_dir/#$HOST_WORKSPACE/$VM_WORKSPACE}"
  smolvm machine exec --name "$VM" -- bash -lc "
    set -euo pipefail
    pkill -x mitmdump >/dev/null 2>&1 || true
    rm -rf '$vm_capture_dir/vm-mitm'
    mkdir -p '$vm_capture_dir/vm-mitm' '$vm_capture_dir/vm-mitm-conf'
    nohup env MITM_CAPTURE_DIR='$vm_capture_dir/vm-mitm' mitmdump \
      --listen-host 127.0.0.1 --listen-port '$MITM_PORT' \
      --set confdir='$vm_capture_dir/vm-mitm-conf' \
      -s '$MITM_ADDON' > '$vm_capture_dir/vm-mitm.log' 2>&1 < /dev/null &
    echo \$! > '$vm_capture_dir/vm-mitm.pid'
    for n in \$(seq 1 40); do
      if bash -c '</dev/tcp/127.0.0.1/$MITM_PORT' >/dev/null 2>&1; then exit 0; fi
      sleep 0.25
    done
    exit 3
  "
}

stop_mitm() {
  local capture_dir="$1"
  local vm_capture_dir="${capture_dir/#$HOST_WORKSPACE/$VM_WORKSPACE}"
  smolvm machine exec --name "$VM" -- bash -lc "
    if [ -f '$vm_capture_dir/vm-mitm.pid' ]; then
      kill -INT \$(cat '$vm_capture_dir/vm-mitm.pid') >/dev/null 2>&1 || true
      sleep 1
    fi
    pkill -x mitmdump >/dev/null 2>&1 || true
  " >/dev/null 2>&1 || true
}

capture_one() {
  local kind="$1"
  local ts capture_dir vm_capture_dir token run_id conclusion flow_count
  ts=$(date -u +%Y-%m-%dT%H-%M-%SZ)
  capture_dir="$RESULTS_ROOT/${SCENARIO%.yml}/$kind/$ts"
  vm_capture_dir="${capture_dir/#$HOST_WORKSPACE/$VM_WORKSPACE}"
  mkdir -p "$capture_dir"
  cancel_stale_runs
  token=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)
  start_mitm "$capture_dir"
  log "MITM started on $VM for $kind"

  local wf_label
  wf_label=$(gh api repos/$GH_REPO/contents/.github/workflows/$SCENARIO --jq .content 2>/dev/null | base64 -d | grep "runs-on:" | head -1 | sed "s/.*\[//;s/\].*//" | tr "," "\n" | grep -v self-hosted | head -1 | tr -d " " )
  [ -z "$wf_label" ] && wf_label="mitm"
  local runner_name="direct-${kind}-${SCENARIO%.yml}-$(date +%s)"
  local root="/tmp/direct-${kind}-$(date +%s)"
  local proxy="http://127.0.0.1:${MITM_PORT}"
  local ca_bundle="$vm_capture_dir/vm-mitm-conf/mitmproxy-ca-cert.pem"

  if [ "$kind" = "aksh" ]; then
    smolvm machine exec --name "$VM" -- bash -lc "
      set -euo pipefail
      export HTTP_PROXY='$proxy' HTTPS_PROXY='$proxy' http_proxy='$proxy' https_proxy='$proxy' NO_PROXY='' no_proxy=''
      export NODE_EXTRA_CA_CERTS='$ca_bundle' SSL_CERT_FILE='$ca_bundle'
      rm -rf '$root'; mkdir -p '$root'
      RUST_LOG=info '$AKSH_RUNNER' --ca-bundle '$ca_bundle' --runner-root '$root' configure \
        --url 'https://github.com/$GH_REPO' --token '$token' --name '$runner_name' \
        --unattended --replace --ephemeral --labels "self-hosted,linux,x64,$wf_label" 2>&1
      RUST_LOG=info '$AKSH_RUNNER' --ca-bundle '$ca_bundle' --runner-root '$root' run --once 2>&1
    " > "$capture_dir/vm-runner.log" 2>&1 &
  else
    smolvm machine exec --name "$VM" -- bash -lc "
      set -euo pipefail
      export HTTP_PROXY='$proxy' HTTPS_PROXY='$proxy' http_proxy='$proxy' https_proxy='$proxy' NO_PROXY='' no_proxy=''
      export GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1 NODE_EXTRA_CA_CERTS='$ca_bundle' SSL_CERT_FILE='$ca_bundle'
      export RUNNER_ALLOW_RUNASROOT=1
      rm -rf '$root'; mkdir -p '$root'
      cd '$OFFICIAL_SRC'
      RUNNER_ALLOW_RUNASROOT=1 ./config.sh remove --unattended 2>/dev/null || rm -f .runner
      ./config.sh --unattended --url 'https://github.com/$GH_REPO' --token '$token' \
        --name '$runner_name' --labels "self-hosted,linux,x64,$wf_label" --work '$root/_work' --replace --ephemeral 2>&1
      timeout 900 ./run.sh --once 2>&1
    " > "$capture_dir/vm-runner.log" 2>&1 &
  fi
  local runner_pid=$!

  log "Waiting for runner to register..."
  sleep 30
  gh workflow run "$SCENARIO" -R "$GH_REPO" --ref "$GH_REF" >/dev/null
  sleep 3
  run_id=$(gh run list -R "$GH_REPO" -w "$SCENARIO" --json databaseId -q '.[0].databaseId')
  echo "$run_id" > "$capture_dir/github-run-id.txt"
  log "$kind dispatched $SCENARIO as $run_id"
  wait_for_run "$run_id" || true
  wait "$runner_pid" >/dev/null 2>&1 || true
  stop_mitm "$capture_dir"
  # Copy flows from VM (mount sync unreliable with smolvm)
  smolvm machine exec --name "$VM" -- bash -lc "tar -C '$vm_capture_dir' -cf - ." 2>/dev/null | tar -C "$capture_dir" -xf - 2>/dev/null || true

  conclusion=$(gh run view "$run_id" -R "$GH_REPO" --json conclusion -q '.conclusion' 2>/dev/null || echo unknown)
  flow_count=0
  [ -f "$capture_dir/vm-mitm/flows.jsonl" ] && flow_count=$(wc -l < "$capture_dir/vm-mitm/flows.jsonl")
  cp "$capture_dir/vm-mitm/flows.jsonl" "$capture_dir/flows.jsonl" 2>/dev/null || true
  gh run view "$run_id" -R "$GH_REPO" --json jobs > "$capture_dir/jobs.json" 2>/dev/null || true
  cat > "$capture_dir/summary.json" <<JSON
{
  "runner": "$kind",
  "scenario": "$SCENARIO",
  "github_repo": "$GH_REPO",
  "run_id": "$run_id",
  "status": "ok",
  "conclusion": "$conclusion",
  "total_ms": 0,
  "flows_count": $flow_count
}
JSON
  local latest="$RESULTS_ROOT/${SCENARIO%.yml}/$kind/latest"
  rm -f "$latest"
  ln -s "$capture_dir" "$latest"
  log "$kind done: conclusion=$conclusion flows=$flow_count"
}

case "$RUNNER_KIND" in
  official) capture_one official ;;
  aksh) capture_one aksh ;;
  both) capture_one official || log "WARN: official failed, continuing with aksh"; sleep 10; capture_one aksh ;;
  *) echo "unknown: $RUNNER_KIND" >&2; exit 1 ;;
esac
