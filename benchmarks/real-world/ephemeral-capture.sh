#!/usr/bin/env bash
# Capture a multi-job workflow with one VM per runner (GitHub-faithful isolation).
# Usage: ./ephemeral-capture.sh <workflow-yml> <vm-template.smolmachine> <official|aksh> [runner-count]
#
# Each runner gets its own isolated VM created from the packed template.
# The template VM must have mitmdump and (for aksh) aksh-runner pre-installed.
# The official runner source is mounted read-only and copied locally in each VM.
set -euo pipefail

SCENARIO="${1:?workflow yaml required}"
TEMPLATE="${2:?path to .smolmachine template required}"
RUNNER_KIND="${3:?official or aksh required}"
RUNNER_COUNT="${4:-3}"
GH_REPO="${GH_REPO:-preloopdev/aksh-conformance-sample}"
GH_REF="${GH_REF:-main}"
MITM_PORT="${MITM_PORT:-18081}"
HOST_WORKSPACE="$PWD"
RESULTS_ROOT="${RESULTS_ROOT:-$PWD/benchmarks/real-world/results/runner-flow}"
AKSH_RUNNER="/usr/local/bin/aksh-runner"
# Host path to the official runner install (mounted read-only into each VM)
OFFICIAL_RUNNER_HOST="${OFFICIAL_RUNNER_HOST:-/Users/bnjoroge/cachingv4}"
OFFICIAL_SRC="/opt/runners/actions-runner"   # guest mount point
MITM_ADDON="/workspace/experiments/mitm/addons/capture.py"

log() { echo "[$(date -u +%H:%M:%S.%3NZ)] $*"; }

cancel_stale_runs() {
  gh run list -R "$GH_REPO" -L 30 --json databaseId,status \
    -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null |
    while read -r rid; do [ -n "$rid" ] && gh run cancel "$rid" -R "$GH_REPO" >/dev/null 2>&1 || true; done
}

wait_for_run() {
  local run_id="$1" deadline status
  deadline=$(($(date +%s) + 1200))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    status=$(gh run view "$run_id" -R "$GH_REPO" --json status -q '.status' 2>/dev/null || echo unknown)
    [ "$status" = completed ] && return 0
    sleep 5
  done
  return 1
}

# Resolve template path
if [[ "$TEMPLATE" != *.smolmachine ]]; then
  if [ -f "${TEMPLATE}.smolmachine" ]; then
    TEMPLATE="${TEMPLATE}.smolmachine"
  else
    echo "ERROR: template must be a .smolmachine file (got: $TEMPLATE)" >&2
    exit 1
  fi
fi
[ -f "$TEMPLATE" ] || { echo "ERROR: template not found: $TEMPLATE" >&2; exit 1; }

SCENARIO_DIR="$RESULTS_ROOT/${SCENARIO%.yml}"
TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
CAPTURE_DIR="$SCENARIO_DIR/$RUNNER_KIND/$TS"
mkdir -p "$CAPTURE_DIR"

cancel_stale_runs
TOKEN=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)
WF_LABEL=$(gh api "repos/$GH_REPO/contents/.github/workflows/$SCENARIO" --jq .content 2>/dev/null |
  base64 -d | grep "runs-on:" | head -1 | sed "s/.*\[//;s/\].*//" | tr "," "\n" | grep -v self-hosted | head -1 | tr -d " ")
[ -z "$WF_LABEL" ] && WF_LABEL=mitm
PROXY="http://127.0.0.1:$MITM_PORT"

VM_NAMES=()

cleanup_vms() {
  log "Cleaning up ${#VM_NAMES[@]} VMs..."
  for vm in "${VM_NAMES[@]}"; do
    smolvm machine stop --name "$vm" >/dev/null 2>&1 || true
    smolvm machine delete --name "$vm" -f >/dev/null 2>&1 || true
  done
}
trap cleanup_vms EXIT

# Spawn one VM per runner
log "Creating $RUNNER_COUNT VMs from $TEMPLATE for $RUNNER_KIND capture"
for i in $(seq 1 "$RUNNER_COUNT"); do
  VM_NAME="cap-${RUNNER_KIND}-${SCENARIO%.yml}-${i}-$$"
  VM_NAMES+=("$VM_NAME")

  # Create VM with mounts: workspace + official runner source (read-only)
  smolvm machine create --name "$VM_NAME" --from "$TEMPLATE" \
    --net -v "$HOST_WORKSPACE:/workspace" -v "${OFFICIAL_RUNNER_HOST}:/opt/runners:ro" >/dev/null 2>&1
  smolvm machine start --name "$VM_NAME" >/dev/null 2>&1
  log "VM $VM_NAME started (runner $i/$RUNNER_COUNT)"
done

# Configure and start each runner
RUNNER_NAME_PREFIX="ephemeral-${RUNNER_KIND}-${SCENARIO%.yml}"
for i in $(seq 1 "$RUNNER_COUNT"); do
  VM_NAME="${VM_NAMES[$((i-1))]}"
  NAME="${RUNNER_NAME_PREFIX}-${i}-$$"
  VM_CAPTURE_DIR="/workspace/${CAPTURE_DIR#$HOST_WORKSPACE/}/runner-${i}"

  # Start MITM proxy
  smolvm machine exec --name "$VM_NAME" -- bash -lc "
    set -euo pipefail
    mkdir -p '$VM_CAPTURE_DIR/vm-mitm' '$VM_CAPTURE_DIR/vm-mitm-conf'
    nohup env MITM_CAPTURE_DIR='$VM_CAPTURE_DIR/vm-mitm' mitmdump \
      --listen-host 127.0.0.1 --listen-port '$MITM_PORT' \
      --set confdir='$VM_CAPTURE_DIR/vm-mitm-conf' -s '$MITM_ADDON' \
      > '$VM_CAPTURE_DIR/vm-mitm.log' 2>&1 < /dev/null &
    echo \$! > '$VM_CAPTURE_DIR/vm-mitm.pid'
    for n in \$(seq 1 40); do
      if bash -c '</dev/tcp/127.0.0.1/$MITM_PORT' >/dev/null 2>&1; then exit 0; fi
      sleep .25
    done
    exit 3
  "
  CA_BUNDLE="$VM_CAPTURE_DIR/vm-mitm-conf/mitmproxy-ca-cert.pem"

  # Start runner (backgrounded smolvm exec)
  if [ "$RUNNER_KIND" = aksh ]; then
    smolvm machine exec --name "$VM_NAME" -- bash -lc "
      set -euo pipefail
      export HTTP_PROXY='$PROXY' HTTPS_PROXY='$PROXY' http_proxy='$PROXY' https_proxy='$PROXY' NO_PROXY='' no_proxy=''
      export NODE_EXTRA_CA_CERTS='$CA_BUNDLE' SSL_CERT_FILE='$CA_BUNDLE'
      ROOT='/tmp/runner-root'
      rm -rf \$ROOT; mkdir -p \$ROOT
      RUST_LOG=info '$AKSH_RUNNER' --ca-bundle '$CA_BUNDLE' --runner-root \$ROOT configure \
        --url 'https://github.com/$GH_REPO' --token '$TOKEN' --name '$NAME' \
        --unattended --replace --ephemeral --labels 'self-hosted,linux,x64,$WF_LABEL' 2>&1
      RUST_LOG=info '$AKSH_RUNNER' --ca-bundle '$CA_BUNDLE' --runner-root \$ROOT run 2>&1
    " >> "$CAPTURE_DIR/vm-runner-${i}.log" 2>&1 &
  else
    smolvm machine exec --name "$VM_NAME" -- bash -lc "
      set -euo pipefail
      export HTTP_PROXY='$PROXY' HTTPS_PROXY='$PROXY' http_proxy='$PROXY' https_proxy='$PROXY' NO_PROXY='' no_proxy=''
      export GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1 NODE_EXTRA_CA_CERTS='$CA_BUNDLE' SSL_CERT_FILE='$CA_BUNDLE' RUNNER_ALLOW_RUNASROOT=1
      # Copy official runner locally (mounted read-only at $OFFICIAL_SRC)
      cp -a '$OFFICIAL_SRC' /tmp/runner-install
      rm -f /tmp/runner-install/.runner /tmp/runner-install/.credentials /tmp/runner-install/.credentials_rsaparams
      rm -rf /tmp/runner-install/_work; mkdir -p /tmp/runner-install/_work
      cd /tmp/runner-install
      RUNNER_ALLOW_RUNASROOT=1 ./config.sh --unattended --url 'https://github.com/$GH_REPO' --token '$TOKEN' \
        --name '$NAME' --labels 'self-hosted,linux,x64,$WF_LABEL' --work _work --replace --ephemeral 2>&1
      rm -rf _work; mkdir -p _work
      timeout 1200 ./run.sh 2>&1
    " >> "$CAPTURE_DIR/vm-runner-${i}.log" 2>&1 &
  fi
  log "Runner $i ($NAME) launched in VM $VM_NAME"
done

# Wait for runners to register, then dispatch workflow
sleep 35
gh workflow run "$SCENARIO" -R "$GH_REPO" --ref "$GH_REF" >/dev/null
sleep 3
RUN_ID=$(gh run list -R "$GH_REPO" -w "$SCENARIO" --json databaseId -q '.[0].databaseId')
echo "$RUN_ID" > "$CAPTURE_DIR/github-run-id.txt"
log "$RUNNER_KIND dispatched $SCENARIO as $RUN_ID"
wait_for_run "$RUN_ID" || true

# Kill runners that never got jobs (ephemeral runners poll forever if no job arrives)
log "Stopping all runners across $RUNNER_COUNT VMs"
for i in $(seq 1 "$RUNNER_COUNT"); do
  VM_NAME="${VM_NAMES[$((i-1))]}"
  smolvm machine exec --name "$VM_NAME" -- bash -lc "
    pkill -x Runner.Listener >/dev/null 2>&1 || true
    pkill -x aksh-runner >/dev/null 2>&1 || true
  " >/dev/null 2>&1 &
done
wait 2>/dev/null || true

# Collect captures from each VM
log "Collecting captures from $RUNNER_COUNT VMs"
mkdir -p "$CAPTURE_DIR/vm-mitm"
for i in $(seq 1 "$RUNNER_COUNT"); do
  VM_NAME="${VM_NAMES[$((i-1))]}"
  VM_RUNNER_CAPTURE="/workspace/${CAPTURE_DIR#$HOST_WORKSPACE/}/runner-${i}"
  # Stop remaining processes
  smolvm machine exec --name "$VM_NAME" -- bash -lc "
    pkill -x Runner.Listener >/dev/null 2>&1 || true
    pkill -x aksh-runner >/dev/null 2>&1 || true
    if [ -f '$VM_RUNNER_CAPTURE/vm-mitm.pid' ]; then kill -INT \$(cat '$VM_RUNNER_CAPTURE/vm-mitm.pid') >/dev/null 2>&1 || true; fi
    sleep 1
    pkill -x mitmdump >/dev/null 2>&1 || true
  " >/dev/null 2>&1 || true
  log "Collected runner-${i} capture from $VM_NAME"
done

# Merge per-runner flows into single capture (files are already on the host via mount)
for i in $(seq 1 "$RUNNER_COUNT"); do
  RUNNER_FLOWS="$CAPTURE_DIR/runner-${i}/vm-mitm/flows.jsonl"
  [ -f "$RUNNER_FLOWS" ] && cat "$RUNNER_FLOWS" >> "$CAPTURE_DIR/vm-mitm/flows.jsonl"
done
# Merge per-runner logs
cat "$CAPTURE_DIR"/vm-runner-*.log > "$CAPTURE_DIR/vm-runner.log" 2>/dev/null || true

CONCLUSION=$(gh run view "$RUN_ID" -R "$GH_REPO" --json conclusion -q '.conclusion' 2>/dev/null || echo unknown)
FLOW_COUNT=0
[ -f "$CAPTURE_DIR/vm-mitm/flows.jsonl" ] && FLOW_COUNT=$(wc -l < "$CAPTURE_DIR/vm-mitm/flows.jsonl")
cp "$CAPTURE_DIR/vm-mitm/flows.jsonl" "$CAPTURE_DIR/flows.jsonl" 2>/dev/null || true
gh run view "$RUN_ID" -R "$GH_REPO" --json jobs > "$CAPTURE_DIR/jobs.json" 2>/dev/null || true
cat > "$CAPTURE_DIR/summary.json" <<JSON
{"runner":"$RUNNER_KIND","scenario":"$SCENARIO","github_repo":"$GH_REPO","run_id":"$RUN_ID","status":"ok","conclusion":"$CONCLUSION","total_ms":0,"flows_count":$FLOW_COUNT}
JSON
rm -f "$SCENARIO_DIR/$RUNNER_KIND/latest"
ln -s "$TS" "$SCENARIO_DIR/$RUNNER_KIND/latest"
log "$RUNNER_KIND done: conclusion=$CONCLUSION flows=$FLOW_COUNT dir=$CAPTURE_DIR"
