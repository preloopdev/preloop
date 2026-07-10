#!/usr/bin/env bash
# Capture a multi-job workflow with one GitHub-ephemeral runner per job.
# Usage: ./ephemeral-capture.sh <workflow-yml> <vm-name> <official|aksh> [runner-count]
set -euo pipefail

SCENARIO="${1:?workflow yaml required}"
VM="${2:?VM name required}"
RUNNER_KIND="${3:?official or aksh required}"
RUNNER_COUNT="${4:-3}"
GH_REPO="${GH_REPO:-preloopdev/aksh-conformance-sample}"
GH_REF="${GH_REF:-main}"
MITM_PORT="${MITM_PORT:-18081}"
HOST_WORKSPACE="$PWD"
VM_WORKSPACE="/workspace"
RESULTS_ROOT="${RESULTS_ROOT:-$PWD/benchmarks/real-world/results/runner-flow}"
AKSH_RUNNER="/usr/local/bin/aksh-runner"
OFFICIAL_SRC="/opt/runners/actions-runner"
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

SCENARIO_DIR="$RESULTS_ROOT/${SCENARIO%.yml}"
TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
CAPTURE_DIR="$SCENARIO_DIR/$RUNNER_KIND/$TS"
VM_CAPTURE_DIR="${CAPTURE_DIR/#$HOST_WORKSPACE/$VM_WORKSPACE}"
mkdir -p "$CAPTURE_DIR"

cancel_stale_runs
TOKEN=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)
WF_LABEL=$(gh api "repos/$GH_REPO/contents/.github/workflows/$SCENARIO" --jq .content 2>/dev/null |
  base64 -d | grep "runs-on:" | head -1 | sed "s/.*\[//;s/\].*//" | tr "," "\n" | grep -v self-hosted | head -1 | tr -d " ")
[ -z "$WF_LABEL" ] && WF_LABEL=mitm
PROXY="http://127.0.0.1:$MITM_PORT"
CA_BUNDLE="$VM_CAPTURE_DIR/vm-mitm-conf/mitmproxy-ca-cert.pem"

smolvm machine exec --name "$VM" -- bash -lc "
  set -euo pipefail
  pkill -x mitmdump >/dev/null 2>&1 || true
  pkill -x Runner.Listener >/dev/null 2>&1 || true
  pkill -x aksh-runner >/dev/null 2>&1 || true
  rm -rf /tmp/ephemeral-official-* /tmp/ephemeral-aksh-*
  rm -rf '$VM_CAPTURE_DIR/vm-mitm' '$VM_CAPTURE_DIR/vm-mitm-conf'
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
log "MITM started for $RUNNER_KIND; launching $RUNNER_COUNT ephemeral runners"

# Each official runner needs its own installation/config directory. Aksh only needs a distinct root.
for i in $(seq 1 "$RUNNER_COUNT"); do
  NAME="ephemeral-${RUNNER_KIND}-${SCENARIO%.yml}-${i}-$(date +%s)"
  ROOT="/tmp/ephemeral-${RUNNER_KIND}-${SCENARIO%.yml}-${i}-$(date +%s)"
  INSTALL="/tmp/ephemeral-official-${SCENARIO%.yml}-${i}-$(date +%s)"
  if [ "$RUNNER_KIND" = aksh ]; then
    smolvm machine exec --name "$VM" -- bash -lc "
      set -euo pipefail
      export HTTP_PROXY='$PROXY' HTTPS_PROXY='$PROXY' http_proxy='$PROXY' https_proxy='$PROXY' NO_PROXY='' no_proxy=''
      export NODE_EXTRA_CA_CERTS='$CA_BUNDLE' SSL_CERT_FILE='$CA_BUNDLE'
      rm -rf '$ROOT'; mkdir -p '$ROOT'
      RUST_LOG=info '$AKSH_RUNNER' --ca-bundle '$CA_BUNDLE' --runner-root '$ROOT' configure \
        --url 'https://github.com/$GH_REPO' --token '$TOKEN' --name '$NAME' \
        --unattended --replace --ephemeral --labels 'self-hosted,linux,x64,$WF_LABEL' 2>&1
      RUST_LOG=info '$AKSH_RUNNER' --ca-bundle '$CA_BUNDLE' --runner-root '$ROOT' run 2>&1
    " >> "$CAPTURE_DIR/vm-runner.log" 2>&1 &
  else
    smolvm machine exec --name "$VM" -- bash -lc "
      set -euo pipefail
      export HTTP_PROXY='$PROXY' HTTPS_PROXY='$PROXY' http_proxy='$PROXY' https_proxy='$PROXY' NO_PROXY='' no_proxy=''
      export GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1 NODE_EXTRA_CA_CERTS='$CA_BUNDLE' SSL_CERT_FILE='$CA_BUNDLE' RUNNER_ALLOW_RUNASROOT=1
      rm -rf '$INSTALL' '$ROOT'; cp -a '$OFFICIAL_SRC' '$INSTALL'; rm -f '$INSTALL/.runner' '$INSTALL/.credentials' '$INSTALL/.credentials_rsaparams'; mkdir -p '$ROOT/_work'
      cd '$INSTALL'
      RUNNER_ALLOW_RUNASROOT=1 ./config.sh --unattended --url 'https://github.com/$GH_REPO' --token '$TOKEN' \
        --name '$NAME' --labels 'self-hosted,linux,x64,$WF_LABEL' --work '$ROOT/_work' --replace --ephemeral 2>&1
      timeout 1200 ./run.sh 2>&1
    " >> "$CAPTURE_DIR/vm-runner.log" 2>&1 &
  fi
done

sleep 35
gh workflow run "$SCENARIO" -R "$GH_REPO" --ref "$GH_REF" >/dev/null
sleep 3
RUN_ID=$(gh run list -R "$GH_REPO" -w "$SCENARIO" --json databaseId -q '.[0].databaseId')
echo "$RUN_ID" > "$CAPTURE_DIR/github-run-id.txt"
log "$RUNNER_KIND dispatched $SCENARIO as $RUN_ID"
wait_for_run "$RUN_ID" || true

smolvm machine exec --name "$VM" -- bash -lc "
  pkill -x Runner.Listener >/dev/null 2>&1 || true
  pkill -x aksh-runner >/dev/null 2>&1 || true
  if [ -f '$VM_CAPTURE_DIR/vm-mitm.pid' ]; then kill -INT \$(cat '$VM_CAPTURE_DIR/vm-mitm.pid') >/dev/null 2>&1 || true; fi
  sleep 1
  pkill -x mitmdump >/dev/null 2>&1 || true
" >/dev/null 2>&1 || true
smolvm machine exec --name "$VM" -- bash -lc "tar -C '$VM_CAPTURE_DIR' -cf - ." 2>/dev/null | tar -C "$CAPTURE_DIR" -xf - 2>/dev/null || true
smolvm machine exec --name "$VM" -- bash -lc "rm -rf /tmp/ephemeral-official-* /tmp/ephemeral-aksh-*" >/dev/null 2>&1 || true

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
