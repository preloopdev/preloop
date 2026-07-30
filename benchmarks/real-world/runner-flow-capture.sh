#!/usr/bin/env bash
# Capture official-runner vs aksh-runner HTTP flows against GitHub.
# Invariant: every workflow job runs in its own smolvm. Each VM runs its own
# local mitmproxy on 127.0.0.1, so runner traffic is captured without relying on
# host<->guest proxy reachability.
set -euo pipefail

SCENARIO="${1:?Usage: $0 <workflow-yml> [official|aksh|both] [job-count]}"
RUNNER_KIND="${2:-both}"
JOB_COUNT="${3:-1}"

GH_REPO="${GH_REPO:-preloopdev/aksh-conformance-sample}"
GH_REF="${GH_REF:-main}"
VM_PREFIX="${VM_PREFIX:-bench-aksh}"
VM_CPUS="${VM_CPUS:-4}"
VM_MEM="${VM_MEM:-8192}"
MITM_PORT="${MITM_PORT:-18081}"
HOST_WORKSPACE="$PWD"
VM_WORKSPACE="/workspace"
RESULTS_ROOT="${RESULTS_ROOT:-$PWD/benchmarks/compatibility/runner/protocol}"
AKSH_RUNNER="${AKSH_RUNNER:-/workspace/target/aarch64-unknown-linux-musl/release/preloop-runner}"
OFFICIAL_SRC="${OFFICIAL_SRC:-/opt/runners/actions-runner}"
OFFICIAL_RUNNER_HOST="${OFFICIAL_RUNNER_HOST:-$HOME/cachingv4}"
MITM_ADDON="/workspace/experiments/mitm/addons/capture.py"

log() { echo "[$(date -u +%H:%M:%S.%3NZ)] $*"; }
ms() { python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
}

ensure_vms() {
  for i in $(seq 1 "$JOB_COUNT"); do
    local vm="${VM_PREFIX}-${i}"
    if ! smolvm machine status --name "$vm" >/dev/null 2>&1; then
      log "Creating $vm (${VM_CPUS} CPU, ${VM_MEM} MiB)"
      smolvm machine create --name "$vm" --image ubuntu:24.04 --cpus "$VM_CPUS" --mem "$VM_MEM" --storage 20 --net >/dev/null
      smolvm machine update --name "$vm" --volume "$PWD:/workspace" >/dev/null
      smolvm machine update --name "$vm" --volume "$OFFICIAL_RUNNER_HOST:/opt/runners" >/dev/null || true
    fi
  done
}

cancel_stale_runs() {
  gh run list -R "$GH_REPO" -L 30 --json databaseId,status \
    -q '.[] | select(.status == "queued" or .status == "in_progress") | .databaseId' 2>/dev/null \
    | while read -r rid; do [ -n "$rid" ] && gh run cancel "$rid" -R "$GH_REPO" >/dev/null 2>&1 || true; done
  for _ in $(seq 1 12); do
    local pending
    pending=$(gh run list -R "$GH_REPO" -L 30 --json status \
      -q '[.[] | select(.status == "queued" or .status == "in_progress")] | length' 2>/dev/null || echo 0)
    [ "$pending" -eq 0 ] && break
    sleep 5
  done
  gh api "repos/$GH_REPO/actions/runners" --jq '.runners[] | select(.status == "offline" and (.name | startswith("flow-"))) | .id' 2>/dev/null \
    | while read -r runner_id; do
        [ -n "$runner_id" ] && gh api -X DELETE "repos/$GH_REPO/actions/runners/$runner_id" >/dev/null 2>&1 || true
      done
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

wait_for_run_started() {
  local run_id="$1" deadline status
  deadline=$(($(date +%s) + 300))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    status=$(gh run view "$run_id" -R "$GH_REPO" --json status -q '.status' 2>/dev/null || echo unknown)
    [ "$status" = "in_progress" ] && return 0
    [ "$status" = "completed" ] && return 1
    sleep 2
  done
  return 1
}

prepare_vm() {
  local vm="$1"
  # smolvm stop/start preserves the guest process tree on some hosts. Kill
  # stale runner children explicitly before recycling the VM, otherwise an
  # old --once runner can retain a busy GitHub registration and steal the next
  # workflow job from the fresh capture process.
  smolvm machine exec --name "$vm" -- sh -c 'pkill -x aksh-runner >/dev/null 2>&1 || true; pkill -x Runner.Listener >/dev/null 2>&1 || true' >/dev/null 2>&1 || true
  smolvm machine stop --name "$vm" >/dev/null 2>&1 || true
  sleep 0.3
  smolvm machine start --name "$vm" >/dev/null 2>&1
  smolvm machine exec --name "$vm" -- bash -lc "
    set -euo pipefail
    # Wait up to 30s for mounts to appear
    for i in \$(seq 1 30); do
      if [ -x '$AKSH_RUNNER' ] && [ -d '$OFFICIAL_SRC' ]; then
        break
      fi
      sleep 1
    done

    export DEBIAN_FRONTEND=noninteractive
    if [ -f '$OFFICIAL_SRC/bin/installdependencies.sh' ]; then
      apt-get update -qq
      bash '$OFFICIAL_SRC/bin/installdependencies.sh' >/dev/null 2>&1 || true
    fi
    if ! command -v mitmdump >/dev/null 2>&1; then
      apt-get update -qq
      apt-get install -y -qq python3-pip >/dev/null
      python3 -m pip install --break-system-packages -q mitmproxy==12.2.3
    fi
  "
}

start_vm_mitm() {
  local vm="$1" capture_dir="$2"
  smolvm machine exec --name "$vm" -- bash -lc "
    set -euo pipefail
    pkill -x mitmdump >/dev/null 2>&1 || true
    rm -rf '$capture_dir/vm-mitm'
    mkdir -p '$capture_dir/vm-mitm' '$capture_dir/vm-mitm-conf'
    nohup env MITM_CAPTURE_DIR='$capture_dir/vm-mitm' mitmdump \
      --listen-host 127.0.0.1 --listen-port '$MITM_PORT' \
      --set confdir='$capture_dir/vm-mitm-conf' \
      -s '$MITM_ADDON' > '$capture_dir/vm-mitm.log' 2>&1 < /dev/null &
    echo \$! > '$capture_dir/vm-mitm.pid'
    for n in \$(seq 1 40); do
      if bash -c '</dev/tcp/127.0.0.1/$MITM_PORT' >/dev/null 2>&1; then exit 0; fi
      sleep 0.25
    done
    cat '$capture_dir/vm-mitm.log' >&2 || true
    exit 3
  "
}

stop_vm_mitm() {
  local vm="$1" capture_dir="$2"
  smolvm machine exec --name "$vm" -- bash -lc "
    if [ -f '$capture_dir/vm-mitm.pid' ]; then
      kill -INT \$(cat '$capture_dir/vm-mitm.pid') >/dev/null 2>&1 || true
      sleep 1
      kill \$(cat '$capture_dir/vm-mitm.pid') >/dev/null 2>&1 || true
    fi
    pkill -x mitmdump >/dev/null 2>&1 || true
  " >/dev/null 2>&1 || true
}

start_vm_runner() {
  local runner_kind="$1" i="$2" host_capture_dir="$3" vm_capture_dir="$4" token="$5"
  local vm="${VM_PREFIX}-${i}"
  local runner_name="flow-${runner_kind}-${SCENARIO%.yml}-${i}-$(date +%s)-$$"
  runner_name="${runner_name:0:60}"
  local root="/root/flow-${runner_kind}-${i}"
  local proxy="http://127.0.0.1:${MITM_PORT}"
  local ca_bundle="$vm_capture_dir/vm-mitm-conf/mitmproxy-ca-cert.pem"
  local vm_log="$host_capture_dir/vm-${i}.log"
  local once_arg="--once"
  local ephemeral_arg="--ephemeral"
  local runner_prefix=""
  local runner_suffix=""
  if [[ "$SCENARIO" == "103-cancellation-background-post.yml" ]]; then
    once_arg=""
    runner_prefix="nohup env"
    runner_suffix="> '$root/runner.log' 2>&1 < /dev/null &"
  fi

  prepare_vm "$vm"
  start_vm_mitm "$vm" "$vm_capture_dir"
  log "Starting $runner_kind runner $i/$JOB_COUNT on $vm"

  if [ "$runner_kind" = "aksh" ]; then
    smolvm machine exec --name "$vm" -- bash -lc "
      set -euo pipefail
      export PATH=/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
      export HTTP_PROXY='$proxy' HTTPS_PROXY='$proxy' http_proxy='$proxy' https_proxy='$proxy' NO_PROXY='' no_proxy=''
      export NODE_EXTRA_CA_CERTS='$ca_bundle' SSL_CERT_FILE='$ca_bundle'
      mkdir -p '$root'
      RUST_LOG=info '$AKSH_RUNNER' --ca-bundle '$ca_bundle' --runner-root '$root' configure \
        --url 'https://github.com/$GH_REPO' --token '$token' --name '$runner_name' \
        --unattended --replace $ephemeral_arg --labels self-hosted,linux,x64,mitm,overnight
      $runner_prefix RUST_LOG=info '$AKSH_RUNNER' --ca-bundle '$ca_bundle' --runner-root '$root' run $once_arg $runner_suffix
    " > "$vm_log" 2>&1 &
  else
    smolvm machine exec --name "$vm" -- bash -lc "
      set -euo pipefail
      export PATH=/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
      export HTTP_PROXY='$proxy' HTTPS_PROXY='$proxy' http_proxy='$proxy' https_proxy='$proxy' NO_PROXY='' no_proxy=''
      export GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1 NODE_EXTRA_CA_CERTS='$ca_bundle' SSL_CERT_FILE='$ca_bundle'
      rm -rf '$root'; mkdir -p '$root/bin'
      cp -a '$OFFICIAL_SRC'/ '$root/bin/actions-runner'
      cd '$root/bin/actions-runner'
      export RUNNER_ALLOW_RUNASROOT=1
      ./config.sh --unattended --url 'https://github.com/$GH_REPO' --token '$token' \
        --name '$runner_name' --labels self-hosted,linux,x64,mitm,overnight --work '$root/_work' --replace --ephemeral
      timeout 900 ./run.sh $once_arg
    " > "$vm_log" 2>&1 &
  fi
  echo $!
}

capture_one() {
  local runner_kind="$1"
  local ts capture_dir vm_capture_dir token t0 t1 run_id conclusion flow_count
  local cancelled_run_id="" replacement_run_id=""
  ts=$(date -u +%Y-%m-%dT%H-%M-%SZ)
  capture_dir="$RESULTS_ROOT/${SCENARIO%.yml}/$runner_kind/$ts"
  mkdir -p "$capture_dir"
  cancel_stale_runs
  token=$(gh api "repos/$GH_REPO/actions/runners/registration-token" --method POST --jq .token)
  t0=$(ms)
  local pids=()
  for i in $(seq 1 "$JOB_COUNT"); do
    vm_capture_dir="${capture_dir/#$HOST_WORKSPACE/$VM_WORKSPACE}/vm-$i"
    pids+=("$(start_vm_runner "$runner_kind" "$i" "$capture_dir" "$vm_capture_dir" "$token")")
    sleep 1
  done
  log "Waiting for runners to register"
  sleep 15
  # Reap registrations that became offline only after VM process cleanup.
  gh api "repos/$GH_REPO/actions/runners" --jq '.runners[] | select(.status == "offline" and (.name | startswith("flow-"))) | .id' 2>/dev/null \
    | while read -r runner_id; do
        [ -n "$runner_id" ] && gh api -X DELETE "repos/$GH_REPO/actions/runners/$runner_id" >/dev/null 2>&1 || true
      done
  if [[ "$SCENARIO" == 103-cancellation-background-post.yml ]]; then
    gh workflow run "$SCENARIO" -R "$GH_REPO" --ref "$GH_REF" -f slot=default >/dev/null
  else
    gh workflow run "$SCENARIO" -R "$GH_REPO" --ref "$GH_REF" >/dev/null
  fi
  sleep 3
  run_id=$(gh run list -R "$GH_REPO" -w "$SCENARIO" --json databaseId -q '.[0].databaseId')
  # This workflow intentionally exercises concurrency cancellation. Start a
  # replacement run in the same concurrency group so GitHub cancels the first
  # run and the second runner receives the cancellation lifecycle messages.
  if [[ "$SCENARIO" == 103-cancellation-background-post.yml ]]; then
    cancelled_run_id="$run_id"
    if ! wait_for_run_started "$cancelled_run_id"; then
      echo "initial cancellation run did not start" >&2
      return 1
    fi
    log "$runner_kind starting replacement run for concurrency cancellation"
    gh workflow run "$SCENARIO" -R "$GH_REPO" --ref "$GH_REF" -f slot=default >/dev/null
    sleep 3
    replacement_run_id=$(gh run list -R "$GH_REPO" -w "$SCENARIO" --json databaseId -q '.[0].databaseId')
    if [[ "$replacement_run_id" == "$cancelled_run_id" ]]; then
      echo "replacement workflow run was not observed" >&2
      return 1
    fi
    printf '%s\n' "$cancelled_run_id" > "$capture_dir/cancelled-github-run-id.txt"
    printf '%s\n' "$replacement_run_id" > "$capture_dir/replacement-github-run-id.txt"
    wait_for_run "$cancelled_run_id" || true
    wait_for_run "$replacement_run_id" || true
    local cancelled_conclusion
    cancelled_conclusion=$(gh run view "$cancelled_run_id" -R "$GH_REPO" --json conclusion -q '.conclusion' 2>/dev/null || echo unknown)
    if [[ "$cancelled_conclusion" != "cancelled" ]]; then
      echo "initial run concluded $cancelled_conclusion instead of cancelled" >&2
      return 1
    fi
    run_id="$cancelled_run_id"
  fi
  echo "$run_id" > "$capture_dir/github-run-id.txt"
  log "$runner_kind dispatched $SCENARIO as $run_id"
  wait_for_run "$run_id" || true
  if [[ "$SCENARIO" == "103-cancellation-background-post.yml" ]]; then
    for i in $(seq 1 "$JOB_COUNT"); do
      smolvm machine exec --name "${VM_PREFIX}-${i}" -- sh -c \
        'pkill -x aksh-runner >/dev/null 2>&1 || true; pkill -x Runner.Listener >/dev/null 2>&1 || true' \
        >/dev/null 2>&1 || true
      vm_log="$capture_dir/vm-${i}.log"
      smolvm machine exec --name "${VM_PREFIX}-${i}" -- sh -c \
        "cat /root/flow-aksh-${i}/runner.log 2>/dev/null || true" > "$vm_log" 2>/dev/null || true
    done
  fi
  for pid in "${pids[@]}"; do wait "$pid" >/dev/null 2>&1 || true; done
  for i in $(seq 1 "$JOB_COUNT"); do
    vm_capture_dir="${capture_dir/#$HOST_WORKSPACE/$VM_WORKSPACE}/vm-$i"
    stop_vm_mitm "${VM_PREFIX}-${i}" "$vm_capture_dir"
  done
  t1=$(ms)
  conclusion=$(gh run view "$run_id" -R "$GH_REPO" --json conclusion -q '.conclusion' 2>/dev/null || echo unknown)
  flow_count=0
  python3 - "$capture_dir" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
flows = []
for path in sorted(root.glob("vm-*/vm-mitm/flows.jsonl")):
    for line in path.read_text().splitlines():
        if line.strip():
            flows.append(json.loads(line))
flows.sort(key=lambda item: (item.get("ts_request") or 0, item.get("flow_index") or 0))
(root / "flows.jsonl").write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in flows))
print(len(flows))
PY
  flow_count=$(wc -l < "$capture_dir/flows.jsonl")
  gh run view "$run_id" -R "$GH_REPO" --json jobs > "$capture_dir/jobs.json" 2>/dev/null || true
  gh run view "$run_id" -R "$GH_REPO" --log > "$capture_dir/run.log" 2>/dev/null || true
  behavior_dir="$HOST_WORKSPACE/benchmarks/compatibility/runner/behavior"
  mkdir -p "$behavior_dir"
  python3 - "$capture_dir/jobs.json" "$runner_kind" "$SCENARIO" "$run_id" "$conclusion" \
    "$behavior_dir/conformance-${runner_kind}.jsonl" <<'PY'
import json
import sys
from pathlib import Path

jobs_path, runner, workflow, run_id, conclusion, output_path = sys.argv[1:]
try:
    jobs = json.loads(Path(jobs_path).read_text()).get("jobs", [])
except (FileNotFoundError, json.JSONDecodeError):
    jobs = []
result = {
    "conclusion": conclusion,
    "jobs": [
        {
            "name": job.get("name", ""),
            "conclusion": job.get("conclusion", ""),
            "steps": [
                {
                    "name": step.get("name", ""),
                    "conclusion": step.get("conclusion", ""),
                    "number": step.get("number"),
                }
                for step in job.get("steps", [])
            ],
        }
        for job in jobs
    ],
}
record = {"runner": runner, "workflow": workflow, "run_id": run_id, "conclusion": conclusion, "result": result}
with Path(output_path).open("a", encoding="utf-8") as stream:
    stream.write(json.dumps(record, ensure_ascii=False) + "\n")
PY
  cat > "$capture_dir/summary.json" <<JSON
{
  "runner": "$runner_kind",
  "scenario": "$SCENARIO",
  "github_repo": "$GH_REPO",
  "run_id": "$run_id",
  "status": "ok",
  "conclusion": "$conclusion",
  "job_count": $JOB_COUNT,
  "total_ms": $((t1 - t0)),
  "flows_count": $flow_count
}
JSON
  local latest="$RESULTS_ROOT/${SCENARIO%.yml}/$runner_kind/latest"
  rm -f "$latest"
  ln -s "$capture_dir" "$latest"
  log "$runner_kind done: conclusion=$conclusion flows=$flow_count dir=$capture_dir"
}

main() {
  ensure_vms
  case "$RUNNER_KIND" in
    official) capture_one official ;;
    aksh) capture_one aksh ;;
    both) capture_one official; sleep 10; capture_one aksh ;;
    *) echo "unknown runner kind: $RUNNER_KIND" >&2; exit 1 ;;
  esac
}

main
