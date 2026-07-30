#!/usr/bin/env bash
# Run one workflow with one official runner per job, each in an independent smolVM.
set -euo pipefail

SCENARIO="${1:?scenario basename without .yml}"
RUNNERS="${2:?number of runner VMs}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TEMPLATE="${TEMPLATE:-/private/tmp/bench-runner.smolmachine}"
RUNNER_HOST="${OFFICIAL_RUNNER_HOST:-$HOME/cachingv4}"
BIN="$ROOT/target/aarch64-unknown-linux-musl/release/preloop-server"
RESULT="$ROOT/benchmarks/compatibility/server/behavior/$SCENARIO/aksh-multi"
PAYLOAD="$ROOT/benchmarks/compatibility/server/behavior/payload-$SCENARIO.json"
mkdir -p "$RESULT/diag"
SERVER="multi-server-${SCENARIO}-$$"
VMS=()
cleanup() {
  for vm in "${VMS[@]:-}" "$SERVER"; do
    smolvm machine stop --name "$vm" >/dev/null 2>&1 || true
    smolvm machine delete --name "$vm" -f >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

smolvm machine create --name "$SERVER" --from "$TEMPLATE" --net-backend virtio-net --net \
  -v "$ROOT:/workspace" >/dev/null
smolvm machine update --name "$SERVER" --rosetta >/dev/null
smolvm machine start --name "$SERVER" >/dev/null
smolvm machine exec --name "$SERVER" -- bash -lc 'mount -t binfmt_misc binfmt_misc /proc/sys/fs/binfmt_misc 2>/dev/null || true; if [ -x /usr/bin/rosetta-wrapper ] && [ -x /mnt/rosetta/rosetta ]; then echo ":rosetta:M::\\x7fELF\\x02\\x01\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\\x00\\x3e\\x00:\\xff\\xff\\xff\\xff\\xff\\xfe\\xfe\\x00\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xfe\\xff\\xff\\xff:/usr/bin/rosetta-wrapper:F" > /proc/sys/fs/binfmt_misc/register 2>/dev/null || true; fi' >/dev/null 2>&1 || true
smolvm machine cp "$BIN" "$SERVER:/usr/local/bin/preloop-server" >/dev/null
SERVER_IP=$(smolvm machine exec --name "$SERVER" -- bash -lc "hostname -I | cut -d' ' -f1")
smolvm machine exec --name "$SERVER" -- bash -lc "
  chmod +x /usr/local/bin/preloop-server
  RUST_LOG=info AKSH_PUBLIC_URL=http://$SERVER_IP preloop-server serve --listen 0.0.0.0:80 > /tmp/server.log 2>&1 &
  sleep 2
  wget -qO- http://127.0.0.1/healthz >/dev/null
  RESULT=\$(wget -qO- --post-file=/workspace/benchmarks/compatibility/server/behavior/payload-$SCENARIO.json \\
    --header='Content-Type: application/json' --header='Authorization: Bearer aksh-system-token' \\
    http://127.0.0.1/api/v1/runs)
  echo \"\$RESULT\" > /workspace/benchmarks/compatibility/server/behavior/$SCENARIO/aksh-multi/submission.json
  echo \$RESULT | python3 -c 'import sys,json; print(next(iter(json.load(sys.stdin).values())))' > /workspace/benchmarks/compatibility/server/behavior/$SCENARIO/aksh-multi/run-id
  cp /tmp/server.log /workspace/benchmarks/compatibility/server/behavior/$SCENARIO/aksh-multi/server-start.log
" >/dev/null

for i in $(seq 1 "$RUNNERS"); do
  vm="multi-runner-${SCENARIO}-${i}-$$"
  VMS+=("$vm")
  smolvm machine create --name "$vm" --from "$TEMPLATE" --net-backend virtio-net --net \
    -v "$RUNNER_HOST:/opt/runners:ro" -v "$ROOT:/workspace" >/dev/null
  smolvm machine update --name "$vm" --rosetta >/dev/null
  smolvm machine start --name "$vm" >/dev/null
  smolvm machine exec --name "$vm" -- bash -lc 'mount -t binfmt_misc binfmt_misc /proc/sys/fs/binfmt_misc 2>/dev/null || true; if [ -x /usr/bin/rosetta-wrapper ] && [ -x /mnt/rosetta/rosetta ]; then echo ":rosetta:M::\\x7fELF\\x02\\x01\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\\x00\\x3e\\x00:\\xff\\xff\\xff\\xff\\xff\\xfe\\xfe\\x00\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xff\\xfe\\xff\\xff\\xff:/usr/bin/rosetta-wrapper:F" > /proc/sys/fs/binfmt_misc/register 2>/dev/null || true; fi' >/dev/null 2>&1 || true
done

for i in $(seq 1 "$RUNNERS"); do
  vm="${VMS[$((i-1))]}"
  smolvm machine exec --name "$vm" -- bash -lc "
    set +e
    export RUNNER_ALLOW_RUNASROOT=1 ACTIONS_RUNNER_DEBUG=true RUNNER_DEBUG=1
    cp -a /opt/runners/actions-runner /tmp/runner
    rm -f /tmp/runner/.runner /tmp/runner/.credentials /tmp/runner/.credentials_rsaparams
    mkdir -p /tmp/runner/_work
    cd /tmp/runner
    ./config.sh --unattended --url 'http://$SERVER_IP' --token 'aksh-system-token' \\
      --name 'multi-$SCENARIO-$i' --labels 'self-hosted,linux,x64' --work _work --replace --ephemeral > /tmp/config.log 2>&1
    timeout 240 ./run.sh > /tmp/runner.log 2>&1
    rc=\$?
    mkdir -p /workspace/benchmarks/compatibility/server/behavior/$SCENARIO/aksh-multi/runner-$i
    cp /tmp/runner.log /workspace/benchmarks/compatibility/server/behavior/$SCENARIO/aksh-multi/runner-$i/official-runner.log
    cp /tmp/config.log /workspace/benchmarks/compatibility/server/behavior/$SCENARIO/aksh-multi/runner-$i/config.log
    cp -a /tmp/runner/_diag/. /workspace/benchmarks/compatibility/server/behavior/$SCENARIO/aksh-multi/runner-$i/diag/ 2>/dev/null || true
    exit \$rc
  " > "$RESULT/runner-$i.exec.log" 2>&1 &
done
wait || true

RUN_ID=$(cat "$RESULT/run-id")
smolvm machine exec --name "$SERVER" -- bash -lc "
  sleep 2
  wget -qO /tmp/status.json --header='Authorization: Bearer aksh-system-token' http://127.0.0.1/api/v1/runs/$RUN_ID || true
  cp /tmp/status.json /workspace/benchmarks/compatibility/server/behavior/$SCENARIO/aksh-multi/status.json 2>/dev/null || true
  cp /tmp/server.log /workspace/benchmarks/compatibility/server/behavior/$SCENARIO/aksh-multi/server.log 2>/dev/null || true
" >/dev/null 2>&1 || true
python3 -c "import json; d=json.load(open('$RESULT/status.json')); print(d['status']); [print(f'{k}: {v}') for k,v in d['jobs'].items()]" 2>/dev/null || echo error
