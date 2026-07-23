#!/usr/bin/env bash
# Capture official/aksh runner wire traffic against a local aksh-server.
# Usage: local-server-mitm-capture.sh <workflow.yml> <official|aksh|both> [job-count]
set -euo pipefail

SCENARIO="${1:?workflow filename required}"
RUNNER_KIND="${2:-both}"
JOB_COUNT="${3:-1}"
ROOT="$PWD"
VM_PREFIX="${VM_PREFIX:-bench-aksh}"
HOST_IP="${HOST_IP:-$(ipconfig getifaddr en1 2>/dev/null || echo 127.0.0.1)}"
SERVER_PORT="${SERVER_PORT:-9191}"
SERVER_URL="http://${HOST_IP}:${SERVER_PORT}"
SERVER_BIN="${SERVER_BIN:-$ROOT/target/release/aksh-runner-server}"
CLIENT_BIN="${CLIENT_BIN:-$ROOT/target/release/aksh-runner-client}"
OFFICIAL_SRC="${OFFICIAL_SRC:-/opt/runners/actions-runner}"
AKSH_RUNNER="${AKSH_RUNNER:-/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner}"
MITM_PORT="${MITM_PORT:-18081}"
OUT_ROOT="${RESULTS_ROOT:-$ROOT/benchmarks/compatibility/runner/protocol-local}"
WORKFLOW="$ROOT/benchmarks/real-world/overnight-workflows/$SCENARIO"
SYSTEM_TOKEN="${AKSH_SYSTEM_TOKEN:-local-mitm-token}"

log() { echo "[$(date -u +%H:%M:%S)] $*"; }
ensure_vms() {
  for i in $(seq 1 "$JOB_COUNT"); do
    vm="$VM_PREFIX-$i"
    smolvm machine status --name "$vm" >/dev/null 2>&1 || {
      smolvm machine create --name "$vm" --image ubuntu:24.04 --cpus 4 --mem 8192 --storage 20 --net >/dev/null
      smolvm machine update --name "$vm" --volume "$ROOT:/workspace" >/dev/null
      smolvm machine update --name "$vm" --volume "$HOME/cachingv4:/opt/runners" >/dev/null || true
    }
  done
}
start_server() {
  pkill -x aksh-runner-server >/dev/null 2>&1 || true
  sleep .3
  STATE_DIR="$(mktemp -d /tmp/aksh-local-state.XXXXXX)"
  RUST_LOG=info AKSH_SYSTEM_TOKEN="$SYSTEM_TOKEN" AKSH_PUBLIC_URL="$SERVER_URL" "$SERVER_BIN" serve --listen "0.0.0.0:$SERVER_PORT" --state-dir "$STATE_DIR" >/tmp/aksh-local-server.log 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 50); do curl -sf "http://127.0.0.1:$SERVER_PORT/healthz" >/dev/null && return; sleep .2; done
  cat /tmp/aksh-local-server.log; exit 1
}
start_proxy() {
  python3 - "$SERVER_PORT" "$SERVER_PORT" >/tmp/aksh-local-proxy.log 2>&1 <<'PY' &
import socket,sys,threading
port=int(sys.argv[1]); target=int(sys.argv[2])
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); s.bind(("0.0.0.0",port+1)); s.listen(128)
def one(c):
 try:
  x=socket.create_connection(("127.0.0.1",target))
  def cp(a,b):
   while (d:=a.recv(65536)): b.sendall(d)
  a=threading.Thread(target=cp,args=(c,x),daemon=True); b=threading.Thread(target=cp,args=(x,c),daemon=True); a.start(); b.start(); a.join(); b.join()
 finally: c.close()
while True:
 c,_=s.accept(); threading.Thread(target=one,args=(c,),daemon=True).start()
PY
  PROXY_PID=$!
}
run_one() {
  kind="$1"; ts=$(date -u +%Y-%m-%dT%H-%M-%SZ); out="$OUT_ROOT/${SCENARIO%.yml}/aksh-server-$kind/$ts"; mkdir -p "$out"
  start_server; start_proxy; trap 'kill "$SERVER_PID" "$PROXY_PID" 2>/dev/null || true' RETURN
  pids=()
  for i in $(seq 1 "$JOB_COUNT"); do
    vm="$VM_PREFIX-$i"; vout="/workspace/${out#$ROOT/}/vm-$i"; mkdir -p "$out/vm-$i"
    smolvm machine stop --name "$vm" >/dev/null 2>&1 || true; sleep 1; smolvm machine start --name "$vm" >/dev/null 2>&1 || true; sleep 2
    smolvm machine exec --name "$vm" -- bash -lc "command -v mitmdump >/dev/null || (apt-get update -qq && apt-get install -y -qq python3-pip >/dev/null && python3 -m pip install --break-system-packages -q mitmproxy==12.2.3); mkdir -p '$vout/vm-mitm' '$vout/vm-mitm-conf'; pkill -x mitmdump 2>/dev/null || true; nohup env MITM_CAPTURE_DIR='$vout/vm-mitm' mitmdump --listen-host 127.0.0.1 --listen-port $MITM_PORT --set confdir='$vout/vm-mitm-conf' -s /workspace/experiments/mitm/addons/capture.py >/dev/null 2>&1 &"
    sleep 2
    name="local-$kind-${SCENARIO%.yml}-$i-$(date +%s)"; name="${name:0:60}"; root="/tmp/local-$kind-$i"; ca="$vout/vm-mitm-conf/mitmproxy-ca-cert.pem"; proxy="http://127.0.0.1:$MITM_PORT"
    if [ "$kind" = aksh ]; then
      cmd="RUST_LOG=info '$AKSH_RUNNER' --ca-bundle '$ca' --runner-root '$root' configure --url '$SERVER_URL' --token '$SYSTEM_TOKEN' --name '$name' --unattended --replace --ephemeral --labels self-hosted,linux,x64,overnight --no-externals && RUST_LOG=info '$AKSH_RUNNER' --ca-bundle '$ca' --runner-root '$root' run --once"
    else
      cmd="rm -rf '$root'; mkdir -p '$root/bin'; cp -a '$OFFICIAL_SRC/' '$root/bin/actions-runner'; cd '$root/bin/actions-runner'; export RUNNER_ALLOW_RUNASROOT=1 GITHUB_ACTIONS_RUNNER_TLS_NO_VERIFY=1 NODE_EXTRA_CA_CERTS='$ca' SSL_CERT_FILE='$ca'; ./config.sh --unattended --url '$SERVER_URL' --token '$SYSTEM_TOKEN' --name '$name' --labels self-hosted,linux,x64,overnight --work '$root/_work' --replace --ephemeral && timeout 900 ./run.sh --once"
    fi
    smolvm machine exec --name "$vm" -- bash -lc "export HTTP_PROXY='$proxy' HTTPS_PROXY='$proxy' http_proxy='$proxy' https_proxy='$proxy' NO_PROXY='' no_proxy=''; $cmd" >"$out/vm-$i.log" 2>&1 & pids+=("$!")
  done
  sleep 12
  submit=$(AKSH_SYSTEM_TOKEN="$SYSTEM_TOKEN" "$CLIENT_BIN" --server "http://127.0.0.1:$SERVER_PORT" submit -W "$WORKFLOW" --event workflow_dispatch --repository local/overnight --git-ref refs/heads/main 2>&1)
  echo "$submit" >"$out/submit.txt"; run_id=$(printf '%s' "$submit" | python3 -c 'import json,sys; print(json.load(sys.stdin)["run_id"])')
  for _ in $(seq 1 360); do status=$(curl -sf -H "Authorization: Bearer $SYSTEM_TOKEN" "$SERVER_URL/api/v1/runs/$run_id" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status", ""))' || true); case "$status" in completed|success|failed|cancelled) break;; esac; sleep 2; done
  for p in "${pids[@]}"; do wait "$p" 2>/dev/null || true; done
  for i in $(seq 1 "$JOB_COUNT"); do smolvm machine exec --name "$VM_PREFIX-$i" -- pkill -x mitmdump >/dev/null 2>&1 || true; done
  python3 - "$out" "$kind" "$SCENARIO" "$run_id" <<'PY'
import json,sys
from pathlib import Path
r=Path(sys.argv[1]); flows=[]
for p in r.glob('vm-*/vm-mitm/flows.jsonl'):
 for l in p.read_text(errors='replace').splitlines():
  if l.strip(): flows.append(json.loads(l))
flows.sort(key=lambda x:(x.get('ts_request') or 0,x.get('flow_index') or 0)); (r/'flows.jsonl').write_text('\n'.join(json.dumps(x) for x in flows)+'\n')
status='unknown'
try: status=json.loads(__import__('subprocess').check_output(['curl','-sf','-H','Authorization: Bearer aksh-system-token',f'http://127.0.0.1:{sys.argv[1]}']))
except: pass
(r/'summary.json').write_text(json.dumps({'runner':sys.argv[2],'scenario':sys.argv[3],'run_id':sys.argv[4],'flows_count':len(flows)},indent=2))
PY
  latest="$OUT_ROOT/${SCENARIO%.yml}/aksh-server-$kind/latest"; rm -f "$latest"; ln -s "$out" "$latest"; log "$kind local capture: $out flows=$(wc -l <"$out/flows.jsonl")"
  kill "$SERVER_PID" "$PROXY_PID" 2>/dev/null || true
}
ensure_vms
case "$RUNNER_KIND" in official|aksh) run_one "$RUNNER_KIND";; both) run_one official; run_one aksh;; *) exit 2;; esac
