#!/bin/bash
set -euo pipefail

SERVER=/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner-server
CLIENT=/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner-client
# Prefer pinned v2.336.0; fall back to container layout.
RUNNER_SRC="${RUNNER_SRC:-}"
if [[ -z "$RUNNER_SRC" ]]; then
  for candidate in \
    /opt/runners/actions-runner-2.336.0 \
    /opt/runners/actions-runner \
    "$HOME/.cache/actions-runner/v2.336.0/linux-arm64" \
    "$HOME/.cache/actions-runner/v2.336.0/osx-arm64" \
    "$HOME/.cache/actions-runner/current"
  do
    if [[ -x "$candidate/bin/Runner.Listener" ]]; then
      RUNNER_SRC="$candidate"
      break
    fi
  done
fi
if [[ -z "${RUNNER_SRC:-}" ]]; then
  echo "error: no official runner install found (set RUNNER_SRC)" >&2
  exit 1
fi
RUNNER_VER=$("$RUNNER_SRC/bin/Runner.Listener" --version 2>/dev/null || echo unknown)
echo "Using official runner at $RUNNER_SRC (version $RUNNER_VER)"
if [[ "$RUNNER_VER" != "2.336.0" ]]; then
  echo "warning: expected runner 2.336.0, got $RUNNER_VER" >&2
fi

OUTDIR=/workspace/benchmarks/v2336-official-vs-aksh
FLOWS=$OUTDIR/combined-flows.jsonl

pkill -f "aksh-runner-server.*:80" 2>/dev/null || true
sleep 1

mkdir -p "$OUTDIR"
chmod 777 "$OUTDIR"

# Official runner strips non-default ports. Must listen on 80.
AKSH_PUBLIC_URL=http://127.0.0.1 $SERVER serve --listen 127.0.0.1:80 --record-flows "$FLOWS" > /tmp/server.log 2>&1 &
SERVER_PID=$!
sleep 2

curl -s http://127.0.0.1/ -o /dev/null -w "server: %{http_code}\n"

su - ubuntu -c "
set -euo pipefail
RUNNER_DIR=\$(mktemp -d)
cp -r $RUNNER_SRC/* \"\$RUNNER_DIR/\"
cd \"\$RUNNER_DIR\"
./config.sh --url http://127.0.0.1 --token dummy-token --name v2336-test --work _work --unattended --replace 2>&1 | tail -3

# Helper to run a workflow and save results
run_wf() {
  local num=\$1
  local yaml=\$2
  
  echo \"=== Running Workflow \$num: \$yaml ===\"
  $CLIENT --server http://127.0.0.1 submit -W /workspace/crates/aksh-conformance/fixtures/\$yaml > /tmp/submit.json
  local run_id=\$(python3 -c \"import json; print(json.load(open('/tmp/submit.json'))['run_id'])\")
  echo \"RUN_ID=\$run_id\"
  
  ./run.sh --once > $OUTDIR/runner-\$num.log 2>&1
  
  mkdir -p $OUTDIR/aksh/\$num
  curl -s -H 'Authorization: Bearer aksh-system-token' http://127.0.0.1/api/v1/runs/\$run_id > $OUTDIR/aksh/\$num/run-result.json
  curl -s -H 'Authorization: Bearer aksh-system-token' http://127.0.0.1/api/v1/runs/\$run_id/logs > $OUTDIR/aksh/\$num/run-logs.txt
}

run_wf 200 v2336-combined.yml
run_wf 201 v2336-background-cancel.yml
run_wf 202 v2336-file-commands.yml
"

echo "Flows: \$(wc -l < $FLOWS 2>/dev/null || echo 0)"
kill $SERVER_PID 2>/dev/null || true
echo "DONE"
