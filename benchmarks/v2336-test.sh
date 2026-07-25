#!/bin/bash
set -euo pipefail

SERVER=/workspace/target/aarch64-unknown-linux-musl/release/preloop-server
CLIENT=/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner-client
RUNNER_SRC=/opt/runners/actions-runner
OUTDIR=/workspace/benchmarks/v2336-official-vs-aksh
FLOWS=$OUTDIR/combined-flows.jsonl

pkill -f "preloop-server.*:80" 2>/dev/null || true
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

$CLIENT --server http://127.0.0.1 submit -W /workspace/crates/aksh-conformance/fixtures/v2336-combined.yml > /tmp/submit.json 2>&1
cat /tmp/submit.json
RUN_ID=\$(python3 -c \"import json; print(json.load(open('/tmp/submit.json'))['run_id'])\")
echo \"RUN_ID=\$RUN_ID\"

./run.sh --once > $OUTDIR/official-runner.log 2>&1 &
RPID=\$!

for i in \$(seq 1 60); do
  sleep 2
  STATUS=\$(curl -s -H 'Authorization: Bearer aksh-system-token' http://127.0.0.1/api/v1/runs/\$RUN_ID | python3 -c \"import json,sys; print(json.load(sys.stdin).get('status','unknown'))\" 2>/dev/null || echo error)
  if [ \"\$STATUS\" = completed ] || [ \"\$STATUS\" = success ] || [ \"\$STATUS\" = failed ]; then
    echo \"Run: \$STATUS\"
    break
  fi
done

curl -s -H 'Authorization: Bearer aksh-system-token' http://127.0.0.1/api/v1/runs/\$RUN_ID > $OUTDIR/run-result.json
curl -s -H 'Authorization: Bearer aksh-system-token' http://127.0.0.1/api/v1/runs/\$RUN_ID/logs > $OUTDIR/run-logs.txt
wait \$RPID 2>/dev/null || true
echo 'Runner done'
"

echo "Flows: $(wc -l < $FLOWS 2>/dev/null || echo 0)"
kill $SERVER_PID 2>/dev/null || true
echo "DONE"
