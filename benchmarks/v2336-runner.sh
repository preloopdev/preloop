#!/bin/bash
set -euo pipefail
CLIENT=/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner-client
RUNNER_SRC=/opt/runners/actions-runner
OUTDIR=/workspace/benchmarks/v2336-official-vs-aksh

RUNNER_DIR=$(mktemp -d)
cp -r $RUNNER_SRC/* "$RUNNER_DIR/"
cd "$RUNNER_DIR"
./config.sh --url http://127.0.0.1 --token dummy-token --name v2336-test --work _work --unattended --replace 2>&1 | tail -3

SUBMIT=$($CLIENT --server http://127.0.0.1 submit -W /workspace/crates/aksh-conformance/fixtures/v2336-combined.yml 2>&1)
echo "Submit: $SUBMIT"
RUN_ID=$(echo "$SUBMIT" | python3 -c "import json,sys; print(json.load(sys.stdin)['run_id'])")
echo "RUN_ID=$RUN_ID"

./run.sh --once > $OUTDIR/official-runner.log 2>&1 &
RPID=$!

for i in $(seq 1 60); do
  sleep 2
  STATUS=$(curl -s "http://127.0.0.1/api/v1/runs/$RUN_ID" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status','unknown'))" 2>/dev/null || echo error)
  if [ "$STATUS" = completed ] || [ "$STATUS" = success ] || [ "$STATUS" = failed ]; then
    echo "Run: $STATUS"
    break
  fi
done

curl -s "http://127.0.0.1/api/v1/runs/$RUN_ID" > $OUTDIR/run-result.json
curl -s "http://127.0.0.1/api/v1/runs/$RUN_ID/logs" > $OUTDIR/run-logs.txt
wait $RPID 2>/dev/null || true
echo "DONE"
