#!/usr/bin/env bash
# run-host-cell.sh — Cell C with the preloop runner on the HOST (no VM pool).
# Same runner binary the VMs use; the completion path avoids the VM teardown
# race that currently kills in-VM runners at job end.
#
# Usage: bash run-host-cell.sh <repo-dir> <workflow-rel> <event> <out> [payload]
set -euo pipefail

REPO_DIR=${1:?repo dir}
WORKFLOW=${2:?workflow rel path}
EVENT=${3:?event}
OUT=${4:?out key}
PAYLOAD=${5:-}

PRELOOP=$HOME/preloop/target/debug/preloop
RUNNER=$HOME/preloop/target/debug/preloop-runner
ENGINE_PORT=127.0.0.1:9091
RESULT_DIR=benchmarks/real-world/results/conformance-4repos/$OUT/c
RUNNER_ROOT=/tmp/conformance-host-runner
export PRELOOP_URL="http://$ENGINE_PORT"

TOKEN=$(cat ~/.preloop/engine.token)

# Configure the host runner once (labels match the rewritten runs-on).
if [ ! -f "$RUNNER_ROOT/.runner" ]; then
  PRELOOP_SYSTEM_TOKEN="$TOKEN" "$RUNNER" --runner-root "$RUNNER_ROOT" configure \
    --url "http://$ENGINE_PORT" --token "$TOKEN" --name host-cf --unattended --replace \
    --labels self-hosted,Linux,X64 >/dev/null 2>&1
fi

PAYLOAD_ARGS=()
if [ -n "$PAYLOAD" ]; then
  PAYLOAD_ARGS=(--payload "$PAYLOAD")
fi
if [ ${#PAYLOAD_ARGS[@]} -gt 0 ]; then
  RUN_LINE=$(cd "$REPO_DIR" && PRELOOP_GITHUB_TOKEN=$(gh auth token) \
    "$PRELOOP" run -f "$REPO_DIR/$WORKFLOW" --event "$EVENT" --payload "$PAYLOAD" --detach | head -1)
else
  RUN_LINE=$(cd "$REPO_DIR" && PRELOOP_GITHUB_TOKEN=$(gh auth token) \
    "$PRELOOP" run -f "$REPO_DIR/$WORKFLOW" --event "$EVENT" --detach | head -1)
fi
RUN_ID=$(echo "$RUN_LINE" | grep -oE '[0-9a-f]{8}-[0-9a-f-]{27}' | head -1)
echo "run: $RUN_ID"

STATUS=""
for _ in $(seq 1 400); do
  STATUS=$(curl -sf -H "Authorization: Bearer $TOKEN" \
    "http://$ENGINE_PORT/api/v1/runs/$RUN_ID" | python3 -c \
    "import json,sys; d=json.load(sys.stdin); print(d.get('status',''))" 2>/dev/null || true)
  case "$STATUS" in
    success|failure|cancelled) break ;;
  esac
  # Claim one queued job with the host runner (--once per job).
  if [ "$STATUS" = "queued" ] || [ "$STATUS" = "in_progress" ]; then
    PRELOOP_SYSTEM_TOKEN="$TOKEN" "$RUNNER" --runner-root "$RUNNER_ROOT" run --once \
      >/tmp/host-cell-$OUT.log 2>&1 || true
  else
    sleep 10
  fi
done
echo "final status: $STATUS"

mkdir -p "$RESULT_DIR"
curl -sf -H "Authorization: Bearer $TOKEN" \
  "http://$ENGINE_PORT/api/v1/runs/$RUN_ID" > "$RESULT_DIR/run.json"
python3 benchmarks/real-world/conformance-4repos/compare-goldens.py --repo "$OUT" 2>/dev/null \
  | grep -A100 "== $OUT/preloop" | head -60 || true
