#!/usr/bin/env bash
# run-preloop-cell.sh — Run one repo's exact workflow through the preloop
# production path (on-demand smolVM pool) and capture the run record.
#
# Cell C: preloop runner (in preloop smolVMs) vs preloop server.
#
# Usage:
#   PRELOOP_RUNNER_LABELS=X64 ./run-preloop-cell.sh <repo-dir> <workflow-rel> <event>
#
# Environment knobs:
#   PRELOOP_RUNNER_LABELS      extra runner labels (e.g. X64 for the campaign repos)
#   PRELOOP_RUNNER_BASE_IMAGE  custom base (.smolmachine sidecar) to avoid registry pulls
#   PRELOOP_RUNNER_OVERLAY_GB  root overlay size per runner (default provider)
#   PAYLOAD                    event payload JSON file (pull_request needs action: opened)
#   PRELOOP_RUNNER_URL            host-reachable runner URL; non-loopback switches
#                              the runner transport from the control socket to
#                              plain TCP (macOS smolvm has no socket relay)
#   PRELOOP_LISTEN             engine listen address (default 127.0.0.1:9091
#                              when the firewall must be bypassed via a proxy)
set -euo pipefail

REPO_DIR=${1:?repo workspace dir}
WORKFLOW=${2:?workflow path relative to repo}
EVENT=${3:-push}
OUT=${4:-$(basename "$REPO_DIR")}

PRELOOP=${PRELOOP:-$HOME/preloop/target/debug/preloop}
ENGINE_PORT=${PRELOOP_LISTEN:-127.0.0.1:9091}
RESULT_DIR=benchmarks/real-world/results/conformance-4repos/$OUT/c

# The CLI otherwise falls back to its configured endpoint (unix socket or the
# default port), where /api/v1/* is gated off and submission 404s.
export PRELOOP_URL="http://$ENGINE_PORT"

if ! curl -sf --max-time 3 "http://${ENGINE_PORT#127.0.0.1:}/healthz" >/dev/null 2>&1 \
  && ! curl -sf --max-time 3 "http://${ENGINE_PORT}/healthz" >/dev/null 2>&1; then
  echo "engine not healthy at ${ENGINE_PORT}" >&2
  exit 1
fi

PAYLOAD_ARGS=()
if [ -n "${PAYLOAD:-}" ]; then
  PAYLOAD_ARGS=(--payload "$PAYLOAD")
fi
if [ ${#PAYLOAD_ARGS[@]} -gt 0 ]; then
  RUN_LINE=$(cd "$REPO_DIR" && PRELOOP_GITHUB_TOKEN=$(gh auth token) \
    "$PRELOOP" run -f "$REPO_DIR/$WORKFLOW" --event "$EVENT" --payload "$PAYLOAD" --detach | head -1)
else
  RUN_LINE=$(cd "$REPO_DIR" && PRELOOP_GITHUB_TOKEN=$(gh auth token) \
    "$PRELOOP" run -f "$REPO_DIR/$WORKFLOW" --event "$EVENT" --detach | head -1)
fi
RUN_ID=$(echo "$RUN_LINE" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
echo "run: $RUN_ID"

TOKEN=$(cat ~/.preloop/engine.token)
for _ in $(seq 1 120); do
  sleep 15
  STATUS=$(curl -sf -H "Authorization: Bearer $TOKEN" \
    "http://${ENGINE_PORT}/api/v1/runs/$RUN_ID" | python3 -c \
    "import json,sys; d=json.load(sys.stdin); print(d.get('status',''))")
  case "$STATUS" in
    success|failure|cancelled) break ;;
  esac
done
echo "final status: $STATUS"

mkdir -p "$RESULT_DIR"
curl -sf -H "Authorization: Bearer $TOKEN" \
  "http://${ENGINE_PORT}/api/v1/runs/$RUN_ID" > "$RESULT_DIR/run.json"
python3 benchmarks/real-world/conformance-4repos/compare-goldens.py --repo "$OUT" 2>/dev/null \
  | grep -A100 "== $OUT/preloop" | head -60 || true
