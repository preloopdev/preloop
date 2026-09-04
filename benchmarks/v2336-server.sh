#!/bin/bash
set -euo pipefail
SERVER=/workspace/target/aarch64-unknown-linux-musl/release/preloop-server
OUTDIR=/workspace/benchmarks/v2336-official-vs-preloop
FLOWS=$OUTDIR/combined-flows.jsonl
pkill -f "preloop-server.*:80" 2>/dev/null || true
sleep 1
mkdir -p "$OUTDIR"
SYSTEM_TOKEN="${PRELOOP_SYSTEM_TOKEN:?set PRELOOP_SYSTEM_TOKEN to the engine administrator token}"
export PRELOOP_SYSTEM_TOKEN="$SYSTEM_TOKEN"

chmod 777 "$OUTDIR"
PRELOOP_PUBLIC_URL=http://127.0.0.1 PRELOOP_SYSTEM_TOKEN="$SYSTEM_TOKEN" exec $SERVER serve --listen 127.0.0.1:80 --record-flows "$FLOWS"
