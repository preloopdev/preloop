#!/bin/bash
set -euo pipefail
SERVER=/workspace/target/aarch64-unknown-linux-musl/release/aksh-runner-server
OUTDIR=/workspace/benchmarks/v2336-official-vs-aksh
FLOWS=$OUTDIR/combined-flows.jsonl
pkill -f "aksh-runner-server.*:80" 2>/dev/null || true
sleep 1
mkdir -p "$OUTDIR"
chmod 777 "$OUTDIR"
AKSH_PUBLIC_URL=http://127.0.0.1 exec $SERVER serve --listen 127.0.0.1:80 --record-flows "$FLOWS"
