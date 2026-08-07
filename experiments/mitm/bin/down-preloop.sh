#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MITM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE="$MITM_DIR/.cache"

PIDFILE="$CACHE/aksh.pid"
if [ -f "$PIDFILE" ]; then
    PID=$(cat "$PIDFILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "stopping aksh (pid $PID)..."
        kill -INT "$PID" 2>/dev/null || true
        for i in $(seq 1 10); do
            if ! kill -0 "$PID" 2>/dev/null; then break; fi
            sleep 1
        done
        if kill -0 "$PID" 2>/dev/null; then
            echo "force-stopping aksh..."
            kill -TERM "$PID" 2>/dev/null || true
        fi
    fi
    rm -f "$PIDFILE"
fi
echo "aksh stopped"
