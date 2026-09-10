#!/usr/bin/env bash
# Verify that exactly the engine host address is reachable from a sandbox.
#
# The posture this asserts: the engine is reachable (the runner must register),
# any other private address is not (the sandbox stays isolated from the LAN),
# and the public internet is (jobs fetch actions and packages).
#
# Usage: verify-egress.sh <engine-ip> [port] [other-private-ip-to-probe]
set -uo pipefail
echo "${SUDO_PASS:-}" | sudo -S -p '' true
HOSTIP="${1:?engine host address, as the guest must reach it}"
PORT="${2:-9490}"
# A second private address that must stay unreachable, so the test proves the
# hole is one /32 wide and not the whole LAN. Defaults to the engine host's
# gateway-ish neighbour; pass your own.
CONTROL="${3:-$(echo "$HOSTIP" | cut -d. -f1-3).1}"

ID=$(aenv start --cold docker.io/library/ubuntu:24.04 --cpu 2 --memory 2048 --timeout 600 -d)
echo "sandbox=$ID"
sleep 1
FC=$(pgrep -f 'firecracker --api-sock' | tail -1)
echo "firecracker=$FC"
echo "--- egress rules for the engine's covering range:"
sudo -n nsenter -t "$FC" -n iptables -S AGENTENV-EGRESS 2>&1 |
  grep -E "$(echo "$HOSTIP" | cut -d. -f1-2 | sed 's/\./\\./g')" | head -25
echo "--- listener:"
nohup python3 -m http.server "$PORT" --bind 0.0.0.0 >/tmp/httpd.log 2>&1 &
HTTPD=$!
sleep 2
ss -ltn "sport = :$PORT" | tail -1
echo "--- from the guest:"
aenv exec "$ID" -- /bin/bash -c "
  timeout 5 bash -c 'echo > /dev/tcp/$HOSTIP/$PORT' && echo ENGINE-REACHABLE || echo ENGINE-BLOCKED
  timeout 5 bash -c 'echo > /dev/tcp/$CONTROL/22' && echo OTHER-LAN-REACHABLE || echo OTHER-LAN-BLOCKED
  timeout 6 bash -c 'echo > /dev/tcp/1.1.1.1/443' && echo INTERNET-REACHABLE || echo INTERNET-BLOCKED
"
kill "$HTTPD" 2>/dev/null
aenv delete "$ID" >/dev/null 2>&1
