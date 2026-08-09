#!/bin/bash
# Submit a run with the DAP debugger enabled; prints the run id.
# Usage: submit.sh [workflow.yml] [event] [payload.json]
set -euo pipefail
URL="${PRELOOP_URL:-http://127.0.0.1:9191}"
TOKEN="${PRELOOP_TOKEN:-preloop-system-token}"
WF="${1:-/tmp/dapdemo/demo.yml}"
EVENT="${2:-workflow_dispatch}"
PAYLOAD="${3:-}"
python3 - "$WF" "$URL" "$TOKEN" "$EVENT" "$PAYLOAD" <<'PY'
import json, sys, urllib.request
wf, url, token, event, payload_path = sys.argv[1:]
body = {
    "workflow_yaml": open(wf).read(),
    "event": event,
    "repository": "demo/demo",
    "git_ref": "refs/heads/main",
    "sha": "0" * 40,
    "enable_debugger": True,
    "debugger_welcome_message": "dap-demo: agent-driven DAP session",
}
if payload_path:
    body["payload"] = json.load(open(payload_path))
req = urllib.request.Request(
    url + "/api/v1/runs", data=json.dumps(body).encode(), method="POST",
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
)
resp = json.load(urllib.request.urlopen(req))
print(resp["run_id"])
PY
