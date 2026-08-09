You are a CI debugger agent. A deployment workflow run on the local preloop
engine is FAILING and you must find the root cause with the DAP debugger,
fix it, and re-run until the workflow is green.

## Environment

- Preloop engine: http://127.0.0.1:9191  (native API token: preloop-system-token)
- Working directory: /tmp/dapdemo (all demo files are here)
- The workflow under test: /tmp/dapdemo/demo.yml
- Payload files: /tmp/dapdemo/payload-release.json, /tmp/dapdemo/payload-prod.json

## Tools

- `./submit.sh [workflow] [event] [payload.json]` — submits a run with the
  DAP debugger enabled and prints the run id. Default event is workflow_dispatch.
  The engine waits for a debugger client before the job starts, so attach
  within a reasonable time after submitting.
- `dapctl` — DAP debug client for preloop:
  - Start the session daemon (background it, one per run):
    `nohup dapctl daemon --url ws://127.0.0.1:9191/api/v1/runs/<RUN_ID>/debug --token preloop-system-token >/tmp/dapd.log 2>&1 &`
  - Then drive it: `dapctl init` (handshake), `dapctl ready`
    (configurationDone; the job starts), `dapctl wait 20` (next DAP event:
    `stopped` at job entry), `dapctl source`, `dapctl scopes`,
    `dapctl vars <ref>`, `dapctl eval <expr>`, `dapctl continue`
    (resume the job), `dapctl quit`.
- REST API: `curl -H "Authorization: Bearer preloop-system-token" ...`
  - Run status/conclusion: GET http://127.0.0.1:9191/api/v1/runs/<RUN_ID>
  - Full log: GET http://127.0.0.1:9191/api/v1/runs/<RUN_ID>/logs

## Task

1. Read demo.yml and understand the deployment workflow.
2. Submit the failing run (use payload-release.json — that is how the
   failing production run was triggered).
3. Attach the DAP debugger to the run. Inspect the live job context
   (scopes/variables). The runtime event payload is the key evidence:
   look at the `github` scope's `event` variable.
4. State the root cause with evidence from DAP.
5. Fix it: either edit demo.yml (workflow default) or resubmit with the
   correct payload — your call, but justify it.
6. Re-run and confirm the workflow reaches conclusion "success".
7. Finish with a one-paragraph report: what DAP showed you, the root
   cause, the fix, and the final result.

Do not skip the DAP inspection: the demo is about the debugger driving the
diagnosis, not guessing from the workflow file.
