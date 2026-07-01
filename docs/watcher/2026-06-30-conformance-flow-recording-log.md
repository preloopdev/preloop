# Conformance flow recording log — 2026-06-30

Scope: record and compare the non-Linux runner scenarios (`06`–`15`) against the official GitHub runner and aksh. Container scenarios `16` and `17` are deferred because they require a Linux runner host.

## Log

- Started from the local merged MITM harness under `experiments/mitm`, not the old sibling MITM worktree.
- Patched `experiments/mitm/bin/record.sh` so `--backend aksh` requires and uses an already-running aksh on `127.0.0.1:9090` instead of rejecting that port. This matches `bin/up-aksh.sh`, which owns starting aksh and writing the local runner-server URL cache.

- Created private GitHub sample repository `preloopdev/aksh-conformance-sample` for isolated official-runner recordings.
- Published workflow fixtures `06`–`15` under `.github/workflows/` and the composite action helper under `.github/actions/greet/action.yml`.
- Pushed sample app commit `df62003` to `preloopdev/aksh-conformance-sample@main`.

- Installed local MITM Python dependencies with `uv sync` under `experiments/mitm/.venv`; recording commands will prepend that venv to `PATH` so `mitmdump` is available without global install.

- First official recording attempt for `06-multi-step` found local port 8080 already occupied by Colima/Lima and exposed that `experiments/mitm/versions.toml` had drifted to runner `2.329.0`.
- Restored `experiments/mitm/versions.toml` to runner `2.335.1` and patched `record.sh` to honor `MITM_PORT` so recording can use a non-conflicting proxy port such as `18080`.

- The first `06-multi-step` attempt reached runner registration but scenario submission failed because the driver inherited proxy env into `gh workflow run`, causing `gh` TLS verification against the MITM CA to fail.
- Patched `_run_scenario.py` so official `gh workflow run`/`gh run list` calls strip proxy env and the invalid `GITHUB_TOKEN`; this keeps GitHub CLI submission outside the MITM capture path while the runner traffic remains proxied.
- Patched `record-golden.sh` to copy a golden only when `summary.json.status == ok` and `flows_count > 0`; removed the failed `06-multi-step` golden that had been copied despite `scenario_failed`.

- Second `06-multi-step` attempt failed because the cached official runner directory was already configured from the prior failed attempt. Patched `record.sh` to remove stale `.runner` configuration before reconfiguring, and fixed missing `flows.jsonl` summary handling.

- Third `06-multi-step` attempt successfully ran the GitHub job but the scenario driver timed out because `job_assigned` only recognized legacy `PipelineAgentJobRequest`; v2.335.1 emits broker `RunnerJobRequest` and `/acquirejob`. Patched `_run_scenario.py` to treat `RunnerJobRequest` as assignment and `/completejob` as completion.

- Official `06-multi-step` recording succeeded with status `ok` and 47 captured flows; golden saved under `experiments/mitm/golden/v2.335.1/06-multi-step`.
- Added `actions/checkout@v4` before the local composite action in `13-composite-action` so the repository-local action path exists in the runner workspace; pushed sample repo commit `bcf6191`.

- Official recordings `07-step-failure`, `08-job-outputs-needs`, and `09-matrix-fan-out` succeeded with statuses `ok` and 50/59/73 flows respectively.
- Immediate batch continuation into `10`–`15` hit a transient `MITM_PORT=18080` listener reuse check after the prior mitmdump shutdown; no persistent listener remained afterward. Retrying remaining scenarios with a fresh port and delay.

- Patched `record.sh` to use a unique runner name per backend/scenario/timestamp to avoid broker session conflicts from reusing `mitm-official`.
- Cancelled stale queued sample-repo runs for checkout/cache/artifact/annotations so a fresh runner cannot pick up an older queued run and corrupt the next golden.

- Retried `10-uses-checkout` and `14-annotations` after cancelling stale runs and using unique runner names; both succeeded.
- Verified official goldens exist for all non-Linux scenarios `06`–`15` with status `ok` and non-zero flows: 47, 50, 59, 73, 36, 40, 45, 36, 28, 30.

- `runner-watch record-golden` currently re-runs recording before copying, so I imported the already-recorded local goldens by copying `experiments/mitm/golden/v2.335.1` to `.runner-watch/golden/v2.335.1`.
