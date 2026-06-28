# MITM runner/control-plane experiment plan

## Context

Build a reproducible experiment that captures the exact HTTP traffic between the unmodified official `actions/runner` binary and a control plane, then compares the official GitHub control plane against a custom control plane. Use `ChristopherHX/runner.server`, not `ChristopherHX/github-act-runner`: `runner.server` is a local GitHub Actions/Azure Pipelines control plane that the official runner can register against; `github-act-runner` is a replacement runner using `nektos/act`, so it would change the runner and control plane at the same time. End state: `experiments/mitm/` contains scripts, mitmproxy capture code, scenarios, and a markdown report generator that show JSON payloads, headers, status codes, raw body bytes, and timing side-by-side.

## Approach

### 1. Create the experiment workspace

Add this tree under the repo root:

```text
experiments/mitm/
├── README.md
├── pyproject.toml
├── versions.toml
├── addons/
│   └── capture.py
├── bin/
│   ├── record.sh
│   ├── compare.sh
│   ├── up-runner-server.sh
│   ├── down-runner-server.sh
│   ├── _run_scenario.py
│   └── _compare.py
├── scenarios/
│   ├── 01-register-and-idle/
│   │   └── scenario.toml
│   ├── 02-trivial-job/
│   │   ├── scenario.toml
│   │   └── workflow.yml
│   └── 03-cancellation/
│       ├── scenario.toml
│       └── workflow.yml
├── captures/      # gitignored
└── reports/       # gitignored
```

Edit `.gitignore` to add exactly:

```gitignore
experiments/mitm/.cache/
experiments/mitm/captures/
experiments/mitm/reports/
```

`pyproject.toml` uses Python 3.11+ and declares `mitmproxy>=10`, `tomli; python_version<'3.11'`, and `rich`. No Rust workspace changes.

`versions.toml` starts as:

```toml
runner_version = "2.317.0"
runner_server_ref = "992ccbbbf9afcde477c38c316e053b1af457ad40"
mitmproxy_version = "recorded-by-record-sh"
```

The `runner_server_ref` value matches this repo's documented upstream reference in `README.md` and `docs/fidelity-gap.md`.

### 2. Implement the mitmproxy capture addon

Create `experiments/mitm/addons/capture.py` with a mitmproxy addon using `request(flow: mitmproxy.http.HTTPFlow)` and `response(flow: mitmproxy.http.HTTPFlow)`. It writes one JSON object per completed response to `$MITM_CAPTURE_DIR/flows.jsonl` with this exact schema:

```json
{
  "flow_index": 1,
  "ts_request": 1719300000.123,
  "ts_response": 1719300000.456,
  "duration_ms": 333,
  "method": "GET",
  "scheme": "https",
  "host": "pipelines.actions.githubusercontent.com",
  "path": "/_apis/distributedtask/pools/1/messages?sessionId=...",
  "request_headers": [["authorization", "***REDACTED***"]],
  "request_body_b64": "...",
  "request_body_json": null,
  "request_body_sha256": "...",
  "status": 200,
  "response_headers": [["content-type", "application/json"]],
  "response_body_b64": "...",
  "response_body_json": {"example": true},
  "response_body_sha256": "..."
}
```

Header redaction is exact: case-fold header names and replace values with `***REDACTED***` when the name is `authorization`, `set-cookie`, `x-vss-session`, `x-tfs-session`, `x-vss-e2eid`, or contains `token`. Do not redact body bytes in `flows.jsonl`; this experiment needs raw bytes. For bodies larger than 256 KiB or invalid UTF-8, also write `flow.<flow_index>.req.bin` and/or `flow.<flow_index>.resp.bin` and keep the base64 fields in JSONL.

The addon should create `$MITM_CAPTURE_DIR` if missing, flush after every JSONL line, and tolerate response-less flows by writing `status: null` and `ts_response: null`.

### 3. Implement control-plane boot scripts

`bin/up-runner-server.sh`:

1. Read `runner_server_ref` from `versions.toml`.
2. Clone `https://github.com/ChristopherHX/runner.server` into `experiments/mitm/.cache/runner.server` if absent; otherwise fetch and checkout the pinned ref.
3. Run `dotnet run --project src/Runner.Server -- --urls http://127.0.0.1:5000` from that checkout.
4. Wait until `curl -fsS http://127.0.0.1:5000/_apis/connectionData` succeeds.
5. Write `http://127.0.0.1:5000/runner/server` to `.cache/runner-server.url`.
6. Write `ThisIsIgnored` to `.cache/runner-server.token` because the upstream README says runner registration token authentication is ignored unless `Runner.Server:RUNNER_TOKEN` is configured.
7. Write the server PID to `.cache/runner-server.pid`.

`bin/down-runner-server.sh` reads `.cache/runner-server.pid`, sends SIGINT, waits 10 seconds, then sends SIGTERM. If port 5000 is already in use before startup, exit 2 and print `port 5000 is already in use`. If `dotnet` is missing, exit 3 and print `install dotnet sdk 8.0 or newer`.

Do not add `github-act-runner` scripts. It is intentionally excluded because it is not a control-plane comparison target.

### 4. Implement the recording driver

`bin/record.sh --backend {official|runner-server} --scenario <scenario-name>` does this in order:

1. Resolve paths relative to `experiments/mitm/`, not the caller's directory.
2. Create `captures/<backend>/<scenario-name>/<utc-iso8601>/`; export `MITM_CAPTURE_DIR` to that absolute path.
3. Start:

```sh
mitmdump \
  --listen-host 127.0.0.1 \
  --listen-port 8080 \
  --set confdir="$PWD/.cache/mitmproxy" \
  -s addons/capture.py \
  --save-stream-file "$MITM_CAPTURE_DIR/flows.mitm"
```

4. Wait for port 8080 to accept connections. If it does not within 10 seconds, exit 4.
5. Prepare runner dir `.cache/runner-<backend>/`. Download and extract the official `actions/runner` release matching `versions.toml.runner_version` for `osx-arm64` on this workstation.
6. Configure the runner:
   - Official backend: require env `GITHUB_OWNER`, `GITHUB_REPO`, `GITHUB_REF`, `GITHUB_RUNNER_TOKEN`; run `./config.sh --unattended --url "https://github.com/$GITHUB_OWNER/$GITHUB_REPO" --token "$GITHUB_RUNNER_TOKEN" --name "mitm-official" --labels mitm --work _work --replace`.
   - `runner-server` backend: require `.cache/runner-server.url` and `.cache/runner-server.token`; run `./config.sh --unattended --url "$(cat .cache/runner-server.url)" --token "$(cat .cache/runner-server.token)" --name "mitm-runner-server" --labels mitm --work _work --replace`.
7. Write the runner `.env` file with:

```env
https_proxy=http://127.0.0.1:8080
http_proxy=http://127.0.0.1:8080
no_proxy=
NODE_EXTRA_CA_CERTS=<absolute path to .cache/mitmproxy/mitmproxy-ca-cert.pem>
SSL_CERT_FILE=<absolute path to .cache/mitmproxy/mitmproxy-ca-cert.pem>
```

The `SSL_CERT_FILE` / `NODE_EXTRA_CA_CERTS` choice is grounded in actions/runner upstream SSL docs: dotnet can use an OpenSSL CA bundle fallback, and Node actions need `NODE_EXTRA_CA_CERTS`.

8. Launch `./run.sh`; tee output to `$MITM_CAPTURE_DIR/runner.log`.
9. Run `bin/_run_scenario.py --backend <backend> --scenario scenarios/<scenario-name>/scenario.toml --capture-dir "$MITM_CAPTURE_DIR"`.
10. On completion or failure, stop `run.sh` and `mitmdump`, write `summary.json`, and update the `captures/<backend>/<scenario-name>/latest` symlink.

`summary.json` exact fields:

```json
{
  "backend": "official",
  "scenario": "01-register-and-idle",
  "started_at": "2026-06-25T00:00:00Z",
  "ended_at": "2026-06-25T00:01:00Z",
  "status": "ok|config_failed|scenario_failed|timeout",
  "runner_exit_code": 0,
  "flows_count": 42,
  "runner_version": "2.317.0",
  "runner_server_ref": "992ccbbbf9afcde477c38c316e053b1af457ad40",
  "mitmproxy_version": "..."
}
```

Failure handling: config failure keeps the capture directory and writes `status: config_failed`; scenario timeout keeps the capture directory and writes `status: timeout`; mitmproxy startup failure exits before configuring a runner.

### 5. Implement scenario execution

`bin/_run_scenario.py` reads TOML with these fields:

```toml
description = "human-readable text"
duration_seconds_max = 300

[[steps]]
kind = "wait_seconds"
n = 30
```

Supported step kinds:

- `wait_seconds`: requires `n`; sleeps for that many seconds.
- `wait_for_event`: requires `event` and `timeout`; polls `flows.jsonl` until matched.
- `submit_workflow`: requires `path`; backend-specific submission below.
- `cancel_workflow`: no fields; cancels the run id captured by the preceding `submit_workflow`.

Event matching:

- `runner_registered`: any flow where `path` contains `/_apis/distributedtask/pools/` and `/agents`, method is `POST`, and status is 200 or 201.
- `job_assigned`: any response JSON with `messageType == "PipelineAgentJobRequest"`, or any decoded response body containing literal `PipelineAgentJobRequest`.
- `job_completed`: any flow where the path contains `/jobrequests/` or decoded request/response body contains `JobCompleted`.

Workflow submission:

- Official backend: run `gh workflow run <workflow.yml basename> -R "$GITHUB_OWNER/$GITHUB_REPO" --ref "$GITHUB_REF"`, then capture the newest run id with `gh run list -R "$GITHUB_OWNER/$GITHUB_REPO" --workflow <basename> --limit 1 --json databaseId --jq '.[0].databaseId'`.
- `runner-server` backend: use the upstream-supported CLI path from `runner.server` README: run `dotnet run --project .cache/runner.server/src/Runner.Client -- --workflow <absolute workflow.yml> --event push --server http://127.0.0.1:5000`. Do not post directly to `_apis/v1/Message`; `Runner.Client` is the documented scheduler and avoids inventing payload shape.

Cancellation:

- Official backend: run `gh run cancel <captured-run-id> -R "$GITHUB_OWNER/$GITHUB_REPO"`.
- `runner-server` backend: after `submit_workflow`, query `http://127.0.0.1:5000/runner/server/_apis/v1/Message/workflow/runs?owner=&repo=` and pick the newest `id`; then `POST http://127.0.0.1:5000/runner/server/_apis/v1/Message/cancelWorkflow/<id>`. These routes are grounded in `MessageController` attributes: `[Route("_apis/v1/[controller]")]`, `[Route("{owner}/{repo}/_apis/v1/[controller]")]`, `[HttpGet("workflow/runs")]`, and `[HttpPost("cancelWorkflow/{runid}")]`.

Unknown TOML keys cause exit 8. Missing required keys cause exit 9. Step timeout causes exit 10.

### 6. Define three scenarios

`01-register-and-idle/scenario.toml`:

```toml
description = "Runner registers and idles for one long-poll interval."
duration_seconds_max = 180

[[steps]]
kind = "wait_for_event"
event = "runner_registered"
timeout = 120

[[steps]]
kind = "wait_seconds"
n = 30
```

`02-trivial-job/workflow.yml`:

```yaml
name: mitm trivial
on: workflow_dispatch
jobs:
  hello:
    runs-on: [self-hosted, mitm]
    steps:
      - run: echo hello
```

`02-trivial-job/scenario.toml` submits `workflow.yml` then waits for `job_completed` for 180 seconds.

`03-cancellation/workflow.yml`:

```yaml
name: mitm cancellation
on: workflow_dispatch
jobs:
  slow:
    runs-on: [self-hosted, mitm]
    steps:
      - run: sleep 60
```

`03-cancellation/scenario.toml` submits `workflow.yml`, waits for `job_assigned`, waits 5 seconds, cancels, then waits for `job_completed` for 120 seconds.

### 7. Implement comparison report

`bin/compare.sh --scenario <scenario-name>` calls `bin/_compare.py` over:

- `captures/official/<scenario-name>/latest/flows.jsonl`
- `captures/runner-server/<scenario-name>/latest/flows.jsonl`

`_compare.py` writes `reports/<scenario-name>/<utc-iso8601>.md` with:

1. Scenario description and both `summary.json` objects.
2. Endpoint matrix table with columns: `method`, `normalized_path`, `official_count`, `runner_server_count`, `official_mean_ms`, `runner_server_mean_ms`, `official_statuses`, `runner_server_statuses`.
3. Missing endpoints: present only in official; present only in runner-server.
4. For each shared endpoint: unified diff of first JSON request body, first JSON response body, and sorted header-name sets.
5. Timing: p50 and p95 duration per endpoint per backend.

Normalization rules, in order:

- Replace GUIDs with `{guid}` using `[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`.
- Replace numeric path segments with `{n}` using `(?<=/)\d+(?=/|$)`.
- Sort query parameters by key.
- Keep query keys but replace values of `sessionId`, `lastMessageId`, `api-version`, `taskInstanceId`, `requestId`, and `agentId` with `{volatile}`.

Report redaction: before markdown output, replace JWT-shaped strings matching `eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}` and PAT-shaped strings matching `[A-Za-z0-9_]{30,}` with `***REDACTED***`.

### 8. Document the operator workflow

`experiments/mitm/README.md` gives this exact order:

```sh
cd experiments/mitm
python3 -m venv .venv
. .venv/bin/activate
pip install -e .

# official capture: set these first
export GITHUB_OWNER=...
export GITHUB_REPO=...
export GITHUB_REF=main
export GITHUB_RUNNER_TOKEN=$(gh api -X POST /repos/$GITHUB_OWNER/$GITHUB_REPO/actions/runners/registration-token --jq .token)
bin/record.sh --backend official --scenario 01-register-and-idle

# runner.server capture
bin/up-runner-server.sh
bin/record.sh --backend runner-server --scenario 01-register-and-idle
bin/down-runner-server.sh

# compare
bin/compare.sh --scenario 01-register-and-idle
```

README also states: if the official-runner capture fails with a TLS validation error, import `.cache/mitmproxy/mitmproxy-ca-cert.pem` into the user keychain and rerun; cleanup command is `security delete-certificate -c mitmproxy ~/Library/Keychains/login.keychain-db`.

## Critical files & anchors

- `README.md` lines 31–33: this repo already pins `ChristopherHX/runner.server` as the upstream reference and names `PRELOOP_UPSTREAM_RUNNER_SERVER_REF`.
- `docs/fidelity-gap.md` lines 93–143: endpoint families the official runner uses: connection data, registration, sessions, messages, timeline, logs, completion, action download, cache, artifacts.
- `docs/architecture.md` lines 12–19: `_apis/...` is the source-of-truth runner protocol surface; conformance belongs in `aksh-conformance`.
- `https://github.com/ChristopherHX/runner.server` README: runner setup uses `./config.sh --url http://localhost/runner/server --token "ThisIsIgnored"`; scheduling uses `Runner.Client --workflow ... --event ... --server http://localhost`; webhook endpoint is `/runner/server/_apis/v1/Message`.
- `https://github.com/ChristopherHX/github-act-runner` README: describes itself as a reverse-engineered compatible self-hosted runner using `nektos/act`, so it is excluded from the control-plane comparison.

## Verification

Run these after implementation:

1. `cd experiments/mitm && python3 -m venv .venv && . .venv/bin/activate && pip install -e .` succeeds.
2. `bin/up-runner-server.sh` succeeds, then `curl -fsS http://127.0.0.1:5000/_apis/connectionData >/tmp/connectionData.json` succeeds.
3. With `GITHUB_OWNER`, `GITHUB_REPO`, `GITHUB_REF`, and `GITHUB_RUNNER_TOKEN` set, `bin/record.sh --backend official --scenario 01-register-and-idle` creates `captures/official/01-register-and-idle/latest/flows.jsonl` with at least one flow whose host is `github.com` or `pipelines.actions.githubusercontent.com`.
4. `bin/record.sh --backend runner-server --scenario 01-register-and-idle` creates `captures/runner-server/01-register-and-idle/latest/flows.jsonl` with at least one path containing `/_apis/connectionData`.
5. `bin/compare.sh --scenario 01-register-and-idle` creates a markdown report containing `Endpoint matrix`, `Missing endpoints`, and at least one normalized path beginning with `/_apis/`.
6. Repeat official and runner-server recording for `02-trivial-job`; the report must contain `PipelineAgentJobRequest` or a binary-body hash entry for the flow that contains the encrypted job message. This proves the experiment reaches the job-dispatch bytes, not just registration.
7. Repeat for `03-cancellation`; the report must include either a `cancelWorkflow` runner-server call or an official GitHub cancellation API call and a later `job_completed` match.

## Assumptions & contingencies

- This plan targets the current workstation (`darwin arm64`). If executing on Linux x64, change the runner download asset from `osx-arm64` to `linux-x64`; do not change capture schema or comparison rules.
- Prefer per-process trust (`SSL_CERT_FILE` and `NODE_EXTRA_CA_CERTS`) over system keychain changes. If official-runner TLS still fails, use the README keychain fallback exactly once and document cleanup.
- The official GitHub capture requires a disposable repository with Actions enabled and a fresh registration token. The scripts do not create repos or tokens automatically beyond the documented `gh api` registration-token command.
- `runner.server` is run over HTTP on localhost; the mitmproxy capture still sees its runner protocol traffic because the official runner uses the configured `http_proxy` for HTTP and HTTPS. If the runner bypasses proxy for localhost despite `no_proxy=`, change the runner-server URL to the machine LAN IP and keep `no_proxy=` empty.
- The comparison intentionally preserves raw body bytes in captures and redacts only reports. Treat `captures/` as sensitive local data and keep it gitignored.
