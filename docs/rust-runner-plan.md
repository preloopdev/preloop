# Rust Runner (`aksh-runner`) — Implementation Plan

## Context

Reimplement the official GitHub Actions runner (`actions/runner`: Runner.Listener + Runner.Worker, C#) as a native Rust binary in this workspace, faithful enough that unmodified workflow YAML behaves identically and the wire traffic it produces is equivalent (under the repo's established normalizers) to the official runner v2.335.1. Deliverables: (1) new `crates/aksh-runner` binary, (2) a differential evaluation harness that gates every milestone against the official runner's behavior, (3) one spec/evidence doc per milestone under `docs/runner/`, (4) measured evidence for the speed/size question (the explicit goal of the rewrite).

**Source of truth: GitHub's real Actions service — never aksh.** aksh (this repo's control plane) is itself only ~65–70% faithful (`docs/fidelity-gap.md` §1) and still evolving; gating the runner against it would bake aksh's bugs into the runner. The fidelity oracle is, in priority order:

1. **Golden captures** — MITM recordings of the official runner v2.335.1 talking to GitHub's real service: `.runner-watch/golden/v2.335.1/<scenario>/flows.jsonl` (`summary.json` `backend: "official"`, verified), 11 scenarios. Raw captures: `~/mitm-proxy/experiments/mitm/captures/official/`.
2. **Live GitHub runs** — the Rust runner registered as a self-hosted runner on a real GitHub test repo; a job must go green in GitHub's own UI, and its MITM-captured traffic must diff clean against the golden capture for the same scenario.
3. **Upstream source** — `actions/runner` v2.335.1 for semantics not observable on the wire.

aksh's role is demoted to a **fast local dev substrate** (Tier-2 smoke loop, offline iteration, controlled benchmarking). **Conflict rule: when the Rust runner works against GitHub but fails against aksh, that is an aksh fidelity bug — file it in `docs/fidelity-gap.md` and do not contort the runner.**

## Settled architecture decisions

- **Location**: new crate `crates/aksh-runner` in this workspace (added to `Cargo.toml` members). Rationale: it reuses `aksh-gha-protocol` (wire DTOs + session crypto), `aksh-gha-expressions`, and the runner-watch/conformance tooling directly. `docs/fidelity-gap.md:29-31` currently says "this repo is the control plane only" — update that paragraph in M0 to say the workspace now also hosts the Rust runner client, while runner *provisioning* (libkrun etc.) stays external.
- **One binary, two processes**: single `aksh-runner` binary with subcommands `configure`, `remove`, `run`, and a hidden `worker`. `run` (the listener) spawns `aksh-runner worker` as a child process per job and talks to it over stdin (newline-delimited JSON). This mirrors the official Listener/Worker process split (crash isolation, kill-on-cancel) without duplicating a binary. IPC framing is internal — it does NOT need byte compatibility with C# `ProcessChannel`/`StreamString`.
- **GitHub-current protocol path is primary**: the flow the official v2.335.1 runner actually speaks to github.com — registration → OAuth → distributedtask session + message poll (AES-encrypted broker-ref messages) → broker/run-service `acquirejob`/`renewjob`/`completejob` → results-service Twirp for step status and logs. This lands in M2–M3. The legacy AzDO full-job path (`PipelineAgentJobRequest` over the message queue + Timeline/Logfiles/FinishJob reporting — what aksh's v2.322-era local loop uses) is a **secondary compat mode** (`--via azdo`), implemented in M9 for aksh/GHES. The golden capture for each scenario is the definitive request sequence; when in doubt, implement exactly what the capture shows.
- **MITM-capturable HTTP client**: the runner's reqwest client honors `HTTPS_PROXY`/`HTTP_PROXY` (reqwest default) and trusts an extra root CA via `--ca-bundle <pem>` / `SSL_CERT_FILE` (`reqwest::Certificate::from_pem` + `add_root_certificate`). Required so Rust-runner↔GitHub traffic can be recorded with the same mitmproxy setup that produced the goldens. The official .NET runner trusts the system store; this flag is the Rust equivalent.
- **Host execution first, containers later**: default self-hosted semantics = run steps on the host (matches official self-hosted runner); job/service/docker-action containers arrive in M7 via the docker CLI, mirroring `ContainerOperationProvider`.
- **OS scope**: macOS + Linux (bash/sh/python/pwsh shells) — user-confirmed. Windows (cmd/powershell, path semantics, service install) is explicitly deferred and tracked in the M12 fidelity audit doc — not silently dropped.
- **Node runtime for JS actions**: downloaded into `<runner-root>/externals/node20|node24` at `configure` time (from nodejs.org dist tarballs, versions pinned to what official v2.335.1 ships — implementer reads `src/Misc/externals.sh` in `actions/runner` for the exact versions). `--no-externals` flag falls back to `node` on PATH.
- **Process-tree kill without `unsafe`**: workspace forbids `unsafe_code`; use the `command-group` crate (async feature) for process-group spawn/kill instead of `pre_exec`.
- **Upstream reference**: `actions/runner` v2.335.1 (`https://github.com/actions/runner`, commit `7d737449ef346f6524f75688d0c9c95fa10ba10a` per `docs/fidelity-gap.md:33`). `preloopdev/prerun` is a private mirror of it (404 publicly); use `actions/runner` paths. `nektos/act` (`pkg/runner`, `pkg/container`, `pkg/exprparser`) is a secondary reference for pragmatic Go ports of the same semantics. A local official runner install exists at `~/mitm-proxy/experiments/mitm/.cache/runner-official` (used by `autoresearch.sh:27`) — its `.runner`/`.credentials`/`.credentials_rsaparams` files are the schema source of truth for M1.

## What already exists (verified this session)

Reuse these; do not reinvent:

- **Golden GitHub captures** (the truth baseline): `.runner-watch/golden/v2.335.1/{01-register-and-idle,06-multi-step,07-step-failure,08-job-outputs-needs,09-matrix-fan-out,10-uses-checkout,11-cache-roundtrip,12-artifact,13-composite-action,14-annotations,15-oidc-id-token}/flows.jsonl` — official runner ↔ real GitHub, `backend: "official"`, mitmproxy 12.2.3. Scenario recipes: `experiments/mitm/scenarios/NN-name/{scenario.toml, NN-name.yml}`.
- `crates/aksh-gha-protocol/src/azdo.rs` — `AgentJobRequestMessage` (plan, timeline, variables, maskHints, resources/endpoints, contextData, steps, actionsDownloadInfo), `TaskAgentMessage {message_id, message_type, body, iv}`, `TaskAgentSession`/`EncryptionKey`, `TimelineRecord` (incl. issues + background-step fields), `TaskStep`, `TaskReference`, `PipelineContextData` (compressed wire encoding), `JobCompletedEvent`, `TaskLog`, `LogReference`, `VssJsonCollectionWrapper<T>`, `message_type` constants (`PIPELINE_AGENT_JOB_REQUEST`, `RUNNER_JOB_REQUEST`, `CANCEL_JOB`, …). Serde renames preserve upstream wire naming.
- `crates/aksh-gha-protocol/src/crypto.rs` — `AgentRsaKeypair` (2048-bit gen, `wrap_key`/`unwrap_key` RSA-OAEP-SHA1, `public_key_xml`), `AgentRsaPublicKey`, `SessionEncryption` (AES-256-CBC + PKCS#7, `generate`/`from_key`/`encrypt`/`decrypt`). Runner side (generate keypair, unwrap session key, decrypt messages) is fully representable.
- `crates/aksh-gha-expressions/src/lib.rs` — `eval_expression(input, &Context) -> Result<Value, ExpressionError>`, `eval_bool`, `is_truthy`, `trim_expression_markers`; `Context { roots, success/failure/cancelled }`. Known gaps to close in M4: `hashFiles` is a stub, no bracket access `a['b']`/`a[0]`, no object filter `a.*.b`, incomplete `format()` escaping.
- `crates/aksh-gha-parser` — `parse_action_metadata()` exists (extend for full `action.yml` in M6); `job_builder::build_agent_job_message` shows what the aksh server puts in `TaskStep` (inputs/env as `TemplateStringMap` type:2, `condition` defaults to `success()`).
- `crates/aksh-runner-server` — local substrate route table (AzDO `_apis/v1/{Agent,AgentSession,Message,AgentRequest,Timeline,Logfiles,TimeLineWebConsoleLog,FinishJob,ActionDownloadInfo}`, broker `/runner/session|message|acknowledge` + `/broker/:id/*job`, Twirp results/cache/artifact stubs, `/api/v3/actions/runner-registration`, `/_apis/v1/oauth2/token`, native `/api/v1/runs` + `events.ndjson`). Key anchors: `serve` (~line 174), `next_message` (~1705), `build_task_agent_message` (~1834), `broker_acquire_job` (~1492), `submit_run` (~928).
- `crates/runner-watch/src/compare.rs` — `render_report`: diffs two `flows.jsonl` dirs with path normalization (GUID/digit), secret redaction, per-endpoint status/header/body comparison. Currently bin-only (`src/main.rs` + `compare.rs` module; no `lib.rs`).
- `crates/aksh-conformance/src/main.rs` — bin with `expand-fixtures`/`compare-command`/`golden` subcommands; `record`/`replay`/`fuzz` are placeholders. This is where the runner harness subcommands land.
- Workflow fixtures: `fixtures/upstream-workflows/*.yml`, `fixtures/golden/{simple-echo,matrix-expand,needs-dag}.yml+json`.
- E2E orchestration to copy from: `scripts/e2e-start.sh` (registration-token → `config.sh` → submit → `run.sh` → assert), `autoresearch.sh` (METRIC emission), `scripts/e2e-setup.sh` (port-80→9090 pfctl redirect — needed **only** for the official runner against local aksh, which strips non-default ports; the Rust runner must NOT strip ports).

## Approach

Milestones are ordered; each ends green (`just test-ci` passes) and each has a doc + gates. **Every milestone has two gate tiers:**

- **Tier 1 (truth, GitHub)**: live run on a real GitHub test repo (Rust runner registered self-hosted, job green in GitHub UI) and/or MITM flow diff of Rust-runner↔GitHub vs the official↔GitHub golden for that scenario. This is the acceptance bar.
- **Tier 2 (local, aksh)**: `runner-e2e` against a local `aksh-runner-server` — fast offline iteration and regression smoke. Never overrides Tier 1; divergences where GitHub passes and aksh fails are filed as aksh gaps.

Dependencies: M0→M1→M2→M3→M4→M5 strictly sequential; M6 after M5; M7 after M6; M8 after M6; M9 after M3 (independent, any time); M10 after M6; M11 after M5; M12 last.

Every milestone Mx begins by writing `docs/runner/<its doc>` with sections `Goal / Upstream references / Deliverables / Acceptance / Status & evidence`, and ends by filling `Status & evidence` with actual gate output. The docs are the "each step in a docs folder" deliverable.

### M0 — Scaffolding, docs skeleton, harness plumbing

1. Add `crates/aksh-runner` to workspace `members` (Cargo.toml:2-12). Crate: `src/main.rs` (clap CLI) + `src/lib.rs` with empty module tree: `settings`, `configure`, `listener` (`mod message_listener; mod broker_listener; mod job_dispatcher; mod oauth;`), `client` (`mod azdo; mod broker; mod run_service; mod results; mod actions_download; mod http;`), `worker` (`mod job_runner; mod execution_context; mod contexts; mod steps_runner; mod job_extension; mod server_queue; mod template; mod commands; mod file_commands; mod matchers; mod handlers; mod actions; mod container_ops;`), `process`. Deps (workspace-inherited where present): `tokio`, `reqwest`, `serde`, `serde_json`, `clap`, `anyhow`, `thiserror`, `tracing`, `uuid`, `base64`, `sha2`, `rand`, `indexmap`, `aksh-gha-protocol`, `aksh-gha-expressions`, `aksh-gha-parser`; add NEW workspace deps `flate2 = "1"`, `tar = "0.4"`, `command-group = { version = "5", features = ["with-tokio"] }` (no existing equivalents in workspace).
2. CLI contract (clap derive):
   - `aksh-runner configure --url <URL> --token <TOKEN> [--name <s>] [--labels a,b] [--work _work] [--runner-group default] [--unattended] [--replace] [--ephemeral] [--no-externals]`
   - `aksh-runner remove --token <TOKEN>`
   - `aksh-runner run [--once] [--via broker|azdo]` (default `broker` — the GitHub-current path)
   - `aksh-runner worker` (hidden; reads NDJSON messages on stdin)
   - Global: `--ca-bundle <pem>` (also honors `SSL_CERT_FILE`) added to the shared `client::http` builder; proxies via standard `HTTPS_PROXY`/`HTTP_PROXY`.
   - `aksh-runner --version` prints `aksh-runner <semver> (protocol-compat 2.335.1)`.
3. Add `[profile.release] lto = "thin"; codegen-units = 1; strip = true` to workspace Cargo.toml (size goal; affects all bins — intended).
4. Create `docs/runner/README.md`: index table (milestone → doc → status → gate commands, both tiers) listing the 13 docs named below, plus the doc template.
5. Create `docs/runner/00-architecture.md`: the two-process model, module map, protocol-path matrix (GitHub-current broker/results = primary; AzDO legacy = compat), the GitHub-as-truth doctrine and conflict rule, IPC framing spec (below).
6. Worker IPC spec (write into 00-architecture.md, implement in M3): listener→worker messages, one JSON object per line on worker stdin: `{"t":"job","body":<AgentJobRequestMessage JSON>}`, `{"t":"cancel","timeout_secs":<u64>}`, `{"t":"shutdown"}`. Worker exit code: `0` = job executed and reported (whatever the job result), `1` = infra failure before/while reporting (listener then abandons the request: broker `completejob` result=Failed, or AzDO AgentRequest PATCH in `--via azdo`). No worker→listener channel; the worker reports to the server directly.
7. Harness plumbing (independent of M1, do in parallel): add `src/lib.rs` to `crates/runner-watch` exposing `pub mod compare;` (keep `main.rs` bin working; move nothing else). Add `runner-watch` as a dependency of `aksh-conformance`.
8. `docs/fidelity-gap.md:29-31` doctrine paragraph: reword to "Runner *provisioning* integrations live in separate repos; the Rust runner protocol client (`aksh-runner`) lives in this workspace."
9. Justfile: add `build-runner: cargo build --release -p aksh-runner`.

Gate: `cargo build --release -p aksh-runner && target/release/aksh-runner --version` prints the version line; `just test-ci` green.

### M1 — Configuration & registration (doc: `docs/runner/01-config-registration.md`)

Port of `src/Runner.Listener/Configuration/` (ConfigurationManager). The request shapes come from the golden capture's `api.github.com` + registration flows, not from aksh.

1. `settings.rs`: `RunnerSettings` and `CredentialData` serde structs mirroring the official `.runner` / `.credentials` / `.credentials_rsaparams` JSON **exactly** — copy field names from the real files at `~/mitm-proxy/experiments/mitm/.cache/runner-official/{.runner,.credentials,.credentials_rsaparams}` (official writes UTF-8 BOM — accept BOM on read like `autoresearch.sh:100` does, write without). Persist/load from the runner root dir (dir containing the binary by default, `--runner-root` override for tests).
2. Extend `crates/aksh-gha-protocol/src/crypto.rs` with `AgentRsaKeypair::to_rsaparams_json()` / `from_rsaparams_json()` matching the C# `RSAParameters` field names used in `.credentials_rsaparams` (d, dp, dq, exponent, inverseQ, modulus, p, q; base64) — verify names against the real file. No existing equivalent (only XML export exists).
3. `configure.rs` flow, mirroring the official ConfigurationManager order **as observed in the `01-register-and-idle` golden**:
   a. `POST https://api.github.com/actions/runner-registration` with header `Authorization: RemoteAuth <token>` and body `{"url": <repo/org url>, "runner_event": "register"}` → response gives the registration OAuth token + pipelines/service URL. (aksh serves the same shape at `/api/v3/actions/runner-registration` for the local loop.)
   b. `GET {service url}/_apis/connectionData` — capture location data.
   c. Generate `AgentRsaKeypair`; register the agent (`POST` to the pool agents route from the capture) with a `TaskAgent` DTO carrying `authorization.publicKey {exponent, modulus}` and labels — reuse `aksh_gha_protocol::azdo::TaskAgent`.
   d. Persist `.runner` (agentId, agentName, poolId, serverUrl, gitHubUrl, workFolder), `.credentials` (scheme=OAuth, authorizationUrl, clientId from registration response), `.credentials_rsaparams`.
   e. Unless `--no-externals`: download node20+node24 tarballs to `externals/` (versions const in `configure.rs`, from upstream `src/Misc/externals.sh`); skip with warning on download failure (do not fail configure — matches "runner works, JS actions need node" degradation, surfaced at job time).
4. `remove` subcommand: agent DELETE per capture flow + delete the three files.
5. Failure handling: non-200 registration → exit 1 with the server's error body; existing `.runner` without `--replace` → exit 1 "already configured".

Gates:
- **Tier 1**: `aksh-runner configure` against a real GitHub test repo (token from `gh api -X POST repos/$REPO/actions/runners/registration-token -q .token`) — runner appears as "Offline" self-hosted runner in the repo's Settings→Actions→Runners UI; the MITM-captured registration flows diff clean vs the golden's registration segment (normalizers: tokens, GUIDs, runner name).
- **Tier 2**: `cargo test -p aksh-runner settings::` (round-trip serde tests against scrubbed copies of the real official files checked into `fixtures/runner/config/`); configure against local aksh on 9090 succeeds; re-running without `--replace` fails.

### M2 — OAuth, session, message listener (doc: `docs/runner/02-session-message-loop.md`)

Port of `MessageListener.cs` + `BrokerMessageListener.cs` + VssOAuth. Implement **the exact v2.335.1 sequence in the `01-register-and-idle` golden capture** (connectionData → OAuth token → session create → long-poll), including both the distributedtask message poll (AES-encrypted messages) and the broker session/message endpoints where the capture uses them.

1. `listener/oauth.rs`: build the client-credentials request the official runner sends: `POST {authorizationUrl}` form-encoded `grant_type=client_credentials&client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer&client_assertion=<JWT>`; JWT = RS256 signed with the runner RSA key, claims `{sub: clientId, iss: clientId, aud: authorizationUrl, jti: uuid, nbf, exp: now+5m}`. Hand-roll RS256 (base64url(header).payload + `rsa` PKCS#1v1.5-SHA256 signature) as `sign_rs256_jwt(header, claims, keypair) -> String` in `aksh-gha-protocol::crypto` — no `jsonwebtoken` dep (workspace already has rsa/sha2/base64). Verify the request shape byte-level against the golden's `token.actions.githubusercontent.com` flows. Cache token, refresh on 401/403 or expiry.
2. `client/azdo.rs` + `client/broker.rs`: typed clients (reqwest via `client::http`, bearer auth); copy exact paths + query strings (`api-version=…`, `sessionId`, `lastMessageId`, `status`) from the golden capture. Methods: `create_session`, `delete_session`, `get_message`, `delete_message` (distributedtask shard); `broker_session`, `broker_get_message`, `broker_acknowledge` (broker shard).
3. `listener/message_listener.rs`: session create → `TaskAgentSession`; unwrap `encryptionKey.value` with `AgentRsaKeypair::unwrap_key` when `encrypted=true`, else raw. Long-poll loop (50s client timeout; retry/backoff porting `ErrorThrottler.cs`); on message: base64-decode body, `SessionEncryption::from_key(session_key).decrypt(body, iv)`, dispatch on `messageType`; ack (DELETE / broker acknowledge, per capture).
4. Message dispatch table (`message_type` constants in `azdo.rs`): `RunnerJobRequest` (broker ref → M3 acquire), `PipelineAgentJobRequest` (legacy full payload → M9), `JobCancellation` → M3/M10; `AgentRefresh` → log `self-update requested by server; aksh-runner does not self-update` and continue (decision: no-op, never crash); `BrokerMigration` → re-resolve broker URL and continue (port from `BrokerMessageListener.cs`); unknown → warn + ack.
5. Signals: SIGINT/SIGTERM → delete session, exit 0. `run --once`: exit after first job completes (ephemeral support).

Gates:
- **Tier 1**: run `aksh-runner run` under mitmproxy against the real test repo for ≥60s idle (harness `runner-e2e --target github --idle-secs 60 --mitm`), then `runner-diff --scenario 01-register-and-idle --target github`: endpoint set, ordering, and status parity vs the golden (normalizers: User-Agent, tokens/JWTs, GUIDs, long-poll timing). Runner shows "Idle" (green) in GitHub UI. Accepted diffs documented in the milestone doc.
- **Tier 2**: same scenario against local aksh (`runner-e2e --idle-secs 20`), no crash, session created and cleanly deleted.

### M3 — Worker spawn, job lifecycle via run-service + results (doc: `docs/runner/03-worker-and-job-lifecycle.md`)

Port of `JobDispatcher.cs` (listener side), `Worker.cs`/`JobRunner.cs` skeleton, and the GitHub-current reporting clients: `RunServer.cs`/`ActionsRunServer.cs`/`BrokerServer.cs` (acquire/renew/complete) + `ResultsServer.cs` (Twirp step updates, signed-blob log upload). **The `06-multi-step` golden capture is the definitive request sequence for job reporting — implement exactly the flows it shows.**

1. `listener/job_dispatcher.rs`: on `RunnerJobRequest` ref message: `POST {run-service}/acquirejob` (client `client/run_service.rs`) → full `AgentJobRequestMessage` (type exists); spawn `current_exe() worker`, write the `{"t":"job",...}` line, wait with cancellation support; background renew loop (`renewjob`, interval = lock duration/2, port from `JobDispatcher.cs`); on `JobCancellation` for the active request send `{"t":"cancel",...}`, and if the worker ignores it for `timeout_secs` kill the process group (`command-group`). Only one job at a time (official behavior for a single runner).
2. `worker/job_runner.rs`: deserialize message; extract plan/timeline/results endpoints + tokens from message `variables`/`resources.endpoints`; execute steps (placeholder in this milestone — M4/M5 make it real; the M4 gate below proves the placeholder is gone); report:
   a. Step status transitions via Twirp `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` (client `client/results.rs`), batched per the intervals below.
   b. Step/job logs: `GetStepLogsSignedBlobURL`/`GetJobLogsSignedBlobURL` → PUT log content to the returned URL, treating the URL as fully opaque (GitHub returns Azure blob URLs; aksh returns local URLs — same client code must work for both). Websocket live-log feed is a deferred gap (logs appear at step completion; recorded in the milestone doc and M12 audit).
   c. Completion: `POST {run-service}/completejob` with result + outputs (+ step results/annotations as the golden shows).
3. `worker/server_queue.rs`: background reporting queue — step-update flush every 1s, log-line buffers flushed per step completion (matching the buffered upload the official runner does when websocket is unavailable). Port batching semantics from `JobServerQueue.cs`; the intervals above are the decision.
4. Result mapping: step conclusions → job result per official rules (any failed non-continue-on-error step → Failed; cancelled → Canceled; else Succeeded).
5. H1 harness lands here (see Harness section) so the gates below run.

Gates:
- **Tier 1**: push `fixtures/golden/simple-echo.yml` (adapted to `runs-on: [self-hosted]`) to the test repo; `runner-e2e --target github --workflow simple-echo` — job goes **green in GitHub's UI**, logs visible in the GitHub log viewer; MITM diff of the job-reporting flows vs the `06-multi-step` golden's reporting segment (endpoint set + status parity).
- **Tier 2**: `runner-e2e --workflow fixtures/golden/simple-echo.yml` against local aksh via broker path: verdict `run_status == "Succeeded"`.

### M4 — Execution context, contexts, runner-side expressions (doc: `docs/runner/04-execution-context.md`)

Port of `ExecutionContext.cs`, `StepsRunner.cs`, contexts (`GitHubContext.cs`, `RunnerContext.cs`, `StepsContext.cs`, `JobContext.cs`), plus expression-engine gap closure.

1. `worker/contexts.rs`: assemble runner-side contexts from `AgentJobRequestMessage.contextData` (`PipelineContextData` dict: github/matrix/needs/strategy/vars/inputs) + locally built: `runner` (name from `.runner`, `os` = `Linux|macOS`, `arch` = `X64|ARM64` via `cfg!`, `temp`, `tool_cache`, `workspace`), `env` (message variables + accumulated GITHUB_ENV), `job` (`status`, later `container`/`services`), `steps` (accumulating `{outcome, conclusion, outputs}`), `secrets` (variables where `isSecret`, values registered as masks).
2. `worker/execution_context.rs`: per-step context: env stack, secret masker (mask set = maskHints + secret variable values + `::add-mask::` additions; applied to every log line before upload — replace with `***`), issue collection (annotations), debug flag (`ACTIONS_STEP_DEBUG` → `##[debug]` lines), cancellation token.
3. `worker/template.rs`: evaluate `${{ }}` in received step fields (`condition`, `inputs`, `environment`, `displayName`, run script) against runner contexts using `aksh_gha_expressions::eval_expression`. Real GitHub sends unresolved template tokens — evaluating runner-side is what the official runner does; it also covers aksh's pre-resolved payloads (literals pass through).
4. Close expression gaps in `crates/aksh-gha-expressions` (each with unit tests mirroring upstream `Expressions2` behavior): bracket access (`a['b']`, `a[0]`), object filter `a.*.b`, `format()` `{{`/`}}` escaping, real `hashFiles(patterns…)` = glob relative to a new `Context::workspace_root: Option<PathBuf>` (setter `with_workspace_root`), SHA-256 of each file's bytes, then SHA-256 over the concatenated digests, empty match → `""` (verify against upstream `src/Runner.Worker/Expressions/` and act `pkg/exprparser`).
5. `worker/steps_runner.rs`: sequential step execution; evaluate `if` with correct status-function semantics (`success()` = no earlier failure in the job, etc. — feed `Context.success/failure/cancelled` from job state); `continue-on-error` (outcome=Failed, conclusion=Succeeded); `timeout-minutes` per step (tokio timeout → kill process group, conclusion=Failed with timeout annotation).
6. Fixture `fixtures/runner/env-parity.yml` (new): steps that `echo` the full `github`/`runner`/`job`/`strategy` contexts via `${{ toJSON(...) }}` and `env | sort`.

Gates:
- **Tier 1 (dual-run on GitHub)**: run `env-parity.yml` and `fixtures/upstream-workflows/dumpcontexts.yml` on the test repo twice — once with the **official runner** registered, once with the Rust runner — and diff the two GitHub-side log outputs (normalizers: paths, runner name, timestamps, versions). The official runner **on GitHub** is the oracle for context population, not aksh's job builder. Live `07-step-failure` scenario green-with-expected-failure in GitHub UI + MITM diff vs its golden.
- **Tier 2**: expression unit tests (`cargo test -p aksh-gha-expressions`); `runner-e2e` local on dumpcontexts.

### M5 — Script steps: process invoker, workflow commands, file commands (doc: `docs/runner/05-script-steps.md`)

Port of `ScriptHandler.cs`/`ScriptHandlerHelpers.cs`, `ProcessInvoker.cs`, `ActionCommandManager.cs`, `FileCommandManager.cs`.

1. `process.rs`: `ProcessInvoker` — spawn via `command-group`, merged env, cwd, line-buffered stdout/stderr streamed to a callback (order-preserving per stream), exit code, kill-tree on cancel/timeout, UTF-8 lossy decoding.
2. `worker/handlers/script.rs`: write script to `_work/_temp/<uuid>.sh`; shell resolution and exact command lines copied from upstream `ScriptHandlerHelpers.cs` (defaults to port: `bash --noprofile --norc -e -o pipefail {0}` for `shell: bash` and the default when bash exists; `sh -e {0}` fallback; `python {0}`; `pwsh -command ". '{0}'"` — implementer verifies each against that file). `working-directory`, step `env` merge (job env < step env < file-command env), exit code → outcome.
3. `worker/commands.rs`: parse `::name k=v,k2=v2::data` lines (and legacy `##[name]`), with the official unescaping (`%25→%`, `%0D→\r`, `%0A→\n`; properties additionally `%3A→:`, `%2C→,` — verify against `ActionCommandManager.cs`). Commands: `add-mask`, `add-path` (prepend to PATH for later steps), `debug`, `notice|warning|error` (properties: `title,file,line,endLine,col,endColumn` → annotation), `group`/`endgroup`, `echo on|off`, `stop-commands <token>`/`<token>` resume, legacy `set-output`/`save-state` (honor + emit the official deprecation warning line), `add-matcher`/`remove-matcher` (wire in M10).
4. `worker/file_commands.rs`: before each step create empty temp files and export `GITHUB_ENV`, `GITHUB_PATH`, `GITHUB_OUTPUT`, `GITHUB_STATE`, `GITHUB_STEP_SUMMARY`; after the step parse: `KEY=VAL` lines and `KEY<<DELIM … DELIM` heredocs (env/output/state), one-path-per-line (path), summary size cap 1MiB. Apply: env → job env for subsequent steps; outputs → `steps.<id>.outputs` and job outputs at completion; state → `state` context for post-steps (M6); step summary: upload via the results-service route the official runner uses (present in goldens if exercised — implementer checks `14-annotations`/`06-multi-step` captures; if not captured, record fresh official capture of a summary workflow first).
5. GITHUB_* env injection (`worker/job_extension.rs`, first half): full official set from `contextData.github` + runner paths (`GITHUB_ACTIONS=true`, `GITHUB_WORKSPACE`, `GITHUB_REPOSITORY`, `GITHUB_RUN_ID`, `GITHUB_SHA`, `GITHUB_REF*`, `GITHUB_EVENT_PATH` (write event JSON), `GITHUB_API_URL`, `RUNNER_OS/ARCH/NAME/TEMP/TOOL_CACHE`, `CI=true`, …) — the acceptance oracle is the M4 dual-run env-parity diff against the **official runner on GitHub**, not a hand-maintained list.
6. Workspace layout (`worker/job_extension.rs`): `_work/{repo-slug}/{repo-name}` convention, `_work/_temp`, `_work/_actions`, `_work/_tool` — port `PipelineDirectoryManager.cs` tracking (`_work/{n}/` numbered dirs with `TrackingConfig` JSON).

Gates:
- **Tier 1**: live `06-multi-step` and `14-annotations` scenarios on the test repo: GitHub UI shows identical step structure, log content (normalized), and annotations as an official-runner run of the same commit; MITM diffs vs both goldens. Masked-secret check runs live: workflow with a repo secret echoed → GitHub log shows `***`.
- **Tier 2**: `runner-e2e` green on `fixtures/upstream-workflows/{multiline_env,stepenv,localenv,job-continue-on-error}.yml`; unit goldens `fixtures/runner/commands/*.txt` (ported from upstream `Test/L0/Worker/ActionCommandManagerL0.cs`) pass.

### M6 — Actions: download, node, composite, pre/post (doc: `docs/runner/06-actions.md`)

Port of `ActionManager.cs`, `ActionManifestManager.cs`, `NodeScriptActionHandler.cs`, `CompositeActionHandler.cs`, `HandlerFactory.cs`, `JobExtension.cs` (pre/post ordering).

1. `worker/actions/manager.rs`: resolve `uses:` refs via the action-download-info flow shown in the `10-uses-checkout` golden (batch resolution + tarball URL + auth mode, incl. `BatchActionResolution`/bearer-codeload behavior as captured); download tar.gz (reqwest), extract with `tar`+`flate2` stripping the top-level dir into `_work/_actions/{owner}/{repo}/{ref}/`; local actions `./path` copy from workspace; `docker://` refs → M7. For the local aksh loop only: extend aksh's stub `action_download_info` handler to return real `https://api.github.com/repos/{o}/{r}/tarball/{ref}` URLs (small server change; keeps Tier-2 usable — optional, never a Tier-1 dependency).
2. `worker/actions/manifest.rs`: full `action.yml`/`action.yaml` parse — extend `aksh_gha_parser::parse_action_metadata` rather than a new parser: `runs.using` (`node12|node16|node20|node24|composite|docker`), `main/pre/post`, `pre-if/post-if`, `inputs` (default/required/deprecationMessage), `outputs` (+`value` for composite), docker `image/entrypoint/args/env`. Node12/16: map to node20 with the official deprecation warning annotation.
3. `worker/handlers/factory.rs` + `node.rs`: env = step env + `INPUT_<NAME>` (uppercased, spaces→underscores; defaults applied from manifest), run `<externals>/node24/bin/node <action>/<main>` via ProcessInvoker; `--no-externals` → PATH `node`.
4. `worker/handlers/composite.rs`: nested steps with their own scope: `inputs` context from `with`+defaults, `github.action_path`, nested steps share the job env but writes via GITHUB_ENV propagate out (official semantics), outputs mapped through `outputs.*.value` expressions evaluated after nested steps.
5. Pre/post: `worker/job_extension.rs` builds the final step list: pre steps (declared order, `pre-if` default `always()`), main steps, post steps (LIFO, `post-if` default `always()`, receive `state` context saved via `save-state`/GITHUB_STATE). Post steps run even when main failed (respect condition).
6. `github.token` / `ACTIONS_RUNTIME_TOKEN` for checkout come from the job message (real values on GitHub; whatever aksh provides locally).

Gates:
- **Tier 1**: live `10-uses-checkout` (real `actions/checkout@v4` cloning the test repo) and `13-composite-action` on the test repo — green in GitHub UI, MITM diffs vs goldens.
- **Tier 2**: pre/post ordering unit test (composite with pre+post asserting execution order via GITHUB_OUTPUT breadcrumbs); local composite fixture via `runner-e2e`.

### M7 — Containers: job/service containers, docker actions (doc: `docs/runner/07-containers.md`) — Linux (+macOS with Docker Desktop)

Port of `ContainerOperationProvider.cs`, `ContainerActionHandler.cs`, `StepHost.cs` (ContainerStepHost).

1. `worker/container_ops.rs`: docker CLI wrapper via ProcessInvoker: `docker version` capability check at job start when containers requested (fail job with the official "Docker not found" message otherwise); per-job `docker network create github_network_<uuid>`; pull/create/start job container with `-v <_work>:/__w` (+ externals/temp mounts), env-file, workdir `/__w/{repo}/{repo}`; service containers with health polling (`docker inspect --format {{.State.Health.Status}}` until healthy/timeout); teardown (rm -f containers, network rm) in a post job step that always runs.
2. `worker/handlers/script.rs` container path: when job container active, run steps via `docker exec -i --workdir <translated> --env-file <tmp> <cid> <shell…>`; path translation host `_work` ↔ `/__w` in env values and args (port `ContainerStepHost.TranslateToContainerPath`).
3. `worker/handlers/container.rs`: docker actions: `docker://image` direct; Dockerfile actions `docker build`; args/entrypoint from manifest with expression eval; `INPUT_*` env.
4. `runner.os`/context adjustments inside container jobs (github.workspace → container path in `github` context env vars).

Gates:
- **Tier 1**: record fresh official↔GitHub goldens for `16-container-job` and `17-service-container` first (scenario recipes exist; goldens deferred upstream), on a Linux host with docker; then live dual-run + MITM diff for the Rust runner.
- **Tier 2**: harness flag `--requires docker` skips cleanly when unavailable.

### M8 — Cache, artifacts, runtime env plumbing (doc: `docs/runner/08-cache-artifacts-env.md`)

The runner's own role here is env plumbing (the actions do the HTTP): inject `ACTIONS_RUNTIME_URL`, `ACTIONS_RUNTIME_TOKEN`, `ACTIONS_RESULTS_URL`, `ACTIONS_CACHE_URL`, `ACTIONS_CACHE_SERVICE_V2`, `ACTIONS_ID_TOKEN_REQUEST_URL/_TOKEN` from `AgentJobRequestMessage` variables/endpoints (port the mapping from `JobExtension.cs`). On real GitHub these values point at GitHub's hosted cache/results services and must work untouched — that is the truth test. For the local aksh loop, verify which variables aksh populates and extend `build_agent_job_message` in `crates/aksh-gha-parser/src/job_builder.rs` server-side if one is missing (aksh-side fix, official variable names).

Gates:
- **Tier 1**: live `11-cache-roundtrip` (actions/cache\@v4 save on run 1, restore-hit on run 2) and `12-artifact` (upload/download-artifact\@v4 round-trip) on the test repo against **GitHub's real cache/artifact services** — green in UI, MITM diffs vs goldens.
- **Tier 2**: same fixtures against aksh's twirp/cache stubs via `runner-e2e`.

### M9 — Legacy AzDO compat mode (`--via azdo`) for aksh/GHES (doc: `docs/runner/09-azdo-compat.md`)

Port of the legacy reporting surface the aksh local loop (and GHES-era servers) speak: `PipelineAgentJobRequest` full-payload messages over the encrypted message queue (M2 already decrypts; this adds the dispatch branch), and AzDO reporting: `PATCH AgentRequest` (receivedTime/result), `PATCH Timeline/{...}` with `VssJsonCollectionWrapper<Vec<TimelineRecord>>` (job record + per-step records with `order`, `LogReference` after upload), `POST Logfiles` create/append, `POST TimeLineWebConsoleLog` batched live lines (≤200 lines / 500ms), `POST FinishJob` with `JobCompletedEvent {jobId, requestId, result, outputs}`. All DTOs exist in `azdo.rs`; all routes exist in aksh (`patch_timeline_records`, `create_log`, `append_log`, `console_log`, `finish_job`, `agent_request_patch`).

This milestone is explicitly **not truth-gated against GitHub** (GitHub no longer speaks this path to v2.335 runners); its oracle is behavioral: the same workflow produces the same verdict via `--via azdo` and `--via broker` against aksh.

Gate: full Tier-2 corpus rerun with `--via azdo` (M3–M6 gate workflows) — identical verdict JSON (normalized) to the broker-path runs; `runner-diff --compare-vias` subreport included in the milestone doc.

### M10 — Cancellation, timeouts, OIDC, problem matchers, hardening (doc: `docs/runner/10-cancellation-oidc-hardening.md`)

1. Cancellation: `JobCancellation` message → dispatcher sends `{"t":"cancel"}` → worker cancels current step (kill process group), evaluates remaining steps' conditions under `cancelled()` semantics, runs `always()`/post steps with the official grace timeout, reports `Canceled`. Listener hard-kills worker after `timeout_secs` (from message, default 5 min). New scenario `18-cancel-mid-step` (long-sleep workflow, cancel via `gh run cancel` on GitHub / `POST /api/v1/runs/:id/cancel` on aksh) + fixture; **record the official↔GitHub golden first**, then diff.
2. Job-level `timeout-minutes` (default 360) enforced in worker alongside step timeouts.
3. Problem matchers (`worker/matchers.rs`): port `IssueMatcher.cs` — owner registry, single- and multi-line `loop` patterns, severity/fromPath/columns, `::add-matcher::path.json` / `::remove-matcher owner=…::`; matched log lines → annotations exactly like official. Unit goldens from upstream `Test/L0/Worker/IssueMatcherL0.cs` cases into `fixtures/runner/matchers/`.
4. OIDC: nothing runner-side beyond M8 env plumbing; gate with scenario `15-oidc-id-token`.
5. Hardening pass: every server call site gets the official retry/backoff semantics (transient 5xx retry ×3 exponential); `run` survives server restart (session recreate on 401/session-gone, port `MessageListener` re-create logic); `--once`/ephemeral unregisters on exit.

Gates:
- **Tier 1**: live `18-cancel-mid-step` (GitHub UI shows Cancelled, post steps ran) and `15-oidc-id-token` (real OIDC token minted, audience assertion passes) + MITM diffs vs goldens.
- **Tier 2**: matcher unit goldens; chaos test (`--chaos restart-server`: restart local aksh during idle poll, assert session re-established and a subsequent job completes).

### M11 — Performance & size evaluation (doc: `docs/runner/11-benchmarks.md`)

The point of the rewrite — measured, not asserted. Benchmarks run against **local aksh** deliberately: it is a controlled, network-noise-free substrate, and both runners face the identical server, so runner overhead is isolated. (GitHub-live timing is not a benchmark substrate; note this in the doc.)

1. `scripts/bench-runner.sh` (pattern: `autoresearch.sh`, which already emits `METRIC` lines): for each of {official runner at `~/mitm-proxy/experiments/mitm/.cache/runner-official`, `target/release/aksh-runner`}: fresh server + state dir, then measure:
   - `configure` wall time;
   - cold start: `run` launch → first message poll observed (server-side flow timestamp);
   - dispatch latency: `/api/v1/runs` POST → run Succeeded, median of 10 runs of `fixtures/golden/simple-echo.yml`;
   - throughput: 20 sequential single-step jobs, total wall time;
   - idle RSS after 60s (`ps -o rss=`), peak RSS during dogfood job;
   - size on disk: official = `du -sh` of runner dir (incl. externals + dotnet), rust = binary size + externals dir, both also with externals excluded.
   Emits `METRIC name=value` lines; `--json` writes `bench-results.json`.
2. `justfile`: `bench-runner: ./scripts/bench-runner.sh`.
3. Write results tables + interpretation into `docs/runner/11-benchmarks.md` (this doc is the deliverable answering "is Rust faster/smaller"). Note: the official runner requires the port-80 pfctl redirect (`just e2e-setup`); the script must check `scripts/e2e-setup.sh --status` before official-runner phases.

Gate: `just bench-runner` completes and produces the populated doc + `bench-results.json` with all metrics for both runners.

### M12 — Fidelity audit + repo docs (doc: `docs/runner/12-fidelity-audit.md`)

1. Write `docs/runner/12-fidelity-audit.md`: scorecard table mirroring `docs/fidelity-gap.md` §1 style — every upstream runner surface (from the `actions/runner` component list: Listener {config, session, broker, self-update, service install, checks}, Worker {contexts, steps, commands, file commands, matchers, actions, containers, DAP debugger, snapshot, hooks (`ACTIONS_RUNNER_HOOK_JOB_STARTED`), background steps coordinator}, OS matrix) with status ✅/⚠️/❌ and **evidence links: golden-diff reports and live-GitHub run URLs per scenario** (Tier-1 evidence mandatory for a ✅). Deferred items land here explicitly: Windows, self-update, DAP, snapshot, service install, job hooks, background steps, websocket live logs.
2. Update `README.md` crate list + `docs/architecture.md` + `AGENTS.md` (crate map, dev commands: `build-runner`, `bench-runner`, `runner-e2e`) to include `aksh-runner`, the harness subcommands, and the GitHub-as-truth doctrine.
3. `docs/runner/README.md` index: mark all milestone statuses with links to evidence.

Gate: `just test-ci` green across the workspace; docs index complete; every corpus scenario listed in the audit has a checked-in report under `.runner-watch/runner-conformance/`.

## Evaluation harness (built incrementally, gates every milestone)

### H1 (land with M3, skeleton in M0) — `runner-e2e` orchestrator

New module `crates/aksh-conformance/src/runner_e2e.rs`, subcommand:

```
aksh-conformance runner-e2e
  --target aksh|github                              # substrate (default aksh)
  (--runner-bin <path> | --official <runner-dir>)   # which runner to drive
  (--workflow <path> | --scenario <NN-name> | --idle-secs <n>)
  [--via broker|azdo] [--mitm] [--server-bin <path>] [--record-flows <dir>]
  [--requires docker] [--keep-temp] [--timeout-secs 300] [--json <out>] [--expect failed]
```

**`--target aksh` (Tier 2)**: temp state dir; spawn `aksh-runner-server serve --listen 127.0.0.1:<ephemeral> --state-dir <tmp> --record-flows <dir>`; obtain registration token via the `/api/v3/.../registration-token` route (copy the sequence from `scripts/e2e-start.sh:140-160`); configure + start the runner (Rust: `aksh-runner configure/run --runner-root <tmp>`; official: `config.sh`/`run.sh` — requires the port-80 redirect, check `scripts/e2e-setup.sh --status` and fail with instructions if absent); submit via `POST /api/v1/runs` (reuse `WorkflowSubmission`); stream `/api/v1/runs/:id/events.ndjson` to terminal state.

**`--target github` (Tier 1)**: requires env `GH_RUNNER_TEST_REPO=owner/repo` and an authenticated `gh` CLI. Registration token via `gh api -X POST repos/$GH_RUNNER_TEST_REPO/actions/runners/registration-token -q .token`; configure runner against `https://github.com/$GH_RUNNER_TEST_REPO`; workflow delivered by committing the fixture to `.github/workflows/` on a scratch branch and triggering `workflow_dispatch` via `gh workflow run` (fixtures get `on: workflow_dispatch` + `runs-on: [self-hosted]` variants under `experiments/mitm/scenarios/*/`, which already exist for the recorded scenarios); wait via `gh run watch`; verdict from `gh run view --json jobs,conclusion` + `gh api .../logs` (zip) for log content. With `--mitm`: launch the runner with `HTTPS_PROXY=<mitmproxy>` and `--ca-bundle <mitm-ca.pem>`, reusing the recording setup under `~/mitm-proxy/experiments/mitm/` that produced the goldens; flows land in the same `flows.jsonl` schema.

Verdict JSON (both targets):

```json
{ "target": "github", "via": "broker", "run_status": "Succeeded",
  "jobs": {"<job>": {"result": "Succeeded"}},
  "steps": [ <normalized step records: names, conclusions, ordinals> ],
  "logs": {"<step-ordinal>": {"sha256": "…", "text_path": "…"}},
  "annotations": [...], "outputs": {...}, "flows_dir": "…", "run_url": "…", "duration_ms": 0 }
```

Exit code 0 iff run Succeeded (or `--expect failed` for failure-shape scenarios like 07-step-failure).

### H2 (land with M3) — flow recording + `runner-diff`

1. Add `--record-flows <dir>` to `aksh-runner-server serve` (`crates/aksh-runner-server/src/main.rs` + `serve` in lib.rs): an axum middleware appending one JSON line per request/response to `flows.jsonl` using the exact existing schema (`method, scheme, host, path, request_headers, request_body_b64, request_body_json, status, response_headers, response_body_b64, response_body_json, ts_request, ts_response, duration_ms`). Local Tier-2 capture without mitmproxy. Skip streaming/long-poll bodies over 1MiB (record `"truncated": true`).
2. Subcommand `aksh-conformance runner-diff (--scenario <NN-name> | --workflow <path>) [--target github|aksh] [--via …] [--official-dir <runner-official>] [--compare-vias]`:
   - **`--target github` (Tier 1, the truth gate)**: runs `runner-e2e --target github --mitm` for the Rust runner and diffs its capture against the checked-in official↔GitHub golden `.runner-watch/golden/v2.335.1/<scenario>/flows.jsonl` via `runner_watch::compare::render_report`. Additionally fetches both runs' GitHub-side outcomes (`gh run view --json`, logs zip) for the semantic diff.
   - **`--target aksh` (Tier 2)**: runs `runner-e2e` twice locally (official runner, then Rust runner) against fresh aksh instances and diffs the two — runner-vs-runner on the same substrate.
   - Both write `.runner-watch/runner-conformance/<name>.md`: (a) flow diff, (b) verdict diff with normalizers: GUIDs→ordinals, timestamps→null, absolute paths→`{root}`, runner name/version strings, JWT/token/signed-URL bodies→`{token}`/`{signed-url}`. Exit non-zero on semantic mismatch; each milestone doc records its accepted/normalized diffs.

### H3 (land with M4, grow through M10) — unit golden corpus

`fixtures/runner/` tree: `config/` (official dotfiles, tokens scrubbed), `commands/` (workflow-command parse cases ported from upstream `Test/L0/Worker/ActionCommandManagerL0.cs`), `filecommands/` (env/output heredoc cases), `matchers/` (IssueMatcher cases), `expressions/` (bracket/filter/format/hashFiles cases), `env-parity.yml`. Driven by `cargo test -p aksh-runner` + `-p aksh-gha-expressions` golden tests — same pattern as `aksh-conformance golden`.

### Justfile additions (M0/H2)

```
build-runner:     cargo build --release -p aksh-runner
runner-e2e WF:    cargo run -p aksh-conformance -- runner-e2e --runner-bin target/release/aksh-runner --workflow {{WF}}
conform-runner S: cargo run -p aksh-conformance -- runner-diff --scenario {{S}} --target github
conform-local S:  cargo run -p aksh-conformance -- runner-diff --scenario {{S}} --target aksh
bench-runner:     ./scripts/bench-runner.sh
```

## Critical files & anchors

- `.runner-watch/golden/v2.335.1/*/flows.jsonl` — the truth baseline (official↔GitHub captures); every protocol decision defers to these.
- `crates/aksh-gha-protocol/src/azdo.rs` — `AgentJobRequestMessage`, `TaskAgentMessage`, `TimelineRecord`, `TaskStep`, `PipelineContextData`, `JobCompletedEvent`, `message_type` — every wire type the runner consumes/produces; extend, never fork.
- `crates/aksh-gha-protocol/src/crypto.rs` — `AgentRsaKeypair`, `SessionEncryption`; add `to/from_rsaparams_json` + `sign_rs256_jwt` here.
- `crates/runner-watch/src/compare.rs` — `render_report` reused by `runner-diff`; needs `lib.rs` exposure (M0.7).
- `scripts/e2e-start.sh` + `autoresearch.sh` — canonical official-runner orchestration (registration token flow, config.sh args, port-80 redirect checks, METRIC format) to port into `runner_e2e.rs` and `bench-runner.sh`.

## Verification

Per-milestone gates are listed inline above; the cross-cutting checks:

- Workspace health after every milestone: `just test-ci` (fmt-check + clippy -D warnings + `cargo test --workspace --quiet`), run from repo root.
- **Tier-1 truth check (first at M1, then every milestone)**: `cargo run -p aksh-conformance -- runner-diff --scenario <NN> --target github` → report at `.runner-watch/runner-conformance/<NN>.md`, exit 0; plus the live run visible green at the `run_url` in the verdict. Prereqs: `gh` CLI authenticated, `GH_RUNNER_TEST_REPO` set to a repo with self-hosted runners enabled, mitmproxy (goldens used 12.2.3) with its CA cert available for `--ca-bundle`.
- **Tier-2 local loop (first at M3)**: `cargo build --release --workspace && cargo run -p aksh-conformance -- runner-e2e --runner-bin target/release/aksh-runner --workflow fixtures/golden/simple-echo.yml --json /tmp/verdict.json` → `run_status == "Succeeded"` and step log contains the echoed line. Requires port 9090-free only; official-runner comparison runs additionally need `~/mitm-proxy/experiments/mitm/.cache/runner-official` + port-80 redirect (`sudo ./scripts/e2e-setup.sh`).
- Dogfood-on-itself (from M5, Tier 2): `runner-e2e --workflow .github/workflows/dogfood.yml` with `vars.AKSH_REPO_ROOT` injected — the Rust runner runs this repo's own fmt/clippy/test job.
- Perf claims come only from `just bench-runner` output (M11); no perf statement ships in docs without a `bench-results.json` behind it.

## Assumptions & contingencies

- **Crate/binary name `aksh-runner`** — user-confirmed. If the product later wants the `prerun` brand, rename is a mechanical `lsp rename_file` + Cargo.toml edit; do not block on it.
- **In-workspace placement** despite `docs/fidelity-gap.md`'s "control plane only" line — updated in M0.8. If the user later wants a separate repo, the crate has no path deps outside `crates/`, so extraction is `git filter-repo` + git-dep on aksh crates.
- **macOS+Linux first; Windows deferred** — user-confirmed; tracked as a post-M12 phase in the fidelity audit. If Windows becomes required mid-stream, it slots after M5 (cmd/powershell handlers + path semantics) without reordering other milestones.
- **GitHub is the oracle; aksh is a substrate** — user-confirmed. Conflict rule: Rust runner passes on GitHub but fails on aksh → file an aksh gap in `docs/fidelity-gap.md`, never contort the runner. The reverse (passes aksh, fails GitHub) is always a runner bug.
- **A GitHub test repo is available** (`GH_RUNNER_TEST_REPO`, self-hosted runners allowed, authenticated `gh`). If unavailable in an execution environment: Tier-1 gates degrade to **static golden diffing** (flows recorded against local aksh but compared for request-shape parity against the GitHub goldens' comparable segments) + Tier 2; mark each affected milestone doc's Tier-1 status "pending live validation" — never silently promote Tier 2 to acceptance.
- **`.runner`/`.credentials` field names** are taken from the real files at `~/mitm-proxy/experiments/mitm/.cache/runner-official`. If that dir is missing (fresh machine), download the official runner v2.335.1 release tarball, run `config.sh` against GitHub or local aksh once (per `scripts/e2e-start.sh`), and read the generated files.
- **Scenario goldens for 16/17/18** don't exist yet (16/17 deferred upstream, 18 is new). Record official↔GitHub captures with the existing mitm scenario flow before diffing; if recording infra is unavailable, gate those milestones on live-GitHub behavioral asserts (`gh run view` verdicts) and mark the flow-diff pending in the milestone doc.
- **Websocket live-log feed** deferred (HTTP buffered upload only): GitHub UI log tail lags until step completion. Recorded in M3 doc + M12 audit. If a scenario diff shows GitHub *requiring* the websocket path, implement it then (tokio-tungstenite, new dep) as an M10 hardening item.
- **Step-summary upload**: implemented against whatever route the goldens show the official runner using; if no golden exercises it, record a fresh official capture of a summary workflow before implementing. aksh-side support (if missing) is filed as a control-plane gap, not invented ad hoc.
