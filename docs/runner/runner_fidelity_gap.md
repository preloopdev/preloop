# aksh-runner — Runner Fidelity Gap Log

Tracks every deviation found between `aksh-runner` (Rust) and the official

`actions/runner` v2.335.1, discovered via E2E validation against the golden

MITM captures at `.runner-watch/golden/v2.335.1/`.

**Source of truth**: the official runner and GitHub's real service — NOT the aksh control plane.

---

## Issues Found &amp; Fixed (F001–F017 — all ✅ Fixed)

### F001 — CRITICAL: JWT algorithm PS256, not RS256

- **Found**: golden flow 10 shows JWT header `{"alg": "PS256"}` (RSA-PSS)
- **Was**: `sign_rs256_jwt` used `rsa::pkcs1v15::SigningKey` (PKCS#1 v1.5 = RS256)
- **Fix**: Changed to `rsa::pss::SigningKey<Sha256>` with `RandomizedSigner` for PS256
- **File**: `crates/aksh-gha-protocol/src/crypto.rs`

### F002 — CRITICAL: Registration response + credential sourcing

- **Found**: golden flow 0 returns `{token, token_schema: "OAuthAccessToken", url}`; golden flow 6 agent creation response contains `authorization.authorizationUrl` and `authorization.clientId` — these are the values the runner persists in `.credentials`
- **Was**: Code expected `token_schema.authorization_url` from registration response and fabricated `clientId` with `Uuid::new_v4()` fallback, breaking the OAuth exchange
- **Fix**: Extract `authorizationUrl` and `clientId` from the agent creation response's `authorization` block; propagate errors instead of inventing defaults
- **File**: `crates/aksh-runner/src/configure.rs`

### F003 — CRITICAL: Wrong agent creation endpoint path

- **Found**: golden flow 6 uses `_apis/distributedtask/pools/{id}/agents`
- **Was**: Code used `_apis/v1/pools/{id}/agents`
- **Fix**: Changed to match official path
- **File**: `crates/aksh-runner/src/configure.rs`

### F004 — CRITICAL: Missing pool discovery step

- **Found**: golden flows 4-5 show pool listing + agent name check before agent creation
- **Was**: Code skipped straight to agent creation
- **Fix**: Added `GET _apis/distributedtask/pools?poolType=Automation` and agent name check
- **File**: `crates/aksh-runner/src/configure.rs`

### F005 — HIGH: Agent creation request missing fields

- **Found**: golden flow 6 request has `maxParallelism`, `createdOn`, `id`, `status`, `provisioningState`
- **Was**: Code sent minimal fields
- **Fix**: Added all required fields matching official request body
- **File**: `crates/aksh-runner/src/configure.rs`

### F006 — CRITICAL: .credentials file format wrong

- **Found**: official format is `{scheme, data: {clientId, authorizationUrl, requireFipsCryptography}}`
- **Was**: Code had `clientId` and `authorizationUrl` as top-level CredentialData fields
- **Fix**: Moved into the `data` map to match official structure
- **File**: `crates/aksh-runner/src/settings.rs`

### F007 — HIGH: .runner file missing fields

- **Found**: official .runner has `isHostedServer`, `useV2Flow`, `serverUrlV2`
- **Was**: Code lacked these fields
- **Fix**: Added fields with correct serde renames
- **File**: `crates/aksh-runner/src/settings.rs`

### F008 — HIGH: Broker URL not derived from agent properties (F008)

- **Found**: golden flow 11 hits `broker.actions.githubusercontent.com/session` (separate host from the service URL)
- **Was**: Code used same base URL for broker endpoints or hardcoded GitHub's broker URL.
- **Fix**: Extract `ServerUrlV2` from the agent creation response's `properties.ServerUrlV2.$value` at configure time, persist it in `.runner` settings as `serverUrlV2`, and use it in the broker listener. Correctly resolves to the broker host on live GitHub and the local server URL on aksh-server.
- **File**: `crates/aksh-runner/src/configure.rs`, `crates/aksh-runner/src/listener/broker_listener.rs`
- **Status**: ✅ Fixed (2026-07-03)

### F009 — MEDIUM: Message poll URL missing query params and wrong values

- **Found**: golden flow 12: `disableUpdate=false` (lowercase), status is dynamic (`Online` idle, `Busy` during job)
- **Was**: Code hardcoded `disableUpdate=True` (wrong case) and `status=Online` (always)
- **Fix**: Changed to `disableUpdate=false`, added `busy` parameter to `get_message()` for dynamic status
- **File**: `crates/aksh-runner/src/client/broker.rs`

### F010 — MEDIUM: Broker acknowledge uses wrong HTTP method and query shape

- **Found**: golden flow 13: `POST /acknowledge?sessionId=X&status=Online&runnerVersion=...&os=...&architecture=...` with body `{"runnerRequestId": "<id>"}`
- **Was**: Code used `DELETE` with `messageId` in query string
- **Fix**: Changed to POST; no `disableUpdate` or `messageId` in query; `runnerRequestId` sent in POST body
- **File**: `crates/aksh-runner/src/client/broker.rs`

### F011 — MEDIUM: Encryption assumed on broker path

- **Found**: golden flow 11 session has no `encryptionKey`; flow 12 body is plaintext
- **Was**: Code required decryption
- **Fix**: Made encryption optional; if no encryptionKey in session, parse body directly
- **File**: `crates/aksh-runner/src/listener/broker_listener.rs`

### F012 — HIGH: Results Twirp service name wrong for log URLs

- **Found**: golden flow 20 uses `results.services.receiver.Receiver/GetStepLogsSignedBlobURL`
- **Was**: Code used `github.actions.results.api.v1.WorkflowStepUpdateService/GetStepLogsSignedBlobURL`
- **Fix**: Changed log URL endpoints to use `results.services.receiver.Receiver`
- **Note**: `WorkflowStepsUpdate` path was already correct
- **File**: `crates/aksh-runner/src/client/results.rs`

### F013 — HIGH: completejob request body incomplete and never POSTed

- **Found**: golden flow 25 body has `planId`, `jobId`, `conclusion`, `outputs`, `stepResults`, `annotations`, `telemetry`, `billingOwnerId`
- **Was**: Body built into a local variable and dropped — never actually POSTed to the server. Also missing `billingOwnerId` and `telemetry`
- **Fix**: Added actual `POST {run-service}/completejob` call using the `SystemVssConnection` endpoint and `AccessToken` from the job message. Added `billingOwnerId` from job message. AzDO path POSTs to FinishJob
- **File**: `crates/aksh-runner/src/worker/job_runner.rs`

### F014 — HIGH: WorkflowStepsUpdate body shape fixed

- **Found**: golden flow 24 body uses proto enums: `status: 6` (completed), `conclusion: 2|3|7` (succeeded/failed/skipped); top-level fields `change_order` (monotonic), `workflow_job_run_backend_id` (=jobId), `workflow_run_backend_id` (=planId)
- **Was**: `StepUpdate` used string fields (`status: "InProgress"`) and lacked `change_order`, `workflow_*_backend_id`
- **Fix**: Rewrote `server_queue.rs` with `WorkflowStepsUpdateBody` struct matching the Twirp proto exactly. `StepUpdate` now uses `u32` status/conclusion enums with constants. `ServerQueue::take_steps_update_body()` builds the full body. Serialization verified in tests
- **File**: `crates/aksh-runner/src/worker/server_queue.rs`

### F015 — HIGH: Cancelled/timed-out steps orphan process trees

- **Found**: official runner kills the process group on cancel/timeout; our `select!` dropped the future without killing the child, leaving processes running
- **Was**: `process::invoke` had no cancel path; `steps_runner` used `select!` that dropped futures (orphaning processes)
- **Fix**: Threaded `cancel_rx` from worker → steps_runner → execute_step → script handler → `process::invoke`. `invoke` now `select!`s on `child.wait()` vs `cancel_rx.changed()` and calls `child.kill()` on the process group before returning. Worker reads cancel messages from stdin concurrently via a spawned task. Removed the outer `select!` in steps_runner that dropped futures
- **File**: `crates/aksh-runner/src/process.rs`, `crates/aksh-runner/src/worker/mod.rs`, `crates/aksh-runner/src/worker/steps_runner.rs`, `crates/aksh-runner/src/worker/handlers/script.rs`

### F016 — HIGH: Broker message body field extraction used wrong case

- **Found**: golden flow 12: `/message` body uses snake_case (`runner_request_id`, `run_service_url`, `billing_owner_id`). Golden flow 13: `/acknowledge` POST body uses camelCase (`runnerRequestId`). Runner extracted camelCase from message body → empty → skipped acknowledge entirely → server re-delivered the same message infinitely.
- **Fix** (runner): Extract snake_case `runner_request_id` from message body with camelCase fallback. Fix `acquire_job_from_ref` to read `run_service_url` / `billing_owner_id` (snake_case). Add in-memory dedup as robustness against re-delivery.
- **Fix** (server): Default long-poll timeout in `next_message_broker_ref_root` changed from 5s to 50s (matching golden behavior).
- **File**: `crates/aksh-runner/src/listener/broker_listener.rs`, `crates/aksh-runner/src/client/broker.rs`
- **Server note**: The primary fix should be server-side: `POST /acknowledge` with `runnerRequestId` body should dequeue the message, and `/message` should long-poll (return empty 200 after ~50s timeout when no work). The server currently removes `session_active_requests` on acknowledge, which is correct behavior, but infinite re-delivery still occurred because the runner never called acknowledge due to the field name mismatch.

### F017 — MEDIUM: Removed non-golden `lastMessageId` query param from broker polls

- **Found**: golden flows 12/27/42/57 show `/message` with only `sessionId`, `status`, `runnerVersion`, `os`, `architecture`, `disableUpdate` — no `lastMessageId`
- **Was**: Runner appended `&lastMessageId=N` on subsequent polls
- **Fix**: Removed `last_message_id` parameter from `BrokerClient::get_message()`. Dedup handled in-memory on the runner side as robustness.
- **File**: `crates/aksh-runner/src/client/broker.rs`

## Issues Found &amp; Fixed — P0 audit pass 2026-07-02 (F018–F025, F027–F028)

Found by auditing every module of `crates/aksh-runner` against the goldens and upstream v2.335.1

semantics. These are fixed in code and verified by targeted unit tests plus a local aksh

simple-echo smoke run. Tier-1 live GitHub validation and MITM flow diffs are still pending.

### F018 — CRITICAL: renewjob never called (job lock never renewed)

- **Found**: official runs a background renew loop (interval = lock duration/2, `JobDispatcher.cs`); `RunServiceClient::renew_job` existed but had zero call sites.
- **Fix**: `worker/job_runner.rs` now creates a `ReportingContext`, starts a renew loop immediately after acquire, calls `RunServiceClient::renew_job`, and stops the loop on completion/cancel.
- **Status**: ✅ Fixed in code; live long-job GitHub validation pending.

### F019 — CRITICAL: WorkflowStepsUpdate never sent during job

- **Found**: `ServerQueue` had the correct Twirp body, but was not instantiated or flushed.
- **Fix**: `job_runner` creates the queue; `steps_runner` queues setup, skipped, in-progress, completed, and complete-job updates; `job_runner` flushes at step boundaries and job end through `ResultsClient::update_workflow_steps`.
- **Status**: ✅ Fixed in code; local smoke attempted the Twirp call against `ResultsServiceUrl`. Local aksh returned 401 for results-service auth, tracked as control-plane/token fidelity rather than a runner URL-shape regression.

### F020 — CRITICAL: step/job logs never uploaded

- **Found**: signed-log URL and blob upload clients had zero call sites.
- **Fix**: step output is buffered per step, masked, uploaded via `GetStepLogsSignedBlobURL` + opaque signed URL `PUT`, then concatenated for final job-log upload through `GetJobLogsSignedBlobURL`.
- **Status**: ✅ Fixed in code; live GitHub log-viewer validation pending.

### F021 — CRITICAL: ACTIONS_* runtime env vars never injected

- **Found**: official injects cache/artifact/results/OIDC env from `SystemVssConnection`; runner only set `GITHUB_*`/`RUNNER_*`.
- **Fix**: `worker/job_extension.rs` injects `ACTIONS_RUNTIME_URL`, `ACTIONS_RUNTIME_TOKEN`, `ACTIONS_RESULTS_URL`, `ACTIONS_CACHE_URL`, `ACTIONS_CACHE_SERVICE_V2`, `ACTIONS_ID_TOKEN_REQUEST_URL`, and `ACTIONS_ID_TOKEN_REQUEST_TOKEN` from endpoint data plus `system.github.*` variables where present.
- **Status**: ✅ Fixed in code; targeted env test passed; live cache/artifact/OIDC scenarios pending.

### F022 — CRITICAL: action resolution endpoint not implemented

- **Found**: golden 10 flow 19 uses the launch service `runnerresolve/actions` batch endpoint before codeload downloads; the runner used an aksh-only stub/fallback path.
- **Fix**: `client/actions_download.rs` implements the official batch resolve call and returns tarball URL/auth/resolved SHA; `worker/actions/manager.rs` uses that data for the `_work/_actions/{owner}/{repo}/{sha}` layout and codeload download, with fallback only for local aksh payloads.
- **Status**: ✅ Fixed in code; checkout live GitHub validation pending.

### F023 — CRITICAL: pre/post step lifecycle missing entirely

- **Found**: no pre/main/post expansion, no pre-if/post-if defaults, and no `STATE_*` env from `GITHUB_STATE`.
- **Fix**: action steps now carry internal pre/main/post entries, post steps are scheduled LIFO with default `always()`, resolved action path/entry overrides are honored, and paired post steps receive `STATE_<name>` env values saved by their main step.
- **Status**: ✅ Fixed in code; targeted lifecycle/state tests passed.

### F024 — CRITICAL: composite outputs not evaluated; nested lifecycle incomplete

- **Found**: composite `outputs.*.value` expressions were ignored and nested step output contexts were unavailable for output evaluation.
- **Fix**: `handlers/composite.rs` evaluates composite outputs after nested steps using the nested `steps` context and enforces the official nesting-depth cap.
- **Status**: ✅ Fixed in code; live composite scenario pending.

### F025 — HIGH: annotations collected but never uploaded

- **Found**: `StepContext` collected annotations, but completejob step results hardcoded `annotations: []` and step updates did not carry annotations.
- **Fix**: `job_runner` and `server_queue` now include collected annotations in per-step result payloads and final `completejob` data.
- **Status**: ✅ Fixed in code; live annotation scenario pending; problem matcher integration remains F032.

### F027 — HIGH: expression engine gaps (bracket access, object filter, hashFiles)

- **Found**: no `[`/`]` access, no `a.*.b` filter, and `hashFiles()` was a stub returning `""`.
- **Fix**: `aksh-gha-expressions` now supports bracket/index access, wildcard collection, and real SHA-256 `hashFiles(...)` relative to the expression context workspace.
- **Status**: ✅ Fixed in code; expression crate tests passed. `format()` escaped braces remain P2.

### F028 — HIGH: no `secrets` expression context; masking literal-only

- **Found**: `${{ secrets.X }}` was unresolvable and masking only replaced literal values.
- **Fix**: `JobContext` builds a `secrets` root from secret variables; masking includes literal, trimmed, base64, base64url, and no-padding base64url variants; log upload uses masked output.
- **Status**: ✅ Fixed in code; runner tests passed.

### F026 — HIGH: container support is dead code

- **Found**: `container_ops.rs` (docker check, network create, start, health, path translation) has zero call sites; `job_runner` never inspects the message container spec; service containers unimplemented; `script.rs` never takes a docker-exec path.
- **File**: `crates/aksh-runner/src/worker/container_ops.rs`, `crates/aksh-runner/src/worker/job_runner.rs`, `crates/aksh-runner/src/worker/handlers/script.rs`
- **Fix**: Full Docker engine lifecycle implemented — TemplateToken decoding, container/service spec parsing, Docker CLI command sequences matching golden traces (create, start, health poll, exec, cleanup), `job.container`/`job.services` runtime contexts populated, synthetic Initialize/Stop containers steps with log upload, service volumes mounted, `format()` `{{`/`}}` brace escaping fixed.
- **Status**: ✅ Fixed (2026-07-04) — E2E validated against live GitHub (scenarios 30-36, run 28706488417) and aksh-server on smolvm.

### F029 — HIGH: step ID / display-name auto-generation missing

- **Found**: official generates `__run`/`__run_2` IDs for id-less steps and display-name fallbacks (action ref, script preview), which appear on the wire in step updates; ours had neither.
- **File**: `crates/aksh-runner/src/worker/job_extension.rs`
- **Status**: ✅ Fixed (2026-07-03) — split wire `id` from `contextName`, generated `__run`/action context names, and verified live runs 28641527947 / 28641641045.

### F030 — HIGH: AzDO compat reporting unwired (`--via azdo`)

- **Found**: `patch_agent_request`, `update_timeline`, `create_log`/`append_log`, `post_console_log`, `finish_job` all have zero call sites; `report_completion()` builds a non-`JobCompletedEvent` shape; `TimelineRecord.order` never populated
- **File**: `crates/aksh-runner/src/client/azdo.rs`, `crates/aksh-runner/src/worker/job_runner.rs`
- **Status**: ❌ Defferred (roadmap P1.3)

### F031 — HIGH: cancellation semantics incomplete; job timeout missing

- **Found**: cancel kills the current step (F015) but remaining steps were not re-evaluated under `cancelled()` semantics (`always()`/post steps didn't run), no grace window before hard kill in `job_dispatcher::kill()`, and job-level `timeout-minutes` (default 360) was not enforced.
- **File**: `crates/aksh-runner/src/worker/steps_runner.rs`, `crates/aksh-runner/src/listener/job_dispatcher.rs`, `crates/aksh-runner/src/worker/job_runner.rs`
- **Status**: ✅ Fixed (2026-07-03) — cancel now unwinds through remaining `always()`/`cancelled()`/post steps with grace-bounded cancellation; review fix DR-002/DR-003 threads cancellation into action/composite processes and implements step timeout through cancellation signalling.

### F032 — HIGH: problem matchers are dead code

- **Found**: `MatcherRegistry` existed but had zero call sites; `::add-matcher::`/`::remove-matcher::` were not wired in `commands.rs`; log lines were never fed through; multi-line `loop:` patterns remain deferred.
- **File**: `crates/aksh-runner/src/worker/matchers.rs`, `crates/aksh-runner/src/worker/commands.rs`
- **Status**: ✅ Fixed (2026-07-03) — matcher registry is job-scoped, add/remove commands are wired, log lines feed matchers, and live run 28655734365 produced GitHub UI annotations.

### F033 — HIGH: no retry/backoff, no 401 session recovery, no ephemeral unregister

- **Found**: no HTTP call site retries transient 5xx (official: ×3 exponential + `ErrorThrottler`); no session re-create on 401/session-gone; `--once` exits without DELETEing the agent registration
- **File**: `crates/aksh-runner/src/client/http.rs`, `crates/aksh-runner/src/listener/broker_listener.rs`
- **Status**: ✅ Fixed (2026-07-03) — 3x retry on 5xx/network errors, session recovery loop, and best-effort unregister implemented.

### F034 — MEDIUM: GITHUB_*/RUNNER_* env set incomplete (28/39)

- **Found**: missing GITHUB_REF_PROTECTED, GITHUB_REPOSITORY_ID, GITHUB_REPOSITORY_OWNER_ID, GITHUB_TRIGGERING_ACTOR, GITHUB_WORKFLOW_REF, GITHUB_WORKFLOW_SHA, GITHUB_RETENTION_DAYS, RUNNER_DEBUG, RUNNER_ENVIRONMENT, RUNNER_PERFLOG, RUNNER_TRACKING_ID
- **File**: `crates/aksh-runner/src/worker/job_extension.rs`, `crates/aksh-runner/src/worker/contexts.rs`
- **Status**: ✅ Fixed (2026-07-03) — missing env keys were added; review fix DR-005 aligned `runner.tool_cache` context with `RUNNER_TOOL_CACHE` derivation.

### F035 — HIGH: step summary never uploaded

- **Found**: GITHUB_STEP_SUMMARY file created and size-capped (1MiB) but never uploaded to the results service
- **File**: `crates/aksh-runner/src/worker/file_commands.rs`, `crates/aksh-runner/src/worker/job_runner.rs`
- **Status**: ✅ Fixed (2026-07-03) — read before file command cleanup, uploaded to results service, and metadata finalized.

### F036 — HIGH: Log upload fails on Azure Blob Storage due to missing `x-ms-blob-type` header

- **Found**: E2E live GitHub run failed to upload step/job logs to production Azure Blob Storage URL (`PUT https://productionresultssa17.blob.core.windows.net/...`). Azure responded with `400 Bad Request` and `MissingRequiredHeader`, specifying that `x-ms-blob-type` is a mandatory header.
- **File**: `crates/aksh-runner/src/client/results.rs`
- **Status**: ✅ Fixed (2026-07-03) — added `x-ms-blob-type: BlockBlob` header to `put_bytes()`. Verified: live GitHub run 28631466481 uploaded logs successfully.

### F037 — HIGH: completejob outputs payload has wrong schema (not wrapped in value object)

- **Found**: E2E live GitHub run for `08-job-outputs-needs` failed `/completejob` with `400 Bad Request` when job outputs were present. Golden captures show outputs must be structured as `{"outputs": { "<name>": { "value": "<val>" } }}` rather than `{"outputs": { "<name>": "<val>" }}`.
- **File**: `crates/aksh-runner/src/worker/job_runner.rs`
- **Status**: ✅ Fixed (2026-07-03) — wrapped each output in `{"value": v}`. Verified: live GitHub run 28631470474 (producer+consumer both succeeded).

### F038 — MEDIUM: completejob fails with connection closed error on annotations

- **Found**: E2E live GitHub run for `14-annotations` failed `/completejob` with a connection closed error (`SendRequest`) when step annotations were reported.
- **File**: `crates/aksh-runner/src/worker/job_runner.rs`
- **Status**: ✅ Fixed (2026-07-03) — annotations now always include `startLine`/`endLine` (defaulting to 1 when no source line present). Verified: live GitHub run 28631483737 completed successfully.

### F039 — HIGH: Action manifest input defaults containing expressions are not evaluated

- **Found**: E2E live GitHub run for `10-uses-checkout` failed because `actions/checkout` requires `INPUT_TOKEN` to contain a valid GitHub token. The runner loaded the default value `"${{ github.token }}"` literally from the action's manifest and set `INPUT_TOKEN="${{ github.token }}"` in the environment instead of evaluating the expression, causing git authentication to fail.
- **File**: `crates/aksh-runner/src/worker/handlers/node.rs`, `crates/aksh-runner/src/worker/handlers/composite.rs`
- **Status**: ✅ Fixed (2026-07-03) — defaults are now evaluated via `evaluate_template()` against the job expression context. Verified: live GitHub run 28631474708 (checkout now fails on Node 26, not on expression evaluation).

### F040 — HIGH: Trailing slash in CacheServerUrl causes CacheService API calls to fail

- **Found**: E2E live GitHub runs for `11-cache-roundtrip` failed because `ACTIONS_CACHE_URL` was set to the raw `CacheServerUrl` from GitHub which contains a trailing slash. When the `@actions/cache` library constructed the request URL, it resulted in a double slash (e.g. `...//_apis/artifactcache/...`), which the API gateway rejected.
- **File**: `crates/aksh-runner/src/worker/job_extension.rs`
- **Status**: ✅ Fixed (2026-07-03) — `CacheServerUrl` is now trimmed via `trim_end_matches('/')`. Verified: live GitHub run 28631476809 no longer has double-slash URL errors.

### F041 — HIGH: Action reference missing @version from job message

- **Found**: GitHub job messages send action references with `name` and `ref` as separate fields in `reference` (e.g. `{"name": "actions/checkout", "ref": "v4"}`). The runner's step parser only read `name`, producing `"actions/checkout"` without `@v4`, which caused `parse_remote_uses` to fail and the action to never be downloaded.
- **File**: `crates/aksh-runner/src/worker/job_extension.rs`
- **Status**: ✅ Fixed (2026-07-03) — step parser now combines `reference.name` and `reference.ref` into `uses@ref`. Verified: live GitHub run 28632740507 downloads and executes `actions/checkout@v4` via codeload.github.com.

---

## New Gaps Found — v2.335.1 upstream source audit 2026-07-04 (F042–F056)

Found by diffing every module of `crates/aksh-runner` against the official `actions/runner` v2.335.1
C# source (cloned at tag `v2.335.1`). These are behavioral differences NOT covered by F001–F041.

### F042 — CRITICAL: Process cancel uses immediate kill — no SIGINT→SIGTERM grace sequence

- **Found**: official sends SIGINT (7.5s timeout), then SIGTERM (2.5s timeout), then hard kill. Our previous implementation called `child.kill().await` immediately.
- **Impact**: Processes could not run cleanup/shutdown handlers, trap handlers, or finally blocks. Affected builds, test suites, anything with graceful shutdown.
- **Upstream**: `Runner.Sdk/ProcessInvoker.cs:32-33` (timeout constants), `Runner.Sdk/ProcessInvoker.cs:443-464` (`CancelAndKillProcessTree` — SIGINT→SIGTERM→kill sequence)
- **File**: `crates/aksh-runner/src/process.rs`
- **Status**: ✅ Fixed (2026-07-04, commit `1ac32cb`) — process cancellation now sends SIGINT, waits 7.5s, sends SIGTERM, waits 2.5s, then hard-kills/reaps the process group. Verified by targeted process cancellation tests and `cargo check -p aksh-runner`.

### F043 — CRITICAL: Docker exec env vars leak secrets in CLI args

- **Found**: official uses `-e KEY` (no value) for docker run/exec so secrets inherit from the process environment without appearing in `docker inspect` or process listings. Our previous implementation always used `-e KEY=VALUE`.
- **Impact**: Secret values were visible in `docker inspect`, process listings, and audit logs. Security issue for container jobs.
- **Upstream**: `Runner.Worker/Container/DockerCommandManager.cs:204-209` (`DockerRun` uses `-e KEY` for all env vars), `Runner.Worker/Container/DockerCommandManager.cs:130-145` (`DockerCreate` checks empty values)
- **File**: `crates/aksh-runner/src/worker/container_ops.rs`, `crates/aksh-runner/src/worker/handlers/container.rs`
- **Status**: ✅ Fixed (2026-07-04, commit `0dcadb0`) — Docker create/exec/run now use `-e KEY` inherited-env form where possible so values are passed through the command environment, not CLI args. Verified by focused docker env secrecy tests and `cargo check -p aksh-runner`.

### F044 — HIGH: `github.action_repository` / `github.action_ref` contexts never set

- **Found**: official calls `SetGitHubContext("action_repository", ...)` and `SetGitHubContext("action_ref", ...)` on every action step execution. Our handlers did not set these.
- **Impact**: `${{ github.action_repository }}` and `${{ github.action_ref }}` returned empty in action steps.
- **Upstream**: `Runner.Worker/ActionRunner.cs:147-153` (sets `action_repository` and `action_ref` from `repoPathReferenceAction.Name`/`.Ref`)
- **File**: `crates/aksh-runner/src/worker/handlers/action.rs`, `crates/aksh-runner/src/worker/contexts.rs`
- **Status**: ✅ Fixed (2026-07-04, commit `a82a1aa`) — action handlers set `github.action_repository`/`github.action_ref` without save/restore (matching official runner behavior). Also set `github.action` to step name. Verified: live GitHub run 28724871604 (`action_repository=actions/checkout`, `action_ref=v4` assertions passed).

### F045 — HIGH: `github.action_status` context not set on composite nested steps

- **Found**: official sets `action_status` before each nested step and updates it on cancel. Our composite handler did not set this.
- **Impact**: `success()`/`failure()` in nested composite steps evaluated against job status, not parent action status.
- **Upstream**: `Runner.Worker/Handlers/CompositeActionHandler.cs:246-248` (set before each step), `Runner.Worker/Handlers/CompositeActionHandler.cs:339-340` (updated on cancel)
- **File**: `crates/aksh-runner/src/worker/handlers/composite.rs`
- **Status**: ✅ Fixed (2026-07-04, commit `81838e4`) — composite handler now updates `github.action_status` before nested steps and preserves success/cancel/failure semantics across composite execution. Verified by focused composite action-status tests and `cargo check -p aksh-runner`.

### F046 — HIGH: Container action `runs.pre-entrypoint`/`runs.post-entrypoint` lifecycle not registered

- **Found**: official registers pre/post lifecycle steps for container actions from `pre-entrypoint`/`post-entrypoint` (LIFO, same as node). Our previous `build_step_list_with_lifecycle` only handled node/composite actions, then the first Docker fix parsed non-official `pre`/`post` keys.
- **Impact**: Container actions with cleanup logic (`runs.post-entrypoint`) never executed post steps.
- **Upstream**: `Runner.Worker/ActionManifestManager.cs:414-425` (Docker metadata keys), `Runner.Worker/ActionRunner.cs` (pre/post registration logic), `Runner.Worker/Handlers/HandlerFactory.cs` (container handler instantiation with pre/post support)
- **File**: `crates/aksh-runner/src/worker/job_extension.rs`, `crates/aksh-runner/src/worker/handlers/factory.rs`
- **Status**: ✅ Fixed (2026-07-04, commits `2b45924` + `dda4616`) — lifecycle expansion now recognizes `runs.using: docker`, parses official `pre-entrypoint`/`post-entrypoint`, registers container pre/main/post entries, and preserves LIFO cleanup ordering. Verified by focused parser/lifecycle tests and `cargo check -p aksh-runner`.

### F047 — HIGH: Container action `runs.entrypoint`/`runs.args`/`runs.env` from manifest not applied

- **Found**: official evaluates `runs.entrypoint`, `runs.args`, and `runs.env` from the action manifest with expression context (`inputs.*`) and injects them into the docker run invocation. Our container handler ignored these manifest fields.
- **Impact**: Container actions with custom entry points, arguments, or environment templating from manifest were silently broken.
- **Upstream**: `Runner.Worker/Handlers/ContainerActionHandler.cs` (evaluates entrypoint/args/env with template context), `Runner.Worker/ActionManifestManager.cs` (manifest evaluation helpers)
- **File**: `crates/aksh-runner/src/worker/handlers/container.rs`, `crates/aksh-runner/src/worker/handlers/factory.rs`, `crates/aksh-runner/src/worker/handlers/composite.rs`
- **Status**: ✅ Fixed (2026-07-04, commit `281d750`) — container actions now evaluate manifest `runs.env`, `runs.entrypoint`, and `runs.args` against inputs/context and apply them to `docker run` while preserving secret-safe inherited env passing. Verified by focused manifest-field/lifecycle regressions and `cargo check -p aksh-runner`.

#### F042–F047 validation notes — 2026-07-04

- **Unit/focused tests**: `process::tests::cancel_` (2 tests), `docker_exec_env_args_do_not_include_secret_values`, `docker_create_env_uses_inherit_form_for_empty_values`, `inherited_env_args_do_not_include_secret_values`, `action_repository_context_*`, `set_github_context_value_updates_context_and_env`, `composite_steps_receive_action_status_context`, `github_status_success_failure_cancelled`, `load_docker_action_manifest`, `lifecycle_registers_docker_action_pre_and_post`, `manifest_env_entrypoint_and_args_evaluate_against_inputs`, and `docker_run_args_apply_entrypoint_args_and_hide_env_values` passed.
- **Build/check**: `cargo fmt --all --check`, `cargo check -p aksh-runner`, and release builds for host macOS arm64 plus smolvm Linux arm64 passed with existing warnings.
- **Live GitHub smolvm smoke**: runner `aksh-smolvm-fidelity-0704` on ARM64 Linux registered against `preloopdev/aksh` and processed GitHub run `28720263632`. Broker/session/acquirejob/renewjob/WorkflowStepsUpdate/log-upload/completejob all succeeded. The job result was failed because existing `fixtures/workflows/dogfood.yml` expanded unset `vars.AKSH_REPO_ROOT` to `cd ""`; this is workflow configuration, not an F042–F047 runner protocol failure.
- **Dedicated live workflow**: `fixtures/fidelity/runner-fidelity-f042-f047.yml` and `fixtures/actions/*` were added in commit `b531312` to validate Docker lifecycle/manifest fields and composite `github.action_status` on a self-hosted runner. GitHub refused branch-only `workflow_dispatch` because the workflow file is not on the default branch yet, so this remains a manual live gate after merge.
- **Local aksh smoke**: `aksh-conformance runner-e2e --workflow crates/aksh-conformance/fixtures/hello-world.yml --record-flows /tmp/smoke-flows.jsonl` returned `{"status":"success","success":true}`; rerun emitted `Address already in use` because a previous failed attempt left port 9191 occupied.
- **Golden replay**: `runner-watch conform --runner v2.335.1 --aksh-url http://127.0.0.1:9090 --skip-cargo-test` failed only known unsupported storage scenarios: `11-cache-roundtrip` (CacheService Twirp 404) and `12-artifact` (ArtifactService Twirp 404).

### F048 — HIGH: Job-level annotations hardcoded to `[]` in completejob

- **Found**: official collects infrastructure failure annotations at the job level in `GlobalContext.JobAnnotations` and passes them to `CompleteJobAsync`. Our implementation hardcoded the top-level `annotations` field to `[]` — F025 only fixed per-step annotations in `stepResults`.
- **Impact**: Infrastructure failures and job-level issues not visible in GitHub UI.
- **Upstream**: `Runner.Worker/JobRunner.cs` (`GlobalContext.JobAnnotations` collection and `CompleteJobAsync` call), `Runner.Worker/ExecutionContext.cs:598-615` (feature-flag gated collection from job-level issues)
- **File**: `crates/aksh-runner/src/worker/job_runner.rs`, `crates/aksh-runner/src/worker/contexts.rs`
- **Fix**: Added `job_annotations: Vec<Annotation>` to `JobContext`. Job-level annotations are now collected on infrastructure failures (job timeout, step execution errors) and included in the completejob body's `annotations` array. Step annotations remain in `stepResults` (F025).
- **Status**: ✅ Fixed (2026-07-05, commit `71ad99a`). Verified: live GitHub run 28725557539 (aksh) and 28725606404 (official v2.335.1) both succeeded with matching annotation behavior.

### F049 — HIGH: Web proxy env not injected into containers

- **Found**: official injects `HTTP_PROXY`/`http_proxy`, `HTTPS_PROXY`/`https_proxy`, `NO_PROXY`/`no_proxy` (both cases) from runner web proxy config into container environment. Our `container_ops.rs` had no proxy handling.
- **Impact**: Container workflows behind corporate proxies fail on Docker image pulls and network access.
- **Upstream**: `Runner.Worker/Container/ContainerInfo.cs:253-271` (`UpdateWebProxyEnv` method — `TryAdd` for each proxy var in both upper/lower case)
- **File**: `crates/aksh-runner/src/worker/container_ops.rs`, `crates/aksh-runner/src/worker/handlers/container.rs`
- **Fix**: Added `inject_proxy_env()` that reads `HTTP_PROXY`/`HTTPS_PROXY`/`NO_PROXY` (both cases) from host environment and injects into container env with `TryAdd` semantics (user-specified env takes precedence). Applied to job containers, service containers, and Docker action containers.
- **Status**: ✅ Fixed (2026-07-05, commit `71ad99a`). Verified: live GitHub runs 28725557542 (container-proxy + service-proxy both succeeded). No proxy configured on test host → no vars injected (correct behavior).

### F050 — MEDIUM: `github.action` context not set from step action name

- **Found**: official calls `SetGitHubContext("action", actionStep.Action.Name)` before each action step. Our steps_runner has no equivalent.
- **Impact**: `${{ github.action }}` returns empty for action steps.
- **Upstream**: `Runner.Worker/StepsRunner.cs:118` (`SetGitHubContext("action", actionStep.Action.Name)`)
- **File**: `crates/aksh-runner/src/worker/steps_runner.rs`
- **Status**: ✅ Fixed (2026-07-04, commit `a82a1aa`) — `github.action` set to step name on each action step in `action.rs`. Matches official `StepsRunner.cs:118`. Verified: live GitHub run 28724871604.

### F051 — MEDIUM: Problem matcher `fromPath` field not supported

- **Found**: official uses `fromPath` as a base directory for resolving relative file paths in matcher output. Our `matchers.rs` did not parse or use this field.
- **Impact**: Relative file paths in annotations resolved incorrectly or dropped.
- **Upstream**: `Runner.Worker/Handlers/OutputManager.cs:283-290` (resolves relative paths against `fromPath`), `Runner.Worker/IssueMatcher.cs:193,210,305-306` (`FromPath` at both pattern and matcher level)
- **File**: `crates/aksh-runner/src/worker/matchers.rs`
- **Fix**: Added `from_path` field to `ProblemMatcher` (matcher-level default) and `MatcherPattern` (capture group index). `match_line()` now resolves relative file paths against `fromPath` directory (pattern capture → matcher default → leave relative). Matches official `OutputManager.cs:283-290` behavior.
- **Status**: ✅ Fixed (2026-07-05, commit `dab6e0c`). Verified: live GitHub run 28725932080 (aksh) and official runner comparison — both produced matching `##[error]`/`##[warning]` annotations from custom problem matcher with `fromPath`.

### F052 — MEDIUM: Missing `.runner` settings fields

- **Found**: official persists `DisableUpdate`, `UseRunnerAdminFlow`, `SkipSessionRecover`, `MonitorSocketAddress` in `.runner`. Our `settings.rs` did not include these fields.
- **Impact**: `SkipSessionRecover` affects session recovery behavior; others are minor runtime flags.
- **Upstream**: `Runner.Common/ConfigurationStore.cs` (`RunnerSettings` class — all four fields with `[DataMember]` attributes)
- **File**: `crates/aksh-runner/src/settings.rs`, `crates/aksh-runner/src/listener/broker_listener.rs`
- **Fix**: Added `disable_update`, `skip_session_recover`, `monitor_socket_address`, and `use_runner_admin_flow` to `RunnerSettings` with `skip_serializing_if` for clean output. Wired `skip_session_recover` into broker listener session recovery logic.
- **Status**: ✅ Fixed (2026-07-05, commit `dab6e0c`). Verified: live GitHub run 28725932090 (runner registered, all 4 steps completed — settings loaded and parsed correctly).

### F053 — MEDIUM: Missing credential data fields for auth migration

- **Found**: official reads `authorizationUrlV2`, `enableAuthMigrationByDefault`, `oauthEndpointUrl` from `.credentials` data block. Our `configure.rs` did not extract these from the agent response and our OAuth exchange ignored them.
- **Impact**: Auth migration to V2 URLs not supported; `oauthEndpointUrl` fallback missing.
- **Upstream**: `Runner.Listener/Configuration/OAuthCredential.cs:28-49` (reads all three fields, selects auth URL), `Runner.Listener/Configuration/ConfigurationManager.cs:410-416` (extracts from agent properties at configure time)
- **File**: `crates/aksh-runner/src/configure.rs`, `crates/aksh-runner/src/listener/oauth.rs`
- **Fix**: At configure time, extract `EnableAuthMigrationByDefault` and `AuthorizationUrlV2` from agent response properties and persist to `.credentials` data. At OAuth time, prefer `authorizationUrlV2` when `enableAuthMigrationByDefault` is set, and use `oauthEndpointUrl` as the token exchange endpoint (falling back to `authorizationUrl`).
- **Status**: ✅ Fixed (2026-07-05, commit `1f08416`). Verified: live GitHub run 28726251502 (aksh registered, acquired job, OAuth token acquired successfully) and official runner comparison (both Succeeded).

### F054 — MEDIUM: Diagnostic log upload missing

- **Found**: official collects runner/worker diagnostic logs from `_diag/`, zips with metadata, and uploads via the results service `CreateResultsDiagnosticLogsAsync`. No equivalent in our codebase.
- **Impact**: No runner diagnostic telemetry collected. Not a workflow blocker but affects debugging.
- **Upstream**: `Runner.Worker/DiagnosticLogManager.cs` (full class — collects `_diag/*.log`, filters by job start time, creates zip with metadata JSON, uploads via results service signed URL)
- **File**: `crates/aksh-runner/src/worker/job_runner.rs`, `crates/aksh-runner/src/client/results.rs`
- **Fix**: Added `upload_diagnostic_logs()` that collects `_diag/*.log` files from the runner root, creates a zip archive with metadata JSON, and uploads via `CreateResultsDiagnosticLogsSignedBlobURL` Twirp endpoint. Called after job log upload, before completion report. Non-fatal if `_diag/` doesn't exist or server doesn't support the endpoint.
- **Status**: ✅ Fixed (2026-07-05, commit `1f08416`). Verified: live GitHub run 28726251502 (aksh) and official runner comparison — both succeeded. Diagnostic upload is best-effort (no `_diag/` logs present in test environment, so upload was skipped gracefully).

### F055 — MEDIUM: `hashFiles()` doesn't support `--follow-symbolic-links` flag

- **Found**: official parses `--follow-symbolic-links` as a flag argument. Our expression engine did not handle this flag, treating it as a glob pattern.
- **Impact**: Symbolic links silently ignored in cache keys when flag is used.
- **Upstream**: `Runner.Worker/Expressions/HashFilesFunction.cs:44-51` (parses `--follow-symbolic-links` from first argument, case-insensitive, passes to `Globber` options; throws on unknown `--` flags)
- **File**: `crates/aksh-gha-expressions/src/lib.rs`
- **Fix**: Added flag parsing in `hash_files()` — first argument starting with `--` is checked for `--follow-symbolic-links` (case-insensitive). Unknown `--` flags are silently skipped. When set, `follow_symlinks` mode follows symlinks during file enumeration.
- **Status**: ✅ Fixed (2026-07-05, commit `6a7a97b`). Verified: live GitHub run 28726677018 (aksh) — both `hashFiles('hashtest/*.txt')` and `hashFiles('--follow-symbolic-links', 'hashtest/*.txt')` produced valid matching hashes. Official runner comparison also succeeded.

### F056 — LOW: `requireFipsCryptography` hardcoded to `"True"`

- **Found**: official reads `properties.RequireFipsCryptography` from agent response. Our `configure.rs` hardcoded `"True"`.
- **Impact**: Minor; always enables FIPS regardless of server preference.
- **Upstream**: `Runner.Listener/Configuration/ConfigurationManager.cs` (reads `RequireFipsCryptography` from agent creation response `properties` block)
- **File**: `crates/aksh-runner/src/configure.rs`
- **Fix**: Read `RequireFipsCryptography` from agent response `properties` at configure time, falling back to `"True"` if not present (preserving prior behavior).
- **Status**: ✅ Fixed (2026-07-05, commit `6a7a97b`). Verified: live GitHub run 28726677018 — runner registered and acquired job successfully with FIPS settings read from agent response.

---

## Live E2E bugs found 2026-07-04 (not in upstream audit)

Found by running aksh-runner against real GitHub via `preloopdev/aksh-conformance-sample`.

### Step env expressions not evaluated

- **Found**: Step-level `env:` values containing `${{ }}` expressions were inserted raw without template evaluation. `${{ matrix['os'] }}` passed through as literal text.
- **Upstream**: `Runner.Worker/StepsRunner.cs:122-128` (`EvaluateStepEnvironment` evaluates step env through template evaluator)
- **Fix**: Added `evaluate_template()` call for step env values in `steps_runner.rs:243`.
- **Status**: ✅ Fixed (2026-07-04, commit `a82a1aa`). Verified: live GitHub run 28725011207.

### Expression parser: no member access after function calls

- **Found**: `fromJSON('[...]').*.name` failed with "unexpected token Dot" — the parser didn't support `.`/`[`/`.*` chaining after function call results.
- **Upstream**: Official expression parser (DTExpressions2) uses operator-precedence parsing where `Dereference` (`.`) and `Wildcard` (`*`) are operators that naturally chain after `EndParameters`.
- **Fix**: Added `MemberAccess` expr variant, `parse_member_suffix()`, and `Context::resolve_value()` in `aksh-gha-expressions`.
- **Status**: ✅ Fixed (2026-07-04, commit `a82a1aa`). Verified: live GitHub run 28725011207 (bracket, wildcard, contains, nested, format all passed).

### Subdirectory action paths dropped from step reference

- **Found**: `uses: owner/repo/subdir@ref` — GitHub sends `reference.path` separately from `reference.name`. Our step parser ignored `reference.path`, so subdirectory actions resolved to the repo root. Manifest not found → lifecycle steps not created.
- **Upstream**: Official runner resolves `reference.path` as the action subdirectory within the downloaded repository.
- **Fix**: Step parser now reads `reference.path` and appends it to construct the full `uses` string in `job_extension.rs`.
- **Status**: ✅ Fixed (2026-07-04, commit `db0325d`). Verified: live GitHub run (F046 Docker lifecycle with subdirectory action).

### vars context not decoded from typed-dict format

- **Found**: `${{ vars.AKSH_REPO_ROOT }}` resolved to empty. GitHub sends `contextData.vars` in Azure DevOps typed-dictionary format (`{type: 4, map: [{Key: ..., Value: ...}]}`). Our code only decoded the flat JSON format.
- **Fix**: Added typed-dict decoding for `vars` context in `worker/contexts.rs` and inserted `vars` into `contextData` in `job_builder.rs`.
- **Status**: ✅ Fixed (2026-07-04, commit `307afe2` + `d986f77`). Verified: dogfood green run.

---

## Disparities found in real-world benchmarks (2026-07-05)

Tested serde, axum, bat against aksh-runner-server with both aksh-runner and official C# runner v2.335.1.
See `docs/runner/15-real-world-benchmarks.md` for full results.

### `defaults.run.working-directory` not implemented
- **Severity**: LOW — parser feature gap, not runner
- **Impact**: Workflows must use `cd` in each step instead of `defaults.run.working-directory`
- **Status**: ✅ Fixed (2026-07-05). Implemented in `aksh-gha-parser` (`Job.defaults` → `StepPlan.working_directory`) and wired through `steps_runner.rs` script execution. Relative paths resolved against job workspace.

### `env.PATH` + `GITHUB_PATH` interaction
- **Severity**: MEDIUM — PATH construction didn't match official runner
- **Impact**: aksh-runner was prepending `GITHUB_PATH` entries to the **system** PATH, ignoring workflow-level `env.PATH`. Official runner uses the step/job environment PATH (which includes `env.PATH`) as the base for prepending. Workflows setting `env.PATH` and then using `GITHUB_PATH` would get the wrong PATH order.
- **Status**: ✅ Fixed (2026-07-05). `build_env()` in `execution_context.rs` now uses the already-built step env PATH as the base for `GITHUB_PATH` prepending, matching the official runner's `AddPrependPathToEnvironment()` at `Handler.cs:205-233`. Test added.

### Broker long-poll 50s idle timeout on `--once`
- **Severity**: LOW — performance issue, not behavioral
- **Impact**: After `--once` job completes, runner waited up to 50s for broker poll to expire before exiting.
- **Status**: ✅ Fixed (2026-07-05). Broker loop now polls every 200ms while a job is active, exiting within 200ms of completion. Changed in `broker_listener.rs` — short-circuit `select!` branch races sleep against long-poll when busy.

### Server `runs-on` label matching not implemented
- **Severity**: HIGH — blocks multi-runner deployments
- **Impact**: Server dispatched jobs from a FIFO queue with no label filtering. Any runner got any job regardless of `runs-on` labels, breaking multi-runner setups (e.g. Linux + macOS runners).
- **Status**: ✅ Fixed (2026-07-05). Server now matches job `runs-on` labels against runner registration labels (case-insensitive). GitHub-hosted aliases (`ubuntu-latest` → `self-hosted`/`linux`, `macos-*` → `macos`, `windows-*` → `windows`) are supported. 6 unit tests added. Jobs skip non-matching runners in the queue (FIFO with filtering, not strict FIFO).

---

## Remaining Known Gaps (deferred)


| Gap                 | Description                                               | Milestone |
| ------------------- | --------------------------------------------------------- | --------- |
| Websocket live logs | HTTP buffered upload only; logs appear at step completion | Post-M12  |
| Self-update         | Intentionally no-op'd (logged, not crashed)               | Never     |
| Service install     | macOS launchd / Linux systemd                             | Post-M12  |
| Windows             | cmd/powershell handlers, path semantics                   | Post-M12  |
| DAP debugger        | Step-level debugging                                      | Post-M12  |
| Job hooks           | `ACTIONS_RUNNER_HOOK_JOB_STARTED` etc.                    | Post-M12  |
| Background steps    | Coordinator for parallel composite steps                  | Post-M12  |

---

## Upstream fixture corpus

`fixtures/upstream-workflows/` contains 74 files from
[ChristopherHX/runner.server](https://github.com/ChristopherHX/runner.server) —
curated to runner-relevant fixtures only (Windows, control-plane cache/artifact/OIDC,
and redundant fixtures were removed).

### Runner-relevant (test now)

- **Env propagation** — `stepenv.yml`, `localenv.yml`, `globalenv.yml`, `multiline_env.yml`
- **Matrix edge cases** — `matrixtest.yml`, `case-insensitive-keys-matrix/`, `case_insensitive_needs/`, `matrix-partial-test/`, `matrix-eq-test/`, `matrix-selector/`
- **Status/control flow** — `job-continue-on-error.yml`, `continue-on-error-bug-3.6.0-4-test.yml`, `skippedjob.yml`, `skipped.yml`
- **Expression edge cases** — `db-disposed-issue/` (6 files: recursive needs, expressions in env names, advanced status functions), `issue70/` (4 variants), `testhashfiles.yml`
- **Parser robustness** — `verify-yaml-anchors.yml` (YAML anchors), `workflowerrors/` (16 malformed YAMLs)
- **Context dump** — `dumpcontexts.yml` (full context regression)
- **Containers** — `linux-container-i386/` (i386 arch), `linux-container-problem-matcher-test1/` (matcher in container)
- **Inputs** — `workflow_dispatch/` (boolean, choice, defaults)

### Future reference (reusable workflows — not yet implemented)

- `called.yml`, `called_template_runs_on.yml`, `called_with_required_secret.yml`
- `inherit_secrets/`, `inherit_vars/`, `reusablesCaseInsensitive/`, `reusablesConsistentWorkflowName/`
- `node16_complex_reusable_workflows/`, `workflow_ref_and_job_workflow_ref/`
- `test_template_runs_on.yml` (4 variants), `test_with_required_secret.yml`

### Planned usage

1. **Parser fuzz/regression** (immediate): feed every `.yml` through `aksh-gha-parser` and assert no panics. `workflowerrors/` is the primary fuzz corpus.
2. **Runner conformance** (next): run env, matrix, status, and expression fixtures against `aksh-runner-server` + `aksh-runner`. Matrix case-insensitivity and `needs` case-insensitivity are high-priority.
3. **Reusable workflow reference** (future): `called.yml` and inheritance fixtures provide the test corpus for `workflow_call` support.
