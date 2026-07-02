# aksh-runner — Runner Fidelity Gap Log

Tracks every deviation found between `aksh-runner` (Rust) and the official
`actions/runner` v2.335.1, discovered via E2E validation against the golden
MITM captures at `.runner-watch/golden/v2.335.1/`.

**Source of truth**: the official runner and GitHub's real service — NOT the aksh control plane.

---

## Issues Found & Fixed (F001–F017 — all ✅ Fixed except F008 ⚠️ Partial)

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

### F008 — HIGH: Broker URL not derived from connectionData (partial fix)
- **Found**: golden flow 11 hits `broker.actions.githubusercontent.com/session` (separate host from the service URL)
- **Was**: Code used same base URL for broker endpoints
- **Partial fix**: BrokerClient accepts a separate broker URL; falls back to `config.settings.server_url` for local aksh. For live GitHub, the broker URL should be derived from connectionData service definitions' location mappings — not yet implemented
- **Impact**: Works against aksh (same host); needs connectionData parsing for real GitHub
- **File**: `crates/aksh-runner/src/listener/broker_listener.rs`
- **Status**: ⚠️ Pending — connectionData parsing still not implemented (see roadmap P1.1)

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

## Pending Issues — full-code audit 2026-07-02 (not yet fixed)

Found by auditing every module of `crates/aksh-runner` against the goldens and upstream v2.335.1
semantics. Priorities and fix guidance in `docs/runner/roadmap.md` (§1–§3).

### F018 — CRITICAL: renewjob never called (job lock never renewed)
- **Found**: official runs a background renew loop (interval = lock duration/2, `JobDispatcher.cs`); `RunServiceClient::renew_job` exists but has zero call sites; lock duration never parsed from acquire
- **Impact**: any job outliving the initial lease is reassigned/failed by GitHub
- **File**: `crates/aksh-runner/src/client/run_service.rs`, `crates/aksh-runner/src/worker/job_runner.rs`
- **Status**: ❌ Pending (roadmap P0 §1.1)

### F019 — CRITICAL: WorkflowStepsUpdate never sent during job
- **Found**: `ServerQueue` (correct Twirp body per F014) is never instantiated; `steps_runner` never queues; no initial step list, no InProgress/Completed transitions (golden 06 flow 24 shows official cadence)
- **Impact**: GitHub UI shows no live step progress; step timeline derived only from completejob
- **File**: `crates/aksh-runner/src/worker/server_queue.rs`, `crates/aksh-runner/src/worker/steps_runner.rs`
- **Status**: ❌ Pending (roadmap P0 §1.2)

### F020 — CRITICAL: step/job logs never uploaded
- **Found**: `get_step_logs_signed_url`/`get_job_logs_signed_url`/`upload_log_blob` (paths correct per F012) have zero call sites
- **Impact**: no logs in GitHub's log viewer at all
- **File**: `crates/aksh-runner/src/client/results.rs`, `crates/aksh-runner/src/worker/job_runner.rs`
- **Status**: ❌ Pending (roadmap P0 §1.3)

### F021 — CRITICAL: ACTIONS_* runtime env vars never injected
- **Found**: official injects ACTIONS_RUNTIME_URL/_TOKEN, ACTIONS_RESULTS_URL, ACTIONS_CACHE_URL, ACTIONS_CACHE_SERVICE_V2, ACTIONS_ID_TOKEN_REQUEST_URL/_TOKEN from job-message variables/endpoints (`JobExtension.cs`); `inject_github_env()` sets only GITHUB_*/RUNNER_* (only occurrences of ACTIONS_* in the crate are test fixtures)
- **Impact**: actions/cache, upload/download-artifact, OIDC all fail on live GitHub (goldens 11/12/15)
- **File**: `crates/aksh-runner/src/worker/job_extension.rs`
- **Status**: ❌ Pending (roadmap P0 §1.4)

### F022 — CRITICAL: action resolution endpoint not implemented
- **Found**: golden 10 flow 19 shows batch `POST …/runnerresolve/actions` on launch.actions.githubusercontent.com → auth tokens, tarball URLs, resolved SHAs; then codeload.github.com download (flow 20). Our stub targets aksh's `_apis/v1/actiondownloadinfo` and is never invoked; downloads use api.github.com tarball with unresolved refs
- **File**: `crates/aksh-runner/src/client/actions_download.rs`, `crates/aksh-runner/src/worker/actions/manager.rs`
- **Status**: ❌ Pending (roadmap P0 §1.5)

### F023 — CRITICAL: pre/post step lifecycle missing entirely
- **Found**: no action discovery phase at job start, no pre list, no LIFO post list, no pre-if/post-if evaluation, no `state` context into post steps; `build_step_list()` builds main steps only
- **Impact**: checkout post cleanup and cache post-save never run; scenario 11 unpassable even with F021
- **File**: `crates/aksh-runner/src/worker/job_extension.rs`
- **Status**: ❌ Pending (roadmap P0 §1.6)

### F024 — CRITICAL: composite outputs not evaluated; nested pre/post not hoisted
- **Found**: `outputs.*.value` expressions never read/evaluated after nested steps; nested `uses:` pre/post not hoisted to job level; no nesting-depth cap (official: 10)
- **File**: `crates/aksh-runner/src/worker/handlers/composite.rs`
- **Status**: ❌ Pending (roadmap P0 §1.7)

### F025 — HIGH: annotations collected but never uploaded
- **Found**: `StepContext` collects annotations correctly, but completejob step results hardcode `annotations: []` and `StepUpdate` has no annotations field (golden 14 shows official shape)
- **File**: `crates/aksh-runner/src/worker/job_runner.rs`, `crates/aksh-runner/src/worker/server_queue.rs`
- **Status**: ❌ Pending (roadmap P0 §1.8)

### F026 — HIGH: container support is dead code
- **Found**: `container_ops.rs` (docker check, network create, start, health, path translation) has zero call sites; `job_runner` never inspects the message container spec; service containers unimplemented; `script.rs` never takes a docker-exec path
- **File**: `crates/aksh-runner/src/worker/container_ops.rs`, `crates/aksh-runner/src/worker/job_runner.rs`, `crates/aksh-runner/src/worker/handlers/script.rs`
- **Status**: ❌ Pending (roadmap P1.2)

### F027 — HIGH: expression engine gaps (bracket access, object filter, hashFiles)
- **Found**: no `[`/`]` tokens (so `a['b']`/`a[0]` fail to parse), no `a.*.b` filter, `hashFiles()` is a stub returning `""` (breaks cache keys); `format()` lacks `{{`/`}}` escaping
- **File**: `crates/aksh-gha-expressions/src/lib.rs`
- **Status**: ❌ Pending (roadmap P0 §1.9)

### F028 — HIGH: no `secrets` expression context; masking literal-only
- **Found**: `${{ secrets.X }}` unresolvable (no secrets root built from isSecret variables); masker replaces literal values only — no trimmed/URL-encoded/base64 variants; masking not applied at the (currently missing) log-upload boundary
- **File**: `crates/aksh-runner/src/worker/contexts.rs`, `crates/aksh-runner/src/worker/execution_context.rs`
- **Status**: ❌ Pending (roadmap P0 §1.10)

### F029 — HIGH: step ID / display-name auto-generation missing
- **Found**: official generates `__run`/`__run_2` IDs for id-less steps and display-name fallbacks (action ref, script preview), which appear on the wire in step updates; ours has neither
- **File**: `crates/aksh-runner/src/worker/job_extension.rs`
- **Status**: ❌ Pending (roadmap P1.11)

### F030 — HIGH: AzDO compat reporting unwired (`--via azdo`)
- **Found**: `patch_agent_request`, `update_timeline`, `create_log`/`append_log`, `post_console_log`, `finish_job` all have zero call sites; `report_completion()` builds a non-`JobCompletedEvent` shape; `TimelineRecord.order` never populated
- **File**: `crates/aksh-runner/src/client/azdo.rs`, `crates/aksh-runner/src/worker/job_runner.rs`
- **Status**: ❌ Pending (roadmap P1.3)

### F031 — HIGH: cancellation semantics incomplete; job timeout missing
- **Found**: cancel kills the current step (F015) but remaining steps are not re-evaluated under `cancelled()` semantics (`always()`/post steps don't run), no grace window before hard kill in `job_dispatcher::kill()`, job-level `timeout-minutes` (default 360) never enforced
- **File**: `crates/aksh-runner/src/worker/steps_runner.rs`, `crates/aksh-runner/src/listener/job_dispatcher.rs`
- **Status**: ❌ Pending (roadmap P1.4/P1.5)

### F032 — HIGH: problem matchers are dead code
- **Found**: `MatcherRegistry` exists but has zero call sites; `::add-matcher::`/`::remove-matcher::` not wired in `commands.rs`; log lines never fed through; multi-line `loop:` patterns unimplemented
- **File**: `crates/aksh-runner/src/worker/matchers.rs`, `crates/aksh-runner/src/worker/commands.rs`
- **Status**: ❌ Pending (roadmap P1.6)

### F033 — HIGH: no retry/backoff, no 401 session recovery, no ephemeral unregister
- **Found**: no HTTP call site retries transient 5xx (official: ×3 exponential + `ErrorThrottler`); no session re-create on 401/session-gone; `--once` exits without DELETEing the agent registration
- **File**: `crates/aksh-runner/src/client/http.rs`, `crates/aksh-runner/src/listener/broker_listener.rs`
- **Status**: ❌ Pending (roadmap P1.7/P1.8)

### F034 — MEDIUM: GITHUB_*/RUNNER_* env set incomplete (28/39)
- **Found**: missing GITHUB_REF_PROTECTED, GITHUB_REPOSITORY_ID, GITHUB_REPOSITORY_OWNER_ID, GITHUB_TRIGGERING_ACTOR, GITHUB_WORKFLOW_REF, GITHUB_WORKFLOW_SHA, GITHUB_RETENTION_DAYS, RUNNER_DEBUG, RUNNER_ENVIRONMENT, RUNNER_PERFLOG, RUNNER_TRACKING_ID
- **File**: `crates/aksh-runner/src/worker/job_extension.rs`
- **Status**: ❌ Pending (roadmap P1.9)

### F035 — HIGH: step summary never uploaded
- **Found**: GITHUB_STEP_SUMMARY file created and size-capped (1MiB) but never uploaded to the results service
- **File**: `crates/aksh-runner/src/worker/file_commands.rs`, `crates/aksh-runner/src/worker/job_runner.rs`
- **Status**: ❌ Pending (roadmap P1.10)

---

## Remaining Known Gaps (deferred)

| Gap | Description | Milestone |
|-----|-------------|-----------|
| Websocket live logs | HTTP buffered upload only; logs appear at step completion | Post-M12 |
| Self-update | Intentionally no-op'd (logged, not crashed) | Never |
| Service install | macOS launchd / Linux systemd | Post-M12 |
| Windows | cmd/powershell handlers, path semantics | Post-M12 |
| DAP debugger | Step-level debugging | Post-M12 |
| Job hooks | `ACTIONS_RUNNER_HOOK_JOB_STARTED` etc. | Post-M12 |
| Background steps | Coordinator for parallel composite steps | Post-M12 |
