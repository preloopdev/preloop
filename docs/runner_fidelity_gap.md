# aksh-runner — Runner Fidelity Gap Log

Tracks every deviation found between `aksh-runner` (Rust) and the official
`actions/runner` v2.335.1, discovered via E2E validation against the golden
MITM captures at `.runner-watch/golden/v2.335.1/`.

**Source of truth**: the official runner and GitHub's real service — NOT the aksh control plane.

---

## Issues Found & Fixed

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
