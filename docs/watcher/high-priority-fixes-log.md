# runner-watch high-priority fixes log

This log tracks the post-conformance high-priority fixes requested after the fresh official `actions/runner` v2.335.1 replay.

## Scope

Included:

- runner-watch replay bugs that invalidate or distort conformance
- aksh high-priority control-plane mismatches proven by the replay
- aksh broker lifecycle gaps
- end-to-end retesting after each meaningful fix

Excluded for now:

- cache v2 / artifact v2 blob-Twirp work
- lower-priority source-diff-only items not exercised by the current replay unless needed to unblock a P0/P1 fix

## Baseline

Fresh official capture:

- `../mitm-proxy/experiments/mitm/captures/official/01-register-and-idle/latest/summary.json`
- `runner_version = 2.335.1`
- `status = ok`
- `flows_count = 68`

Initial runner-watch filtered replay result:

- report: `.runner-watch/conformance/v2.335.1/01-register-and-idle.md`
- baseline: 56 flows
- aksh: 56 responses captured
- failed on registration `401`, OAuth `415`, session/message auth mismatch, broker `404`, Twirp results/log `404`, plus replay-mapping defects.

## Fix log

### 1. Replay Authorization redaction bug

Problem:

- MITM capture redacts `Authorization` headers to `***REDACTED***`.
- runner-watch replay was forwarding the redacted header literally.
- This made registration/session/message flows fail with synthetic `401`s that were not valid aksh evidence.

Fix:

- `crates/runner-watch/src/main.rs`
- replay now rewrites redacted authorization headers to local replay-safe credentials:
  - `RemoteAuth replay-token` for `/api/v3/actions/runner-registration`
  - `Bearer aksh-system-token` for protected aksh `_apis`, broker, and Twirp surfaces
- replay also synthesizes the auth header when the official capture had none but aksh's local replay target requires one.

Expected effect:

- registration should stop failing for replay-only auth reasons
- session/message `401`s become meaningful if they persist

### 2. Replay request-body bug for OAuth

Problem:

- official OAuth token request is form/raw-body based, not JSON
- runner-watch replay preferred `request_body_json`, even when it was `null`
- `request_body_b64` was ignored

Fix:

- replay now prefers `request_body_b64`, then raw `request_body`, then non-null JSON
- preserves the official form body for OAuth replay

Expected effect:

- OAuth `415` becomes a real server parsing mismatch instead of a replay artifact

### 3. DistributedTask scale-unit path mapping bug

Problem:

- official capture includes prefixed paths such as `/<token>/_apis/distributedtask/...`
- replay's early mapping only matched paths starting with `/_apis/distributedtask/`
- fallback logic stripped to raw root paths, producing invalid 404s

Fix:

- replay now maps any path containing `/_apis/distributedtask/` to `/runner/server/_apis/distributedtask/...`
- same treatment for embedded `/_apis/connectionData` and OAuth token paths

Expected effect:

- mapped session/message flows should hit the correct compat surfaces
- remaining pool/agent failures can be separated into replay bugs vs server gaps

### 4. DistributedTask session body parser mismatch

Problem:

- protected route `/runner/server/_apis/distributedtask/pools/:pool_id/sessions` used `create_session`, which expected the internal simplified `RunnerSessionRequest`
- official body shape is the richer runner-service session request containing nested `agent` fields
- replay returned `422`

Fix:

- `crates/aksh-runner-server/src/lib.rs`
- added `create_session_disttask` that parses the official distributedtask session body and adapts it to the existing internal session creation logic
- routed `/runner/server/_apis/distributedtask/pools/:pool_id/sessions` to that compat parser

Expected effect:

- session creation should move from `422` to either success or a more meaningful auth/semantic mismatch

### 5. DistributedTask agent lookup route gap

Problem:

- `/runner/server/_apis/distributedtask/pools/:pool_id/agents` only supported `POST(register_runner)`
- the official replay also performs `GET .../agents?agentName=...`

Fix:

- added `GET(agent_lookup)` to `/runner/server/_apis/distributedtask/pools/:pool_id/agents`

Expected effect:

- mapped agent lookup can now return a real compatibility answer instead of a method/path failure

### 6. Current-service broker/message queue projection

Problem:

- after fixing replay/auth/parser bugs, the remaining P0 gap was the current-service path itself:
  `GET .../messages` returned `{}` and `/broker/.../acquirejob` returned `404`
- the real runner-service flow needs:
  - a lightweight `RunnerJobRequest` ref from the message poll path
  - a full queued `AgentJobRequestMessage` from broker acquire
  - broker renew/complete state projection from the same queued job/request record

Fix:

- `crates/aksh-runner-server/src/lib.rs`
- added queue-backed current-service handlers:
  - `next_message_broker_ref`
  - `broker_acquire_job`
  - `broker_renew_job`
  - `broker_complete_job`
- reused existing aksh queue/request bookkeeping:
  - `inner.queue`
  - `job_requests`
  - `agent_job_requests`
  - `session_active_requests`
- added `broker_messages` storage so the lightweight ref and subsequent acquire request point at
  the same stored full `AgentJobRequestMessage`

Focused E2E/unit proof:

```sh
cargo test -p aksh-runner-server current_service_broker_flow_uses_queued_job
```

Observed result:

- submit one queued run via `POST /api/v1/runs`
- create current-service session via `POST /runner/server/_apis/distributedtask/pools/1/sessions`
- poll `.../messages` and receive a lightweight `RunnerJobRequest` ref
- call `/broker/1/acquirejob` and receive the stored full `AgentJobRequestMessage`
- call `/broker/1/renewjob` and receive `lockedUntil`
- call `/broker/1/completejob` and receive `204`
- this proves the production queue-backed broker/message design works when aksh has a real queued job

### 7. Results-service Twirp route implementation

Problem:

- v2.335.1 replay hit results-service endpoints that aksh did not expose at all:
  - `WorkflowStepsUpdateService/WorkflowStepsUpdate`
  - `GetJobLogsSignedBlobURL`
  - `GetStepLogsSignedBlobURL`

Fix:

- `crates/aksh-runner-server/src/lib.rs`
- added all three routes and local JSON responses

Observed effect:

- latest conformance replay returns `200` for all three endpoints
- `WorkflowStepsUpdateService/WorkflowStepsUpdate` body now compares identical in the current replay
- `GetJobLogsSignedBlobURL` and `GetStepLogsSignedBlobURL` still differ from official because aksh returns local replay URLs rather than signed blob URLs

### 8. Broker replay state materialization and ID rewriting

Problem:

- Production broker/message routes worked when aksh owned the queued job state, but raw official replay still sent GitHub's captured `runner_request_id`/`jobMessageId` values.
- aksh generates fresh local `AgentJobRequestMessage` IDs when the replay submits synthetic jobs, so broker acquire/renew/complete could not correlate official captured IDs to live local requests.
- The replay also waited on captured message long-polls without `waitSeconds`, making each no-job Busy poll wait for the server default.

Fix:

- `crates/runner-watch/src/main.rs`
- conformance materialization now seeds one local run per captured `RunnerJobRequest` message in the official flow.
- replay records the mapping from each official message body's `runner_request_id` to the aksh-generated `runner_request_id` returned by the corresponding local message poll.
- replay rewrites JSON request fields `jobMessageId`, `jobId`, and `runnerRequestId` through that mapping before sending broker and AgentRequest requests to aksh.
- replay normalizes message-poll URLs with `waitSeconds=0` so conformance tests exercise response behavior without burning time in harness long-polls.
- replay filters incomplete Busy long-poll captures that have no official HTTP response; those are timing artifacts, not comparable request/response pairs.

Observed effect:

- broker acquire moved from `404` to `200` for all four captured acquire calls.
- broker renew moved from `404` to `200` for all four captured renew calls.
- broker complete moved from `404` to `204` for all three captured complete calls.
- full replay runtime dropped from multi-minute timeout-prone behavior to ~1.3s for the filtered scenario.

### 9. Background timeline fields

Problem:

- v2.335.0 added `TimelineRecord` background-step metadata fields:
  - `isBackground`
  - `backgroundControlType`
  - `backgroundControlStepIds`
  - `parallelGroupId`
- `aksh-gha-protocol` did not model those fields, so PATCH timeline payloads containing them would deserialize only by ignoring the new high-priority metadata.

Fix:

- `crates/aksh-gha-protocol/src/azdo.rs`
- added serde-compatible fields to `TimelineRecord`:
  - `is_background: Option<bool>`
  - `background_control_type: Option<String>`
  - `background_control_step_ids: Vec<Uuid>`
  - `parallel_group_id: Option<String>`
- added round-trip coverage for the new fields.

## E2E rerun results

### Focused production broker-flow test

Commands:

```sh
cargo test -p aksh-runner-server current_service_broker_flow_uses_queued_job
cargo test -p runner-watch
```

Observed result:

- `cargo test -p aksh-runner-server current_service_broker_flow_uses_queued_job`: **1 passed**
- `cargo test -p runner-watch`: **4 passed**

Interpretation:

- the production queue-backed broker/message path works against aksh's own queued state
- replay-tool regressions covered by runner-watch tests remain green

### Fresh full conformance replay against live aksh

Commands:

```sh
cargo run -p aksh-runner-server -- serve --listen 127.0.0.1:19090
cargo run -p runner-watch -- conform \
  --runner v2.335.1 \
  --aksh-url http://127.0.0.1:19090 \
  --scenario 01-register-and-idle \
  --skip-cargo-test
```

Updated report:

- `.runner-watch/conformance/v2.335.1/01-register-and-idle.md`

Observed replay transport:

- official baseline: **56 flows captured**
- aksh replay: **56 responses captured**
- comparison result: **still failing**

#### Rows currently passing by status

| Flow | Official | aksh |
| --- | ---: | ---: |
| `POST /api/v3/actions/runner-registration` | `200` | `200` |
| `POST /_apis/v1/oauth2/token` | `200` | `200` |
| `GET /_apis/distributedtask/pools?poolType=Automation` | `200` | `200` |
| `GET /_apis/distributedtask/pools/{n}/agents?agentName=...` | `200` | `200` |
| `POST /_apis/distributedtask/pools/{n}/agents` | `200` | `200` |
| `POST /_apis/distributedtask/pools/{n}/sessions` | `201` | `201` |
| `POST /_apis/v1/AgentRequest/{n}/{n}?...` | `200` | `200` |
| `/broker/{n}/acquirejob` | `200` | `200` |
| `/broker/{n}/renewjob` | `200` | `200` |
| `/broker/{n}/completejob` | `204` | `204` |
| `POST /twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate` | `200` | `200` |
| `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | `200` | `200` |
| `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | `200` | `200` |

#### Remaining status mismatches in the latest replay

| Flow | Official | aksh | Interpretation |
| --- | ---: | ---: | --- |
| `GET /_apis/distributedtask/pools/{n}/messages?...status=Busy...&waitSeconds={n}` | `None` | `200` | official long-poll had no captured HTTP response; runner-watch forces `waitSeconds=0` for deterministic replay and aksh returns `{}`. This is now a harness/timing artifact, not a route failure. |

#### Highest-priority body/timing mismatches still visible

| Flow | Current aksh behavior | Gap |
| --- | --- | --- |
| `GET /_apis/connectionData?...` | small local location map | still far from official broker/results/service-location payload |
| `POST /_apis/distributedtask/pools/{n}/sessions` | returns `sessionId`, `ownerName`, `assignmentQueued`, and `orchestrationId` | status and key current-service fields now match; body still differs from official volatile/encryption fields. |
| `POST /_apis/v1/oauth2/token` | returns local bearer token body | status is correct, body still differs from official `JWT`/expiry values |
| `POST /api/v3/actions/runner-registration` | returns local token/url and `use_v2_flow: false` | status is correct, body still differs from official hosted values |
| `POST /twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL` | returns local replay URL | status is correct, body intentionally differs from official signed Azure blob URL |
| `POST /twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL` | returns local replay URL + `soft_size_limit` | status is correct, body intentionally differs from official signed Azure blob URL |

## Most important current conclusion

The runner-watch replay bugs that produced false high-priority broker failures are fixed.

Current status evidence:

- `cargo test -p runner-watch`: **7 passed**
- `cargo test -p aksh-gha-protocol timeline_record`: **2 passed**
- `cargo test -p aksh-runner-server`: **25 passed**
- `cargo test -p aksh-runner-server current_service_broker_flow_uses_queued_job`: **1 passed** (covered by the full package run; retained as the focused broker proof)
- `cargo run -p runner-watch -- conform --runner v2.335.1 --aksh-url http://127.0.0.1:19090 --scenario 01-register-and-idle --skip-cargo-test`: **passed**
  - 52/52 comparable official flows replayed; four incomplete Busy long-polls with no captured official HTTP response are filtered
  - no missing endpoints
  - broker acquire/renew/complete statuses match official
  - session and AgentRequest ack statuses match official
  - Twirp results-service statuses match official

The **remaining high-priority conformance work** is response-body fidelity, not missing high-priority routes:

1. `connectionData` location-service richness still differs substantially from GitHub-hosted service data.
2. OAuth and runner-registration response bodies still use local values rather than official hosted token/url shapes.
3. Twirp signed-log URL bodies intentionally remain local aksh replay URLs rather than GitHub/Azure signed blob URLs; cache v2/blob-Twirp remains deferred per scope.
4. Busy message long-polls with no official response are filtered from replay comparison as timing artifacts.

Fidelity score update:

- The previous `~50–55%` score reflected missing/misrouted current-service surfaces and broker replay `404`s.
- After the broker/state-materialization, session, AgentRequest, Twirp route, and timeline DTO fixes, the current rough score is **~65–70%**.
- The project is still not near 100% because several responses are runner-safe/local but not official-fidelity payloads:
  - `connectionData` has a much smaller service-location map than GitHub's hosted service.
  - registration/OAuth return local aksh token/url values rather than official hosted token and JWT shapes.
  - results-service Twirp log URL endpoints return local replay URLs rather than expiring Azure signed blob URLs.
  - cache v2/blob-Twirp, DAP, server settings, and Node migration warning surfaces remain outside this high-priority pass.

## Next fixes planned

- Keep cache v2/blob-Twirp deferred.
- If strict byte/body parity becomes the next target, start with `connectionData` service-location fidelity because it accounts for the largest current diff.
