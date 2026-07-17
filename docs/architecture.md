# Architecture

aksh is split by protocol responsibility:

- `aksh-gha-protocol` owns versioned wire/domain types. Anything sent to a
runner or emitted to an agent passes through this crate. Includes AzDO wire
DTOs, `SecretString`, NDJSON events, and session crypto.
- `aksh-gha-parser` owns workflow YAML normalization, trigger matching, job graph
construction, matrix expansion, and expression evaluation.
- `aksh-gha-expressions` owns expression parsing and evaluation (the core
`${{ }}` engine).
- `aksh-server` owns HTTP routes, queueing, cancellation, reruns, and
runner sessions. Exposes two protocol surfaces:
  - `_apis/...` — the AzDO protocol the official runner speaks (source of truth)
  - `/api/v1/...` — native REST + NDJSON for agents and tools (read projection)
- `aksh-runner-client` is the local submission/inspection CLI.
- `aksh-cache` and `aksh-artifacts` own file-backed protocol storage.
- `aksh-conformance` owns comparisons against the pinned
`ChristopherHX/runner.server` reference.
- `aksh-runner` is the Rust reimplementation of the GitHub Actions runner
(Listener + Worker). Single binary with `configure`/`run`/`worker` subcommands;
the listener spawns a worker child process per job via stdin NDJSON IPC.
See `docs/runner/00-architecture.md` for the full module map.

## Pluggable backends

aksh is execution-agnostic. The only thing that differs between runner hosts
is how a runner instance is created and destroyed. This is modeled as the
`RunnerProvider` trait in the orchestrator layer:

- `**RunStore**` — in-memory (local) or `sqlx` (server).
- `**AuthProvider**` — loopback-trust (local) or OAuth + mTLS (server).
- `**RunnerProvider**` — creates/destroys runners (process, container, libkrun,
cloud VM, k8s pod, bare BYO). Optional — aksh works with external runners.

See [fidelity-gap.md §4](fidelity-gap.md) for the full design.

## State Model

The default server uses an in-memory run queue and file-backed cache/artifact
stores under `.aksh/`. This keeps the local feedback loop fast and makes the
initial protocol behavior easy to inspect. Durable run state should be added
behind an explicit repository trait before adopting `sqlx` or another database
layer.

## Secrets

Secrets use `SecretString` in `aksh-gha-protocol`. It redacts `Debug`,
`Display`, and serialized output. Code that needs the raw payload must call
`expose()` explicitly at a protocol boundary.

## Compatibility Position

As of 2026-06-26, aksh is a proven working control plane for the official `actions/runner`.
The runner completes the full lifecycle: configure → session → message → execute → complete.

Implemented and verified with the real `Runner.Listener` v2.322.0:

- Full AzDO lifecycle routes (connectionData, AgentPools, Agent, AgentSession, Message,
AgentRequest, Timeline, Logfiles, FinishJob, ActionDownloadInfo)
- GitHub-compatible registration (`/api/v3/actions/runner-registration` with `RemoteAuth`)
- GHES org-prefix routing (`/:org/_apis/...` for all lifecycle endpoints)
- AES session key exchange (unencrypted mode — RSA wrapping planned)
- Encrypted `TaskAgentMessage` delivery with message ack
- Full `AgentJobRequestMessage` with plan, requestId, system context, steps
- `needs` DAG scheduling with dependency-gated dispatch and outputs propagation
- Trigger matching (branches/tags/paths/types/schedule/workflow_dispatch)
- Matrix expansion with IndexMap order preservation and GitHub name format
- Expression evaluation wired into job builder
- `fail-fast` / `max-parallel` matrix strategy support

Known limitations:

- Worker reports job as "Failed" (timeline/log endpoint fidelity gap)
- Session AES key sent unencrypted (RSA-OAEP wrapping of runner's public key TODO)
- Cache/artifact endpoints are in-memory stubs; v2 blob protocols not implemented
- Conformance harness needs golden tests, fuzz targets, wire capture/replay
- Expression engine lacks bracket access, object-filter, format escaping

## Module Map (post-Plans 012–017)

### `aksh-gha-protocol/src/`


| Module       | Owns                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `azdo/`      | Runner wire DTOs: `lifecycle` (ConnectionData, TaskAgent, sessions), `messages` (TaskAgentMessage, message_type), `job` (AgentJobRequestMessage, TaskStep + custom codec), `variables` (VariableValue, MaskHint), `timeline` (TimelineRecord, TaskResult, Issue), `resources` (ServiceEndpoint, EndpointAuthorization), `context_data` (PipelineContextData + custom codec), `completion` (JobCompletedEvent, TaskLog) |
| `crypto.rs`  | RSA/AES session crypto, JWT signing, key import/export                                                                                                                                                                                                                                                                                                                                                                 |
| `masking.rs` | Secret masking (longest-first, DAP-keyword exclusion)                                                                                                                                                                                                                                                                                                                                                                  |
| `lib.rs`     | Shared protocol types: RunId, JobId, ExecutionStatus, OutputMap, NDJSON events, LiveLogFeedLinesWrapper                                                                                                                                                                                                                                                                                                                |


### `aksh-gha-parser/src/`


| Module           | Owns                                                                                                         |
| ---------------- | ------------------------------------------------------------------------------------------------------------ |
| `models.rs`      | Workflow, Job, Step, Trigger, Concurrency, Matrix, Strategy, ActionMetadata — all type defs with serde attrs |
| `trigger.rs`     | Trigger filter matching, glob matching                                                                       |
| `yaml.rs`        | `parse_workflow`, `parse_action_metadata`, YAML key normalization                                            |
| `expand.rs`      | `expand_jobs`, matrix expansion, reusable workflow inlining, input coercion                                  |
| `eval.rs`        | Expression context builder, `resolve_string`                                                                 |
| `dag.rs`         | Workflow dependency graph validation                                                                         |
| `job_builder.rs` | Build `AgentJobRequestMessage` from parsed workflow data                                                     |


### `aksh-gha-expressions/src/`


| Module           | Owns                                                                 |
| ---------------- | -------------------------------------------------------------------- |
| `context.rs`     | Hierarchical expression context                                      |
| `conditions.rs`  | `effective_condition`, `contains_status_check_function`, `is_truthy` |
| `ast.rs`         | `Expr`, `BinaryOp`                                                   |
| `evaluator.rs`   | Expression evaluation, function dispatch, `hashFiles`                |
| `lexer.rs`       | Token definitions, lexer                                             |
| `expr_parser.rs` | Recursive-descent expression parser                                  |


### `aksh-runner-server/src/`


| Module                | Owns                                                                    |
| --------------------- | ----------------------------------------------------------------------- |
| `routes.rs`           | All axum route definitions and middleware wiring                        |
| `auth.rs`             | Bearer token extraction and auth middleware                             |
| `state.rs`            | `AppState`, `SharedState`, OIDC/HMAC key loading, runtime token minting |
| `models.rs`           | `InnerState`, `QueuedJob`, run/job state                                |
| `runs.rs`             | `/api/v1/runs` handlers: submit, get, cancel, rerun, events             |
| `broker.rs`           | Broker protocol: session, message, acquire/renew/complete job           |
| `distributed_task.rs` | AzDO `/_apis/distributedtask/` handlers                                 |
| `oidc.rs`             | OIDC token minting, JWKS, discovery, certificate management             |
| `concurrency.rs`      | Concurrency group evaluation and queue management                       |
| `scheduler.rs`        | Cron/schedule-based workflow triggering                                 |
| `errors.rs`           | `ApiError` type and error conversions                                   |
| `bootstrap.rs`        | Server startup, TLS, GitHub App registration                            |


### `aksh-runner/src/worker/`


| Module                  | Owns                                                                    |
| ----------------------- | ----------------------------------------------------------------------- |
| `job_runner.rs`         | Job orchestration: `run_job`, renew loop                                |
| `reporting.rs`          | Step/log/diagnostic upload to server                                    |
| `completion.rs`         | `report_completion`, completejob payload building                       |
| `action_preparation.rs` | Remote action download/resolution                                       |
| `helpers.rs`            | Shared utilities (timestamps, endpoint extraction)                      |
| `steps_runner.rs`       | Sequential step execution, condition evaluation, container init         |
| `job_extension.rs`      | Workspace setup, GITHUB_* env injection, step ordering                  |
| `contexts.rs`           | `JobContext` — all sub-contexts and accumulated state                   |
| `execution_context.rs`  | `StepContext` — per-step env, logging, annotations                      |
| `execution_types.rs`    | `Annotation`, `AnnotationLevel` (shared DTOs)                           |
| `server_queue.rs`       | Step update queue for server reporting                                  |
| `handlers/`             | Action handlers: `node.rs`, `composite.rs`, `container.rs`, `script.rs` |


