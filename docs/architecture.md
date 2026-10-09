# Architecture

preloop is split by protocol responsibility:

- `preloop-gha-protocol` owns versioned wire/domain types. Anything sent to a
runner or emitted to an agent passes through this crate. Includes AzDO wire
DTOs, `SecretString`, NDJSON events, and session crypto.
- `preloop-gha-parser` owns workflow YAML normalization, trigger matching, job graph
construction, matrix expansion, and expression evaluation.
- `preloop-gha-expressions` owns expression parsing and evaluation (the core
`${{ }}` engine).
- `preloop-runner-server` owns HTTP routes, queueing, cancellation, reruns, and
runner sessions. Exposes two protocol surfaces:
  - `_apis/...` — the AzDO protocol the official runner speaks (source of truth)
  - `/api/v1/...` — native REST + NDJSON for agents and tools (read projection)
- `preloop-cli` is the shipping CLI (submit, status, webhooks, debug);
`preloop-runner-client` is the lighter submission/inspection client.
- `preloop-cache` and `preloop-artifacts` own file-backed protocol storage.
- `preloop-dap` owns the Debug Adapter Protocol bridge (DAP server, session
state machine, debugger attachment for paused jobs).
- `preloop-conformance` owns comparisons against the pinned
`ChristopherHX/runner.server` reference.
- `preloop-runner` is the Rust reimplementation of the GitHub Actions runner
(Listener + Worker). Single binary with `configure`/`run`/`worker` subcommands;
the listener spawns a worker child process per job via stdin NDJSON IPC.
- `preloop-vm` defines the `VmProvider` trait (crates/preloop-vm/src/lib.rs:349)
abstracting runner-host substrates — SmolVM (libkrun, the default) and AgentENV
(Firecracker).
- `preloop-orchestrator` owns job scheduling and VM lifecycle orchestration on
top of `preloop-vm`.
- `preloop-observability` owns metrics, logs, traces, and status snapshots.
- `preloop-socket-activation` owns systemd-style socket activation support.
- `runner-watch` tracks upstream `actions/runner` releases, diffs source, emits
TOML specs, and replays golden wire captures for protocol conformance.

## Pluggable backends

preloop is execution-agnostic. The only thing that differs between runner hosts
is how a runner instance is created and destroyed. This is modeled as the
`preloop_vm::VmProvider` trait (crates/preloop-vm/src/lib.rs:349); the
`ControlBackend` trait (control/backend.rs) covers durable state. (The `RunnerProvider`/`AuthProvider`
design in fidelity-gap §4 is aspirational.)

- `**ControlBackend**` — durable control-plane state behind one trait: SQLite
  (default) or Postgres (shared nodes). See [State Model](#state-model).
- `**AuthProvider**` — loopback-trust (local) or OAuth plus bearer tokens
  (system token or minted job JWTs).
- `**RunnerProvider**` — creates/destroys runners (process, container, libkrun,
cloud VM, k8s pod, bare BYO). Optional — preloop works with external runners.

See [fidelity-gap.md §4](fidelity-gap.md) for the full design.

## State Model

The control database is the source of truth. Every durable control mutation is
one typed command on the `ControlBackend` trait
(`preloop-runner-server/src/control/backend.rs`): a handler parses the request
and maps the domain result onto the wire, and never holds a transaction, a SQL
string, or a mutable record. A backend runs each command as **one short
transaction of targeted statements** — conditional `UPDATE`s (zero rows means
someone else won) and `FOR UPDATE SKIP LOCKED` queues — instead of loading a
working set and writing it back, so there is no process-local control state to
diverge between nodes. Scheduling decisions that need Rust evaluation live in
`control/logic.rs` and both backends call them, so they answer identically.

- `preloop-runner-server/src/control/lite/` — the SQLite backend (default).
  One writer connection (`BEGIN IMMEDIATE`, WAL) serializes writers, so the row
  locks Postgres needs are unnecessary; reads run on a pool of `query_only`
  connections.
- `preloop-runner-server/src/control/pg/` — the Postgres backend. Each node
  opens its own pools (`PRELOOP_PG_WRITERS` / `PRELOOP_PG_READERS`), claims
  leases with `FOR UPDATE SKIP LOCKED`, and uses `LISTEN`/`NOTIFY` so a job
  submitted through one node wakes runners polling another. Several engine
  nodes may share one database.

Backends are selected by `--store` / `PRELOOP_STORE_URL` (`sqlite://<path>`, a
bare path, or `postgres://…`), defaulting to SQLite at `<state_dir>/preloop.db`.
The control schema is greenfield **v1**: created on first use and stamped in
`schema_meta`, with no migrations — a `control` schema at another version is
refused.
The in-memory state that remains (`state.rs::InnerState`) is node-local — live
logs and log feeds, cache/artifact reservations, runner liveness, and debug
sessions — and is never the authority for runs, jobs, runners, or sessions.

Cache and artifact payloads stay in file-backed stores under `.preloop/`, and
their reservation bookkeeping is node-local, so a multi-node deployment must
route a job's cache/artifact requests to the node that reserved them (or run
one node per job).

Known gaps and their tradeoffs are tracked in the repository issue tracker.

## Transactional event flow

Every state transition writes its versioned event to `outbox_events` in the
same database transaction as the run/job mutation. The durable `check-runs`
consumer holds a persisted `consumer_offsets` lease, reads from its bookmark
(`(txid,event_id)` on Postgres and `event_id` on SQLite), and atomically
projects the newest desired state for each `(run_id,job_id)` into
`check_run_updates`. Older versions are ignored.

```text
run/job transaction -> outbox_events
                           |
                 check-runs projector
                           v
              check_run_updates (coalesced)
                           |
                  one leased sender
                           v
                    GitHub Checks API
```

The sender is the only check-run HTTP writer. It leases due rows, renders
the existing check name/summary/details/output at send time, chunks
annotations at GitHub's 50-item limit, and owns token caching, circuit
breaking, rate-limit handling, retries and exponential backoff. A webhook
therefore commits durable state and returns without waiting for GitHub.
Postgres `NOTIFY preloop_events`, the local dirty signal, and a short polling
fallback wake the projector. Outbox pruning is bounded by age only when no
durable consumer exists; otherwise rows at or beyond the slowest consumer
bookmark are retained.

## Secrets

Secrets use `SecretString` in `preloop-gha-protocol`. It redacts `Debug`,
`Display`, and serialized output. Code that needs the raw payload must call
`expose()` explicitly at a protocol boundary.

## Compatibility Position

As of 2026-06-26, preloop is a proven working control plane for the official `actions/runner`.
The runner completes the full lifecycle: configure → session → message → execute → complete.

Implemented and verified with the real `Runner.Listener` v2.336.0:

- Full AzDO lifecycle routes (connectionData, AgentPools, Agent, AgentSession, Message,
AgentRequest, Timeline, Logfiles, FinishJob, ActionDownloadInfo)
- GitHub-compatible registration (`/api/v3/actions/runner-registration` with `RemoteAuth`)
- GHES org-prefix routing (`/:org/_apis/...` for all lifecycle endpoints)
- AES session key exchange with RSA-OAEP wrapping of the runner's registered public key (SHA-1 default, SHA-256 in FIPS mode)
- Encrypted `TaskAgentMessage` delivery with message ack
- Full `AgentJobRequestMessage` with plan, requestId, system context, steps
- `needs` DAG scheduling with dependency-gated dispatch and outputs propagation
- Trigger matching (branches/tags/paths/types/schedule/workflow_dispatch)
- Matrix expansion with IndexMap order preservation and GitHub name format
- Expression evaluation wired into job builder
- `fail-fast` / `max-parallel` matrix strategy support
- Cache/artifact v1 + v2 (Twirp + protobuf) served from file-backed `preloop-cache`/`preloop-artifacts` stores

Known limitations:

- Fuzz targets for the YAML/expression parsers remain to be added

## Module Map (post-Plans 012–017)

### `preloop-gha-protocol/src/`


| Module       | Owns                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `azdo/`      | Runner wire DTOs: `lifecycle` (ConnectionData, TaskAgent, sessions), `messages` (TaskAgentMessage, message_type), `job` (AgentJobRequestMessage, TaskStep + custom codec), `variables` (VariableValue, MaskHint), `timeline` (TimelineRecord, TaskResult, Issue), `resources` (ServiceEndpoint, EndpointAuthorization), `context_data` (PipelineContextData + custom codec), `completion` (JobCompletedEvent, TaskLog) |
| `crypto.rs`  | RSA/AES session crypto, JWT signing, key import/export                                                                                                                                                                                                                                                                                                                                                                 |
| `masking.rs` | Secret masking (longest-first, DAP-keyword exclusion)                                                                                                                                                                                                                                                                                                                                                                  |
| `lib.rs`     | Shared protocol types: RunId, JobId, ExecutionStatus, OutputMap, NDJSON events, LiveLogFeedLinesWrapper                                                                                                                                                                                                                                                                                                                |


### `preloop-gha-parser/src/`


| Module           | Owns                                                                                                         |
| ---------------- | ------------------------------------------------------------------------------------------------------------ |
| `models.rs`      | Workflow, Job, Step, Trigger, Concurrency, Matrix, Strategy, ActionMetadata — all type defs with serde attrs |
| `trigger.rs`     | Trigger filter matching, glob matching                                                                       |
| `yaml.rs`        | `parse_workflow`, `parse_action_metadata`, YAML key normalization                                            |
| `expand.rs`      | `expand_jobs`, matrix expansion, reusable workflow inlining, input coercion                                  |
| `eval.rs`        | Expression context builder, `resolve_string`                                                                 |
| `dag.rs`         | Workflow dependency graph validation                                                                         |
| `job_builder.rs` | Build `AgentJobRequestMessage` from parsed workflow data                                                     |


### `preloop-gha-expressions/src/`


| Module           | Owns                                                                 |
| ---------------- | -------------------------------------------------------------------- |
| `context.rs`     | Hierarchical expression context                                      |
| `conditions.rs`  | `effective_condition`, `contains_status_check_function`, `is_truthy` |
| `ast.rs`         | `Expr`, `BinaryOp`                                                   |
| `evaluator.rs`   | Expression evaluation, function dispatch, `hashFiles`                |
| `lexer.rs`       | Token definitions, lexer                                             |
| `expr_parser.rs` | Recursive-descent expression parser                                  |


### `preloop-runner-server/src/`


| Module                  | Owns                                                                    |
| ----------------------- | ----------------------------------------------------------------------- |
| `routes.rs`             | All axum route definitions and middleware wiring                        |
| `auth.rs`               | Bearer token extraction and auth middleware                             |
| `state.rs`              | `AppState`, `SharedState`, `InnerState`, OIDC/HMAC key loading, runtime tokens |
| `models.rs`             | `QueuedJob`, `WebhookDeliveryRecord` and wire-facing run/job models       |
| `runs.rs`               | `/api/v1/runs` handlers: submit, get, cancel, rerun, events             |
| `github.rs`             | GitHub webhook receiver, durable queue worker, check runs               |
| `rerequest.rs`         | `check_run`/`check_suite` rerequest webhook handling: in-place run reruns |
| `github_breaker.rs`     | Circuit breaker + rate-limit classification for GitHub calls           |
| `webhook_watchdog.rs`   | Delivery-history poll, phantom-ack join, redelivery                    |
| `webhook_health.rs`     | Periodic App subscription / delivery-URL drift checks                  |
| `webhook_status.rs`     | Live repair-layer status behind one lock                               |
| `webhook_api.rs`        | `/api/v1/webhooks` listing, replay, health                             |
| `control/`              | Database-authoritative control plane: the `ControlBackend` trait (`backend.rs`), backend-neutral domain types (`types.rs`), shared decision functions (`logic.rs`), wake-ups (`wake.rs`), transaction timings (`txn_stats.rs`), and the SQLite (`lite/`) and Postgres (`pg/`) implementations |
| `runtime_scheduling.rs` | Pure dependency/need and runner-matching helpers reused by `control/logic.rs` |
| `broker.rs`             | Broker protocol: session, message, acquire/renew/complete job           |
| `distributed_task.rs`   | AzDO `/_apis/distributedtask/` handlers                                 |
| `oidc.rs`               | OIDC token minting, JWKS, discovery, certificate management             |
| `concurrency.rs`        | Concurrency group evaluation and queue management                       |
| `scheduler.rs`          | Cron/schedule-based workflow triggering                                 |
| `errors.rs`             | `ApiError` type and error conversions                                   |
| `bootstrap.rs`          | Server startup, TLS, background reaper, status telemetry                |

### `preloop-runner/src/worker/`


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


