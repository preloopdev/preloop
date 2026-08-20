# Plan 002: Make Preloop observable without making a backend mandatory

> **Executor instructions**: Implement this plan as the ordered PR-sized steps below. Run every
> verification command and confirm the expected result before moving to the next step. Preserve the
> official runner wire exactly. If a STOP condition occurs, stop and report it instead of improvising.
>
> **Drift check (run first)**:
>
> ```sh
> git diff --stat 673bdfa0..HEAD -- \
>   Cargo.toml Cargo.lock \
>   crates/preloop-observability crates/preloop-cli crates/preloop-runner-server \
>   crates/preloop-orchestrator crates/preloop-vm crates/preloop-runner \
>   docs contrib/openobserve scripts rules justfile versions.toml CHANGELOG.md
> ```
>
> If any in-scope file changed since this plan was written, compare the current-state excerpts and
> named symbols below with live code. A semantic mismatch is a STOP condition.

## Status

- **Priority**: P1
- **Effort**: L, split into seven independently reviewable changes
- **Risk**: MED; the HTTP and runner lifecycle paths are protocol-critical, while telemetry must be fail-open
- **Depends on**: none
- **Category**: direction, architecture, operations, security, DX
- **Planned at**: commit `84d92cfd`, 2026-08-17
- **Revised at**: commit `673bdfa0`, 2026-08-20. Every current-state excerpt, file:line anchor, and
  external version claim below was re-verified against live code at that commit. The revision also
  closed six coverage gaps the first draft missed: bounded-buffer/limit drops, the scheduled-workflow
  subsystem, concurrency-group queueing, GitHub rate-limit budget, persistent-storage growth, and a
  general background-task heartbeat registry (the first draft named only two loops out of fifteen).

## Executive decision

Build observability in three layers, in this order:

1. **Zero-dependency operator diagnostics**: truthful liveness/readiness, an authenticated aggregate
   status endpoint, `preloop status --json`, structured stderr/journald logs, and authenticated
   Prometheus text at `/metrics`, including host-observed resource use for Preloop-owned microVMs.
   These work with no sidecar and answer “why is this job not moving?” even when no telemetry backend
   exists.
2. **Vendor-neutral telemetry**: OpenTelemetry metrics, logs, and short-lived traces, exported through
   bounded OTLP/HTTP batches only when standard `OTEL_*` configuration is present. Export failure must
   never reject, delay, cancel, or change a workflow.
3. **OpenObserve as an optional reference backend**: a pinned, private single-node deployment plus
   importable dashboards and alerts. Preloop must neither embed OpenObserve nor require it. Any OTLP
   backend remains interchangeable.

OpenObserve is a good fit for the optional third layer: its single-node mode is one binary/container,
uses SQLite plus local disk by default, accepts logs/metrics/traces over OTLP, and includes dashboards
and standard alerts. It is not a sound architectural dependency and its HA mode is not minimal. Keep
the product boundary at OTLP and Prometheus.

The highest-priority deliverable is not a dashboard. It is `preloop status`: backend telemetry is
least useful during exporter, network, credential, or storage failures, which are exactly when an
operator needs a direct answer.

## Why this matters

Today Preloop can accept work while the pool repeatedly fails to provision, can leave an operator
unable to distinguish “no compatible runner” from “runner has stopped polling,” and can report a
healthy process while critical background behavior is degraded. The only built-in aggregate view is
a recent-runs table. That makes incident diagnosis depend on manually correlating unstructured logs,
HTTP endpoints, server state, and VM state.

This plan makes every important control-plane question answerable:

- Is the process alive and is its critical event loop making progress?
- How many jobs are ready, dependency-blocked, concurrency-blocked, expanding, or unclaimable?
- Is compatible capacity absent, preparing, provisioning, idle, busy, paused, or stale?
- Are runners polling and renewing leases?
- Are VM CPU, host memory, throttling, sparse-disk allocation, or runtime health limiting capacity?
- Are durable-state writes and GitHub check updates succeeding?
- Is telemetry itself exporting, dropping, or failing?
- Which run/job/session/machine was involved, without putting identifiers into metric labels?

## Current state

### Logging is local, duplicated, and not export-ready

`crates/preloop-cli/src/main.rs:743-745` initializes a plain formatting subscriber:

```rust
tracing_subscriber::fmt()
    .with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    )
    .init();
```

The standalone server (`crates/preloop-runner-server/src/main.rs:78-80`, which uses
`EnvFilter::from_default_env()` and therefore has no `info` fallback) and the Rust runner
(`crates/preloop-runner/src/main.rs:17-21`) each initialize their own subscriber. Three binaries,
three slightly different filter defaults, no JSON selection, no OTLP pipeline, no metrics provider,
no exporter-health state, and no coordinated flush.

`tracing-subscriber` is already a workspace dependency at `0.3` with features
`["env-filter", "json"]`, so `PRELOOP_LOG_FORMAT=json` needs a layer switch, not a new dependency.

**Security gate**: current INFO/WARN events are unsafe to export unchanged:

- `crates/preloop-runner-server/src/artifact_twirp.rs:94` —
  `info!(token, name = request.name, "artifact v2 create")` logs an artifact upload capability token.
- `crates/preloop-runner-server/src/results_twirp.rs:713` —
  `info!(token, "cache v2 create entry")` logs a cache upload capability token.
- `crates/preloop-runner-server/src/blob_store.rs:63, 78, 95, 113, 121, 127, 131` — seven
  `warn!`/`info!` sites carry `kind, token` blob capability tokens.
- `crates/preloop-runner-server/src/distributed_task.rs:327` —
  `info!(?body, "agent_request_patch received")` logs the complete runner PATCH JSON body.
- `crates/preloop-runner-server/src/recording.rs:1-90` deliberately records every header and both
  bodies for conformance capture, including authorization material.

Do not enable log export until the first four are removed or reduced to safe fields. Flow recording
must remain an explicit local conformance facility, stored mode 0600, and must never pass through the
normal logging/OTLP pipeline.

The step-1 audit must be a scan, not a fixed list: the four sites above are the ones that exist at
`673bdfa0`, and the churn between `84d92cfd` and `673bdfa0` moved three of them. Re-run the scan
rather than trusting these anchors:

```sh
grep -rnE '(info|warn|error)!\(' crates/preloop-runner-server/src crates/preloop-orchestrator/src \
  | grep -E '\b(token|authorization|cookie|headers|body|payload|signed_url|secret|password)\b'
```

### Health and status do not diagnose the control plane

`crates/preloop-runner-server/src/runs.rs:4-9` always returns `ok: true`:

```rust
pub(crate) async fn healthz(State(shared): State<Arc<SharedState>>) -> Json<serde_json::Value> {
    Json(json!({
        "ok": true,
        "protocol_version": PROTOCOL_VERSION,
        "shutdown_requested": shared.shutdown.is_cancelled(),
    }))
}
```

The router exposes only `GET /healthz` for health and installs
`TraceLayer::new_for_http()` at `crates/preloop-runner-server/src/routes.rs:805`. The default trace
span records the request URI; exporting it would leak query strings and create unbounded URI values.
Use Axum's matched route template, never raw URI/path-and-query, in telemetry. There is no
`/readyz`, no `/metrics`, and no Prometheus dependency anywhere in the workspace today; `/healthz`
at `routes.rs:256` is the only health surface.

`preloop status` at `crates/preloop-cli/src/main.rs:2831` calls only
`GET /api/v1/runs?limit=20` and renders recent runs. The native runner API can already report queued
versus claimable work for a run (`runner_lifecycle.rs:145-164`), but there is no one-call operational
snapshot. `wait_for_engine_socket` (`crates/preloop-cli/src/main.rs:1458-1475`) probes
`http://localhost/healthz` with a 500 ms per-attempt timeout over a 30 s window, so CLI startup
currently treats "process accepts connections" as "server is usable".

`Command::Status` is a unit variant. The repository already has a `--json` convention to copy:
`PlanArgs { #[arg(long)] json: bool }` at `crates/preloop-cli/src/main.rs:706-707`.

### The state needed for useful diagnostics already exists

`AppState` (`crates/preloop-runner-server/src/state.rs:358-503`) and `InnerState`
(`state.rs:1067-1153`) already contain:

- ready, dependency-held, concurrency-blocked, and expansion queues;
  `pending_jobs`, `pending_expansions`, `expanding`, `queued_at`;
- registered runners, sessions, last-poll timestamps, claims, active requests, and leases;
  `session_last_seen`, `runner_liveness_timeout`, `inflight_messages`, `broker_messages`,
  `claimed_jobs`, `cancellation_queue`;
- pool assignment/provision reservations and the `queue_depth` / `pool_preparing` atomics;
  `job_assignments`, `pool_pending`, `pool_proven_runners`, `pool_assignments_enabled`,
  `require_job_assignments`;
- debug sessions and scheduler state, plus the GitHub App surface added since the first draft:
  `github_app`, `github_apps`, `dispatch_token_cache`, `dispatch_actor_cache`, `github_pat`,
  `github_urls`, `pr_config`, `action_sha_cache`, `pending_registrations`, `secrets`.

`AppState::emit` (`state.rs:819`, lock released at `state.rs:827-832`) deliberately releases the state lock before persistence,
logs store failure, and broadcasts regardless. In-memory state is authoritative; the database is a
restart source. `docs/architecture.md:39-56` explicitly says two servers sharing SQLite or Postgres
still diverge. Every signal must include `service.instance.id`; this plan does not imply Preloop HA.

Core lifecycle boundaries are centralized enough to instrument without scattering counters:

- session create/delete: `broker.rs:368-410`;
- broker polling, claim, acquire, renew, and complete: `broker.rs:637-1202`;
- restart orphan reconciliation: `broker.rs:959-975`;
- no-matching-runner, job timeout, expired lease, and deaf-runner reaping: the reaper loop at
  `bootstrap.rs:396-410`, which ticks on a 10-second `tokio::time::interval`;
- store abstraction: the private `Store` trait at `store.rs:33-55`, declared `#[async_trait]` and
  `pub(crate) trait Store: Send + Sync` with exactly seven methods — `load_into`, `store_inner`,
  `store_meta_only`, `store_run_event`, `store_workflow_run_counter`, `store_log_chunk`,
  `append_event`.

Instrument `Store` once with a decorator in `store.rs`; do not duplicate measurement in SQLite and
Postgres implementations. The decorator is cheap because `#[async_trait]` already erases the futures
and the trait is consumed as `Arc<dyn Store>` (`state.rs:360`), constructed in one factory at
`store.rs:261-267`. Wrap the value that factory returns; no call site changes.

`store_pg.rs:95-103` spawns a detached `tokio-postgres` connection task that logs one ERROR and exits
if the connection dies. Every subsequent store call then fails. Instrument the connection task's
liveness directly (`preloop.store.connection.up`), not only the per-operation failures it causes.

### Embedded pool state is not visible to the server

`RunnerPool::run` in `crates/preloop-orchestrator/src/lib.rs:1880-2100` owns its idle, building,
provisioning, golden-registry, and slot state.

`RunnerPoolConfig` does not merely expose "selected shared atomics" — it already carries **four
independent ad-hoc shared handles** wired one at a time as needs arose:

| Field | Type | Purpose |
|---|---|---|
| `pending_jobs` | `Option<Arc<AtomicUsize>>` | queue depth pushed into the pool |
| `preparing_signal` | `Option<Arc<AtomicBool>>` | golden/image warm state pushed out |
| `next_job_runs_on` | `Option<Arc<RwLock<Vec<String>>>>` | label hint pushed into the pool |
| `pending_registrations` | `Option<Arc<RwLock<BTreeMap<String, SystemTime>>>>` | in-flight registrations |

Therefore the directive is **consolidation, not addition**: introduce one neutral `PoolStatus` handle
in `preloop-observability`, move these four channels onto it, and delete the ad-hoc fields. Adding a
fifth parallel channel beside four existing ones is a regression, and four `Option<Arc<…>>` fields
that may each independently be `None` already make "what is the pool doing" unanswerable. Do not add a
server-to-orchestrator dependency and do not make the status endpoint shell out to SmolVM.

### VM resource telemetry has stable host-side inputs but no Preloop surface

`VmProvider` in `crates/preloop-vm/src/lib.rs:290-331` exposes lifecycle plus `MachineState` only
(`create`, `start`, `start_forkable`, `fork`, `stop`, `delete`, `status`, `list`, `exec`,
`exec_with_secret_env`, `exec_stream`, `rearm_fork_base`, `copy`, `pack`).
`SmolVmProvider::status` at `crates/preloop-vm/src/lib.rs:1018-1043` shells out to `smolvm machine status` and substring-
matches lowercased human text, discarding the PID and configured resource data needed for telemetry:

```rust
let text = String::from_utf8_lossy(&output.stdout).to_ascii_lowercase();
Ok(if text.contains("running") {
    MachineState::Running
} else if text.contains("stopped") {
    MachineState::Stopped
} else {
    MachineState::Unknown
})
```

The supported SmolVM floor is **`1.8.1`** (`versions.toml:8`, `smolvm_min_version`); the golden pin is
`1.8.2` (`versions.toml:16`). The v1.8.1 contract is stronger than the first draft assumed:

- `machine status --json` (`src/cli/machine.rs:3783-3789` → `vm_common::status_vm_json`) and
  `machine ls --json` (`src/cli/machine.rs:3857-3862`) emit **the same per-machine object**, built by
  one shared `machine_status_json` helper (`src/cli/vm_common.rs:2360-2412`) explicitly so "the two
  outputs never drift apart".
- Fields: `name`, `state`, `cpus`, `memory_mib`, `pid`, `mounts`, `ports`, `created_at`, `storage_gb`,
  `overlay_gb`, `image`, `ephemeral`, `forkable`, `forkpoint_held`, `labels`, `restart_policy`,
  `restart_max_retries`, `restart_count`, `health_cmd`, `health_interval_secs`, `health_timeout_secs`,
  `health_retries`, `health_startup_grace_secs`, `network`, plus GPU/CUDA fields.
- `state` is **not** the persisted record state. `machine_status_json` resolves it through
  `agent::state_probe::resolve_state`, a vsock liveness probe that yields a distinct
  **`Unreachable`** state when the record says `Running` and the VMM PID is alive but the guest agent
  does not answer (`src/agent/state_probe.rs:6-76`). That is a first-class "the VM is wedged" signal
  Preloop currently throws away by substring-matching for `running`.
- `machine data-dir` is a public command (`src/cli/machine.rs:365, 4290-4310`) exposing the
  hash-derived machine path; the JSON object does not carry it, so disk-path resolution still needs
  this call.

Because `machine ls --json` returns the identical object for every machine in one process, the fleet
sampler needs **one subprocess per pass, not one per VM** — and Preloop already parses exactly this
output today in `SmolVmProvider::list` (`crates/preloop-vm/src/lib.rs:1045-1060`). Use `machine ls
--json` for periodic fleet inspection and `machine status --json` only for a single machine on a
lifecycle/reconciliation path. Never call either from `/metrics` or `/api/v1/status`.

On Linux, Preloop already establishes a delegated cgroup-v2 root and enables `cpu`, `memory`, and
`pids` before the pool starts (`init_vm_cgroup_delegation`, `preloop-vm/src/lib.rs:1360` ff., and
`preloop-cli/src/main.rs:1057-1067`). SmolVM's own supervisor then creates a capped per-VMM
`vm-<pid>` leaf under that delegated root — this is upstream-documented behavior at the supported
floor (`smolvm v1.8.1 src/process.rs:375-393`, `place_in_cgroup`), not a Preloop-side invariant, so a
missing leaf must degrade to the process fallback rather than be treated as an error. Those
host files provide CPU time/throttling, host memory current/limit/events, and VMM/helper PID counts.
On macOS and cgroup-unavailable Linux, fall back to host process CPU time and RSS with PID-start-time
validation.

These are **host-observed VM/VMM metrics**, not guest-kernel metrics. Host RSS is not guest “memory
used”; cgroup PID counts are VMM/helper processes, not processes inside the guest; host cgroup OOM
events do not detect every guest-kernel OOM. Never run periodic commands inside the guest to fill the
gap: that changes the workload being measured and can hang behind a sick guest.

### Local and self-hosted topology is deliberately small

`docs/self-hosting.md` defines `preloop serve` as one process containing the control plane and VM pool.
The native API is bearer-authenticated and loopback/private by default; only the GitHub webhook needs
a public route. The systemd unit captures stdout/stderr in journald. Therefore the default profile
must add no process and must retain stderr/journald output even when OTLP is configured.

### Six blind spots the first draft missed

Re-auditing at `673bdfa0` found six classes of operator-visible state with no surface at all. They are
not refinements of the sections above; each can independently make Preloop behave incorrectly in a way
no log line, metric, or status field currently reports.

#### 1. Bounded buffers and hard limits drop data silently

Preloop enforces several caps. Most discard user-visible data with no counter and, in the worst cases,
no log line at all:

| Limit | Location | Value | Observable today? |
|---|---|---|---|
| per-job live-log retained bytes | `live_logs.rs:25` `DEFAULT_MAX_BYTES` | 64 MiB | **No.** Tail-drops oldest wrappers silently (`live_logs.rs:46`) |
| oversized live-log batch | `live_logs.rs:250` | > per-job cap | WARN only, no counter |
| `concurrency: queue max` pending holders | `concurrency.rs:255` `QUEUE_MAX_PENDING` | 100 | **No.** Overflow sets `cancel_arrival` and cancels a user's job (`concurrency.rs:281`) |
| archived debug sessions | `debug_sessions.rs:64` `MAX_ARCHIVED_SESSIONS` | 64 | **No.** Ring eviction |
| debug session events | `debug_sessions.rs:68` `MAX_SESSION_EVENTS` | 512 | **No.** Ring eviction |
| debug session **audit** entries | `debug_sessions.rs:71` `MAX_SESSION_AUDIT` | 512 | **No.** Ring eviction — silent audit-trail loss |
| completed debug operations | `debug_sessions.rs:74` `MAX_COMPLETED_OPS` | 256 | **No.** Ring eviction |
| git request body | `snapshots.rs:20` `MAX_GIT_REQUEST_BYTES` | 16 MiB | Rejected, not counted |
| reusable-workflow nesting | `remote_workflows.rs:6` `MAX_REUSABLE_WORKFLOW_DEPTH` | 4 | Rejected, not counted |

A cap that silently discards data is worse than an outage: the operator sees a plausible but wrong
answer. `queue max` overflow is the sharpest case — a user's job is cancelled by policy and nothing
distinguishes that from any other cancellation. Silent audit eviction is a compliance problem, not a
telemetry nicety.

This plan therefore adds one bounded instrument family, `preloop.limit.*`, whose `limit` attribute is
the **constant's name** (a compile-time-finite set), never a value or an identifier. Every constant in
the table above must be registered through it, and any future cap must be added with it.

#### 2. Scheduled workflows have a history endpoint and no telemetry

`scheduler.rs` drives cron/scheduled workflows, spawns its scan from `bootstrap.rs:479` and
`bootstrap.rs:485`, and exposes `GET /api/v1/scheduler/history` (`routes.rs:419`). Nothing reports
whether a schedule fired, fired late, was skipped because the previous instance was still running, or
silently stopped firing because the scan task died. "My nightly job did not run" is a first-order
operator question with a zero-coverage answer today.

#### 3. GitHub dependency budget is untracked

`AppState` holds `dispatch_token_cache` and `dispatch_actor_cache` (60 s TTL,
`dispatch_auth.rs:46, 49`), `action_sha_cache`, `github_pat`, and `github_app`/`github_apps`.
`actions.rs:32` sets `ACTION_TICKET_TTL_SECS` to six hours; `oidc.rs:25` sets `TOKEN_TTL_SECS` to 300.
Nothing reads `x-ratelimit-remaining`/`x-ratelimit-reset`, tracks installation-token expiry, or reports
cache hit rate. GitHub secondary-rate-limit exhaustion is the single most common integration outage
for a control plane of this shape, and it is currently indistinguishable from "GitHub is slow".

#### 4. Persistent storage growth is unbounded and unreported

Preloop writes to the state dir (`preloop.db` plus WAL, checkpointed every
`store.rs:211` `WAL_CHECKPOINT_INTERVAL` = 128 commits), the cache and artifact stores, the run-log
store, the snapshot object cache (`snapshots.rs:1187` `ObjectCache`, GC spawned at `snapshots.rs:1896`),
replay results (pruned at `distributed_task.rs:895`), and VM images/overlays. Only the VM volume gets a
free-space signal in the first draft. A full state-dir filesystem is a hard outage, and the first
symptom today is a store write failure with no capacity context.

Plan 001 owns cache quotas and eviction policy. Plan 002 owns the measurement contract: 001 must emit
through the `preloop.storage.*` family defined here rather than inventing its own.

#### 5. Fifteen background tasks, two proposed heartbeats

The first draft made `/readyz` depend on "the state sampler and reaper heartbeats". The real inventory
of long-lived tasks whose death is currently silent:

| Task | Location | Cadence |
|---|---|---|
| reaper (timeouts, leases, deaf runners) | `bootstrap.rs:396-410` | 10 s interval |
| scheduler scan | `bootstrap.rs:479`, `bootstrap.rs:485` | timer |
| GitHub App event loop | `bootstrap.rs:497`, `bootstrap.rs:517` | event-driven |
| shutdown/lifecycle supervisor | `bootstrap.rs:568` | event-driven |
| listener accept loops (TCP + unix) | `bootstrap.rs:601, 615, 660, 679, 720` | per connection |
| snapshot object-cache GC | `snapshots.rs:1896` | periodic |
| replay-result prune | `distributed_task.rs:895` | periodic |
| Postgres connection task | `store_pg.rs:95-103` | lifetime |
| GitHub check dispatch | `github.rs:1064` | event-driven |
| auto-PR gate | `github_pr.rs:397` | event-driven |
| `RunnerPool::run` supervisor | `preloop-orchestrator/src/lib.rs:1880-2100` | loop |
| guest pause watchers | `preloop-orchestrator/src/lib.rs:3027, 4395, 4450` | per VM |
| pool run/watch tasks | `preloop-orchestrator/src/lib.rs:3367, 3532, 5854, 5875` | per operation |
| key rotation | `preloop-orchestrator/src/keys.rs:74` | periodic |

Hand-wiring two heartbeats and leaving twelve silent is the same failure mode this plan exists to fix.
Replace the ad-hoc approach with one `TaskHeartbeat` registry in `preloop-observability`: a task
registers a stable name at spawn, beats each iteration, and deregisters on clean exit. Readiness and
status read the registry; a task that stops beating or panics is visible generically. Only the tasks
marked "critical" in the registry gate `/readyz`, so an event-driven task idling is not a failure.

#### 6. HTTP surfaces are wider than the first draft's classification

`routes.rs` serves thirteen path families, not the eight the first draft enumerated: `/_apis`,
`/broker`, `/runner`, `/runner/server`, `/api/v1`, `/twirp`, `/twirp-blob`, `/internal/test`,
`/ws/live-logs`, `/snapshots`, `/.well-known`, `/oidc`, `/repos`, plus `/healthz`. Routers are
assembled by merge (`routes.rs:39` `protected_apis`, `:209` `results_metadata`, `:233` `dispatch_api`,
merged at `:349, 797, 799, 813`). Four middlewares carry the auth contract: `require_native_bearer`,
`require_results_bearer`, `require_job_runtime_bearer`, and `resolve_runner_identity`, with
`auth.rs::runner_surface_only` layered only on the unix router.

Two of these need different treatment from a request-duration histogram:

- `/ws/live-logs` is a long-lived WebSocket. A duration histogram over its lifetime measures nothing
  useful and pollutes the latency SLI. Use a connection gauge plus a close-reason counter.
- `/snapshots` and `/repos` serve git objects to VMs. These are the path by which workflow source
  reaches a job; a failure here fails every job with a misleading in-workflow error.

## Architecture and invariants

```mermaid
flowchart LR
    CLI[preloop status] -->|native bearer| STATUS[/api/v1/status]
    PROBE[systemd / operator probes] --> LIVE[/healthz + /readyz]
    PROM[Prometheus-compatible scraper] -->|native bearer| METRICS[/metrics]

    SERVER[Control plane] --> OBS[preloop-observability]
    POOL[Runner pool] --> OBS
    VMHOST[Host VM sampler: cgroup/process/sparse disk] --> OBS
    OBS --> STDERR[stderr / journald]
    OBS --> METRICS
    OBS -. optional bounded OTLP/HTTP .-> O2[OpenObserve or any OTLP backend]
    COLLECTOR[Optional OTel Collector for host telemetry] -.-> O2

    SERVER --> RUNLOGS[Existing workflow run-log store]
    CAPTURE[Explicit protocol flow capture] --> LOCALFILE[Local 0600 capture file]
```

### New crate

Create `crates/preloop-observability` with a small, explicit API and no dependency on server or
orchestrator internals:

- `ObservabilityConfig::from_env()` parses logging and standard OTel configuration without exposing
  header values in `Debug` or errors.
- `Observability::noop()` is allocation-light and makes unit tests and library-only consumers perform
  no network I/O.
- `Observability` is a cloneable handle containing pre-created instruments, the cached operational
  snapshot, pool/VM-status handles, critical-task heartbeats, and telemetry-export health.
- `TaskHeartbeat` registry: `register(name, criticality) -> HeartbeatHandle`, `beat()`,
  `Drop` deregisters. Names come from a compile-time-finite set; readiness reads only entries marked
  critical. This replaces per-task ad-hoc `AtomicU64` timestamps.
- `LimitRegistry`: `record_drop(limit, count)` / `record_reject(limit)` where `limit` is a
  `&'static str` constant name, backing the `preloop.limit.*` family and the status `limits` block.
- `ObservabilityRuntime` owns subscriber/provider guards and performs bounded shutdown/flush.
- `OperationalSnapshot`, `Condition`, `PoolSnapshot`, `VmFleetSnapshot`, `VmSample`, and their bounded
  enums are serializable DTOs shared by server and CLI.
- OpenTelemetry API/instrument types are always available; SDK, OTLP/HTTP, Prometheus, tracing bridge,
  and formatting layers are host-only Cargo features. Disable default features on exporter crates and
  do not pull the tonic/gRPC stack in the first implementation.

Both `preloop` and standalone `preloop-server` construct one handle/runtime before building
`ServerConfig`; the same handle is cloned into `AppState` and `RunnerPoolConfig`. The guest
`preloop-runner` gets only the structured local logger and never exports directly by default.

### Non-negotiable invariants

1. **Fail open**: telemetry creation or export failure produces a sanitized warning and status
   condition, never a failed request or process exit. It cannot change queue, runner, or run state.
2. **No request-path export**: logs, metrics, and spans enqueue into bounded nonblocking SDK batches.
   No handler awaits an exporter. Queue overflow drops telemetry and increments local drop health.
3. **Bounded shutdown**: attempt flush for at most two seconds after server/pool shutdown. Then exit.
4. **No backend by default**: absent explicit `OTEL_EXPORTER_OTLP_*` endpoint configuration, Preloop
   opens no telemetry connection. Local metrics/status/logging still work. This is a **deliberate
   deviation from the OTel specification**, whose default `OTEL_EXPORTER_OTLP_ENDPOINT` is
   `http://localhost:4318`. Preloop treats an absent variable as "disabled", not "localhost", because
   a CI control plane must not emit background connection attempts on an operator's machine. Document
   the deviation in `docs/observability.md`; an operator who wants spec behavior sets the variable.
5. **Always retain stderr**: OTLP augments, never replaces, terminal/journald logs.
6. **No wire changes**: do not add fields, alter status codes, or change bodies on `/_apis`, `/broker`,
   `/runner`, or Twirp/result-service routes. Instrument around existing behavior.
7. **No high-cardinality metric attributes**: IDs and user-controlled strings are logs/traces only.
8. **No workflow output export**: workflow stdout/stderr stays in the existing run-log store.
9. **No raw HTTP capture**: do not collect headers, bodies, raw URI, query strings, or error text into
   metrics. Trace attributes are allowlisted, not denylisted.
10. **No state-lock callbacks**: OTel observable callbacks cannot await or lock `InnerState`. A periodic
    async sampler updates a small cached snapshot; exporters read that snapshot.
11. **No avoidable hot-path allocation**: instruments and attribute arrays are prebuilt where static;
    poll/renew success is metrics-only and DEBUG-filtered, not an INFO log allocation.
12. **No guest polling**: VM sampling reads host cgroup/process/filesystem state only. It never executes
    `free`, `df`, `ps`, `dmesg`, or another command inside a runner VM.
13. **No fake zeroes**: an unsupported or stale VM metric is absent and its capability is reported
    unavailable. Zero means the source measured zero.
14. **No silent drop**: any code path that discards, evicts, truncates, or rejects data because of a
    cap MUST record it through `LimitRegistry`. A cap without a counter is a defect. This applies to
    telemetry's own bounded queues as much as to live-log buffers and concurrency queues.
15. **No unregistered long-lived task**: every `tokio::spawn` that outlives a request registers a
    `TaskHeartbeat`. A background loop whose death is invisible is the failure this plan exists to
    remove; the fifteen-task inventory above is the acceptance baseline, not an example list.

## Operator surfaces

### `/healthz`: liveness only

Keep it unauthenticated and intentionally shallow. Return 200 while the process can serve requests and
503 once shutdown begins. Response fields: schema version, `ok`, protocol version. It must not call
SQLite/Postgres, GitHub, OpenObserve, SmolVM, or acquire `InnerState`.

### `/readyz`: critical-loop readiness

Keep it unauthenticated but reveal only boolean state and stable reason codes. Return 200 after durable
state restoration and router startup, while every `TaskHeartbeat` marked critical is fresh and
shutdown has not started. Return 503 for `starting`, `shutting_down`, or `task_stale` with the stale
task's registry name as the reason code. The critical set is exactly: the state sampler, the reaper
(`bootstrap.rs:396`), the scheduler scan (`bootstrap.rs:479`), and — when the backend is Postgres —
the connection task (`store_pg.rs:95`). Everything else is reported in `/api/v1/status` but does not
gate readiness.

Reason codes are the registry names, so adding a critical task adds a code without a schema change.
Non-critical task staleness must never return 503: an event-driven task with no events is healthy.

Do **not** make readiness depend on runner capacity, GitHub reachability, store write success, workflow
success, or telemetry export. This is a single-authoritative-process design; making a dependency
failure fail readiness would hide the only control plane that can explain it and could create a
restart loop. Rich degradation belongs in `/api/v1/status`.

Change `wait_for_engine_socket` (`crates/preloop-cli/src/main.rs:1458-1475`) to probe `/readyz`, not
`/healthz`. Keep its 500 ms per-attempt timeout and 30 s window; on window expiry, report the last
`/readyz` reason code instead of a generic timeout, so "server started but the scheduler never came
up" is distinguishable from "server never bound".

### `/api/v1/status`: authenticated operational diagnosis

Add an authenticated native endpoint returning a stable, versioned snapshot. It reads the most recent
sampler snapshot without waiting on `InnerState` and reports `snapshot_age_seconds`; sampling every
five seconds is sufficiently current and remains responsive when the state lock is the incident.
Limit problem exemplars to five per condition.

Required shape (field names may be Rust snake_case internally but the JSON contract is fixed):

```json
{
  "schema_version": 1,
  "observed_at": "RFC3339 timestamp",
  "snapshot_age_seconds": 0.4,
  "overall": "ok|degraded|blocked|shutting_down",
  "service": {
    "version": "0.x.y",
    "instance_id": "uuid-per-process",
    "uptime_seconds": 123,
    "shutdown_requested": false
  },
  "runs": {"queued": 0, "in_progress": 1, "completed": 12},
  "jobs": {
    "ready": 2,
    "dependency_blocked": 1,
    "concurrency_blocked": 0,
    "pending_expansion": 0,
    "expanding": 0,
    "claimable": 1,
    "unclaimable": 1,
    "oldest_ready_seconds": 14.2
  },
  "concurrency": {
    "groups_active": 3,
    "groups_contended": 1,
    "pending_holders": 4,
    "deepest_group_pending": 4,
    "queue_max_pending": 100,
    "overflow_cancellations": 0
  },
  "scheduler": {
    "enabled": true,
    "schedules": 4,
    "last_scan_at": "RFC3339 timestamp",
    "next_fire_at": "RFC3339 timestamp",
    "fired": 12,
    "skipped_overlapping": 1,
    "late_fires": 0,
    "max_fire_delay_seconds": 2.1
  },
  "runners": {
    "registered": 2,
    "sessions": 2,
    "idle": 1,
    "busy": 1,
    "stale": 0,
    "max_poll_age_seconds": 3.1,
    "max_lease_age_seconds": 8.0
  },
  "pool": {
    "mode": "warm|on_demand|external|disabled",
    "desired": 2,
    "preparing": false,
    "building": 0,
    "provisioning": 0,
    "idle": 1,
    "busy": 1,
    "paused": 0,
    "consecutive_provision_failures": 0,
    "last_transition_at": "RFC3339 timestamp"
  },
  "vms": {
    "source": "cgroup_v2|process|mixed|unavailable",
    "sample_age_seconds": 1.2,
    "capabilities": {
      "cpu": true,
      "memory": true,
      "cpu_throttling": true,
      "host_oom_events": true,
      "host_pids": true,
      "sparse_disk_allocation": true,
      "block_io": false,
      "network_io": false,
      "guest_os": false
    },
    "count": {"runner": 2, "golden": 1, "unavailable": 0},
    "configured": {
      "vcpus": 10,
      "memory_bytes": 21474836480,
      "storage_bytes": 85899345920,
      "overlay_bytes": 21474836480
    },
    "host_usage": {
      "cpu_cores": 2.4,
      "memory_bytes": 7516192768,
      "sparse_disk_allocated_bytes": 12884901888
    },
    "top_consumers": [
      {
        "machine_name": "preloop-runner-0",
        "role": "runner",
        "activity": "busy",
        "run_id": "authenticated-detail-only",
        "job_id": "authenticated-detail-only",
        "cpu_cores": 1.8,
        "memory_bytes": 4294967296,
        "memory_limit_bytes": 7516192768,
        "sparse_disk_allocated_bytes": 5368709120,
        "host_cpu_throttled_seconds": 0.0,
        "host_oom_kills": 0,
        "sample_age_seconds": 1.2
      }
    ]
  },
  "store": {
    "backend": "sqlite|postgres",
    "consecutive_failures": 0,
    "last_success_at": "RFC3339 timestamp",
    "last_failure_at": null
  },
  "github": {
    "configured": true,
    "last_webhook_at": "RFC3339 timestamp",
    "pending_check_updates": 0,
    "last_check_success_at": "RFC3339 timestamp",
    "last_check_failure_at": null
  },
  "debug": {"active_sessions": 0, "oldest_session_seconds": null},
  "storage": {
    "state_dir": "/var/lib/preloop",
    "state_fs_free_bytes": 41231234560,
    "state_fs_free_ratio": 0.42,
    "components": [
      {"store": "database", "bytes": 184549376},
      {"store": "cache", "bytes": 2147483648},
      {"store": "artifacts", "bytes": 536870912},
      {"store": "run_logs", "bytes": 268435456},
      {"store": "snapshots", "bytes": 1073741824},
      {"store": "vm_images", "bytes": 68719476736}
    ],
    "last_gc_at": "RFC3339 timestamp"
  },
  "limits": [
    {
      "limit": "LIVE_LOG_MAX_BYTES",
      "value": 67108864,
      "dropped": 0,
      "rejected": 0,
      "last_at": null
    }
  ],
  "tasks": [
    {
      "name": "reaper",
      "critical": true,
      "heartbeat_age_seconds": 3.2,
      "state": "running|idle|stale|exited"
    }
  ],
  "telemetry": {
    "otlp_enabled": false,
    "last_export_success_at": null,
    "last_export_failure_at": null,
    "dropped_records": 0
  },
  "conditions": []
}
```

The `github` block gains dependency-budget fields, which are the difference between "GitHub is slow"
and "we are rate limited":

```json
"github": {
  "configured": true,
  "last_webhook_at": "RFC3339 timestamp",
  "pending_check_updates": 0,
  "last_check_success_at": "RFC3339 timestamp",
  "last_check_failure_at": null,
  "rate_limit": {
    "resource": "core",
    "limit": 5000,
    "remaining": 4812,
    "reset_at": "RFC3339 timestamp",
    "observed_at": "RFC3339 timestamp"
  },
  "installation_token_expires_in_seconds": 2841,
  "token_cache": {"hits": 91, "misses": 4, "ttl_seconds": 60}
}
```

`limits` and `tasks` are arrays, not fixed objects, because both sets grow with the code. Each entry's
`limit`/`name` is a compile-time constant identifier, so the array length stays bounded and the JSON
contract does not change when a cap or task is added. `limits` reports every registered cap, including
those with zero drops — an operator must be able to see the ceiling before hitting it.

Conditions use stable codes and safe messages. Initial codes:

- `state_sampler_stale`
- `task_stale`
- `task_exited`
- `queue_no_registered_runner`
- `queue_label_mismatch`
- `concurrency_queue_overflow`
- `concurrency_group_starved`
- `scheduler_scan_stale`
- `scheduler_fire_late`
- `scheduler_skipped_overlapping`
- `pool_preparing`
- `pool_provisioning_deficit`
- `pool_repeated_provision_failure`
- `runner_poll_stale`
- `runner_lease_stale`
- `vm_sampler_stale`
- `vm_sample_unavailable`
- `vm_unreachable`
- `vm_host_memory_pressure`
- `vm_host_cpu_throttled`
- `vm_host_oom_kill`
- `vm_sparse_disk_pressure`
- `store_write_failure`
- `store_connection_down`
- `storage_capacity_pressure`
- `limit_drop_active`
- `limit_reject_active`
- `github_check_update_failure`
- `github_terminal_check_pending`
- `github_rate_limit_low`
- `github_installation_token_expiring`
- `debug_session_stale`
- `debug_audit_evicted`
- `telemetry_export_failure`

Authenticated condition exemplars may contain run/job/runner/session/machine IDs and `runs-on` labels,
but never tokens, environment values, request bodies, or URLs with query strings. Metrics must contain
only the condition code, never the exemplar fields.

### `preloop status`

Change `Status` to `Status(StatusArgs)` with `--json`. Human output must show, in order:

1. service and snapshot age;
2. ready/blocked queue classes and oldest wait;
3. concurrency-group contention and scheduler state;
4. pool capacity and runner/session freshness;
5. VM fleet capacity, current host usage, capability gaps, and bounded top consumers;
6. store, storage capacity, GitHub budget, debug, and telemetry state;
7. any limit with a nonzero drop/reject count (omit clean limits from human output; keep all of them
   in `--json`);
8. background tasks that are stale or exited (omit healthy ones from human output);
9. typed conditions with one-line actions;
10. the existing recent-runs table.

`--json` prints the endpoint response exactly and no prose, so operators can use `jq` and monitoring
scripts. Preserve native bearer behavior. Do not add a watch loop in this plan.

Follow the existing flag convention (`PlanArgs { #[arg(long)] json: bool }`,
`crates/preloop-cli/src/main.rs:706-707`) rather than inventing a new one.

## Signal contract

### Resource attributes on every exported signal

- `service.name=preloop` (overridable only through standard `OTEL_SERVICE_NAME`)
- `service.version=<preloop package version>`
- `service.instance.id=<new UUID each process start>`
- `deployment.environment.name` only when supplied through standard resource attributes
- host/OS attributes only from an explicitly enabled resource detector

Do not attach repository, workflow, branch, SHA, runner name, or machine name as a resource attribute.

### Metric attribute policy

Allowed metric values are bounded enums or finite route templates:

- HTTP method, Axum matched route, protocol surface, status-code class;
- execution state/conclusion and stable termination reason;
- queue kind, dispatch outcome, pool mode/state, operation, backend, result;
- VM role/activity/runtime state, host sample source, storage kind, capability, and stable sample error;
- webhook/check/cache/artifact/debug operation and stable outcome.
- `&'static str` constant names from a compile-time-finite set: `limit` (cap identifiers),
  `task` (heartbeat registry names), `store` (storage component identifiers). These are code
  identifiers, not data, so their cardinality is bounded by the source, not by traffic.

Forbidden metric attributes:

- run/job/request/runner/session/machine IDs;
- repository/workflow/ref/SHA, `runs-on` label values, cache keys, artifact names;
- raw URL/path/query, webhook delivery ID, GitHub installation ID;
- error message/type text, filenames, user agent, IP address;
- any token, header, body, secret, or environment value.

Add a cardinality test that drives at least 1,000 distinct IDs/names through instrumentation, gathers
the Prometheus registry, asserts a fixed upper bound on series count, and asserts none of the unique
values occur in exposition text.

### Metrics catalog

Use seconds for durations and bytes for sizes. Histograms count observations; do not add duplicate
request counters when histogram count answers the same question.

| Instrument | Type | Required attributes | Purpose |
|---|---|---|---|
| `http.server.request.duration` | histogram | method, matched route, surface, status class | API latency/error rate using OTel HTTP semantics |
| `http.server.active_requests` | up/down counter | method, matched route, surface | current HTTP concurrency |
| `preloop.broker.poll` | counter | surface, outcome=`job|cancel|empty|error` | distinguish healthy empty long-polls from dispatch failures |
| `preloop.run.active` | observable gauge | state | runs by current state |
| `preloop.run.completed` | counter | conclusion, termination reason | terminal runs without user identifiers |
| `preloop.run.duration` | histogram | conclusion | run wall time |
| `preloop.job.active` | observable gauge | state | jobs by current state |
| `preloop.job.completed` | counter | conclusion, termination reason | terminal jobs and bounded failure taxonomy |
| `preloop.job.duration` | histogram | conclusion | execution time after claim |
| `preloop.job.queue.depth` | observable gauge | queue kind | ready/dependency/concurrency/expansion depth |
| `preloop.job.queue.oldest_age` | observable gauge | queue kind | detect stalls, not just depth |
| `preloop.job.queue.wait` | histogram | outcome | ready-to-claim or terminal-unclaimed wait |
| `preloop.job.claimability` | observable gauge | reason | claimable vs temporary/permanent unclaimability |
| `preloop.concurrency.group.active` | observable gauge | state=`holding|pending` | concurrency-group occupancy without group names |
| `preloop.concurrency.pending.depth` | observable gauge | none | deepest pending queue across groups |
| `preloop.concurrency.decision` | counter | queue mode, action=`park|cancel_pending|cancel_arrival|admit` | why a job was parked or cancelled by `concurrency:` policy |
| `preloop.scheduler.fire` | counter | outcome=`fired|skipped_overlapping|error` | did a schedule actually run |
| `preloop.scheduler.fire.delay` | histogram | none | scheduled time to actual dispatch |
| `preloop.scheduler.schedules` | observable gauge | none | registered schedule count |
| `preloop.runner.count` | observable gauge | state=`registered|idle|busy|stale` | runner inventory |
| `preloop.runner.session.count` | observable gauge | state | active sessions |
| `preloop.runner.session.transition` | counter | operation, reason, outcome | create/delete/reap/reconcile lifecycle |
| `preloop.runner.poll.max_age` | observable gauge | none | deaf-runner leading indicator |
| `preloop.runner.lease.max_age` | observable gauge | none | expired-lease leading indicator |
| `preloop.pool.runner.count` | observable gauge | mode, state=`desired|idle|busy|building|provisioning|paused` | capacity and deficit |
| `preloop.pool.preparing` | observable gauge | mode | image/golden warm state |
| `preloop.pool.operation.duration` | histogram | operation, outcome, reason | prepare/provision/register/assign/delete/replace |
| `preloop.vm.count` | observable gauge | role, activity, runtime state, pool mode | active runner/golden VM inventory |
| `preloop.vm.configured.vcpus` | observable gauge | role, pool mode | configured virtual CPU capacity |
| `preloop.vm.configured.memory` | observable gauge | role, pool mode | configured guest-memory ceiling in bytes |
| `preloop.vm.configured.storage` | observable gauge | role, storage kind=`root|overlay` | configured logical disk capacity in bytes |
| `preloop.vm.host.cpu.time` | counter | role, pool mode | sampled host CPU seconds attributable to VMM cgroups/processes |
| `preloop.vm.host.cpu.cores` | observable gauge | role, activity, pool mode | current host CPU-seconds/second, expressed as cores |
| `preloop.vm.host.cpu.throttled_time` | counter | role, pool mode | Linux cgroup throttle seconds; absent elsewhere |
| `preloop.vm.host.cpu.throttled_periods` | counter | role, pool mode | Linux cgroup throttled-period count; absent elsewhere |
| `preloop.vm.host.memory.usage` | observable gauge | role, activity, source | current cgroup memory or process RSS in bytes |
| `preloop.vm.host.memory.limit` | observable gauge | role, source | cgroup/configured host memory limit in bytes |
| `preloop.vm.host.memory.events` | counter | role, event=`low|high|max|oom|oom_kill` | Linux host-cgroup events; not guest-kernel OOM |
| `preloop.vm.host.pids.current` | observable gauge | role | Linux VMM/helper process count; not guest process count |
| `preloop.vm.host.pids.limit` | observable gauge | role | Linux VMM/helper process limit |
| `preloop.vm.host.pids.events` | counter | role, event=`max` | Linux host-cgroup PID-limit events |
| `preloop.vm.sparse_disk.allocated` | observable gauge | role, storage kind | physically allocated host blocks for VM-private/shared files |
| `preloop.vm.storage.available` | observable gauge | storage class=`smolvm_data|preloop_state` | authoritative free bytes on the filesystem holding VM state |
| `preloop.vm.age.max` | observable gauge | role, activity | oldest active VM age for leaked-machine detection |
| `preloop.vm.sampler.available` | observable gauge | capability, source | whether each VM signal can be measured on this host |
| `preloop.vm.sampler.age` | observable gauge | source | age of the last successful fast sample |
| `preloop.vm.sampler.errors` | counter | source, reason | bounded sampler failures such as permission, parse, PID reuse, process gone |
| `preloop.store.operation.duration` | histogram | backend, operation, outcome | one decorator around all store methods |
| `preloop.store.consecutive_failures` | observable gauge | backend | restart-durability risk |
| `preloop.store.connection.up` | observable gauge | backend | Postgres connection task alive; SQLite always 1 |
| `preloop.storage.bytes` | observable gauge | store=`database|cache|artifacts|run_logs|snapshots|vm_images` | persistent footprint per component |
| `preloop.storage.fs.available` | observable gauge | mount=`state_dir|smolvm_data` | authoritative free bytes |
| `preloop.storage.gc` | counter | store, outcome | eviction/prune passes; Plan 001 emits through this |
| `preloop.limit.dropped` | counter | limit | records discarded by a cap (live-log tail-drop, ring eviction) |
| `preloop.limit.rejected` | counter | limit | requests/arrivals refused by a cap (queue max, body size, nesting depth) |
| `preloop.limit.value` | observable gauge | limit | the configured ceiling, so alerts compare against it without hardcoding |
| `preloop.task.heartbeat.age` | observable gauge | task | seconds since last beat; the generic dead-loop detector |
| `preloop.task.exited` | counter | task, outcome=`clean|error|panic` | background task termination |
| `preloop.github.operation.duration` | histogram | operation, outcome | token/check/API dependencies |
| `preloop.github.check.propagation_delay` | histogram | outcome | terminal run to GitHub acknowledgement |
| `preloop.github.rate_limit.remaining` | observable gauge | resource | `x-ratelimit-remaining` from the last response |
| `preloop.github.rate_limit.limit` | observable gauge | resource | `x-ratelimit-limit` |
| `preloop.github.rate_limit.reset_in` | observable gauge | resource | seconds until the window resets |
| `preloop.github.token.expires_in` | observable gauge | kind=`installation|oidc|action_ticket` | credential expiry countdown |
| `preloop.github.token.cache` | counter | kind, outcome=`hit|miss|expired` | dispatch/actor/action-SHA cache effectiveness |
| `preloop.webhook.duration` | histogram | event class, outcome, dedup outcome | webhook processing without delivery/repo labels |
| `preloop.cache.operation.duration` | histogram | operation, outcome | cache behavior |
| `preloop.cache.transfer.bytes` | counter | direction, outcome | payload volume |
| `preloop.artifact.operation.duration` | histogram | operation, outcome | artifact behavior |
| `preloop.artifact.transfer.bytes` | counter | direction, outcome | payload volume |
| `preloop.debug.session.count` | observable gauge | state | active/paused/detached sessions |
| `preloop.debug.session.duration` | histogram | terminal reason | leaked-session detection |
| `preloop.snapshot.operation.duration` | histogram | operation, outcome | git object serving that feeds every job |
| `preloop.snapshot.cache` | counter | outcome=`hit|miss|evicted` | snapshot object-cache effectiveness |
| `preloop.livelog.connections` | up/down counter | none | open `/ws/live-logs` WebSockets; not a duration histogram |
| `preloop.livelog.connection.closed` | counter | reason | WebSocket termination taxonomy |
| `preloop.livelog.buffer.bytes` | observable gauge | none | retained live-log bytes against the 64 MiB per-job cap |
| `preloop.telemetry.export` | counter | signal, outcome | exporter self-health, visible locally at `/metrics` |
| `preloop.service.uptime` | observable gauge | none | no-data heartbeat and process continuity |

Use explicit bucket views:

- HTTP/store/GitHub: 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 seconds.
- queue wait: 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 900 seconds.
- pool preparation/provision: 1, 5, 10, 30, 60, 120, 300, 600 seconds.
- scheduler fire delay: 1, 5, 15, 30, 60, 300, 900, 3600 seconds. A cron job is not late at 10 ms
  resolution, and reusing the HTTP buckets would waste eight of eleven boundaries.

`/ws/live-logs` is deliberately excluded from `http.server.request.duration`. A long-lived WebSocket
would otherwise dominate the p99 and silently break the availability SLI's denominator.

The five-second sampler captures counts and bounded condition exemplars under `InnerState` once, then
releases the mutex before updating the shared snapshot. It must not copy log buffers, artifacts,
cache bytes, full run records, or bodies.

### VM telemetry contract

VM metrics are first-class Preloop application telemetry because the embedded pool owns these
processes and already knows their lifecycle/assignment. General node telemetry remains optional.

#### Collection architecture

1. Replace the human-text `VmProvider::status` contract with one typed inspection contract that all
   provider implementations and test fakes implement. For SmolVM at the `1.8.1` floor:
   - single machine on a lifecycle path (start/fork/adoption/reconciliation): `machine status --json`;
   - whole fleet on the slow sampler pass: **one** `machine ls --json` call, which returns the
     identical per-machine object for every machine (upstream `smolvm src/cli/vm_common.rs:2360`
     builds both outputs from one helper). `SmolVmProvider::list`
     (`crates/preloop-vm/src/lib.rs:1045-1060`) already parses this response —
     extend it to a full `VmRuntimeInfo` instead of discarding everything but `name`.
   - `machine data-dir` only when a disk path is needed; the JSON object does not carry it.

   Parse into a versioned internal `VmRuntimeInfo` and cache machine role, configured resources
   (`cpus`, `memory_mib`, `storage_gb`, `overlay_gb`), `pid`, process start identity, `created_at`,
   `restart_count`, `ephemeral`, `forkable`/`forkpoint_held`, and relevant disk paths. Migrate every
   status caller; leave no text-parser alias.

   Map `state` faithfully, including **`Unreachable`**. SmolVM resolves `state` through a vsock
   liveness probe (`src/agent/state_probe.rs:41-76`), so `Unreachable` means "record says Running,
   VMM PID alive, guest agent not answering" — a wedged VM. Today's `text.contains("running")` cannot
   even represent it. Surface it as `MachineState::Unreachable`, metric attribute
   `runtime_state=unreachable`, and status condition `vm_unreachable`. A pool that keeps forking from
   an unreachable golden is a real and currently invisible failure.
2. Register/deregister that runtime info with a `VmTelemetryRegistry` owned by the runner pool and
   exposed through the neutral observability handle. Paused debug VMs remain registered until they are
   actually deleted.
3. Run one host sampler task, not one task or CLI subprocess per VM. Fast cadence: five seconds for
   process identity, CPU, memory, cgroup events, and PID counts. Slow cadence: 60 seconds for sparse
   allocated blocks, filesystem capacity, and the single `machine ls --json` fleet reconciliation.
   The fast path touches only host cgroup/proc files and spawns no subprocess at all.
4. Validate PID start identity on every fast sample. PID reuse invalidates the cache, records
   `pid_reused`, and triggers one bounded re-inspection; never attribute a new process's resources to
   an old VM.
5. Publish one immutable fleet snapshot to status and observable gauges after a complete pass. A
   partial/error sample preserves the last good values with explicit age and availability; it never
   overwrites a missing field with zero.

Do not run `smolvm machine status` during `/metrics` or `/api/v1/status`. Those endpoints read the
cached snapshot only.

#### Source priority and exact semantics

| Signal | Linux preferred source | macOS/fallback source | Meaning |
|---|---|---|---|
| configured CPU/memory/storage | cached SmolVM status JSON | same | requested VM capacity, not current use |
| CPU time | `cpu.stat` `usage_usec` | cumulative VMM process CPU time | host CPU consumed by the VM runtime |
| current CPU cores | delta CPU seconds / delta monotonic wall seconds | same | host cores currently consumed; divide by configured vCPUs for a utilization ratio |
| CPU throttling | `cpu.stat` throttle fields | unavailable | host cgroup quota pressure, not guest scheduler steal time |
| host memory | `memory.current` / `memory.max` | VMM RSS plus configured limit | host memory charged to the VM runtime; not guest free/used memory |
| host memory events | deltas from `memory.events` | unavailable | host cgroup events; `oom_kill` means the host killed VMM/helper work, not necessarily a guest process |
| host PID count | `pids.current` / `pids.max` / `pids.events` | unavailable | VMM/helper host processes; not the guest process table |
| disk allocation | filesystem allocated blocks (`st_blocks × 512`) for known VM files | same | physical blocks attributed to VM files, not sparse logical length |
| host free disk | `statvfs`/equivalent on the VM state filesystem | same | authoritative capacity pressure for the host volume |

Counter instruments record positive deltas between samples so they remain monotonic across VM
deletion. A first sample establishes a baseline; PID change/restart establishes a new baseline rather
than adding the new process's cumulative value.

Sparse/CoW accounting has two caveats:

- Count shared golden/image files once under `role=golden`; runner entries count only their private
  overlay/runtime files.
- `st_blocks` may still double-count shared extents on filesystems that do not expose exclusive-block
  ownership. Therefore filesystem free bytes are authoritative for alerts; per-VM allocated bytes are
  a comparative diagnostic, not a billing total.

Network byte/packet/drop counters and block-I/O operation/byte counters are not available through the
current stable SmolVM/Preloop boundary on every supported platform. Omit those series and report their
capabilities false. Do not emit zero. Add them only after SmolVM exposes virtio counters or Preloop
deliberately enables and validates a cgroup-IO/interface source at the supported runtime floor.

Guest OS signals—guest memory free, guest load average, guest process count, guest filesystem free,
guest OOM killer events, and guest network I/O—are likewise absent in this phase. A future
implementation must use an out-of-band SmolVM agent push/snapshot API, not periodic guest shell
commands and not a modified official Actions runner.

#### Cardinality and diagnostics

Metrics aggregate by bounded `role=runner|golden`, `activity=idle|busy|paused|unassigned`, runtime
state, pool mode, source, capability, and storage kind. Never label a metric with machine, slot,
runner, run, or job identity; ephemeral VM names would create permanent time-series churn.

`/api/v1/status` returns aggregate totals plus at most five top consumers, ordered deterministically by
pressure and carrying authenticated machine/run/job correlation. Emit transition-based,
hysteresis/rate-limited structured events for historical attribution:

- `vm.host.memory.pressure` / `.recovered`
- `vm.host.cpu.throttled` / `.recovered`
- `vm.host.oom_kill`
- `vm.sparse_disk.pressure` / `.recovered`
- `vm.sampler.unavailable` / `.recovered`

Threshold events name the machine/run/job in logs/traces only. They must not log environment,
commands, mounted paths, image credentials, or guest data.

### Structured log catalog

Use `event.name` plus stable fields. OTel trace/span IDs provide request correlation; do not allocate a
second UUID on every HTTP poll.

| Event name | Level | Required fields |
|---|---|---|
| `server.started` / `server.stopping` | INFO | instance ID, version, listen scheme/address, store backend, pool mode |
| `run.accepted` / `run.completed` | INFO | run ID, workflow path/repository only when known, conclusion, duration |
| `job.ready` / `job.claimed` / `job.completed` | INFO | run ID, job ID, request/runner ID when relevant, conclusion/reason |
| `job.requeued` / `job.unclaimable` | WARN | run ID, job ID, stable reason, safe labels |
| `job.concurrency.cancelled` | WARN | run ID, job ID, queue mode, group hash (not the raw group key), action |
| `schedule.fired` / `schedule.skipped` | INFO/WARN | schedule ID, workflow path, delay seconds, reason |
| `runner.registered` | INFO | runner ID, runner name, safe labels |
| `runner.session.created` / `runner.session.deleted` | INFO | runner ID, session ID, reason |
| `runner.session.reaped` | WARN | runner/session ID, `deaf|lease_expired|startup_orphan` |
| `pool.prepare.started` / `pool.prepare.completed` | INFO | machine/golden name, mode, duration, outcome |
| `pool.provision.started` / `pool.provision.completed` | INFO/WARN | machine name, slot, duration, stable outcome/reason |
| `pool.supervisor.exited` | ERROR | stable reason; full safe error as log text |
| `task.exited` | WARN/ERROR | registry task name, outcome, uptime; ERROR when the task is critical |
| `limit.exceeded` | WARN | limit constant name, configured value, dropped/rejected delta; rate-limited per limit |
| `vm.host.memory.pressure` / `vm.host.memory.recovered` | WARN/INFO | machine/run/job IDs, current bytes, limit bytes, ratio, source |
| `vm.host.cpu.throttled` / `vm.host.cpu.recovered` | WARN/INFO | machine/run/job IDs, bounded interval/ratio, source |
| `vm.host.oom_kill` | WARN | machine/run/job IDs, host event delta, source |
| `vm.sparse_disk.pressure` / `vm.sparse_disk.recovered` | WARN/INFO | machine/run/job IDs, allocated bytes, host free bytes |
| `vm.sampler.unavailable` / `vm.sampler.recovered` | WARN/INFO | capability, source, stable reason, sample age |
| `vm.unreachable` / `vm.reachable` | WARN/INFO | machine name, role, run/job IDs, PID, age since last reachable |
| `store.operation.failed` | ERROR | backend, operation, consecutive failures, safe error |
| `store.connection.lost` | ERROR | backend, safe error; the Postgres connection task exiting |
| `storage.pressure` / `storage.recovered` | WARN/INFO | mount, free bytes, free ratio, largest component |
| `github.webhook.processed` | INFO/WARN | event class, delivery ID, dedup/outcome, duration |
| `github.check.updated` | INFO/WARN | run ID, operation, outcome, duration |
| `github.rate_limit.low` | WARN | resource, remaining, limit, reset-in seconds; never the token |
| `debug.session.created` / `debug.session.closed` | INFO/WARN | run/job/session ID, terminal reason, duration |
| `debug.audit.evicted` | WARN | session ID, evicted count, retained cap |
| `telemetry.export.failed` | WARN | signal, failure class, consecutive failures; no endpoint/header |

`job.concurrency.cancelled` logs a **hash** of the concurrency group key, never the key itself: the
group expression is user-controlled and routinely interpolates branch names, PR titles, and inputs.
The hash still correlates all jobs in one group without exporting user data.

Level policy:

- INFO: low-frequency control-plane, pool, and lifecycle transitions.
- WARN: recoverable degradation requiring operator attention.
- ERROR: process/supervisor terminal failure, invariant break, or sustained durability failure.
- DEBUG: successful long-poll/renew/detail paths. Never INFO-log every poll or lease renewal.
- A workflow's own failure is data, not an ERROR about Preloop.

Export control-plane and pool/VM lifecycle logs. Keep workflow step output in its current run-log store.
Keep protocol flow capture isolated. Do not add request/response body logging as a troubleshooting
shortcut.

### Trace policy

Trace synchronous work, not an hours-long workflow:

- HTTP server request spans using matched route and OTel HTTP semantic attributes;
- workflow submission/build, webhook processing, broker claim/acquire;
- SQLite/Postgres operations through the decorator;
- GitHub token/check/API requests;
- image/golden preparation and individual VM provision/register/assign/delete phases;
- cache/artifact operations where they perform storage/network I/O.

A run/job crosses many requests and processes. Correlate those transitions with structured
`run.id`, `job.id`, `request.id`, `runner.id`, `session.id`, and `machine.name` fields on logs/traces.
Do not retain one span for the complete run and do not persist span context in `InnerState` merely to
force a single trace.

Suppress successful health/metrics probes, successful renewals, and empty broker long-polls from
normal trace export. Always record their metrics; trace errors and job/cancellation deliveries. The
reference OpenObserve profile should set a standard parent-based ratio sampler for ordinary HTTP
traffic and document how to raise it temporarily.

## SLOs and alerts

Instrument first, collect a two-week baseline, then ratify thresholds. Initial objectives are starting
points, not promises:

| SLI | Initial objective | Denominator/exclusions |
|---|---|---|
| Control API/broker availability | 99.9% successful | exclude user/auth 4xx and expected empty long-polls |
| Dispatch latency | 99% within 30s | only ready jobs for which compatible capacity exists; pool preparation is reported separately |
| Terminal propagation | 99% within 30s | job terminal to run finalization and configured GitHub check acknowledgement |
| Durable-state writes | no sustained failure over 5m | all store operations; one transient failure warns, sustained failure pages |
| Runner liveness | no deaf/leaked active session beyond configured bound | active sessions only; completed sessions excluded |
| VM telemetry freshness | no stale fast sample beyond three intervals | Preloop-owned active VMs on hosts where the relevant source is supported |
| Scheduled-trigger fidelity | 99% of schedules fire within 60s of their slot | excludes deliberate overlap skips and periods where the service was down |
| Critical-task liveness | no critical `TaskHeartbeat` stale beyond three of its intervals | tasks marked critical in the registry |
| Data retention honesty | zero unreported drops | every `preloop.limit.dropped` increment must have a matching visible condition |

Reference alerts:

1. **Telemetry absent**: no `preloop.service.uptime` for five minutes when the service is expected.
2. **Dispatch stalled with capacity**: oldest claimable ready job exceeds the baseline threshold,
   compatible idle/provisioning capacity exists, and pool preparation is false.
3. **Unclaimable queue**: ready unclaimable jobs persist beyond grace with no preparing/provisioning
   capacity; distinguish no registered runner from label mismatch.
4. **Pool deficit**: desired exceeds idle+busy+building+provisioning while work is queued, or provision
   failures repeat.
5. **Runner deaf/lease stale**: max poll/lease age approaches its configured timeout or reap events occur.
6. **Store failing**: consecutive failures or error rate persists; restart would risk losing live state.
7. **GitHub terminal check pending**: a terminal run lacks successful check propagation beyond 30s.
8. **Debug session stale**: a session remains active beyond its configured/operator-approved lifetime.
9. **Exporter failing**: OTLP was configured but has not succeeded and local drop/failure counts rise.
10. **VM host OOM event**: any increase in host-cgroup `oom_kill`; identify the VM/job from the
    corresponding structured event.
11. **VM memory pressure**: sustained aggregate or top-consumer host memory above 90% of its measured
    limit while busy; baseline before paging because lazy/ballooned mappings differ by platform.
12. **VM CPU throttling**: sustained throttled-period/time ratio while work is queued or running, not
    a one-sample burst.
13. **VM disk pressure**: VM-state filesystem has both low percentage and low absolute free space;
    filesystem free bytes, not summed CoW allocation, is authoritative.
14. **VM sampler stale/unavailable**: active owned VMs exist but the supported fast source is stale for
    three intervals or errors persist.
15. **Background task dead**: `preloop.task.heartbeat.age` for any critical task exceeds three of its
    intervals, or `preloop.task.exited{outcome!="clean"}` increments. This is the generic replacement
    for writing one bespoke alert per loop.
16. **Schedule did not fire**: a registered schedule's slot passed with no `preloop.scheduler.fire`
    increment, or `fire.delay` p99 exceeds the objective. Page separately from dispatch latency: a
    cron that never fires produces no queued job and therefore trips no queue alert.
17. **Concurrency overflow cancelling users' jobs**: `preloop.concurrency.decision{action="cancel_arrival"}`
    increments. This is policy working as designed, so warn rather than page, but it must be visible —
    the user sees an unexplained cancellation.
18. **Data being dropped**: any `preloop.limit.dropped` or `preloop.limit.rejected` increase. Page on
    `MAX_SESSION_AUDIT` eviction specifically; warn on the rest.
19. **GitHub budget exhaustion**: `rate_limit.remaining / rate_limit.limit` below 10%, or
    `github.token.expires_in` below twice the refresh interval. Both precede a total integration
    outage by minutes and are currently invisible.
20. **Storage capacity**: `preloop.storage.fs.available{mount="state_dir"}` low in both ratio and
    absolute terms, or a single `preloop.storage.bytes` component growing monotonically across a full
    GC interval.
21. **VM unreachable**: any VM in `runtime_state=unreachable` for more than one slow-sample interval.
    A wedged golden silently poisons every fork taken from it.

Do not page on workflow failure rate. User code fails legitimately. VM-attributable host
CPU/memory/disk pressure is part of the built-in application telemetry minimum. Whole-node load,
unrelated processes, network interfaces, kernel health, and hardware remain optional collector
signals.

## OpenObserve evaluation

### Verdict

Use OpenObserve as the documented, optional “batteries available” backend. Do not make it a runtime
dependency, do not embed/link it, and do not shape Preloop signals around OpenObserve-specific fields
or APIs.

| Criterion | Assessment | Decision |
|---|---|---|
| Minimal local deployment | Good: one native binary or container in single-node mode | Provide opt-in pinned compose/binary instructions, never auto-start it |
| Unified signals | Good: OTLP/HTTP and OTLP/gRPC ingest logs, metrics, and traces | Preloop implements OTLP/HTTP first to avoid tonic/gRPC dependency |
| Dashboards/alerts | Good: dashboards plus scheduled/realtime/composite standard alerts | Ship importable reference assets after signal names stabilize |
| Storage | Good for small installs, but it is another stateful data volume | Short default retention, persistent volume, backup guidance |
| HA | Poor fit for “minimal”: Kubernetes, object storage, PostgreSQL, NATS, and five roles | Do not ship an HA profile; link upstream docs |
| Security | Basic private deployment is usable; SSO, RBAC, and audit trail are enterprise features | Bind UI to loopback/private network and front it with operator auth if shared |
| Licensing | OSS repository is AGPL-3.0 | Keep it a separate process; do not vendor or relicense; legal review before distributing assets/binary bundles |
| Failure isolation | Co-hosting can compete with runner VMs for CPU/RAM/disk | Opt-in, resource-capped; prefer a separate host/volume for durable self-hosting |

OpenObserve's own documentation says single-node local mode uses SQLite and local disk (or object
storage), while HA requires Kubernetes/Helm, object storage, PostgreSQL, NATS, and Router, Ingester,
Compactor, Querier, and Scheduler roles. Its storage guide says losing SQLite metadata makes the
installation inoperable. The optional profile must persist and back up both metadata and stream data;
for stronger durability, use object storage and a separately protected metadata store according to
upstream guidance.

Do not put the OpenObserve UI on the public webhook origin. The OSS/enterprise feature split makes a
private network or an external auth proxy the conservative default.

### Deployment profiles

1. **Default local**: no backend, pretty logs on a TTY, JSON/compact logs when noninteractive,
   authenticated `/metrics`, direct status/health. Zero external process and zero export network.
2. **Local enhanced**: one OpenObserve binary/container bound to loopback with persistent local volume,
   short retention, resource caps, and direct OTLP/HTTP. Intended for debugging and baselining.
3. **Self-hosted single node**: Preloop and OpenObserve may share a host only at measured low volume;
   use separate persistent volumes, explicit CPU/memory/disk budgets, private networking, backups, and
   preferably object storage for OpenObserve stream data.
4. **Existing observability estate**: point the same OTLP output at the operator's backend. No
   OpenObserve assets required.
5. **Advanced host telemetry**: optional OpenTelemetry Collector receives Preloop OTLP and adds
   whole-node CPU/memory/filesystem/network/kernel metrics unrelated to a specific Preloop VM, then
   forwards. Built-in VM-attributable metrics do not require the collector.
6. **OpenObserve HA**: operator-owned upstream deployment only. Preloop documents compatibility but
   does not provision or support its dependencies.

### Primary sources

- Architecture and deployment modes: <https://openobserve.ai/docs/architecture/>
- Single-node binary/container quickstart: <https://openobserve.ai/docs/getting-started/>
- OTLP logs/metrics/traces over HTTP and gRPC:
  <https://openobserve.ai/docs/ingestion/logs/otlp/>
- Trace endpoint: <https://openobserve.ai/docs/ingestion/traces/opentelemetry/>
- Prometheus remote-write support: <https://openobserve.ai/docs/ingestion/metrics/prometheus/>
- Dashboards: <https://openobserve.ai/docs/user-guide/analytics/dashboards/dashboards-in-openobserve/>
- Alerts: <https://openobserve.ai/docs/user-guide/analytics/alerts/>
- Storage and SQLite metadata warning:
  <https://openobserve.ai/docs/administration/maintenance/storage-management/storage/>
- Enterprise-only SSO/RBAC/audit features: <https://openobserve.ai/docs/features/enterprise/>
- OpenObserve repository license: <https://github.com/openobserve/openobserve/blob/main/LICENSE>
- OTel Rust exporter guidance (Collector recommended for larger production topologies):
  <https://opentelemetry.io/docs/languages/rust/exporters/>
- SmolVM minimum-version (`v1.8.1`, `versions.toml:8`) JSON status contract — `machine_status_json`
  at `src/cli/vm_common.rs:2360` and `status_vm_json` at `:2416`, shared with `machine ls --json`:
  <https://github.com/smol-machines/smolvm/blob/v1.8.1/src/cli/vm_common.rs>
- SmolVM `machine status --json` / `machine ls --json` flags and the public `machine data-dir`
  command (`src/cli/machine.rs:365, 3783, 3857, 4290`):
  <https://github.com/smol-machines/smolvm/blob/v1.8.1/src/cli/machine.rs>
- SmolVM vsock state probe defining the `Unreachable` state (`src/agent/state_probe.rs:6-76`):
  <https://github.com/smol-machines/smolvm/blob/v1.8.1/src/agent/state_probe.rs>
- SmolVM per-VMM `vm-<pid>` cgroup-v2 leaf creation (`src/process.rs:375-393`, `place_in_cgroup`),
  the upstream basis for Preloop's Linux VM metrics:
  <https://github.com/smol-machines/smolvm/blob/v1.8.1/src/process.rs>
- `sysinfo` 0.39.6 process API (`memory`, `start_time`, `cpu_usage`, and
  `accumulated_cpu_time`) and feature controls:
  <https://docs.rs/sysinfo/0.39.6/sysinfo/struct.Process.html>,
  <https://docs.rs/crate/sysinfo/0.39.6/features>

## Configuration contract

Add only one Preloop-specific setting:

- `PRELOOP_LOG_FORMAT=auto|pretty|json`; `auto` means human-readable on a TTY and structured JSON when
  noninteractive, without ANSI in journald/files.

Honor standard variables rather than inventing backend-specific ones:

- `RUST_LOG`
- `OTEL_SERVICE_NAME`
- `OTEL_RESOURCE_ATTRIBUTES`
- `OTEL_EXPORTER_OTLP_ENDPOINT` and signal-specific endpoint variants
- `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`
- `OTEL_EXPORTER_OTLP_HEADERS` and signal-specific header variants
- `OTEL_EXPORTER_OTLP_TIMEOUT`
- `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER`
- `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`
- standard batch/log/metric interval variables supported by the selected OTel Rust release

Export is disabled unless an OTLP endpoint is explicitly present. `none` disables the corresponding
signal. Reject unsupported protocol values from the telemetry pipeline with a sanitized status
condition while continuing the control plane. Never print endpoints containing userinfo/query data or
any header values.

Absent-means-disabled is a documented deviation from the OTel default of `http://localhost:4318`
(invariant 4). State it in `docs/observability.md` next to the variable list, because an operator who
knows the spec will otherwise assume a local collector is being contacted.

For OpenObserve, docs should show placeholder-only examples using the generic base endpoint without a
trailing slash and signal-specific authorization/stream headers. Credentials belong in the protected
systemd environment/credential mechanism already documented by Preloop, not committed compose files.

## Commands the executor will need

| Purpose | Command | Expected on success |
|---|---|---|
| Format check | `cargo fmt --all --check` | exit 0 |
| Workspace check | `cargo check --locked --workspace` | exit 0 |
| Observability crate tests | `cargo test --locked -p preloop-observability` | all pass |
| Server observability tests | `cargo test --locked -p preloop-runner-server observability -- --nocapture` | all matching tests pass |
| Pool observability tests | `cargo test --locked -p preloop-orchestrator observability -- --nocapture` | all matching tests pass |
| VM telemetry tests | `cargo test --locked -p preloop-vm observability -- --nocapture` | cgroup/process/disk fixtures and capability cases pass |
| CLI status tests | `cargo test --locked -p preloop-cli status -- --nocapture` | all matching tests pass |
| Structural security rules | `just sg-scan-strict` | exit 0 |
| Full local gate | `just test-ci` | ends with `CI: all checks passed` |
| Official-server protocol check | `just conform-server-light` | no protocol diff |
| Real runner smoke | `just dogfood` | workflow reaches expected terminal success |
| OpenObserve integration | `just observability-openobserve-smoke` | one correlated log, metric, and trace query succeeds; assets import |

All recipes above except `observability-openobserve-smoke` exist in the `justfile` at `673bdfa0`
(`test-ci` at line 55, `sg-scan-strict` at 74, `dogfood` at 79, `serve` at 94, `conform-server-light`
at 121). `observability-openobserve-smoke` is created by step 6. Note that `just test-ci` runs
`fmt-check clippy zizmor test`, so the zizmor workflow gate applies to any CI workflow this plan adds.

Match existing conventions: `anyhow` at binary boundaries, `ApiError` in HTTP handlers,
`thiserror` in libraries, `Arc<Mutex<...>>` plus atomics/Notify for shared state, and
`SecretString::expose()` only at protocol boundaries. Never await or export while holding the global
state mutex.

Toolchain and existing dependencies verified at `673bdfa0`:

- `rust-toolchain.toml` pins channel `1.97`; workspace `rust-version = "1.97"`.
- `tracing = "0.1"`, `tracing-subscriber = "0.3"` with `["env-filter", "json"]` already present.
- `reqwest = "0.12"` with `["json", "rustls-tls", "stream"]` — reuse this exact client for OTLP
  `http/protobuf` through `opentelemetry_http::HttpClient` rather than adding a second HTTP stack.
- No `opentelemetry*`, `prometheus`, or `sysinfo` dependency exists yet; all are net-new.
- The pinned versions named in step 2 were re-checked against crates.io on 2026-08-20 and are still
  the current stable releases (`opentelemetry*` 0.32.x, `tracing-opentelemetry` 0.33.0,
  `prometheus` 0.14.0, `sysinfo` 0.39.6). `opentelemetry-prometheus` 0.32.0 shipped the same day as
  the core 0.32.0 release, so the historical lag that made that crate risky does not apply to this
  release line. Re-verify before implementing; if it has fallen behind again, that is a STOP.

## Scope

**In scope**:

- `Cargo.toml`, `Cargo.lock`, `versions.toml` (only if the SmolVM floor must move)
- new `crates/preloop-observability/**`
- `crates/preloop-cli/src/main.rs`, `crates/preloop-cli/src/server_install.rs`, its Cargo manifest/tests
- `crates/preloop-runner-server/src/{main.rs,lib.rs,bootstrap.rs,routes.rs,runs.rs,state.rs,store.rs,store_pg.rs,broker.rs,runner_lifecycle.rs,scheduler.rs,concurrency.rs,live_logs.rs,snapshots.rs,github.rs,github_app.rs,github_pr.rs,github_push.rs,dispatch_auth.rs,oidc.rs,actions.rs,remote_workflows.rs,debug_sessions.rs,cache_artifacts.rs,artifact_twirp.rs,results_twirp.rs,blob_store.rs,distributed_task.rs,openapi.rs,lib_tests.rs}` and Cargo manifest
- `crates/preloop-orchestrator/src/lib.rs` and its tests/Cargo manifest
- `crates/preloop-vm/src/lib.rs` and its tests/Cargo manifest
- `crates/preloop-runner/src/main.rs` only for safe local logging initialization
- `docs/{architecture.md,self-hosting.md,cli_reference.md}` — note the file is `cli_reference.md`;
  there is no `docs/cli.md` — plus new `docs/observability.md`
- `contrib/openobserve/**` as optional, pinned deployment/dashboard/alert assets
- `scripts/openobserve-observability-smoke.sh`, `justfile`
- one structural rule under `rules/` preventing sensitive tracing fields, matching the ast-grep YAML
  format of the three existing rules (`no-expose-in-loop.yml`, `no-inline-masking.yml`,
  `no-raw-secret-replace.yml`)
- `CHANGELOG.md`, `plans/README.md`

**Out of scope**:

- any official runner protocol body/status/header change;
- exporting directly from guest runners;
- exporting workflow step stdout/stderr, annotations, environment, or secrets;
- changing protocol flow recording into general telemetry;
- installing or starting OpenObserve automatically from `preloop serve`;
- bundling or modifying the OpenObserve binary/image;
- OpenObserve HA provisioning, multi-server Preloop, or a distributed state bus;
- host eBPF, automatic kernel/container instrumentation, or a required OTel Collector;
- periodic commands inside guests or changes to the official runner for telemetry;
- guest-kernel memory/process/OOM/filesystem/network metrics until SmolVM provides a stable
  out-of-band source;
- per-machine metric labels, or fabricated network/block-I/O zeroes where no stable source exists;
- retrying business operations because telemetry failed;
- a new web UI inside Preloop; the direct CLI/API and reference backend are the deliverables.

## Git workflow

- Branch: `advisor/002-observability-strategy` unless the operator supplies another branch.
- One commit/PR per implementation step below. Keep protocol instrumentation changes separate from
  reference-backend assets.
- Match the repository's imperative commit-message style observed in current history.
- Do not push or open a PR unless instructed.

## Implementation steps

### Step 1: Freeze the signal/security contract and make existing logs safe

Targets:

- add `docs/internal/observability.md` (internal contract, kept in `docs/internal/` which is
  `.gitignore`'d; the public `docs/observability.md` will be a redacted subset later) containing the
  architecture, cardinality rules, log classes, status
  semantics, signal catalog, SLO definitions, deployment profiles, and runbook links from this plan;
- re-run the grep in "Logging is local, duplicated, and not export-ready" and fix **every** hit, not
  just the anchors below — three of the original four moved between `84d92cfd` and `673bdfa0`, so a
  fixed list rots. At `673bdfa0` the hits are `artifact_twirp.rs:94`, `results_twirp.rs:713`,
  `blob_store.rs:63, 78, 95, 113, 121, 127, 131`, and `distributed_task.rs:327`;
- preserve useful fields: operation/kind, byte count, request ID, parsed terminal result, and duration;
- add a structural `sg` rule rejecting INFO/WARN/ERROR tracing fields named `token`, `authorization`,
  `cookie`, `headers`, `body`, `payload`, or `signed_url` unless the source is the explicitly excluded
  `recording.rs` conformance path;
- audit every non-DEBUG tracing macro in server/orchestrator code for raw URLs, query strings,
  headers, bodies, secrets, and opaque Debug dumps;
- document flow capture as sensitive local data and verify its permissions at creation.

Do not “redact” tokens by logging a prefix, suffix, length, or stable hash; those are still unnecessary
capability correlators. Log the operation and outcome instead.

**Tests**:

- behavioral logging test with sentinel token/body/header values captured by an in-memory subscriber;
  assert no sentinel appears while safe operation fields remain;
- flow-capture test remains local and proves its file mode is 0600 on Unix;
- structural rule fixture accepts safe lifecycle fields and rejects a token/raw-body field.

**Verify**:

```sh
just sg-scan-strict
cargo test --locked -p preloop-runner-server observability_log_safety -- --nocapture
```

Expected: exit 0; sentinels absent; flow capture behavior unchanged.

### Step 2: Add the observability crate and unify process initialization

Targets:

- add workspace-member `crates/preloop-observability` with the API described above;
- pin the current compatible release line: `opentelemetry`, `opentelemetry_sdk`,
  `opentelemetry-otlp`, `opentelemetry-prometheus`, `opentelemetry-appender-tracing`, and
  `opentelemetry-semantic-conventions` at `0.32`, `tracing-opentelemetry` at `0.33`, and
  `prometheus` at `0.14`;
- disable default exporter features; enable OTLP `http-proto` plus `trace`, `metrics`, and `logs`
  explicitly (the `http-proto` feature alone does not enable logs), using the existing
  rustls/reqwest stack through an `opentelemetry_http::HttpClient` adapter rather than pulling the
  tonic stack or a second reqwest major/minor line;
- provide no-op, Prometheus-only, and Prometheus+OTLP initialization paths;
- add the `TaskHeartbeat` registry and `LimitRegistry` described in "New crate"; both must be usable
  from a no-op handle so `preloop-orchestrator` and `preloop-vm` can register without an SDK;
- install stderr fmt/JSON, OTel trace layer, and OTel log bridge once in `preloop` and standalone
  `preloop-server`; initialize the guest Rust runner with stderr only;
- parse `PRELOOP_LOG_FORMAT`; preserve `RUST_LOG=info` fallback and give the standalone server the
  same fallback the CLI has — `preloop-runner-server/src/main.rs:78-80` currently uses
  `EnvFilter::from_default_env()`, so an unset `RUST_LOG` silently disables its logging;
- create OTel resources and per-process instance ID;
- add bounded queues/timeouts and a two-second explicit shutdown path around command completion;
- sanitize all init/export errors and expose telemetry health through the shared handle.

Do not make OTel globals the test seam. Constructors accept an explicit handle; tests use no-op or
recording exporters with scoped subscribers.

**Tests**:

- no endpoint/env means no DNS/socket attempt and no exporter worker;
- malformed/down endpoint does not fail initialization or delay a synthetic request;
- queue capacity and shutdown timeout are bounded;
- pretty/JSON/auto formats preserve structured fields and omit ANSI when noninteractive;
- config `Debug` never reveals OTLP headers or credential-bearing endpoint components;
- one event inside a span reaches the recording log exporter with trace/span correlation.
- a registered heartbeat that stops beating becomes stale and a dropped `HeartbeatHandle`
  deregisters; a registry with a critical stale entry reports not-ready;
- `LimitRegistry` counts drops and rejects separately and reports registered limits with zero counts.

**Verify**:

```sh
cargo test --locked -p preloop-observability
cargo check --locked -p preloop-cli -p preloop-runner-server -p preloop-runner
```

Expected: all tests pass and all three binaries compile.

### Step 3: Add truthful liveness, readiness, aggregate status, and CLI output

Targets:

- add `status.rs` in the observability crate for neutral DTOs/handles and in server for state sampling;
- add a five-second sampler in `bootstrap.rs` with a heartbeat and bounded five-exemplar conditions;
- register a `TaskHeartbeat` for every long-lived task in the fifteen-task inventory, without changing
  any task's cadence or behavior; mark the state sampler, reaper, scheduler scan, and (Postgres only)
  the store connection task critical;
- wire the consolidated `PoolStatus` handle through `ServerConfig`, `RunnerPoolConfig`, and CLI
  construction, **removing** the four ad-hoc handles (`pending_jobs`, `preparing_signal`,
  `next_job_runs_on`, `pending_registrations`) rather than adding a fifth beside them;
- register every cap in the limits table with `LimitRegistry` and report them in status;
- add `/readyz` and authenticated `/api/v1/status`; make authenticated `/metrics` available from the
  provider registry;
- keep `/healthz` lock/dependency-free and return 503 during shutdown;
- update OpenAPI for all operator routes and native bearer requirements;
- replace `Command::Status` with `Status(StatusArgs)`, implement exact JSON plus sectional human output,
  and preserve recent runs; copy the `#[arg(long)] json: bool` shape from `PlanArgs`
  (`preloop-cli/src/main.rs:706-707`);
- change `wait_for_engine_socket` (`preloop-cli/src/main.rs:1458-1475`) to probe `/readyz` and to
  surface the last reason code on timeout.

The sampler computes claimability using existing runner label matching. It distinguishes:

- claimable now;
- temporarily unclaimable because pool is preparing/provisioning;
- no registered runner;
- registered runners exist but labels do not match.

It must not call the external backend and must remain responsive when OTLP is down.

**Tests** (follow the existing router/auth test pattern in `lib_tests.rs`, which builds state with
`AppState::new(temp.path().to_path_buf()).await.unwrap()` and the `app(...)` helper):

- health is public/shallow; ready returns 503 with the stale task's registry name as reason, and a
  stale **non-critical** task does not affect readiness;
- status and metrics reject missing/invalid native bearer and accept valid bearer;
- every queue class and pool mode appears correctly;
- concurrency, scheduler, storage, limits, and tasks blocks render with correct values, and `limits`
  includes registered caps whose counters are zero;
- a `queue: max` overflow at `QUEUE_MAX_PENDING` produces the `concurrency_queue_overflow` condition;
- claimability distinguishes absence, label mismatch, preparing, provisioning, and compatible runner;
- VM fleet totals, source capabilities, sample age, and the deterministic five-entry top-consumer
  bound render in status/CLI without creating metric labels;
- snapshot fallback remains available when a deliberately held state lock prevents a fresh sample;
- JSON schema version is fixed; CLI JSON is byte-for-byte valid endpoint JSON;
- human output names an actionable cause for a stuck job.

**Verify**:

```sh
cargo test --locked -p preloop-runner-server status_observability -- --nocapture
cargo test --locked -p preloop-cli status -- --nocapture
```

Expected: all matching tests pass; unauthorized cases return 401; readiness cases return 200/503 as specified.

### Step 4: Instrument HTTP, runs/jobs, dispatch, runners, and the store

Targets:

- replace default `TraceLayer` with custom matched-route/surface instrumentation;
- classify routes into finite surfaces covering all thirteen path families actually served:
  `native` (`/api/v1`), `runner` (`/_apis`, `/runner`, `/runner/server`), `broker` (`/broker`),
  `results` (`/twirp`, `/twirp-blob`), `webhook`, `git` (`/snapshots`, `/repos`), `oidc`
  (`/oidc`, `/.well-known`), `live_logs` (`/ws/live-logs`), `public` (`/healthz`, `/readyz`),
  `test` (`/internal/test`), and `unknown`; `unknown` is a constant label, never a raw path;
- exclude the `live_logs` surface from `http.server.request.duration` and instrument it with
  `preloop.livelog.connections` plus a close-reason counter instead;
- create exact HTTP metrics and safe spans without headers/body/query;
- instrument run/job terminal transitions exactly once using central transition/event boundaries;
- record queue wait at claim or terminal-unclaimed failure;
- instrument poll outcomes, session transitions, renew errors, lease expiry, no-matching-runner,
  deaf-runner reaping, and restart-orphan reconciliation;
- wrap the private `Store` trait in `store.rs` with `InstrumentedStore`, including all methods and
  backend/outcome; preserve every return/error and best-effort persistence rule. Wrap the
  `Arc<dyn Store>` the factory at `store.rs:261-267` returns; because the trait is `#[async_trait]`
  and already consumed as `Arc<dyn Store>` (`state.rs:360`), no call site changes;
- instrument the Postgres connection task at `store_pg.rs:95-103` with a heartbeat plus
  `preloop.store.connection.up`, so connection death is visible before the first failed write;
- instrument concurrency-group decisions at `concurrency.rs::apply_queue_mode` — every
  `park`/`cancel_pending`/`cancel_arrival`/`admit`, with `cancel_arrival` also recording a
  `LimitRegistry` reject against `QUEUE_MAX_PENDING`;
- instrument the scheduler scan: fire, late fire, overlap skip, and registered-schedule count;
- feed gauges only from cached sampler values.

Do not count an emitted duplicate status event as a second completion. Add transition guards or record
at the state mutation that proves old-state to terminal-state movement.

**Tests**:

- route template is `/api/v1/runs/:run_id`, never a concrete ID/query;
- 1,000 unique HTTP/run/job/runner IDs do not increase metric series beyond the fixed bound;
- a claim emits one wait observation and one claim outcome;
- successful completion, timeout, no runner, lease expiry, deaf runner, and startup orphan each emit
  exactly one bounded terminal reason;
- each `Store` method records one duration/outcome while preserving success/error values;
- all seven `Store` methods are covered — a new trait method without instrumentation must fail a test,
  not pass silently;
- killing the Postgres connection task flips `store.connection.up` and emits `store.connection.lost`;
- `queue: single`, `queue: max` under the cap, and `queue: max` at the cap each emit exactly one
  bounded decision outcome, and the overflow case increments the limit reject counter;
- slow/failing recording exporter does not measurably serialize handler completion; use paused time or
  synchronization, not a flaky wall-clock threshold.

**Verify**:

```sh
cargo test --locked -p preloop-runner-server observability -- --nocapture
just conform-server-light
```

Expected: tests pass and official runner flow comparison has no diff.

### Step 5: Instrument pool, VM resources, GitHub, webhook, cache/artifacts, and debug sessions

Targets:

- move pool idle/building/provisioning/busy/paused/preparing counters behind the consolidated
  `PoolStatus` handle from step 3 and delete the four ad-hoc `Option<Arc<…>>` fields;
- update state through RAII guards so cancellation/error cannot leak a count;
- instrument golden/artifact preparation, provision phases, registration, assignment, pause/resume,
  delete/replacement, and supervisor exit with stable outcomes/reasons;
- replace the substring matching in `SmolVmProvider::status` (`preloop-vm/src/lib.rs:1018-1043`) with
  typed inspection: `machine status --json` for one machine on a lifecycle path, and extend the
  existing `machine ls --json` parser in `SmolVmProvider::list`
  (`crates/preloop-vm/src/lib.rs:1045-1060`) into the
  fleet-wide `VmRuntimeInfo` source used by the slow sampler. Resolve `machine data-dir` only when a
  disk path is needed;
- add `MachineState::Unreachable` and map SmolVM's vsock-probed `Unreachable` state to it end to end
  (metric attribute, status condition `vm_unreachable`, `vm.unreachable` event). Audit every existing
  `MachineState` match arm — adding a variant is a breaking change for the pool's decision logic, and
  treating `Unreachable` as `Unknown` would silently keep forking from a wedged golden;
- add workspace dependency
  `sysinfo = { version = "0.39.6", default-features = false, features = ["system"] }` to
  `preloop-vm`; retain one process snapshot and refresh only registered VMM PIDs rather than scanning
  every process or enabling unrelated component, disk, network, and user collectors;
- add the shared VM registry plus one five-second host resource sampler and one 60-second disk sampler;
  register runner/golden role and assignment on create/fork/adopt, preserve paused debug VMs, and
  unregister only after confirmed deletion;
- on Linux, read the validated `vm-<pid>` cgroup-v2 CPU/memory/PID files under the root
  `init_vm_cgroup_delegation` (`preloop-vm/src/lib.rs:1360` ff.) already delegates. The leaf is created
  by SmolVM's supervisor, not by Preloop, so a missing leaf degrades to the process fallback and
  records `capability=false`; it is never an error. On macOS/cgroup-unavailable hosts, read validated
  process CPU/RSS; on every platform, sample known sparse-file allocated blocks and
  state-filesystem free bytes;
- compute counter deltas and CPU-core rate without attributing a reused PID; carry unsupported/stale
  fields as unavailable rather than zero;
- aggregate VM metrics by bounded role/activity/source and publish five authenticated top consumers;
- emit rate-limited pressure/recovery events for host memory, CPU throttling, host OOM, sparse disk,
  and sampler health;
- instrument GitHub App token/check/API calls at centralized request boundaries, never token values;
- parse `x-ratelimit-limit`/`x-ratelimit-remaining`/`x-ratelimit-reset` from every GitHub response and
  publish the budget gauges; track installation-token expiry and the `dispatch_token_cache`
  /`dispatch_actor_cache`/`action_sha_cache` hit rates (`dispatch_auth.rs:46, 49`);
- track pending terminal check updates and propagation delay for status/SLOs;
- instrument webhook processing/dedup with event class and outcome, delivery ID only in logs/traces;
- instrument cache/artifact operations and bytes without key/name/token labels;
- instrument snapshot/git object serving (`snapshots.rs`), including `ObjectCache` hit/miss/evict and
  the GC pass at `snapshots.rs:1896`;
- add the storage sampler: per-component bytes for database, cache, artifacts, run logs, snapshots,
  and VM images, plus `statvfs` free bytes for the state-dir and SmolVM-data mounts. Run it on the
  60-second cadence; a recursive directory walk must never run on the fast path or under a state lock;
- instrument debug session create/pause/resume/detach/close/expire/crash and age;
- register the four `debug_sessions.rs` ring caps with `LimitRegistry` and emit `debug.audit.evicted`
  when `MAX_SESSION_AUDIT` evicts;
- register the `live_logs.rs` per-job cap and count both tail-drops and oversized-batch rejects;
- add short synchronous spans around these operations; no workflow-lifetime span.

Pool status is authoritative from transitions, while the periodic server sampler merges it with queue
and runner state. Avoid double-maintaining separate pool counters solely for metrics.

**Tests**:

- each pool state transition balances under success, error, cancellation, and paused-debug paths;
- SmolVM JSON parsing covers every state **including `Unreachable`**, optional PID/resource field,
  malformed output, missing machine, and a fixture captured from the supported floor `v1.8.1`;
- `machine ls --json` fixture yields one `VmRuntimeInfo` per machine and the fast sampler spawns zero
  subprocesses;
- an `Unreachable` golden is not used as a fork source;
- cgroup parser fixtures cover CPU units/deltas, throttle counters, `max` limits, memory events, PID
  events, missing controller files, permission errors, counter reset, process exit, and PID reuse;
- a missing `vm-<pid>` leaf falls back to the process source and reports the capability false rather
  than erroring or emitting zero;
- process fallback tests use injected process snapshots and prove unsupported cgroup-only values are
  absent, not zero;
- sparse-file tests compare allocated blocks rather than logical length, count shared goldens once,
  enforce the slow cadence, and treat filesystem free space as authoritative;
- cancellation, pause, adoption, replacement, and deletion keep the VM registry balanced and never
  sample a deleted/reused process;
- 1,000 ephemeral machine names produce a constant metric series set while status retains at most five
  deterministic top consumers;
- repeated provision failure produces status condition and metrics but does not create a log storm;
- GitHub/check failure uses bounded outcome and retains safe correlation IDs;
- rate-limit headers populate the budget gauges, and a response missing them leaves the previous
  values with an explicit `observed_at`, never zero;
- the storage sampler reports per-component bytes and free space from a temp-dir fixture and does not
  block the fast sampler;
- live-log tail-drop and debug-session ring eviction each increment `preloop.limit.dropped` with the
  correct constant name;
- cache/artifact unique keys and webhook delivery IDs never become metric labels;
- debug session expiry/crash produces the correct terminal reason.

**Verify**:

```sh
cargo test --locked -p preloop-vm observability -- --nocapture
cargo test --locked -p preloop-orchestrator observability -- --nocapture
cargo test --locked -p preloop-runner-server observability -- --nocapture
```

Expected: all matching tests pass with balanced gauges and bounded labels.

### Step 6: Add the optional OpenObserve reference profile and assets

Targets:

- add `contrib/openobserve/compose.yml` pinned to a reviewed immutable OpenObserve version/digest;
- bind the UI/API to loopback by default, use a persistent data volume, healthcheck, explicit CPU/memory
  limits, short retention, and placeholder-only credentials sourced outside version control;
- do not vendor the binary/image; include upstream license/source notices and a legal-review note;
- add six importable dashboards: overview, scheduling/queue (including cron schedules and
  concurrency-group contention), runners/pool, VM host resources, dependencies (GitHub budget, store,
  storage capacity) and telemetry, and a "limits and background tasks" board showing every registered
  cap and heartbeat;
- add alert definitions matching the catalog above, with baseline placeholders where thresholds require
  measured data;
- add `docs/observability.md` setup for native binary, container, direct OTLP/HTTP, existing backend,
  optional Collector, backup/retention, private access, and troubleshooting;
- add a smoke script/just recipe that starts the pinned profile, starts Preloop with sentinel-safe OTLP
  config, performs health/status and a small workflow, queries OpenObserve for at least one log,
  metric, and trace sharing expected resource/correlation fields, exercises CPU/memory/disk activity
  in one VM, imports assets, and tears down.

Before choosing resource limits, measure idle and ingest/query use on the target host. OpenObserve's
querier caching can consume substantial memory; do not guess a cap that starves the VM pool. Record the
measured default and explain how to change it.

**Verify**:

```sh
just observability-openobserve-smoke
```

Expected: the pinned single-node service becomes healthy; Preloop continues working; one safe log,
one domain metric, VM CPU/memory/disk series, and one trace are queryable; dashboard/alert assets
import; neither logs nor query results contain sentinel secrets.

### Step 7: Baseline, ratify alerts, document the incident workflow, and close the loop

Targets:

- run local/self-hosted telemetry for two representative weeks or an agreed workload-equivalent soak;
- record series count, logs/day, sampled spans/day, exporter drops, VM sampler overhead, VM CPU/RSS
  correlation, sparse/CoW disk-accounting behavior, OpenObserve CPU/RAM/disk growth, and query latency;
- tune histogram buckets, sampling, retention, and alert thresholds based on evidence without changing
  signal names casually;
- document runbooks for queue stall, pool deficit, VM host memory/throttling/OOM/disk pressure, stale
  VM sampling, deaf runner, store failure, GitHub check lag, telemetry failure, and OpenObserve
  disk/metadata recovery;
- integrate direct diagnosis: every alert links first to `preloop status --json`, then dashboard/log/trace
  queries, then existing `preloop debug` for a failed job;
- update architecture, self-hosting, CLI docs and changelog;
- run the complete compatibility and dogfood gates.

**Verify**:

```sh
just test-ci
just conform-server-light
just dogfood
```

Expected: all gates pass; dogfood completes with official-runner protocol behavior unchanged; the
status snapshot and reference dashboards explain every observed transition.

## Test plan

Permanent tests must defend observable contracts:

1. **Disabled path**: no OTel endpoint creates no network work; status/metrics/logging remain usable.
2. **Failure isolation**: malformed endpoint, unavailable backend, slow exporter, full queue, and flush
   timeout never alter API result or workflow state.
3. **Security**: sentinel tokens, headers, bodies, signed URLs, workflow output, and flow-capture bytes
   never enter ordinary logs/traces/metrics.
4. **Cardinality**: 1,000 unique user/domain identifiers produce a constant bounded metric series set.
5. **HTTP semantics**: matched templates and finite surfaces only; query values absent.
6. **Status**: all queue/capacity/store/GitHub/debug/telemetry states and conditions, with auth and stale
   snapshot behavior.
7. **Lifecycle exactness**: accepted/ready/claimed/completed/requeued/reaped/reconciled counters and
   durations record once under success, error, retry, cancellation, and restart paths.
8. **Pool balance**: no gauge leak under every early return/cancellation.
9. **VM sampling**: minimum-version JSON fixtures, cgroup/process source fallback, PID reuse/counter
   reset, missing capabilities, sparse allocation, filesystem pressure, cadence, registry lifecycle,
   top-consumer bound, and sampler overhead.
10. **VM semantics**: tests and docs never present host RSS/PIDs/OOM as guest memory/processes/OOM and
    never emit unsupported network/block-I/O metrics as zero.
11. **Shutdown**: exporters flush within the bound and process exits if backend is hung.
12. **Backend E2E**: pinned OpenObserve ingests/query-correlates all three signals, VM resource series,
    and imports assets.
13. **Protocol fidelity**: committed conformance flows and real official runner remain byte/behavior
    compatible.
14. **Limit honesty**: every constant registered with `LimitRegistry` has a test that drives it past
    its cap and asserts the drop/reject counter moved. A cap with no such test is an unproven claim.
15. **Task liveness**: a task that panics, returns early, or stops beating is reported; a dropped
    handle deregisters; a stale non-critical task does not fail readiness.
16. **Scheduler**: a schedule that fires, one skipped for overlap, and one whose scan task is dead
    produce distinct, correct signals.
17. **GitHub budget**: rate-limit headers, a response lacking them, and an expiring installation token
    each produce the right gauge/condition without exporting credential material.

Avoid source-text unit tests, sleeps, and exact wall-clock assertions. Use in-memory exporters,
paused Tokio time, barriers, real router requests, and Prometheus registry gathering.

## Done criteria

All must hold:

- [ ] `preloop status` explains queue, capacity, runner freshness, store, GitHub, debug, and telemetry state.
- [ ] `preloop status --json` returns the versioned authenticated snapshot with no prose.
- [ ] `/healthz`, `/readyz`, `/api/v1/status`, and `/metrics` have the specified auth/semantics.
- [ ] No backend is contacted when OTLP endpoint configuration is absent.
- [ ] OTLP failure cannot block or fail a request/workflow; queues and shutdown are bounded.
- [ ] Existing known capability-token/raw-body logs are removed and the structural guard passes.
- [ ] Workflow output and flow recordings are absent from ordinary telemetry.
- [ ] Metric labels pass the 1,000-identifier cardinality/sentinel test.
- [ ] Pool, queue, runner, store, GitHub, cache/artifact, and debug lifecycle signals exist and are exact.
- [ ] Every cap in the limits table is registered, reported in `/api/v1/status` even at zero, and
      counted when exceeded; no code path discards data without a counter.
- [ ] Every long-lived task in the fifteen-task inventory registers a heartbeat; the four critical
      tasks gate `/readyz` and the rest are visible in status.
- [ ] Scheduled workflows report fire, late fire, overlap skip, and scan-task liveness.
- [ ] Concurrency-group contention and `queue: max` overflow cancellation are visible with a hashed —
      never raw — group key.
- [ ] GitHub rate-limit budget, installation-token expiry, and auth-cache hit rate are reported.
- [ ] Per-component persistent storage bytes and state-dir free space are reported; Plan 001 emits
      through `preloop.storage.*` rather than a parallel family.
- [ ] The four ad-hoc `RunnerPoolConfig` shared handles are gone, replaced by one `PoolStatus`.
- [ ] `MachineState::Unreachable` exists, is honored by pool fork decisions, and surfaces as a condition.
- [ ] Active Preloop VMs expose bounded aggregate configured capacity, host CPU, host memory,
      Linux throttling/events/PIDs when supported, sparse allocated disk, filesystem free space, and
      sampler freshness without running a guest command.
- [ ] `/api/v1/status` reports VM capability gaps and at most five correlated top consumers; unsupported
      metrics are absent rather than zero.
- [ ] OpenObserve remains optional, pinned, private-by-default, resource-capped, and separately licensed.
- [ ] Six dashboards, including VM host resources and limits/tasks, and initial alerts import and query
      real emitted fields.
- [ ] `cargo fmt --all --check`, `just sg-scan-strict`, `just test-ci`, `just conform-server-light`,
      `just dogfood`, and `just observability-openobserve-smoke` pass.
- [ ] Docs include signal definitions, deployment profiles, retention/backup/security, SLOs, alerts, and
      incident runbooks.
- [ ] `plans/README.md` status row is updated.

## STOP conditions

Stop and report; do not improvise if:

- instrumentation requires changing an official runner wire response or guest runner behavior;
- OTel Rust crate versions cannot provide multiple metric readers (Prometheus plus OTLP) without a
  second independently updated instrument set;
- the selected OTel logs bridge cannot preserve trace/span correlation or bounded nonblocking export;
- an exporter can perform network I/O on the handler thread/runtime path rather than a batch worker;
- OpenObserve asset format/API is unstable across the pinned version and cannot be smoke-tested;
- legal review rejects distributing the compose/dashboard/alert assets under the repository license;
- status sampling must hold `InnerState` while awaiting I/O or copies unbounded payload/log data;
- VM sampling requires one SmolVM subprocess per VM per scrape, periodic guest execution, or an
  unsupported private SmolVM storage/database layout rather than the public CLI/cgroup/process
  boundaries;
- the supported SmolVM floor (`versions.toml` `smolvm_min_version`, currently `1.8.1`) lacks the
  `machine status --json` / `machine ls --json` / `machine data-dir` contract, stops sharing one
  `machine_status_json` shape between the two JSON outputs, or changes its field semantics;
- `opentelemetry-prometheus` has fallen behind the `opentelemetry` release line again, forcing either
  a downgrade of the whole OTel stack or a second independently versioned instrument set;
- adding `MachineState::Unreachable` cannot be done without changing pool scheduling behavior — that
  is a correctness fix, not an observability change, and belongs in its own reviewed PR;
- PID start identity cannot be validated on a supported host, making cross-process attribution unsafe;
- a metric requires a user-controlled/unbounded label to answer the intended question;
- registering a cap or heartbeat would require changing the behavior of the code it observes;
- the current code no longer has the named centralized lifecycle/store/pool boundaries;
- conformance or dogfood changes after instrumentation, even if unit tests pass.

## Maintenance notes

- Signal names, units, label sets, status schema, condition codes, and termination reasons are public
  operational APIs. Review changes like wire changes; additive first, documented migration when not.
- Every new queue/state/runner failure path must update status, metrics, structured lifecycle logs, and
  tests together. Do not add a dashboard-only derivation for state the server already knows.
- Every new metric label needs a bounded-cardinality review and the unique-ID test.
- Every new cap, ring buffer, or truncation point must be registered with `LimitRegistry` in the same
  PR that introduces it. Every new long-lived `tokio::spawn` must register a `TaskHeartbeat`. Both are
  review checklist items, not follow-ups — this plan exists because fifteen tasks and nine caps
  accumulated without either.
- Every new tracing field needs a secret/PII review. `Debug` on request/config/domain structs is unsafe
  unless the type has an explicit redacted implementation.
- Anchors in this plan rot fast: the churn from `84d92cfd` to `673bdfa0` moved three of four named
  logging sites, the `TraceLayer` line, the `preloop status` implementation, the CLI subscriber, and
  every `broker.rs` range. Prefer the greps and symbol names over the line numbers, and re-run the
  drift check before each step.
- VM metric names/documentation must retain the `host` distinction: cgroup/process RSS, PID, OOM, and
  throttling are observations of the VMM on the host, not guest-kernel counters.
- A new SmolVM release must run the JSON inspection and cgroup/process telemetry fixtures in the
  existing runtime verification workflow before the verified version moves.
- Network/block-I/O or guest OS telemetry requires a separate source/semantics review; absence is more
  correct than a portable-looking zero from one platform.
- Keep OpenObserve versions/digests and assets tested together. Preloop's OTLP contract must continue to
  work with other vendors.
- If Preloop later becomes multi-server, re-design gauges and status around a shared authoritative bus;
  `service.instance.id` is not a substitute for distributed coordination.
- If workflow-log export is ever proposed, treat it as a separate security/retention product decision:
  workflow output is high-volume, user-controlled, and may contain masked or unrecognized secrets.
