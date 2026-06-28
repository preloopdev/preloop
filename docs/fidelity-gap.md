# aksh — GitHub Actions Control Plane Fidelity Gap &amp; Roadmap

**aksh** is a faithful Rust reimplementation of the GitHub Actions control plane

(`ChristopherHX/runner.server`) — a host-side service that the **official `actions/runner`**

**(`Runner.Listener`)** can register against, poll for jobs, execute, and report results to,

without GitHub-hosted minutes.

**aksh is not tied to any specific runner host.** It speaks the runner protocol and accepts

incoming runner connections; the runner itself handles execution. This means aksh works

equally well with:

- libkrun microVMs (what **Preloop** — the local CI product — uses)
- Docker / Podman containers
- Virtual machines (cloud or local)
- Bare processes on the same machine
- Remote runners on other servers

**Preloop** is the *product* that combines aksh (control plane) + a libkrun-based

ephemeral runner host for local CI. aksh is its control plane. But aksh is independently

usable: anyone can `cargo install aksh` and point their own runners at it.

Execution engine and runner host integrations live in **separate repos/crates**. This repo

is the control plane only.

Upstream reference: `actions/runner` v2.335.1 (commit `7d737449ef346f6524f75688d0c9c95fa10ba10a`)

runner.server reference: `ChristopherHX/runner.server` v3.14.0 (commit `069646146c90d649c74dfd7a34569c9420195838`)

(overridable via `AKSH_UPSTREAM_RUNNER_SERVER_REF`).

---

## 0. Naming


| Term                | Meaning                                                                        |
| ------------------- | ------------------------------------------------------------------------------ |
| **aksh**            | This repo: the GitHub Actions control plane service (protocol, scheduler, API) |
| **Preloop**         | Local CI product: aksh + libkrun runner host for ephemeral microVMs            |
| **Runner.Provider** | Pluggable trait: creates/destroys runners (any substrate)                      |
| **Runner.Listener** | The unmodified official `actions/runner` binary                                |


---

## 1. TL;DR scorecard

**As of 2026-06-26, this is achieved.** The official `actions/runner` v2.322.0 successfully
configures against aksh, creates encrypted sessions, receives job messages, executes jobs,
and reports completion. The full control plane protocol is working end-to-end.

**Note**: The scorecard below reflects aksh's state against v2.322.0. The deep diff in §1a
documents what v2.335.1 (latest) requires that is not yet implemented. Runner versions
v2.329.0+ are **enforced minimum** by GitHub since March 2026.

Rough completeness against "100% faithful control plane (v2.335.1)": **~55–60%** (was ~70–75%
against v2.322.0; the gap widened because upstream added background steps, DAP debugger, and
admin flow features).



| Layer                                            | State                                                     | Faithful?                                    |
| ------------------------------------------------ | --------------------------------------------------------- | -------------------------------------------- |
| Workflow YAML parse + typed model                | present, IndexMap preserves order                         | ✅ good                                       |
| Matrix expansion                                 | IndexMap order, GitHub name format                        | ✅ good                                       |
| Expression engine                                | wired into job builder, status functions from context     | ✅ good                                       |
| Trigger matching                                 | branches/tags/paths/types/schedule/dispatch               | ✅ good                                       |
| `needs` DAG scheduling                           | dependency-gated scheduler, outputs propagation           | ✅ good                                       |
| `if` / contexts / outputs propagation            | evaluated, needs outputs threaded                         | ✅ good                                       |
| Secrets policy / masking on the wire             | `SecretString` + mask hints in wire messages              | ✅ good                                       |
| **Runner session handshake (RSA/AES)**           | AES key exchange (unencrypted for now)                    | ⚠️ working, RSA wrap TODO                     |
| **Encrypted message queue (`TaskAgentMessage`)** | AES-encrypted body, iv, message ack                       | ✅ good                                       |
| `**AgentJobRequestMessage**`                     | full DTO with plan, request, context, steps               | ✅ good                                       |
| **OAuth / `connectionData` / location services** | 18 service GUIDs, GHES org-prefix routing                 | ✅ good                                       |
| **Timeline / logs / web-console feed**           | PATCH records, create/append logs, console feed           | ⚠️ partial (worker fidelity)                  |
| **Job/step completion events + annotations**     | AgentRequest PATCH with lockedUntil, result tracking      | ⚠️ partial (worker reports Failed)             |
| **Action download info**                         | stub endpoint                                             | ⚠️ stub                                       |
| Cache v1 / Artifact v1 shapes                    | in-memory stubs                                           | ⚠️ partial                                   |
| Cache v2 / Artifact v2 (blob/twirp)              | absent                                                    | ❌ missing                                    |
| **Background steps (concurrent execution)**      | absent                                                    | ❌ missing (new in v2.335.0)                  |
| **DAP debugger integration**                     | absent                                                    | ❌ missing (new in v2.335.0)                  |
| **Request acknowledgment**                       | absent                                                    | ❌ missing (new in v2.329.0)                  |
| **V2 admin flow / Broker URL**                   | absent                                                    | ❌ missing (new in v2.329.0)                  |
| **Runner config refresh**                        | absent                                                    | ❌ missing (new in v2.323.0)                  |
| **Server-enforced runner settings**              | absent                                                    | ❌ missing (new in v2.323.0)                  |
| **Node 20→24 migration / deprecation warnings**  | absent                                                    | ❌ missing (new in v2.328.0)                  |

---

## 1a. Deep source diff: runner.server v3.14.0 vs actions/runner v2.335.1

**Methodology**: Structural diff of `Runner.Listener/`, `Runner.Worker/`, `Runner.Common/`,
`Runner.Sdk/`, and Chris's `Runner.Server/Controllers/` against the official v2.335.1 source.
This is a C#-to-C# diff of the shared fork base, isolating protocol-relevant divergence.

### 1a.1 What official v2.335.1 has that runner.server v3.14.0 does NOT

These are features in the latest official runner that Chris's fork has not merged. Each one
represents a protocol surface change aksh must eventually support.

#### Background Steps (v2.335.0) — NEW execution model

The official runner now supports **concurrent background steps** — steps that run in parallel
with subsequent steps, coordinated via wait/cancel control-flow steps.

**Files only in official** (absent from Chris):
- `Runner.Worker/BackgroundStepCoordinator.cs` — coordinates concurrent step execution,
  manages slots via `SemaphoreSlim`, handles wait-all/cancel with grace periods
- `Runner.Worker/BackgroundStepControlFlowData.cs` — data class for control-flow step types:
  `Wait`, `WaitAll`, `Cancel`

**Files modified in official** (vs Chris):
- `Runner.Worker/StepsRunner.cs` — background steps are queued via coordinator instead of
  run synchronously; DAP debugger hooks wrap normal steps
- `Runner.Worker/StepsContext.cs` — **thread-safety**: official adds `lock(_lock)` around all
  step context mutations (GetStep, SetOutput, SetConclusion, SetOutcome); Chris has no locks
- `Runner.Worker/ExecutionContext.cs` — adds `IsBackground`, `BackgroundControlType`,
  `BackgroundControlStepIds`, `ParallelGroupId` fields on `TimelineRecord`
- `Runner.Worker/JobRunner.cs` — adds safety net: waits for unwaited background steps before
  post-hooks; integrates DAP debugger
- `Runner.Worker/JobExtension.cs` — validates `BackgroundControlTypes` (Wait/WaitAll/Cancel)
- `Runner.Common/JobServerQueue.cs` — merges `IsBackground`, `BackgroundControlType`,
  `BackgroundControlStepIds`, `ParallelGroupId` into timeline records on PATCH

**Protocol impact for aksh**: The runner sends `TimelineRecord` PATCHes with new fields:
`isBackground`, `backgroundControlType`, `backgroundControlStepIds`, `parallelGroupId`.
aksh's `TimelineController` must accept and store these fields. The `AgentJobRequestMessage`
may contain steps with `background: true` and control-flow steps with `type: "wait"/"waitAll"/"cancel"`.

**New SDK types** (in official, absent from Chris):
- `Sdk/DTPipelines/Pipelines/BackgroundStepControl.cs` — `BackgroundControlTypes` constants
- `Sdk/DTWebApi/WebApi/TimelineRecord.cs` — adds `BackgroundControlType`, `BackgroundControlStepIds`
- `Sdk/RSWebApi/Contracts/StepResult.cs` — adds same fields

#### DAP Debugger (v2.335.0) — NEW debugging protocol

The official runner integrates a **Debug Adapter Protocol (DAP)** debugger for live job debugging.

**Files only in official** (10 files in `Runner.Worker/Dap/`):
- `DapDebugger.cs`, `IDapDebugger.cs` — debugger lifecycle (on step start/complete, job init)
- `DapMessages.cs` — DAP protocol message types
- `DapReplExecutor.cs` — REPL command execution inside job containers
- `DapReplParser.cs` — REPL output parsing
- `DapVariableProvider.cs` — variable inspection for debugger
- `DebuggerConfig.cs` — debugger configuration
- `WebSocketDapBridge.cs`, `IWebSocketDapBridge.cs` — WebSocket transport for DAP
- `JobExecutionView.cs` — job execution state model for debugger UI

**Protocol impact for aksh**: The runner connects to a debugger WebSocket endpoint. If aksh
doesn't serve this, the runner simply doesn't enable debugging — **non-blocking**. But the
feature flag `actions_runner_override_debugger_welcome_message` is checked, and the runner
expects a `Debugger?.Enabled` flag in the job context. aksh should advertise debugger support
as `false` to avoid the runner attempting connection.

#### Request Acknowledgment (v2.329.0) — protocol change

The official runner now sends an explicit **acknowledgment** after receiving a job message.

**Both repos have this** (Chris merged it) — but the behavior differs:
- Official: `RunnerJobRequestRef.ShouldAcknowledge` is a feature-flagged field
- Chris: same field exists, same code path

**Protocol impact for aksh**: The runner calls `AcknowledgeRunnerRequestAsync` on the broker
server. aksh must handle this endpoint or the runner logs a warning (best-effort, non-fatal).

#### V2 Admin Flow & Broker URL (v2.329.0) — new control plane surface

The official runner splits management operations into two flows:
- `UseV2Flow` — V2 API for runner deletion/management
- `UseRunnerAdminFlow` — separate admin flow with its own auth URLs

**Both repos have the config fields** (`UseV2Flow`, `UseRunnerAdminFlow`, `ServerUrlV2` in
`ConfigurationStore.cs`). But Chris's `ConfigurationManager.cs` **skips the connection
validation** for `UseRunnerAdminFlow`:
```csharp
// Official:
if (!runnerSettings.UseRunnerAdminFlow)
{
    await _runnerServer.ConnectAsync(new Uri(runnerSettings.ServerUrl), creds);
}

// Chris:
await _runnerServer.ConnectAsync(new Uri(runnerSettings.ServerUrl), creds);
```

**Protocol impact for aksh**: When the runner is configured with `UseRunnerAdminFlow`, it
expects `auth_url` AND `auth_url_v2` in the connection data response. It uses a separate
`BrokerUrl` for admin operations. aksh must populate these fields in `ConnectionDataController`.

#### Runner Config Refresh (v2.323.0) — backend migration protocol

**Both repos have `RunnerRefreshConfigMessage`** — Chris merged this. The runner handles a
`RunnerRefreshConfig` message type that triggers config file exchange with the control plane.

**Protocol impact for aksh**: If aksh sends a `RunnerRefreshConfig` message, the runner will
attempt to exchange `.runner` and `.credentials` files. aksh can safely ignore this for now
(don't send the message type), but must accept it if the runner sends a refresh request.

#### Server-Enforced Runner Settings (v2.323.0)

The official runner accepts settings pushed by the control plane. Chris has this merged.

**Protocol impact for aksh**: aksh can optionally push settings to the runner. Low priority.

#### Feature Flags & Environment Variables (v2.321.0–v2.335.0)

Official v2.335.1 has these feature flags absent from Chris:

| Flag | Purpose | Impact on aksh |
---|---|---|
| `RunnerVersionDeprecated` (7) | Version deprecation check | aksh should return this if runner is too old |
| `ServiceContainerCommand` | Service container command support | Container actions may need this |
| `SendJobLevelAnnotations` | Job-level annotation telemetry | Timeline records may include annotations |
| `EmitCompositeMarkers` | Composite action markers | Debug/trace feature |
| `BatchActionResolution` | Batch action download | Action download may use batch API |
| `UseBearerTokenForCodeload` | Bearer auth for action tarballs | Action download auth change |
| `OverrideDebuggerWelcomeMessage` | Custom debugger greeting | DAP feature |
| `WarnOnNode20Flag` | Node 20 deprecation warning | Runner emits deprecation annotation |
| `DeprecateLinuxArm32Flag` | ARM32 deprecation | Platform check |
| `DisableStdoutMultilineLogPrefixing` | Log format control | Logging change |
| `SymlinkCachedActions` | Symlink instead of copy cached actions | Performance optimization |

**Environment variables only in official**:

| Variable | Purpose |
---|---|
| `ACTIONS_RUNNER_RETURN_VERSION_DEPRECATED_EXIT_CODE` | Exit code for deprecated runner |
| `ACTIONS_RUNNER_DISABLE_STDOUT_MULTILINE_LOG_PREFIXING` | Log format |
| `ACTIONS_RUNNER_SYMLINK_CACHED_ACTIONS` | Cache optimization |
| `ACTIONS_RUNNER_EMIT_COMPOSITE_MARKERS` | Debug markers |
| `GITHUB_ACTIONS_RUNNER_FORCE_EMPTY_GITHUB_URL_IS_HOSTED` | Hosted runner inference |
| `GITHUB_ACTIONS_RUNNER_FORCE_GHES` | Force GHES mode |

#### JobDispatcher Changes

Official `JobDispatcher.cs` returns `TaskResult` from `RunAsync()`; Chris returns `void`.
Official tracks job result for hosted runner telemetry (`ACTIONS_RUNNER_RETURN_JOB_RESULT_FOR_HOSTED`);
Chris strips this. The `RunOnceJobCompleted` type changed from `TaskResult` to `bool`.

**Protocol impact for aksh**: None directly — this is runner-internal. But it means Chris's
fork doesn't support the "return job result for hosted" telemetry path.

### 1a.2 What runner.server v3.14.0 has that official v2.335.1 does NOT

Chris's additions (not relevant to aksh's control plane protocol):

| File | Purpose |
---|---|
| `Runner.Worker/ExternalToolHelper.cs` | Chris's external tool utility |
| `Runner.Worker/Handlers/GoActionHandler.cs` | Go action handler (Chris addition) |
| `Runner.Sdk/GharunUtil.cs` | Chris's utility for gharun |

### 1a.3 Chris's behavioral divergences from official

These are places where Chris's code **differs in behavior** from the official runner,
which may cause issues when aksh serves the official runner:

1. **BrokerServer.cs**: Chris removes `VssUnauthorizedException` from the retry condition.
   Official retries on `AccessDeniedException || VssUnauthorizedException || RunnerNotFoundException
   || HostedRunnerDeprovisionedException`. Chris skips `VssUnauthorizedException`.
   **Impact**: If aksh returns a 401, Chris's runner retries; official doesn't.

2. **ConfigurationManager.cs**: Chris skips `UseRunnerAdminFlow` connection validation.
   **Impact**: Chris's runner always validates connection; official skips for admin flow.

3. **ConfigurationStore.cs**: Chris removes hosted-runner inference logic (checking
   `ServerUrl`/`ServerUrlV2` against `*.actions.githubusercontent.com` etc.).
   **Impact**: Chris's runner can't auto-detect if it's talking to GitHub-hosted infrastructure.

4. **StepsContext.cs**: Chris has no thread-safety locks. Official wraps all mutations in
   `lock(_lock)`. **Impact**: Concurrent background steps in official would race on Chris's
   impl; irrelevant for aksh (control plane, not runner).

5. **JobServerQueue.cs**: Chris adds `_webconsole_queue_all` variable controlled by
   `system.runner.server.webconsole_queue_all`. This is a Chris-specific feature for
   runner.server's web console. **Impact**: aksh doesn't need this.

6. **Platform detection**: Chris replaces `#if OS_WINDOWS`/`#if OS_LINUX` preprocessor
   directives with runtime `RuntimeInformation.IsOSPlatform()` checks. This makes Chris's
   runner a single cross-platform binary instead of platform-specific builds.
   **Impact**: None for aksh — this is runner-internal.

### 1a.4 Summary: what aksh needs to implement (priority order)

| Priority | Change | Upstream Version | aksh Status |
---|---|---|---|
| **P0** | Background step fields in TimelineRecord (`isBackground`, `backgroundControlType`, `backgroundControlStepIds`, `parallelGroupId`) | v2.335.0 | ❌ missing |
| **P0** | Thread-safe StepsContext (lock-based) | v2.335.0 | N/A (runner-side) |
| **P1** | Request acknowledgment endpoint (`AcknowledgeRunnerRequestAsync`) | v2.329.0 | ❌ missing |
| **P1** | `auth_url_v2` and `BrokerUrl` in connectionData | v2.329.0 | ❌ missing |
| **P1** | V2 admin flow support (`UseRunnerAdminFlow` response) | v2.329.0 | ❌ missing |
| **P1** | `RunnerVersionDeprecated` feature flag response | v2.321.0 | ❌ missing |
| **P2** | DAP debugger endpoint (WebSocket) | v2.335.0 | ❌ missing (non-blocking) |
| **P2** | `SendJobLevelAnnotations` in timeline | v2.323.0 | ❌ missing |
| **P2** | `BatchActionResolution` for action downloads | v2.328.0 | ❌ missing |
| **P2** | `UseBearerTokenForCodeload` for action tarballs | v2.328.0 | ❌ missing |
| **P3** | Node 20 deprecation warning annotation | v2.328.0 | ❌ missing |
| **P3** | `DisableStdoutMultilineLogPrefixing` env var | v2.335.0 | ❌ missing |
| **P3** | Server-enforced runner settings | v2.323.0 | ❌ missing |

---

## 2. Upstream surface we must emulate

The 23 controllers in `runner.server/src/Runner.Server/Controllers/` define the contract.

Grouped by the role they play for the official runner:

### 2.1 Runner lifecycle (mandatory for any job to run)

- `ConnectionDataController` — `GET _apis/connectionData`: AzDO `ConnectionData` +

  `LocationServiceData` GUID→location map. **First call the runner makes.**
- `RunnerRegistrationController` / `AgentController` — agent (runner) registration; the

  runner sends an **RSA public key**, server stores it for session-key wrapping.
- `AgentPoolsController` — pool discovery.
- `AgentSessionController` — `POST .../sessions`: returns `TaskAgentSession` with an

  **AES `encryptionKey`, RSA-wrapped** with the runner's pubkey. All later message bodies

  are AES-encrypted with this key.
- `MessageController` — `GET .../messages?sessionId&lastMessageId` long-poll returning

  `TaskAgentMessage{ messageId, messageType, iV, body }`; `DELETE .../messages/{id}` ack.

  **This is the 6,839-line heart**: it also runs the whole evaluation (triggers,

  expressions, matrix, needs, contexts) and builds the job. Upstream leans on GitHub's

  real `DistributedTask.ObjectTemplating`, `Expressions2`, and `Pipelines.ContextData`

  SDKs — that is the semantic bar.
- `AgentRequestController` — job request lease/renew/lock semantics.
- `AuthController` / `OidcController` — OAuth client-credentials token issuance; the

  runner attaches a bearer token to every subsequent call. `OidcController` mints job

  OIDC tokens (`id-token: write`).

### 2.2 Job reporting (mandatory for status/logs/annotations)

- `TimelineController` — `PATCH .../timelines/{id}/records`: per-job and per-step

  `TimelineRecord`s (state, result, start/finish, `**issues[]` = annotations**).
- `LogfilesController` — create/append log files per timeline record.
- `TimeLineWebConsoleLogController` — live console `feed` lines.
- `FinishJobController` — `JobCompleted` event with **job outputs** + final result.

  This is where `needs.<job>.outputs` originate.

### 2.3 Asset services

- `ActionDownloadInfoController` — resolves `uses:` → tarball download URLs (+ auth).

  Without it the runner cannot fetch actions.
- `CacheController` (v1 `_apis/artifactcache`) + `CacheControllerV2` (blob/twirp).
- `ArtifactController` (v1 pipelines) + `ArtifactControllerV2` (blob).
- `ArtifactCacheManagementController` — cache listing/eviction.

### 2.4 Support

- `VssControllerBase` / `ApiResponder` — AzDO envelope conventions (error shapes,

  `Content-Type`, API-version negotiation headers).
- `GitHubAppIntegrationBase`, `PipelineContext`, `CounterFunction`, `TaskController`.

---

## 3. What exists today (and where it diverges)

Paths are in this repo. Updated 2026-06-26.

- `aksh-gha-parser/src/lib.rs`
  - ✅ Typed `Workflow`/`Job`/`Step`/`Trigger`/`RunsOn`/`Needs`/`Strategy`/`Matrix`.
  - ✅ `Trigger::matches_with_context` — `branches`/`tags`/`paths`/`types`/`schedule`/`workflow_dispatch`.
  - ✅ `expand_matrix` uses `IndexMap` preserving declaration order; GitHub `name (v1, v2)` format.
  - ✅ `can_merge_include` compares only original dimensions.
  - ✅ Expression evaluation wired into job builder via `eval` module.
- `aksh-gha-expressions/src/lib.rs`
  - ✅ Pratt parser + evaluator; `contains/startsWith/endsWith/format/join/fromJSON/toJSON`.
  - ✅ **Wired** into job builder — expressions resolved in env, with, run fields.
  - ✅ `success()/failure()/cancelled()` use context state (not hardcoded).
  - ⚠️ No index/bracket access (`matrix['os']`), no `*` object-filter (`steps.*.outputs`),
  
    no `format` `{{`/`}}` escaping.
  - ⚠️ Empty object/array is falsey; GitHub treats non-null object/array as truthy.
- `aksh-runner-server/src/lib.rs`
  - ✅ axum router with GHES org-prefix routing, graceful shutdown, NDJSON broadcast.
  - ✅ Full AzDO lifecycle: `connectionData` (18 GUIDs), `AgentPools`, `Agent`, `AgentSession`,
  
    `Message`, `AgentRequest`, `Timeline`, `Logfiles`, `FinishJob`, `ActionDownloadInfo`.
  - ✅ GitHub-compatible registration: `/api/v3/actions/runner-registration` with `RemoteAuth`.
  - ✅ AES session key exchange (unencrypted mode — RSA wrapping TODO).
  - ✅ Encrypted `TaskAgentMessage` delivery with `messageId` and `DELETE` ack.
  - ✅ `AgentJobRequestMessage` with `plan`, `requestId`, `system` context, full steps.
  - ✅ `AgentRequest` PATCH handler with `lockedUntil` for job renewal.
  - ✅ `needs` DAG scheduling with dependency-gated dispatch and outputs propagation.
  - ✅ `fail-fast` / `max-parallel` matrix strategy support.
  - ⚠️ Timeline/log endpoints exist but worker reports job as "Failed" (fidelity gap).
  - ⚠️ Cache/artifact handlers use in-memory maps; file-backed stores not wired.
- `aksh-gha-protocol/src/lib.rs`
  - ✅ `SecretString` redaction-safe; AzDO wire DTOs in `azdo` module.
  - ✅ `AgentJobRequestMessage` with `PlanReference`, `request_id`, `EndpointAuthorization`.
  - ✅ `ServiceEndpoint.authorization` is `EndpointAuthorization` directly (not nested map).
  - ✅ `TaskResources.repositories` is `Vec` (not `BTreeMap`).
  - ✅ RSA/AES crypto module in `crypto` module.
- `aksh-conformance/src/main.rs`
  - ⚠️ Only parses/counts fixtures + diffs two commands' stdout.
  - ❌ No golden tests, fuzz targets, or wire capture/replay.

---
## 4. Pluggable backends &amp; deployment modes

The official runner protocol already decouples execution from the control plane: the runner

*connects in* and pulls work; aksh never reaches *out* to execute anything. So there is

exactly **one plug point**: how a runner instance is created, given credentials, and torn

down. Everything else — sessions, messages, timeline, logs, cancel, rerun — is identical

regardless of where the runner lives.

### 4.1 The `RunnerProvider` trait

```rust
use async_trait::async_trait;

/// How aksh creates and destroys runner instances.
pub trait RunnerProvider: Send + Sync {
    /// Labels this provider can satisfy (for `runs-on` routing).
    fn labels(&self) -> &LabelMatcher;

    /// Start a runner that will phone home and self-register via the normal protocol.
    /// aksh only handles birth; the protocol does the rest.
    async fn provision(
        &self,
        spec: &RunnerSpec,
        registration: RunnerRegistration,
    ) -> Result<RunnerHandle, ProviderError>;

    /// Tear down (ephemeral cleanup / scale-down).
    async fn terminate(
        &self,
        handle: &RunnerHandle,
    ) -> Result<(), ProviderError>;

    /// Optional: current capacity for backpressure.
    async fn capacity(&self) -> Capacity {
        Capacity::unbounded()
    }
}
```

- `RunnerRegistration` = what the runner needs to call back: **aksh's URL** (reachable

  from *its* network namespace — the **provider's** responsibility), a **single-use scoped**

  **registration token**, labels, `ephemeral` flag, unique name.
- `RunnerSpec` = derived from the job: required labels + resource hints (`runs-on` can be

  an object for size/image).
- `RunnerHandle` = opaque provider id (pid / container id / vm id), correlated to the

  registered agent via the injected name+token.
- `LabelMatcher` = set-intersection matching: `runner.labels ⊇ job.labels`.

Each backend is an impl: `provision` = boot a container (`docker run`), a microVM

(libkrun), a cloud VM, a k8s pod, a `std::process::Command`, etc. **None of them touch**

**the protocol.**

### 4.2 The base case is BYO (no provider needed)

This is the critical design decision for generality:

**Make aksh fully work with zero providers.** Self-hosted runners just register and poll.

So aksh is usable without any provider crate at all — just point runners at it.

```mermaid
sequenceDiagram
  participant J as Job queued (runs-on labels)
  participant S as Scheduler
  participant P as Provider (optional)
  participant R as Runner
  J->>S: enqueue
  S->>S: idle registered runner matching labels?
  alt match exists (BYO or warm pool)
    S-->>R: (runner pulls job via message queue)
  else none + provider routes these labels
    S->>P: provision(spec, registration)
    P->>R: boot VM/container/process + creds
    R-->>S: register + poll
    S-->>R: deliver job
  else none + no provider
    S->>S: queue waits, emit "waiting for runner"
  end
  Note over R: job completes
  S->>P: if ephemeral → terminate(handle)
```

Label routing mirrors GitHub: a job's `runs-on` set must be ⊆ runner labels. No new

matching semantics.

### 4.3 Three more pluggable seams

All three are traits; default impls cover local use.

`**RunStore**` — run/job state persistence.


| Implementation              | Use case                                          |
| --------------------------- | ------------------------------------------------- |
| `InMemory` (default)        | Local: instant, no deps, state lost on restart    |
| `sqlx` (SQLite or Postgres) | Server: durable, idempotent restart, multi-tenant |


`**AuthProvider` / tenancy** — who can talk to aksh.


| Mode                              | Use case                                              |
| --------------------------------- | ----------------------------------------------------- |
| Loopback / dev token              | Local: single implicit tenant, no crypto              |
| OAuth + mTLS + per-tenant scoping | Server: namespaced tenants, per-tenant queues/secrets |


`**SecretStore**` — where secret values come from.


| Mode                         | Use case                                                   |
| ---------------------------- | ---------------------------------------------------------- |
| Submission payload (default) | Local: secrets come with the workflow JSON                 |
| Environment / vault          | Server: secrets pulled from AWS SM / HashiCorp Vault / env |


### 4.4 Local vs server = profiles, not two codebases

One binary. One control plane. Different trait impls selected by `aksh serve --profile`.


| Concern       | `--profile local` (default)                                     | `--profile server`                                    |
| ------------- | --------------------------------------------------------------- | ----------------------------------------------------- |
| Runner host   | in-process: process/container/libkrun, ephemeral, scale-to-zero | remote: k8s/Firecracker/cloud pools, **or** BYO fleet |
| Persistence   | `InMemory` `RunStore`                                           | `sqlx` `RunStore`                                     |
| Auth/tenancy  | loopback / dev token, single tenant                             | OAuth + mTLS, namespaced tenants                      |
| Networking    | `127.0.0.1`                                                     | routable base URL, token-scoped callbacks             |
| Lifecycle     | JIT ephemeral                                                   | pools + autoscale + fairness                          |
| Secret source | payload                                                         | vault / env / SM                                      |


### 4.5 Suggested crate layout

```
aksh/                              ← this repo (the control plane)
├── crates/
│   ├── aksh-server            # axum service; protocol-only; provider-agnostic
│   ├── aksh-orchestrator      # RunnerProvider/RunnerSpec traits + scheduler
│   ├── aksh-gha-protocol      # AzDO wire DTOs, SecretString, NDJSON, crypto
│   ├── aksh-gha-parser        # Workflow YAML parse + expression eval + matrix
│   ├── aksh-cache             # Cache store trait + file-backed impl
│   ├── aksh-artifacts         # Artifact store trait + file-backed impl
│   └── aksh-conformance       # Differential tests vs upstream runner.server

preloop-providers/              ← separate repo (runner hosts)
├── aksh-provider-process      # spawn (fastest, least isolation)
├── aksh-provider-container    # docker / podman
├── aksh-provider-libkrun      # microVM (Preloop's default)
└── aksh-provider-remote       # k8s / cloud VM / Firecracker / SSH

preloop/                        ← the product that ties it together
├── preloop-cli                # CLI that wraps aksh-server + a provider
└── preloop-vm-image           # libkrun runner VM image builder
```

Control plane depends only on **traits**, never on a concrete provider. Adding "huge VMs"

or a new cloud backend later = a new crate, zero control-plane edits. BYO mode =

`providers = []`.

### 4.6 Two gotchas to design in now

1. **Callback reachability.** The URL the runner uses to call back must resolve from inside

   its sandbox. Host-gateway IP for containers, guest-network IP for libkrun, service DNS /

   public URL for remote runners. This is why `control_plane_url` lives in

   `RunnerRegistration` and is the **provider's** job to fill — aksh never hardcodes an

   address for the runner to use.
2. **Scaling path.** For large deployments you'll eventually split into stateless aksh

   replicas behind an LB + separate orchestrator(s) + a durable `RunStore`. The trait

   boundaries make that a deployment change, not a rewrite. Design the seams now even though

   the first implementation is single-process.

---

## 5. Design principle: upstream truth + aksh projections

Keep faithfulness and your added advantages **without forking semantics**:

- Model the **AzDO/runner protocol as the source of truth** in `aksh-gha-protocol`.
- Layer aksh extras as **read-model projections / sidecars**, never as replacements:
  - **NDJSON agent feed** = a projection *derived from* timeline records, not a parallel
  
    status path.
  - `**SecretString` redaction** = how `variables`/`maskHints` render in logs and any API.
  - **Native `/api/v1` REST** = an *additional* ergonomic surface for agents/tools, served
  
    **alongside** the runner-compatible `_apis/...` surface, both reading the same state.
- This keeps it general (anyone's official runner works) while retaining the local-first

  ergonomics already built.

---

## 6. Implementation plan (phased, each phase independently testable)

Ordering is by dependency: the runner cannot reach phase N+1 until phase N answers

correctly. Make **small commits per step** with the tradeoff notes called out.

### Phase A — AzDO wire DTOs + envelope conventions

**Goal:** typed, versioned, golden-tested wire models; no behavior yet.

Steps:

1. Add `aksh-gha-protocol::azdo` module: `ConnectionData`, `LocationServiceData`,

   `TaskAgentSession`, `TaskAgent`, `TaskAgentMessage`, `AgentJobRequestMessage`,

   `TaskOrchestrationPlanReference`, `TimelineReference`, `TimelineRecord`, `Issue`,

   `VariableValue { value, is_secret }`, `MaskHint`, `ServiceEndpoint`,

   `PipelineContextData` (the AzDO context-data union: string/array/dict/bool/number).
2. Exact field names/casing (`camelCase`, GUIDs lowercased) to match upstream JSON.
3. `serde` round-trip + `#[serde(deny_unknown_fields)]` off (runner sends extras), but keep

   golden fixtures strict.

**Validate compatibility (Phase A):**

- Capture real wire JSON: run upstream `runner.server` + a runner once, record every

  request/response under `fixtures/wire/` (a `--record` flag on the conformance tool).
- Golden test: every captured upstream body deserializes into our DTO and **re-serializes**

  **byte-identically** (modulo documented field-order normalization).
- Property test: arbitrary DTO → serialize → deserialize is identity.

### Phase B — `connectionData`, location services, OAuth/auth

**Goal:** the runner gets past discovery + authenticates.

Steps:

1. Implement `GET _apis/connectionData` returning the full service-GUID location map

   (copy GUIDs from the captured fixture; they are stable).
2. `AuthController` equivalent: OAuth2 client-credentials `POST .../oauth2/token` → bearer.

   Issue/verify a local signing key; accept the runner's `.credentials` client auth.
3. Bearer middleware (tower layer) gating all `_apis/...` routes; map missing/invalid →

   AzDO 401 envelope.
4. `OidcController`: mint a local OIDC JWT for `id-token: write` jobs (configurable issuer).

**Validate (Phase B):**

- Point the **real `Runner.Listener config`** at aksh; it must register and store

  credentials without error (`./config.sh --url http://localhost:PORT --token X`).
- Golden: our `connectionData` response contains the same service-location set as the

  fixture (assert superset of the GUIDs the runner indexes).
- Negative: unauthenticated `_apis/...` → 401 with the AzDO error shape.

### Phase C — Registration + session key exchange (RSA/AES)

**Goal:** an encrypted session the runner trusts.

Steps:

1. `POST .../pools/{id}/agents`: parse runner RSA public key (XML/JWK form upstream uses),

   persist per-agent.
2. `POST .../pools/{id}/sessions`: generate a random AES key, **RSA-OAEP wrap** it with the

   runner's pubkey, return `TaskAgentSession { encryptionKey: { value: <wrapped>, ... } }`.
3. Keep the AES key server-side keyed by `sessionId`.
4. **Crypto isolation:** all RSA/AES lives in one reviewed module (`protocol::crypto`);

   `unsafe` stays forbidden; use `rsa`/`aes-gcm`/`cbc` crates. Document algorithm choices.
5. **Known FIPS gap:** upstream `actions/runner` uses RSA-OAEP-SHA1 by default but switches to

   RSA-OAEP-SHA256 when `UseFipsEncryption` is enabled. aksh currently implements the default

   SHA-1 OAEP path only; FIPS-mode runners require an explicit algorithm switch before they can

   decrypt `TaskAgentSession.encryptionKey`.


**Validate (Phase C):**

- The real runner's `Runner.Listener run` reaches the message-poll loop (it only does so

  after it can decrypt the session key) — assert via runner logs / a test harness.
- Unit: round-trip wrap/unwrap with a known test keypair vs an OpenSSL-generated reference

  (golden ciphertext is non-deterministic, so test *decrypt of upstream-captured* wrapped

  key with a fixed test private key).

### Phase D — Wire the evaluator: build a real `AgentJobRequestMessage`

**Goal:** one job, fully resolved, ready for the runner. (Still no `needs` graph yet —

single job.)

Steps:

1. Create `aksh-gha-parser::eval` that **consumes `aksh-gha-expressions`** and produces

   resolved job material:
  - interpolate `${{ }}` in `env`, `with`, `run`, `runs-on`, matrix values;
  - build `contextData`: `github`, `env`, `vars`, `matrix`, `strategy`, `inputs`, `needs`
  
    (empty for single job), `secrets`;
  - compile `if` to a condition the runner evaluates (emit the **expression string** in the
  
    step/job `condition` field — the runner has its own evaluator; do **not** pre-collapse).
2. Materialize `variables`: env + system vars as `VariableValue`, secrets as

   `{ value, isSecret: true }`, and add `maskHints` for every secret value.
3. Replace `RunnerJobMessage` payload with `AgentJobRequestMessage`

   (`messageType = "PipelineAgentJobRequest"`); AES-encrypt the body, set `iV`, wrap in

   `TaskAgentMessage`.
4. `MessageController` queue: long-poll (await on a per-session channel up to ~50s),

   monotonically increasing `messageId`, redeliver until `DELETE` ack, `JobCancellation`

   message on cancel.

**Validate (Phase D):**

- The real runner **accepts and starts** the job: timeline records begin arriving (proves

  the message decrypted and parsed).
- Golden: for each `fixtures/upstream-workflows/*`, our `AgentJobRequestMessage` matches the

  upstream-emitted one field-by-field (normalize volatile ids/timestamps). This is the core

  conformance assertion.
- Property test (`proptest`): expression eval vs a table of GitHub-documented cases

  (truthiness, `==` case-insensitivity, numeric coercion, `format`, `fromJSON`).

### Phase E — Timeline, logs, web-console, completion, annotations

**Goal:** status, logs, and annotations flow back; `JobCompleted` carries outputs.

Steps:

1. `TimelineController PATCH records`: upsert `TimelineRecord`s; map state/result; collect

   `issue` entries → annotations. Project each change into an NDJSON event.
2. `LogfilesController` + `TimeLineWebConsoleLogController`: store logs, stream live feed;

   redact via `SecretString` masking using the job's `maskHints`.
3. `FinishJobController`: ingest `JobCompletedEvent` → final result + **job outputs**; persist.
4. NDJSON feed becomes a pure projection of timeline + completion state.

**Validate (Phase E):**

- End of a real-runner job: our run record shows correct per-step results, captured logs,

  and any `::error::`/`::warning::` annotations the runner emitted.
- Golden: timeline record sequence for a known workflow matches upstream's (state

  transitions + final results), volatile fields normalized.
- Masking test: a job with a secret in `env` never appears un-redacted in stored logs/feed.

### Phase F — `needs` DAG, outputs propagation, contexts across jobs

**Goal:** multi-job workflows behave like GitHub.

Steps:

1. Replace FIFO with a **dependency-gated scheduler**: a job becomes dispatchable only when

   all `needs` complete; compute its `if` against real job-status functions

   (`success()/failure()/cancelled()/always()`), which now reflect dependency results.
2. Thread `needs.<job>.outputs` + `needs.<job>.result` into the dependent job's `contextData`.
3. `fail-fast` / `max-parallel` honoring; skipped vs failed vs cancelled propagation per

   GitHub's `NeedsTaskResult` rules (see upstream `MessageController` enum).

**Validate (Phase F):**

- Real runner over a diamond `needs` graph + matrix: dispatch order and skip/fail

  propagation match upstream run.
- Golden: expanded job set + per-job `contextData.needs` matches upstream for

  `fixtures/.../case_insensitive_needs`, `node16_complex_reusable_workflows`, etc.
- Property test: random DAGs never dispatch a job before its dependencies; no cycles accepted.

### Phase G — Triggers, matrix fidelity, reusable workflows

**Goal:** front-end parsing matches GitHub exactly.

Steps:

1. Trigger matching: `branches`/`branches-ignore`/`tags`/`paths`/`paths-ignore` (globset),

   `types:` activity types, `workflow_dispatch` inputs, `schedule`.
2. Matrix: preserve declaration order (carry `IndexMap` end-to-end), GitHub job-name format

   `name (v1, v2)`, correct `include` (append vs merge on original dimensions only) and

   `exclude` precedence.
3. Reusable workflows: `secrets: inherit`, required secrets, `with:` inputs typing, output

   mapping; nested depth limit (upstream `MaxWorkflowDepth`).

**Validate (Phase G):**

- Golden expansion diff vs upstream for every in-scope fixture (this is what

  `aksh-conformance compare` should actually assert, not stdout-diff two arbitrary cmds).
- Fuzz (`cargo-fuzz`): `parse_workflow` never panics on arbitrary YAML; malformed triggers

  produce typed errors.

### Phase H — Action download, cache v2, artifact v2

**Goal:** the runner can fetch actions and use cache/artifacts end-to-end.

Steps:

1. `ActionDownloadInfoController`: resolve `uses: owner/repo@ref` and `./local` →

   download URLs (proxy to GitHub or serve local tarballs for vendored actions).
2. Cache v2 (`CacheControllerV2`) + Artifact v2 (`ArtifactControllerV2`) blob protocols;

   back them with `aksh-cache`/`aksh-artifacts` (retire the in-memory duplicates).
3. Wire the file-backed stores; remove `#[allow(dead_code)]`.

**Validate (Phase H):**

- Real `actions/checkout` + `actions/cache` + `actions/upload-artifact` run green against

  aksh.
- Golden: cache reserve/commit/lookup and artifact create/upload/list responses match

  upstream shapes.

---

## 7. Conformance harness (the spec's headline deliverable)

Build `aksh-conformance` into a real differential tester:

- `record` — drive upstream `runner.server` (+ optionally a runner) over each fixture,

  capturing wire traffic and final state to `fixtures/wire/<case>/`.
- `expand` — our parser/evaluator over each fixture → expanded jobs + `contextData`.
- `compare` — assert our expansion/messages/timeline/cache/artifact responses match the

  recorded upstream, with a documented **normalizer** for volatile fields (GUIDs,

  timestamps, ports, tokens).
- `replay` — feed recorded upstream `AgentJobRequestMessage`s to our DTOs and back.
- Test taxonomy required by the spec:
  - **Golden tests** — expansion, contexts, message bodies, timeline sequences.
  - **Property tests** — expression eval + matrix expansion invariants.
  - **Protocol-compat tests** — DTO round-trips vs captured wire JSON.
  - **Fuzz tests** — `parse_workflow` + expression lexer/parser (`cargo-fuzz`).
  - **Integration** — real `Runner.Listener` against aksh (later: inside a provider host).

Normalization policy must be explicit and reviewed, so "match" is meaningful, not lax.

---

## 8. End-to-end acceptance (definition of done)

A run is faithful when, with the **unmodified official `actions/runner`**:

1. `config.sh` registers the runner against aksh (Phases B–C).
2. A submitted workflow is parsed, triggered, matrix-expanded, and `needs`-scheduled

   matching upstream (Phases F–G).
3. The runner long-polls, receives an **encrypted `TaskAgentMessage`**, decrypts it, and

   starts the job (Phases C–D).
4. Steps run; timeline records, logs, live console, and annotations stream back; secrets are

   masked (Phase E).
5. `JobCompleted` delivers outputs; downstream `needs` jobs see `needs.<job>.outputs` and

   evaluate their `if` correctly (Phases E–F).
6. `actions/checkout`/`cache`/`upload-artifact` work via action-download + cache/artifact

   services (Phase H).
7. Cancellation mid-job delivers a `JobCancellation` message and the run/jobs settle to

   `cancelled`; rerun re-queues from a clean state.
8. `aksh-conformance compare` is **green** across all in-scope fixtures, with golden,

   property, protocol, and fuzz suites passing.
9. The NDJSON agent feed is a faithful projection of the same timeline/completion state —

   aksh's added value, layered on a faithful core.

### Provider integration (step 10)

Once 1–9 hold against a local `Runner.Listener`:

10. Repeat 1–9 with the listener running inside a provider host (container, libkrun, etc.)

    to close the integration loop. The `RunnerProvider` trait is validated by running the

    same golden fixtures through a real provider and confirming identical timeline/results.

---

## 9. Sequencing summary

```
A (DTOs) → B (connectionData/auth) → C (session crypto) → D (job message + evaluator wiring)
     → E (timeline/logs/completion) → F (needs DAG/outputs) → G (triggers/matrix/reusable)
     → H (action download/cache v2/artifact v2)
conformance harness grows alongside, asserting each phase against recorded upstream traffic.
```

Phases A–E are the critical path to "a real runner runs one job." F–H reach

"a real runner runs *any* in-scope workflow exactly like GitHub." Provider integration

(step 10) closes the loop for Preloop and every other host.