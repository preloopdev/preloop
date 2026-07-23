# aksh — Improvement Opportunities

Companion to `fidelity-gap.md`. Two categories:

1. **Additive** — improvements that maintain 100% runner protocol compatibility
2. **Slightly breaking** — small protocol/semantic deviations that unlock significant benefits

Goal context: building a modern CI platform for agents, with GitHub Actions as the initial model.

Upstream reference: `actions/runner` v2.336.0 (2026-07-20)

---

## 1. Additive Improvements (100% Compatible)

These can be implemented without any protocol deviation — the official runner works identically.

### 1.1 Server-side improvements

#### A01: Persistent run store (SQLite/Postgres)

**Current:** All state is in-memory (`Arc<Mutex<InnerState>>`). Lost on restart.
**Opportunity:** Implement the `RunStore` trait with SQLite (local) or Postgres (server) backends.
**Benefit:** Durable runs survive server restarts; enables multi-tenant deployments, run history, and analytics.
**Files:** `crates/aksh-runner-server/src/state.rs` (19.3 KB) — extract trait, implement backends.
**Effort:** Medium. The state struct is already well-factored; the trait boundary is implied but not explicit.

#### A02: Webhook-driven workflow dispatch

**Current:** Workflows are submitted via `POST /api/v1/runs`. GitHub App webhook handler exists (`github.rs`, 33.4 KB) but workflow file fetching and trigger evaluation from webhooks is incomplete.
**Opportunity:** Complete the push/PR webhook → workflow fetch → trigger eval → job dispatch pipeline.
**Benefit:** Users can `git push` and have aksh automatically run matching workflows, just like GitHub Actions. Essential for the "drop-in" promise.
**Files:** `crates/aksh-runner-server/src/github.rs`, `crates/aksh-runner-server/src/events/push.rs` (8.8 KB)

#### A03: Runner group routing enforcement

**Current:** `runner_group_id`/`runner_group_name` are stored in `.runner` settings and matched during registration. `runs-on: { group: ... }` is parsed into `JobPlan.runner_group`. But server-side routing only matches labels, not groups.
**Opportunity:** Enforce group-based routing: runners in group X only receive jobs targeting group X.
**Benefit:** Multi-tenant isolation — different teams/orgs get dedicated runner pools.
**Files:** `crates/aksh-runner-server/src/scheduler.rs` (34.7 KB), `runtime_scheduling.rs` (40.4 KB)

#### A04: Streaming NDJSON → structured agent feed

**Current:** NDJSON broadcast exists as a projection of timeline/completion state.
**Opportunity:** Enrich the NDJSON feed with structured events (step start/end, log lines, annotations, artifacts, cache hits/misses) that AI agents can consume programmatically.
**Benefit:** Agents don't need to parse log text — they get machine-readable events. This is the primary differentiation for an agent-first CI platform.
**Files:** `crates/aksh-runner-server/src/live_logs.rs` (4.8 KB), `runs.rs` (44 KB)

#### A05: DAP debugger improvements — conditional breakpoints by expression context

**Current:** DAP integration is fully implemented (4,527 LOC, 67 tests). Breakpoints can be set by line.
**Opportunity:** Add expression-aware conditional breakpoints: break when `steps.build.outcome == 'failure'` or `matrix.os == 'ubuntu'`. Expose the full GitHub Actions expression context in DAP variable scopes.
**Benefit:** Interactive CI debugging with full workflow context awareness — unique to aksh.
**Files:** `crates/aksh-dap/src/` (4,527 LOC)

#### A06: Server-side step log streaming with masking

**Current:** Logs are uploaded after step completion. Live console feed exists but is basic.
**Opportunity:** Real-time log streaming through the server with automatic secret masking applied server-side (using the job's `maskHints`).
**Benefit:** Faster feedback for long-running steps; double masking (runner + server) prevents secret leaks even if the runner has bugs.
**Files:** `crates/aksh-runner-server/src/timeline_logs.rs` (16.6 KB), `live_logs.rs`

#### A07: Parallel matrix job scheduling

**Current:** Matrix jobs are expanded and scheduled, respecting `max-parallel`. The scheduler dispatches jobs to available runners.
**Opportunity:** Implement warm-pool pre-provisioning: when a matrix workflow is submitted, pre-provision runners for all matrix combinations before the first job completes.
**Benefit:** Matrix fan-out latency drops from serial (provision → run → provision → run) to parallel (provision all → run all). Critical for agent workloads with large matrices.
**Files:** `crates/aksh-runner-server/src/scheduler.rs`, `runtime_scheduling.rs`

#### A08: Action caching / vendoring

**Current:** Actions are downloaded from GitHub on every run via `runnerresolve/actions` batch endpoint.
**Opportunity:** Cache resolved action tarballs locally (keyed by `owner/repo@sha`). Optionally vendor actions into the workspace for air-gapped environments.
**Benefit:** Faster job startup (no network for cached actions), offline support, reproducibility.
**Files:** `crates/aksh-runner/src/client/actions_download.rs` (10.9 KB), `crates/aksh-runner-server/src/actions.rs` (9.4 KB)

#### A09: Structured completion metadata

**Current:** `completejob` sends `stepResults`, `annotations`, `outputs`, `telemetry`.
**Opportunity:** Extend the native `/api/v1` completion surface with structured metadata: timing breakdowns, resource usage (CPU/memory/disk per step), cache hit rates, artifact sizes.
**Benefit:** Rich analytics without parsing logs. Agents can make data-driven decisions about which steps to optimize.
**Files:** `crates/aksh-runner/src/worker/completion.rs` (22.2 KB), server-side `runs.rs`

#### A10: Expression function extensions

**Current:** All 12 official functions implemented.
**Opportunity:** Add aksh-specific functions in a dedicated namespace (e.g., `aksh.fileSize()`, `aksh.elapsed()`, `aksh.runnerLoad()`) that are available in `if:` conditions and expressions. These would be no-ops / not-found on GitHub, so workflows that use them are aksh-aware but gracefully degrade.
**Benefit:** Richer conditional logic for local CI — skip expensive steps on low-resource runners, etc.
**Files:** `crates/aksh-gha-expressions/src/evaluator.rs` (15.9 KB)

#### A11: Run-level artifact and log retention policies

**Current:** Artifacts and cache stored in file-backed `aksh-cache`/`aksh-artifacts` with no automatic eviction.
**Opportunity:** Implement configurable retention: by age, by size, by run count. LRU eviction for cache.
**Benefit:** Prevents disk exhaustion on long-running servers. Essential for production deployments.
**Files:** `crates/aksh-cache/`, `crates/aksh-artifacts/`

#### A12: Job hooks (pre/post job scripts)

**Current:** `ACTIONS_RUNNER_HOOK_JOB_STARTED` / `ACTIONS_RUNNER_HOOK_JOB_COMPLETED` hooks are partially implemented — `completion.rs` has `make_hook_step()`.
**Opportunity:** Complete the hook lifecycle: execute pre-job hooks before step execution, post-job hooks after completion. Support hook scripts for setup/teardown of job-level resources.
**Benefit:** Enables custom per-job initialization (e.g., agent workspace preparation, credential injection).
**Files:** `crates/aksh-runner/src/worker/completion.rs`, `job_runner.rs`

### 1.2 Runner-side improvements

#### A13: Server queue delta optimization

**Current:** `ServerQueue` in `server_queue.rs` already tracks `dirty_keys` and sends only changed steps — this is a delta approach matching the official runner's `JobServerQueue.cs` merge behavior.
**Opportunity:** Verify that the delta merge logic (`step_records.rs` `merge_step_update`) produces exactly the same batch sizes as the official runner. Golden comparison shows aksh sends ~10 updates vs official's ~5 in the same scenario.
**Benefit:** Reduced network traffic; exact wire-level conformance.
**Files:** `crates/aksh-runner/src/worker/server_queue.rs` (16.1 KB), `step_records.rs` (25.7 KB)

#### A14: Process isolation improvements

**Current:** Steps run as child processes. `RUNNER_TRACKING_ID` is set for orphan cleanup.
**Opportunity:** Add optional cgroup isolation (Linux): each step gets its own cgroup for resource limiting and reliable cleanup. On macOS, use `sandbox-exec` or process groups.
**Benefit:** Prevent runaway steps from consuming all resources; clean process tree cleanup.
**Files:** `crates/aksh-runner/src/process.rs` (25.2 KB)

---

## 2. Slightly Breaking Improvements

These introduce small protocol deviations from the official runner behavior. The deviations
are minimal and narrowly scoped — the runner still registers, polls, executes, and reports
successfully. The benefits justify the deviation for a modern CI platform.

### 2.1 Expression and workflow language extensions

#### B01: Typed matrix values

**What changes:** Matrix values are always strings in GitHub Actions. Allow typed values (numbers, booleans, objects) to flow through without string coercion.
**Deviation:** `matrix.count` would be `42` (number) instead of `"42"` (string) in expression evaluation.
**Benefit:** Eliminates `fromJSON()` hacks in workflows. `if: matrix.count > 10` works naturally.
**Risk:** Low — workflows using string comparison on matrix values could behave differently. Can be gated behind `aksh.typed_matrix: true` in workflow metadata.

#### B02: Step output streaming (not just file commands)

**What changes:** Official runner reads `GITHUB_OUTPUT` file at step completion. aksh could support real-time output streaming via a Unix socket or pipe.
**Deviation:** Outputs would be visible to the server mid-step, not just after step completion.
**Benefit:** Long-running steps can report intermediate results. Agents can react to partial outputs without waiting for the step to finish. Critical for AI agent workflows.
**Risk:** Very low — the final output values are identical; only the timing of visibility changes.

#### B03: Extended `runs-on` with resource requests

**What changes:** `runs-on` currently takes labels or `{ group, labels }`. Allow `{ labels, resources: { cpu: 4, memory: "8G", gpu: true } }`.
**Deviation:** Official runner ignores the `resources` key (unknown fields are ignored by serde). The server uses it for scheduling.
**Benefit:** Resource-aware scheduling — route GPU-intensive agent workloads to appropriate runners. Forward-compatible (GitHub could adopt the same schema).
**Risk:** Minimal — extra keys in `runs-on` are silently ignored by the official runner and GitHub.

#### B04: Step-level concurrency

**What changes:** GitHub Actions has `concurrency` at workflow and job level. Allow `concurrency` at step level.
**Deviation:** Step-level `concurrency` key would be parsed but ignored by GitHub. Aksh would enforce it.
**Benefit:** Serialize access to shared resources (databases, ports, GPUs) across parallel jobs without serializing entire jobs.
**Risk:** Low — the key is ignored by GitHub, so workflows remain portable. Steps in GitHub just don't get the serialization.

#### B05: Expression-based step retry

**What changes:** Add `retry: { max: 3, on: failure(), delay: "5s" }` to steps. Official runner has no retry mechanism.
**Deviation:** New step-level key; ignored by GitHub (unknown keys in step are silently dropped).
**Benefit:** Eliminates boilerplate retry wrappers. Essential for flaky integration tests in agent workflows.
**Risk:** Low — workflows degrade gracefully on GitHub (no retries, runs once as normal).

### 2.2 Protocol extensions

#### B06: Binary log transport

**What changes:** Replace text log upload (PUT to signed blob URL) with optional binary framing (length-prefixed protobuf or MessagePack).
**Deviation:** Server-side only — the runner still uploads text. The server could accept both formats via content-type negotiation.
**Benefit:** 30-50% reduction in log storage and transfer for large builds. Structured log fields (timestamp, level, step) preserved without parsing.
**Risk:** Minimal if content-type negotiated — official runner sends text, aksh-runner can send binary.

#### B07: Job-level environment inheritance

**What changes:** Allow `env:` at the `runs-on` / runner-provider level, inherited by all jobs on that runner.
**Deviation:** Runner-level env vars would be visible to all steps without explicit `env:` in the workflow.
**Benefit:** Runner-fleet configuration without modifying workflows — inject proxy settings, tool paths, credentials per-runner-group.
**Risk:** Medium — could leak env vars into workflows that don't expect them. Should be opt-in via runner config.

#### B08: Workflow-level outputs and events

**What changes:** After all jobs complete, aksh emits a workflow-level completion event with aggregated outputs from all jobs, not just per-job `needs.*.outputs`.
**Deviation:** New event type; official runner doesn't emit workflow-level events.
**Benefit:** Agents can subscribe to a single event for workflow completion instead of tracking individual jobs. Enables workflow-to-workflow output passing.
**Risk:** Low — additive event; nothing breaks.

### 2.3 Performance optimizations

#### B09: Lazy action manifest parsing

**What changes:** Official runner downloads and parses all action manifests during "Set up job" before any step runs. aksh could lazily download/parse actions just before their step executes.
**Deviation:** Setup Job step would be faster but wouldn't catch action download failures upfront.
**Benefit:** Faster job startup for workflows with many actions where early steps might fail (fail-fast without downloading unused actions).
**Risk:** Medium — changes error timing. A missing action is caught at step execution instead of setup. Can be opt-in.

#### B10: Incremental workspace checkout

**What changes:** `actions/checkout` always does a full clone or shallow clone. aksh-runner could intercept the checkout action and provide an optimized path: git worktree from a persistent bare repo, or a filesystem snapshot.
**Deviation:** The checkout action itself is unmodified, but the workspace state it produces may differ (same content, different git history depth).
**Benefit:** Sub-second checkout for large repos. Critical for agent workflows that run frequently.
**Risk:** Low — workspace content is identical; only `.git` internals differ.

#### B11: Zero-copy step log capture

**What changes:** Official runner captures stdout/stderr to in-memory buffers, then writes to files, then uploads. aksh could `tee` directly from the process to both the log file and the upload stream.
**Deviation:** None observable — same log content, same upload format.
**Benefit:** Lower memory usage for steps with large output (build logs). No double-buffering.
**Risk:** None — this is a pure implementation optimization.

### 2.4 Agent-first features

#### B12: Step-level MCP tool registration

**What changes:** Steps can declare `tools:` in their action manifest, registering MCP-compatible tools that downstream steps (or agent orchestrators) can invoke.
**Deviation:** New action manifest key; ignored by GitHub.
**Benefit:** CI steps become composable tool providers. An agent orchestrator can discover available tools from the workflow definition and invoke them during execution.
**Risk:** Low — additive manifest key.

#### B13: Workflow-as-conversation

**What changes:** A special `agent:` step type that sends a prompt to an LLM agent with the current workflow context (env, outputs, logs from previous steps) and executes the response as a script.
**Deviation:** New step type; not parseable by GitHub.
**Benefit:** Native LLM-in-the-loop CI. Agents can make decisions based on build output without external orchestration.
**Risk:** High — workflows using `agent:` steps are not portable to GitHub. Should be clearly marked as aksh-only.

#### B14: Parallel step execution within a job

**What changes:** Allow steps within a job to declare `parallel: true` or `depends-on: [step-id]` for intra-job parallelism.
**Deviation:** Step execution order changes from sequential to DAG-based.
**Benefit:** Massive speedup for jobs with independent steps (lint + test + build can run simultaneously).
**Risk:** High — fundamentally changes step execution semantics. Must be opt-in. Workflows relying on sequential step ordering would break.

#### B15: Deterministic replay

**What changes:** Record all external inputs (network, filesystem, time) during a run. Replay the same run deterministically without executing steps.
**Deviation:** No protocol deviation — this is a server-side feature.
**Benefit:** Debug CI failures by replaying the exact run that failed. Agents can analyze failures without re-running. Time-travel debugging.
**Risk:** None — additive feature, no protocol changes.

---

## 3. Granular File-Level Gap Analysis (v2.336.0)

This section maps specific files/modules in aksh against their official runner counterparts
at the most granular level.

### 3.1 Listener / broker polling (`crates/aksh-runner/src/listener/broker_listener.rs`)

| Line range | Feature | Official runner file | Status | Notes |
| --- | --- | --- | --- | --- |
| 100-300 | Main broker loop + cancel handling | `Runner.Listener/BrokerMessageListener.cs` | ✅ good | 9 message types, cancel timing, FIPS |
| 303-505 | Message dispatch + job overlap | `Runner.Listener/JobDispatcher.cs` | ✅ good | Cancel-immediately on overlap matches official |
| 505-570 | Error handling (unauthorized, session expired) | `Runner.Listener/BrokerMessageListener.cs` | ⚠️ partial | Status codes checked; `RunnerSessionInvalid` structured body not parsed (v2.336.0 #4556) |
| N/A | Ephemeral exit on ack job-not-found | `Runner.Listener/BrokerMessageListener.cs` | ❌ missing | v2.336.0 #4540: ack 404 → clean exit for ephemeral |
| N/A | Session file cleanup on errors | `Runner.Listener/BrokerMessageListener.cs` | ❌ missing | v2.336.0 #4551: delete `.session` on specific errors |
| N/A | Session conflict retry bypass | `Runner.Listener/Runner.cs` | ❌ missing | v2.336.0 #4557: don't cap retry on session conflict |

### 3.2 Worker / step execution (`crates/aksh-runner/src/worker/steps_runner.rs`)

| Line range | Feature | Official runner file | Status | Notes |
| --- | --- | --- | --- | --- |
| 24-200 | Sequential step execution | `Runner.Worker/StepsRunner.cs` | ✅ good | Condition eval, cancel semantics, timeout |
| 36-37 | `is_background` flag | `Runner.Worker/StepsRunner.cs` | ⚠️ partial | Flag exists, DAP skip works, but no `BackgroundStepCoordinator` |
| N/A | `BackgroundStepCoordinator` | `Runner.Worker/BackgroundStepCoordinator.cs` | ❌ missing | v2.336.0 #4482: drain + aggregate, explicit-cancel exclusion |

### 3.3 Job extension / env vars (`crates/aksh-runner/src/worker/job_extension.rs`)

| Line range | Feature | Official runner file | Status | Notes |
| --- | --- | --- | --- | --- |
| 51-200 | `GITHUB_*` env injection | `Runner.Worker/JobExtension.cs` | ✅ good | Comprehensive coverage of all known vars |
| N/A | `GITHUB_ARTIFACTS` / `GITHUB_ARTIFACTS_LIST` | `Runner.Worker/Handlers/FileCommandManager.cs` | ❌ missing | v2.336.0 #4527: new file commands |
| N/A | `ACTIONS_CACHE_MODE` | `Runner.Worker/Handlers/NodeScriptActionHandler.cs` | ❌ missing | v2.336.0 #4538: `actions_cache_mode` → env |
| N/A | Locked dependencies announcement | `Runner.Worker/JobExtension.cs` | ❌ missing | v2.336.0 #4546: log line from `ActionsDependencies` |

### 3.4 Actions resolution (`crates/aksh-runner/src/client/actions_download.rs`)

| Line range | Feature | Official runner file | Status | Notes |
| --- | --- | --- | --- | --- |
| 40-194 | `runnerresolve/actions` batch resolve | `Runner.Worker/ActionManager.cs` | ✅ good | Batch POST, 422 partial, bearer auth |
| N/A | `$/` self-repository reference | `Runner.Worker/ActionManager.cs` | ❌ missing | v2.336.0 #4457: `selfRepository` type, depth-aware resolution |

### 3.5 Completion (`crates/aksh-runner/src/worker/completion.rs`)

| Feature | Official runner file | Status | Notes |
| --- | --- | --- | --- |
| `completejob` body shape | `Runner.Worker/JobServerQueue.cs` | ✅ good | planId, jobId, conclusion, outputs, stepResults, annotations |
| Step result aggregation | `Runner.Worker/StepsRunner.cs` | ✅ good | Skipped steps omit action_name/type |
| Background step result exclusion | `Runner.Worker/BackgroundStepCoordinator.cs` | ❌ missing | v2.336.0 #4482 |

### 3.6 Server queue (`crates/aksh-runner/src/worker/server_queue.rs`)

| Feature | Official runner file | Status | Notes |
| --- | --- | --- | --- |
| Delta step updates | `Runner.Common/JobServerQueue.cs` | ✅ good | `dirty_keys` tracking, merge logic in `step_records.rs` |
| 500ms periodic drain | `Runner.Common/JobServerQueue.cs` | ✅ good | Background tokio task with `MissedTickBehavior::Skip` |
| Batch size matching | `Runner.Common/JobServerQueue.cs` | ⚠️ partial | 10 vs 5 updates in golden comparison; merge coalescing differs |

### 3.7 Protocol DTOs (`crates/aksh-gha-protocol/src/`)

| Module | Status | Notes |
| --- | --- | --- |
| `azdo/job.rs` (21.3 KB) | ✅ good | `AgentJobRequestMessage`, steps, variables, endpoints |
| `azdo/lifecycle.rs` (6.7 KB) | ✅ good | `RunnerServerSettings`, session, pool |
| `azdo/timeline.rs` (5.0 KB) | ✅ good | `TimelineRecord` with background step fields |
| `azdo/completion.rs` (3.3 KB) | ✅ good | Completion body |
| `azdo/context_data.rs` (8.3 KB) | ✅ good | AzDO typed-dictionary decode |
| `crypto.rs` (30.3 KB) | ✅ good | RSA-OAEP SHA1/SHA256, AES-CBC, FIPS mode |
| `masking.rs` (2.9 KB) | ✅ good | SecretString redaction |

### 3.8 Parser (`crates/aksh-gha-parser/src/`)

| Module | Status | Notes |
| --- | --- | --- |
| `models.rs` (28.6 KB) | ✅ good | Full workflow model, `run-name`, `concurrency`, `permissions` |
| `expand.rs` (22.3 KB) | ✅ good | Matrix expansion with IndexMap order |
| `dag.rs` (14.8 KB) | ✅ good | `needs` DAG with cycle detection |
| `trigger.rs` (11.6 KB) | ✅ good | All event types, filter patterns |
| `job_builder.rs` (28.3 KB) | ✅ good | Full `AgentJobRequestMessage` construction |
| `eval.rs` (9.6 KB) | ✅ good | Expression wiring into job builder |
| `matrix_expand.rs` (27.2 KB) | ✅ good | Include/exclude semantics |

### 3.9 Expressions (`crates/aksh-gha-expressions/src/`)

| Module | Status | Notes |
| --- | --- | --- |
| `evaluator.rs` (15.9 KB) | ✅ good | All 12 functions, type coercion, case-insensitive `==` |
| `lexer.rs` (5.4 KB) | ✅ good | Token types, `{{`/`}}` escaping |
| `expr_parser.rs` (8.6 KB) | ✅ good | Pratt parser, operator precedence |
| `context.rs` (4.5 KB) | ✅ good | Bracket access, `*` filter |
| `conditions.rs` (2.5 KB) | ✅ good | `success()`/`failure()`/`cancelled()`/`always()` |

### 3.10 Server routes (`crates/aksh-runner-server/src/routes.rs`)

Full route audit — 100+ routes covering:

| Category | Route count | Status | Notes |
| --- | --- | --- | --- |
| Connection/auth | 8 | ✅ good | connectionData, OAuth, registration |
| Runner lifecycle | 24 | ✅ good | Pool/agent/session CRUD, GHES prefixed |
| Message polling | 12 | ✅ good | Broker + legacy DistributedTask paths |
| Job reporting | 16 | ✅ good | Timeline, logs, console, finish job |
| Broker (modern) | 12 | ✅ good | session/message/acknowledge + acquire/renew/complete |
| Results Twirp | 8 | ✅ good | WorkflowStepsUpdate, signed blob URLs, log metadata |
| Cache v2 Twirp | 3 | ✅ good | Create/finalize/get download URL |
| Artifact v2 Twirp | 5 | ✅ good | Create/finalize/list/get-signed/delete |
| Blob store | 1 | ✅ good | PUT/GET for cache and artifact blobs |
| OIDC | 5 | ✅ good | Discovery, JWKS, token endpoint |
| Native API | 12 | ✅ good | Runs, logs, cancel, rerun, events, debug |
| Action download | 3 | ✅ good | Tarball download, runnerresolve |
| Cache/Artifact v1 | 4 | ⚠️ partial | Reserve/lookup/upload/commit stubs |

### 3.11 Concurrency (`crates/aksh-runner-server/src/concurrency.rs`)

| Feature | Status | Notes |
| --- | --- | --- |
| Group-based serialization | ✅ good | 722 LOC + 87 property tests |
| `cancel-in-progress` | ✅ good | Bool or expression |
| `queue: single` / `queue: max` | ✅ good | Up to 100 pending |
| Scope-aware expression eval | ✅ good | github/inputs/vars + needs/strategy/matrix at job level |
| FIFO ordering | ✅ good | By wait-start time |
| Reusable workflow `EmbeddedConcurrency` | ✅ good | Propagated correctly |

---

## 4. Implementation Priority for Agent CI Platform

Recommended order for implementing opportunities, grouped by impact on the agent CI use case:

### Phase 1: Foundation (enables basic agent workflows)
1. **A02** — Webhook-driven dispatch (agents push code, CI runs automatically)
2. **A01** — Persistent run store (agents need run history for analysis)
3. **A04** — Structured NDJSON agent feed (agents consume events, not logs)

### Phase 2: Performance (makes agent workflows practical)
4. **A08** — Action caching (sub-second startup for repeated runs)
5. **A07** — Parallel matrix scheduling (agent test matrices)
6. **B11** — Zero-copy log capture (free performance win)

### Phase 3: Agent-native features (differentiators)
7. **B02** — Step output streaming (agents react to intermediate results)
8. **B12** — MCP tool registration (CI steps as agent tools)
9. **B15** — Deterministic replay (debug without re-running)
10. **A05** — DAP expression breakpoints (interactive CI debugging)

### Phase 4: Scale and isolation
11. **A03** — Runner group routing (multi-tenant)
12. **A14** — Process/cgroup isolation (safety for untrusted agent code)
13. **B03** — Resource-aware scheduling (GPU routing for ML agents)
14. **A11** — Retention policies (disk management)

---

## 5. Compatibility Risk Assessment

| Risk level | Count | Examples |
| --- | --- | --- |
| **No risk** (pure additive) | 11 | A01-A14 (server/runner internals, no protocol change) |
| **Minimal risk** (unknown keys ignored) | 5 | B03, B04, B05, B08, B12 (new YAML keys, silently dropped by GitHub) |
| **Low risk** (timing/behavior subtle) | 4 | B01, B02, B09, B10 (value types, output timing, download order) |
| **Medium risk** (semantic change) | 2 | B07, B13 (env inheritance, new step type) |
| **High risk** (execution model change) | 1 | B14 (parallel steps) |

All "slightly breaking" features should be gated behind explicit opt-in configuration
(workflow metadata or runner config), ensuring the default behavior matches the official
runner exactly.
