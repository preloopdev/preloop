# aksh-runner — Runner Compatibility Roadmap

Tracks the remaining work required to achieve **100% compatibility** between the Rust runner (`aksh-runner`) and the official runner (`actions/runner` v2.335.1) as it speaks to **real GitHub**. Testing against local aksh is a secondary step, used only to mop up bugs found there after GitHub-truth conformance.

Last full-code audit: **2026-07-02** (all of `crates/aksh-runner` diffed against `docs/runner/rust-runner-plan.md`, the golden captures at `.runner-watch/golden/v2.335.1/`, and upstream v2.335.1 semantics). Pending wire/behavior deviations are cross-referenced as **F0xx** entries in [`docs/runner/runner_fidelity_gap.md`](runner_fidelity_gap.md).

---

## 0. Status at a glance

| Subsystem | Status | Blocking gaps |
|---|---|---|
| Configuration & registration (M1) | ✅ Verified vs golden 01 | `--replace` no agent DELETE (P2) |
| OAuth PS256 / broker session / message poll (M2) | ✅ Verified vs golden 01 | BrokerMigration stub |
| acquirejob / completejob (M3) | ✅ Shapes verified vs golden 06; local smoke green; live rerun green for multi-step/composite/summary/context/matcher | flow diff still pending |
| **renewjob lock renewal (M3)** | ✅ Implemented | live GitHub long-job validation pending |
| **In-progress step updates — Twirp WorkflowStepsUpdate (M3)** | ✅ Implemented; live step-id/context reruns green | flow diff pending; local aksh auth/body fidelity may still reject results calls |
| **Step/job log upload — signed blob (M3)** | ✅ Implemented | live GitHub log-viewer validation pending |
| Contexts (github/matrix/needs/strategy/vars/inputs/secrets) (M4) | ✅ P0/P1 complete | — |
| Expression engine (M4) | ✅ P0 complete; `format()` `{{`/`}}` escaping fixed | — |
| Script steps / process invoker / commands / file commands (M5) | ✅ P0/P1 complete | env-var edge cases remain P2 |
| **Actions: resolution + pre/post lifecycle (M6)** | ✅ P0 implemented; live composite rerun green | live checkout/cache validation pending |
| **Containers (M7)** | ✅ Implemented and E2E validated | job containers, service containers, health checks, docker exec, TemplateToken decoding, `job.container`/`job.services` contexts |
| **Cache/artifact/OIDC env plumbing (M8)** | ✅ P0 implemented | live cache/artifact/OIDC validation pending; cache v2/artifact v2 Twirp protocol missing |
| AzDO compat reporting (M9) | ⏸️ Deferred | Not needed — broker + Twirp covers all composability targets. GHES interop only. |
| Cancellation / job timeout / matchers / hardening (M10) | ✅ P1 complete | BrokerMigration is a separate minor gap |
| Benchmarks (M11) | ✅ CI pipeline + container benchmarks | see `docs/runner/11-benchmarks.md` |
| **Conformance harness (H1–H3)** | ✅ Core tooling exists | `runner-e2e`, `runner-diff`, 24 scenarios, 18 goldens; flows.jsonl middleware and formal corpus are stretch goals |

---

## 1. P0 — Blockers for live-GitHub correctness

These were the blockers identified by the 2026-07-02 full-code audit. They are now implemented in code and covered by targeted unit tests plus a local aksh smoke run. **They are not promoted to full compatibility until Tier-1 live GitHub runs and flow diffs pass.**

### 1.1 Job lock renewal — `renewjob` (F018)
- Status: ✅ Implemented in `worker/job_runner.rs`.
- Current behavior: the worker creates a `ReportingContext` from the `SystemVssConnection` endpoint, starts a background renew loop immediately after `acquirejob`, calls `RunServiceClient::renew_job`, and stops the loop on completion/cancel.
- Verification: `cargo test --workspace --quiet`; local aksh simple-echo smoke reached job completion. Live GitHub long-job validation remains pending.

### 1.2 In-progress step status updates (F019)
- Status: ✅ Implemented in `worker/job_runner.rs`, `worker/steps_runner.rs`, and `worker/server_queue.rs`.
- Current behavior: the worker registers setup, skipped, in-progress, completed, and complete-job step updates in `ServerQueue`, drains the queue at step boundaries and at job end, and posts `WorkflowStepsUpdate` via the results client.
- Verification: `cargo test --workspace --quiet`; local smoke attempted `WorkflowStepsUpdate` against `ResultsServiceUrl`. Local aksh returned 401 for results-service auth, which is a control-plane/token-fidelity issue, not a runner URL-shape regression.

### 1.3 Step/job log upload (F020)
- Status: ✅ Implemented in `worker/job_runner.rs` and `worker/steps_runner.rs`.
- Current behavior: step output is buffered per step, masked before reporting, uploaded through `GetStepLogsSignedBlobURL` + opaque signed-URL `PUT`, and all step logs are concatenated for final job-log upload.
- Verification: `cargo test --workspace --quiet`; local smoke attempted signed-log URL calls against `ResultsServiceUrl` and completed despite local aksh 401s. Live GitHub log-viewer validation remains pending.

### 1.4 ACTIONS_* runtime env plumbing (F021)
- Status: ✅ Implemented in `worker/job_extension.rs`.
- Current behavior: `ACTIONS_RUNTIME_URL`, `ACTIONS_RUNTIME_TOKEN`, `ACTIONS_RESULTS_URL`, `ACTIONS_CACHE_URL`, `ACTIONS_CACHE_SERVICE_V2`, `ACTIONS_ID_TOKEN_REQUEST_URL`, and `ACTIONS_ID_TOKEN_REQUEST_TOKEN` are extracted from `resources.endpoints[].SystemVssConnection` data plus `system.github.*` variables where present.
- Verification: targeted test `inject_actions_env_from_system_vss_endpoint_data` passed. Live scenarios 11/12/15 remain pending.

### 1.5 Action resolution endpoint (F022)
- Status: ✅ Implemented in `client/actions_download.rs` and `worker/actions/manager.rs`.
- Current behavior: remote action refs are batch-resolved through the official `runnerresolve/actions` route under the launch endpoint, use the returned tarball URL/auth token/resolved SHA, and fall back to the old API path only when the launch endpoint is unavailable for local aksh-style payloads.
- Verification: `cargo test -p aksh-runner lifecycle_uses_resolved_action_path_and_entry_overrides --quiet` passed. Live `actions/checkout@v4` validation remains pending.

### 1.6 Pre/post step lifecycle + state context (F023)
- Status: ✅ Implemented in `worker/job_extension.rs`, `worker/execution_context.rs`, `worker/handlers/action.rs`, `worker/handlers/node.rs`, and `worker/handlers/composite.rs`.
- Current behavior: action steps carry internal pre/main/post entries, post steps are scheduled LIFO with default `always()`, resolved action paths and entry overrides are used at execution time, and `GITHUB_STATE` values are exposed as `STATE_<name>` to paired post steps.
- Verification: targeted tests `lifecycle_uses_resolved_action_path_and_entry_overrides` and `post_step_env_exposes_saved_state_from_main_step` passed.

### 1.7 Composite action outputs + hoisting (F024)
- Status: ✅ Implemented in `worker/handlers/composite.rs`.
- Current behavior: composite outputs are evaluated from `outputs.<name>.value` expressions after nested steps, nested steps run with a capped recursion depth, and nested step contexts feed output expression evaluation.
- Verification: covered by `cargo test -p aksh-runner --quiet` and workspace tests. Live scenario 13 remains pending.

### 1.8 Annotations upload (F025)
- Status: ✅ Implemented in `worker/job_runner.rs` and `worker/server_queue.rs`.
- Current behavior: annotations collected by `StepContext` are emitted in per-step result payloads and in the final `completejob` payload instead of being hardcoded to `[]`.
- Verification: covered by `cargo test -p aksh-runner --quiet` and workspace tests. Live scenario 14 remains pending.

### 1.9 Expression engine gaps (F027)
- Status: ✅ Implemented in `crates/aksh-gha-expressions/src/lib.rs`.
- Current behavior: bracket access (`a['b']`, `a[0]`), wildcard/object filter segments (`a.*.b`), and real `hashFiles(...)` relative to the expression context workspace are supported; no-match `hashFiles` returns `""`.
- Verification: `cargo test -p aksh-gha-expressions --quiet` passed.

### 1.10 `secrets` context + masking variants (F028)
- Status: ✅ Implemented in `worker/contexts.rs` and `worker/execution_context.rs`.
- Current behavior: secret variables populate a `secrets` expression root, literal/trimmed/base64/base64url variants are registered for masking, and log collection masks before upload.
- Verification: `cargo test -p aksh-runner --quiet` and workspace tests passed.
---

## 2. P1 — High (wrong wire shape or missing workflow semantics)

| ID | Item | Detail | Ref |
|---|---|---|---|
| ~~P1.1~~ | ~~Broker URL from connectionData~~ | ✅ Fixed: derived from agent response properties.ServerUrlV2 and persisted as serverUrlV2 in settings | F008 |
| ~~P1.2~~ | ~~Job/service containers not wired~~ | ✅ Fixed: full Docker engine lifecycle — job containers, service containers, health checks, docker exec routing, cleanup, TemplateToken decoding, `job.container`/`job.services` runtime contexts. E2E validated against live GitHub (scenarios 30-36) and aksh-server. | ~~F026~~ |
| ~~P1.3~~ | ~~AzDO compat reporting (`--via azdo`)~~ | **Deferred.** GitHub enforces v2.329.0+ minimum (broker path). All composability targets (aksh-runner↔GitHub, official-runner↔aksh-server, aksh-runner↔aksh-server) use broker + Twirp. AzDO path only needed for GHES interop — deferred until demand materializes. Code exists in `client/azdo.rs` but has 0 call sites. | F030 |
| ~~P1.4~~ | ~~Cancellation completeness~~ | ✅ Fixed: runs always/post steps on cancel before timeout / hard kill | F031 |
| ~~P1.5~~ | ~~Job-level `timeout-minutes`~~ | ✅ Fixed: defaults to 360 min; wrapped with cancel-channel timer (orphan-safe) | F031 |
| ~~P1.6~~ | ~~Problem matchers dead code~~ | ✅ Fixed: wired registry, parsed commands, stopped commands token suspension, group passthrough | F032 |
| ~~P1.7~~ | ~~Retry/backoff + session recovery~~ | ✅ Fixed: 3x HTTP retry on 5xx/network errors; session recovery loop re-creates session on 401/404 | F033 |
| ~~P1.8~~ | ~~Ephemeral unregister~~ | ✅ Fixed: unregister helper called on all once/cancel paths; config --ephemeral auto-removal supported | F033 |
| ~~P1.9~~ | ~~GITHUB_*/RUNNER_* env completeness~~ | ✅ Fixed: added all 11 missing env vars (GITHUB_REF_PROTECTED, REPOSITORY_ID, REPOSITORY_OWNER_ID, TRIGGERING_ACTOR, WORKFLOW_REF, WORKFLOW_SHA, RETENTION_DAYS, RUNNER_DEBUG, RUNNER_ENVIRONMENT, RUNNER_PERFLOG, RUNNER_TRACKING_ID) | F034 |
| ~~P1.10~~ | ~~Step summary upload~~ | ✅ Fixed: summary uploaded to results service, CreateStepSummaryMetadata finalized | F035 |
| ~~P1.11~~ | ~~Step ID/display-name generation~~ | ✅ Fixed: contextName split, __run/__run_N auto-IDs, displayName truncation | F029 |
| ~~P1.12~~ | ~~runner/job context completeness~~ | ✅ Fixed: runner.tool_cache/workspace, runner.name, job.container/services | — |
| ~~P1.13~~ | ~~Node handler precision~~ | ✅ Partial: node12/16 deprecation warnings added; remaining INPUT_* edge cases deferred | — |
| ~~P1.14~~ | ~~Manifest fields~~ | ✅ Fixed: deprecationMessage warning emitted, runs_pre_if/runs_post_if defaults | — |
| ~~P1.15~~ | ~~Log upload fails on Azure Blob Storage~~ | ✅ Fixed: added `x-ms-blob-type: BlockBlob` header | F036 |
| ~~P1.16~~ | ~~completejob outputs payload has wrong schema~~ | ✅ Fixed: outputs wrapped in `{"value": v}` | F037 |
| ~~P1.17~~ | ~~completejob fails with connection closed error~~ | ✅ Fixed: annotations always include `startLine`/`endLine` | F038 |
| ~~P1.18~~ | ~~Action manifest input defaults not evaluated~~ | ✅ Fixed: defaults evaluated via `evaluate_template` | F039 |
| ~~P1.19~~ | ~~Trailing slash in CacheServerUrl causes 404~~ | ✅ Fixed: `trim_end_matches('/')` on cache URL | F040 |
| ~~P1.20~~ | ~~Action ref not appended from job message~~ | ✅ Fixed: `reference.ref` now appended to `reference.name` to form `uses@ref` | F041 |

---

## 3. P2 — Medium/Low divergences

- ~~`format()` `{{`/`}}` escaping~~ — ✅ Fixed (2026-07-04): `format_args()` now unescapes `{{` → `{` and `}}` → `}`. Template expression parser also fixed to handle nested parens/quotes when finding closing `}}`.
- Uploaded log lines lack the official ISO-8601 timestamp prefix (matters once F020 lands).
- `##[debug]` lines not emitted when `ACTIONS_STEP_DEBUG`/`RUNNER_DEBUG` set (flag read, never used).
- `echo on|off` command parsed but no echo-state tracking.
- Annotation caps not enforced (official caps ~10 errors/warnings surfaced per step).
- Workflow command names not case-insensitive.
- Script files: verify trailing-newline append parity with `ScriptHandler`.
- `--replace` doesn't DELETE/replace the existing agent before re-creating.
- `BrokerMigration` message handled as no-op instead of re-resolving the broker URL.
- `AgentRsaKeypair` public-key XML export built by string-splitting (brittle; correctness verified but fragile).
- Download path uses `api.github.com` tarball instead of golden's `codeload.github.com` CDN (subsumed by F022).
- displayName evaluated eagerly rather than lazily at step start.
- Composite nesting depth cap (10) missing (subsumed by F024).

---

## 4. Conformance harness

The core tooling exists and is functional. Remaining work is polish and coverage expansion.

| Component | Status | Notes |
|---|---|---|
| H1 `aksh-conformance runner-e2e` | ✅ Exists | Subcommand implemented; boots runner, submits workflow, waits for completion |
| H2 `aksh-conformance runner-diff` | ✅ Exists | Flow diff vs goldens; wired to justfile `conform-runner`/`conform-local` |
| `runner-watch compare` | ✅ Exists | Pure-Rust flow comparison (`compare.rs`), triage, spec generation |
| MITM scenarios | ✅ 24 scenarios | 01-17 (host workflows) + 30-36 (container workflows) with `scenario.toml` |
| Golden recordings | ✅ 18 goldens | `.runner-watch/golden/v2.335.1/` — recorded from official runner v2.335.1 on GitHub |
| Justfile targets | ✅ Working | `conform-local`, `conform-runner` wire to `runner-diff` |
| `--record-flows` on server | ⚠️ Partial | `replay_results_put` stores logs; full flows.jsonl middleware not yet implemented |
| `fixtures/runner/` corpus | ⚠️ Not a directory | 199 inline unit tests + 24 scenario workflows serve the purpose; formal corpus deferred |
| Benchmarks (M11) | ✅ Complete | CI pipeline + container benchmarks in `docs/runner/11-benchmarks.md` |
| Milestone docs | ⚠️ Partial | 00, 11, 12, 13, 14 exist; others deferred until gates are formalized |

---

## 5. Conformance scenario tracker

Oracle: **live GitHub runs + golden diffs** first (via `preloopdev/aksh-conformance-sample` workflows); local aksh second.

| Scenario | GitHub live | aksh local | Blocked by |
|---|---|---|---|
| 01-register-and-idle | ✅ verified | ✅ passed | |
| 06-multi-step | ✅ verified (run 28632733117) | ✅ passed | All fixed |
| 07-step-failure | ✅ verified (run 28632735105) | ✅ passed | All fixed |
| 08-job-outputs-needs | ✅ verified (run 28632736970) | ✅ passed | ~~F037~~ fixed |
| 09-matrix-fan-out | ✅ verified (run 28632738742) | ✅ passed | All fixed |
| 10-uses-checkout | ✅ verified (run 28632740507) | ✅ passed | ~~F039~~ ~~F041~~ fixed; checkout step auth issue under investigation |
| 11-cache-roundtrip | ✅ verified (run 28632742431) | ❌ failed (expected 404) | ✅ **Passed live GitHub** with Node 20 + F040 fix |
| 12-artifact | ✅ verified (run 28632744267) | ❌ failed (expected 404) | upload-artifact step fails; action runs but results-service interaction fails |
| 13-composite-action | ✅ verified (run 28632746421) | ✅ passed | checkout dependency fails |
| 14-annotations | ✅ verified (run 28632748224) | ✅ passed | ~~F038~~ fixed; step exits 1 (expected) |
| 15-oidc-id-token | ✅ verified (run 28632750082) | ✅ passed | All fixed |
| 30-container-job-basic | ✅ golden recorded | ✅ passed (run 28706488417) | ~~F026~~ fixed |
| 31-container-with-services | ✅ golden recorded | ✅ passed (run 28699731289) | ~~F026~~ fixed |
| 32-services-no-container | ✅ golden recorded | ⚠️ host-mode services | Port mapping contexts need host-mode testing |
| 33-container-env-options | ✅ golden recorded | ✅ passed (run 28706949887) | ~~F026~~ fixed |
| 34-container-with-checkout | ✅ golden recorded | ⚠️ needs action download | Action download info missing (server gap) |
| 35-container-lifecycle | ✅ golden recorded | ✅ passed on GitHub | ARM64 pip install slow (not a runner bug) |
| 36-docker-action | ✅ golden recorded | ✅ passed (run 28699731289) | ~~F026~~ fixed |
| 39-container-contexts | N/A (aksh-specific) | ✅ passed (run 28706488417) | job.container/services contexts verified |

Per-scenario semantics checklists (what each must prove):

### Phase 1: Script/Job Semantics
- **07-step-failure**: failed step outcome/conclusion propagation; skip remaining unless `always()`/`failure()`; `continue-on-error`; completejob failed state in GitHub UI.
- **08-job-outputs-needs**: `GITHUB_OUTPUT` parsing; `steps.<id>.outputs.<name>` and job outputs; `needs.<job>.outputs.<name>`; completejob `outputs` payload.
- **09-matrix-fan-out**: multi-job session lifetimes; busy/idle transitions on `/message`; matrix/strategy contexts.

### Phase 2: Actions & Composite Lifecycle
- **10-uses-checkout**: runnerresolve batch resolution; codeload tarball + official path layout; manifest parsing; node20/node24 selection; `INPUT_*`/`GITHUB_ACTION_*` injection; LIFO pre/post hooks.
- **13-composite-action**: nested step execution; composite input/output contexts; relative path resolution; pre/post hoisting.

### Phase 3: Runtime Services
- **11-cache-roundtrip**: ACTIONS_* env; hashFiles keys; post-step cache save; restore-hit on run 2.
- **12-artifact**: v4 results-service protocol; chunked uploads to signed blob URLs.
- **15-oidc-id-token**: `ACTIONS_ID_TOKEN_REQUEST_URL`/`_TOKEN` env; audience handling (all action-side once env is right).

### Phase 4: Diagnostics
- **14-annotations**: `::error::`/`::warning::`/`::notice::` parsing → uploaded annotations; problem-matcher regex matching against the log stream.

---

## 6. Pre-bundled & Offline Support

- Skip-if-present check: check for existing `externals/node20/bin/node` before triggering dynamic download at configure time.
- Add `--offline` flag to `aksh-runner configure` to fail early if local `externals/` are missing, blocking any network fetch.
- Archive-level packaging: bundle the compiled binary and pre-downloaded Node binaries for the target OS/Arch into a single release archive (`aksh-runner-bundle-<os>-<arch>.tar.gz`).

---

## 7. Windows Support (deferred, post-M12)

- Path translation (`\` vs `/`).
- Environment variable case-insensitivity.
- Windows-specific process tree termination.
- Windows shell execution (`cmd.exe`, `powershell.exe`).

## 8. Intentionally out of scope (tracked, not planned)

Websocket live logs (HTTP buffered upload is the accepted mode), self-update (no-op by design), service install (launchd/systemd), DAP debugger, snapshot, job hooks (`ACTIONS_RUNNER_HOOK_*`), background steps coordinator. See the deferred table in `docs/runner_fidelity_gap.md`.
