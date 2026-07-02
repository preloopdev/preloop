# aksh-runner — Runner Compatibility Roadmap

Tracks the remaining work required to achieve **100% compatibility** between the Rust runner (`aksh-runner`) and the official runner (`actions/runner` v2.335.1) as it speaks to **real GitHub**. Testing against local aksh is a secondary step, used only to mop up bugs found there after GitHub-truth conformance.

Last full-code audit: **2026-07-02** (all of `crates/aksh-runner` diffed against `docs/runner/rust-runner-plan.md`, the golden captures at `.runner-watch/golden/v2.335.1/`, and upstream v2.335.1 semantics). Pending wire/behavior deviations are cross-referenced as **F0xx** entries in [`docs/runner/runner_fidelity_gap.md`](runner_fidelity_gap.md).

---

## 0. Status at a glance

| Subsystem | Status | Blocking gaps |
|---|---|---|
| Configuration & registration (M1) | ✅ Verified vs golden 01 | `--replace` no agent DELETE (P2) |
| OAuth PS256 / broker session / message poll (M2) | ✅ Verified vs golden 01 | connectionData unparsed → broker URL (F008); BrokerMigration stub; no retry/backoff |
| acquirejob / completejob (M3) | ✅ Shapes verified vs golden 06; local smoke green | live GitHub flow diff pending |
| **renewjob lock renewal (M3)** | ✅ Implemented | live GitHub long-job validation pending |
| **In-progress step updates — Twirp WorkflowStepsUpdate (M3)** | ✅ Implemented | live GitHub flow diff pending; local aksh auth/body fidelity may still reject results calls |
| **Step/job log upload — signed blob (M3)** | ✅ Implemented | live GitHub log-viewer validation pending |
| Contexts (github/matrix/needs/strategy/vars/inputs/secrets) (M4) | ✅ P0 complete | runner.tool_cache/workspace still incomplete |
| Expression engine (M4) | ✅ P0 complete | `format()` `{{`/`}}` escaping remains P2 |
| Script steps / process invoker / commands / file commands (M5) | ✅ P0 complete | summary upload (F035), env-var completeness (F034) |
| **Actions: resolution + pre/post lifecycle (M6)** | ✅ P0 implemented | live checkout/cache/composite validation pending |
| **Containers (M7)** | ❌ Helpers exist, never wired | F026 |
| **Cache/artifact/OIDC env plumbing (M8)** | ✅ P0 implemented | live cache/artifact/OIDC validation pending |
| AzDO compat reporting (M9) | ❌ Dispatch only; reporting endpoints have 0 call sites | F030 |
| Cancellation / job timeout / matchers / hardening (M10) | ⚠️ Partial | F031, F032, F033 |
| Benchmarks (M11) | ⚠️ Size + cold start only | dispatch latency, throughput, RSS missing |
| **Conformance harness (H1–H3)** | ❌ `runner-e2e`, `runner-diff`, `--record-flows`, `fixtures/runner/` all missing | §4 |

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
| P1.1 | Broker URL from connectionData | Still falls back to `.runner` `serverUrl`; must parse `connectionData` serviceDefinitions/location mappings for `broker.actions.githubusercontent.com` (golden 01 flow 11) | F008 (pending) |
| P1.2 | Job/service containers not wired | `container_ops.rs` (network create, start, health poll, path translation) is **dead code**; `job_runner.rs` never inspects the message's container resources; `handlers/script.rs` never takes the `docker exec` path; service containers have zero code | F026 |
| P1.3 | AzDO compat reporting (`--via azdo`) | `client/azdo.rs` has `patch_agent_request`, `update_timeline`, `create_log`/`append_log`, `post_console_log`, `finish_job` — **all 0 call sites**; `report_completion()` builds a non-`JobCompletedEvent` shape; `TimelineRecord` missing `order` population | F030 |
| P1.4 | Cancellation completeness | On cancel: remaining steps are not re-evaluated under `cancelled()` semantics (`always()` steps and post steps don't run), no grace window before hard kill in `job_dispatcher::kill()`; official runs always/post steps then reports Canceled | F031 |
| P1.5 | Job-level `timeout-minutes` | Default 360 min never read or enforced (step-level timeout works) | F031 |
| P1.6 | Problem matchers dead code | `matchers.rs` registry/matching exists but has **no call sites**: `::add-matcher::`/`::remove-matcher::` not wired in `commands.rs`, log lines never fed through, multi-line `loop:` patterns unimplemented, `fromPath`/`defaultSeverity` untested | F032 |
| P1.7 | Retry/backoff + session recovery | No retry on any HTTP call site (official: transient 5xx ×3 exponential, `ErrorThrottler`); no session re-create on 401/session-gone mid-poll; listener dies on server restart | F033 |
| P1.8 | Ephemeral unregister | `--once` exits but never DELETEs the agent registration (official ephemeral runners unregister) | F033 |
| P1.9 | GITHUB_*/RUNNER_* env completeness | Missing: `GITHUB_REF_PROTECTED`, `GITHUB_REPOSITORY_ID`, `GITHUB_REPOSITORY_OWNER_ID`, `GITHUB_TRIGGERING_ACTOR`, `GITHUB_WORKFLOW_REF`, `GITHUB_WORKFLOW_SHA`, `GITHUB_RETENTION_DAYS`, `RUNNER_DEBUG`, `RUNNER_ENVIRONMENT`, `RUNNER_PERFLOG`, `RUNNER_TRACKING_ID` (28/39 of official set injected) | F034 |
| P1.10 | Step summary upload | `GITHUB_STEP_SUMMARY` file created + size-capped but never uploaded to the results service | F035 |
| P1.11 | Step ID/display-name generation | No `__run`/`__run_2` auto-ID for id-less steps, no display-name fallback (action ref / script preview); step naming must match official for wire parity in step updates | F029 |
| P1.12 | `runner`/`job` context completeness | `runner.tool_cache` and `runner.workspace` missing (4/6 fields); `job` context only has `status` (no `container`/`services`) | — |
| P1.13 | Node handler precision | INPUT_* set even for inputs with no value and no default (official omits); `NODE_OPTIONS` not merged; node12/16 deprecation warning only in dispatcher, not handler; no `ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION` | — |
| P1.14 | Manifest fields | `inputs.*.deprecationMessage` not surfaced as warning; `outputs.*.value` not extracted (see F024); `pre-if`/`post-if` parsed but no `always()` default applied | — |

---

## 3. P2 — Medium/Low divergences

- `format()` `{{`/`}}` escaping not handled (`aksh-gha-expressions`).
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

## 4. Conformance harness — the gating infrastructure (all missing)

The plan gates every milestone on this harness; none of it exists yet, so **no Tier-1/Tier-2 gate has ever actually run**. Rebuilding it is a prerequisite for calling anything "conformant".

| Component | Plan | Reality | Work |
|---|---|---|---|
| H1 `aksh-conformance runner-e2e` | E2E orchestrator, `--target aksh\|github`, verdict JSON | **Missing** — no such subcommand in `crates/aksh-conformance/src/main.rs` | Implement `runner_e2e.rs` per plan §H1 |
| H2 `aksh-conformance runner-diff` | Flow diff vs goldens via `runner_watch::compare::render_report`, writes `.runner-watch/runner-conformance/<name>.md` | **Missing** | Implement per plan §H2; `runner-watch` `lib.rs` export already done |
| H2 `--record-flows` on `aksh-runner-server serve` | Local flows.jsonl capture middleware | **Missing** | Axum middleware, existing flows.jsonl schema |
| H3 `fixtures/runner/` corpus | config/, commands/, filecommands/, matchers/, expressions/, env-parity.yml | **Missing** (50+ inline unit tests exist instead) | Create corpus; port upstream L0 cases |
| Justfile | `runner-e2e`, `conform-runner`, `conform-local` | Targets exist but **fail at runtime** (subcommands absent) | Fix by landing H1/H2 |
| `scripts/bench-runner.sh` (M11) | configure time, cold start→first poll, dispatch latency ×10, throughput ×20, RSS, size ± externals | Only binary size + `--version` cold start | Extend; check `e2e-setup.sh --status` before official phases |
| Milestone docs 01–10 | One spec/evidence doc per milestone | **Missing** (only 00, 11, 12 exist) | Write with real gate evidence as gaps close |
| `.runner-watch/runner-conformance/` | One report per scenario | **Empty (0/11)** | Generated by runner-diff once it exists |

---

## 5. Conformance scenario tracker

Oracle: **live GitHub runs + golden diffs** first (via `preloopdev/aksh-conformance-sample` workflows); local aksh second.

| Scenario | GitHub live | aksh local | Blocked by |
|---|---|---|---|
| 01-register-and-idle | ✅ verified (code audit vs golden; no checked-in report) | ✅ | report generation (§4) |
| 06-multi-step | ⚠️ pending live rerun | ✅ local smoke green; results-service calls now target `ResultsServiceUrl` but aksh returns 401 | live GitHub report/log diff |
| 07-step-failure | ⚠️ pending live rerun | ⚠️ targeted semantics unit-covered | live GitHub failure/reporting diff |
| 08-job-outputs-needs | ⚠️ pending live rerun | ⚠️ targeted semantics unit-covered | live GitHub outputs/needs diff |
| 09-matrix-fan-out | ⚠️ pending live rerun | ⚠️ not rerun after F018/F019 | live GitHub multi-job renew/status diff |
| 10-uses-checkout | ⚠️ pending live rerun | ⚠️ local fallback path only | live GitHub runnerresolve + checkout post cleanup |
| 11-cache-roundtrip | ⚠️ pending live rerun | ⚠️ needs aksh cache/results services | live GitHub cache restore/save |
| 12-artifact | ⚠️ pending live rerun | ⚠️ needs aksh artifact/results services | live GitHub artifact round-trip |
| 13-composite-action | ⚠️ pending live rerun | ⚠️ targeted semantics unit-covered | live GitHub composite output/hoisting diff |
| 14-annotations | ⚠️ pending live rerun | ⚠️ targeted semantics unit-covered | live GitHub annotation diff; matchers still F032 |
| 15-oidc-id-token | ⚠️ pending live rerun | ⚠️ needs aksh OIDC token surface | live GitHub OIDC token validation |
| 16-container-job (golden TBD) | ❌ | ❌ | F026; record official golden first |
| 17-service-container (golden TBD) | ❌ | ❌ | F026; record official golden first |
| 18-cancel-mid-step (golden TBD) | ❌ | ❌ | F031; record official golden first |

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
