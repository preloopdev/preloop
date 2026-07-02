# aksh-runner — Runner Compatibility Roadmap

Tracks the remaining work required to achieve **100% compatibility** between the Rust runner (`aksh-runner`) and the official runner (`actions/runner` v2.335.1) as it speaks to **real GitHub**. Testing against local aksh is a secondary step, used only to mop up bugs found there after GitHub-truth conformance.

Last full-code audit: **2026-07-02** (all of `crates/aksh-runner` diffed against `docs/rust-runner-plan.md`, the golden captures at `.runner-watch/golden/v2.335.1/`, and upstream v2.335.1 semantics). Pending wire/behavior deviations are cross-referenced as **F0xx** entries in [`docs/runner_fidelity_gap.md`](../runner_fidelity_gap.md).

---

## 0. Status at a glance

| Subsystem | Status | Blocking gaps |
|---|---|---|
| Configuration & registration (M1) | ✅ Verified vs golden 01 | `--replace` no agent DELETE (P2) |
| OAuth PS256 / broker session / message poll (M2) | ✅ Verified vs golden 01 | connectionData unparsed → broker URL (F008); BrokerMigration stub; no retry/backoff |
| acquirejob / completejob (M3) | ✅ Shapes verified vs golden 06 | — |
| **renewjob lock renewal (M3)** | ❌ Never called | F018 |
| **In-progress step updates — Twirp WorkflowStepsUpdate (M3)** | ❌ Queue exists, never flushed | F019 |
| **Step/job log upload — signed blob (M3)** | ❌ Client exists, never called | F020 |
| Contexts (github/matrix/needs/strategy/vars/inputs) (M4) | ✅ Mostly correct | `secrets` root missing (F028); runner.tool_cache/workspace missing |
| Expression engine (M4) | ⚠️ Partial | bracket access, `a.*.b` filter, `hashFiles()` stub (F027) |
| Script steps / process invoker / commands / file commands (M5) | ✅ Mostly correct | annotations upload (F025), summary upload (F035), env-var completeness (F034) |
| **Actions: resolution + pre/post lifecycle (M6)** | ❌ Resolution endpoint + pre/post missing | F022, F023, F024 |
| **Containers (M7)** | ❌ Helpers exist, never wired | F026 |
| **Cache/artifact/OIDC env plumbing (M8)** | ❌ ACTIONS_* vars never injected | F021 |
| AzDO compat reporting (M9) | ❌ Dispatch only; reporting endpoints have 0 call sites | F030 |
| Cancellation / job timeout / matchers / hardening (M10) | ⚠️ Partial | F031, F032, F033 |
| Benchmarks (M11) | ⚠️ Size + cold start only | dispatch latency, throughput, RSS missing |
| **Conformance harness (H1–H3)** | ❌ `runner-e2e`, `runner-diff`, `--record-flows`, `fixtures/runner/` all missing | §4 |

---

## 1. P0 — Blockers for live-GitHub correctness

These break real workflows on GitHub today. Order is the recommended implementation order.

### 1.1 Job lock renewal — `renewjob` (F018)
- Official: parses `lockedUntil`/lock duration from acquire, runs a background renew loop (interval = lock duration / 2, port of `JobDispatcher.cs`).
- Ours: `RunServiceClient::renew_job` exists (`client/run_service.rs`) but is **never called**; lock duration never parsed. Any job outrunning the initial lease is reassigned/failed by GitHub.
- Fix: spawn renew loop in `worker/job_runner.rs` (or listener dispatcher) for the life of the job; stop on completion/cancel; abandon job on repeated renew failure.

### 1.2 In-progress step status updates (F019)
- Official: Twirp `WorkflowStepsUpdate` sent when steps are registered (initial full list before step 1), on each InProgress transition, on each completion; `change_order` monotonic (golden 06 flow 24).
- Ours: `worker/server_queue.rs` builds the correct body (F014) but **`ServerQueue` is never instantiated** — `steps_runner.rs` never queues, nothing is ever POSTed. GitHub UI shows no live step progress.
- Fix: instantiate `ServerQueue` in `job_runner`, queue transitions from `steps_runner`, flush every ~1s + at step boundaries via `ResultsClient::update_workflow_steps`.

### 1.3 Step/job log upload (F020)
- Official: per step — `GetStepLogsSignedBlobURL` (Twirp `results.services.receiver.Receiver`) → PUT content to signed URL (Azure blob; `x-ms-blob-type: BlockBlob`); job log likewise at completion (golden 06 flow 20+).
- Ours: `ResultsClient::{get_step_logs_signed_url,get_job_logs_signed_url,upload_log_blob}` exist, **zero call sites**. No logs ever reach GitHub's log viewer.
- Fix: buffer step output in `ServerQueue`, upload at step completion; job log at job end; treat signed URL as opaque (works for aksh's local URLs too). Log lines need the official `2026-...Z ` timestamp prefix (see §3).

### 1.4 ACTIONS_* runtime env plumbing (F021)
- Official: injects `ACTIONS_RUNTIME_URL`, `ACTIONS_RUNTIME_TOKEN`, `ACTIONS_RESULTS_URL`, `ACTIONS_CACHE_URL`, `ACTIONS_CACHE_SERVICE_V2`, `ACTIONS_ID_TOKEN_REQUEST_URL`, `ACTIONS_ID_TOKEN_REQUEST_TOKEN` from job-message variables/endpoints (`JobExtension.cs`). Goldens 11/12/15 show `@actions/cache`/artifact/OIDC hitting `results-receiver.actions.githubusercontent.com` Twirp — those hostnames only reach the action via these vars.
- Ours: `worker/job_extension.rs` injects only `GITHUB_*`/`RUNNER_*`. **None of the ACTIONS_* vars are set** (the only occurrences in the crate are test fixtures in `contexts.rs`).
- Impact: `actions/cache`, `actions/upload-artifact`, `actions/download-artifact`, OIDC (`core.getIDToken()`) all fail or hang on live GitHub. Blocks scenarios 11, 12, 15.
- Fix: port the `JobExtension.cs` mapping from message variables + `resources.endpoints` into `inject_github_env()`.

### 1.5 Action resolution endpoint (F022)
- Official: single batch `POST …/runnerresolve/actions` on `launch.actions.githubusercontent.com` (golden 10 flow 19) → per-action auth token, tarball URL, **resolved SHA**; then downloads from `codeload.github.com/{owner}/{repo}/tar.gz/{sha}` (flow 20).
- Ours: `client/actions_download.rs` has a stub for aksh's `_apis/v1/actiondownloadinfo` that is **never invoked**; `worker/actions/manager.rs` falls back to `api.github.com/repos/{o}/{r}/tarball/{ref}` with the unresolved ref.
- Impact: wrong endpoint set vs golden; unresolved refs break extraction-path parity and caching; private-action auth tokens never obtained.
- Fix: implement the runnerresolve batch call (URL comes from the job message's actions-download endpoint), use resolved SHA for the `_work/_actions/{owner}/{repo}/{sha}/` layout and codeload download.

### 1.6 Pre/post step lifecycle + state context (F023)
- Official: "Set up job" downloads all referenced actions, reads manifests, builds pre list (declared order, `pre-if` default `always()`) and post list (LIFO, `post-if` default `always()`); post steps receive the `state` context saved via `save-state`/`GITHUB_STATE`; post runs even when main failed. Nested composite pre/post are **hoisted** to the parent job lists.
- Ours: `job_extension.rs::build_step_list()` builds main steps only. **No discovery phase, no pre steps, no post steps, no state context injection.** (`GITHUB_STATE` file is created/parsed by `file_commands.rs`, but its contents never reach any post step because post steps never exist.)
- Impact: `actions/checkout` post cleanup never runs; `actions/cache` **post-save never runs** (cache scenario 11 can never pass even with F021 fixed).
- Fix: add discovery in `job_extension.rs` (download + parse manifests up front), build pre/main/post lists, thread `state` per action into post-step context.

### 1.7 Composite action outputs + hoisting (F024)
- Official: composite `outputs.<name>.value` are expressions evaluated after nested steps (against nested `steps` context); nested `uses:` pre/post hoisted to job level; nesting depth capped (10).
- Ours: `handlers/composite.rs` runs nested steps but **never evaluates `outputs.*.value`** (outputs lost), no hoisting, no depth cap.
- Blocks scenario 13.

### 1.8 Annotations upload (F025)
- Official: `::error::`/`::warning::`/`::notice::` (and matcher hits) become annotations attached to step results (Twirp updates + `completejob` `annotations`/`stepResults[].annotations`); golden 14.
- Ours: `execution_context.rs` collects `Annotation`s correctly, but `job_runner.rs` hardcodes `annotations: []` in completejob step results and `server_queue.rs::StepUpdate` has no annotations field. **Nothing is ever uploaded.**
- Blocks scenario 14.

### 1.9 Expression engine gaps (F027)
- Missing vs upstream Expressions2 (`crates/aksh-gha-expressions`):
  - **Bracket access** `a['b']`, `a[0]` — no `[`/`]` tokens in the lexer at all.
  - **Object filter** `a.*.b` — `*` segment unsupported.
  - **`hashFiles(...)`** — stub returning `""`; must glob relative to `GITHUB_WORKSPACE`, SHA-256 each file, SHA-256 the concatenated digests, `""` on no match. Used by virtually every cache workflow key → blocks scenario 11.
  - `format()` `{{`/`}}` escaping (P2, §3).
- Verified present: operators/precedence, case-insensitive string equality, short-circuit `&&`/`||`, `contains`/`startsWith`/`endsWith`/`join`/`toJSON`/`fromJSON`/`format`/status functions.

### 1.10 `secrets` context + masking variants (F028)
- Official: builds a `secrets` expression root from isSecret variables (`${{ secrets.FOO }}` resolvable runner-side); masker also matches trimmed/URL-encoded/base64 variants of each value, applied to every uploaded line.
- Ours: masks are registered and `mask_secrets()` replaces literal values, but **no `secrets` context root is built** (any `${{ secrets.X }}` reference fails), and no encoded-variant masking. Masking must also be applied at the log-upload boundary once F020 lands.

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
| 06-multi-step | ⚠️ job conclusion green previously; **in-progress step updates + log upload not exercised** | ⚠️ same | F018, F019, F020 |
| 07-step-failure | ❌ | ❌ | F019, F020 (semantics verified in unit tests) |
| 08-job-outputs-needs | ❌ | ❌ | F019; outputs mapping verified in completejob |
| 09-matrix-fan-out | ❌ | ❌ | F018 (multi-job sessions), F019 |
| 10-uses-checkout | ❌ | ❌ | F022, F023 (post cleanup), F021 (runtime token) |
| 11-cache-roundtrip | ❌ | ❌ | F021, F023 (post save), F027 (hashFiles) |
| 12-artifact | ❌ | ❌ | F021 |
| 13-composite-action | ❌ | ❌ | F023, F024 |
| 14-annotations | ❌ | ❌ | F025, F032 |
| 15-oidc-id-token | ❌ | ❌ | F021 |
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
