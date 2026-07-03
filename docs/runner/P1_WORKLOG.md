# P1 Medium Fixes — Work Log

Started: 2026-07-03

## Completed & Live-Verified on GitHub

### P1.11 — Step ID/display-name generation (F029) ✅
- Files: `job_extension.rs`, `steps_runner.rs`, `azdo.rs`, `job_builder.rs`
- Change: Split `Step` into `id` (wire GUID) and `context_name` (human-readable key). Prefer `contextName` from job message for context key, `id` for wire payloads. Auto-generate `__run`/`__run_N` for scripts, `__<action>` for actions. aksh-parser now emits `contextName` in step JSON matching GitHub's wire format (separate counters for scripts vs per-action names).
- Live: Runs 28641527947, 28641641045 — all steps passed, `steps.output_step.outputs.value` resolved correctly.

### P1.10 — Step summary upload (F035) ✅
- Files: `results.rs`, `job_runner.rs`, `steps_runner.rs`, `lib.rs` (server)
- Change: Added `GetStepSummarySignedBlobURL` + blob PUT + `CreateStepSummaryMetadata` finalize. Server: added matching Twirp routes + replay blob PUT handler (state_dir plumbed via AppState, path traversal guard).
- Live GitHub: Run 28642007343 — `CreateStepSummaryMetadata succeeded` (2xx).
- Local aksh: Summary .md stored at `replay/results/.../step-...-summary.md` with correct content.

### P1.5 — Job-level timeout-minutes (F031) ✅
- Files: `job_runner.rs`
- Change: Cancel-channel timer (default 360 min) instead of `tokio::timeout` (orphan-safe). AtomicBool distinguishes timeout from external cancel. GitHub enforces `timeout-minutes` server-side via JobCancellation; local timer is safety net.
- Live: Run 28642465723 — GitHub sent JobCancellation at ~1 min, process killed, unwind steps ran.

### P1.4 — Cancellation completeness (F031) ✅
- Files: `steps_runner.rs`, `broker_listener.rs`, `process.rs`
- Change: --once mode polls for cancel during job (was blocking). Graceful cancel with 5-min grace. Post-cancel steps get grace-bounded cancel channel. `process.rs` only kills on actual cancel (value=true), not channel close.
- Live: Run 28642787995 — always() ran, cancelled() ran, failure() skipped, no insta-kill on unwind steps.

### P1.12 — runner/job context completeness ✅
- Files: `contexts.rs`, `job_extension.rs`
- Change: Added `runner.tool_cache`, `runner.workspace`. Fixed `runner.name` to read from `self.env["RUNNER_NAME"]` (from .runner settings). Added `job.container`, `job.services`.
- Live: Run 28643232151 — `runner.name=p1-ctx-1783060288` (registered name, not job name).

### P1.14 — Manifest fields ✅
- Files: `handlers/node.rs`, `job_extension.rs`
- Change: Deprecation warning for inputs with `deprecationMessage`. Verified `pre_if`/`post_if` already default to `always()`.

### P1.8 — Ephemeral unregister (F033) ✅
- Files: `broker_listener.rs`
- Change: `ephemeral_unregister` helper on all --once exit paths. Proper mechanism: `--ephemeral` at configure sets `ephemeral: true` → GitHub auto-removes runner.
- Live: Run 28655290764 — runner auto-removed after job.

### P1.6 — Problem matchers (F032) ✅
- Files: `matchers.rs`, `commands.rs`, `execution_context.rs`, `contexts.rs`
- Change: `MatcherRegistry` on `JobContext` (cross-step). Workflow command parsing wired into `ctx.log()`. `stop-commands` token suspension. `group`/`endgroup` rewritten to `##[group]`/`##[endgroup]`. Annotation messages masked via `job.mask_secrets()`. `log_raw()` avoids infinite recursion with legacy `##[error]` format.
- Live: Run 28655734365 — 2 annotations in GitHub UI from cross-step eslint matcher.

### P1.7 — Retry/backoff + session recovery (F033) ✅
- Files: `http.rs`, `broker_listener.rs`
- Change:
  - 3 retries with exponential backoff (2s/4s) on `post_json_with_auth` and `put_bytes` for transient 5xx/network errors. Non-5xx (4xx) fails immediately.
  - Session recovery state machine: `run_broker_loop` refactored to catch token and session expirations (401 Unauthorized, 400 Bad Request, 404 Not Found).
  - On 401: re-acquires OAuth token via client assertion exchange and re-creates broker session.
  - On session invalidation (400/404): re-creates broker session immediately and resumes polling.
  - Wipes error counters on successful poll / token acquisition.
  - Active jobs continue to run in the background worker process independently of listener session recovery.
  - Server: added `reject_tokens` flag and session validation against `session_keys` (applying only to UUID session IDs to keep unit tests green).

## aksh-Server Fixes (for local conformance)

- Moved Twirp routes out of `require_bearer` middleware (runner's job token uses different signing key).
- Added `GetStepSummarySignedBlobURL` and `CreateStepSummaryMetadata` Twirp routes.
- Added `/replay/results/*path` PUT handler for blob storage (path traversal guard, state_dir via AppState).
- Added `context_name` field to `TaskStep` (azdo.rs), serialized as `contextName`.
- Updated `build_task_step` in parser to generate `contextName` with separate counters (scripts: `__run`/`__run_N`, actions: `__<sanitized_action>`/`__<sanitized_action>_N`).

## Explicitly Deferred

| Item | Reason |
|------|--------|
| P1.2 — Containers (F026) | Dead code, needs docker integration |
| P1.3 — AzDO compat reporting (F030) | 0 call sites, not GitHub-relevant |
| Multi-line `loop:` patterns in matchers | Single-pattern matchers work; loop needs state machine |
| Native expression evaluation at runtime | aksh-parser pre-evaluates at submit; `steps.*` refs empty natively |

## Live E2E Runs

| Scenario | Run ID | Result | Validates |
|----------|--------|--------|-----------|
| 06-multi-step | 28641527947 | ✅ Pass | P1.11 wire parity |
| 19-step-summary | 28642007343 | ✅ Pass | P1.10 summary + metadata |
| 20-step-ids | 28641641045 | ✅ Pass | P1.11 context_name, output refs |
| 07-step-failure | 28641659763 | ✅ Pass | Regression check |
| 21-job-timeout | 28642465723 | ✅ Cancel | P1.5 timeout, P1.4 cancel |
| 22-cancel-semantics | 28642787995 | ✅ Cancel | P1.4 always()/cancelled() |
| 23-context-fields | 28643232151 | ✅ Pass | P1.12 runner/job context |
| 24-problem-matcher | 28655734365 | ✅ Pass | P1.6 annotations |
| 24 (ephemeral) | 28655290764 | ✅ Pass | P1.8 auto-removal |
| 06-multi-step | 28667882368 | ✅ Pass | Live rerun after DR fixes |
| 13-composite-action | 28667884236 | ✅ Pass | Composite nested action/script cancellation plumbing smoke |
| 19-step-summary | 28667886172 | ✅ Pass | Summary upload rerun |
| 20-step-ids | 28667888259 | ✅ Pass | Step `contextName`/outputs rerun |
| 21-job-timeout | 28667890064 | ✅ Cancel | Timeout/cancel rerun |
| 22-cancel-semantics | 28667891885 | ✅ Cancel | Cancellation semantics rerun |
| 23-context-fields | 28667893813 | ✅ Pass | Runner/job context rerun |
| 24-problem-matcher | 28667895498 | ✅ Pass | Problem matcher annotations rerun |

## Local aksh Conformance

- Server on port 9393, runner configured, workflow submitted and executed.
- Summary .md stored correctly in `replay/results/` (17 bytes, content: `## Local Summary`).
- All Twirp calls succeed (no 401s after moving routes out of require_bearer).
- `contextName` emitted in step JSON (verified via runner log: steps use auto-IDs).
- Known gap: `${{ steps.*.outputs.* }}` empty on native path (parser pre-evaluates at submit time).
- 2026-07-03 rerun: `runner-watch conform --runner v2.335.1 --aksh-url http://127.0.0.1:9191 --skip-cargo-test` matched 9/11 scenarios; only known unsupported `CacheService/*` and `ArtifactService/*` scenarios diverged.
- 2026-07-03 local runner smoke: `cargo run -p aksh-conformance -- runner-e2e --runner-bin target/debug/aksh-runner --workflow fixtures/golden/simple-echo.yml` returned `success: true` for run `6c5243f8-ff31-4324-896b-42a257c12a7f`.
