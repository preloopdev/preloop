# aksh-runner P0/P1 Deep Review Log — 2026-07-03

Scope: review recent P0/P1 runner compatibility changes for official `actions/runner` compatibility and Rust correctness, excluding container jobs.

## Review Inputs

- `docs/runner/roadmap.md`
- `docs/runner/runner_fidelity_gap.md`
- `docs/runner/P1_WORKLOG.md`
- `docs/runner/conformance-test-log-2026-07-03-live-runner-rust.md`
- Recent commits touching `crates/aksh-runner`, `crates/aksh-runner-server`, `crates/aksh-gha-parser`, and `crates/aksh-gha-protocol`.

## Issues Found

### DR-001 — `run --once` unregisters non-ephemeral runners

- Severity: blocker
- Files: `crates/aksh-runner/src/listener/broker_listener.rs`
- Finding: the listener called `ephemeral_unregister(...)` on every `--once` exit path, regardless of whether `.runner` was configured with `ephemeral: true`.
- Why it matters: official runner semantics distinguish `run --once` from ephemeral registration. `--once` should exit after one job but must not delete a normal persistent self-hosted runner registration; an ephemeral-configured runner should exit after one job even if the run command did not also pass `--once`.
- Fix: gate unregister cleanup on `config.settings.ephemeral`, and treat `config.settings.ephemeral` as a one-job exit condition alongside `--once`.

### DR-002 — Action and composite processes ignore cancellation

- Severity: blocker
- Files: `crates/aksh-runner/src/worker/steps_runner.rs`, `crates/aksh-runner/src/worker/handlers/action.rs`, `crates/aksh-runner/src/worker/handlers/node.rs`, `crates/aksh-runner/src/worker/handlers/composite.rs`
- Finding: cancellation was threaded only into top-level script steps. Node action processes were launched with `cancel_rx: None`; composite nested scripts also used `None`; action dispatch did not accept a cancellation receiver.
- Why it matters: a live GitHub `JobCancellation` or job timeout can leave JavaScript action or composite child processes running until the listener hard-kills only the worker process. That does not guarantee child process-tree cleanup and diverges from official runner cancellation semantics.
- Fix: thread `watch::Receiver<bool>` through action dispatch, node actions, and composite execution, and pass it to `process::invoke` / nested script calls.

### DR-003 — Step-level `timeout-minutes` uses `tokio::timeout` and can orphan scripts

- Severity: blocker
- Files: `crates/aksh-runner/src/worker/steps_runner.rs`
- Finding: step-level timeouts wrap `execute_step(...)` in `tokio::time::timeout(...)`. When the timeout fires, the step future is dropped rather than signalling `process::invoke` through the cancellation channel.
- Why it matters: dropping the future can orphan a running process tree, reintroducing the class of cancellation bug the P0/P1 work explicitly aimed to remove.
- Fix: implement step timeout by creating a per-step timeout cancellation channel and passing it into step execution, so the process layer kills the process group normally.

### DR-004 — Local aksh broker DELETE route does not match the runner client

- Severity: concern
- Files: `crates/aksh-runner-server/src/lib.rs`, `crates/aksh-runner/src/client/broker.rs`
- Finding: `BrokerClient::delete_session` sends `DELETE /session/{session_id}`, but aksh-server only registered `DELETE /session` and looked for `?sessionId=`.
- Why it matters: local conformance cleanup silently fails to remove broker sessions, weakening session recovery and local replay fidelity.
- Fix: add path-form DELETE routes for `/session/:session_id` and equivalent runner-prefixed broker paths.

### DR-005 — `runner.tool_cache` expression context derives the wrong directory

- Severity: concern
- Files: `crates/aksh-runner/src/worker/contexts.rs`
- Finding: `RUNNER_TOOL_CACHE` env is derived as `<runner-root>/_work/_tool`, while `runner.tool_cache` context was derived from only one parent of `GITHUB_WORKSPACE`, producing `<runner-root>/_work/<repo>/_tool` for standard workspaces.
- Why it matters: expressions using `runner.tool_cache` disagree with the environment variable and with the directory created by workspace setup.
- Fix: derive `runner.tool_cache` with the same two-parent rule used by `RUNNER_TOOL_CACHE`.

### DR-006 — Review documentation has stale statuses

- Severity: concern
- Files: `docs/runner/P1_WORKLOG.md`, `docs/runner/runner_fidelity_gap.md`, `docs/runner/conformance-test-log-2026-07-03-live-runner-rust.md`
- Finding: some issues marked fixed in the roadmap still appear as pending/deferred elsewhere, and the conformance log says F038 was fixed by sending empty annotations while code/docs now describe fixed `startLine`/`endLine` annotation payloads.
- Why it matters: stale tracker state makes it hard to know which compatibility gaps are actually closed.
- Fix: update the review log with corrected state; update existing tracker docs after code fixes are verified.

### DR-007 — Local timeout result was internally inconsistent

- Severity: concern
- Files: `crates/aksh-runner/src/worker/job_runner.rs`
- Finding: when the local job-timeout safety timer fired, `job_ctx.job_status` was set to failure but the run-service completion result still came from `run_steps(...)`, usually `Cancelled`.
- Why it matters: a local timeout could report contradictory state between expression context / complete-job step results and the final `completejob` conclusion.
- Fix: when the local timeout flag is set, report final result/conclusion as `Failed`.

### DR-008 — Static protocol replay rejected recorded official session IDs

- Severity: blocker
- Files: `crates/aksh-runner-server/src/lib.rs`
- Finding: message-poll and broker acknowledgement handlers rejected UUID `sessionId` values absent from local `session_keys`.
- Why it matters: `runner-watch conform` replays recorded official traffic, so the replay can contain official session IDs not minted by the current local aksh process. Rejecting those IDs changed `_apis/distributedtask/pools/{pool}/messages?...sessionId=...` from `200` to `400` in every replay scenario, masking real protocol diffs behind a local replay artifact.
- Fix: allow unknown replay session IDs on message-poll and acknowledgement paths; still remove `session_active_requests` when present.

## Fix Log

- DR-001 fixed in `broker_listener.rs`: `ephemeral_unregister(...)` now runs only when `config.settings.ephemeral` is true, and `config.settings.ephemeral` exits after one job even without `run --once`.
- DR-002 fixed in `steps_runner.rs`, `handlers/action.rs`, `handlers/node.rs`, and `handlers/composite.rs`: cancellation receivers are now threaded through action dispatch, JavaScript action execution, and composite nested scripts/actions.
- DR-003 fixed in `steps_runner.rs`: step `timeout-minutes` now trips a cancellation channel instead of dropping the executing future via `tokio::time::timeout`.
- DR-004 fixed in `aksh-runner-server/src/lib.rs`: local broker session deletion now accepts path-form `DELETE /session/:session_id` plus runner-prefixed equivalents, matching the runner client.
- DR-005 fixed in `contexts.rs`: `runner.tool_cache` now uses the same two-parent `_work/_tool` derivation as `RUNNER_TOOL_CACHE`.
- DR-006 fixed in docs: removed stale P1.1/P1.7 deferrals, updated status-at-a-glance rows, updated F029/F031/F032/F034 statuses, and corrected the F038 conformance-log fix description.
- DR-007 fixed in `job_runner.rs`: local job timeout now forces final result/conclusion to `Failed`.
- DR-008 fixed in `aksh-runner-server/src/lib.rs`: message-poll and broker acknowledgement paths now tolerate replayed official session IDs instead of returning `400`.

## Verification

- `cargo check --workspace --all-targets` passed after code fixes, with pre-existing warnings only.
- `cargo fmt --all --check` passed.
- `cargo test --workspace` passed: 190 tests across 22 suites; warnings are pre-existing.
- Local `runner-watch conform --runner v2.335.1 --aksh-url http://127.0.0.1:9191 --skip-cargo-test` rerun after DR-008: 9/11 in-scope scenarios matched; only known unsupported `CacheService/*` and `ArtifactService/*` scenarios diverged.
- Local runner E2E smoke rerun: `cargo run -p aksh-conformance -- runner-e2e --runner-bin target/debug/aksh-runner --workflow fixtures/golden/simple-echo.yml` returned `success: true` for run `6c5243f8-ff31-4324-896b-42a257c12a7f`.
- Live GitHub rerun against `preloopdev/aksh-conformance-sample` with runner `aksh-live-validate-20260703` registered as agent `73` and labels `self-hosted, mitm, macOS, ARM64`: six success scenarios and two expected cancelled scenarios completed (run IDs recorded in `P1_WORKLOG.md`).
