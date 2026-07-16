# Plan 004: Handle ForceTokenRefresh, HostedRunnerShutdown, and RunnerRefresh broker messages

> **Executor instructions**: Follow step by step; run every verification; STOP conditions are
> binding. Update this plan's row in `plans/README.md` when done.
>
> **Drift check (run first)**:
> `git diff --stat 839791c..HEAD -- crates/aksh-runner/src/listener/broker_listener.rs`
> This file carried uncommitted concurrency changes at planning time (+112 lines around the
> JobCancellation arm). The message-dispatch `match` may have moved; anchor on the
> `match message_type.as_str()` block and its `"AgentRefresh"` / `"BrokerMigration"` arms.
> If the match arms listed under "Current state" are absent or renamed, STOP.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW (additive match arms)
- **Depends on**: none (coordinate file access with any in-flight concurrency work)
- **Category**: bug (protocol compatibility vs real GitHub)
- **Planned at**: commit `839791c`, 2026-07-13

## Why this matters

The official listener dispatches nine broker message types
(`src/Runner.Listener/Runner.cs:574-780` in the mirror at
`~/mitm-proxy/experiments/mitm/.cache/runner.server/src/`). aksh-runner handles five and
silently drops the rest. Against **real GitHub** (a supported deployment: aksh-runner is meant
to be interchangeable with the official runner), the missing ones have concrete effects:

- `ForceTokenRefresh` — service demands an immediate OAuth token refresh (auth migration);
  ignoring it risks 401s on every subsequent call until the proactive 5-min refresh fires.
- `HostedRunnerShutdown` — service tells the runner to stop; ignoring it leaves a zombie
  session.
- `RunnerRefresh` (self-update request) — aksh-runner intentionally does not self-update, but
  must still **acknowledge and log** the message rather than treat it as unknown (the official
  runner's handling is at `Runner.cs:611-626`; aksh already does exactly this for
  `AgentRefresh` — extend the same treatment).

## Current state

- `crates/aksh-runner/src/listener/broker_listener.rs` — the dispatch `match` (was lines
  262–341 at planning): arms exist for `"RunnerJobRequest"`, `"PipelineAgentJobRequest"`,
  `"JobCancellation"`, `"AgentRefresh"`, `"BrokerMigration"`; unknown types fall through to a
  catch-all. Excerpt of the existing style to copy:

  ```rust
  "AgentRefresh" => {
      info!("Self-update requested; aksh-runner does not self-update");
  }
  "BrokerMigration" => {
      info!("Broker migration requested — re-resolving broker URL...");
      ...
  }
  ```

- OAuth refresh helper already exists and is called before job acquisition:
  `crate::listener::oauth::get_oauth_token(http, config)` (see its use inside the
  `"RunnerJobRequest"` arm; it returns `(token, expires_at)`).
- Graceful shutdown path already exists at the top of the loop (the `shutdown` select branch
  deletes the session and returns) — `client.delete_session(&token, &session_id)`.
- Official reference for names/semantics: `Runner.cs:574-780`; message-type string constants in
  `src/Sdk` (`JobRequestMessageTypes.cs` and friends — confirm exact casing there:
  `ForceTokenRefresh`, `HostedRunnerShutdown`, `RunnerRefresh`).
- Existing test pattern: `crates/aksh-runner/src/listener/message_listener.rs:225-280` has
  table-style `process_message` tests constructing `serde_json::json!` messages.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Runner tests | `cargo test -p aksh-runner --quiet` | all pass |
| Full gate | `cargo fmt --all --check && cargo clippy --workspace --all-targets && cargo test --workspace --quiet` | exit 0 |

## Scope

**In scope**:
- `crates/aksh-runner/src/listener/broker_listener.rs` (new match arms + minimal helper wiring)

**Out of scope** (do NOT touch):
- Self-update implementation — explicitly not a goal; `RunnerRefresh` is log-and-ack only.
- `crates/aksh-runner/src/listener/message_listener.rs` (AzDO legacy path — deferred by design).
- The server (`aksh-runner-server`) — it doesn't emit these types today; server-side emission
  is not this plan.

## Git workflow

- Branch: `advisor/004-broker-message-types`; conventional commits; no push/PR unless told.

## Steps

### Step 1: Confirm exact message-type strings

Read the constants in the mirror: `grep -rn "ForceTokenRefresh\|HostedRunnerShutdown\|RunnerRefresh"
~/mitm-proxy/experiments/mitm/.cache/runner.server/src/Sdk --include=*.cs` and
`Runner.cs:574-780`. Record the exact casing in the commit message.

**Verify**: you can quote the constant definitions (file:line) for all three.

### Step 2: Add the three arms

In the broker dispatch `match`, before the catch-all:

- `"ForceTokenRefresh"` → call `get_oauth_token` immediately, replace `token`/`token_expires_at`
  on success, `warn!` on failure (copy the exact pattern from the `"RunnerJobRequest"` arm).
- `"HostedRunnerShutdown"` → `info!`, best-effort `delete_session`, then `return Ok(())` —
  mirror the existing shutdown-branch cleanup (if a job is active, follow the official
  behavior at `Runner.cs` for this message: read its handler first; if it cancels the active
  job, reuse the existing JobCancellation cleanup; if it just exits, exit).
- `"RunnerRefresh"` → `info!("Self-update requested (RunnerRefresh); aksh-runner does not self-update")`
  — same treatment as the existing `"AgentRefresh"` arm.

Also downgrade the catch-all from silent to `warn!(%message_type, "unhandled broker message type")`
if it isn't already.

**Verify**: `cargo check -p aksh-runner` → exit 0.

### Step 3: Tests

The dispatch loop is not directly testable without a broker; extract a small pure helper if one
doesn't exist (e.g. `fn classify_message(t: &str) -> MessageKind`) OR, if extraction would
disturb the uncommitted concurrency edits in this file, limit testing to: a unit test asserting
the three strings are matched (compile-time exhaustiveness via the helper), modeled on the
`message_listener.rs` test style.

**Verify**: `cargo test -p aksh-runner --quiet` → all pass, new test(s) included.

## Test plan

- `classify_message` (or equivalent) covers: the three new types, the five existing, and an
  unknown type → logged-unknown. Pattern: `message_listener.rs:225-280` tests.

## Done criteria

- [ ] Full gate exits 0
- [ ] All three types have explicit arms (grep the file for each string → 1 match each)
- [ ] `ForceTokenRefresh` arm demonstrably replaces the session token (code review level)
- [ ] No files outside scope modified (`git status`)
- [ ] `plans/README.md` row updated

## STOP conditions

- The dispatch match has been restructured beyond recognition vs the excerpt (concurrency work
  landed) — coordinate/rebase rather than guess.
- `Runner.cs`'s `HostedRunnerShutdown` handler does something materially different from
  "stop the runner" (e.g. drain semantics) — report before implementing.
- The exact message-type strings in the Sdk differ from the three assumed names — use the
  source's strings and note the delta.

## Maintenance notes

- If/when the aksh **server** gains emission of these types (production profile), add
  server-side tests mirroring `cancel_run_delivers_cancellation_message`.
- Reviewer: check the `HostedRunnerShutdown` path against an active job — no orphan worker
  processes (the existing kill/cleanup path must run).
