# Plan 016: Decompose the worker execution engine by responsibility

> **Executor instructions**: Staged, behavior-preserving decomposition of the worker
> modules. Worker execution is **wire-critical** (step order, cancellation, completion
> payloads) — every move preserves observable request ordering and outcomes. Add
> characterization tests BEFORE moving each high-risk module. STOP on any stop
> condition. Update `plans/README.md` when done.
>
> **Drift check (run first)**: `git diff --stat 3505476..HEAD -- crates/aksh-runner/src/worker/`
> Re-derive boundaries with `grep -n` if drifted.

## Status

- **Priority**: P2
- **Effort**: L
- **Risk**: HIGH (wire-critical hot path: step ordering, cancellation, completion)
- **Depends on**: 012 (move worker tests out first), 014 (shared masker/context builder
  so split modules import canonical helpers)
- **Category**: tech-debt / architecture
- **Planned at**: commit `3505476`, 2026-07-16

## Why this matters

Three worker god-modules each mix several responsibilities, so a change to one concern
risks silently altering another (synthetic-step numbering, cancellation unwind, upload
timing, completion payload shape):

- `job_runner.rs` (2011 lines): lifecycle orchestration + results-service reporting +
  remote-action prep + completejob serialization.
- `steps_runner.rs` (2136): condition eval + step execution + lifecycle reporting +
  container startup, all in `run_steps`.
- `job_extension.rs` (1968): workspace/env injection + typed-token decode + step
  planning + defaults/cleanup.

Plus duplication: action input/env resolution is reimplemented in `node.rs`,
`composite.rs`, `container.rs` (~150 lines); `JobContext`↔`StepContext` have a
bidirectional dependency (`contexts.rs` ↔ `execution_context.rs`).

## Current state (boundaries — re-verify after Plan 012)

- `crates/aksh-runner/src/worker/job_runner.rs`: `run_job` orchestration `51-594`;
  reporting/log upload `692-1185`; remote action prep `1187-1277`; completion
  serialization `1372-1821`; unused `job_status_conclusion` `~1516-1522` (dead — delete).
- `crates/aksh-runner/src/worker/steps_runner.rs`: `run_steps` `64-780`;
  process/action dispatch `814-878`; container startup `884-1042`.
- `crates/aksh-runner/src/worker/job_extension.rs`: env injection `10-327`; typed decode
  `329-405`; step planning `407-708`; defaults/cleanup `710-937`.
- Action input dup: `handlers/node.rs:41-105`, `handlers/composite.rs:59-98`,
  `handlers/container.rs:174-268`.
- `contexts.rs` (`JobContext`) ↔ `execution_context.rs` (`StepContext`, holds
  `&mut JobContext`); `Annotation` defined in `execution_context`, owned by `JobContext`.

Official reference for behavior parity: `Runner.Worker/{JobRunner,StepsRunner,ExecutionContext,
JobExtension}.cs`.

## Commands you will need

| Purpose | Command | Expected |
|---------|---------|----------|
| Check | `cargo check -p aksh-runner` | exit 0 |
| Tests | `cargo test -p aksh-runner --quiet` | ≥ post-012 baseline |
| Clippy | `cargo clippy -p aksh-runner --all-targets -- -D warnings` | exit 0 |

## Scope

**In scope**: `crates/aksh-runner/src/worker/{job_runner,steps_runner,job_extension}.rs`
splits; a shared `action_inputs.rs`; a neutral `execution_types.rs` for `Annotation`/
step-result DTOs; the `handlers/*` input-resolution dedup. Delete dead
`job_status_conclusion`.

**Out of scope**: changing step ordering, cancellation timing, completion payload shape,
or expression semantics; the `contexts`/`execution_context` API narrowing (that is a
deeper refactor — see STOP conditions; this plan only moves shared DTOs to a neutral
module, it does NOT re-architect the `&mut JobContext` access).

## Steps

### Step 0: Characterization tests first (gate for everything else)

Before moving anything, add wire/behavior snapshot tests (these are the safety net;
current tests exercise local execution but not reporting payloads):
- `completejob` payload shape (step results, annotations, job outputs).
- `WorkflowStepsUpdate` sequence for: skip, failure, continue-on-error, cancellation,
  timeout, pre/post steps.
- Container job setup/teardown ordering.
Model after existing worker tests (now relocated by Plan 012). Put them in
`crates/aksh-runner/tests/worker_*.rs` where possible.

**Verify**: new tests pass against current code.

### Step 1: Delete dead code

Remove `job_status_conclusion` (`job_runner.rs`) after confirming no callers
(`grep -rn job_status_conclusion crates/`).

**Verify**: `cargo test -p aksh-runner --quiet` → baseline.

### Step 2: Consolidate action input/env resolution

Add `worker/action_inputs.rs` with one resolver returning evaluated input values + an
`INPUT_*` env projection (filter `__aksh_*`, apply manifest defaults, evaluate `${{ }}`).
Route `node.rs`, `composite.rs`, `container.rs` through it, preserving each handler's
deliberate manifest-env/lifecycle differences and composite's env isolation (Plan from
commit `4c1a6e5`).

**Verify**: cross-handler tests (provided values, defaults, non-string, `${{ }}`,
`__aksh_*`, composite outer-context isolation) pass; `cargo test -p aksh-runner --quiet`
→ baseline.

### Step 3: Split `job_runner.rs`

Keep a thin `job_runner.rs` orchestrator; move reporting/log-upload to `reporting.rs`,
remote action prep to `action_preparation.rs`, completejob model/serialization to
`completion.rs`. Pure moves; ordering preserved.

**Verify**: Step 0 completion/reporting snapshots unchanged; `cargo test -p aksh-runner
--quiet` → baseline.

### Step 4: Split `steps_runner.rs` and `job_extension.rs`

`steps_runner.rs` → step-plan/condition, step-executor, lifecycle/reporting,
`container_lifecycle.rs`. `job_extension.rs` → `runtime_env.rs`, `template_tokens.rs`,
`step_plan.rs`, `process_cleanup.rs`, `workspace.rs`. Pure moves.

**Verify**: Step 0 snapshots (queue order, outcomes, container setup) unchanged;
`cargo test -p aksh-runner --quiet` → baseline.

### Step 5: Neutral shared DTO module

Move `Annotation` + step-result/reporting DTOs to `worker/execution_types.rs` so
`contexts.rs` and `execution_context.rs` both import from it instead of each other
(breaks the bidirectional dependency at the type level). Do NOT change the `&mut
JobContext` access model here.

**Verify**: `cargo check -p aksh-runner` → 0; `cargo test -p aksh-runner --quiet` →
baseline.

## Test plan

- Step 0 characterization suite is the core contract (completion payload, step-update
  sequences per outcome, container ordering).
- Action-input tests across node/composite/container.
- Verification: `cargo test -p aksh-runner --quiet` → ≥ baseline at every step.

## Done criteria

- [ ] `cargo check -p aksh-runner` exits 0; `cargo clippy -p aksh-runner --all-targets -- -D warnings` exits 0
- [ ] Characterization tests for completion + step-update sequences exist and pass
- [ ] `job_runner.rs`, `steps_runner.rs`, `job_extension.rs` each ≤ ~700 lines
- [ ] Action input/env resolution has one home (`action_inputs.rs`); handlers call it
- [ ] `grep -rn job_status_conclusion crates/` → no matches
- [ ] `Annotation` lives in a neutral module; `contexts.rs`/`execution_context.rs` don't import each other's types
- [ ] `cargo test -p aksh-runner --quiet` == post-012 baseline; `plans/README.md` updated

## STOP conditions

- Any move that changes step-update ordering, cancellation timing, or completion payload
  bytes → STOP; it is not a pure move.
- If the `&mut JobContext` access model blocks a clean split (Step 5 needs more than
  moving DTOs) → STOP and report; narrowing `StepContext`'s access to `JobContext` is a
  separate, deeper refactor and must not be attempted opportunistically here.
- Step 0 characterization can't be written without huge new test infra → STOP and
  report; do NOT move high-risk modules without the safety net.

## Maintenance notes

- The deeper follow-up (explicit `JobState` API instead of `&mut JobContext`) should be
  its own plan after these modules exist.
- A reviewer should confirm each split commit is a relocation and that the Step 0
  snapshots are byte-identical before/after.
