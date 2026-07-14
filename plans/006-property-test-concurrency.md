# Plan 006: Establish model-based property testing for concurrency

> **Executor instructions**: Follow this plan step by step. Run every verification command and confirm the expected result before moving on. When a generated counterexample reveals a production defect, fix the defect at the state-transition source, retain the minimized regression seed, and rerun the focused property before continuing. Do not weaken a documented invariant to make a test pass.
>
> **Drift check (run first)**: `git diff --stat 2de7ea9080b571691e13d09ea907f41feb4e8d27..HEAD -- crates/aksh-runner-server/src/concurrency.rs crates/aksh-runner-server/src/scheduling.rs crates/aksh-runner-server/src/lib.rs crates/aksh-gha-parser/src/lib.rs crates/aksh-gha-expressions/src/lib.rs crates/aksh-runner/src/listener/broker_listener.rs crates/aksh-runner/src/listener/job_dispatcher.rs benchmarks/real-world`
>
> Reconcile any drift in these symbols and official references before editing. STOP if GitHub documentation or the pinned official runner contradicts an invariant below.

## Status

- **Priority**: P1
- **Effort**: L; counterexamples are expected to require production fixes to concurrency transitions and TimeSpan parsing
- **Risk**: HIGH because the properties exercise broker cancellation and multi-key scheduling state
- **Depends on**: rebased concurrency implementation and `origin/main` DAG property harness
- **Category**: correctness + tests
- **Planned at**: commit `2de7ea9080b571691e13d09ea907f41feb4e8d27`, 2026-07-14
- **Execution status**: IMPLEMENTED; live differential recording is **BLOCKED** because this workstation has no configured `GH_TOKEN` + disposable GitHub test repository and no pinned unmodified official runner registered through the privileged port-80 redirect. Credential-free properties, HTTP sequences, corpus validation, and contamination rejection are complete.

## Why this matters

The concurrency implementation is a state machine with interacting workflow, job, matrix, reusable-workflow, queue, cancellation, and runner-dispatch gates. Example-based tests cover selected paths but cannot economically cover arbitrary transition orderings. Property testing should prove invariants after generated operation sequences, shrink failures to minimal workflows, and preserve every differential mismatch as a deterministic seed.

GitHub's control plane is authoritative for group semantics. The official `actions/runner` is authoritative only for runner-observable effects: job delivery, `JobCancellation`, job-ID matching, graceful cancellation, forced-kill timing, and overlapping dispatch. The runner never evaluates `concurrency:` itself.

## Normative oracle

### GitHub control-plane invariants

Primary sources:

- [Control workflow concurrency](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency)
- [Workflow-level `concurrency`](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency)
- [`jobs.<job_id>.concurrency`](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#jobsjob_idconcurrency)
- [Reusable workflow behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/reusing-workflow-configurations)
- [Expression semantics](https://docs.github.com/en/actions/reference/workflows-and-actions/expressions)

Use these invariant IDs in test names and failure messages:

| ID | Normative property |
|---|---|
| GH-GROUP-01 | Group identity is `(repository, case-insensitive evaluated group name)`. Equal names in different repositories do not interact. |
| GH-SLOT-01 | Each group has at most one running holder. Workflow-level holders serialize runs; job-level holders serialize jobs. |
| GH-SINGLE-01 | Default `queue: single` permits one running and at most one pending holder. A new contended arrival cancels/replaces the previous pending holder, not the running holder. |
| GH-MAX-01 | `queue: max` permits one running and at most 100 pending holders. Arrival 101 is cancelled without changing running or pending order. |
| GH-FIFO-01 | Pending holders are promoted by the time they started waiting on that group. Test admission order, not workflow submission timestamps. |
| GH-CANCEL-01 | Effective `cancel-in-progress: true` atomically cancels the running holder and makes the arrival running. It does not require a pending intermediary. |
| GH-VALIDATE-01 | Effective `queue: max` plus `cancel-in-progress: true` is invalid and must not mutate group state. |
| GH-CTX-WF-01 | Workflow concurrency expressions may use only `github`, `inputs`, and `vars`. |
| GH-CTX-JOB-01 | Job concurrency expressions may use `github`, `inputs`, `vars`, `needs`, `strategy`, and `matrix`. |
| GH-MATRIX-01 | `strategy.max-parallel` and job concurrency are conjunctive gates. A job must own neither a worker slot nor concurrency slot until both gates permit it. |
| GH-REUSE-01 | Concurrency on a caller `uses:` job covers the complete reusable-workflow invocation. Called workflow-level concurrency also applies to the expanded inner job set. |
| GH-STATUS-01 | Invalid/empty concurrency evaluation produces terminal failure, never an indefinitely queued run. |

Documented case-insensitivity currently conflicts with a local live capture. Unit/model properties MUST follow the documentation. Differential captures should classify a repeated live mismatch as `docs-vs-live`, preserve it, and require a maintainer decision rather than silently changing the oracle.

### Official runner invariants

Pin differential work to official runner commit [`32e89e2afd4549a362dbec337a589b81fd17a0c5`](https://github.com/actions/runner/tree/32e89e2afd4549a362dbec337a589b81fd17a0c5) or update the pin and review source changes explicitly.

Primary sources:

- [`JobDispatcher.cs`](https://github.com/actions/runner/blob/32e89e2afd4549a362dbec337a589b81fd17a0c5/src/Runner.Listener/JobDispatcher.cs)
- [`Runner.cs`](https://github.com/actions/runner/blob/32e89e2afd4549a362dbec337a589b81fd17a0c5/src/Runner.Listener/Runner.cs)
- [`JobCancelMessage.cs`](https://github.com/actions/runner/blob/32e89e2afd4549a362dbec337a589b81fd17a0c5/src/Sdk/DTWebApi/WebApi/JobCancelMessage.cs)
- [Official `JobDispatcherL0` tests](https://github.com/actions/runner/blob/32e89e2afd4549a362dbec337a589b81fd17a0c5/src/Test/L0/Listener/JobDispatcherL0.cs)

| ID | Normative property |
|---|---|
| RUN-MSG-01 | Message type is exactly `JobCancellation`; body contains `jobId: Guid` and `timeout: TimeSpan`. |
| RUN-ID-01 | Cancellation for a job ID not present in the active dispatcher map is ignored; matching active ID is accepted. |
| RUN-TIME-01 | Cancellation timeout is clamped to at least 60 seconds; forced-kill token fires at `effective_timeout - 15 seconds`, therefore never before 45 seconds. |
| RUN-IDEMP-01 | The first matching cancellation fires graceful cancellation once. Every repeated matching cancellation is safe and updates the forced-kill deadline to `max(new_timeout, 60 seconds) - 15 seconds`, matching `JobDispatcher.Cancel`/`CancelAfter`; it may extend or shorten that deadline. |
| RUN-ORDER-01 | Worker receives the job before its cancel message. Cancel before dispatch and cancel after dispatcher removal are ignored. |
| RUN-OVERLAP-01 | For run-service overlap, the prior dispatch is cancelled and awaited before the successor worker begins. The listener may remain responsive through dispatcher task separation, but two workers must not execute jobs concurrently. |
| RUN-SCOPE-01 | No runner property should inspect concurrency group, queue mode, matrix, or reusable metadata; those are server-only. |

## Current state

- `Cargo.toml:39` already defines `proptest = "1.4"`.
- `crates/aksh-runner-server/Cargo.toml:44-47` and `crates/aksh-runner/Cargo.toml:51-53` already enable it as a dev dependency.
- Existing property-test style is in `crates/aksh-gha-expressions/src/lib.rs:1149-1160` and `crates/aksh-gha-parser/src/lib.rs:1859-1963`.
- Pure seams:
  - `crates/aksh-runner-server/src/concurrency.rs::concurrency_key`
  - `apply_queue_mode`
  - `evaluate_concurrency`
  - `holder_is_terminal`
  - `context_data_to_json`
  - `crates/aksh-runner/src/listener/job_dispatcher.rs::parse_timespan_secs`
- `crates/aksh-runner-server/src/scheduling.rs` already provides the pure DAG scheduler, `dag_config`, `arb_dag`, and an independent proptest oracle. Extend this harness for concurrency cross-gates; do not build a second DAG model.
- Stateful seams, private but visible to a child test module of `lib.rs`:
  - `try_acquire_concurrency`
  - `promote_next_from_group`
  - `release_concurrency_for_run`
  - `release_concurrency_for_job`
  - `cancel_run_inner`
  - `cancel_job_inner`
  - `promote_ready_jobs`
  - `under_max_parallel`
- State to inspect after every operation is in `InnerState` at `crates/aksh-runner-server/src/lib.rs:910-969`: `runs`, `queue`, `pending_jobs`, `cancellation_queue`, `concurrency_groups`, `held_runs`, `concurrency_blocked`, and `holder_keys`.
- Existing HTTP helpers and deterministic concurrency examples are in the inline `tests` module beginning at `lib.rs:7112`; existing fixed regressions are named `c01_`, `c02_`, `c05_`, `c06_`, and `c07_` near the end of the file.

## Commands

| Purpose | Command | Expected |
|---|---|---|
| Fast pure properties | `PROPTEST_CASES=256 cargo test -p aksh-runner-server 'concurrency::properties' -- --test-threads=1` and `PROPTEST_CASES=256 cargo test -p aksh-runner-server concurrency_properties::pure -- --test-threads=1` | exit 0 |
| Scheduler model | `PROPTEST_CASES=256 cargo test -p aksh-runner-server concurrency_properties::state_machine -- --test-threads=1` | exit 0 |
| HTTP sequences | `PROPTEST_CASES=64 cargo test -p aksh-runner-server concurrency_http_properties -- --test-threads=1` | exit 0 |
| Expressions/parser | `PROPTEST_CASES=256 cargo test -p aksh-gha-expressions -- --test-threads=1` and `cargo test -p aksh-gha-parser concurrency_ -- --test-threads=1` | exit 0 |
| Runner properties | `PROPTEST_CASES=256 cargo test -p aksh-runner timespan_tests -- --test-threads=1` | exit 0 |
| Full affected suites | `cargo test -p aksh-gha-expressions -p aksh-gha-parser -p aksh-gha-protocol -p aksh-runner-server --quiet` | exit 0 |
| Format | `cargo fmt --all --check` | exit 0 |
| Nightly property profile | `PROPTEST_CASES=10000 PROPTEST_MAX_SHRINK_ITERS=100000 cargo test -p aksh-runner-server concurrency_properties --release -- --test-threads=1` plus the pure and 1,000-case HTTP filters in CI | exit 0 |
| Confirm filters match | CI lists tests and asserts nonzero matches for `concurrency::properties`, `concurrency_properties`, `concurrency_http_properties`, `timespan_tests`, and parser `concurrency_` before case-count runs | each filter reports at least one named property |

Do not put environment assignments after `--`; they are not libtest arguments. Proptest persists minimized failures in source-adjacent `proptest-regressions` files; commit those regression files.

## Scope

**In scope**:

- `crates/aksh-runner-server/src/concurrency.rs`
- `crates/aksh-runner-server/src/lib.rs`
- `crates/aksh-runner-server/src/concurrency_properties.rs` (new)
- `crates/aksh-gha-expressions/src/lib.rs`
- `crates/aksh-gha-parser/src/lib.rs`
- `crates/aksh-runner/src/listener/broker_listener.rs`
- `crates/aksh-runner/src/listener/job_dispatcher.rs`
- `crates/aksh-runner/src/listener/dispatcher_properties.rs` (new if a separate module is cleaner)
- `benchmarks/real-world/concurrency-property-cases.json` (new seed corpus)
- `benchmarks/real-world/run-concurrency-property-probes.py` (new differential harness)
- `.github/workflows/ci.yml` only for the bounded CI property profile

**Out of scope**:

- Legacy AzDO concurrency semantics.
- Replacing `proptest` with another framework.
- Using live GitHub as an oracle in ordinary unit/PR CI.
- Random sleeps, wall-clock timing assertions, or tests requiring a particular thread schedule. New property-test files must pass a mechanical search showing no `sleep(` calls.
- Treating ChristopherHX Runner.Server as authoritative.
- General expression conformance unrelated to concurrency expressions.
- Production persistence/distributed scheduling; current state is in-memory by design.

## Git workflow

- Commit each layer separately: pure properties, scheduler model, expression/parser properties, runner reducer/properties, HTTP sequences, differential harness/CI.
- Use conventional commits, e.g. `test: add concurrency state-machine properties`.
- Do not rewrite the five commits preceding this plan.

## Implementation design

### Independent reference model

Create a test-only model that does not call production transition helpers. It may share protocol enums such as `ConcurrencyQueue`, but it must implement transitions independently in fewer than roughly 150 lines.

Model state:

```text
Model {
  groups: Map<GroupKey, { running: HolderToken?, pending: Deque<HolderToken> }>,
  holder_state: Map<HolderToken, Submitted|Pending|Running|Cancelled|Terminal>,
  wait_order: Map<GroupKey, Vec<HolderToken>>,
  max_parallel: Map<BaseJobToken, usize>,
  active_by_base: Map<BaseJobToken, Set<JobToken>>,
  holder_keys: Map<RunToken, Set<GroupKey>>,
}
```

Generated operations:

```text
Submit { repo, group, holder_kind, queue, cancel_in_progress }
StartOrPoll { runner }
Complete { existing_holder_index, conclusion }
Cancel { existing_holder_index }
Release { existing_holder_index }
UnblockNeeds { job_index }
ChangeMatrixActivity { base, complete_job_index }
```

Use small integer tokens in the model and map them to deterministic `RunId`, `JobId`, and UUID values in production state. Never compare random UUID ordering.

After every generated operation, compare the independent model with a normalized production snapshot and run all structural invariants. The snapshot should sort maps/sets and represent holders as stable tokens so failure output shrinks cleanly.

### Structural invariants after every state transition

1. At most one running holder per group.
2. Running and pending sets are disjoint; no holder appears twice in one group.
3. A holder does not occupy the same key as both running and pending.
4. Default single mode has at most one pending holder; running plus one pending is valid.
5. Max mode has at most 100 pending; overflow does not mutate existing order.
6. Pending promotion order equals group wait-admission order.
7. Every running/pending holder has the exact reverse key in `holder_keys`; no reverse key points to a group without that run.
8. Terminal/cancelled holders are absent from all dispatch and concurrency queues.
9. `queue`, `pending_jobs`, `concurrency_blocked`, and `held_runs` contain no duplicate `(run_id, job_id)`.
10. Job status agrees with placement: concurrency-blocked/held jobs are `Pending`; dispatchable jobs are `Queued`; acquired jobs may become `InProgress`; terminal jobs are nowhere dispatchable.
11. `RunRecord.status == summarize_run(run.jobs)` except the documented pre-dispatch normalization of aggregate `InProgress` to `Queued`.
12. Empty groups are removed; runs with no group presence have no `holder_keys` entry.

## Steps

### Step 1: Add pure queue, key, holder, and serialization properties

In `crates/aksh-runner-server/src/concurrency.rs`, add a `#[cfg(test)] mod properties` following existing proptest style.

Generators:

- Repository and group names from `[A-Za-z0-9_.\-/]{1,32}` with deliberate upper/lowercase pairs.
- `Holder` variants with JobSet cardinality `1..=8`; keep all IDs deterministic from generated integers.
- Pending deques of `0..=105` distinct holders.
- Terminal/non-terminal status maps for every holder member.

Properties:

- `GH-GROUP-01`: case variants produce equal keys; applying normalization twice is idempotent; changing repository keeps keys distinct.
- `GH-SINGLE-01`: single mode returns every old pending holder in `cancel_pending`, never cancels arrival, and parks arrival; no order-sensitive omission.
- `GH-MAX-01`: lengths `0..99` park, `100..` cancel arrival; existing queue is not mutated by the pure decision.
- `RUN-MSG-01`: generated UUIDs produce body with exactly `jobId` and `timeout`, parse back to the same UUID and valid TimeSpan string.
- Holder membership and terminality agree with an independently computed `all()` over generated status maps.
- `context_data_to_json` preserves scalar/list/dictionary shape recursively for bounded depth `0..=4`.

In `crates/aksh-runner/src/listener/job_dispatcher.rs`, property-test `parse_timespan_secs` with generated days/hours/minutes/seconds/fraction strings and malformed strings. Official `TimeSpan` requires minute/second fields below 60 and preserves fractional ticks; the current parser accepts out-of-range fields and truncates fractions, so expose and fix that production defect. Shrink toward `59/60` boundaries.

**Verify**: fast pure-properties command exits 0 with at least 256 cases/property.

### Step 2: Add the scheduler reference model and transition properties

Create `crates/aksh-runner-server/src/concurrency_properties.rs` and include it from `lib.rs` with `#[cfg(test)] mod concurrency_properties;`. As a child of the root module, it can use private state/functions without making production APIs public.

Implement generators in separate submodules:

- `generators`: holder tokens, groups, queue policies, max-parallel limits, acyclic needs DAGs, operation sequences of length `1..=64`.
- `model`: independent transition reducer.
- `snapshot`: canonical production-state projection.
- `invariants`: assertions listed above.
- `pure`, `state_machine`, and `http_sequences`: test modules matching command filters.

State-machine properties:

- Mixed Run/Job/JobSet acquisition and release sequences match the model.
- Cancellation is idempotent and terminal-state locked.
- `cancel-in-progress` replaces running atomically and queues exactly one cancellation only for an actually in-progress job with an agent job ID.
- Single replacement cancels all prior pending holders and leaves only the arrival pending.
- Max mode promotes FIFO and preserves all prior pending holders.
- Completion/release is idempotent; repeated release never promotes twice.
- No operation sequence leaves stale `holder_keys`.

Shrinking:

- Shrink operation vectors first.
- Shrink holder kinds `JobSet -> Job -> Run` only when the failing property remains meaningful.
- Shrink group count toward one, queue depth toward `0/1/99/100/101`, and max-parallel toward `1`.
- Print the normalized operation list and state snapshots, never raw `InnerState` debug dumps.

**Verify**: scheduler-model command exits 0 with 256 cases and no generated counterexample.

### Step 3: Add cross-gate matrix, needs, and JobSet properties

Generate valid workflow DAGs rather than arbitrary YAML text:

- Job count `1..=8`.
- Needs edges only from lower to higher indices, guaranteeing acyclicity.
- Matrix cardinality `1..=6` and `max-parallel 1..=cardinality`.
- Group expression chosen from literal, `matrix.axis`, `needs.<job>.outputs.key`, `inputs.name`, `vars.name`, and `strategy.job-total`.
- Optional caller and embedded reusable-workflow groups; sometimes equal, sometimes different; sometimes contended by an external run.

Properties:

- `GH-MATRIX-01`: active plus dispatchable matrix siblings never exceed `max-parallel`; a job does not become group-running while the matrix gate is full.
- Completing any active sibling eventually makes the oldest eligible blocked sibling dispatchable; bounded liveness is checked after at most `number_of_jobs + pending_holders` release steps.
- Needs-blocked jobs never acquire job-level concurrency early. Once all needs are terminal-success, concurrency evaluates from the hydrated outputs and acquisition happens exactly once.
- `GH-REUSE-01`: no JobSet member dispatches until caller and embedded keys are both acquired.
- Multi-key JobSet acquisition is all-or-nothing. If key 2 blocks or overflows, key 1 must not remain held by a non-dispatchable set.
- Releasing one JobSet member does not release the set; releasing the final member releases every held key once.
- Cancelling a JobSet makes every non-terminal member cancelled and releases keys only after terminal aggregation.
- Nested reusable calls preserve outer and embedded scope without duplicate inner jobs.
- When caller and called workflow use the same evaluated group with effective `cancel-in-progress: true`, called-workflow acquisition cancels the already-running caller workflow as GitHub documents; it must not be modeled as a harmless re-entrant JobSet acquisition.

This phase is expected to expose partial-acquisition bugs. Fix them with deterministic sorted-key acquisition and rollback; do not encode current partial behavior into the model.

**Verify**: scheduler model plus fixed `c01_`/`c02_` regressions all pass.

### Step 4: Add expression-scope and validation properties

Extend existing proptest modules in `aksh-gha-expressions` and `aksh-gha-parser`; add server properties around `evaluate_concurrency`.

Generate JSON scalar/list/object values at bounded depth and context maps with unique sentinel values per context.

Properties:

- Workflow expressions can observe `github/inputs/vars` and cannot observe `needs/strategy/matrix`.
- Job expressions observe all six allowed contexts.
- Changing an unused context leaves evaluated group and cancellation unchanged (metamorphic property).
- Changing the referenced allowed context changes the group predictably.
- `cancel-in-progress` result equals `eval_bool` for generated true/false/truthy/falsy expressions.
- `GH-VALIDATE-01`: effective max+true returns an error. Workflow-level rejection leaves the whole scheduler snapshot unchanged. Job-level rejection terminates the job/run as Failure while leaving the concurrency substate byte-for-byte unchanged: `concurrency_groups`, `holder_keys`, `held_runs`, and `concurrency_blocked`.
- Empty group, malformed expression, missing required fallback, and invalid context reach terminal failure; no run remains queued/pending.
- Serialization/deserialization of bare string and mapping concurrency forms preserves group, queue, and cancel expression.

Do not broaden this into general GitHub expression conformance; plan 002 owns that domain.

**Verify**: expression/parser command and fixed `c05_`/`c06_` regressions pass.

### Step 5: Extract a deterministic runner-dispatch reducer and property-test it

The current broker listener mixes HTTP polling, worker processes, timers, and dispatch decisions. Extract only the decision logic needed for tests into a private reducer used by production code; do not create a second test-only implementation.

Suggested input events:

```text
JobArrived { job_id, payload }
JobFinished { job_id, result }
CancellationArrived { job_id, timeout }
GraceExpired { job_id }
Shutdown
```

Suggested effects:

```text
Start(job)
SendCancel(job, effective_timeout)
ArmKill(job, deadline)
Kill(job)
IgnoreCancellation(job)
Acknowledge(message)
ExitEphemeral
```

Properties:

- `RUN-ID-01`: wrong-ID cancellation produces only Ignore and never changes active state.
- `RUN-TIME-01`: effective timeout is `max(input, 60s)` and kill delay is `effective - 15s`.
- `RUN-IDEMP-01`: repeated matching cancellation emits graceful cancellation only once and deterministically replaces the forced-kill deadline with `max(new_timeout, 60s) - 15s`; generated traces cover both deadline extension and shortening.
- Cancel before job start and after completion is ignored.
- `RUN-ORDER-01`: every SendCancel for a job is preceded by Start for that job.
- `RUN-OVERLAP-01`: overlapping job arrival cancels the prior job; successor Start occurs only after prior JobFinished/Kill. No generated trace has two active workers.
- Once/ephemeral exits only after the first dispatch becomes terminal; a successor is not acknowledged then silently discarded.

Replace the current inline 300-second broker cancellation behavior with reducer-driven effective timeout handling in `broker_listener.rs`, `job_dispatcher.rs`, and the active `RunningJob` cancellation path. Production must consume the cancellation message timeout; the reducer must be the single decision source. Use pure logical deadlines where possible. If Tokio paused time is required, enable the `test-util` feature only in `aksh-runner` dev-dependencies and use paused time; never sleep.

**Verify**: runner-properties command exits 0 with 256 cases. Existing `parse_timespan_secs`, broker-listener, and job-dispatcher tests pass.

### Step 6: Add HTTP-level generated workflow sequences

Use the actual Axum router and pin generated operations to `/api/v1/runs`, `/api/v1/runs/{run_id}`, `/api/v1/runs/{run_id}/cancel`, `/internal/test/jobs/complete`, and broker `/broker/completejob`. Generate operation sequences of length `1..=24`; use a fresh `AppState`/tempdir per case.

Keep the generator semantic, then render YAML:

```text
WorkflowSpec { repo, workflow_group?, queue, cancel_expr, jobs[] }
JobSpec { id, needs, matrix?, max_parallel?, concurrency?, reusable_call? }
```

After every operation, inspect public JSON plus a normalized locked-state snapshot.

Properties:

- Native run/job statuses and internal status maps agree.
- Pending runs expose no dispatchable broker job.
- Cancellation of an in-progress holder emits exactly one `JobCancellation` for the official agent job GUID.
- Pending-only replacement emits no runner cancellation.
- A successor becomes dispatchable after the predecessor's terminal completion.
- Different repositories never interfere; case variants in one repository do.
- No cross-run timeline/log markers appear in captured output.

Configure 64 cases in PR CI because these tests are heavier. Persist every failure seed.

**Verify**: HTTP-sequence command exits 0; no test uses wall-clock sleeps.

### Step 7: Add a bounded official differential seed harness

Create `benchmarks/real-world/concurrency-property-cases.json` with schema-versioned, named cases. Initial deterministic corpus:

- single replacement with 3 arrivals;
- max FIFO around depths 0, 1, 99, 100, 101;
- cancel expression true and false;
- case variant pair;
- matrix max-parallel plus shared/different group;
- needs-output-derived group;
- caller-only, embedded-only, equal-key, different-key, and nested JobSet;
- wrong-ID, before-start, during-run, after-finish, and repeated `JobCancellation`;
- timeouts 0, 44, 45, 59, 60, 61, 300, and multi-day TimeSpan forms.

Create `run-concurrency-property-probes.py` to:

1. Render each case to workflow/submission/message sequences.
2. Run control-plane cases against live GitHub only when explicitly configured with a test repository and token.
3. Run runner-side cases against the pinned unmodified official runner and `aksh-runner` against the same aksh server trace.
4. Normalize infrastructure-only differences.
5. Compare conclusions, start/end partial order, pending/cancelled states, broker message type/body, job ID, cancellation order, and worker overlap.
6. Write one isolated result directory per case.
7. Exit nonzero for contamination, contradictory status, unexpected overlap, or semantic mismatch.
8. Emit a minimized JSON case that can be copied into the deterministic corpus.

Live probes are not random CI. Generate candidate cases locally, shrink locally, and promote only minimized counterexamples into the corpus. The harness must print the official runner commit and documentation retrieval date in its report.

**Verify**:

- A dry-run/schema validation command exits 0 without credentials.
- Create a small named contaminated fixture in `benchmarks/real-world/fixtures/` and prove the harness rejects it; do not depend on an unverified historical capture.
- One official-runner cancel case and one live GitHub control-plane case pass when credentials/port redirect are available.

### Step 8: Add bounded CI and an intensive scheduled profile

In `.github/workflows/ci.yml`:

- PR/default job: pure/model/expression/runner properties at `PROPTEST_CASES=256`; HTTP properties at 64.
- Scheduled/manual job: release mode, 10,000 cases, `PROPTEST_MAX_SHRINK_ITERS=100000`, single test thread for deterministic trace output.
- Upload `proptest-regressions`, normalized failing operation trace, and differential report on failure.
- Never run live GitHub or privileged official-runner port-redirection probes in ordinary PR CI.

Set a deterministic per-job timeout and fail if no tests match a filter. Before case-count runs, use `cargo test -- --list` and an explicit match-count assertion so a filtered command cannot silently run zero properties. Mechanically search new property-test files and fail if any contains `sleep(`.

## Test plan summary

| Layer | Cases/profile | Oracle | Main risks covered |
|---|---:|---|---|
| Pure functions | 256 PR / 10k scheduled | algebra + official shape | boundaries, serialization, key normalization |
| Scheduler model | 256 / 10k | independent reducer + GitHub docs | queueing, promotion, release, cancellation, leaks |
| Cross-gate | 256 / 10k | independent reducer + GitHub docs | max-parallel, needs, matrix, JobSet atomicity |
| Expressions/parser | 256 / 10k | allowed-context tables + expression engine | missing context, truthiness, invalid combinations |
| Runner reducer | 256 / 10k | official runner source | IDs, timing, idempotency, overlap ordering |
| HTTP sequences | 64 / 1k | public API + model | integration state drift and message emission |
| Differential corpus | deterministic | live GitHub + pinned official runner | undocumented behavior and wire compatibility |

## Done criteria

- [x] Every GH-* and RUN-* invariant has at least one named property.
- [x] Stateful sequences run at least 256 cases in the fast profile and 10,000 in the intensive profile.
- [x] Generated sequences check invariants after every operation, not only at the end.
- [x] Model transitions do not call production transition helpers.
- [x] Queue boundaries include 99/100/101 pending holders.
- [x] Matrix shrinking reaches `max-parallel: 1` and a two-cell counterexample.
- [x] JobSet properties cover caller, embedded, equal-key, different-key, nested, rollback, cancellation, and final-member release.
- [x] Runner properties prove wrong-ID ignore, minimum timeout, kill offset, cancellation idempotency, and no worker overlap.
- [x] HTTP properties prove pending jobs are not dispatched and pending-only cancellation emits no runner message.
- [x] All proptest regression files are committed.
- [x] `cargo fmt --all --check` exits 0.
- [x] All commands in the Commands table exit 0 and execute a nonzero number of matching properties.
- [x] Differential harness dry-run passes without secrets and rejects the newly committed named contaminated fixture.
- [x] Official-runner differential recording is explicitly BLOCKED by the missing privileged prerequisites listed in Status.
- [x] `plans/README.md` keeps plan 006 non-DONE until the blocked live gate passes.

## STOP conditions

- Official GitHub documentation no longer contains the queue, FIFO, context, case, or validation semantics listed above.
- The pinned official runner changes `JobCancelMessage`, cancellation timing, or overlap dispatch behavior.
- A property requires reading concurrency fields inside runner code; that violates the control-plane boundary.
- A test oracle begins reusing production transition helpers instead of remaining independent.
- A live GitHub mismatch cannot be reproduced from an isolated minimized case.
- Property failures occur only under nondeterministic sleep timing; replace timing with logical/fake time before proceeding.
- Multi-key JobSet correctness would require silently weakening all-or-nothing acquisition.

## Maintenance notes

- Update the official runner pin deliberately. Review `JobDispatcher.cs`, `Runner.cs`, `JobCancelMessage.cs`, and official L0 tests before accepting new seeds.
- Documentation and live GitHub may diverge. Keep `documented`, `official-runner`, and `live-observed` oracle labels separate.
- Any production concurrency transition added later must add a model operation and preserve all after-each-operation invariants.
- Never delete a minimized regression merely because random generation no longer reaches it.
- Property tests complement, not replace, official-runner broker/Twirp E2E.
