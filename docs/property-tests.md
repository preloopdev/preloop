# Runner-Compatibility Property Tests

## Purpose and scope

This document defines the **native property tests required for the first four implementation areas** in `docs/property-testing-impl-log.md`:

1. DAG scheduling and dependency ordering.
2. Matrix expansion.
3. Step/job condition evaluation.
4. Step-update merging and cancellation reconciliation.

These are not all tests for one binary. The four areas cross the runner boundary:

| Area | Aksh code under test | Compatibility owner |
|---|---|---|
| DAG and dependency ordering | `crates/aksh-gha-parser`, `crates/aksh-runner-server` | Workflow/control-plane semantics; the official runner consumes the resulting job graph and status context. |
| Matrix expansion | `crates/aksh-gha-parser`, `crates/aksh-runner-server` | Workflow parser/control-plane semantics; the official runner receives one job payload per expanded combination. |
| Conditions | `crates/aksh-gha-expressions`, `crates/aksh-runner/src/worker/step_conditions.rs`, `steps_runner.rs` | Official runner worker condition registration/evaluation; job dependency gating is partly service-side. |
| Step records/cancellation | `crates/aksh-runner/src/worker/step_records.rs`, `server_queue.rs`, reporting code | Official runner worker reporting and Twirp `WorkflowStepsUpdate` semantics. |

A property test is incomplete if it tests only a pure helper while the production parser, server, or worker uses a different path. Each area therefore has two required layers:

- **Model properties:** fast, structured generators with an independent reference model.
- **Production-path properties:** the generated case passes through the real parser/server/worker boundary and the observable result is checked.

The official reference is pinned to `actions/runner` **v2.335.1**, commit [`7d737449ef346f6524f75688d0c9c95fa10ba10a`](https://github.com/actions/runner/tree/7d737449ef346f6524f75688d0c9c95fa10ba10a). The source links below use that commit so an upstream change cannot silently alter the oracle.

> A source link is a reference model, not permission to copy an implementation into the test subject. The expected-value model must not call the function under test.

## Common requirements

Every property suite MUST:

- use bounded generators and explicit maximum depth/size;
- use a deterministic `ProptestConfig` and preserve the seed on failure;
- shrink the structured input before rendering YAML/JSON;
- include at least one regression fixture for every fixed compatibility bug;
- distinguish `omitted`, `null`, empty, false, zero, and present values where the wire contract distinguishes them;
- assert errors explicitly instead of treating any `Err` as success;
- fail if the generated case is not exercised by the production path;
- test duplicate and out-of-order delivery where the protocol can retry;
- avoid broad normalization of IDs, names, statuses, or conclusions;
- record the official source path and commit beside each oracle rule.

The test harness MUST fail loudly when a filtered test discovers zero tests. A successful Cargo command with all tests filtered out is not verification.

---

## 1. DAG scheduling and dependency ordering

### Official references

The official runner does not own the GitHub control-plane queue, so this area has two references:

- GitHub workflow dependency contract: [`jobs.<job_id>.needs`](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#jobsjob_idneeds).
- The official runner worker condition implementation is [`StepsRunner.cs`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/StepsRunner.cs), especially its expression-function registration, condition evaluation, cancellation re-test, and skipped/failed completion branches. The implicit job/dependency behavior is partly control-plane/service-side rather than a public method in the runner repository; use the pinned GitHub workflow syntax contract and captured official behavior for that portion. Do not cite an unverified `ConvertToIfCondition` path as an official source file.
- Aksh implementation:
  - `crates/aksh-runner-server/src/lib.rs`: `promote_ready_jobs`, `need_satisfied`, run completion.
  - `crates/aksh-runner-server/src/scheduling.rs`: pure scheduler model.
  - `crates/aksh-gha-parser/src/dag.rs`: parser DAG validation/model.

The server property suite must not claim that `Success | Skipped` is the complete official behavior. GitHub documents that failed or skipped dependencies cause dependent jobs to skip unless the dependent condition overrides that behavior.

### Generator

Generate a structured workflow before rendering YAML:

```text
1–8 unique job IDs
0–8 needs edges, including cycles and unknown needs
job-level if: absent, success(), failure(), cancelled(), always(), and compounds
runs-on labels and max-parallel values
terminal outcomes: success, failure, cancelled, skipped
operation sequences: submit, validate, promote, poll, acquire, complete, cancel, retry
```

Generate both valid DAGs and intentionally cyclic graphs. Do not generate only edges to lower indices: that proves acyclicity by construction and cannot exercise parser rejection.

### Required model properties

For every operation prefix:

1. A job is in at most one of pending, queued, running, or terminal states.
2. A job is acquired at most once unless the protocol explicitly models a lease retry for the same owner.
3. A job cannot dispatch before every required dependency is terminal.
4. Once all dependencies are terminal, the dependent reaches exactly one of:
   - dispatched because its condition permits execution;
   - `Skipped` because the default dependency rule blocks it.
5. Failed or skipped dependencies do not leave a dependent pending forever.
6. `if: always()` can opt a dependent into execution after a failed/skipped dependency.
7. `if: failure()` and `if: cancelled()` follow the official status context, not a local “need satisfied” shortcut.
8. A terminal job never returns to pending, queued, or running.
9. Duplicate completion is idempotent and cannot promote a dependent twice.
10. Cancellation races have one deterministic terminal outcome.
11. Every valid acyclic graph eventually settles when all runnable jobs are completed.
12. Every cyclic graph is rejected before a job is queued.
13. Reordering map insertion or equivalent input declaration order does not change the semantic graph or terminal results. Queue ordering is checked separately where the official contract defines it.

### Production-path properties

The generated workflow MUST pass through:

```text
structured workflow → YAML → parse_workflow → expand_jobs → server submission → promote/acquire/complete
```

Assert production observations, not only model state:

- parser returns a structured cycle/unknown-needs error before submission succeeds;
- queued job IDs equal the parser’s expanded IDs;
- no duplicate broker acquisition occurs;
- downstream jobs emit one terminal status;
- run status becomes terminal rather than remaining `InProgress` with permanently blocked pending jobs.

### Required regression fixtures

- `build` fails; default `test` becomes skipped.
- `build` is skipped; default `test` becomes skipped.
- `build` fails; `cleanup` with `if: always()` runs.
- `build` fails; `cleanup` with `if: failure()` follows the official result.
- diamond graph: `build → test-a/test-b → deploy`.
- cyclic graph rejected before dispatch.
- duplicate completion does not create a second promotion.

---

## 2. Matrix expansion

### Official references

- Official source: [`MatrixBuilder.cs`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/WorkflowParser/Conversion/MatrixBuilder.cs).
- Official documentation and overwrite example: [Running variations of jobs in a workflow](https://docs.github.com/en/actions/using-jobs/using-a-matrix-for-your-jobs).
- Aksh implementation:
  - `crates/aksh-gha-parser/src/matrix_expand.rs`: model/expander.
  - `crates/aksh-gha-parser/src/lib.rs`: parser integration.
  - `crates/aksh-runner-server/src/lib.rs`: job promotion and expanded job identity.

### Generator

Generate a `MatrixSpec` with:

```text
0–4 axes, declared in random order
0–4 values per axis
booleans, integers, decimals, strings, null where accepted
0–5 exclude objects
0–5 include objects
include objects containing axis keys, extra keys, and overlapping extra keys
empty axes and all-excluded products
include-only entries that match no original row
deferred fromJSON(needs.*.outputs.*) axes
producer outcome: success, failure, cancelled, missing output, malformed JSON
```

The generator must preserve the distinction between:

```text
original Cartesian axis values
fields added by include
internal scheduling ID
base job ID
display name
matrix context
```

### Required model properties

Use an independent reference expander that retains the original Cartesian rows. For every generated matrix:

1. Cartesian count equals the product of axis lengths before exclusions, with overflow handled explicitly.
2. Every original combination is deterministic and has the expected axis declaration order.
3. An `exclude` object removes every matching original row and no non-matching row.
4. Include filters are matched against original axis compatibility, not fields added by an earlier include.
5. One include applies to every compatible original row, not only the first compatible row.
6. A later include can overwrite an earlier include-added key when official semantics allow it.
7. An unmatched include creates exactly one include-only row.
8. Later includes do not accidentally mutate an earlier include-only row as though it were an original Cartesian row.
9. Duplicate axis values are handled according to the official expansion contract; do not assert uniqueness merely because serialized value maps happen to match.
10. Empty-axis, all-excluded, and include-only behavior is explicit and regression-tested.
11. Expansion is deterministic for the same structured input.
12. Expanded IDs, base IDs, display names, and matrix contexts do not collapse into one normalized job name.
13. Deferred output either expands into concrete combinations or preserves the unresolved display template according to producer outcome; it must never silently collapse to the bare base ID.
14. Size limits are enforced before expansion; a generated case cannot cause unbounded allocation.

### Required fixed include regression

This documented case MUST be present because it catches the most important include bug:

```yaml
matrix:
  animal: [cat, dog]
  include:
    - color: green
    - animal: cat
      color: pink
```

The second entry must overwrite the earlier `color: green` on the original `cat` combination. It must not become a separate include-only row merely because `color` was introduced by the first include.

### Production-path properties

The generated workflow MUST pass through:

```text
structured workflow → YAML → parse_workflow → expand_jobs → server queue
```

Assert:

- the number of actual job plans equals the reference result;
- each plan has the expected matrix context;
- each actual scheduling ID is unique and stable;
- excluded jobs are never queued;
- include-only rows do not invent axis display values;
- deferred producer failure preserves the actual production display identity.

### Required regression fixtures

- official green→pink include overwrite.
- include applies to multiple original rows.
- all Cartesian rows excluded and no include-only rows.
- include-only row with an extra key.
- declaration-order permutation with stable combination set and official display ordering.
- scenario 63 unresolved identity:

```text
matrix-build-${{ matrix.case }}-${{ matrix.mode }}
```

---

## 3. Step and job condition evaluation

### Official references

- [`StepsRunner.cs`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/StepsRunner.cs) — step execution loop, status-function registration, condition evaluation, cancellation re-test, and skipped/failed completion.
- [`SuccessFunction.cs`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/Expressions/SuccessFunction.cs) — `success()` behavior, including the official null-status fallback to success.
- [`FailureFunction.cs`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/Expressions/FailureFunction.cs), [`AlwaysFunction.cs`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/Expressions/AlwaysFunction.cs), and [`CancelledFunction.cs`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/Expressions/CancelledFunction.cs) — status functions.
- The implicit job/dependency gate is partly service-side and is not represented by a verified public `ConvertToIfCondition` source file in this pinned runner tree. Anchor it to the [workflow syntax contract](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#jobsjob_idneeds) and official captured behavior; keep the expected-value model independent of aksh.
- Expression truthiness documentation: [Evaluate expressions in workflows and actions](https://docs.github.com/en/actions/reference/workflows-and-actions/expressions).
- Aksh implementation:
  - `crates/aksh-gha-expressions/src/lib.rs`.
  - `crates/aksh-runner/src/worker/step_conditions.rs`.
  - `crates/aksh-runner/src/worker/steps_runner.rs`.

### Generator

Generate a bounded expression AST, not only arbitrary strings:

```text
status calls: success, failure, cancelled, always
literals: null, booleans, integers, decimals, strings
context paths and missing paths
operators: !, &&, ||, ==, !=, <, <=, >, >=
function nesting and parentheses
optional ${{ }} markers
whitespace-only and malformed source strings
single-quoted strings with doubled quote escapes
```

Use a separate arbitrary-string strategy for parser safety. Do not confuse grammar generation with malformed-input generation.

### Required independent-oracle properties

The expected result MUST come from a small independent truth-table/reference evaluator. It must not call `effective_condition`, `contains_status_check_function`, or `evaluate_step_condition`.

Assert:

1. Default/empty/whitespace-only condition follows the documented default.
2. `success()`, `failure()`, `cancelled()`, and `always()` match the official status table.
3. A skipped state is not silently converted into a success/failure/cancelled status.
4. A condition containing no status function receives the implicit `success() && (...)` gate.
5. A real status-function call suppresses the implicit gate according to official conversion.
6. Text such as `"always()"` or `'failure()'` inside a string is not detected as a function call.
7. Doubled single-quote escapes do not terminate a string early.
8. Parentheses, harmless whitespace, and expression markers preserve semantics.
9. Compound conditions use normal precedence and do not mutate the context.
10. Missing paths, null, empty strings, zero, negative zero, arrays, and objects follow the pinned official truthiness/coercion rules.
11. Malformed conditions return a structured error, never panic, hang, or silently run an unintended step.
12. Evaluation is deterministic and bounded by an explicit maximum expression depth/size.

### Production-path properties

Run generated conditions through the actual worker decision path:

```text
workflow Step → job/step context construction → steps_runner::should_run_step → emitted step record
```

For job-level conditions, run through:

```text
workflow Job → parser plan → server promotion → terminal job state
```

Assert that the condition decision, step record, job status, and downstream scheduling decision agree.

### Required regression fixtures

- default condition after a prior failure.
- `always()` after failure and cancellation.
- `failure() || cancelled()` compound expression.
- quoted text containing `always()`.
- doubled quote escape containing status-function text.
- whitespace-only condition.
- malformed unterminated expression.
- missing context path versus explicit `null`.

---

## 4. Step-update merging and cancellation reconciliation

### Official references

- [`StepsRunner.cs`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/StepsRunner.cs) — lifecycle transitions that produce step updates.
- [`JobRunner.cs`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/JobRunner.cs) — job lifecycle, completion, and cancellation handling.
- [`StepsRunner.cs` search for `WorkflowStepsUpdate`](https://github.com/actions/runner/search?q=WorkflowStepsUpdate&type=code) — official reporting call sites; pin the exact containing type/method when updating the oracle.
- Results-service schema/reference: [`WorkflowStepUpdateService`](https://github.com/actions/runner/tree/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/RSWebApi) and the repository’s captured Twirp golden flows under `.runner-watch/golden/v2.335.1/`.
- Aksh implementation:
  - `crates/aksh-runner/src/worker/step_records.rs`.
  - `crates/aksh-runner/src/worker/server_queue.rs`.
  - `crates/aksh-runner/src/worker/steps_runner.rs`.
  - server Twirp handler in `crates/aksh-runner-server/src/lib.rs`.

### Generator

Generate:

```text
1–12 dispatched tasks
external IDs, numbers, names, synthetic setup/main/post/complete records
partial updates with each optional field omitted, null, or present
status sequence: pending/in-progress/completed
conclusion sequence: unset/succeeded/failed/skipped/cancelled mapping
out-of-order permutations
duplicate and retry updates
received records missing from dispatched and dispatched records missing from received
cancellation at every sequence position
late completion after cancellation
```

Keep external ID, number, and display name independent. Do not generate them as one identity.

### Required merge properties

Maintain an independent reference map keyed by `external_id`. After every generated update:

1. At most one record exists per external ID.
2. External ID, not number or name, controls identity.
3. Same number with different external IDs never merges.
4. Omitted fields preserve existing values.
5. Explicit null behavior follows the wire contract; it is not automatically treated as omission.
6. Status never regresses from completed to in-progress.
7. A non-zero conclusion is not erased by an unset/zero partial update.
8. A duplicate identical update is idempotent.
9. Out-of-order delivery follows the official cumulative-update rule.
10. Unrelated records and synthetic setup/complete records are preserved.
11. Final ordering is deterministic according to the official ordering field.
12. Every generated record has valid field types and valid timestamp syntax where timestamps are present.

### Required cancellation properties

For every cancellation point:

1. Every interrupted in-flight task receives exactly one terminal cancellation representation.
2. Completed successful/failed/skipped tasks are preserved and are not rewritten as cancelled.
3. Setup and complete-job synthetic records follow the official lifecycle behavior.
4. Received records not present in the dispatched list are handled according to the captured protocol contract; they must not be silently dropped by an accidental intersection operation.
5. Re-running reconciliation is idempotent.
6. Duplicate cancellation notifications do not create duplicate records.
7. A late completion cannot resurrect or regress a cancelled terminal record.
8. Cancellation conclusion mapping is tested from the actual Twirp schema; do not invent a timestamp sentinel such as `"cancelled"` in a timestamp field.
9. Final record order and `change_order` behavior are deterministic.

### Production-path properties

Run generated sequences through:

```text
actual worker step transition → ServerQueue::queue_update → WorkflowStepsUpdate body → server Twirp handler → stored/emitted record state
```

For cancellation, use the real worker cancellation path rather than calling only `reconcile_cancelled_steps`.

Assert both:

- the cumulative body sent to the results service;
- the final server-visible record set.

### Required regression fixtures

- duplicate in-progress update.
- completed update followed by stale in-progress update.
- same number with two external IDs.
- partial update that omits conclusion.
- out-of-order start/complete updates.
- cancellation with completed setup and open main step.
- cancellation with open post step.
- duplicate cancellation notification.
- late completion after cancellation.

---

## Completion gates

Do not mark an area complete until all gates pass:

- property tests are declared in the crate module tree and appear in `cargo test -- --list`;
- model and production-path suites both run;
- generated cases shrink to bounded, reproducible fixtures;
- no generated case panics, hangs, or allocates without a configured limit;
- the expected-value model is independent of the subject under test;
- every compatibility-sensitive rule cites the pinned official runner source or a captured official wire artifact;
- known gaps are marked incomplete rather than encoded as passing properties;
- fixed regressions are promoted to fixtures;
- targeted tests, `cargo fmt --all --check`, and the relevant workspace checks pass.

## Source-reference maintenance

When upgrading the runner version:

1. update the commit in this document;
2. re-open every linked official source file;
3. regenerate the fixed oracle tables and wire fixtures;
4. rerun the property suites and official capture replay;
5. record any semantic change as a new minimized regression fixture.
