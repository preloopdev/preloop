# Concurrency Deep Review

**Reviewed:** 2026-07-13  
**Snapshot:** working tree at `839791c` plus the uncommitted concurrency implementation  
**Authority:** current GitHub Actions documentation and `actions/runner` `main`; local plans and reports are evidence, not protocol authority

## Executive conclusion

The implementation has a sound basic shape: repository-scoped groups, workflow- and job-level holders, FIFO pending queues, `queue: single|max`, cancellation delivery, terminal-state locking, and focused tests. The official `JobCancellation` wire path is substantially closer to the official runner than the pre-change code.

It is **not ready to claim GitHub/official-runner concurrency parity**. One scheduler path can permanently self-block a matrix job, reusable-workflow concurrency is represented but never acquired, several GitHub-supported expression contexts are absent, run conclusions can remain stale after concurrency evaluation failures, and the runner's busy-job behavior contradicts the official run-service dispatcher. The existing “12/12” local comparison also contains cross-run log contamination and cannot support its stated score.

### Priority summary

| ID | Priority | Finding | Confidence |
|---|---:|---|---:|
| C-01 | P0 | Job promotion can install a concurrency holder before `max-parallel` permits dispatch, then park the job behind itself | High |
| C-02 | P0 | Reusable workflow `JobSet` concurrency is data-only; no acquisition path constructs `Holder::JobSet` | High |
| C-03 | P1 | Workflow/job concurrency expressions are evaluated without required `inputs`, `needs`, and `strategy` contexts | High |
| C-04 | P1 | Broker listener blocks for 45 seconds and may drop a new job, unlike official run-service dispatch | High |
| C-05 | P1 | Job concurrency evaluation failures can leave the run permanently `Queued`/`Pending` | High |
| C-06 | P1 | `cancel-in-progress` uses string equality instead of expression truthiness; dynamic `queue: max` conflicts bypass validation | High |
| C-07 | P2 | `holder_keys` grows for the lifetime of the server | High |
| V-01 | Release gate | Official-runner E2E is missing and the local 12/12 report is contaminated | High |
| V-02 | Investigation | Live case-sensitivity probe conflicts with current GitHub documentation | High evidence of conflict; unresolved cause |

## Reference behavior

Current GitHub documentation establishes these invariants:

1. A group is scoped to a repository and has at most one running job or workflow.
2. Default `queue: single` replaces the existing pending holder; `queue: max` admits up to 100 pending holders in FIFO wait order.
3. `queue: max` and an effective `cancel-in-progress: true` are incompatible.
4. Group names are documented as case-insensitive.
5. Workflow-level expressions may use `github`, `inputs`, and `vars`.
6. Job-level expressions may additionally use `needs`, `strategy`, and `matrix`.
7. `cancel-in-progress` may be an expression and therefore must use GitHub expression evaluation semantics, not string-only parsing.

Primary references:

- [GitHub: Control the concurrency of workflows and jobs](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency)
- [GitHub: workflow-level `concurrency` syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency)
- [GitHub: `jobs.<job_id>.concurrency` syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#jobsjob_idconcurrency)
- [GitHub: reusable workflow behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/reusing-workflow-configurations)
- [`actions/runner`: `JobDispatcher`](https://github.com/actions/runner/blob/main/src/Runner.Listener/JobDispatcher.cs)

## Findings

### C-01 — P0 — Promoted matrix job can park behind its own holder

**Problem**

`promote_next_from_group` installs the next job as `group.running` before rechecking `max-parallel`:

- `crates/aksh-runner-server/src/lib.rs:1871-1874` assigns `group.running = Some(next)`.
- The `Holder::Job` branch checks `under_max_parallel` only afterward at `:1916-1929`.
- When the limit is full, the job is moved to `pending_jobs`, but the concurrency holder remains installed.
- When `promote_ready_jobs` later retries that job (`:4444-4548`), it calls `try_acquire_concurrency` again.
- `try_acquire_concurrency` (`:1980-2051`) has no same-holder/idempotent-acquire case. It sees the job's own holder as running and parks the same job in the group's pending queue.
- No running worker can complete that holder, so the group and job are stuck.

**Concrete reproduction**

Use a matrix with `max-parallel: 1` and a matrix-derived concurrency group. Have an external run hold the group needed by cell A while cell B acquires a different group and starts. Release A's external group while B remains active. A is promoted into `group.running`, fails the max-parallel check, returns to `pending_jobs`, and later contends with itself.

**GitHub conflict**

GitHub composes `strategy.max-parallel` and job-level concurrency as gates; neither gate may create a permanent self-dependency. A pending job must eventually dispatch once both constraints are free.

**Decision**

Do not install a `Holder::Job` as running until every non-concurrency gate is satisfied. The safest state machine is:

1. Pop a candidate from the group.
2. Locate the blocked job.
3. If `max-parallel` is full, leave the holder at the front of the concurrency pending queue and leave the job in `concurrency_blocked`; do not use `pending_jobs` and do not set `running`.
4. Trigger another promotion attempt when a same-base matrix job completes.
5. Install `group.running` and enqueue the job atomically only when both gates are satisfied.

A narrower alternative is an idempotent same-holder acquire plus explicit ownership retention, but that leaves a non-running job occupying the group and can unnecessarily block unrelated runs. Avoid it.

**Affected symbols**

- `promote_next_from_group`
- `promote_ready_jobs`
- `try_acquire_concurrency`
- `under_max_parallel`

**Required tests**

- Matrix, `max-parallel: 1`, matrix-derived group, one cell initially concurrency-blocked and another active.
- Release the external holder while the sibling remains active.
- Assert the blocked cell is not installed as running yet.
- Complete the sibling and assert the blocked cell dispatches exactly once.
- Assert the group has neither a duplicate pending holder nor a running holder equal to a pending holder.

---

### C-02 — P0 — Reusable workflow concurrency is not enforced as a job set

**Problem**

The parser records reusable-call concurrency:

- `ReusableCallMetadata.caller_concurrency`
- `ReusableCallMetadata.embedded_concurrency`
- `ReusableCallMetadata.inner_job_ids`

The server defines and handles `Holder::JobSet`, but the only occurrences are the enum definition and match arms. No production acquisition path constructs `Holder::JobSet`. `RunRecord.reusable_calls` is used for reusable outputs, not concurrency acquisition. The implementation log itself records the JobSet path as partial at `docs/concurrency-implementation-log.md:70-75`.

Consequences:

- `concurrency` on a caller `uses:` job does not hold one slot for the complete called-workflow invocation.
- A called workflow's workflow-level concurrency is not acquired/released across its expanded inner jobs.
- Expanded inner jobs can overlap with holders that GitHub would serialize or cancel.

**GitHub conflict**

A reusable workflow invocation is still the caller job. Job-level concurrency applies to that invocation, while the called workflow also retains its own workflow-level concurrency semantics. Expanding the call into ordinary independent jobs cannot discard either scope.

**Decision**

Implement an explicit reusable-call gating phase after expansion and before individual inner jobs become runnable:

1. Resolve each `ReusableCallMetadata.inner_job_ids` to concrete `JobId`s.
2. Construct a `Holder::JobSet { run_id, job_ids }` for caller-level concurrency.
3. Construct a second JobSet holder for embedded workflow-level concurrency when present; the two keys may differ and must both be acquired before any member dispatches.
4. Park the entire set, not individual members, while either group is blocked.
5. On cancel, cancel every non-terminal member and emit consistent job/run state.
6. Release each holder only after all member jobs are terminal.
7. Support nested reusable calls without flattening away caller/embedded scope.

If atomic multi-key acquisition is required, establish a deterministic key order and rollback partial acquisitions to prevent lock-order deadlocks.

**Affected symbols**

- `submit_run_inner`
- `ReusableCallMetadata`
- `Holder::JobSet`
- `try_acquire_concurrency`
- `release_concurrency_for_job`
- `promote_next_from_group`

**Required tests**

- Two invocations of the same reusable caller job with caller-level concurrency serialize as whole JobSets.
- Called workflow-level concurrency conflicts across distinct callers.
- Caller and embedded groups differ; no member starts until both are acquired.
- Cancellation cancels every non-terminal member and releases both groups once.
- Nested reusable workflows preserve both scopes.

---

### C-03 — P1 — Required expression contexts are absent

**Problem**

`evaluate_concurrency` accepts `inputs`, `matrix`, `strategy`, and `needs`, but call sites supply empty values:

- Workflow level, `submit_run_inner:1192-1204`: `inputs` is an empty map. `WorkflowSubmission` at `aksh-gha-protocol/src/lib.rs:131-161` has no inputs field.
- Initial job level, `try_enqueue_with_job_concurrency:1510-1520`: `inputs` is empty, `strategy` is `{}`, and `needs` is `None`.
- Dependency-unblocked job level, `promote_ready_jobs:4498-4506`: `inputs` is empty, `strategy` is `{}`, and `needs` is `None`, even though `hydrate_needs_context` has already populated the runner message.
- `QueuedJob` at `lib.rs:1008-1024` carries only matrix values and `max_parallel`; it cannot reconstruct the full allowed context.

Examples that therefore group incorrectly or fail:

```yaml
concurrency: deploy-${{ inputs.environment }}

jobs:
  deploy:
    needs: build
    concurrency:
      group: deploy-${{ needs.build.outputs.target }}-${{ strategy.job-total }}
```

The implementation also inserts empty contexts rather than validating context availability, so unsupported workflow-level references such as `matrix.*` may degrade to empty values instead of a GitHub-style validation failure.

**GitHub conflict**

GitHub explicitly allows `github`, `inputs`, and `vars` at workflow level and adds `needs`, `strategy`, and `matrix` at job level.

**Decision**

Create one typed concurrency evaluation context rather than seven positional arguments:

```text
ConcurrencyContext {
  scope: Workflow | Job,
  github,
  inputs,
  vars,
  needs,
  strategy,
  matrix,
}
```

Populate it from the same canonical data used to build the runner job context:

- Add/default submission inputs in the native submission protocol and client, including workflow-dispatch/call input plumbing.
- Carry `JobPlan.inputs` and the resolved strategy object into `QueuedJob`.
- Extract `needs` through a helper shared with `hydrate_needs_context`; do not parse it back out of serialized job messages.
- Enforce the workflow/job context allowlist before evaluation.

**Affected files/symbols**

- `crates/aksh-gha-protocol/src/lib.rs::WorkflowSubmission`
- `crates/aksh-runner-server/src/concurrency.rs::evaluate_concurrency`
- `crates/aksh-runner-server/src/lib.rs::{QueuedJob,submit_run_inner,try_enqueue_with_job_concurrency,promote_ready_jobs,hydrate_needs_context}`
- runner client submission plumbing

**Required tests**

- Workflow-level group and cancellation expressions using `inputs` and `vars`.
- Job-level group and cancellation expressions using `inputs`, `needs` outputs, `strategy`, and `matrix`.
- The same job evaluated at initial readiness and after `needs` completion produces the same group.
- A context not allowed at that scope fails validation instead of resolving to an empty string.

---

### C-04 — P1 — Busy broker job behavior contradicts the official runner

**Problem**

`crates/aksh-runner/src/listener/broker_listener.rs:272-339` handles a new job while busy by awaiting the current job for up to 45 seconds inside the broker listener. On timeout it drops the new job message. During that await, the listener cannot poll or process `JobCancellation`.

The comment cites official `JobDispatcher` behavior, but the official run-service branch does something materially different:

- `JobDispatcher.Run` creates the next dispatcher without blocking the listener.
- `RunAsync` calls `EnsureDispatchFinished` for the previous dispatch.
- For `_isRunServiceJob`, `EnsureDispatchFinished` logs the invalid overlap, cancels the old worker immediately, and returns.
- The 45-second wait belongs to the legacy server-state recovery branch after the old request is already completed server-side; it is not the normal run-service behavior.

Source: [`actions/runner/src/Runner.Listener/JobDispatcher.cs`](https://github.com/actions/runner/blob/main/src/Runner.Listener/JobDispatcher.cs).

**Impact**

A server/runner race can lose a valid job after it has already been acknowledged. The listener also becomes cancellation-blind for up to 45 seconds. This is particularly dangerous during concurrency replacement, where cancellation and successor delivery are intentionally adjacent.

**Decision**

Mirror the official dispatcher architecture:

1. Keep the broker polling loop non-blocking.
2. Represent current and next dispatches explicitly.
3. If a run-service job arrives while another is active, cancel the previous dispatch immediately.
4. Queue/start the successor through dispatcher state; never drop an acknowledged job merely because the previous worker did not exit within 45 seconds.
5. Keep forced-kill timing attached to the previous dispatch, not the broker listener.
6. Treat the AzDO `PipelineAgentJobRequest` branch separately; the project targets broker/run-service compatibility.

**Required tests**

- Active job ignores graceful cancellation; successor arrives. Assert the listener continues polling and successor is retained.
- Cancellation arrives while successor is waiting. Assert it reaches the correct job ID.
- No new job is dropped after acknowledge.
- Official runner and `aksh-runner` produce equivalent dispatch order for cancel-in-progress replacement.

---

### C-05 — P1 — Concurrency evaluation failure leaves a stale run conclusion

**Problem**

Two paths mark a job failed without recomputing the run:

- Initial ready jobs: `try_enqueue_with_job_concurrency:1521-1531` writes `Failure`; `submit_run_inner:1461-1473` then inserts the run with `status: Queued` unconditionally.
- Later ready jobs: `promote_ready_jobs:4507-4519` writes `Failure` for evaluation error or empty group and continues without calling `summarize_run` or emitting a terminal transition.

By contrast, normal completion and cancellation recompute the run aggregate. A single-job workflow can therefore expose `job=Failure` and `run=Queued` forever.

**GitHub conflict**

The live empty-group probe shows GitHub concludes the run as `failure` and creates no jobs. Invalid job-level concurrency must likewise produce a terminal, internally consistent workflow conclusion rather than a permanently queued run.

**Decision**

Centralize terminal job transitions in one helper that:

1. Applies the terminal lock.
2. Updates the job status.
3. Applies matrix fail-fast if relevant.
4. Promotes dependents or marks them skipped according to existing dependency semantics.
5. Recomputes `run.status` with `summarize_run`.
6. Returns the job/run events that must be emitted after the lock is released.

Use it for initial evaluation failure, deferred evaluation failure, queue overflow, cancellation, and normal completion. Avoid scattered direct writes to `run.jobs`.

**Required tests**

- Single initial-ready job with invalid concurrency expression: job and run both fail.
- Needs-unblocked job with invalid `needs.*`-derived group: job and run reach terminal status.
- Multi-job run: one evaluation failure aggregates according to existing failure/skip rules.
- API and NDJSON events expose the same final state.

---

### C-06 — P1 — Cancellation expressions are reduced to string equality

**Problem**

`crates/aksh-runner-server/src/concurrency.rs:91-96` resolves `cancel-in-progress` to a string and treats only case-insensitive `"true"` as true. The expression crate already provides `eval_bool` and GitHub-style truthiness. Numeric or other truthy expression values are therefore coerced differently from ordinary GitHub expressions.

Parser validation at `aksh-gha-parser/src/lib.rs:680-685` rejects only a literal value whose stored string is exactly `"true"`. This passes validation:

```yaml
concurrency:
  group: deploy
  queue: max
  cancel-in-progress: ${{ true }}
```

At runtime it resolves to true and follows the cancel-running branch before queue behavior, even though current GitHub documentation says `queue: max` and `cancel-in-progress: true` are incompatible.

**Decision**

- Evaluate `cancel-in-progress` with `aksh_gha_expressions::eval_bool` against the same typed context used for the group expression.
- After evaluation, reject an effective `queue: max` + `cancel=true` before state acquisition.
- Probe GitHub with both `${{ true }}` and `${{ false }}` to determine whether GitHub rejects the syntax statically or rejects only an effective true value. Match its validation timing and conclusion.
- Keep the parser's literal fast-fail, but do not rely on it as the only guard.

**Required tests**

- Boolean literal true/false.
- Expression true/false.
- Truthy/falsy expression values accepted by the expression engine.
- Dynamic true with `queue: max` cannot cancel an existing holder or acquire a slot.
- Validation failure leaves no concurrency group mutation.

---

### C-07 — P2 — `holder_keys` is never reclaimed

**Problem**

`track_holder_key` inserts keys into `InnerState.holder_keys` at `lib.rs:2054-2059`. `release_concurrency_for_run` reads the entry at `:1775-1804`, but no `remove`, `retain`, or equivalent cleanup exists. Every run that touches concurrency leaves an entry for the server lifetime.

This is auxiliary unbounded growth even if run-history retention is intentional. It also leaves stale reverse-index data that repeated release calls must scan.

**Decision**

Make reverse-index ownership explicit:

- Remove a key from the run's vector when that holder is removed from running or pending state.
- Remove the run entry when the vector becomes empty.
- Do not simply delete the whole entry on the first release: one run may hold multiple keys because reusable caller and embedded concurrency can coexist.
- Add a debug invariant that every reverse-index key points to a group containing a holder from that run.

**Required tests**

- Terminal workflow-level holder leaves no reverse-index entry.
- Pending replacement/queue overflow leaves no stale entry.
- A run holding two keys releases them independently and is removed after the second release.
- Repeated release is idempotent.

## Verification and evidence review

### V-01 — Release gate — Existing parity evidence is insufficient

The implementation log explicitly says official-runner E2E was not run (`docs/concurrency-implementation-log.md:70-75`). The available E2E uses `aksh-runner` against the aksh server, so both sides can share the same divergence.

The local comparison report claims “12/12,” but its own details invalidate that score:

- `LOG-CONTENT-COMPARE.md:63-67` records `SHOULD_NOT_REACH_EXECUTED` in the cancelled run's aksh capture.
- The same capture contains `DONE=01` from another scenario.
- It records `job map listed success but run conclusion cancelled` and still treats the result as a match.
- Additional scenarios contain accumulated markers from unrelated runs (`:89-90`, `:104-116`).

This is evidence of capture/log-isolation failure, not parity. The report must not be used as a release sign-off.

**Required release gate**

1. Fix capture isolation so every run/job/step log is keyed by the official identifiers and contains no marker from another run.
2. Make any cross-run marker, contradictory job/run conclusion, or executed `SHOULD_NOT_REACH` marker a hard failure.
3. Run the cancel/pending/successor scenarios with an unmodified official `actions/runner` through the broker + Twirp path.
4. Compare message order, cancellation body, step conclusion, `Complete job`, run conclusion, and successor dispatch.
5. Add targeted scenarios for C-01 through C-06 before rerunning the broad suite.

### V-02 — Investigation — Case sensitivity remains unresolved

Current GitHub documentation explicitly says group names are case-insensitive, and `concurrency_key` lowercases repository and group. The live probe ran `CaseGroup` and `casegroup` concurrently twice. That is real contradictory evidence, but it is not sufficient reason to diverge from the documented contract.

**Decision**

Keep case-insensitive normalization until a controlled probe establishes otherwise. Re-run within one workflow definition using dispatch inputs to vary only the group casing, and capture the exact evaluated group values and repository. If live behavior remains case-sensitive, preserve the discrepancy as a versioned compatibility decision with both documentation and capture attached; do not silently change it.

## Verified strengths

The review found these paths aligned with the current documented/official shape:

- `concurrency_key` scopes by repository and normalizes case as GitHub currently documents.
- Basic uncontended acquire, default pending replacement, `queue: max` FIFO/100-entry overflow, and run-level holder release have focused state-machine tests.
- The server emits `JobCancellation` with `jobId` and TimeSpan-shaped `timeout` and uses a separate high message-ID range.
- `aksh-runner` matches cancellation by job GUID, parses `hh:mm:ss` and `d.hh:mm:ss[.fraction]`, clamps the cancellation timeout to at least 60 seconds, and keeps cancellation handling non-blocking once the message is received.
- Late success/failure cannot overwrite a terminal cancellation.
- The local live GitHub probes substantiate basic cancel-in-progress, FIFO pending, pending replacement, job-level serialization, workflow-level whole-run holding, empty-group failure, expression group by ref, and matrix serialization behavior.

These strengths do not close the findings above; several pass only the uncomplicated single-gate cases.

## Checks executed during review

```text
cargo test -p aksh-runner-server concurrency --quiet
  15 passed, 0 failed

cargo test -p aksh-gha-parser concurrency --quiet
  5 passed, 0 failed

cargo test -p aksh-runner parse_timespan_secs --quiet
  targeted TimeSpan tests passed
```

These checks prove the existing targeted tests pass. They do not exercise C-01, reusable JobSet acquisition, the missing expression contexts, official-runner dispatch, or capture isolation.

## Recommended implementation order

1. **C-01:** repair the gate/promotion state machine and add the deadlock regression test.
2. **C-05:** centralize terminal job transitions so new failures cannot leave stale runs.
3. **C-03 + C-06:** introduce a typed evaluation context, wire all allowed contexts, use boolean evaluation, and enforce the dynamic queue conflict before mutation.
4. **C-02:** implement reusable caller/embedded JobSet acquisition with deterministic multi-key handling.
5. **C-04:** replace listener-local 45-second blocking/drop behavior with official-style dispatcher state.
6. **C-07:** reclaim and invariant-check reverse-index state.
7. **V-01:** fix capture isolation and run the official-runner broker/Twirp E2E.
8. **V-02:** rerun the controlled case probe; change normalization only if the evidence justifies an explicit docs-vs-live compatibility choice.

## Exit criteria

Concurrency parity may be claimed only when all of the following are true:

- No holder is installed as running before all earlier gates permit dispatch.
- Caller and embedded reusable-workflow concurrency hold complete job sets.
- Every GitHub-allowed context is available at the correct scope and disallowed contexts fail validation.
- `cancel-in-progress` uses expression boolean semantics and cannot bypass `queue: max` validation.
- Every concurrency error produces consistent terminal job/run state and events.
- The broker listener never blocks polling for 45 seconds or drops an acknowledged successor job.
- Reverse-index state is reclaimed after holders leave all groups.
- The targeted regression suite passes.
- An unmodified official runner passes cancel, pending, forced-kill, and successor-dispatch E2E over broker + Twirp.
- Per-run captures are isolated and the comparison harness hard-fails contradictory conclusions or cross-run markers.
