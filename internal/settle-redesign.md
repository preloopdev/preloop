# Settle Path Redesign: Targeted Queries Instead of Full Graph Load

## Goal

Replace `settle_job`'s load-entire-graph → sweep → flush pattern with targeted
queries and conditional writes. A completion is a one-job event with local
consequences; it should not materialize the whole run.

Current cost per completion: full `load_graph` (all jobs + needs + messages),
O(n²) dependent scan in `settle_node`, full `sweep`, full `flush` — all under
the run lock. For a 500-leg matrix: 500 rows × 500 completions = 250k row reads,
serialized behind the lock (133s of lock wait in the load profile).

Target cost per completion: ~6 indexed queries + a handful of writes, run lock
held only for the final status transition.

## New Flow

```
settle_job(run_id, job_id, status):
  0. Reject a non-terminal status (BadRequest) before any write
  1. Decide the effective status (first verdict, continue-on-error)
  2. Mark the job terminal (outputs/annotations only when reported)
  3. Retire the attempt rows, clear the machine binding, settle the newest
     attempt's orphaned in-progress steps
  4. Release the job's concurrency hold (promotes the group's next waiter)
  5. Fail-fast siblings (when this job failed)
  6. Fold reusable callers, walking up nested callers
  7. Retry the cohort waiters that `promote_waiter` re-parked at max-parallel,
     on each free gate holding one of the settled legs' cohort waiters (FIFO)
  8. Refresh `remaining_needs` for the dependents of everything that just
     turned terminal (one indexed UPDATE per settled job; it returns the
     dependents left `blocked` with no needs outstanding), and note each
     settled job's cohort for parked-leg admission
  9. Summarize the run from the DB aggregate; on the first terminal
     transition release the workflow-level hold and emit run.completed.v1
 10. Emit job.completed.v1 for every job this completion concluded (the job,
     its fail-fast siblings, the folded callers)
 11. Promote, over the rows of the jobs involved: admit the parked legs of the
     settled cohorts a freed slot can take, then decide and promote every
     dependent step 8 left unblocked
```

No step of a completion loads the run graph. Promotion's decisions — needs
hydration into the runner message, job/JobSet gate admission, max-parallel
cohorts, deferred matrix expansion, unhostable `runs-on` rejection — are not
conditional writes, but each reads only the promoted job's own rows
(`jobs`, `job_specs`, `job_messages`, `job_needs`), the run's submission when a
gate or environment needs it, and, for the job's `needs`, a `NeedsView`: the
rows its declared needs reach. `Sweep::promote` stays the implementation for
submit, rerun and cancel, which do build the whole run; the
completion path shares its pure decisions (environment name and verdict, gate
evaluation, `dependency_decision`) through `promote.rs` and `runtime_scheduling`.
Deferred expansion is job-scoped too (see Deferred Expansion).

A dependent is handled by a work queue inside the completion's transaction:
its `if:` decides `Run`, `Skip` or `Error`. A skip or error settles the
dependent without running it (the same writes a settled job gets, through
`settle_unrun`), which may unblock its own dependents; those form the next
pass, so a skip cascade finishes in the completion that started it. A job that
ends during promotion (unservable labels, denied environment, failed gate)
cascades the same way. `Run` hands a reusable caller (after its JobSet gates)
or a deferred matrix to the expansion queue and everything else to
`promote_one`.

## Lock Scope Decision

We keep a lock, but shrink it dramatically.

**Why any lock at all:** three things need a single serialization point.
1. The run-complete decision (check-then-act on the aggregate). Two completions
   must not both fire `run.completed.v1`.
2. Cancel-vs-complete races. Conditional UPDATEs make the job-level outcome
   safe (whoever commits first wins; the loser matches zero rows), but the
   run-level "did it complete or was it cancelled?" still needs one decider.
3. Deadlock ordering with concurrency group locks (run-before-group, per the
   existing audit).

**What we do NOT lock for:** dependent unblocking (conditional UPDATE is
atomic — check and write in one statement, no lost wakeup), fail-fast
(idempotent conditional UPDATE), the job's own status write (row lock is
implicit).

**Phase 1 (this redesign):** hold the run lock for the whole transaction, as
today. The win comes from targeted queries replacing the graph load — hold
time drops from ~7ms to ~1ms per completion even without shrinking lock scope.

**Phase 2 (follow-up, optional):** shrink to short-lock-only-at-step-8. Requires
auditing every lock_run call site for the new ordering protocol. Not in scope
here.

## The Needs-Met Conditional UPDATE

The critical SQL. Unblock a dependent only if every need is satisfied,
with the matrix rule: a need on a base_id is met only when *every* leg is
terminal (a leg's own id never appears in job_needs).

```sql
UPDATE jobs
SET queue_state = 'ready', enqueued_at = now()
WHERE run_id = $1 AND job_id = $2
  AND queue_state = 'blocked'
  AND NOT EXISTS (
    -- Is there any need with at least one non-terminal matching job?
    SELECT 1 FROM job_needs n
    WHERE n.run_id = $1 AND n.job_id = $2
      AND EXISTS (
        SELECT 1 FROM jobs dep
        WHERE dep.run_id = $1
          AND (dep.job_id = n.needs_job_id OR dep.base_id = n.needs_job_id)
          AND dep.status NOT IN ('success','failure','cancelled','skipped','timed_out')
          -- Expanded matrix parents don't count; their legs do.
          AND NOT (
            dep.kind = 'matrix_parent'
            AND EXISTS (
              SELECT 1 FROM jobs c
              WHERE c.run_id = dep.run_id AND c.parent_job_id = dep.job_id
            )
          )
      )
  )
```

If this matches zero rows, the dependent wasn't ready (or another completion
already unblocked it). Atomic — no separate lock, no lost wakeup.

Uses: `job_needs_reverse ON job_needs(run_id, needs_job_id)` for the outer
lookup; `jobs` PK on `(run_id, job_id)`. The `dep.base_id` join needs a
`(run_id, base_id)` index — verify it exists, add if not.

## Fail-Fast Siblings

When a job fails and fail-fast is enabled for its matrix:

```sql
UPDATE jobs
SET status = 'cancelled', queue_state = 'none',
    completed_at = COALESCE(completed_at, now())
WHERE run_id = $1 AND base_id = $2 AND job_id != $3
  AND status NOT IN ('success','failure','cancelled','skipped','timed_out')
```

Idempotent — concurrent failures both run it, second matches zero rows.
Each cancelled sibling then flows through the dependent-unblock step
(their dependents may now unblock with a cancelled need, which is correct:
cancelled counts as terminal for needs-met).

Fail-fast flag location: check `job_specs` / run config at implementation time.
Default behavior (per existing code): fail-fast is ON unless explicitly disabled.

## Reusable Workflow Caller Fold

When a job that is an inner job of a reusable call completes:

1. `SELECT parent_job_id FROM jobs WHERE run_id=$1 AND job_id=$2`
   — if the parent's `kind = 'reusable_caller'`, proceed.
2. All-terminal check:
   ```sql
   SELECT COUNT(*) FROM jobs
   WHERE run_id=$1 AND parent_job_id=$2
     AND status NOT IN ('success','failure','cancelled','skipped','timed_out')
   ```
   Zero → ready to fold.
3. Load inner outputs: `SELECT job_id, outputs FROM jobs WHERE run_id=$1 AND parent_job_id=$2`
4. Evaluate `output_definitions` expressions (existing `preloop_gha_expressions`
   code, unchanged) against inner outputs.
5. Terminalize the caller:
   ```sql
   UPDATE jobs SET status=$3, outputs=$4, queue_state='none',
          completed_at=COALESCE(completed_at, now())
   WHERE run_id=$1 AND job_id=$2
   ```
   Status = `aggregate_need_status` over inner statuses (failure > cancelled >
   skipped > success).
6. Treat the caller as a completed job: refresh the `remaining_needs` of every job
   that declares it (or its matrix base) as a need, the same unblock step each
   settled job gets (`Sweep::refresh_remaining_needs`). This replaces pg's
   in-memory sweep propagation.
7. Emit `job.completed.v1` for the caller. **Note:** pg currently skips this
   (divergence from lite); the redesign fixes it for parity.
8. Walk up: if the caller itself has a `parent_job_id` pointing to another
   caller, repeat from step 2. (Nesting is bounded, practically 2–3 deep.)

Metadata lives in `job_specs.reusable_call` JSONB (`caller_job_id`,
`output_definitions`, `inner_job_ids`, `inputs`). The `parent_job_id` column
is the indexed lookup path; fall back to the metadata's `inner_job_ids` list
if stricter fidelity is needed.

A deferred caller (a `uses:` whose subtree materializes at runtime) starts with
`inner_job_ids` empty. Its expansion fills them: the registered subtree becomes
the caller's `inner_job_ids`, the callee metadata lands on the callee spec rows,
and a deferred matrix parent named in a caller's list is replaced by its legs
when it expands. These are lite's `write_reusable_meta` and
`splice_matrix_parents`. Without them a deferred caller never folds on PG.

## If: Condition Evaluation (Per Newly-Unblocked Job)

When the conditional UPDATE unblocks a dependent, evaluate its `if:` before
enqueueing. Three targeted queries replace the graph-derived context
(investigated from source):

**Q1** — the job's needs, base context, and condition:
```sql
SELECT n.needs_job_id, m.condition_context::text, s.if_condition
FROM jobs j
LEFT JOIN job_needs n ON n.run_id=j.run_id AND n.job_id=j.job_id
LEFT JOIN job_messages m ON m.run_id=j.run_id AND m.job_id=j.job_id
LEFT JOIN job_specs s ON s.run_id=j.run_id AND s.job_id=j.job_id
WHERE j.run_id=$1 AND j.job_id=$2
```

**Q2** — statuses + outputs for direct needs (matrix-aware):
```sql
SELECT n.needs_job_id, j.status, j.outputs::text
FROM job_needs n
JOIN jobs j ON j.run_id=n.run_id
  AND (j.job_id=n.needs_job_id OR j.base_id=n.needs_job_id)
WHERE n.run_id=$1 AND n.job_id=$2
```
Group by `needs_job_id` in code; aggregate with existing `aggregate_need_status()`;
merge outputs with `extend()` (later legs overwrite — matches current behavior).

**Q3** — transitive ancestor statuses (for `success()`/`failure()`/`cancelled()`).
The recursion must follow the *matched rows*, not the declared need ids: a need
names a job or every job with that base id, and an ancestor reached through a
matrix base is the base's legs' own needs. Recursing on need ids alone loses
every ancestor behind a matrix base (`leaf needs mid`, `mid needs fan`, a leg of
`fan` failed: `failure()` in `leaf` must be true). `NeedsView::load` does it in
one recursive query over `jobs` joined to `job_needs` by `job_id = needs_job_id
OR base_id = needs_job_id`, excluding an expanded matrix parent (its legs speak
for it), and returns each reached row's status, base id, outputs and own needs.
Q2's direct statuses and outputs and Q3's ancestors come out of that one view;
`dependency_decision_of` and `hydrate_needs_context` read it through the
`NeedsSource` trait exactly as they read a whole `RunRecord`.

Then call `dependency_decision_of` with the dependent's own node (needs, `if:`,
condition context) and the view. Outcomes: `Run` → hand off (reusable caller
through its JobSet gates, deferred matrix to expansion, anything else to
`promote_one`); `Skip` → settle Skipped; `Error` → settle Failure. A settle
unblocks the dependent's own dependents, which the work queue decides in the
next pass.

## Run Status (Short Lock)

After the job write, take the runs-row lock and run the existing
`summarize_run_tx` (DB aggregate — already proven in the reaper and cancel
paths). It writes `runs.status`/`conclusion` directly. If the run newly
transitioned to complete: release the workflow-level concurrency slot
(existing `release_concurrency_for_run`) and emit `run.completed.v1`.

This replaces `graph.resummarize()` + `flush_run`. The old path computed from
the in-memory graph before flush; the new path computes from the DB after
the write. The flush-ordering bug that killed the earlier attempt does not
apply — there is no in-memory graph anymore.

## Enqueue

For each job that passes its `if:` check: insert the queue/request rows.
Extract the exact INSERT set from the existing sweep promotion code
(`Sweep::decide` → queue insertion). The columns and tables don't change,
only the trigger (per-job targeted instead of whole-run scan).

## What Gets Deleted (Completion Path Only)

- the unconditional `load_graph` in `settle_job` / `complete_job`
- the O(n²) dependent scan in `settle_node` (replaced by the targeted
  `remaining_needs` refresh)
- the `Sweep`'s in-memory node map, dirty tracking and `flush()` for the
  settled job itself
- `graph.resummarize()` for the settled job (replaced by `summarize_run_tx`)
- the in-memory `propagate_reusable_outputs` fold in `complete_job_inner`
  (replaced by the targeted, metadata-driven fold)
- the in-memory `Sweep::complete_node` and its fail-fast helper, with
  `CompletionApplied` and the graph-based `enqueue_cancellation` (no callers
  once the targeted settle landed)
- `PgBackend::promote_unblocked`, the sweep over the loaded graph a completion
  ran to promote its dependents, with the graph load and flush it implied

Promotion of newly-unblocked dependents and of capped cohort legs is
job-scoped too: parked legs of the settled cohorts are admitted in job-id order
over their own rows by `promote::admit_cohorts` (one candidate query per step; a
leg that settles at its `if:`/label/environment/gate check frees the slot and
admission continues), dependents go through the work queue in
`pg/dependents.rs`, and `promote::promote_one` is the shared job-scoped
hydrate → environment → gate → enqueue routine. Nothing on the completion path
calls `Sweep` or `load_graph`; `Sweep` remains for submit, rerun, cancel and
the reaper.

## Deferred Expansion

A reusable caller or a `fromJSON` matrix whose `if:` passed waits in the
expansion queue (`pending_expansion`); `pg/expansion.rs` runs both halves of its
expansion without the run graph.

`claim_expansion` leases the oldest node (`expanding`, `expand_generation + 1`)
and snapshots the build inputs from the node's own rows (`jobs`, `job_specs`,
`job_messages`, `job_needs`), a `NeedsView` of the jobs its declared needs reach
(their outputs, merged per base id by `NeedsView::outputs_by_base`) and the run's
planning inputs (submission, GitHub context, workspace snapshot, workflow
identity).

`apply_expansion` runs under the run lock and the generation fence (a node
cancelled or re-leased mid-build discards the subtree). Per build:

- **failed build / empty matrix**: the node settles through `settle_unrun`
  (`failure` / `skipped`) and its dependents are decided by
  `promote_dependents`, so a skip cascade finishes in the apply;
- **matrix subtree**: the legs' rows are written, the caller metadata
  `inner_job_ids` naming the parent are spliced (found with one JSONB
  containment query, not a scan of every spec), the parent leaves the run's
  status map (its legs name it as `parent_job_id`) and its placeholder request
  is purged;
- **reusable subtree**: the callee rows and metadata are written, the caller's
  `inner_job_ids` become the subtree, the caller turns `in_progress` and the run
  gets its `started_at`.

Registration inserts every row of the subtree first, then counts
`remaining_needs` for the new jobs from the rows
(`Sweep::recount_remaining_needs`, the count `refresh_remaining_needs` keeps for
dependents). A leg inherits its node's `needs:`, which settled before the node
expanded, and a need may name several legs, so the count at insert time
(`needs.len()`) is not the outstanding count. Jobs no runner can host settle
`failure` after that, so a later job of the subtree sees them as terminal.

The jobs the subtree can start — those with no outstanding needs, plus the
dependents the unhostable settles unblocked — go through `promote_dependents`
in job-id order: the same decide-and-admit queue a completion uses (`if:`,
JobSet gates and expansion for nested callers and matrices, `promote_one` for
the rest). The run's status is stamped by `conclude_run` (`summarize_run_tx`,
then the workflow-hold release and `run.completed.v1` when it just completed).

## What Stays Unchanged

- `submit_job` (creating the world needs the full graph)
- `cancel_run` / `cancel_job` (already targeted)
- `release_concurrency_for_job` / `release_concurrency_for_run`
- Outbox topics and payload shapes (`job.completed.v1`, `run.completed.v1`)
- Wire protocol, broker/AzDO shapes
- `summarize_run_tx` (reused, not rewritten)
- `dependency_decision` / expression eval (reused per-job)
- `aggregate_need_status` (reused)

## Testing

- The backend-neutral `control::tests::suite` runs the same scenarios against
  SQLite and Postgres: matrix fail-fast (sibling cancel + dependent settle +
  run finalize), deferred matrix expansion, workflow-level concurrency
  release on run completion, reusable-caller outputs across a reload, and
  completion bookkeeping. Postgres runs the whole suite per major version in
  `control-plane.yml`.
- New pg tests pin the targeted paths: cohort admission (a parked `max-parallel`
  leg is admitted when a settled leg frees its slot, and a dependent of the
  capped base waits for every leg), gate-waiter retry (a re-parked waiter keeps
  its FIFO slot and is admitted when its cohort frees on another gate), nested
  reusable callers that share inner job ids, and rejection of non-terminal
  completions. `refresh_remaining_needs` matrix and chain cases and fail-fast
  sibling cancellation (with the `fail_fast=false` opt-out) stay pinned too.
- The shared suite runs the deferred reusable caller scenarios (static callee,
  deferred matrix callee, pre-seeded placeholder, pre-populated control) on both
  backends.
- The shared suite pins dependent promotion on both backends: a dependent of a
  matrix base waits for every leg and reads the legs' merged outputs, a failed
  leg skips its dependents transitively, `if: always()` runs after a failure,
  `failure()` reads an ancestor reached only through a matrix base, a reusable
  caller that depends on another job follows its needs, and a job that fails at
  promotion skips the jobs behind it in the same completion.
- Load-test A/B: confirm per-completion lock hold time drops and throughput
  rises on the 25 rps harness.

## Open Questions (resolved)

1. Fail-fast flag — read off the base's legs (`job_specs.fail_fast`), default
   true, matching lite's `fail_fast_for_base`.
2. Enqueue INSERT set — unchanged: `promote::promote_one` writes the same
   queue/request columns the sweep's `enqueue` does, so they are untouched.
3. `complete_job` (non-settle path) — ported to the same targeted core.
4. Lite backend parity — the pg fold now emits `job.completed.v1` for folded
   callers and resolves outputs with the same expression context as
   `propagate_reusable_outputs`.
