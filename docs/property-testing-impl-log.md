# Property Testing Implementation Log

Tracking implementation of the first four families from
`docs/property-testing-plan.md`, using official `actions/runner` v2.335.1
semantics as the reference oracle.

## Session start

Date: 2026-07-13

### Scope

1. DAG scheduler model and dependency ordering
2. Matrix include/exclude and deferred-matrix generator
3. Condition truth tables (especially after prior failure)
4. Step-update merge and cancellation reconciliation

### Official-runner references consulted

| Surface | Official source | aksh counterpart |
|---|---|---|
| Condition functions | `L0/Worker/Expressions/ConditionFunctionsL0.cs` | `aksh-gha-expressions` + `step_conditions` |
| Implicit `success() &&` gate | StepsRunner condition evaluation | `evaluate_step_condition` / `effective_condition` |
| Matrix expand | GitHub Actions matrix docs + runner.server | `matrix_expand::expand_matrix_spec` |
| Needs / DAG | Workflow validation + job dispatch on `needs` | `scheduling` + `dag` |
| Step updates | Twirp `WorkflowStepsUpdate` by `external_id` | `step_records::merge_step_update` |
| Cancel reconciliation | Cancel → terminal records for open steps | `reconcile_cancelled_steps` |

### Design choices

- **Pure models first.** Free functions / small state machines mirror production
  rules so proptest can shrink failures without HTTP/VMs.
- **Structured generators.** Job graphs, matrix specs, and step records are
  generated as Rust values, not random YAML.
- **No protocol divergences.** Where production is incomplete (job-level `if`
  after failed needs; full deferred matrix expansion), pure models encode the
  official contract and tests pin the expected identity behavior.

---

## Work log

### [1] DAG scheduler model — DONE

**Files:**
- `crates/aksh-runner-server/src/scheduling.rs` (new)
- `crates/aksh-gha-parser/src/dag.rs` (new)
- `crates/aksh-runner-server/src/lib.rs` — `need_satisfied` delegates to pure model

**API:**
- `detect_needs_cycle`, `need_satisfied`, `under_max_parallel`, `promote_ready_jobs`
- `seed_from_jobs`, `complete_job`, `acquire_next`, `run_settled`
- Parser-side `dag::detect_needs_cycle`, `topo_layers`

**Properties (proptest, 128–256 cases):**
- No duplicate placement in queue/pending
- Success wave settles every generated DAG
- Never dispatch before needs are Success|Skipped
- Promote is deterministic
- Failure blocks dependents under default rules
- Cycle detector stable / flags mutual edges
- Topo layers respect needs ordering

**Official alignment:**
- Default need gate = Success | Skipped (not Failure/Cancelled/InProgress)
- Matrix base-id prefix `"base ("` requires all siblings terminal success/skip
- Cycles rejected before dispatch (validation model; server still does not
  reject at submit — model is ready for wiring)

### [2] Matrix include/exclude + deferred — DONE

**Files:**
- `crates/aksh-gha-parser/src/matrix_expand.rs` (new)
- `crates/aksh-gha-parser/src/lib.rs` — `expand_matrix` delegates to pure expander

**API:**
- `MatrixSpec`, `MatrixCombination`, `DeferredMatrixAxis`, `ExpandOutcome`
- `expand_matrix_spec`, `cartesian_count`, `expanded_job_id`
- `expand_with_deferred`, `unresolved_display_identity`, `matrix_to_spec`

**Properties:**
- Never empty / never panics
- Unique combos when axis values unique (dup axis values fan out, as GitHub)
- Exclude applies only to cartesian product (before include)
- Deterministic expansion
- Cartesian count = product of axis lengths
- Deferred failure preserves `${{ matrix.* }}` template (scenario 63 family)
- Include-only rows tagged

**Official alignment:**
- Cartesian → exclude (partial match) → include merge-or-append → empty fallback
- Scenario 63: unresolved display must not collapse to bare base id

**Gap still open:** runtime expansion of `fromJSON(needs.*.outputs.*)` matrices
in the server/parser path is not implemented; the pure model + properties pin
the contract for when it is.

### [3] Condition truth tables — DONE

**Files:**
- `crates/aksh-gha-expressions/src/lib.rs` — expanded proptest truthiness + status
- `crates/aksh-runner/src/worker/step_conditions.rs` (new)
- `crates/aksh-runner/src/worker/steps_runner.rs` — uses shared condition logic

**API:**
- `StatusFlags`, `contains_status_check_function`, `effective_condition`
- `evaluate_step_condition`

**Properties / tables:**
- null/bool/number/string/array/object truthiness (aksh semantics locked)
- status flags ↔ success/failure/cancelled/always for all 8-ish flag combos
- Context flags not mutated by eval
- `skipped` is not a status function
- Default condition ≡ `success()`
- Bare literals gated by `success() && (expr)` (official StepsRunner)
- `always()` constant true; `failure() || cancelled()` compound
- After-failure: default skip, `failure()` run, `always()` run

**Official alignment:** ConditionFunctionsL0 + StepsRunner implicit gate.
**Fidelity note:** empty array/object falsy in aksh; some GitHub paths treat
non-null containers as truthy — documented, not papered over.

### [4] Step merge + cancel reconciliation — DONE

**Files:**
- `crates/aksh-runner/src/worker/step_records.rs` (new)
- `crates/aksh-runner/src/worker/server_queue.rs` — `queue_update` uses merge

**API:**
- `PartialStepUpdate`, `merge_step_update`, `apply_step_update`
- `DispatchedTask`, `reconcile_cancelled_steps`, `status_rank`

**Properties:**
- Merge idempotent
- Status monotonic (Completed ↛ InProgress)
- Conclusion not erased by zero/omitted partial
- external_id identity (number alone never merges distinct steps)
- Cancel reconcile: completed preserved; each open task exactly one cancelled
  terminal; deterministic number order; no duplicate ids

**Official alignment:**
- Cumulative WorkflowStepsUpdate keyed by external_id
- Twirp has no cancel conclusion → cancelled maps to FAILED (existing convention)
- Setup completed records survive cancel

---

## Test results (2026-07-13)

```
aksh-gha-parser --lib          60 passed
aksh-gha-expressions --lib     30 passed
aksh-runner-server scheduling  12 passed
aksh-runner worker::step_*     all passed
aksh-runner worker::server_queue / steps_runner condition tests  passed
```

Pre-existing unrelated failures (not introduced here):
- `listener::job_dispatcher::tests::test_worker_dispatch_run_new_job`
- `listener::job_dispatcher::tests::test_worker_dispatch_cancellation`
  (fail on clean tree too; CLI `via` option noise)

---

## Follow-ups

1. Implement real deferred matrix expansion at job promotion time; keep
   scenario 63 fixture under `fixtures/` once server path exists.
2. Next plan items: action lifecycle generator, DTO round-trips, secrets/commands.

---

## Official-oracle alignment pass (2026-07-13, continued)

### What “official reference” means here

Tier-1 property tests cannot boot the official binary per generated case. The
oracle is the documented GitHub Actions contract plus pinned runner source for
worker-side expression semantics. GitHub's service-side DAG implementation is
private, so Aksh does not claim to copy its internal graph algorithm.

| Property family | Exact oracle | Aksh property/model |
|---|---|---|
| `needs`, unknown jobs, cycles, ordering | [Workflow syntax: `jobs.<job_id>.needs`](https://docs.github.com/en/actions/writing-workflows/workflow-syntax-for-github-actions#jobsjob_idneeds) | `aksh-gha-parser::dag` | 
| `success()`, `failure()`, `cancelled()`, `always()` | [Status-check functions](https://docs.github.com/en/actions/learn-github-actions/expressions#status-check-functions) + `actions/runner` v2.335.1 `src/Runner.Worker/Expressions/*Function.cs` | `scheduling::oracle_should_run` |
| Implicit condition gate | `actions/runner` v2.335.1 `src/Runner.Worker/StepsRunner.cs` and `WorkflowTemplateConverter.ConvertToIfCondition` | `effective_condition` / scheduler model |
| Matrix sibling dependency | GitHub workflow syntax matrix/needs contract + `actions/runner` v2.335.1 matrix expansion source | matrix scheduling properties |
| Production observable state | Same documented contract, exercised through Aksh's real router | `lib.rs` production DAG tests |

The implementation algorithm is intentionally not treated as an oracle: GitHub
does not publish the control-plane scheduler, so DFS/Kahn choices are Aksh
internals validated by their observable contract.

### Fidelity fixes found by re-reading official source

1. **Include merges into ALL matching product rows**, not just the first
   (`MatrixBuilder.MatrixInclude.Match` loops every vector). Fixed in
   `expand_matrix_spec`.
2. **All-excluded product → zero jobs**, not a synthetic empty combination.
3. **`JobContext.Status == null` ⇒ `success()` true** (official null-coalesce).
   Encoded as `OfficialJobStatus::Unset` in `official_oracles.rs`.

### New module

- `crates/aksh-runner/src/worker/official_oracles.rs` — L0 tables + p0 oracle +
  ConvertToIfCondition parity properties at **10_000** cases.

### Stress run

All property suites set to `ProptestConfig::with_cases(10_000)` and re-run:

```
aksh-gha-parser matrix_expand   17 passed (~1.5s)
aksh-gha-parser dag              5 passed
aksh-runner-server scheduling   13 passed
aksh-runner official_oracles     7 passed
aksh-runner step_records         9 passed (~2.8s)
aksh-runner step_conditions     17 passed
aksh-gha-expressions            30 passed
```

### Still not done (honest)

- No per-case official **binary** differential (Tier 3) — that needs constrained
  workflow generation + `actions/runner` v2.335.1 execution harness.

---

## DAG production-path completion (2026-07-13)

The DAG and dependency-ordering area now satisfies the model and production
layers in `docs/property-tests.md`:

- expanded plans reject unknown `needs` and cycles before server state changes;
- failed or skipped dependencies terminal-skip default downstream jobs instead
  of leaving them pending;
- `always()`, `failure()`, and `cancelled()` are evaluated after all direct
  dependencies become terminal, with `failure()` including transitive ancestors;
- matrix base dependencies wait for every expanded sibling;
- duplicate completion and late completion after cancellation are idempotent;
- unfinished runs remain non-terminal while conditionally promoted cleanup jobs run;
- model properties use an independent status/condition oracle;
- production tests traverse structured YAML through parse, expansion, HTTP
  submission, promotion, completion, and final server-visible state.

Primary oracles remain the GitHub `jobs.<job_id>.needs` and status-function
contracts plus the pinned `actions/runner` v2.335.1 condition implementations.

### Generated production-path stress validation (2026-07-13)

Added `generated_server_dag_properties_1000_cases`, which generates 1,000
bounded deterministic acyclic workflows and drives each through the local
Aksh router using the privileged `/internal/test/jobs/complete` simulation
endpoint. The test compares final job states against an independent
transitive-ancestor oracle.

Result:

```
tests::generated_server_dag_properties_1000_cases ... ok
1,000 generated workflows passed in 6.38s
```

This validates parser, dependency promotion, terminal skip propagation, and
run settlement through the real server state machine. It does not execute
shell steps in microVMs; those remain covered by the real-world runner/SmolVM
benchmarks.
