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

---

## Tier 2 runner contracts, excluding concurrency (2026-07-14)

### Scope and authority

Implemented the requested Tier 2 properties for action lifecycle expansion,
runner-facing DTO codecs, workflow commands, secret masking, file commands, and
production-path workflow-step updates. Concurrency and interleaving properties
were intentionally excluded.

Every property uses a deterministic 1,000-case `ProptestConfig`. Oracles are
derived from GitHub's public documentation and the official `actions/runner`
v2.335.1 source pinned at commit
`7d737449ef346f6524f75688d0c9c95fa10ba10a`, rather than from Aksh's own
implementation. Exact source URLs are adjacent to the corresponding production
logic and property blocks.

Primary sources:

- action lifecycle: [`ActionManager.cs`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionManager.cs)
  and [`ActionRunner.cs#L79-L110`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionRunner.cs#L79-L110);
- workflow-command parsing: [`ActionCommand.cs#L19-L114`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Common/ActionCommand.cs#L19-L114)
  and GitHub's [workflow-command reference](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands);
- secret masking: [`ActionCommandManager.cs#L419-L448`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionCommandManager.cs#L419-L448);
- environment files: [`FileCommandManager.cs#L113-L209`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/FileCommandManager.cs#L113-L209)
  and [`FileCommandManager.cs#L296-L451`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/FileCommandManager.cs#L296-L451);
- DTOs: [`VariableValue.cs#L8-L38`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTWebApi/WebApi/VariableValue.cs#L8-L38),
  [`ActionStep.cs#L9-L46`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTPipelines/Pipelines/ActionStep.cs#L9-L46),
  [`PipelineContextDataJsonConverter.cs#L20-L151`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTPipelines/Pipelines/ContextData/PipelineContextDataJsonConverter.cs#L20-L151),
  [`TaskAgentSessionKey.cs#L8-L32`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTWebApi/WebApi/TaskAgentSessionKey.cs#L8-L32),
  and [`AgentJobRequestMessage.cs#L15-L267`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTPipelines/Pipelines/AgentJobRequestMessage.cs#L15-L267);
- step-update transport: [`RunServiceHttpClient.cs#L25-L166`](https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/RSWebApi/RunServiceHttpClient.cs#L25-L166)
  plus the captured v2.335.1 `WorkflowStepsUpdate` golden exchange.

### Properties added

- `crates/aksh-runner/src/worker/job_extension.rs`: declaration-ordered pre
  and main stages, LIFO post stages, default and explicit stage conditions,
  metadata preservation, missing/unsupported definitions, and the official
  local-action rule that skips `pre` but retains `main` and `post`.
- `crates/aksh-gha-protocol/src/azdo.rs`: six `tier2_codec_` properties for
  `VariableValue` omission/null/empty semantics, canonical and compatibility
  `TaskStep` environment tokens, recursive `PipelineContextData`, base64
  session-key bytes and encrypted flag, and an independently constructed
  field-by-field `AgentJobRequestMessage` wire oracle.
- `crates/aksh-runner/src/worker/commands.rs`: modern command escaping and
  parsing, case-insensitive properties, malformed-input safety, exact-token
  `stop-commands` behavior, and masked annotation fields.
- `crates/aksh-runner/src/worker/contexts.rs`: raw, trimmed-line, base64,
  overlapping, empty, multiline, idempotent, and live-mask behavior.
- `crates/aksh-runner/src/worker/file_commands.rs`: ordinary and heredoc
  key/value parsing for LF and CRLF, missing delimiters, duplicate keys,
  step-local outputs, pre/post state ownership, path order, and the
  case-insensitive `NODE_OPTIONS` prohibition.
- `crates/aksh-runner-server/src/lib.rs`: generated runner
  `ServerQueue` updates are serialized, posted through the real Axum router,
  decoded by the typed production handler, and checked for cumulative snapshots,
  merge semantics, change order, IDs, and invalid field types.

### Fidelity fixes exposed by the properties

1. Local JavaScript actions no longer receive an unsupported pre stage.
2. Workflow-command parsing now follows the official leading-whitespace and
   case-insensitive property rules.
3. `add-mask` registers the raw value and each non-empty trimmed CR/LF line,
   and ignores whitespace-only values.
4. `GITHUB_PATH` entries retain official file order; composite execution now
   blocks every case variant of `NODE_OPTIONS` too.
5. Session-key bytes serialize as the official base64 JSON string instead of a
   numeric array.
6. Empty TemplateToken maps and the canonical `environment` member survive
   `TaskStep` decode/encode round trips.
7. `WorkflowStepsUpdate` now uses typed request DTOs at the HTTP boundary rather
   than accepting arbitrary JSON.

### Local verification

```
aksh-gha-protocol tier2_codec_                         6 passed
aksh-runner worker::commands::tests                  20 passed
aksh-runner worker::contexts::tests::masking_         4 passed
aksh-runner worker::file_commands::tests             28 passed
aksh-runner lifecycle                                12 passed
aksh-runner-server workflow_steps_update_             2 passed
cargo fmt --all --check                              passed
```

Each property function above executes 1,000 deterministic generated cases.
The live workflow rejects Cargo's otherwise-successful `running 0 tests`
behavior, so renamed or missing filters fail explicitly.

### Official GitHub-hosted live conformance

The final live test ran on the official runner v2.335.1, Ubuntu 24.04.4,
`ubuntu-24.04` image `20260705.232.1`:

- run: <https://github.com/preloopdev/aksh/actions/runs/29337898129>
- job: <https://github.com/preloopdev/aksh/actions/runs/29337898129/job/87101716570>
- branch commit: `aa5d058593bde985bcf093e0c4e69bbb5d49fa1c`
- result: **success**, all six non-zero property filters passed with counts
  `5 / 20 / 4 / 28 / 12 / 2`.

The remote action oracle observed pre hooks in declaration order, mains in
declaration order, and posts in reverse declaration order. Its per-action
`GITHUB_STATE` checks passed, declared outputs reached the caller, multiline
`GITHUB_ENV` and `GITHUB_OUTPUT` values were preserved, and the `GITHUB_PATH`
probe executed successfully. While command handling was stopped, an emitted
`::error::` string produced no error annotation; the resumed notice was
processed. The post-registration masking log was exactly
`tier2-mask-sentinel-after=***`, with no raw sentinel present afterward.

GitHub cannot execute a local JavaScript action's `pre` hook, so the fixture is
pinned to isolated action-only commit
`88e5328dd1ee742206bc5e4f12823613744aa311`. This also avoids unrelated tracked
benchmark symlinks in the normal repository archive. The only live annotation
warning was GitHub's Node.js 20 deprecation notice for `actions/checkout@v4`;
the hosted runner forced that action to Node.js 24.

Workspace cleanup checks on the final tree:

- `cargo fmt --all --check`: passed.
- `cargo clippy --workspace --all-targets`: exited successfully with the
  repository's existing warning set; new typed step-update request fields also
  produce a non-fatal dead-code warning because deserialization, rather than
  field reads, is the boundary being tested.
- `cargo test --workspace --quiet`: **656 passed, 1 ignored**.

### Dispatcher repair (2026-07-14)

The two previously failing dispatcher tests were fixed. Unit tests were
spawning Cargo's test harness executable instead of the `aksh-runner` CLI; the
harness correctly rejected the worker-only `--via` argument. Test dispatch now
builds and launches the real `aksh-runner` binary once, while production keeps
the existing direct-binary and `AKSH_RUNNER_BIN` resolution paths. The
cancellation assertion now allows the documented SIGINT/SIGTERM grace budget
while still rejecting the ten-second sleep payload.

Focused result:

```
listener::job_dispatcher::tests::test_worker_dispatch  2 passed
```

The full workspace test run is now green: 656 passed, 1 ignored.
