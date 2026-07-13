# Property Testing Plan

## Purpose

Increase compatibility with the official GitHub Actions runner v2.335.1 by testing both:

1. Internal invariants of the parser, evaluator, scheduler, protocol DTOs, runner worker, and storage layers.
2. Differential behavior between the official runner and aksh.

Property tests are not a replacement for the official runner oracle. They find invariant violations cheaply; official-runner differential tests establish compatibility.

The workspace already provides `proptest` and uses it in `aksh-gha-expressions` and `aksh-gha-parser`. The conformance documentation also calls for property tests, protocol round trips, and fuzzing. The existing `aksh-conformance` fuzz/replay commands should evolve into a real generated-case and differential-test harness rather than remain broad random-input smoke tests.

## Testing tiers

### Tier 1 — Native property tests

Run thousands of generated cases without a VM. Target pure semantics and deterministic state machines:

- expression evaluation
- workflow parsing and job graphs
- matrix expansion
- input coercion
- protocol DTO serialization
- step-update merging
- cancellation reconciliation
- workflow commands
- secret masking
- cache/artifact state transitions

### Tier 2 — Protocol/state-machine tests

Run generated HTTP/request sequences against an in-process or local aksh server. Target runner lifecycle behavior:

- registration
- session creation
- polling
- acknowledgement
- acquisition
- lease renewal
- cancellation
- completion
- reconnects and retries

### Tier 3 — Official-runner differential tests

Generate constrained, small workflows and execute them against:

1. Official `actions/runner` v2.335.1 with GitHub or a controlled replay.
2. The same official runner v2.335.1 against aksh.
3. Eventually, the Rust `aksh-runner` against aksh.

Compare job conclusions, step records, outputs, annotations, logs, and protocol flows after normalizing only explicitly volatile fields.

Do not boot a VM for every generated case. Run large native suites per commit, smaller differential batches for changes, and larger VM sweeps nightly. Preserve every minimized failing case as a fixture or scenario seed.

## Property families

## 1. Expression evaluator

Target: `crates/aksh-gha-expressions`.

### Parser safety

For arbitrary bounded expression strings:

- parsing never panics;
- malformed input returns a structured error;
- evaluation never hangs or overflows the stack;
- deeply nested and unterminated expressions remain bounded.

Generate nested parentheses, escaped strings, malformed `${{ }}` markers, property chains, bracket access, wildcards, repeated operators, long identifiers, and mixed literals.

### Truthiness and status functions

Generate JSON values and job/step contexts. Check deterministic behavior for:

- `null`
- empty and non-empty strings
- zero and non-zero numbers
- empty and non-empty arrays/objects
- booleans
- `success()`
- `failure()`
- `cancelled()`
- `always()`

Important invariants:

- `always()` remains true regardless of prior failure.
- `success()`, `failure()`, and `cancelled()` are not confused with skipped state.
- A status context is not mutated by evaluation.

### Coercion and formatting

Generate numeric strings, malformed numbers, whitespace, exponent notation, case variants, escaped braces, missing format arguments, and extra format arguments. Check numeric coercion, case-insensitive equality, `format()` escaping, and error classes against documented behavior.

## 2. Workflow parser and job graph

Target: `crates/aksh-gha-parser`.

Generate a structured workflow model first, then render it to YAML. Structured generation shrinks failures much better than arbitrary YAML generation.

### DAG properties

Generate jobs with random `needs`, conditions, and terminal outcomes. Check:

- cycles are rejected;
- no job dispatches before dependencies settle;
- failed/skipped dependencies produce the correct default behavior;
- `always()` conditions can opt jobs into execution after failure;
- scheduling is deterministic regardless of map iteration order;
- no job is lost or dispatched twice.

### Matrix properties

Generate zero or more axes, booleans, numbers, strings, `include`, `exclude`, include-only entries, and deferred `fromJSON(needs.*.outputs.*)` matrices. Check:

- every expanded combination is unique;
- excluded combinations never dispatch;
- include-only values do not accidentally become display axes;
- expansion is deterministic;
- the Cartesian-product count is correct;
- deferred matrices preserve unresolved display placeholders when the producer fails.

Scenario 63 should become a minimized regression case for unresolved identity:

```text
matrix-build-${{ matrix.case }}-${{ matrix.mode }}
```

versus the current aksh form:

```text
matrix-build
```

Keep internal scheduling IDs, base IDs, display names, and matrix context as separate generated values. Do not test them as one normalized job name.

## 3. Workflow-dispatch inputs

Target: `crates/aksh-gha-parser` and `crates/aksh-runner-server`.

Generate string, boolean, number, choice, and environment inputs with defaults, required flags, omitted values, explicit false, explicit zero, and explicit empty strings.

Check:

- submitted input overrides the declared default;
- `inputs.*` preserves the declared type;
- `github.event.inputs.*` is string-valued;
- required-input failures are deterministic;
- false, zero, and empty string are not confused with omission;
- input-dependent conditions evaluate correctly after a prior failure.

This directly targets scenario 89.

## 4. Wire DTO and protocol properties

Target: `crates/aksh-gha-protocol`.

For generated DTOs, test:

```text
deserialize(serialize(value)) == value
```

Cover:

- `AgentJobRequestMessage`
- `TaskAgentMessage`
- `TaskStep`
- `TaskReference`
- `TimelineRecord`
- `VariableValue`
- endpoint authorization
- broker acquire/renew/complete DTOs
- Twirp update structures
- NDJSON events

Use a narrowly defined normalized comparison only where the wire contract permits it. Normalize GUIDs, timestamps, RSA material, and SAS signatures only when documented. Add a guard that fails when a new normalizer rule matches an unexpected field category.

Also assert exact wire names such as `contextName`, `displayNameToken`, and `isSecret`, and test omission versus explicit null where the runner distinguishes them.

## 5. Server protocol state machine

Target: `crates/aksh-runner-server` and `crates/aksh-gha-protocol`.

Model:

```text
Unregistered
→ Registered
→ SessionCreated
→ Polling
→ JobOffered
→ Acquired
→ Renewing
→ Completing
→ Completed
```

Generate operations including duplicate registration, session reconnects, repeated polling, acknowledgement, acquisition, renewal, cancellation, completion, stale leases, and out-of-order requests.

### Required invariants

- A queued job is acquired by at most one runner.
- Duplicate polls do not duplicate assignment.
- Acknowledgement is idempotent.
- Duplicate completion does not duplicate events or downstream dispatch.
- Completion cannot be followed by successful renewal that resurrects the job.
- An expired lease cannot create two owners.
- Terminal state transitions are deterministic under cancellation races.
- A run eventually settles when all jobs are terminal.

Test interleavings such as:

```text
cancel → acquire
acquire → cancel
cancel → complete(success)
complete(success) → cancel
cancel → complete(cancelled)
```

## 6. Step records and completion

Target: `crates/aksh-runner/src/worker/steps_runner.rs` and `crates/aksh-runner-server/src/lib.rs`.

### Status monotonicity

Generate partial updates and verify valid progression:

```text
Pending → InProgress → Completed
```

Reject or safely ignore invalid regressions such as `Completed → InProgress`, unless the official protocol explicitly permits them.

### Identity-safe merge

Generate updates with duplicate external IDs, duplicate numbers, conflicting names, omitted fields, and out-of-order delivery. Check:

- `external_id` takes precedence over number;
- omitted fields preserve existing values;
- unrelated steps never merge;
- duplicate updates are idempotent;
- conclusion is not erased by a partial update.

### Cancellation reconciliation

Generate a dispatched task list plus a partial received-record set. Check:

- every interrupted in-flight task gets exactly one cancelled record;
- completed steps are not rewritten as cancelled;
- setup and complete-job synthetic records remain valid;
- final order is deterministic.

## 7. Workflow commands, logs, and secrets

Target: `crates/aksh-runner/src/worker/commands.rs`, `execution_context.rs`, and `file_commands.rs`.

### Command parsing

Generate escaped percent, comma, colon, and newline values; duplicate properties; unknown properties; malformed delimiters; embedded ordinary output; and `stop-commands` sequences.

Check:

- supported command parse/unparse round trips;
- ordinary output is not interpreted as a command;
- `stop-commands` suspends parsing;
- only the matching resume token restores parsing;
- malformed commands do not corrupt later log lines.

### Secret masking

Generate overlapping masks, prefixes/suffixes, whitespace, base64 forms, regex metacharacters, Unicode, and empty values. Check:

- no registered secret appears in uploaded logs;
- masking is idempotent;
- longer overlapping secrets are masked first;
- empty secrets do not erase log content;
- annotations and live logs apply the same masking policy.

Treat no-secret-leak as a security invariant, not merely a compatibility assertion.

### Annotations

Generate warning/error/notice annotations with missing lines, line zero, large line numbers, duplicate entries, long messages, and over-limit counts. Check message caps, line defaults, step numbers, severity mapping, and secret masking. This directly targets scenario 54.

## 8. Shell and template rendering

Target: `crates/aksh-runner/src/worker/template.rs`, script handlers, and job environment construction.

Generate shell bodies containing GitHub expressions, shell variables, quoting, heredocs, braces, escaped dollar signs, multiline values, and operators.

Check:

- ordinary shell variables are not interpreted as GitHub expressions;
- GitHub expressions are substituted;
- quoting and line boundaries are preserved;
- malformed template output is rejected consistently;
- substituted secrets are masked without changing surrounding syntax;
- rendered scripts, environment, logs, and exit codes are consistent.

Use scenario 85 as the initial minimized case:

```sh
TOKEN="${{ secrets.TOKEN }}"
echo "$TOKEN"
```

Compare rendered script text, process environment, log output, and exit status against the official runner.

## 9. Action lifecycle and Node runtime

Target: `crates/aksh-runner/src/worker/handlers` and `steps_runner.rs`.

Generate manifests with `node12`, `node16`, `node20`, `node24`, composite actions, pre/post scripts, `pre-if`, `post-if`, nested actions, failed main actions, and `always()` cleanup.

Check:

- node12/node16 map to node20;
- node20 upgrades to node24 under the migration flag;
- `ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION=true` opts back into node20;
- `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true` upgrades node20 in phase 1;
- node24 remains node24;
- post actions execute in correct order;
- saved state reaches post actions;
- eligible cleanup runs after main-step failure;
- each lifecycle phase produces the expected record.

This is the primary property suite for scenarios 61 and 100.

## 10. Cache and artifact state machines

Target: `crates/aksh-cache`, `crates/aksh-artifacts`, and server routes.

Generate sequences of reserve, upload, commit, restore, delete, duplicate commit, restore-before-commit, and concurrent writers.

Check:

- committed entries can be restored;
- partial uploads are not visible as complete;
- duplicate commits are idempotent;
- concurrent writes do not corrupt metadata;
- signed URLs validate and expire correctly;
- path traversal cannot escape storage roots;
- cross-job visibility matches the protocol contract.

Normalize endpoint URLs in differential comparisons, but test storage transitions exactly.

## 11. Concurrency groups

Concurrency groups are currently deferred, but they are well suited to model-based property tests.

Generate runs with equal and different groups, `cancel-in-progress` true/false, queued runs, running jobs, and cancellation during acquisition/execution.

Check:

- at most one active run exists per group when cancellation is enabled;
- the correct prior run is cancelled;
- unrelated groups do not interfere;
- queued runs settle deterministically.

This targets scenario 84.

## 12. Metamorphic tests

Use transformations where an exact expected output is difficult to state.

### No-op step insertion

Adding a successful no-op step should add one successful step without changing unrelated conclusions or outputs. Numbering must adjust consistently.

### Irrelevant environment variable

Adding an unused environment variable should not change action selection, unrelated conditions, or job conclusion.

### Matrix-axis permutation

Permuting matrix declaration order should preserve the set of combinations. Display ordering must be checked against the official runner rather than assumed invariant.

### Duplicate completion

Submitting the same completion twice should leave one terminal job state and one downstream promotion.

### Poll repetition

Repeating a poll must not duplicate assignment or change job identity.

### Action rename

Changing only a step display name should not change action runtime, outputs, cache/artifact behavior, or conclusion.

### Secret substitution

Changing only a secret value may change masked output but must never expose the raw value.

## 13. Differential harness design

Generate constrained workflows first:

1. local `run:` steps;
2. local Node actions;
3. composite actions;
4. pre/post actions;
5. remote actions;
6. cache/artifact actions;
7. containers and services.

Every failing generated case should preserve:

- deterministic seed;
- minimized workflow YAML;
- runner version;
- server configuration;
- official artifact;
- aksh artifact;
- normalized protocol diff;
- classification: semantic, environment, protocol, or expected volatile difference.

Start with a corpus seeded from the existing 20-scenario sweep and official v2.335.1 golden captures. Promote minimized failures into `experiments/mitm/scenarios/` or `fixtures/`.

## Recommended implementation order

1. DAG scheduler model and dependency ordering.
2. Matrix include/exclude and deferred-matrix generator.
3. Condition truth tables, especially after prior failure.
4. Step-update merge and cancellation reconciliation state machine.
5. Action pre/main/post lifecycle generator.
6. Protocol DTO round-trip and normalized wire-shape properties.
7. Secret masking and workflow-command properties.
8. Shell/template rendering properties.
9. Cache/artifact state machines.
10. Generated official-runner differential workflows.
11. Concurrency groups after the deferred feature is reactivated.

## Success criteria

A property-testing milestone is complete when:

- generated native cases are deterministic and shrink to minimal failures;
- no generated input causes a panic, hang, or unbounded resource use;
- all security invariants hold, especially secret non-disclosure;
- state-machine operations are idempotent where the protocol requires it;
- differential failures are categorized rather than hidden by broad normalization;
- every fixed compatibility bug gains a minimized regression seed;
- official-runner differential cases remain reproducible with the pinned v2.335.1 binary.
