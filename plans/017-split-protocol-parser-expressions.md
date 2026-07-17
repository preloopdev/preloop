# Plan 017: Split the protocol, parser, and expression god-files behind facades

> **Executor instructions**: Three independent, behavior-preserving module splits, each
> shippable alone. Every split keeps public paths working via `pub use` re-exports and
> keeps serde wire shape byte-identical. STOP on any stop condition. Update
> `plans/README.md` when done.
>
> **Drift check (run first)**: `git diff --stat 3505476..HEAD -- crates/aksh-gha-protocol/src/azdo.rs crates/aksh-gha-parser/src/lib.rs crates/aksh-gha-expressions/src/lib.rs`
> Re-derive boundaries with `grep -n` if drifted.

## Status

- **Priority**: P3
- **Effort**: L
- **Risk**: HIGH (serde renames / custom codecs / parser output are wire contracts)
- **Depends on**: 012 (tests moved out), 013 (matrix unified — touches parser lib.rs),
  014 (context-data conversion moved into protocol)
- **Category**: tech-debt / architecture
- **Planned at**: commit `3505476`, 2026-07-16

## Why this matters

Three foundational crates each have one oversized file mixing unrelated concerns, making
protocol/parser/expression changes error-prone (a "simple" edit can alter a serde rename,
a custom codec, or grammar precedence):

- `aksh-gha-protocol/src/azdo.rs` (2285): lifecycle + job-message + `TaskStep` custom
  codec + `PipelineContextData` codec + variables/timeline/results/logs DTOs.
- `aksh-gha-parser/src/lib.rs` (2648): models + YAML parse + trigger/filter eval + job/
  reusable/matrix expansion — despite `dag`/`eval`/`job_builder`/`matrix_expand` existing.
- `aksh-gha-expressions/src/lib.rs` (1492): context + conditions + AST + evaluator +
  functions + `hashFiles` + lexer + parser in one file.

## Current state (boundaries — re-verify after deps land)

- `azdo.rs`: lifecycle/session/message `16-222`; job message + `TaskStep` custom
  ser/de + template-token helpers `224-766`; variables/timeline/results/issue/resource
  `767-1022`; `PipelineContextData` custom codec `1024-1171`; completion/log/wrapper/error
  `1173-1253`. Custom serializers are NOT derive-only — they live next to their types.
- `parser lib.rs`: models `133-435`; trigger/filter matching+validation `437-659`; YAML
  parse+normalize `1154-1210`; expansion/reusable/needs/DAG orchestration `1266-1677`.
- `expressions lib.rs`: context/conditions `8-252`; AST/operators/functions/eval
  `254-507`; `hashFiles` `509-615`; lexer+parser `617-1062`.

Official reference: `Sdk/DTWebApi`, `Sdk/DTPipelines`, `Sdk/Expressions`.

## Commands you will need

| Purpose | Command | Expected |
|---------|---------|----------|
| Check | `cargo check --workspace` | exit 0 |
| Protocol tests | `cargo test -p aksh-gha-protocol --quiet` | baseline |
| Parser tests | `cargo test -p aksh-gha-parser --quiet` | baseline |
| Expr tests | `cargo test -p aksh-gha-expressions --quiet` | baseline |
| Clippy | `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |

## Scope

**In scope**: splitting the three files into submodules, each behind a facade
(`lib.rs`/`azdo.rs` re-export everything currently public). No serde attribute, field
name, default, enum casing, custom codec, grammar, or parser output change.

**Out of scope**: expression semantics (Plan 002), matrix logic (Plan 013), context-data
conversion (Plan 014 — already moved into the protocol crate), DTO deletion (Plan 018).

## Steps

### Step A: Split `azdo.rs` (protocol)

Create `lifecycle.rs`, `messages.rs`, `job.rs` + `task_step.rs`, `variables.rs`,
`timeline.rs`, `resources.rs`, `context_data.rs`, `completion.rs`/`logs.rs`. **Keep each
custom serializer/deserializer and its private helpers next to the type it encodes**
(`TaskStep`, `PipelineContextData`, `EncryptionKey`). Make `azdo.rs` a facade with
`pub use` for every existing `azdo::Type` and `azdo::message_type::*` path. Move the
azdo test oracles (post-012) into per-area test files.

**Verify**: `cargo test -p aksh-gha-protocol --quiet` → baseline; a serde round-trip/
golden test for `TaskStep` and `PipelineContextData` passes unchanged.

### Step B: Split `parser lib.rs`

Create `models.rs` (Workflow/Job/Step/Env/Strategy/Matrix/reusable metadata with their
serde attrs — `rename="on"`, `rename="if"`, flattened axes, untagged enums, defaults),
`trigger.rs` (matching/validation/globs), `yaml.rs` (parse + key normalization),
`expand.rs` (expand_jobs, reusable recursion, input coercion, needs rewrite, DAG
orchestration). `lib.rs` becomes a facade re-exporting `Workflow`, `Trigger`,
`parse_workflow`, `expand_jobs`, etc.

**Verify**: `cargo test -p aksh-gha-parser --quiet` → baseline; parser golden tests
unchanged (accepted YAML + serialized `JobPlan` identical).

### Step C: Split `expressions lib.rs`

Create `context.rs`, `conditions.rs`, `ast.rs`, `lexer.rs`, `parser.rs`,
`evaluator.rs`, `functions.rs`/`hash_files.rs`. `lib.rs` re-exports `eval_expression`,
`validate_expression`, `eval_bool`, `effective_condition`, `trim_expression_markers`,
`Context`. Preserve private AST/token behavior and precedence.

**Verify**: `cargo test -p aksh-gha-expressions --quiet` → baseline.

## Test plan

Invariance per crate: identical test count and identical golden/serde outputs before and
after each split. Add a serde round-trip golden for `TaskStep` and `PipelineContextData`
if one doesn't already exist (highest custom-codec risk). Model after
`aksh-gha-protocol/tests/protocol_golden.rs`.

## Done criteria

- [ ] `cargo test --workspace --quiet` == baseline counts per crate
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [ ] `azdo.rs`, parser `lib.rs`, expressions `lib.rs` are thin facades (re-exports only)
- [ ] All previously-public paths still resolve (`cargo check --workspace`)
- [ ] Custom codecs (`TaskStep`, `PipelineContextData`) have golden round-trip tests that pass
- [ ] `plans/README.md` row updated

## STOP conditions

- Any serde rename/default/casing or custom-codec behavior would change → STOP; the wire
  shape is the contract, the module layout is not.
- A public path used by another crate (`aksh-runner`, `aksh-runner-server`, `aksh-dap`,
  clients) would break and can't be preserved with `pub use` → STOP and report.
- Splitting a custom serializer away from its type would require exposing private helpers
  broadly → keep the serializer with its type.

## Maintenance notes

- After this, protocol areas have clear ownership; a new DTO or grammar node has an
  obvious home.
- A reviewer should diff serialized golden outputs, not just compile success.
