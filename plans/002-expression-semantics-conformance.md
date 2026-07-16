# Plan 002: Conform expression evaluation to the official Sdk/Expressions semantics

> **Executor instructions**: Follow this plan step by step. Run every verification command and
> confirm the expected result before moving on. On any STOP condition, stop and report. When
> done, update this plan's row in `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat 839791c..HEAD -- crates/aksh-gha-expressions/`
> If `crates/aksh-gha-expressions/src/lib.rs` changed since planning, compare the "Current
> state" excerpts before proceeding; mismatch = STOP.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED (changes `if:` outcomes; must be gated by tests asserting official behavior)
- **Depends on**: none
- **Category**: bug (workflow-semantic compatibility)
- **Planned at**: commit `839791c`, 2026-07-13

## Why this matters

`${{ }}` expressions gate which steps and jobs run. aksh's evaluator diverges from the official
`Sdk/Expressions` engine in five ways; each silently flips condition outcomes versus GitHub for
existing workflows. Reference sources (read-only) live in the official runner mirror:
`~/mitm-proxy/experiments/mitm/.cache/runner.server/src/Sdk/Expressions/`.

Divergences (official behavior first):

1. `startsWith`/`endsWith` are case-insensitive (`StartsWith.cs:25` —
   `StringComparison.OrdinalIgnoreCase`); aksh is case-sensitive.
2. `==`/`!=` across mixed kinds coerce to numbers (`EvaluationResult.cs` AbstractEqual: null→0,
   bool→0/1, string→parsed number or NaN; NaN equals nothing). On GitHub `0 == ''` is true,
   `null == 0` is true, `true == 1` is true; in aksh all are false.
3. Objects and arrays are ALWAYS truthy, even empty (`EvaluationResult.cs:51-72`); aksh treats
   empty object/array as falsey (already flagged in `docs/fidelity-gap.md` §3).
4. `contains(array, item)` uses the same coercive equality (`Contains.cs:35-38`); aksh uses
   strict `values_equal`.
5. `format()` throws on malformed templates and `hashFiles()` throws on unknown flags; aksh
   silently skips both (silent-skip hides workflow bugs GitHub would surface as failures).

## Current state

- `crates/aksh-gha-expressions/src/lib.rs` (~1333 lines) — the whole evaluator. Key excerpts:

  Equality (lines 274–279):
  ```rust
  fn values_equal(left: &Value, right: &Value) -> bool {
      match (left, right) {
          (Value::String(left), Value::String(right)) => left.eq_ignore_ascii_case(right),
          _ => left == right,
      }
  }
  ```

  Comparison (lines 281–300): `compare_values` already coerces via `numeric_value`
  (null→0, bool→0/1, string→parse) — reuse `numeric_value` for equality coercion.

  Functions (lines 319–324):
  ```rust
  "startswith" => Ok(Value::Bool(
      string_arg(&values, 0).starts_with(&string_arg(&values, 1)),
  )),
  "endswith" => Ok(Value::Bool(
      string_arg(&values, 0).ends_with(&string_arg(&values, 1)),
  )),
  ```

  contains (lines 339–347): string haystack is already case-insensitive (correct); the
  `Value::Array` arm uses `values_equal` (needs the coercive equality).

  Truthiness: lines ~168–176 (empty object/array → false).

- Callers that must keep passing: `crates/aksh-gha-parser/src/eval.rs` (`build_context`,
  job-level eval) and `crates/aksh-runner/src/worker/template.rs` (`evaluate_condition`),
  `crates/aksh-runner/src/worker/steps_runner.rs:771-810` (`should_run_step`). No signature
  changes are needed — semantics only.
- Conventions: pure functions, no new dependencies, inline `#[cfg(test)]` tests at the bottom
  of `lib.rs` (match the existing test module style there).

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Targeted tests | `cargo test -p aksh-gha-expressions --quiet` | all pass |
| Downstream tests | `cargo test -p aksh-gha-parser -p aksh-runner --quiet` | all pass |
| Full gate | `cargo fmt --all --check && cargo clippy --workspace --all-targets && cargo test --workspace --quiet` | exit 0 |

## Scope

**In scope**:
- `crates/aksh-gha-expressions/src/lib.rs`

**Out of scope** (do NOT touch):
- `crates/aksh-gha-parser/src/eval.rs`, `crates/aksh-runner/src/worker/template.rs` — callers;
  if a change seems required there, STOP.
- Object filters (`a.*.b`) and index-access gaps — known missing features
  (`docs/fidelity-gap.md` §3), separate work, not this plan.
- The official mirror sources — read-only reference.

## Git workflow

- Branch: `advisor/002-expression-semantics`
- Commits per step, conventional style (`fix: ...`). No push/PR unless instructed.

## Steps

### Step 1: Characterization tests for official semantics (write first, watch them fail)

Add a test module `official_semantics` in `lib.rs` asserting the OFFICIAL behavior via the
public API (`eval_expression`/`eval_bool`):

- `startsWith('Hello world', 'HELLO')` → true; `endsWith('Hello world', 'WORLD')` → true
- `0 == ''` → true; `null == 0` → true; `true == 1` → true; `'abc' == 0` → false (NaN);
  `'1' == 1.0` → true; `null == ''` → true (both coerce to 0)
- `fromJSON('{}') && true` → true-ish (empty object truthy); same for `fromJSON('[]')`
- `contains(fromJSON('[1,2]'), '1')` → true (coercive)
- `format('{0}{1}', 'a')` → Err; `format('{{literal}}')` → `"{literal}"` (escaping preserved —
  if the current implementation already handles `{{ }}`, keep it green)
- `hashFiles('--bogus-flag', 'x')` → Err

**Verify**: `cargo test -p aksh-gha-expressions --quiet` → exactly the new tests fail; note
which pass already (e.g. `{{ }}` escaping) and leave those implementations alone.

### Step 2: Fix functions

- `startswith`/`endswith`: compare `to_ascii_lowercase()` forms (mirror the existing
  `contains` string arm style at lines 341–343).
- `contains` array arm: replace `values_equal(value, needle)` with the new `abstract_equal`
  (Step 3).
- `format`: return `Err(ExpressionError::...)` on an index with no argument or malformed
  braces, matching the existing error enum variants (add one if none fits, following the
  existing `ExpressionError` naming style).
- `hashFiles`: unknown `--flag` → `Err` instead of `continue` (site at lines ~415–420).

### Step 3: Coercive equality

Add and wire:

```rust
/// Official AbstractEqual (EvaluationResult.cs): same-kind compares directly
/// (strings case-insensitively); mixed kinds coerce to numbers (null→0,
/// bool→0/1, string→parsed or NaN); NaN equals nothing; objects/arrays are
/// equal only by kind+identity semantics — keep current strict behavior there.
fn abstract_equal(left: &Value, right: &Value) -> bool { ... }
```

Reuse `numeric_value` (lines 292–300) for the coercion. Route `BinaryOp::Eq`/`Ne` and the
`contains` array arm through it. Keep `values_equal` only if still referenced; otherwise delete
it (clean cutover — `grep -n "values_equal" crates/aksh-gha-expressions/src/lib.rs` must return
only the new call sites or nothing).

### Step 4: Truthiness

Locate the truthiness function (lines ~168–176): make `Value::Object(_)` and `Value::Array(_)`
unconditionally true. Numbers: 0 and NaN false; strings: empty false — verify these already
match official (`EvaluationResult.cs:51-72`) and add assertions.

**Verify (steps 2–4)**: `cargo test -p aksh-gha-expressions --quiet` → all pass including the
step-1 suite.

### Step 5: Downstream regression sweep

**Verify**: `cargo test -p aksh-gha-parser -p aksh-runner -p aksh-runner-server --quiet` → all
pass. Any failing test here encodes the OLD semantics: update it only if its assertion
contradicts the official semantics documented above, and say so in the commit message;
otherwise STOP.

## Test plan

Covered by Step 1 (characterization-first). Every fixed divergence has at least one test named
after the behavior (e.g. `starts_with_is_case_insensitive`, `mixed_type_equality_coerces`,
`empty_collections_are_truthy`, `format_invalid_index_errors`).

## Done criteria

- [ ] Full gate command exits 0
- [ ] All Step-1 official-semantics tests pass
- [ ] `grep -n "starts_with(&string_arg" crates/aksh-gha-expressions/src/lib.rs` → no match
- [ ] No files outside scope modified (`git status`)
- [ ] `plans/README.md` row updated

## STOP conditions

- Excerpts don't match live code (drift).
- A fix requires changing a caller signature or behavior outside `aksh-gha-expressions`.
- More than 3 downstream tests fail after Step 5 — the blast radius is bigger than estimated;
  report the list instead of mass-updating tests.
- You discover official `format`/`hashFiles` error behavior differs from the plan's claim when
  reading `src/Sdk/Expressions/Sdk/Functions/Format.cs` / `src/Runner.Worker/Expressions/HashFilesFunction.cs`
  — trust the C# source over this plan and report the discrepancy.

## Maintenance notes

- Reviewer should scrutinize the NaN cases and `null == ''` in `abstract_equal` — easiest to
  get subtly wrong.
- Deferred (tracked in `docs/fidelity-gap.md` §3, not this plan): index/bracket access
  `matrix['os']`, `steps.*.outputs` object filters.
- Any future function additions should copy the case-insensitivity conventions established
  here (function names are already lowercased at dispatch, line 303).
