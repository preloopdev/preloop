# Plan 001: Emit the official MaskHint wire shape so jobs with secrets don't break the official worker

> **Executor instructions**: Follow this plan step by step. Run every verification command and
> confirm the expected result before moving to the next step. If anything in the "STOP
> conditions" section occurs, stop and report — do not improvise. When done, update the status
> row for this plan in `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat 839791c..HEAD -- crates/aksh-gha-protocol/src/azdo.rs crates/aksh-gha-parser/src/job_builder.rs`
> The working tree at planning time already carried uncommitted changes to
> `crates/aksh-gha-protocol/src/azdo.rs` (+7/−0, unrelated to MaskHint). Compare the "Current
> state" excerpts against live code before proceeding; on a mismatch, treat as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: bug (protocol compatibility)
- **Planned at**: commit `839791c`, 2026-07-13

## Why this matters

aksh invents a `MaskType::Hash` variant that does not exist in the official protocol. The
official runner's `MaskType` enum has exactly two values, `Variable` and `Regex`
(`src/Sdk/DTWebApi/WebApi/MaskType.cs` in the official runner sources). When the unmodified
official `Runner.Worker` deserializes an aksh `AgentJobRequestMessage` whose `maskHints`
contains `{"type": "hash"}`, Newtonsoft.Json fails to map the unknown enum string — at best the
hint is useless, at worst the entire job message fails to deserialize and the job dies before
it starts. Every workflow that defines at least one non-empty secret hits this path, because
aksh emits one Hash hint per secret. Secret *masking* itself does not depend on these hints:
the official worker masks every variable with `isSecret: true` independently
(`src/Runner.Worker/Worker.cs:147-165`), which aksh already sets.

## Current state

- `crates/aksh-gha-protocol/src/azdo.rs` — wire DTOs. The offending enum (lines 768–774):

  ```rust
  /// Type of masking hint.
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
  #[serde(rename_all = "camelCase")]
  pub enum MaskType {
      /// A literal string to redact.
      Hash,
  }
  ```

- `crates/aksh-gha-parser/src/job_builder.rs` — the only emitter (lines 195–202):

  ```rust
  // Mask hints for secrets
  let mask_hints: Vec<MaskHint> = resolved_secrets
      .values()
      .filter(|v| !v.is_empty())
      .map(|v| MaskHint {
          hint_type: MaskType::Hash,
          value: v.clone(),
      })
      .collect();
  ```

- Official reference (read-only, do not modify): the vendored official runner mirror at
  `~/mitm-proxy/experiments/mitm/.cache/runner.server/src/`. The consumer is
  `Runner.Worker/Worker.cs:168-183`: only `MaskType.Regex` triggers `SecretMasker.AddRegex`;
  every other type logs "Unsupported mask type".
- Existing test asserting current behavior: `job_builder.rs:602-632`
  (`secrets_become_variables_and_mask_hints`) — asserts `mask_hints[0].value == "s3cr3t"`.
- Repo conventions: wire DTOs mirror upstream naming via `#[serde(rename)]`; prefer
  compatibility over local renaming (`AGENTS.md`). Match the existing enum style in `azdo.rs`.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Format | `cargo fmt --all --check` | exit 0 |
| Lint | `cargo clippy --workspace --all-targets` | exit 0, no new warnings |
| Targeted tests | `cargo test -p aksh-gha-parser --quiet` | all pass |
| Full tests | `cargo test --workspace --quiet` | all pass |

## Scope

**In scope** (the only files you should modify):
- `crates/aksh-gha-protocol/src/azdo.rs` (the `MaskType` enum + its doc comments)
- `crates/aksh-gha-parser/src/job_builder.rs` (the emitter + its tests)

**Out of scope** (do NOT touch, even though they look related):
- `crates/aksh-runner/*` — the Rust runner tolerates any mask hint; no change needed.
- `Runner.Worker` mirror sources — read-only reference.
- Secret redaction logic in `SecretString` / server logging — unrelated mechanism.

## Git workflow

- Branch: `advisor/001-mask-hint-official-wire`
- Commit style (from `git log`): conventional, e.g. `fix: emit official MaskHint wire shape`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Replace the enum with the official variants

In `crates/aksh-gha-protocol/src/azdo.rs`, replace the `MaskType` enum body:

```rust
/// Type of masking hint.
///
/// Upstream source: `MaskType.cs` — `Variable = 1`, `Regex = 2`. The official
/// worker only acts on `Regex` (`Worker.cs` InitializeSecretMasker); values are
/// serialized as camelCase strings on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum MaskType {
    /// Mask the value of a named variable.
    Variable,
    /// Mask everything matching a regular expression.
    Regex,
}
```

**Verify**: `cargo check -p aksh-gha-protocol` → exits 0. `grep -rn "Hash" crates/aksh-gha-protocol/src/azdo.rs` → no matches in the MaskType region.

### Step 2: Emit Regex hints with regex-escaped values

In `crates/aksh-gha-parser/src/job_builder.rs:195-202`, change the emitter to the official
semantics — a `regex` hint whose value is the *escaped* secret (GitHub sends regex-escaped
literals; the worker feeds the value to `AddRegex`):

```rust
let mask_hints: Vec<MaskHint> = resolved_secrets
    .values()
    .filter(|v| !v.is_empty())
    .map(|v| MaskHint {
        hint_type: MaskType::Regex,
        value: regex_escape(v),
    })
    .collect();
```

Add a private `fn regex_escape(s: &str) -> String` in the same file escaping the regex
metacharacters `\ . + * ? ( ) [ ] { } | ^ $ /` by backslash-prefixing (no existing equivalent
found in the workspace; do not add a regex crate dependency for escaping only).

**Verify**: `cargo test -p aksh-gha-parser --quiet` → the existing
`secrets_become_variables_and_mask_hints` test FAILS (it asserts the raw value) — expected;
fixed next step.

### Step 3: Update tests

In the `job_builder.rs` tests:
- Update `secrets_become_variables_and_mask_hints` to assert
  `msg.mask_hints[0].hint_type == MaskType::Regex` and that the value equals
  `regex_escape("s3cr3t")` (which for this input is `"s3cr3t"` — also add a secret containing
  a metacharacter, e.g. `p@$$(word)`, and assert the escaped form `p@\$\$\(word\)`).
- Add a serialization assertion: `serde_json::to_value(&msg.mask_hints[0]).unwrap()["type"] == "regex"`.

**Verify**: `cargo test -p aksh-gha-parser --quiet` → all pass, including updated tests.

## Test plan

- Updated: `secrets_become_variables_and_mask_hints` (shape + escaping + wire literal
  `"regex"`). Model additions after the existing inline test style in `job_builder.rs:600+`.
- The load-bearing wire assertion is the serialized `"type": "regex"` string — that is what
  the official worker parses.

## Done criteria

- [ ] `cargo fmt --all --check` exits 0
- [ ] `cargo clippy --workspace --all-targets` exits 0 (no new warnings)
- [ ] `cargo test --workspace --quiet` exits 0
- [ ] `grep -rn "MaskType::Hash" crates/` returns no matches
- [ ] Serialized mask hint is `{"type":"regex","value":"<escaped>"}` (asserted by test)
- [ ] No files outside the in-scope list modified (`git status`)
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:

- The `MaskType` enum or the `job_builder.rs:195-202` emitter no longer matches the excerpts
  (someone else touched mask hints since planning).
- Any test outside `aksh-gha-parser`/`aksh-gha-protocol` fails after the change — that means
  something else consumes `MaskType::Hash`; report the consumer instead of patching it.
- You find golden fixtures under `fixtures/` or `.runner-watch/golden/` asserting `"hash"` —
  report them; they need a deliberate regeneration decision, not an in-plan edit.

## Maintenance notes

- E2E validation (out of this plan's scope, note for the reviewer): run a workflow with a
  secret against the unmodified official runner (`./scripts/e2e-start.sh`) and confirm the job
  starts and the secret is masked in logs.
- If aksh later supports `add-mask` propagation to the server, reuse `MaskType::Variable`
  vs `Regex` distinction rather than inventing variants.
