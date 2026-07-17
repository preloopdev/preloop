# Plan 018: Codebase hygiene — HTTP client, error mapping, dead code, deps, module map

> **Executor instructions**: Five small, independent hygiene fixes. Do each as its own
> commit; each is shippable alone. Verify each; STOP on a stop condition; update
> `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 3505476..HEAD -- crates/ Cargo.toml`

## Status

- **Priority**: P3
- **Effort**: M (sum of small parts; do the high-value ones first)
- **Risk**: LOW (dead code, deps, docs) → MED (HTTP client, error mapping)
- **Depends on**: 015 (error mapping is cleaner after `errors.rs` exists) for part B;
  others independent.
- **Category**: tech-debt / dx
- **Planned at**: commit `3505476`, 2026-07-16

## Why this matters

Cross-cutting hygiene that lowers maintenance cost and removes footguns, without
touching wire compatibility:

- **A. HTTP client sprawl**: the runner has a configured `HttpClient` (timeouts, CA
  bundle, retries, typed errors) but action downloads (`actions_download.rs:194-208`)
  and server GitHub calls (`github.rs:123-151`, `scheduler.rs:362-393`) construct raw
  `reqwest::Client::new()`, bypassing pooling/TLS/retry policy and duplicating auth/UA
  setup. CLI/conformance do too.
- **B. Error mapping**: `ApiError` maps parser/protocol/crypto/cache/artifact/`io::Error`
  all to 400 (`lib.rs:8563-8596`) — server I/O failures return `400`. `github.rs` handlers
  return bare `StatusCode`. `crypto.rs` public APIs return `anyhow` despite a `CryptoError`
  enum existing (`:479-500`).
- **C. Dead code + broad suppressions**: `#[allow(dead_code)]` blankets `InnerState` and
  `QueuedJob` and several DTOs; `complete_job_compat_org` (`lib.rs:7717`) is explicitly
  dead with no route.
- **D. Stale deps**: `aksh-runner-server/Cargo.toml` declares `sha1` and `thiserror` with
  no source usage.
- **E. No module map / CONTRIBUTING**: no root `CONTRIBUTING.md`; `docs/architecture.md`
  maps crates but not the god-files' internal structure.

## Current state

- A: `crates/aksh-runner/src/client/http.rs:17-47` (the good wrapper);
  `actions_download.rs:194-208`, `github.rs:123-151`, `scheduler.rs:362-393`,
  `aksh-runner-client/src/main.rs:84-96`, `aksh-conformance/src/main.rs:568-580` (raw clients).
- B: `lib.rs:8563-8602` (`ApiError` conversions); `github.rs:509-579,864-879` (bare
  StatusCode); `crypto.rs:73-107,369-417,479-500` (`anyhow` vs `CryptoError`).
- C: `lib.rs:1439-1441,1566-1568,3891-3897,4678-4718,7717-7724,7841-7855`;
  `aksh-dap/src/harness.rs:623-628`, `debugger.rs:196-203`.
- D: `crates/aksh-runner-server/Cargo.toml` (`sha1`, `thiserror`).

## Commands you will need

| Purpose | Command | Expected |
|---------|---------|----------|
| Check | `cargo check --workspace` | exit 0 |
| Tests | `cargo test --workspace --quiet` | baseline |
| Clippy | `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |
| Dead-code scan | `cargo clippy --workspace --all-targets -- -D warnings` after removing an `#[allow]` | reveals real dead code |

## Scope

**In scope**: the files above; a shared HTTP client (move the runner's `HttpClient`
policy to a small shared crate/module and inject it); `ApiError`/`CryptoError` mapping;
dead-code removal + narrowing suppressions; `Cargo.toml` dep removal; a root
`CONTRIBUTING.md` + a module map appended to `docs/architecture.md`.

**Out of scope**: changing HTTP behavior that affects wire (timeouts that change runner
registration semantics — preserve current effective values); status codes runners rely
on for retry (verify before changing — see STOP).

## Steps

### Step A: One HTTP client policy

Extract the runner's `HttpClient` policy (timeouts, connect timeout, UA, CA bundle,
retries, typed status errors) into a shared location usable by server + clients (a small
`aksh-http` module/crate, or `aksh-gha-protocol` if that's where shared infra lives).
Replace raw `reqwest::Client::new()` sites with the shared client, preserving each
endpoint's auth/Accept headers as typed config. Action downloads MUST use the configured
CA bundle.

**Verify**: `cargo test --workspace --quiet` → baseline; a test that action download uses
the configured client (or at least compiles through the shared path).

### Step B: Typed error mapping

Route `ApiError` conversions so validation → 4xx, upstream/GitHub failures → 502/503,
I/O/storage failures → 500 (not 400). Extend `CryptoError` for serialization/signature
cases and have `crypto.rs` public APIs return it instead of `anyhow`. Give `github.rs`
handlers a consistent error type/response shape (reuse `ApiError`).

**Verify**: `cargo test -p aksh-runner-server --quiet` → passes with tests asserting an
I/O failure maps to 500, a validation failure to 4xx.

### Step C: Remove dead code, narrow suppressions

Delete `complete_job_compat_org` after confirming no route/callsite
(`grep -rn complete_job_compat_org crates/`). Replace struct-wide `#[allow(dead_code)]`
on `InnerState`/`QueuedJob` with field-level allowances **only** where serde needs an
unread field (add a one-line reason each). Remove the DAP `_record_surface` placeholder
if unimplemented, or implement it. Run clippy after each removal to surface newly-visible
dead code.

**Verify**: `cargo clippy --workspace --all-targets -- -D warnings` → 0;
`grep -rn complete_job_compat_org crates/` → none.

### Step D: Remove stale deps

Delete `sha1` and `thiserror` from `aksh-runner-server/Cargo.toml` after confirming no
source usage (`grep -rn "sha1\|thiserror" crates/aksh-runner-server/src`). Regenerate
lockfile only if Cargo changes it.

**Verify**: `cargo check -p aksh-runner-server` → exit 0.

### Step E: Module map + CONTRIBUTING

Add a root `CONTRIBUTING.md` (check/test/clippy/conformance commands + a
compatibility-change checklist referencing the official-source authority and golden
captures). Append a module map to `docs/architecture.md` listing, for the (now-split)
server/worker/protocol crates, which module owns which protocol surface. Link from
`README.md` and `AGENTS.md`.

**Verify**: links resolve; commands in `CONTRIBUTING.md` are the real `justfile` ones.

## Test plan

- B: tests asserting I/O→500, validation→4xx, and a `CryptoError` variant surfaces
  through the crypto API.
- A/C/D: invariance — `cargo test --workspace --quiet` baseline preserved.
- Verification: `cargo test --workspace --quiet` → baseline; clippy clean.

## Done criteria

- [ ] `cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [ ] Raw `reqwest::Client::new()` removed from action-download + server GitHub paths (`grep -rn "Client::new()" crates/`)
- [ ] `ApiError` maps I/O/storage → 5xx, validation → 4xx (tests prove it)
- [ ] `grep -rn complete_job_compat_org crates/` → none; struct-wide dead-code allows narrowed
- [ ] `sha1`/`thiserror` removed from `aksh-runner-server/Cargo.toml`
- [ ] Root `CONTRIBUTING.md` + module map in `docs/architecture.md` exist and link back
- [ ] `cargo test --workspace --quiet` == baseline; `plans/README.md` updated

## STOP conditions

- Changing an `ApiError` status code that a runner uses to decide retry/terminal behavior
  → verify against the official client expectations (or golden captures) before changing;
  if a 400 is load-bearing for a runner path, keep it and note it.
- Removing a `#[allow(dead_code)]` reveals a serde field that IS needed for
  deserialization (unread but required) → keep it with a field-level allow + reason, do
  not delete the field.
- The shared HTTP client change would alter an effective timeout/retry that affects runner
  registration or lease timing → preserve current effective values; this is a structural
  dedup, not a policy change.

## Maintenance notes

- Add a dependency-hygiene check to CI (e.g. `cargo-udeps` or `cargo machete`) so stale
  deps and dead code don't re-accumulate.
- The module map should be updated as part of Plans 015/016/017 landing.
