# Plan 012: Move oversized inline test suites out of production modules

> **Executor instructions**: Follow step by step. This is a **pure test relocation** —
> no production code, no test logic, no assertions change. Verify each crate's test
> count is identical before and after each move. STOP on any stop condition. Update
> `plans/README.md` when done.
>
> **Drift check (run first)**: `git diff --stat 3505476..HEAD -- crates/`
> If a target file below changed, re-read its `#[cfg(test)]` boundary before moving.

## Status

- **Priority**: P1 (refactor anchor — highest leverage, lowest risk)
- **Effort**: M
- **Risk**: LOW (parser/expr/protocol/events) → MED (server, needs a narrow test seam)
- **Depends on**: none. Do this FIRST in the refactor phase — it shrinks every
  god-file before the structural splits (013–017) touch them.
- **Category**: tech-debt / tests
- **Planned at**: commit `3505476`, 2026-07-16

## Why this matters

Test-only code dominates the largest "production" files, inflating the bloat the
refactor targets and coupling integration tests to private internals:

| File | Inline test lines | % of file |
|------|-------------------|-----------|
| `aksh-runner-server/src/lib.rs` | 8925–15402 (~6,478) | ~42% |
| `aksh-gha-protocol/src/azdo.rs` | 1255–2285 (~1,031) | ~45% |
| `aksh-runner/src/worker/steps_runner.rs` | 1044–2136 (~1,093) | ~51% |
| `aksh-runner/src/worker/job_extension.rs` | 939–1968 (~1,030) | ~52% |
| `aksh-runner-server/src/scheduling.rs` | 346–1371 (~1,026) | ~75% |
| `aksh-gha-parser/src/lib.rs` | 1847–2648 (~802) | ~30% |
| `aksh-runner/src/worker/contexts.rs` | 538–1201 (~664) | ~55% |
| `aksh-gha-expressions/src/lib.rs` | 1064–1492 (~429) | ~29% |
| `aksh-runner-server/src/events/property_tests.rs` | whole file (~413) | already separate |

Moving these makes the structural splits (013–017) tractable and cuts normal-build
typecheck surface. Two mechanics apply depending on private access:
- **Public-behavior tests** → a crate `tests/` integration file (no private access).
- **Tests using private items** (`super::*`, private helpers, private state) → a
  sibling unit-test file via `#[cfg(test)] #[path = "..."] mod tests;` which preserves
  `super::*` visibility. Use this whenever moving to `tests/` would require exposing
  internals.

**Also fix here** (ArchWorkerCrate ARCH-10): `job_runner.rs:1844` declares `mod tests`
**without** a preceding `#[cfg(test)]`, so its test code compiles into normal builds.
Add the attribute.

## Current state

- All target modules use `#[cfg(test)] mod tests { use super::*; ... }` at the bottom
  of the production file (except `job_runner.rs`, missing the attribute).
- `aksh-runner-server` tests construct `AppState` directly and use a gated internal
  test API (`app_with_test_api`, `TEST_API_TOKEN`) plus private `InnerState` — so the
  server suite must use the `#[path]` sibling-unit-file mechanic, not `tests/`.
- Convention: crates already have some `tests/` dirs (e.g. `aksh-gha-protocol/tests/protocol_golden.rs`).

## Commands you will need

| Purpose | Command | Expected |
|---------|---------|----------|
| Per-crate test count (before/after each move) | `cargo test -p <crate> --quiet 2>&1 \| tail -5` | identical pass count before & after |
| Full check | `cargo check --workspace` | exit 0 |
| Full tests | `cargo test --workspace --quiet` | same total as baseline |
| Clippy | `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |

## Scope

**In scope**: relocating existing `#[cfg(test)]` modules from the files above into
sibling `*_tests.rs` files (via `#[path]`) or `tests/` integration files; adding the
missing `#[cfg(test)]` to `job_runner.rs`. No change to any assertion, fixture, or
production line.

**Out of scope**: writing new tests; changing test logic; the structural production
splits (those are 013–017); moving `events/property_tests.rs` and
`concurrency_properties.rs`/`concurrency_http_properties.rs` (already separate files —
leave them).

## Steps

Do one crate at a time; commit per file; confirm identical test count each time.

### Step 0: Baseline

Run `cargo test --workspace --quiet 2>&1 | tail -20` and record the per-crate pass
counts. This is the invariant every step must preserve.

### Step 1: `job_runner.rs` — add the missing `#[cfg(test)]`

Add `#[cfg(test)]` immediately before `mod tests` at `job_runner.rs:1844`.

**Verify**: `cargo test -p aksh-runner --quiet 2>&1 | tail -5` → same pass count;
`cargo build -p aksh-runner` no longer compiles the test module in a normal build
(the module is now gated).

### Step 2: Low-risk public/near-public crates first

For each of `aksh-gha-expressions/src/lib.rs`, `aksh-gha-parser/src/lib.rs`,
`aksh-gha-protocol/src/azdo.rs`: cut the `#[cfg(test)] mod tests { ... }` block into a
sibling file (e.g. `src/lib_tests.rs`, `src/azdo_tests.rs`) and replace the block with:
```
#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
```
This keeps `use super::*;` working (the file is still a submodule). Do NOT move to
`tests/` unless the tests only use public API.

**Verify** (per file): `cargo test -p <crate> --quiet 2>&1 | tail -5` → identical count.

### Step 3: Worker modules

Same `#[path]` sibling-file move for `steps_runner.rs`, `job_extension.rs`,
`contexts.rs` → `steps_runner_tests.rs`, etc.

**Verify** (per file): `cargo test -p aksh-runner --quiet 2>&1 | tail -5` → identical.

### Step 4: Server modules (needs the sibling-file mechanic)

Move `scheduling.rs` tests then `lib.rs` tests into `scheduling_tests.rs` /
`lib_tests.rs` via `#[path]`. These access private `InnerState`/`AppState` and the
gated test API — the `#[path]` submodule preserves that access, so no new public
surface is required. Keep `const TEST_API_TOKEN` and helpers (`app`, `request_json`)
with the tests.

**Verify**: `cargo test -p aksh-runner-server --quiet 2>&1 | tail -5` → identical count;
`wc -l crates/aksh-runner-server/src/lib.rs` → ~8,900 (down from 15,402).

### Step 5: Full-workspace confirmation

**Verify**: `cargo test --workspace --quiet` → total pass count equals the Step 0
baseline; `cargo clippy --workspace --all-targets -- -D warnings` → exit 0.

## Test plan

No new tests. The test plan is invariance: the set and count of passing tests is
byte-for-byte identical before and after. If any test is dropped or fails to compile in
its new location, that is a STOP.

## Done criteria

- [ ] `cargo test --workspace --quiet` total pass count == Step 0 baseline
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [ ] `job_runner.rs` `mod tests` is gated by `#[cfg(test)]`
- [ ] `wc -l crates/aksh-runner-server/src/lib.rs` shows ~8,900 (production only)
- [ ] No production (non-test) line changed: `git diff` shows only moved test blocks + `#[path]` stubs
- [ ] `plans/README.md` row updated

## STOP conditions

- A test fails to compile in its new location because it referenced something more
  private than `super::*` exposes → revert that one file to inline and report it; do not
  add `pub` to production items to make a test move (that defeats the purpose).
- The post-move test count differs from baseline (a test was silently dropped) → STOP.
- A file's `#[cfg(test)]` block is intertwined with a `#[cfg(test)]` helper that
  production also uses under `cfg(test)` — keep such helpers in the production file.

## Maintenance notes

- After this lands, plans 013–017 operate on much smaller production files.
- Prefer the `#[path]` sibling-unit-file pattern for anything needing private access;
  reserve `tests/` for genuine public-API integration tests.
- A reviewer should diff only for accidental production-line changes.
