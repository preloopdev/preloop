# Code Review Guide

This document explains how we review changes to `preloop`, a Rust reimplementation
of the GitHub Actions control plane and runner. Our north star is byte-for-byte
protocol fidelity with the official `actions/runner`, and that goal reorders the
usual review priorities. Correctness and wire compatibility come before
everything else, so a change that reads elegantly but diverges subtly from the
official protocol is worse than one that is boring and identical.

Before you review anything non-trivial, read `AGENTS.md`, `CONTRIBUTING.md`, and
`docs/architecture.md` so you understand the crate boundaries and the
compatibility constraints.

---

## 0. Reviewer's contract

Review the diff against its intent instead of against your personal preference.
Start by asking what the change is trying to accomplish, then judge whether it
does that safely and completely.

Every blocking comment should carry evidence, whether that is a file and line
reference, a golden capture, a spec section, or a failing command. A comment that
amounts to "I don't like this" is a style suggestion, so label it as one.

Classify every comment you leave with one of these prefixes so the author knows
how to triage it:

- `blocker:` for anything that must be fixed before merge, such as a correctness
  bug, protocol drift, a security hole, or data loss.
- `concern:` for something that should be fixed or explicitly justified, and that
  the reviewer and author need to resolve together.
- `nit:` for optional polish that is entirely the author's call and never blocks a
  merge.
- `question:` for information you need before you can finish reviewing.

When you approve, you are saying you would be comfortable owning the regression.
If it breaks at 2am, you should be fine being the one who gets paged.

---

## 1. Correctness and protocol fidelity (the top priority)

This codebase lives or dies on wire compatibility, so this is where most of your
review effort belongs.

Start by asking whether the change touches a runner-facing surface, meaning
anything under `/_apis/`, `/broker/`, `/twirp/`, or the DTOs in
`preloop-gha-protocol/src/azdo/`. If it does, walk through the CONTRIBUTING
compatibility checklist and confirm the change does not alter any JSON field
name, casing, or default, any HTTP status code that a runner uses for retry or
terminal decisions, or the lease timing, session lifetime, and message delivery
order that runners depend on.

Then confirm it was validated against the official runner rather than against
unit tests alone. Passing unit tests is necessary but far from sufficient, so
look for `just dogfood` or `just conform` evidence, or golden-capture diffs under
`.runner-watch/golden/`. Treat "the tests pass" as insufficient proof for any
protocol change.

Serde round-trip fidelity has to survive for every wire DTO the change touches. A
field rename without a matching `#[serde(rename)]` or a golden update is a
blocker.

Expression and parser semantics have to match GitHub, not whatever happens to
seem reasonable. The `preloop-gha-expressions` and `preloop-gha-parser` crates mirror
upstream quirks such as truthiness rules, matrix ordering through `IndexMap`, and
glob semantics, so any divergence introduced in the name of cleanliness is
actually a bug.

Make sure the edge cases are enumerated, including empty inputs, missing optional
fields, cancellation mid-job, dependency failure, and the matrix `fail-fast` and
`max-parallel` paths. The concurrency state machines are where this codebase
tends to break, so give them extra scrutiny.

Finally, check that the error paths are real and not silently swallowed. A `let _ =`
on a `Result` that carries protocol meaning, or an `unwrap()` or `expect()` on
runner-supplied input, deserves a blocker.

As a rule of thumb, if the change alters what bytes a runner sees or when it sees
them, the burden of proof is a golden diff or a live dogfood run rather than a
green `cargo test`.

---

## 1a. Bug fixes must be reproduced (non-negotiable)

A fix is not reviewable until the bug it fixes has been reproduced first.

The PR should include a failing reproduction and cite it directly, whether that
is the exact command, the workflow fixture, the test, or the golden diff that
triggers the bug on `main`, along with its failing output. A PR that says "fixed
X" without a reproduction is incomplete work, not a finished fix.

That same reproduction should then pass once the change is applied, because the
before-and-after is what proves the fix works. A green but unrelated test does
not demonstrate anything about the bug in question.

Wherever possible, the reproduction should be captured so it guards against
regression. Prefer a committed test or fixture under `fixtures/` or a crate's
`tests/` directory that fails without the fix and passes with it, and for
protocol bugs that means a golden capture or a `just conform` case rather than
only a unit test.

The fix should address the root cause instead of the symptom. Be suspicious of a
change that suppresses a warning, swallows an error, or special-cases the single
input from the reproduction, and reject it unless the root cause genuinely is
that narrow.

If reproduction turns out to be impossible, the author needs to say so explicitly
and explain what makes it unreproducible, such as an external flake, a timing
window, or upstream behavior. In that case the fix needs extra scrutiny and a
plan for detecting a recurrence.

The short version is that "it should be fixed now" is not a claim we accept. Show
the bug, then show it gone, using the same trigger both times.

---

## 2. Security

Secrets and session crypto are load-bearing, and the repo enforces some of this
with `ast-grep` rules under `rules/*.yml` that you can run through `just
sg-scan-strict`. A green scan is the floor and not the ceiling, so keep
reviewing beyond it.

Confirm that `SecretString` discipline holds, which means `.expose()` is called
only at a genuine protocol boundary and never for logging, `Debug`, or `Display`.
Watch in particular for `.expose()` inside loops or iterators, which
`rules/no-expose-in-loop.yml` is designed to catch.

Make sure there is no inline masking, because all redaction has to go through
`preloop_gha_protocol::masking::mask_secrets`, which guarantees longest-first
ordering, empty-secret filtering, and DAP-keyword exclusion. A hand-rolled
`.replace(secret, "****")` is banned by `rules/no-inline-masking` and
`no-raw-secret-replace`.

Trace every path along which a secret value could reach a log, timeline, NDJSON
event, or error message, and confirm none of them leak. Keep crypto inside
`crypto.rs` so there is no ad-hoc key handling, nonce reuse, or home-grown JWT
signing scattered elsewhere.

Treat workflow YAML, action metadata, and runner payloads as attacker-influenced
input, which means bounding allocation, preventing path traversal in the cache,
artifact, and action-download paths, and preventing command injection into step
execution. Finally, keep the authentication and authorization boundaries intact,
since the server relies on OAuth plus mTLS while the local path relies on
loopback trust, and any change that widens that trust is a blocker.

---

## 3. Correctness of concurrency and state

The server holds run state behind `Arc<Mutex<…>>` together with `Notify` and
broadcast channels, and it is in-memory by default.

Confirm that lock scope stays minimal and that a lock is never held across an
`.await` in a way that could deadlock or serialize the whole server. Check that
`Notify` and broadcast changes preserve the publish-after-mutate ordering so that
a new subscriber cannot miss an event. Verify that cancellation is honored
promptly and leaves behind no zombie leases or sessions. Any change to durable
state should go behind the repository trait rather than reaching for `sqlx`
directly, as described in the State Model section of `docs/architecture.md`.

---

## 4. Performance

Fast local feedback is a product feature, and cold-start remains the tracked
blocker documented in `docs/preloop-performance-engineering.md`, so review with a
sense of what the code compiles to.

Look for avoidable allocation or copying on the hot paths, which include message
encode and decode, log streaming, masking, and expression evaluation. Prefer
borrows, and question every `.clone()`, `.to_string()`, or `.collect()` that sits
inside a loop. Watch for accidental quadratic behavior such as masking across
many secrets, repeated timeline scans, or per-line reprocessing, and remember
that `mask_secrets` resolves secrets once so nothing should re-resolve them per
iteration. Confirm that async code is genuinely concurrent instead of being a
sequential chain of `.await` calls where a `join!` or `try_join!` was intended.

Hold performance claims to measurement instead of assertion, so a stated
improvement should come with a before-and-after number from the benchmark harness
under `benchmarks/` or `just bench-preloop`. At the same time, avoid blocking on
micro-optimizations outside the hot paths, where readability wins.

---

## 5. Elegance and simplicity

Taste matters here in service of the next maintainer, though never at the expense
of the correctness priorities in section 1.

Push for the smallest change that accomplishes the goal, and reject scope creep
such as retries, validation, telemetry, or abstraction added "while we're here"
unless the task actually asked for it. Resist speculative abstraction, since one
concrete caller does not justify a trait, and lean on the existing
`RunStore`, `AuthProvider`, and `RunnerProvider` patterns when a real seam
exists rather than inventing new ones.

Keep one way to do each thing, because a second convention living beside an
existing one is a defect, so a new masking helper sitting next to `mask_secrets`
should be rejected. Expect names to carry meaning, control flow to read top to
bottom, and deep nesting to be flattened with early returns. Expect dead code to
be deleted instead of commented out, with no leftover shims, aliases, or
re-exports after a migration, since we default to a clean cutover. Above all,
remember that elegance never buys a protocol divergence, so if the clean version
changes the bytes then the clean version is wrong.

---

## 6. Maintainability

Confirm that the change respects the crate layering, where wire and domain types
live in `preloop-gha-protocol`, parsing lives in `preloop-gha-parser`, the expression
engine lives in `preloop-gha-expressions`, and routes and queueing live in
`preloop-runner-server`. A DTO defined outside the protocol crate, or business logic
leaking into a route handler, is a structural smell worth flagging.

Check that error handling matches its layer, which means `thiserror` enums in
libraries, `ApiError` in HTTP handlers, and `anyhow` only at the top level or in
binaries. Expect the reasoning behind anything non-obvious to be captured in a
comment, especially quirks that exist to match the official runner, because a
note like `// matches Runner.Worker v2.336.0 casing` keeps a future cleanup from
reintroducing a bug, and it helps to link the upstream source or golden capture.

Make sure the docs move with the behavior, including the `docs/architecture.md`
module map, `docs/fidelity-gap.md` for protocol status, and any relevant files
under `plans/`. Finally, expect tests to defend observable contracts, since a
good test fails on a plausible bug while asserting struct field defaults or
source text is just noise. Tests should follow the existing conventions and stay
deterministic and safe to run in the full suite, which includes respecting
`PROPTEST_CASES`.

---

## 7. The mechanical gate

These checks are table stakes that the author runs and the reviewer confirms, and
a PR that has not cleared them is not ready for human review yet.

- [ ] `just test-ci` passes, covering `fmt-check`, `clippy -D warnings`, and the
  workspace tests.
- [ ] `just sg-scan-strict` passes, covering the secret-handling structural rules.
- [ ] For a bug fix, a failing reproduction is shown on `main` and passes with the
  change, as described in section 1a.
- [ ] For a protocol-touching change, `just dogfood` or `just conform` evidence is
  attached.
- [ ] For a wire-shape change, the golden captures under `.runner-watch/golden/`
  are updated and diffed.
- [ ] No new `clippy` `#[allow(...)]` appears without a one-line justification.

---

## 8. Where bugs actually hide here

Concentrate your attention on the hotspots this codebase has taught us to watch,
listed roughly in order of how often they bite:

1. Casing and defaults on wire DTOs, which are the classic source of silent drift.
2. `.expose()` call sites, since each one is a potential leak whose sink you
   should trace.
3. Masking order and empty secrets, meaning anything that bypasses `mask_secrets`.
4. Matrix and DAG expansion ordering, which has to stay `IndexMap`-stable.
5. Status codes and retry semantics, since a wrong code changes runner behavior.
6. Locks held across `.await` and `Notify` ordering, which produce deadlocks and
   lost wakeups.
7. Cold-start and allocation regressions on the job-dispatch path.

When you are unsure about protocol behavior, treat the official runner source and
the golden captures as the source of truth, ahead of the unit tests and ahead of
this document.
