# Specula TLA+ experiment — 2026-08-04

A full [Specula](https://github.com/SpeculaIO/Specula) run (code analysis →
spec generation → validation → bug confirmation) against this repository,
using its own TLA+ model-checking pipeline. The model was built from the
preloop source by Specula's spec-generation agent, manually repaired during
validation (typed sentinels, successor-state completeness, over-permissive
action guards), and bug-hunted with real SANY + TLC runs.

Run id: `20260804-120119-9811` (work dir kept at
`/private/tmp/Specula/runs/20260804-120119-9811/preloop`).

> The spec files are reconciled to the current working tree. Historical fixes
> are represented as repaired model paths. As of 2026-08-06 no source defect
> from this run remains open; `spec/bug-report.md` records each one as it was
> found, and `spec/findings.json` carries the current `status` per finding.

## Result: all findings fixed

| Finding | Current disposition |
|---|---|
| MC-S2 / MC-2 / MC-S3 / MC-S5 / MC-S6 / CR-2 | Fixed in the current Rust tree and synchronized in `base.tla`; see `spec/bug-report.md` for source locations and model semantics. |
| MC-R1 | **Fixed** (code review, 2026-08-06): `apply_matrix_fail_fast` now calls `release_concurrency_for_job` for every sibling it cancels. Regression test: `concurrency_properties.rs` `fail_fast_releases_the_cancelled_sibling_concurrency_slot`. |
| MC-R2 | **Fixed** (code review, 2026-08-06): the `cancel_in_progress` arm of `try_acquire_concurrency` no longer cancels a predecessor belonging to the arriving run, so `release_concurrency_for_run` can no longer evict the holder just admitted. Regression test: `concurrency_properties.rs` `same_run_cancel_in_progress_keeps_the_arriving_holder`. |

Both residuals were confirmed by reverting each fix and observing the
regression test fail on the predicted symptom (MC-R1: the slot stays
`Some(Job { .. job_id: "j2" })`; MC-R2: the group records no holder after a
successful admission), then pass with the fix restored.

CR-1 (broker messageId collision) was dropped during confirmation: already
fixed in review commit `193986ce`.

## Files

- `spec/base.tla` — the repaired TLA+ spec (single module; SANY-valid, TLC-checkable)
- `spec/Trace.tla`, `spec/MC.tla` — trace-replay / model-checking modules
- `spec/*.cfg` — TLC configs: `MC.cfg` (main), `base.cfg`, `Trace.cfg`,
  `Trace-replay-s1.cfg`, and per-scenario hunt configs
  `MC_hunt_s{1..6}_*.cfg` (+ `.safety.cfg` variants used for the final runs)
- `spec/counterexamples/` — the four TLC counterexample traces (S2, S3, S5, S6)
- `spec/findings.json` — current reconciliation: one entry per finding with a
  `status` field (all six now fixed or withdrawn), plus bounded verification
- `spec/candidates.json` — consolidated confirmation candidates
- `spec/bug-report.md` — per-bug Rust-source evidence
- `spec/repair-requests/RR-001.md` — resolved spec-repair request (consumed;
  the S3 violation is a code bug, not a model bug)
- `confirmed-bugs.md` — Specula Phase 4a confirmation report
- `repro/` — reproduction scripts written by the confirmation agents
- `confirmation/<finding>/` — per-finding verdicts, debate logs, investigations

## Re-running

Prerequisites: Java 21 (`tla2tools.jar`), then:

```sh
cd experiments/specula-20260804/spec
java -XX:+UseParallelGC -cp /path/to/tla2tools.jar tla2sany.SANY base.tla
java -XX:+UseParallelGC -cp /path/to/tla2tools.jar tlc2.TLC \
  -config MC_hunt_s2_concurrency.safety.cfg -workers auto -deadlock MC
```

Known TLC pitfalls hit during this run (documented for the next person):
- Run TLC hunts sequentially or with separate `-metadir` dirs; concurrent runs
  sharing the default `states/` collide and corrupt each other's state files.
- `CONSTRAINT` entries must be single names; TLC parses `/\` inside a
  constraint as the constraint's name.
- TLC is strictly typed at runtime: equality between records/integers/strings/
  model values of different classes throws instead of returning FALSE.
- Bare `*` comment lines are invalid TLA+; use `\*`.
- Parenthesize disjunctive RHS of primed assignments (`x' = y \/ P` parses
  as `(x' = y) \/ P`).

## Spec repairs and current-source synchronization

The validation-time model repairs remain below; they are model correctness
changes, not claims about Rust behavior.

1. Replaced the universal `NONE = "NONE"` sentinel with type-homogeneous
   sentinels (`NoStatus`, `NoHolder`, `NoReq`, …) — TLC strict typing.
2. `CancelJob` rewritten to mirror real `release_concurrency_for_job`
   (release from *every* group, holder-kind semantics, C-07 key pruning).
3. `CompleteJobApply` ELSE branch now fully specifies `runJobs'`.
4. `EnqueuePending` guard: a job reserved for expansion (`expanding` /
   `pendingExp`) cannot re-enter `pendingJobs` (matches `defer_expansion`
   popping it first).
5. Non-terminal guard on all `Arrive*` acquisition actions — those actions
   represent external arrival/enqueue paths; promotion-time acquisition is
   modelled separately, and a terminal run never re-acquires.

The current-source synchronization additionally covers completion discard,
timeout-trigger guards, expansion request retirement, promotion-time gate
acquisition, failed step-update requeue, brace escaping, substring masking,
and recursive process-tree kill. `spec/bug-report.md` is authoritative for
the current disposition; the older `confirmed-bugs.md` remains the historical
confirmation record.
