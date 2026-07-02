# aksh-runner — Milestone Documentation

Implementation plan: [`rust-runner-plan.md`](rust-runner-plan.md)
Compatibility roadmap: [`roadmap.md`](roadmap.md)
Fidelity gap log (F001–F017 fixed, F018+ pending): [`runner_fidelity_gap.md`](runner_fidelity_gap.md)

Statuses below reflect the **2026-07-02 full-code audit** (see the status table in `rust-runner-plan.md`). "Verified" = code diffed against golden captures; **no Tier-1/Tier-2 gate has run yet** because the H1/H2 harness is unbuilt.

## Milestone Index

| # | Milestone | Doc | Status |
|---|-----------|-----|--------|
| M0 | Scaffolding, docs skeleton, harness plumbing | [00-architecture.md](00-architecture.md) | ✅ Done |
| M1 | Configuration & registration | — (doc pending) | ✅ Done, verified vs golden 01 |
| M2 | OAuth, session, message listener | — (doc pending) | ✅ Done, verified vs golden 01 (F008 broker-URL pending) |
| M3 | Worker spawn, job lifecycle | — (doc pending) | ⚠️ Partial — renewjob (F018), step updates (F019), log upload (F020) missing |
| M4 | Execution context, contexts, expressions | — (doc pending) | ⚠️ Partial — expression gaps (F027), secrets context (F028) |
| M5 | Script steps, process invoker, commands | — (doc pending) | ⚠️ Mostly done — annotations (F025), summary (F035), env vars (F034) |
| M6 | Actions: download, node, composite, pre/post | — (doc pending) | ⚠️ Partial — resolution (F022), pre/post (F023), composite outputs (F024) |
| M7 | Containers: job/service/docker actions | — (doc pending) | ❌ Not wired (F026) |
| M8 | Cache, artifacts, runtime env plumbing | — (doc pending) | ❌ Missing (F021) |
| M9 | Legacy AzDO compat mode | — (doc pending) | ❌ Reporting unwired (F030) |
| M10 | Cancellation, timeouts, hardening | — (doc pending) | ⚠️ Partial (F031–F033) |
| M11 | Performance & size evaluation | [11-benchmarks.md](11-benchmarks.md) | ⚠️ Partial — size/cold-start only |
| M12 | Fidelity audit + repo docs | [12-fidelity-audit.md](12-fidelity-audit.md) | ✅ Scorecard corrected 2026-07-02 |
| H1 | `runner-e2e` orchestrator | — | ❌ Missing |
| H2 | `runner-diff` + `--record-flows` | — | ❌ Missing |
| H3 | `fixtures/runner/` golden corpus | — | ❌ Missing (inline unit tests only) |

## Gate commands

### Working today

```sh
just build-runner                        # cargo build --release -p aksh-runner
target/release/aksh-runner --version     # version check
cargo test -p aksh-runner --quiet        # 50+ unit tests
just test-ci                             # fmt-check + clippy + workspace tests
just bench-runner                        # size + cold-start metrics only
```

### Declared but non-functional (blocked on H1/H2 — see roadmap §4)

```sh
just runner-e2e fixtures/golden/simple-echo.yml   # runner-e2e subcommand missing
just conform-runner 01-register-and-idle          # runner-diff subcommand missing
just conform-local 01-register-and-idle           # runner-diff subcommand missing
```
