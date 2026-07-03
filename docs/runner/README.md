# aksh-runner — Milestone Documentation

Implementation plan: [`rust-runner-plan.md`](rust-runner-plan.md)
Compatibility roadmap: [`roadmap.md`](roadmap.md)
Fidelity gap log (F001–F017 fixed, F018+ pending): [`runner_fidelity_gap.md`](runner_fidelity_gap.md)

Statuses below reflect the current codebase status. All core runner/worker features, environment plumbing, expression evaluation, action lifecycle hooks, and the gating test harness (H1/H2) are fully implemented and verified against both local aksh and live GitHub.

## Milestone Index

| # | Milestone | Doc | Status |
|---|-----------|-----|--------|
| M0 | Scaffolding, docs skeleton, harness plumbing | [00-architecture.md](00-architecture.md) | ✅ Done |
| M1 | Configuration & registration | — (doc pending) | ✅ Done, verified vs golden 01 |
| M2 | OAuth, session, message listener | — (doc pending) | ✅ Done, verified vs golden 01 (F008 broker-URL pending) |
| M3 | Worker spawn, job lifecycle | — (doc pending) | ✅ Done, verified vs golden 06 (log uploads, Twirp updates, lock renewal) |
| M4 | Execution context, contexts, expressions | — (doc pending) | ✅ Done, expression wildcards, bracket access, hashFiles, and secrets context implemented |
| M5 | Script steps, process invoker, commands | — (doc pending) | ✅ Done, annotations, step summary, and GITHUB_ENV/PATH fully working |
| M6 | Actions: download, node, composite, pre/post | — (doc pending) | ✅ Done, action resolution, pre/post hooks LIFO, composite outputs/nesting implemented |
| M7 | Containers: job/service/docker actions | — (doc pending) | ❌ Not wired (F026) |
| M8 | Cache, artifacts, runtime env plumbing | — (doc pending) | ✅ Done, ACTIONS_* env plumbing fully working (cache and artifact steps pass live) |
| M9 | Legacy AzDO compat mode | — (doc pending) | ❌ Reporting unwired (F030) |
| M10 | Cancellation, timeouts, hardening | — (doc pending) | ⚠️ Partial (F031–F033) |
| M12 | Fidelity audit + repo docs | [12-fidelity-audit.md](12-fidelity-audit.md) | ✅ Scorecard corrected 2026-07-02 |
| H1 | `runner-e2e` orchestrator | — | ✅ Done (subcommand in aksh-conformance) |
| H2 | `runner-diff` + `--record-flows` | — | ✅ Done (subcommand in aksh-conformance) |

## Gate commands

### Conformance & E2E Validation

```sh
just build-runner                                    # Build aksh-runner in release
cargo test --workspace                               # Run all workspace unit/integration tests
just test-ci                                         # fmt-check + clippy + test

# Run H1 E2E tests locally against aksh-runner-server
just runner-e2e fixtures/golden/simple-echo.yml

# Run H2 diff comparison against golden scenario captures
just conform-runner 01-register-and-idle             # Diff against official target
just conform-local 01-register-and-idle              # Diff against aksh target
```
