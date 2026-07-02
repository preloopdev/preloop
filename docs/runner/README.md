# aksh-runner — Milestone Documentation

Implementation plan: [`docs/rust-runner-plan.md`](../rust-runner-plan.md)  
Compatibility roadmap: [`roadmap.md`](roadmap.md)

## Milestone Index

| # | Milestone | Doc | Status |
|---|-----------|-----|--------|
| M0 | Scaffolding, docs skeleton, harness plumbing | [00-architecture.md](00-architecture.md) | Implemented |
| M1 | Configuration & registration | - | Implemented |
| M2 | OAuth, session, message listener | - | Implemented |
| M3 | Worker spawn, job lifecycle | - | Implemented |
| M4 | Execution context, contexts, expressions | - | Implemented |
| M5 | Script steps, process invoker, commands | - | Implemented |
| M6 | Actions: download, node, composite, pre/post | - | Implemented |
| M7 | Containers: job/service/docker actions | - | Implemented |
| M8 | Cache, artifacts, runtime env plumbing | - | Implemented |
| M9 | Legacy AzDO compat mode | - | Implemented |
| M10 | Cancellation, timeouts, hardening | - | Implemented |
| M11 | Performance & size evaluation | [11-benchmarks.md](11-benchmarks.md) | Implemented |
| M12 | Fidelity audit + repo docs | [12-fidelity-audit.md](12-fidelity-audit.md) | Implemented |

*Note: Milestones indicate codebase feature implementations. Conformance scenarios (GitHub vs. aksh) are tracked in the [roadmap](roadmap.md).*

## Gate commands

### Tier 2 (local, aksh)

```sh
# Build
just build-runner

# Version check
target/release/aksh-runner --version

# Unit tests
cargo test -p aksh-runner --quiet

# Full workspace
just test-ci
```

### Tier 1 (GitHub truth)

```sh
# Conformance diff against golden captures
just conform-runner 01-register-and-idle

# E2E against local aksh
just runner-e2e fixtures/golden/simple-echo.yml
```
