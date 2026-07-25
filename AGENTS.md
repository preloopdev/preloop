# Repository Guidelines

## Overview

`aksh` reimplements the GitHub Actions control plane and official `actions/runner` in Rust. The unmodified runner registers, polls, executes, and reports against it. Also contains `runner-watch` for protocol conformance testing.

## Crates


| Crate                               | Role                                                                                       |
| ----------------------------------- | ------------------------------------------------------------------------------------------ |
| `aksh-runner-server`                | HTTP control plane: `/_apis/…` (runner protocol) + `/api/v1/…` (native REST) + `/broker/…` |
| `aksh-gha-parser`                   | Workflow YAML → typed model → job DAG/matrix expansion                                     |
| `aksh-gha-expressions`              | `${{ }}` parser/evaluator                                                                  |
| `aksh-gha-protocol`                 | Wire DTOs, session crypto, secret wrappers, NDJSON events                                  |
| `aksh-runner`                       | Rust runner: Listener + Worker (faithful to `actions/runner` v2.336.0)                     |
| `aksh-runner-client`                | CLI for submitting workflows                                                               |
| `aksh-cache` / `aksh-artifacts`     | File-backed protocol storage                                                               |
| `aksh-dap`                          | Debug Adapter Protocol bridge                                                              |
| `aksh-conformance` / `runner-watch` | Conformance harnesses and protocol-diff tooling                                            |


## Commands

```sh
just test-ci    # fmt-check + clippy + test (the full gate)
just serve      # cargo run --release -p aksh-runner-server -- serve --listen 127.0.0.1:9090
just dogfood    # E2E with real runner
```

## Key Conventions

- **Toolchain**: Rust 1.97, `cargo fmt`, `cargo clippy --workspace --all-targets`.
- **Error handling**: `anyhow` at top-level; `ApiError` in HTTP handlers; `thiserror` enums in libraries.
- **State**: in-memory behind `Arc<Mutex<…>>` + `Notify`/broadcast. Secrets use `SecretString` — call `expose()` only at protocol boundaries.
- **Wire compatibility**: `/_apis/…` is the source of truth. Validate protocol changes against the **official runner**, not only unit tests.
- **Broker path only**: all work targets the modern broker + Twirp results-service protocol (v2.329.0+).
- **ARM64 local target**: smolvm on Apple Silicon.

## Important Files

- `docs/architecture.md` — crate map + module map
- `docs/fidelity-gap.md` — protocol gaps and conformance status
- `docs/preloop-performance-engineering.md` — perf campaign record: harness, measurements, rejected ideas, and the cold-start blocker
- `CONTRIBUTING.md` — dev workflow and compatibility checklist
- `fixtures/workflows/dogfood.yml` — local self-hosted validation workflow
- `.runner-watch/golden/v2.335.1/` — protocol golden captures (prior baseline)
- `versions.toml` — pinned official runner (`2.336.0`)
- Official runner binary cache: `~/.cache/actions-runner/current` (osx-arm64)
- Official runner source checkout: `/tmp/runner-v2.336.0` (commit `98aabcd`)

## Agent Preferences

- **Be critical.** Push back with evidence when a plan hides risk or a claim is wrong.
- **Composability is the goal.** Any runner should work with any server. Never introduce protocol divergences.
- **Local CI is mandatory.** After every large chunk of work or task, run `just test-ci` to validate the changes and dogfood the workflow.
- **Drop-in workflows.** Users should be able to run their workflows in local CI unmodified.