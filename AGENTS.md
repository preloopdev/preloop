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
- **Store backends**: the `Store` trait (`store.rs`, async, object-safe) is the only surface the server sees; backends are SQLite (`store.rs`, default, `<state_dir>/aksh.db`) and Postgres (`store_pg.rs`), selected via `AKSH_STORE_URL` (`sqlite://<path>` / bare path / `postgres://…`). Both are single-writer: one connection behind a mutex. Two servers on the same SQLite file (or same PG database) still diverge in-memory — the DB is a restart source, not a shared bus.
- **Store is best-effort**: in-memory state is the source of truth, the database is a restart source. Store failures are logged; the affected event is still broadcast (see `state.rs::emit`). Per-backend `MIGRATIONS` is the schema source of truth (SQLite: `PRAGMA user_version`; PG: `schema_migrations` version table).
- **Encryption-at-rest is obfuscation, not security**: `<state_dir>/hmac-key.bin` and `aksh.db` sit in the same directory; the store key is HKDF-derived from the JWT HMAC key with domain separation. It stops a stolen DB file, not a compromised state dir. Key loss = unbootable state. The envelope is backend-independent (sealed blobs), so it applies to Postgres rows too — for remote PG, rely on TLS + DB auth instead.

## Important Files

- `docs/architecture.md` — crate map + module map
- `docs/store-known-issues.md` — durable-store follow-ups (perf, retention, layering)
- `docs/fidelity-gap.md` — protocol gaps and conformance status
- `docs/preloop-performance-engineering.md` — perf campaign record: harness, measurements, rejected ideas, and the cold-start blocker
- `docs/smolvm-packed-socket-handoff.md` — fork implementation brief and upstream issue for packed-machine socket publication
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

