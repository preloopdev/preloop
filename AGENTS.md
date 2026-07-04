# Repository Guidelines

## Project Overview

`aksh` is a Rust reimplementation of the GitHub Actions control plane. It speaks the official runner protocol so an unmodified `actions/runner` can register, poll for jobs, execute workflows, and report results locally.

The repo also contains `runner-watch`, a protocol-diff/conformance toolchain for comparing aksh against upstream runner behavior.

## Architecture & Data Flow

### High-level structure

The workspace is split by protocol responsibility rather than by binary:

- `crates/aksh-runner-server` — main HTTP control plane; serves both runner-compatible and native APIs.
- `crates/aksh-gha-parser` — parses workflow YAML, expands matrices/reusables, builds job graphs and runner job payloads.
- `crates/aksh-gha-expressions` — `${{ ... }}` parser/evaluator.
- `crates/aksh-gha-protocol` — shared wire/domain types, secret wrappers, runner/session DTOs, NDJSON events.
- `crates/aksh-runner-client` — CLI for submitting workflows and inspecting runs.
- `crates/aksh-cache` / `crates/aksh-artifacts` — file-backed protocol storage.
- `crates/aksh-conformance` — comparison harnesses/fixtures.
- `crates/runner-watch` — protocol-sync and conformance analysis tooling.

### Protocol surfaces

`crates/aksh-runner-server/src/lib.rs` exposes two surfaces over the same internal state:

- `/_apis/...` and `/runner/server/_apis/...` — Azure DevOps-style endpoints the official runner speaks.
- `/api/v1/...` — native REST + NDJSON projection for local tools and agents.
- `/broker/...`, `/runner/...`, `/session|message|acknowledge` — run-service/broker endpoints used by newer official runner flows.

### Main data flow

1. Workflow is submitted through `aksh-runner-client` or `/api/v1/runs`.
2. `aksh-gha-parser` parses YAML, evaluates triggers/expressions, expands job DAGs/matrices, and builds `AgentJobRequestMessage` payloads.
3. `aksh-runner-server` queues jobs in memory, manages runner registration/sessions, and serves runner-compatible HTTP routes.
4. Official runner polls for work, acquires job payloads, renews leases, uploads logs/timeline updates, and completes jobs.
5. Cache/artifact crates persist file-backed protocol data under state directories.

### State model and patterns

- Default run state is **in-memory** in `aksh-runner-server`; cache/artifacts are file-backed (`docs/architecture.md`).
- `AppState` / `SharedState` wrap mutable server state behind `Arc<Mutex<...>>` plus `Notify`/broadcast channels (`crates/aksh-runner-server/src/lib.rs`).
- Secret handling is explicit via `SecretString`; call `expose()` only at protocol boundaries (`docs/architecture.md`).

## Key Directories

- `crates/` — all Rust workspace crates.
- `crates/aksh-runner-server/src/` — most runner/server logic; `lib.rs` is large and includes routes, state, helpers, and many integration-style tests.
- `crates/runner-watch/src/` — protocol comparison and sync tooling.
- `.github/workflows/` — CI and local dogfood workflows.
- `docs/` — architecture, fidelity gaps, conformance notes, runner-watch plans, diagrams.
- `scripts/` — local E2E/bootstrap helpers, especially port-80 redirect setup.
- `.runner-watch/` — local conformance artifacts, recordings, prompts, reports.
- `fixtures/` — workflow/wire/golden fixtures.
- `logs/e2e/` — local E2E logs produced by helper scripts.

## Development Commands

Prefer the `justfile` for common tasks:

```sh
just build         # cargo build --release -p aksh-runner-server
just build-all
just check
just fmt
just fmt-check
just clippy
just test
just test-ci       # fmt-check + clippy + test
just dogfood       # ./autoresearch.sh
just serve         # cargo run --release -p aksh-runner-server -- serve --listen 127.0.0.1:9090
just submit-dogfood
```

Direct Cargo commands used by the repo/workflows:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets
cargo test --workspace --quiet
cargo run -p aksh-runner-client -- submit -W .github/workflows/dogfood.yml
```

E2E helpers:

```sh
./scripts/e2e-setup.sh --status
sudo ./scripts/e2e-setup.sh
./scripts/e2e-start.sh
./autoresearch.sh
```

## Code Conventions & Common Patterns

### Rust/tooling conventions

- Rust 2021 workspace, toolchain pinned to **1.86** (`rust-toolchain.toml`).
- Workspace lints: `unsafe_code = "forbid"`, `missing_docs = "warn"` (`Cargo.toml`).
- Formatting is `cargo fmt`; clippy is expected to pass in local dogfood.

### Error handling

- Use `anyhow` for top-level command/server startup errors.
- Use structured enums / `thiserror`-style domain errors where appropriate in libraries.
- HTTP handlers in `aksh-runner-server` commonly return `Result<..., ApiError>`.

### Async/server patterns

- Server stack is `tokio` + `axum` + `tower-http` tracing.
- Shared mutable state is centralized rather than heavily abstracted; reuse existing `AppState`/`InnerState` patterns before introducing new layers.
- Route handlers tend to live close to related helper functions and tests in `crates/aksh-runner-server/src/lib.rs`.

### Naming/data patterns

- Protocol/wire types mirror upstream naming and JSON shape; prefer compatibility over local renaming.
- Many DTOs use `serde(rename = ...)` / `rename_all` to preserve runner wire format.
- `runner-watch` code is analysis/reporting oriented; concise string-processing helpers are common there.

### What to preserve when editing

- Treat `/_apis/...` compatibility as the source of truth.
- Keep native `/api/v1/...` behavior a projection of the same internal state, not a separate execution path.
- When changing runner-facing JSON or routes, validate with the **official runner** rather than only unit tests.

## Important Files

- `Cargo.toml` — workspace membership, shared deps, workspace lints.
- `justfile` — canonical local commands.
- `rust-toolchain.toml` — required toolchain/components.
- `README.md` — project purpose and crate map.
- `docs/architecture.md` — best concise architecture reference.
- `docs/fidelity-gap.md` — current protocol limitations and gaps.
- `docs/runner-watch-plan.md` — runner-watch pipeline design.
- `.github/workflows/ci.yml` — GitHub-hosted CI shape and current aksh compatibility notes.
- `.github/workflows/dogfood.yml` — locally runnable self-hosted validation workflow.
- `crates/aksh-runner-server/src/lib.rs` — core server logic, routes, in-memory state, many tests.
- `crates/aksh-runner-server/src/main.rs` — server CLI entry point.
- `crates/runner-watch/src/main.rs` / `compare.rs` — conformance/reporting entry points.
- `scripts/e2e-setup.sh` — port 80 redirect setup for official runner testing.
- `scripts/e2e-start.sh` / `autoresearch.sh` — E2E automation scripts.

## Runtime/Tooling Preferences

- **Use Cargo**, not another package manager.
- Required Rust toolchain: **1.86** with `rustfmt` and `clippy`.
- Runtime stack is Rust/Tokio/Axum; no Node/Bun runtime is required for normal development.
- Official runner E2E on macOS depends on redirecting **port 80 -> 9090** because the runner strips non-default HTTP ports (`scripts/e2e-setup.sh`).
- Local dogfood uses injected workflow vars such as `AKSH_REPO_ROOT` (`.github/workflows/dogfood.yml`).
- `.github/workflows/ci.yml` uses actions like `actions/checkout`; `dogfood.yml` intentionally avoids those for local aksh validation while action-download support is incomplete.

## Testing & QA

### Main checks

Run these before shipping meaningful Rust changes:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets
cargo test --workspace --quiet
```

### Workflow-level validation

Use the real local workflow when validating runner compatibility:

```sh
.github/workflows/dogfood.yml
```

That workflow runs:

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets`
- `cargo test --workspace --quiet`

### Protocol/E2E validation

When changing runner-facing routes, payloads, or lease/session logic:

- Prefer rerunning the real dogfood workflow against the **official runner**.
- Check `logs/e2e/`, `scripts/e2e-start.sh`, and `autoresearch.sh` for local procedures.
- Consult `.runner-watch/conformance/` and `.runner-watch/golden/` for protocol comparison artifacts.

### Test layout notes

- Many server tests are inline in `crates/aksh-runner-server/src/lib.rs`; targeted filtering is practical.
- `runner-watch` and conformance docs under `docs/watcher/` record recent protocol investigations.
- If a workflow passes locally but CI shape differs, compare `.github/workflows/ci.yml` vs `.github/workflows/dogfood.yml` before changing production behavior.

## Agent Interaction Preferences

- **Be critical.** Push back when a plan hides risk, a claim is wrong, or a prioritization doesn't make sense. Don't be a yes-man — question assumptions, challenge decisions, and propose alternatives with evidence. The user values direct, honest critique over agreement.
- **Composability is the goal.** The aksh runner and server must be interchangeable with the official runner and GitHub's control plane. Any runner should work with any server. Never introduce protocol divergences that break this composability.
- **Broker path only.** The AzDO legacy protocol path is deferred. All work targets the modern broker + Twirp results-service protocol (v2.329.0+). GitHub enforces this as the minimum runner version.
- **ARM64 is the local target.** smolvm on Apple Silicon creates ARM64 VMs. ~90% of workflows work natively. x86 emulation via Rosetta is blocked on smolvm/libkrun limitations (see `docs/runner/13-x86-emulation-research.md`).
