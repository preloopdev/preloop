# aksh — GitHub Actions Control Plane

**aksh** is a faithful Rust reimplementation of the GitHub Actions control plane
([`ChristopherHX/runner.server`](https://github.com/ChristopherHX/runner.server)). It
speaks the official runner protocol so the unmodified `actions/runner` (`Runner.Listener`)
can register, poll for jobs, execute, and report — without GitHub-hosted minutes.

aksh is **execution-agnostic**: it doesn't care whether runners live in containers, VMs,
microVMs, or bare processes. The runner connects to aksh; aksh feeds it jobs.

**[Preloop](https://github.com/preloop/preloop)** is a local CI product that combines
aksh with libkrun microVM runner hosts. aksh is Preloop's control plane. But aksh is
independently usable — anyone can `cargo install aksh` and point their own runners at it.

## Crates

- `aksh-server`: host-side HTTP service, runner-compatible APIs, run queue, cancellation,
  reruns, NDJSON event stream.
- `aksh-runner-client`: CLI equivalent to `Runner.Client` for submitting workflows and
  inspecting runs.
- `aksh-parser`: typed GitHub Actions workflow parsing, trigger matching, job graph
  construction, matrix expansion.
- `aksh-gha-expressions`: expression parser/evaluator for `${{ }}` in workflows, matrices,
  `if`, contexts, and outputs.
- `aksh-protocol`: versioned domain and wire models (AzDO wire DTOs, `SecretString`,
  runner session DTOs, NDJSON events).
- `aksh-cache`: local cache service compatible with the runner cache protocol shape.
- `aksh-artifacts`: local artifact/container service compatible with runner
  upload/download behavior.
- `aksh-conformance`: fixtures and harnesses comparing aksh behavior with upstream
  `runner.server`.
- `aksh-runner`: Rust reimplementation of the GitHub Actions runner (Listener + Worker),
  faithful to `actions/runner` v2.335.1. Registers, polls, executes workflows, reports results.

## Why aksh Exists

aksh keeps the upstream runner-server contract where it matters, but adds features useful
for anyone building or testing GitHub Actions workflows outside GitHub:

- single native Rust host process with a small distribution footprint
- execution-agnostic: works with any runner substrate (containers, VMs, microVMs, bare)
- NDJSON event output for AI agents and developer tooling
- redaction-safe secret types in the protocol layer
- pluggable backend traits (`RunnerProvider`, `RunStore`, `AuthProvider`, `SecretStore`)
- local cache and artifact stores that work without GitHub-hosted infrastructure
- runner-compatible HTTP surfaces compatible with the official `Runner.Listener`

## Current Status

**As of 2026-06-29, aksh is tracked by runner-watch against the official `actions/runner` v2.335.1 protocol surface.**

aksh currently supports the core runner lifecycle:

1. Registers against aksh (GHES-style org URL)
2. Creates encrypted sessions (AES key exchange)
3. Receives and decrypts job messages
4. Executes jobs and reports completion
5. Supports `needs` DAG, matrix strategies, trigger matching, expression evaluation

Workspace tests pass via `cargo test --workspace`. runner-watch records protocol-sync artifacts under `.runner-watch/`; remaining fidelity work is tracked in [docs/fidelity-gap.md](docs/fidelity-gap.md).

## Toolchain

The workspace targets Rust 1.86 or newer and uses `tokio`, `axum`, `serde_yaml`, `tracing`,
`thiserror`, `anyhow`, and `clap`.

```sh
cargo fmt --all
cargo test --workspace
cargo run -p aksh-server -- serve --listen 127.0.0.1:8080
cargo run -p aksh-runner-client -- submit --workflow .github/workflows/ci.yml --event push
```

## Upstream Reference

The conformance target is `ChristopherHX/runner.server` at commit
`992ccbbbf9afcde477c38c316e053b1af457ad40` unless `AKSH_UPSTREAM_RUNNER_SERVER_REF` is
set. See [docs/reference/runner-server.md](docs/reference/runner-server.md) for the mapped
surface and deliberate differences.

## Architecture

aksh exposes two protocol surfaces simultaneously:

1. **Runner-compatible `_apis/...`** — the AzDO protocol the official runner speaks
   (encrypted messages, timeline, logs, OAuth). This is the source of truth.
2. **Agent-friendly `/api/v1/...`** — native REST + NDJSON for AI agents, CLIs, and
   developer tools. A projection of the same internal state.

Both read from and write to the same state; the native surface is strictly additive.
See [docs/architecture.md](docs/architecture.md) for the design.
