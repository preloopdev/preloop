# Preloop Runner Server

Preloop is a macOS-first, Linux-capable local CI control plane for GitHub Actions-style jobs. This repository reimplements the host-side behavior of [`ChristopherHX/runner.server`](https://github.com/ChristopherHX/runner.server) in Rust so a `Runner.Listener` inside an ephemeral Preloop libkrun Linux microVM can register, poll, execute, and report jobs without consuming GitHub-hosted runner minutes.

The implementation is intentionally split into crates that match durable product boundaries:

- `preloop-runner-server`: host-side HTTP service, runner-compatible APIs, run queue, cancellation, reruns, NDJSON event stream.
- `preloop-runner-client`: CLI equivalent to `Runner.Client` for submitting workflows/events/payloads and inspecting runs.
- `preloop-gha-parser`: typed GitHub Actions workflow parsing, trigger matching, job graph construction, matrix expansion.
- `preloop-gha-expressions`: expression parser/evaluator used by workflows, matrices, `if`, contexts, and outputs.
- `preloop-gha-protocol`: versioned domain and wire models, including redaction-safe secrets and runner session DTOs.
- `preloop-cache`: local cache service compatible with the runner cache protocol shape.
- `preloop-artifacts`: local artifact/container service compatible with runner upload/download behavior.
- `preloop-conformance`: fixtures and harnesses that compare Preloop behavior with upstream `runner.server`.

## Current Status

This is a Rust implementation scaffold with the core parser, expression evaluator, server, client, and conformance harness under active construction. The workspace forbids unsafe code by default. Protocol surfaces are versioned in `preloop-gha-protocol` and should gain golden fixtures before behavior is treated as stable.

## Toolchain

The workspace targets Rust 1.86 or newer and uses `tokio`, `axum`, `serde_yaml`, `tracing`, `thiserror`, `anyhow`, and `clap`.

```sh
cargo fmt --all
cargo test --workspace
cargo run -p preloop-runner-server -- serve --listen 127.0.0.1:8080
cargo run -p preloop-runner-client -- submit --workflow .github/workflows/ci.yml --event push
```

## Upstream Reference

The conformance target is `ChristopherHX/runner.server` at commit `992ccbbbf9afcde477c38c316e053b1af457ad40` unless `PRELOOP_UPSTREAM_RUNNER_SERVER_REF` is set. See [docs/reference/runner-server.md](docs/reference/runner-server.md) for the mapped surface and deliberate Preloop differences.
