# Aksh Runner and Control Plane

## Role in Preloop

Aksh is the GitHub Actions-compatible brain of Preloop. It should own workflow semantics, runner-compatible protocol behavior, and job execution orchestration, while Preloop owns VM lifecycle, host isolation, policy, cache, artifacts, telemetry, and product UX.

Aksh is runtime-agnostic: it emits standard runner job payloads and rewrites the
service URLs (cache/results/runtime/log) to point at itself regardless of which
microVM executor runs the job. The executor is chosen per tier behind the
`VmProvider` seam (see
[doc 14](14_runtime_tiers_and_portable_handoff.md)).

Earlier plans treated `ChristopherHX/runner.server` as the local control-plane core. Given the current `runner-rust` branch, the better architecture is:

```text
Aksh native path          = primary Preloop engine
Official actions/runner  = conformance oracle and fallback
runner.server            = conformance oracle and local-service behavior reference
```

## Current archive observations

The uploaded branch already has a good Rust workspace shape:

```text
crates/aksh-artifacts
crates/aksh-cache
crates/aksh-conformance
crates/aksh-gha-expressions
crates/aksh-gha-parser
crates/aksh-gha-protocol
crates/aksh-runner
crates/aksh-runner-client
crates/aksh-runner-server
crates/runner-watch
```

The workspace also sets important defaults:

```toml
[workspace.lints.rust]
unsafe_code = "forbid"
missing_docs = "warn"
```

That is the right posture. Keep unsafe out of Aksh; isolate unsafe to the eventual libkrun FFI crate.

## Runner architecture

The existing `docs/runner/00-architecture.md` describes `aksh-runner` as a Rust reimplementation of the official GitHub Actions runner v2.335.1. The key design is the two-process Listener/Worker split:

```text
aksh-runner run (Listener)
  |
  +-- token/session setup
  +-- long-poll loop
  +-- on job message:
        |
        +-- spawn aksh-runner worker child process
              - reads job from stdin NDJSON
              - sets up workspace, contexts, env
              - executes steps sequentially
              - reports results to server
              - exits
```

This split should remain. It gives you crash isolation, clearer cancellation, and closer behavioral parity with the official runner.

## Control plane responsibilities

The Aksh control plane should eventually provide:

- workflow submission,
- webhook ingestion,
- workflow YAML parsing,
- trigger matching,
- expression evaluation,
- matrix expansion,
- `needs` DAG scheduling,
- runner registration,
- session creation,
- job leasing,
- message polling,
- timeline/log/result APIs,
- action download metadata,
- cache/artifact APIs,
- cancellation and reruns,
- check-run lifecycle integration,
- NDJSON event projection for agents,
- and durable state for self-hosted/managed modes.

## Protocol surfaces

Maintain explicit protocol boundaries:


| Surface                         | Purpose                                       | Stability requirement    |
| ------------------------------- | --------------------------------------------- | ------------------------ |
| `_apis/...` AzDO-style protocol | Local Aksh and GHES-like runner compatibility | Golden-tested            |
| Broker/run-service protocol     | GitHub.com-style runner compatibility         | Golden-tested            |
| `/api/v1/...` native REST       | CLI, local tools, agent UI                    | Versioned                |
| NDJSON event stream             | Agents/MCP/TUI                                | Versioned and documented |
| GitHub App webhooks/checks      | Managed/self-hosted CI                        | Idempotent and secure    |


Do not let the native API leak into the runner protocol implementation. Runner-facing behavior should be validated with real runner fixtures.

## Immediate refactor target

The current server should move from large shared state structures toward traits. This is required before self-hosted and managed CI.

Recommended store traits:

```rust
trait RunStore { /* runs, jobs, conclusions, attempts */ }
trait RunnerStore { /* runner registrations, labels, keys */ }
trait SessionStore { /* sessions, leases, polling state */ }
trait JobLeaseStore { /* acquire, renew, complete, cancel */ }
trait TimelineStore { /* job/step timeline records */ }
trait LogStore { /* live logs, finalized logs, masking state */ }
trait ArtifactStore { /* upload/download metadata */ }
trait CacheStore { /* cache keys, restore keys, provenance */ }
trait WebhookDeliveryStore { /* idempotency and replay protection */ }
```

Local mode can use in-memory + filesystem stores. Self-hosted and managed modes need SQLite/Postgres/object storage implementations.

## Aksh runner responsibilities

The runner must implement or faithfully emulate:

- shell `run:` steps,
- Node.js actions,
- composite actions,
- Docker/container actions,
- `GITHUB_ENV`, `GITHUB_OUTPUT`, `GITHUB_PATH`, `GITHUB_STATE`, `GITHUB_STEP_SUMMARY`,
- workflow command parsing,
- secret masking,
- annotations,
- problem matchers,
- pre/post action steps,
- cancellation,
- timeout handling,
- job/step conclusions,
- `continue-on-error`,
- `if:` conditions,
- and log streaming.

Container jobs and service containers are not optional if Preloop is meant to run real CI.

## Aksh inside Preloop

The intended run topology should be:

```text
host: preloopd + aksh-control
        |
        | vsock/proxy/local bridge
        v
smolvm Linux microVM (Firecracker on the scale tier)
  +-- preloop-guest-agent
  +-- aksh-runner listener/worker
  +-- private Docker/buildkit services
  +-- /workspace overlay
```

For the first integration milestone, keep Aksh control on the host and run `aksh-runner` inside the VM. Later, selected local-only modes may run both inside the VM, but managed mode should keep trusted control-plane state outside untrusted job VMs.

Preloop consumes smolvm as the microVM substrate; it does not reimplement the VM
layer. The same guest agent and `aksh-runner` run inside whichever executor the
`VmProvider` seam selects (SmolvmProvider for Local/smolvm-KVM, FirecrackerProvider
for the scale tier), so control-plane behavior is identical across tiers.



## Acceptance gates

Before Aksh is the default Preloop engine:

- P0 conformance must pass against the official runner or runner.server oracle.
- Basic success, failure, matrix, needs, outputs, and logs must work inside a smolvm microVM (and the Firecracker tier behind the same seam).
- Cache/artifact behavior must either pass or be clearly marked unsupported.
- Docker/service gaps must fail loudly, not green silently.
- Cancellation must kill the full process tree.
- Secrets must never print raw through Debug, Display, serialized output, logs, or artifact metadata.

