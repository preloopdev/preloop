# Architecture

aksh is split by protocol responsibility rather than by binary:

- `aksh-protocol` owns versioned wire/domain types. Anything sent to a
  runner or emitted to an agent passes through this crate. Includes AzDO wire
  DTOs, `SecretString`, NDJSON events, and session crypto.
- `aksh-parser` owns workflow YAML normalization, trigger matching, job graph
  construction, matrix expansion, and expression evaluation.
- `aksh-gha-expressions` owns expression parsing and evaluation (the core
  `${{ }}` engine).
- `aksh-server` owns HTTP routes, queueing, cancellation, reruns, and
  runner sessions. Exposes two protocol surfaces:
  - `_apis/...` — the AzDO protocol the official runner speaks (source of truth)
  - `/api/v1/...` — native REST + NDJSON for agents and tools (read projection)
- `aksh-runner-client` is the local submission/inspection CLI.
- `aksh-cache` and `aksh-artifacts` own file-backed protocol storage.
- `aksh-conformance` owns comparisons against the pinned
  `ChristopherHX/runner.server` reference.

## Pluggable backends

aksh is execution-agnostic. The only thing that differs between runner hosts
is how a runner instance is created and destroyed. This is modeled as the
`RunnerProvider` trait in the orchestrator layer:

- **`RunStore`** — in-memory (local) or `sqlx` (server).
- **`AuthProvider`** — loopback-trust (local) or OAuth + mTLS (server).
- **`RunnerProvider`** — creates/destroys runners (process, container, libkrun,
  cloud VM, k8s pod, bare BYO). Optional — aksh works with external runners.

See [fidelity-gap.md §4](fidelity-gap.md) for the full design.

## State Model

The default server uses an in-memory run queue and file-backed cache/artifact
stores under `.aksh/`. This keeps the local feedback loop fast and makes the
initial protocol behavior easy to inspect. Durable run state should be added
behind an explicit repository trait before adopting `sqlx` or another database
layer.

## Secrets

Secrets use `SecretString` in `aksh-protocol`. It redacts `Debug`,
`Display`, and serialized output. Code that needs the raw payload must call
`expose()` explicitly at a protocol boundary.

## Compatibility Position

As of 2026-06-26, aksh is a proven working control plane for the official `actions/runner`.
The runner completes the full lifecycle: configure → session → message → execute → complete.

Implemented and verified with the real `Runner.Listener` v2.322.0:

- Full AzDO lifecycle routes (connectionData, AgentPools, Agent, AgentSession, Message,
  AgentRequest, Timeline, Logfiles, FinishJob, ActionDownloadInfo)
- GitHub-compatible registration (`/api/v3/actions/runner-registration` with `RemoteAuth`)
- GHES org-prefix routing (`/:org/_apis/...` for all lifecycle endpoints)
- AES session key exchange (unencrypted mode — RSA wrapping planned)
- Encrypted `TaskAgentMessage` delivery with message ack
- Full `AgentJobRequestMessage` with plan, requestId, system context, steps
- `needs` DAG scheduling with dependency-gated dispatch and outputs propagation
- Trigger matching (branches/tags/paths/types/schedule/workflow_dispatch)
- Matrix expansion with IndexMap order preservation and GitHub name format
- Expression evaluation wired into job builder
- `fail-fast` / `max-parallel` matrix strategy support

Known limitations:

- Worker reports job as "Failed" (timeline/log endpoint fidelity gap)
- Session AES key sent unencrypted (RSA-OAEP wrapping of runner's public key TODO)
- Cache/artifact endpoints are in-memory stubs; v2 blob protocols not implemented
- Conformance harness needs golden tests, fuzz targets, wire capture/replay
- Expression engine lacks bracket access, object-filter, format escaping
