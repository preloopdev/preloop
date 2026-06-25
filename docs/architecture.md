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

This implementation is not yet a proven byte-for-byte replacement for upstream
`runner.server`. It is structured to become one:

- runner-compatible routes are isolated in `aksh-server`;
- protocol DTOs are versioned in `aksh-protocol`;
- upstream workflow fixtures are checked into `fixtures/upstream-workflows`;
- conformance commands are documented in `docs/conformance.md`.

Implemented in the current Rust slice:

- all in-scope upstream GitHub Actions workflow fixtures parse and expand;
- local `action.yml` / `action.yaml` metadata parses for composite, Node, and
  Docker action definitions;
- local reusable workflow call jobs can be expanded when the caller supplies the
  referenced workflow YAML;
- cache and artifact stores have HTTP endpoint coverage, including
  runner-shaped cache reserve/upload/commit/lookup routes;
- expression evaluation covers common boolean logic, equality, comparisons,
  status helpers, JSON conversion, `format`, `contains`, `startsWith`,
  `endsWith`, `join`, and a local-safe `hashFiles` placeholder.

Known staged areas:

- expression functions cover common local CI paths but still need complete
  GitHub Actions coercion and object-filter semantics;
- reusable workflow expansion handles local call jobs, but input mapping,
  outputs, nested call graphs, and secret inheritance still need
  conformance-backed execution tests;
- runner protocol endpoints need golden fixtures captured from a real
  `Runner.Listener`;
- provider integration is documented as the final gate and not implemented in
  this repository yet.
