# Architecture

Preloop is split by protocol responsibility rather than by binary:

- `preloop-gha-protocol` owns versioned wire/domain types. Anything sent to a
  runner or emitted to an agent should pass through this crate.
- `preloop-gha-parser` owns workflow YAML normalization, trigger matching, job
  graph construction, and matrix expansion.
- `preloop-gha-expressions` owns expression parsing and evaluation.
- `preloop-runner-server` owns HTTP routes, queueing, cancellation, reruns, and
  runner sessions.
- `preloop-runner-client` is the local submission/inspection CLI.
- `preloop-cache` and `preloop-artifacts` own file-backed protocol storage.
- `preloop-conformance` owns comparisons against the pinned
  `ChristopherHX/runner.server` reference.

## State Model

The default server uses an in-memory run queue and file-backed cache/artifact
stores under `.preloop/`. This keeps the local feedback loop fast and makes the
initial protocol behavior easy to inspect. Durable run state should be added
behind an explicit repository trait before adopting `sqlx` or another database
layer.

## Secrets

Secrets use `SecretString` in `preloop-gha-protocol`. It redacts `Debug`,
`Display`, and serialized output. Code that needs the raw payload must call
`expose()` explicitly at a protocol boundary.

## Compatibility Position

This implementation is not yet a proven byte-for-byte replacement for upstream
`runner.server`. It is structured to become one:

- runner-compatible routes are isolated in `preloop-runner-server`;
- protocol DTOs are versioned in `preloop-gha-protocol`;
- upstream workflow fixtures are checked into `fixtures/upstream-workflows`;
- conformance commands are documented in `docs/conformance.md`.

Known staged areas:

- expression functions cover common local CI paths but not the full GitHub
  Actions expression language;
- reusable workflows parse, but full call graph execution and secret inheritance
  still need conformance-backed implementation;
- runner protocol endpoints are skeletal and must be expanded with golden
  fixtures from a real `Runner.Listener`;
- cache and artifact stores are local and functional, but HTTP protocol parity
  needs endpoint-level golden tests;
- libkrun integration is documented as the final gate and not implemented in
  this repository yet.

