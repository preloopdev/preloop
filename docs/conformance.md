# Conformance

aksh treats compatibility as a test artifact, not an assertion in prose.

## Current State (2026-07-10)

The official `actions/runner` v2.335.1 completes the broker lifecycle against aksh:
configure → session → message → acquire → execute → report completion. The current
workspace runner test suite passes.

**Verified with real GitHub service (scenario 61):**
- Three independent GitHub-ephemeral runners receive the three jobs.
- `actions/cache@v4` v2 save/restore works across runner instances.
- Cache `CreateCacheEntry`, Azure Blob upload/download, `FinalizeCacheEntryUpload`,
  and `GetCacheEntryDownloadURL` all complete successfully.
- Runner-side ephemeral cleanup and subpath action-resolution fixes are committed in
  `ab77a23` and `32ee008` respectively.

**Still server-side / intentionally separate:**
- aksh's local server CacheService/ArtifactService v2 blob endpoints remain a separate
  implementation gap; the scenario above exercises the Rust runner against GitHub's
  service, not the local control plane.
- Timeline/log payload fidelity remains partial.

## Fixture Expansion

```sh
cargo run -p aksh-conformance -- expand-fixtures
```

This parses GitHub Actions YAML fixtures copied from the pinned upstream
`ChristopherHX/runner.server` commit and expands jobs/matrices with aksh's
parser. Azure Pipelines fixtures are skipped until that feature is explicitly in
scope.

## Command Comparison

```sh
cargo run -p aksh-conformance -- compare-command \
  --upstream /path/to/Runner.Client \
  --aksh target/debug/aksh-runner-client \
  -- -W fixtures/upstream-workflows/matrixtest.yml --event push
```

The harness runs both commands with the same arguments and compares stdout and
success/failure status. Higher-level comparisons should normalize volatile
fields before asserting equality.

## Provider Integration Gate

```sh
cargo run -p aksh-conformance -- libkrun-plan
```

The final compatibility gate uses a real `Runner.Listener` inside a provider
host (container, microVM, or bare process). The test must compare the reference
server and aksh for job dispatch, contexts, logs, annotations, cache, artifacts,
outputs, failure states, cancellation, and reruns.

## Planned Conformance Harness

The conformance harness should grow into a real differential tester:

- `record` — drive upstream `runner.server` over each fixture, capturing wire
  traffic and final state to `fixtures/wire/<case>/`.
- `expand` — our parser/evaluator over each fixture → expanded jobs + contextData.
- `compare` — assert our expansion/messages/timeline/cache/artifact responses
  match the recorded upstream, with a documented normalizer for volatile fields.
- `replay` — feed recorded upstream `AgentJobRequestMessage`s to our DTOs and back.

Test taxonomy:
- **Golden tests** — expansion, contexts, message bodies, timeline sequences.
- **Property tests** — expression eval + matrix expansion invariants.
- **Protocol-compat tests** — DTO round-trips vs captured wire JSON.
- **Fuzz tests** — `parse_workflow` + expression lexer/parser (`cargo-fuzz`).
- **Integration** — real `Runner.Listener` against aksh (verified ✅).
