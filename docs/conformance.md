# Conformance

aksh treats compatibility as a test artifact, not an assertion in prose.

## Current State (2026-06-26)

The official `actions/runner` v2.322.0 completes the full lifecycle against aksh:
configure → session → message → execute → report completion. 62 workspace tests pass.

**Verified with real runner:**
- Registration (GHES-style org URL, `RemoteAuth` header)
- ConnectionData (18 service GUIDs, org-prefix routing)
- Session creation (AES key exchange)
- Message delivery (encrypted `TaskAgentMessage`)
- Job execution (runner runs steps, reports completion)
- Ephemeral mode (cleanup after job)

**Not yet verified:**
- Timeline/log endpoint fidelity (worker reports "Failed")
- Cache/artifact v2 protocols
- Action download (stub endpoint only)

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
