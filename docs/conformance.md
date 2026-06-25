# Conformance

aksh treats compatibility as a test artifact, not an assertion in prose.

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
  --preloop target/debug/aksh-runner-client \
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
