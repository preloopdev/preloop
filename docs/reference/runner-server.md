# ChristopherHX runner.server Reference

Reference repository: <https://github.com/ChristopherHX/runner.server>

Pinned reference commit used during this implementation pass:

```text
992ccbbbf9afcde477c38c316e053b1af457ad40
```

## Compatibility Scope

aksh mirrors the parts of `runner.server` required to run GitHub Actions locally through an official `Runner.Listener` process:

- CLI workflow submission equivalent to `Runner.Client`.
- Workflow/event/payload ingestion.
- GitHub Actions YAML parsing, triggers, expressions, matrices, `needs`, contexts, env propagation, job outputs, and step outputs.
- Runner registration, configuration, session creation, job message polling, job completion, timeline/log upload, annotations, cache, artifacts, cancellation, and reruns.
- Local-only secrets policy with redaction-safe values.
- Machine-readable NDJSON event feed for AI agents and developer tools.

Azure Pipelines support from upstream is out of initial scope. The Rust domain model keeps protocol crates independent enough to add it later without mixing it with GitHub Actions semantics.

## Upstream Files Mapped

- `src/Runner.Client/*` maps to `aksh-runner-client`.
- `src/Runner.Server/Controllers/*` maps to `aksh-server` plus `aksh-cache` and `aksh-artifacts`.
- `src/Runner.Server/Models/*` maps to `aksh-protocol`.
- `src/Runner.Server/Services/*` maps to parser, expression, secrets, and run orchestration modules.
- `testworkflows/*` maps to `fixtures/upstream-workflows` and `aksh-conformance`.

## Deliberate aksh Choices

- aksh stores durable run state only behind explicit repository traits. The default server starts with an in-memory store for fast local feedback.
- Secret values are represented by a redaction-safe type whose `Debug`, `Display`, and serialization output never expose the secret payload.
- Runner protocol DTOs are versioned and isolated from internal domain models.
- All long-running async work receives an explicit cancellation token and shutdown path.
- NDJSON events are a first-class output, not a side effect of human log formatting.
