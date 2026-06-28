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

**Verified with real `Runner.Listener` v2.322.0 (2026-06-26):**
- Registration via `/api/v3/actions/runner-registration` (GHES-style)
- ConnectionData with 18 service GUIDs and GHES org-prefix routing
- AgentPools, Agent lookup/registration, AgentSession creation
- Message delivery with encrypted `TaskAgentMessage`
- AgentRequest renewal with `lockedUntil` response
- Full job lifecycle: configure → session → message → execute → complete

Azure Pipelines support from upstream is out of initial scope. The Rust domain model keeps protocol crates independent enough to add it later without mixing it with GitHub Actions semantics.
