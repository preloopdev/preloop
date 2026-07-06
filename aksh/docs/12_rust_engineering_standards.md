# Rust Engineering Standards

## Goal

Preloop and Aksh should be idiomatic, testable, and safe Rust systems. This matters because the project is security-sensitive and long-running: it will supervise untrusted code, runner protocols, microVMs, logs, secrets, caches, and external APIs.

## Workspace structure

Keep crates focused by responsibility:

```text
aksh-core              typed IDs, domain models, state machines
aksh-gha-protocol      runner-compatible DTOs and wire types
aksh-gha-parser        YAML normalization and workflow graph
aksh-gha-expressions   expression parser/evaluator
aksh-runner            Listener/Worker
aksh-runner-server     control-plane HTTP routes
aksh-runner-client     CLI
aksh-cache             cache protocol/store
aksh-artifacts         artifact protocol/store
aksh-conformance       differential test harness
preloop-krun           libkrun FFI/runtime
preloop-guest          static guest agent
preloop-policy         trust tiers and enforcement
preloop-worker         self-hosted/managed worker daemon
preloop-telemetry      NDJSON/OpenTelemetry/tracing
```

Avoid god crates. Especially avoid letting the server crate own all domain logic.

## Unsafe policy

- `unsafe_code = "forbid"` for Aksh/control-plane crates.
- Unsafe only allowed in `preloop-krun` FFI boundary if needed.
- Every unsafe block requires a safety comment and a focused review.
- Prefer existing safe wrappers where they do not hide critical libkrun knobs.

## Typed IDs and state machines

Use typed IDs instead of strings:

```rust
struct RunId(Uuid);
struct JobId(String);
struct StepId(String);
struct VmId(Uuid);
struct TenantId(Uuid);
struct RunnerId(i64);
struct CacheKey(String);
```

Model states explicitly:

```rust
enum JobState {
    Queued,
    Leased { runner_id: RunnerId, lease_until: DateTime<Utc> },
    Running,
    Canceling,
    Completed { conclusion: Conclusion },
}
```

This prevents entire classes of stringly typed bugs.

## Error handling

Recommended boundaries:

- Libraries return domain-specific `thiserror` errors.
- Binaries may use `anyhow` at the top level.
- HTTP handlers map errors to structured API errors.
- Agent NDJSON emits classified errors.
- Do not log secret-containing errors.

Example:

```rust
#[derive(thiserror::Error, Debug)]
pub enum SchedulerError {
    #[error("no runner matches labels: {labels:?}")]
    NoRunner { labels: Vec<String> },

    #[error("job {job_id} lease expired")]
    LeaseExpired { job_id: JobId },

    #[error("workflow feature unsupported: {feature}")]
    Unsupported { feature: String },
}
```

## Async lifecycle rules

Every long-running task needs:

- a cancellation token,
- a shutdown path,
- bounded channels,
- task naming/tracing spans,
- timeout handling,
- and a parent supervisor.

Avoid orphan tasks.

For external processes:

- spawn in process groups,
- kill tree on cancellation,
- collect exit status,
- stream logs live,
- enforce stdout/stderr size limits,
- and reap on parent shutdown.

## Secrets

Rules:

- Secret types do not implement raw `Debug`, `Display`, or normal serialization.
- Raw exposure requires an explicit method such as `expose_secret()`.
- Redaction must happen before logs/artifacts leave the trusted boundary.
- Tests should assert that known secrets do not appear in logs, errors, JSON, or debug output.

## Protocol modeling

Runner-facing protocol structs should be:

- versioned,
- serde-tested against captured fixtures,
- roundtrip-tested,
- protected from accidental field renaming,
- and stored with golden files.

Do not casually change wire structs to satisfy internal ergonomics.

## Observability

Use `tracing` everywhere.

Standard fields:

```text
run_id
job_id
step_id
attempt
vm_id
tenant_id
repo
workflow
engine
trust_tier
```

Emit both:

- developer-readable logs,
- and machine-readable NDJSON events.

For managed mode, add OpenTelemetry traces and metrics.

## Persistence

Hide storage behind traits.

Local mode:

```text
in-memory + SQLite/filesystem
```

Self-hosted/managed:

```text
Postgres + object storage
```

All mutating APIs should have idempotency keys where they can be retried:

- webhook delivery,
- job completion,
- artifact upload finalize,
- cache reserve/save,
- check-run updates.

## Testing standards

Required test classes:

- unit tests for parsers and state machines,
- golden tests for protocol fixtures,
- property tests for expression/matrix evaluation,
- fuzz tests for workflow parsing and expression parsing,
- integration tests against official runner and runner.server,
- libkrun smoke tests,
- Docker/service tests,
- malicious workflow corpus,
- cache/artifact poisoning tests,
- secret redaction tests.

## CI gates

Preloop's own CI should run:

```text
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test -p aksh-conformance -- p0
cargo deny check   # or equivalent dependency audit
cargo audit        # if adopted
```

For libkrun-specific tests, use a separate hardware-gated workflow.

## Feature flags

Use feature flags sparingly and clearly:

```toml
[features]
default = ["local"]
local = []
managed = []
libkrun = []
microsandbox = []
github-app = []
official-runner-oracle = []
```

Avoid using feature flags to hide broken behavior. Unsupported features should fail loudly at runtime when encountered.

## API stability

Version external APIs:

```text
/api/v1/runs
/api/v1/events
/api/v1/cache
/api/v1/artifacts
NDJSON schema v1
```

Keep internal Rust APIs flexible, but golden-test external formats.

## Code review checklist

Before merging security-sensitive code:

- Does it expose secrets through logs, errors, Debug, Display, or JSON?
- Does it trust user-controlled paths?
- Does it follow symlinks unexpectedly?
- Does it create unbounded memory/disk/log growth?
- Does it have cancellation and cleanup?
- Does it handle retries idempotently?
- Is behavior covered by conformance or a focused test?
- Does it behave differently in local vs managed mode?
- Is that difference explicit in policy/run metadata?
