# Cache, Artifacts, and Toolchains

## Goal

Fast CI is mostly a cache problem, not a VM boot problem. Preloop should make action, toolchain, package, OCI, cache, and artifact storage first-class subsystems with security-aware provenance.

## Cache layers

| Store | Contents | Policy focus |
|---|---|---|
| Action store | downloaded actions by owner/repo/ref -> resolved SHA | pinning, provenance, offline use |
| OCI layer store | rootfs images, container actions, service images | content addressing, arch separation |
| Tool cache | Node, Python, Go, Rust, JDK, browsers | OS/arch/version/source |
| Package cache | npm/pnpm/yarn, Cargo, pip/uv, Go, Gradle, Maven | write-on-success, quarantine |
| Workflow cache | actions/cache-compatible keys and restore keys | GitHub semantics |
| Artifact store | uploads, summaries, screenshots, videos | quotas, expiry, masking |
| Log store | raw logs, masked logs, timeline mapping | streaming, redaction |
| Repro bundle store | plan/env/vm/cache/source metadata | debugging and support |

## Trust-aware namespacing

Cache keys must include trust and provenance dimensions.

Recommended namespace:

```text
tenant_id / repo_id / trust_tier / workflow / job / arch / image_digest / cache_kind / key
```

Metadata:

```json
{
  "tenant": "t_123",
  "repo": "owner/name",
  "trust_tier": "internal-pr",
  "workflow": "ci.yml",
  "job": "test",
  "arch": "arm64",
  "guest_image": "sha256:...",
  "source_sha": "abc123",
  "lockfiles": {
    "package-lock.json": "sha256:...",
    "Cargo.lock": "sha256:..."
  },
  "created_by_run": "run_123",
  "write_policy": "quarantine",
  "promoted": false
}
```

## Cache write policy

| Trust tier | Cache read | Cache write |
|---|---|---|
| local-dev | yes | yes |
| agent-local | yes | quarantine failed writes |
| trusted-branch | yes | write-on-success |
| internal-pr | base + branch-safe | quarantine |
| untrusted-fork-pr | base/public read-only | disabled |
| managed-untrusted | tenant/repo scoped | quarantine or disabled |

## Local cache ergonomics

Built-in profiles:

```text
preloop run --profile node
preloop run --profile rust
preloop run --profile python
preloop run --profile go
preloop run --profile java-gradle
preloop run --profile browser-tests
```

Example mapping:

| Ecosystem | Paths |
|---|---|
| Node | npm cache, pnpm store, Yarn cache, optional node_modules policy |
| Rust | cargo registry, cargo git, target cache policy |
| Python | pip, uv, poetry, virtualenv policy |
| Go | GOMODCACHE, GOCACHE |
| Java | Gradle, Maven |
| Browser tests | Playwright/Cypress browser cache, fonts |

## Artifact behavior

Artifacts need:

- local and durable storage backends,
- upload/download protocol compatibility,
- expiry and quota policy,
- path traversal prevention,
- masking before display,
- content hash metadata,
- per-run association,
- and tenant/repo isolation for managed mode.

Artifact path rules:

```text
- normalize paths before upload
- reject absolute paths unless explicitly allowed
- reject traversal outside workspace/artifact root
- store original requested path separately from normalized storage path
- mask secrets in metadata and previews
```

## Log storage

Logs should be stored in three forms:

1. raw internal log stream,
2. masked log stream for users/agents,
3. normalized conformance log stream for diffs.

Live log requirements:

- preserve step order,
- preserve stdout/stderr ordering where feasible,
- stream before step completion,
- emit annotations as structured events,
- retain enough raw data for debugging,
- never expose raw secrets through agent-visible APIs.

## Repro bundle

Every failed run should be able to emit:

```text
.preloop/runs/<run-id>/
  run.json
  workflow-expanded.json
  event.json
  env.redacted.json
  contexts.redacted.json
  vm-spec.json
  policy.json
  cache-manifest.json
  artifact-manifest.json
  logs.masked.ndjson
  annotations.json
  failure.json
  retry-plan.json
  repro.sh
```

For managed CI, avoid exposing internal host paths and raw secret material.

## Cache/artifact service design

Local mode:

```text
filesystem + SQLite metadata
```

Self-hosted mode:

```text
Postgres metadata + filesystem or S3-compatible object storage
```

Managed mode:

```text
Postgres metadata + object storage + tenant isolation + retention policies
```

## Acceptance tests

- actions/cache key exact hit.
- restore-key fallback.
- cache miss classified correctly.
- failed run cache write quarantine.
- trusted branch cache write promotion.
- untrusted PR cannot poison trusted cache.
- artifact upload/download roundtrip.
- artifact path traversal rejected.
- log masking works before agent output.
- local offline action mirror works.
- top common setup actions work with cache.
