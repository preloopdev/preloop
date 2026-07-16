# Conformance and Fidelity

## Principle

Conformance is the thing that lets Preloop claim GitHub Actions compatibility without hand-waving.

Do not say “faithful” unless a test can show where behavior matches, where it differs, and whether the difference matters.

## Oracles

Use four behavior sources:

| Oracle | What it proves |
|---|---|
| GitHub-hosted Actions | closest hosted truth for supported workflows |
| Official self-hosted runner | official Listener/Worker behavior |
| `ChristopherHX/runner.server` | local GitHub Actions service emulation behavior |
| Aksh | Preloop's Rust-native behavior |

Aksh should be judged against the others, not against prose.

## Existing conformance assets

The uploaded branch already contains valuable assets:

- `.runner-watch/golden/v2.335.1/` wire captures for multiple scenarios.
- `docs/conformance.md` with planned `record`, `expand`, `compare`, and `replay` modes.
- `aksh-conformance` crate.
- upstream workflow fixtures under `fixtures/upstream-workflows/`.
- golden simple/matrix/needs fixtures under `fixtures/golden/`.

These should become the center of the project.

## Fidelity tiers

### P0: must pass before local alpha

- basic success workflow,
- step order,
- per-step exit codes,
- job conclusion,
- failure conclusion,
- matrix fan-out shape,
- `matrix.include` / `matrix.exclude`,
- `needs` ordering,
- basic outputs,
- logs arrive and are retrievable.

### P1: must pass before self-hosted beta

- `if:` conditions,
- `continue-on-error`,
- `timeout-minutes`,
- job outputs propagation,
- environment files,
- `GITHUB_OUTPUT`,
- `GITHUB_PATH`,
- `GITHUB_STEP_SUMMARY`,
- post-step cleanup,
- cache roundtrip,
- artifact roundtrip,
- annotations,
- composite actions,
- JavaScript actions,
- cancellation.

### P2: must pass before managed public beta or be explicitly unsupported

- container actions,
- job-level containers,
- services with health checks,
- reusable workflows,
- OIDC behavior,
- token permissions,
- problem matchers,
- log formatting details,
- `pull_request` / `pull_request_target` security semantics,
- Docker build workflows.

## Normalized run record

Every engine should produce a normalized record:

```json
{
  "case": "matrix-fan-out",
  "engine": "aksh",
  "runner_version": "aksh-...",
  "official_runner_version": "v2.335.1",
  "jobs": [],
  "step_order": [],
  "contexts": {},
  "env": {},
  "outputs": {},
  "timeline": [],
  "logs_normalized": [],
  "annotations": [],
  "cache_events": [],
  "artifacts": [],
  "conclusion": "success",
  "unsupported": [],
  "known_diffs": [],
  "fidelity_score": 0.94
}
```

## Fidelity score

Each Preloop run should emit a machine-readable score:

```json
{
  "fidelity": {
    "score": 0.86,
    "engine": "aksh",
    "guest": "ubuntu-24.04-arm64",
    "host": "macos-arm64",
    "network": "allowlist",
    "source_mode": "live-virtiofs-overlay",
    "unsupported": ["services.health.options"],
    "host_diffs": ["case-insensitive-apfs"],
    "policy_diffs": ["fake-github-token"]
  }
}
```

Agents should be able to read this and decide whether final strict verification is required.

## Test commands to build

```text
aksh conformance record --oracle runner-server --case matrix-fan-out
aksh conformance record --oracle official-runner --case basic-success
aksh conformance expand --case matrix-fan-out
aksh conformance compare --case matrix-fan-out --left aksh --right runner-server
aksh conformance replay --case step-failure
preloop conformance run --inside-libkrun --tier p0
preloop conformance diff --against github-hosted --case cache-roundtrip
```

## Corpus

Minimum corpus:

```text
p0-basic-success
p0-step-failure
p0-matrix-include-exclude
p0-needs-outputs
p0-skipped-job
p1-env-files
p1-github-output
p1-github-path
p1-step-summary
p1-continue-on-error
p1-timeout-minutes
p1-cache-roundtrip
p1-artifact-upload-download
p1-js-action
p1-composite-action
p1-annotations
p1-cancellation-mid-step
p2-reusable-workflow
p2-container-action
p2-container-job
p2-service-postgres
p2-oidc-id-token
p2-permissions
p2-pull-request-event
p2-pull-request-target-unsafe
```

## Differential testing strategy

1. Run the same fixture on oracle A and Aksh.
2. Normalize volatile values:
   - timestamps,
   - GUIDs,
   - absolute temp paths,
   - runner IDs,
   - token bytes,
   - service URLs,
   - activity IDs.
3. Compare semantic outputs:
   - job graph,
   - context values,
   - step conclusions,
   - outputs,
   - timeline states,
   - cache/artifact events,
   - annotations,
   - cancellation behavior.
4. Record known intentional diffs.
5. Fail on unexplained diffs.

## Release gates

| Release | Gate |
|---|---|
| Local alpha | P0 pass inside a smolvm microVM |
| Local beta | P1 pass or documented unsupported features |
| Self-hosted beta | P1 + GitHub App integration + trust-tier tests |
| Managed private beta | P2 security-relevant cases + malicious corpus |
| Managed public beta | sustained conformance across runner versions |

## Never-green rule

If Preloop cannot faithfully support a feature, it must fail loudly or lower the fidelity score. Silent green runs are more dangerous than obvious failures.
