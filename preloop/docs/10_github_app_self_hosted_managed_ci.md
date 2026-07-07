# GitHub App, Self-Hosted, and Managed CI

## Goal

Preloop should eventually support both:

1. **Preloop-managed GitHub App mode**, where GitHub sends webhooks and Preloop reports Check Runs.
2. **GitHub-connected self-hosted runner mode**, where Preloop provides ephemeral runners that register with GitHub.

The GitHub App path is better for Preloop's managed platform because Preloop owns scheduling, policy, job metadata, retry, and VM orchestration.

## GitHub App flow

```text
GitHub webhook
  -> verify signature
  -> classify event and trust tier
  -> fetch workflows at exact SHA
  -> evaluate triggers
  -> create check runs
  -> schedule Aksh jobs
  -> lease jobs to workers
  -> boot libkrun VM per job
  -> stream logs/artifacts/results
  -> update check runs
```

## Current Aksh webhook progress

The uploaded branch includes `docs/github-app-webhook.md`, which describes:

- webhook receiver,
- HMAC-SHA256 signature verification,
- local workspace workflow fetching,
- GitHub API workflow fetching,
- GitHub Checks API reporting,
- queued/in-progress/completed lifecycle,
- and a local development GitHub App manifest flow.

That is a good dev foundation. Production needs a stricter implementation.

## Production GitHub App requirements

- Verify `X-Hub-Signature-256` on raw request body.
- Use webhook delivery ID for idempotency.
- Store webhook deliveries and processing status.
- Prevent replay or duplicate run creation.
- Fetch workflow files at exact commit SHA, not mutable branch names.
- Correctly classify `push`, `pull_request`, `pull_request_target`, `workflow_dispatch`, `schedule`, and rerun events.
- Mint installation tokens through GitHub App auth.
- Store private keys in a secrets manager or KMS, not plaintext local files.
- Create and update check runs with durable IDs.
- Support GitHub UI rerun actions where possible.
- Never let local development manifest shortcuts leak into production.

## Trust classification

Event classification should drive policy:

| Event | Example | Trust tier |
|---|---|---|
| push to protected main | internal commit | `trusted-branch` |
| push to feature branch | internal branch | `internal-pr` or configured |
| pull_request from same repo | internal PR | `internal-pr` |
| pull_request from fork | untrusted code | `untrusted-fork-pr` |
| pull_request_target | privileged context | special locked-down policy |
| workflow_dispatch by admin | manual run | configured/admin |
| release/tag | deployment | `deployment` |

`pull_request_target` should be treated as dangerous if it checks out untrusted fork code with privileged secrets.

## Worker architecture

Self-hosted/managed worker:

```text
preloop-control-plane
  |
  +-- job queue and leases
  +-- artifacts/cache/log metadata
  +-- GitHub check reporting
  |
  v
preloop-worker fleet
  |
  +-- accepts job lease
  +-- creates jailed libkrun VM
  +-- runs aksh-runner inside VM
  +-- streams logs/events
  +-- exports artifacts
  +-- destroys VM
  +-- reports cleanup proof
```

Worker responsibilities:

- register with control plane,
- advertise labels/resources,
- acquire/renew/complete leases,
- enforce job timeout,
- enforce resource limits,
- run one job per VM,
- maintain warm pools where safe,
- clean up failed/zombie VMs,
- report health and capacity,
- prove cleanup after job.

## Durable state

Self-hosted and managed modes need durable state:

```text
runs
jobs
attempts
runner registrations
worker leases
check-run IDs
logs
artifacts
cache metadata
webhook deliveries
policy decisions
secret access decisions
network denials
billing/resource usage
```

Local mode can use in-memory state. Do not mix that with production code paths.

## GitHub-connected self-hosted runner mode

This mode is optional but useful for compatibility:

```text
Preloop worker boots VM
  -> official runner registers as ephemeral with GitHub
  -> GitHub assigns exactly one job
  -> job runs
  -> runner exits/deregisters
  -> VM destroyed
```

Pros:

- highest GitHub Actions service fidelity,
- useful for customers who already use GitHub self-hosted runner semantics,
- reduces Aksh service emulation burden.

Cons:

- less control over pause/retry/fork,
- GitHub service still owns queueing,
- hard to provide Preloop-managed agent UX,
- runner update lifecycle applies,
- less suitable for managed Preloop-specific platform features.

Treat it as a separate mode, not the primary managed architecture.

## Managed CI requirements

Managed CI adds:

- tenant isolation,
- per-tenant quotas,
- object storage isolation,
- cache encryption or strong namespacing,
- network egress metering,
- abuse detection,
- worker recycling policy,
- host patch pipeline,
- audit logs,
- incident response hooks,
- support tooling,
- billing meters,
- and security review.

## Self-hosted beta gate

Before self-hosted beta:

- GitHub App webhook flow works end-to-end.
- Check run lifecycle is durable.
- Worker can acquire a job and run it in a libkrun VM.
- Worker cleanup is proven.
- Trust-tier policy applies to PRs.
- Logs/artifacts are stored durably.
- Cache namespaces prevent obvious poisoning.
- Conformance P1 passes or gaps are documented.

## Managed private beta gate

Before managed private beta:

- malicious workflow corpus passes,
- tenant caches/artifacts/logs isolated,
- worker cannot access other tenant state,
- untrusted PRs cannot reach real secrets,
- network policy and audit work,
- cache writes are quarantined or disabled for untrusted tiers,
- worker fleet has recycling and cleanup guarantees,
- security review completed,
- incident/debug bundle redacts secrets.
