# Roadmap, Milestones, and Risk Register

## Principle

Do not start by building everything. First prove the four hardest truths:

1. Aksh can conform closely enough to real runner behavior.
2. libkrun can host the job environment on macOS and Linux.
3. private Docker/services work inside the chosen VM profile.
4. untrusted code policies are enforceable in self-hosted/managed mode.

## Phase 0 — Truth table and doc cleanup, 1 week

Goal: make the current state of Aksh unambiguous.

Tasks:

- Generate a feature status table from code/tests.
- Mark each feature as `pass`, `partial`, `stub`, `dead-code`, or `missing`.
- Reconcile stale docs.
- Separate local-only behavior from production-ready behavior.
- Add `cargo xtask status` or equivalent.

Exit criteria:

```text
aksh status report is generated from code/tests
docs no longer conflict on major feature status
container/service support clearly marked
```

## Phase 1 — Conformance harness, 1–2 weeks

Goal: make compatibility measurable.

Tasks:

- Implement/finish `record`, `expand`, `compare`, and `replay` flows.
- Add runner.server oracle path.
- Add official runner oracle path.
- Normalize volatile fields.
- Add P0 corpus.
- Gate CI on P0.

Exit criteria:

```text
cargo xtask conformance p0
aksh vs runner.server diff generated
aksh vs official runner diff generated
P0 failures become explicit known gaps
```

## Phase 2 — Runner completeness, 2–3 weeks

Goal: close real-workflow blockers.

Tasks:

- Wire container ops into job execution.
- Support Docker/container actions at least through private Docker.
- Support `container:` jobs or fail loudly.
- Support `services:` with health checks or fail loudly.
- Fix live ordered log streaming.
- Validate cancellation mid-step.
- Validate cache/artifact roundtrip.
- Expand JS/composite action coverage.

Exit criteria:

```text
postgres service workflow passes or emits explicit unsupported
container action workflow passes or emits explicit unsupported
cache/artifact roundtrip passes
cancellation mid-step kills process tree
```

## Phase 3 — libkrun substrate, 2 weeks

Goal: prove VM control independent of Actions.

Tasks:

- Implement DirectLibkrunRuntime smoke path.
- Boot Linux guest on Apple Silicon.
- Boot Linux guest on Linux/KVM.
- Build static guest agent.
- Implement exec, PTY shell, health check.
- Implement read-only workspace mount.
- Implement overlay setup.
- Implement cleanup/reaper.

Exit criteria:

```text
preloop vm exec -- echo hello
preloop vm shell <vm>
preloop vm mount-ro . /host_ro
preloop vm overlay --mode hybrid
preloop vm reap reports clean shutdown
```

## Phase 4 — Docker substrate gate, 1 week

Goal: choose the runtime path honestly.

Tasks:

- Test microsandbox with private Docker.
- Test direct libkrun with custom rootfs/kernel.
- Test dockerd, BuildKit, docker build, docker run, Postgres service.
- Measure boot-to-Docker-ready and cleanup reliability.

Exit criteria:

```text
chosen local alpha runtime
chosen self-hosted/managed runtime
Docker gate memo committed
```

## Phase 5 — Aksh inside microVM, 2 weeks

Goal: run real workflows in Preloop VMs.

Tasks:

- Host runs Aksh control plane.
- VM runs guest agent and Aksh runner.
- Bridge traffic via vsock/proxy.
- Mount workspace and caches.
- Stream logs/events.
- Run P0 conformance inside VM.
- Keep failed VM alive for shell.

Exit criteria:

```text
preloop run .github/workflows/ci.yml --engine aksh
P0 conformance passes inside libkrun VM
preloop shell attaches after failure
preloop retry --job works
```

## Phase 6 — Policy and untrusted PR mode, 2 weeks

Goal: make security visible and enforced.

Tasks:

- Implement trust-tier engine.
- Implement fake/scoped token broker.
- Implement network off/allowlist/proxy.
- Implement action pinning policy.
- Implement secret redaction pipeline.
- Implement cache quarantine.
- Implement symlink escape scanner.
- Add malicious workflow corpus.

Exit criteria:

```text
preloop run --trust untrusted-fork-pr
real secrets unavailable
network denial classified
cache writes quarantined/disabled
logs masked before agent output
```

## Phase 7 — Local alpha, 2–3 weeks

Goal: make it useful for developers and agents.

Tasks:

- CLI: `run`, `status`, `retry`, `shell`, `doctor`, `verify`.
- NDJSON schema.
- MCP tools.
- Failure bundles.
- Final clean verification.
- Warm VM pools.
- Action mirror.
- Tool cache profiles.
- `.github/preloop.toml`.

Exit criteria:

```text
preloop run
preloop retry --step failed
preloop verify --clean --strict
preloop doctor
MCP agent loop works end-to-end
```

## Phase 8 — Self-hosted worker beta, 4–6 weeks

Goal: run Preloop on team-owned workers.

Tasks:

- Worker daemon.
- Worker registration and job leasing.
- Durable control-plane state.
- GitHub App webhook/check-run integration.
- External log/artifact storage.
- Ephemeral VM per job.
- Worker health and cleanup proof.
- Labels/resources/scheduling.

Exit criteria:

```text
GitHub PR triggers Preloop run
worker executes one job per VM
check run updates
logs/artifacts durable
worker proves cleanup
```

## Phase 9 — Managed CI private beta, 8–12+ weeks

Goal: safely run multi-tenant untrusted code.

Tasks:

- Tenant isolation.
- Worker pool design.
- Host recycling.
- Cache/artifact isolation.
- Object storage policies.
- Egress accounting.
- Billing meters.
- Abuse detection.
- Audit logs.
- Security review.
- Malicious workload suite.

Exit criteria:

```text
untrusted public PR cannot access secrets
tenant caches/artifacts/logs isolated
malicious corpus passes
worker recycling policy operational
security review completed
```

## Risk register

| Risk | Severity | Mitigation |
|---|---:|---|
| Aksh drifts from GitHub semantics | Very high | Conformance against GitHub, official runner, and runner.server |
| Private Docker fails in chosen guest | Very high | Week-one Docker gate; direct libkrun/custom kernel escape hatch |
| Managed CI security underestimated | Very high | Hardened Linux jail, tenant isolation, malicious corpus, security review |
| Container/service support remains partial | Very high | Make services/container actions release gates |
| Local shortcuts leak into production | High | Separate local/self-hosted/managed configs and stores |
| libkrun host-resource exposure | High | Host jail, no host home, path allowlists, seccomp/cgroups |
| Cache poisoning | High | Trust-tier namespaces, quarantine, write-on-success |
| Secrets leak through logs/artifacts | High | redaction-safe types, masking pipeline, tests |
| Network exfiltration | High | off/allowlist/proxy, audit, metadata block |
| APFS case-insensitive drift | Medium/high | detector, fidelity score, strict source snapshot |
| Overlay copy-up hides host edits | Medium/high | changed-path invalidation, overlay rebuild, strict verify |
| Performance focuses on boot only | Medium | measure edit-to-verdict and final clean verification |

## Immediate next 14 days

1. Generate Aksh feature truth table.
2. Finish conformance harness P0.
3. Wire or hard-fail container/service support.
4. Implement direct libkrun smoke test.
5. Run Docker gate.
6. Expand NDJSON events.
7. Start trust-tier policy engine.
8. Create malicious workflow corpus.
9. Draft macOS and Linux host-security notes.
10. Split local-only shortcuts from production paths.
