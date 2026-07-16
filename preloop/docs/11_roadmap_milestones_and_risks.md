# Roadmap, Milestones, and Risk Register

## Principle

Do not start by building everything. First prove the four hardest truths:

1. Aksh can conform closely enough to real runner behavior.
2. smolvm can host the job environment across tiers (macOS, Linux/KVM), and Firecracker can host the scale tier.
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

## Phase 3 — smolvm integration, 2 weeks

Goal: drive the smolvm substrate from Preloop; prove VM control independent of Actions.

Tasks:

- Implement the `VmProvider` seam and `SmolvmProvider` (smolvm CLI/HTTP API/SDK).
- Boot from OCI image and `.smolmachine` on Apple Silicon and Linux/KVM.
- Wire exec, PTY shell (`machine shell`), health check, and `machine cp`.
- Working-tree delta sync onto the ext4 storage disk (uncommitted changes).
- Snapshot/fork: `pack create --from-vm`, `machine create --from`, `machine fork`.
- Cleanup/reaper: no zombie VMM, no leaked disk images.

Exit criteria:

```text
smolvm boot + exec via VmProvider prints hello
preloop shell attaches over vsock
machine cp delta sync round-trips
pack create --from-vm && machine create --from boots warm
reap reports clean shutdown
```

## Phase 4 — Docker-in-smolvm gate + Firecracker bringup, 1–2 weeks

Goal: validate container workloads on smolvm and stand up the scale tier behind the same seam.

Tasks:

- Test dockerd/BuildKit/docker build/docker run/Postgres service inside smolvm (macOS + Linux/KVM).
- Confirm smolvm libkrunfw kernel/rootfs has the required configs; escalate gaps upstream to the tier image.
- Implement `FirecrackerProvider` behind `VmProvider`; boot/exec/stream/reap under the jailer; pass P0.
- Validate portable handoff: local `pack create --from-vm` → resume on a smolvm-KVM host with warm cache.
- Measure boot-to-Docker-ready and cleanup reliability per tier.

Exit criteria:

```text
Docker gate passes on smolvm (both hosts) or emits classified unsupported
FirecrackerProvider passes P0 behind the seam
pack handoff resumes remotely with cache intact
Docker + tier gate memo committed
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
P0 conformance passes inside a smolvm microVM
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
- Portable handoff: `pack create --from-vm` → resume on a remote smolvm-KVM worker (see doc 14).

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
- Firecracker jailer worker fleet (primary scale runtime); smolvm-KVM option.
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
| Private Docker fails in guest | Very high | Docker-in-smolvm gate; fix libkrunfw kernel config in the tier image; Firecracker tier as alternate |
| Managed CI security underestimated | Very high | Hardened Linux jail, tenant isolation, malicious corpus, security review |
| Container/service support remains partial | Very high | Make services/container actions release gates |
| Local shortcuts leak into production | High | Separate local/self-hosted/managed configs and stores |
| VMM host-resource exposure | High | tier host jail (smolvm-KVM launcher / Firecracker jailer), no host home, path allowlists, seccomp/cgroups |
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
4. Wire the `VmProvider` seam over smolvm (boot/exec/shell/cp/snapshot/fork).
5. Run the Docker-in-smolvm gate and stand up the Firecracker provider.
6. Expand NDJSON events.
7. Start trust-tier policy engine.
8. Create malicious workflow corpus.
9. Draft macOS and Linux host-security notes.
10. Split local-only shortcuts from production paths.
