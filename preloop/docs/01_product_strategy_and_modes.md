# Product Strategy and Operating Modes

## Thesis

Preloop should own the safe, fast, agent-native GitHub Actions-compatible execution loop:

> Local speed plus hardware-backed isolation plus private Docker-in-VM plus conformance-tested GitHub Actions behavior.

The important shift from earlier drafts is that Preloop is no longer only a local developer tool. It now needs to support three deployment classes:

1. **Local CI on macOS and Linux** for developers and coding agents.
2. **Self-hosted CI** on customer-controlled workers.
3. **Managed CI** where Preloop runs untrusted code and PRs for customers.

These classes should share the same core runner/control-plane logic, conformance harness, workflow semantics, and cache/artifact protocols. They should not share the same security defaults.

## Defensible wedge

The uploaded master plan frames the wedge around five capabilities:

- Hardware isolation.
- Private Docker inside the VM.
- Deny-by-default egress and non-leaking secrets.
- Clean-room fidelity mode.
- Fork-from-failure.

Keep that wedge. Do not drift into “generic local CI runner” positioning.

## Runtime tiers

The three deployment classes map onto three interchangeable microVM executors
behind one control plane (aksh). See
[Runtime Tiers and Portable Handoff](14_runtime_tiers_and_portable_handoff.md).

| Tier | Runtime | Role |
|---|---|---|
| Local | **smolvm** (libkrun) on macOS/Linux/Windows | primary local product |
| Portable / smolvm-KVM | **smolvm** on Linux KVM | "start local, continue remote"; a valid production deployment target |
| Scale-CI | **Firecracker** | primary production runtime for high-scale managed CI |

Preloop consumes smolvm as its microVM substrate — it does not reimplement a
hypervisor. Firecracker leads production on density/spot/autoscaling; smolvm-KVM
is production-capable too and uniquely enables portable `.smolmachine` handoff
from a developer's laptop to a remote host with warm cache intact.

## Product modes

### 1. Local developer CI

This is the fastest path to adoption.

```text
preloop run
preloop retry --step failed
preloop shell <run/job>
preloop verify --clean --strict
```

Default posture:

- Read-only source mount.
- Hybrid writable overlay.
- Local cache enabled.
- Network allowlist or proxy, not totally unrestricted by default.
- Fake `GITHUB_TOKEN` unless the user opts into a scoped token.
- Machine-readable fidelity warnings.

Success metric:

- Time from local edit to useful verdict.
- The product should feel interactive for agents.

### 2. Local agent loop

This is local CI but with stricter assumptions. Treat agent-generated code as potentially malicious.

Default posture:

- No real secrets.
- Egress allowlist.
- Cache writes quarantined on failed runs.
- No host home mount.
- No SSH-agent forwarding.
- No host Docker socket.
- Failure bundles are compact and redacted before the agent sees them.

Success metric:

- Agent can iterate quickly without gaining access to host credentials or silently depending on dirty host state.

### 3. Self-hosted CI

This runs on team-owned machines but executes code that may be untrusted, especially pull requests.

Default posture:

- Linux worker preferred for hard host jailing.
- One fresh smolvm microVM per job (Firecracker on the scale tier).
- Durable state for runs, jobs, leases, logs, artifacts, and cache metadata.
- GitHub App or self-hosted runner integration.
- Trust-tier policies based on event type and repository relationship.

Success metric:

- Safer self-hosted runner economics and performance without persistent runner contamination.

### 4. Managed CI

This is the hardest mode. Assume hostile tenants and adversarial PRs.

Default posture:

- Linux hardened workers only for untrusted multi-tenant workloads.
- Tenant-scoped caches, artifacts, logs, and object storage.
- Per-VM host jail.
- Network metering and egress policy.
- Worker recycling and attestation strategy.
- Security corpus must pass before public managed beta.

Success metric:

- Secure, deterministic, cost-effective CI with predictable tenant isolation.

## Strategic architecture choice

Aksh is the primary Rust-native control plane and runner path. The official
GitHub Actions runner and `ChristopherHX/runner.server` remain conformance
oracles, compatibility fallback paths, protocol drift detectors, and test
fixtures.

The microVM substrate is **smolvm**, consumed as a dependency (CLI, HTTP API, or
embedded SDK), not rebuilt. A thin `VmProvider` seam swaps smolvm for Firecracker
on the scale tier while keeping the aksh runner, guest agent, and conformance
harness identical across tiers.

Because Preloop needs managed CI, agent pause/retry, network policy, secret
policy, VM lifecycle hooks, and failure forking, owning the runner/control plane
(aksh) is strategically valuable — but owning the hypervisor is not. smolvm
already provides isolation, snapshots, egress control, and secrets; Preloop's
leverage is the control plane and the efficiency levers layered on top
([CI Efficiency Levers](15_ci_efficiency_levers.md)).

## What not to build first

Avoid these traps:

- Do not build broad CI-system compatibility before GitHub Actions parity.
- Do not start with Jenkins, Circle, or GitLab CI abstractions.
- Do not build a generic VM platform without a GitHub Actions use case driving it.
- Do not reimplement the microVM/hypervisor layer. smolvm is the substrate; consume it. Build a `preloop-krun`/libkrun-FFI crate or a hypervisor-agnostic runtime abstraction only if smolvm provably cannot expose a knob Preloop needs.
- Do not optimize boot time before measuring edit-to-verdict.
- Do not ship managed untrusted PR execution without durable state, trust tiers, and cache isolation.

## Product promise by version

| Version | Promise |
|---|---|
| v0.0 | Proves the smolvm execution substrate and Aksh conformance direction. |
| v0.1 | Useful local CI for macOS/Linux/Windows with Aksh inside smolvm microVMs. |
| v0.2 | Real-world workflows: cache, artifacts, services, Docker, strict mode. |
| v0.3 | Self-hosted worker beta (smolvm-KVM) with GitHub App integration. |
| v0.4 | Hardened managed CI private beta on Firecracker; portable smolvm handoff. |

## Positioning sentence

Preloop is not a faster Docker wrapper. Preloop is the hardware-isolated GitHub Actions-compatible CI loop for autonomous coding agents and teams that need local speed without trusting the code they run.
