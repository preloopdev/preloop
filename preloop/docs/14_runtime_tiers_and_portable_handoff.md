# Runtime Tiers and Portable Handoff

## Goal

Preloop is one control plane (aksh) over interchangeable microVM executors. The
same job payload, guest agent, and conformance harness run across three tiers.
This doc defines the tiers, the `VmProvider` seam that makes them
interchangeable, and the "start CI locally, continue remotely" handoff that is
unique to the smolvm path.

## The three tiers

| Tier | Runtime | Host | Role | Primary strengths |
|---|---|---|---|---|
| **Local** | smolvm (libkrun) | dev laptop — macOS / Linux / Windows | primary local product | fast edit→verdict, pause/resume, `machine shell` debug, offline |
| **Portable / smolvm-KVM** | smolvm (libkrun) on Linux KVM (x86_64 / aarch64) | cheap cloud host or self-hosted box | portable-remote CI **and a valid production deployment target** | `.smolmachine` portability, one runtime shared with local dev, cheap ARM, self-hosted/air-gapped |
| **Scale-CI / Firecracker** | Firecracker microVM | Linux cloud fleet | **primary production runtime** for high-scale managed/self-hosted CI | `jailer` multi-tenancy, thousands of microVMs/host, spot + autoscaling |

Firecracker is the production leader on density, spot economics, and
autoscaling. smolvm-on-KVM is production-capable too — via smolvm's Linux/KVM
integration — and wins where `.smolmachine` portability, air-gapped/self-hosted
simplicity, or "same runtime as the developer's laptop" matter more than raw
fleet density.

## Why not one runtime everywhere

- **smolvm is the only local option** that runs natively on macOS
  (Hypervisor.framework) and Windows (WHP) and gives a developer a real Linux
  microVM with <200ms boot, GPU, Rosetta, and portable snapshots. Firecracker is
  Linux/KVM-only and has no local macOS story.
- **Firecracker is the mature scale option**: it powers AWS Lambda/Fargate, has a
  hardened `jailer`, tiny attack surface, and is designed for packing thousands of
  hostile-tenant microVMs per host. That density and multi-tenant track record is
  what managed CI at scale needs.
- **smolvm-KVM bridges them**: it is the *same runtime* as local, so a
  `.smolmachine` produced on a laptop resumes on a Linux KVM host with its warm
  cache intact. That is the handoff Firecracker cannot do.

Do not force one runtime to be all three. Keep them behind a seam.

## The `VmProvider` seam

Everything above the seam — aksh runner, guest agent, policy, cache/artifacts,
conformance — is identical across tiers. Only the executor differs.

```rust
trait VmProvider {
    async fn boot(&self, spec: VmSpec) -> Result<VmHandle>;   // from OCI image, .smolmachine, or fork
    async fn exec(&self, vm: VmId, req: ExecRequest) -> Result<ExecResult>;
    async fn shell(&self, vm: VmId, req: PtyRequest) -> Result<PtyHandle>;
    async fn cp(&self, vm: VmId, xfer: CopyRequest) -> Result<()>;
    async fn snapshot(&self, vm: VmId) -> Result<PackRef>;    // smolvm: pack create --from-vm
    async fn fork(&self, vm: VmId) -> Result<VmHandle>;       // smolvm: machine fork (macOS/Linux)
    async fn reap(&self, vm: VmId, mode: ShutdownMode) -> Result<ReapReport>;
}
```

- `SmolvmProvider` — implements the trait via smolvm CLI or HTTP API
  (`smolvm serve`, REST + SSE). Used by the Local and smolvm-KVM tiers.
- `FirecrackerProvider` — implements the trait via the Firecracker API + `jailer`.
  `snapshot`/`fork` map to Firecracker's own snapshot/diff-snapshot support or to
  image rebuild where a feature is absent.

Keep the seam smolvm-shaped (its real verbs), not a hypervisor-agnostic
abstraction that hides the knobs Preloop needs.

### Feature parity is not identical — be honest per tier

| Capability | Local (smolvm) | smolvm-KVM | Firecracker |
|---|---|---|---|
| macOS host | yes | — | no |
| Windows host | yes (TSI net, no GPU/snapshot) | — | no |
| `.smolmachine` boot | yes | yes | no (rebuild equivalent image) |
| `machine fork` (CoW) | macOS/Linux | yes | Firecracker snapshot/clone |
| Rosetta x86-on-ARM | yes (macOS) | n/a (native x86 host) | native per-arch |
| GPU passthrough | yes (not Windows) | yes (virglrenderer) | out of scope for CI |
| Hostile multi-tenant density | dev-grade | good | best (jailer) |
| Spot / autoscale | manual | possible | native ecosystem |

Record the tier and its capability set in the run's fidelity metadata (doc 08).

## Portable handoff: "start local, continue remote"

The differentiator. A developer (or agent) boots a job locally, and the **exact
warm VM + cache** is packed and resumed on a remote smolvm-KVM host — no
re-priming, no cold cache, matching guest arch.

```text
Local (laptop, smolvm)                          Remote (Linux KVM, smolvm)
──────────────────────                          ──────────────────────────
preloop run                                     preloop run --continue <handle>
  boot .smolmachine (~250ms)                       machine create --from job.smolmachine
  tar | machine cp  (uncommitted source)           (warm cache already inside)
  run steps on ext4 ────── fails / times out        resume from the failed/next step
  pack create --from-vm ─► job.smolmachine ──────►  finish on cheaper/bigger remote host
```

Mechanics:

1. Local run executes on smolvm; every write lands on the ext4 storage disk.
2. On handoff (explicit, or on hitting a resource/time budget), Preloop runs
   `smolvm pack create --from-vm` to capture the whole warm filesystem — deps,
   build cache, `.pyc`, cargo `target/`, partial outputs — into a
   `.smolmachine`.
3. The pack is pushed to the control plane / registry
   (`registry.smolmachines.com` or Preloop's own).
4. A remote smolvm-KVM worker does `machine create --from job.smolmachine`
   (~250ms, no image pull) and resumes from the recorded step boundary.

### Constraints (state them; do not paper over)

- **Guest arch must match.** A laptop arm64 `.smolmachine` resumes on Linux
  aarch64 KVM, not on x86_64. Cross-arch handoff means rebuild, not resume. This
  is why "linuxarm smolvm can also run on a remote CI platform" is the natural
  path.
- **Secrets do not travel.** `.smolmachine` packs carry **no** resolvable secret
  refs (smolvm rejects them on untrusted surfaces). The remote re-resolves
  secrets from its own trusted source at launch (doc 06).
- **Firecracker is not a handoff target for packs.** `.smolmachine` does not boot
  on Firecracker. The Firecracker tier is for jobs that start remote and scale,
  not for resuming a laptop's pack. If a job must scale on Firecracker, it is
  re-dispatched from source + cache keys, not resumed from a pack.
- **Trust reset on handoff.** Remote resume re-applies the destination tier's
  trust policy and host jail; local dev posture does not leak into a managed host.

## When to use which tier

| Scenario | Tier |
|---|---|
| Developer preflight before push | Local (smolvm) |
| Agent red/green loop | Local (smolvm) |
| Offline / air-gapped / on a plane | Local (smolvm) |
| "This is slow on my laptop, finish it remotely" | handoff → smolvm-KVM |
| Self-hosted team CI, modest concurrency | smolvm-KVM |
| Air-gapped enterprise / regulated on-prem | smolvm-KVM (single portable runtime) |
| Public managed CI, high concurrency, spot | Firecracker |
| Hostile multi-tenant fork PRs at scale | Firecracker (`jailer`) |

## Image supply chain across tiers

The same source spec (Smolfile / OCI image) produces per-tier artifacts:

- Local + smolvm-KVM: `.smolmachine` packs (identical, arch-matched).
- Firecracker: a rootfs + kernel baked from the same image spec, with the
  identical guest agent + aksh runner inside. Toolchain pre-baking (doc 15) is
  applied per tier image.

Keep image builds reproducible and content-addressed so a given source spec maps
to a known artifact digest on every tier (doc 07, doc 12).

## Acceptance gates

- Same P0 conformance corpus passes on Local, smolvm-KVM, and Firecracker behind
  the `VmProvider` seam.
- A local run can `pack create --from-vm` and resume on a remote smolvm-KVM host
  with cache intact and no secret material in the pack.
- Cross-arch handoff is rejected with a clear "rebuild required" message, not a
  silent wrong-arch boot.
- Firecracker tier boots, execs, streams logs, and reaps under the jailer with
  per-VM UID + cgroups.
