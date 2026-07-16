# MicroVM Isolation and the smolvm Runtime

## Goal

Preloop runs every CI job inside a Linux microVM. The microVM substrate is
**smolvm** (libkrun-backed): macOS-first for local developer usage, and Linux/KVM
for portable and self-hosted execution. **Firecracker is the primary production
runtime** for the high-scale CI tier, with **smolvm-on-KVM as a valid production
deployment target** in its own right. Preloop does not reimplement a hypervisor layer.

## The substrate decision is already made

Earlier drafts framed the VM layer as something Preloop must build: a
`KrunRuntime` trait, a `DirectLibkrunRuntime` vs `MicrosandboxRuntime` vs
`CrunKrunRuntime` bakeoff, a `preloop-krun` libkrun-FFI crate, and a "one-week
backend bakeoff" to pick a runtime. That is obsolete.

**smolvm is the runtime.** Preloop consumes it as a dependency. libkrun FFI and
all `unsafe` live in smolvm and libkrunfw, not in Preloop. There is no runtime
bakeoff. The only open questions are:

1. how Preloop drives smolvm (CLI vs HTTP API vs embedded SDK), and
2. which runtime *tier* a given job lands on (see
   [Runtime Tiers and Portable Handoff](14_runtime_tiers_and_portable_handoff.md)).

smolvm (https://github.com/preloopdev/smolvm, Apache-2.0, Rust) already provides
hardware isolation, sub-200ms boot, OCI images, portable `.smolmachine`
snapshots, CoW `machine fork`, egress allowlists, reference-based secrets,
SSH-agent forwarding, GPU passthrough, Rosetta x86 translation, and a
vsock-backed exec/shell/copy plane. Preloop's job is orchestration, policy,
cache/artifacts, conformance, and product UX on top.

## How Preloop drives smolvm

Three surfaces, chosen per mode — not a new abstraction on top:

| Surface | Mechanism | Use |
|---|---|---|
| CLI | `smolvm machine …`, `smolvm pack …` | local CLI product, scripts, dogfood |
| HTTP API | `smolvm serve start`; REST + SSE (`exec/stream`, `logs`), OpenAPI | `preloopd`/aksh programmatic control, worker fleet |
| Embedded SDK | smolvm as a Rust library crate | in-process control where the HTTP hop is unwanted |

Default: `preloopd` drives smolvm over the HTTP API (unix socket locally) or the
embedded SDK. The CLI is the thin local path. The `VmProvider` seam that hides
this (and swaps in Firecracker for the scale tier) is described in doc 14 — keep
it thin and shaped like smolvm's own verbs, not a hypervisor-agnostic god-trait.

```rust
// The seam is small and smolvm-shaped, not a re-abstraction of libkrun.
trait VmProvider {
    async fn boot(&self, spec: VmSpec) -> Result<VmHandle>;     // from image / .smolmachine / fork
    async fn exec(&self, vm: VmId, req: ExecRequest) -> Result<ExecResult>;
    async fn shell(&self, vm: VmId, req: PtyRequest) -> Result<PtyHandle>;
    async fn cp(&self, vm: VmId, xfer: CopyRequest) -> Result<()>;
    async fn snapshot(&self, vm: VmId) -> Result<PackRef>;      // pack create --from-vm
    async fn fork(&self, vm: VmId) -> Result<VmHandle>;         // CoW clone (macOS/Linux)
    async fn reap(&self, vm: VmId, mode: ShutdownMode) -> Result<ReapReport>;
}
```

`smolvm` implements this via CLI/HTTP; `firecracker` implements it via the
Firecracker API + jailer. Everything above the seam (aksh runner, guest agent,
policy, conformance) is identical.

## Isolation model (provided by smolvm)

- One microVM per workload, its own kernel — not a shared-kernel namespace.
- Hypervisor.framework on macOS Apple Silicon (arm64 guest), KVM on Linux
  (`/dev/kvm`, x86_64/aarch64), WHP on Windows (x86_64 guest).
- Host filesystem, network, and credentials are separated by the hypervisor
  boundary. Network is **off by default**.

### Host-security reality still applies

A microVM is strong isolation, but the VMM process proxies host resources for
the guest and must be treated as part of the guest trust domain. smolvm handles
the in-VM boundary; Preloop is still responsible for **host-side jailing of the
VMM process** in the smolvm-KVM and Firecracker tiers (see below). Do not assume
"microVM" alone is sufficient for hostile multi-tenant code.

## Tier-specific host jail

| Tier | Host jail responsibility |
|---|---|
| Local (smolvm/macOS) | dev-grade: no implicit home mount, read-only repo mount, fake tokens, no SSH-agent by default. Explicitly *not* multi-tenant isolation. |
| Portable / smolvm-KVM (prod-capable) | Linux launcher wraps the smolvm VMM: per-VM UID/GID, private runtime dir, mount + user namespace, cgroups v2, seccomp, no host home, no cloud metadata, network policy, parent-death cleanup, zombie reaper. |
| Scale-CI / Firecracker (prod primary) | Firecracker `jailer` provides chroot/namespaces/cgroups/seccomp per microVM; Preloop adds tenant scoping, worker recycling, and attestation. |

Managed/self-hosted CI must not run untrusted code without the tier's host jail.

## macOS local mode

Apple Silicon macOS is the primary local target. Treat it as local developer
isolation, not managed isolation. smolvm defaults align here:

- network off by default; no implicit home mount,
- read-only / snapshot repo mounts (Preloop policy on top of smolvm mounts),
- fake tokens by default; `--ssh-agent` opt-in only,
- explicit warning that macOS local mode is not equivalent to Linux/Firecracker jailing,
- case-insensitive APFS drift detection (see doc 04).

## Network modes → smolvm flags

Preloop's network policy maps directly onto smolvm egress controls:

| Preloop mode | smolvm mechanism | Purpose |
|---|---|---|
| `off` | default (no `--net`) | deterministic/untrusted code, no egress |
| `allowlist` | `--allow-host HOST` (egress + DNS filter) / `--allow-cidr CIDR` | default for agent/untrusted-PR mode |
| `localhost-only` | `--outbound-localhost-only` | services that only talk to co-located deps |
| `proxy` | egress via `-e https_proxy=…` to a Preloop policy proxy | policy, logging, secret masking, replay |
| `full` | `--net` | trusted local dev, service-heavy workflows |
| inbound | `-p HOST:GUEST` | expose a service port to the host/runner |

Windows is TSI-only (no virtio-net). Record the mode in run metadata and the
fidelity score; do not pretend all modes have equal fidelity.

## Snapshots, forks, and portability (the cache substrate)

smolvm gives real snapshot/fork primitives — use them by name; do not describe
this as "CoW disk checkpoint + re-exec from step boundary":

| Primitive | smolvm command | Preloop use |
|---|---|---|
| Warm snapshot → portable artifact | `pack create --from-vm NAME -o x.smolmachine` | cache-as-filesystem, warm pools, tier handoff |
| Boot from artifact | `machine create --from x.smolmachine` (~250ms, no pull) | cold-start elimination |
| CoW clone | `machine fork` (macOS/Linux; not Windows) | warm pools, fork-from-failure, per-step time-travel |
| Persistent overlay | `machine exec` writes / `/workspace` | state across exec + stop/start |

A `.smolmachine` captures the **entire warm filesystem** — installed deps, build
caches, `.pyc`, cargo `target/` — not just a named cache dir, and all reads stay
on ext4. This is the basis for the efficiency levers in doc 15 and for
fork-from-failure in doc 09. Windows has no fork/snapshot; fall back to
rebuild-from-image there.

## Filesystem performance rule

smolvm has two I/O paths:

- **Storage disk (fast):** ext4 on a raw disk image over virtio-blk. Metadata
  ops (stat/open/readdir/unlink) stay in the guest kernel; the host sees
  sequential I/O to one file. 2.5–3.8× faster small-file I/O than macOS APFS.
- **Bind mount (slow):** virtiofs to host APFS/ext4; every file op round-trips.

**Rule: keep CI's hot I/O (deps, build output, test scratch) on the ext4 storage
disk, not on a virtiofs bind mount.** Get the working tree in — including
uncommitted changes — via `machine cp` onto the storage disk, ideally as a
*delta over a warm snapshot* rather than a full re-copy; run on ext4; extract
results with `machine cp`. A live `-v` bind mount is offered only as an explicit
low-throughput "edit-visibility" mode. Doc 04 defines the sync strategy
(delta-over-snapshot, incremental sync with deletion semantics, ignore rules);
doc 13 has the benchmarks.

## Secrets and credentials (provided by smolvm)

- Reference-based secrets: `--secret-env GUEST=HOST_ENV`, `--secret-file
  GUEST=/abs/path`, Smolfile `[secrets]`. Late-bound at launch, never persisted to
  the VM record, DB, or `.smolmachine`. Untrusted surfaces (HTTP bodies, packs)
  reject refs. Bridge Vault/1Password/AWS through the env/file seam.
- SSH-agent forwarding (`--ssh-agent`): host agent signs; private keys never
  enter the guest. Opt-in per trust tier.
- This is defense-in-depth, not zero-knowledge (root in guest can read
  `/proc/*/environ`); doc 06 defines when each is allowed.

## Resources and cost

Defaults: 4 vCPU / 8 GiB / 20 GiB storage / 2 GiB overlay. Memory is **elastic
via virtio balloon** — the host commits only what the guest uses and reclaims the
rest; idle vCPUs sleep in the hypervisor. Over-provisioning is near-zero cost,
which is a lever for dynamic sizing and warm pools (doc 15). Override with
`--cpus`/`--mem`/`storage`/`overlay` or the Smolfile.

## Required runtime smoke tests

These validate the smolvm substrate before aksh integration (CLI shown; the HTTP
API mirrors each):

```text
smolvm machine run --image ubuntu -- echo hello           # boot + exec
smolvm machine shell --name <vm>                          # vsock PTY, no SSH/net
smolvm machine cp ./ <vm>:/workspace/repo.tar.gz          # source in (fast path)
smolvm machine exec --name <vm> -- touch /workspace/ok    # writable overlay
smolvm machine run --image alpine -- nslookup example.com # must fail (net off)
smolvm machine run --net --allow-host registry.npmjs.org --image alpine \
  -- wget -q -O /dev/null https://google.com              # must fail (not allowed)
smolvm pack create --from-vm <vm> -o warm.smolmachine     # snapshot
smolvm machine create --from warm.smolmachine --name <vm2> # boot from snapshot
smolvm machine delete --name <vm> -f                      # reap
```

## Acceptance gates for the substrate

- Boot + exec + shell + cp on Apple Silicon (local) and Linux/KVM x86_64+aarch64 (smolvm-KVM tier).
- Egress off/allowlist enforced (denied hosts fail).
- Snapshot roundtrip: warm VM → `.smolmachine` → boot elsewhere with cache intact.
- `machine fork` produces an independent CoW clone (macOS/Linux).
- Clean reap: no zombie VMM processes, no leaked disk images.
- Firecracker tier passes the same `VmProvider` contract behind the seam.
