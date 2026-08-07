# MicroVM Isolation Research: Firecracker vs libkrun for CI Runners

Research notes covering CI runner architecture, microVM isolation models, and the
engineering tradeoffs between Firecracker and libkrun for running untrusted workloads.

---

## 1. CI Runner Architecture Primer

### Concepts

**Workflow** — a YAML file that declares what should happen when a trigger fires.

**Job** — a unit of work inside a workflow. Each job runs independently on its own
machine. Jobs can depend on each other via `needs:` but do not share a filesystem.

**Step** — a single instruction inside a job. Steps run sequentially, share the
same machine and filesystem, and execute as child processes of the runner.

**Runner** — the machine that executes a job. It polls the control plane for work,
receives a job payload, runs each step as a child process, and reports results back.

```
Workflow (triggered)
  └── Job (assigned to a Runner)
        ├── Step 1 (child process)
        ├── Step 2 (child process)
        └── Step 3 (child process)
```

### Ephemeral Runner Model (Production Standard)

A fresh runner is created for each job, executes it, then is destroyed. This is
how GitHub-hosted runners work and how production self-hosted setups are built
(actions-runner-controller on Kubernetes, Fly.io, AWS Lambda, Preloop/libkrun).

```
Job queued on control plane
         ↓
Orchestrator detects queued job
         ↓
Spins up fresh VM / container / microVM
         ↓
Runner registers with --ephemeral flag
         ↓
Runner picks up exactly that one job, runs it
         ↓
Runner exits → GitHub auto-removes registration
         ↓
VM destroyed
```

Benefits: clean environment every run, no secret leakage between runs, scales
horizontally.

### Scale Reality

```
100 engineers × 50 PRs/day × 4 jobs/workflow = 20,000 runner instances/day
Peak concurrent (5 min avg job) ≈ 70 runners alive simultaneously
```

At 70 concurrent runners, the official C# `actions/runner` uses ~100–150 MB RSS
per process just for the .NET runtime. That is ~10 GB RAM in runner processes
before any actual build work begins. This is the core motivation for lightweight
microVM runners.

---

## 2. The Two MicroVM Options

Both Firecracker and libkrun are built on the `rust-vmm` crate ecosystem
(shared by Amazon, Red Hat, Intel, Google). They are not wrappers of each other —
they are independent implementations of a VMM using the same low-level primitives.

### Firecracker

Built by AWS for Lambda and Fargate. A standalone VMM binary controlled via a
REST API over a Unix domain socket. Designed from the ground up for hostile
multi-tenant workloads.

**Security model:** The Firecracker binary is launched by a companion `jailer`
process that drops privileges, enters a chroot, assigns cgroups, and applies strict
seccomp filters before the VMM starts. If a guest VM exploits a kernel bug and
escapes to the VMM, it lands inside this highly restricted jail — it cannot read
host files or open host network connections.

**Minimal device model (intentional):** Only 6 virtual devices:
`virtio-net`, `virtio-block`, `virtio-balloon`, `virtio-vsock`,
serial port, keyboard controller. No GPU, no audio, no shared filesystem.
Fewer devices = fewer code paths = smaller attack surface.

### libkrun

Built by Red Hat engineers to bring lightweight VM isolation to container runtimes
(`crun`, `krunvm`, `podman machine`). A dynamic library (`libkrun.so` /
`libkrun.dylib`) exposing a C API. The VMM runs inside the calling process.

**Security model:** The VMM runs in the same security context as the host
application that linked it. If a guest escapes the hypervisor, the attacker has
the host privileges of the parent process. No built-in jailing.

**Rich device model:** `virtio-net`, `virtio-block`, `virtio-vsock`,
`virtio-fs` (directory sharing), `virtio-gpu` (Vulkan/Metal on libkrun-efi),
`virtio-snd`. Bundled kernel via `libkrunfw` — no external kernel image needed.

---

## 3. Deep Feature Comparison

### Architecture

| Dimension | Firecracker | libkrun |
|---|---|---|
| Delivery | Standalone binary, REST API socket | Dynamic library (`.so` / `.dylib`) |
| Execution context | Separate jailed process | In-process (same memory as caller) |
| Kernel | External ELF `vmlinux` required | Bundled in `libkrunfw` |
| Hypervisor | Linux KVM only | Linux KVM + macOS Hypervisor.framework |

### Security

| Dimension | Firecracker | libkrun |
|---|---|---|
| Guest escape impact | Trapped in hardened jailer process | Compromises host parent process |
| Jailer | Built-in (chroot, cgroups, seccomp, capability drop) | None by default |
| Seccomp profile | ~30–40 syscalls (extremely tight) | Hundreds (networking + fs + gpu) |
| Device attack surface | 6 minimal virtio devices | Extended device model |
| Multi-tenant use | Production-proven (AWS Lambda/Fargate) | Not recommended without extra jailing |

### Filesystem

| Dimension | Firecracker | libkrun |
|---|---|---|
| Host directory sharing | No (`virtio-block` only) | Yes (`virtio-fs`) |
| CI workspace mount | Must write to block image, copy in/out | Direct host folder mount |
| Path traversal risk | None (guest sees raw block device) | Present (VMM resolves host paths) |

### Networking

| Dimension | Firecracker | libkrun |
|---|---|---|
| Mechanism | TAP/TUN devices | Transparent Socket Impersonation (TSI) |
| Root required | Yes (`CAP_NET_ADMIN` for TAP setup) | No (user-space socket translation) |
| Setup complexity | High (bridges, iptables, CNI) | Zero config |

### Platform

| Dimension | Firecracker | libkrun |
|---|---|---|
| Linux (KVM) | Yes | Yes |
| macOS (Apple Silicon) | No | Yes (Hypervisor.framework) |
| Windows | No | Experimental (WHPX backend) |

### Performance

| Dimension | Firecracker | libkrun |
|---|---|---|
| Boot time | 5ms–125ms | 100ms–200ms |
| Memory overhead per VM | ~5 MB | ~15 MB |
| File I/O (CI workspace) | High overhead (block image copy) | Near-native (virtio-fs) |


### Guest Kernel — A Critical Distinction

Both projects describe themselves as "minimal" but the word means completely
different things in each context. This distinction explains why Firecracker
supports DinD and systemd while libkrun does not.

**Firecracker's minimalism = VMM device model only.**
The guest kernel is whatever you supply. You bring your own `vmlinux` ELF binary
compiled with whatever kernel config you need. Production teams compile a
full-featured kernel:

```
CONFIG_OVERLAY_FS=y       # Docker layer storage
CONFIG_BLK_DEV_LOOP=y     # Loop devices for images
CONFIG_NF_TABLES=y        # nftables / iptables
CONFIG_BRIDGE=y           # Docker bridge networking
CONFIG_CGROUPS=y          # Container resource limits
CONFIG_NAMESPACES=y       # Container isolation
CONFIG_BINFMT_MISC=y      # Interpreter support
```

Boot is still fast because the VMM has so few devices to initialize that the
kernel has almost nothing to probe. The kernel itself can be 8–20 MB compressed.

**libkrun's minimalism = VMM device model AND bundled kernel.**
The guest kernel is pre-compiled and baked into the `libkrunfw` library. To keep
the library small and boot fast, it ships with an aggressively stripped kernel
config. Loop devices, overlayfs, netfilter, and most kernel module infrastructure
are absent. You cannot easily swap it unless you use `libkrun-efi`, but that
variant only supports booting distribution kernels on macOS, not Linux.

| | Firecracker | libkrun |
|---|---|---|
| **VMM device model** | Minimal (6 devices) | Rich (fs, gpu, sound) |
| **Guest kernel source** | User-supplied, any config | Pre-bundled `libkrunfw`, stripped |
| **Kernel flexibility** | Full — compile any config you need | Low — stuck with bundled |
| **overlayfs / DinD** | Yes — just enable in your kernel | No |
| **iptables / netfilter** | Yes | No |
| **Systemd** | Yes | Unreliable |
| **Why boot is fast** | Minimal devices to initialize | Minimal devices + minimal kernel |

---

## 4. How Teams Solve Firecracker's Gaps in Production

Teams building on Firecracker (E2B, Fly.io, AWS) do not fork it. They work around
its limitations using user-space helpers and in-VM agents.

### Gap 1: No Directory Sharing

**Solution: Guest Agent over virtio-vsock**

Instead of mounting host directories, they boot the VM with a static rootfs and
run a daemon inside the guest. The host orchestrator streams files into the VM
over the virtual socket channel, and the agent unpacks them. Artifacts are pulled
back the same way.

```
[Host Orchestrator]
     │
     └── (tarball stream over virtio-vsock)
                │
         [Guest Agent Daemon]
                │
         (unpacks to /workspace)
```

This completely eliminates host path traversal risk because no host filesystem
paths are ever exposed to the VMM.

### Gap 2: Rootless Networking

**Solution: `slirp4netns` or `passt` (pasta)**

The TAP device is created inside a host-level user namespace. `slirp4netns` or
`passt` intercepts raw Ethernet frames from the VM's virtio-net device and
translates them into standard unprivileged host TCP/IP socket calls. The VMM
runs entirely rootless.

### Gap 3: No macOS Support

**Solution: Hypervisor abstraction layer**

Tools like Docker Desktop, OrbStack, and Podman implement a driver abstraction:
- On Linux: boot Firecracker or Cloud-Hypervisor
- On macOS: boot via Apple Virtualization.framework or QEMU

---

## 5. Adding Firecracker-Equivalent Security to libkrun

### The One Existing Implementation: `crun` + `libkrun`

Red Hat's OCI runtime `crun`, when invoked as `krun`, implements the hybrid model:

1. **Namespace setup**: `crun` creates host-level Linux namespaces (User, Mount,
   Net, PID) and cgroups limits first.
2. **VMM spawn**: Inside this jailed child process, `crun` loads `libkrun.so` and
   boots the microVM.
3. **Result**: A guest VM escape traps the attacker inside the container namespace
   jail — they cannot see host files or processes outside the cgroup.

```
[crun orchestrator]
     │
     └── (CLONE_NEWUSER, CLONE_NEWNS, CLONE_NEWNET, cgroups)
               │
         [Jailed launcher process]
               │
           libkrun.so
               │
          [Guest VM]
               │
         [CI Step / Untrusted Code]
```

### Why Reaching Full Firecracker Parity Is Extremely Hard

| Gap | Work Required | Effort |
|---|---|---|
| Process isolation (in-process → out-of-process) | Rewrite caller to use VMM launcher subprocess + IPC | 4–6 weeks |
| Linux host jailing | Custom jailer: `namespaces`, `cgroups`, `pivot_root`, capability drop | 4–6 weeks |
| Seccomp filter | Audit libkrun syscall footprint (hundreds vs Firecracker's ~40), write BPF filter | 4–8 weeks |
| virtio-fs path restriction | Restrict launcher mount namespace to workspace dir only | 2–4 weeks |
| Ongoing maintenance | Filter breaks on every libkrun/dependency update | 2–4 hrs/week |

**macOS is a fundamental blocker.** macOS lacks Linux namespaces and cgroups. The
only equivalent is macOS App Sandbox profiles (`sandbox-exec`), which are poorly
documented, fragile across OS updates, and cannot be programmatically composed the
way Linux namespaces can. Any cross-platform solution must accept a weaker security
model on macOS.

**Total realistic effort: 3–6 engineer-months** to reach a security posture
approximately equivalent to Firecracker, with ongoing maintenance cost.

---

## 6. microsandbox and smolvm

Both projects use `libkrun` as their VMM backend. Neither implements host-level
namespace/cgroups/seccomp wrapping around the VMM process.

| Feature | `superradcompany/microsandbox` | `smol-machines/smolvm` |
|---|---|---|
| VMM engine | libkrun | libkrun |
| Host-level jail (namespaces/cgroups) | None | None |
| Seccomp on VMM process | None | Seccomp + Landlock on boot subprocess |
| Isolation boundary | Hypervisor only | Hypervisor + limited seccomp/Landlock |
| Platform | Linux, macOS, Windows (experimental) | Linux, macOS |
| Boot time | Sub-100ms | Sub-200ms |
| OCI compatible | Yes | Yes |
| `exec` into running VM | Not supported | Not supported |
| Primary use case | AI agent sandboxes, local dev | Portable self-contained AI sandboxes |

**SmolVM** is the more security-conscious of the two: it applies seccomp and
Linux Landlock filesystem restrictions to the VM boot subprocess. This reduces
the syscall attack surface of the launcher process but does not achieve full
namespace isolation equivalent to crun+libkrun or Firecracker's jailer.

**Neither is suitable for hosting arbitrary untrusted CI workloads** from
external contributors without accepting the risk that a guest VM escape grants
host process access.

---

## 7. crun + libkrun: Production Readiness for CI

### What Works

| Workload | Status |
|---|---|
| Single-process script execution (bash, python, node) | Works |
| Stateless microservice containers | Works |
| Standard OCI image boot | Works |
| Direct host workspace mount via virtio-fs | Works |
| Resource limits via cgroups | Works |

### What Breaks

#### `docker exec` / `podman exec` — Not Supported
Once a container is running inside the microVM, there is no mechanism to inject
a new process into it. This is an open issue (`containers/crun` #2090, open as
of 2026). Every CI system relies on exec to run job steps inside a running
container environment.

#### Docker-in-Docker — Partially Broken
The libkrunfw kernel (verified from actual config, Linux 6.12.91) includes
overlayfs, loop devices, bridge networking, veth pairs, nftables, and conntrack.
These are the primitives Docker actually needs. DinD is not blocked by a stripped
kernel. The real blockers are:

1. **No kernel modules** (`CONFIG_MODULES` not set). Everything is built-in.
   If Docker or any tool calls `modprobe` for a driver not compiled in, it fails.
2. **No `exec`** — Docker itself requires exec to run commands inside containers.
   Without crun+libkrun exec support (#2090), the inner Docker daemon cannot
   serve `docker exec` calls.
3. **`--privileged` semantics** differ at the hypervisor boundary (see below).

Legacy iptables (`CONFIG_IP_NF_IPTABLES`) is also absent — only nftables is
present. Docker ≥20.10 uses nftables by default so this is usually fine.

#### `--privileged` / Host Namespace Flags — Semantically Broken
`--net=host`, `--pid=host`, and `--privileged` are meaningless across a
hypervisor boundary. A guest cannot share host namespaces because there is a
hard kernel boundary between them.

#### Systemd / Multi-Process Init — Works (cgroup v2 present)
The libkrunfw kernel includes full cgroup v2 support (`CONFIG_CGROUPS`,
`CONFIG_MEMCG`, `CONFIG_CGROUP_BPF`) and all namespace types. Systemd's
requirements are met. The earlier claim that systemd is unreliable due to a
stripped kernel was inaccurate — the issue is more likely init system startup
ordering inside the minimal libkrunfw initrd, not missing kernel features.

#### `/dev/kvm` Permission Complexity
Even for users in the `kvm` group, krun requires ACL-level access to `/dev/kvm`
beyond standard group permissions (`containers/crun` #1894). This creates
friction on hardened enterprise Linux hosts.

---

## 8. Decision Matrix

### Security is the Absolute Priority

Use **Firecracker** with a guest agent over `virtio-vsock` (the E2B / Fly.io model).

- Accept: Linux-only, block-device-only storage, TAP network complexity
- Gain: Proven multi-tenant security at scale (AWS Lambda, Fargate, E2B)

For OCI compatibility on top of Firecracker, use **Kata Containers** — it runs
a full unstripped kernel with a proper init system, implements exec over vsock,
and supports Docker-in-Docker. It is the closest production-ready equivalent
that gives OCI compatibility and strong isolation.

### macOS / Apple Silicon Support Required

Use **libkrun** (via `crun --handler krun` for host-level namespace wrapping)
and accept a weaker security boundary.

- Accept: No exec into running containers, no DinD, weaker host isolation
- Gain: Same VMM toolchain on macOS and Linux, virtio-fs workspace mounts,
  rootless networking, fast boot

### General Summary

| Priority | Tool | Accept |
|---|---|---|
| Multi-tenant security at scale | Firecracker + jailer | Linux-only, no virtio-fs, complex storage |
| OCI-compatible + strong isolation | Kata Containers | Heavier overhead, complex operation |
| Cross-platform (macOS + Linux) | libkrun via crun | Weaker VMM boundary, no exec, no DinD |
| Local dev / AI agent sandboxes | microsandbox / smolvm | Weakest isolation, hypervisor-only |

## 10. virtio-fs as a Shared Cache Layer

One underexplored advantage of libkrun over Firecracker for CI is using
virtio-fs to share a persistent cache directory on the host across all runner
VMs on the same machine. Firecracker cannot do this at all — any file transfer
into a Firecracker VM goes through the vsock agent over a virtual socket,
adding serialization and copy overhead regardless of whether the file is on the
same physical disk.

### The Problem virtio-fs Solves

Standard CI cache behaviour today:

```
Runner 1 (job A)  →  downloads deps  →  writes to /tmp/cache  →  VM destroyed
Runner 2 (job B)  →  downloads same deps  →  VM destroyed
Runner 3 (job C)  →  downloads same deps  →  VM destroyed
```

With a host-persistent virtio-fs mount:

```
Host: /var/cache/ci/cargo    (survives all VM lifecycles)

Runner 1  →  cache miss  →  downloads  →  writes to host mount
Runner 2  →  cache hit   →  reads from host at near-native speed
Runner 3  →  cache hit   →  same
```

### Why the Speed Advantage Is Real

virtio-fs in DAX (Direct Access) mode maps host filesystem pages directly into
guest memory using shared memory — the guest reads straight from host page cache
with no data copy and no serialization. A `cargo build` reading `~/.cargo/registry`
from a host NVMe at 3 GB/s is meaningfully faster than pulling the same data from
S3 at 100–300 MB/s over the network. For dependency-heavy workloads (Rust, npm,
Maven) this is the hottest path to optimize.

Multiple VMs can mount the same host directory read-only simultaneously with no
contention.

### Practical Layout

```
[Host Machine]
  ├── /var/cache/ci/cargo/registry   ← persistent across all runner VMs
  ├── /var/cache/ci/npm
  ├── /var/cache/ci/pip
  └── /var/cache/ci/docker-layers    ← read-only OCI layer cache
         │
         ├── [Runner VM 1]  ←── virtio-fs mount  (fast path, intra-host)
         ├── [Runner VM 2]  ←── virtio-fs mount
         └── [Runner VM N]  ←── virtio-fs mount

[Network Cache Tier]  ←── fallback for cross-host misses (S3, Artifactory)
```

On a cache miss in the virtio-fs mount, the runner falls through to the network
tier, downloads, and writes back to the host mount — warming it for future
runners on the same host.

### Complications to Solve

#### Write Contention
Two runners resolving the same uncached dependency simultaneously will race.
Most package managers (`cargo`, `npm`, `pip`) already write atomically via
temp-file-then-rename, which is safe on POSIX. For tools that do not, the
options are:
- Mount shared cache **read-only** inside the VM; write new entries to a local
  temp dir and sync back to the host at job end via a separate write path.
- Rely on virtio-fs POSIX `flock` support (present in virtiofsd) — only works
  if the cache client actually uses `flock`.

#### Cache Poisoning
A compromised runner can write malicious content into the shared host cache,
affecting every subsequent runner on the same host. Mitigations:
- Namespace cache directories per repo and branch
  (`/var/cache/ci/<org>/<repo>/<branch>/cargo`).
- Verify content hashes on read (same model GitHub Actions cache uses with
  cache keys).
- Mount cache directories read-only for all but the owning job; only allow
  writes through a privileged host-side daemon that validates content before
  committing.

#### Host Disk Pressure
A shared cache with no eviction grows unbounded. At 70 concurrent runners per
host resolving different dependency versions, disk fills quickly. A cache
eviction daemon on the host running LRU eviction by last-access time on the
directory entries is required.

#### Cross-Host Cache Miss
virtio-fs is host-local. A runner on host A cannot read the cache built by a
runner on host B. For a multi-machine fleet the network tier (S3, Artifactory,
self-hosted registry) remains necessary for cross-host hits. virtio-fs wins only
the intra-host layer — but that layer is the hot path for repeat builds on the
same machine.

### Comparison with Firecracker

| | libkrun + virtio-fs | Firecracker |
|---|---|---|
| Intra-host cache reads | Near-native disk speed (DAX shared memory) | Must copy through vsock agent |
| Concurrent readers | Multiple VMs, no copy | One at a time through agent protocol |
| Cache persistence | Host directory survives VM lifecycle | Must explicitly sync out before destroy |
| Cross-host cache | Network tier required | Network tier required (same) |
| Security: cache poisoning | Requires namespace + hash verification | Same risk if agent accepts writes |

---


## 11. iii/sandbox_daemon — Deep Dive and Combined Architecture


Source: `https://github.com/iii-hq/iii/tree/main/crates/iii-worker/src/sandbox_daemon`

### What iii sandbox_daemon Is

A libkrun-based microVM runtime embedded inside `iii-worker`. When `iii-sandbox`
appears in `config.yaml`, the engine starts `iii-worker sandbox-daemon` as a
subprocess that registers 16 `sandbox::*` triggers. Each sandbox boots in a few
hundred milliseconds, executes commands in isolation, and is reaped on stop.

The key implementation files are `create.rs` (VM boot), `adapters.rs` (the
`IiiWorkerLauncher` that spawns `__vm-boot`), `exec.rs` (command dispatch),
`overlay.rs` (per-sandbox filesystem layout), and `mod.rs` (daemon lifecycle).

### Architecture

```
[iii-worker daemon]
    └── spawn (bare, unjailed — current model)
          └── [__vm-boot child process]
                └── libkrun.so
                      └── [Guest VM]
                            └── iii-init / init.krun (PID 1)
                                  └── shell.sock listener
                                        └── sandbox::exec commands
```

### What iii Solves That Others Don't

#### 1. exec Into a Running VM
The single biggest CI blocker with `crun+libkrun` (issue #2090) is that there is
no mechanism to inject new processes into a running microVM. iii solves this by
shipping a custom init binary (`init.krun`) into the guest rootfs that runs as
PID 1 and listens on a Unix socket (`shell.sock`). `sandbox::exec` sends commands
over that socket. Multiple sequential exec calls against the same running VM work
correctly. This is the core architectural innovation.

#### 2. Per-Sandbox Rootfs Isolation (overlayfs)
Each sandbox gets its own overlayfs layout:
```
/tmp/iii-sandbox/<uuid>/
  ├── upper/    ← writable tmpfs layer (per-VM, ephemeral)
  ├── work/     ← overlayfs work dir
  └── merged/   ← unified view handed to libkrun as rootfs
```
The shared read-only rootfs is the lower layer. Each VM's writes land in its own
`upper/`. On `sandbox::stop`, `cleanup()` removes the entire sandbox dir. No
writes from one sandbox are visible in another.

#### 3. VM Orphan Prevention (Lifeline Pipe)
A pipe is created between the daemon and each `__vm-boot` child before spawn.
The write end is stored in the sandbox's registry entry. If the daemon dies by
any means — including SIGKILL — the kernel closes the pipe, the child detects
EOF, and the VM self-terminates. VMs cannot outlive the daemon as orphaned
processes holding memory.

#### 4. Network Off by Default
The `network` field on `sandbox::create` defaults to `false`. The VM receives no
network interface unless explicitly opted in per sandbox.

#### 5. Fail-Closed Image Allowlist
An empty `image_allowlist` denies all `sandbox::create` requests with `S100`.
Only images explicitly listed by the operator can be booted.

### What iii Does NOT Solve

| Concern | Status |
|---|---|
| Host-level VMM process isolation | ❌ `__vm-boot` runs with the daemon's full host privileges — no namespace jail |
| virtio-fs path traversal on guest kernel escape | ❌ No mount namespace restriction on the launcher |
| VMM in same security context as host | ❌ Confirmed by libkrun maintainer (`slp`, libkrun discussion #538, Feb 2026): guest escape = host process access at the daemon's privilege level |

The libkrun maintainer's own answer to this (verbatim, from discussion #538):
> *"All in all, would you say the safest way to run untrusted workloads with
> libkrun (with virtio-fs/vsock/gpu enabled) would be to spawn libkrun inside a
> runc/crun container on the host for user/network/mount namespacing, and inside
> the guest, run the workload as a non-root user?"*
>
> **slp (libkrun maintainer):** *"Yes."*

---

## 12. The Combined Model: iii + Namespace Jail

Combining iii's exec-over-socket approach with `crun`-style Linux namespace
jailing around the VMM process closes every significant security gap while
retaining iii's CI-specific advantages.

### Architecture

```
[iii-worker daemon]
    └── spawn inside namespace jail
          ├── CLONE_NEWUSER  (unprivileged user namespace)
          ├── CLONE_NEWNS    (mount namespace — sees only sandbox merged/ + shell.sock dir)
          ├── CLONE_NEWNET   (empty network namespace unless network=true)
          ├── cgroups        (CPU + memory hard limits)
          └── [__vm-boot] (jailed)
                └── libkrun.so
                      └── [Guest VM]
                            └── iii-init (shell.sock)
```

The daemon and the jailed `__vm-boot` still communicate via `shell.sock`. The
socket directory is bind-mounted into the jail's mount namespace before
`pivot_root`. Unix sockets are identified by kernel inode, not path, so both
sides refer to the same socket object correctly.

### What the Combined Model Solves

| Problem | How |
|---|---|
| exec into running VM | iii-init socket protocol — unchanged |
| Per-sandbox rootfs isolation | overlayfs per-VM upper layer — unchanged |
| VM orphans on daemon crash | lifeline pipe — unchanged |
| Host VMM process isolation | User + mount + net namespaces jail the libkrun process |
| virtio-fs path traversal | Mount namespace limits the jailed VMM to only the sandbox overlay dirs — it physically cannot resolve host paths outside |
| Network isolation | CLONE_NEWNET — empty by default, enforced at kernel level |
| Resource abuse | cgroups on the jailed VMM process |
| Syscall surface | seccomp filter on `__vm-boot` |

### Three Engineering Complications

#### Complication 1: overlayfs Inside a User Namespace

overlayfs requires `CAP_SYS_ADMIN`. Inside an unprivileged user namespace you
cannot mount it directly. Three solutions:

- **`fuse-overlayfs`** — FUSE implementation that works unprivileged. ~15–30%
  metadata overhead. Correct and portable.
- **Privileged helper** — small setuid binary mounts overlayfs, then drops
  back. This is the Podman approach.
- **Pre-mount on host, bind-mount merged/ into jail** — the daemon mounts
  overlayfs on the host as it already does, then bind-mounts only the `merged/`
  directory into the jail's mount namespace. The VMM sees one flat directory
  (`/sandbox-root`), not the overlay stack. Cleanest for this use case.

#### Complication 2: shell.sock Path Visibility

The daemon references `shell.sock` at a host path
(`/tmp/iii-sandbox/<uuid>/shell.sock`). The jailed VMM is in a separate mount
namespace. Fix: before `pivot_root`-ing the jail, bind-mount the specific sandbox
directory into the jail. The daemon uses the host-side path; the VMM uses the
jail-side path. Both are the same kernel socket inode — correct across mount
namespaces.

#### Complication 3: macOS Has No Namespaces

`CLONE_NEWUSER`, `CLONE_NEWNS`, `CLONE_NEWNET` do not exist on macOS. The
combined model only applies on Linux. Platform split:

```rust
#[cfg(target_os = "linux")]
fn spawn_jailed(params: &BootParams) -> Result<BootHandle> {
    // full namespace + cgroup + seccomp jail
}

#[cfg(target_os = "macos")]
fn spawn_jailed(params: &BootParams) -> Result<BootHandle> {
    // hypervisor boundary only — no namespace layer available
    spawn_bare(params)
}
```

macOS `Hypervisor.framework` provides a real hardware VM boundary. Running a
trusted guest kernel with workloads as non-root inside the VM is a reasonable
security posture for internal CI on developer machines, even without the
namespace layer.

### Combined Model vs Firecracker + Jailer

| Feature | Firecracker + Jailer | iii + Namespace Jail |
|---|---|---|
| Jailed VMM process | ✅ | ✅ |
| User namespace | ✅ | ✅ |
| Mount namespace (restricted rootfs) | ✅ | ✅ |
| Network namespace | ✅ | ✅ |
| cgroups | ✅ | ✅ |
| Seccomp | ✅ (~40 syscalls, very tight) | ⚠️ (wider — libkrun needs more syscalls than Firecracker) |
| exec into running VM | ❌ (requires vsock agent build) | ✅ iii-init |
| virtio-fs directory sharing | ❌ | ✅ |
| Host cache via virtio-fs | ❌ | ✅ |
| macOS support | ❌ | ✅ (weaker — no namespace layer) |
| Guest kernel flexibility | ✅ (user-supplied) | ⚠️ (libkrunfw bundled, no modules) |

The seccomp gap is real: libkrun's syscall footprint is larger than Firecracker's
because it handles user-space networking, virtio-fs, and GPU devices. A seccomp
profile for libkrun cannot be as tight as Firecracker's ~40-syscall list. But
it is still far better than no filter — you can deny the most dangerous classes
(`ptrace`, `process_vm_writev`, `kexec_load`, etc.) while allowing libkrun's
actual working set.

---

## 13. Kata Containers — What It Actually Is and CI Suitability

### What Kata Containers Is

Kata Containers is commonly described as a "container runtime" but that is
misleading. It is more precisely a **microVM-per-container runtime** that is
OCI-compatible at the API surface. Each "container" runs inside its own
dedicated VM with a full unstripped Linux guest kernel. The VM is not shared
between containers in the same pod (unlike Docker containers sharing a host
kernel).

```
[containerd / CRI-O]
    └── kata-shim (OCI-compatible shim)
          └── hypervisor (Firecracker, Cloud Hypervisor, or QEMU)
                └── [Guest VM — full Linux kernel]
                      └── kata-agent (listens on vsock)
                            └── container workload (OCI namespace isolation inside the VM)
```

The OCI surface means you use `docker run`, `kubectl`, and standard container
tooling and they transparently boot a VM instead of a container. The VM is the
isolation boundary — not the container namespace.

### How Kata Exec Works

Kata solves the exec problem — `docker exec` / `kubectl exec` work — by running
a `kata-agent` daemon inside the guest that listens on a `virtio-vsock` channel.
The host shim forwards exec requests over vsock. This is the same model iii uses
(`iii-init` + `shell.sock`) but over vsock instead of a Unix socket.

### Boot Time

Kata's boot overhead depends on the backend VMM:

| Backend | Boot time |
|---|---|
| Firecracker (Kata) | 125ms |
| Cloud Hypervisor (Kata) | 200ms |
| QEMU (Kata, default) | 400–500ms |

This is cold-start latency from kubelet pod creation event to container ready.
For CI jobs that take minutes to run, 200–500ms is negligible. For sub-second
serverless invocations it matters more.

### Is Kata Well Suited for Ephemeral CI Runners?

Partially, but with real gaps:

**Kata is good for:**
- Linux-only CI fleets running in Kubernetes
- Workloads where OCI compatibility matters (existing container images work
  without modification)
- Environments where the security team mandates VM-level isolation but the
  platform team wants to keep using standard container tooling
- DinD (Docker-in-Docker) — Kata runs a full unstripped kernel, so overlayfs,
  loop devices, and iptables all work inside the VM

**Kata is problematic for CI because:**

1. **Kubernetes dependency**: Kata is designed to run inside Kubernetes as a
   `RuntimeClass`. Running it outside Kubernetes as a standalone ephemeral
   runner spawner is possible but requires stripping away a lot of the
   Kubernetes-specific orchestration code. It is not designed for the pattern
   where a control plane directly spawns one VM per CI job.

2. **No macOS support**: Kata requires KVM on Linux. macOS developer machines
   cannot run Kata.

3. **No virtio-fs host cache sharing** (in the Firecracker backend): When using
   Kata with Firecracker as the backend, the filesystem model is block-device
   based. The shared host cache idea — mounting a persistent host directory into
   multiple VMs via virtio-fs at near-native speed — does not apply.

4. **Heavier operational surface**: Kata requires containerd (or CRI-O),
   kata-runtime, kata-agent inside the image, and a kata-shim per container.
   For a standalone CI runner, this is significant complexity compared to a
   single Rust binary that links libkrun.

5. **Image preparation**: Kata runs standard OCI images but the kata-agent must
   be present or injected. Building CI runner images that also include
   kata-agent adds a non-trivial layer to the image pipeline.

### Kata vs the Combined iii Model for CI

| Dimension | Kata Containers | iii + Namespace Jail |
|---|---|---|
| Guest kernel | Full, user-supplied | Bundled libkrunfw (no modules) |
| DinD support | ✅ (full kernel) | ⚠️ (no kernel modules) |
| exec into running VM | ✅ (kata-agent over vsock) | ✅ (iii-init over shell.sock) |
| macOS support | ❌ | ✅ (weaker isolation) |
| virtio-fs host cache | ❌ with Firecracker backend | ✅ |
| Standalone (no Kubernetes) | ⚠️ (designed for k8s) | ✅ |
| Operational complexity | High (containerd + kata stack) | Low (single binary) |
| Security boundary | VM + namespace (strong) | VM + namespace (equivalent on Linux) |
| Seccomp tightness | ✅ (Firecracker ~40 syscalls) | ⚠️ (libkrun wider footprint) |

### When to Choose Each

**Choose Kata if:**
- You are already running Kubernetes and want VM isolation without changing
  your container workflow
- DinD is a hard requirement and you cannot accept the no-modules limitation
- You are Linux-only and do not need macOS developer machine support
- Operational complexity is acceptable and you have a platform team to manage it

**Choose iii + Namespace Jail if:**
- You need macOS + Linux from the same codebase
- You want the virtio-fs shared host cache for dependency caching
- You want a self-contained binary without a Kubernetes dependency
- Your CI jobs do not require kernel modules (most application-layer jobs don't)

---

## 9. Relationship Between Projects

```
rust-vmm (shared crate ecosystem: kvm-ioctls, vmm-sys-util, ...)
   │
   ├── Firecracker (AWS)          — hardened standalone VMM daemon
   ├── Cloud Hypervisor (Intel)   — richer device model, OCI support
   ├── crosvm (Google)            — ChromeOS VMM
   └── libkrun (Red Hat)          — embedded library VMM
         │
         ├── krunkit              — Podman Machine backend on macOS
         ├── crun --handler krun  — OCI runtime with namespace wrapper
         ├── microsandbox         — AI agent sandbox SDK
         └── smolvm               — portable self-contained VM tool
```

Firecracker and libkrun are siblings, not parent/child. libkrun's README
acknowledges incorporating code from Firecracker and Cloud-Hypervisor, but
both draw from the same `rust-vmm` foundation.
