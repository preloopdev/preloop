# MicroVM Isolation and Krun Runtime

## Goal

Preloop should use libkrun-backed Linux microVMs as the execution boundary for local, self-hosted, and managed CI. The runtime must be macOS-first for local developer usage and Linux-hardened for self-hosted and managed execution.

## Runtime principle

Use a libkrun-shaped abstraction, not a hypervisor-agnostic abstraction.

```rust
trait KrunRuntime {
    async fn create_vm(&self, spec: KrunVmSpec) -> Result<VmHandle>;
    async fn exec(&self, vm: VmId, req: ExecRequest) -> Result<ExecResult>;
    async fn open_pty(&self, vm: VmId, req: PtyRequest) -> Result<PtyHandle>;
    async fn checkpoint_disk(&self, vm: VmId) -> Result<DiskCheckpoint>;
    async fn shutdown(&self, vm: VmId, mode: ShutdownMode) -> Result<()>;
    async fn reap(&self, vm: VmId) -> Result<ReapReport>;
}
```

This interface should expose libkrun concepts directly:

- rootfs and raw disks,
- virtio-fs mounts,
- vsock/console,
- TSI or virtio-net networking,
- CPU/memory sizing,
- guest kernel/profile selection,
- lifecycle and cleanup.

## Runtime implementations

| Runtime | Role | Notes |
|---|---|---|
| `DirectLibkrunRuntime` | production target | Owns libkrun FFI and all substrate knobs. |
| `MicrosandboxRuntime` | local prototype/possible alpha | Use if it passes Docker/service/exec/shell gates. |
| `CrunKrunRuntime` | Linux OCI experiment | Useful for OCI behavior and kernel experiments, not primary. |
| Official runner process provider | conformance | Not an isolation runtime. |

## Why direct libkrun remains important

Wrappers can accelerate the first demo, but Preloop's requirements are lower-level than a generic sandbox API:

- private in-guest Docker,
- service container networking,
- exact workspace overlay topology,
- mounted cache policies,
- vsock shell and control channel,
- host-side jail,
- kill/reap lifecycle,
- network mode switching,
- VM pools,
- and disk checkpoint/retry behavior.

If a wrapper hides these knobs, switch to direct libkrun earlier.

## Host-security reality

A microVM is not enough by itself. Libkrun proxies host resources for the guest; the VMM process must be treated as part of the guest trust domain. A guest escape or virtio-fs/network proxy bug can have the privileges of the host process that launched the VM.

Therefore Preloop needs host-side isolation around the VMM process.

## Linux worker jail

For self-hosted and managed CI, the Linux host launcher should provide:

```text
preloop-worker
  |
  +-- preloop-vmm-jailer
        - unique UID/GID per VM
        - private runtime directory
        - mount namespace
        - user namespace where viable
        - cgroups v2 CPU/memory/pids/io
        - seccomp profile
        - Landlock or path allowlist where useful
        - no host home mount
        - no Docker socket
        - no cloud metadata access by default
        - network namespace/proxy policy
        - lifeline pipe and parent-death cleanup
        - zombie VM reaper
```

Managed CI should not run arbitrary untrusted code without this jail.

## macOS local mode

Apple Silicon macOS is a core local target. Treat it as local developer isolation, not multi-tenant managed isolation.

macOS defaults:

- no implicit home mount,
- read-only repo mount,
- narrow path allowlists,
- fake tokens by default,
- no SSH-agent forwarding by default,
- constrained helper process where possible,
- explicit warning that macOS local mode is not equivalent to Linux host jailing,
- case-insensitive filesystem drift detection.

## Network modes

Expose network modes explicitly:

| Mode | Purpose |
|---|---|
| `off` | deterministic/untrusted code, no egress |
| `allowlist` | default for agent/untrusted PR mode |
| `proxy` | policy, logging, secret masking, replay hooks |
| `tsi` | simple libkrun transparent networking where safe enough |
| `virtio-net` | service-heavy workflows, private Docker networks |
| `record-replay` | later reproducibility mode |

Do not pretend all network modes have the same fidelity. Include the mode in the run metadata and fidelity score.

## Snapshot/checkpoint reality

Do not promise live RAM snapshots unless libkrun exposes a supported API for it. The practical design is:

- prewarmed VM pools,
- disk checkpoints,
- CoW disk clones,
- workflow state checkpoints,
- tool/cache warmup,
- and step-boundary replay.

When Preloop says “fork from failure,” it should mean:

```text
copy-on-write disk checkpoint + re-exec from step boundary
```

not live process memory fork.

## Required runtime smoke tests

Before Aksh integration:

```text
preloop vm boot --image ubuntu-24.04
preloop vm exec <vm> -- echo hello
preloop vm shell <vm>
preloop vm mount-ro . /host_ro
preloop vm overlay /host_ro /workspace --upper hybrid
preloop vm exec <vm> -- touch /workspace/ok
preloop vm exec <vm> -- touch /host_ro/should-fail   # must fail
preloop vm exec <vm> -- curl example.com --net=off   # must be denied
preloop vm stop <vm>
preloop vm reap
```

## Runtime decision gate

Run a one-week backend bakeoff:

| Test | Microsandbox | Direct libkrun | Notes |
|---|---|---|---|
| Boot on Apple Silicon | required | required | macOS local gold path |
| Boot on Linux/KVM | required | required | self-hosted/managed path |
| Exec command | required | required | baseline |
| Read-only workspace | required | required | security |
| Overlay workspace | required | required | CI writes |
| Vsock/PTY shell | required | required | debugging |
| Docker daemon | required | required | key wedge |
| `docker build` | required | required | real CI |
| `services: postgres` | required | required | service tests |
| Network off/allowlist | required | required | untrusted code |
| Reap zombie VM | required | required | production reliability |

If microsandbox fails Docker/service/exec/shell gates, switch local alpha to direct libkrun immediately.
