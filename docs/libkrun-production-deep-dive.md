# Technical Analysis: libkrun Core & III Sandbox Integration on Production Linux Hosts

This document provides a deep, codebase-level architectural analysis of `libkrun` and `libkrunfw` under the assumption that the VMM is wrapped in the **III Sandbox** architecture (host-level namespace/cgroup jailing) and uses **`iii-init`** (guest-side socket agent) for execution. The target deployment environment is a production Linux bare-metal or VM host running KVM.

---

## 1. Target Architecture Definition

The evaluated architecture combines the following components to run isolated CI jobs:

```
[Host Orchestrator]
       │
       ▼ (spawns under CLONE_NEWUSER, CLONE_NEWNS, CLONE_NEWNET, cgroups)
[Jailed VMM Process]
       │ 
       ├── libkrun.so (in-process VMM)
       │     ├── virtio-fs server ── (points to merged/ overlayfs rootfs)
       │     └── virtio-vsock ────── (bound to host shell.sock)
       │
       ▼ (hardware VM boundary via KVM)
[Guest VM (libkrunfw kernel)]
       └── iii-init (PID 1 guest agent)
             └── listens on shell.sock for sandbox::exec commands
```

*   **Sandboxing wrapper (III Sandbox)**: The host launches the VMM process inside Linux namespaces (`CLONE_NEWUSER`, `CLONE_NEWNS`, `CLONE_NEWNET`) and `cgroups` (CPU and memory limits). This isolates the VMM process from the host system.
*   **Mount Isolation**: A restricted mount namespace mounts only the specific sandbox `merged/` overlay directory and the `shell.sock` socket directory, then executes `pivot_root`. The VMM physically cannot resolve host paths outside this boundary.
*   **Execution Channel (`exec` problem)**: A custom PID 1 init binary (`init.krun` / `iii-init`) is baked into the guest VM rootfs. It listens on a Unix domain socket (`shell.sock` or vsock) mapped from/to the host. The host worker sends command payloads over this channel, which the guest agent executes internally to run steps.

---

## 2. Codebase-Level Analysis of libkrun & libkrunfw

Auditing the Rust and C source code of the `containers/libkrun` and `containers/libkrunfw` repositories reveals several architectural characteristics, bottlenecks, and limitations that affect this production setup:

### 2.1 VMM Concurrency & Lock Contention
*   **Code Location**: `src/devices/src/virtio/mmio.rs` and `src/vmm/src/device_manager/`
*   **Mechanism**: `libkrun` models virtual devices using the `BusDevice` trait:
    ```rust
    pub trait BusDevice: AsAny + Send {
        fn read(&mut self, vcpuid: u64, offset: u64, data: &mut [u8]) {}
        fn write(&mut self, vcpuid: u64, offset: u64, data: &[u8]) {}
    }
    ```
*   **Bottleneck**: Devices (such as console, block, and fs) are registered on the MMIO bus wrapped in `Arc<Mutex<dyn BusDevice>>` to allow thread-safe multi-threaded access. On a multi-vCPU guest (e.g. a 16-core VM compiling a large parallel code base), multiple vCPUs will trigger VM-exits due to simultaneous MMIO writes to `virtio-block` or `virtio-fs`.
*   **Consequence**: Because the device logic is locked behind a `Mutex`, the vCPUs will experience lock contention and serialize waiting for the lock of the shared block or FS device. This degrades CPU utilization during parallel I/O-intensive workloads.

### 2.2 TSI (Transparent Socket Impersonation) Proxy Limits
*   **Code Location**: `src/devices/src/virtio/vsock/` and `libkrunfw` kernel source
*   **Mechanism**: TSI redirects network calls at the guest kernel socket syscall layer over `/dev/vsock` to VMM-side proxy submodules: TCP (`stream_proxy.rs`), UDP (`dgram_proxy.rs`), and Unix domain socket translation (`pipe_proxy.rs`).
*   **Limitations**:
    1.  **No Raw Sockets**: TSI only intercepts `SOCK_STREAM` and `SOCK_DGRAM` families. Raw sockets are completely unsupported by the proxy. Commands like `ping` or network diagnostics utilizing raw packets will fail.
    2.  **No Guest UDP Listening**: Listening on UDP sockets from inside the guest is unsupported by the proxy.
    3.  **Flow Control Choke**: The TSI proxy uses credit-based flow control (tracking `tx_credit` and `rx_credit` counters) to prevent the guest from overflowing the host VMM buffers. Under heavy throughput (e.g. pulling large docker layers or dependencies), the credit exhaustion window throttles performance.
    4.  **Nested Container Networking**: If a workflow runs nested containers inside the guest VM (e.g. via `runc` or `crun` inside the VM), these containers cannot establish standard virtual bridge networks because TSI intercepts socket calls, not raw Ethernet frames. The containers must share the VM's loopback namespace (`--network host` relative to the guest).
*   **Alternative**: Compile libkrun with `NET=1` (virtio-net) and run `passt` or `gvproxy` on the host per VM. This bypasses TSI but adds another process to manage and sandbox on the host.

### 2.3 virtio-fs I/O Performance & Security
*   **Code Location**: `src/devices/src/virtio/fs/`
*   **Mechanism**: `virtio-fs` runs a VMM-side FUSE server (`fs/server.rs`). The guest FUSE client translates filesystem calls into FUSE requests sent over virtio queues, which the VMM maps to host filesystem APIs (using `preadv`/`pwritev`).
*   **Bottleneck**: File operations (like `readdir`, `lookup`, or `write`) require a guest-to-host context switch. Workloads performing millions of metadata operations (such as `npm install` or cargo compilation of deep dependency trees) suffer significant performance degradation compared to native speeds.
*   **Security Gaps**: The VMM-side FUSE implementation maps FUSE paths directly to the host filesystem and does **not** sanitize or validate path traversal (e.g. escaping directory trees via parent path `..` resolution or symlink races). 
*   **III Sandbox Impact**: Running `libkrun` inside a mount namespace (`CLONE_NEWNS` + `pivot_root`) protects the host filesystem because the VMM physically cannot resolve paths outside the sandbox directories. However, the FUSE context-switching overhead remains.

### 2.4 Monolithic Guest Kernel (`libkrunfw`)
*   **Configuration**: The bundled Linux kernel in `libkrunfw` has `CONFIG_MODULES` disabled and `nomodules` hardcoded in `CONFIG_CMDLINE`.
*   **Limitations**: Workflows attempting to load kernel modules (`modprobe`, `insmod`) or use features that depend on modular loading will fail.
*   **Maintenance**: To customize the kernel configuration or enable module loading, you must clone `containers/libkrunfw`, edit the defconfigs, remove `nomodules` from the command line, run `make`, manually trigger `make modules` in the kernel source, and distribute the custom `libkrunfw.so` library across your production host fleet.

### 2.5 Intel TDX Sizing Limits
*   **Configuration**: `libkrun`'s Intel TDX variant is hardcoded to support a maximum of **1 vCPU** and **3072 MB** memory per guest instance in the VMM configuration.
*   **Limitations**: If you intend to use Intel TDX for secure, hardware-encrypted confidential execution in production CI, you cannot allocate multi-core configurations or memory footprints larger than 3GB to the build jobs, rendering parallel compilation ineffective.

---

## 3. Comparative Evaluation: libkrun + III Sandbox vs. Firecracker

| Vector | libkrun + III Sandbox (Guest Socket Exec) | Firecracker + vsock Agent (aksh's Substrate) |
| :--- | :--- | :--- |
| **VMM Process Isolation** | Jailed manually on the host using namespaces (`CLONE_NEWUSER`, `CLONE_NEWNS`, `CLONE_NEWNET`) and cgroups. | Jailed natively by the `jailer` daemon (drops privileges, chroots, assigns cgroups, and applies a ~40 syscall seccomp profile). |
| **Exec Channel** | `iii-init` listening on `shell.sock` (Unix socket mapping mapped through mount namespace). | Custom guest daemon communicating over `virtio-vsock` (no host filesystem paths exposed). |
| **Networking** | TSI (Transparent Socket Impersonation) or `virtio-net` + `passt`. | `virtio-net` + TAP devices bridged to host virtual interfaces (requires `CAP_NET_ADMIN` to configure). |
| **Storage / Directory Sharing** | Direct host directories mounted via `virtio-fs` (high metadata performance penalty). | Raw block devices only (`virtio-block`). No direct directory sharing. Files must be streamed over vsock. |
| **Kernel Management** | Bundled in `libkrunfw`. Rebuilding requires compiling the VMM library itself. | Standalone uncompressed ELF kernel (`vmlinux`). Can load precompiled cloud kernels. |
| **Concurrency Contention** | High (VMM uses `Mutex` locks on MMIO device dispatch). | Low (uses separate queue structures and lock-free rings). |
| **Platform Portability** | Works on Linux and macOS (Hypervisor.framework). | Linux KVM only. |
