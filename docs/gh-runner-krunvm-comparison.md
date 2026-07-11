# Technical Comparison: efrecon/gh-runner-krunvm vs. aksh

This document provides a deep, evidence-based architectural comparison between `efrecon/gh-runner-krunvm` and the `aksh` system (Rust control plane + Rust runner). The analysis evaluates both approaches across local developer loops and remote/production CI environments.

---

## 1. Architectural Overview & System Scope

| Dimension | `efrecon/gh-runner-krunvm` | `aksh` (Rust Control Plane & Runner) |
| :--- | :--- | :--- |
| **System Scope** | Host-level wrapper/provisioner scripts for the official runner. Does not implement a control plane. | Full reimplementation of both the GHA control plane (`aksh-runner-server`) and the GHA runner (`aksh-runner`). |
| **Control Plane** | Upstream GitHub (`github.com` or GitHub Enterprise Server). | Local lightweight Rust control plane server, projecting native REST + NDJSON APIs alongside compatible AzDO/Broker APIs. |
| **Runner Implementation** | Official C# `.NET` `actions/runner` (Listener/Worker). | Native Rust `aksh-runner` (Listener + Worker) utilizing a two-process model (IPC over stdin NDJSON). |
| **Orchestration Codebase** | Pure POSIX shell (`orchestrator.sh` and `runner.sh`). | Compiled Rust workspace (`aksh-runner-server`, `aksh-runner`, `aksh-gha-parser`, `aksh-gha-expressions`). |
| **Fidelity Strategy** | Relies on the official C# runner binaries for protocol and execution fidelity. | Native reimplementation of GHA expression evaluation, context assembly, workflow command parsing, and lifecycle APIs. |

---

## 2. Virtualization & Isolation Substrates

### `efrecon/gh-runner-krunvm`
*   **Substrate**: `libkrun` (a dynamic library VMM built on `rust-vmm` primitives) executed either via `krunvm` or the `krun` OCI runtime under Podman.
*   **Guest OS**: Prebuilt OCI container images (Ubuntu or Fedora) booted as microVMs.
*   **Hypervisor Interface**: KVM on Linux; macOS Hypervisor.framework.
*   **Process Model**: In-process execution. The VMM runs directly inside the calling process's memory space. If a guest exploits a hypervisor escape bug, it gains the host privileges of the orchestrating parent process. No built-in jailing, chrooting, or capability drops are applied.

### `aksh` (Rust Runner)
*   **Substrate**: Pluggable `RunnerProvider` model. Supports local host execution, host Docker container execution, or microVM/VM isolation.
*   **Production Substrate**: Firecracker microVMs are the preferred isolation engine for hosted/production mode.
*   **Sandboxing & Security**: Firecracker runs in a separate process and is launched via the `jailer` helper. The jailer drops privileges, chroots into a read-only rootfs, applies cgroups, and enforces an extremely restrictive seccomp profile (~30–40 allowed system calls). If a guest escapes the hypervisor, it remains trapped in the jailed host process.
*   **macOS / Linux Portability**: Natively cross-platform (Rust). Local mode runs on both Linux and macOS hosts with identical behavior. For VM isolation, Firecracker is Linux-only, while `libkrun` can be utilized as a macOS Hypervisor.framework provider.

---

## 3. Docker-in-Docker (DinD) & Container / Service Compatibility

A major limitation of running GitHub Actions inside microVMs is how nested container execution (`jobs.<id>.container:` and `jobs.<id>.services:`) is handled.

```mermaid
graph TD
    subgraph gh-runner-krunvm (Docker-less Podman Emulation)
        A[Official C# Runner] -->|Invokes docker CLI| B(docker CLI Shim)
        B -->|Injects --network host| C[Guest Podman Service]
        C -->|Runs nested container| D[Guest Podman Container]
        D -.->|No custom network/bridge| E[Guest Host Network]
    end
    
    subgraph aksh (Native Docker Engine in VM)
        F[aksh Rust Runner] -->|Invokes docker CLI| G[Guest Docker Daemon]
        G -->|Creates bridge network| H[github_network_uuid]
        G -->|Runs job & services| I[Docker Containers]
        I -->|Isolated bridge networking| H
    end
```

### `efrecon/gh-runner-krunvm`
*   **Kernel & Hypervisor Constraints**: 
    *   *Historical context*: When `gh-runner-krunvm` was originally written, the pre-bundled `libkrunfw` kernel lacked basic container primitives (like `overlayfs` and loop devices) entirely.
    *   *Modern libkrunfw state*: Although modern versions of `libkrunfw` (Linux 6.12+) compile in basic support for `overlayfs` and loop devices, a standard `dockerd` daemon remains blocked from running inside the VM because:
        1. **No kernel modules**: `CONFIG_MODULES` is disabled; all compiled features are built-in. If `dockerd` or network tools attempt to dynamically load missing drivers or kernel modules via `modprobe`, they fail.
        2. **Lack of `exec` support** (crun/libkrun issue #2090): Under `crun` + `libkrun` virtualization, there is no native way to inject a new process (`exec`) into a running container. Because `dockerd` runs tasks in separate namespaces, `docker exec` (which GHA relies on to execute steps within the job container) is entirely broken.
        3. **Rootless/Privileged VM constraints**: `krunvm` runs as an unprivileged, rootless hypervisor. Configuring loop devices and nested bridges inside a rootless guest VM requires permissions and capabilities that are highly complex to configure under user-space KVM bindings.
*   **Workaround**: Uses `podman` in the guest running in system-service emulation mode (listening on the standard `/var/run/docker.sock` socket).
*   **Networking Caveat**: Due to the missing kernel bridge and netfilter support inside the guest, nested containers cannot use container-specific networks. All containers must run with `--network host` relative to the guest.
*   **The CLI Shim**: Injects a custom `docker` shell wrapper (`docker.sh`) that intercepts arguments and appends `--network host` to command runs. This works for simple containers but breaks workflows using multi-container services with port collisions, custom network configurations, or specific volume mounts.

### `aksh` (Rust Runner)
*   **Architecture**: Docker-compatible first. The runner interacts natively with standard Docker APIs via process-based commands.
*   **Production VM Design**: Boots a guest VM with a full Linux kernel compiled with `CONFIG_OVERLAY_FS`, `CONFIG_BLK_DEV_LOOP`, `CONFIG_NF_TABLES`, and `CONFIG_BRIDGE`. A real `dockerd` stack runs inside the guest.
*   **Compatibility**: Supports native container setups. Creates isolated job networks (`github_network_<uuid>`), starts service containers with network aliases, and translates workspace mounts (`_work` -> `/__w`) without CLI wrappers or shims.
*   **Solving the `exec` Problem (VM-Local vs. Host-to-Guest)**:
    *   *Option A: Host-to-Guest Socket Injection (`iii-init` / `kata-agent` model)*: The runner remains on the host, and the container is launched as an isolated guest VM. A custom PID 1 init daemon (like `iii-init` listening on a host-binded Unix socket `shell.sock`, or `kata-agent` listening on `virtio-vsock`) is baked into the guest VM rootfs. When the host runner requests execution inside a container, it sends the command over the socket/vsock channel, which the guest agent executes internally.
    *   *Option B: VM-Bundled Runner & Docker Daemon (aksh-runner default)*: Rather than orchestrating guest VM containers from the host, the `aksh-runner` binary and the entire container execution stack (`dockerd`, `containerd`, `runc`) are bundled *together* inside a single guest VM per job. The runner runs natively inside the VM and calls `docker exec` against the VM's local Docker socket. The VM's native guest kernel namespaces execute the container commands locally, completely bypassing the host-to-guest hypervisor `exec` limitation.
*   **Enabling Kernel Module Loading (`CONFIG_MODULES=y`) under libkrun**:
    If `libkrun` is used as the VM substrate for hosted runner execution, kernel module loading can be enabled by compiling a custom version of `libkrunfw`:
    1. *Clone and Navigate*:
       ```bash
       git clone https://github.com/containers/libkrunfw.git
       cd libkrunfw
       ```
    2. *Modify Configurations*: Edit the guest kernel configuration files (e.g., `config-libkrunfw_x86_64` and `config-libkrunfw_aarch64`) to set:
       ```ini
       CONFIG_MODULES=y
       CONFIG_MODULE_UNLOAD=y
       ```
       Also, remove `nomodules` from the kernel command-line parameters (`CONFIG_CMDLINE`) in the corresponding configuration.
    3. *Compile the Kernel & Modules*: Run `make` to compile the library and download the kernel sources. Because the default Makefile does not automate module compilation, manually compile the module binaries (`.ko` files) inside the compiled kernel directory:
       ```bash
       make
       cd linux-<version>/ # Navigate to the downloaded kernel directory
       make modules
       ```
    4. *Install and Provision*:
       * Install the newly compiled `libkrunfw.so` (or `libkrunfw.dylib` on macOS) on the host system library path.
       * Copy the compiled module binaries (`.ko` files) from the build directory into the guest VM's filesystem under `/lib/modules/$(uname -r)/` so they are discoverable by `modprobe` at runtime.

---

## 3.5. Kernel Maintenance & Operational Overhead

Maintaining a custom guest kernel configuration and compiling libraries like `libkrunfw` is generally **not recommended** for standard production CI engineering teams. Doing so introduces significant operational debt:
1. **Security Patching (CVEs)**: You become responsible for tracking Linux kernel CVEs, backporting security patches, and rebuilding/redistributing the hypervisor library.
2. **Architecture Support**: You must compile, test, and maintain different builds for `x86_64` and `aarch64` hosts.
3. **Hypervisor Patches**: `libkrunfw` contains specific downstream patches (such as virtio mappings and TSI networking helpers). Upgrading the kernel version requires rebasing these custom patches, which is prone to build breakages.

### Recommended Alternatives to Avoid Kernel Maintenance

To achieve full container compatibility without maintaining a custom guest kernel:

*   **Approach 1: Out-of-Process VMMs (Firecracker / Cloud-Hypervisor)**
    *   *Why it avoids maintenance*: Firecracker and Cloud-Hypervisor do not bundle the kernel inside the VMM binary or library. They boot standard, uncompressed Linux ELF kernels (`vmlinux`) passed via command-line paths.
    *   *The Strategy*: You can download precompiled cloud-optimized kernels (such as official Ubuntu or Fedora cloud-kernel packages) or use standard builds maintained by microVM orchestration projects (e.g. E2B or Fly.io). No custom VMM compilation is needed.
*   **Approach 2: OCI-Compatible VM Runtimes (Kata Containers)**
    *   *Why it avoids maintenance*: The Kata Containers project compiles, maintains, and packages the guest kernels, initrd, and `kata-agent` binaries for you as standard releases.
    *   *The Strategy*: You consume their prebuilt assets directly, mounting them into your container engine (containerd/CRI-O) configuration.
*   **Approach 3: Host-Native Docker Execution (Trusted/Local)**
    *   *Why it avoids maintenance*: No guest kernels or virtualization are involved. `aksh-runner` runs natively on the host, sending tasks straight to the host's existing Docker daemon. Appropriate for internal, trusted developer workstations.
*   **Approach 4: Accept Monolithic libkrun Constraints**
    *   *Why it avoids maintenance*: You use the stock `libkrunfw` shipped by Red Hat or package managers.
    *   *The Strategy*: You accept that `dockerd` is unsupported, and you implement Podman emulation with `--network host` and custom CLI wrappers (the `gh-runner-krunvm` approach).

### When is a Custom Kernel Actually Necessary?

Compiling and maintaining a custom guest kernel is only justified for specialized infrastructure use cases:

1. **Sub-50ms Cold Starts (Boot Speed Optimization)**:
   * Standard distribution kernels compile many drivers as modules or include heavy boot-time hardware probing (taking 500ms to 2 seconds to initialize).
   * A custom kernel disables module loading (`CONFIG_MODULES=n`), removes all unused device drivers (leaving only basic `virtio` block, net, and vsock drivers), and bypasses hardware probing. This drops the guest boot latency to **5–15 milliseconds**.
2. **Hypervisor-Specific Network/FS Hooks**:
   * Some VMMs rely on custom kernel patches that are not present in upstream Linux kernels. For example, `libkrun` requires the **TSI (Transparent Socket Impersonation)** patch to transparently redirect Guest TCP/IP operations straight to Host socket syscalls without bridge or TAP device configurations.
3. **Host Hardware Pass-Through & GPU Acceleration**:
   * If the VM needs direct access to host hardware (GPUs, TPUs, or FPGAs) for workloads like ML training or GPU-accelerated builds, a custom kernel must be compiled with the host's exact kernel configuration (e.g., enabling `VFIO`, `IOMMU`, `virtio-gpu` with Vulkan/Metal backends, and compiling matching guest GPU drivers).
4. **Attack Surface Hardening**:
   * To prevent host compromise via guest-kernel privilege escalation, security teams compile highly restricted monolithic kernels. This involves disabling loadable module support entirely (`CONFIG_MODULES=n`) and stripping out unused subsystems (e.g. sound, legacy buses, unused filesystems, or debugging hooks like `kprobes`).
5. **High-Density Memory Minimization**:
   * Running thousands of concurrent microVMs per host requires reducing the memory overhead of the kernel itself. A stripped custom kernel fits in a 5MB image and runs with minimal page tables, saving tens of megabytes of RAM per guest instance.

### CI Workloads Requiring Guest Kernel Module Support

If the guest VM kernel completely disables loadable module support (`CONFIG_MODULES` not set / `nomodules` enabled), certain classes of GitHub Actions workflows will fail. The specific CI use cases that require dynamically loading modules or using kernel-level features include:

1. **Docker-in-Docker (DinD) & Multi-Container Networking**:
   * **Storage Layer**: The `overlay` module is required by `dockerd` to mount image layers.
   * **Networking Bridges**: Modules like `bridge` and `veth` are needed to create isolated container networks.
   * **Port Translation & Firewalls**: Docker relies on `iptable_filter`, `iptable_nat`, and `xt_conntrack` to perform Network Address Translation (NAT) and expose guest container ports.
2. **Kernel-Space Driver Development & Testing**:
   * Build jobs that compile custom Linux kernel drivers (`.ko` files) must be able to load and verify their compiled drivers using `insmod` / `modprobe` inside the test pipeline.
3. **VPN & Tunneling Configurations**:
   * CI steps that need to connect to private databases or endpoints via VPNs (e.g. mounting a `wireguard` interface or spawning an OpenVPN client requiring the `tun` module) must have the corresponding network tunneling modules loaded in the guest.
4. **Security Auditing, Tracing, & Observability (eBPF)**:
   * Running security agents (like Falco), network policy engines (like Cilium), or system profiling tools (like `bpftrace`) inside a workflow requires the guest kernel to compile and load eBPF bytecode or kernel probes (`kprobes`).
5. **Virtual Filesystem Mounts (FUSE)**:
   * Mounting cloud object stores (e.g. S3 buckets) as local folders using FUSE utilities (like `rclone mount` or `s3fs`) requires the guest kernel's `fuse` module to be loaded.

---

## 4. Local CI Experience

### `efrecon/gh-runner-krunvm`
*   **Developer Loop Setup**: Heavy local workstation requirements. Requires the host to have KVM enabled, `krunvm`, `buildah` (or `krun` and `podman`), `curl`, and `jq` installed. Easiest to run on Fedora; highly complex to configure on Debian/Ubuntu or macOS.
*   **Execution Overhead**:
    *   **Memory**: Each local runner loop boots a full microVM. Inside, the C# `.NET` runner process consumes 100–150 MB RSS before executing any job steps, plus guest kernel overhead.
    *   **Cold Start**: VM boot and runner registration take several seconds, as the runner must register against `api.github.com` via OAuth.
*   **Offline Capability**: Minimal. Relies on constant connectivity to `api.github.com` to receive jobs unless pointed at a local GHE or third-party server.

### `aksh` (Rust Runner)
*   **Developer Loop Setup**: Zero-dependency local execution. The developer launches `aksh-runner-server` and `aksh-runner` directly on the host (macOS or Linux).
*   **Execution Overhead**:
    *   **Memory**: The native Rust `aksh-runner` worker process consumes <15 MB RSS.
    *   **Cold Start**: Runs start instantly (sub-millisecond process spawn).
*   **Container execution**: Natively uses the host's local Docker Desktop or socket, bypassing VM overhead entirely in local/trusted mode.
*   **Offline Capability**: Complete. The server and runner run entirely offline on localhost. Workflows are submitted via `aksh-runner-client` directly to the local control plane.

---

## 5. Remote / Production CI Experience

### `efrecon/gh-runner-krunvm`
*   **Secrets Provisioning**: The host orchestrator generates a temporary `.env` file containing the runner registration token. This file is mounted into the guest VM via `virtio-fs`, sourced by the guest entrypoint script, and immediately deleted. While functional, it exposes secrets to the guest filesystem during the boot phase.
*   **Runner Lifecycle Management**: The orchestrator spawns persistent loops. Each loop registers an ephemeral runner (`--ephemeral`), waits for GitHub to schedule a job, executes the job, and tears down the VM. This creates a high volume of runner registration and deletion API calls on `api.github.com`.
*   **Security Bounds**: The hypervisor (libkrun) runs inside the same process context as the host shell loop. Guest escapes pose a direct threat to the host orchestrator process.

### `aksh` (Rust Runner)
*   **Secrets Provisioning**: The worker process receives the job payload directly from the listener process over an encrypted stdin NDJSON channel. The host never writes job secrets or tokens to shared filesystems or env files. The worker process handles log masking (`***`) natively.
*   **Runner Lifecycle Management**: The `aksh-runner-server` acts as the queue authority. The host orchestrator listens to job queue events (NDJSON stream). When a job is queued, it boots an isolated VM (Firecracker/KVM), feeds the job payload to the internal Rust worker, and tears the VM down.
*   **Security Bounds**: Sandboxing is enforced by the Firecracker `jailer` (chroot, cgroups, capability drops, and a 6-device minimal virtio model), providing robust multi-tenant protection.
*   **Scale Density**: Due to the low resource usage of the Rust runner (<15 MB RSS vs. 150 MB RSS for .NET), hosts can achieve a significantly higher concurrent job density on the same hardware.

---

## 6. Pros & Cons Matrix

### `efrecon/gh-runner-krunvm`

*   **Pros**:
    *   **Fidelity**: Uses the official GitHub `.NET` runner, guaranteeing 100% compatibility with standard workflow step execution.
    *   **Platform**: Supports macOS (via Hypervisor.framework) and Linux out-of-the-box.
    *   **No Code Maintenance**: No need to maintain a parser or expression engine; upstream changes to GHA yaml schemas are handled by the official runner.
*   **Cons**:
    *   **Host Scripting**: Relies on a complex chain of POSIX shell scripts that are difficult to unit-test and debug.
    *   **Docker Compatibility**: Lacks a real Docker daemon in the VM. The Podman + `--network host` shim breaks multi-container workflows, sidecars, and advanced networking.
    *   **Security**: libkrun lacks a hardened jailer wrapper by default, meaning guest-to-host escapes land in the orchestrator's process space.
    *   **Resource Footprint**: Heavy memory consumption per runner instance (150 MB RSS).

### `aksh` (Rust Control Plane & Runner)

*   **Pros**:
    *   **Lightweight**: Native Rust runner with a tiny memory (<15 MB RSS) and disk footprint.
    *   **True Sandbox Isolation**: Production-ready Firecracker VM isolation with a jailer wrapper, strict cgroups, and tight seccomp profiles.
    *   **Full Docker Fidelity**: Boots a complete Docker daemon stack inside the guest VM, allowing native multi-container job support (`container:` and `services:`) with isolated bridge networks.
    *   **Self-Contained Local Loop**: Offers a fully offline control plane and runner, enabling local testing without registering runners on `github.com`.
    *   **Secure Secrets Handling**: Direct stdin NDJSON IPC pipeline for job payloads; no host-mounted env files.
*   **Cons**:
    *   **Reimplementation Overhead**: Requires maintaining the expression engine, YAML parser, and protocol handlers to match upstream GHA behavior.
    *   **Evolving Compatibility**: Reimplemented components (e.g. Twirp log upload, cache/artifact APIs, certain expression behaviors) are still closing fidelity gaps.
