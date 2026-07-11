# MicroVM Container Isolation — Consolidated Benchmark Report

This report consolidates all benchmark results from Phase 0 testing of three real microVM runtimes and two runner platforms against Host Docker, comparing Host Daemon Mode (Topology B) vs In-VM Daemon Mode (Topology A), and validating real GitHub Actions workflows end-to-end.

## Runtimes Tested

| Runtime | Version | VMM Engine | Install Method |
| :--- | :--- | :--- | :--- |
| **Docker (OrbStack)** | 29.4.0 | OrbStack hypervisor | `docker context use orbstack` |
| **Microsandbox (`msb`)** | 0.6.2 | `libkrun` via `libkrunfw` | `curl -sSL https://get.microsandbox.dev \| sh` |
| **Stock libkrun (`krunvm`)** | 0.2.6 | `libkrun` via `libkrunfw` | `brew tap libkrun/krun && brew install krunvm` |
| **SmolVM (`smol-machines/smolvm`)** | 1.4.1 | `libkrun` via custom `libkrunfw` (cgroup2+overlayfs+bridge enabled) | `curl -sSL https://smolmachines.com/install.sh \| bash` |

## Environment

- **Host:** macOS arm64 (Apple M4 Max)
- **Hypervisor:** Apple Hypervisor.framework
- **Docker Engine:** OrbStack (16 CPUs, 15.65 GiB RAM)
- **Runs per benchmark:** 3

---

## Executive Summary

### Can each runtime run Docker containers?

| Runtime | In-VM Daemon (Topology A) | Host Daemon via socat (Topology B) |
| :--- | :--- | :--- |
| **Docker (OrbStack)** | N/A (is the host daemon) | N/A |
| **Microsandbox** | ❌ No — `libkrunfw` kernel lacks cgroups, loop devices, netfilter | ✅ Yes — requires `--net-default allow` for private network egress |
| **Stock libkrun (`krunvm`)** | ❌ No — same `libkrunfw` kernel limitations | ✅ Yes — open egress by default, works immediately |
| **SmolVM** | ✅ Yes — custom `libkrunfw` kernel with cgroup2, overlayfs, bridge, netfilter | ✅ Yes — TSI maps `127.0.0.1` directly to host loopback |

### Why In-VM Daemon fails on stock `libkrunfw`

All three microVM runtimes (SmolVM, microsandbox, krunvm) use `libkrun` + `libkrunfw` on macOS. The difference is the guest kernel config compiled into `libkrunfw`:

**Stock `libkrunfw`** (microsandbox, krunvm) is aggressively stripped for fast boot. It lacks:
- `CONFIG_BLK_DEV_LOOP` (loop devices for image layers)
- `CONFIG_CGROUPS` mounting (containerd requires cgroup2)
- `CONFIG_BRIDGE` / `CONFIG_NF_TABLES` (Docker bridge networking)
- `CONFIG_MODULES` (no kernel module loading at all)

**SmolVM's custom `libkrunfw`** includes all of these, enabling nested Docker at the cost of a slightly larger kernel (~13MB). Raw VM boot is still **~370ms** — competitive with the stock builds (~200-300ms).

### Why Unix socket bind-mounting fails

Unix domain sockets (`/var/run/docker.sock`) **cannot** be shared across the VM boundary via `virtio-fs`. The socket file appears in the guest filesystem but connection attempts fail because the guest kernel cannot route IPC calls to the host kernel's socket listener. The proven workaround is TCP proxying:

```bash
# Host side
socat TCP-LISTEN:2375,reuseaddr,fork,bind=127.0.0.1 \
  UNIX-CONNECT:/path/to/docker.sock &

# Guest side (libkrun TSI routes 127.0.0.1 to host loopback)
export DOCKER_HOST=tcp://127.0.0.1:2375
docker ps  # works
```

---

## Performance Comparison

### Benchmark 1: Direct Container Execution (`docker run --rm alpine echo hello`)

Measures cold-start container execution latency. All images pre-pulled.

| Runtime | Average Time (s) | Run 1 | Run 2 | Run 3 |
| :--- | :--- | :--- | :--- | :--- |
| **Docker (Host)** | **0.168** | 0.189 | 0.158 | 0.156 |
| **Microsandbox** | **0.222** | 0.248 | 0.207 | 0.211 |
| **Stock libkrun (`krunvm`)** | **0.206** | 0.211 | 0.198 | 0.209 |
| **SmolVM (Host Daemon, Topology B)** | **0.236** | 0.241 | 0.261 | 0.206 |
| **SmolVM (In-VM Daemon, Topology A)** | **0.131** (warm) | 1.697 (cold) | 0.155 | 0.107 |

### Benchmark 2: Node.js Runtime Load (`docker run --rm node:24-bookworm node -v`)

| Runtime | Average Time (s) |
| :--- | :--- |
| **Docker (Host)** | **0.166** |
| **Microsandbox** | **0.250** |
| **Stock libkrun (`krunvm`)** | **0.224** |

### Benchmark 3: Workspace I/O via virtio-fs Mount

| Runtime | Average Time (s) |
| :--- | :--- |
| **Docker (Host)** | **0.171** |
| **Microsandbox** | **0.243** |
| **Stock libkrun (`krunvm`)** | **0.214** |

### Benchmark 4: Parallel Execution (3 concurrent `sleep 1`)

| Runtime | Average Time (s) |
| :--- | :--- |
| **Docker (Host)** | **1.242** |
| **Microsandbox** | **1.276** |
| **Stock libkrun (`krunvm`)** | **1.258** |

### Benchmark 5: Nginx Port Forwarding

| Runtime | Average Time (s) | Status |
| :--- | :--- | :--- |
| **Docker (Host)** | **0.269** | ✅ |
| **Microsandbox** | **1.254** | ✅ (requires explicit `-- nginx -g "daemon off;"` command) |
| **Stock libkrun (`krunvm`)** | **1.616** | ✅ |

---

## Official Runner Inside MicroVMs

The official `actions/runner` v2.335.1 (Linux arm64 ELF) was executed inside each microVM runtime, running the 5 container conformance workflows against **GitHub.com** (`preloopdev/aksh-conformance-sample`).

### Test Topologies

| Runtime | Docker Mode | Runner Binary | VM Boot |
| :--- | :--- | :--- | :--- |
| **SmolVM (In-VM Daemon)** | Nested `dockerd --storage-driver=vfs` inside guest | ✅ Works (glibc/Ubuntu 24.04) | 2.5s |
| **Microsandbox (Host Daemon)** | `socat` TCP proxy to host Docker at gateway IP | ✅ Works (glibc/Ubuntu 24.04) | 0.3s |
| **krunvm (Host Daemon)** | `socat` TCP proxy to host Docker at 127.0.0.1 | ❌ Crashes (musl/Alpine, missing `__isnan`) | N/A |

### SmolVM In-VM Daemon Results

Runner runs inside a libkrun/HVF guest (custom `libkrunfw` with cgroup2+overlayfs+bridge) with nested `dockerd`. Full filesystem and network namespace sharing — the runner, Docker daemon, workspace, and containers all coexist inside the VM.

| Workflow | Status | Runner Time | Notes |
| :--- | :--- | :--- | :--- |
| `21-host-docker-build` | ✅ PASS | **8.0s** | `docker build` + `docker run` inside VM |
| `22-host-docker-container-action` | ✅ PASS | **5.5s** | `docker://alpine:3.20` action executed |
| `23-host-docker-container-files` | ✅ PASS | **7.6s** | `container:` job with `--cpus 1`, file commands work |
| `24-host-docker-service-ports` | ✅ PASS | **13.8s** | Nginx service, port 18080, curl verification |
| `20-host-docker-node-services` | ❌ FAIL | **258s** (timeout) | VFS storage driver too slow for ~1GB image pulls |

**4/5 passed.** The only failure is VFS performance pulling large images (Postgres + Node + Redis ≈ 1GB), not a compatibility issue. Fix: pre-bake images or use `overlayfs` snapshotter.

**Setup overhead:** 2.5s boot → 7.7s install deps → 5s dockerd start → 4s runner config ≈ **19s cold start**. Warm (VM already running): **~9s** per job.

### Microsandbox Host Daemon Results

Runner runs inside a `libkrun`/HVF guest. Docker commands go to the host's Docker daemon via `socat` TCP proxy at the VM's gateway IP (`172.16.0.x`).

| Workflow | Status | Runner Time | Notes |
| :--- | :--- | :--- | :--- |
| `21-host-docker-build` | ✅ PASS | **6.6s** | Host Docker executes the build |
| `22-host-docker-container-action` | ✅ PASS | **5.6s** | `docker://alpine:3.20` action pulled and run on host |
| `23-host-docker-container-files` | ❌ FAIL | — | `can't open '/__w/_temp/...sh'` — workspace bind-mount path mismatch |
| `24-host-docker-service-ports` | ❌ FAIL | — | Service containers publish on host, runner in VM can't reach localhost |
| `20-host-docker-node-services` | ❌ FAIL | — | Same workspace path translation issue |

**2/5 passed.** Host Daemon mode works for stateless Docker commands (`docker build`, `docker run`, `docker://` actions) but fails for `container:` jobs and `services:` because:
1. **Workspace path mismatch:** The runner creates scripts at `/__w/_temp/` inside the VM, but host Docker tries to bind-mount that path on the host where it doesn't exist.
2. **Service port isolation:** Service containers publish ports on the host's network, but the runner inside the VM can't reach `localhost:port` on the host without TSI (microsandbox uses gateway IP, not TSI).

### krunvm Host Daemon Results

| Workflow | Status | Notes |
| :--- | :--- | :--- |
| All workflows | ❌ FAIL | `config.sh` crashes — Alpine musl missing `__isnan`/`__isnanf` for `libcoreclr.so` |

**0/5 passed.** krunvm only supports Alpine (musl-based) images, but the official runner requires glibc symbols that `gcompat` doesn't fully provide. Additionally, krunvm panics with `InvalidAscii` on commands containing escaped quotes.

### Runner-in-VM Key Findings

1. **In-VM Daemon is the only topology that fully works** for `container:` jobs and `services:` — the runner, Docker, workspace, and job containers must share the same filesystem and network namespace.
2. **Host Daemon mode works for basic Docker** — `docker build`, `docker run`, `docker://` actions succeed because they don't need workspace bind-mounts.
3. **SmolVM TSI routes `127.0.0.1` → host**; microsandbox requires gateway IP detection (`172.16.0.x`); krunvm TSI also routes `127.0.0.1` → host but can't run the runner.
4. **krunvm is unusable** for the official runner due to Alpine/musl limitations.

---

## agent-ci Comparison

[`@redwoodjs/agent-ci`](https://github.com/redwoodjs/agent-ci) v0.x — runs the official `actions/runner` v2.335.1 inside an OrbStack Docker container, with its own local control plane.

| Workflow | Status | Duration | Notes |
| :--- | :--- | :--- | :--- |
| `20-host-docker-node-services` | ✅ PASS | **16.7s** | Node + Postgres + Redis services all healthy |
| `21-host-docker-build` | ✅ PASS | **6.1s** | `docker build` + `docker run` inside runner container |
| `22-host-docker-container-action` | ❌ FAIL | 6.2s | `docker://` action resolution not implemented in local API |
| `23-host-docker-container-files` | ❌ FAIL | 5.1s | OCI runtime create failed — DinD nesting limitation |
| `24-host-docker-service-ports` | ❌ FAIL | 41.4s | `wget` missing from minimal runner image |
| `25-agent-ci-test` | ✅ PASS | **6.5s** | Basic echo + env |
| `26-agent-ci-comprehensive` | ✅ PASS | **16.1s** | 3 jobs with env scoping and `needs:` chain |

**4/7 passed (2/5 container workflows).** agent-ci handles `services:` containers and basic `docker` commands but lacks `docker://` action support and `container:` job mode — the exact features aksh's Phase 1 is building.

---

## Architectural Findings

### 1. TSI Loopback Routing
Under libkrun's Transparent Socket Impersonation, `127.0.0.1` inside the guest VM maps directly to the host's loopback interface. This allows zero-config TCP proxying without gateway IP detection or private network egress rules.

### 2. Microsandbox Egress Filtering
`microsandbox` blocks connections to private IP ranges by default. Reaching the host's `socat` proxy requires `--net-default allow` or explicit `--net-rule` entries. This is a security feature, not a bug.

### 3. SmolVM In-VM Daemon Cold Start
The first `docker run` inside a SmolVM guest takes **~1.7s** because the nested daemon must pull the OCI image over the network into its isolated VFS store. Subsequent runs with cached images drop to **~0.13s**, which is actually **faster** than Host Daemon Mode (~0.24s) because it avoids the TCP proxy hop.

### 4. krunvm Setup Overhead
`krunvm` requires a dedicated case-sensitive APFS volume on macOS (`diskutil apfs addVolume disk3 "Case-sensitive APFS" krunvm`). This is a one-time setup cost but adds friction compared to `microsandbox` and `smolvm` which work out of the box.

### 5. Workspace Path Translation is the Hard Problem
When using Host Daemon mode, the runner creates workspace files at paths like `/__w/_temp/script.sh` inside the VM, but host Docker cannot bind-mount those paths because they don't exist on the host filesystem. This is the fundamental reason In-VM Daemon mode is required for `container:` jobs — everything must share a single filesystem view.

### 6. The Runner Needs glibc
The official `actions/runner` ships a .NET runtime (`libcoreclr.so`) that requires glibc. Alpine/musl guests (krunvm) cannot run it even with `gcompat`. This eliminates krunvm as a viable runtime for the official runner.

### 7. Nested Docker Networking & Storage Driver in libkrun (TSI Limitation)
During testing of `docker build` workloads inside a SmolVM guest:
1. **Bridge networks fail outbound DNS/TCP/UDP:** Because libkrun has no real network interfaces (no `eth0`) and uses Transparent Socket Impersonation (TSI) at the guest system call level, raw packets routed over `docker0` bridge networks do not have outbound connectivity.
   +- *Fix:* Run builds and containers with `--network host` (which uses the VM's network namespace, intercepted by TSI).
2. **VFS driver causes extreme performance degradation & crashes:** Running nested `dockerd` over the guest rootfs (which is an overlayfs mount) requires using the `vfs` storage driver. This causes high write amplification and latency: `npm ci` during a UI build took 3m48s and failed due to NPM timeouts (`Exit handler never called`).
   +- *Fix:* Bind-mount `/var/lib/docker` to a real ext4 partition inside the guest (such as `/storage` / `/dev/vda`), enabling the native **`overlay2`** driver.
   +- *Result:* Re-running with `overlay2` + `--network host` resolved the network failure and sped up builds by **more than 10x** (API image built in **9.7s**, UI image built in **16.9s**).
---

## Cross-Platform Compatibility Matrix

| Capability | SmolVM (In-VM) | SmolVM (Host) | Microsandbox (Host) | krunvm (Host) | agent-ci (Docker) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Run official runner binary | ✅ | ✅ | ✅ | ❌ (musl) | ✅ |
| Basic `docker build`/`run` | ✅ | ✅ | ✅ | ❌ | ✅ |
| `docker://` actions | ✅ | ✅ | ✅ | ❌ | ❌ |
| `container:` jobs | ✅ | ❌ (paths) | ❌ (paths) | ❌ | ❌ |
| `services:` containers | ✅ | ❌ (ports) | ❌ (ports) | ❌ | ✅ |
| Hardware VM isolation | ✅ | ✅ | ✅ | ✅ | ❌ |
| VM boot time (raw) | 0.37s | 0.37s | 0.3s | 0.2s | N/A |
| Cold job execution (with image) | ~19s | ~8s | ~7s | N/A | ~6s |

---

## Decision: SmolVM as the VM Substrate

**SmolVM is the selected microVM runtime** for both local (macOS/Linux) and remote Linux production, pending KVM validation.

### Why SmolVM

- **Same VMM as microsandbox/krunvm** (`libkrun` + Hypervisor.framework on macOS, KVM on Linux) but ships a custom `libkrunfw` kernel with cgroup2, overlayfs, bridge, netfilter, and loop device support — the exact features Docker needs.
- **Only runtime where nested Docker works.** 4/5 container conformance workflows passed with the real official `actions/runner` inside a SmolVM guest. Microsandbox and krunvm cannot run nested Docker due to their stripped kernel.
- **Single tool for local and production.** Same binary and VM model on macOS (HVF) and Linux (KVM). No separate toolchain per platform.
- **Raw VM boot: ~370ms.** Competitive with stock libkrun (~200-300ms) despite the richer kernel. The 2.5s measured earlier was Ubuntu image layer extraction, not boot.
- **Built-in production primitives.** Egress filtering (`--allow-host`, `--allow-cidr`), secrets (`--secret-env`/`--secret-file` with late-binding), SSH agent forwarding, GPU passthrough, HTTP API for programmatic control, `.smolmachine` packs for pre-baked images.

### Pending: Linux KVM Validation

All testing so far was macOS/HVF on Apple Silicon. Before committing to production, validate on Linux KVM:

| Test | What to verify |
| :--- | :--- |
| Boot time | Raw VM boot on KVM — target ≤200ms (libkrun on KVM should be faster than HVF) |
| Nested Docker | `dockerd` starts, containers run, bridge networking works inside KVM guest |
| overlayfs snapshotter | Use `overlayfs` instead of `vfs` for Docker storage — critical for image pull performance |
| virtio-fs throughput | Workspace I/O latency and throughput under KVM vs HVF |
| Multi-tenant density | How many concurrent job VMs per host, memory overhead per VM |
| x86_64 guests | Verify x86_64 guest support on x86_64 KVM hosts (macOS is arm64-only) |
| Cleanup reliability | No orphaned VM processes after job completion/timeout/OOM |

### SmolVM-Specific Limitations

1. **x86_64 not supported on macOS.** Hypervisor.framework only runs arm64 guests. Workflows needing x86_64 images/binaries must run on Linux KVM hosts. This is a platform constraint, not a SmolVM bug.

2. **Image layer caching across ephemeral VMs.** Each ephemeral VM starts with empty Docker state. Pulling large images (postgres:16 ~400MB, node:24 ~350MB) through `vfs` inside the VM caused the only workflow failure (258s timeout). Solutions:
   +- Pre-baked `.smolmachine` packs with common images already cached (`machine create --from`)
   +- Shared read-only OCI layer store mounted via virtio-fs from host
   +- Switch from `vfs` to `overlayfs` snapshotter (using our bind-mount to `/storage` to run `overlay2` natively)

3. **DNS propagation to nested containers (libkrun TSI).** Default container bridge networks have no default route under TSI. We must:
   +- Configure `/etc/docker/daemon.json` inside the guest with `{"dns": ["8.8.8.8", "1.1.1.1"]}`
   +- Or run nested Docker builds and containers using host network mode (`--network host`) so TSI intercepts the socket calls.

4. **No VM pool manager.** SmolVM provides the VM lifecycle primitives (`create`/`start`/`exec`/`stop`/`delete`) and an HTTP API, but there's no pool of pre-booted VMs with Docker ready. Cold start is ~370ms boot + 8-19s for Docker + image setup. Production needs a warm pool to amortize this to near-zero per job.

5. **Network isolation per job is manual.** SmolVM has egress filtering (`--allow-host`, `--allow-cidr`, network-off-by-default) but there's no automatic per-job network policy. Each job VM must be configured with appropriate egress rules to prevent cross-VM communication or data exfiltration.

6. **`smolvm machine cp` doesn't support directories.** File copy is file-only; directories must be shared via `-v` volume mounts. This affects runner binary injection — use volume mounts or pre-baked images instead of runtime copy.

7. **Docker group setup required in-guest.** The official runner refuses to run as root, but `dockerd` creates a root-owned socket. The runner user must be added to the `docker` group inside the VM. Pre-baked images should include this.

8. **SmolVM on Linux KVM is unproven at scale.** Firecracker is battle-tested at AWS Lambda/Fargate scale. SmolVM's KVM backend exists but has no known large-scale production deployments. Risk mitigation: the `libkrun` VMM underneath is maintained by Red Hat and used in production (Podman machine), so the core is solid.
### Eliminated Alternatives

| Runtime | Reason eliminated |
| :--- | :--- |
| **Microsandbox** | Stock `libkrunfw` kernel lacks cgroups/overlayfs/bridge — cannot run nested Docker. Host Daemon mode breaks on `container:` jobs due to workspace path mismatch. |
| **krunvm** | Alpine-only images (musl) — official runner needs glibc. `config.sh` crashes on `__isnan`/`__isnanf` missing symbols. CLI panics on escaped quotes (`InvalidAscii`). |
| **agent-ci** | Different tool entirely (TypeScript, containerized runner). No VM isolation. Doesn't support `container:` jobs or `docker://` actions. Useful as a comparison baseline only. |
| **Firecracker** | Linux-only (no macOS). Excellent for production but can't serve the "same tool locally and in prod" goal. Could be a future Linux-only backend behind a trait boundary. |

---

## Firecracker vs. SmolVM (libkrun) for Docker CI

If we built the remote production runner host using **Firecracker** instead of SmolVM, here is how they would compare for running `docker.yml` or container-heavy CI:

### 1. Networking Parity (Winner: Firecracker)
- **SmolVM:** Uses TSI (Transparent Socket Impersonation) under libkrun. As shown in our test, guest bridge networks (`docker0`) have no packet-level routing, so nested containers **cannot** access the internet unless they run with `--network host` to expose them to TSI.
- **Firecracker:** Boots with a real TAP device (`/dev/net/tun`) mapped to the host's bridge/iptables NAT. This is a real packet-level interface (`eth0`). Inside a Firecracker VM, default Docker bridge networks work out of the box with full outbound access without needing `--network host`.

### 2. File System & Storage (Tie)
- **SmolVM:** Needs `/var/lib/docker` bind-mounted onto a real loop device or block device formatted as ext4 to run the native `overlay2` storage driver (otherwise falls back to the slow `vfs` driver).
- **Firecracker:** Exactly the same. You attach an ext4 disk block device to the microVM and mount it at `/var/lib/docker` to use `overlay2`.

### 3. Boot Time & Density (Winner: Firecracker)
- **SmolVM:** Raw boot is ~370ms. Memory overhead is ~5-10MB.
- **Firecracker:** Raw boot is ≤125ms (often ~10ms for minimal kernels). Memory overhead is <5MB per VM. Firecracker is much more optimized for packing thousands of VMs per host.

### 4. Cross-Platform DX (Winner: SmolVM)
- **SmolVM:** Runs on macOS (Hypervisor.framework) and Linux (KVM) using the exact same CLI/SDK.
- **Firecracker:** Runs on Linux only. Local macOS testing requires running inside a heavy QEMU Linux VM first, which breaks the "same tool locally" developer experience.

### Conclusion for Production
For **hosted production scale**, Firecracker is the superior choice due to its native TAP networking (nested Docker bridge networks work immediately) and faster boot. However, SmolVM's `--network host` workaround gives us the same image build capabilities locally on macOS and Linux with a single tool, making it the best hybrid development/CI substrate.



## Files

| File | Description |
| :--- | :--- |
| `docs/runner/microvm-benchmark-final.md` | This consolidated report |
| `docs/runner/runner-in-vm-report.md` | Detailed SmolVM runner-in-VM test data |
| `docs/runner/agent-ci-test-log.md` | Detailed agent-ci test results |
| `docs/runner/libkrun_benchmark_report.md` | Detailed per-run data: Docker vs msb vs krunvm |
| `docs/runner/smolvm_real_report.md` | Detailed per-run data: SmolVM Host vs In-VM Daemon |
| `docs/runner/benchmark_report.md` | DinD simulation baseline (not real microVMs) |
| `scripts/benchmark_libkrun.py` | Benchmark script: msb vs krunvm vs Docker |
| `scripts/benchmark_smol_machines.py` | Benchmark script: real smolvm topology comparison |
| `scripts/benchmark_runner_in_vms.py` | Official runner-in-VM benchmark script |
