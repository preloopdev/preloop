# Container Runtime Comparison Report

Consolidated comparison of all tested container/microVM runtimes for running GitHub Actions workflows locally.

## Runtimes Tested

| Runtime | Type | What It Does |
| :--- | :--- | :--- |
| **agent-ci** (RedwoodJS) | Container-based control plane | Emulates GitHub's API, runs the official `actions/runner` binary inside a Docker container |
| **Docker (OrbStack)** | Host container engine | Runs containers directly on the host Docker daemon |
| **Microsandbox (`msb`)** | libkrun microVM | Boots a hardware-isolated VM per command via `libkrunfw` kernel |
| **Stock libkrun (`krunvm`)** | libkrun microVM | Raw `libkrun` VM CLI, boots OCI images as microVMs |
| **SmolVM** (`smol-machines/smolvm`) | QEMU/HVF microVM (macOS), Firecracker (Linux) | Full Linux kernel guest, supports nested Docker |

## Environment

- **Host:** macOS arm64 (Apple M4 Max, 16 CPUs, ~16 GiB RAM)
- **Docker Engine:** OrbStack v29.4.0
- **agent-ci runner image:** `ghcr.io/actions/actions-runner:latest` (official runner v2.335.1)

---

## Performance Comparison: Simple Container Execution

Time to execute `echo hello` inside a fresh container/VM (images pre-cached, 3-run average):

| Runtime | Avg Time (s) | Isolation Model | Notes |
| :--- | :--- | :--- | :--- |
| **Docker (OrbStack)** | **0.168** | Linux namespace (shared kernel) | Fastest — persistent daemon, no VM boot |
| **SmolVM In-VM Daemon** | **0.131** (warm) | Hardware VM + nested Docker | Warm runs faster than host TCP proxy; cold start ~1.7s |
| **krunvm** | **0.206** | Hardware VM (libkrun) | Includes VM create + start + destroy |
| **Microsandbox** | **0.222** | Hardware VM (libkrun) | Single-command ephemeral VM |
| **SmolVM Host Daemon** | **0.236** | Hardware VM + TCP proxy to host | TSI loopback adds ~0.07s proxy hop |
| **agent-ci** | **~6.1s** | Docker container + official runner | Boots full runner binary + job orchestration overhead |

### Why agent-ci is ~30x slower for simple tasks

agent-ci is not slow because Docker is slow. It is slow because it does **much more work** per job:

1. **Container creation** (~1s) — Creates a fresh Docker container from the `ghcr.io/actions/actions-runner` image for every job.
2. **Runner binary startup** (~2s) — The official C# `actions/runner` binary boots the .NET CLR runtime, loads assemblies, and initializes the worker process.
3. **Job protocol handshake** (~1s) — The runner registers with agent-ci's local API, acquires the job message, parses steps, and sets up the working directory.
4. **Step execution** (~0.2s) — The actual `echo hello` command.
5. **Cleanup & reporting** (~1s) — Timeline updates, log uploads, job completion reporting, container teardown.

The microVM runtimes skip all of this. They boot a VM, run a single command, and exit. There is no runner binary, no protocol, no job orchestration. They measure raw virtualization overhead only.

**This is not an apples-to-apples comparison.** agent-ci provides full GitHub Actions compatibility (env contexts, step outputs, `needs:` chains, service containers, caching). The microVM runtimes provide raw isolated execution.

---

## Feature Compatibility Comparison

| Feature | agent-ci | Docker | msb | krunvm | SmolVM |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `run:` shell steps | ✅ | ✅ | ✅ | ✅ | ✅ |
| `env:` scoping (job/step) | ✅ | ✅ | ✅ | ✅ | ✅ |
| `needs:` job dependencies | ✅ | N/A | N/A | N/A | N/A |
| Matrix strategies | ✅ | N/A | N/A | N/A | N/A |
| `services:` containers | ✅ | ✅ | ❌ | ❌ | ✅ (In-VM) |
| `container:` job | ❌ | ✅ | ❌ | ❌ | ✅ (In-VM) |
| `docker://` actions | ❌ | ✅ | ❌ | ❌ | ✅ (In-VM) |
| `actions/checkout` | ✅ | N/A | N/A | N/A | N/A |
| `actions/cache` | ✅ (~0ms) | N/A | N/A | N/A | N/A |
| `actions/setup-node` | ✅ | N/A | N/A | N/A | N/A |
| `docker build` / `docker run` | ✅ | ✅ | ❌ | ❌ | ✅ (In-VM) |
| Pause on failure + retry | ✅ | ❌ | ❌ | ❌ | ❌ |
| NDJSON event stream | ✅ | ❌ | ❌ | ❌ | ❌ |
| Hardware VM isolation | ❌ | ❌ | ✅ | ✅ | ✅ |
| Nested Docker daemon | N/A | N/A | ❌ | ❌ | ✅ |
| Host Docker via TCP proxy | N/A | N/A | ✅ | ✅ | ✅ |
| Unix socket sharing (virtio-fs) | N/A | N/A | ❌ | ❌ | ❌ |

---

## Workflow Test Results

### agent-ci (7 workflows)

| Workflow | Status | Time | Failure Reason |
| :--- | :--- | :--- | :--- |
| `20-host-docker-node-services.yml` | ✅ | 16.7s | — |
| `21-host-docker-build.yml` | ✅ | 6.1s | — |
| `22-host-docker-container-action.yml` | ❌ | 6.2s | `docker://` action download info not served |
| `23-host-docker-container-files.yml` | ❌ | 5.1s | `container:` job — OCI runtime create failed |
| `24-host-docker-service-ports.yml` | ❌ | 41.4s | `wget` not in runner image (workflow bug) |
| `25-agent-ci-test.yml` | ✅ | 6.5s | — |
| `26-agent-ci-comprehensive.yml` | ✅ | 16.1s | — |

### MicroVM Runtimes (5 benchmarks, 3 runs each)

| Benchmark | Docker | msb | krunvm | SmolVM Host | SmolVM In-VM |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Alpine echo | 0.168s | 0.222s | 0.206s | 0.236s | 0.131s (warm) |
| Node version | 0.166s | 0.250s | 0.224s | — | — |
| I/O mount | 0.171s | 0.243s | 0.214s | — | — |
| Parallel sleep | 1.242s | 1.276s | 1.258s | — | — |
| Nginx port | 0.269s | 1.254s | 1.616s | — | — |

---

## SmolVM Topology Deep-Dive (Host Daemon vs In-VM Daemon)

Tested inside a real `smol-machines/smolvm` VM running Alpine with Docker installed:

| Topology | Status | Run 1 | Run 2 | Run 3 | Average |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Host Daemon (B)** | ✅ SUCCESS | 0.241s | 0.261s | 0.206s | **0.236s** |
| **In-VM Daemon (A)** | ✅ SUCCESS | 1.697s | 0.155s | 0.107s | **0.131s** (warm) |

### Key Findings

1. **TSI Loopback Routing:** Under libkrun's Transparent Socket Impersonation, `127.0.0.1` inside the guest VM maps directly to host loopback. Setting `DOCKER_HOST=tcp://127.0.0.1:2375` inside the VM transparently reaches the host's `socat` proxy with zero configuration.

2. **In-VM Daemon is faster warm.** Once the nested Docker daemon has cached images, executing containers locally via Unix socket (~0.13s) beats the TCP proxy round-trip to the host daemon (~0.24s).

3. **In-VM Daemon has a cold start penalty.** The first run pulls the OCI image inside the VM (~1.7s). Subsequent cached runs drop to ~0.13s.

4. **Unix socket sharing is impossible over virtio-fs.** `/var/run/docker.sock` cannot be bind-mounted across the VM boundary. The socket file appears but connections fail. TCP proxying via `socat` is the proven workaround.

---

## Conclusions

1. **For full GitHub Actions compatibility** (actions, caching, matrix, services, retry): **agent-ci** is the most complete tool today. Its ~6s per-job overhead is the cost of running the real runner binary with full protocol fidelity. The two features it lacks (`container:` jobs and `docker://` actions) are control-plane gaps, not runner limitations.

2. **For raw isolated execution speed**: **Docker** (0.17s) and **SmolVM In-VM warm** (0.13s) are the fastest. The microVM runtimes add ~0.05–0.08s of virtualization overhead per command, which is negligible.

3. **For production multi-tenant isolation**: **SmolVM** is the only runtime that supports both In-VM Docker daemon (full `container:`/`services:` compatibility) and hardware VM isolation. On Linux, it defaults to Firecracker for even stronger jailing.

4. **For the aksh runner**: The container support implementation should target Host Daemon Mode first (using `socat` TCP proxy for microVM deployments), with In-VM Daemon Mode as the production isolation target once SmolVM or Firecracker guest images with pre-installed Docker are available.
