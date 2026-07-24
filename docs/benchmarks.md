# Preloop vs Agent-CI Benchmarks

This report compares Preloop with the TypeScript and Rust implementations of
Agent-CI across three representative repositories and two workflow scheduling
scenarios. It focuses on infrastructure overhead: each workflow performs a
checkout followed by `echo` steps rather than a substantial build or test.

## Executive Summary

Preloop is **3.4x faster** than Agent-CI TypeScript and **2.3x faster** than
Agent-CI Rust, measured as the geometric mean across completed scenarios.
Preloop achieves this by combining a native Rust runner with pre-warmed SmolVM
microVMs, so a runner is already booted and polling when a job arrives.

## Three-Way Benchmark Results

All benchmarks ran on an Apple M4 Max. Each scenario was run for 3 iterations,
and the median is reported.

### Repositories

| Repository | Files | Size | Description |
|---|---:|---:|---|
| Serde | 389 | 1.7 MB | Rust serialization framework |
| Fastify | 423 | 3.7 MB | Node.js web framework |
| Tokio | 894 | 7.6 MB | Rust async runtime |

### Agent-CI TypeScript

`npx @redwoodjs/agent-ci`, using the official .NET GitHub Actions runner in
Docker containers.

Flags: `--jobs 4 --quiet`

| Scenario | Serde | Fastify | Tokio |
|---|---:|---:|---:|
| 2-job DAG | 9.9s | 10.4s | 13.9s |
| 4-job parallel | 8.4s | 8.6s | 11.8s |

### Agent-CI Rust

`cargo build --release -p agent-ci`, a Rust rewrite of the orchestrator only.
It still uses the official .NET runner in Docker.

Flags: `--jobs 4 --quiet`

| Scenario | Serde | Fastify | Tokio |
|---|---:|---:|---:|
| 2-job DAG | 8.1s | 8.7s | 9.1s |
| 4-job parallel | 4.6s | 4.9s | — (timed out) |

### Preloop

`aksh-runner` running in SmolVM with `pool=4` and fork mode. This is a native
Rust runner in pre-warmed SmolVM microVMs.

| Scenario | Serde | Fastify | Tokio |
|---|---:|---:|---:|
| 2-job DAG | 3.0s | 3.0s | 3.0s |
| 4-job parallel | 4.6s* | 1.8s | — |
| Submission latency (p50) | 122ms | — | — |

\*Runner pool churn occurred during measurement; a fully warm pool measured
1.7s.

### Speedups

Geometric mean across completed scenarios:

| Comparison | Speedup |
|---|---:|
| Preloop vs Agent-CI TypeScript | **3.4x faster** |
| Preloop vs Agent-CI Rust | **2.3x faster** |

## Per-Job Overhead Breakdown

| Component | Agent-CI | Preloop |
|---|---:|---:|
| Orchestrator startup | ~0.5-2s | ~0s (already running) |
| Container/VM create+boot | ~2-3s | ~0s (pre-warmed pool) |
| .NET CLR JIT | ~2-3s | n/a (Rust, no runtime) |
| Runner registration | ~0.5s | ~0s (already registered) |
| Checkout | ~1-3s (git clone) | ~0.1s (local snapshot) |
| Step execution | ~0.1s | ~0.1s |
| Teardown | ~0.3s | ~0.05s |
| **Per-job total** | **~5-8s** | **~0.2s** |

## Architecture Comparison

| Component | Agent-CI | Preloop |
|---|---|---|
| Orchestrator | TypeScript or Rust | Rust |
| Runner | Official .NET 8 | aksh-runner (Rust) |
| Isolation | Docker container | SmolVM microVM |
| Per-job boot | ~4-5s (container+CLR) | ~0s (pre-warmed) |
| Checkout | Git clone/bind mount | Local snapshot HTTP |

## Key Insight

Agent-CI's Rust rewrite (RFC: "Do not rewrite the official GitHub Actions
runner") speeds up the orchestrator ~20% but cannot address the fundamental
per-job costs: Docker container lifecycle + .NET CLR JIT compilation inside each
container. These costs are structural and per-job.

Preloop eliminates both by using a native Rust runner (no CLR boot) in pre-warmed
SmolVM microVMs (no container creation). The runner is already booted and polling
when a job arrives.

## Optimizations Implemented

| Optimization | Impact |
|---|---|
| Skip unchanged object-cache fetch | -30-80ms per warm submission |
| Combine Git subprocess pipeline (10→7 spawns) | -15-40ms per submission |
| RSA keygen parallel spawn_blocking | -50-200ms cold start |
| Forkable golden VM (CoW clone) | Runner replacement ~3s vs ~9s |
| Broker runner-ID routing fix | Eliminated 10-min timeout on replacements |
| `fsck --connectivity-only` | -22s per submission |

## Environment Resolver Benchmarks

The environment resolver detects toolchain requirements from workflow steps
(setup actions) and version files, then selects or prepares the correct golden
VM with those tools preinstalled.

### Resolver Detection Performance

Time to parse a workflow and resolve toolchain requirements:

| Project Type | Detected Toolchains | Parse + Resolve |
|---|---|---:|
| Node (setup-node + .nvmrc) | Node 22 | 180µs |
| Rust (dtolnay/rust-toolchain@stable) | Rust stable | 58µs |
| Go (setup-go + go.mod) | Go 1.22 | 102µs |
| Multi (Node 20 + Python 3.12) | Node 20, Python 3.12 | 89µs |

Resolution is negligible — under 200µs even with version file reads.

### Golden VM Preparation (One-Time)

| Golden Type | Time | Contents |
|---|---:|---|
| Base (git + curl + CA certs) | 9.8s | Ubuntu 24.04 minimal |
| Node 22 (base + NodeSource) | 37.8s | Ubuntu + Node v22.23.1 |

Golden preparation runs once per unique environment fingerprint. The golden
is then kept forkable and reused across all jobs with the same toolchain
requirements.

### Fork Performance (Per-Job)

| Golden Source | Fork Time (median, 5 runs) |
|---|---:|
| Base golden | 63ms |
| Node golden | 63ms |

Fork time is identical regardless of golden size — CoW clones share memory
and disk pages, only allocating new pages on write.

### Toolchain Preloading Impact

Comparison: Node 22 available at job start vs installed per-job.

| Approach | Per-Job Cost | Node Available At |
|---|---:|---|
| No preloading (setup-node in base fork) | 58.7s | After curl + apt |
| Preloaded golden (fork only) | 0.062s | Immediately |
| **Savings** | **58.6s per job** | **~950x faster** |

The 58.7s without preloading includes: fork (0.06s) + curl NodeSource
setup script (54.7s) + apt-get install nodejs (3.9s). With a preloaded
golden, Node is already installed — the fork inherits it via CoW.

### Environment Fingerprinting

Jobs with identical toolchain requirements share a golden VM. The fingerprint
is a SHA-256 of the canonical environment specification:

- Rust project: both `check` and `test` jobs resolve to fingerprint
  `cefb59bb...` → share one Rust golden
- Node + Python project: fingerprint `df81a99b...` → unique composite golden
- Base-only jobs: fingerprint based on `ubuntu:24.04` alone

### Supported Detection Sources

| Priority | Source | Example |
|---:|---|---|
| 1 | Setup action `uses` + `with` | `actions/setup-node@v4` with `node-version: 22` |
| 2 | Setup action + version file ref | `setup-go` with `go-version-file: go.mod` |
| 3 | Standalone version files | `.nvmrc`, `rust-toolchain.toml`, `.python-version` |
| 4 | Repository manifests | `Cargo.toml`, `package.json` (prefetch hints only) |

## Methodology

- All tests warm: Docker images cached, SmolVM pool pre-warmed, artifacts built.
- 3 iterations per scenario, median reported.
- Workflows: checkout + echo steps (isolates infrastructure overhead from build time).
- Machine: Apple M4 Max, macOS, OrbStack for Docker.
