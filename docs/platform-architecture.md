# Managed CI Platform — Architecture & Design

## Summary

A managed CI platform that beats GitHub Actions on Linux job performance by
combining a Rust control plane, a Rust runner with chunk-based process I/O,
Firecracker microVMs on rented bare metal, and colocated object storage. macOS
and Windows jobs fall back to GitHub Actions.

---

## Performance Wins vs GitHub Actions

| Metric | GitHub Actions | Our Platform | Win |
|---|---|---|---|
| Runner binary | C# / .NET 8 (~80 MB) | Rust (~4 MB static) | 20× smaller |
| Runner baseline RSS | ~75 MB | ~800 KB | 90× less |
| Cold start | ~8s (Azure VM) | ~2s / ~0.1s warm pool | 4–80× |
| Log upload latency | ~10–50ms (cross-region Azure Blob) | ~0.1ms (local MinIO) | 100–500× |
| Docker pull | ~10s (Docker Hub over internet) | ~1s (local registry mirror) | 10× |
| Cache restore | ~5s (Azure Blob) | ~0.5s (local MinIO) | 10× |
| Log memory per step | ~1.8 GB (per-line String alloc) | ~264 KB (chunk-based, constant) | 7,000× |

These wins come from eliminating every source of latency and allocation that
isn't the actual build/test work.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     CONTROL PLANE (Rust)                          │
│                                                                    │
│  aksh-runner-server (single binary, ~4 MB)                        │
│  ├── /broker/*                     ← runner protocol              │
│  ├── /twirp/.../WorkflowStepsUpdate ← step status                 │
│  ├── /twirp/.../GetStepLogsSignedBlobURL                           │
│  ├── /ws/live-logs/:job_id          ← WebSocket feed             │
│  └── /api/v1/*                      ← native API                 │
│                                                                    │
│  PostgreSQL (job state, queue, results, runner registry)          │
│  MinIO / S3  (logs, artifacts, caches per region)                │
└──────────────────────────────────────────────────────────────────┘
          │
          │  Control plane runs in ONE region (Ashburn).
          │  Runners in every region poll the same control plane.
          │  MinIO is deployed PER REGION for local log/cache storage.
          │
          ▼
┌──────────────────────────────────────────────────────────────────┐
│                     RUNNER FLEET (per region)                      │
│                                                                    │
│  Rented bare metal (Equinix Metal, Hetzner, OVH, PhoenixNAP)     │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │ Host machine (64 vCPU, 256 GB RAM, 2× NVMe 4 TB)             │ │
│  │                                                               │ │
│  │  Per-host services:                                           │ │
│  │    Docker registry mirror (distribution/registry:2)           │ │
│  │    Warm overlay pool (pre-built ext4 images with cached deps) │ │
│  │                                                               │ │
│  │  ┌──────────────────────────────────────────────────────┐    │ │
│  │  │ Firecracker microVM × 40-50                           │    │ │
│  │  │                                                       │    │ │
│  │  │  rootfs: ubuntu-24.04 + aksh-runner (1 GB, r/o,     │    │ │
│  │  │           shared across all VMs)                      │    │ │
│  │  │  overlay: per-job ext4 (10-40 GB, destroyed after    │    │ │
│  │  │           job)                                        │    │ │
│  │  │  docker:  per-job ext4 (20 GB, destroyed after job)  │    │ │
│  │  │                                                       │    │ │
│  │  │  Lifecycle: boot (1s) → runner connects → job runs   │    │ │
│  │  │  → cleanup (0.5s) → destroy VM (0.2s)               │    │ │
│  │  └──────────────────────────────────────────────────────┘    │ │
│  └──────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

---

## Runner Architecture (inside the microVM)

### Process I/O — chunk-based, zero per-line allocation

```
bash stdout/stderr
     │
     │  64 KB kernel pipe buffer (guest RAM)
     ▼
spawn_chunk_reader
     │
     │  read(buf, 65536) — raw bytes, no newline splitting
     │  sends Bytes through bounded mpsc (capacity 1024)
     ▼
push_chunk → ChunkCallback(&[u8])
     │
     │  log_file.lock().write_all(chunk) — one syscall per 64 KB
     │  No UTF-8 check, no String alloc, no secret masking,
     │  no timestamp formatting. Bytes land on disk.
     ▼
tempfile() on /dev/vdb (overlay ext4)
     │
     │  Guest kernel page cache → async flush to Firecracker
     │  Firecracker virtio-blk → host ext4 on local NVMe
     │
     ▼
STEP COMPLETES:
  log_content():
    flush → try_clone() → seek(0) → read_to_string()
    (one allocation, brief, then freed)
    split on \n → mask_secrets → format timestamps
     │
     ▼
  PUT <signed-url> → local MinIO (same rack, ~0.1ms)
```

### Memory profile (verified in smolvm)

| Output volume | Lines | RSS peak | RSS delta |
|---|---|---|---|
| 5 MB | 100K | 808 KB | 264 KB |
| 50 MB | 1M | 812 KB | 264 KB |
| 250 MB | 5M | 808 KB | 264 KB |
| 500 MB | 10M | 808 KB | 264 KB |

Memory is constant regardless of output volume. The 264 KB growth is the
tokio runtime + bounded mpsc channel + BufWriter buffer.

### Step execution lifecycle

```
1. Evaluate step condition (success(), failure(), always(), cancelled())
2. Queue step as InProgress → POST WorkflowStepsUpdate (200 bytes)
3. Create StepContext with anonymous tempfile for logs
4. Execute step by type:
   - Script step: write script to _temp/, spawn bash, pipe stdout
   - Node action:  download + extract tarball, spawn node
   - Docker action: docker build + docker run
   - Composite:     recurse into sub-steps
5. During execution:
   - Every 500ms: POST WorkflowStepsUpdate (status only, ~200 bytes)
   - Every 250ms (first 60s): LiveLogQueue drain → WebSocket (best-effort)
   - If cancelled: SIGINT → 7.5s grace → SIGTERM → 2.5s → SIGKILL
6. Step completes:
   - Check exit code, process GITHUB_OUTPUT/ENV/PATH/STATE files
   - log_content() → flush, read, mask, format
   - PUT step log to signed blob URL (MinIO)
   - POST WorkflowStepsUpdate (Completed)
   - Record in job-level log file (tempfile on disk)
7. After all steps:
   - Queue "Complete job" synthetic step
   - Upload job log (all step logs merged)
   - POST completeJob → control plane
   - Close WebSocket, drain LiveLogQueue
   - Upload diagnostic logs (_diag/)
```

---

## Caching

### Docker images

Host-local registry mirror (`distribution/registry:2`) caches images on each
machine. First pull of `node:20-alpine` fetches from Docker Hub. Subsequent pulls
hit the mirror at local NVMe speed (~2 GB/s vs ~100 MB/s over internet).

### Build artifacts

`actions/cache@v4` protocol backed by local MinIO (same rack, not cross-region
Azure Blob). Cache key: `runner.os + hash(files)`. Restore and save both hit
MinIO at local SSD speed (~0.5ms vs ~50ms for Azure Blob).

### Warm overlay pool

Pre-built ext4 images with common dependencies pre-cached:

```
rust.ext4:    /home/runner/.cargo/registry/cache
              /home/runner/.cargo/git/db
              /home/runner/.rustup/toolchains
node.ext4:    /home/runner/.npm/_cacache
python.ext4:  /home/runner/.cache/pip
```

Cloned per VM via ext4 reflink (instant). Turns a cold `cargo build` from 120s
to ~10s.

---

## Scaling

### Baseline

Keep N warm machines per region. Runners are already polling — first job starts
in < 1 second.

### Burst

When queue depth exceeds threshold, provision more machines via provider API.

| Provider | Typical availability | Provision time |
|---|---|---|
| Equinix Metal | 5–15 machines in 2–5 min | 5 min to first job |
| Hetzner | 3–10 machines | 5 min |
| OVH | 2–8 machines | 5 min |
| PhoenixNAP | 0–5 machines | 5 min |

Multi-provider federation smooths inventory risk. Spill to the cheapest
provider first; burst to more expensive ones on demand.

### Ceiling

Bare metal has finite inventory. Unlike Azure VMs (which can overcommit),
you can't create 500 machines in 60 seconds. The fleet size is the ceiling.
Idle machines cost money. This is the fundamental tradeoff vs cloud elasticity.

### Shrink

When machines are idle for > 30 minutes, deprovision them. Warm pool shrinks
to the configured minimum.

---

## What We Can't Do

### macOS runners

Apple license requires Apple hardware. No VM isolation (macOS has no KVM,
no Firecracker equivalent). Must use physical Mac minis — 1 job per device.
Cleanup via ephemeral user accounts (not VM destroy). 30–60s reboot between
jobs.

Apple Silicon (M4) mandatory by 2026 — Intel Macs losing macOS support.
x86 emulation via Rosetta does not work for Docker containers or VMs.

Cost: ~$0.08/min. Slowest and most expensive tier. No meaningful way to
improve density beyond what GitHub already does.

**Decision:** macOS jobs fall back to GitHub Actions. Not our core competency.

### Windows runners

Must use Hyper-V (Windows native hypervisor). No Firecracker for Windows.
Images are 30–50 GB. VM boot is 10–15s (vs 1s for Firecracker). Licensing
per core (~$1,000/core). Ecosystem mismatch with Linux-native tooling.

**Decision:** Windows jobs fall back to GitHub Actions.

### Geographic reach

Each region needs its own fleet + MinIO + registry mirror. Multi-region
means multi-cost. Logs and artifacts stay local to the region (MinIO in
region). Control plane is single-region; runners everywhere poll it.
RTT overhead for log upload from remote region to local MinIO: negligible.
RTT overhead for runner polling (150ms): negligible.

Target: 3 regions (US East, EU West, APAC). Beyond that, cost/reach
tradeoff favors GitHub.

---

## Workflow Submission & Routing

```
git push → webhook → POST /api/v1/runs
  → parse workflow YAML (Rust, sub-millisecond)
  → expand matrix → create N jobs
  → queue in PostgreSQL

Runner polls: POST /broker/:id/acquireJob
  → match labels (e.g., self-hosted, linux, rust)
  → dispatch oldest matching job
  → FIFO within same label set
  → Starvation prevention: jobs > 60s old get label relaxation

Runner receives job payload (JSON, ~5 KB)
  → executes steps
  → reports completion
  → Server triggers next job in DAG
```

---

## Rootfs Design

```
rootfs.ext4 (1-2 GB, read-only, shared by all VMs)
───────────────────────────────────────────────────
  Ubuntu 24.04 base
  aksh-runner binary (/actions/runner)
  Docker daemon
  git, curl, jq, unzip, tar
  CA certificates

  NO: Node, Python, Rust, Go, .NET, Java, Ruby, PHP.

  These are installed at runtime by setup actions:
    - uses: actions/setup-node@v4
    - uses: dtolnay/rust-toolchain@stable
    - uses: actions/setup-python@v5

  Users declare dependencies explicitly. No version ambiguity.
  No 30 GB image to maintain. No weekly rebuilds.
```

---

## Costs (approximate)

| Item | Cost |
|---|---|
| 1 bare metal machine (64-core, 256 GB) | ~$1,000–1,500/mo |
| 40 concurrent jobs per machine | |
| 1M CI minutes per month | ~3 machines = ~$3,000–4,500/mo |
| Control plane (4 vCPU, 8 GB) | ~$50–200/mo |
| MinIO (3 nodes, per region) | ~$300–600/mo |
| Engineering (1-2 people) | ~$200–400k/yr |

**Break-even vs GitHub Actions Team plan:** ~80-120 concurrent jobs
(~2-3 machines). Below that, GitHub is cheaper. Above that, we win.

---

## Key Design Decisions

1. **Rust runner + Rust control plane.** Eliminates .NET/Go runtime overhead.
   Runner baseline RSS is 800 KB vs GitHub's 75 MB.

2. **Chunk-based process I/O.** Raw bytes from child process pipes go straight
   to disk. Zero per-line String allocation. Masking and formatting deferred to
   step completion. Verified constant memory at 10M lines / 500 MB output.

3. **Bare metal, single-hop virt.** No nested virtualization (Azure VM →
   Firecracker). Runner's `write_all` hits host NVMe directly through a single
   Firecracker virtio-blk translation.

4. **Colocated object storage.** MinIO on the same rack as the compute fleet.
   Log uploads and cache restores never leave the local network.

5. **Warm VM pool.** Pre-booted Firecracker VMs with runners already polling.
   Job starts in < 1s (vs 8s cold Azure VM or 2s cold Firecracker).

6. **macOS and Windows fall back to GitHub Actions.** Not worth the operational
   burden. Linux is 85% of the market and where we have a real advantage.

---

## Files Changed (aksh-runner)

See commits for details:

- `bc5a6b8` — disk-backed step and job logging (StepContext, ServerQueue)
- `ee939c0` — chunk-based process I/O (ChunkCallback, spawn_chunk_reader,
  handlers simplified to single write_chunk closure)
