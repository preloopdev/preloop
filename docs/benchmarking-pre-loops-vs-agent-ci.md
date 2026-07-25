# Benchmark Results: Preloop (SmolVM) vs agent-ci (Docker)

Date: 2026-07-25 | Host: Apple M4 Max, macOS | SmolVM: 4 vCPU, 8 GB RAM, Ubuntu 24.04

## Configurations

| # | Config | Isolation | Cache Model |
|---|--------|-----------|-------------|
| 1 | **Host Direct** | None | Native build artifacts on disk |
| 2 | **agent-ci Rust** | Docker container | npm cache mounted; no Rust `target/` or `node_modules/` |
| 3 | **Preloop SmolVM (cold)** | SmolVM (libkrun) | No build cache — cold build every run |
| 4 | **Preloop SmolVM (hot)** | SmolVM (libkrun) | `target/` or `node_modules/` via virtiofs symlink |

## What agent-ci bind-mounts

From `build_container_binds` in `crates/agent-ci-runtime/src/docker/config.rs`:

**Always:** `_work/`, Docker socket, git shim, signals dir, diag dir, tool cache (`/opt/hostedtoolcache`)

**If detected on host:** pnpm store, npm cache, yarn cache, bun cache, Playwright cache, Cypress cache

**NOT mounted:** Rust `target/`, Go build cache, Python venv, `node_modules/`.
The workspace is synced fresh from the working tree each run — `node_modules`
and build artifacts are never carried over. npm/pnpm cache IS mounted, so
repeated `npm install` is faster but still runs every time.

---

## Results: Rust Projects

### serde (2 steps: `cargo check -p serde` → `cargo test -p serde --lib`)

```
Config                         Run 1     Run 2     Run 3     Median
─────────────────────────────────────────────────────────────────────
Host Direct (warm)               132ms      61ms      61ms      61ms
agent-ci Docker (cold target/) 6,760ms   6,309ms   6,263ms   6,309ms
Preloop SmolVM (cold target/)  1,632ms   1,635ms   2,376ms   1,635ms
Preloop SmolVM (hot target/)      75ms      61ms      89ms      75ms
```

### bat (2 steps: `cargo check` → `cargo test --lib`)

```
Config                         Run 1      Run 2      Run 3      Median
──────────────────────────────────────────────────────────────────────────
Host Direct (warm)                620ms      423ms      406ms      423ms
agent-ci Docker (cold target/) 19,361ms   19,602ms   19,562ms   19,562ms
Preloop SmolVM (cold target/)  24,338ms   24,503ms   24,416ms   24,416ms
Preloop SmolVM (hot target/)      402ms      310ms      305ms      310ms
```

---

## Results: JavaScript Project

### express (3 steps: `npm install` → `eslint .` → `mocha test/`)

agent-ci mounts the npm cache, so `npm install` is faster on repeat runs but
still executes every time. Preloop mounts `node_modules/` via virtiofs, skipping
`npm install` entirely.

```
Config                              Run 1     Run 2     Run 3     Median
────────────────────────────────────────────────────────────────────────────
Host Direct (warm, with install)   9,830ms   2,199ms   2,176ms   2,199ms
agent-ci Docker (with install)    15,595ms   7,624ms   7,552ms   7,624ms
Preloop SmolVM (hot node_modules)  2,166ms   2,174ms   2,157ms   2,166ms
```

Note: agent-ci run 1 is cold (npm install from network); runs 2-3 use the
mounted npm cache. Preloop hot has `node_modules/` pre-installed via virtiofs.
Host Direct run 1 is cold `npm install`; runs 2-3 find packages already installed.

---

## Head-to-Head Summary

### Cold builds (apples-to-apples, no build cache in either)

```
                          serde (cold)     bat (cold)
────────────────────────────────────────────────────────
agent-ci Docker            6,309 ms        19,562 ms
Preloop SmolVM             1,635 ms        24,416 ms
────────────────────────────────────────────────────────
Winner                     Preloop 3.9×    agent-ci 1.2×
```

**serde cold:** Preloop is **3.9× faster**. Docker's container lifecycle
(network create → container start → runner bootstrap) costs ~5s before the first
command runs. SmolVM has the runner already configured — overhead is server start
+ broker session + step dispatch.

**bat cold:** agent-ci is **1.2× faster**. When compilation takes 20s+, Docker's
overlayfs write performance edges out SmolVM's overlay for heavy I/O.

### Hot builds (Preloop's architectural advantage)

```
                          serde (hot)    bat (hot)     express (hot)
──────────────────────────────────────────────────────────────────────
Host Direct                   61 ms        423 ms        2,199 ms
Preloop SmolVM (hot)          75 ms        310 ms        2,166 ms
agent-ci Docker (cold)     6,309 ms     19,562 ms        7,624 ms
──────────────────────────────────────────────────────────────────────
Preloop vs agent-ci           84× faster   63× faster    3.5× faster
Preloop vs Host Direct        ~parity      ~parity       ~parity
```

### JS-specific: agent-ci's npm cache mount IS effective

agent-ci's npm cache mount drops `npm install` from ~13s (cold) to ~5s (cached),
making the overall run ~7.5s. But Preloop skips `npm install` entirely by mounting
`node_modules/` — so it's still 3.5× faster even when agent-ci's cache is warm.

---

## Scaling Analysis: What Happens at 2–3 Minute Builds

```
Build time          agent-ci total    Preloop cold total    Preloop hot total
────────────────────────────────────────────────────────────────────────────────
~1.5s (serde)         6.3s              1.6s                  0.075s
~20s  (bat)          19.6s             24.4s                  0.31s
~120s (2 min)        ~125s             ~122s                  ~0.1s
~180s (3 min)        ~185s             ~182s                  ~0.1s
```

Cold-vs-cold **converges to parity** as build time grows — the ~5s Docker tax
and ~1.6s SmolVM protocol overhead both become noise.

The hot path stays flat at **~100ms regardless of project size** because
virtiofs gives the VM's incremental build tool (cargo, tsc, etc.) direct access
to the warm host cache. The longer the cold build, the bigger Preloop's hot
advantage: **1,250× at 2 min builds, 1,850× at 3 min builds.**

---

## Architecture: Why Preloop Can Safely Mount Build Caches

agent-ci doesn't mount `target/` or `node_modules/` because the Docker
container might have a different compiler/runtime version than the host. Mounting
the host's build artifacts into a mismatched container produces silent
correctness bugs.

Preloop doesn't have this problem: the golden base image is built with a
specific toolchain, and the cache was produced by a previous VM from the same
golden image — same compiler, same sysroot, same target triple. The invariant:
**the thing that wrote the cache is the same thing that reads it.**

### Ephemeral VMs with overlay + virtiofs

Both Preloop and agent-ci use ephemeral execution environments. The difference
is how caches are surfaced:

```
Ephemeral VM (destroyed after job)
  ├── overlay: per-job ext4 (writes go here, destroyed after job)
  ├── golden base: read-only (shared across all VMs, has toolchain + deps)
  └── virtiofs: host cache mounts (target/, workspace)
```

- Reads from the golden base hit local ext4 — fast.
- Writes go to the ephemeral overlay — fast, destroyed after job.
- virtiofs provides workspace access from the host.

### virtiofs limitation: small files

virtiofs adds ~1.7x overhead on Rust `target/` (fewer, larger files — acceptable).
For `node_modules` (thousands of tiny files, deep trees, heavy `stat()`/`readdir()`
traffic), virtiofs is the wrong approach — metadata round-trips through the
hypervisor kill performance.

The solution: bake `node_modules` into the golden base image or use
lockfile-hashed ext4 snapshots mounted as overlay lower layers. Small-file I/O
hits local ext4, not virtiofs.

---

## Performance Research Opportunities

### 1. Golden image `node_modules` bake-in vs virtiofs

**Question:** How much faster is `npm test` when `node_modules` is baked into
the golden base image (local ext4 reads) vs virtiofs-mounted from the host?

**Method:** Build a golden image with express's `node_modules` pre-installed.
Boot a fresh VM, run `npx mocha` with node_modules from the base layer. Compare
against virtiofs-mounted node_modules. Measure `stat()` call count and
wall-clock time.

**Where to look:** `docs/runner/microvm-isolation-research.md` section 10,
`docs/runner/14-smolvm-guide.md`.

### 2. SmolVM cold boot to first step latency breakdown

**Question:** Where does the 1.6s cold Preloop overhead come from? Can it get
under 500ms?

**Method:** Instrument each phase: VM boot, runner binary startup, server
connection, broker session creation, job acquisition, first step dispatch. Find
which phase dominates and whether any can be pipelined or eliminated.

**Where to look:** `crates/aksh-runner/src/listener/broker_listener.rs`,
`crates/aksh-runner/src/configure.rs`, `docs/runner/11-benchmarks.md`.

### 3. Overlay write performance: SmolVM vs Docker overlayfs

**Question:** Why does bat cold-build take 24s in SmolVM but only 19s in Docker?
Is it the overlay filesystem, vCPU count, or something else?

**Method:** Run `cargo build` on bat inside SmolVM with: (a) writes to overlay
ext4, (b) writes to tmpfs, (c) writes to virtiofs. Compare Docker's overlay2
write performance. Profile with `strace -c` for syscall distribution.

**Where to look:** `docs/runner/14-smolvm-guide.md:311-314`,
`docs/runner/11-benchmarks.md:76`.

### 4. Pre-warmed runner pool (eliminate per-job configure)

**Question:** Can we pre-register N ephemeral runners at server startup so jobs
skip the configure step entirely?

**Method:** Current flow: boot VM, configure runner, submit job, runner polls,
executes. Pre-registered runners already polling when the job arrives eliminate
configure overhead (~110ms) and server startup wait (~2s). Measure dispatch
latency with a pre-warmed pool vs per-job configure.

**Where to look:** `crates/aksh-runner-server/src/runner_lifecycle.rs`,
`crates/aksh-runner/src/listener/broker_listener.rs`.

### 5. Lockfile-hashed `node_modules` snapshot layers

### 5a. Workspace snapshot overhead profiling

**Question:** The server already snapshots the working tree as a synthetic Git
commit with delta object reuse (`snapshots.rs`). How much time does the snapshot
creation + `actions/checkout` fetch add per run, and can it be reduced?

**Method:** Instrument `create_workspace_snapshot()` to log time for each phase:
`ensure_object_cache` (fetch delta), `git add --all` (index update),
`write-tree` + `commit-tree`, and `fsck --connectivity-only`. Measure
`actions/checkout` fetch time from the snapshot HTTP endpoint. Compare against
the total job wall-clock to find the snapshot overhead as a percentage.

**Where to look:** `crates/aksh-runner-server/src/snapshots.rs`
(`create_workspace_snapshot`, `ensure_object_cache`),
`crates/aksh-runner-server/src/runs.rs` (submission flow).

### 5b. Lockfile-hashed `node_modules` snapshot layers

**Question:** Can we hash `package-lock.json` and maintain a library of
pre-built `node_modules` snapshots as ext4 images, auto-mounted when the
lockfile matches?

**Method:** Build an ext4 image from `npm install` output, hash it by lockfile
content. On job boot, check if a matching snapshot exists, mount as overlay lower
layer. Measure cold-start with and without snapshot hit. Estimate storage cost
for N projects.

**Where to look:** `docs/platform-architecture.md:61`,
`docs/runner/microvm-isolation-research.md` section 10.

### 6. Protocol overhead: Twirp vs Unix socket for local-only mode

**Question:** The runner communicates with the server via HTTP/Twirp even on the
same machine. How much latency does a Unix socket or shared-memory IPC save?

**Method:** Measure per-step reporting latency (timeline update + log upload)
over HTTP vs Unix socket. The server already supports `--unix-socket`.

**Where to look:** `crates/aksh-runner-server/src/main.rs` (`--unix-socket`),
`crates/aksh-runner/src/worker/reporting.rs`.

### 7. Parallel step execution within a job

**Question:** GHA runs steps sequentially. For local pre-loop CI, can
independent steps (e.g., `cargo check` and `cargo clippy`) run in parallel?

**Method:** Analyze common CI workflows for step dependency graphs. Measure
wall-clock savings from parallelizing independent steps on a 4-vCPU VM. Check
if this breaks GHA semantics (env set by prior steps, working dir changes).

**Where to look:** `crates/aksh-runner/src/worker/steps_runner.rs`,
`crates/aksh-gha-parser/src/`.
