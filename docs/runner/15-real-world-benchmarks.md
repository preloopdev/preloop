# Real-World Benchmark Report

Date: 2026-07-05 | VM: SmolVM `build-runner` (x86_64, 4 vCPU, 8 GB RAM)

## Repos Tested

| Repo | Version | Steps | Rust Toolchain |
|------|---------|-------|---------------|
| serde | HEAD (1.0.228) | fmt, build×2, clippy×2, test×2 | 1.86.0 |
| axum | HEAD (0.8.9) | fmt, clippy, test, doc | stable (1.96.1) |
| bat | HEAD (0.26.1) | fmt, build, clippy, test | stable (1.96.1) |

## Results: serde (fair comparison — sequential, same cache state)

The only apples-to-apples comparison. Both baseline and aksh-runner ran
sequentially with identical warm cargo cache.

```
Step                             Baseline     aksh-runner    Official C#
────────────────────────────────────────────────────────────────────────
Setup Rust PATH                     —           102ms           —
Rust version                      12ms          104ms          <1s
Rustfmt                          312ms          406ms          <1s
Build serde (rc)                 140ms          103ms          <1s
Build serde (no-default)          24ms          103ms          <1s
Clippy serde                      78ms          104ms          <1s
Clippy serde_derive              209ms          105ms          <1s
Test serde_core                 3536ms         3666ms          ~4s
Test serde_derive                568ms          609ms          <1s
────────────────────────────────────────────────────────────────────────
JOB TOTAL                       4879ms         5302ms         6000ms
```

- **aksh-runner overhead: +423ms (8.7%)** over direct execution
- **aksh-runner vs official: 5.3s vs 6.0s (aksh 1.13x faster)**
- Per-step setup overhead: ~100ms (process spawn + env setup + log upload)
- Compute-heavy steps (test): <4% overhead (3666ms vs 3536ms)
- All 3 runners: **Succeeded**

## Results: axum (warm cache, aksh ran after baseline)

```
Step                             Baseline     aksh-runner
─────────────────────────────────────────────────────────
Setup Rust PATH                     —           102ms
Rust version                      11ms          103ms
Rustfmt                          317ms          407ms
Clippy                           246ms          307ms
Test                           41256ms        41051ms
Doc                              393ms          205ms
─────────────────────────────────────────────────────────
JOB TOTAL                      42223ms        42175ms
```

- Apparent overhead: -48ms (0%) — cache was warmer for aksh run
- Test step (41s) dominates — identical between runners
- Official runner: **Failed** (Clippy step, exit 101)
- aksh-runner: **Succeeded**

## Results: bat (hot cache, aksh ran after baseline + clippy)

```
Step                             Baseline     aksh-runner
─────────────────────────────────────────────────────────
Setup Rust PATH                     —           102ms
Rust version                      11ms          103ms
Rustfmt                          189ms          204ms
Build                           1188ms          509ms
Clippy                         29306ms          306ms
Test                           64739ms        14870ms
─────────────────────────────────────────────────────────
JOB TOTAL                      95433ms        16094ms
```

- **NOT a fair comparison** — baseline ran Clippy cold (first time
  with stable toolchain, compiled test deps), aksh ran with hot cache
- The 79s difference is compilation time, not runner overhead
- aksh-runner: **Succeeded**
- Official runner: **Failed** (Clippy step, exit 101)

## Official Runner Failures

The official C# runner v2.335.1 failed on axum and bat but succeeded on serde:

| Repo | Failure Step | Exit Code | Root Cause |
|------|-------------|-----------|------------|
| axum | Clippy | 101 | cargo MSRV check — needs rustc 1.88+ |
| bat  | Clippy | 101 | cargo MSRV check — needs rustc 1.88+ |
| serde | — | 0 | Builds with default 1.86.0 |

Both repos need `RUSTUP_TOOLCHAIN=stable` to select 1.96.1 instead of
the default 1.86.0. The workflow sets this via `env:` at the workflow
level, and aksh-runner correctly applies it. The official runner
appears to also apply it (Rust version step shows 1.96.1), but the
Clippy step still resolves to 1.86.0 — likely a PATH ordering issue
where `/home/bnjoroge/.cargo/bin/cargo` (rustup proxy) is found before
the direct toolchain binary.

**Disparity found**: aksh-runner handles `env.RUSTUP_TOOLCHAIN` + 
`GITHUB_PATH` correctly for all steps. The official runner has an
inconsistency where the toolchain override doesn't propagate reliably
to all steps (further investigation needed to determine if this is a
runner bug or a test setup issue).

## Per-Step Runner Overhead

Across all repos, the per-step overhead pattern is consistent:

| Component | Time |
|-----------|------|
| Step setup (env, working dir, script file) | ~80-100ms |
| Log upload per step | ~5-15ms |
| WorkflowStepsUpdate RPC per step | <5ms |
| Process spawn overhead | ~10-20ms |
| **Total per-step overhead** | **~100ms** |

For N steps, the fixed overhead is approximately `N × 100ms`. For serde
(9 steps), this is ~900ms. The remaining overhead is within noise.

## Protocol Overhead (aksh-server)

| Phase | Time |
|-------|------|
| Server startup | ~230ms |
| Runner configure (--no-externals) | ~500ms |
| Runner configure (with Node.js download) | ~4000ms |
| Workflow submit (client → server) | ~7ms |
| Broker session creation | <5ms |
| Job acquisition | <5ms |
| Job completion report | <5ms |
| Broker session idle timeout (--once mode) | ~50s |
| **Total protocol overhead (excluding broker timeout)** | **~750ms** |

The 50s broker timeout after `--once` is a known overhead in the
current implementation. The broker long-poll has a 50s timeout, and
when the job completes during a poll, the runner must wait for the
poll to expire before checking the result.

## Disparities Found

1. **`defaults.run.working-directory`**: Not implemented in aksh-gha-parser.
   Workflows must use `cd` in each step instead. Not a runner issue —
   parser feature gap. (LOW priority)

2. **`env.PATH` handling**: aksh-runner applies workflow-level `env.PATH`
   overrides directly. Official runner ignores `env.PATH` — PATH can only
   be modified via `GITHUB_PATH` file command. aksh behavior is more
   permissive but diverges from official. (MEDIUM — document as intentional)

3. **Broker long-poll timeout**: 50s idle wait after `--once` job completion.
   Not a behavioral disparity — both runners have idle timeouts — but it
   inflates wall-clock benchmarks. (LOW — benchmarks should measure job
   time from logs, not process wall time)

## Methodology

- **VM**: SmolVM `build-runner` (x86_64, Ubuntu 24.04, 4 vCPU, 8 GB RAM)
- **Rust**: 1.86.0 (serde), stable 1.96.1 (axum, bat)
- **aksh-runner**: locally compiled, v0.1.0
- **Official runner**: v2.335.1, preinstalled
- **Server**: aksh-runner-server on port 80, `AKSH_PUBLIC_URL=http://127.0.0.1`
- **Cache state**: Warm (cargo target/ from prior build; only serde is fully sequential)
- **Timing**: aksh steps from RUST_LOG=info timestamps (ms resolution);
  official runner from `_diag/Worker_*.log` (1s resolution);
  baseline from `date +%s%3N` around each cargo command

### Reproducing

```sh
# On vm103 (SmolVM build-runner)
# 1. Clone repos
mkdir -p /tmp/bench-repos && cd /tmp/bench-repos
git clone --depth 1 https://github.com/serde-rs/serde.git
git clone --depth 1 https://github.com/tokio-rs/axum.git
git clone --depth 1 https://github.com/sharkdp/bat.git

# 2. Run benchmarks
~/cachingv4/benchmarks/real-world/run-all-benchmarks.sh serde
~/cachingv4/benchmarks/real-world/run-all-benchmarks.sh axum
~/cachingv4/benchmarks/real-world/run-all-benchmarks.sh bat
```
