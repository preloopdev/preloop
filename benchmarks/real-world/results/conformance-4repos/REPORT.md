# Real-World Conformance Campaign — preloop Production Path (Cell C)

Cells:
- **A** (golden): official runner vs GitHub — recent successful runs, captured via the GitHub API
- **C**: aksh runner (in preloop smolVM runners) vs local aksh server — **this run**

Repos: sharkdp/bat, vitejs/vite, astral-sh/uv, nextcloud/server.
Workflows are the exact upstream files, byte-for-byte. No `runs-on:` rewriting:
the preloop on-demand pool declares `self-hosted, Linux, X64` labels and the
scheduler's `job_matches_runner` claims X64-pinned jobs.

Execution engine: `preloop serve` + on-demand smolVM pool
(`PRELOOP_RUNNER_POOL_SIZE=0`, one VM per queued job, 4 vCPU / 4 GiB / 30 GiB
overlay each), runners transported over plain TCP (`AKSH_RUNNER_URL` LAN
address) because smolvm's macOS socket relay is broken
(`docs/smolvm-packed-socket-handoff.md`).

## bat / aksh (cell C) — run 7895d4b8

Run conclusion: **failure** (environment). 23 jobs; 4 succeeded with full step
records, 4 failed on host environment, 15 never claimed.

### Successes (full step-level records, comparable to golden)

| Job | Steps | Notes |
|---|---|---|
| crate_metadata | 5 | checkout via workspace snapshot, `cargo metadata` ok |
| lint | 7 | `rust-toolchain@stable`, `cargo fmt --check`, `cargo clippy -D warnings` all green |
| cargo-audit | 6 | `cargo install cargo-audit --locked`, checkout, `cargo audit` ok |
| min_version | 6 | checkout, rust-toolchain v1.88, tests ok |

Step names match the golden (Set up job / Run actions/checkout@v6 / Complete
job). Checkout of the workspace snapshot works end-to-end in the VM
(server-side snapshot commit + `x-preloop-local-workspace` header).

### Failures (environment)

- license_checks, test_with_new_syntaxes_and_themes, test_with_system_config,
  documentation — failed at checkout with `I/O error (os error 5)` /
  `No space left on device`: smolvm overlay/pack-cache corruption under
  concurrent provisioning on the near-full host disk. Not protocol defects.
- all-jobs — dependency gate (correct: upstream jobs failed).

### Never claimed (queued at capture)

The 13-job `build` matrix + winget. The pool churned VMs while the host disk
and smolvm extraction cache were failing; the scheduler's label matching
itself is verified correct (see below).

## Findings

1. **All four repos pin `runs-on: [self-hosted, Linux, X64]`** — none use
   `ubuntu-latest`. The scheduler's label matching correctly left X64 jobs
   unclaimed by ARM64-labeled runners (verified via agents API + claim polls).
   The pool must declare the `X64` label explicitly to serve these workflows.
2. **Job-name fidelity gap**: the runs API reports the YAML key
   (`crate_metadata`) where GitHub's jobs API reports the evaluated
   `name:` (`Extract crate metadata`). Naming-classified diff.
3. **Environment-change replacement bug (fixed)**: with a custom base image
   the orchestrator compared the next job's implied stock base
   (`ubuntu:24.04`) against the configured artifact path, replacing every
   idle runner forever. Fixed by disabling env-based replacement for custom
   bases (`next_job_runs_on: None`).
4. **Base-toolchain gap**: `cargo metadata` in crate_metadata requires cargo
   preinstalled (GitHub ubuntu images carry it). The selected runner image must
   provide that toolchain.

## Source changes this campaign

- `crates/preloop-cli/src/main.rs` — `control_socket` fallback to plain TCP
  for non-loopback `AKSH_RUNNER_URL`; `PRELOOP_RUNNER_LABELS` extra labels;
  custom-base disables env-replacement; `PRELOOP_RUNNER_OVERLAY_GB` knob.
- `crates/preloop-orchestrator/src/lib.rs` — `overlay_gib` config passthrough.
- `crates/preloop-vm/src/lib.rs` — `MachineSpec.overlay_gib` wired to
  `smolvm machine create --overlay`.

## Remaining work

- Vite / uv / nextcloud cells: run on a healthy host (or the x86_64
  production box) once disk/cache corruption is resolved.
- Re-run bat cell B (official runner vs aksh server) on the same pool.
- Job-name fidelity: surface the evaluated job `name:` in the runs API.
