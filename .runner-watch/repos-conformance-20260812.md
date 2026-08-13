# Real-world repo conformance campaign — 2026-08-12

Five large repositories run unmodified against the preloop stack (local
aarch64 engine with packed-golden smolvm forks; x86 leg on the remote
`aksh.preloop.dev` x86_64 control plane). Goal: find and fix whatever breaks
real workflows end to end.

## Repos

| Repo | Workflow | Result | Notes |
|---|---|---|---|
| `moby/moby` | `ci.yml` | **success** | docker buildx/bake builds (binary + dynbinary, amd64/arm64 cells), govulncheck SARIF export, cross, build-dind |
| `neovim/neovim` | `test.yml` | **partial** | lintc/lint/clang-analyzer/zig-build + ubuntu posix cells pass; macos/windows cells fail via the starvation sweep (no such runners) |
| `microsoft/TypeScript` | `ci.yml` | **partial** | ubuntu test cells pass; windows/macos cells starve-fail |
| `astral-sh/ruff` | `ci.yaml` | **success** | 32 of 39 jobs correctly skipped by the changed-files gates; cargo jobs pass |
| `nodejs/node` | `test-linux.yml` | **partial** | ubuntu + arm jobs both pass checkout/tool setup/sccache and full `make build-ci`, then fail during `Test`; final test logs were not uploaded, so the underlying assertion is unknown |

## Bugs found and fixed (all in preloop)

1. **Packed-golden rootfs ownership + setuid stripping** (smolvm pack
   extraction through an unprivileged virtiofs server). Every file lands
   host-user-owned (`502` macOS / `1000` Linux) with setuid cleared —
   `sudo` breaks instantly. Fixed in `provision_runner`:
   `repair_leaked_rootfs_ownership` — chown pass, tar-roundtrip rebuild of
   the chown-resistant residue, and setuid/setgid modes re-derived from the
   pack's layer tar (the only surviving record) and re-applied.
2. **SmolVM non-streaming execs die after ~30s without output.** The repair
   (and any quiet provisioning step) was killed mid-flight. Fixed: provider
   execs pass `--timeout 30m`.
3. **Job-level `env:` missing from the wire `environmentVariables`.**
   moby's bake resolved `${DESTDIR}` empty and dropped the govulncheck
   output. Fixed in `job_builder.rs`.
4. **Queued runs wedged after a server restart.** The pool's
   `queue_depth` atomic was never re-armed for recovered queues. Fixed in
   `bootstrap.rs`.
5. **Concurrency group deadlock on unclaimable jobs.** A run stuck on
   external-host (macos/windows) jobs never goes terminal, so its run-level
   concurrency holder parks every later submission forever. Fixed in
   `reconcile_concurrency_groups` (restore-time; releases holders whose
   remaining jobs all need an external host with none registered).
6. **Golden missing Chromium/playwright runtime libs.** 21 pinned browser
   libs added to `versions.toml` + the golden bake.
7. **`ACTIONS_CACHE_URL`/`ACTIONS_RESULTS_URL` without trailing slashes.**
   sccache's GHA backend concatenates its twirp path directly onto the
   base URL → `http://host:9090twirp/…` → `invalid port number` → storage
   probe fails → sccache's compiler shim emits nothing → node's configure
   reports "Could not determine compiler version info". Fixed: both
   service URLs carry trailing slashes (matching the results-receiver),
   and the worker no longer trims them.
8. **Cache twirp routes were JSON-only.** sccache sends twirp **protobuf**
   (the `ghac` crate); the `Json` extractor answered 415. The cache v2
   create/finalize/get-download-url routes now decode protobuf requests
   and encode protobuf responses (field numbers from ghac 0.2.0's
   `cache.proto`: metadata=1, key=2, restore=3, version=4, ok=1 as bool
   varint). Node's `main`-ref build (`CC: sccache clang-19`) needs this.
9. **Reusable-callee jobs on unhostable platforms** were only checked at
   submit time; a materialized `windows-2025` callee could be claimed by a
   Linux VM and wedge the run in cleanup. Concluded as failures at
   materialization now.
10. **Rootfs repair missed the account database.** `/etc/passwd` stayed
    host-owned → `useradd` wedged in D-state on every fork. The repair
    now chowns the account files explicitly.
11. **`github.workspace` in workflow env collapsed to ""** at job-build
    time (neovim's `BIN_DIR: ${{ github.workspace }}/bin` → `/bin`).
    Runtime-only context keys now survive server-side resolution and are
    evaluated by the runner at step time.
12. **Worker crashes left active steps and partial logs inconsistent.**
    A terminal job could retain `Test: in_progress`, while `/logs` returned
    empty if `job-logs.txt` was never uploaded. Terminal completion now
    closes active steps, and aggregation falls back to uploaded step blobs.
13. **SmolVM force-delete raced final agent/log writes.** SmolVM 1.7.7
    intermittently returned `Directory not empty`, stranding spent VMs.
    Deletion now retries that transient error and treats absence as success.

## Environmental notes

- Local server wedged twice (machine churn, no execs, ignored SIGTERM);
  restart with logs clears it. Pre-restart runs must be resubmitted
  (covered by fix #4 going forward).
- Shallow (`--depth 1`) workspace clones break the snapshot's changed-files
  diffing (null parent → `not our ref 00000000…`); use full clones.
- Remote x86 pool runners lost their control bridge → service restart.
- Remote checkout of foreign repos needs the App installation grant or a
  workspace snapshot on the host.

## Verification

- Ownership fix verified on fresh forks: `/usr/bin/sudo` = `0:0 4755`,
  zero leaked files outside `/home/ubuntu`, `sudo -n true` passes.
- sccache cache routes verified against the real `ghac` crate proto
  (round-trip unit tests); node's configure now passes its compiler
  probe with sccache installed and the full `make build-ci` compiles.
- Worker-crash regressions verify terminal step reconciliation and results-
  service step-log fallback when the final job log is missing.
- VM-provider regression verifies retry after a transient nonempty-directory
  delete and idempotent success for an already-missing machine.
- `just test-ci` run before commit (see repo gate).
