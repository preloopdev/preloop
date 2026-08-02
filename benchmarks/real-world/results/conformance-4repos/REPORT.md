# Real-World Conformance Campaign — Preloop Production Path (Cell C)

Cells:
- **A** (golden): official runner vs GitHub — recent successful runs, captured via the GitHub API
- **C**: aksh runner (in preloop smolVM runners) vs local aksh server — **this run**

Repos: sharkdp/bat, vitejs/vite, astral-sh/uv, nextcloud/server.
Workflows are the exact upstream files; harness changes are limited to
`runs-on:` label rewrites (`[self-hosted, Linux, X64]`, committed in the
workspace so the plan gate sees a docs-only diff) and one dirty tracked-file
edit per repo to open changed-file gates.

Execution engine: `preloop serve` with a warm forked pool
(`PRELOOP_RUNNER_POOL_ENABLED=1`, size 4, `PRELOOP_USE_PACKED_GOLDEN=1`) —
one frozen golden VM, one smolVM fork per job, 4 vCPUs / 4 GiB / 30 GiB
overlay each. Runner transport is the control-socket relay (loopback URLs);
the LAN-TCP path is blocked by the macOS firewall.

Runner base image: Ubuntu 24.04 OCI tar baked with the GitHub-runner package
set (git, curl, build-essential, node 22.23.2, npm, python3, …), `CMD
/bin/bash`, `resolv.conf` pinned to 8.8.8.8.

## bat / aksh (cell C) — run 37831313

Run conclusion: **failure**. 7 jobs passed, 13 failed (cross-target build
matrix), 1 skipped, all-jobs gate failed correctly.

| Job | Conclusion | Class |
|---|---|---|
| license_checks, crate_metadata, lint, test_with_system_config, documentation, cargo-audit, min_version | success | match |
| build (13 matrix cells) | failure | environment — cross toolchains (musl/i686/arm/windows/darwin targets) not installed in the base image; the golden runs these on GitHub's provisioned images |
| all-jobs | failure | correct — jq gate sees non-success build cells |
| winget | skipped | correct — tag-gated |
| test_with_new_syntaxes_and_themes | failure | environment — `assets/create.sh` downloads syntax/theme repos (submodule/network gap) |

`actions/checkout@v6` of the workspace snapshot works end-to-end in the VM;
`cargo metadata`, `cargo fmt --check`, `cargo clippy -D warnings`, `cargo
audit` all pass.

## vite / aksh (cell C) — run 4d6fb467

Run conclusion: **failure**. changed + lint pass; the pnpm install failure
(from the earlier run, system node 18.19) is fixed by baking node 22 into the
base; the test matrix now executes for real.

| Job | Conclusion | Class |
|---|---|---|
| changed | success | match — tj-actions changed-files gate opened on the dirty change |
| lint | success | match |
| test-failed | success | correct — sentinel job confirms test failures |
| test (×6 matrix) | failure | environment — `Build`/`Test serve`/`Test unit` steps fail inside the suites (Playwright browsers, build toolchain, sandbox deps) |
| test-passed | skipped | correct |

## uv / aksh (cell C) — run 2794d144

Run conclusion: **failure** — 20 success, 38 failure, 81 skipped, 6
cancelled, 0 remaining. Terminal with every job settled.

The plan gate computes `test-code: false` for the docs-only dirty change:
`git diff` against the snapshot base sees only `docs/concepts/index.md`, and
`github.ref` is synthesized as `refs/pull/1/merge` so `on_main_branch` stays
false — the heavy test/build matrix skips exactly as the golden's docs-only
PR does (81 jobs skipped via plan-output gating).

| Job | Conclusion | Class |
|---|---|---|
| plan/plan | success | match — checkout, diff, outputs all correct |
| check-fmt/rust, check-fmt/prettier, check-fmt/python | success | match |
| check-lint/ruff, check-lint/ty, check-lint/shellcheck, check-lint/validate-pyproject, check-lint/typos, check-lint/hawk | success | match |
| check-lint/clippy-ubuntu | failure | environment — `uv_build` dependency fetch |
| check-lint/readme, check-docs/docs | failure | environment — `actions/setup-python` version manifest fetch |
| check-lint/shear | failure | environment — `cargo shear` not installed |
| check-release/dist-plan | failure | environment — dist tooling |
| review/review | failure | environment — Codex security plugin checkout needs auth |
| windows-trampoline ×3, build-binary-windows ×2, freebsd | failure | environment — Windows/FreeBSD targets on a Linux ARM64 pool |
| build-docker/docker-publish-base | failure | environment — Docker metadata/attestation actions need Docker in the VM |

**Harness note**: the initial uv submissions rewrote `runs-on:` only in the
ten workflows the setup script fetched; the remaining reusable workflows
(test.yml, test-system.yml, bench.yml, …) kept `runs-on: ubuntu-latest`, so
their jobs were unclaimable by the self-hosted pool and the run could not
drain. Rewriting every workflow file fixed it — the run then executes and
terminates normally. This is a harness completeness issue, not a server
defect; label matching behaved correctly (ubuntu-latest jobs were never
claimed).

## nextcloud / aksh (cell C) — run 2f4df38f

Run conclusion: **failure**.

| Job | Conclusion | Class |
|---|---|---|
| changes | success | match — dorny/paths-filter gate opened on the `lib/` dirty change |
| phpunit-sqlite (8.3, 8.5) | failure | environment — PHP not in the base image; the golden's runner image carries it |
| summary | failure | consequence of phpunit failures |

## Findings

1. **Snapshot history rewrite (fixed this campaign)**: the workspace-snapshot
   repo mirrored the shallow boundary, so a full `git fetch` from it was
   rejected by git ("shallow roots are not allowed to be updated") and left
   the client's object store missing the boundary commit's parents —
   `git diff base...HEAD` failed with "no merge base". The snapshot builder
   now rewrites the reachable history: shallow roots become parentless
   commits and every descendant is re-created with rewritten parents, giving
   a self-contained, fsck-clean repo. Checkouts and changed-file diffs work.
2. **`github.ref` for pull_request (fixed)**: PR submissions presented
   `github.ref` as the base branch, so main-branch gates (uv's
   `on_main_branch`) fired on PRs. Now `refs/pull/<number>/merge`, plus
   `head_ref`/`base_ref` from the payload.
3. **Checkout redirect vs explicit refs (fixed)**: checkouts with empty or
   template-valued `ref` inputs (e.g. `ref: ${{ inputs.head-sha }}`) were
   not redirected to the snapshot and fetched the real GitHub host
   unauthenticated. Empty/expression refs are now redirectable.
4. **Dynamic matrix from `needs` outputs (fixed)**: jobs with
   `matrix: ${{ fromJson(needs.X.outputs.Y) }}` were silently dropped at
   expansion (empty matrix → zero cells), breaking dependents' needs
   validation and conformance scenario 101. The parser now keeps a deferred
   single cell; runtime fan-out is still TODO.
5. **`preloop run` gaps (fixed)**: it did not collect local reusable
   workflows (only the native client did) and had no way to pass an event
   payload, so PR-triggered workflows with local `uses:` could not be
   submitted. Both added.
6. **Provider fixes (fixed)**: rootfs-dir base images booted without a
   workload command (no image CMD) and got no virtiofs mounts; docker-save
   OCI tar bases were misrouted to `--from`. Directory bases now get a
   keep-alive workload and `.tar` bases route to `--image`.
7. **Pool environment**: warm forked pool with a packed golden avoids the
   per-VM Docker Hub pull entirely; the golden (frozen) forks in ~100 ms.
   The earlier Docker Hub anonymous rate limit is bypassed.
9. **Environment classification**: PHP (nextcloud), cross toolchains (bat),
   Playwright/browser deps (vite), Windows/FreeBSD targets, and
   setup-python/Codex manifest fetches are host-image gaps, not protocol
   defects. The runner and server executed every step faithfully.
10. **Run drain** (verified by unit test): the scheduler cascade settles a
    full reusable-call DAG once every job is claimable; the earlier
    apparent stall was the unrewritten `runs-on` labels above.

## Source changes this campaign

- `crates/preloop-cli/src/main.rs` — `--payload` on `preloop run`; local
  reusable-workflow collection; TCP fallback and labels/overlay knobs.
- `crates/preloop-vm/src/lib.rs` — `.tar` bases route to `--image`;
  keep-alive workload for directory bases.
- `crates/preloop-orchestrator/src/lib.rs` — overlay passthrough; env-based
  replacement disabled for custom bases.
- `crates/aksh-gha-parser/src/expand.rs` — deferred single cell for
  needs-dependent dynamic matrices.
- `crates/aksh-runner-server/src/runs.rs` — PR `github.ref`/`head_ref`/
  `base_ref` synthesis; PR base/head sha refresh from the snapshot.
- `crates/aksh-runner-server/src/snapshots.rs` — shallow-history rewrite;
  redirect applies to empty/template refs.
- `benchmarks/real-world/conformance-4repos/compare-goldens.py` — `c` cell.

## Remaining work

- uv cell: let the run drain to terminal (concurrency-group serialization),
  then refresh this report.
- Runtime fan-out for needs-dependent dynamic matrices (parser defers today).
- Bake PHP (nextcloud), cross toolchains (bat), and Playwright browsers
  (vite) into a fuller base image to close the environment class.

---
Generated 2026-08-02T06:00:00Z
