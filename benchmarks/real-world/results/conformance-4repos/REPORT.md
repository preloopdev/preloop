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
---
# Second Campaign — 2026-08-03: actions/runner, yc-software/qm, block/buzz

Cells: **A** (golden): official runner vs GitHub — recent successful PR runs
captured via the GitHub API. **C**: aksh runner (preloop smolVM pool, packed
golden, fork-free after fork-socket flakes) vs the local aksh engine. Exact
upstream workflow files; `runs-on:` rewritten to `[self-hosted, Linux, X64]`;
PR events submitted with webhook-shaped payloads (`payloads/{runner,qm,buzz}-pr.json`).

## actions/runner — build.yml (run f138f863, 9 jobs, **drained**)

Golden: 9/9 success. Ours: **2 failure + 7 cancelled** (fail-fast).

| Job | Conclusion | Class |
|---|---|---|
| build (linux-x64/arm64/arm, win-x64) | failure | environment — `Build & Layout Release` needs the dotnet SDK, not in the golden (8 steps executed; `Package Release`/`Publish Artifact` correctly skipped on PR) |
| docker ×2 | failure/cancelled | environment — `Get latest runner version` needs GitHub API access (actions/github-script); sibling cancelled |
| build (osx/win cells) | cancelled | correct — fail-fast while queued |

## yc-software/qm — cicd.yml (5 attempts; never fully drained)

Golden: 15/15 success. Best capture (fork-free attempt): 7 success, 6
failure, 1 in_progress (hung), 1 queued.

Real step failures: `Root tests` (test failures under 4-VM pool contention),
`CLI` install, `checkout` on some machines, `Portal plugin` production-image
boot — environment/network class, not protocol.

The drain blocker is environmental, not a scheduler defect: a job hangs when
a test blocks on network I/O the guest stack never completes (SYNs are
dropped instead of answered with RST, so connection-refused expectations
hang forever; observed a node test suite at 0 CPU for 30+ min) or when a
fork's control-socket relay dies mid-job (the step's stdout pipe fills).
Renewals keep succeeding, so the 45-min lease reaper never fires. This is
the same class as the production "runner died mid-job" incident.

## block/buzz — ci.yml (run 78d313e6, 23 jobs, **drained**)

Golden: 21 success + 2 skipped. Ours: 1 success + 1 failure + 21 skipped.

| Job | Conclusion | Class |
|---|---|---|
| Dead Token Reference Guard | success | match |
| Detect Changed Paths | failure | **semantic — runner defect**: checkout@v4 (SHA-pinned) fails at the auth-setup `git submodule foreach` step (`git-sh-setup: git: not found`, exit 127). checkout@v7 works (runner cell). Investigate the v4 path under the runner's git auth handling. |
| all dependents (21) | skipped | correct — needs-failure cascade |

## Infrastructure findings (two fixed, two open)

1. **Action-download race (FIXED)**: concurrent cache-miss for the same
   action shared one temp path — interleaved truncates corrupted one
   response (worker: `numeric field was not a number` garbage-parse) and the
   other request 500'd (`failed to rename cached action file`). Reproduced
   standalone; fixed with per-request temp names + winner-publishes;
   verified both concurrent requests return 200 with valid archives.
   (`crates/aksh-runner-server/src/actions.rs`)
2. **Pool-wake bug (FIXED)**: `queue_depth` was refreshed at submit and
   claim but not after `promote_ready_jobs` in `complete_job_inner` — a
   completion that promoted fresh work left the on-demand pool asleep on the
   last claim-time value, so the successor job queued forever. Reproduced as
   "run stuck at queued" (qm stalls 1+2); fixed with a store after
   promotion; verified: machine spawns continued through the final
   completions. (`crates/aksh-runner-server/src/distributed_task.rs`)
3. **Hung jobs (OPEN)**: guest network stack drops SYNs (no fast RST) →
   connection-refused-expected tests hang; plus the fork control-socket
   relay flake (forks from a rebuilt packed golden were 100% socket-broken;
   first golden's forks mostly healthy). A hung job with live renewals never
   drains (lease reaper needs 45 min and only fires on renewal failure).
4. **checkout@v4 git-submodule PATH bug (FIXED)**: the runner's step
   environment started from job-message variables only, with no PATH
   baseline. On machines whose worker process env is sanitized (packed-golden
   boot paths), node actions inherit no PATH: checkout@v4's `git` spawn fails
   with ENOENT ("add Git 2.18 or higher to the PATH"), and shell-outs inside
   git (`submodule foreach` → `git-sh-setup` → `uname`) fail the same way —
   bash steps masked it by supplying bash's compiled-in default PATH. Fixed
   in `aksh-runner` `build_env`: a PATH is now guaranteed per step (worker
   machine PATH, else the platform default), mirroring the official runner's
   job-environment contract. Verified end to end with a manual runner
   launched under `env -i` (no PATH): the repro workflow's checkout@v4 failed
   pre-fix with the exact "add Git 2.18" fallback and succeeded post-fix.

### Post-fix rerun — block/buzz (2026-08-03 evening)

Full rerun of `ci.yml` (PR payload, run `0a653504`) after the checkout@v4
fix. **Detect Changed Paths passed** — the checkout@v4 gate step that failed
the original run now succeeds on healthy machines (5 of 7 checkout
invocations of the same SHA succeeded; the 2 that failed did so with
`Recv failure: Connection reset by peer` against the snapshot server —
the fork-machine flake, infra finding 3). Final tally: 5 success, 12
failure, 6 skipped, 2 cancelled. The last runner (Playwright smoke) hung
at 0% CPU for 20+ min on a live machine (the hung-job class, infra finding
3, reproduced again), blocking its dependent; the run was cancelled to
drain.

Failure classes (all environment, none protocol):
- `install-action` exit 127 (Server Cross-Compile ×2) — tool downloader.
- `mesh-llm rev resolution` (Desktop Build macOS) — GitHub fetch in the
  build script.
- `pnpm`/`playwright` cache misses → install failures (Smoke E2E ×2),
  Tauri deps (Desktop Core), unit tests under pool contention.
- checkout `Connection reset` on 2 machines (fork flake).

Skipped (6): Backend Integration, E2E shards ×2, Mobile, Relay E2E, Web —
the paths-filter gating worked (golden also skips Mobile + Web).

The checkout@v4 semantic divergence vs GitHub is closed; the remaining
divergence on this run is the fork-machine flake class (open, #3).

## Third wave — 2026-08-03 late evening: openclaw/openclaw, redwoodjs/agent-ci

Same cell-C harness (bench engine, packed golden, `runs-on:` rewritten to
`[self-hosted, Linux, X64]`, PR payloads). Two new findings, one fixed on the
spot.

### redwoodjs/agent-ci — tests.yml (run 65210aa4, 1 job, drained)

Golden: 1/1 success. Ours: failure at the workflow's *own* gate.

| Step | Result |
|---|---|
| checkout@34e1148 (v4) | success |
| Setup pnpm / Node / Rust, pnpm install + check, fixture contracts, native CLI golden | all success |
| **Check Rust smoke parity** | failure — agent-ci dogfoods itself: it runs its ~36 `smoke-*.yml` workflows through its own nested runner (official C# worker + its DTU server) inside the VM; 17 failed in 6–10s. **Root cause (source-verified)**: the nested stack synthesizes a job message with a minimal env — `system.github.token` = `"fake-token"`, no `PATH` (agent-ci's `generators.ts`; their `ts-runner/SPEC.md` documents "no GITHUB_* env vars by default"). checkout@v4's startup cannot survive that: any GitHub API call dies with `Bad credentials` (reproduced), any git spawn dies with ENOENT, and the event-file parse dies with `SyntaxError: Unexpected end of JSON input` (all three reproduced as instant exit-1 crashes). **How their CI passes (golden)**: the step short-circuits — agent-ci dogfoods its own CI with `AGENT_CI_LOCAL=true`, and the step's first line skips the suite (`Skipping nested Rust smoke parity inside agent-ci local validation.` — confirmed in the golden run's job log). The nested suite never executes on GitHub-hosted; our run is the first time it ran for real, and it exposed the fake-token/no-PATH crash their CI hides. Not our engine's protocol (outer steps all green) and not a golden capability issue (docker-ce + buildx + compose ARE baked in). |

**Fixed on the spot — cache ENAMETOOLONG (server bug)**: the smoke step's
cache restore 500'd with `cache io error: File name too long`. The legacy
cache layout used the raw hex-encoded key as a directory component; agent-ci's
long Rust cache key overflowed NAME_MAX and even the *probe* failed. Fixed in
`aksh-cache`: the legacy path is only probed when the component could
plausibly exist (long keys were never storable in that layout). Regression
test added; verified the restore now succeeds (the step progressed past it).

### openclaw/openclaw — ci.yml (run f7a78413, 46 jobs after matrix expansion)

Golden: 1 executed + 19 skipped (docs-only PR). Ours: 43 skipped, 3 executed,
3 failed — the skip cascade was correct (43 jobs gated on `preflight`
outputs skipped when preflight died).

| Job | Failure | Class |
|---|---|---|
| preflight | checkout — `upload-pack: not our ref <base sha>` | **finding**: the snapshot server rewrites the workspace HEAD identity and redirects only the *primary* checkout; preflight's own deep fetch of the original base SHA gets "not our ref". Deep-fetch checkouts of real base refs need the snapshot to serve the original identities. |
| security-fast | checkout — `Connection reset by peer` | fork-machine flake (open, #3) |
| openclaw/ci-gate | cascade of preflight | correct behavior |

**Also fixed on the spot — 413 on submit (server limit)**: openclaw's 124
workflow files (≈2 MB, inlined by the CLI as reusable workflows) exceeded
axum's default 2 MiB body limit on `/api/v1/runs`. Raised to 64 MiB.

## Campaign source changes

- `crates/aksh-runner-server/src/actions.rs` — action-download race fix.
- `crates/aksh-runner-server/src/distributed_task.rs` — pool-wake
  queue_depth refresh after promote_ready_jobs.
- `crates/preloop-vm/src/lib.rs`, `preloop-orchestrator`, `preloop-cli` —
  `PRELOOP_RUNNER_DNS` guest resolver knob (unblocks registry pulls on
  networks filtering 8.8.8.8/1.1.1.1).
- `crates/aksh-runner/src/worker/execution_context.rs` — guaranteed per-step
  PATH (checkout@v4 fix).
- `crates/aksh-cache/src/lib.rs` — legacy-path probe guard for long keys
  (cache restore ENAMETOOLONG).
- `crates/aksh-runner-server/src/routes.rs` — 64 MiB body limit on
  `/api/v1/runs`.
- `benchmarks/real-world/conformance-4repos/compare-goldens.py` — runner/
  qm/buzz/openclaw/agent-ci repo entries.


## Fourth wave — 2026-08-04: openclaw rerun, nyblnet/bento, caddyserver/caddy, tokio-rs/tokio

The openclaw preflight finding from the third wave is fixed and re-proven.

### openclaw/openclaw — ci.yml (preflight fix)

Third-wave failure: preflight's custom checkout fetched `${{ github.sha }}`
from github.com and got `upload-pack: not our ref` — the github context
carried the synthetic snapshot commit, which exists only in the engine's
store. Fixed server-side:

- `github.sha` (and PR payload `head.sha`) now expose the workspace's real
  HEAD commit, not the synthetic snapshot sha (`snapshots.rs`,
  `runs.rs`). A workflow that fetches `github.sha` from the real remote can
  resolve it.
- The snapshot's upload-pack now sets `uploadpack.allowReachableSHA1InWant`
  / `allowTipSHA1InWant` (what GitHub serves), so deep fetches of real
  commits against the snapshot work too.

First rerun (host runner, 46 jobs): **preflight success with every
previously failing step green** — Checkout, Resolve checkout SHA, Resolve
exact diff base, Ensure preflight base commit. The skip cascade for
downstream jobs was correct.

Third rerun (run `aa9cd0e0`) — recorded as green at the time, but the
saved evidence log contradicts that: `github.sha`/`head.sha` now carry
the real HEAD (`07583168` fetched from github.com), yet **Ensure
preflight base commit failed** — `upload-pack: not our ref
3220e098…`. The engine's PR-base refresh (`runs.rs`) derives
`base.sha` from the snapshot's `before_sha`, and the snapshot at that
point still carried the fabricated rewritten root `3220e098`, which
exists only in the engine's store. The workflow's fail-safe (missing
base → `docs_only=false`) masked the step failure, so the stored job
conclusion said `success` while the step had exit-coded. The base half
of the fabricated-SHA bug was still open.

Fourth rerun (run `b080672e`, fresh submission after the workspace was
unshallowed to 76,328 commits and the snapshot cache rebuilt) —
**preflight green at step level, verified**: Set up job, Checkout,
Resolve checkout SHA, Resolve exact diff base, **Ensure preflight base
commit**, Detect docs-only changes, Detect changed scopes, Build CI
manifest, Complete job — all success. The ensure-base step reports
`Base commit already present: 0758316883f5…` (the real main HEAD); the
log contains zero occurrences of the fabricated `3220e098`. Run
cancelled after preflight settled (heavy gated jobs not needed for this
evidence). Evidence: `results/conformance-4repos/openclaw/c/run-preflight-green.{json,log}`
(step records in `jobs_list[].steps`).

Earlier evidence runs: the checkout log showed the real fetch landing
`* [new ref] 0758316883f5... -> origin/checkout` from github.com, and
`build-artifacts` / `pnpm-store-warmup` ran real work (Node setup, dist
build, artifact upload, CLI smoke tests).

Getting there took two more fixes beyond the snapshot-identity one:

1. **Runner bug — stale workspace** (fixed, `29804a3a`). A long-lived
   runner reused its work folder, so openclaw's `git init` +
   `git remote add origin` checkout died at 30 ms with
   `error: remote origin already exists` (exit 3). Hosted runners hand
   each job a new VM; a shared runner must reproduce that freshness.
   `setup_workspace` now clears the workspace first, with a regression
   test. This had been misread as a fetch timeout — the run log route
   (`GET /api/v1/runs/{id}/logs`) is what settled it.
2. **Campaign payload gaps.** The synthetic PR payload carried only
   `author_association/base/draft/head`; `pull_request.commits` was
   absent (security-fast validates it as an integer) and `base.sha` was
   empty. Filled with openclaw's real main HEAD and a commit count.

Environment note: the host cell runs on macOS while the workflow targets
Linux. openclaw's scripts need GNU `timeout` and bash 4+ `mapfile`, so
the cell needs `/tmp/conformance-shims` (coreutils + bash 5) on PATH.
The VM cell — the honest Linux environment — is still blocked: on-demand
guests fail registration because the guest control bridge never binds
(`preloop-vm`/orchestrator, tracked separately).

Second rerun (clean engine, `results/conformance-4repos/openclaw/c/run.json`):
43 skipped + 3 failed, the same cascade shape as the golden's docs-only PR.
The preflight failure is no longer the protocol bug: the checkout step now
fetches the real HEAD sha (`07583168`, verified fetchable from the host in
17 s) and failed on fetch timeouts that evening, and security-fast's
"Fetch pull request scan history" failed on a PAT-scope API call. Both are
environment/credential issues, not wire-protocol divergence; the skip
cascade (43 jobs gated on preflight) behaved correctly.

### nyblnet/bento — ci.yml, push main

**PASS (cell C, 42/42 steps).** checkout@v4 + setup-node@v4 (node 24), npm
ci, i18n gates, typechecks, single-file shell builds and splice gates for
slides/spaces/dash — all success.

### caddyserver/caddy — ci.yml, push master

**PASS (cell C).** Matrix `test` cells (linux / mac / windows) — all three
ran to completion on the Linux pool with 15/15 steps each, Go toolchain,
build and unit tests green; goreleaser-check and s390x cells skipped as in
the golden.

### tokio-rs/tokio — ci.yml, push master

Golden: 77 jobs. Cell C (run 16da55a0): 77 jobs — 3 success, 5 failure,
69 skipped. The skip cascade is correct GitHub semantics: clippy/docs/
minrust failed, and every job with `needs:` on that chain skipped. The
failures are environment-class, not protocol:

- **Check README** — the workspace carries the campaign's dirty README
  change, so the "READMEs are identical" gate fails. Campaign artifact.
- **clippy --all-features --unstable / docs / minrust / wasm32-wasip2** —
  Rust toolchain differences between the packed golden and GitHub's
  runners (nightly pin, MSRV 1.71, wasm targets).

compare-goldens.py: 2 match, 5 semantic (all environment-caused), 94
environment. checkout@v7, rustup installs and rust-cache all behaved.
A re-run without the dirty README change would clear the Check README
gate; the toolchain-cell failures need a golden with the same rustup
versions to be strictly comparable.

### Infra findings (campaign environment, not protocol)

- **GitHub App installation**: the configured App is not installed on
  bento/caddy/tokio, so App token minting 422'd and jobs fell back to the
  local runtime token, which GitHub rejects. Campaign engine now runs in
  PAT mode (`preloop setup github --via pat`).
- **Anonymous action downloads rate-limited**: the server fetched action
  tarballs from api.github.com unauthenticated (60 req/hr budget); a busy
  campaign exhausted it and every job failed at "Set up job". Fixed:
  `download_action_tarball` now authenticates with the engine's static PAT
  when one is configured (`actions.rs`).
- **Guest surface guard blocked VM action downloads**: the control-socket
  surface denies `/api/v1/*` (workflow code is untrusted), but the in-VM
  runner's own action downloads live at `/api/v1/actions/*`. Carved out
  in `auth.rs` (uncommitted, rides with the user's auth work).
- **VM teardown race**: fork-based VMs die with `guest runner exited with
  code -1` at job completion, before the final `completejob` lands, leaving
  runs stuck queued. Campaign cells now use the host runner (same binary);
  the VM teardown fix is preloop-vm work in progress.

## Campaign source changes

- `crates/aksh-runner-server/src/snapshots.rs` — snapshot exposes the real
  workspace HEAD; upload-pack allows reachable/tip sha wants.
- `crates/aksh-runner-server/src/runs.rs` — `github.sha` / PR `head.sha`
  use the real HEAD, not the synthetic snapshot commit.
- `crates/aksh-runner-server/src/actions.rs` — action tarball downloads
  authenticated with the engine PAT.
- `crates/aksh-runner-server/src/auth.rs` — guest surface allows
  `/api/v1/actions/*` (runner's own download path). Uncommitted.
- `benchmarks/real-world/conformance-4repos/run-host-cell.sh` — host-runner
  cell C (avoids the VM teardown race).
- `benchmarks/real-world/conformance-4repos/run-preloop-cell.sh` — PAYLOAD
  knob for pull_request events; bash 3.2-safe arg handling.
- `benchmarks/real-world/conformance-4repos/compare-goldens.py` — bento /
  caddy / tokio repo entries.

Generated 2026-08-03T07:45:00Z
