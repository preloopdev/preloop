# Real-world repo conformance campaign — 2026-08-05

Four medium-sized repos run unmodified against the aksh stack (engine
`:9091`, host runner + smolvm Linux pool), following the earlier openclaw /
aksh-trigger campaigns. Goal: exercise real-world workflows end to end and
fix environmental issues (smolvm) that block them.

## Repos

| Repo | Workflow | Result | Notes |
|---|---|---|---|
| `bento` (bento.dev slides) | ci.yml | **SUCCESS** | full gate: checkout, setup-node, npm ci, tsc builds, i18n checks, shell-gate |
| `caddy` | ci.yml | **SUCCESS** | full gate: go vet/build/test matrix (linux/mac/windows cells) |
| `tokio` | ci.yml | **partial** | basics ✗ taskdump (mac host); clippy ✗ on mac → **✓ on smolvm Linux VM**; minrust ✗ (taskdump / VM rerun log-less) |
| `uv` | ci.yml | **partial** | plan ✓; linux-aarch64 ✓ (VM); linux-libc ✓ (VM); macos-aarch64 ✓ (host); linux-musl ✗; armv7/windows cells in flight |

## Environmental findings

1. **Host platform mismatch (mac host runner, Linux-targeted workflows)**
   - tokio's `taskdump` feature is Linux-only: `compile_error!("The taskdump
     feature is only currently supported on linux, aarch64, x86, x86_64 and
     s390x")` — the mac host fails every all-features job.
   - uv's ci needs `rustup` and assumes the hosted runner layout
     (`mkdir /Users/runner: Permission denied` from setup-python's cache
     paths).
   - **Fix**: enable the local smolvm Linux pool for the engine
     (`PRELOOP_RUNNER_POOL_ENABLED=1 PRELOOP_RUNNER_POOL_SIZE=2
     PRELOOP_RUNNER_USE_FORK=false PRELOOP_RUNNER_LABELS=X64`). Linux-targeted
     jobs now claim the pool VMs: **tokio clippy (all-features, taskdump)
     passes on the VM** — previously failed on the host.
2. **smolvm state pileup**: 395 stuck `created` machines from earlier
   pool-enabled experiments (each engine start created machines that never
   started; the control bridge never came up). All cleaned; the stale
   pool-enabled engine on `:9090` (old build, no runs API) stopped.
3. **Pool labels**: the pool's default labels carry the host arch
   (`aarch64`), so `X64`-labelled jobs stayed queued. `PRELOOP_RUNNER_LABELS`
   adds the missing labels.
4. **taiki-e/install-action**: one early failure — `'checksum' input option
   must be 'true' or 'false': ''` — input default not applied. Not
   reproducible on the current binary (composite, local-node, and remote
   install-action probes all apply defaults); treated as transient.
5. **Runner env hygiene**: probe workflows show `0` `BASH_FUNC_*` vars in
   step envs — the install-action's bash-function-injection guard does not
   trip (contrary to one earlier log read).

## Verification

- bento + caddy full workflows pass on the host runner.
- tokio clippy passes on the smolvm Linux VM after the pool fix.
- uv matrix: linux/aarch64 + libc builds pass on the VM; macos build passes
  on the host.

## Open items

- tokio minrust and uv linux-musl failed with empty run logs on the
  restarted engine (log capture for post-restart runs needs a check).
- PR #19's `plan` job still needs the App installation to grant
  `contents: write` (see the PR thread) or an engine-side fallback.
