# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases before v0.27.0 predate the changelog.

## [Unreleased]

## [0.29.9-rc] - 2026-08-11

### Fixed

- Serialize `machine fork` per golden VM (#112). SmolVM keeps one RAM
  checkpoint per golden; concurrent forks raced the freeze, and the loser's
  rollback resumed the base and dropped the checkpoint, after which every
  fork failed with `golden '<name>' is already paused; a valid retained
  checkpoint is required` and queued jobs stalled until the engine restarted.
  Forks from different goldens still run in parallel.
- Re-arm a spent fork base atomically and retry the fork once (#112): partial
  clone cleanup, the live-clone check, and the stop/restart run under the
  provider's per-golden fork lock, and cleanup must succeed before the base
  is touched. A base with live clones is never restarted — it falls back to
  direct creation with an error explaining why.
- `preloop debug <reference>` now resolves a run id that has several paused
  jobs: it lists them (with their run ids) instead of answering
  `no paused job matching`; non-404 failures propagate their real cause.
  When nothing matches but other sessions are paused, the reply says what is
  paused instead of a bare 404 (#112).
- Persist GitHub check-run ids at creation so a server restart mid-queue no
  longer orphans checks in "queued" forever while the jobs run and complete
  (#113).
- Dead-bound job requeue: stale bindings stay claimable in strict non-pool
  mode (no more stranded jobs), and the original first-bound stamp survives
  the requeue so repeated provisioning failures cannot extend the bounded
  claim window (#113).

## [0.29.8] - 2026-08-09

### Added

- Custom base images (`PRELOOP_RUNNER_BASE_IMAGE` / `build-golden --base-image`)
  are now used as-is: the curated toolchain bake applies only to the stock
  digest-pinned Ubuntu bases, so an operator's own image is never modified.
- `check_run` / `check_suite` rerequest webhook handling, so GitHub's "re-run
  failed jobs" lands on the right run.
- Background-step execution in the runner, and runner-internal job variables
  are filtered out of step environments.
- Docs-only pull requests skip CI via `paths-ignore` filters.

### Fixed

- Debug-session starvation: a job paused in a debug session no longer pins a
  pool concurrency permit — the pool releases the slot for the pause's
  duration and re-acquires it on resume, so unanswered sessions cannot freeze
  the pool.
- Stale snapshot checkout tokens: the credential pinned onto the checkout
  step at submission is re-minted when a job is finally claimed and refreshed
  again on retry verdicts; the snapshot Git surface answers with a Bearer
  challenge instead of prompting for a username.
- `preloop run` reports queued-job and paused-session counts when a run
  stalls instead of hanging silently, and detaches cleanly from the debug
  prompt.
- `macos`/`windows` jobs wait for a registered external host instead of being
  failed by the Linux-only starvation sweep.
- macOS BSD `tar` missing `--verbatim-files-from` is handled in sync.

## [0.29.8] - 2026-08-09

### Fixed

- Preserve remote action references across job restarts: the job wire format
  now uses the canonical `ref` field and still accepts the legacy `version`
  name when reading.
- Recover golden runner provisioning when the exact hosted package pins are
  unavailable by falling back to archive versions.
- Complete cancellation bookkeeping so cancelled runs settle terminal state
  and next-job label scheduling stays in sync.
- Expose the worker half of live debugging (token exchange, session verdict,
  and close) through the runner control socket.
- Fall back to direct VM creation when forking the default packed golden
  fails, without changing the job image for environment-specific goldens.

## [0.29.7] - 2026-08-09

### Fixed

- Keep packed golden forking enabled when the warm runner pool is disabled.
- Avoid pre-provisioning unused replacement runners in on-demand mode.
- Treat routine Unix socket shutdowns as debug-level teardown noise.
- Default local pull request runs to the `synchronize` activity.

## [0.29.6] - 2026-08-09

<!-- preloop:skip-golden -->

### Added

- Document stock and custom golden image construction, publishing, and
  runtime configuration.

### Fixed

- Enable packed golden downloads by default.
- Build the packed-golden release URL from the CLI release version rather than
  the independently versioned orchestrator crate.
- Pin stock Ubuntu bases to the explicit `mirror.gcr.io` registry so fallback
  provisioning does not depend on unauthenticated Docker Hub pulls.

## [0.29.5] - 2026-08-08

### Added

- `preloop dap`: integrated DAP client for debugger-enabled runs (demo under `docs/demo/dap`)
- Pool: pause the queued-job starvation clock while the warm runs, so the first job on a fresh machine survives the artifact download or build
- Pool: verify the downloaded pre-baked golden against the release checksum before using it
- Release CI: publish the golden checksum and build the aarch64 golden on GitHub-hosted macOS runners

### Changed

- smolvm installs are pinned to 1.7.4, the last macOS release exposing the virtio-net symbol; virtio-net remains the default net backend

### Fixed

- Pool: on-demand slot failure backoff now escalates across reap cycles instead of resetting every cycle
- `preloop update`: install a smolvm with `--mount-socket` support, warn when PATH shadows the install, and preserve symlinks when copying the agent rootfs
- `preloop serve`: report the GitHub App stored in `config.toml` when env vars are absent

## [0.29.1] - 2026-08-08

### Fixed

- `preloop setup`: omit the webhook when there is no public URL

### Changed

- `README`: play the demo inline as an animated GIF, link upstream smolvm
- `preloop setup` docs: explain the app-vs-webhooks decision and named-tunnel persistence
- Add the caching strategy plan under `docs/plans`

## [0.29.0] - 2026-08-08

### Added

- `preloop setup`: include `hook_attributes.url` in the GitHub App manifest

### Changed

- Pool: drop the environment resolver; bake a fixed curated toolset
- CLI: default to the TCP native surface instead of the guest unix socket
- Linux runner bundle handling: install matches cargo-dist asset names, update always ensures the bundle on macOS, engine warns about the missing bundle with the remedy
- Remove the pullfrog workflow entirely
- Release cross-build pins the rust toolchain for the runner-bundle build

### Fixed

- Server: fail queued jobs no runner can ever claim, with a reason
- Pool: keep the Go resolver's indentation through the string escape

## [0.28.0] - 2026-08-08

First release through the cargo-dist binary pipeline: `preloop-cli`
installers for macOS and Linux (shell and PowerShell), checksums, and the
source tarball.

Large accumulation of work since v0.27.0. By scoped change count the
dominant areas were protocol (43), runner (31), server (29), tooling (18),
live-logs (8), and golden (8).

## [0.27.0] - 2026-08-07

Bootstrap the cargo-dist release pipeline for `preloop-cli` (binary
installers for macOS and Linux).

[Unreleased]: https://github.com/preloopdev/preloop/compare/v0.29.8...HEAD
[0.29.8]: https://github.com/preloopdev/preloop/compare/v0.29.7...v0.29.8
[0.29.7]: https://github.com/preloopdev/preloop/compare/v0.29.6...v0.29.7
[0.29.6]: https://github.com/preloopdev/preloop/compare/v0.29.5...v0.29.6
[0.29.1]: https://github.com/preloopdev/preloop/compare/v0.29.0...v0.29.1
[0.29.0]: https://github.com/preloopdev/preloop/compare/v0.28.0...v0.29.0
[0.28.0]: https://github.com/preloopdev/preloop/compare/v0.27.0...v0.28.0
[0.27.0]: https://github.com/preloopdev/preloop/releases/tag/v0.27.0
