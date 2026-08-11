# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases before v0.27.0 predate the changelog.

## [Unreleased]

### Added

- `docs/internal/threatmodel.md`: threat model overview — attacker
  assumptions, deployment topologies, defenses enforced for each attack
  class (VM escape, hostile egress, secret theft, control-plane
  impersonation, resource sabotage, supply chain), and candid current
  limitations (internal doc).

### Security

- Harden standalone SmolVM execution for hostile workflow code: every Linux
  operation that can boot or restart a machine (create, start, start_forkable,
  fork, exec, pack) and every direct `smolvm machine exec`/`cp`/`shell` call
  in the CLI (which connect to a machine, starting it when stopped) now run
  `smolvm` with `SMOLVM_SECCOMP=enforce` and `SMOLVM_LANDLOCK=enforce` — the
  hardening `smolvm serve` applies — so each `_boot-vm` is confined by
  the syscall allowlist and filesystem Landlock rules instead of only
  `harden_self`. A pre-set operator value wins (upstream precedence) but is
  validated: modes SmolVM does not recognize fail the operation rather than
  silently booting unconfined. macOS is unchanged (both controls are no-ops
  upstream there). The policy is one exported function shared by the
  provider and the CLI, with per-path command-environment tests.
  Note that Landlock matches upstream exactly, while seccomp does not:
  upstream `serve` defaults it on Linux/x86_64 only, though the boot
  subprocess honours it on aarch64 too, so on Linux/aarch64 Preloop enables a
  filter upstream leaves off. `docs/setup.md` documents the `Seccomp: 2`
  check to confirm it on a new aarch64 host.
- Per-VM host resource containment on Linux: the systemd service delegates
  its cgroup subtree (`Delegate=cpu memory pids`) so every `_boot-vm` places
  itself in a `vm-<pid>` leaf capped on CPU, PIDs, and memory. `Delegate=`
  alone is insufficient — systemd chowns the subtree to the service user but
  leaves `cgroup.subtree_control` empty, so child leaves get no limit files —
  so the **server** performs the same one-time setup `smolvm serve` does
  (vacate into a `preloop-supervisor` leaf, then enable the controllers on the
  now-empty unit cgroup) via an explicit `init_vm_cgroup_delegation()` at
  startup. The CLI never calls it: `preloop shell` and the debug session use a
  read-only check and never mutate the cgroup hierarchy. No usable delegation,
  no variable; the standalone path never claims per-VM UID isolation, which
  requires a privileged supervisor.
- The generated systemd service now runs under a dedicated `preloop` system
  account (created at install, `kvm` group when `/dev/kvm` exists) instead of
  root: a guest→VMM escape inherits the service identity, so root would hand
  it the host. The unit adds an empty capability bounding set,
  `ProtectKernelModules`/`ProtectKernelLogs`/`ProtectClock`,
  `LockPersonality`, `RestrictRealtime`, and — critically — no longer grants
  the serving unit write access to its own executable (only the root update
  oneshot can replace the binary). State paths, socket activation, networking,
  and `/dev/kvm` access are preserved; SmolVM data is pinned under
  `PRELOOP_HOME/smolvm` and the installer bootstraps a service-visible
  smolvm when only a root-home install exists — copied to
  `/usr/local/lib/preloop/smolvm-prefix` and refreshed on re-install when the
  source is newer, never shadowing an independently installed system binary.
  The refresh is atomic: the new prefix is fully assembled in a sibling
  staging directory and swapped into place with a rename, so a running service
  never observes a mixed prefix; staging and backup are cleaned up on success
  and failure alike.
- **Keep privileged install artifacts out of the service-writable state dir.**
  `PRELOOP_HOME` must be writable by the service, and on Unix the *directory*
  write bit governs unlink and rename, so anything inside it can be replaced
  by the service whatever the file's own owner and mode are. Three artifacts
  therefore moved out, each closing a reproduced escalation:
  - the bootstrapped smolvm prefix now lives at
    `/usr/local/lib/preloop/smolvm-prefix`, root-owned and `a+rX` (never
    chowned to the service). `/usr/local/bin/smolvm` points into it and root
    executes that path when `preloop update` probes smolvm, so the previous
    service-owned copy was a direct service-user → root escalation. Re-install
    also repairs an already-chowned prefix.
  - the systemd environment file now lives at `/etc/preloop/environment`,
    `root:root` 0600. It previously ended up service-owned after any
    re-install (the state-dir chown preceded the rewrite, and the rewrite
    truncates in place rather than replacing the inode), and because
    `EnvironmentFile=` overrides the unit's `Environment=`, a compromised VMM
    could persist `SMOLVM_SECCOMP=off` and return unconfined via
    `Restart=on-failure`.
  - the staged GitHub App key now lives at
    `/etc/preloop/github-app-key.pem`, `root:preloop` 0640 in a
    `root:preloop` 0750 directory — readable by the service, writable only by
    root, and no longer replaceable by it.
  `preloop server uninstall` removes all three, so secrets no longer outlive
  an uninstall that only purges the state dir.

### Changed

- `preloop server install` (Linux, system scope) creates the `preloop` service
  account, chowns the state dir to it, stages a `root:preloop` 0640 copy of
  `--github-app-key` into `/etc/preloop` (the caller's original is never
  modified — chowning a key under `/root` would be useless because the service
  user cannot traverse the parent), preparing the directories first so the very
  install with a key works against a not-yet-existing (default or nested)
  `--home`, and prints the `sudo -u preloop env PRELOOP_HOME=… preloop setup
  github --save` flow for writing `config.toml`; see `docs/setup.md` "VM
  sandbox (Linux)" for the verification procedure and the macOS limitation.
- `preloop server install` now rejects a system-scope `--home` under `/home`,
  `/root`, or `/run/user`: the `preloop` account cannot traverse those
  whatever the state dir's own mode is, so the previous `ReadWritePaths`
  carve-out produced a unit that looked correct and failed at first start.

### Fixed

- `preloop-cli` and `preloop-vm` did not compile for Linux at all (a
  use-after-move in `add_to_kvm_group`, a `format!` arity bug that also
  silently dropped the webhook hint from the install summary, and two test
  errors). Every one of these lives behind `#[cfg(target_os = "linux")]`, so a
  macOS development host never parsed them, and the work had not yet been
  pushed for CI — which does build on Linux — to see.
  Also fixed the pre-existing `clippy::needless_return` in
  `preloop-socket-activation`, and the swapped `dir`/timer arguments that made
  the install summary print
  `units:  + preloop-update.{service,timer}/preloop.{service,socket}/etc/...`.

## [0.30.0] - 2026-08-11

### Security

- Confine SmolVM VMMs against hostile workflow code (#114): one sandbox policy
  (seccomp + Landlock) applies to every operation that can boot or restart a
  VMM — including `preloop shell` and debug-session exec paths — and
  operator-set values are validated instead of silently booting unconfined on
  a typo.
- Run the control plane as a dedicated non-root service identity (#114): the
  smolvm prefix, the environment file, the staged App key, and the engine
  state each move to least-privilege ownership, closing reproduced privilege
  escalations (root execution through the smolvm wrapper, service-persisted
  `SMOLVM_SECCOMP=off` via EnvironmentFile, and a writable App key). `server
  uninstall` removes the moved artifacts so secrets do not outlive the state
  dir.

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
