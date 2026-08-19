# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases before v0.27.0 predate the changelog.

## [Unreleased]

### Fixed

- SmolVM compatibility now comes from the central `versions.toml`
  `smolvm_min_version` pin. `preloop-cli` and `preloop-vm` compile the same
  floor, and `preloop update --ensure-runtime` installs the latest stable
  SmolVM when the local runtime is below it or lacks required capabilities.
  Preloop relies on SmolVM's packed-ownership implementation instead of
  rewriting guest rootfs ownership.
- A golden download had 10 minutes to complete, body included. The packed
  arm64 golden is ~9.6 GB, so that budget demanded 128 Mbps sustained and
  was unreachable on an ordinary link (measured: 84 Mbps from ghcr.io, so
  the pull needs ~15 minutes). The deadline killed the transfer around
  two-thirds through, `preloop serve` reported the official golden as
  "unavailable", and the run fell through to a local bake. The budget is
  now one hour, which covers any link above ~21 Mbps.

### Changed

- Golden download progress now reads as a percentage and a completion bar
  in megabytes — `golden download (OCI): [########------------] 40%
  (3850 MB / 9630 MB)` — instead of raw byte counts, and reports every
  256 MB rather than every 1 GiB (about one line per 25 s on a 100 Mbps
  link).

## [0.30.10] - 2026-08-18

### Fixed

- A crashed server orphaned its detached `_boot-vm` hypervisor processes:
  the pool never stopped them on death, and if the machine data dir was
  cleaned out from under them (a home cleanup), the smolvm DB no longer
  knew the machines — the `_boot-vm` kept the storage fds open and the
  unlinked blocks leaked until the process exited (observed holding
  hundreds of GB for 47 h). Pool startup and shutdown now purge orphaned
  `_boot-vm` processes by their boot-config path under the Preloop home,
  SIGKILLing whatever the smolvm delete could not reach.

## [0.30.9] - 2026-08-18

### Fixed

- The exec-as-image-user branch of the runner wrapper used
  `setpriv --init-groups`, which fails as a non-root user (setgroups needs
  root) — so on the official golden (image USER runner) every configure/run
  exec died with `initgroups failed: Operation not permitted`. The non-root
  branch now self-drops with `--keep-groups` (verified on the official
  smolvm: both the root and image-user branches land on uid 1001).
- The packed-golden path now adopts an existing running, fingerprint-matched
  golden instead of re-unpacking tens of GiB on every `serve` restart.
- `conformance-5repos.sh` campaign fixes: golden symlink carries the
  environment fingerprint, stale campaign home is cleaned between runs,
  deno targets the generated `ci.generated.yml`, and the runner storage
  default is 160 GiB (the runner-large golden unpacks past 80 GiB).

## [0.30.8] - 2026-08-18

### Changed

- Guest commands no longer run through `smolvm machine exec --user root`
  (a flag only the retained smolvm fork shipped). The wrapper now branches on
  the uid it lands on: root runs the provisioning directly (locally baked
  goldens), any other image user runs it via passwordless sudo (the official
  runner image declares `USER runner`), then the runner still drops to uid
  1001 via `setpriv`. This removes the fork dependency entirely: `preloop
  update --ensure-runtime` installs the official smolvm again and v0.30.7's
  fork-pointing is reverted.

## [0.30.7] - 2026-08-18

### Fixed

- Every pool exec runs as root via `smolvm machine exec --user root`, a flag
  that only exists in the retained fork's smolvm 1.8.2+ — but `preloop
  update --ensure-runtime` installed the official smolvm, so on fresh
  machines every golden/runner exec failed with `unexpected argument
  '--user' found`. The runtime now comes from the retained fork
  (`preloopdev/smolvm` v1.8.2 line), the compatibility probe also checks
  `machine exec --user`, and `smolvm_min_version` is raised to 1.8.2.

## [0.30.6] - 2026-08-17

### Fixed

- The SmolVM guest agent rootfs was never found on standard installs: it
  lives in smolvm's platform data directory (`~/Library/Application
  Support/smolvm` on macOS, `~/.local/share/smolvm` on Linux), but the
  runtime environment only probed the derived Preloop data dir and the
  legacy `~/.smolvm` layout. With the isolated macOS `HOME`, every golden
  machine start failed with `verify rootfs: agent rootfs not found`. The
  probe now checks the real host's platform data dir first, then the legacy
  location, and an explicit `SMOLVM_DATA_DIR` still wins.

## [0.30.5] - 2026-08-17

### Fixed

- OCI golden download decompressed the packed layer, but the published
  `application/vnd.preloop.smolmachine.v1+zstd` layer is the raw
  `.smolmachine` sidecar — zstd asset frames followed by the uncompressed
  manifest and `SMOLPACK` footer — so every OCI pull failed and fell back to
  the release asset or a slow local bake. The download now verifies the
  layer digest and installs the sidecar as-is, which `machine create --from`
  consumes directly.

## [0.30.4] - 2026-08-17

### Added

- The official-runner packed golden is now the arm64 default:
  `download_prebaked_golden` pulls the digest-pinned OCI artifact
  (`ghcr.io/preloopdev/preloop-golden@sha256:a2f7caf3…`, overridable with
  `PRELOOP_GOLDEN_OCI_REF`) when no `PRELOOP_GOLDEN_URL` is configured, with
  bearer-token registry auth, layer digest verification, and zstd decoding of
  the packed VM layer. The release asset remains the fallback and
  `PRELOOP_GOLDEN_URL` still selects it over the OCI default.
- `PRELOOP_CLIENT_TIMEOUT_SECONDS` bounds runner-client requests; rejected
  workflow submissions now surface the server status and body.
- `benchmarks/real-world/conformance-5repos.sh` plus `just conform-5repos`:
  five-repository campaign runner against the official runner golden.

### Fixed

- OCI golden download parsed the layer descriptor's `mediaType` as
  `media_type` (every standard manifest failed to parse, silently falling
  back to the release asset) and installed the compressed layer without
  decoding it; the download now renames the field, logs parse failures, and
  zstd-decodes the verified layer into the `.smolmachine` payload.
- SmolVM runtime environment now applies to recovery commands (status, list,
  stop, delete), so they target the same registry and macOS `HOME` as boot;
  derived `SMOLVM_DATA_DIR` and macOS `HOME` directories are created before
  spawn; macOS `HOME` isolation works without an explicit `PRELOOP_HOME`;
  the agent rootfs is probed from the SmolVM data directory first.
- Runner-client remote workflow fetches reuse the timeout-configured client
  (`lint` can no longer hang on a stalled GitHub API request).
- conformance campaign script: INT/TERM traps exit with the conventional
  statuses instead of resuming, curl calls are bounded, and polling fails
  fast when the local server dies.

## [0.30.3] - 2026-08-13

### Fixed

- `preloop update` is now content-aware when the remote release version
  equals the installed version: it downloads the checksummed release asset,
  verifies its SHA-256, and byte-compares the extracted binary against the
  installed executable, reinstalling on mismatch. A version string is
  self-reported and can lie (a source build or tampered binary claiming a
  release version), so the old version-only gate declared such installs up
  to date forever — this is how the v0.30.2 deaf-runner fix never reached
  production. Lower versions still never downgrade; a failed content check
  (fetch/checksum error) keeps the installed binary and retries next run.

### Security

- Fail closed on cache writes when the calling job no longer resolves. A
  fork PR job's runtime JWT survives the job's retirement
  (`RequestRetirement::Purge` drops the correlation records the fork-tier
  lookup walks); treating that unresolvable token as a control-plane caller
  let a fork worker smuggle a cache write past the read-only guard with a
  leaked token. `fork_restricted_from_token` now denies any job-shaped
  token whose subject/scope no longer resolves to a live job, instead of
  only when it positively resolves to a fork-restricted tier. Non-job
  bearers (system token, runner-listen, debug-worker) are unaffected.

## [0.30.2] - 2026-08-13

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

- Enforce GitHub's read-only fork profile for untrusted fork pull requests:
  fork PR jobs (and fail-closed unknown events) now receive a
  `GITHUB_TOKEN` permission set clamped to read regardless of the workflow's
  declared `permissions:` block, never get an OIDC request URL or token
  grant, and can no longer receive the configured PAT — neither as a
  mint-failure fallback nor as the build-time PAT override. The special
  `id-token` permission is excluded rather than advertised as a read scope,
  and the App installation-token request carries only real App repository
  permissions (never `id-token`) for trusted jobs too. The trust tier is
  applied as a single job-authorization policy shared by the runner wire
  variable, the App installation-token request, the OIDC grant, and the
  token fallback path, so a fork PR declaring `checks: write` and
  `id-token: write` is downgraded end to end while `pull_request_target`,
  internal PRs, push, schedule, and deployment runs keep their declared
  permissions. Broker claims now keep every runner-visible token alias
  (`system.github.token`, `github_token`, `GITHUB_TOKEN`, and the `github`
  context token) on the minted App token, restate narrowed installation
  grants without erasing a trusted job's `IdToken` metadata, and treat
  persisted token requests that predate the `untrusted` field as untrusted
  so a restart can never re-enable the PAT fallback for them.

- Fork PR runs also get GitHub's read-only cache access: cache writes (the
  `/_apis/artifactcache` reserve/upload/commit routes and the Twirp
  `CreateCacheEntry`/`FinalizeCacheEntryUpload` handlers) are refused with
  403 for fork-restricted jobs while restores stay open — a fork can no
  longer poison cache entries that a trusted run later restores.

- A deferred GitHub App token request no longer outlives the job it was built
  for. It is deliberately retained past the first claim so a re-claim after a
  runner disconnect re-mints under the build-time permission set instead of
  rebuilding from the broader defaults, and it is now dropped wherever a job
  request becomes terminal — the shared completion path (broker `completejob`,
  the legacy `/_apis` finish endpoints, and the lease-expiry reaper alike) and
  the scheduler's node retirement for cancelled, skipped, and
  expansion-failed nodes.

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

- The runner's in-guest control bridge no longer deafens the runner on a
  transient upstream outage. The bridge previously exited after 10
  consecutive upstream connect failures; when the guest network was not yet
  up at VM fork, the runner's first polls burned that budget and the bridge
  died permanently — the runner kept polling a dead loopback address
  ("Connection refused") while its job sat in_progress with no logs. The
  bridge now stays up and retries forever, matching the runner's own poll
  loop, so a brief boot-window outage self-heals.
- The control plane now sweeps runner sessions that stop polling and reaps
  the deaf runner: the unfinished job is requeued for a fresh machine and
  the dead VM is recycled, bounding the stall to
  `PRELOOP_RUNNER_LIVENESS_TIMEOUT_SECS` (default 30 minutes) instead of
  the 45-minute job lease (which failed the job rather than requeueing it).
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

## [0.30.1] - 2026-08-11

### Added

- Sign and attest golden packs, and verify base image provenance when
  building goldens.

### Fixed

- Switch runtime acquisition from the temporary preloopdev/smolvm fork to
  the official smol-machines/smolvm v1.7.7 release, which carries reusable
  retained-fork checkpoints and the macOS network symbol preloop-vm needs
  (#117, #118). `preloop update` and the golden-release workflow now install
  the compressed `.zst` disk templates 1.7.7 ships.
- Upgrading smolvm now removes a previous install's uncompressed
  storage/overlay templates before copying the archive's variants, so an
  upgraded installation can no longer keep silently using the old 1.7.4
  payload.
- Webhook ingestion: explicit body limit above GitHub's payload ceiling
  (#116).

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

[Unreleased]: https://github.com/preloopdev/preloop/compare/v0.30.3...HEAD
[0.30.3]: https://github.com/preloopdev/preloop/compare/v0.30.2...v0.30.3
[0.29.8]: https://github.com/preloopdev/preloop/compare/v0.29.7...v0.29.8
[0.29.7]: https://github.com/preloopdev/preloop/compare/v0.29.6...v0.29.7
[0.29.6]: https://github.com/preloopdev/preloop/compare/v0.29.5...v0.29.6
[0.29.1]: https://github.com/preloopdev/preloop/compare/v0.29.0...v0.29.1
[0.29.0]: https://github.com/preloopdev/preloop/compare/v0.28.0...v0.29.0
[0.28.0]: https://github.com/preloopdev/preloop/compare/v0.27.0...v0.28.0
[0.27.0]: https://github.com/preloopdev/preloop/releases/tag/v0.27.0
