# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Releases before v0.27.0 predate the changelog.
## [Unreleased]


### Security

- **The static GitHub PAT no longer crosses the network over plain HTTP**:
  action resolution and action tarball downloads attached the PAT to any
  request for a configured GitHub origin regardless of scheme, so a
  configured *remote* `http://` API URL received the token in cleartext. Plain
  HTTP now qualifies only for a loopback origin (`localhost`, `127.0.0.0/8`,
  `::1`) — local emulators such as gh-simulate keep authenticating — and
  every other origin requires HTTPS. Point a remote emulator or GHES at an
  `https://` URL (with `PRELOOP_GITHUB_CA_FILE` for a private CA) to keep it
  authenticated.

### Fixed

- **Job containers can reach the engine again** (#F15, local mode): the engine
  advertises itself to jobs at its loopback origin (the runner's in-guest
  control bridge), which a container's network namespace resolves to the
  container itself — so every `container:` job whose first step was a
  redirected `actions/checkout`, plus anything using cache, artifacts or OIDC
  from a container (`ACTIONS_*`), died with `connect ECONNREFUSED
  127.0.0.1:<port>` (valkey's `debian:bookworm` and `almalinux:8` legs). The
  runner now binds the same bridge on the job network's gateway (free port,
  one job per VM) and rewrites the engine origin in every environment a
  container process receives — `docker exec` for run steps and node actions,
  `docker run` for `docker://` actions, and the workflow-declared env of the
  job and service containers themselves — the same layer where the official
  runner rewrites container paths — and hands containers an injected
  `NO_PROXY`/`no_proxy` list already carrying the container-facing address
  (a list the workflow declares is extended in place; a host with no list
  gets none). Hosted deployments, where the engine URL is routable and no
  control bridge exists, are untouched. Host steps,
  the runner itself, and the official runner's Docker command shape are
  unchanged.

- **Server integration tests no longer fail on a leaked static PAT**:
  `cargo test` shares one process environment across a whole test binary, so
  a `PRELOOP_GITHUB_TOKEN` set by a neighbouring test — or injected into the
  job VM — could turn an unrelated submit into a live 401 and a
  `403 refusing to embed an invalid PAT`. Test-support builds now treat the
  default GitHub API base as unverifiable and never introspect it over the
  network: the PAT is withheld and the run proceeds on the job-scoped runtime
  token, while a test that wants a verdict points `PRELOOP_GITHUB_API_URL` at
  its own stub. The action-resolution test now pins the behavior
  `#351` introduced: the PAT follows the *configured* GitHub origin, and never
  follows a request to an unconfigured origin.

- **Disconnected-runner lease test tracks the actual reaper boundary**:
  the integration test now brackets the 10-minute dead-session threshold,
  rather than the 45-minute runner-facing lock, with enough headroom to
  remain deterministic under CI load.

- **Linux packed-golden runners survive engine restarts again** (#371): the
  orphaned-data-dir sweep treated every unregistered directory under SmolVM's
  `vms/` as a leaked machine, including SmolVM's own shared pack store
  (`_shared/`) that every packed machine's lease points into. After a restart
  each fork failed with `copy shared pack lease: No such file or directory`,
  and the direct-create fallback re-extracted the whole golden (~90 GB) per
  runner. The sweep now only removes 16-hex machine data dirs.

- **A create in flight keeps its data dir through the orphan sweep** (#371):
  SmolVM creates a machine's data directory before it publishes the registry
  row, and a packed create extracts for minutes — longer than the sweep's
  120 s grace — so a sweep tick could delete the live, still unregistered
  directory, and the same tick's `_boot-vm` purge then killed the create
  (`smolvm create failed with exit code -1`). `machine create` and `machine
  pack` hold the lifecycle lock for their whole run; the sweep now holds the
  read side across its registry snapshot and filesystem scan and waits for a
  create in flight. `machine fork` stays concurrent by design (its clone
  directory is created fresh, so the grace window covers it until its
  registry row lands), and a directory left behind by a create whose engine
  died is still reclaimed after a restart.

- **The official golden unpacks without `SMOLVM_PACK_MAX_EXTRACT_BYTES`**
  (#371): SmolVM caps extraction at 128 GiB of declared size, which the
  golden's sparse disks exceed. Preloop now passes a 512 GiB ceiling to SmolVM
  unless the operator sets one.

- **Scheduled golden refreshes no longer launch an impossible hosted bake**:
  the apt-index freshness workflow now opens one idempotent draft PR asking for
  a manual host-side rebuild. Webhook `workflow_dispatch` boolean inputs are
  coerced before job conditions, so GitHub's string `"false"` no longer runs a
  truthy-gated job; the reuse path also resolves the target release without
  requiring a checkout.

- **Skipped checks now settle instead of remaining queued**: GitHub may return
  `404 Not Found` when an immediate completion PATCH races replication of a
  newly created check run. Preloop retries that narrow case, allowing
  submit-time `if:` skips to report `completed/skipped`. Automated golden
  reminder PRs now carry `[skip ci]` and `[skip preloop]` markers so the draft
  notification does not consume GitHub or Preloop runners.

- **Packed goldens on Apple Silicon bake again**: the Rosetta multiarch step
  spliced an apt-scoping fragment that ended in a newline, so the composed
  guest script had a line starting with `;` and every fresh packed-golden
  bake failed with `sh: 15: Syntax error: ";" unexpected`. The pool then fell
  back to direct per-runner creation from the plain Ubuntu base, so jobs ran
  without the official runner image's tools (`cmake`,
  `/opt/hostedtoolcache`). Regressed in 0.33.8; a test now parses the full
  composed script.

- **Runners register on images that run as a non-root user**: the official
  runner image declares `USER runner`, so the pool runs `preloop-runner
  configure` through `sudo`, whose `env_reset` dropped the injected
  `PRELOOP_RUNNER_TOKEN`. Every pooled runner failed provisioning with
  `--token <TOKEN> not provided`. The command now runs under
  `sudo --preserve-env`.

- **A workflow-set `PATH` is used verbatim**: when a job or step set
  `env: PATH`, the runner still prepended `/home/runner/.cargo/bin` and
  `/home/runner/go/bin` whenever those directories existed (as they do on
  the official runner image). GitHub-hosted runners leave an explicit `PATH`
  alone; the toolchain shims now only extend the default.

- **Runner no longer drops the job handed out at job completion**: when a
  job finished, `preloop-runner` aborted its in-flight `status=Busy` broker
  poll, the same pattern as actions/runner#4728. On github.com the service
  hands the next job out on that poll, so the job was lost and later cancelled
  as "not acquired by Runner". The poll now stays open across job completion
  and the new status goes out on the next poll. To match, the server ends a
  Busy poll within one second once the session's job has finished, rather than
  holding it for the full 50 seconds, so the following Online poll claims the
  next job without delay. The server still never dispatches a job on a Busy
  poll.
- **Broker poll client timeout outlasts the server's long poll**: the runner
  gave up on `GET /message` after 50 s, the same length as the broker's
  long-poll window. A job claimed in the final moments of that window was
  written to a request the runner had already abandoned. The poll timeout is
  now 100 s, matching the official runner's default `SendTimeout`.
- **Empty broker polls are no longer read as messages**: a `200` with a
  `null` body was treated as a message with id 0 and type `unknown`. It is
  now an empty poll, as in the official listener.

## [0.33.9] - 2026-10-02

### Changed

- **Standalone runner release builds target Linux and macOS.** Windows users
  can run the Linux runner under WSL2; the native Windows matrix leg and its
  release assets are no longer built. On a co-hosted Preloop engine, set
  `PRELOOP_GITHUB_SKIP_WORKFLOWS=release-runner.yml` to let GitHub Actions
  alone own that release workflow and avoid duplicate checks.
- **Golden smolvm updated to 1.19.0** (`a6072aae`): the verified-release
  golden pin tracks upstream; `smolvm_min_version` remains the runtime floor.

### Fixed

- **Ubuntu 22.04 environment goldens on Apple Silicon.** Rosetta multiarch
  setup assumed Ubuntu 24.04's `ubuntu.sources`; the pinned 22.04 rootfs uses
  `/etc/apt/sources.list`. Both layouts now scope native apt sources to arm64
  before adding amd64 repositories, so 22.04 jobs can provision runners
  instead of looping on a missing-file error. The bake additionally uses a
  status-preserving sudo wrapper, so a refused passwordless sudo or a failed
  apt step fails the bake instead of producing a golden without the amd64
  loader.

## [0.33.8] - 2026-10-02

### Added

- The control plane now enforces per-namespace state and quotas on both store
  backends. A `suspended` or `deleted` namespace starts no jobs; a `draining`
  one finishes its queued jobs. `namespace_limits.max_running_jobs` and
  `namespace_pool_limits` cap concurrent claims (on Postgres a capped claim
  locks the limit row, so two nodes cannot both take the last slot); jobs over
  a cap wait queued and are never bound to an idle warm runner. API and CLI
  submits need an `active` namespace and are checked against
  `max_queued_jobs` and `submit_rate_per_minute` (HTTP 429) and
  `max_jobs_per_run` (HTTP 400). Webhook-driven runs are never refused by a
  limit or a suspension — GitHub does not redeliver, so they are recorded and
  wait at claim; only a `deleted` namespace refuses them. Namespaces with no
  limit rows behave as before.
- Check-run updates now use a durable, coalescing transactional-outbox
  projector and one leased background sender. GitHub API retries, rate-limit
  backoff, stale-id reconciliation and restart recovery happen outside
  webhook/request paths on both SQLite and PostgreSQL. Each check run carries
  an `external_id` of `{run_id}:{job_id}`, and crash-after-POST
  reconciliation adopts only a check with that id — never another workflow's
  or app's same-named check. The control schema moves to **SQLite v4 /
  Postgres v5** in place: an existing control database at the old version is
  refused and must be recreated. The Postgres backend must **connect
  directly, not through a transaction pooler** (e.g. PgBouncer in transaction
  mode): it relies on per-connection `search_path` and on a dedicated
  `LISTEN` connection, which transaction pooling breaks.

### Changed

- **Job messages now carry only the secrets the job references.**
  `build_job_artifacts` used to stamp every name `SecretProvider::resolve()`
  returned for the job's scope into `spec.names`, so each job's `secrets`
  context (and `toJSON(secrets)`) contained the whole stored set. The spec
  now carries the referenced subset, collected statically from the expanded
  plan (`preloop_gha_parser::collect_job_secret_reads`); reads that cannot
  be enumerated — `secrets[expr]` indexing, `*` object filters, a bare
  `secrets` argument, an expression that fails to parse — keep the full
  scope. Secret masking still covers the entire scope server-side.

### Fixed

- A job's `/tmp` is now backed by the VM's ext4 data disk. Runner
  provisioning removed the guest's small tmpfs `/tmp`, which left it on the
  overlayfs root, where `name_to_handle_at` is unsupported: fanotify
  file-ID watchers failed with "operation not supported" (126 of
  TypeScript's `internal/fswatch` tests). GitHub-hosted runners keep `/tmp`
  on ext4.

- With a static PAT and no GitHub App, a job that sat in the queue for more
  than five minutes after the last submit was dispatched with the local
  runtime token instead of the PAT, so its checkout failed with "could not
  read Username for 'https://github.com'". The PAT's verified scopes were only
  refreshed by submits; acquire now re-verifies them when the cache entry has
  expired.

- A job attempt whose runner session is gone (closed, purged, or silent past
  the runner liveness timeout) now fails 10 minutes after its last lease
  renewal instead of 45. A machine that just died still fails at the
  3-minute hung-worker window. The lease the runner is told about
  (`LockedUntil`) is unchanged. The reaper now evaluates lease expiry for
  every in-flight attempt on each tick, and completed-run memory trimming no
  longer runs on the event path. Before, a reaper-driven completion could
  wedge the only reaper task there and leave attempts `in_progress`
  indefinitely after a node kill.

- With several nodes on one Postgres, a client attached to one node now sees
  events produced through another node as they happen. Before, `preloop run`
  on node B did not see a run finish through node A until its stream timed out
  (5 minutes) and it reconnected. Each node reads the transactional outbox
  when another node signals (one batched `NOTIFY` per ~15 ms, sent outside the
  command transactions) and at least once a second, skips its own rows, and
  drops a state older than one it already delivered. Status events carry the
  version of the job or run they report: `jobs.version` and `runs.version`
  count status changes and are bumped by a trigger under the row lock the
  change already holds. `runs.event_seq` (a counter on the run row that every
  event took a lock to bump, and that nothing read) and `outbox_events.run_seq`
  are removed. A status event appended after its row settled on a different
  final state is not published. The outbox is pruned after
  `PRELOOP_OUTBOX_RETENTION_SECONDS` (default 3600). **Schema versions are now
  `4` (Postgres) and `3` (SQLite); an existing control database is refused and
  must be recreated.**

- Postgres read paths that scanned whole tables on every command now use an
  index: the latest-attempt lookup and the `jobs` -> `job_requests` cascade
  (`job_requests_attempts`), the claim and ready-queue reads (`jobs_ready` no
  longer carries `namespace_id` between the pool key and the priority, which
  forced a sort of the whole ready queue), and the reaper's per-attempt
  `timeout-minutes` read (a stored generated column, `job_messages.job_timeout_s`,
  instead of extracting it from the toasted message template on every tick).

- `PRELOOP_CREDENTIAL_STORE` selects where the engine's own credentials — the
  system token and the GitHub App/PAT — are kept: `os` (the native
  keychain/secret-service, default), `file` (`0600` files under
  `$PRELOOP_HOME/credentials`, for headless hosts and containers where keychain
  prompts are unacceptable), or `memory` (non-persistent; tests only).

- **`preloop init`** — one command for first-run setup, replacing the split
  between `preloop setup` and choosing a golden by hand. In a terminal it is a
  four-step wizard (credentials, golden, run mode, preflight); with stdin or
  stdout not a TTY it takes the same answers as flags, never prompts, and exits
  `2` on a missing flag, `3` on a failed preflight (disk, architecture,
  hypervisor), `4` on a failure to resolve, pull, or build the base image.
  `preloop init --probe --json` reports host capabilities (arch, free space on
  the SmolVM data volume, hypervisor, docker, smolvm, existing config,
  credential state, GHCR reachability) without side effects. The golden choices
  are the packed official GitHub runner image (the default: drop-in parity,
  ~60 GB on disk), an OCI reference verified anonymously before it is written,
  a Dockerfile built here with `docker build`/`docker save` into a local tar
  (`*.tar` is what smolvm's `--image` branch accepts), and a local
  `.smolmachine` pack or rootfs directory. The credential step *is*
  `preloop setup github` and is verified live the way `preloop doctor` does.
  The choice is recorded in the existing config file as `[golden] base_image`,
  which `serve` and `server install` read, with `PRELOOP_RUNNER_BASE_IMAGE`
  still taking precedence; `serve` with no golden configured offers the wizard
  on a TTY and otherwise prints one hint and proceeds with the official image.
- Golden disk preflight. A golden download is refused before the transfer
  starts when the artifact cannot fit on its volume, and a golden build is
  refused when the SmolVM data volume has less than the builder disk + 20 GiB
  of pack staging free (the rule the golden workflows already enforce).
  Unpacking a packed golden warns when the volume cannot hold one golden at
  its storage ceiling. `PRELOOP_SKIP_DISK_PREFLIGHT=1` proceeds with a
  warning instead.
- Runtime VM-state reconciliation. While `serve` runs, the pool sweeps
  orphaned machine data directories and purges orphaned `_boot-vm`
  hypervisors every 10 minutes instead of only at startup, so a `machine
  delete` that fails mid-run no longer leaks its disk until the next engine
  restart (long-lived engines grew 99 GB → 168 GB of VM state in an
  afternoon). The mid-flight purge spares any hypervisor whose boot config is
  still on disk, so a registered machine, a golden fork base, and a create in
  flight are never touched; `remove_stale_machines` stays startup-only.
- Job-VM disk reserve. Before a warm or on-demand slot forks or creates a job
  VM it measures free space on the SmolVM data volume and waits (logging
  `waiting for disk: … free on …, reserve …`) while it is below
  `PRELOOP_RUNNER_MIN_FREE_DISK_GB` (default 20 GiB, `0` disables) instead of
  filling the host or failing the job. The wait is not a provisioning
  failure, so a full host does not trip the repeated-provision-failure alert;
  an unmeasurable volume warns once and proceeds.
- **`$/` self-repository actions resolve** (#346): job preparation read the
  workflow identity from `system.github.*` variables (never sent) and from the
  `github` context as plain JSON, but `contextData` is typed on the wire, so
  every `$/` reference was left unstaged and failed with `action reference
  must contain @ref` (pytest's `setup-tox`, curl's `pkg-install`). It now reads
  `job.workflow_repository`/`job.workflow_sha` like runner v2.336.0, so a
  reusable workflow resolves against the callee. Local-workspace runs fetch the
  tested tree from the run's snapshot (including uncommitted edits), without
  requiring `actions/checkout`; the broker refreshes its snapshot credential
  at claim time. A composite restores its own `action_repository`/`action_ref`
  after nested actions complete.
- **`job.workflow_sha` is the workspace HEAD for local runs** (#346): it kept
  the pre-snapshot fallback (all zeros for a payload-less run) while
  `github.sha` moved to the HEAD.
- **A step's own `env:` is visible to its `with:` expressions** (#346):
  `tool: mdbook@${{ env.MDBOOK_VERSION }}` evaluated against the job env only.
- **Expression tokens in `container:`/`services:` are evaluated** (#346):
  `container: ${{ matrix.build.container }}` decoded to no container, and the
  job ran on the VM instead; evaluation errors now fail setup, and logs report
  presence without serializing credentials or environment.

### Changed

- Control-plane state is database-authoritative. Runs, jobs, runners, sessions,
  webhook deliveries and logs are mutated through one `ControlBackend` trait,
  one short transaction per command, instead of an in-memory working set that
  was snapshotted to the database. SQLite remains the default (single writer,
  WAL); Postgres implements the same contract so **several engine nodes can
  share one database** — jobs are claimed with `FOR UPDATE SKIP LOCKED` and
  nodes wake each other through `LISTEN`/`NOTIFY`. The old `Store` snapshot
  layer is gone. Cache and artifact reservations remain node-local, so a
  multi-node deployment must route a job's cache/artifact requests back to the
  node that reserved them (or run one node per job).
- The control schema is greenfield v1, created on first use and versioned in
  `schema_meta`. There is no upgrade path from an older control database:
  SQLite refuses a pre-`ControlBackend` `preloop.db` and offers only
  "recreate the database", while Postgres creates a fresh `control` schema and
  leaves an older `public` layout untouched and unread. Export anything you need
  and start from a fresh database.
- The debug controller API moved to a single `/api/v1/debug/sessions/…` surface
  from the older controller-only namespace, and the standalone verdict POST
  folded into the same lease-gated, idempotent `/operations` surface as retry
  and abort. Agents and scripts written against the old namespace must be
  repointed; the bundled CLI already is.
- Secret values are never written to the control database: a stored job message
  carries secret names, and the values are resolved when a runner acquires the
  job. The builtin secret provider is node-local (its tiers come from this
  node's config, its run tiers from `<state_dir>/run-secrets/`), so a multi-node
  deployment needs a shared secret provider.
- Job messages and AzDO responses now match `actions/runner` v2.337.0: remote
  action references always emit `repositoryType`, `actionsEnvironment.url` is an
  explicit `null` when the workflow defines no deployment URL, and `plan.env`
  is emitted as `environmentVariables` template maps.

### Fixed

- Pre-baked golden downloads now check free space on the destination filesystem
  before writing the multi-gigabyte payload. An undersized host gets an
  actionable size error instead of downloading a partial image and falling
  through to an even larger local bake.

## [0.33.7] - 2026-09-28

### Added

- Homebrew and npm install channels: `brew install preloopdev/tap/preloop` and
  `npm install -g @preloop-dev/cli`. The release workflow builds both
  (`installers = ["shell", "homebrew", "npm"]`, tap `preloopdev/homebrew-tap`).
  Homebrew skips with a workflow warning while `HOMEBREW_TAP_DEPLOY_KEY` is
  unset, so a tag never fails on a missing credential; npm authenticates with
  OIDC trusted publishing, so it stores no credential at all. 0.33.6 was the
  first release published to both channels (by hand, to bootstrap them).
- `install.sh` and `install.sh.sha256` now ship as release assets, so the
  documented install path can be verified against a published checksum instead
  of being reachable only through a moving branch.
- Release assets additionally carry keyless build provenance
  (`gh attestation verify <asset> --repo preloopdev/preloop`), complementing the
  detached RSA signature (`<asset>.sig`) that `preloop update` already verifies
  against its pinned public key.
- A documentation-only crate under `packaging/crates-io/preloop/` reserves the
  `preloop` name on crates.io; it is released by hand (`cargo publish` from that
  directory) and no workflow touches it. The CLI itself is not published to
  crates.io: it embeds the server, and the workspace's path dependencies and
  `build.rs` version pins make it unpackable there.
- **Runner version end-of-life lookup API** (`09e1f2b6`, #335):
  `GET /api/v3/actions/runners/deprecations/<version>`, plus the
  `/orgs/<org>/` and `/repos/<owner>/<repo>/` spellings, matching the route
  GitHub announced in September 2026. Preloop runs no brownouts and gates no
  runner version out of registration, so the schedule it reports is the
  published one; the org and repo segments are accepted and ignored. System
  token only, like the neighbouring runner-registration routes.
- **Run, check, and status retention** (`c26b1f0e`, #333): a new
  `retention_days` server setting (default 90, `PRELOOP_RETENTION_DAYS`) plus a
  sweep that runs at startup and then hourly, deleting terminal runs and their
  check-run records, commit statuses, artifacts, and logs from memory and from
  the durable store so a restart cannot resurrect them. Queued, pending, and
  in-progress runs are never touched. `github.retention_days` now reports the
  effective window instead of a hard-coded 90. Documented in
  `docs/self-hosting.md`.
- **Execution-protection rules REST API** (`6d3e4071`, #332): a system-token
  API under `/api/v1/execution-protection` to read the policy, flip
  evaluate/enforce mode, and list, create, update, or delete event and actor
  deny rules. Mutations rewrite the operator config file atomically — rules
  stay file-owned and survive restarts — and the shipped default is GitHub's
  own unscoped `pull_request_target` deny in evaluate (log-only) mode.
  `DELETE /api/v1/execution-protection/rules/event-0`, or an explicit empty
  `event_rules`, removes it.
- **Standalone runner install** (`d32d7416`): `install.sh --runner` fetches
  just `preloop-runner` — no control plane, no SmolVM — and `docs/setup.md`
  grows a full standalone guide (install, registration token, configure, run)
  with the runner container image documented next to it.

### Changed

- Install guidance no longer pipes a script into a shell. README,
  `docs/setup.md`, and `docs/self-hosting.md` fetch the installer at an
  immutable release tag (read it, then run it), or unpack the release archive
  after verifying its published `sha256`; `docs/self-hosting.md` documents the
  artifact-by-artifact layout, including the Linux guest runner the engine
  discovers.
- `preloop update` and the `preloop-vm` socket-mount error point at the SmolVM
  releases instead of telling users to run `curl … | bash`.
- **Node 12, 16, and 20 are no longer action runtimes** (`66116cd8`, #331):
  GitHub removed them for JavaScript actions on 2026-09-23 and deleted the
  `ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION` opt-out, so a job using them now
  fails the step with an actionable error instead of running on an unsupported
  runtime. Unknown or empty `runs.using` values fall back to `node24`, Linux
  ARM32 fails with a clear "no Node runtime available" message, and the
  migration-era machinery (`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`, the
  `usenode24bydefault`/`warnonnode20`/`requirenode24` variables, the node20
  deprecation warning) is gone.
- **Workspace migrated to Rust edition 2024** (`78362347`): mechanical
  `cargo fix` migrations plus a `collapsible_if` clippy sweep. The workspace
  lint moves `unsafe_code` from `forbid` to `deny` — edition 2024 makes
  `std::env::set_var`/`remove_var` unsafe and the environment-mutating test
  fixtures need targeted `#[allow(unsafe_code)]` — while production code stays
  unsafe-free.

### Fixed

- **Concurrency keys are namespaced per trust tier** (`7c002776`, `bd1225f4`,
  #337): a group key now carries the run's trust tier, encoded in the key's
  repo component with a `\x1f` separator so a workflow that literally names its
  group `trusted::deploy` cannot share a slot, and restored `JobSet`
  admissions are rekeyed before reconcile/promote. A fork-controlled
  `pull_request_target` run can no longer collide with and cancel a trusted
  deployment's concurrency group, across restarts included.
- **`job.workflow_*` reports the defining workflow for reusable jobs**
  (`134f8e15`, #336): `job.workflow_ref`, `workflow_sha`,
  `workflow_repository`, and `workflow_file_path` now describe the callee
  workflow instead of the caller that triggered the run, and the runner
  surfaces `job.workflow_file_path` in the expression context at all. Caller
  placeholder nodes and top-level jobs still report the run's own workflow.
- **Internal job variables no longer reach step processes** (`47841a07`,
  #329): host steps are spawned by clearing the inherited environment instead
  of extending it, so `system.*`, `DistributedTask.*`, and lowercase
  `actions_*` bookkeeping can no longer be read by running `env` in a step.
  Uppercase `ACTIONS_*` plumbing (`ACTIONS_RUNTIME_URL`, `ACTIONS_STEP_DEBUG`,
  …) is deliberately preserved.
- **Orphaned engine state is swept at startup** (`d2b32c8b`, #322): workspace
  snapshots whose run is gone or whose discard timer died with a restart,
  `.tmp-golden-*` staging files left behind by a crash between pack and rename,
  and golden payloads stranded by an environment-fingerprint change are
  reclaimed instead of accumulating in `state/snapshots/` and `vms/`.
- **Golden downloads resume and fall back** (`f88cc076`, `1e3d50ad`, #342): a
  multi-gigabyte transfer that dies partway now resumes from a `.partial` file
  across retries and engine restarts, and an engine whose own release carries
  no golden resolves the newest release that actually has one (GitHub API,
  cached five minutes) before falling back to the `/releases/latest/` URL and
  finally baking locally. Both install paths verify first — checksum for a
  release asset, layer digest for the OCI blob — so a resumed transfer cannot
  publish a mixed image.
- **`vulnerability-alerts` is a real `GITHUB_TOKEN` scope** (`4c5dbb80`,
  #330): it joins `PERMISSION_SCOPES`, so `permissions: read-all` grants it and
  explicit declarations flow into effective permissions, the runner's
  token-permission map, and the App setup checklist. `write-all` caps it at
  read, because GitHub defines no write level for the scope.
- **Node-migration variables restored in the broker's job message**
  (`0dc451e9`, `bd7ee7bb`): GitHub's service still sends
  `actions.runner.usenode24bydefault`, `actions.runner.warnonnode20`,
  `actions.runner.requirenode24`, and `actions_runner_node20_removal_date` in
  the acquirejob response, so the server keeps emitting them — matched to the
  live v2.336.0 capture — even though the runner no longer acts on them.
  Conformance is back to 36/36.
- **Release legs off the retired `macos-13` image** (`86f95685`, #325): the
  `x86_64-apple-darwin` runner build moves to `macos-15-intel`, and the
  supply-chain policy guard diffs the merge base (`465c320b`, #323) so it no
  longer fails on shallow fetches.

### Merged pull requests

- #342 — Resume golden downloads and resolve the newest release that carries one
- #339 — Restore Rust jobs from the shared R2 content-addressed cache
- #338 — Sweep stale claims across docs
- #337 — Isolate concurrency group keys by trust tier
- #336 — Report the defining workflow in `job.workflow_*` for reusable jobs
- #335 — Runner version end-of-life lookup API
- #333 — Run, check, and status retention lifecycle
- #332 — REST management API for execution-protection rules
- #331 — Remove Node 12/16/20 runtimes for JavaScript actions
- #330 — Support the `vulnerability-alerts` `GITHUB_TOKEN` permission
- #329 — Filter internal job variables from step environments
- #327 — Consolidate duplicated test helpers and replace weak `run_job` tests
- #326 — Allow the runner image push on `workflow_dispatch`
- #325 — Move `x86_64-apple-darwin` off the retired `macos-13` image
- #323 — Diff the merge base in the supply-chain policy guard
- #322 — Sweep orphaned preloop state at startup

## [0.33.6] - 2026-09-24

### Fixed

- **Forks pass `--freeze-source` instead of an env pin** (`53a93db9`): the
  `SMOLVM_BRANCH_CONTINUE=0` pin shipped in 0.33.5 was a no-op — upstream
  PR #1376 delivered the mechanism as `machine fork --freeze-source`, not an
  environment variable. `SmolVmProvider::fork` now passes the flag on every
  fork, so goldens actually stay frozen as reusable branch bases on Linux
  and macOS.
- **Rotated the pinned release-signing key** (`f64293de`): the private key matching the
  public key pinned in 0.33.5 was unrecoverable, so
  `PRELOOP_RELEASE_SIGNING_KEY` could never be set and the release
  workflow's signing step hard-failed. A fresh RSA-3072 keypair replaces the
  pin; the private key is stored as the repo secret. Safe to rotate because
  the pin first shipped in 0.33.5 — no released updater ever verified a
  signature against the old key.
- **Windows runner build repaired** (`90f1c1ad`): two Windows-only compile
  errors — a moved `upstream_addr` in `control_bridge.rs` and the unix-only
  `cap_std::fs::Dir::symlink` in `manager.rs` — failed the
  x86_64-pc-windows-msvc leg of release-runner, which gates the runner
  container image publish.

## [0.33.5] - 2026-09-24


### Added
- **SmolVM 1.18.1 runtime floor and golden pin**: `smolvm_min_version` and
  `smolvm_golden_version` both move to v1.18.1, the first upstream release
  carrying `machine fork --freeze-source` (smol-machines/smolvm#1376) — the
  upstreamed form of the retained fork patch. `preloop update
  --ensure-runtime` now upgrades any engine below it.
- **Frozen fork bases on every platform**: `SmolVmProvider::fork` passes
  `--freeze-source`, disabling live forking on Linux/x86_64 and macOS alike.
  Live forking resumed the golden onto new CoW layers per fork, accumulating
  qcow2 backing layers until `MAX_FORK_LINEAGE_DEPTH` (32) wedged the pool;
  the frozen base serves arbitrary fanout from one retained checkpoint.

- **Guest Liveness Watchdog**: Runner execution streams (`run_until_exit`) now race a guest liveness watchdog (`true` probe every 60s with 30s timeout). After 3 consecutive failed probes, the stream is aborted with `VmError::GuestUnresponsive`, preventing wedged runner VMs from holding pool slots indefinitely.
- **Transitory Fork Retries**: The orchestrator now retries transient packed-golden fork failures (`FORK_RETRY_ATTEMPTS = 2`) before falling back to cold image instantiation, shielding pool replenishment from transient hypervisor or agent initialization jitter.
- **State Directory Exclusive Lock**: `preloop serve` acquires an exclusive `flock` on `serve.lock` at startup, failing fast if another engine is already running against the same state directory.
- **Fail-Fast for Unsatisfiable Labels**: The scheduler validates job labels against available runner pools and fails fast when a job asks for labels no runner can satisfy.
- **Workflow Execution Protections & Fork Policies**: Administrative controls to enforce security ceilings, fork PR approval gates, and environment protection rules.

### Changed

- **Concurrent VM Startup**: `SmolVmProvider::start` now runs under the shared/concurrent lifecycle lock (`RwLock::read`) rather than exclusive write lock. Starting an already-created VM builds no base image, eliminating pool-wide starvation where one cold-boot blocked all fork operations.
- **Detached GitHub Check-Run Reporting**: Check-run PATCH updates during `cancel_run`, `complete_job_inner`, and `submit_run` (`--push`) are now offloaded to detached background tasks. Local state transitions, SQLite persistence, and HTTP responses return immediately, preventing client timeouts from stranding check runs in `queued`.
- **macOS Launchd Interactive QoS**: The macOS engine plist template now specifies `ProcessType = Interactive` instead of `Background`. This prevents XNU from throttling engine and VM threads to efficiency cores and low-priority I/O queues (~68× CPU and ~50× I/O throughput improvement).
- **Expression Depth Ceiling**: `MAX_EXPRESSION_DEPTH` in `preloop-gha-expressions` was lowered from 256 to 128, preventing recursive-descent stack overflow in unoptimized debug builds on default 2 MiB thread stacks while remaining well above the official GitHub Actions AST depth limit of 50.
- **Starvation Ceiling Extended**: Extended the queued-job starvation backstop from 30 minutes to 1 hour (`MAX_QUEUED_GRACE = 3600s`).
- **Server Clippy Cleanups**: Refactored `try_enqueue_with_job_concurrency` to return a strongly-typed `JobEnqueueOutcome` enum instead of `Result<bool, ()>`, and explicitly marked the serve lock file as non-truncating (`truncate(false)`).

### Fixed

- **Action Post-Step Execution Test**: Fixed a version mismatch failure in `steps_runner_tests` by staging the host's Node binary into mock runner externals (with fallback), removing an environmental dependency on system Node 24.
- **Supply-Chain Audit Policy Diff**: Updated `supply-chain.yml` to compare base and HEAD trees directly (`git diff $base HEAD`) rather than symmetric difference (`$base...HEAD`), avoiding `fatal: no merge base` on shallow PR fetches.
- **Starvation Grace Test**: Adjusted the starved-job test assertion in `registration.rs` to age past the updated 3600s ceiling.
- **Hermetic Lifecycle Test Fixtures**: Isolated `externals_dir` per test fixture in `runner_pool_lifecycle.rs` with pre-staged mock runtimes, eliminating cross-process network download races and timeouts on fresh CI nodes.
- **Cargo.lock Integrity**: Resolved a missing `libc` dependency in `Cargo.lock` that previously caused `--locked` build failures across CI shards.
- **Reusable Workflow Expression Context**: Reusable workflow `with:` expressions now evaluate strictly in the caller context.
- **Secret Value Resolution**: Job environments now receive real secret values rather than log placeholders.
- **Runs-On List Expressions**: Dynamic `runs-on` expressions returning lists are correctly unpacked into runner labels.

### Merged pull requests

- #321 — Fix runs-on expressions reading needs outputs being evaluated too early
- #320 — Starvation sweep ignores pool provisioning time and publishes the starvation limit
- #319 — Reusable caller `with:` expressions evaluate in the caller context
- #318 — Keep dispatch inputs in jobs with a needs-deferred matrix
- #317 — Regression test for action post-step execution
- #316 — Detach check-run reporting from complete_job and submit_run
- #315 — Unpack runs-on expressions that yield a list into labels
- #314 — Job environment receives real secret values instead of the log placeholder
- #313 — Drop pack/ intermediates after golden finalize (fixes #295)
- #312 — Fail closed when no runner bundle carries node externals
- #311 — Action tarball checksum verification + #291 regression test
- #310 — Adopt packed golden across engine restarts (fixes #293)
- #309 — Accept @actions/cache v6's 64 MiB staged blocks (fixes #292)
- #308 — Codify fork PR workflow policy
- #307 — Environment protection rules
- #306 — Codified GITHUB_TOKEN permissions ceiling
- #305 — Codified workflow execution protections
- #304 — Compare withheld-PAT token by job identity, not bytes
- #303 — Align toJSON/join output formatting with the official runner
- #302 — Require HTTPS for PAT scope introspection
- #301 — On-demand LFS fetch for local snapshots + stable repo identity
- #299 — Align string comparison and case-insensitivity with the official runner
- #298 — Close remaining masking races
- #297 — Sign blob tokens as JWTs, enforce owner liveness on bearer writes
- #296 — rematerialization test matches record/head split
- #283 — Remove aarch64 golden bake jobs
- #282 — Bump guest disk template to 200G for golden pack
- #280 — Bump guest storage to 200G for golden pack
- #279 — Target dispatches and provision Jammy runners
- #278 — Free host disk for golden pack, isolate retry staging
- #277 — Tolerate indented smolvm pin and fail loudly when unresolved
- #273 — Scope caches by repo and ref, cap upload memory
- #272 — Confine hashFiles, bound runner memory, cap bridge connections, mask multiline secrets
- #271 — Encrypt secrets and tokens at rest, retroactive log masking
- #270 — Scope PAT tokens, verify updater assets, pin action refs, bind webhook signer
- #269 — Authenticate blob endpoints, validate OAuth assertions, bind result writes
- #268 — Bind live-log ingest and reads to job identity
- #267 — Lazy per-run LFS blob cache for checkout cache
- #265 — Opt-in checkout object cache (run-scoped, repository)

## [0.33.2] - 2026-09-17

### Fixed

- Release gate fix: `0.33.1` changelog headings were indented, failing the entry check. Headings restored to column zero.

## [0.33.1] - 2026-09-17

 ### Added

 - Host memory observability: the engine now samples host RAM/swap, engine
   RSS, guest VM RSS, and top processes, exposing them in status, OTLP
   metrics, and `host_memory_pressure` / `host_memory_critical` conditions.
 - Pullfrog reviews run only on explicit request (`/pullfrog review`
   comments, manual dispatch, or newly opened/updated PRs).

 ### Fixed

 - Release notes again include the auto-generated merged-PR list.


## [0.33.0] - 2026-09-17

### Added

- **AgentENV is now a first-class VM substrate, opt-in on KVM hosts.**
  `crates/preloop-vm/src/agentenv.rs` implements `VmProvider` over the `aenv`
  CLI (Firecracker + overlaybd + ublk), selected with
  `PRELOOP_VM_BACKEND=agentenv` on a host with Linux ≥ 6.8 and `/dev/kvm`;
  `PRELOOP_VM_BACKEND=smolvm|agentenv` overrides, and an unrecognized value is
  a hard error rather than a silent fallback. macOS is unchanged (SmolVM is the
  only libkrun-capable runtime there). AgentENV is faster at every VM lifecycle
  operation the pool performs (boot, fork, pause, resume); guest CPU is
  identical and guest I/O trades in both directions. Rationale, mapping, and
  operational caveats in `docs/vm-substrates.md`; harnesses in
  `benchmarks/substrates/`.
- `VmProvider::capabilities()` reports what a backend can express
  (`live_host_volumes`, `socket_mounts`, `file_packs`), defaulting to SmolVM's
  behaviour so existing providers need no change. The orchestrator branches on
  it instead of assuming: a backend whose packs are not host files no longer
  builds, downloads, or relocates an artifact — it prepares its golden directly
  from the base image and forks per job.
- The engine manages its own AgentENV egress exception: with the AgentENV
  backend selected, pool startup reconciles the surgical node deny-list
  complement and the ordered host firewall rules from `PRELOOP_RUNNER_URL`
  (verify-first, mutate only on drift, restart `aenv` only on config change).
  Narrow `sudoers` scope, `PRELOOP_AENV_MANAGE_EGRESS=0|dry-run` escape hatches,
  and `PRELOOP_AENV_ENGINE_IP` override in `docs/vm-substrates.md`.

### Changed

- `preloop shell` and `preloop debug` reach guests through the resolved backend
  (`guest_exec_command` / `guest_upload_command` / `guest_shell_command`)
  instead of spawning `smolvm` directly. On AgentENV they resolve the
  server-assigned sandbox id from the engine's registry
  (`$PRELOOP_HOME/agentenv-machines.json`) and report a clear error when it is
  missing, rather than failing obscurely.
- On the AgentENV backend `preloop serve` forces the TCP control transport
  (`control_socket = None`): AgentENV cannot forward a host Unix socket into a
  guest. `PRELOOP_RUNNER_URL` must therefore be guest-reachable.
- **Debugging is explicit on AgentENV, and unattached debug VMs suspend.**
  `preloop run --debug` is the opt-in for a live debug session (wire field
  `preloopDebugOnFailure`); with no flags an AgentENV failed job is cleaned up
  instead of parking a paid sandbox — SmolVM keeps its terminal-attached
  default for compatibility, and `--preserve-on-failure` remains the
  shell-only hold. The orchestrator suspends an unattached AgentENV debug
  sandbox after 15 s (`aenv pause`, measured 0.19 s) and resumes it when
  `preloop debug`/`preloop shell` attaches (measured 0.09 s), gated on the new
  `VmProvider::capabilities().preserves_runtime_state_on_suspend` so backends
  whose stop is a real shutdown are never parked mid-session. `preloop shell`
  exit now demotes its marker instead of deleting it, holding the VM for the
  remaining 10-minute idle window; the engine renews the AgentENV TTL
  keepalive on attach and re-suspends on detach.
  Verified live on a KVM host: real Firecracker debug sessions suspend after
  15 s unattached and resume for attach/verdict (same-VM retry passed
  end-to-end). Hardening that fell out of the live run: the guest
  provisioning wrapper now survives base images without `sudo` (missing
  `/etc/sudoers.d`, `useradd` off the exec PATH — the bare `ubuntu:24.04`
  sandbox broke configure), and `preloop debug`/`preloop shell` poll for
  envd readiness after `aenv resume` instead of racing its proxy ("410
  Gone: sandbox is not proxyable").

### Fixed

- Runner teardown now releases stale job bindings immediately, status snapshots
  expose active runs and stalled claimable queues, and GitHub Check Run updates
  report every annotation in API-sized batches.
- CI jobs target the cpane microVM pool; only release packaging and the
  aarch64 golden bake use GitHub-hosted runners.
- A restart no longer leaves the pool full of phantom capacity or fails old
  queued jobs while replacement VMs are warming. Persisted ephemeral runner
  identities are purged before the server accepts traffic, unfinished request
  correlations are released from dead runner ownership before redelivery, and
  pre-provisioned successors without polling sessions are no longer reported as idle. A run
  left `in_progress` with nothing executing it raises a
  `run_in_progress_without_execution` condition instead of vanishing from the
  operator's view.
- `preloop run` declared its change set as known even when path derivation had
  not run, so an empty list read as "nothing changed" and every `paths:` filter
  rejected the run with a 400. The flag now mirrors whether derivation actually
  produced a list.
- Every path that attaches a controller — the REPL, the inline failure prompt,
  `preloop debug --verdict`, and `preloop shell` — now holds the same attach
  marker for as long as it may act. Previously a one-shot verdict claimed no
  marker at all, so the orchestrator could suspend the VM under the controller,
  and the marker was demoted before the watcher observed it. Marker release
  after a verdict now waits for the worker's session transition, which is what
  lets the watcher resume the VM and re-acquire the pool concurrency permit;
  without it a resumed job's slot stayed released.
- `AgentEnvProvider::exec_with_secret_env` returned success for a guest command
  that exited non-zero, so a failing secret-bearing step reported as passed.
  The exit status now propagates while the secret payload is still removed.
- The substrate benchmark harnesses purged sandboxes they did not own: both
  `benchmarks/substrates/e2e-bench.sh` and `benchmarks/substrates/project-bench.sh` swept every AgentENV sandbox on the
  host instead of the ids in their own `$PRELOOP_HOME` registry. They now read
  their registry and tolerate ids that are already gone.
- `benchmarks/substrates/aenv-egress-allow-host.sh` is idempotent and sets its
  firewall exception up in an order that survives an `aenv` restart: a second
  run no longer appends a duplicate deny-list complement, and the engine-port
  `ACCEPT` rules are re-inserted at the head of `INPUT` after the service
  re-adds its blanket veth `REJECT`.
- Debug attach no longer strands a live heartbeat when the guest resume fails:
  `DebugAttach::claim` resumes before writing ACTIVE, and `preloop debug
  --export` holds the same attach guard as every other controller path.
- The AgentENV provider no longer leaks sandboxes on partial startup (the id
  is recorded before readiness probes run), orphans clones on retried forks
  (duplicate names are rejected), or lets a failed pause cancel the TTL
  keepalive out from under a live session (cancelled only on success; transient
  keepalive failures retry). Unenforceable specs (`Restricted` network,
  per-sandbox `dns`, zero storage) are rejected at create time instead of
  silently substituted.
- On-demand pool slots honor the packed-golden fallback like warm slots, and a
  loopback `PRELOOP_RUNNER_URL` with the AgentENV backend fails pool startup
  with the fix — loopback is the guest itself there, so every job would starve.

## [0.32.9] - 2026-09-15

### Fixed

- Rust and Go toolchains are baked into the same homes the runner exports to
  jobs (`/usr/local/rustup`, `/usr/local/cargo`, `/usr/local/go`). Installing
  to `$HOME` while exporting the system paths left `rustup toolchain install`
  writing to a root-owned directory it could not create, failing every Rust
  job with `could not create home directory`. Both layers also restore their
  `/usr/local/bin` shims, which step shells need because `bash --noprofile
  --norc` never sources `profile.d`.
- `preloop-cli` and `preloop-runner-server` share one workspace version, so
  `--version` and `preloop status`'s `service.version` both identify the
  deployed build instead of reporting stale, unrelated numbers.

## [0.32.7] - 2026-09-10

### Fixed

- The release golden bake works again. `SMOLVM_MAX_IMAGE_BYTES` had been
  dropped from both architecture steps by a comment-only cleanup, so every
  `build-golden` attempt aborted in `smolvm create`: the pinned runner-large
  base unpacks to roughly 17 GiB through `docker save`, over smolvm's 8 GiB
  default local-archive cap. No golden had been produced since 2026-08-10.
- The aarch64 golden builds on hosted Apple Silicon again. It had been
  repointed at a `[self-hosted, macOS, ARM64]` runner that was never
  registered, so the job was never dispatched and GitHub cancelled it at the
  24-hour ceiling on every release since 2026-08-09.
- CI Rust jobs install the pinned 1.97 toolchain and `lld` again. The
  prebaked-golden change that removed them landed while the bake was broken,
  leaving jobs to fail immediately with `cargo: command not found`. The
  toolchain step now precedes `rust-cache`, whose `rustc -vV` probe needs it.
- Legacy runner compatibility aliases now require runner-management or
  one-time provisioning credentials for registration in strict production mode,
  bind sessions and message polling to the verified runner identity, and reject
  unauthenticated reporting traffic. The JSON OAuth compatibility path now
  requires the trusted system credential instead of treating a client id as
  proof; permissive registration remains an explicit TCP-only conformance
  opt-in, and the mounted socket stays strict.

### Changed

- Both golden bake jobs carry an explicit `timeout-minutes`, so a job that is
  never dispatched fails in minutes rather than occupying a runner slot for a
  full day.

## [0.32.5] - 2026-09-02

### Added

- `preloop logs` gained working `--job`/`--step` selection and `--follow`.
  Step selection resolves through the job's step manifest rather than the
  order log blobs happen to land in, so `--step N` names the step a user can
  point at in their YAML. A job still streaming has no durable per-step
  blobs, so `--step` against a live job now answers 409 instead of guessing
  from raw console blocks; the whole-job read still serves the stream.
- Conformance coverage for `actions/runner` v2.337.0: golden captures, the
  server wire-compatibility fixes they exposed, and a second five-repo
  real-world campaign.
- A supply-chain gate in CI: `cargo vet`, `cargo audit`, `cargo deny`, Node
  externals OSV scanning with SBOM emission, and action-pin parity checks.
  Renovate now enforces a seven-day `minimumReleaseAge` and every action pin
  is resolvable, with a `zizmor` check that pin comments match their SHAs.
- Batch `ActionDownloadInfo` resolution with bearer-token codeload auth.
- `runner-watch` gained a structured gate, value normalizers, a coverage map,
  and tree-sitter-based deltas.

### Changed

- The runner warns on node20 usage ahead of deprecation, and the node
  externals pins moved to current releases.

### Fixed

- Step identity and order now come from the job request message, which is
  the only record of what the server actually dispatched. Previously the
  server inferred both from whatever the runner reported, so a job's step
  list depended on report arrival order and a re-dispatch could overwrite the
  mapping the previous attempt's `step-<id>.txt` blobs are named after. Step
  records live in a dedicated per-attempt table keyed by `agent_job_id`, are
  seeded at dispatch in workflow order, and reconcile — rather than
  rebuild — as reports arrive.

  Several consequences were fixed with it: a completion is reconciled against
  the attempt that reported it instead of the oldest matching one; one
  ordering rule (the runner's reported position, else declared position)
  replaced three inconsistent ones across the run projection, whole-job log
  concatenation and both restore queries; step rows persist per attempt from
  the paths that change them, instead of every run event resealing the whole
  run; `GET /api/v1/runs` hydrates step records instead of returning empty
  arrays; a manifest left behind by a purged deferred-matrix placeholder is
  removed with its request; and an attempt dispatched but never reported has
  its manifest rebuilt at startup from the persisted request message, so a
  restart in that window no longer loses the job's declared steps.

- `preloop debug` reported step positions a user could not find in their
  workflow, because runner-generated steps — `Set up job`, action pre/post
  hooks, container lifecycle, host hooks, `Complete job` — were counted as
  workflow steps, and `--from N` selected the wrong one. A step is now
  classified as synthetic exactly when its id is absent from the job request
  message's step list, which is decidable rather than inferred from name
  prefixes. `--from-start` past the failing step is rejected with an explicit
  message instead of advertising an unreachable step.

- Node externals are validated on install: manifest shape and archive
  checksum are verified, with refresh hooks when either is stale. The test
  fixtures now derive their expected digests from the same pinned table the
  installer verifies against, so bumping a node version cannot leave the
  fixture asserting a previous release's checksum on one architecture only.

- The orchestrator no longer runs its starvation sweep while warm-pool
  runners are still booting, which cancelled work that had not had a chance
  to start.

- Snapshot fetch honours the `Git-Protocol` v2 header.

- `cargo audit` respects an explicitly configured LOW/MODERATE database
  severity instead of overriding it.

### Security

- Action archives are extracted through `cap-std` capability handles, so a
  crafted archive cannot escape the extraction root by path traversal.
- Debugger welcome text supplied by the server is masked and sanitized
  before display.
- Authenticated-runner sinks are memory-bound, so a registered runner cannot
  drive unbounded server-side allocation.
- CVSS v3 base scores are computed to the standard, with a fail-closed
  fallback when a vector cannot be parsed.

## [0.32.0] - 2026-08-28

<!-- preloop:skip-golden -->

### Fixed

- Cancelling a job no longer leaves background processes running on the
  runner host. Cancellation signalled the process group through the child
  handle, but the wait loop reaps the shell as soon as it exits, and a reaped
  handle addresses nothing — so any descendant that outlived its shell was
  never signalled. The leader's exit was also read as "the group is gone",
  which returned success from the SIGINT stage and skipped the SIGTERM and
  SIGKILL escalation entirely. A step that backgrounds a process ignoring
  SIGINT/SIGTERM (databases, daemons, `nohup`) therefore survived
  cancellation, was reparented to init, and accumulated on the host across
  runs. The group id is now captured at spawn and the escalation runs against
  the group itself, matching `ProcessInvoker.cs`, which kills the remaining
  process tree. The same gap in the stream-drain grace path is fixed too.

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

- The GitHub App webhook-subscription check no longer warns about events
  GitHub can never report. `pull_request_target` is a workflow trigger
  synthesized from the `pull_request` webhook, not a deliverable event, and
  `check_run`/`check_suite` are auto-subscribed for Apps holding `checks:
  write` and so never appear in `GET /app`'s `events`. Both made the startup
  warning fire forever on a correctly configured App. The check now also
  reads the App's permissions from the same response and separates events
  that can be ticked today from those whose checkbox GitHub does not render
  until the gating permission is granted — the previous message sent
  operators looking for controls that were not on the page.

### Changed

- Golden download progress now reads as a percentage and a completion bar
  in megabytes — `golden download (OCI): [########------------] 40%
  (3850 MB / 9630 MB)` — instead of raw byte counts, and reports every
  256 MB rather than every 1 GiB (about one line per 25 s on a 100 Mbps
  link).

## [0.31.1] - 2026-08-23

<!-- preloop:skip-golden -->

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
- Real-world large-repo conformance campaign (moby, neovim, TypeScript,
  ruff, node): five unmodified workflows run end to end; campaign writeup
  in `.runner-watch/repos-conformance-20260812.md` and `docs/fidelity-gap.md`
  §1c.

### Fixed

- **Packed-machine ownership now comes from SmolVM**: the runtime updater
  requires SmolVM 1.8.1 or newer and installs the latest stable release when
  an older or incompatible runtime is detected. Preloop no longer rewrites
  guest rootfs ownership or re-derives setuid modes itself.
- **SmolVM non-streaming `machine exec` dropped after ~30s without
  output**: quiet provisioning commands such as toolchain installs were
  killed mid-flight. Provider execs now pass an explicit `--timeout`.
- **Job/workflow-level `env:` never reached action processes**: the server
  wrote job env only into the message `variables` map, leaving the wire
  `environmentVariables` (which the official runner materializes into step
  environments) empty — `docker buildx bake` HCL variables like moby's
  `DESTDIR` silently resolved to their defaults and targets lost their
  outputs. The job message builder now populates `environmentVariables`.
- **Queued runs wedged across a server restart**: the on-demand pool only
  forks while its shared `queue_depth` atomic is non-zero, and after a
  restart no runner exists to refresh it; recovered queues sat "pending"
  forever. `serve` re-arms the atomic from the recovered queue.
- **Concurrency-group deadlock on unclaimable jobs**: a run whose remaining
  jobs all need an external host (macos/windows) never goes terminal, so its
  run-level concurrency holder parked every later submission in the same
  group forever. Restore-time group reconciliation now releases holders
  stuck that way (the run stays queued and re-acquires if a host appears).
- **Golden missing Chromium/playwright runtime libraries**: 21 browser
  runtime libs pinned in `versions.toml` and added to the golden bake
  (fingerprint-invalidating, so the pool rebuilds).

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

[0.33.2]: https://github.com/preloopdev/preloop/compare/v0.33.1...v0.33.2
[0.33.1]: https://github.com/preloopdev/preloop/compare/v0.33.0...v0.33.1
[0.32.7]: https://github.com/preloopdev/preloop/compare/v0.32.5...v0.32.7
[0.32.5]: https://github.com/preloopdev/preloop/compare/v0.32.0...v0.32.5
[0.30.3]: https://github.com/preloopdev/preloop/compare/v0.30.2...v0.30.3
[0.29.8]: https://github.com/preloopdev/preloop/compare/v0.29.7...v0.29.8
[0.29.7]: https://github.com/preloopdev/preloop/compare/v0.29.6...v0.29.7
[0.29.6]: https://github.com/preloopdev/preloop/compare/v0.29.5...v0.29.6
[0.29.1]: https://github.com/preloopdev/preloop/compare/v0.29.0...v0.29.1
[0.29.0]: https://github.com/preloopdev/preloop/compare/v0.28.0...v0.29.0
[0.28.0]: https://github.com/preloopdev/preloop/compare/v0.27.0...v0.28.0
[0.27.0]: https://github.com/preloopdev/preloop/releases/tag/v0.27.0
[Unreleased]: https://github.com/preloopdev/preloop/compare/v0.33.7...HEAD
[0.33.7]: https://github.com/preloopdev/preloop/compare/v0.33.6...v0.33.7
[0.33.6]: https://github.com/preloopdev/preloop/compare/v0.33.5...v0.33.6
[0.33.5]: https://github.com/preloopdev/preloop/compare/v0.33.2...v0.33.5
