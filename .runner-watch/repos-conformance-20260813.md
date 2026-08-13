# Real-world repo conformance campaign 2 — 2026-08-13

Second round: five more large repositories run unmodified against the preloop
stack, targeting fidelity surfaces the first campaign never touched — service
containers, job containers, reusable workflows, composite actions with nested
actions, dynamic matrices from job outputs, and changed-file gates.

## Repos

| Repo | Workflow | Result | New surface exercised |
| --- | --- | --- | --- |
| `mastodon/mastodon` | `test-ruby.yml` | **partial** | `services:` (postgres/redis) with health checks + port mapping, local composite actions with nested `uses:`, assets cache with `restore-keys`, Ruby toolchain install |
| `prometheus/prometheus` | `ci.yml` | **partial** | `container:` job containers, dynamic matrix from `needs.*.outputs` (`fromJSON`), reusable-workflow calls, golangci |
| `home-assistant/core` | `ci.yaml` | **partial** | `dorny/paths-filter` changed-file gates over a snapshot checkout |
| `apache/kafka` | `ci.yml` → `build.yml` | **partial** | `workflow_call` with inputs + secrets inheritance, dual-branch checkout (test-catalog), Gradle/Java toolchains |
| `vercel/next.js` | `build_and_test.yml` | **not run** | workspace snapshot of the 31k-file monorepo never completes; submit blocks > 25 min |

## Bugs found and fixed (all in preloop)

1. **`docker create` output parsing picked the wrong line as the container
   ID.** Stdout and stderr are merged into one stream, and docker's platform
   warning can land before the ID (pipe scheduling). The old code took
   `result.lines.first()`; the failing job ran `docker start WARNING: ...` →
   `No such container: WARNING: ...`. Fixed in `container_ops.rs`: the ID is
   the first 64-hex-character line, scanned rather than assumed first. This
   broke every `container:` and `services:` job that produced any stderr
   (prometheus's amd64 image warning; any warning at all).
2. **Job-message expression inputs collapsed to "" across a persist → restore
   round-trip.** `extract_template_map` read only `lit` from wire tokens; a
   type-3 expression token has `expr`, not `lit`, so the deserializer fell
   back to `""`. Any server restart restored every job message with every
   expression-valued step input emptied — mastodon's cache `key:` (a
   `format(...)` template) became "", and actions/cache died with
   "Input required and not supplied: key". Fixed in `azdo/job.rs`: type-3
   tokens reconstruct as `${{ expr }}`.
3. **Composite inner remote actions were never staged.** Job-start action
   preparation only stages the message's own steps; a `uses:` nested inside a
   local composite (mastodon's `setup-ruby` → `ruby/setup-ruby@v1`, kafka's
   `setup-gradle` → `actions/setup-java@v5`) resolved to a missing `_actions`
   directory. Fixed: the composite downloads nested remote actions on demand
   (`ensure_remote_action_staged`), cached under `_actions/` like prepared
   actions.
4. **Composite inner-step failures were swallowed.** The inner-step loop
   `break`s on failure, but the composite returned `Ok(())` regardless —
   mastodon's setup-ruby reported success while Ruby was never installed, and
   the workflow died later with a confusing Ruby version mismatch. Fixed:
   the composite propagates the inner failure (GitHub semantics).
5. **Composite inner-action `with:` values ignored inner-step outputs.**
   `with: path: ${{ steps.yarn-cache-dir-path.outputs.dir }}` resolved against
   the job context without the composite's nested step results → empty →
   "Input required: path". Fixed: inner `with:` values evaluate against the
   composite context (inputs + nested step outputs).
6. **The hosted toolcache was not writable by the runner user.** The golden
   bake sets `/opt/hostedtoolcache` to uid 1001, but the fork-time ownership
   repair re-roots leaked files, so `ruby/setup-ruby` failed with
   `EACCES mkdir /opt/hostedtoolcache/Ruby`. Fixed: the runner-user
   provisioning wrapper chowns the toolcache after the repair.

## Verified surfaces

- **Service containers work end to end** (probe workflow): postgres:14-alpine
  and redis:7-alpine created, health-checked, started, and reachable from
  steps on their mapped ports (`OK: postgres accepts connections on
  127.0.0.1:5432`), then cleaned up.
- **Dynamic matrices from job outputs** (prometheus): `list_lts_releases`
  emitted `{"versions": "[\"3.13.2\",\"3.5.5\"]"}`, consumed downstream.
- **Reusable workflows** (kafka): `workflow_call` with typed inputs +
  `secrets: inherit`, including a second-branch checkout (test-catalog).
- **Composite nested actions** (mastodon/kafka): `ruby/setup-ruby`,
  `actions/setup-node`, `actions/setup-java` all staged on demand and run.
- **Rails asset pipeline** (mastodon): `bin/rails assets:precompile` passes
  after Ruby install; assets cache key renders correctly.
- **Change gates** (home-assistant): 21 of 23 jobs correctly skipped for a
  README-only change.

## Remaining findings (documented, not fixed)

- **Changed-file actions fetch the base commit without auth.** HA's
  `dorny/paths-filter` fetches `origin <before-sha>` after checkout; the
  snapshot credential is registered as a checkout-scoped includeIf config that
  the checkout post-step removes, so the fetch gets a 401
  (`could not read Username for 'http://127.0.0.1:9090'`). The base commit
  also isn't local to the snapshot checkout. Needs a job-lifetime credential
  for the snapshot origin.
- **next.js cannot submit**: the workspace snapshot of its 31k-file tree does
  not finish within 25+ min, and submission is synchronous — the CLI blocks
  until the snapshot completes. The submit path should not hold the request
  for the whole snapshot.
- **amd64 job containers still fail at exec on arm64 guests** (prometheus's
  `golang-builder:1.26-base`): the image is pulled and started, but executing
  amd64 binaries requires the (unregistered) Rosetta/binfmt path — the same
  gap documented in the first campaign.
- **Gradle OOM under pool contention** (kafka): the Gradle daemon is
  OOM-killed when four 8 GiB VMs compile concurrently on the host.
- **Mastodon's production assets tar** fails on `tmp/cache/vite/last-build*.json`
  not existing after `assets:precompile` (vite writes it in test mode only,
  or the plugin's ESM-loading warning suppresses the write). Workflow-specific.

## Timing

Poller-attached wall times (seconds, from run creation; jobs interleaved on a
4-runner pool):

| Repo | Status | Jobs | Notes |
| --- | --- | --- | --- |
| prometheus | failure | 33 | golangci ~60 s; container jobs ~60 s each (ID bug); LTS matrix from outputs |
| mastodon | failure | 13 | build ~25 min (Ruby compile + Rails assets) before the tar step |
| home-assistant | failure | 23 | 21 correctly skipped; paths-filter gate failed |
| kafka | failure | 12 | configure/load-catalog/setup-gradle pass; Gradle compile OOM |
| services probe | failure | 1 | containers up + reachable; probe lacked client tools |

## Verification

- Each fix carries a regression test (container-ID parse, expression round-
  trip, composite failure propagation, nested `with:` output resolution,
  cached staging path).
- `just test-ci` (fmt + clippy + full workspace tests) passes.
- Campaign runs on the local aarch64 engine with 4 × 8192 MiB VMs; timings
  recorded by a poller attached to each run (`/tmp/campaign2/timing/*.json`).
