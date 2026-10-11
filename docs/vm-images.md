# VM images &amp; version tracking

Preloop executes jobs on three substrates, one of which is a packed microVM
image ("the golden"). This page covers what a golden contains, how it is built,
and which versions are tracked where.

A golden comes from exactly one of two sources. The **official packed golden**
is the published official GitHub runner image with `preloop-runner` baked in;
Preloop downloads it per architecture, digest-pinned, and verifies it before
use. A **configured image** (`PRELOOP_RUNNER_BASE_IMAGE`, or the `[golden]
base_image` that `preloop init` records) is baked as it is, plus the
GitHub-runner machinery described below. There is no third source and no local
fallback: a golden download that cannot complete fails the job that needed it.

## Execution substrates


| Mode                 | How jobs run                                                                                                                 | Enabled by                                                        |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| **MicroVM**          | A libkrun guest (Hypervisor.framework on macOS, KVM on Linux) boots the packed golden image and runs the job inside it       | `preloop serve`                                                   |
| **Fork pool**        | CoW clones (`machine fork`) of a prepared golden microVM — same guest, no boot/provision                                    | `PRELOOP_USE_FORK=true` (the default)                             |
| **External runners** | Any runner that registers against the server: the official `actions/runner`, `preloop-runner` on another machine, containers | `preloop-runner configure` + `run`                                |


The VM image and the fork pool share the same artifact (the golden); fork
mode just skips the boot.

## Image sources and pins

Three kinds of image appear in the execution path:


| Image                            | Purpose                                                                  | How it is selected                                                                                            |
| -------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| **Official golden**              | The packed official GitHub runner image (Ubuntu 24.04) with `preloop-runner` baked in, per architecture | `runs-on: ubuntu-latest` / `ubuntu-24.04`, and any pool with no image configured. `PRELOOP_GOLDEN_OCI_REF` overrides the per-arch reference; `PRELOOP_GOLDEN_URL` selects a release-asset mirror instead |
| **Configured image**             | Root filesystem a golden is baked from, used as-is                       | `PRELOOP_RUNNER_BASE_IMAGE`, or `[golden] base_image` recorded by `preloop init`; `--base-image` for `build-golden` |
| **Workflow images**              | Job `container:` and `services:` environments                            | Workflow YAML                                                                                                 |


`ubuntu-22.04` maps nowhere: the 22.04 archive baseline went with the deleted
stock bake, so the label keeps the configured image (the official golden when
nothing is configured) instead of silently selecting a 24.04 one. `ubuntu-slim`
has no separate slim image either, and resolves the same way.

## What the golden contains

A golden is a pre-provisioned microVM image, packed by smolvm into a single
bootable, architecture-specific file. The pool boots it directly, and the fork
pool runs CoW clones (`machine fork`) of the same golden — the same guest, no
boot or provision.

Why pack one: provisioning (pulling the base, applying the runner contract) is
the expensive part, and a golden does it exactly once. A runner then starts in
one or two seconds from a local artifact instead of re-pulling and re-baking
the base for every new VM. Because a golden is one checksummed file, it is also
reproducible: the same artifact produces the same runner on any host, and a
stale build is caught by the checksum.

The **official golden** contains the official GitHub-hosted runner image for
its architecture, with the Linux `preloop-runner` added. It is fetched, never
baked locally: `runs-on: ubuntu-latest` and `ubuntu-24.04` name it, and so does
a pool with no image configured.

A golden baked from a **configured image** contains that image, exactly as it
is, plus the GitHub-runner machinery — and nothing else:

1. **The runner account**: `runner` (uid 1001) with its home, `_work`, and
   passwordless sudo, created only when the image does not already have it.
2. **Ownership of the runner home**: fixed once at build time with an
   owner-only walk, `find … ! -user 1001 -exec chown -h 1001:1001 {} +`. It
   touches only inodes whose owner is wrong, so it leaves a runner-owned file's
   group alone (GitHub's `~/.docker` stays `runner:docker`), and it is never
   repeated per VM — a fork inherits the result.
3. **The tool cache**: a writable `/opt/hostedtoolcache`, with
   `RUNNER_TOOL_CACHE` and `AGENT_TOOLSDIRECTORY` in `/etc/environment`, so
   `setup-*` actions can install into it.
4. **The build record**: `/etc/preloop-bake.json`, naming the image, its digest
   when the reference pins one, the bake fingerprint, and the versions the
   guest reported at build time.
5. **A glibc check**: the one requirement a bake enforces. smolvm boots a Linux
   guest and every dynamically linked binary — the runner included — resolves
   through the guest's glibc loader; an image without one (musl-only, scratch)
   fails the bake, naming what was missing.
6. **Apt package indices**: `apt-get update` at build time (best-effort, bounded
   to five minutes, skipped on an image without apt), so a step's
   `sudo apt-get install <pkg>` resolves with no `apt-get update` first, as on
   a hosted runner. The runner-image dump ships with its lists wiped; the
   official golden gets them from this step when it is baked, and an
   already-published pack that lacks them is refreshed once when it is
   unpacked, before it is frozen — never per fork.

No packages, no toolchains, no PATH or environment overrides. A missing `bash`,
`git`, or `docker` fails the step that needs it, exactly as on a GitHub-hosted
runner. Workflows select language versions with the ecosystem's `setup-*`
actions at job time, so the same golden serves every project.

## Building a golden

Goldens are native-architecture artifacts. The runner bundle, OCI image, and
host must all use the same guest architecture.

On Apple Silicon, Preloop enables Rosetta 2 x86_64 translation for every VM
it creates (`smolvm machine update --rosetta`, applied automatically, with
the machine deleted if translation cannot be enabled), so an x86_64 golden
also runs on an arm64 Mac. The golden must still be built on a host of its
own architecture; translation only covers execution. Docker actions are not
supported yet on this path: containers created by the in-guest dockerd do not
carry the Rosetta mount, so amd64-only images fail inside Docker (a mount
injection fix is in progress).

| Host / guest | Runner target               | Suggested artifact suffix |
| ------------ | --------------------------- | ------------------------- |
| ARM64        | `aarch64-unknown-linux-gnu` | `aarch64`                 |
| x86-64       | `x86_64-unknown-linux-gnu`  | `x86_64`                  |


Build the Linux runner bundle, then bake the image you want the golden to
carry:

```sh
just build-preloop

preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/debug \
  --base-image 'ghcr.io/acme/preloop-base@sha256:<digest>' \
  --output dist/acme-aarch64
```

- `--runner-bundle`: directory containing the Linux `preloop-runner` binary
(`just build-preloop` cross-builds it).
- `--base-image`: the image to bake. There is no default: the official golden
is published packed and cannot be built locally, so with no flag the command
uses the configured image (`PRELOOP_RUNNER_BASE_IMAGE`, else `[golden]
base_image`) and refuses when nothing is configured. An OCI reference (digest
pinned for reproducibility) or a local `.smolmachine`/archive path both work.
- `--output`: destination for the packed golden.
- The bake applies the runner contract (see "What the golden contains") and
  writes `/etc/preloop-bake.json`; it installs no packages (it refreshes apt's
  indices and nothing else).
- Release publication first seeds both architecture assets from the newest
  complete release, so a newly tagged engine never points at missing goldens.
  Every GitHub release then triggers `release-golden.yml`; release notes do not
  carry an opt-in marker. The x86_64 refresh runs on GitHub-hosted
  `ubuntu-latest` (KVM-enabled) in `release-golden.yml`. Successful refreshes
  replace the seeded `preloop-ubuntu-24.04-x86_64` asset; failed refreshes
  stay visible without removing the valid seeded pair. The pool stores the
  downloaded artifact at a base-and-fingerprint-specific path below
  `<preloop_home>/vms/` (`preloop_home` is `~/.preloop` unless
  `PRELOOP_HOME` says otherwise); `preloop golden-path --home <preloop home>
  --base-image <ref>` prints the exact payload path.
- The pool fetches the official golden from the per-architecture OCI reference
  first, then from its own release asset, then from the newest release that
  actually carries it (a GitHub API lookup, cached for five minutes, stable
  releases over prereleases), and finally from `/releases/latest/download/…`,
  which needs no API call and covers a lookup that failed. `PRELOOP_GOLDEN_URL`
  replaces every release-asset candidate (and skips the OCI path). A download
  that cannot complete fails the job: there is no local bake and nothing else
  to fall back to.
- The OCI goldens the engine downloads by default are baked out-of-band —
  aarch64 on an Apple Silicon host, x86_64 on a Linux KVM host — and published
  to GHCR as `preloop-<arch>-smolvm-golden`, digest-pinned in the engine and
  overridable with `PRELOOP_GOLDEN_OCI_REF`. On a host whose guest unpacks
  layers slower than GHCR's ~5-minute signed blob URLs allow, pull the pinned
  base on the host first (`crane pull --platform linux/amd64 --format tarball
  <ref> base.tar`) and bake from that archive; `PRELOOP_REQUIRE_BASE_DIGEST`
  accepts local archives.
- Transfers resume. The in-flight download lives at `<payload>.partial` and
  each retry asks for `Range: bytes=<have>-`, so a link that drops mid-body
  costs the bytes in flight rather than the whole artifact — the packed golden
  runs to several gigabytes and the registry edge drops long bodies often
  enough that a single attempt is not a reliable transfer. The artifact is
  installed only after its checksum (release asset) or layer digest (OCI)
  matches, and a failed transfer leaves the partial file for the next attempt,
  including one after an engine restart.
- When the pool prepares a golden, it also pre-pulls the `container:` /
`services:` images declared by the current workspace's workflows.

### Adding organization-wide software

`build-golden` does not accept an apt package list, Dockerfile, or
post-provisioning script, and a configured image is not modified: put
organization-wide software in the OCI image itself and configure that image as
the golden. Use workflow steps or setup actions instead when software differs
by repository.

Example custom base, derived from the official snapshot so the image keeps
hosted-runner parity:

```dockerfile
ARG BASE_IMAGE
FROM ${BASE_IMAGE}

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      cmake \
      ninja-build \
      postgresql-client \
 && rm -rf /var/lib/apt/lists/*
```

Build and publish it for the architecture on which the golden will run. Pass
the official snapshot pin for that architecture
(`official_runner_image_base_arm64` / `official_runner_image_base_amd64` in
`official-image.toml`) as `BASE_IMAGE`:

```sh
docker buildx build \
  --platform linux/arm64 \
  --build-arg BASE_IMAGE='ghcr.io/preloopdev/runner-images:ubuntu24-arm64-runner-large-latest@sha256:…' \
  --tag ghcr.io/acme/preloop-base:2026-08-08 \
  --push \
  .
```

Then configure it by its immutable digest — not the mutable tag — and start
the engine:

```sh
PRELOOP_RUNNER_BASE_IMAGE='ghcr.io/acme/preloop-base@sha256:<digest>' \
preloop serve
```

The first job's pool environment bakes the image once — as it is, plus the
runner contract — and forks that golden per job. To bake where there is more
disk and move the artifact, run `preloop build-golden --base-image <ref>
--output <dir>` on the build host, then copy the payload to the path `preloop
golden-path --home <preloop home> --base-image <ref>` prints on the serving
host.

The image must carry a glibc dynamic loader, and it should carry what your
workflows invoke implicitly: a missing `bash`, `git`, or `docker` fails the
step that needs it. Nothing else is installed and no PATH or environment
override is applied at bake time.

### Verifying a base image's provenance

For enterprise use, `build-golden` can refuse to bake from a base image whose
provenance does not check out. The dump-style images published by the
snapshot pipeline carry Sigstore keyless signatures plus in-toto attestations
(SLSA provenance and an SPDX SBOM), all signed by the publishing workflow's
GitHub Actions OIDC identity and stored as OCI referrers in GHCR; they are
verified before the golden is built:

```sh
PRELOOP_VERIFY_BASE_IMAGE=1 \
PRELOOP_VERIFY_BASE_IMAGE_REPO=acme/runner-image-blobs \
preloop build-golden --base-image 'ghcr.io/acme/runner-images@sha256:<digest>' ...
```

`cosign` must be installed on the build host. The signature identity
is pinned to the publishing repository's `dump.yml`/`attest-local.yml`
workflows; override with `PRELOOP_BASE_IMAGE_IDENTITY_REGEXP` if the
publishing workflow differs. A mirror that signs with a long-lived key instead of
keyless OIDC can set `PRELOOP_BASE_IMAGE_PUBKEY` to the public key file;
verification then uses `cosign verify --key` rather than the identity check.

### Golden provenance

The official golden is baked from the digest-pinned official runner image in
`official-image.toml`, republished as OCI by the snapshot pipeline. The release
workflow resolves the pinned digest, records the selected platform manifest and
its OCI attestation descriptors, and preserves the upstream SPDX SBOM beside
the golden. The digest and the upstream image metadata are the provenance
inputs.

Each release golden then receives:

1. a SHA-256 checksum;
2. a Cosign keyless blob signature (`<golden>.bundle`);
3. a GitHub SLSA provenance attestation over the golden, the base evidence,
   the upstream SBOM, and a signed provenance manifest;
4. a `<golden>.provenance.json` record binding the golden hash to the exact
   base index/platform digest and release workflow.

Verify the golden's two independent signatures from an online build host:

```sh
cosign verify-blob \
  --bundle preloop-ubuntu-24.04-aarch64.bundle \
  --certificate-identity-regexp \
    '^https://github.com/preloopdev/preloop/.github/workflows/release-golden.yml@refs/(heads/main|tags/)' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  preloop-ubuntu-24.04-aarch64

gh attestation verify \
  preloop-ubuntu-24.04-aarch64 \
  --repo preloopdev/preloop
```

Release builds require the base to be digest-pinned with
`PRELOOP_REQUIRE_BASE_DIGEST=1`. This protects the golden cache and the
provenance record from mutable image tags. The snapshot's attached SPDX SBOM is
evidence about the upstream input; it is not treated as a substitute for the
Preloop golden attestation.

### Using a snapshot of the official hosted image

Since v0.30.3 the release golden is baked directly from the official
GitHub-hosted runner image snapshot (republished as OCI by the
[preloopdev/runner-image-blobs](https://github.com/preloopdev/runner-image-blobs)
dump pipeline, then scanned, signed, and SLSA-attested before the floating tag
advances). The release assets (`preloop-ubuntu-24.04-x86_64` and its
`.provenance.json` / `.base-sbom.spdx.json` / `.bundle` sidecars) are therefore
the official runner image with the Preloop runner baked in — no extra
provisioning needed. That is also what a pool with no image configured
downloads, so this section matters only when you need a snapshot or an
architecture the published golden does not carry.

GitHub's hosted runner images are not published as OCI images, but the
community [runner-image-blobs](https://github.com/ChristopherHX/runner-image-blobs)
project captures their root filesystems and republishes them as
architecture-specific registry tags. The upstream tags
(`ghcr.io/christopherhx/runner-images:ubuntu24-runner-large-latest-arm64`) can
be used directly, or publish your own copy: fork the snapshot pipeline
(Preloop maintains
[preloopdev/runner-image-blobs](https://github.com/preloopdev/runner-image-blobs))
and run its dump workflow, which publishes the same tags under your fork's
own GHCR namespace. Public GHCR packages are free, so publishing costs
nothing while the fork and its packages stay public.

You can configure the snapshot as the golden's image:

```sh
PRELOOP_RUNNER_BASE_IMAGE='ghcr.io/<your-org>/runner-images:ubuntu24-runner-large-latest-arm64' \
PRELOOP_RUNNER_STORAGE_GB=80 \
preloop serve
```

The pool pulls the OCI image and bakes it into its golden once
(`PRELOOP_RUNNER_STORAGE_GB=80` covers the ~60 GB extracted snapshot), then
forks that golden per job. The snapshots are large (about 20 GB compressed,
60 GB extracted), so the first bake on a host is expensive. To pay it once and
reuse it — or to bake on a host with more disk — bake explicitly:

```sh
OFFICIAL_IMAGE='ghcr.io/<your-org>/runner-images:ubuntu24-runner-large-latest-arm64'
preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/release \
  --base-image "$OFFICIAL_IMAGE" \
  --storage-gb 80 \
  --output dist/official-ubuntu-24.04-aarch64
```

Copy the artifact to the payload path `preloop golden-path --home <preloop
home> --base-image "$OFFICIAL_IMAGE"` prints on the serving host, and the pool
adopts it instead of baking again. The tradeoffs are artifact size (tens of GB
packed) and the rebuild step: a golden is fixed at bake time, so refresh it
when the snapshot changes. Pin a digest for reproducibility, and use the
architecture matching your host (`arm64` with an aarch64 bundle, `amd64` with
x86-64).

### Installing repository-specific software

Keep repository-specific versions in the workflow so it stays portable to
GitHub Actions:

```yaml
jobs:
  test:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 24
      - run: |
          sudo apt-get update
          sudo apt-get install -y cmake ninja-build
      - run: npm test
```

Use a job `container:` for a fully controlled userspace and `services:` for
databases. These installs and setup-action tool downloads belong to the
ephemeral job environment; they do not mutate the shared golden.

## Where the engine finds the runner bundle

The pool needs a **Linux** `preloop-runner` (the runner executes inside a
Linux microVM, so the host's own binary never qualifies). The engine searches,
in order:

1. `PRELOOP_RUNNER_BUNDLE` — a directory containing a Linux `preloop-runner`.
2. `<prefix>/lib/preloop/runner/<triple>/` — where `install.sh` and
`preloop update` place the bundle on macOS releases (the host's Linux
`triple` first, then any installed triple).
3. `target/<triple>/{debug,release}` under a development build.

On Linux hosts the installed `preloop-runner` is already a Linux binary, so no
bundle is needed. Missing on macOS, the engine logs a startup warning and jobs
fail after the queue grace window; install the bundle with `preloop update` or
set `PRELOOP_RUNNER_BUNDLE`.

## Version tracking (`versions.toml`, `official-image.toml`)

Every pinned version lives in one of two flat files — `versions.toml` for the
runner, runtime, and externals pins, `official-image.toml` for the official
runner images (bumping those means rebuilding goldens, so they trigger
separately) — and is consumed by the build:

| Key                                                     | What it pins                                                                     | Bump when                                                               |
| ------------------------------------------------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `runner_version`                                        | Official `actions/runner` protocol target (currently `2.336.0`)                  | Upstream runner changes protocol surface                                |
| `smolvm_min_version`                                    | SmolVM runtime floor `preloop update --ensure-runtime` accepts and upgrades from | A future SmolVM drops a capability preloop needs (rare, human-driven)   |
| `smolvm_golden_version`                                 | SmolVM release the golden workflow builds with                                    | Upstream ships a newer stable (Renovate opens a bump PR, `smolvm-release-verify` gates it) |
| `node20_externals_version` / `node24_externals_version` | Node runtimes the guest runner mounts for JavaScript actions (downloaded on the host, never baked into a golden) | A new runtime pin is needed |
| `runner_image_docker_version` / `runner_image_buildx_version` | Docker CLI and Buildx baked into the standalone `preloop-runner` container image (`ci/runner.Dockerfile`) | The official `actions/runner` image moves |
| `official_runner_image_base_amd64` (`official-image.toml`) | Official GitHub-hosted runner image OCI reference the x86_64 golden is baked from | Bump the digest after re-running `runner-image-blobs` attestation and verifying the new digest |
| `official_runner_image_base_arm64` (`official-image.toml`) | Official GitHub-hosted runner image OCI reference the aarch64 golden is baked from | Same as above |

`versions.toml` no longer carries OS bases, toolchain versions, or apt package
pins: a golden's contents are its image's, and an operator-selected image is
configured at runtime rather than compiled in.


The protocol target (`runner_version`) and the VM image are independent:
the image always runs *our* runner; `runner_version` is the fidelity oracle
that `runner-watch` compares against.

The SmolVM pins are independent the same way and tracked with the same
tooling: Renovate (`renovate.json`) watches the `actions/runner` and
`smol-machines/smolvm` releases via the `github-releases` datasource and
opens bump PRs against `versions.toml`. Runner bumps enter the
watch → diff → triage → conform pipeline (`docs/conformance.md`). SmolVM
golden bumps are gated by `.github/workflows/smolvm-release-verify.yml`,
which installs the candidate on a GitHub-hosted Ubuntu runner and boots a real
microVM with it (create → start → exec → delete, including a `--mount-socket`
mount) before merge — a green run also blesses the updater's automatic
latest-stable adoption of that release. `smolvm_min_version` is deliberately
not auto-bumped: it is a capability floor, not a tracked release.

The job runs on Renovate's `smolvm_golden_version` bump PRs (head branches
`renovate/**`) and manual dispatches, and targets `ubuntu-latest`. The runner
must expose usable `/dev/kvm` access for the nested microVM smoke test and
provide registry access for the pinned official runner image. Renovate
auto-merges smolvm golden bumps once the verify check and `ci.yml` pass — the
merge gate is Renovate's auto-merge, not branch protection.

`versions.toml` defines Preloop's compiled distribution defaults. It is not a
per-install user configuration file. Operators select the image a golden is
baked from with `--base-image` or `PRELOOP_RUNNER_BASE_IMAGE`, and the official
golden's download source with `PRELOOP_GOLDEN_OCI_REF` / `PRELOOP_GOLDEN_URL`;
downstream distributions can change the pins before compiling. Editing
`versions.toml` does not change an already-built or installed `preloop`
executable. Rebuild the CLI and replace the deployed binary before expecting
`preloop serve` to use a changed pin:

```sh
just build-preloop
./target/debug/preloop serve

# For a release-mode deployment:
cargo build --release -p preloop-cli
```

Install or deploy `target/release/preloop` through the same mechanism used for
the existing executable. Check `which preloop` when a shell still launches an
older installed copy.

## What a golden adds

Nothing but the GitHub-runner machinery. The official golden is the official
hosted image with `preloop-runner` baked in; a configured image is used as it
is. Neither path installs packages or toolchains, and neither sets PATH or
environment overrides, so there is no parity bake list to maintain.

A workflow that needs a language version declares it with the ecosystem's
`setup-*` action (`actions/setup-node`, `actions/setup-python`,
`actions/setup-go`, `actions/setup-java`, `actions/setup-dotnet`, Ruby's
`ruby/setup-ruby`, Rust's `dtolnay/rust-toolchain`, and so on), exactly as on
GitHub-hosted runners; the golden only guarantees the writable
`/opt/hostedtoolcache` those actions install into. Databases belong in
`services:`, and a fully controlled userspace belongs in a job `container:`.
Cloud CLIs, browsers, Android SDKs, and similar large toolchains are
deliberately not added by Preloop — use the action or service container that
owns the version.

**Rule of thumb**: the image decides what is present; the workflow declares
what it needs. A tool the image lacks (`bash`, `git`, `docker`, a compiler)
fails the step that needs it, exactly as on a GitHub-hosted runner, so put
what your workflows invoke implicitly into the image you configure.

### Process limits (raised per launch, not baked)

`actions/runner-images` `images/ubuntu/scripts/build/configure-limits.sh`
doubles the kernel's 8192 KiB process stack for hosted runners — the image
comment reads "Double stack size from default 8192KB" — and raises the file
descriptor limit:

| Limit           | Hosted value                                                    | Where the image sets it                                             |
| --------------- | --------------------------------------------------------------- | ------------------------------------------------------------------- |
| `RLIMIT_STACK`  | **16 MiB soft, `unlimited` hard** (soft 16384 KiB)               | `DefaultLimitSTACK=16M:infinity` in `/etc/systemd/system.conf` and `* soft stack 16384` in `/etc/security/limits.conf` |
| `RLIMIT_NOFILE` | **65536 soft/hard** (`DefaultLimitNOFILE=65536`)                 | `DefaultLimitNOFILE=65536` in `/etc/systemd/system.conf` and `* soft/hard nofile 65536` in `/etc/security/limits.conf` |

A hosted step inherits them from the `actions.runner.*` service, and a
container step inherits them from the `dockerd` service. The guest boots
straight into the job workload, so neither the image's systemd units nor a
PAM session ever reads those files: the guest runner wrapper
([`GUEST_STACK_ULIMIT`], [`GUEST_NOFILE_ULIMIT`]) and the container-engine
launch raise both pairs explicitly, and the values are the image's — a probe
of a real hosted runner (image `20260927.320.1`, kernel `6.17.0-1022-azure`)
reads back `Max stack size 16777216 / unlimited` and `Max open files
65536 / 65536` in a step, a `container:` job and an ad-hoc `docker run`
alike. Those privileged launch sites use the strict forms, which end the
launch when a raise they ran for fails: a workload never starts on limits it
was not meant to have. Without the stack raise the runner chain keeps the VM
init's 8192 KiB and deep-recursion tests that pass on GitHub overflow the C
stack — a `SIGSEGV`/exit 139 where Python's `RecursionError` is expected.
Without the descriptor raise a job keeps the exec channel's defaults (1024
soft / 4096 hard on AgentENV), below what suites that raise their own soft
limit ask for (valkey's test suite requests 10032), and the runner dies with
`EPERM` on `setrlimit`.

The raise is applied per launch, wherever a workload starts:

- the runner wrapper, for the runner process and every step it spawns —
  with `runner_user` set it drops privileges after the raise, and unset or
  `root` launches are wrapped in a shell that raises and then `exec`s the
  same argv, so the exec channel's identity is untouched (those launches
  raise both pairs best-effort: a hard limit can only be raised by root, so
  they keep what they inherited and say on stderr which limit stayed);
- the container engine, because a container's processes inherit the limits
  of the chain that spawns them: the preload daemon baked into the golden
  (the engine a fork inherits) starts with the raise, a per-runner engine
  start applies it, and an engine chain inherited from a golden baked before
  this existed is re-raised in place with `prlimit`, since a running process
  keeps the limits it was born with.

### Hosted runtime init (applied per machine, not baked)

The other half of what a hosted VM's own init does — the parts that are not
per-process — is applied once per machine by `guest_hosted_runtime_init_script()`
on the same post-boot exec path as the runner-ownership reconciliation,
before the runner registers. It is idempotent (a machine already at the
hosted values pays one exec round trip and no writes), escalates through the
runner account's passwordless sudo when the exec lands on the image user, and
fails provisioning rather than leaving a machine half converted.

**Kernel limits.** The hosted `ubuntu-24.04` image writes three sysctls into
`/etc/sysctl.conf` at build time (`actions/runner-images`
`images/ubuntu/scripts/build/configure-environment.sh`), and a probe job on a
hosted runner reads them back from `/proc/sys` (image `20260927.320.1` x64 /
`20260927.135.1` arm64, kernel `6.17.0-1022-azure`):

| Sysctl | Hosted value | Why it matters |
| ------ | ------------ | -------------- |
| `vm.max_map_count` | **262144** | mmap-heavy workloads: Redis/Valkey's test suites, Elasticsearch-style tooling |
| `fs.inotify.max_user_watches` | **655360** | file watchers in bundlers, test runners, `kind` |
| `fs.inotify.max_user_instances` | **1280** | same; the kernel default (128) breaks tooling that opens many watchers |

Preloop's guest boots straight into the job workload — no init runs
`/etc/sysctl.d` or `/etc/sysctl.conf` — so an image-level sysctl file would
never take effect and jobs saw raw kernel defaults (`vm.max_map_count` 65530,
inotify watches 64372, instances 128, read back from a live job VM on the
campaign golden, kernel 6.12.95 aarch64). The init applies the hosted values
per machine, and writes only the keys the guest kernel exposes *and* that
differ: a kernel missing one hosted key still gets every other key instead of
aborting the apply (a missing key makes both `sysctl -w` and a direct
`/proc/sys` write fail), exactly as the hosted image's `/etc/sysctl.conf`
lines for unknown keys are inert there. A write the kernel exposes but does
not take fails provisioning.

**Hostname.** The curated bake writes the *golden's* name into `/etc/hosts`,
but every fork boots under a new name, so the machine's own name resolved
nowhere and `sudo` printed `sudo: unable to resolve host <name>` on every
invocation — 25 such lines in a single Valkey job, where a hosted-runner log
has none. Resolving is not enough: an AgentENV guest booted with a hosts file
mapping its name to an address it did not own (`10.1.0.59 runnervm…` while
its interfaces carried `169.254.0.21`), and a check that only asks whether
the name resolves passed there while every consumer of the name got an
unreachable address. The init therefore requires the name to resolve to an
address of *this* machine — loopback or a local-interface address, which is
what a hosted runner's own entry does (GitHub's `/etc/hosts` maps `runnervm…`
to the VM's interface address) — and replaces a foreign mapping with the
bake's `127.0.0.1 <host>` convention (the stale line is removed, not shadowed:
a resolver answers a name with its first match). The check runs on `getent`,
which the golden and every Ubuntu base carry; a machine without it fails
provisioning with that reason instead of reporting an apply that never
verified anything.

**Step environment.** A hosted step's environment has no `TERM` and no
`COLORTERM`; the `dumb` a bash step sees is bash's own default for an unset
`TERM` (`bash/variables.c`: `set_if_not ("TERM", "dumb")`), which a hosted
probe prints too. The guest's exec channel inherits `TERM` from the VM's init
(`TERM=linux`), which reached every job's environment through the runner, so
the runner drops `TERM`/`COLORTERM` it did not receive from the workflow.

`fixtures/workflows/hosted-runtime-parity.yml` is the guest-level check:

```sh
preloop run -f fixtures/workflows/hosted-runtime-parity.yml
```

It asserts every value above — the limits, the sysctls, the hostname's
locality, the environment keys — in a step, in a `container:` job and in an
ad-hoc `docker run`, and runs the deep-recursion probe pydantic's own
`test_recursive_call` shape uses (a `RecursionError` inside 16384 KiB where
8192 KiB dies with `SIGSEGV`).

## Runtime knobs


| Env var                                      | Effect                                                                                                             |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `PRELOOP_GOLDEN_OCI_REF`                     | Override the per-architecture packed golden OCI reference the official golden is fetched from                      |
| `PRELOOP_GOLDEN_URL`                         | Select a release-asset mirror of the official packed golden instead. A mirror **must** be verifiable: the checksum is this URL plus `.sha256`, or `PRELOOP_GOLDEN_SHA256` names the digest; an unverified mirror is refused |
| `PRELOOP_GOLDEN_SHA256`                      | Expected SHA-256 of the packed golden, for mirrors that publish no `.sha256` sidecar                             |
| `PRELOOP_USE_FORK`                           | Run the pool as host forks instead of booting microVMs (default true)                                              |
| `PRELOOP_RUNNER_POOL_SIZE`                   | Pool size (warm forks / VMs)                                                                                       |
| `PRELOOP_RUNNER_CPUS`                        | vCPUs allocated to each runner VM (default 8)                                                                      |
| `PRELOOP_RUNNER_BASE_IMAGE`                  | Image a custom golden is baked from, used as-is plus the runner contract. `runs-on: ubuntu-latest`/`ubuntu-24.04` and an unconfigured pool use the official golden instead |
| `PRELOOP_RUNNER_LABELS`                      | Extra `runs-on` labels the pool's runners declare (comma-separated)                                                |
| `PRELOOP_RUNNER_USER` / `PRELOOP_RUNNER_UID` | Guest runner account (default `runner`/1001, GitHub-hosted parity); `root` restores root; empty disables switching |


## Troubleshooting

- **A job misbehaves after a golden change**: the pool caches unpacked pack
dirs per VM; deleting the per-VM pack cache forces a clean unpack.
- **Missing toolchain in the VM**: the image does not carry it. `setup-*`
actions download the exact version at job time — the intended path. For a tool
a workflow needs implicitly (no setup action), add it to the image you
configure (`PRELOOP_RUNNER_BASE_IMAGE` / `[golden] base_image`) and let that
golden rebuild.
- **Wrong OS inside the VM**: a configured image overrode the official golden.
Check `PRELOOP_RUNNER_BASE_IMAGE`, the `[golden] base_image` in the config
file, and `which preloop` — a stale executable keeps its old compiled default.
- **The official golden will not download**: the download is the only source.
There is no local bake and no stock-Ubuntu fallback, so the job fails and the
error names both the OCI reference it tried and the release asset. Check egress
to `ghcr.io` (or the mirror behind `PRELOOP_GOLDEN_URL`) and that
`PRELOOP_GOLDEN_OCI_REF` names a reference your registry serves. A partial
transfer is kept at `<payload>.partial` and the next attempt resumes it, so a
dropped connection costs the bytes in flight, not the artifact.
- **A job VM pulls an OCI image instead of using the prepared golden**: that is
the configured-image bake. The pool pulls the image once in a machine named
`<prefix>-builder`, applies the runner contract, and packs it; the fork base
`<prefix>-golden` then serves every job. A pull on every job means the pack is
not being adopted — check free disk (`PRELOOP_RUNNER_MIN_FREE_DISK_GB`,
`PRELOOP_SKIP_DISK_PREFLIGHT`) and that the payload path the engine logs
exists.

### Building the official-runner-image golden on Apple Silicon

The official GitHub-hosted runner image (`ubuntu24-runner-large`, published
per-arch to `ghcr.io/preloopdev/runner-images` by the runner-image-blobs
fork) is tens of GiB and declares `USER=runner`. Building a golden from it
locally exercises several smolvm/preloop sharp edges. All were fixed in
smolvm's `src/cli/internal_boot.rs`,
`src/cli/machine.rs`, `src/pack_export.rs`, `crates/smolvm-agent/src/…`, and
`crates/preloop-cli`/`preloop-vm`; the notes below describe the failure each
one produced so a regression is recognizable.

- **`krun_start_enter returned: -22 (EINVAL)` — `Building the microVM failed:
  Internal(Vm(VmSetup(VmCreate)))`**: the `smolvm` binary was re-signed with
  an ad-hoc identity that dropped the `com.apple.security.hypervisor`
  entitlement, so `hv_vm_create` returns `HV_UNSUPPORTED`. Re-sign with the
  entitlement:
  ```sh
  cat > /tmp/hv.entitlements <<'EOF'
  <?xml version="1.0" encoding="UTF-8"?>
  <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
  <plist version="1.0"><dict>
    <key>com.apple.security.hypervisor</key><true/>
  </dict></plist>
  EOF
  codesign --force --sign - --entitlements /tmp/hv.entitlements ~/.smolvm/smolvm-bin
  ```
  Sanity-check with a tiny C probe calling `hv_vm_create` (returns 0 only
  when the entitlement is present).
- **Disk stays 20 GiB while the record says 120 GiB**: smolvm's
  `open_or_create_at` never resizes an existing raw disk, and a machine
  recreated under the same name reuses the same hash dir. A large flatten
  then fills the small disk, and the guest surfaces the failure as
  `io operation failed: Resource temporarily unavailable (os error 35)`.
  `internal_boot.rs::open_boot_disk` now grows the raw disk to the requested
  size (`set_len`; the guest `resize2fs` expands the ext4 at boot).
- **Machine created with default resources despite `--mem/--storage`**:
  `machine create` treats everything after `--` as the workload, so any flag
  appended after the keep-alive workload (`-- /bin/sh -c sleep infinity`) is
  silently swallowed — the machine boots with 8192 MiB / 20 GiB and no
  network. The workload must be the *last* positional: preloop now emits
  every flag before `--`.
- **`unknown variant \`flatten_layers\`` during pack**: the pack binary and
  the guest agent's protocol disagree. The agent in
  `~/.smolvm/agent-rootfs/usr/local/bin/` must be rebuilt from the *same*
  checkout as the CLI (`cargo zigbuild --profile release-small -p smolvm-agent
  --target aarch64-unknown-linux-musl`) and copied into the rootfs.
- **`read file: guest streamed N bytes, exceeding the 4294967296 byte cap`**:
  pack export streams multi-GiB flattened layers, but the general 4 GiB
  file-transfer cap applies unless raised. preloop sets
  `SMOLVM_FILE_TRANSFER_MAX_BYTES=64GiB` on the pack command.
- **`tar archive exceeds max total size (137438953472 bytes)` unpacking a
  golden**: SmolVM caps extraction at 128 GiB of header-declared size, and a
  golden's sparse disks declare far more than they allocate. preloop passes
  `SMOLVM_PACK_MAX_EXTRACT_BYTES=549755813888` (512 GiB) to every SmolVM
  command unless the variable is already set.
- **`mkdir: cannot create directory '/var/lib/preloop-runner': Permission
  denied` during the bake**: the official image declares `USER=runner`, and
  `machine exec` runs as the image's declared user. The exec path now
  accepts `--user` (added to smolvm's `ExecCmd`) and preloop passes
  `--user root` for bake commands.
- **Packed layer contains `archive.tar`, not a rootfs (no `/bin/sh`)**:
  a `local:<hash>` machine's cache dir holds the *archive*, not the
  flattened rootfs. The pack export must flatten from the machine's own
  storage (`image-archives/packed_layers/0000_rootfs`), not the cache dir;
  `pack_export.rs` now sources the base that way for archive machines.
- **`crun create` hangs then fails with EAGAIN**: never pipe crun's stderr
  in the agent (`crun.rs` keeps `Stdio::null()`); capturing it for debugging
  makes the two-step `crun create` deadlock.
- **Disk fills from stale machine dirs**: a failed create can leak
  `~/Library/Caches/smolvm/vms/<hash>/storage.raw` sparse files that a later
  `machine delete` does not reclaim. The engine now reconciles these while
  serving: every 10 minutes it removes data dirs the SmolVM registry no longer
  knows (older than 2 minutes) and kills hypervisors whose data dir is gone,
  so a restart is no longer needed to reclaim them. It also refuses golden
  builds/downloads that cannot fit (`PRELOOP_SKIP_DISK_PREFLIGHT` overrides)
  and holds new job VMs below `PRELOOP_RUNNER_MIN_FREE_DISK_GB`. The golden
  unpack follows the same discipline: it stalls until the volume has
  `PRELOOP_GOLDEN_UNPACK_FACTOR` (default 6) times the pack's compressed size
  free (serialized so concurrent goldens cannot spend the same free bytes),
  and a failed attempt prunes its `pack/` intermediates itself — including
  the unregistered data dir a create that died mid-extraction leaves — while
  the startup sweep reclaims whatever a crashed delete left. If the host
  still reports "No space left on device", check what is growing with
  `du -sh <preloop home>/smolvm-home/Library/Caches/smolvm/vms/*` and verify
  free space with `df -h /System/Volumes/Data`.
