# VM images &amp; version tracking

Preloop executes jobs on three substrates, one of which is a packed microVM  
image ("the golden"). This page covers what the image contains, how it is  
built, exactly which versions are tracked where  and which versions we  
match to the official GitHub runner image to avoid drift.

## Execution substrates


| Mode                 | How jobs run                                                                                                                 | Enabled by                                                        |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| **MicroVM**          | A libkrun guest (Hypervisor.framework on macOS, KVM on Linux) boots the packed golden image and runs the job inside it       | `preloop serve`; packed golden use is the default                 |
| **Fork pool**        | CoW clones (`machine fork`) of a prepared golden microVM — same guest, no boot/provision                                    | `PRELOOP_USE_FORK=true` (default when a packed golden is present) |
| **External runners** | Any runner that registers against the server: the official `actions/runner`, `preloop-runner` on another machine, containers | `preloop-runner configure` + `run`                                |


The VM image and the fork pool share the same artifact (the golden); fork
mode just skips the boot.

## Image layers and pins

Four different kinds of image appear in the execution path:


| Image                            | Purpose                                                                  | How it is selected                                                                                            |
| -------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| **GitHub runner image snapshot** | Upstream parity reference for preinstalled tool and package versions     | `github_runner_image_version` in `versions.toml`                                                              |
| **OCI base image**               | Root filesystem from which Preloop provisions a golden                   | `ubuntu_24_04_base` / `ubuntu_22_04_base` in `versions.toml`, or `--base-image` / `PRELOOP_RUNNER_BASE_IMAGE` |
| **Packed golden**                | Pre-provisioned, architecture-specific microVM artifact used by the pool | Release asset by default, or `PRELOOP_GOLDEN_URL`                                                             |
| **Workflow images**              | Job `container:` and `services:` environments                            | Workflow YAML                                                                                                 |




## What the golden contains

A stock golden is a pre-provisioned microVM image: the OCI base,
`preloop-runner`, and the curated toolchain baseline, provisioned once and packed by smolvm
into a single bootable, architecture-specific file. The pool boots it
directly, and the fork pool runs CoW clones (`machine fork`) of the same
golden — the same guest, no boot or provision.

Why pack one: provisioning (pulling the base, installing packages, baking toolchains) is the expensive part, and a golden does it exactly once. A runner then starts in one or two seconds from a local artifact instead of re-pulling and re-baking the base for every new VM. Because a golden is one checksummed  
file, it is also reproducible: the same artifact produces the same runner on any host, and a stale build is caught by the checksum.

The stock golden contains:

1. **Base OS**: the published packed golden is Ubuntu 24.04, pinned by digest;
`ubuntu-latest` currently selects that 24.04 base. `ubuntu-22.04` selects a
separate digest-pinned plain Ubuntu 22.04 OCI base and builds its environment
golden on demand; there is no published packed 22.04 golden. `ubuntu-slim` has
no separate slim image and currently falls back to the configured base.
Preloop's microVM pool runs Linux guests; the release workflow's macOS builds
use GitHub-hosted runners.
2. **The runner**: `preloop-runner` cross-built for `aarch64-unknown-linux-gnu`
(`cargo-zigbuild`), fidelity-tracked against the official `actions/runner`
(see `versions.toml`).
3. **Curated toolchains**: a fixed Rust/Go baseline is baked into **stock
Ubuntu goldens** for workflows that invoke those tools implicitly. An
official GitHub runner-image golden is used as-is; `setup-*` actions resolve
the exact language version requested by each workflow at job time.
4. **Base dependencies**: the apt set `install_base_dependencies` installs
git, curl, build-essential, python3, jq, unzip/zip, locales, …).
5. **Docker**: daemon + CLI, so `container:` / `services:` jobs work.

Because setup actions own version selection, the same golden serves every
project without baking a repository-specific language version.

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


Build the Linux runner bundle, then pack the default digest-pinned Ubuntu
base:

```sh
just build-preloop

preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/debug \
  --output dist/preloop-ubuntu-24.04-aarch64
```

- `--runner-bundle`: directory containing the Linux `preloop-runner` binary
(`just build-preloop` cross-builds it).
- `--base-image`: optional Ubuntu-derived OCI image or `.smolmachine`
artifact. It defaults to the digest-pinned Ubuntu 24.04 image compiled from
`versions.toml`.
- `--workspace`: retained as workspace context for build automation. It does
not currently change the packed artifact, install packages, or derive
toolchains from `.nvmrc`, `rust-toolchain.toml`, or similar files.
- `--output`: destination for the packed golden.
- Release publication first seeds both architecture assets from the newest
  complete release, so a newly tagged engine never points at missing goldens.
  Every GitHub release then triggers `release-golden.yml`; release notes do not
  carry an opt-in marker. The x86_64 refresh runs on GitHub-hosted
  `ubuntu-latest` (KVM-enabled) in `release-golden.yml`. Successful refreshes
  replace the seeded `preloop-ubuntu-24.04-x86_64` asset; failed refreshes
  stay visible without removing the valid seeded pair. The pool stores the
  downloaded artifact at a base-image-specific path below
  `<preloop_home>/vms/` (`preloop_home` is `~/.preloop` unless
  `PRELOOP_HOME` says otherwise).
- The pool fetches that asset from its own release first, then from the newest
  release that actually carries it (a GitHub API lookup, cached for five
  minutes, stable releases over prereleases), and finally from
  `/releases/latest/download/…`, which needs no API call and covers a lookup
  that failed. An engine whose release carries no golden — the seed step
  failed, or the tag predates the artifact — therefore still finds the
  published one instead of baking locally. `PRELOOP_GOLDEN_URL` replaces every
  candidate.
- The OCI goldens the engine downloads by default are baked out-of-band —
  aarch64 on the `macstudio` host, x86_64 on the `cpane` host — and published
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
- When the pool warms a golden, it also pre-pulls the `container:` /
`services:` images declared by the current workspace's workflows.

### Adding organization-wide software

`build-golden` does not accept an apt package list, Dockerfile, or
post-provisioning script. Put organization-wide software in an Ubuntu-derived
OCI base, then ask Preloop to provision and pack that image. Use workflow
steps or setup actions instead when software differs by repository.

Example custom base:

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
the current `ubuntu_24_04_base` value from `versions.toml` as `BASE_IMAGE`:

```sh
docker buildx build \
  --platform linux/arm64 \
  --build-arg BASE_IMAGE='<ubuntu_24_04_base from versions.toml>' \
  --tag ghcr.io/acme/preloop-base:2026-08-08 \
  --push \
  .
```

Use the immutable digest returned by the registry, not the mutable tag, when
building the golden:

```sh
CUSTOM_BASE='ghcr.io/acme/preloop-base@sha256:<digest>'
GOLDEN='acme-ubuntu-24.04-aarch64'

preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/release \
  --base-image "$CUSTOM_BASE" \
  --output "dist/$GOLDEN"

(cd dist && shasum -a 256 "$GOLDEN" > "$GOLDEN.sha256")
```

On Linux, use `sha256sum "$GOLDEN" > "$GOLDEN.sha256"` instead.

Publish both files:

```text
https://artifacts.acme.example/acme-ubuntu-24.04-aarch64
https://artifacts.acme.example/acme-ubuntu-24.04-aarch64.sha256
```

Configure both the base identity and packed artifact URL:

```sh
PRELOOP_RUNNER_BASE_IMAGE='ghcr.io/acme/preloop-base@sha256:<digest>' \
PRELOOP_GOLDEN_URL='https://artifacts.acme.example/acme-ubuntu-24.04-aarch64' \
preloop serve
```

The pool fetches the checksum from exactly
`${PRELOOP_GOLDEN_URL}.sha256`. A missing checksum is tolerated with a
warning, but publishing it is strongly recommended. Setting both variables
keeps the golden's cache identity and its packed payload tied to the same OCI
base.

To provision directly from the custom OCI base without publishing a packed
golden:

```sh
PRELOOP_RUNNER_BASE_IMAGE='ghcr.io/acme/preloop-base@sha256:<digest>' \
PRELOOP_USE_PACKED_GOLDEN=false \
preloop serve
```

Provisioning currently assumes an Ubuntu 24.04 or 22.04 userspace and uses
`apt-get`. Use a workflow `container:` image for another distribution.

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

The stock base in `versions.toml` is served from `mirror.gcr.io`, Google's
cache of the Docker Official Ubuntu image. It is not a Google-built Ubuntu
image. The release workflow resolves the pinned digest, records the selected
platform manifest and its OCI attestation descriptors, and preserves the
upstream SPDX SBOM beside the golden. The cache is useful for availability and
rate-limit avoidance; the digest and the upstream image metadata are the
provenance inputs.

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
provenance record from mutable image tags. The stock image's attached SPDX
SBOM is evidence about the upstream input; it is not treated as a Google
signature or as a substitute for the Preloop golden attestation.

### Using a snapshot of the official hosted image

Since v0.30.3 the release golden is baked directly from the official
GitHub-hosted runner image snapshot (republished as OCI by the
[preloopdev/runner-image-blobs](https://github.com/preloopdev/runner-image-blobs)
dump pipeline, then scanned, signed, and SLSA-attested before the floating tag
advances). The release assets (`preloop-ubuntu-24.04-x86_64` and its
`.provenance.json` / `.base-sbom.spdx.json` / `.bundle` sidecars) are therefore
the official runner image with the Preloop runner baked in — no extra
provisioning needed.

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

You can point Preloop at the snapshot directly, with no golden:

```sh
PRELOOP_RUNNER_BASE_IMAGE='ghcr.io/<your-org>/runner-images:ubuntu24-runner-large-latest-arm64' \
PRELOOP_USE_PACKED_GOLDEN=false \
PRELOOP_RUNNER_STORAGE_GB=80 \
preloop serve
```
Cold provisioning pulls the OCI image and bakes the runner baseline into each
new VM (`PRELOOP_RUNNER_STORAGE_GB=80` covers the ~60 GB extracted snapshot).
That works, but the official snapshots are large (about 20 GB compressed,
60 GB extracted), so every new runner pays a multi-GB pull and bake before
its first job. For the official image we recommend packing a golden once
instead:

```sh
OFFICIAL_IMAGE='ghcr.io/<your-org>/runner-images:ubuntu24-runner-large-latest-arm64'
preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/release \
  --base-image "$OFFICIAL_IMAGE" \
  --storage-gb 80 \
  --output dist/official-ubuntu-24.04-aarch64
```

The golden then serves every runner with a fast local boot (or a fork-pool
start), the pull and bake happen once per host, and the artifact can be
published and reused via `PRELOOP_GOLDEN_URL`. The tradeoffs are artifact
size (tens of GB packed) and the rebuild step: a golden is fixed at bake
time, so refresh it when the snapshot changes. Pin a digest for
reproducibility, and use the architecture matching your host (`arm64` with
an aarch64 bundle, `amd64` with x86-64).

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

 Every pinned version lives in one of two flat files — `versions.toml` for
 toolchain and runtime pins, `official-image.toml` for the official runner
 bases (bumping those means rebuilding goldens, so they trigger
 separately) — and is consumed by the build:

 | Key                                                     | What it pins                                                                     | Bump when                                                               |
 | ------------------------------------------------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `runner_version`                                        | Official `actions/runner` protocol target (currently `2.336.0`)                  | Upstream runner changes protocol surface                                |
| `smolvm_min_version`                                    | SmolVM runtime floor `preloop update --ensure-runtime` accepts and upgrades from | A future SmolVM drops a capability preloop needs (rare, human-driven)   |
| `smolvm_golden_version`                                 | SmolVM release the golden workflow builds with                                    | Upstream ships a newer stable (Renovate opens a bump PR, `smolvm-release-verify` gates it) |
| `github_runner_image_version`                           | Official `actions/runner-images` Ubuntu 24.04 snapshot used as the parity source | Refreshing the hosted-image parity bake list                            |
| `ubuntu_24_04_base`                                     | Base image by digest (`ubuntu:24.04@sha256:…`)                                   | You want a newer OS snapshot — always bump the digest, never a bare tag |
| `ubuntu_22_04_base`                                     | Second pinned base                                                               | Same                                                                    |
| `official_runner_image_base_amd64` (`official-image.toml`) | Official GitHub-hosted runner image OCI reference for the x86_64 golden | Bump the digest after re-running `runner-image-blobs` attestation and verifying the new digest |
| `official_runner_image_base_arm64` (`official-image.toml`) | Official GitHub-hosted runner image OCI reference for the aarch64 golden | Same as above |
| `node_version`                                          | System node baked into the golden                                                | A workflow needs a newer default Node                                   |
| `node20_externals_version` / `node24_externals_version` | Node runtimes baked as the runner's externals                                    | Same                                                                    |
| `rustup_version`                                        | Rustup used to install baked Rust toolchains                                     | Toolchain bootstrap changes                                             |
| `cargo_shear_version`                                   | Auxiliary cargo tooling                                                          | Same                                                                    |


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
provide registry access for the pinned Ubuntu base. Renovate auto-merges smolvm
golden bumps once the verify check and `ci.yml` pass — the merge gate is
Renovate's auto-merge, not branch protection.

`versions.toml` defines Preloop's compiled distribution defaults. It is not a
per-install user configuration file. Operators select custom OCI bases and
packed goldens with `--base-image`, `PRELOOP_RUNNER_BASE_IMAGE`, and
`PRELOOP_GOLDEN_URL`; downstream distributions can change the pins before
compiling. Editing `versions.toml` does not change an already-built or
installed `preloop` executable. Rebuild the CLI and replace the deployed
binary before expecting `preloop serve` to use a changed pin:

```sh
just build-preloop
./target/debug/preloop serve

# For a release-mode deployment:
cargo build --release -p preloop-cli
```

Install or deploy `target/release/preloop` through the same mechanism used for
the existing executable. Check `which preloop` when a shell still launches an
older installed copy.

## GitHub-hosted parity bake list

The official runner image (`actions/runner-images` ubuntu-24.04) preinstalls
~100 tools; our golden deliberately bakes only the ones workflows touch
*implicitly* — the hidden dependencies that cause drift when missing. The
versions below are taken directly from the official image's toolset
identified by `github_runner_image_version` in `versions.toml`. These are the
parity targets to bake (or pin) so CI results on Preloop match GitHub:

### Tier 1 — proven drift sources (bake these)


| Item                 | Exact version (official image)                                  | Why                                                                                                                                                                                                    |
| -------------------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Node.js (system)     | **22.23.1**                                                     | The #1 proven failure: workflows call `node`/`npm`/`npx` directly — 6/7 repos in the 2026-07-28 campaign failed on "Node 24 missing". Runner-internal node is covered by externals; system node is not |
| npm / yarn / nvm     | **npm 10.9.8, yarn 1.22.22, nvm 0.40.6**                        | Same hidden-dependency class                                                                                                                                                                           |
| Docker stack         | **client 28.0.4, server 28.0.4, buildx 0.35.0, compose 2.38.2** | Container/service jobs are a whole workflow category; apt's older docker + missing buildx/compose changes `docker buildx` / `docker compose` behavior                                                  |
| Clang family         | **clang/format/tidy 16.0.6, 17.0.6, 18.1.3**                    | There is no standard GitHub setup action; C/C++ workflows commonly invoke versioned binaries directly                                                                                                  |
| GNU compiler family  | **gcc/g++/gfortran 12.4.0, 13.3.0, 14.2.0**                     | Same implicit system-tool contract; `build-essential` supplies only the default compiler                                                                                                               |
| Runner user contract | **`runner` (uid 1001), `HOME=/home/runner`, `/run/user/1001`**  | Every `id -u` / `env_var('USER')` / `runtime_directory()` check drifts without it (implemented — see `PRELOOP_RUNNER_USER`/`PRELOOP_RUNNER_UID` in self-hosting.md §4)                                 |


### Tier 2 — behavior parity (bake when size allows)


| Item                | Exact version (official image) | Why                                                                                                           |
| ------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| git                 | **2.54.0** + **Git LFS 3.7.1** | checkout-adjacent behavior: safe.directory, submodules, protocol quirks (apt ships 2.43.x)                    |
| `ubuntu` admin user | **uid 1000**                   | Workflows/actions that `chown` to 1000 or assume the admin account (a documented GitHub container-job gotcha) |


### Setup-action boundary

GitHub maintains first-party setup actions for Node
(`actions/setup-node`), Python/PyPy (`actions/setup-python`), Go
(`actions/setup-go`), Java (`actions/setup-java`), and .NET
(`actions/setup-dotnet`). Those toolchains do not need every hosted-image
version baked for correctness; their actions install a requested version.
Official runner-image goldens therefore do not receive an extra preloop
Rust/Go version. They provide a runner-writable `/opt/hostedtoolcache` for
setup actions, plus the system tools already present in the image.

The following have ecosystem-owned setup actions: Ruby (`ruby/setup-ruby`),
Julia (`julia-actions/setup-julia`), Haskell (`haskell-actions/setup`), PHP
(`shivammathur/setup-php`), Android (`android-actions/setup-android`), Rust
(`dtolnay/rust-toolchain` or `actions-rust-lang/setup-rust-toolchain`), CMake
(`jwlawson/actions-setup-cmake`), browsers (`browser-actions/setup-*`), and
Docker (`docker/setup-docker-action`). These are not GitHub-maintained, but a
workflow can declare the version instead of depending on the hosted image.
`docker/setup-buildx-action` installs Buildx, not the Docker daemon.

There is no standard setup action for the hosted Clang and GNU compiler
matrices, so those are baked. Databases should be declared with `services:`.
Cloud authentication actions generally do not install their CLIs:
`aws-actions/configure-aws-credentials` and `azure/login` assume the AWS and
Azure CLIs are already present, while `google-github-actions/setup-gcloud`
does install gcloud.

Browsers + drivers, Android SDK, .NET SDKs, Java, Ruby/PHP/Julia/Kotlin/
Swift, cloud CLIs, and databases remain deliberately unbaked. Baking that
set would add tens of gigabytes; setup actions or service containers provide
the explicit version at job time. Rust is already baked through rustup, with
the workspace pin applied by normal Rust workflows.

**Rule of thumb**: match what workflows touch implicitly (system node, the
user contract, docker, git); leave what they must declare anyway to job-time
installs. New parity targets belong in `versions.toml` with a comment
naming the official image version they were taken from.

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
| `PRELOOP_USE_PACKED_GOLDEN`                  | Use a release or locally cached packed golden (default on; set `false` for cold OCI provisioning)                  |
| `PRELOOP_GOLDEN_URL`                         | Override the packed golden URL; its optional checksum is fetched from the same URL plus `.sha256`                  |
| `PRELOOP_USE_FORK`                           | Run the pool as host forks instead of booting microVMs (default true with a golden)                                |
| `PRELOOP_RUNNER_POOL_SIZE`                   | Pool size (warm forks / VMs)                                                                                       |
| `PRELOOP_RUNNER_CPUS`                        | vCPUs allocated to each runner VM (default 8)                                                                      |
| `PRELOOP_WORKSPACE`                          | Workspace context for daemon deployments; it does not install packages or derive toolchains for a packed golden    |
| `PRELOOP_RUNNER_BASE_IMAGE`                  | Override the base image at serve time (default: digest-pinned Ubuntu 24.04)                                        |
| `PRELOOP_RUNNER_LABELS`                      | Extra `runs-on` labels the pool's runners declare (comma-separated)                                                |
| `PRELOOP_RUNNER_USER` / `PRELOOP_RUNNER_UID` | Guest runner account (default `runner`/1001, GitHub-hosted parity); `root` restores root; empty disables switching |


## Troubleshooting

- **A job misbehaves after a golden change**: the pool caches unpacked pack
dirs per VM; deleting the per-VM pack cache forces a clean unpack.
- **Missing toolchain in the VM**: the toolchain is not in the curated bake
(or the workflow pins a version outside the baked toolcache). `setup-*`
actions download the exact version at job time — the intended path. If a
toolchain is needed implicitly (no setup action), add it to the curated
bake in `base_install_script`.
- **Wrong OS inside the VM**: `--base-image` was overridden; the default is
the digest-pinned Ubuntu 24.04.
- **A job VM pulls the OCI base instead of using a packed golden**:
`PRELOOP_USE_PACKED_GOLDEN` defaults to `true` in current builds. At startup,
an enabled packed path logs `Downloading pre-baked golden from release asset
(this may take several minutes)`. If the download is unavailable, Preloop
pulls the OCI base once in a machine named `<prefix>-builder`, provisions it,
and packs a local artifact. That one-time builder pull is expected.

  A pull from a job machine such as `preloop-runner-0-1`, with no preceding
  golden download attempt, means the running process has packed golden use
  disabled. Check for an override and for a stale executable:
  ```sh
  printenv PRELOOP_USE_PACKED_GOLDEN
  which preloop
  preloop --version
  ```

  Force the current behavior while diagnosing the installed copy:
  ```sh
  PRELOOP_USE_PACKED_GOLDEN=true ./target/debug/preloop serve
  ```

  Rebuild first with `cargo build -p preloop-cli` if that debug binary predates
  the current `versions.toml`. `PRELOOP_RUNNER_POOL_ENABLED=false` only
  disables the warm pool; it does not disable packed artifacts in current
  builds.
- **Docker Hub reports `TOOMANYREQUESTS` while pulling Ubuntu**: current stock
pins use `mirror.gcr.io`, so a log that says `Pulling ubuntu:24.04` or
fetches `index.docker.io` means the running CLI has an older compiled pin or
`PRELOOP_RUNNER_BASE_IMAGE` overrides it. Check both:
  ```sh
  which preloop
  printenv PRELOOP_RUNNER_BASE_IMAGE
  ```

  Rebuild or reinstall Preloop after changing `versions.toml`. As an immediate
  override, pass the current digest-pinned `ubuntu_24_04_base` value:
  ```sh
  PRELOOP_RUNNER_BASE_IMAGE='mirror.gcr.io/library/ubuntu:24.04@sha256:4fbb8e6a8395de5a7550b33509421a2bafbc0aab6c06ba2cef9ebffbc7092d90' \
  preloop serve
  ```

  The subsequent SmolVM log must name
  `mirror.gcr.io/library/ubuntu:24.04@sha256:...`, not a bare
  `ubuntu:24.04@sha256:...`.

### Building the official-runner-image golden on Apple Silicon

The official GitHub-hosted runner image (`ubuntu24-runner-large`, published
per-arch to `ghcr.io/preloopdev/runner-images` by the runner-image-blobs
fork) is tens of GiB and declares `USER=runner`. Building a golden from it
locally exercises several smolvm/preloop sharp edges that a stock Ubuntu
base never hits. All were fixed in smolvm's `src/cli/internal_boot.rs`,
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
  and holds new job VMs below `PRELOOP_RUNNER_MIN_FREE_DISK_GB`. If the host
  still reports "No space left on device", check what is growing with
  `du -sh <preloop home>/smolvm-home/Library/Caches/smolvm/vms/*` and verify
  free space with `df -h /System/Volumes/Data`.
