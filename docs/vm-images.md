# VM images &amp; version tracking

Preloop executes jobs on three substrates, one of which is a packed microVM  
image ("the golden"). This page covers what the image contains, how it is  
built, exactly which versions are tracked where  and which versions we  
match to the official GitHub runner image to avoid drift.

## Execution substrates


| Mode                 | How jobs run                                                                                                                 | Enabled by                                                        |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| **MicroVM**          | A libkrun guest (Hypervisor.framework on macOS, KVM on Linux) boots the packed golden image and runs the job inside it       | `preloop serve`; packed golden use is the default                 |
| **Fork pool**        | The runner runs as a host process tree, no VM, same job semantics, much faster warm start                                    | `PRELOOP_USE_FORK=true` (default when a packed golden is present) |
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

A golden is a pre-provisioned microVM image: the OCI base, `preloop-runner`,
and the curated toolchain baseline, provisioned once and packed by smolvm
into a single bootable, architecture-specific file. The pool boots it
directly, and the fork pool runs the same artifact as host processes without
starting a VM at all.

Why pack one: provisioning (pulling the base, installing packages, baking toolchains) is the expensive part, and a golden does it exactly once. A runner then starts in one or two seconds from a local artifact instead of re-pulling and re-baking the base for every new VM. Because a golden is one checksummed  
file, it is also reproducible: the same artifact produces the same runner on any host, and a stale build is caught by the checksum.

The stock golden contains:

1. **Base OS**: Ubuntu 24.04, pinned by **digest**. Ubuntu 22.04 is also pinned for workflows that select it. We dont support macos/windows runners yet.
2. **The runner**: `preloop-runner` cross-built for `aarch64-unknown-linux-gnu`
cargo-zigbuild), fidelity-tracked against the official `actions/runner`
see `versions.toml`).
3. **Curated toolchains**: a fixed toolchain set is baked into every golden, currently Rust stable, plus the GitHub-hosted parity toolset in  
base_install_script`(node/python/go toolcaches, git, git-lfs, docker, vm, yarn).`setup-*` actions download any version a job asks for at job time, the same model GitHub-hosted runners use.
4. **Base dependencies**: the apt set `install_base_dependencies` installs
git, curl, build-essential, python3, jq, unzip/zip, locales, …).
5. **Docker**: daemon + CLI, so `container:` / `services:` jobs work.

Because the toolchain set is fixed, the same stock golden serves every project. 

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
  --runner-bundle target/aarch64-unknown-linux-gnu/release \
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
- On releases, `release-golden.yml` builds this artifact and uploads it as
`preloop-ubuntu-24.04-<arch>`. The pool looks for it at
a base-image-specific path below `<preloop_home>/vms/` (`preloop_home` is
`~/.config/preloop` unless `PRELOOP_HOME` says otherwise).
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
snapshot pipeline carry a GitHub-signed SLSA attestation (stored in GHCR) and
a Sigstore keyless signature from the publishing workflow; both are verified
before the golden is built:

```sh
PRELOOP_VERIFY_BASE_IMAGE=1 \
PRELOOP_VERIFY_BASE_IMAGE_REPO=acme/runner-image-blobs \
preloop build-golden --base-image 'ghcr.io/acme/runner-images@sha256:<digest>' ...
```

`gh` and `cosign` must be installed on the build host. The signature identity
is pinned to the publishing repository's `dump.yml` workflow on the default
branch; override with `PRELOOP_BASE_IMAGE_IDENTITY_REGEXP` if the publishing
workflow differs.

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
preloop update` place the bundle on macOS releases (the host's Linux
riple first, then any installed triple).
3. `target/<triple>/{debug,release}` under a development build.

On Linux hosts the installed `preloop-runner` is already a Linux binary, so no
bundle is needed. Missing on macOS, the engine logs a startup warning and
submitted jobs queue until a runner exists.

## Version tracking (`versions.toml`)

Every pinned version lives in one place — `versions.toml` — and is consumed
by the build:


| Key                                                     | What it pins                                                                     | Bump when                                                               |
| ------------------------------------------------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| `runner_version`                                        | Official `actions/runner` protocol target (currently `2.336.0`)                  | Upstream runner changes protocol surface                                |
| `smolvm_min_version`                                    | SmolVM runtime floor `preloop update --ensure-runtime` accepts and upgrades from | A future SmolVM drops a capability preloop needs (rare, human-driven)   |
| `smolvm_golden_version`                                 | SmolVM release the golden workflow builds with                                    | Upstream ships a newer stable (Renovate opens a bump PR, `smolvm-release-verify` gates it) |
| `github_runner_image_version`                           | Official `actions/runner-images` Ubuntu 24.04 snapshot used as the parity source | Refreshing the hosted-image parity bake list                            |
| `ubuntu_24_04_base`                                     | Base image by digest (`ubuntu:24.04@sha256:…`)                                   | You want a newer OS snapshot — always bump the digest, never a bare tag |
| `ubuntu_22_04_base`                                     | Second pinned base                                                               | Same                                                                    |
| `node_version`                                          | Node baked as the runner's externals                                             | A workflow needs a newer default Node                                   |
| `node20_externals_version` / `node24_externals_version` | Additional Node externals                                                        | Same                                                                    |
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
which installs the candidate on the `smolvm-host` and boots a real microVM
with it (create → start → exec → delete, including a `--mount-socket`
mount) before merge — a green run also blesses the updater's automatic
latest-stable adoption of that release. `smolvm_min_version` is deliberately
not auto-bumped: it is a capability floor, not a tracked release.

The verify gate is a required check on `main`, but the job is skipped (and
merges unblocked) except on Renovate's `smolvm_golden_version` bump PRs
(head branches `renovate/**`) and manual dispatches; a skipped required
check passes. The job also needs a `smolvm-host`-labeled self-hosted runner
(KVM on Linux or Hypervisor.framework on macOS) and
`SMOLVM_VERIFY_HOST_WORKSPACE` set on the repo; the host needs registry
access for the pinned Ubuntu base. Renovate auto-merges smolvm golden bumps
once the verify check and `ci.yml` pass.

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
| Runner user contract | `**runner` (uid 1001), `HOME=/home/runner`, `/run/user/1001`**  | Every `id -u` / `env_var('USER')` / `runtime_directory()` check drifts without it (implemented — see `docs/push.md`'s runner-user section)                                                             |


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
The golden therefore does not pre-populate versioned Node, Python, or Go
toolcaches. It provides a runner-writable `/opt/hostedtoolcache` for those
actions, plus system Node and Python for workflows that invoke them directly.

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

## Runtime knobs


| Env var                                      | Effect                                                                                                             |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `PRELOOP_USE_PACKED_GOLDEN`                  | Use a release or locally cached packed golden (default on; set `false` for cold OCI provisioning)                  |
| `PRELOOP_GOLDEN_URL`                         | Override the packed golden URL; its optional checksum is fetched from the same URL plus `.sha256`                  |
| `PRELOOP_USE_FORK`                           | Run the pool as host forks instead of booting microVMs (default true with a golden)                                |
| `PRELOOP_RUNNER_POOL_SIZE`                   | Pool size (warm forks / VMs)                                                                                       |
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
an enabled packed path logs `Attempting to download pre-baked golden microVM image`. If the download is unavailable, Preloop pulls the OCI base
once in a machine named `<prefix>-builder`, provisions it, and packs a local
artifact. That one-time builder pull is expected.

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

