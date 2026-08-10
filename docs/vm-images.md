# VM images & version tracking

Preloop executes jobs on three substrates, one of which is a packed microVM
image ("the golden"). This page covers what the image contains, how it is
built, exactly which versions are tracked where — and which versions we
match to the official GitHub runner image to avoid drift.

## Execution substrates

| Mode | How jobs run | Enabled by |
|---|---|---|
| **MicroVM** | A libkrun guest (Hypervisor.framework on macOS, KVM on Linux) boots the packed golden image and runs the job inside it | `preloop serve`; packed golden use is the default |
| **Fork pool** | The runner runs as a host process tree so no VM, same job semantics, much faster warm start | `PRELOOP_USE_FORK=true` (default when a packed golden is present) |
| **External runners** | Any runner that registers against the server: the official `actions/runner`, `preloop-runner` on another machine, containers | `preloop-runner configure` + `run` |

The VM image and the fork pool share the same artifact (the golden); fork
mode just skips the boot.

## Image layers and pins

Four different kinds of image appear in the execution path:

| Image | Purpose | How it is selected |
|---|---|---|
| **GitHub runner image snapshot** | Upstream parity reference for preinstalled tool and package versions | `github_runner_image_version` in `versions.toml` |
| **OCI base image** | Root filesystem from which Preloop provisions a golden | `ubuntu_24_04_base` / `ubuntu_22_04_base` in `versions.toml`, or `--base-image` / `PRELOOP_RUNNER_BASE_IMAGE` |
| **Packed golden** | Pre-provisioned, architecture-specific microVM artifact used by the pool | Release asset by default, or `PRELOOP_GOLDEN_URL` |
| **Workflow images** | Job `container:` and `services:` environments | Workflow YAML |

The GitHub runner image snapshot is not downloaded or booted. It is the
versioned source of truth used when selecting the parity pins in
`versions.toml`. The OCI base is the actual guest filesystem input. Preloop
adds its runner baseline to that filesystem and packs the result as a golden.

## What the golden contains

The golden is a single self-contained file built from:

1. **Base OS**: Ubuntu 24.04, pinned by **digest**. Ubuntu 22.04 is also pinned for workflows that select it.
2. **The runner**: `preloop-runner` cross-built for `aarch64-unknown-linux-gnu`
   (cargo-zigbuild), fidelity-tracked against the official `actions/runner`
   (see `versions.toml`).
3. **Curated toolchains**: a fixed toolchain set is baked into every golden 
   currently Rust stable, plus the GitHub-hosted parity toolset in
   `base_install_script` (node/python/go toolcaches, git, git-lfs, docker,
   nvm, yarn). The bake is deliberately *not* workspace-derived: per-project
   version files were fragile (a broken resolver silently stalled every
   provisioning attempt) and every project would need bespoke resolution
   code. `setup-*` actions download any version a job asks for at job time so
   the same model GitHub-hosted runners use.
4. **Base dependencies**: the apt set `install_base_dependencies` installs
   (git, curl, build-essential, python3, jq, unzip/zip, locales, …).
5. **Docker**: daemon + CLI, so `container:` / `services:` jobs work.

Because the toolchain set is fixed, the same stock golden serves every
project.

## Building a golden

Goldens are native-architecture artifacts. The runner bundle, OCI image, and
host must all use the same guest architecture.

| Host / guest | Runner target | Suggested artifact suffix |
|---|---|---|
| ARM64 | `aarch64-unknown-linux-gnu` | `aarch64` |
| x86-64 | `x86_64-unknown-linux-gnu` | `x86_64` |

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
- `PRELOOP_RUNNER_PACK_PROXY`: optional HTTP proxy passed to smolvm's export
  VM when it re-pulls and flattens the registry image. `HTTPS_PROXY`,
  `https_proxy`, `HTTP_PROXY`, and `http_proxy` are fallback sources.
- `PRELOOP_RUNNER_PACK_NO_PROXY`: optional proxy bypass list for the export
  VM. `NO_PROXY` and `no_proxy` are fallback sources.
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

smolvm packs a registry-backed builder by starting a separate export VM and
re-pulling the base image before flattening it. If that export VM cannot reach
the registry directly, configure its proxy explicitly:

```sh
PRELOOP_RUNNER_PACK_PROXY='http://proxy.example:8080' \
PRELOOP_RUNNER_PACK_NO_PROXY='localhost,127.0.0.1,.internal' \
preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/release \
  --output dist/preloop-ubuntu-24.04-aarch64
```

Local Docker-save archives can provision a VM but cannot be exported by
smolvm's `pack create --from-vm`; use a registry reference when building a
golden.

### Adding organization-wide software

`build-golden` does not yet accept an apt package list, Dockerfile, or
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

### Using a snapshot of the official hosted image

Preloop can also start from a community-published OCI snapshot of a
GitHub-hosted runner image. The
[runner-image-blobs project](https://github.com/ChristopherHX/runner-image-blobs)
captures the root filesystem of an official hosted runner and publishes
architecture-specific registry tags. A fork can publish the same tags under
its own GHCR namespace:

```sh
OFFICIAL_IMAGE='ghcr.io/<owner>/runner-images:ubuntu24-runner-large-latest-arm64'

preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/release \
  --base-image "$OFFICIAL_IMAGE" \
  --storage-gb 80 \
  --output dist/official-ubuntu-24.04-aarch64
```

The equivalent environment-based configuration is useful for a long-running
server:

```sh
OFFICIAL_IMAGE='ghcr.io/<owner>/runner-images:ubuntu24-runner-large-latest-arm64'
PRELOOP_RUNNER_BASE_IMAGE="$OFFICIAL_IMAGE" \
PRELOOP_RUNNER_STORAGE_GB=80 \
preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/release \
  --output dist/official-ubuntu-24.04-aarch64
```

Use `arm64` with an `aarch64-unknown-linux-gnu` runner bundle and an ARM64
host. Use `amd64` with an `x86_64-unknown-linux-gnu` bundle and an x86-64
host. The snapshot is an OCI registry reference, so Preloop pulls it directly
when provisioning; no Dockerfile conversion is required.

These snapshots are large, approximately 20 GB compressed and 60 GB
extracted according to the publishing project. Set
`PRELOOP_RUNNER_STORAGE_GB` to at least `80` for the full image and leave
additional host disk headroom for the image cache, temporary layers, and the
packed golden. The value is per guest and applies to both golden builds and
runtime VMs.

The snapshot is a copy maintained by the publishing project, not a
GitHub-supported Preloop artifact. Pin a digest when reproducibility matters,
and rebuild the golden when the snapshot changes. Preloop still adds its own
runner bundle and provisioning steps, while workflow `container:` and
`services:` images remain separate job environments.

For a custom packed golden, publish it and its optional checksum, then set
both variables:

```sh
PRELOOP_RUNNER_BASE_IMAGE="$OFFICIAL_IMAGE" \
PRELOOP_GOLDEN_URL='https://artifacts.acme.example/official-ubuntu-24.04-aarch64' \
preloop serve
```

If `PRELOOP_GOLDEN_URL` is not set, a custom base builds its golden locally;
Preloop does not silently use the stock release golden for that base.

A custom base is the operator's contract and is used **as-is**: the curated
toolchain bake (section 3) applies only to the stock digest-pinned Ubuntu
bases. A custom OCI image gets the runner mounted, but no apt/toolchain
curation on top — if the image needs extra packages, put them in the image
or in workflow `setup-*` actions.

### Installing repository-specific software

Keep repository-specific versions in the workflow so it stays portable to
GitHub Actions. It's also the idiomatic way to run Actions.

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
   triple first, then any installed triple).
3. `target/<triple>/{debug,release}` under a development build.

On Linux hosts the installed `preloop-runner` is already a Linux binary, so no
bundle is needed. Missing on macOS, the engine logs a startup warning and
submitted jobs queue until a runner exists.

## Version tracking (`versions.toml`)

Every pinned version lives in one place — `versions.toml` — and is consumed
by the build:

| Key | What it pins | Bump when |
|---|---|---|
| `runner_version` | Official `actions/runner` protocol target (currently `2.336.0`) | Upstream runner changes protocol surface |
| `github_runner_image_version` | Official `actions/runner-images` Ubuntu 24.04 snapshot used as the parity source | Refreshing the hosted-image parity bake list |
| `ubuntu_24_04_base` | Base image by digest (`ubuntu:24.04@sha256:…`) | You want a newer OS snapshot — always bump the digest, never a bare tag |
| `ubuntu_22_04_base` | Second pinned base | Same |
| `node_version` | Node baked as the runner's externals | A workflow needs a newer default Node |
| `node20_externals_version` / `node24_externals_version` | Additional Node externals | Same |
| `rustup_version` | Rustup used to install baked Rust toolchains | Toolchain bootstrap changes |
| `cargo_shear_version` | Auxiliary cargo tooling | Same |

The protocol target (`runner_version`) and the VM image are independent:
the image always runs *our* runner; `runner_version` is the fidelity oracle
that `runner-watch` compares against.

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

| Item | Exact version (official image) | Why |
|---|---|---|
| Node.js (system) | **22.23.1** | The #1 proven failure: workflows call `node`/`npm`/`npx` directly — 6/7 repos in the 2026-07-28 campaign failed on "Node 24 missing". Runner-internal node is covered by externals; system node is not |
| npm / yarn / nvm | **npm 10.9.8, yarn 1.22.22, nvm 0.40.6** | Same hidden-dependency class |
| Docker stack | **client 28.0.4, server 28.0.4, buildx 0.35.0, compose 2.38.2** | Container/service jobs are a whole workflow category; apt's older docker + missing buildx/compose changes `docker buildx` / `docker compose` behavior |
| Clang family | **clang/format/tidy 16.0.6, 17.0.6, 18.1.3** | There is no standard GitHub setup action; C/C++ workflows commonly invoke versioned binaries directly |
| GNU compiler family | **gcc/g++/gfortran 12.4.0, 13.3.0, 14.2.0** | Same implicit system-tool contract; `build-essential` supplies only the default compiler |
| Runner user contract | **`runner` (uid 1001), `HOME=/home/runner`, `/run/user/1001`** | Every `id -u` / `env_var('USER')` / `runtime_directory()` check drifts without it (implemented — see `docs/push.md`'s runner-user section) |

### Tier 2 — behavior parity (bake when size allows)

| Item | Exact version (official image) | Why |
|---|---|---|
| git | **2.54.0** + **Git LFS 3.7.1** | checkout-adjacent behavior: safe.directory, submodules, protocol quirks (apt ships 2.43.x) |
| `ubuntu` admin user | **uid 1000** | Workflows/actions that `chown` to 1000 or assume the admin account (a documented GitHub container-job gotcha) |

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

| Env var | Effect |
|---|---|
| `PRELOOP_USE_PACKED_GOLDEN` | Use a release or locally cached packed golden (default on; set `false` for cold OCI provisioning) |
| `PRELOOP_GOLDEN_URL` | Override the packed golden URL; its optional checksum is fetched from the same URL plus `.sha256` |
| `PRELOOP_USE_FORK` | Run the pool as host forks instead of booting microVMs (default true with a golden) |
| `PRELOOP_RUNNER_POOL_SIZE` | Pool size (warm forks / VMs) |
| `PRELOOP_WORKSPACE` | Workspace context for daemon deployments; it does not install packages or derive toolchains for a packed golden |
| `PRELOOP_RUNNER_BASE_IMAGE` | Override the base image at serve time (default: digest-pinned Ubuntu 24.04) |
| `PRELOOP_RUNNER_LABELS` | Extra `runs-on` labels the pool's runners declare (comma-separated) |
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
  an enabled packed path logs `Attempting to download pre-baked golden
  microVM image`. If the download is unavailable, Preloop pulls the OCI base
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
