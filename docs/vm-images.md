# VM images & version tracking

Preloop executes jobs on three substrates, one of which is a packed microVM
image ("the golden"). This page covers what the image contains, how it is
built, exactly which versions are tracked where — and which versions we
match to the official GitHub runner image to avoid drift.

## Execution substrates

| Mode | How jobs run | Enabled by |
|---|---|---|
| **MicroVM** | A libkrun guest (Hypervisor.framework on macOS, KVM on Linux) boots the packed golden image and runs the job inside it | `preloop serve` with a golden present (`PRELOOP_USE_PACKED_GOLDEN=true`) |
| **Fork pool** | The runner runs as a host process tree — no VM, same job semantics, much faster warm start | `PRELOOP_USE_FORK=true` (default when a packed golden is present) |
| **External runners** | Any runner that registers against the server: the official `actions/runner`, `preloop-runner` on another machine, containers | `preloop-runner configure` + `run` |

The VM image and the fork pool share the same artifact (the golden); fork
mode just skips the boot.

## What the golden contains

The golden is a single self-contained file built from:

1. **Base OS**: Ubuntu 24.04, pinned by **digest** (immutable — a tag alone
   would drift). Ubuntu 22.04 is also pinned for workflows that select it.
2. **The runner**: `preloop-runner` cross-built for `aarch64-unknown-linux-gnu`
   (cargo-zigbuild), fidelity-tracked against the official `actions/runner`
   (see `versions.toml`).
3. **Curated toolchains**: a fixed toolchain set is baked into every golden —
   currently Rust stable, plus the GitHub-hosted parity toolset in
   `base_install_script` (node/python/go toolcaches, git, git-lfs, docker,
   nvm, yarn). The bake is deliberately *not* workspace-derived: per-project
   version files were fragile (a broken resolver silently stalled every
   provisioning attempt) and every project would need bespoke resolution
   code. `setup-*` actions download any version a job asks for at job time —
   the same model GitHub-hosted runners use.
4. **Base dependencies**: the apt set `install_base_dependencies` installs
   (git, curl, build-essential, python3, jq, unzip/zip, locales, …).
5. **Docker**: daemon + CLI, so `container:` / `services:` jobs work.

Because the toolchain set is fixed, the same golden serves every project;
`--workspace` only contributes the `container:` / `services:` image preload.

## Building a golden

```sh
preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/release \
  --workspace . \
  --output dist/preloop-ubuntu-24.04-aarch64
```

- `--runner-bundle`: directory containing the Linux `preloop-runner` binary
  (`just build-preloop` cross-builds it; `--base-image` overrides the OS).
- `--workspace`: the repo whose `container:` / `services:` images are
  pre-pulled into the golden (`PRELOOP_WORKSPACE` overrides it for daemon
  deployments). Toolchains are the fixed curated set, not workspace-derived.
- On releases, `release-golden.yml` builds this artifact and uploads it as
  `preloop-ubuntu-24.04-aarch64`. The pool looks for it at
  `<preloop_home>/vms/preloop-ubuntu-24.04-<arch>` (`preloop_home` is
  `~/.config/preloop` unless `PRELOOP_HOME` says otherwise).
- When the pool warms a golden, it also pre-pulls the `container:` /
  `services:` images declared by the current workspace's workflows.

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
| `ubuntu_24_04_base` | Base image by digest (`ubuntu:24.04@sha256:…`) | You want a newer OS snapshot — always bump the digest, never a bare tag |
| `ubuntu_22_04_base` | Second pinned base | Same |
| `node_version` | Node baked as the runner's externals | A workflow needs a newer default Node |
| `node20_externals` / `node24_externals` | Additional Node externals | Same |
| `rustup_version` | Rustup used to install baked Rust toolchains | Toolchain bootstrap changes |
| `cargo_shear_version` | Auxiliary cargo tooling | Same |

The protocol target (`runner_version`) and the VM image are independent:
the image always runs *our* runner; `runner_version` is the fidelity oracle
that `runner-watch` compares against.

## GitHub-hosted parity bake list

The official runner image (`actions/runner-images` ubuntu-24.04) preinstalls
~100 tools; our golden deliberately bakes only the ones workflows touch
*implicitly* — the hidden dependencies that cause drift when missing. The
versions below are taken directly from the official image's toolset
(20260720.247.2). These are the parity targets to bake (or pin) so CI
results on Preloop match GitHub:

### Tier 1 — proven drift sources (bake these)

| Item | Exact version (official image) | Why |
|---|---|---|
| Node.js (system) | **22.23.1** | The #1 proven failure: workflows call `node`/`npm`/`npx` directly — 6/7 repos in the 2026-07-28 campaign failed on "Node 24 missing". Runner-internal node is covered by externals; system node is not |
| Node toolcache | **22.23.1, 24.18.0** | `setup-node` hits the toolcache first; without it every job re-downloads and "is Node X installed?" checks drift |
| npm / yarn / nvm | **npm 10.9.8, yarn 1.22.22, nvm 0.40.6** | Same hidden-dependency class |
| Docker stack | **client 28.0.4, server 28.0.4, buildx 0.35.0, compose 2.38.2** | Container/service jobs are a whole workflow category; apt's older docker + missing buildx/compose changes `docker buildx` / `docker compose` behavior |
| Runner user contract | **`runner` (uid 1001), `HOME=/home/runner`, `/run/user/1001`** | Every `id -u` / `env_var('USER')` / `runtime_directory()` check drifts without it (implemented — see `docs/push.md`'s runner-user section) |

### Tier 2 — behavior parity (bake when size allows)

| Item | Exact version (official image) | Why |
|---|---|---|
| git | **2.54.0** + **Git LFS 3.7.1** | checkout-adjacent behavior: safe.directory, submodules, protocol quirks (apt ships 2.43.x) |
| Python toolcache | **3.10.20, 3.11.15, 3.12.13, 3.13.14, 3.14.6** (system 3.12.3 already matches via apt) | `setup-python` version checks + no per-job download |
| Go toolcache | **1.24.13, 1.25.12, 1.26.5** | Same for `setup-go` |
| `ubuntu` admin user | **uid 1000** | Workflows/actions that `chown` to 1000 or assume the admin account (a documented GitHub container-job gotcha) |

### Deliberately not baked (keeps the image small)

Browsers + drivers, Android SDK, .NET SDKs, Java, Ruby/PHP/Julia/Kotlin/
Swift, cloud CLIs (aws/az/gcloud), databases. Workflows that use these
almost always go through `setup-*` or `services:`, which install the exact
version at job time — baking them would triple the golden for marginal
parity. Rust already matches: the workspace pin (`rust-toolchain.toml`) and
the official image both ship 1.97.x; rustup 1.29.0 matches exactly.

**Rule of thumb**: match what workflows touch implicitly (system node, the
user contract, docker, git); leave what they must declare anyway to job-time
installs. New parity targets belong in `versions.toml` with a comment
naming the official image version they were taken from.

## Runtime knobs

| Env var | Effect |
|---|---|
| `PRELOOP_USE_PACKED_GOLDEN` | Use the packed golden artifact for the pool (default off; the release layout enables it) |
| `PRELOOP_USE_FORK` | Run the pool as host forks instead of booting microVMs (default true with a golden) |
| `PRELOOP_RUNNER_POOL_SIZE` | Pool size (warm forks / VMs) |
| `PRELOOP_WORKSPACE` | Workspace whose `container:` / `services:` images the golden pre-pulls (build-time); toolchains are the fixed curated set |
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
