# Golden vs official runner image: what we have, what we don't

A side-by-side of the official GitHub-hosted Ubuntu 24.04 image
(`actions/runner-images`, image version **20260720.247.2**) against Preloop's
packed microVM golden (the "golden" in `docs/vm-images.md`). The official
manifest is the repo's `images/ubuntu/Ubuntu2404-Readme.md`; the golden side
is `crates/preloop-orchestrator/src/environment.rs`,
`crates/preloop-orchestrator/src/lib.rs`, `benchmarks/real-world/Dockerfile.aksh-runner`,
and `versions.toml`.

## The fundamental difference

The official image is a **fully provisioned workstation**: OS + runner + ~150
preinstalled tools (tool cache, browsers, databases, cloud CLIs, Android SDK).
The golden is a **minimal Ubuntu base + the runner + only the toolchains the
workspace pins** (Node / Rust / Python / Go), everything else installs at job
time. Both run the same Ubuntu 24.04 family; the golden's base is
digest-pinned (`ubuntu:24.04@sha256:4fbb8e6a…`), the official image is an
azure-kernel snapshot (24.04.4, kernel 6.17.0-1020-azure).

The official image runs the official `actions/runner`; the golden runs
`preloop-runner` (Rust, cross-built aarch64-unknown-linux-gnu), fidelity
tracked against `actions/runner` v2.336.0.

## What the golden DOES have (and the pins)

| Component | Golden | Pin location |
|---|---|---|
| Base OS | Ubuntu 24.04 by digest (+ 22.04 pinned for `ubuntu-22.04` workflows) | `versions.toml` → `ubuntu_24_04_base`, `ubuntu_22_04_base` |
| Runner | `preloop-runner` (cargo-zigbuild) | `runner_version = "2.336.0"` (fidelity oracle, not the image) |
| System packages | git, curl, ca-certificates, nodejs, npm, build-essential, pkg-config, libssl-dev, docker-ce, docker-ce-cli, containerd.io, buildx, compose plugin | `Dockerfile.aksh-runner` apt layer |
| Rust | rustup-init pinned `1.29.0`, toolchain = workspace channel (`stable` by default), rustfmt + clippy, cargo-shear | `versions.toml` → `rustup_version`, `cargo_shear_version` |
| Node | Default `22.23.1` as runner externals; workspace `.nvmrc`/`setup-node` pins resolved against nodejs.org; extra externals Node 20.19.0 / 24.3.0 | `versions.toml` → `node_version`, `node20_externals_version`, `node24_externals_version` |
| Python | Workspace `.python-version` → apt `python3.x` + pip | workspace file |
| Go | `go.mod` minimum resolved against go.dev index | workspace file |
| Container images | `container:` / `services:` images from the workspace's workflows pre-pulled into the VM | runtime scan (`environment.rs::scan_workflow_images`) |

Every install is verified post-bake (`verify_binary`); a toolchain that
silently failed fails the machine rather than the job.

## What the official image has that the golden does NOT

**Tool cache (setup-* would download per run):**
- Java 8/11/17/21/25 (Temurin), .NET SDK 8.0.x ×4 + 9.0.x ×3 + 10.0.x ×3
- Go 1.24.13 / 1.25.12 / 1.26.5 (we resolve `go.mod` minimums at bake; multi-version cache absent)
- Python 3.10–3.14 (5 versions) + PyPy 3.9/3.10/3.11 — we bake only the workspace's single `.python-version`
- Ruby 3.2–4.0 (4 versions), PHP 8.3 + Composer/PHPUnit, Haskell (GHC 9.14.1, Cabal, Stack, GHCup), Julia 1.12.6, Kotlin 2.4.10, Swift 6.3.3
- nvm / n / yarn 1.22.22 / pnpm? (via npm), Miniconda 26.5.3

**Browsers & drivers:** Chrome 150 + ChromeDriver, Chromium, Edge + WebDriver,
Firefox 152 + Geckodriver, Selenium 4.46.0, xvfb. **Absent entirely** — E2E
workflows need install steps or a browser container.

**Databases & servers:** PostgreSQL 16.14, MySQL 8.0 (root/root), sqlite3,
apache2, nginx — as disabled systemd services. The golden's equivalent is the
pre-pulled `container:`/`services:` images (e.g. `postgres:16`), which is the
GitHub-recommended path anyway — but the *service-with-credentials*
convenience is not there.

**Cloud & CLI tools:** AWS CLI + SAM + Session Manager, Azure CLI (+devops),
Google Cloud SDK, GitHub CLI, AzCopy, gh. The golden has none — `gh auth` /
cloud SDK installs happen in the job (or via actions).

**Kubernetes & containers:** kubectl 1.36.2, Helm 3.21.3, kind, minikube,
kustomize, buildah, podman, skopeo, packer, pulumi, ansible, fastlane. Docker
CLI/daemon is the only container tooling in the golden (sufficient for
DinD-style jobs, missing the rest).

**Build systems:** CMake 3.31.6, Ninja 1.13.2, Bazel 9.2.0, Bazelisk, vcpkg,
Gradle 9.6.1, Maven 3.9.16, Ant, Lerna, Parcel, CodeQL Action Bundle 2.26.1.
The golden ships only `build-essential` + pkg-config.

**System utilities (~70 apt packages):** acl, aria2, autoconf/automake,
binutils, bison, flex, gnupg2, haveged, iproute2, iputils-ping, libicu-dev,
libnss3-tools, libyaml-dev, locales, m4, make, net-tools, netcat, openssh-client,
p7zip-full/rar, parallel, patchelf, pigz, pollinate, rpm, rsync, shellcheck,
sphinxsearch, sshpass, swig, systemd-coredump, telnet, texinfo, time, tk,
tree, unzip, upx, wget, xz-utils, zip, zsync, mediainfo, mercurial, git-ftp…
The golden's apt set is the 8-package Dockerfile list above.

**Other:** PowerShell 7.6.3 + Az/Microsoft.Graph/Pester modules, Android SDK
(platforms 34–37.1, NDK 27/28/29, build-tools), Homebrew (off PATH), git-lfs.

## Version-pin parity (deliberate vs drift)

| Version | Official image (20260720.247.2) | `versions.toml` | Match |
|---|---|---|---|
| Default Node | 22.23.1 | `node_version = 22.23.1` | ✅ exact |
| Rustup | 1.29.0 | `rustup_version = 1.29.0` | ✅ exact |
| Rust toolchain | 1.97.1 | workspace-driven (`stable`); repo MSRV 1.97 | ≈ (channel-resolved at bake) |
| Node tool cache | 22.23.1, 24.18.0 | `node24_externals_version = 24.3.0` | ⚠️ **drift**: official ships 24.18.0 |
| Node 20 | not on the image anymore | `node20_externals_version = 20.19.0` | ⚠️ we carry a legacy external |
| cargo-shear | — | `cargo_shear_version = 1.12.4` | ⚠️ `Dockerfile.aksh-runner` still pulls `latest` (`releases/latest/download`), the pin is not consumed there |

## Practical impact

- **Works for**: the common CI stack — Node/Rust/Python/Go workflows get baked
  toolchains at the exact pinned versions; container/service jobs work via
  pre-pulled images; Docker-in-Docker works (docker-ce in the guest).
- **Costs a per-job install**: Java, .NET, Ruby, PHP, browsers, cloud CLIs,
  k8s tools, CMake/ninja, PowerShell, Android — `setup-java`/`setup-dotnet`/
  `setup-ruby` download at job time (which GitHub's own actions are designed
  to do, so it mostly works; browsers and Android are the painful ones).
- **The two flagged asymmetries to fix**: consume `cargo_shear_version` in
  the bake (it's pinned but ignored), and decide whether `node24_externals`
  should track the official tool cache (24.18.0) or stay a deliberate
  minimum.

## What to add first: coverage per megabyte

The goal is maximizing *workflows that run unmodified* per MB added. The
split is: things workflows run **bare** (no setup step — a missing binary
fails the job) vs things `setup-*` actions install anyway (pre-baking only
saves a download). Prioritize the bare-run set, skip the download-set.

### Tier 1 — tiny apt layer, huge bare-run coverage (~150–250 MB)

`jq`, `unzip`, `zip`, `xz-utils`, `rsync`, `locales`, `net-tools`,
`openssh-client`, `time`, `tree`, `shellcheck`, `sshpass`, `p7zip-full`,
`parallel`, `pigz`, `patchelf`, `file`, `gnupg2`, `dnsutils`,
`iputils-ping`, `sqlite3`, `git-lfs`. All of these are used bare in
real-world shell steps (`jq` and `rsync` especially); each is a few MB of
apt package. `git-lfs` is the single biggest coverage win in this tier —
`git lfs pull` in a checkout step fails hard without it (official ships
3.7.1).

### Tier 2 — Node ecosystem (~150 MB)

- **pnpm** (standalone binary, pinned `pnpm_version`): the `packageManager`
  field makes bare `pnpm install` the norm in modern JS repos; corepack
  exists on Node 22 but downloads on first use and is being removed.
- **yarn 1.22.x** (single static binary): legacy JS workflows run `yarn`
  bare constantly.
- **nvm 0.40.x** (~1 MB): workflows that `source ~/.nvm/nvm.sh` and
  `nvm use` break without it (official ships 0.40.6).

### Tier 3 — one legacy Python + one Go (~250–300 MB)

- **Python 3.10** (python-build-standalone tarball, pinned; +3.11 if budget
  allows): apt on 24.04 only has 3.12, and a large share of older repos pin
  `3.10`/`3.11` — with no deadsnakes and no network, those fail. `setup-python`
  would download them, but baking the two legacy majors covers the offline
  case at ~100 MB each.
- **Go 1.24.x** (tarball from go.dev, e.g. 1.24.13 = official's oldest):
  go.mod minimums in the wild cluster around 1.22–1.24; one baked version
  covers repos whose `go.mod` matches, and `setup-go` handles the rest.

### Tier 4 — optional, medium cost

- **GitHub CLI `gh`** (~50 MB, single static binary): bare `gh pr comment` /
  `gh release create` in automation workflows. High coverage for a
  GitHub-centric CI.
- **CMake** (~80 MB): bare `cmake` in C/C++ workflows (official 3.31.6).
- **yq** (~10 MB): bare `yq` in YAML-munging steps.

### Deliberately NOT prioritized (cost >> coverage)

- **Browsers + drivers** (~1.5 GB): huge; Playwright/Cypress install their
  own, and the rest is niche.
- **Android SDK / NDK** (multi-GB), **.NET** (multi-GB), **Java multi-JDK**
  (~200 MB each): `setup-*` actions download these; pre-baking buys nothing
  but speed.
- **Databases / web servers as services**: covered by the pre-pulled
  `container:`/`services:` images, which is the GitHub-recommended path.
- **Cloud CLIs** (AWS/Azure/GCloud, ~1 GB combined): auth-scoped, setup
  actions exist, rarely bare-run.
- **Ruby/PHP/Haskell/Julia/Kotlin/Swift, PowerShell, Miniconda**: niche,
  `setup-*` handles.

New `versions.toml` keys to add: `pnpm_version`, `yarn_version`,
`nvm_version`, `python_3_10_version`, `go_1_24_version`, `git_lfs_version`
— and fix the two existing asymmetries (`cargo_shear_version` unconsumed,
`node24_externals` drift) from the parity table above.

### Node delivery: externals mount, not bake

The "node issues" (externals drift, per-version bakes) collapse into one
design choice: **every node in the system comes from the host-side externals
mount, nothing is baked into the golden.**

The mechanism already exists: `aksh-runner configure` downloads node20/node24
for JS actions (`configure.rs`, `download_externals`), and the orchestrator
mounts the host's `externals/` dir read-only into every VM at
`<RUNNER_ROOT>/externals` (`preloop-orchestrator/src/lib.rs`, `mount_externals`),
so VMs never download node themselves. What is still baked today is the
*job-visible* node from workspace pins (`ToolchainLayer::Node` tarball
installs into `/usr/local`) — and toolchain layers are part of the golden's
fingerprint, so a node bump costs a full golden rebuild (~249 s).

Plan:

1. **Extend the externals layout** to carry job node as well:
   `externals/node20`, `externals/node24` (runner JS actions, unchanged) plus
   `externals/job-node/<version>/` (tarball layouts) for the versions the
   workspace pins. Version bumps = replacing host files + `versions.toml`,
   zero VM or golden changes.
2. **Job exposure**: at provision, the resolved workspace node version
   (existing `.nvmrc`/`setup-node` resolver in `environment.rs`) selects the
   mounted tarball and symlinks `node`/`npm`/`npx`/`corepack` onto PATH. The
   mount is read-only, which is fine — npm writes live in `$HOME/.npm`
   (writable). `lts/*` and major ranges still resolve at bake time against
   nodejs.org, but only to *pick* from the mounted set.
3. **Drop `ToolchainLayer::Node` from the golden fingerprint** — node stops
   being image content entirely. The apt `nodejs`/`npm` in
   `Dockerfile.aksh-runner` (Ubuntu 24.04's node 18) becomes a redundant
   fallback and can go.
4. **Fix the stale externals pin**: `configure.rs` still targets "official
   runner v2.335.1 externals" — bump to v2.336.0's set and decide node20
   retention deliberately (official image dropped it; old JS actions may
   still need it — that's a compat choice, not drift).

Result: the golden's node story becomes "the runner + the mount", the
`node24_externals` drift becomes a host-file bump (24.3.0 → 24.18.0), and
node version changes stop invalidating the golden.
