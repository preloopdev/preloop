# VM images & version tracking

Preloop executes jobs on three substrates, one of which is a packed microVM
image ("the golden"). This page covers what the image contains, how it is
built, and exactly which versions are tracked where.

## Execution substrates

| Mode | How jobs run | Enabled by |
|---|---|---|
| **MicroVM** | A libkrun guest (Hypervisor.framework on macOS, KVM on Linux) boots the packed golden image and runs the job inside it | `preloop serve` with a golden present (`PRELOOP_USE_PACKED_GOLDEN=true`) |
| **Fork pool** | The runner runs as a host process tree — no VM, same job semantics, much faster warm start | `PRELOOP_USE_FORK=true` (default when a packed golden is present) |
| **External runners** | Any runner that registers against the server: the official `actions/runner`, `preloop-runner` on another machine, containers | `preloop-runner configure` + `run` |

The VM image and the fork pool share the same artifact (the golden); fork
mode just skips the boot.

## What the golden image contains

The golden is a single self-contained file built from:

1. **Base OS**: Ubuntu 24.04, pinned by **digest** (immutable — a tag alone
   would drift). Ubuntu 22.04 is also pinned for workflows that select it.
2. **The runner**: `preloop-runner` cross-built for `aarch64-unknown-linux-gnu`
   (cargo-zigbuild), fidelity-tracked against the official `actions/runner`
   (see `versions.toml`).
3. **Pre-baked toolchains**: at build time the workspace is scanned for
   version files — `rust-toolchain.toml`, `.nvmrc` / `.node-version`,
   `.python-version` — and for `setup-*` action pins
   (`dtolnay/rust-toolchain`, `actions-rust-lang/setup-rust-toolchain`,
   `setup-node`) in the workflow(s). Those exact versions are installed into
   the image so jobs don't download toolchains per run. Anything not baked is
   installed at job time.

Because the toolchains are baked from the *workspace's* version files, the
same golden serves different projects: build it with
`--workspace <repo>` to get that project's toolchain set.

## Building a golden

```sh
preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/release \
  --workspace . \
  --output dist/preloop-ubuntu-24.04-aarch64
```

- `--runner-bundle`: directory containing the Linux `preloop-runner` binary
  (`just build-preloop` cross-builds it; `--base-image` overrides the OS).
- `--workspace`: the repo whose toolchain files are baked
  (`PRELOOP_WORKSPACE` overrides it for daemon deployments).
- On releases, `release-golden.yml` builds this artifact and uploads it as
  `preloop-ubuntu-24.04-aarch64`. The pool looks for it at
  `<preloop_home>/vms/preloop-ubuntu-24.04-<arch>` (`preloop_home` is
  `~/.config/preloop` unless `PRELOOP_HOME` says otherwise) — drop a release
  artifact there, or let `preloop serve` build one on first use.
- When the pool warms a golden, it also pre-pulls the `container:` /
  `services:` images declared by the current workspace's workflows, so those
  jobs do not re-pull on every run.

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

## Runtime knobs

| Env var | Effect |
|---|---|
| `PRELOOP_USE_PACKED_GOLDEN` | Use the packed golden artifact for the pool (default off; the release layout enables it) |
| `PRELOOP_USE_FORK` | Run the pool as host forks instead of booting microVMs (default true with a golden) |
| `PRELOOP_RUNNER_POOL_SIZE` | Pool size (warm forks / VMs) |
| `PRELOOP_WORKSPACE` | Workspace whose toolchains the golden should carry (build-time) |
| `PRELOOP_RUNNER_BASE_IMAGE` | Override the base image at serve time (default: digest-pinned Ubuntu 24.04) |
| `PRELOOP_RUNNER_LABELS` | Extra `runs-on` labels the pool's runners declare (comma-separated) |

## Troubleshooting

- **A job misbehaves after a golden change**: the pool caches unpacked pack
  dirs per VM; deleting the per-VM pack cache forces a clean unpack.
- **Missing toolchain in the VM**: the version file wasn't present in
  `--workspace` at build time (or the workflow uses a version range that
  resolves differently at job time). Rebuild the golden with the repo as the
  workspace — or accept the per-job install.
- **Wrong OS inside the VM**: `--base-image` was overridden; the default is
  the digest-pinned Ubuntu 24.04.
