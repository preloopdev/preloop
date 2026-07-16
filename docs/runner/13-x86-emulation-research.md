# x86 Emulation on Apple Silicon — Research Notes

## Problem

aksh runs workflows locally in smolvm VMs on Apple Silicon (ARM64). Most GitHub Actions workflows assume `ubuntu-latest` (x86_64). When a workflow installs x86-only packages or downloads amd64 binaries, it fails or runs slowly on ARM64.

## macOS Virtualization Stack

```
┌──────────────────────────────────────────────────┐
│  Apple Hypervisor.framework                      │
│  (≈ Linux KVM)                                   │
│  Low-level: runs ARM instructions on real HW     │
└───────────┬──────────────────────┬───────────────┘
            │                      │
    ┌───────▼──────┐       ┌───────▼──────────────┐
    │   libkrun     │       │ Virtualization       │
    │   (≈ QEMU)    │       │  .framework          │
    │               │       │  (Apple's own VMM)   │
    │  smolvm 1.5   │       │                      │
    │  Rosetta ✓    │       │  Rosetta 2 support ✓ │
    │  via virtiofs │       │  VirtioFS ✓          │
    └───────┬──────┘       │  GPU passthrough ✓   │
            │              └───────┬──────────────┘
        smolvm                       │
        krunvm                    Docker Desktop
                                  UTM, Lima

```

**Hypervisor.framework** — CPU virtualization primitive (like KVM). Both libkrun and Virtualization.framework use it underneath.

**Virtualization.framework** — Apple's high-level VM manager. Adds virtual devices, networking, file sharing, and Rosetta 2 for Linux. Docker Desktop uses this.

**smolvm 1.5.2** — now exposes Rosetta 2 on Apple Silicon through a virtiofs mount and a guest binfmt wrapper. The runtime is opt-in with `--rosetta`; it is not enabled for every VM by default.

The benchmark host now has smolvm 1.5.2 installed. The comparison scripts pass `--rosetta` when creating their benchmark VMs.

## Rosetta 2 for Linux

Apple provides a Rosetta binary that can run inside ARM64 Linux VMs to translate x86_64 binaries at near-native speed (~0.9x). It's exposed via `VZLinuxRosettaDirectoryShare` in Virtualization.framework.

### How Rosetta verifies its environment

Rosetta checks it's running inside a Virtualization.framework VM by:

1. Opening `/proc/self/exe` (the rosetta binary itself)
2. Issuing an `ioctl(fd, _IOC(_IOC_READ, 0x61, 0x22, 0x45), buf)`
3. Expecting a specific response string (Apple's copyright notice)

The virtio-fs implementation in Virtualization.framework intercepts this ioctl and returns the expected string. Without this, Rosetta refuses to run.

Reference: [Quick look at Rosetta on Linux](https://threedots.ovh/blog/2022/06/quick-look-at-rosetta-on-linux/)

### libkrun's Rosetta attempt (reverted)

libkrun [added Rosetta support](https://github.com/containers/libkrun/pull/88) by intercepting the same ioctl in its virtio-fs passthrough layer and returning the expected response from a file (`~/.krunvm-rosetta`). This worked but was [reverted](https://github.com/containers/libkrun/pull/176/commits/6c2b9289b6a39826c9505f2fad5b04cc83982165) — likely due to the legal implications of spoofing Apple's verification.

Detailed analysis (Japanese): [Rosettaはなぜ特定のVMM上の仮想マシンでないと使えないか](https://zenn.dev/orimanabu/articles/rosetta-libkrun)

Key code locations in the reverted PR:
- `src/devices/src/virtio/fs/macos/passthrough.rs` — `rosetta_data` field on `PassthroughFs`
- `read_rosetta_data()` reads `${HOME}/.krunvm-rosetta`
- ioctl handler checks `cmd == IOCTL_ROSETTA` (type `0x61`) and returns the data

### smolvm 1.5.2 implementation

smolvm now provides an opt-in implementation without requiring a full
Virtualization.framework VMM migration. It mounts Apple's Linux Rosetta runtime through
virtiofs, installs a guest `rosetta-wrapper`, and registers the x86_64 ELF interpreter
through `binfmt_misc`. The host-side CLI flag is `--rosetta`.

Verified on this workstation after installing smolvm 1.5.2:

```text
smolvm 1.5.2
command: smolvm machine run --net --rosetta --image ubuntu:24.04 -- uname -m
guest uname -m: x86_64
Rosetta runtime: /mnt/rosetta/rosetta
guest wrapper: /usr/bin/rosetta-wrapper
```

The command pulled the amd64 Ubuntu image and returned `x86_64` inside the ARM64
guest, confirming that the Rosetta runtime and binfmt integration execute an x86_64
image successfully. This does not prove that every x86 binary or package is compatible.

The old reverted-libkrun analysis remains useful historical context, but it no longer
describes the current smolvm 1.5.2 path.

### Performance comparison

| Backend | x86 binary speed | Legal status |
|---------|-----------------|--------------|
| Rosetta 2 (Virtualization.framework) | ~0.9x native | Supported by Apple |
| QEMU user-static (binfmt) | ~0.1-0.2x native (5-10x slower) | Open source, works in smolvm |
| No emulation | Fails (`exec format error`) | N/A |

## What works on ARM64 today (~90% of CI workflows)

| Category | Examples | ARM64 status |
|----------|----------|-------------|
| Docker base images | node, python, golang, rust, alpine, ubuntu, postgres, redis | All multi-arch |
| JS/TS toolchains | npm, yarn, pnpm, bun | Arch-independent |
| pip packages | numpy, pandas, django, flask, pytest, httpx | ARM64 wheels available |
| Compiled languages | cargo build/test, go build/test | Source compilation, any arch |
| apt-get (Ubuntu) | curl, git, jq, make, gcc, openssl | ARM64 .debs available |

## What breaks on ARM64 (~10% of workflows)

1. **Hardcoded `linux-amd64` binary downloads** — `curl tool-linux-amd64`. Main offender, but popular tools (terraform, helm, kubectl) all have ARM64 builds now.
2. **x86-only Docker images** — Increasingly rare. Most Docker Hub images are multi-arch.
3. **GitHub Actions with hardcoded x86 binaries** — Some third-party actions. Popular ones (setup-node, setup-python, checkout) handle ARM64.
4. **Niche apt packages** — Some compile from source on ARM64 (slow but works).

## Options for aksh

| Path | Speed | Status | Notes |
|------|-------|--------|-------|
| ARM64-native only | 1x | Default | Works for most workflows; use when images and tools publish ARM64 variants. |
| QEMU binfmt in smolvm | 0.1–0.2x | Fallback | Broad compatibility but too slow for normal CI. |
| smolvm 1.5.2 `--rosetta` | Near-native | Available | Opt-in Rosetta 2 runtime via virtiofs and guest binfmt wrapper on Apple Silicon. |
| Docker Desktop fallback | Near-native | Alternative | Delegate x86 jobs to Docker Desktop's VM when a separate VM boundary is preferred. |
| Encourage ARM64 workflows | 1x | Recommended where possible | Avoids translation and improves portability. |

## Current recommendation

Keep ARM64-native execution as the default. Enable `--rosetta` for smolvm VMs that need
x86-only binaries or `linux/amd64` images. The benchmark harness now passes `--rosetta`
to its comparison VMs and was smoke-tested with smolvm 1.5.2 using an Ubuntu image:

```sh
smolvm machine run --net --rosetta --image ubuntu:24.04 -- \
  sh -lc 'uname -m; ls -l /mnt/rosetta/rosetta'
```

This selects an x86_64 image and exposes `/mnt/rosetta/rosetta` inside the guest. A
follow-up benchmark is still required for workflows that execute x86-only binaries
inside ARM64 guest/container filesystems; enabling the mount is not itself proof that
every binary or package is compatible.
