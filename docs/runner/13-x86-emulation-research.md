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
    │  Open-source  │       │                      │
    │  lightweight  │       │  Rosetta 2 support ✓ │
    │  No Rosetta ✗ │       │  VirtioFS ✓          │
    │               │       │  GPU passthrough ✓   │
    └───────┬──────┘       └───────┬──────────────┘
            │                      │
        smolvm                 Docker Desktop
        krunvm                 UTM, Lima
        Podman <5              Podman ≥5
```

**Hypervisor.framework** — CPU virtualization primitive (like KVM). Both libkrun and Virtualization.framework use it underneath.

**Virtualization.framework** — Apple's high-level VM manager. Adds virtual devices, networking, file sharing, and Rosetta 2 for Linux. Docker Desktop uses this.

**libkrun** — Open-source VM manager from Red Hat/containers project. Also uses Hypervisor.framework for CPU, but implements its own device layer. Lighter weight than Virtualization.framework but lacks Rosetta.

**smolvm uses libkrun** → ARM64-only VMs → no native x86 binary support.

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

| Path | Speed | Effort | Notes |
|------|-------|--------|-------|
| ARM64-native only | 1x | Done | Works for ~90% of workflows |
| QEMU binfmt in smolvm | 0.1-0.2x | Low (apt-get install qemu-user-static) | Too slow for CI |
| Rosetta via Virtualization.framework | ~0.9x | High (new VM backend) | Best UX, needs Apple's framework |
| Docker Desktop fallback | ~0.9x | Medium | Delegate x86 jobs to Docker Desktop's VM |
| Encourage ARM64 workflows | 1x | Documentation | Growing ecosystem support |

## Current recommendation

Ship ARM64-native. Document which workflow patterns need ARM64-compatible images. Most web development CI (Node/Python/Go/Rust + postgres/redis) works without changes. Monitor libkrun upstream for any future Rosetta re-integration.
