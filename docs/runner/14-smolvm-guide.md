# smolvm Guide — Reference for aksh Development

## Overview

smolvm creates lightweight ARM64 Linux VMs on Apple Silicon using libkrun (a VMM built on Apple's Hypervisor.framework). We use it to run the aksh runner with Docker inside a Linux VM for E2E testing against GitHub.

## Architecture

```
Host (macOS, Apple Silicon)
  └─ smolvm CLI
       └─ libkrun (Hypervisor.framework)
            └─ ARM64 Linux VM (ubuntu:24.04)
                 ├─ smolvm-agent (vsock service)
                 ├─ Docker CE (overlay2 on ext4)
                 └─ /workspace (virtio-fs mount → host repo)
```

Communication between host and guest is over **vsock**, bridged to a Unix domain socket on the host (`~/.cache/smolvm/machines/<name>/agent.sock`). The smolvm-agent inside the VM handles all requests.

## Key Commands

```sh
# Create a VM
smolvm machine create --name build-runner --image ubuntu:24.04 \
  --cpus 4 --mem 8192 --storage 20 --net \
  -v ~/myrepo:/workspace

# Lifecycle
smolvm machine start --name build-runner    # 1.2s boot
smolvm machine stop --name build-runner
smolvm machine rm --name build-runner

# Execute commands
smolvm machine exec --name build-runner -- echo hello          # quick commands (<30s)
smolvm machine exec --name build-runner --stream -- cargo build # long commands (streaming)
smolvm machine exec --name build-runner --timeout 10m -- cargo test  # explicit timeout
smolvm machine exec --name build-runner --detach -- ./server   # background daemon
```

## exec Modes — Critical for Long-Running Commands

`smolvm machine exec` has a **30-second socket read timeout** by default. The command's stdout/stderr is **fully buffered** until it exits. This means:

| Mode | Flag | Timeout | Output | Use for |
|---|---|---|---|---|
| Buffered (default) | none | 30s | All at once on exit | Quick commands: ls, echo, apt-get |
| Streaming | `--stream` | None | Incremental | Builds, tests, long installs |
| Explicit timeout | `--timeout 10m` | Custom + 5s buffer | Buffered | Known-duration commands |
| Background | `--detach` | None | Returns PID | Daemons, servers |
| Interactive | `-it` | None | Live stdin/stdout | Shells, debugging |

**If a command takes >30s without `--stream` or `--timeout`, the host-side socket times out and kills it (exit code 137).** The process inside the VM becomes a zombie. This is the #1 gotcha.

### Recovery from zombies

If exec kills a command mid-flight:
```sh
smolvm machine stop --name build-runner   # clean shutdown (agent.shutdown() → sync())
smolvm machine start --name build-runner  # fresh boot, zombies gone
```

## Docker Setup in VMs

Docker is not pre-installed in base images. Setup once:

```sh
# Install Docker CE
smolvm machine exec --name myvm --stream -- sh -c '
  apt-get update -qq && apt-get install -y ca-certificates curl
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
    https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -qq && apt-get install -y docker-ce docker-ce-cli containerd.io
'

# Docker needs a separate ext4 block device for overlay2 storage
smolvm machine exec --name myvm -- sh -c '
  mkfs.ext4 /dev/vdb && mkdir -p /var/lib/docker-ext4
  mount /dev/vdb /var/lib/docker-ext4
  dockerd --data-root /var/lib/docker-ext4 &
'
```

### Storage driver

Docker defaults to **overlay2** on ext4. The VM's root filesystem is an overlayfs itself (from the container image layers), so Docker needs a real block device (`/dev/vdb` from the `--storage` option) formatted as ext4. Without this, Docker falls back to `vfs` which is extremely slow (copies entire image on every container create).

## Packing VMs

### Image-based pack (recommended)

```sh
smolvm pack create --image ubuntu:24.04 --output ./my-tool
# Result: clean, reproducible, ~37 MB compressed
./my-tool run -- echo hello  # boots in ~9s (first run extracts assets)
```

### VM-snapshot pack

```sh
# IMPORTANT: clean the VM before packing
smolvm machine exec --name myvm -- sh -c '
  pkill dockerd 2>/dev/null
  rm -f /var/run/docker.pid /var/run/docker.sock
  rm -f /var/run/*.pid
  apt-get clean
'
smolvm machine stop --name myvm  # triggers sync() + clean exit
smolvm pack create --from-vm myvm --output ./my-tool
```

**`--from-vm` exports the overlayfs `upper/` directory** — every filesystem change since VM creation. This includes:
- Installed packages, config files, compiled binaries (good)
- PID files, socket files, `/var/run` state, Docker daemon state (bad for fresh boot)

If the VM has stale state (zombie processes, crashed daemons, leftover locks), the packed VM may fail to boot because the agent inherits that dirty state. Always clean up before packing.

### Volume mount collision

Packed VMs have an internal storage disk mounted at `/workspace`. If you also mount a host directory at `/workspace`:
```sh
./my-tool start -v ~/myrepo:/workspace  # SHADOWED by internal /workspace!
```
The internal mount wins. Use a different path:
```sh
./my-tool start -v ~/myrepo:/work       # works
```

Regular `smolvm machine` VMs don't have this problem because the virtio-fs mount takes precedence.

## x86 Emulation

smolvm VMs are ARM64-only (libkrun doesn't support Rosetta). See `docs/runner/13-x86-emulation-research.md` for details.

**QEMU fallback** (slow, 5-10x overhead):
```sh
smolvm machine exec --name myvm -- sh -c '
  apt-get install -y qemu-user-static binfmt-support
  echo ":qemu-x86_64:M::\x7fELF\x02\x01\x01\x00...::/usr/bin/qemu-x86_64-static:FPC" \
    > /proc/sys/fs/binfmt_misc/register
'
# Now x86_64 Docker images work (slowly)
docker run --platform linux/amd64 alpine uname -m  # x86_64
```

## Performance Notes

| Metric | Value |
|---|---|
| VM boot (from stopped) | 1.2s |
| VM boot (packed, first run) | ~9s (asset extraction) |
| VM boot (packed, cached) | ~3s |
| virtio-fs overhead vs native | ~1.7x for I/O-heavy workloads |
| Docker overlay2 on ext4 | Same perf as native Linux |
| Rust compilation (warm cache) | ~1.7x slower than host bare metal |
| Rust compilation (cold cache) | 118s vs 27s host (4.4x, includes I/O overhead) |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Exit code 137 on exec | 30s socket timeout | Use `--stream` or `--timeout` |
| Zombie processes after crash | Exec killed mid-command | `stop` + `start` the VM |
| Packed VM won't boot | Dirty state in overlay | Clean VM before packing |
| Docker uses vfs (slow) | No ext4 block device | Mount `/dev/vdb` as ext4 |
| Host mount not visible | Path collision with internal disk | Use different mount point |
| `cargo: command not found` | Non-login shell, PATH not set | `export PATH="$HOME/.cargo/bin:$PATH"` |
