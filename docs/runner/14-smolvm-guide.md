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

## Host / Registry DNS for Docker Pulls

Docker image pulls need normal guest DNS before container networking matters. If `docker pull alpine:3.20` fails with Docker trying to query loopback IPv6 DNS:

```text
lookup registry-1.docker.io on [::1]:53: read: connection refused
```

the VM was started with a broken resolver. Do **not** debug this as a Docker bridge or `127.0.0.11` service-container problem; Docker has not reached container networking yet.

Create the runner VM with an explicit DNS resolver:

```sh
smolvm machine create --name p0-linux \
  --image ubuntu:24.04 \
  --cpus 4 --mem 8192 --storage 40 --net \
  --dns 1.1.1.1 \
  -v /tmp/smolvm-share:/share
```

Use the resolver that works on your network. Public examples:

- `--dns 1.1.1.1`
- `--dns 8.8.8.8`
- corporate/VPN resolver IP if public DNS is blocked

Validate before installing/running Docker:

```sh
smolvm machine exec --name p0-linux -- sh -c '
  cat /etc/resolv.conf
  nslookup registry-1.docker.io
'
```

Expected:

```text
nameserver 1.1.1.1
Name: registry-1.docker.io
Address: ...
```

`smolvm machine update` currently cannot change DNS on an existing machine, so recreate the VM if it was created with the wrong resolver. Editing `/etc/resolv.conf` inside an image-based VM is not a reliable fix: depending on the image/rootfs layout it may be absent, generated at boot, or not writable in the layer you expect.

After DNS works, start Docker and confirm a fresh pull:

```sh
smolvm machine exec --name p0-linux --stream -- sh -c '
  dockerd --storage-driver vfs >/tmp/dockerd.log 2>&1 &
  sleep 3
  docker pull alpine:3.20
  docker run --rm alpine:3.20 echo docker-pull-and-run-ok
'
```

This fixes host/registry resolution only. Docker service-container name resolution (`http://web/`) is a separate limitation covered below.

## Docker Service Containers & DNS Limitations

When running workflows with Docker service containers (e.g., `services:` blocks mapping a DB or Web container), standard Docker container-to-container DNS resolution (`http://web/`) fails under the default `smolvm` user-space network mode (`tsi`). This is because the statically compiled `libkrun` guest kernel lacks the Netfilter NAT modules that Docker's embedded DNS server (`127.0.0.11`) relies on to intercept queries.

Here are the three workarounds for local CI:

### Workaround 1: Localhost Port Mapping (No Host VM changes)
Configure the service container to bind its ports to the runner VM, and have the job access the service via `localhost` instead of the service name:
```yaml
services:
  web:
    image: nginx:1.27-alpine
    ports:
      +- 80:80 # Maps container port 80 to VM port 80
steps:
  +- name: Access Service
    run: curl -fsS http://localhost:80/
```
* **Pros:** 100% compatible with production GitHub Actions (where mapping ports to `localhost` is the standard way to connect to services).
* **Cons:** Requires adding `ports:` to workflow YAML files.

### Workaround 2: Use virtio-net Backend (Root VM start)
Configure `smolvm` to use the host-native networking backend instead of `tsi` when creating the VM:
```sh
smolvm machine create --name my-runner --net --net-backend virtio-net ...
```
* **Pros:** Standard container-to-container DNS works out of the box. No YAML changes required.
* **Cons:** Requires running the host `smolvm start` command with `sudo` on macOS to allow the VM to attach to the host `vmnet.framework` interface.

### Workaround 3: Use Lima/Colima on macOS (Zero-YAML, Zero-Sudo Local CI)
Instead of running nested microVMs via `smolvm` for every test run, developers can run `aksh-runner` directly inside a persistent lightweight Linux VM managed by **Lima** or **Colima** on their Macs:
```sh
# Start standard docker VM
limactl start template://docker
# Shell in and run the local runner
lima aksh-runner --runner-root /tmp/runner run
```
* **Pros:** Standard Docker bridge networks, DNS, and `overlay2` storage work natively inside the VM with 100% local-production parity. No YAML changes and no `sudo` required.
* **Cons:** Pays a one-time 10-15s boot time cost to start the Lima/Colima VM in the morning (subsequent workflow jobs run instantly in milliseconds inside the persistent VM).
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

### Disk I/O: virtio-fs vs virtio-blk

**For normal workflow execution** (runner checks out code inside the VM, cargo
writes `./target` inside the VM), disk I/O goes through the **guest's own
virtio-blk overlay disk** — not a host mount. virtio-fs is not involved and is
not a bottleneck.

**virtio-fs** (FUSE over vsock) is only in play when you explicitly mount a host
directory into the VM with `-v HOST:GUEST` and then do write-heavy work on that
path from inside the VM. Each filesystem syscall crosses the hypervisor boundary,
which is cheap for reads but expensive for write storms (thousands of small
`.rlib`/`.rmeta`/`.d` files). If you find yourself in that situation, the fix is
to use a guest tmpfs as a staging area and copy results out with
`smolvm machine cp` rather than writing back through the mount.

smolvm exposes host volumes exclusively via virtio-fs. The internal `--storage`
and `--overlay` disks are virtio-blk (ext4 images handed directly to libkrun),
but there is no CLI flag to present a host directory as a block device.

### Workflow build performance: vCPU count is the bottleneck

For CI workflows, the dominant factor is **vCPU count**, not I/O. `cargo build`
on a large workspace (e.g. axum) is CPU-bound once the source is checked out
inside the VM. The default VM has 4 vCPUs; the host has 16.

Increase vCPUs at VM create time for compile-heavy workloads:
```bash
smolvm machine create --name p0-linux --cpus 8 ...
```

### General table

| Metric | Value |
|---|---|
| VM boot (from stopped) | 1.2s |
| VM boot (packed, first run) | ~9s (asset extraction) |
| VM boot (packed, cached) | ~3s |
| virtio-fs overhead vs native (read-heavy) | ~1.7× |
| virtio-fs overhead (write storm on host mount) | significant; use guest tmpfs instead |
| Docker overlay2 on ext4 (guest disk) | Same perf as native Linux |
| Rust compilation (warm cache, 4 vCPUs) | ~1.7× slower than 16-vCPU host |
| Rust compilation (cold cache, 4 vCPUs) | 118s vs 27s on host (4.4×) |
## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Exit code 137 on exec | 30s socket timeout | Use `--stream` or `--timeout` |
| Zombie processes after crash | Exec killed mid-command | `stop` + `start` the VM |
| Packed VM won't boot | Dirty state in overlay | Clean VM before packing |
| `docker pull` uses `[::1]:53` and fails | Broken guest resolver | Recreate VM with `--net --dns <resolver-ip>` |
| Docker uses vfs (slow) | No ext4 block device | Mount `/dev/vdb` as ext4 |
| Host mount not visible | Path collision with internal disk | Use different mount point |
| `cargo: command not found` | Non-login shell, PATH not set | `export PATH="$HOME/.cargo/bin:$PATH"` |
