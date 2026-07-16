# Docker, Services, and Container Actions

## Goal

Preloop's isolation claim only works if Docker and container workflows run without the host Docker socket. Real CI workflows often require:

- `docker build`,
- `docker run`,
- Docker Compose,
- service containers such as Postgres and Redis,
- container actions,
- job-level `container:` blocks,
- and BuildKit cache behavior.

This is a critical product gate, not an enhancement.

smolvm boots OCI images directly as microVMs — no Docker daemon is needed to
*run* an image. A Docker daemon is only needed when the *workflow itself* invokes
`docker build`/`docker run`/compose, which requires `dockerd`/buildkitd running
inside the guest (Path B below).

## Non-negotiable rule

```text
Never mount the host Docker socket as a core Preloop execution path.
```

The host Docker socket gives untrusted CI code too much access to the host. If a local user explicitly opts into it for a trusted repo, mark the run unsafe and lower the fidelity/security score.

## Execution paths

### Path A: direct job steps

For normal `run`, JavaScript, and composite actions:

```text
Aksh runner executes directly in the job VM.
```

No Docker required.

### Path B: private in-guest Docker

For user workflows that invoke Docker:

```text
smolvm microVM (Firecracker on the scale tier)
  +-- dockerd/buildkitd
  +-- docker CLI
  +-- job process runs docker build/run/compose
```

This is the most faithful approach for user-invoked Docker.

### Path C: smolvm-native OCI boot

For container actions and service containers, prefer mapping OCI images straight into smolvm microVMs (smolvm already runs any OCI image) instead of routing through in-guest Docker, where semantics match.

Do this only after private Docker works, because private Docker is the compatibility baseline.

## Kernel/rootfs requirements

Private Docker likely requires:

```text
CONFIG_OVERLAY_FS
CONFIG_BLK_DEV_LOOP
CONFIG_CGROUPS / cgroups v2
CONFIG_NAMESPACES
CONFIG_VETH
CONFIG_BRIDGE
CONFIG_NETFILTER / nftables
CONFIG_SECCOMP
CONFIG_BINFMT_MISC optional
fuse-overlayfs optional fallback
```

This is why the tier's guest kernel/rootfs must ship these features. If smolvm's bundled libkrunfw kernel lacks a required config for in-guest Docker, resolve it upstream in the tier image (libkrunfw kernel config / rootfs), not by reimplementing the hypervisor. The Firecracker tier bakes an equivalent rootfs+kernel.

## Service strategies

| Strategy | When | Pros | Cons |
|---|---|---|---|
| Same-VM service processes | MVP | simplest, fast | less isolation between service and job |
| Private Docker services | compatibility baseline | matches many CI workflows | requires Docker working in guest |
| One microVM per service | isolation mode | strong isolation | networking complexity |
| Host service bridge | local trusted opt-in only | fast | weak isolation/fidelity |

Recommended progression:

1. Same-VM or private Docker services for MVP.
2. Private Docker services with health checks for v0.2.
3. One-service-per-microVM for stronger managed isolation later.

## Container actions

Support order:

1. JavaScript actions.
2. Composite actions.
3. Docker container actions through private in-guest Docker.
4. smolvm-native OCI boot for container actions/services.

Container action requirements:

- action metadata parsing,
- image pull/build,
- entrypoint/cmd semantics,
- env propagation,
- workspace mount,
- `GITHUB_*` env files,
- log streaming,
- exit code propagation,
- pre/post cleanup where applicable,
- cache and artifact access policy.

## Job-level containers

`jobs.<job>.container` should eventually run the job inside a container environment within the microVM.

Implementation options:

- Use private Docker to run the job container.
- Run container rootfs directly with namespaces inside the VM.
- Map the OCI image directly into a smolvm microVM (VM-per-container) later.

For fidelity, private Docker is the safest first implementation.

## Service health checks

Service containers must support:

- startup ordering,
- health check command/interval/timeout/retry,
- port exposure inside the job VM,
- logs on failure,
- cleanup on cancellation,
- network denial classification,
- and deterministic failure if unsupported.

Minimum test:

```yaml
services:
  postgres:
    image: postgres:16
    env:
      POSTGRES_PASSWORD: postgres
    options: >-
      --health-cmd pg_isready
      --health-interval 10s
      --health-timeout 5s
      --health-retries 5
```

## Docker gate

Run this Docker gate on smolvm (Apple Silicon + Linux/KVM) and on the Firecracker tier:

```text
1. VM boots.
2. dockerd starts.
3. docker info succeeds.
4. docker run hello-world succeeds.
5. docker build a small Dockerfile succeeds.
6. docker build uses BuildKit or reports classified unsupported.
7. docker run postgres succeeds.
8. postgres health check succeeds.
9. job can connect to postgres.
10. cleanup removes containers/images/networks.
11. no host Docker socket is mounted.
12. VM can be killed/reaped cleanly mid-build.
```

If any of these fail, either fix the rootfs/kernel/runtime or explicitly defer Docker-dependent workflows.

## Cross-architecture concerns

macOS Apple Silicon local mode will often run arm64 Linux guests. Many workflows expect amd64 images.

Options:

- native arm64 images where available,
- multi-arch OCI resolution,
- `binfmt_misc` + qemu-user inside the VM,
- smolvm `--rosetta` x86-on-ARM translation (~17% overhead vs QEMU's 5–10×),
- or fail loudly with a fidelity warning.

Never silently run the wrong architecture.

## Product UX

Add clear support messages:

```text
This workflow uses services.postgres.
Preloop can run it with private in-guest Docker.
Runtime: smolvm-docker
Network: virtio-net
Fidelity: high
```

or:

```text
This workflow uses Docker services, but the current runtime profile cannot start dockerd.
Run: preloop doctor docker
Suggested runtime: smolvm-docker (or firecracker-docker on the scale tier)
```
