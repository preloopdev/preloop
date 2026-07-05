# CI Parity Plan: Local aksh ↔ GitHub-hosted Runners

How to make `aksh` local CI match GitHub-hosted runner behavior closely enough that workflows work identically on both, while keeping local execution fast.

---

## The Gap

| Dimension | GitHub-hosted | aksh local today |
|-----------|--------------|------------------|
| Environment | Fresh Ubuntu 24.04 VM per job, destroyed after | Persistent host or smolvm, state leaks between jobs |
| Tools | ~200 pre-installed (Docker, git, node, python, go, rust, jq, gh, aws, ...) | Whatever's on the host or a bare alpine/ubuntu image |
| Workspace | `actions/checkout` clones the repo into `$GITHUB_WORKSPACE` | `runner-e2e` doesn't populate workspace; `dogfood.yml` uses `vars.AKSH_REPO_ROOT` |
| Actions | Downloaded from GitHub via `actionsDownloadInfo` + codeload tarball | ✅ Implemented (F022), but local runner-e2e doesn't have a git repo to checkout from |
| Docker | Pre-installed, overlay2 on ext4, full bridge networking | Host Docker works; smolvm needs setup (`dockerd` + ext4 block device) |
| Secrets/OIDC | Injected by GitHub control plane | Passed via `--secret` flag to `aksh-runner-client submit` |
| Network | Unrestricted outbound | smolvm needs `--net`; Firecracker needs TAP setup |
| Hardware | 4 vCPU / 16 GB (public), 2 vCPU / 8 GB (private) | Whatever the host has; smolvm defaults 4 vCPU / 8 GB |

## Strategy: Three Tiers

Don't try to replicate all 200 tools. Instead, build three tiers that cover 95% of real workflows:

### Tier 1: Bare-metal local (fastest, no VM)

```
aksh-runner-server + aksh-runner on the host
Docker daemon on the host
Workspace = the actual repo checkout on disk
```

**What it proves**: Runner protocol correctness, step execution, Docker container jobs, service containers, action lifecycle. This is `dogfood.yml` today.

**What it can't prove**: Ephemeral isolation, tool version parity, clean-state guarantees.

**Cost**: Zero — already works.

### Tier 2: Ephemeral smolvm (matches GitHub's isolation model)

```
Host orchestrator (aksh-runner-server)
  └── Per-job: smolvm machine run --net --image <runner-image> -- aksh-runner run --once
        ├── aksh-runner (inside VM)
        ├── dockerd (inside VM, ext4 block device)
        ├── git, curl, jq, node (pre-installed in image)
        └── /workspace (virtio-fs mount to host repo)
```

**What it proves**: Ephemeral clean state per job, Docker-in-VM, tool availability, workspace isolation. This is the production shape.

**What it can't prove**: Exact GitHub tool versions (we pick our own), network latency to GitHub APIs, OIDC federation.

**Cost**: Build a runner image (~2 hours one-time), ~1.5s boot overhead per job.

### Tier 3: Live GitHub (the truth)

```
Push to preloopdev/aksh-conformance-sample
Official or aksh runner registered as self-hosted
GitHub's control plane dispatches the job
```

**What it proves**: Everything — real secrets, real OIDC, real artifact storage, real caching, real action resolution. This is the acceptance gate.

**Cost**: Requires GitHub repo access, network, runner registration. Slow feedback loop (~30-60s per job).

---

## Implementation Plan

### Phase 1: Build the runner image (Tier 2 enabler)

Build a Docker image that approximates `ubuntu-latest` for the tools that matter:

```dockerfile
FROM ubuntu:24.04

# Core tools (matches GitHub runner-images)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl wget jq ca-certificates gnupg lsb-release \
    build-essential gcc g++ make cmake \
    python3 python3-pip python3-venv \
    zip unzip tar gzip xz-utils \
    sudo openssh-client \
    && rm -rf /var/lib/apt/lists/*

# Docker CE (needed for container jobs)
RUN install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
      https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
      > /etc/apt/sources.list.d/docker.list && \
    apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io && \
    rm -rf /var/lib/apt/lists/*

# Node.js 20 (most common in actions)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && rm -rf /var/lib/apt/lists/*

# gh CLI
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    -o /usr/share/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] \
      https://cli.github.com/packages stable main" \
      > /etc/apt/sources.list.d/github-cli.list && \
    apt-get update && apt-get install -y gh && rm -rf /var/lib/apt/lists/*

# Runner user (non-root, with sudo)
RUN useradd -m -s /bin/bash runner && echo "runner ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
USER runner
WORKDIR /home/runner
```

This covers: git, Docker, Node.js 20, Python 3, gcc/g++, cmake, jq, gh, curl/wget — enough for ~90% of GitHub Actions workflows.

**Not included** (install via actions if needed): Go, Rust, Java, .NET, AWS CLI, Azure CLI. These are large and workflow-specific. The official `dtolnay/rust-toolchain`, `actions/setup-go`, etc. handle them.

Save this image locally and use with smolvm:
```sh
docker build -t aksh-runner-image .
docker save aksh-runner-image -o runner-image.tar
```

### Phase 2: Ephemeral job execution via smolvm

Wire the orchestrator to boot a fresh smolvm VM per job:

```
Per job dispatch:
1. smolvm machine run \
     --net \
     --image ./runner-image.tar \
     --cpus 2 --mem 4096 \
     -v /path/to/repo:/workspace \
     -v /var/cache/ci:/cache:ro \
     -- /workspace/target/release/aksh-runner \
        --runner-root /tmp/runner \
        run --once

2. VM boots (~1.5s), runner registers, picks up job, executes, reports, exits
3. smolvm cleans up VM (ephemeral mode — automatic)
```

Key details:
- **Ephemeral**: `machine run` destroys VM after exit — clean state guaranteed
- **Workspace**: Host repo mounted via virtio-fs at `/workspace`
- **Cache**: Shared read-only cache mount at `/cache` for cargo registry, npm, pip
- **Docker**: `dockerd` started via init script inside the image, uses `/dev/vdb` (ext4 storage disk)
- **Network**: `--net` for outbound (action downloads, Docker pulls)

### Phase 3: Workspace population (actions/checkout parity)

The local server needs to provide workspace content to the runner. Three approaches, pick one:

**Option A: virtio-fs mount (current approach)**
Mount the host repo directory into the VM. Steps see the repo at `/workspace`. This is what `dogfood.yml` does with `vars.AKSH_REPO_ROOT`. Pros: instant, no copy. Cons: not ephemeral (runner can modify host files), doesn't simulate `actions/checkout`.

**Option B: Git clone inside the VM**
The aksh server provides a git URL in the job payload. The runner's `actions/checkout` implementation clones it. Pros: matches GitHub behavior. Cons: requires a local git server or access to the remote repo.

**Option C: Tarball injection** *(recommended for local CI)*
`aksh-runner-server` tarballs the workspace directory on submission, serves it at a URL the runner can download, and the runner extracts it to `$GITHUB_WORKSPACE` during the "Set up job" phase. Pros: ephemeral (each job gets a snapshot), fast (local HTTP), matches the isolation model. Cons: requires implementation.

### Phase 4: Action resolution for local actions

For `uses: ./path/to/action` to work, the workspace must contain the action files. This is solved by Phase 3 — once the workspace is populated, relative action paths resolve naturally.

For `uses: actions/checkout@v4` and other remote actions, the runner already implements F022 (action resolution + codeload download). This works when the VM has network access (`--net`).

### Phase 5: Docker-in-VM validation

Validate the full Docker lifecycle inside smolvm:

```yaml
# Test workflow: container job + service containers
jobs:
  test:
    runs-on: self-hosted
    container:
      image: node:20
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: test
        ports:
          - 5432:5432
    steps:
      - run: node --version
      - run: pg_isready -h localhost -p 5432
```

This requires:
- `dockerd` running inside the smolvm guest
- ext4 block device for Docker storage (overlay2)
- Bridge networking inside the guest (libkrunfw kernel supports this)
- Service container health checks

### Phase 6: Cache layer via virtio-fs

Mount a persistent host directory into each ephemeral VM for dependency caching:

```sh
smolvm machine run \
  -v /var/cache/ci/cargo:/home/runner/.cargo/registry:ro \
  -v /var/cache/ci/npm:/home/runner/.npm:ro \
  ...
```

After a job completes successfully, sync new cache entries from the VM back to the host (write path). This avoids re-downloading dependencies on every job while maintaining ephemeral isolation.

---

## What's Reasonable vs Overkill

| Approach | Effort | Parity | Verdict |
|----------|--------|--------|---------|
| Bare-metal + host Docker (Tier 1) | Already done | ~70% | ✅ Keep as fast-feedback loop |
| Runner image + smolvm ephemeral (Tier 2) | 2-3 days | ~90% | ✅ Do this — it's the production shape |
| Replicate all 200 GitHub tools | 1-2 weeks | ~95% | ❌ Overkill — use setup actions instead |
| OIDC federation locally | 1 week | +2% | ❌ Skip — test on real GitHub (Tier 3) |
| Custom Firecracker rootfs | 3-5 days | ~85% | ❌ Skip for now — smolvm is easier and cross-platform |
| Live GitHub self-hosted (Tier 3) | Already done | 100% | ✅ Keep as acceptance gate |

## Execution Order

1. **Build runner image** → Dockerfile + `docker save` → test with `smolvm machine run --image ./runner-image.tar -- echo hello`
2. **Validate ephemeral execution** → `smolvm machine run --net --image ./runner-image.tar -- aksh-runner run --once` against aksh-server
3. **Validate Docker-in-VM** → Start dockerd inside image, run container job workflow
4. **Wire into runner-e2e** → `aksh-conformance runner-e2e --isolation smolvm` flag that boots per-job VMs
5. **Cache layer** → Add virtio-fs cache mounts, measure warm-build speedup
6. **Conformance replay on x86** → Run `runner-watch conform` suite on vm103

---

## Success Criteria

- [ ] `dogfood.yml` passes inside ephemeral smolvm VM on both ARM64 (macOS) and x86_64 (Linux)
- [ ] Container job workflow (node:20 + postgres service) passes inside smolvm VM
- [ ] Runner image boots in <3s, total job overhead <5s compared to bare-metal
- [ ] No state leaks between consecutive job runs (verified by marker file test)
- [ ] `runner-watch conform` suite produces identical results on x86_64 as on ARM64
