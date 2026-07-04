# Container &amp; Service-Container Support for the aksh Rust Runner (Docker-compatible first, VM-per-job ready)

## Context

Add GitHub Actions `jobs.<id>.container:` and `jobs.<id>.services:` support end-to-end in aksh. The control plane (`aksh-runner-server` + `aksh-gha-parser`) must emit the same container fields GitHub's control plane sends, and the native Rust runner (`crates/aksh-runner`) must execute container jobs with behavior matching the official `actions/runner`.

Updated architecture decision: **Docker engine compatibility is the primary implementation path.** The first supported backend is a real Docker-compatible engine, because the official runner itself uses Docker for job containers and service containers. The production isolation model is **fresh VM/microVM per job with the runner/worker and Docker stack inside the VM**. 

The main plan for local macos ci is to use Smolvm. THey bundle a kernel that supports all thats needed to run nested docker i.e cgroups, overlayfs2

## Implementation Status (2026-07-04)

| Phase | Status | Evidence |
|---|---|---|
| Phase 0 — Record official goldens | ✅ Complete | 7 scenarios (30-36) recorded under `.runner-watch/golden/v2.335.1/` |
| Phase 1 — Control plane emits container fields | ✅ Complete | `container`/`services` fields in Job, JobPlan, AgentJobRequestMessage |
| Phase 2 — Runner container execution | ✅ Complete | Full Docker lifecycle, TemplateToken decoding, runtime contexts, synthetic steps |
| E2E validation (GitHub) | ✅ Passed | Scenarios 30, 31, 33, 35, 36 on live GitHub; container-contexts test (run 28706488417) |
| E2E validation (aksh-server) | ✅ Passed | Container job via aksh-runner-client → aksh-runner-server → aksh-runner on smolvm |
| Review fixes | ✅ Applied | Runtime contexts, service volumes, synthetic step logs, format brace escaping, template parser |

Decisions locked:

- **Compatibility first**: match official `actions/runner` container behavior by driving Docker-compatible commands first.
- **Primary backend**: Docker engine (`dockerd`/`containerd`/`runc` or equivalent) visible to the runner.
- **Local mode**: aksh worker can run on the developer host and use the host Docker daemon.
- **Hosted/prod mode**: host orchestrator boots a fresh job VM/microVM containing the aksh worker plus Docker stack; the VM is destroyed after the job.
- **libkrun role**: a VM transport option for local/macOS and possibly Linux; if used for hosted jobs, the libkrun guest image must include a Docker-compatible stack. libkrun does **not** bundle `dockerd`, `containerd`, or `runc`.
- **Docker-less `crun` role**: out of scope for first implementation; keep a seam for future work, but do not make it the initial compatibility path.

## Target Runtime Topologies

### Local/trusted developer mode

```text
macOS/Linux developer host
├── aksh-runner / aksh worker
└── Docker engine on host
    ├── job container, if `container:` exists
    └── service containers, if `services:` exists
```

This is the fastest local implementation path and the easiest conformance baseline. It is appropriate for trusted local repositories. It is not the hosted multi-tenant isolation model.

### Hosted/prod VM-per-job mode

```text
trusted host orchestrator
└── fresh job VM/microVM
    ├── aksh worker/runner
    ├── dockerd
    ├── containerd
    ├── runc or crun as Docker's low-level OCI runtime
    ├── job container, if `container:` exists
    └── service containers, if `services:` exists
```

This matches the GitHub-hosted security shape: each job runs on a fresh machine boundary, and containers are nested inside that job machine. The host orchestrator schedules, boots, streams logs/results, enforces timeouts, and destroys the VM. User code executes inside the VM, not on the trusted host.

### Future Docker-less optimization path, not first implementation

```text
trusted host orchestrator
└── fresh job VM/microVM
    ├── aksh worker
    ├── custom Docker-compatible engine shim
    └── crun/runc directly
        ├── job container
        └── service containers
```

This path avoids `dockerd`, but aksh must implement enough Docker behavior to satisfy the official runner semantics. Keep the source layout open to this, but do not spend first-pass implementation complexity here.

## Reference: official-runner behavior to replicate

Verified from `actions/runner` source paths used by the official runner container implementation:

- `src/Sdk/DTPipelines/Pipelines/AgentJobRequestMessage.cs`
- `src/Runner.Worker/ContainerOperationProvider.cs`
- `src/Runner.Worker/Container/ContainerInfo.cs`
- `src/Runner.Worker/Handlers/ContainerActionHandler.cs`
- `src/Runner.Worker/Handlers/ContainerStepHost.cs`
- `src/Runner.Worker/Container/DockerCommandManager.cs`

When implementing, read these files at the repo's pinned upstream runner version, currently **v2.335.1**, and prefer that tag over any older notes.

### Wire format: control plane → runner

`AgentJobRequestMessage` carries two modern container fields:

- `jobContainer` — TemplateToken: either a mapping (`{image, options, env, ports, volumes, credentials{username,password}}`) or a plain string image name. Omitted entirely when the job has no container.
- `jobServiceContainers` — TemplateToken mapping `<alias> -> {image, options, env, ports, volumes, credentials}`. Omitted when absent.
- TemplateTokens should be passed through un-evaluated by the control plane. Embedded `${{ ... }}` strings remain strings. The runner evaluates them after matrix/needs contexts exist.
- Legacy resource-alias fields (`JobContainer` as resource alias + `Resources.Containers` + `JobSidecarContainers`) are not the first implementation target. Emit only the modern token form unless official goldens prove otherwise.

### Evaluated workflow schema

```yaml
container:              # or bare string == { image: <string> }
  image: string          # required
  credentials:
    username: string
    password: string
  env: map<string,string>
  ports: [string]        # "8080:80", "80", "127.0.0.1:8080:80"
  volumes: [string]      # "src:dst", "dst", "src:dst:ro"
  options: string        # raw Docker create options, shell-split by runner logic
services:
  <alias>: same shape
```

### Runtime sequence to match

The official runner's container path is Docker-centric. aksh should match it, not invent new semantics.

1. Validate container support. Official runner supports container operations on Linux only and refuses when the runner itself is already inside an unsupported containerized environment. For local Docker mode, match official messages. For hosted VM mode, the guest OS is Linux, so container support is available.
2. Register post-job step **"Stop containers"** with `always()` semantics.
3. Clean up stale containers/networks using the runner instance label: `docker ps --all --quiet --no-trunc --filter "label=<6-hex>"` then `docker network prune --force --filter "label=<6-hex>"`.
4. Create a per-job Docker network named `github_network_<uuid-no-dashes>` with `--label <6-hex>`.
5. Pull all container images. For ghcr.io / containers.pkg.github.com with no explicit credentials, use GitHub actor/token fallback credentials. For `docker://` actions, pull appears as a separate timeline step named `"Pull <image>"`.
6. Start the job container first, if present. Override entrypoint to keep it running (`--entrypoint "tail" <image> "-f" "/dev/null"`). Mount Docker socket (`/var/run/docker.sock`) automatically. Container name: `<32-hex-uuid>_<sanitized-image>_<6-hex>`.
7. Start service containers with `--network <job-net>` and `--network-alias <service-name>`. Same naming and label conventions.
8. Publish runtime context:
  - `job.container.id` — full 64-char container ID
  - `job.container.network` — network name
  - `job.services.<name>.id` — full 64-char container ID
  - `job.services.<name>.network`
  - `job.services.<name>.ports` — **empty when job container present** (use DNS alias); mapped host port when no job container
9. Wait for service health checks. Poll Docker health status while `starting` with backoff (2s, 3s, then interval); log `"<alias> service is starting, waiting N seconds before checking again."` / `"<alias> service is healthy."`. Fail the **"Initialize containers"** step if any service becomes unhealthy or never reaches healthy state.
10. Run workflow steps:
  - without `container:`: execute steps directly on the runner machine/VM.
  - with `container:`: execute steps with `docker exec` inside the long-running job container.
11. Run Docker/container actions: `docker run --rm` (NOT `docker exec`), attached to job network if present, with same instance label. Action container name uses shorter format: `<sanitized-image>_<6-hex>`.
12. Stop containers in post-job cleanup. Order: stop+remove job container → per service (print logs with `docker logs --details`, then stop+remove) → remove network.

## Pre-Phase 0 — host-Docker smoke environment

Before implementing the full runner container backend, start with a small **host Docker daemon smoke suite** on this workstation. This is the highest-signal first environment because local mode will use the host daemon, and the official runner's container semantics are Docker-centric.

Do **not** run a large macOS microVM substrate bake-off yet. This Mac is useful for later local-VM ergonomics work, but it cannot prove the hosted Linux/KVM production stack:

- it cannot validate KVM/Firecracker behavior.
- it cannot validate Linux host cgroups/networking.
- it naturally tests arm64 images on Apple Silicon unless emulation is added.
- it should not be used as final hosted-performance evidence.

The implementation invariant is: **make Docker container jobs correct first; decide where Docker lives second**.

```text
Initial local smoke:
  aksh worker on macOS/Linux host
  └── host Docker daemon
      ├── job container, if `container:` exists
      └── service containers, if `services:` exists

Later production shape:
  Linux/KVM host orchestrator
  └── fresh job VM/microVM
      ├── aksh worker
      ├── Docker-compatible engine
      ├── job container, if `container:` exists
      └── service containers, if `services:` exists
```

The VM-per-job path should reuse the same runner/container semantics after the host-Docker backend is correct. A later Linux/KVM bake-off can compare Firecracker, libkrun-on-KVM, or other runtimes using the same workflow fixtures.

### Host-Docker smoke workflow files

Keep the initial five smoke workflows as real fixtures, not only as markdown snippets:

- `fixtures/workflows/20-host-docker-node-services.yml`
- `fixtures/workflows/21-host-docker-build.yml`
- `fixtures/workflows/22-host-docker-container-action.yml`
- `fixtures/workflows/23-host-docker-container-files.yml`
- `fixtures/workflows/24-host-docker-service-ports.yml`

These files are the starting smoke matrix for local host-Docker mode. They should later run unchanged inside the VM-per-job backend once the aksh worker and Docker engine move into the guest.

### What the smoke suite proves

The host-Docker smoke suite should prove:

- `container:` jobs start and steps execute through Docker.
- `services:` containers start before user steps.
- service aliases resolve from a job container.
- health-check options are honored.
- published service ports work from a non-container job.
- `docker build` / `docker run` works on the runner machine.
- `docker://` actions are either supported or explicitly tracked as expected-fail until implemented.
- file-command paths (`GITHUB_ENV`, `GITHUB_OUTPUT`) work from inside a job container.
- cleanup removes containers/networks after the job.

## Pre-Phase 0.5 — microVM substrate &amp; ergonomics comparison

Once the host-Docker baseline works, use this workstation to evaluate the **microVM sandbox ergonomics**. This phase compares `smolvm`, `microsandbox`, and stock `libkrun` to answer a critical architecture question: **should the production backend run the Docker daemon inside the guest VM (Docker-in-VM), or on the host with socket-mapping (Host-Docker-in-VM)?**

### 1. The two VM topologies to test

For each microVM substrate, configure two execution models and run the five smoke workflows (`fixtures/workflows/20-host-docker-*.yml`):

#### Topology A: Docker-in-VM (Guest Docker)

```text
macOS/Linux Host
└── microVM (smolvm / microsandbox / stock libkrun)
    ├── dockerd (running inside guest)
    ├── aksh-runner (running inside guest)
    ├── job container (nested inside guest)
    └── service containers (nested inside guest)
```

- **Mechanism:** The guest VM boots a Linux kernel with bridge networking, netfilter, and overlayfs enabled. An ext4 virtual disk `/dev/vda` is mounted to hold `/var/lib/docker` (avoiding overlay-on-overlay issues). `dockerd` runs inside the guest. The host shares the workspace directory via `virtio-fs`.
- **Pros:** True isolation. No workflow code can talk to the host's Docker socket or see other host containers.
- **Cons:** Higher guest footprint (needs `dockerd`/`containerd` memory). Requires guest kernel support for netfilter/overlayfs.

#### Topology B: Host-Docker-in-VM (TCP Socket Proxying)

```text
macOS/Linux Host
├── dockerd (running on host)
├── socat TCP proxy (127.0.0.1:2375 → /var/run/docker.sock)
└── microVM (smolvm / microsandbox / stock libkrun)
    ├── aksh-runner (running inside guest)
    └── DOCKER_HOST=tcp://127.0.0.1:2375 (routed to host via TSI loopback)
```

- **Mechanism:** The guest VM has no local Docker daemon. A `socat` TCP listener on the host forwards `127.0.0.1:2375` to the host's Docker Unix socket. Under libkrun's Transparent Socket Impersonation (TSI), connecting to `127.0.0.1` inside the guest VM transparently routes to the host's loopback interface. The guest sets `DOCKER_HOST=tcp://127.0.0.1:2375` and runs Docker CLI commands that execute containers on the host.
- **Important:** Unix domain sockets (`/var/run/docker.sock`) **cannot** be bind-mounted across the VM boundary via `virtio-fs`. The socket file appears in the guest filesystem but connection attempts fail because the guest kernel cannot route IPC calls back to the host kernel's socket listener. TCP proxying via `socat` is the proven workaround.
- **Pros:** Lower guest memory footprint. No nested virtualization or complex guest kernel netfilter requirements. Zero-config under TSI (loopback maps directly to host).
- **Cons:** Weaker security boundary (guest VM can control host Docker daemon). Complex workspace path translations (host path vs guest path vs container path). Requires host-side `socat` process per runner.

### 2. Substrate evaluation criteria

Compare the three microVM runtimes using the following metrics:


| Metric                       | smolvm[smol-machines/smolvm: Tool to build &amp; run portable, lightweight, self-contained virtual machines.](https://github.com/smol-machines/smolvm) | microsandbox[superradcompany/microsandbox: 🧱 easy, fast and local-first microVM runtime](https://github.com/superradcompany/microsandbox) | stock libkrun([libkrun/libkrun: A dynamic library providing Virtualization-based process isolation capabilities](https://github.com/libkrun/libkrun) |
| :---------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------ | :---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Cold Boot Latency**        | VM spawn to `docker info` readiness                                                                                                                    | VM spawn to `docker info` readiness                                                                                                        | VM spawn to `docker info` readiness                                                                                                                  |
| **Memory Overhead**          | Host RSS for VM process at idle                                                                                                                        | Host RSS for VM process at idle                                                                                                            | Host RSS for VM process at idle                                                                                                                      |
| **Disk Footprint**           | Size of guest image + runtime overlay                                                                                                                  | Size of guest image + runtime overlay                                                                                                      | Size of guest image + runtime overlay                                                                                                                |
| **DX / Control API**         | Ease of Rust SDK integration                                                                                                                           | Ease of Rust SDK integration                                                                                                               | Ease of direct Rust binding usage                                                                                                                    |
| **File Sharing (virtio-fs)** | Latency/conformance for workspace                                                                                                                      | Latency/conformance for workspace                                                                                                          | Latency/conformance for workspace                                                                                                                    |
| **Network Isolation**        | Bridge/DNS alias behavior in guest                                                                                                                     | Bridge/DNS alias behavior in guest                                                                                                         | Bridge/DNS alias behavior in guest                                                                                                                   |
| **Cleanup Reliability**      | No orphaned VM processes or containers                                                                                                                 | No orphaned VM processes or containers                                                                                                     | No orphaned VM processes or containers                                                                                                               |


### 3. Substrate-specific implementation tasks

- **smolvm:**
  - Build on `examples/docker-in-vm/docker.smolfile`.
  - Test Topology A using an Alpine/Debian image with Docker pre-installed.
  - Test Topology B by passing `/var/run/docker.sock` as a volume mount.
- **microsandbox:**
  - Investigate if microsandbox allows running `dockerd` inside its OCI VM container, or if it is strictly limited to single-image execution.
  - Check if its Rust SDK supports passing custom mounts (like the Docker socket) for Topology B.
- **stock libkrun:**
  - Build a minimal Rust harness using `libkrun` crate bindings.
  - Construct a custom guest rootfs (ext4) containing `dockerd` and the runner.
  - Configure `virtio-fs` mounts and test VSOCK-based log/control streaming.

### 4. Selection criteria for local mode

Use these results to decide:

1. If **Topology A** is under 1.5s cold boot and runs workflows 1, 2, and 5 successfully, it is selected as the default localmodel for its superior security.
2. If **Topology A** fails due to kernel module limitations (e.g., bridge/netfilter lacking in libkrunfw) or has excessive latency (&gt;5s), fallback to **Topology B** or host-Docker local mode.
3. Select the substrate (`smolvm`, `microsandbox`, or `libkrun`) that provides the most stable Rust API, fastest cleanup, and least operational overhead.

## Phase 0 — record official goldens

Before declaring full compatibility, record official runner goldens for the container scenarios. This gates conformance, not compilation.

Add or use fixtures:

- `fixtures/workflows/16-container-job.yml`
- `fixtures/workflows/17-service-container.yml`

Run them against official `actions/runner` v2.335.1 on Linux with Docker and store recordings under `.runner-watch/golden/v2.335.1/`. These goldens are the authority for exact JSON encoding, timeline step numbering, log-group strings, health-check output, and service context shape.

### Completed golden recordings

Seven container/service workflows were run against the official GitHub Actions runner v2.335.1 on `ubuntu-latest` and recorded under `.runner-watch/golden/v2.335.1/`:

| Scenario | What it tests |
| :--- | :--- |
| `30-container-job-basic` | Basic `container: node:20` job, Docker env verification |
| `31-container-with-services` | Container + postgres/redis services, DNS alias resolution, health checks |
| `32-services-no-container` | Service containers (nginx+redis) with port mapping, no job container |
| `33-container-env-options` | Container env vars, `--cpus` option, `GITHUB_ENV`/`GITHUB_OUTPUT`/`GITHUB_PATH` file commands from inside container |
| `34-container-with-checkout` | Container job + `actions/checkout@v4`, workspace mount verification |
| `35-container-lifecycle` | `job.container` context fields, `continue-on-error`, conditional steps |
| `36-docker-action` | `docker://` actions on host and inside container job (2 jobs) |

Each golden directory contains: `run.json`, `jobs.json`, `timing.json`, full job logs, workflow YAML, and `summary.json`. Scenario definitions added to `experiments/mitm/scenarios/`.

### Observed behavior from golden traces

The following details were verified from golden trace logs and were not previously documented. They supplement the "Runtime sequence to match" and "Required behavior" sections below.

#### Container naming convention

Job containers: `<32-hex-job-uuid>_<sanitized-image>_<6-hex>` (e.g., `bc0b1a2164fe484f88c0d6da518dc2a0_node20bookworm_623c4d`). Image name is sanitized: colons, dots, and dashes are removed. Service containers follow the same pattern.

Docker action containers (from `docker://` steps) use a shorter form: `<sanitized-image>_<6-hex>` (no UUID prefix).

#### Instance label format

A 6-character hex label (e.g., `607ed7`, `c5b131`) is assigned per job. All containers and the job network are tagged with `--label <hex>`. Cleanup uses `--filter "label=<hex>"`. The label appears to be derived from the runner session or job ID.

#### Docker socket auto-mount

The host Docker socket is automatically mounted into job containers: `-v "/var/run/docker.sock":"/var/run/docker.sock"`. This is NOT done for service containers or for service-only jobs (no job container). This enables Docker-in-Docker workflows from inside job containers.

#### Stale container cleanup at job start

Before creating the job network, the runner runs:
1. `docker ps --all --quiet --no-trunc --filter "label=<hex>"` — find stale containers from a previous run with the same label.
2. `docker network prune --force --filter "label=<hex>"` — remove stale networks.

#### docker:// action execution model

- **On host (no job container):** `docker run --rm` with `--workdir /github/workspace`. Environment variables are passed via `-e "VAR_NAME"` (key only, not `KEY=VALUE` — Docker resolves from host env). Image pull appears as a separate timeline step: `"Pull <image>"`.
- **Inside a container job:** `docker://` actions use `docker run` (NOT `docker exec`), attached to the same job network with the same instance label.

#### Step number reservation

GitHub reserves step number slots between user steps and post-job steps. The gap pattern observed: 5 user steps → post-job at step 14; 8 user steps → post-job at step 16; 10 user steps → post-job at step 24. Post-job steps ("Stop containers", "Complete job") use the reserved high numbers.

#### Health check backoff timing

Health check polling uses sequential waits: 2s, then 3s, then continues at the configured `--health-interval`. Log messages follow the pattern:
- `"<alias> service is starting, waiting N seconds before checking again."`
- `"<alias> service is healthy."`

#### Service port context behavior

- **With job container** (services on same Docker network): `job.services.<name>.ports[<port>]` is **empty**. Services are accessed via DNS alias at their container port directly.
- **Without job container** (host mode with `-p` port mapping): `job.services.<name>.ports['<container-port>']` resolves to the **mapped host port** (e.g., `job.services.nginx.ports['80']` = `8080`).

#### Toolcache mount

The golden logs show `/opt/hostedtoolcache` → `/__t` as an additional mount. This supplements the `_work/_tool` → `/__w/_tool` mount in the plan's table.

#### Container environment variable ordering

In `docker create`, user env vars from `container.env` appear BEFORE auto-injected vars (`HOME=/github/home`, `GITHUB_ACTIONS=true`, `CI=true`). User env vars use `-e "KEY=VALUE"` quoting; auto-injected vars for `GITHUB_ACTIONS` and `CI` omit the `=value` form.

#### Post-job cleanup order

1. Stop and remove job container: `docker rm --force <id>`.
2. For each service container:
   a. Print logs: `docker logs --details <id>`.
   b. Stop and remove: `docker rm --force <id>`.
3. Remove network: `docker network rm <network-name>`.

Log group strings: `"Stop and remove container: <name>"`, `"Print service container logs: <name>"`, `"Remove container network: <network-name>"`.

## Phase 1 — control plane emits container fields

### `crates/aksh-gha-parser/src/lib.rs`

Add raw YAML fields to `Job`:

```rust
/// Job container (`container:`) — raw template value, evaluated runner-side.
#[serde(default)]
pub container: Option<Value>,

/// Service containers (`services:`) — raw template values, evaluated runner-side.
#[serde(default)]
pub services: Option<Value>,
```

Copy these through `expand_jobs` / `expand_jobs_with_reusables` into each expanded `JobPlan`. Matrix-expanded jobs keep the raw expression strings; runner-side expression evaluation resolves `${{ matrix.* }}`.

### `crates/aksh-gha-protocol/src/lib.rs`

Add to `JobPlan`:

```rust
/// Raw `container:` value, string or mapping, un-evaluated.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub container: Option<serde_json::Value>,

/// Raw `services:` mapping, un-evaluated.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub services: Option<serde_json::Value>,
```

### `crates/aksh-gha-protocol/src/azdo.rs`

Add to `AgentJobRequestMessage`:

```rust
/// Job container spec (`container:`) — TemplateToken-compatible JSON.
#[serde(rename = "jobContainer", default, skip_serializing_if = "Option::is_none")]
pub job_container: Option<serde_json::Value>,

/// Service container specs (`services:`) — alias → spec mapping.
#[serde(rename = "jobServiceContainers", default, skip_serializing_if = "Option::is_none")]
pub job_service_containers: Option<serde_json::Value>,
```

Fix every `AgentJobRequestMessage { ... }` construction site. `build_agent_job_message` is the load-bearing one; test constructors can set both new fields to `None`.

### `crates/aksh-gha-parser/src/job_builder.rs`

In `build_agent_job_message`, pass:

```rust
job_container: plan.container.clone(),
job_service_containers: non_empty_services(plan.services.clone()),
```

`non_empty_services` should omit empty `{}` to match `EmitDefaultValue=false` behavior.

### Server behavior

No route changes should be needed. `QueuedJob.message` carries the new fields through acquire-job responses automatically. Add an inline server test asserting:

- workflow with `container:` produces acquirejob JSON containing `jobContainer`.
- workflow without `container:` omits `jobContainer` entirely.
- workflow with `services:` produces `jobServiceContainers`.
- workflow with empty `services: {}` omits `jobServiceContainers`.

## Phase 2 — runner Docker engine support

All first-pass execution work lives in `crates/aksh-runner`.

Create `src/worker/container/` and move/replace the current dead `container_ops.rs` helpers there. Keep the implementation Docker-first. Do not implement a Docker-less `crun` engine in this phase.

### Core types

```rust
pub struct ContainerInfo {
    pub is_job_container: bool,
    pub image: String,
    pub display_name: String,
    pub network_alias: Option<String>,
    pub options: String,
    pub env: HashMap<String, String>,
    pub user_ports: Vec<String>,
    pub user_volumes: Vec<String>,
    pub registry_server: String,
    pub registry_username: Option<String>,
    pub registry_password: Option<String>,
    pub container_id: String,
    pub network: String,
    pub runtime_path: Option<String>,
    pub port_mappings: Vec<(String, String)>,
    pub failed_initialization: bool,
}

pub enum ContainerEngine {
    Docker(docker::DockerEngine),
}
```

Keep `ContainerEngine` as an enum so a future `DockerInVm`, `RemoteDocker`, or `DockerlessCruntime` variant can be added without contaminating the workflow semantics.

### Container evaluation

Add:

```rust
pub fn evaluate_containers(
    msg: &serde_json::Value,
    job: &JobContext,
) -> Result<(Option<ContainerInfo>, Vec<ContainerInfo>)>
```

Responsibilities:

- read `msg["jobContainer"]` and `msg["jobServiceContainers"]`.
- evaluate every string leaf using the existing runner expression engine and `job.to_expression_context()`.
- bare string container becomes `{ image: <string> }`.
- missing/empty image fails with `Container image cannot be empty`.
- parse registry server for auth fallback.
- preserve `options`, `ports`, `volumes`, `env`, and `credentials`.
- decode typed TemplateToken form too if official goldens require it.

### DockerEngine

`worker/container/docker.rs` should drive the Docker CLI through the existing `process::invoke` path. Port behavior from official runner files rather than inventing a reduced implementation.

Required Docker commands/features:

- `docker version`
- `docker network create`
- `docker network rm`
- `docker network prune`
- `docker login --config <tempdir>`
- `docker pull`
- `docker create`
- `docker start`
- `docker ps --filter status=running`
- `docker inspect`
- `docker port`
- `docker logs`
- `docker exec`
- `docker rm --force`

Required behavior (see also "Observed behavior from golden traces" in Phase 0):

- instance labels for cleanup: 6-hex label per job, applied via `--label <hex>` to all containers and networks.
- stale container/network cleanup before network creation using label filter.
- image pull retry behavior.
- ghcr credential fallback.
- log group strings matching official runner (see Phase 0 cleanup order for exact strings).
- job network naming: `github_network_<uuid-no-dashes>`.
- container naming: `<32-hex-uuid>_<sanitized-image>_<6-hex>` (image name: colons/dots/dashes removed).
- service network aliases via `--network-alias <service-name>`.
- job container keepalive entrypoint: `--entrypoint "tail" <image> "-f" "/dev/null"`.
- Docker socket auto-mount into job containers: `-v "/var/run/docker.sock":"/var/run/docker.sock"`.
- official mount table:
  - `_work` → `/__w`
  - `externals` → `/__e` read-only
  - `_work/_temp` → `/__w/_temp`
  - `_work/_actions` → `/__w/_actions`
  - `/opt/hostedtoolcache` → `/__t`
  - `_work/_temp/_github_home` → `/github/home`
  - `_work/_temp/_github_workflow` → `/github/workflow`
- env var injection: user `container.env` vars first, then `HOME=/github/home`, `GITHUB_ACTIONS=true`, `CI=true`.
- path translation for step scripts and file-command paths.
- PATH extraction from image config and prepending on exec.
- health check polling with 2s/3s/interval backoff (see Phase 0 golden observations).
- service port context: empty when job container present (DNS alias access); mapped host port when no job container.
- cleanup order: stop+remove job container → (per service: print logs → stop+remove) → remove network.
- docker:// action execution: `docker run --rm` (not `docker exec`), on job network if present, shorter container name format.

### Job lifecycle integration

In `worker/job_runner.rs::run_job`:

1. build normal `JobContext`.
2. inject GitHub env.
3. evaluate container specs.
4. construct `ContainerEngine::Docker` if any container/service exists.
5. pass container lifecycle into `run_steps`.

In `worker/steps_runner.rs::run_steps`:

- keep **"Set up job"** as the first synthetic step.
- insert **"Initialize containers"** after setup when containers/services exist.
- shift user step numbering accordingly.
- insert **"Stop containers"** before **"Complete job"** and run it under always semantics.
- if initialize fails, mark job failure, skip user steps, still cleanup.

### Step execution inside job container

When `container:` is active:

- script steps write their temp script under `_work/_temp`.
- translate script path and workdir to container paths.
- execute through `docker exec`.
- translate path-valued env vars:
  - `GITHUB_WORKSPACE`
  - `GITHUB_ENV`
  - `GITHUB_PATH`
  - `GITHUB_OUTPUT`
  - `GITHUB_STATE`
  - `GITHUB_STEP_SUMMARY`
  - `RUNNER_TEMP`
  - `RUNNER_TOOL_CACHE`
  - `HOME`
- file-command files remain host-readable through mounted `_work/_temp`.
- Node actions execute with `/__e/node24/bin/node` inside the container.
- Docker/container actions attach to the same job network.

### Engine selection

Add a CLI/env option but keep only Docker implemented initially:

```text
--container-engine docker
AKSH_CONTAINER_ENGINE=docker
```

Future values can be added later. Do not expose `crun` or `microvm` values until they work.

## Phase 3 — VM-per-job with Docker inside the VM

This phase moves the already-compatible Docker runner into an ephemeral job VM. It should not change workflow behavior.

### Host orchestrator responsibility

```text
host orchestrator
└── boot fresh job VM
    └── run aksh worker inside VM
```

Host responsibilities:

- receive or observe queued job.
- provision VM/microVM.
- inject one-job registration/session material.
- stream logs/results or allow worker to report directly.
- enforce wall-clock timeout.
- destroy VM after completion.
- manage VM image updates and cache mounts.

The host must not run user code for hosted/prod CI.

### Guest image requirements

The job VM image must contain:

- aksh worker/runner binary.
- Docker CLI.
- `dockerd`.
- `containerd`.
- `runc` or Docker-compatible OCI runtime.
- cgroup v2 setup.
- overlayfs support.
- bridge/veth networking tools.
- nftables or iptables compatibility.
- CA certificates.
- Git.
- shell/coreutils.
- Node externals required for JavaScript actions.
- enough writable disk for `/var/lib/docker` and workspace.

For libkrun, this means building a real Docker-capable Linux guest rootfs/disk. libkrun does not provide this automatically.

### libkrun-specific considerations

`docs/runner/microvm-isolation-research.md` notes that `libkrunfw` includes many Docker-needed kernel primitives: overlayfs, loop devices, bridge networking, veth pairs, nftables, conntrack, cgroup v2, and namespaces. That makes Docker-in-guest plausible. The remaining risks are operational:

- no kernel modules; Docker or helper tools must not rely on `modprobe` for unavailable drivers.
- legacy iptables may be absent; prefer nftables-compatible Docker versions.
- init/service supervision must reliably start `dockerd` before the runner begins the job.
- storage sizing for `/var/lib/docker` must be explicit.
- macOS libkrun security is hypervisor-only; Linux can add namespace/cgroup/seccomp jailing around the VMM process.

### Firecracker-specific considerations

Firecracker remains the strongest hosted Linux isolation choice when macOS support is not required:

- full custom kernel/rootfs possible.
- jailer provides process isolation around the VMM.
- common production CI pattern: official runner image + Docker daemon inside the VM.
- no virtio-fs; workspace/cache transfer needs block devices or vsock agent.

## Phase 4 — workflows, conformance, docs

### Fixtures

Add `fixtures/workflows/16-container-job.yml`:

```yaml
name: container-job
on: push
jobs:
  in-container:
    runs-on: ubuntu-latest
    container:
      image: node:24-bookworm
      env: { MARKER: from-container-env }
      volumes: [ "my_vol:/volume_mount" ]
      options: --cpus 1
    steps:
      - run: echo "home=$HOME workspace=$GITHUB_WORKSPACE"
      - run: test "$MARKER" = from-container-env
      - run: node --version
      - run: echo "container-id=${{ job.container.id }} network=${{ job.container.network }}"
```

Add `fixtures/workflows/17-service-container.yml`:

```yaml
name: service-container
on: push
jobs:
  services-in-container:
    runs-on: ubuntu-latest
    container: postgres:16
    services:
      db:
        image: postgres:16
        env: { POSTGRES_PASSWORD: ci }
        options: >-
          --health-cmd "pg_isready -U postgres" --health-interval 5s
          --health-timeout 5s --health-retries 10
    steps:
      - run: pg_isready -h db -p 5432 -U postgres
      - run: echo "db-id=${{ job.services.db.id }}"
  services-on-host:
    runs-on: ubuntu-latest
    services:
      web:
        image: nginx:alpine
        ports: [ "8080:80" ]
    steps:
      - run: curl -fsS http://localhost:${{ job.services.web.ports['80'] }} | head -1
```

Add `.github/workflows/container-dogfood.yml` with jobs for:

- bare string job container.
- mapping job container.
- service container from host job.
- job container + service container.
- matrix-selected container image.

### Conformance

Extend `aksh-conformance` scenarios with:

- `16-container-job`
- `17-service-container`

Mark them as requiring Docker until a future Docker-less backend exists.

### Docs

Create or update `docs/runner/07-containers.md` with:

- Docker-first architecture.
- local host-Docker mode.
- hosted VM-with-Docker mode.
- libkrun guest image requirements.
- Firecracker comparison.
- explicit note that `crun` alone is not Docker-compatible.
- future Docker-less engine as an optimization, not the first implementation.

Update `docs/runner/roadmap.md` and `docs/runner/runner_fidelity_gap.md` F026/M7 notes to say the compatibility target is Docker engine behavior, with VM-per-job placement handled separately.

## Critical implementation files

- `crates/aksh-gha-parser/src/lib.rs` — parse `container:` / `services:` on `Job`, copy through expansion.
- `crates/aksh-gha-parser/src/job_builder.rs` — set `jobContainer` / `jobServiceContainers` on `AgentJobRequestMessage`.
- `crates/aksh-gha-protocol/src/lib.rs` — `JobPlan` fields.
- `crates/aksh-gha-protocol/src/azdo.rs` — wire fields.
- `crates/aksh-runner/src/worker/job_runner.rs` — evaluate containers and construct lifecycle.
- `crates/aksh-runner/src/worker/steps_runner.rs` — synthetic Initialize/Stop container steps.
- `crates/aksh-runner/src/worker/handlers/script.rs` — `docker exec` path for script steps.
- `crates/aksh-runner/src/worker/handlers/node.rs` — node action execution inside job container.
- `crates/aksh-runner/src/worker/handlers/container.rs` — Docker/container action behavior.
- New: `crates/aksh-runner/src/worker/container/mod.rs`.
- New: `crates/aksh-runner/src/worker/container/docker.rs`.

## Verification

Standard Rust gate after meaningful changes:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets
cargo test --workspace --quiet
```

Phase-specific checks:

1. **Protocol tests**
  - `job_container: None` omits `jobContainer`.
  - `Some(json!("node:24"))` serializes as plain string.
  - mapping form round-trips.
  - `services: {}` is omitted.
2. **Parser tests**
  - bare string `container: alpine:3.20` is preserved.
  - mapping `container:` is preserved.
  - `services:` mapping is preserved.
  - matrix expression `container: ${{ matrix.image }}` remains unevaluated.
3. **Server acquirejob test**
  - workflow with container emits `jobContainer`.
  - workflow without container omits it.
  - official runner can consume the emitted message.
4. **Docker behavior test on Linux**
  - start aksh server.
  - run aksh native runner with Docker available.
  - submit `fixtures/workflows/17-service-container.yml`.
  - assert service health passes and user steps can reach services.
5. **Official-runner cross-check**
  - same control plane, official `actions/runner` v2.335.1 as executor.
  - same fixture workflows pass.
  - proves control-plane payloads match GitHub-compatible runner expectations.
6. **VM-per-job Docker check**
  - boot a fresh job VM image containing aksh worker + Docker stack.
  - run the same fixture workflows inside the VM.
  - destroy VM after job.
  - compare logs/timeline/contexts with host-Docker mode.

## Open risks

- Exact TemplateToken wire encoding must be confirmed against official goldens.
- Docker option parsing should be ported from official runner behavior, not approximated.
- Docker-in-libkrun guest image must be proven with a real `dockerd` smoke test.
- libkrun's bundled kernel has no modules; Docker image/storage/network paths must avoid unavailable module loads.
- macOS hosted isolation with libkrun is weaker than Firecracker-on-Linux because macOS lacks Linux namespace/cgroup jailing around the VMM process.
- Docker-less `crun` backend remains a significant future project, not a prerequisite for container support.

