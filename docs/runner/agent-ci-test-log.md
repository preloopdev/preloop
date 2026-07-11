# agent-ci Test Log

Tested 7 fixture workflows using `npx @redwoodjs/agent-ci` v0.x (TypeScript path).
Docker backend: OrbStack. Runner image: `ghcr.io/actions/actions-runner:latest` (official runner v2.335.1).

## Results

| # | Workflow | Status | Duration | Notes |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `20-host-docker-node-services.yml` | ✅ PASS | 16.7s | Node + Postgres + Redis services all healthy, steps passed |
| 2 | `21-host-docker-build.yml` | ✅ PASS | 6.1s | `docker build` and `docker run` executed inside runner container |
| 3 | `22-host-docker-container-action.yml` | ❌ FAIL | 6.2s | `docker://` action resolution failed at "Set up job" |
| 4 | `23-host-docker-container-files.yml` | ❌ FAIL | 5.1s | `container:` job — runc create failed (OCI runtime error) |
| 5 | `24-host-docker-service-ports.yml` | ❌ FAIL | 41.4s | `wget` not found in runner image (not ubuntu-latest) |
| 6 | `25-agent-ci-test.yml` | ✅ PASS | 6.5s | Basic echo + env + math |
| 7 | `26-agent-ci-comprehensive.yml` | ✅ PASS | 16.1s | 3 jobs, env scoping, `needs:` chain |

**Summary: 4/7 passed, 3 failed**

## Failure Analysis

### `22-host-docker-container-action.yml` — `docker://` action not supported locally

The workflow uses `uses: docker://alpine:3.20` which is a Docker container action. The runner tries to resolve this via `Getting action download info` and fails because agent-ci's local API doesn't serve the action download endpoint for `docker://` image references. This is a known limitation — `docker://` actions require the runner to pull the image and create a container, but the local control plane can't provide the download info metadata the runner expects.

### `23-host-docker-container-files.yml` — `container:` job fails with OCI runtime error

The workflow uses `container: { image: alpine:3.20, options: --cpus 1 }`. agent-ci tried to start the job inside that container but hit:
```
failed to create task for container: failed to create shim task: 
OCI runtime create failed: runc create failed
```
This is a Docker-in-Docker nesting issue. The runner itself runs inside a container (`ghcr.io/actions/actions-runner`), and trying to run a nested `container:` job requires Docker socket access or privileged mode, which the default agent-ci container configuration doesn't grant.

### `24-host-docker-service-ports.yml` — `wget` missing from runner image

The workflow's step uses `wget` to verify port connectivity. agent-ci's runner image (`ghcr.io/actions/actions-runner:latest`) is a minimal container — it does NOT include the full `ubuntu-latest` toolset that GitHub's hosted runners ship. `wget` is not installed. agent-ci helpfully suggests creating a `.github/agent-ci.Dockerfile` to install missing tools.

**This is a workflow bug, not an agent-ci bug.** The workflow should use `curl` (which is available) instead of `wget`, or install `wget` first.

## Key Observations

1. **agent-ci successfully runs the official `actions/runner` v2.335.1 binary** inside a Docker container and feeds it jobs via its local API emulation. This is architecturally identical to what aksh does.
2. **Service containers work.** Workflow 20 (Node + Postgres + Redis) passed — agent-ci correctly wired up the service containers with health checks and network aliases.
3. **`docker build`/`docker run` work from inside the runner container** (workflow 21 passed), meaning Docker-in-Docker or socket mounting is configured for basic Docker CLI operations.
4. **`container:` job mode and `docker://` actions are not yet supported** — these require deeper control-plane emulation of the `JobContainer` and `JobServiceContainers` fields in the `AgentJobRequestMessage`, which is exactly the feature we are building in aksh.
5. **The runner image is minimal, not ubuntu-latest.** Workflows that depend on tools like `wget`, `gcc`, `python`, etc. need a custom Dockerfile.
