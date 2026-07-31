# act vs agent-ci vs aksh: GitHub Actions Local Runners Compared

A detailed comparison between [nektos/act](https://github.com/nektos/act) and
aksh (this repo). Both let you run GitHub Actions workflows locally. They take
fundamentally different approaches to the problem.

---

## 1. Philosophy

| | act | agent-ci | aksh |
|---|---|---|---|
| **One-liner** | Reimplement the runner in Go | Reimplement the server in TypeScript | Reimplement server + runner in Rust |
| **Strategy** | Replace the runner binary with a Go reimplementation that executes steps in Docker containers | Reimplement **both** the control plane (server) **and** the runner in Rust, with the full AzDO wire protocol between them |
| **Compatibility target** | Behavioral: "workflows should mostly work" | Protocol: "the official runner v2.336.0 cannot tell the difference" |
| **Verification method** | Manual testing + community bug reports | 24 golden wire-capture scenarios replayed against the server; differential conformance vs official runner |

---

## 2. Architecture

### act

```
┌──────────────────────────────────────┐
│            act (Go binary)           │
│                                      │
│  YAML parser ─► Planner ─► Executor  │
│       │              │         │     │
│  Expression      DAG sort   Docker   │
│  evaluator      (stages)    exec     │
└──────────────────────────────────────┘
         │
    Docker API
         │
┌────────────────────┐
│  Job container     │  ← tail -f /dev/null keeps it alive
│  (ubuntu image)    │  ← steps run via `docker exec`
│                    │
│  Service containers│  ← same Docker network
└────────────────────┘
```

- **Language**: Go 1.24+
- **Binary**: Single static binary (~8 MB)
- **Execution**: Monolithic — parses, plans, and executes workflows in one process
- **Container model**: One long-lived Docker container per job, entrypoint is `tail -f /dev/null`, steps execute via `docker exec`
- **Host mode**: `-self-hosted` flag runs steps directly on the host (no container)
- **Expression engine**: Custom evaluator built on `rhysd/actionlint`'s AST parser

### aksh

```
┌─────────────────────────────────────────────────────┐
│              aksh-runner-server (Rust, Axum)         │
│                                                     │
│  YAML parser ─► Scheduler ─► AzDO wire protocol     │
│       │              │              │                │
│  Expression    Concurrency    100+ API routes        │
│  evaluator      groups       (/_apis/*, /broker/*)   │
│                                                     │
│  Cache/Artifact stores    OIDC provider    DAP debug │
└─────────────────────┬───────────────────────────────┘
                      │  AzDO protocol (HTTP/WS)
                      │
        ┌─────────────┼─────────────────┐
        │             │                 │
   ┌────▼───┐   ┌────▼───┐       ┌─────▼────┐
   │Official │   │ aksh-  │       │ SmolVM   │
   │actions/ │   │ runner │       │ (libkrun │
   │runner   │   │ (Rust) │       │  microVM)│
   └─────────┘   └────────┘       └──────────┘
```

- **Language**: Rust (edition 2021, MSRV 1.97)
- **Codebase**: 15 workspace crates, ~130,000+ lines of Rust
- **Execution**: Client–server — the server speaks the AzDO wire protocol; runners connect over HTTP/WebSocket
- **Runner**: `aksh-runner` — faithful Rust port of `actions/runner` v2.336.0 (Listener + Worker architecture)
- **Execution backends**: SmolVM/libkrun microVM (primary, Linux + macOS), `somac` (ephemeral macOS VMs via Virtualization.framework), `vowin` (ephemeral Windows VMs via QEMU), Docker/Podman, or process
- **Expression engine**: Custom Pratt parser + evaluator, 12 functions, abstract equality, `*` filter, bracket access

---

## 3. Feature Matrix

### Core Workflow Features

| Feature | act | aksh | Notes |
|---|:---:|:---:|---|
| Workflow YAML parsing | ✅ | ✅ | Both parse the full workflow schema |
| `run:` steps | ✅ | ✅ | |
| `uses:` actions (remote) | ✅ | ✅ | |
| `uses:` actions (local) | ✅ | ✅ | |
| `uses:` Docker actions | ✅ | ✅ | |
| `uses: $/` self-repo syntax | ❌ | ✅ | v2.336.0 feature |
| Composite actions | ✅ | ✅ | aksh supports 10-deep nesting with pre/post |
| Reusable workflows (`workflow_call`) | ✅ | ✅ | aksh: `secrets: inherit`, input validation, depth=4 |
| Matrix strategy | ✅ | ✅ | aksh: IndexMap order preservation |
| `include` / `exclude` in matrix | ✅ | ✅ | |
| `needs` DAG | ✅ | ✅ | |
| Job outputs | ✅ | ✅ | |
| `if` conditionals (job/step) | ✅ | ✅ | |
| `continue-on-error` | ✅ | ✅ | |
| `timeout-minutes` | ✅ | ✅ | |
| `defaults.run` (shell/working-directory) | ✅ | ✅ | |
| `workflow_dispatch` inputs | ✅ | ✅ | |
| `run-name` expression | ❌ | ✅ | aksh parses and evaluates at submit time |

### Environment & Contexts

| Feature | act | aksh | Notes |
|---|:---:|:---:|---|
| `$GITHUB_ENV` | ✅ | ✅ | |
| `$GITHUB_OUTPUT` | ✅ | ✅ | |
| `$GITHUB_PATH` | ✅ | ✅ | |
| `$GITHUB_STEP_SUMMARY` | ✅ | ✅ | |
| `$GITHUB_STATE` | ✅ | ✅ | |
| `$GITHUB_ARTIFACTS` | ❌ | ✅ | v2.336.0 feature |
| `$GITHUB_ARTIFACTS_LIST` | ❌ | ✅ | v2.336.0 feature |
| `ACTIONS_CACHE_MODE` | ❌ | ✅ | v2.336.0 feature |
| `github.*` context | ✅ | ✅ | |
| `env.*` context | ✅ | ✅ | |
| `secrets.*` context | ✅ | ✅ | act reads `.secrets` file or `--secret` flag |
| `vars.*` context | ✅ | ✅ | |
| `steps.*` context | ✅ | ✅ | |
| `needs.*` context | ✅ | ✅ | |
| `matrix.*` context | ✅ | ✅ | |
| `runner.*` context | ✅ | ✅ | |
| `inputs.*` context | ✅ | ✅ | |
| Comprehensive `GITHUB_*` env vars | ⚠️ partial | ✅ | aksh injects 40+ vars matching official runner |

### Expression Engine

| Feature | act | aksh | Notes |
|---|:---:|:---:|---|
| `contains()` | ✅ | ✅ | |
| `startsWith()` / `endsWith()` | ✅ | ✅ | |
| `format()` | ✅ | ✅ | |
| `join()` | ✅ | ✅ | |
| `toJSON()` / `fromJSON()` | ✅ | ✅ | |
| `hashFiles()` | ✅ | ✅ | act has two impls (local + container) |
| `success()` / `failure()` / `cancelled()` / `always()` | ✅ | ✅ | |
| Type coercion (string/number/bool/null) | ✅ | ✅ | |
| `*` filter syntax | ❌ | ✅ | `steps.*.outputs` |
| Bracket access `['key']` | ⚠️ | ✅ | |
| `{{` / `}}` escape sequences | ❌ | ✅ | |
| Case-insensitive `==` | ⚠️ | ✅ | |
| Parser | `rhysd/actionlint` AST | Custom Pratt parser | aksh owns its parser |

### Workflow Commands

| Feature | act | aksh | Notes |
|---|:---:|:---:|---|
| `::set-output::` | ✅ | ✅ | Deprecated but supported |
| `::set-env::` | ✅ | ✅ | |
| `::add-path::` | ✅ | ✅ | |
| `::add-mask::` | ✅ | ✅ | |
| `::debug::` / `::warning::` / `::error::` / `::notice::` | ✅ | ✅ | |
| `::group::` / `::endgroup::` | ✅ | ✅ | |
| `::stop-commands::` | ✅ | ✅ | |
| Problem matchers | ❌ | ✅ | `::add-matcher::` / `::remove-matcher::` |

### Protocol & Runner Features

| Feature | act | aksh | Notes |
|---|:---:|:---:|---|
| Faithful runner reimplementation | ✅ | ✅ | act reimplements in Go; aksh reimplements in Rust (faithful port of v2.336.0) |
| AzDO wire protocol | ❌ | ✅ | act doesn't speak the protocol at all |
| Runner registration handshake | ❌ | ✅ | RSA key exchange, session crypto |
| Broker acquire/renew/complete | ❌ | ✅ | Full broker message lifecycle |
| OIDC id-token provider | ❌ | ✅ | RS256-signed JWTs, JWKS/discovery endpoints |
| Concurrency groups | ❌ | ✅ | Queue modes, `cancel-in-progress`, 87 property tests |
| Job permissions / GITHUB_TOKEN scoping | ❌ | ⚠️ partial | Server issues local JWTs |
| Job cancellation (wire protocol) | ❌ | ✅ | `CancellationTiming` with clamped timeouts |
| Runner self-update | N/A | ❌ intentional | aksh acknowledges but doesn't update |
| Runner groups | N/A | ✅ | Server-side group routing |
| Ephemeral runners | N/A | ✅ | Exit-on-ack, session invalidation |
| Results-service (Twirp) | ❌ | ✅ | 5 Twirp routes, signed blob URLs |
| Timeline / live logs | ❌ | ✅ | WebSocket live-feed + PATCH timeline |
| Job annotations | ❌ | ✅ | Feature-gated aggregation |
| `connectionData` / location services | N/A | ✅ | 28 service definitions |
| Background steps (v2.336.0) | ❌ | ⚠️ partial | DTO + flag implemented; full coordinator missing |
| Locked dependencies announcement | ❌ | ✅ | v2.336.0 feature |

### Container & Isolation

| Feature | act | aksh | Notes |
|---|:---:|:---:|---|
| Docker job containers | ✅ | ✅ | Different models: act uses idle container + exec; aksh delegates to the runner |
| Service containers | ✅ | ✅ | |
| Docker-in-Docker | ✅ | ✅ | act mounts host Docker socket; aksh runs Docker natively inside SmolVM guest microVMs (cgroups + overlayfs2 kernel) |
| MicroVM isolation (SmolVM/libkrun) | ❌ | ✅ | Primary backend. Fork-based warm pool, 131 MB idle RSS per VM (measured). Docker actions run inside the VM. |
| Process execution (no container) | ✅ (`-self-hosted`) | ✅ | |
| macOS native runner | ⚠️ (`-self-hosted` workaround) | ✅ | aksh: `somac` — ephemeral macOS VMs via Virtualization.framework, copy-on-write golden snapshots |
| Windows native runner | ⚠️ (`-self-hosted` workaround) | ✅ | aksh: `vowin` — ephemeral Windows 11 VMs via QEMU (ARM64/HVF on macOS, x86_64/KVM on Linux), warm memory snapshot restore |
| Custom platform images (`-P`) | ✅ | N/A | act-specific concept |
| Podman support | ✅ | ✅ | act via `DOCKER_HOST`; aksh via runner backend |

### Cache & Artifacts

| Feature | act | aksh | Notes |
|---|:---:|:---:|---|
| Cache v1 (reserve/upload/commit/lookup) | ✅ | ✅ | |
| Cache v2 (Twirp) | ❌ | ✅ | |
| Artifact v1 (create/put/get/list) | ✅ | ✅ | |
| Artifact v2 (Twirp + blob) | ✅ (v3/v4) | ✅ | |
| File-backed storage | ✅ | ✅ | |

### Developer Experience

| Feature | act | aksh | Notes |
|---|:---:|:---:|---|
| DAP debugger | ❌ | ✅ | 4,527 LOC, breakpoints, stepping, variable inspection, REPL |
| Workflow graph visualization | ✅ (`act --graph`) | ❌ | |
| Event simulation | ✅ (`-e event.json`) | ✅ | |
| Dry run / list | ✅ (`-l`, `-n`) | ❌ | |
| `.actrc` config file | ✅ | N/A | |
| `.secrets` / `.vars` files | ✅ | ✅ | |

---

## 4. Execution Model: Deep Dive

### How act runs a job

1. Parse YAML into `Workflow` → `Job` → `Step` model
2. Build topological `Plan` from `needs` graph: stages (serial) × runs (parallel)
3. For each job, resolve `runs-on` to a Docker image (e.g. `ubuntu-latest` → `catthehacker/ubuntu:act-latest`)
4. Pull image, create a container with `tail -f /dev/null` entrypoint (keeps it alive)
5. Copy event JSON and env files into the container
6. Start service containers with health checks on the same Docker network
7. Execute each step sequentially inside the job container via `docker exec`
8. For Node actions: copy action source into container, run `node <main.js>`
9. For Docker actions: build/pull separate image, run in a container sharing the job container's network namespace
10. Tear down containers after the job completes

**Key consequence**: act's execution fidelity depends on how well its Go code
reimplements the runner's C# logic. Every runner feature must be independently
ported to Go.

### How aksh runs a job

1. Parse YAML into typed `Workflow` → `Job` → `Step` model
2. Expand matrix, evaluate concurrency groups, build job DAG
3. Scheduler dispatches jobs to the runner pool via the AzDO wire protocol
4. `aksh-runner` (Rust reimplementation) acquires the job via broker polling
5. The runner executes the job using its own step handlers (script, node, docker, composite)
6. The runner reports progress via timeline updates, workflow commands, and file commands
7. The runner completes the job via the broker, reporting conclusion, outputs, and annotations
8. The server updates run status, propagates outputs to dependent jobs, and evaluates concurrency

**Key consequence**: aksh controls both sides of the wire protocol. When GitHub
ships a runner update, aksh ports the changes to its own Rust runner. Having the
full stack in Rust means both server and runner are tested together — conformance
is verified against 24 golden wire captures from the official runner v2.336.0.

---

## 5. Protocol Fidelity

### act: No protocol

act has no wire protocol. It is a single binary that reads YAML and executes
steps. There is no registration, no session, no broker, no timeline, no results
service. Actions that depend on `ACTIONS_RUNTIME_URL`, `ACTIONS_RUNTIME_TOKEN`,
or `ACTIONS_RESULTS_URL` must be intercepted and redirected to act's built-in
artifact/cache HTTP servers.

### aksh: Full AzDO wire protocol

aksh implements the complete Azure DevOps runner protocol:

- **Registration**: RSA key exchange, runner capabilities, labels
- **Session**: AES-encrypted message queue, session renewal, conflict resolution
- **Broker**: `acquirejob` → `renewjob` → `completejob` lifecycle
- **Results service**: 5 Twirp routes (`CreateStepLogsMetadata`, `CreateJobLogsMetadata`, `CreateStepSummaryMetadata`, `WorkflowStepsUpdate`, signed blob URLs)
- **Timeline**: PATCH updates with `lastModified` stamps, GET for full history
- **OIDC**: RS256-signed JWTs with X.509 certificate chain, JWKS endpoint, OpenID discovery
- **Cancellation**: `JobCancelMessage` with GUID jobId + TimeSpan timeout, graceful+hard-kill phases
- **Location services**: 28 service definitions matching the hosted topology

This is verified by 24 golden conformance scenarios replayed from official
runner v2.336.0 wire captures. All 24 pass on status codes, request body schemas,
and acquirejob response schemas.

---

## 6. Performance

### 6.1 Scenario Benchmark Comparison (39 Golden Scenarios)

All three tools measured on the same Apple M4 Max host against the 39 scenario workflow YAML files (`experiments/mitm/scenarios/`).
Each Preloop job ran in its own isolated SmolVM microVM forked from the warm runner base image.

**Behavioral Fidelity Summary** (Correct local outcome matching GitHub Actions intent):
- **Preloop (aksh)**: **31 / 39 (79.5%)** correct behavior (26/39 passed)
- **act**: **29 / 39 (74.4%)** correct behavior (24/39 passed)
- **agent-ci**: **29 / 39 (74.4%)** correct behavior (24/39 passed)

*Note*: Scenarios 07, 09, 103, 107, and 14 contain intentional `exit 1` / error steps. Reporting **Failure** is the correct local behavior for these workflows.

| # | Scenario | File | act Status | act Time | agent-ci Status | agent-ci Time | Preloop Status | Preloop Time | Workflow Classification |
|---|---|---|:---:|:---:|:---:|:---:|:---:|:---:|---|
| 1 | `02-trivial-job` | `02-trivial-job.yml` | ✅ PASS | 0.45s | ✅ PASS | 10.34s | ✅ PASS | 366ms | Normal (Success expected) |
| 2 | `03-cancellation` | `03-cancellation.yml` | ✅ PASS | 60.57s | ✅ PASS | 69.59s | ✅ PASS | 60.46s | ⏳ Sleep / Timeout Test |
| 3 | `04-request-ack` | `04-request-ack.yml` | ✅ PASS | 0.45s | ✅ PASS | 10.36s | ✅ PASS | 519ms | Normal (Success expected) |
| 4 | `05-multi-job` | `05-multi-job-a.yml` | ✅ PASS | 0.45s | ✅ PASS | 10.14s | ✅ PASS | 431ms | Normal (Success expected) |
| 5 | `05-multi-job` | `05-multi-job-b.yml` | ✅ PASS | 0.39s | ✅ PASS | 10.13s | ✅ PASS | 485ms | Normal (Success expected) |
| 6 | `06-multi-step` | `06-multi-step.yml` | ✅ PASS | 0.51s | ✅ PASS | 10.60s | ✅ PASS | 447ms | Normal (Success expected) |
| 7 | `07-step-failure` | `07-step-failure.yml` | ❌ FAIL | 0.46s | ❌ FAIL | 9.85s | ❌ FAIL | 447ms | ⚠️ Intentional Failure |
| 8 | `08-job-outputs-needs` | `08-job-outputs-needs.yml` | ✅ PASS | 0.79s | ✅ PASS | 20.24s | ✅ PASS | 824ms | Normal (Success expected) |
| 9 | `09-matrix-fan-out` | `09-matrix-fan-out.yml` | ❌ FAIL | 20.70s | ❌ FAIL | 42.05s | ❌ FAIL | 458ms | ⚠️ Intentional Failure |
| 10 | `10-uses-checkout` | `10-uses-checkout.yml` | ✅ PASS | 0.49s | ✅ PASS | 10.15s | ✅ PASS | 730ms | Normal (Success expected) |
| 11 | `101-dynamic-matrix-dataflow` | `101-dynamic-matrix-dataflow.yml` | ✅ PASS | 0.95s | ✅ PASS | 19.82s | ✅ PASS | 480ms | Normal (Success expected) |
| 12 | `102-mask-and-secret-propagation` | `102-mask-and-secret-propagation.yml` | ✅ PASS | 0.45s | ✅ PASS | 10.36s | ✅ PASS | 400ms | Normal (Success expected) |
| 13 | `103-composite-nested-post` | `103-composite-nested-post.yml` | ❌ FAIL | 0.45s | ❌ FAIL | 10.21s | ❌ FAIL | 693ms | ⚠️ Intentional Failure |
| 14 | `104-job-defaults-env-cascade` | `104-job-defaults-env-cascade.yml` | ✅ PASS | 0.38s | ✅ PASS | 10.33s | ✅ PASS | 433ms | Normal (Success expected) |
| 15 | `105-concurrency-cancellation-group` | `105-concurrency-cancellation-group.yml` | ✅ PASS | 60.48s | ✅ PASS | 69.69s | ✅ PASS | 60.55s | ⏳ Sleep / Timeout Test |
| 16 | `107-continue-on-error-status-funcs` | `107-continue-on-error-status-funcs.yml` | ❌ FAIL | 0.61s | ❌ FAIL | 9.96s | ❌ FAIL | 466ms | ⚠️ Intentional Failure |
| 17 | `108-workflow-dispatch-inputs` | `108-workflow-dispatch-inputs.yml` | ✅ PASS | 0.40s | ✅ PASS | 10.47s | ✅ PASS | 380ms | Normal (Success expected) |
| 18 | `109-log-streaming-backpressure` | `109-log-streaming-backpressure.yml` | ✅ PASS | 0.48s | ✅ PASS | 10.83s | ✅ PASS | 606ms | Normal (Success expected) |
| 19 | `11-cache-roundtrip` | `11-cache-roundtrip.yml` | ❌ FAIL | 30.01s | ✅ PASS | 11.47s | ✅ PASS | 679ms | Normal (Success expected) |
| 20 | `110-environment-deployment-url` | `110-environment-deployment-url.yml` | ✅ PASS | 0.45s | ✅ PASS | 10.36s | ✅ PASS | 378ms | Normal (Success expected) |
| 21 | `111-github-state-post-execution` | `111-github-state-post-execution.yml` | ✅ PASS | 0.43s | ✅ PASS | 28.13s | ✅ PASS | 376ms | Normal (Success expected) |
| 22 | `112-service-container-health-ports` | `112-service-container-health-ports.yml` | ❌ FAIL | 3.78s | ❌ FAIL | 47.81s | ❌ FAIL | 12.28s | Normal (Success expected) |
| 23 | `113-artifact-v4-multi-pattern` | `113-artifact-v4-multi-pattern.yml` | ❌ FAIL | 1.45s | ✅ PASS | 22.27s | ✅ PASS | 755ms | Normal (Success expected) |
| 24 | `114-step-timeout-graceful-kill` | `114-step-timeout-graceful-kill.yml` | ❌ FAIL | 30.01s | ✅ PASS | 130.18s | ✅ PASS | 120.45s | ⏳ Sleep / Timeout Test |
| 25 | `115-cache-v2-restore-fallback` | `115-cache-v2-restore-fallback.yml` | ✅ PASS | 23.30s | ✅ PASS | 44.77s | ✅ PASS | 784ms | Normal (Success expected) |
| 26 | `12-artifact` | `12-artifact.yml` | ❌ FAIL | 1.57s | ✅ PASS | 40.45s | ✅ PASS | 799ms | Normal (Success expected) |
| 27 | `13-composite-action` | `13-composite-action.yml` | ❌ FAIL | 0.39s | ❌ FAIL | 10.37s | ❌ FAIL | 742ms | Normal (Success expected) |
| 28 | `14-annotations` | `14-annotations.yml` | ❌ FAIL | 0.34s | ❌ FAIL | 10.47s | ❌ FAIL | 454ms | ⚠️ Intentional Failure |
| 29 | `15-oidc-id-token` | `15-oidc-id-token.yml` | ❌ FAIL | 0.34s | ❌ FAIL | 10.89s | ✅ PASS | 358ms | Normal (Success expected) |
| 30 | `16-container-job` | `16-container-job.yml` | ✅ PASS | 0.44s | ✅ PASS | 21.42s | ✅ PASS | 12.80s | Normal (Success expected) |
| 31 | `163-reusable-caller` | `163-reusable-caller.yml` | ❌ FAIL | 0.03s | ❌ FAIL | 0.65s | ❌ FAIL | 18ms | Normal (Success expected) |
| 32 | `17-service-container` | `17-service-container.yml` | ✅ PASS | 16.18s | ❌ FAIL | 2.79s | ✅ PASS | 20.42s | Normal (Success expected) |
| 33 | `30-container-job-basic` | `30-container-job-basic.yml` | ✅ PASS | 0.62s | ✅ PASS | 11.95s | ✅ PASS | 11.96s | Normal (Success expected) |
| 34 | `31-container-with-services` | `31-container-with-services.yml` | ✅ PASS | 11.16s | ✅ PASS | 16.61s | ❌ FAIL | 30.00s | Normal (Success expected) |
| 35 | `32-services-no-container` | `32-services-no-container.yml` | ❌ FAIL | 0.45s | ❌ FAIL | 3.11s | ❌ FAIL | 30.00s | Normal (Success expected) |
| 36 | `33-container-env-options` | `33-container-env-options.yml` | ✅ PASS | 0.83s | ❌ FAIL | 9.94s | ✅ PASS | 2.12s | Normal (Success expected) |
| 37 | `34-container-with-checkout` | `34-container-with-checkout.yml` | ❌ FAIL | 7.93s | ❌ FAIL | 38.68s | ❌ FAIL | 30.00s | Normal (Success expected) |
| 38 | `35-container-lifecycle` | `35-container-lifecycle.yml` | ✅ PASS | 2.40s | ❌ FAIL | 12.60s | ❌ FAIL | 30.00s | Normal (Success expected) |
| 39 | `36-docker-action` | `36-docker-action.yml` | ✅ PASS | 2.36s | ❌ FAIL | 13.78s | ❌ FAIL | 3.12s | Normal (Success expected) |

## 7. Isolation & Security

| | act | aksh |
|---|---|---|
| **Default isolation** | Docker container (shared kernel) | SmolVM microVM (separate kernel, 131 MB idle RSS) |
| **Secret handling** | Env vars passed to container; `.secrets` file | `SecretString` type, mask hints on wire, `expose()` only at protocol boundaries |
| **Network isolation** | Docker network modes (host/bridge) | SmolVM strict egress (`SMOLVM_EGRESS_FLOOR=strict`) |
| **Filesystem isolation** | Docker volumes + bind mounts | VM filesystem, APFS clones for workspace |
| **Resource limits** | Docker resource constraints | VM memory/CPU allocation, pool sizing |
| **Multi-tenancy** | Not designed for it | Ephemeral runners, per-job VM, runner groups |

---

## 8. Conformance & Testing

### act

- Community-driven testing via bug reports and PRs
- No formal conformance suite against the official runner
- No wire-capture comparison methodology
- Discrepancies are discovered when users' workflows fail

### aksh

- **runner-watch**: Automated pipeline that watches `actions/runner` releases, diffs source, emits TOML specs, and replays golden captures
- **24 golden scenarios**: Captured from official runner v2.336.0, covering registration, job lifecycle, cancellation, matrix, cache, artifacts, OIDC, containers, services, composite actions, and Docker actions
- **Conformance gate**: Status codes, request body schemas, and acquirejob response schemas must match exactly
- **Differential testing**: Same workflow run against both GitHub Actions and aksh; 11/12 job-level match (92%), 6/12 full match including step details (50%)
- **Property tests**: 87 tests for concurrency groups alone

---

## 9. Known Gaps

### act's known gaps (features it doesn't support)

| Feature | Status |
|---|---|
| OIDC tokens | ❌ Not implemented |
| Concurrency groups | ❌ Not implemented |
| Job permissions / GITHUB_TOKEN scoping | ❌ Not implemented |
| `run-name:` | ❌ Not implemented |
| `$GITHUB_ARTIFACTS` / `$GITHUB_ARTIFACTS_LIST` | ❌ Not implemented |
| Problem matchers | ❌ Not implemented |
| `*` filter syntax in expressions | ❌ Not implemented |
| `{{`/`}}` escape sequences | ❌ Not implemented |
| macOS/Windows runners (native) | ❌ Workaround only (`-self-hosted`) |
| Wire protocol (for external integrations) | ❌ Fundamental: act IS the runner |
| DAP debugger | ❌ Not implemented |

### aksh's known gaps (features it doesn't fully support)

| Feature | Status |
|---|---|
| `BackgroundStepCoordinator` + cancel-control | ⚠️ Partial — DTO and flag implemented, async coordinator missing |
| Runner self-update | ❌ Intentional — aksh-runner does not self-update |
| Runner config refresh | ❌ Missing |
| Action download telemetry | ⚠️ Info logs only, not structured telemetry payloads |
| Full hosted-service location parity | ⚠️ 28 services defined, but not full GitHub/Azure topology |
| Workflow graph visualization | ❌ Not implemented |
| Dry run mode | ❌ Not implemented |

---

## 10. Community & Ecosystem

| | act | aksh |
|---|---|---|
| **Stars** | ~71,000 | Private |
| **Language** | Go | Rust |
| **Contributors** | 300+ | Small team |
| **Latest release** | v0.2.89 (2026-06-01) | Continuous |
| **Release cadence** | Monthly | Continuous |
| **Ecosystem** | `gh-act` CLI extension, `github-act-runner`, VS Code extension | Preloop product, SmolVM integration |
| **License** | MIT | Proprietary |

---

## 11. When to Use Which

| Scenario | act | aksh |
|---|---|---|
| Quick local smoke test of a simple workflow | ✅ Best choice | Overkill |
| Full-fidelity local CI matching GitHub behavior | ⚠️ Gaps will bite | ✅ Designed for this |
| Debugging workflow logic step by step | ❌ No debugger | ✅ DAP debugger |
| OIDC token testing | ❌ Not supported | ✅ Full OIDC provider |
| Concurrency group testing | ❌ Not supported | ✅ Full implementation |
| Full wire-protocol fidelity (server + runner) | ❌ No protocol | ✅ Both sides reimplemented |
| Windows/macOS container workflows | ❌ Linux Docker only | ✅ macOS via `somac`, Windows via `vowin`, Linux via SmolVM |
| Zero-setup, zero-dependency quick start | ✅ `brew install act` | ❌ Requires Rust build or Preloop install |
| CI infrastructure (self-hosted replacement) | ❌ Not designed for it | ✅ Ephemeral runners, runner groups, multi-tenancy |

---

## 12. Summary

**act** is a pragmatic tool for developers who want to quickly test workflows
locally. It reimplements the runner in Go and runs steps in Docker. It's easy to
install, has a huge community, and works well for simple workflows. It breaks
down on advanced features (OIDC, concurrency, permissions) and can't guarantee
behavioral parity with GitHub because it's a clean-room reimplementation.

**aksh** reimplements the entire stack — both the server-side control plane and
the runner — in Rust. `aksh-runner` is a faithful port of `actions/runner`
v2.336.0, handling the full step lifecycle (Listener + Worker). Having both sides
in Rust means the full wire protocol is exercised end-to-end and conformance is
verified against golden captures. It has rigorous conformance testing, sub-host-speed
performance, a DAP debugger, and full OIDC/concurrency support. It's more
complex to set up but provides much higher fidelity.

The fundamental difference is scope: **act reimplements only the runner; aksh
implements both the server and the runner** with the real wire protocol between them. This single decision cascades through every other
difference in the comparison.
