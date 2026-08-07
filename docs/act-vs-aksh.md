# act vs agent-ci vs aksh vs Gitea Actions: GitHub Actions Runners Compared

A detailed comparison between [nektos/act](https://github.com/nektos/act) and
aksh (this repo). Both let you run GitHub Actions workflows locally. They take
fundamentally different approaches to the problem.

> **Updated 2026-08-05** — this comparison now includes **Gitea Actions
> (`gitea.com/gitea/runner`, aka act_runner)**: the act fork running against
> Gitea's own control plane. It reuses act's execution engine but adds a real
> server/runner split with a proprietary Connect-RPC protocol, so several of
> act's limitations are fixed (by the server or by the fork) while others
> persist. Section 6 is the dedicated deep dive; the philosophy table below
> positions it at a glance.

---

## 1. Philosophy

| | act | agent-ci | aksh | Gitea Actions (act_runner) |
|---|---|---|---|---|
| **One-liner** | Reimplement the runner in Go (CLI) | Reimplement the server in TypeScript | Reimplement **both** the control plane (server) **and** the runner in Rust, with the full AzDO wire protocol between them | Reimplement the runner in Go (act fork), **with a server**: Gitea's monolith does scheduling, the runner executes |
| **Strategy** | Replace the runner binary with a Go reimplementation that executes steps in Docker containers | Emulate the GitHub API (DTU) locally, drive the unmodified official runner in Docker | Replace the server and the runner; speak the official AzDO protocol between them | Replace the runner; the Gitea server supplies scheduling (needs/matrix/concurrency/max-parallel) over a **proprietary Connect-RPC protocol** (`/api/actions`) |
| **Compatibility target** | Behavioral: "workflows should mostly work" | Protocol: "the official runner v2.336.0 cannot tell the difference" | Protocol: "the official runner v2.336.0 cannot tell the difference" | Workflow-level: GitHub YAML syntax + the action ecosystem, on Gitea's own wire protocol — the official runner binary is never used |
| **Verification method** | Manual testing + community bug reports | Differential conformance vs official runner | 24 golden wire-capture scenarios replayed against the server; differential conformance vs official runner | Unit tests + community bug reports; no official-runner wire conformance (by design) |

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

### Gitea Actions runner (act_runner)

```
┌──────────────────────────────────────────────┐
│            Gitea server (Go monolith)         │
│                                               │
│  Workflow detect ─► job DAG ─► concurrency    │
│  needs/matrix/max-parallel ─► label match     │
│                                               │
│  Task = workflow YAML + context + secrets     │
└───────────────────────┬──────────────────────┘
                        │ Connect RPC (protobuf)
                        │ /api/actions: Ping,
                        │ Register/Declare/
                        │ FetchTask/UpdateTask/
                        │ UpdateLog
        ┌───────────────▼────────────────┐
        │  act_runner (Go, act fork)     │
        │  parses YAML, plans stages,    │
        │  executes steps                │
        │                                │
        │  Docker job container + exec   │
        │  or host mode (labels)         │
        └────────────────────────────────┘
```

- **Language**: Go 1.26 (module `gitea.com/gitea/runner`); the act fork is
  fully vendored in-tree under `act/`
- **Execution**: Client–server — Gitea's monolith does all scheduling and
  sends a `Task` protobuf (raw workflow YAML + `github` context + secrets +
  vars + `needs` outputs); the runner **parses and interprets the workflow
  itself** (`internal/app/run/workflow.go`)
- **Wire protocol**: proprietary Connect RPC (`connectrpc.com/connect` +
  `gitea.dev/actions-proto-go`), 5 unary methods + Ping; auth via
  `x-runner-uuid` / `x-runner-token` headers
- **Container model**: identical to act — per-job Docker container (label
  `docker://image`), steps via `docker exec`, service containers on a job
  network; `host` schema labels run steps directly on the host
- **Expression engine**: fork's own `act/exprparser` on `rhysd/actionlint`
  AST — includes `*` filter, `{{`/`}}` escapes, bracket access (see §6)

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

| Feature | act | aksh | Gitea | Notes |
|---|:---:|:---:|:---:|---|
| Workflow YAML parsing | ✅ | ✅ | ✅ | Gitea: server parses for scheduling, runner re-parses for execution |
| `run:` steps | ✅ | ✅ | ✅ | |
| `uses:` actions (remote) | ✅ | ✅ | ✅ | |
| `uses:` actions (local) | ✅ | ✅ | ✅ | |
| `uses:` Docker actions | ✅ | ✅ | ✅ | |
| `uses: $/` self-repo syntax | ❌ | ✅ | ❌ | v2.336.0 feature |
| Composite actions | ✅ | ✅ | ✅ | |
| Reusable workflows (`workflow_call`) | ✅ | ✅ | ✅ | Gitea: depth 5; `github.event_name` overridden to `workflow_call` in callees (documented FIXME) |
| Matrix strategy | ✅ | ✅ | ✅ | |
| `include` / `exclude` in matrix | ✅ | ✅ | ✅ | |
| `needs` DAG | ✅ | ✅ | ✅ | Gitea: resolved server-side (`jobEmitterQueue`) |
| Job outputs | ✅ | ✅ | ✅ | |
| `if` conditionals (job/step) | ✅ | ✅ | ✅ | |
| `continue-on-error` | ✅ | ✅ | ✅ | |
| `timeout-minutes` | ✅ | ✅ | ✅ | Gitea: `runner.timeout` (default 3h) + server-side `StopEndlessTasks` |
| `defaults.run` (shell/working-directory) | ✅ | ✅ | ✅ | |
| `workflow_dispatch` inputs | ✅ | ✅ | ✅ | Gitea: manual run with inputs |
| `run-name` expression | ❌ | ✅ | ⚠️ | Gitea displays a run name; expression evaluation not verified in this audit |

### Environment & Contexts

| Feature | act | aksh | Gitea | Notes |
|---|:---:|:---:|:---:|---|
| `$GITHUB_ENV` | ✅ | ✅ | ✅ | |
| `$GITHUB_OUTPUT` | ✅ | ✅ | ✅ | |
| `$GITHUB_PATH` | ✅ | ✅ | ✅ | |
| `$GITHUB_STEP_SUMMARY` | ✅ | ✅ | ✅ | |
| `$GITHUB_STATE` | ✅ | ✅ | ✅ | |
| `$GITHUB_ARTIFACTS` | ❌ | ✅ | ❌ | v2.336.0 feature |
| `$GITHUB_ARTIFACTS_LIST` | ❌ | ✅ | ❌ | v2.336.0 feature |
| `ACTIONS_CACHE_MODE` | ❌ | ✅ | ❌ | Gitea uses `CACHE_SERVICE_V2=true` instead |
| `github.*` context | ✅ | ✅ | ✅ | Gitea: `event_name` divergence under `workflow_call`; `gitea` context alias |
| `env.*` context | ✅ | ✅ | ✅ | |
| `secrets.*` context | ✅ | ✅ | ✅ | Gitea: server-scoped secrets (repo/org/global, encrypted) |
| `vars.*` context | ✅ | ✅ | ✅ | Gitea: Repo > Org > Global precedence |
| `steps.*` context | ✅ | ✅ | ✅ | |
| `needs.*` context | ✅ | ✅ | ✅ | |
| `matrix.*` context | ✅ | ✅ | ✅ | |
| `runner.*` context | ✅ | ✅ | ✅ | Gitea: `runner.name` from registration |
| `inputs.*` context | ✅ | ✅ | ✅ | |
| Comprehensive `GITHUB_*` env vars | ⚠️ partial | ✅ | ⚠️ | Gitea injects GITEA_* extras; not the full official set |

### Expression Engine

| Feature | act | aksh | Gitea | Notes |
|---|:---:|:---:|:---:|---|
| `contains()` | ✅ | ✅ | ✅ | |
| `startsWith()` / `endsWith()` | ✅ | ✅ | ✅ | |
| `format()` | ✅ | ✅ | ✅ | Gitea fork implements the `{{`/`}}` brace state machine |
| `join()` | ✅ | ✅ | ✅ | |
| `toJSON()` / `fromJSON()` | ✅ | ✅ | ✅ | |
| `hashFiles()` | ✅ | ✅ | ✅ | Gitea: container + local impls |
| `success()` / `failure()` / `cancelled()` / `always()` | ✅ | ✅ | ✅ | |
| Type coercion (string/number/bool/null) | ✅ | ✅ | ✅ | |
| `*` filter syntax | ❌ | ✅ | ✅ | fixed in the fork (`ArrayDerefNode`) |
| Bracket access `['key']` | ⚠️ | ✅ | ✅ | fixed in the fork (`IndexAccessNode`) |
| `{{` / `}}` escape sequences | ❌ | ✅ | ✅ | fixed in the fork |
| Case-insensitive `==` | ⚠️ | ✅ | ✅ | fixed in the fork (`compareString` lowercases) |
| Parser | `rhysd/actionlint` AST | Custom Pratt parser | `act/exprparser` on actionlint AST | Gitea: fork-owned evaluator, not the upstream act one |

### Workflow Commands

| Feature | act | aksh | Gitea | Notes |
|---|:---:|:---:|:---:|---|
| `::set-output::` | ✅ | ✅ | ✅ | Deprecated but supported |
| `::set-env::` | ✅ | ✅ | ✅ | |
| `::add-path::` | ✅ | ✅ | ✅ | |
| `::add-mask::` | ✅ | ✅ | ✅ | |
| `::debug::` / `::warning::` / `::error::` / `::notice::` | ✅ | ✅ | ✅ | Gitea folds `::error::` etc. into the log with source location (no annotation store) |
| `::group::` / `::endgroup::` | ✅ | ✅ | ✅ | Gitea: web UI folds on these |
| `::stop-commands::` | ✅ | ✅ | ✅ | |
| Problem matchers | ❌ | ✅ | ❌ | Gitea reporter handles no `::add-matcher::` |

### Protocol & Runner Features

| Feature | act | aksh | Gitea | Notes |
|---|:---:|:---:|:---:|---|
| Faithful runner reimplementation | ✅ | ✅ | ✅ | act_runner embeds the act fork in-tree under `act/` |
| AzDO wire protocol | ❌ | ✅ | ❌ | Gitea: proprietary Connect RPC (`/api/actions`) |
| Runner registration handshake | ❌ | ✅ | ⚠️ | Gitea: own token → uuid/agent-token flow; no RSA/AES session crypto |
| Broker acquire/renew/complete | ❌ | ✅ | ⚠️ | Gitea: `FetchTask` / `UpdateTask` (heartbeat) instead of broker messages |
| OIDC id-token provider | ❌ | ✅ | ❌ | Gitea: not wired on main — server never sets `actions_id_token_request_url` |
| Concurrency groups | ❌ | ✅ | ✅ | Gitea: server-side (`services/actions/concurrency.go`), cancel-in-progress |
| Job permissions / GITHUB_TOKEN scoping | ❌ | ✅ | ⚠️ | aksh mints GitHub App installation tokens per job, scoped to the repo and the effective `permissions:` (declared sets minted verbatim, implicit defaults clamped to installation grants; PAT fallback is opt-in, never silent). Gitea: one repo-scoped task token (`GITEA_TOKEN`/`GITHUB_TOKEN`) |
| Job cancellation (wire protocol) | ❌ | ✅ | ⚠️ | Gitea: `RESULT_CANCELLED` returned on `UpdateTask` heartbeat; no `CancellationTiming` |
| Runner self-update | N/A | ❌ intentional | ❌ | Gitea: nightly Docker images instead |
| Runner groups | N/A | ✅ | ✅ | Gitea: repo / org / global runner scoping |
| Ephemeral runners | N/A | ✅ | ✅ | Gitea: `--ephemeral` / `GITEA_RUNNER_EPHEMERAL`, cleaned up server-side |
| Results-service (Twirp) | ❌ | ✅ | ❌ | Gitea: log/artifact HTTP APIs instead of Twirp |
| Timeline / live logs | ❌ | ✅ | ⚠️ | Gitea: `UpdateLog` rows → DBFS → object storage; web UI polls, no WebSocket timeline |
| Job annotations | ❌ | ✅ | ❌ | Gitea folds annotations into log text |
| `connectionData` / location services | N/A | ✅ | ❌ | not applicable to Gitea's protocol |
| Background steps (v2.336.0) | ❌ | ⚠️ partial | ❌ | |
| Locked dependencies announcement | ❌ | ✅ | ❌ | v2.336.0 feature |

### Container & Isolation

| Feature | act | aksh | Gitea | Notes |
|---|:---:|:---:|:---:|---|
| Docker job containers | ✅ | ✅ | ✅ | act and act_runner share the idle-container + `docker exec` model |
| Service containers | ✅ | ✅ | ✅ | Gitea: not in host mode |
| Docker-in-Docker | ✅ | ✅ | ✅ | Gitea: `dind` / `dind-rootless` image flavours bundle a private daemon |
| MicroVM isolation (SmolVM/libkrun) | ❌ | ✅ | ❌ | Gitea: shared-kernel Docker only |
| Process execution (no container) | ✅ (`-self-hosted`) | ✅ | ✅ (`host` labels) | |
| macOS native runner | ⚠️ (`-self-hosted` workaround) | ✅ | ✅ (`macos:host`) | |
| Windows native runner | ⚠️ (`-self-hosted` workaround) | ✅ | ✅ (`windows:host`) | |
| Custom platform images (`-P`) | ✅ | N/A | ✅ | Gitea: label schema `name:docker://image` |
| Podman support | ✅ | ✅ | ✅ | act/act_runner via `DOCKER_HOST`; aksh via runner backend |

### Cache & Artifacts

| Feature | act | aksh | Gitea | Notes |
|---|:---:|:---:|:---:|---|
| Cache v1 (reserve/upload/commit/lookup) | ✅ | ✅ | ✅ | Gitea: **runner-side** embedded cache server (per-runner, not shared) |
| Cache v2 (Twirp) | ❌ | ✅ | ⚠️ | Gitea: runner-side v2 API; action bundles patched to redirect URLs; no Twirp |
| Artifact v1 (create/put/get/list) | ✅ | ✅ | ✅ | Gitea: server-side v3 REST (AzDO-style chunked upload) |
| Artifact v2 (Twirp + blob) | ✅ (v3/v4) | ✅ | ⚠️ | Gitea: v4 Connect API + HMAC-signed URLs (not Twirp); `upload-artifact@v4` works via patching |
| File-backed storage | ✅ | ✅ | ✅ | Gitea: DBFS → object storage for logs/artifacts; local dir for cache |

### Developer Experience

| Feature | act | aksh | Gitea | Notes |
|---|:---:|:---:|:---:|---|
| DAP debugger | ❌ | ✅ | ❌ | aksh: 4,527 LOC, breakpoints, stepping, variable inspection, REPL |
| Workflow graph visualization | ✅ (`act --graph`) | ❌ | ❌ | |
| Event simulation | ✅ (`-e event.json`) | ✅ | ⚠️ | Gitea: manual run via web UI; no event-file injection |
| Dry run / list | ✅ (`-l`, `-n`) | ❌ | ❌ | |
| `.actrc` config file | ✅ | N/A | N/A | Gitea: YAML config (`config.yaml`) |
| `.secrets` / `.vars` files | ✅ | ✅ | N/A | Gitea: server-side secrets/vars instead |

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

## 6. Gitea Actions runner (act_runner): the act fork with a control plane

Gitea Actions pairs a **server** (the Gitea monolith itself) with **act_runner**
(`gitea.com/gitea/runner`), a Go daemon that embeds the act fork in-tree under
`act/`. It is the only entry here that is GitHub-compatible at the *workflow*
level while being wire-incompatible with GitHub by design: the official runner
binary is never used, and the server/runner link is Gitea's own protocol.

### Division of labor — the opposite of aksh

| | aksh | Gitea Actions |
|---|---|---|
| Who parses the workflow YAML | **Server** — builds a fully materialized `AgentJobRequestMessage` (every step, env, context) | **Runner** — the server sends the raw YAML (`Task.WorkflowPayload`) plus `needs` outputs/results; `generateWorkflow()` re-parses it and the act engine plans/executes |
| Who schedules | Server (needs DAG, matrix, concurrency, fail-fast) | Server (`jobEmitterQueue` resolves needs/if/matrix/concurrency/max-parallel; `TryPickTask` claims atomically) |
| What the runner sees | A pre-built job message | Raw YAML + `github` context + secrets + vars + needs — it must interpret |

### Wire protocol: Connect RPC, not AzDO

Base URL `<instance>/api/actions`; auth via `x-runner-uuid` + `x-runner-token`
headers (constant-time compare server-side). All unary Connect RPCs:

| RPC | Request → Response | Purpose |
|---|---|---|
| `ping.v1.PingService/Ping` | name → ok | Pre-registration connectivity check |
| `runner.v1.RunnerService/Register` | name, token, version, labels, ephemeral, capabilities → runner `{id, uuid, token}` | Registration token exchanged for a per-runner agent token |
| `/Declare` | version, labels, capabilities | Re-declare on daemon start; capability negotiation via response header |
| `/FetchTask` | `tasks_version` → task or empty | **Long-poll** with server-side version counter; empty = idle, exponential backoff |
| `/UpdateTask` | task state (steps, result, timestamps) + outputs → ack | Heartbeat + state; server can reply `RESULT_CANCELLED` to cancel the job |
| `/UpdateLog` | `{index, rows[], no_more}` → `ack_index` | Batched log upload with ack |

Artifacts are the one AzDO-compatible surface: a **v3 REST API**
(create-upload-url → PUT chunks with md5/content-range → confirm → download,
Bearer JWT) plus a **v4 Connect API** with HMAC-signed URLs, both server-side
under `/api/actions_pipeline/`.

### act limitations: fixed vs persisting

| act CLI limitation | Status in act_runner | Evidence |
|---|---|---|
| Concurrency groups ❌ | **Fixed — server-side** | `services/actions/concurrency.go`; `PrepareToStartJobWithConcurrency` cancels prior group members |
| `*` filter (`steps.*.outputs`) ❌ | **Fixed in fork** | `act/exprparser/interpreter.go` (`ArrayDerefNode`, slice property walk) |
| `{{` / `}}` escapes ❌ | **Fixed in fork** | `format()` brace state machine (`functions.go`) |
| Bracket access / case-insensitive `==` ⚠️ | **Fixed** | `IndexAccessNode`; `compareString` lowercases both sides |
| macOS / Windows native ⚠️ | **Fixed** | first-class `host` labels (`macos:host`, `windows:host`) |
| OIDC id tokens ❌ | **Missing on current main** | `generateTaskContext` sets only `token` + `gitea_runtime_token` — no `actions_id_token_request_url`; the runner forwards `ACTIONS_ID_TOKEN_REQUEST_URL/TOKEN` only if the server provides it |
| GITHUB_TOKEN scoping ❌ | **Partial** | one task token (`GITEA_TOKEN` or `GITHUB_TOKEN`), repo-scoped secret; no per-permission JWT |
| Problem matchers ❌ | **Still missing** | reporter handles `add-mask`/`debug`/`notice`/`warning`/`error`/`group`/`endgroup`/`stop-commands` — no `add-matcher` |
| Wire protocol ❌ | **Still not GitHub-compatible** | by design — Connect RPC, Gitea-proprietary |
| DAP debugger ❌ | Still missing | |

### Gitea-specific divergences (act-vs-aksh table items that do *not* carry over)

- **Cache is runner-local** — Gitea's server implements no `@actions/cache`;
  act_runner runs an embedded cache server (or an external shared
  `cache-server`), and it **patches action bundles** (`toolkit_patch.go`) so
  `actions/upload-artifact@v4` and cache v2 work against Gitea URLs.
- **Annotations are folded into log text** — "Gitea has no annotation store"
  (`internal/pkg/report/reporter.go`, `formatAnnotation`).
- **Host-mode jobs get no service containers** (README: "Unlike GitHub, a job
  whose steps run on the host starts no service containers").
- **`workflow_call` diverges**: the server overrides `event_name` to
  `"workflow_call"` for callee jobs — explicit `FIXME` in
  `services/actions/context.go`.
- Gitea extras: `ACT=true` and `GITEA_ACTIONS=true` env by default, `GITEA_TOKEN`
  priority over `GITHUB_TOKEN`, configurable action shallow-clone, job
  hooks / post-task scripts.
- The execution model is **the same Docker model as act** (per-job container,
  `docker exec` per step, service containers on a job network); `dind` image
  flavours bundle a private daemon. No microVM story.

### Conformance

Gitea Actions has no wire-level conformance methodology: behavior is enforced
by unit tests and real-world deployment. This is the mirror image of aksh's
golden-capture pipeline — Gitea optimizes for *workflow* compatibility on its
own protocol, aksh for *protocol* compatibility with the official runner.

---

## 7. Performance

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

## 8. Isolation & Security

| | act | aksh |
|---|---|---|
| **Default isolation** | Docker container (shared kernel) | SmolVM microVM (separate kernel, 131 MB idle RSS) |
| **Secret handling** | Env vars passed to container; `.secrets` file | `SecretString` type, mask hints on wire, `expose()` only at protocol boundaries |
| **Network isolation** | Docker network modes (host/bridge) | SmolVM strict egress (`SMOLVM_EGRESS_FLOOR=strict`) |
| **Filesystem isolation** | Docker volumes + bind mounts | VM filesystem, APFS clones for workspace |
| **Resource limits** | Docker resource constraints | VM memory/CPU allocation, pool sizing |
| **Multi-tenancy** | Not designed for it | Ephemeral runners, per-job VM, runner groups |

---

## 9. Conformance & Testing

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

## 10. Known Gaps

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

### act_runner's known gaps (features still missing despite the fork)

| Feature | Status |
|---|---|
| OIDC id tokens | ❌ Not wired on Gitea main — server never sets `actions_id_token_request_url` |
| Problem matchers (`::add-matcher::`) | ❌ Not implemented |
| `$GITHUB_ARTIFACTS` / `$GITHUB_ARTIFACTS_LIST` | ❌ Not implemented |
| Fine-grained GITHUB_TOKEN permission scoping | ⚠️ Partial — one repo-scoped task token |
| Server-side `@actions/cache` | ❌ Runner-local cache; action bundles are patched to redirect |
| Annotation store | ❌ Annotations folded into log text |
| Service containers in host mode | ❌ Not supported (GitHub does support them) |
| `github.event_name` inside `workflow_call` callees | ❌ Overridden to `"workflow_call"` (documented FIXME) |
| Wire compatibility with the official runner | ❌ By design — Gitea-proprietary Connect RPC |
| DAP debugger | ❌ Not implemented |
| MicroVM isolation | ❌ Docker shared-kernel only |

---

## 11. Community & Ecosystem

| | act | act_runner | aksh |
|---|---|---|---|
| **Stars** | ~71,000 | Gitea org (~mirrors act_runner) | Private |
| **Language** | Go | Go 1.26 | Rust |
| **Contributors** | 300+ | Gitea team + community | Small team |
| **Latest release** | v0.2.89 (2026-06-01) | Continuous (nightly images) | Continuous |
| **Release cadence** | Monthly | Continuous | Continuous |
| **Ecosystem** | `gh-act` CLI extension, `github-act-runner`, VS Code extension | Gitea server, Docker images (`basic`/`dind`/`dind-rootless`), runner fleet | Preloop product, SmolVM integration |
| **License** | MIT | MIT | Proprietary |

---

## 12. When to Use Which

| Scenario | act | act_runner | aksh |
|---|---|---|---|
| Quick local smoke test of a simple workflow | ✅ Best choice | ❌ Needs a Gitea server | Overkill |
| Self-hosted CI platform for a team | ❌ | ✅ Mature product: approval gates, org runners, reruns, retention | ⚠️ In development |
| Full-fidelity local CI matching GitHub behavior | ⚠️ Gaps will bite | ⚠️ Workflow-level compat; protocol is Gitea's | ✅ Designed for this |
| Debugging workflow logic step by step | ❌ No debugger | ❌ No debugger | ✅ DAP debugger |
| OIDC token testing | ❌ Not supported | ❌ Not wired | ✅ Full OIDC provider |
| Concurrency group testing | ❌ Not supported | ✅ Server-side implementation | ✅ Full implementation |
| Full wire-protocol fidelity (server + runner) | ❌ No protocol | ❌ Gitea-proprietary protocol | ✅ Both sides reimplemented |
| Windows/macOS container workflows | ❌ Linux Docker only | ⚠️ `host` labels run natively; Docker jobs Linux-only | ✅ macOS via `somac`, Windows via `vowin`, Linux via SmolVM |
| Zero-setup, zero-dependency quick start | ✅ `brew install act` | ❌ Needs a Gitea instance | ❌ Requires Rust build or Preloop install |
| CI infrastructure (self-hosted replacement) | ❌ Not designed for it | ✅ Production-grade (MIT, deployed fleets) | ✅ Ephemeral runners, runner groups, multi-tenancy |

---

## 13. Summary

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

**Gitea Actions (act_runner)** is act with a control plane: the Gitea server
does all scheduling (needs, matrix, concurrency, max-parallel, fork-PR
approval) and hands each runner a task containing the raw workflow YAML; the
runner interprets it with the in-tree act fork and executes in Docker (or on
the host via `host` labels). The fork closed most of act's expression gaps
(`*` filter, `{{`/`}}` escapes, bracket access) and the server fixed
concurrency, but OIDC id tokens, problem matchers, fine-grained token scoping,
and a server-side cache are still missing, and the wire protocol is Gitea's
own Connect RPC — the official runner binary cannot connect. It is the most
production-deployed entry here (MIT, mature server features), but "GitHub
compatible" stops at the workflow syntax.

The fundamental difference is scope: **act reimplements only the runner;
act_runner reimplements the runner behind Gitea's own protocol; agent-ci
reimplements the server; aksh implements both the server and the runner** with
the real AzDO wire protocol between them. That single decision cascades
through every other difference in the comparison.
