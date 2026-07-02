# aksh-runner — Runner Compatibility Roadmap

Tracks the remaining work required to achieve full compatibility between the Rust runner (`aksh-runner`) and the official runner (`actions/runner` v2.335.1), structured by conformance milestones and execution subsystems.

---

## 1. Conformance Scenarios Plan

The compatibility oracle is **live GitHub runs** first, then local aksh, using the `preloopdev/aksh-conformance-sample` repository workflows.

### Phase 1: Script/Job Semantics
- **07-step-failure.yml**:
  - Failed step outcome/conclusion propagation.
  - Skip remaining steps unless `always()` / `failure()` is met.
  - `continue-on-error` validation.
  - Completejob status matching failed state in GitHub UI.
- **08-job-outputs-needs.yml**:
  - `GITHUB_OUTPUT` file parsing.
  - Output context: `steps.<id>.outputs.<name>` and `jobs.<job>.outputs`.
  - Upstream needs propagation: `needs.<job>.outputs.<name>`.
  - Completejob `outputs` payload mapping.
- **09-matrix-fan-out.yml**:
  - Multi-job session lifetimes.
  - Runner busy/idle state transitions on `/message` polling.
  - Matrix/strategy variables in runner-side context.

### Phase 2: Actions & Composite Lifecycle
- **10-uses-checkout.yml**:
  - Actions download via `ActionDownloadInfo` endpoints.
  - Tarball extraction with official path layout.
  - Manifest parsing (`action.yml`).
  - Node.js runtime selection (`node20`/`node24`).
  - Environment injection (`INPUT_*`, `GITHUB_ACTION_*`).
  - LIFO pre/post execution lifecycle hooks.
- **13-composite-action.yml**:
  - Nested step execution in composite contexts.
  - Composite input/output contexts.
  - Relative path resolution inside composite steps.

### Phase 3: Runtime Services
- **11-cache-roundtrip.yml**:
  - Cache protocol client (`_apis/artifactcache`).
  - Cache reserve, upload, commit, and restore.
  - `ACTIONS_CACHE_URL` and tokens.
- **12-artifact.yml**:
  - Artifact upload/download protocols (v4 results service).
  - Chunked uploads to signed blob URLs.
- **15-oidc-id-token.yml**:
  - OIDC token acquisition and audience handling.
  - `ACTIONS_ID_TOKEN_REQUEST_URL` / `TOKEN`.

### Phase 4: Diagnostics
- **14-annotations.yml**:
  - `::error::` / `::warning::` / `::notice::` parsing and mapping.
  - Problem matcher regex matching against log streams.

---

## 2. Execution Subsystems & Gaps

### Shell Parity
- Implement shell resolving for `sh`, `python`, `pwsh`, `cmd`, and Windows PowerShell.
- Match quoting rules and script file extensions per target OS/shell.

### Windows Support
- Path translation (`\` vs `/`).
- Environment variable case-insensitivity.
- Windows-specific process tree termination.
- Windows shell execution (`cmd.exe`, `powershell.exe`).

### Containers & Services
- Job-level docker containers (`container:`).
- Service container lifetime management.
- Network, volume, and port mappings.

### Live Logs & Twirp Results
- Websocket live logs (live console streaming).
- Progressive step status updates via Twirp.
- Signed blob upload for job/step logs.

### Cancellation & Hardening
- SIGINT/SIGTERM graceful cleanup.
- Process group signal propagation.
- Step timeout enforcement (`timeout-minutes`).

### Pre-bundled & Offline Support
- Skip-if-present check: check for existing `externals/node20/bin/node` before triggering dynamic download at configure time.
- Add `--offline` flag to `aksh-runner configure` to fail early if local `externals/` are missing, blocking any network fetch.
- Archive-level packaging: bundle the compiled binary and pre-downloaded Node binaries for the target OS/Arch into a single release archive (`aksh-runner-bundle-<os>-<arch>.tar.gz`).
---

## Legend & Status

- **GitHub Live Conformance**:
  - `06-multi-step.yml` — **Passed** ✅
  - Others — **Pending** ❌
- **Local aksh Parity**:
  - `06-multi-step.yml` — **Passed** ✅
  - Others — **Pending** ❌
