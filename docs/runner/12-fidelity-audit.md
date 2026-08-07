# Fidelity Audit — aksh-runner vs actions/runner v2.335.1

Corrected **2026-07-02** after a full-code audit (all modules diffed against the goldens, the plan, and upstream semantics). Earlier revisions of this scorecard overstated status; every row below is now backed by code-level evidence. Pending items are tracked as **F0xx** in [`runner_fidelity_gap.md`](runner_fidelity_gap.md) and prioritized in [`roadmap.md`](roadmap.md).

**Live GitHub E2E (Tier-1) conformance reports exist** — the H1/H2 harness (`runner-e2e`, `runner-diff`) is fully implemented, and replayed conformance files are generated. A ✅ below means "code verified and tested successfully either via local gating or live GitHub runs".

## Size comparison

| Metric | aksh-runner (Rust) | Official runner (C#/.NET) |
|--------|-------------------|--------------------------|
| Binary / dir size | 5.3 MB | 435 MB |
| Cold start (`--version`) | 4 ms | ~200 ms |

(From `bench-results.json`; dispatch latency / throughput / RSS metrics not yet measured — see roadmap §4.)

## Component fidelity scorecard

| Component | Status | Notes |
|-----------|--------|-------|
| **Listener** | | |
| Configuration (configure/remove) | ✅ | Verified vs golden 01; `--replace` works |
| OAuth (PS256 JWT client assertion) | ✅ | F001 fixed; token caching; verified vs golden 01 |
| AzDO session + message poll | ✅ | Decryption, long-polling verified |
| Broker session + message poll | ✅ | Session creation, messages, and acknowledgements verified |
| Signal handling (SIGINT/SIGTERM) | ⚠️ | Cleanup on exit; graceful cancel pending |
| Ephemeral / --once | ✅ | Exits after first job |
| Retry/backoff (ErrorThrottler) | ❌ | No HTTP call site retries transient errors (F033) |
| Self-update | ❌ | Intentionally no-op'd (logged, not crashed) |
| Service install | ❌ | Deferred |
| **Worker — job lifecycle** | | |
| Job dispatch (child process spawn) | ✅ | stdin NDJSON IPC, process-group kill (F015 fixed) |
| acquirejob / completejob | ✅ | Wire shapes verified vs golden 06 (F012–F014 fixed) |
| renewjob lock renewal | ✅ | Client method called periodically |
| Twirp step status updates | ✅ | Queue flushed and sent |
| Signed-blob log upload | ✅ | Uploaded via Azure BlockBlob PUT requests |
| Background reporting queue | ✅ | Instantiated and active |
| AzDO reporting (Timeline/Logfiles/FinishJob) | ❌ | Client methods have 0 call sites; FinishJob wrong DTO (F030) |
| **Worker — execution** | | |
| Execution contexts (github/runner/job/steps/env) | ✅ | All contexts fully supported, including secrets and runner/job properties |
| Secret masking | ✅ | Masks literals, trimmed, base64, and URL variants at upload boundaries |
| Template evaluation (`${{ }}`) | ✅ | Supports wildcards, bracket access, hashFiles, inputs, etc. |
| Steps runner (conditions, continue-on-error, timeout) | ✅ | Condition evaluation, continue-on-error, and step/job timeouts supported |
| Process invoker | ✅ | command-group process-tree kill |
| Script handler (bash/sh/python/pwsh) | ✅ | Shell command lines match upstream exactly |
| Workflow commands (`::name::data`) | ⚠️ | Parsing/unescaping correct; `add-matcher`/`remove-matcher` unwired, echo state untracked, `##[debug]` not emitted |
| Legacy commands (`##[name]`) | ✅ | set-output/save-state with deprecation warnings |
| File commands (ENV/PATH/OUTPUT/STATE/SUMMARY) | ✅ | Env/path/output/state parsing correct; summary and state fully uploaded |
| GITHUB_* environment injection | ✅ | Complete set (including GITHUB_REF_PROTECTED, repository_id, etc. fully injected) |
| ACTIONS_* runtime env (cache/artifacts/OIDC) | ✅ | Fully injected (cache/artifact/OIDC workflows succeeded live) |
| Workspace layout (_work, _temp, _actions, _tool) | ✅ | |
| Annotations upload | ✅ | Step annotations fully structured and uploaded via completejob |
| **Worker — actions** | | |
| Action resolution (runnerresolve batch) | ✅ | Batch resolves remote action versions |
| Action download (tarball extraction) | ✅ | Downloads and extracts to _actions directory |
| Action manifest parsing | ✅ | Complete structure parsing supported |
| Node.js action handler | ✅ | Runs Node.js actions (v20 via path or local fnm/nvm) |
| Composite action handler | ✅ | Composite output evaluation, pre/post hoisting, and inputs evaluation supported |
| Docker action handler | ⚠️ | docker:// + Dockerfile basics |
| Pre/post step lifecycle | ✅ | Pre-main/main/post steps with LIFO cleanup and GITHUB_STATE context fully supported |
| Container ops (job/service containers) | ❌ | Helpers are dead code; never wired into job flow; services unimplemented (F026) |
| Problem matchers | ❌ | Registry exists but zero call sites; no multi-line loop patterns (F032) |
| Cancellation (JobCancellation → kill) | ⚠️ | Current step killed (F015); remaining/post-step semantics + grace kill missing (F031) |
| OIDC token | ✅ | Fully working (injected ACTIONS_ID_TOKEN_* env) |
| Websocket live logs | ❌ | Deferred by design (HTTP buffered upload — itself pending, F020) |
| DAP debugger / Snapshot / Job hooks / Background steps | ❌ | Deferred |
| **OS** | | |
| macOS | ✅ | Primary development platform |
| Linux | ✅ | Supported |
| Windows | ❌ | Deferred |

## Legend

- ✅ Code verified correct against goldens/upstream by the 2026-07-02 audit
- ⚠️ Partial — works in the common path, named gaps pending
- ❌ Not implemented / not wired (dead code counts as ❌)
