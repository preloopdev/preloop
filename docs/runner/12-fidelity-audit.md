# Fidelity Audit — aksh-runner vs actions/runner v2.335.1

Corrected **2026-07-02** after a full-code audit (all modules diffed against the goldens, the plan, and upstream semantics). Earlier revisions of this scorecard overstated status; every row below is now backed by code-level evidence. Pending items are tracked as **F0xx** in [`runner_fidelity_gap.md`](runner_fidelity_gap.md) and prioritized in [`roadmap.md`](roadmap.md).

**No Tier-1 (live GitHub) conformance report exists yet** — the H1/H2 harness (`runner-e2e`, `runner-diff`, `--record-flows`) is unbuilt and `.runner-watch/runner-conformance/` is empty. A ✅ below means "code verified correct against golden captures/upstream semantics by audit", not "gate passed".

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
| Configuration (configure/remove) | ✅ | Verified vs golden 01 (F002–F007 fixed); `--replace` doesn't DELETE existing agent |
| OAuth (PS256 JWT client assertion) | ✅ | F001 fixed; token caching; verified vs golden 01 |
| AzDO session + message poll | ✅ | AES-CBC decryption, long-poll |
| Broker session + message poll | ✅ | F009–F011, F016–F017 fixed; verified vs golden 01. Broker URL from connectionData still pending (F008) |
| Signal handling (SIGINT/SIGTERM) | ⚠️ | Session cleanup on exit; no graceful-cancel of an active job; no 401 session recreate (F033) |
| Ephemeral / --once | ⚠️ | Exits after first job but never unregisters the agent (F033) |
| Retry/backoff (ErrorThrottler) | ❌ | No HTTP call site retries transient errors (F033) |
| Self-update | ❌ | Intentionally no-op'd (logged, not crashed) |
| Service install | ❌ | Deferred |
| **Worker — job lifecycle** | | |
| Job dispatch (child process spawn) | ✅ | stdin NDJSON IPC, process-group kill (F015 fixed) |
| acquirejob / completejob | ✅ | Wire shapes verified vs golden 06 (F012–F014 fixed) |
| renewjob lock renewal | ❌ | Client method exists, **never called** (F018) |
| Twirp step status updates | ❌ | Body shape correct, **queue never flushed — nothing sent** (F019) |
| Signed-blob log upload | ❌ | Client exists, **never called — no logs reach GitHub** (F020) |
| Background reporting queue | ❌ | `ServerQueue` correct but never instantiated (F019) |
| AzDO reporting (Timeline/Logfiles/FinishJob) | ❌ | Client methods have 0 call sites; FinishJob wrong DTO (F030) |
| **Worker — execution** | | |
| Execution contexts (github/runner/job/steps/env) | ⚠️ | Roots correct; missing `secrets` root (F028), `runner.tool_cache`/`workspace`, `job.container`/`services` |
| Secret masking | ⚠️ | Literal replace only; no encoded variants; not applied at upload boundary (F028) |
| Template evaluation (`${{ }}`) | ⚠️ | Works; expression engine lacks bracket access, `a.*.b`, real `hashFiles` (F027) |
| Steps runner (conditions, continue-on-error, timeout) | ⚠️ | Correct for sequential run; post-cancel `always()`/post continuation missing; no job-level timeout (F031) |
| Process invoker | ✅ | command-group process-tree kill |
| Script handler (bash/sh/python/pwsh) | ✅ | Shell command lines match upstream exactly |
| Workflow commands (`::name::data`) | ⚠️ | Parsing/unescaping correct; `add-matcher`/`remove-matcher` unwired, echo state untracked, `##[debug]` not emitted |
| Legacy commands (`##[name]`) | ✅ | set-output/save-state with deprecation warnings |
| File commands (ENV/PATH/OUTPUT/STATE/SUMMARY) | ⚠️ | Parsing correct; **summary never uploaded** (F035); state never reaches post steps (F023) |
| GITHUB_* environment injection | ⚠️ | 28/39 of official set (F034) |
| ACTIONS_* runtime env (cache/artifacts/OIDC) | ❌ | **Never injected** — breaks cache/artifact/OIDC actions (F021) |
| Workspace layout (_work, _temp, _actions, _tool) | ✅ | |
| Annotations upload | ❌ | Collected but completejob sends `[]`; StepUpdate has no annotations field (F025) |
| **Worker — actions** | | |
| Action resolution (runnerresolve batch) | ❌ | Official endpoint not implemented; api.github.com tarball fallback with unresolved refs (F022) |
| Action download (tarball extraction) | ⚠️ | Extract/strip correct; wrong source endpoint, no SHA-resolved layout (F022) |
| Action manifest parsing | ⚠️ | Structure parsed; `outputs.*.value`, `deprecationMessage`, pre/post-if defaults missing |
| Node.js action handler | ⚠️ | INPUT_*/externals work; INPUT precision, NODE_OPTIONS, unsecure-node checks missing |
| Composite action handler | ⚠️ | Nested steps run; **outputs never evaluated, pre/post not hoisted, no depth cap** (F024) |
| Docker action handler | ⚠️ | docker:// + Dockerfile basics; entrypoint/args from manifest not wired |
| Pre/post step lifecycle | ❌ | **No discovery, no pre list, no LIFO post, no state context** (F023) |
| Container ops (job/service containers) | ❌ | Helpers are dead code; never wired into job flow; services unimplemented (F026) |
| Problem matchers | ❌ | Registry exists but zero call sites; no multi-line loop patterns (F032) |
| Cancellation (JobCancellation → kill) | ⚠️ | Current step killed (F015); remaining/post-step semantics + grace kill missing (F031) |
| OIDC token | ❌ | Requires F021 env plumbing (nothing else runner-side) |
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
