# Fidelity Audit — aksh-runner vs actions/runner v2.335.1

## Size comparison

| Metric | aksh-runner (Rust) | Official runner (C#/.NET) |
|--------|-------------------|--------------------------|
| Binary / dir size | 5.3 MB | 435 MB |
| Cold start (`--version`) | 4 ms | ~200 ms |

## Component fidelity scorecard

| Component | Status | Notes |
|-----------|--------|-------|
| **Listener** | | |
| Configuration (configure/remove) | ✅ | RSA keypair gen, .runner/.credentials/.credentials_rsaparams persistence |
| OAuth (RS256 JWT client assertion) | ✅ | Hand-rolled RS256, token caching |
| AzDO session + message poll | ✅ | AES-CBC decryption, long-poll with retry/backoff |
| Broker session + message poll | ✅ | Broker endpoints, message acknowledgment |
| Signal handling (SIGINT/SIGTERM) | ✅ | Clean session deletion on shutdown |
| Ephemeral / --once | ✅ | Exit after first job |
| Self-update | ❌ | Intentionally no-op'd (logged, not crashed) |
| Service install | ❌ | Deferred (Windows-only feature) |
| **Worker** | | |
| Job dispatch (child process spawn) | ✅ | stdin NDJSON IPC, process-group kill |
| Run-service acquire/renew/complete | ✅ | Broker path (GitHub-current) |
| Results service (Twirp step updates) | ✅ | WorkflowStepsUpdate, signed blob log upload |
| AzDO reporting (Timeline/Logfiles/FinishJob) | ✅ | Legacy compat via --via azdo |
| Execution contexts (github/runner/job/steps/env) | ✅ | Built from contextData + local state |
| Secret masking | ✅ | maskHints + isSecret variables + add-mask |
| Template evaluation ($\{\{ \}\}) | ✅ | Runner-side expression evaluation |
| Steps runner (conditions, continue-on-error, timeout) | ✅ | success()/failure()/cancelled() semantics |
| Process invoker | ✅ | command-group for process-tree kill |
| Script handler (bash/sh/python/pwsh) | ✅ | Shell resolution matching upstream |
| Workflow commands (::name::data) | ✅ | Full command set with unescaping |
| Legacy commands (##[name]) | ✅ | set-output/save-state with deprecation warnings |
| File commands (GITHUB_ENV/PATH/OUTPUT/STATE/SUMMARY) | ✅ | KEY=VAL and heredoc parsing |
| GITHUB_* environment injection | ✅ | Full set from contextData + runner paths |
| Workspace layout (_work/{n}/) | ✅ | _temp, _actions, _tool directories |
| Action download (tarball extraction) | ✅ | Strip top-level dir, flate2+tar |
| Action manifest parsing | ✅ | node/composite/docker, inputs/outputs, pre/post |
| Node.js action handler | ✅ | INPUT_* env, externals/node20/node24 |
| Composite action handler | ✅ | Nested steps, input context, GITHUB_ACTION_PATH |
| Docker action handler | ✅ | docker:// direct, Dockerfile build |
| Pre/post step ordering | ✅ | Pre: declared order, Post: LIFO, conditions |
| Container ops (job/service containers) | ✅ | Docker CLI wrapper, network, volumes, path translation |
| Problem matchers | ✅ | JSON-based matcher registry, single-pattern matching |
| Cancellation (JobCancellation → kill) | ✅ | IPC cancel message, process-group kill |
| Background reporting queue | ✅ | Batched step updates and log uploads |
| OIDC token | ⚠️ | Env plumbing only (ACTIONS_ID_TOKEN_REQUEST_URL/TOKEN) |
| Websocket live logs | ❌ | HTTP buffered upload only; logs appear at step completion |
| DAP debugger | ❌ | Deferred |
| Snapshot | ❌ | Deferred |
| Job hooks (ACTIONS_RUNNER_HOOK_*) | ❌ | Deferred |
| Background steps coordinator | ❌ | Deferred |
| **OS** | | |
| macOS | ✅ | Primary development platform |
| Linux | ✅ | Supported |
| Windows | ❌ | Deferred (cmd/powershell handlers, path semantics) |

## Legend

- ✅ Implemented and tested
- ⚠️ Partial (env plumbing only, or known limitations)
- ❌ Not implemented (intentionally deferred)
