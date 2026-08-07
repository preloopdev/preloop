# Architecture — aksh-runner

## Overview

`aksh-runner` is a Rust reimplementation of the official GitHub Actions runner (`actions/runner` v2.335.1). It speaks the same wire protocol so it can register with GitHub, poll for jobs, execute workflow steps, and report results.

## Two-process model

```
aksh-runner run (Listener)
  │
  ├── OAuth token acquisition (RS256 JWT client assertion)
  ├── Session creation (AzDO distributedtask or broker)
  └── Long-poll loop
        │
        └── On job message:
              │
              └── aksh-runner worker (Worker, child process)
                    ├── Reads job from stdin (NDJSON)
                    ├── Sets up workspace, contexts, env
                    ├── Executes steps sequentially
                    ├── Reports results to server
                    └── Exits (0 = success, 1 = infra failure)
```

The listener spawns `aksh-runner worker` as a child process per job, communicating via stdin NDJSON. This mirrors the official Listener/Worker process split for crash isolation and kill-on-cancel.

## IPC framing (listener → worker, stdin)

One JSON object per line:

```json
{"t":"job","body":<AgentJobRequestMessage>}
{"t":"cancel","timeout_secs":300}
{"t":"shutdown"}
```

Worker exit codes:
- `0` — job executed and reported (regardless of job result)
- `1` — infrastructure failure before/while reporting

No worker→listener channel; the worker reports to the server directly.

## Protocol paths

| Path | Endpoints | When used |
|------|-----------|-----------|
| **Broker** (default, `--via broker`) | `/runner/session`, `/runner/message`, `acquirejob`/`renewjob`/`completejob`, Twirp results | GitHub.com (v2.335.1 current) |
| **AzDO** (`--via azdo`) | `_apis/v1/sessions`, `_apis/v1/messages`, Timeline/Logfiles/FinishJob | Local aksh, GHES |

## Module map

```
crates/aksh-runner/src/
├── main.rs              CLI entry point
├── lib.rs               Crate root, module declarations
├── cli.rs               Clap argument definitions
├── settings.rs          .runner/.credentials/.credentials_rsaparams persistence
├── configure.rs         Registration and removal flows
├── process.rs           ProcessInvoker (command-group spawn/kill)
├── client/
│   ├── http.rs          Shared reqwest client (CA bundle, proxy)
│   ├── azdo.rs          AzDO distributedtask API client
│   ├── broker.rs        Broker API client
│   ├── run_service.rs   Run-service client (acquire/renew/complete)
│   ├── results.rs       Results service client (Twirp)
│   └── actions_download.rs  Action tarball resolution/download
├── listener/
│   ├── mod.rs           Listener entry point (run_listener)
│   ├── oauth.rs         OAuth JWT token acquisition
│   ├── message_listener.rs  AzDO message polling loop
│   ├── broker_listener.rs   Broker message polling loop
│   └── job_dispatcher.rs    Worker process spawning
└── worker/
    ├── mod.rs           Worker entry point (stdin NDJSON)
    ├── job_runner.rs    Job execution orchestration
    ├── contexts.rs      GitHub/runner/job/steps/env contexts
    ├── execution_context.rs  Per-step context, masking, annotations
    ├── steps_runner.rs  Sequential step execution with conditions
    ├── template.rs      ${{ }} expression evaluation in step fields
    ├── commands.rs      Workflow command parser (::name::data)
    ├── file_commands.rs GITHUB_ENV/PATH/OUTPUT/STATE/STEP_SUMMARY
    ├── job_extension.rs Workspace setup, env injection, step ordering
    ├── server_queue.rs  Background reporting queue
    ├── matchers.rs      Problem matcher support
    ├── container_ops.rs Docker container management
    ├── handlers/
    │   ├── script.rs    Inline run: step handler
    │   ├── action.rs    uses: step dispatcher
    │   ├── node.rs      Node.js action handler
    │   ├── composite.rs Composite action handler
    │   ├── container.rs Docker action handler
    │   └── factory.rs   Action manifest parser
    └── actions/
        └── manager.rs   Action download/extraction
```

## Source of truth

**GitHub's real Actions service — never aksh.** Golden MITM captures of the official runner v2.335.1 talking to GitHub (`.runner-watch/golden/v2.335.1/`) are the wire-level oracle. When the Rust runner works against GitHub but fails against aksh, that is an aksh fidelity bug.

## Dependencies

Reuses from the workspace:
- `aksh-gha-protocol` — wire DTOs, session crypto (RSA-OAEP, AES-CBC, RS256 JWT signing)
- `aksh-gha-expressions` — `${{ }}` expression evaluation
- `aksh-gha-parser` — action manifest parsing

New deps: `command-group` (process-tree kill without unsafe), `flate2`+`tar` (action tarball extraction), `hostname`, `glob`.
