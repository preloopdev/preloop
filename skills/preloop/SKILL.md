---
name: preloop
description: 'Run and debug Preloop local CI — GitHub Actions-compatible workflows executed in one isolated microVM per job (smolvm/libkrun). Use when the user asks to run or plan a workflow locally, submit CI, debug a failed job, retry a failed step in its live VM, open a shell in a preserved VM, manage the local runner pool or GitHub credentials, or work with the preloop CLI, preloopd, or the aksh control plane. Triggers: "preloop run", "run CI locally", "run this workflow", "CI failed", "debug the failure", "retry the step", "agent loop", "smolvm runner".'
---

# Preloop

Preloop is a macOS-first local/self-hosted/managed CI platform. It reimplements the
GitHub Actions control plane (`aksh`) and a faithful Rust runner, and executes each
job in an isolated Linux microVM via **smolvm** (libkrun) on Apple Silicon, with
Firecracker as the scale-tier executor. Workflows are drop-in GitHub Actions YAML;
the unmodified official runner also works against aksh.

Source of truth for this skill: the CLI at `crates/preloop-cli/src/main.rs` (current
subcommands only — docs under `preloop/docs/` are partly aspirational; verify a
command exists before using it).

## Non-negotiables (trust model)

- **One isolated microVM per job.** No host Docker socket as a core dependency.
- **No real secrets in untrusted jobs by default.** Secrets are reference-based:
  `preloop secret list` returns names only, never values.
- **No silent fidelity gaps.** Unsupported workflow behavior must be explicit and
  machine-readable, never silently approximated.
- **Dirty retry is not a clean verdict.** After fixing a failure in a live VM, the
  final "fixed" call requires a clean run from a fresh VM/overlay with strict policy.

## Agent core loop

```text
preloop run -f <workflow>        # run; on failure a TTY run pauses the VM
preloop debug --json             # read the paused session as JSON (agents/scripts)
<edit source on the host>
preloop debug --verdict retry --sync --from <step>
                                 # sync host edits into the VM, re-run from that step
preloop debug --verdict continue # keep going from the paused step
preloop debug --verdict abort    # tear down, mark failed
```

Interactive `preloop run` **pauses** at a failed step and holds the microVM open so
you can fix and retry from that step. Non-interactive runs (`--detach`, piped
stdout, CI) never pause; pass `--preserve-on-failure` to keep the failed VM alive
for a later `preloop shell`.

## Command reference (current CLI)

### Run workflows

```text
preloop run [-f <path>] [--job <id>] [--event <trigger>] [--payload <file.json>]
            [--base <ref>] [--secret NAME=VALUE] [-d|--detach] [--no-debug]
            [--preserve-on-failure]
```

- `-f` — workflow file. A bare filename like `ci.yml` resolves inside
  `.github/workflows/`; a path is used as-is. When omitted, discovers workflows
  whose triggers match the current repo state/event.
- `--job` — single job by its YAML key; includes the `needs:` dependency closure.
- `--event` — simulate a trigger (`push`, `pull_request`, `merge_group`, …);
  `--payload` supplies a webhook body JSON; `--base` sets the base ref for
  `pull_request`/`merge_group`.
- `--detach` — submit and return without streaming events.
- `--no-debug` — tear down on failure instead of pausing (default when detached).

### Inspect and control

```text
preloop plan        # expanded job DAG + matrix, no execution
preloop status      # active and recent runs
preloop logs        # run logs
preloop cancel      # cancel current run
```

### Debug a failed job

```text
preloop shell [session-id|run-id|job-name]        # PTY into the preserved VM
preloop debug [session] --json                    # session as JSON, exit (agents)
preloop debug [session] --verdict retry [--sync] [--force]
              [--from <step>|--from-start] [--revert none|untracked|all]
preloop debug [session] --verdict continue|abort
preloop debug [session] --export [--patch-only]   # pull VM-side edits back to host
```

- `--sync` — copy host source changes into the VM before retrying. A file changed
  on both sides aborts unless `--force`.
- `--from <step>` — re-run from a 1-based step number or display name (must be at
  or before the failed step); `--from-start` re-runs from the first user step.
- `--revert` — undo the failed attempt's workspace debris: `none` (default),
  `untracked`, or `all`.
- `--export` — bring edits made inside the VM back to the host workspace.

### Secrets and GitHub credentials

```text
preloop secret set <NAME> [--value V | stdin] [--repo owner/repo]
preloop secret list [--repo owner/repo]           # names only, never values
preloop secret rm <NAME> [--repo owner/repo]
preloop setup github                              # GitHub App or fine-grained PAT
preloop doctor                                    # verify credential config
```

### Serve and update

```text
preloop serve [--listen ADDR] [--public-url URL] [--github-app-id ID]
              [--github-app-key PATH] [--github-app-installation-id ID]
              [--webhook-secret SECRET] [--save]
preloop update   # poll GitHub Releases, atomically install matching binary
```

`serve` is the self-hosting entry point: control plane + microVM runner pool in the
foreground, serving the GitHub webhook and Checks endpoints. `--public-url` must be
the URL GitHub and remote runners can actually reach. Hidden alias: `engine`.

## Environment

| Variable | Meaning |
|---|---|
| `AKSH_URL` | server base URL; default `http://127.0.0.1:9090` |
| `AKSH_TOKEN` | API token for the server |
| `PRELOOP_HOME` | state dir; default `~/.preloop` |
| `PRELOOP_LISTEN` / `PRELOOP_PUBLIC_URL` | override `serve --listen` / `--public-url` |
| `PRELOOP_RUNNER_LABELS` | comma-separated extra `runs-on` labels (e.g. declare `X64` on an ARM host) |
| `PRELOOP_RUNNER_POOL_SIZE` | override warm runner pool size (memory-bounded, cap 8) |
| `PRELOOP_RUNNER_BASE_IMAGE` / `PRELOOP_RUNNER_BUNDLE` / `PRELOOP_RUNNER_DNS` / `PRELOOP_RUNNER_OVERLAY_GB` / `PRELOOP_RUNNER_NAME_PREFIX` / `PRELOOP_RUNNER_POOL_ENABLED` / `PRELOOP_USE_FORK` / `PRELOOP_USE_PACKED_GOLDEN` / `PRELOOP_WORKSPACE` | runner pool / VM tuning knobs (full semantics in `crates/preloop-cli/src/main.rs`) |

Runner execution defaults: packed goldens enabled, warm pool disabled, one
single-use VM provisioned per queued job, and concurrency capped by host CPU.
Each VM receives 4 vCPUs and a 4096 MiB ballooned memory ceiling. Set
`PRELOOP_RUNNER_POOL_ENABLED=true` to keep warm runners (size is memory-bounded,
cap 8), or `PRELOOP_USE_PACKED_GOLDEN=false` to force cold OCI provisioning.

## Failure classification (from `preloop/docs/09_agent_loop_retry_and_fork.md`)

Classify failures before retrying — do not burn loops on the wrong fix:

| Classification | Suggested action |
|---|---|
| `test_failure` / `compile_failure` | edit source, retry step |
| `missing_secret` | request/update policy, don't hack around it |
| `network_blocked` | inspect the egress allowlist/policy |
| `cache_miss` | continue, or warm the cache |
| `resource_oom` / `disk_quota` | raise resources or fix the test |
| `timeout` | optimize or raise the timeout |
| `infra_failure` | retry job or report a bug |
| `fidelity_unsupported` | unsupported workflow feature — record the gap, don't fake it |

## Developing Preloop itself (this repo)

```sh
just build-preloop           # macOS CLI + ARM64 Linux runner (zigbuild)
just preloop-run WF=fixtures/workflows/failing.yml   # run, pause on failure
just preloop-run-detached WF=...                     # submit, keep VM preserved
just preloop-shell           # shell into the most recent preserved VM
just serve / serve-dev       # aksh control plane on 127.0.0.1:9090
just test-ci                 # fmt-check + clippy + tests (the full gate)
just dogfood                 # E2E with the real official runner
just conform / conform-server-deep   # protocol conformance gates
```

Key paths: `crates/preloop-cli/src/main.rs` (CLI), `crates/preloop-orchestrator/`,
`crates/preloop-vm/` (smolvm provider), `preloop/docs/00_index.md` (design docs),
`fixtures/workflows/` (sample workflows), `goals/` (agent CI benchmarks).

## Verification rule

Never report "fixed" on a dirty retry alone. The final verdict must come from a
clean run: fresh VM, clean overlay, strict network/secrets/cache policy. The repo
gate is `just test-ci` plus `just conform` for protocol fidelity.
