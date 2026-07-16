# Agent Loop, Retry, and Forking

## Goal

Preloop should give agents a fast, safe feedback loop:

```text
run -> fail -> compact structured failure -> edit -> retry -> verify clean
```

The agent loop is a product feature, not just a CLI wrapper around CI.

## Step transaction model

Every step should produce a transaction record:

```json
{
  "run_id": "run_123",
  "job_id": "test",
  "step_id": "pytest",
  "attempt": 2,
  "engine": "aksh",
  "vm_id": "vm_456",
  "checkpoint_id": "ckpt_789",
  "cwd": "/workspace",
  "env_digest": "sha256:...",
  "path_digest": "sha256:...",
  "source_digest": "sha256:...",
  "cache_digest": "sha256:...",
  "exit_code": 1,
  "classification": "test_failure",
  "annotations": [],
  "files_written": [],
  "network_denials": [],
  "cache_events": [],
  "retry": {
    "recommended": "retry-step",
    "available": ["retry-step", "retry-from-checkpoint", "retry-job", "retry-clean"]
  }
}
```

## Failure classifications

Classify failures so agents do not waste loops:

| Classification | Example | Suggested action |
|---|---|---|
| `test_failure` | assertion failed | inspect annotations/logs, edit code |
| `compile_failure` | compiler error | edit source, retry-step |
| `missing_secret` | secret denied | request policy change or skip |
| `network_blocked` | egress denied | inspect allowlist/policy |
| `cache_miss` | dependency download slow | continue or warm cache |
| `resource_oom` | memory limit hit | increase resource or fix test |
| `disk_quota` | upper layer full | switch upper mode/increase quota |
| `timeout` | step timed out | optimize or increase timeout |
| `infra_failure` | VM/runner protocol issue | retry-job or report bug |
| `fidelity_unsupported` | unsupported workflow feature | run official/fallback or mark gap |

## Retry ladder

| Retry | Meaning | Fidelity | Speed | Use |
|---|---|---:|---:|---|
| `retry-step` | re-run failed step in same live VM after edits | medium | fastest | default local agent loop |
| `retry-from-checkpoint` | restore a `machine fork`/snapshot and run from the step boundary | high | fast | state drift suspected |
| `retry-job` | re-run the whole job from a warm `.smolmachine` (cache intact) | high | medium | before wider confidence |
| `retry-clean` | new VM, clean overlay, strict policy | highest | slowest | final verification |
| `fork-from-failure` | N `machine fork` CoW clones from the failed VM, trying fixes in parallel | high | fast-ish | speculative agents |

## Fork-from-failure semantics

smolvm provides real snapshot/fork primitives, so this is more than "re-exec from
a step boundary." In Preloop terms:

```text
fork-from-failure = smolvm `machine fork` (CoW clone, macOS/Linux) from the failed VM,
                    or resume from a `pack create --from-vm` .smolmachine snapshot
```

Each branch starts from the same warm on-disk state, cache, and workflow
progress. This is on-disk state, not live process RAM; Windows has no
fork/snapshot, so fall back to rebuilding from the warm image.

The same failure checkpoint can be handed off remotely: pack the failed VM and
resume the agent loop on a smolvm-KVM host (see
[doc 14](14_runtime_tiers_and_portable_handoff.md)). Secrets are re-resolved
remotely and never travel in the pack.

## Agent event stream

Emit versioned NDJSON events:

```json
{"v":1,"type":"preloop.run.started","run_id":"run_123"}
{"v":1,"type":"preloop.vm.booted","vm_id":"vm_456","ms":182}
{"v":1,"type":"preloop.step.started","job_id":"test","step_id":"pytest"}
{"v":1,"type":"preloop.step.failed","job_id":"test","step_id":"pytest","exit_code":1,"classification":"test_failure"}
{"v":1,"type":"preloop.shell.available","command":"preloop shell run_123 --job test"}
{"v":1,"type":"preloop.retry.recommended","level":"retry-step"}
```

Required event types:

```text
preloop.run.started
preloop.run.completed
preloop.job.started
preloop.job.completed
preloop.step.started
preloop.step.completed
preloop.step.failed
preloop.vm.booted
preloop.vm.paused
preloop.vm.reaped
preloop.shell.available
preloop.network.denied
preloop.secret.denied
preloop.cache.hit
preloop.cache.miss
preloop.cache.write_quarantined
preloop.artifact.uploaded
preloop.policy.warning
preloop.retry.started
preloop.retry.completed
preloop.verify.required
```

## Failure bundle

The agent should receive a compact bundle, not raw megabytes of logs:

```json
{
  "summary": "3 tests failed in parser module",
  "top_errors": [
    {"file":"tests/parser_test.rs","line":42,"message":"expected Ident, got EOF"}
  ],
  "last_log_lines": [],
  "annotations": [],
  "changed_files_hint": [],
  "retry_command": "preloop retry --step pytest",
  "shell_command": "preloop shell run_123 --job test",
  "strict_verify_command": "preloop verify --clean --strict"
}
```

## Shell attach

Debug shell uses smolvm `machine shell` (vsock PTY, auto-starts the VM), not SSH.

```text
preloop shell <run-id> --job test
```

Requirements:

- no guest network required,
- no SSH keys,
- PTY resize support,
- clear trust warnings,
- command logging for audit if managed/self-hosted,
- disabled or admin-gated for managed untrusted tenants.

## Final verification rule

Agents should not be allowed to declare success after only dirty step retry.

Before final “done”:

```text
preloop verify --clean --strict
```

Strict verify should:

- use fresh VM,
- use source snapshot,
- avoid dirty live overlay,
- apply stricter network/secrets/cache policy,
- produce final fidelity score,
- and fail if unsupported high-risk workflow features are present.

## Agent MCP tools

Expose tools:

```text
preloop.run
preloop.status
preloop.logs
preloop.failure_bundle
preloop.retry
preloop.verify
preloop.shell
preloop.doctor
preloop.policy_explain
```

Each tool should be deterministic, typed, and safe to call from an autonomous agent.

## Acceptance tests

- Failing step emits structured `preloop.step.failed` under 100 ms after exit.
- Agent can edit host file and `retry-step` sees the edit.
- Overlay stale copy-up is invalidated or detected.
- `retry-clean` catches stateful false greens.
- Network denial produces a classified event.
- Missing secret produces a classified event, not a confusing test failure.
- Shell attach works without SSH/network.
- Fork-from-failure creates independent branches from the same checkpoint.
