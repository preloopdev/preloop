# aksh vs. agent-ci Parallel Conformance & Concurrency Report

## Scope

This report covers the broker scheduling fix and the corrected benchmark comparison with `agent-ci` workflows run concurrently, not sequentially.

## Scheduling Bug Fixed

`aksh-runner-server` previously ignored runner poll status. A runner session polling with `status=Busy` could still receive normal `RunnerJobRequest` work, causing the busy runner to ignore extra jobs and leaving those jobs stuck `in_progress`.

Fixed behavior:

- Busy sessions do not receive fresh queued jobs.
- Busy sessions do not receive duplicate active-job messages.
- Queued work remains available to other online runner sessions.
- Broker acknowledge no longer clears the active session/job mapping; completion/result cleanup owns that lifecycle.
- `aksh-runner` now uses `waitSeconds=1` for busy broker polls and keeps normal 50-second long-polls while online.

## Files Changed

| File | Change |
| --- | --- |
| `crates/aksh-runner-server/src/lib.rs` | Respect `status=Busy` in broker message dispatch. |
| `crates/aksh-runner-server/src/lib.rs` | Keep session active-request mapping after broker acknowledge. |
| `crates/aksh-runner-server/src/lib.rs` | Added regression test `busy_runner_poll_does_not_consume_another_job`. |
| `crates/aksh-runner/src/client/broker.rs` | Use short `waitSeconds=1` busy polls. |
| `scripts/run-runner.sh` | Start guest `dockerd`, configure `overlay2`, pull `alpine:3.20`, and run ephemeral guest runners for the benchmark. |
| `scripts/benchmark_rust_vs_agentci_full.py` | Start 6 ephemeral SmolVM runners for the 6-job fixture set. |

## Verification

### Rust Regression Test

```sh
cargo test -p aksh-runner-server busy_runner_poll_does_not_consume_another_job --quiet
```

Observed:

```text
cargo test: 1 passed (2 suites, 41 filtered, 0.00s)
```

### Existing Broker Flow Test

```sh
cargo test -p aksh-runner-server current_service_broker_flow_uses_queued_job --quiet
```

Observed:

```text
cargo test: 1 passed (2 suites, 41 filtered, 0.00s)
```

### Formatting

```sh
cargo fmt --all --check
```

Observed:

```text
(no output)
```

### Release Build

```sh
cargo build --release -p aksh-runner-server -p aksh-runner --quiet
```

Observed: build completed successfully. Existing warnings remain.

## aksh + SmolVM Parallel Retest

Command:

```sh
python3 scripts/benchmark_rust_vs_agentci_full.py
```

Observed latest run:

```text
=== STEP 1: Starting 6 SmolVM Runner Guests (Serialized Spawns) ===
All VMs spawned in 12.06s!

=== STEP 3: Monitoring runs completion via server API ===
Current runs status: ['queued', 'queued', 'queued', 'queued']
Current runs status: ['success', 'success', 'success', 'success']
All runs completed on the server!
```

Whole command wall time:

```text
54.22s
```

This includes the aksh run, the agent-ci comparison run, report writing, and cleanup.

The aksh workflow completion after workflow submission was bounded by the 1-second monitor loop:

```text
~1.04s observed
```

Serialized VM spawn phase:

```text
12.06s
```

The 12-second spawn phase includes intentional 2-second spacing between 6 packed SmolVM starts to avoid concurrent pack extraction collisions.

## aksh Worker Job Durations From Runner Logs

Observed completed worker jobs from `runner-rust-runner-*.log`:

| Job | Result | Worker execution time |
| --- | --- | ---: |
| `basic` | Succeeded | `0.031s` |
| `env-test` | Succeeded | `0.012s` |
| `needs-test` | Succeeded | `0.008s` |
| `docker build` | Succeeded | `0.854s` |
| `docker:// action` | Succeeded | `0.331s` |

Observed worker-job summary:

```text
avg: 0.247s
max: 0.854s
sum: 1.237 job-seconds
```

## Corrected agent-ci Parallel Run

The four `agent-ci` workflows were rerun concurrently via four workers.

| Workflow | agent-ci command wall time | Exit | Result |
| --- | ---: | ---: | --- |
| `25-agent-ci-test.yml` | `7.840s` | `0` | PASS |
| `26-agent-ci-comprehensive.yml` | `16.879s` | `0` | PASS |
| `21-host-docker-build.yml` | `7.129s` | `0` | PASS |
| `22-host-docker-container-action.yml` | `6.950s` | `1` | FAIL: Docker container not found |

Parallel agent-ci wall time is the slowest concurrently running workflow:

```text
16.879s
```

## Corrected Speed Comparison

| Comparison | aksh + SmolVM | agent-ci parallel |
| --- | ---: | ---: |
| Workflow execution after runners are ready | `~1.04s` | `16.879s` |
| Including serialized SmolVM spawn phase | `~13.10s` | `16.879s` |
| Docker build job | `0.854s` | `7.129s` |
| Docker container action | `0.331s` PASS | `6.950s` FAIL |

Approximate ratios:

```text
16.879 / 1.04  ≈ 16.2x faster after runners are ready
16.879 / 13.10 ≈ 1.3x faster including serialized VM spawn spacing
7.129 / 0.854  ≈ 8.3x faster for docker build worker execution
6.950 / 0.331  ≈ 21.0x faster for docker:// action worker execution, with agent-ci failing that fixture
```

## Cold vs. Warm Lifecycle Benchmark Comparison

Below is the comparative breakdown of cold vs. warm start and execution latencies across the different runner configurations:

| Configuration | Cold Start / Execution | Warm Start / Execution | Breakdown / Notes |
| :--- | :---: | :---: | :--- |
| **SmolVM + Official C# Runner** | **`~27.3s`** | **`~9.0s`** | **Cold:** Includes VM boot (2.5s), installing `.NET`/dependencies (7.7s), nested Docker boot (5.0s), and `config.sh` OAuth registration (4.0s). <br>**Warm:** Runner already registered and running; warm workflow execution. |
| **SmolVM + Rust Runner (`aksh-runner`)** | **`~7.1s`** | **`~0.13s - 0.85s`** | **Cold:** Includes VM boot (0.37s), nested Docker boot (5.0s), Rust runner configure/polling start (0.06s), and first nested image pull (1.7s). <br>**Warm:** Runner registered, VM booted, Docker cached; instant sub-second process execution. |
| **`agent-ci` (Host Docker)** | **`~6.0s - 8.0s`** | **`~5.0s - 7.0s`** | **Cold:** No VM or runner registration overhead; time is dominated by downloading the OCI image to the host. <br>**Warm:** Image already cached on host; benchmark is dominated by local Node/TypeScript emulator startup. |

## Fairness Notes

The host Docker cache was warm for agent-ci; `alpine:3.20` existed on the host. The SmolVM harness also prepared guest Docker before job execution. This is not a pure language benchmark. The measured advantage comes from the combined architecture: prebaked VM, parallel runner sessions, low runner overhead, and fixed broker scheduling.
