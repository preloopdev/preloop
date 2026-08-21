# Retry, backoff, and pool memory bounds

How preloop retries failures, when it backs off, and how the runner pool
keeps itself inside the host's memory budget. Written after the 2026-08-20
outage, where a 22 GiB production host OOMed because on-demand fork
concurrency was sized by CPU only.

## Retry philosophy, in one line

> **Server: retry-once-and-coordinate** — the control plane avoids in-process
> retry loops almost entirely, leaning on GitHub redelivery, runner
> long-polls (bounded by `waitSeconds`), and periodic reaper sweeps.
> **Runner: mirror the official runner** — bounded 3-attempt exponential HTTP
> retries, jittered session backoff (15–60 s) reset on success, bounded lease
> renewal; listener/session loops and the container-health poll retry forever
> by design (the guest is expected to recover).
> **Pool: retry-forever with exponential damping** — slot respawns back off
> 500 ms → 30 s cap with success reset, golden re-arm exponential (10 s → 60 s,
> ≤ 5 min budget) before fallback
> before fallback.

## Control plane (`preloop-runner-server`)

| Site | What | Policy | Bounded? |
|---|---|---|---|
| `github.rs` `resolve_check_run_token` | App check-run token mint, transient 422 | 2 attempts, 500 ms fixed | yes |
| `github_app.rs` `mint_for_repository` | Installation token mint, 422 ungranted scope | one re-mint with clamped permissions | yes |
| `store.rs` `checkpoint_wal` | SQLite `wal_checkpoint(TRUNCATE)` blocked | 10 × 10 ms (blocking sleep), then bail | yes (~100 ms) |
| `debug.rs` `pump_axum_ws_to_dap` | DAP bridge WebSocket connect | 50 × 200 ms (≤10 s) | yes |
| `snapshots.rs` `acquire_cache_lock` | Object-cache dir lock | 25 ms poll, stale >60 s force-removed, 10 s deadline | yes |
| `broker.rs` / `distributed_task.rs` | Job-claim long poll | `loop` until `wait` deadline (default 50 s), wakes on notify ≤3 s slices | yes |
| `debug_sessions.rs` `poll_verdict` / `agent_events` | Worker verdict / events long poll | `loop` until 25 s cap (`VERDICT_POLL_MAX`) | yes |
| `bootstrap.rs` `run_background_reaper` | Stalled job/session sweep | 10 s interval, process lifetime | wall-time unbounded, periodic |
| `actions.rs` / `state.rs` | Action ref → SHA resolution | negative cached 60 s, positive 300 s; re-attempt after TTL | TTL-bounded |
| `broker.rs` `broker_acquire_job` | Dispatch token mint refusal (error policy) | **deliberately no retry** — config fault, job failed terminally | n/a |

GitHub webhooks are **not retried in-process**: the reservation is released
on failure so GitHub's own redelivery is accepted (dedup window 300 s).

## Pool (`preloop-orchestrator`)

| Site | What | Policy | Bounded? |
|---|---|---|---|
| `run_on_demand` slot supervisor | Re-spawn failed slots | exp 500 ms → 30 s cap, reset to 0 on success | attempts unbounded, sleep ≤30 s |
| `run_on_demand_slot` provision failure | Fork/create failed | fixed 500 ms + continue; no counter | **unbounded** (tight-ish) |
| `provision_runner` golden re-arm | Spent checkpoint after clone drain | exponential: 10 s → 60 s cap, total 300 s budget, then direct OCI create | yes (≤5 min) |
| `await_guest_ready` | Guest agent readiness probe | 25 ms poll, 30 s deadline | yes |
| golden download | Release/OCI asset | single attempt, 1 h timeout, then local-build fallback | n/a |
| `preload_images` / `docker_start_command` | dockerd readiness / start | shell polls (30 s / 10 s+5 s), start retried once | yes |

## SmolVM provider (`preloop-vm`)

| Site | What | Policy | Bounded? |
|---|---|---|---|
| `delete` | `smolvm machine delete` | 3 attempts: "directory not empty" 100 ms, "database is locked" 500 ms; "not found" = success | yes |

fork / create / exec / start / stop have **no retry** — errors propagate to
the orchestrator, which retries at the slot level.

## Runner (`preloop-runner`)

| Site | What | Policy | Bounded? |
|---|---|---|---|
| `client/http.rs` POST/PUT | AzDO/broker JSON, log append | 3 attempts, exp 2 s / 4 s, transient (5xx / network) only | yes |
| `client/http.rs` `SessionBackoff` | Listener reconnect | jittered: ≤5 errors [15,30) s, then [30,60) s; reset on success | unbounded attempts, long sleeps |
| `broker_listener.rs` `run_broker_loop` | Session create + message poll | SessionBackoff; 409 retriable; OAuth linear `min(n*5,60)` s; deprecated → exit | unbounded (until shutdown) |
| `message_listener.rs` | Session create + poll (classic) | conflict: 8 × 30 s (~4 min); transient: 30 s fixed, no cap | conflict yes, transient **unbounded** |
| `container_ops.rs` `wait_for_services_healthy` | Docker service health poll | exp 2 → 32 s (official GetExponentialBackoff) | **unbounded attempts** |
| `container_ops.rs` `docker_registry_login` | `docker login` | 3 attempts, 5 s / 10 s | yes |
| `debug_pause.rs` `await_verdict` | Debug verdict poll | fixed 2 s; **infinite by design** (server suspends timeout) | unbounded by design |
| `live_logs.rs` send / connect | Live-log WebSocket | 3 attempts, random 100–500 ms backoff, 30 s connect timeout | yes |
| `steps_runner.rs` `run_steps` | Debug `:retry` verdicts | capped at `MAX_DEBUG_ATTEMPTS` = 25, then fail | yes |
| `job_runner.rs` `first_renew_gate` | First `renewjob` | 5 retries, random 1–10 s; 404 → Abandoned | yes |
| `job_runner.rs` renew loop | Lease renewal (every 60 s) | random 5–15 s (first 5), 15–30 s after; lease expired `LockedUntil + 5 min`; 401 → re-acquire once; 404 → cancel | yes (lease window) |
| `reporting.rs` `flush_step_updates` | Timeline publish | failed → requeue, retried every 500 ms drain tick | **unbounded attempts** |
| `control_bridge.rs` | TCP splice to upstream | bridge never exits; runner's poll loop retries forever through it | architecture-level |

## Runner client (`preloop-runner-client`)

No retry loops — single-shot submits with a reqwest timeout.

## Gaps worth knowing

1. **`run_on_demand_slot` provision failure retries at fixed 500 ms, unbounded.**
   The slot-supervisor exponential backoff above it does **not** damp this
   inner loop — a broken smolvm spins ~2 attempts/s. Not the OOM cause, but
   noisy; a per-slot attempt counter with escalation would be an improvement.
2. **`wait_for_services_healthy` polls forever** (exp 2→32 s, no cap). Only
   the outer job timeout ends it. Matches the official runner, so changing
   it needs a fidelity note.
3. **`flush_step_updates` retries every 500 ms forever** on a permanently
   failing publish endpoint. Bounded CPU, but unbounded wall time.
4. **`reporting.rs` requeue** is retry-by-drain-tick; harmless but
   unbounded.

## Pool memory bounds (the OOM guard)

Before 2026-08-20, on-demand fork concurrency was sized **by CPU only**:

```
(available_parallelism / cpus_per_runner) - 1   // floor 1
```

On the 6-core / 22 GiB production host with `PRELOOP_RUNNER_MEMORY_MIB=8192`,
that allowed enough 8 GiB forks to exhaust RAM and OOM the control plane —
the golden alone commits 8 GiB, every fork inherits that footprint and grows
toward the ceiling while its job runs, and warm mode provisions a successor
mid-job (so `size + 1` VMs are live).

The guard, `on_demand_memory_cap`:

```
runner_mib  = memory_mib.max(1)
golden_mib  = runner_mib
by_memory   = (host_total - golden_mib - 2048 MiB reserve) / runner_mib   // floor 1
max_concurrent = min(cpu_term, by_memory)
```

- The 2 GiB reserve keeps the control plane, OS, and page cache alive —
  without it the host OOMs *after* the forks are up.
- Applied in **both** pool modes:
  - size=0 on-demand: `max_concurrent = min(by_cpu, by_memory)`
  - warm mode: `warm_size = min(configured size, by_memory)`, logged when
    reduced. `PRELOOP_RUNNER_POOL_SIZE` still wins as an explicit override
    only up to the memory cap — the cap is a safety floor, not a knob.
- A host whose memory can't be read (`/proc/meminfo` unavailable, non-Unix)
  falls back to CPU-only sizing rather than refusing to run.
- Floors at 1 so a tiny host still runs a single job.

### Test vectors (`on_demand_memory_cap`)

| Host | Runner ceiling | Cap |
|---|---|---|
| 22 GiB | 8 GiB | 1 (production case) |
| 64 GiB | 8 GiB | 6 |
| 32 GiB | 4 GiB | 6 |
| 8 GiB | 8 GiB | 1 (floor) |
| 4 GiB | 8 GiB | 1 (floor) |
