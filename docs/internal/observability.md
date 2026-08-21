# Observability — Internal Signal & Security Contract

> **Status:** Internal contract for Plan 002 (`plans/002-observability-strategy.md` revised at `673bdfa0`). This is the **internal** contract that must land before any OTLP export is wired. The public `docs/observability.md` will be a redacted subset later. Do not publish this file.

## Why

Preloop can report healthy while the pool repeatedly fails to provision, can't distinguish "no runner" from "stale runner," and diverges two servers sharing the same SQLite file. The only aggregate view is `preloop logs`. This contract makes every control-plane question answerable from one endpoint.

## Architecture — Three Layers

1. **Zero-dependency diagnostics** — `/healthz`, `/readyz`, `GET /api/v1/status`, `preloop status --json`, structured `stderr`/`journald`, `GET /metrics` (Prometheus text). No sidecar. Answers "why is this job not moving?" with no backend.
2. **Vendor-neutral telemetry** — OpenTelemetry metrics/logs/short traces via bounded `OTLP/HTTP` batches only when an `OTEL_EXPORTER_OTLP_*_ENDPOINT` is set. Signal-specific variables (`..._LOGS_/TRACES_/METRICS_ENDPOINT`) are used as-is; the generic `OTEL_EXPORTER_OTLP_ENDPOINT` is a base with the `/v1/<signal>` suffix appended. Headers follow the same per-signal pattern with the generic fallback. Fail-open: export never rejects/delays a workflow.
3. **Optional reference backend** — pinned single-node OpenObserve (SQLite + local disk, loopback) + importable dashboards. Product boundary is `OTLP + Prometheus`; any backend is interchangeable.

```
CLI --bearer--> /api/v1/status
probes --------> /healthz + /readyz
prom ----------> /metrics (bearer)
control plane + pool + host VM sampler --> preloop-observability --> stderr/journald
                                                          `--> /metrics
                                                          -. OTLP/HTTP .-> OpenObserve (opt-in)
```

## Security — What Must Not Be Exported

**Scrubbed at `673bdfa0`:**

- `crates/preloop-runner-server/src/artifact_twirp.rs:94` — `info!(token, name)` → `info!(name, workflow_run_backend_id, workflow_job_run_backend_id)` (token removed, keep registry coordinates)
- `crates/preloop-runner-server/src/results_twirp.rs:713` — `info!(token, "cache v2 create")` → `info!(key, version)` (storage identity, not capability)
- `crates/preloop-runner-server/src/blob_store.rs:63,78,95,113,121,127,131` — all `warn!/info!(kind, token)` → `kind` + `block`/`size`/`blocks` (blob operation, not token)
- `crates/preloop-runner-server/src/distributed_task.rs:327` — `info!(?body, …)` full PATCH JSON → `info!(pool_id, request_id, result, has_result)` (bounded enum, not raw body)
- `crates/preloop-runner-server/src/recording.rs:1-90` — **exempt** conformance capture (headers + bodies, mode `0600`, never through OTLP). Rule explicitly ignores this file.

**Guard:** `rules/no-sensitive-log-fields.yml` rejects `INFO/WARN/ERROR` fields `token`, `authorization`, `cookie`, `headers`, `body`, `payload`, `signed_url` outside `recording.rs`. Run `just sg-scan-strict` — must be `0`.

**Never log:** raw URLs/query strings, error text into metrics, workflow `stdout/stderr` (stays in `live_logs.rs:25` run-log store, 64 MiB cap), environment values, secrets.

## Cardinality Rules

- Allowed metric attributes: bounded enums / finite route templates — HTTP method + matched `route` (never raw URI), queue kind, pool mode/state, backend, `limit` constant name (finite set), `task` registry name (finite set).
- Forbidden: `run_id`, `job_id`, `runner_id`, `machine_name`, repo/workflow/ref/SHA, `runs-on` values, cache keys, artifact names, raw URL/query, `x-delivery-id`, tokens.
- IDs belong in **logs/traces only** (`event.name` + `run.id`/`job.id`/`machine.name`), correlated via `trace_id`/`span_id`.
- Test: drive 1,000 distinct IDs through instrumentation, gather Prometheus registry, assert fixed series count, no sentinel in exposition.

## Status Semantics

- **`GET /healthz`** — public, shallow, lock-free. `200` while process serves, `503` during shutdown. Fields: `schema_version`, `ok`, `protocol_version`, `shutdown_requested`. Touches nothing (no DB, no SmolVM, no `InnerState`).
- **`GET /readyz`** — public, reason codes. `200` when durable state restored + every **critical** `TaskHeartbeat` fresh (state sampler, reaper `bootstrap.rs:396`, scheduler scan `bootstrap.rs:479`, PG `store_pg.rs:95`). `503` → `starting | task_stale{task} | shutting_down`. Non-critical staleness does not gate readiness.
- **`GET /api/v1/status`** — `bearer` required, `schema_version: 1`, sampled every 5s without holding `InnerState`, reports `snapshot_age_seconds`. Full shape in `plans/002-observability-strategy.md` § Operator surfaces — includes `jobs`, `concurrency`, `scheduler`, `pool`, `vms` (with `capabilities` + 5 top consumers), `store`, `storage`, `limits[]`, `tasks[]`, `github` (with `rate_limit`, `token_cache`), `conditions[]` (bounded stable codes, ≤5 exemplars, safe messages).
- **`preloop status`** — unit variant → `Status(StatusArgs)` with `--json` (shape of `PlanArgs` at `main.rs:706`). Human output ordered 1..10 (service → queue → concurrency/scheduler → pool/runners → VM fleet → store/storage/GitHub/debug/telemetry → non-zero limits/tasks → conditions → recent runs). `--json` prints the endpoint body exactly.
- **`GET /metrics`** — `bearer` required, Prometheus text from the same snapshot, never per-machine labels.

`wait_for_engine_socket` (`main.rs:1458`) now probes `/readyz` and surfaces the last `reason` on 30s timeout.

## Log Classes

- `INFO` — low-frequency control-plane/pool/scheduler transitions
- `WARN` — recoverable degradation (`vm.host.memory.pressure`, `limit.exceeded{limit="QUEUE_MAX_PENDING"}`, `github.rate_limit.low`, `vm.unreachable`)
- `ERROR` — supervisor death, invariant break, `store.connection.lost` (PG), `task.exited{critical=true}`
- `DEBUG` — successful long-poll/renew (never `INFO` per poll)

Catalog (excerpt): `job.concurrency.cancelled` (hash of group key, not raw — branch names are PII), `schedule.fired/skipped`, `task.exited`, `limit.exceeded` (rate-limited), `vm.unreachable/reachable`, `store.connection.lost`, `storage.pressure`, `github.rate_limit.low`, `debug.audit.evicted`.

## VM Telemetry Contract

- `VmProvider::status` at `preloop-vm/src/lib.rs:1018` today substring-matches human text — replaced with typed `machine status --json` (one machine on lifecycle path) + `machine ls --json` (whole fleet, one subprocess per 60s slow pass, already parsed in `list` at `1045`) + `machine data-dir` for disk paths, at `smolvm 1.8.1` floor (`versions.toml:8`, `machine_status_json` at `src/cli/vm_common.rs:2360`).
- `state` includes `Unreachable` (vsock probe, `src/agent/state_probe.rs:41`) → `MachineState::Unreachable` metric + `vm_unreachable` condition. A missing `vm-<pid>` cgroup leaf (SmolVM creates it, `src/process.rs:375`) degrades to process fallback, `capability=false`, never zero.
- Host-only: `cpu.stat usage_usec`, `memory.current`, `pids.current`, `st_blocks` + `statvfs`. Never guest `free/df/ps`.

## Deployment Profiles

- **Default local** — no backend, pretty on TTY / JSON when piped, `PRELOOP_LOG_FORMAT=auto`.
- **Local enhanced** — single OpenObserve container, loopback, persistent volume, short retention, resource caps.
- **Self-hosted single node** — Preloop + OpenObserve may share a host only with separate volumes + caps.
- **Existing estate** — point same OTLP at your backend.
- **Optional Collector** — whole-node host telemetry + fan-out (VM `preloop.vm.*` does not need it).
- **Quickwit+S3 alternative** — `Vector → Quickwit → S3` for cheap searchable logs/traces; metrics stay on `Mimir`/`VictoriaMetrics` until Quickwit metrics matures.

## Flow Capture

`recording.rs` is an explicit local conformance facility, stored `0600`, never through the normal logging/OTLP pipeline. Verified at creation.

## References

- Plan: `plans/002-observability-strategy.md` (and HTML companion with mockups)
- Code anchors re-verified at `673bdfa0`; re-run `git diff --stat 673bdfa0..HEAD -- …` before each step
- Rules mirror `rules/no-expose-in-loop.yml` / `no-raw-secret-replace.yml` / `no-inline-masking.yml`
