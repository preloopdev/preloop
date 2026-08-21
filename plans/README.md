# Implementation Plans

Generated and reconciled by the improve skill on 2026-08-17. Plan 002 was re-verified and revised
against live code at commit `673bdfa0` on 2026-08-20. Execute plans in the order selected by the
maintainer; the two current plans are independent. Each executor must read its plan fully, honor its
STOP conditions, run every verification gate, and update the corresponding status row.

## Execution order and status

| Plan | Title | Priority | Effort | Depends on | Status |
|---|---|---:|---:|---|---|
| [001](001-caching-performance-strategy.md) | Caching strategy for local and self-hosted CI | P1 | L | — | TODO |
| [002](002-observability-strategy.md) | Make Preloop observable without making a backend mandatory | P1 | L | — | IN PROGRESS (revised at `673bdfa0`) |

Status values: `TODO` | `IN PROGRESS` | `DONE` | `BLOCKED: <reason>` | `REJECTED: <reason>`.

## Dependency notes

- Plans 001 and 002 can execute independently.
- When both touch cache/artifact operations, Plan 002 owns the stable observability contract and Plan
  001 must emit through that contract rather than introduce separate metric/log conventions.
- Plan 002 also owns the `preloop.storage.*` measurement contract. Plan 001 adds cache quotas and
  eviction; it must report them through `preloop.storage.bytes` / `preloop.storage.gc` and register
  any new cap with `LimitRegistry`, not invent a parallel family.
- Plan 002 is intentionally split into seven PR-sized steps. Its security/log-safety step must land
  before OTLP log export is enabled.

## Findings considered and rejected

- **Make OpenObserve a required or embedded Preloop component**: rejected. Direct status, local logs,
  and Prometheus metrics must work with no backend; OTLP keeps backend choice interchangeable.
- **Require an OpenTelemetry Collector for the minimum profile**: rejected. It adds another process;
  direct bounded OTLP/HTTP is enough for low-volume application telemetry. Preloop collects
  host-observed CPU, memory, throttling, PID, and sparse-disk metrics for VMs it owns; a Collector
  remains optional for whole-node and unrelated-process metrics, buffering, sampling, or fan-out.
- **Export workflow step logs by default**: rejected. They are high-volume, user-controlled, and may
  contain secrets; they remain in Preloop's existing run-log store.
- **Represent each workflow as one hours-long trace**: rejected. Workflows cross asynchronous requests,
  sessions, and processes; use short operation traces plus structured run/job correlation fields.
- **Treat OpenObserve HA as the self-hosted default**: rejected. Its Kubernetes, object storage,
  PostgreSQL, NATS, and multi-role topology conflicts with the minimal-dependency requirement.
- **Use observability to imply multi-server Preloop support**: rejected. In-memory state remains
  authoritative per instance; shared database storage is only a restart source, not a shared bus.
- **Keep the first draft's two ad-hoc readiness heartbeats**: rejected on revision. Preloop runs
  fifteen long-lived tasks; wiring two by hand and leaving thirteen silent reproduces the problem the
  plan is meant to solve. One `TaskHeartbeat` registry with a critical subset replaces it.
- **Add a fifth shared handle to `RunnerPoolConfig`**: rejected on revision. Four ad-hoc
  `Option<Arc<…>>` channels already exist; the plan consolidates them into one `PoolStatus` instead of
  extending the pattern.
- **Emit a duration histogram for `/ws/live-logs`**: rejected. A long-lived WebSocket would dominate
  p99 and corrupt the availability SLI denominator; use a connection gauge and close-reason counter.
