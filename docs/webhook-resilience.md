# Webhook resilience

GitHub sends each webhook **once**. A non-2xx response, a 10-second timeout,
or a host that is not there produces a red row in the App's delivery history
and nothing else — there is no automatic retry, ever. preloop's answer is a
durable boundary plus four repair layers, each covering a failure the previous
one structurally cannot.

The durable boundary: the raw delivery is committed to `webhook_deliveries`
**before** the handler answers `202`. A `202` therefore means "we own this
payload", not "we received some bytes". Everything after that is preloop's
problem, and everything below is how it gets fixed.

## Failure modes and what handles them

| # | Failure | Symptom | Handled by |
|---|---------|---------|-----------|
| 1 | Store down before the ack | `500`, GitHub logs a failure | Bounded enqueue retry (8s / 2 attempts), then a truthful `500` |
| 2 | Transient error after the ack | Row stays `received` | Leased worker, backoff 1→5→15→30s, 6 attempts |
| 3 | Edge failure (Funnel stale, host rebooting, TLS) | GitHub recorded `failure`, no local row | **Delivery watchdog** |
| 4 | Phantom ack (restore from snapshot, corruption) | GitHub recorded success, no local row | **Delivery watchdog** (GUID join) |
| 5 | Delayed / throttled delivery | Arrives minutes late | Watchdog **grace window**; reconciler grace window |
| 6 | GitHub API outage or rate limit mid-processing | Every queued delivery fails on the same dependency | **Circuit breaker** + parking |
| 7 | No delivery ever generated | Nothing in the history to redeliver | **Source-state reconciler** |
| 8 | Silent misconfiguration (events narrowed, URL repointed) | Silence, indistinguishable from "no pushes" | **App health monitor** |
| 9 | Payload older than GitHub's 3-day window | Redelivery impossible | **Local replay** (`preloop webhooks replay`) |

## Layer 1 — delivery watchdog

`src/webhook_watchdog.rs`. Polls `GET /app/hook/deliveries` with an App JWT and
repairs two shapes:

- **Remote failure**: GitHub recorded a non-2xx and no local row exists.
- **Phantom ack**: GitHub recorded success and no local row exists. Both sides
  look healthy; only the GUID join finds it.

Repair is `POST /app/hook/deliveries/{id}/attempts` — the same GUID, so the
ingress deduplicates it and no second run number is allocated.

Invariants worth keeping:

- The watermark **never advances on a failed poll** and never past the grace
  boundary, so no range of history is silently skipped.
- Nothing is redelivered while the local store is unhealthy: replaying into a
  broken store loses the payload a second time and spends a finite
  redelivery opportunity doing it.
- The poll is skipped while the breaker is open — during a GitHub outage the
  poll would fail anyway.
- Per-GUID backoff (0, 5m, 15m, 1h, 6h) with a cap; a GUID that hits the cap
  stays **open** as a standing finding rather than disappearing.

## Layer 2 — outage circuit breaker

`src/github_breaker.rs`, wired through every GitHub call on the webhook path.

Three consecutive 5xx/transport failures open it; a `429`, or a `403` with
`x-ratelimit-remaining: 0`, opens it immediately for exactly as long as
`retry-after` / `x-ratelimit-reset` says. While open:

- `drain_webhook_queue` **claims nothing**. Claiming charges an attempt, and
  every claim would fail on the same dependency.
- A delivery already in flight is **parked**: returned to `received` with its
  attempt refunded (`Store::park_webhook_delivery`). A 30-minute GitHub
  incident can no longer dead-letter good pushes.
- One half-open probe is admitted after the window, so recovery costs GitHub
  one request rather than the whole backlog.

Payload-level failures are untouched: bad YAML and permanent errors still
dead-letter fast. Only GitHub-unavailability parks.

## Layer 3 — App health monitor

`src/webhook_health.rs`. The startup subscription read-back catches a
misconfigured App on day one and nothing after. This re-reads `GET /app` and
`GET /app/hook/config` on a schedule and publishes:

- missing trigger events (and the permission that gates each checkbox),
- delivery-URL drift against `PRELOOP_PUBLIC_URL`,
- read errors — never rendered as a clean bill of health.

GitHub exposes no API to change an App's event subscription, and no `active`
flag on `/app/hook/config`: this layer pages a human, it cannot self-heal.

## Layer 4 — source-state reconciler

`src/webhook_reconciler.rs`. **Opt-in**, because every synthesis is a CI run
GitHub never asked for. Set `PRELOOP_WEBHOOK_RECONCILE_REPOS`; empty means
disabled and zero HTTP calls.

Compares the default-branch head and open-PR heads against existing runs, and
for a gap enqueues a webhook-shaped payload into the *same* durable queue —
same adapters, same trigger evaluation, same check reporting. No second code
path.

Guards against the one race delivery-id dedup cannot catch (a late real
webhook arriving after synthesis, under a different GUID):

- a grace window far longer than any plausible delivery delay, and
- a durable reservation keyed `repository | event | ref | head sha`, which
  survives restarts even though in-memory run state does not.

It cannot rebuild multi-commit push history or action-specific PR semantics; a
PR is synthesized as `synchronize`, the only claim current state supports.

## Layer 5 — operator surface

Native API (bearer-authenticated):

| Endpoint | Purpose |
|----------|---------|
| `GET /api/v1/webhooks/deliveries?state=&limit=` | Queue listing plus counters. Never returns payloads. |
| `POST /api/v1/webhooks/deliveries/{id}/replay` | Requeue from the retained local payload. `404` unknown, `409` still active. |
| `GET /api/v1/webhooks/health` | Queue, watchdog, reconciler, breaker and App config in one document. |

CLI:

```sh
preloop webhooks list [--state failed] [--limit 50] [--json]
preloop webhooks replay <delivery-id>
preloop webhooks health [--json]
```

Local retention is 30 days against GitHub's 3-day redelivery window, so
`replay` is frequently the only repair still available — and it needs neither
GitHub nor an App JWT.

## Status conditions

The operational snapshot (`GET /api/v1/status`, `/readyz`) carries:

| Condition | Meaning |
|-----------|---------|
| `webhook_dead_letter` | Deliveries failed permanently |
| `webhook_queue_stalled` | Oldest unprocessed delivery older than 15 minutes |
| `github_unavailable` | Breaker open; deliveries parked, not failing |
| `webhook_watchdog_stale` | No successful history poll in 30 minutes — lost deliveries would go unnoticed |
| `webhook_repairs_pending` | Redeliveries requested that have not arrived |
| `webhook_config_drift` | App events, permissions or delivery URL wrong |
| `webhook_reconciler_failing` | Source-state reconciliation erroring |

## Configuration

| Variable | Default | Effect |
|----------|---------|--------|
| `PRELOOP_WEBHOOK_WATCHDOG` | on | `off` disables the delivery watchdog |
| `PRELOOP_WEBHOOK_WATCHDOG_INTERVAL_SECS` | 300 | Poll cadence (floor 10; stay well inside GitHub's 3-day history) |
| `PRELOOP_WEBHOOK_WATCHDOG_GRACE_SECS` | 120 | How long a delivery may be late before it counts as missing |
| `PRELOOP_WEBHOOK_WATCHDOG_MAX_ATTEMPTS` | 5 | Redelivery cap per GUID |
| `PRELOOP_WEBHOOK_WATCHDOG_MAX_PAGES` | 5 | History pages per App per poll |
| `PRELOOP_GITHUB_BREAKER` | on | `off` keeps observing but never trips |
| `PRELOOP_GITHUB_BREAKER_THRESHOLD` | 3 | Consecutive failures before opening |
| `PRELOOP_WEBHOOK_HEALTH_INTERVAL_SECS` | 900 | App config re-read cadence (floor 60) |
| `PRELOOP_WEBHOOK_RECONCILE_REPOS` | *(empty)* | Comma-separated `owner/repo`; empty disables the reconciler |
| `PRELOOP_WEBHOOK_RECONCILE_INTERVAL_SECS` | 900 | Reconciler cadence (floor 60) |
| `PRELOOP_WEBHOOK_RECONCILE_GRACE_SECS` | 900 | How old a head must be before synthesis |
| `PRELOOP_PUBLIC_URL` | — | Also the expected delivery URL for drift detection |

## Durable state

Migration 9 (`webhook-delivery-repair-state`), both backends:

- `webhook_watchdog` — per-App history cursor and last-success timestamps.
- `webhook_redeliveries` — repair records keyed by delivery GUID.
- `webhook_synthetic_events` — reconciler idempotency keys. Never pruned:
  they are tiny, and forgetting one re-fires CI for a head that already ran.

## Deliberately out of scope

- **HA ingress gateway.** A second durable queue in front of preloop would buy
  2xx during host downtime and cost another failure domain. Build it only if
  "ack while preloop is down" is a requirement.
- **Active-active preloop.** Instances diverge in memory today
  (`docs/architecture.md` §State Model); that needs a shared-bus refactor
  first.
