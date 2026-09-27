# Control-plane load and chaos report

Date: 2026-09-27
Branch: `Bnjoroge/lock-refactor`

## Scope

The harness submits real workflow-shaped REST/webhook traffic. Runner VMs are mocked by HTTP runner clients; they register, create sessions, long-poll, acquire jobs, renew leases, patch timelines, and complete jobs. Postgres is real. Two control-plane nodes share one database and one HMAC cluster key.

The harness uses an open-loop submission rate, workflow-shape mix, log-normal job durations, webhook delivery, per-node transaction timing, latency percentiles, and database integrity queries. This follows the open-model guidance in [Grafana k6 constant-arrival-rate documentation](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/). The mocked runner population models sequential runner tasks and weighted workload behavior similar to [Locust's user/task model](https://docs.locust.io/en/stable/writing-a-locustfile.html).

Chaos rounds define a measurable steady state, inject a bounded fault, and compare recovery/integrity, following the [Principles of Chaos Engineering](https://principlesofchaos.org/).

## Round results

| Round | Load | Workflows | Jobs submitted | Acquired | Completed | Backlog | Webhooks | Submit p50/p99 ms | Complete p50 ms | Timeline p50 ms | Completed/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| r3 fixed-multinode | 2/s, 90s, 60 runners | 250 | 745 | 138 | 100 | 645 | 84 | 9,576 / 57,355 | 7,921 | 3,214 | 0.47 |
| r4 direct-paths | 2/s, 90s, 60 runners | 249 | 791 | 141 | 82 | 709 | 82 | 39,565 / 48,243 | 36,548 | 6,036 | 0.39 |
| r5 attribution | 2/s, 60s, 60 runners | 171 | 607 | 67 | 27 | 580 | 64 | 16,886 / 52,833 | 38,318 | 3,267 | 0.30 |
| r6 concurrent-polls | 2/s, 90s, 60 runners | 277 | 765 | 465 | 402 | 363 | 93 | 1,075 / 2,111 | 6,204 | 1,956 | 1.91 |
| r7 run-locks | 2/s, 90s, 60 runners | 259 | 621 | 521 | 474 | 147 | 89 | 826 / 1,637 | 2,064 | 725 | 2.25 |
| r8 auth/archive | 2/s, 90s, 60 runners | 228 | 732 | 822 | 664 | 68 | 69 | 1,730 / 5,897 | 2,052 | 930 | 3.15 |
| r9 burst | 5/s + 5x burst, 60s, 100 runners | 707 | 2,120 | 443 | 285 | 1,835 | 230 | 9,483 / 13,769 | 12,558 | 4,161 | 1.58 |
| r10 no-global-poll-lock | 5/s + 5x burst, 60s, 100 runners | 742 | 2,038 | 543 | 260 | 1,778 | 243 | 8,874 / 10,903 | 11,886 | 3,709 | 1.44 |
| r11 chaos | 2/s, 90s, 60 runners | 214 | 597 | 310 | 178 | 419 | 72 | 787 / 9,255 | 274 | 150 | 0.84 |

Round data is retained in `load-results/<label>/summary.json`, including rates, latency distributions, database samples, transaction statistics, and integrity output.

## Integrity

Successful steady-state rounds r6–r10 reported:

- `duplicate_inflight = 0`
- `duplicate_runners = 0` after the session-diff and fixed-deadline changes
- no duplicate job claims in the cross-node poll regression test
- no failed control-plane unit/lib tests after the changes

## Chaos

r11 injected:

- 50 ms bidirectional database latency for 10 seconds
- a 10-second database network partition
- a connection reset epoch at 70 seconds

Observed behavior:

- database remained structurally valid: `duplicate_inflight = 0`, `duplicate_runners = 0`
- 5,538 transient `connection closed` backend errors were observed by runners during the fault window
- 30 webhook submissions returned 500 after retry exhaustion; redelivery/replay remains an operational recovery requirement
- backlog grew to 419 jobs

## Findings and fixes

1. Full TxState submit/poll transactions caused multi-second to minute-scale latency. Narrow scopes and concurrent poll claims reduced submit p50 from roughly 40 s to below 2 s in steady state.
2. Polling while taking the global advisory lock serialized runners behind maintenance. Removing that redundant lock reduced poll global-lock wait from roughly 240 ms to 70–79 ms in the comparable burst round.
3. Delete-and-reinsert side tables caused duplicate session/attempt errors under concurrent writers. Sessions, broker messages, token requests, grants, OIDC contexts, assignments, pool state, and cancellations now use keyed diffs/upserts.
4. Global writers could race run-scoped writers. Global transactions now lock loaded runs in a deterministic advisory-lock order; the archiver uses nonblocking run locks.
5. Repeated wakeups extended long-poll requests indefinitely. All long-poll handlers now use one fixed deadline.
6. Remaining dominant hotspots are timeline reconciliation and concurrency-aware completion. Both still perform substantial TxState work and keep the system well below the 5M jobs/day target.

## Capacity conclusion

5M jobs/day requires approximately 57.9 completed jobs/s. The highest measured burst result was approximately 3.15 completed jobs/s in r8 and 1.58 jobs/s in the 5x burst r9; r10 measured 1.44 jobs/s. The control plane is therefore **not yet validated for 5M jobs/day**.

Required next work:

- replace timeline reconciliation with direct SQL row updates/upserts;
- merge broker completion bookkeeping with completion persistence to remove the second transaction;
- remove remaining hot-path TxState loads, culminating in deleting the TxState backend model;
- rerun burst profiles at progressively higher arrival rates until backlog remains bounded;
- repeat chaos rounds with node kill/restart and database failover, not only proxy faults.
