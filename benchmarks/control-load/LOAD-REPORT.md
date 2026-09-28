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
| r12 node-kill | 2/s, 90s, 60 runners, node 0 killed at 30s | 174 | 458 | 681 | 381 | 77 | 60 | 979 / 2,949 | 927 | 482 | 1.81 |
| r13 database-restart | 2/s, 90s, 60 runners, Postgres restarted at ~35s | 112 | 359 | 179 | 124 | 235 | 42 | 331 / 8,132 | 140 | 84 | 0.59 |


Direct-SQL cutover rounds (the new `control/lite` / `control/pg` backends):

| Round | Load | Workflows | Jobs submitted | Acquired | Completed | Backlog | Webhooks | Submit p50/p99 ms | Complete p50 ms | Timeline p50 ms | Completed/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| r18 new engine | 10/s, 90s, 200 runners | 330 | 1,609 | 1,583 | 1,570 | 0.44/s | 185 | 745 / 6,408 | 40 | 7 | 17.44 |
| r19 new engine | 25/s, 90s, 400 runners | 297 | 3,283 | 1,894 | 1,871 | 15.70/s | 388 | 4,650 / 13,039 | 277 | 84 | 20.78 |
| r20 `SKIP LOCKED` | 25/s, 90s, 400 runners | 270 | 3,107 | 1,955 | 1,933 | 13.05/s | 634 | 11,341 / 25,539 | 484 | 102 | 21.47 |
| r21 pool/webhook fixes | 25/s, 90s, 400 runners | 216 | 3,479 | 2,054 | 2,041 | 15.96/s | 796 | 7,601 / 18,627 | 149 | 19 | 22.71 |
| r23 nested-pool fix | 25/s, 90s, 400 runners | 265 | 2,849 | 2,049 | 2,032 | 9.08/s | 677 | 9,693 / 27,194 | 77 | 11 | 22.57 |
| r24 node-kill chaos | 25/s, 90s, 400 runners, node 0 killed at 45s | 408 | 2,437 | 2,108 | 2,056 | 4.23/s | 254 | 5,895 / 14,164 | 390 | 69 | 22.85 |

The first direct-SQL round exposed a Postgres-only startup crash: the
`control/pg` module declaration for `webhooks.rs` had been lost in a merge,
so every webhook trait forwarder recursively called itself. `15bd7901`
restored the module and added a shared inbox regression test. The first
10/s round then reached 17.44 completed jobs/s versus 6.27 jobs/s in the
old r16 baseline.

At 25/s, the claim path initially spent 563 seconds in 23,036 conditional
claim updates with only 4,444 wins. `b892be5a` changed the candidate claim
to `FOR UPDATE SKIP LOCKED` and joined assignments into the candidate read.
The dominant remaining cost is per-run serialization (`runs.event_seq` and
the run mutex); database CPU was not saturated.

`ffe1bff4` fixed a separate pool self-deadlock: `acquire_for_runner`,
status snapshots, OIDC grants, and completion paths could hold one pooled
connection while checking out another. It also bounded pool checkout at 30s
and added eight concurrent webhook drain loops. The dedicated regression
test fails by timeout without the connection release and passes with it.

The node-kill round retained `duplicate_inflight = 0` and completed at
22.85 jobs/s, but left 150 jobs in progress after the killed node. Claim
recovery and runner re-registration still need hardening before capacity is
claimed.
Round data is retained in `load-results/<label>/summary.json`, including rates, latency distributions, database samples, transaction statistics, and integrity output.

## Capacity conclusion

5M jobs/day requires approximately 57.9 completed jobs/s. The direct-SQL
cutover reached 22.85 completed jobs/s at 25 workflow submissions/s and
17.44 jobs/s at 10 submissions/s. This is a substantial improvement over
the old r16 baseline (6.27 jobs/s), but the control plane is **not yet
validated for 5M jobs/day**.

Remaining capacity work:

- reduce per-run `event_seq`/run-mutex serialization;
- harden runner identity/session re-registration after node restart;
- eliminate the remaining webhook enqueue timeouts and drain backlog;
- repeat 50/s and burst rounds after those fixes;
- run the chaos matrix again after claim recovery reaches zero in-progress
  orphaned jobs.

r11 injected:

- 50 ms bidirectional database latency for 10 seconds
- a 10-second database network partition
- a connection reset epoch at 70 seconds

r12 killed node 0 at 30 seconds and restarted it through the round harness. r13 stopped and restarted the real test Postgres process at approximately 35 seconds.

Observed behavior:

- r11 retained `duplicate_inflight = 0` and `duplicate_runners = 0`; it left 419 queued jobs.
- r12 requeued 49 orphaned claims but left 30 duplicate runner registrations after runner re-registration; `duplicate_inflight = 0`, backlog 77.
- r13 recovered database connectivity and retained `duplicate_inflight = 0`, but returned 64 API 500s and left 235 queued jobs.
- Network/database faults generated connection-closed errors and webhook retry exhaustion; redelivery/replay remains an operational recovery requirement.

## Findings and fixes

1. The direct-SQL backends raised completed throughput from 6.27 jobs/s
   (old r16) to 17.44 jobs/s at 10 submissions/s and 22.85 jobs/s during
   the 25/s node-kill round.
2. A lost `pg/mod.rs` module declaration left every Postgres webhook
   forwarder recursively calling itself. `15bd7901` restored `webhooks.rs`
   and added a shared inbox regression test.
3. Ready-job claims initially thrashed on one head row. `b892be5a` added
   `FOR UPDATE SKIP LOCKED` and removed the per-candidate assignment query.
4. Nested Postgres pool checkouts could hold every reader indefinitely.
   `ffe1bff4` releases connections before nested reads, uses transaction-
   scoped readers, bounds checkout at 30s, and drains webhook deliveries
   with bounded concurrency.
5. The remaining dominant hotspot is per-run serialization: `runs.event_seq`
   and the run mutex. At r21, the top event-sequence statement consumed
   77 seconds; CPU was not saturated. Webhook enqueue timeouts and
   post-node-kill orphan recovery also remain.

The control plane remains **not validated for 5M jobs/day**: 57.9 completed
jobs/s is still above the current 22.85 jobs/s peak.
