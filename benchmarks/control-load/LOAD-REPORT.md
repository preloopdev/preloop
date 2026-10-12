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
The dominant remaining cost at the time was believed to be per-run
serialization (`runs.event_seq` and the run mutex); database CPU was not
saturated. See the e2–e4 follow-up: that was tested and refuted.

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

- find what the database is actually busy with (see the e2–e4 follow-up; the
  per-run `event_seq`/run-mutex hypothesis was tested and refuted);
- harden runner identity/session re-registration after node restart;
- eliminate the remaining webhook enqueue timeouts and drain backlog;
- repeat 50/s and burst rounds after those fixes;
- run the chaos matrix again at 25/s: post-kill recovery reached zero
  in-progress orphaned jobs at 8/s (r39, below).

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
5. The remaining dominant hotspot was thought to be per-run serialization
   (`runs.event_seq` and the run mutex); the e2 experiment below shows it is
   not: whole-table reads were. Webhook enqueue timeouts and post-node-kill
   orphan recovery also remained (the latter fixed in r39).

The control plane remains **not validated for 5M jobs/day**: 57.9 completed
jobs/s is still above the current 22.85 jobs/s peak.

## Follow-up: per-path run-row attribution (p0, 2026-10-02)

Round `p0-baseline-25rps` re-ran the r23 configuration (2 nodes, 25
submissions/s for 90 s, 400 runners) on this workstation after adding
per-statement labels to `control::txn_stats` (`lock_run`, `flush_run`,
`event_seq.command`, `event_seq.append_event`) — the r21 "top event-sequence
statement" is now split by call path. The local run is throughput-throttled
by laptop hardware (6.25 completed jobs/s vs cpane's 22.85) and self-inflicted
`completejob` client timeouts churned runner sessions; ratios are
representative, absolute numbers are not.

Run-row statement time (in-transaction wall clock, both nodes, 449 s total):

| Op | Time | Share | Calls | Note |
|---|---|---|---|---|
| `event_seq.command` | 181.1 s | 40.3% | 17,944 | outbox bumps inside command txns |
| `event_seq.append_event` | 109.1 s | 24.3% | 15,880 | post-commit `emit()` appends |
| `flush_run` | 116.5 s | 25.9% | 9,733 | `UPDATE runs SET status ..` |
| `lock_run` | 42.3 s | 9.4% | 3,735 | `FOR NO KEY UPDATE` mutex |

Two corrections to the r21 reading: (1) ~65% of run-row time is `event_seq`
bumps, not the mutex itself — and `pg_stat_statements` attributes only
55.6 s of executor time to `UPDATE runs SET event_seq` across the same
33,824 calls, so ~235 s of the instrumented span is round-trip and client
scheduling overhead, not lock queueing: executor time already includes
row-lock waits. The overhead still matters, because every statement after
the bump runs with the run row locked until COMMIT. (2) Whole-database
`pg_stat_statements` is dominated by
scheduler reads, not run-row writes: the ready-queue front probe
(`SELECT runs_on::text FROM jobs WHERE queue_state='ready' ORDER BY pool_key,
priority DESC, run_order, job_order`) accumulated 354.3 s at 36.1 ms mean —
`jobs_ready` leads with `pool_key`, which these queries do not constrain, so
each call sorts the entire ready queue (~9,800 calls over the round). The
claim-side point-read fanout (`SELECT j.job_id, j.kind, .. FROM jobs`,
429.9 s across 116,439 calls at 3.7 ms mean) is the next target.

Integrity: `duplicate_inflight=0`, `names_running_twice=0`,
`active_owner_mismatch=0`. Two new invariants replace the retired
`active_request_id` joins after the `runner_sessions` schema change.

Fixes landed from this round: timeline `PATCH` projected step `succeeded`
records as job status events, masking the job's real conclusion for
subscribers (`patch_timeline_records` now gates `JobStatus` on non-step
records; regression test
`successful_step_does_not_mask_the_jobs_real_completion`). `RunAccepted`,
submit-time `JobStatus`, held `RunStatus`, `CheckRunCreated`, and timeline
`Annotation` events moved from post-commit `emit()` appends into their
producing transactions (`SubmitOutcome::events`, `set_job_check_run`,
`patch_timeline`), cutting the `event_seq.append_event` class and making
records+annotations atomic. Post-expansion `RunStatus` re-reads
(`complete_job_settling`, starved-run report) deliberately stay on `emit` —
their status is only knowable post-commit.

Regression noted for later investigation, not fixed here: a `fork policy:
approval sweep failed — operator does not exist: bigint < timestamp with
time zone` warning loop in the node logs (fork-gate SQL type mismatch,
pre-existing).

## Follow-up: post-kill orphan recovery (r38–r39, 2026-10-02)

Configuration: 2 nodes, 8 submissions/s for 30 s, 100 runners, node 0
killed at 15 s and restarted 10 s later, 650 s drain.

| Round | In-flight after drain | Stale leases >600 s | `duplicate_inflight` | `names_running_twice` | SQL errors |
|---|---:|---:|---:|---:|---:|
| r38 | 51 (40 leased, 11 unclaimed) | 40 | 0 | 0 | 0 |
| r39 | 0 (all 254 runs completed and archived) | 0 | 0 | 0 | 0 |

In r39 the 40 attempts orphaned by the kill failed 3:00.8–3:07 after their
claim. A session that went silent still counts as live inside the 30 min
runner liveness timeout, so the 180 s hung-worker window applies. The new
600 s `DEAD_SESSION_LEASE_SECONDS` (previously the 2,700 s runner-facing
lease, which is unchanged) bounds attempts whose session row is gone.

Reaper changes behind r39: lease expiry is evaluated for every in-flight
attempt each tick in both backends, not only for runs flagged due.
Completed-run memory trimming now runs off the event path (spawned,
single-flight), because a reaper-driven completion had wedged the only
reaper task inside `broadcast` → `trim_completed_run_state`. Each
reaper-driven completion is bounded at 30 s, and the reaper heartbeat beats
after the sweep, so a wedged sweep surfaces as a stale critical task.

r38's orphans came from a local regression that never landed: the
background loop's `reap_once` call was dropped while removing diagnostic
logging, so the loop ticked and beat its heartbeat without sweeping. Tests
that call `reap_once` directly could not see it. The harness's
`drain.backlog_left = 58` counts jobs its own runners never completed (40
lost with the killed node, 18 never acquired). The database settled all of
them: 764 success, 53 failure, 14 skipped, 8 cancelled.

## Follow-up: `run_seq` experiment, read-path fixes, versions and the event feed (e1–e4, r40, 2026-10-03)

All rounds: 2 nodes, 25 submissions/s for 90 s, 400 runners, 150 s drain,
this workstation (one machine hosts Postgres, both nodes, the harness and
all runners, so absolute numbers are low and noisy; the comparisons are what
carry information).

**Invalid pair (`e1-*`).** Both rounds ran against a node left over from an
earlier kill round: the harness restarted node 0 from a subshell and never
recorded its pid, so it kept port 18080 and served half the traffic from an
old binary and database. `run-round.sh` now records the restarted pid, kills
it on exit, and refuses to start when a node port is already in use. The e1
numbers are discarded.

**`run_seq` is not the ceiling (`e2-*`).** The hypothesis was that the
per-event `UPDATE runs SET event_seq` serialized runs and capped throughput.
With the bump (as shipped) and with it skipped by a temporary switch
(removed afterwards), same binary, same configuration:

| Round | Completed jobs/s | Submit p50 | Complete p99 | `lock_run` | `flush_run` |
|---|---:|---:|---:|---:|---:|
| `e2-with-run-seq` | 19.1 | 10.9 s | 17.5 s | 49.2 s / 40.3 s | 68.0 s / 68.8 s |
| `e2-no-run-seq` | 17.9 | 10.3 s | 16.9 s | 41.5 s / 42.7 s | 66.3 s / 67.4 s |

Skipping the bump removed ~170 s of `event_seq` statement time per node and
did not move throughput (−6%, within noise) or the `lock_run`/`flush_run`
totals. So finding 5 above and the "per-run serialization" conclusion in the
capacity section are wrong as a cause of the ceiling: waiting on the run row
was a symptom of the database being busy elsewhere.

**What the database was busy with: whole-table reads.** The `e2` top
statements by total time were reads, not run-row writes:

| Statement | Total | Calls | Mean |
|---|---:|---:|---:|
| per-run job rows for check-run reporting (`run_dispatch_info`) | 415.7 s | 4,958 | 83.8 ms |
| claim candidate read | 199.3 s | 13,859 | 14.4 ms |
| run graph load | 189.5 s | 25,266 | 7.5 ms |
| ready-queue front probe | 142.6 s | 9,928 | 14.4 ms |
| `reap_inputs` active-attempt query | 43.9 s | 48 | 914 ms |

Causes, each reproduced with `EXPLAIN (ANALYZE, BUFFERS)` on a scratch copy
holding 5.9k runs, 48.8k jobs and 3.1k ready jobs:

| Statement | Before | After | Cause and fix |
|---|---:|---:|---|
| `run_dispatch_info` job rows | 14.2 ms, 3,948 buffers | 1.2 ms | the latest-attempt subquery seq-scanned `job_requests`: the only `(run_id, job_id)` index was partial (`WHERE result IS NULL`). Added `job_requests_attempts (run_id, job_id, request_id DESC)`, which also serves the `jobs` → `job_requests` cascade (11 ms → 0.4 ms in `DELETE FROM runs`). |
| claim read (`LIMIT 64`) | 10.5 ms | 0.6 ms | `jobs_ready` had `namespace_id` between the pool key and the priority, so `ORDER BY pool_key, priority ..` sorted the whole ready queue. Index is now `(pool_key, priority DESC, run_order, job_order)`. |
| ready-queue front probe | 6.3 ms | 0.07 ms | same index |
| `reap_inputs` | 141.6 ms | 27.4 ms | the reaper extracted `jobTimeout` from the large toasted `message_template` for every unfinished attempt every tick (isolated: 323 ms with the extraction, 20 ms for the same join without it). It is now a stored generated column, `job_messages.job_timeout_s`. |

`e3-read-fixes` (these changes only): **21.0 completed jobs/s** (+10% over the
`e2` pair's 19.1/17.9), submit p50 6.5 s, complete p99 11.4 s. The first
three statements above fell out of the top ten. Not fixed: the run graph load
(6.7–8.5 ms × ~30k calls, one per command), `SELECT count(*) FROM jobs WHERE
queue_state='ready'` (6.8–10.4 ms × ~11.8k calls; the cause is not verified —
likely heap fetches, since the table is updated too constantly for index-only
scans), and `lock_run`/`flush_run`.

**Versions and the event feed (`e4-versions-outbox`).** `jobs.version` and
`runs.version` (triggers), `outbox_events.{job_id,version,origin}` instead of
`run_seq`, a per-node consumer that reads the outbox on a batched NOTIFY or
every second, and retention (see CHANGELOG). 21.2 completed jobs/s — the same
as `e3`, as expected: this change removes a lock and a lost-event gap, it was
never going to raise the ceiling. `INSERT INTO outbox_events` averages 0.3 ms.
Smoke test (two nodes, one Postgres): a stream attached to node B received the
`cancelled` `run_status` for a run cancelled through node A and closed 0.21 s
later; before this change that client would have waited out its 5-minute
stream timeout.

`r40-kill-versions` (8/s for 30 s, 100 runners, node 0 killed at 15 s,
restarted 10 s later, 420 s drain): `inflight_requests = 0`, all 95 webhooks
`done`, `duplicate_inflight = 0`, `names_running_twice = 0`,
`active_owner_mismatch = 0`, `sql_errors = 0`.

Open at 25/s on this machine (identical in e2–e4, so not caused by these
changes): the webhook inbox falls behind (≈800 deliveries still `received`
after the drain) and the submit p50 is several seconds.

## Follow-up: rerun lifecycle (2026-10-09, #445)

The rerun driver runs `POST /api/v1/runs/:id/rerun` concurrently with the
normal submit/acquire/complete traffic. It reads completed failed-run
candidates from Postgres, sends all/failed/job modes at an open-loop rate, and
records rerun request latency. Both rounds used RELEASE, two nodes, 2
submissions/s, 60 mocked runners, a 90-second submission window, 60-second
drain, and 500 ms median mocked jobs. Absolute throughput is workstation
capacity, not a production capacity claim.

| Round | Workflows / jobs submitted | Reruns accepted | Jobs completed | Backlog | Rerun p50 / p95 / p99 ms | Submit p50 / p99 ms | Complete p50 / p99 ms | Acquire p50 / p99 ms | Rerun txn count / total / max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline (`rerun_fraction=0`) | 245 / 585 | 0 | 584 | 1 | — / not captured / — | 225 / 448 | 14 / 101 | 3.8 / 29.2 | 0 / 0 / 0 |
| rerun-heavy (`rerun_fraction=0.25`) | 264 / 849 | 24 (8 all, 8 failed, 8 job) | 1,037 | 0 | 79 / not captured / 716 | 210 / 741 | 14 / 191 | 4.8 / 18.6 | 24 / 1,627 ms / 460 ms |

The harness emitted p50/p90/p99 (not p95) for this pair; the p95 column is
intentionally marked **not captured**, rather than substituting p90. The
rerun-heavy request sample had p90 527 ms and p99 716 ms.

Postgres integrity after the rerun-heavy drain was clean after excluding
history rows for runs still live at the sampling cutoff: duplicate in-flight
requests 0, history job-count mismatches 0, live pointer mismatches 0,
orphaned archived request pointers 0, names running twice 0, owner mismatches
0, SQL errors 0, and deadlocks 0. The original raw round query counted 128
“orphaned” pointers because 18 rerun runs still had live request rows not yet
copied by the archiver; the corrected query distinguishes live runs and
returns 0.

### Bottleneck analysis and recommendations

The rerun transaction is not a dominant throughput bottleneck at this load.
The 24 successful rerun transactions took 1.627 s total across two nodes
(67.8 ms mean; 460 ms maximum), while the normal run-row lock/flush totals
were 7.823 s / 9.862 s in the rerun-heavy round. Reruns did increase submit
p99 from 448 ms to 741 ms and complete p99 from 101 ms to 191 ms, but they
also doubled completed jobs (584 → 1,037), so this is not a controlled
single-variable latency comparison. The activity sampler observed brief
`active:Lock` samples and zero deadlocks; there was no sustained global queue
lock state. `pg_stat_statements` was unavailable to the harness connection,
so statement-level rerun attribution is not available from these rounds.

The transaction's snapshot INSERT…SELECT and per-job reset/mint work should be
profiled with `pg_stat_statements` enabled in a follow-up, especially at
higher rerun rates. Keep the direct `(run_id, job_id, request_id DESC)`
request index (already present) and the pointer integrity query in the
round harness. Repeat with p95 enabled and a larger 25/s round before making
capacity claims. SQLite was not load-tested: this harness drives a shared
Postgres control plane and has no SQLite server orchestration.
