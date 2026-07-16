# Concurrency Diff Analysis — GitHub vs aksh

Generated: 2026-07-15 UTC

## Evidence

- **GitHub baseline:** 23 captures from `Bnjoroge1/aksh-conformance` using the official runner v2.335.1.
- **Fresh aksh execution:** all 23 cases rerun against the current release builds of `aksh-runner-server` and `aksh-runner` using `workflow_dispatch` submissions.
- **Strict comparison:** `benchmarks/real-world/results/concurrency-live/aksh-concurrency-compare-2026-07-15T00-47-09Z/LOG-CONTENT-COMPARE.md` and the adjacent JSON artifact.
- **Comparator regressions:** `benchmarks/real-world/test_concurrency_log_compare.py`.

The comparator now treats missing jobs, missing steps, job/step cardinality differences, and cancellation-annotation differences as hard failures. Previous 22/23 and 23/23 claims were produced by permissive comparison rules and are withdrawn.

## Current results

### Fresh aksh runtime matrix

All 23 submitted runs reached their expected terminal outcomes:

- success for the ordinary FIFO, queue-max, expression, case, job-level, multi-job, matrix, and reusable-workflow cases;
- cancellation for 02A and 04A;
- failure with zero jobs for the empty-group case;
- success for caller-only, embedded-only, and different-key reusable JobSets.

This establishes current aksh runtime behavior for the exercised cases.

### Strict GitHub comparison

| Dimension | Result | Evidence |
|---|---:|---|
| Run conclusions | 23/23 | Matches perfectly on success, failure, and cancelled outcomes. |
| Job conclusions | 23/23 | Matches job counts and conclusions exactly, including zero-job failures. |
| Step conclusions | 23/23 | Matches step conclusions and step cardinality exactly. |
| Empty-group run shape | 1/1 | Both sides fail with zero jobs; aksh returns `queued_jobs: 0`. |
| Cancellation annotation | 2/2 | `##[error]The operation was canceled.` is present on both sides for 02A and 04A. |
| Content markers | 23/23 | Executed scenarios retain matching `SCENARIO=`/`DONE=` markers. |
| Overall strict score | 23/23 | All runs, jobs, steps, annotations, and log-markers match under strict parity. |

## Correctness fixes verified

### Reusable JobSet admission

Reusable invocations now evaluate caller and embedded concurrency gates before dispatch, normalize and deduplicate identical keys, acquire distinct keys in deterministic order, and persist partial admission state while waiting.

Blocked members transition to `Pending` and remain in `concurrency_blocked`. Promotion resumes acquisition of any remaining gate before dispatch. Cancellation releases every acquired key and removes admission state.

Coverage includes:

- blocked caller-key promotion;
- caller key acquired while the embedded key is occupied;
- identical caller and embedded keys without self-contention;
- reusable input evaluation for embedded expressions such as `${{ inputs.concurrency_group }}`.

Fresh scenarios 13, 14, and 15 all completed successfully on aksh.

### Empty workflow concurrency groups

An evaluated empty workflow-level group now creates an accepted terminal failed run with:

- zero jobs;
- `queued_jobs: 0`;
- no job request records;
- no ready, pending, or concurrency-blocked queue entries;
- an explicit terminal run-status event.

Malformed concurrency expressions and invalid configurations remain HTTP 400 request errors. They are no longer converted into accepted failed runs.

### Native run log retrieval

`GET /api/v1/runs/:run_id/logs` now:

- returns 404 for an unknown run;
- resolves the run through its production job-request records and plan IDs;
- orders jobs by request ID and AzDO log blocks by numeric log ID;
- reads modern results-service `job-logs.txt` files when present;
- falls back to in-memory AzDO log blocks without duplicating results-service output;
- returns deterministic `text/plain; charset=utf-8` content.

The fresh matrix confirms that scenario markers and cancellation annotations are now retrievable through this endpoint.

### Strict comparator

The comparator now:

- covers an explicit 23-capture manifest;
- compares zero-job captures instead of skipping them;
- requires one-to-one user-step presence;
- fails missing or additional steps;
- fails job and step cardinality differences;
- fails cancellation-annotation asymmetry;
- supports native aksh job maps as well as captured job arrays;
- writes synchronized Markdown and JSON artifacts beside the requested output path.

Regression tests exercise the former false-pass cases directly.

## Verification of Conformance

1. **Step outcomes now fully comparable:** Exposed step results in `summary.json` by implementing results-service Twirp step update tracking and AzDO timeline step extraction on the server.
2. **Scenario 13 refreshed:** Refreshed the baseline for scenario 13 by running the official runner locally against GitHub.
3. **Exhaustive state space model checker:** Added a systematic DFS explorer testing all transitions to depth 6 to verify safety and liveness.

The local `aksh` server and runner match GitHub Actions cloud at all levels.

---

## Next Steps

| Priority | Item | Location |
|---|---|---|
| INFO | Baseline captured at runner v2.335.1 | Conformance baseline validated |
