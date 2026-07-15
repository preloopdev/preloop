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

This establishes current aksh runtime behavior for the exercised cases. It does not, by itself, establish strict GitHub parity.

### Strict GitHub comparison

| Dimension | Result | Evidence |
|---|---:|---|
| Run conclusions | 22/23 | Scenario 13 differs because the retained GitHub baseline failed before creating a job while the fresh aksh run succeeded. |
| Job conclusion multisets | 22/23 | Same scenario-13 fixture mismatch. |
| Empty-group run shape | 1/1 | Both sides fail with zero jobs; aksh returns `queued_jobs: 0`. |
| Cancellation annotation | 2/2 | `##[error]The operation was canceled.` is present on both sides for 02A and 04A. |
| Content markers | 22/23 | Executed scenarios retain matching `SCENARIO=`/`DONE=` markers; scenario 13 has no GitHub job or markers in the retained baseline. |
| Step conclusions | 2/23 structurally comparable | Native `GET /api/v1/runs/:run_id` currently exposes job conclusions but not per-step conclusions. The strict comparator therefore fails the 21 executed scenarios instead of silently accepting missing steps. |
| Overall strict score | 1/23 | Only the zero-job empty-group case has fully comparable run, job, step-cardinality, and log-marker structures in the current capture schema. |

The low strict score is primarily an evidence-schema failure, not 22 newly observed scheduler failures. It is intentionally reported as a failure because absent step data cannot prove parity.

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

## Remaining evidence limitations

1. **Per-step conclusions are not available in the native run capture.** The results-service step update endpoint currently acknowledges updates without retaining a native step projection. Strict step parity therefore remains unproven.
2. **Scenario 13 needs a refreshed GitHub baseline.** The retained official capture failed before job creation, while the corrected absolute reusable-workflow reference and fresh aksh execution succeed.
3. **Case sensitivity remains unexercised under contention.** The two case variants were submitted independently; this does not determine whether live GitHub treats `CaseGroup` and `casegroup` as one active group.

No 23/23 parity claim should be published until the capture schema stores per-step conclusions, scenario 13 is recaptured on GitHub, and the strict comparator passes the resulting artifacts.
