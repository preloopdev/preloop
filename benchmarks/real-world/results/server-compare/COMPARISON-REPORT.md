# Server Comparison Report: Official Runner vs GitHub / aksh

**Date:** 2026-07-10
**Method:** Official `actions/runner` v2.335.1 run against both GitHub Actions and aksh server, each in an independent smolVM (Alpine Linux, arm64). Results compared at job-conclusion, step-result, and step-order levels.

**What this tests:** The *server* behavior. The runner is the constant — same binary, same version, same VM image. Only the server changes.

---

## Summary

| # | Scenario | Type | GitHub | aksh | Job Match | Step Match | Notes |
|---|----------|------|--------|------|-----------|------------|-------|
| 07 | step-failure | single | failure | failure | ✅ | ⚠️ | `if: success()` step runs when it should be skipped |
| 08 | job-outputs-needs | multi (2) | success | success | ✅ | ✅ | `needs:` + outputs propagation works |
| 09 | matrix-fan-out | multi (3) | failure | failure | ✅ | ⚠️ | fail-fast cancellation correct; step name rendering differs |
| 10 | uses-checkout | single | failure | failure | ✅ | ✅ | Both fail (action download stubbed) |
| 14 | annotations | single | failure | failure | ✅ | ⚠️ | Step result reporting: GH=failure, aksh=succeeded |
| 52 | expression-features | single | success | failure | ❌ | ⚠️ | Job name includes matrix `arm64`; step 4 (nested bracket) fails |
| 53 | secret-masking | single | success | success | ✅ | ✅ | All 7 steps match including add-mask |
| 80 | custom-shells | multi (3) | failure | failure | ✅ | ⚠️ | Multi-job: all 3 job conclusions match; step data partial |
| 87 | multiline-output | single | success | success | ✅ | ✅ | Heredoc GITHUB_OUTPUT works |
| 88 | state-and-post | single | success | success | ✅ | ✅ | GITHUB_STATE + post steps work |
| 90 | shell-exit-behavior | single | failure | failure | ✅ | ⚠️ | Step 2 pipefail handling: GH=success, aksh=failed |
| 98 | outcome-vs-conclusion | single | failure | failure | ✅ | ✅ | continue-on-error, outcome vs conclusion, if: failure() |

**Job-level: 11/12 match (92%)** — only 52 mismatches (expression evaluator bug).
**Full match (job + step): 6/12 (50%)** — the remaining 6 have step-level differences.

---

## Detailed Findings

### Full Matches (6 scenarios)

**08-job-outputs-needs** — Multi-job with `needs:` dependency and output propagation. Producer sets `GITHUB_OUTPUT`, consumer reads via `${{ needs.producer.outputs.value }}`. Both jobs succeed on both servers. This validates the TemplateToken `jobOutputs` fix.

**10-uses-checkout** — `actions/checkout@v4` fails on both (action download is stubbed on aksh). Both report `failure`. Expected behavior.

**53-secret-masking** — 7 steps testing direct secret output, base64, trimming, embedding, multiline, add-mask, and verification. All succeed on both servers.

**87-multiline-output** — Heredoc-style `GITHUB_OUTPUT` with JSON, read-back, and jq parsing. All 3 steps succeed. Validates the TemplateToken `inputs` fix.

**88-state-and-post** — `GITHUB_STATE` file commands, post-step execution, state variable propagation. All 4 steps match.

**98-outcome-vs-conclusion** — `continue-on-error: true`, `steps.*.outcome` vs `steps.*.conclusion` in expressions, `if: failure()` conditional. All 6 steps match perfectly.

### Job-Level Matches with Step Differences (5 scenarios)

**07-step-failure** — 3 steps: `exit 1`, `if: failure()`, `if: success()`. The `if: success()` step runs on aksh but is skipped on GitHub. Server-side issue: after a step failure, the default `success()` condition should prevent subsequent steps from running, but the step with explicit `if: success()` is being evaluated differently.

**09-matrix-fan-out** — Matrix `[1,2,3]` with `fail-fast: true`. Job conclusions match perfectly (build(1)=failure, build(2,3)=cancelled). Step name rendering differs: GitHub renders `${{ format(...) }}` expressions in step names differently than aksh.

**14-annotations** — Step runs `echo "::warning ..."` then `exit 1`. GitHub reports the step as `failure`; aksh reports `succeeded`. The step exit code should cause failure but the aksh server's step result reporting path differs.

**80-custom-shells** — 3 jobs: python shell, sh shell, exit codes. All 3 job conclusions match. Step data from aksh is partial because multi-runner Worker diagnostics don't always correlate to jobs.

**90-shell-exit-behavior** — 6 steps. Step 2 ("Test bash pipefail failure handling") differs: GitHub=success, aksh=failed. The step uses `set +e` to suppress `errexit`, but the pipeline still fails with pipefail. This is a shell interaction edge case in how the runner wraps bash scripts.

### Mismatches (1 scenario)

**52-expression-features** — Job name includes extra matrix dimension (`arm64`) that GitHub doesn't show. Step 4 "Nested bracket and dot access" fails on aksh — the expression `fromJSON(...)['a']['b']['c']` isn't evaluating correctly. This is an expression evaluator bug in `aksh-gha-expressions`.

---

## Protocol Fixes Validated by This Comparison

1. **`jobOutputs` TemplateToken shape** — GitHub sends `{type:2, map:[{Key:{...}, Value:{...}}]}` not a plain map. Fixed in `job_builder.rs`. Validated by scenario 08.

2. **Step input TemplateToken encoding** — GitHub sends script inputs as type `0` literal tokens or type `3` `format(...)` expression tokens. Fixed in `azdo.rs`. Validated by scenario 87.

3. **Broker acknowledge endpoint** — Was returning wrong status. Fixed in server. Validated by all scenarios completing.

4. **Broker complete outputs** — Job outputs weren't being forwarded. Fixed. Validated by scenario 08.

5. **CreateStepLogsMetadata/CreateJobLogsMetadata** — Missing Twirp endpoints. Added. Validated by all scenarios.

---

## Test Infrastructure

- `scripts/compare-servers.sh` — Single-scenario comparison (GitHub + aksh sides)
- `scripts/batch-aksh-compare.sh` — Fast batch: persistent VM, all scenarios, ~3.5 min
- `scripts/compare-artifacts.py` — Deep step-level comparison from Worker diagnostics
- Results in `benchmarks/real-world/results/server-compare/<scenario>/`
- VM template: `/private/tmp/bench-runner.smolmachine` (Alpine + official runner v2.335.1)

---

## Next Steps

1. **Fix 52**: Nested bracket access in expression evaluator (`fromJSON(...)['a']['b']['c']`)
2. **Fix 14**: Step result reporting — exit code should mark step as failed
3. **Fix 07**: `if: success()` evaluation after prior failure
4. **Fix 90**: Pipefail + `set +e` shell interaction
5. **Add server-side log retrieval** — aksh server receives Twirp log uploads but doesn't expose them for retrieval. Adding a log retrieval API would enable full output comparison.
