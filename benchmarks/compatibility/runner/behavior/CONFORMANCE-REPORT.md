# Runner Conformance Comparison Report

Generated from conformance JSONL data.
Official scenarios: 10, Aksh scenarios: 10

## Summary Matrix

| # | Scenario | Official | Aksh | Match | Issues |
|---|---|---|---|---|---|
| 101 | Dynamic Matrix Dataflow | success | success | ✅ |  |
| 102 | Failure and Needs Conditions | failure | failure | ✅ |  |
| 103 | Cancellation and Background Process | success | success | ✅ |  |
| 104 | Nested Lifecycle | success | success | ✅ |  |
| 105 | Command Logs and Annotations | success | success | ✅ |  |
| 106 | Cache Artifact Pipeline | failure | failure | ✅ | step-conclusion, step-count, step-extra-in-aksh |
| 107 | Remote Action Resolution | failure | failure | ✅ | step-conclusion, step-count, step-extra-in-aksh |
| 108 | Environment Shell Filesystem | success | success | ✅ |  |
| 109 | DAG Matrix Scheduler | success | success | ✅ | step-display-name |
| 110 | Synthetic Workspace Checkout | failure | failure | ✅ |  |

**Totals**: 10 matching, 0 mismatched, 0 incomplete/missing

## Detailed Comparison

### 106 — Cache Artifact Pipeline

- Official run: 29723078636
- Aksh run: 29753024649
- Conclusions: official=failure, aksh=failure

- Step count: official=1, aksh=6 (raw: official=1, aksh=6)
- Step 1 'Set up job': official=failure, aksh=success
- Extra in aksh: 'Create unusual files' (success)
- Extra in aksh: 'Restore cache' (failure)
- Extra in aksh: 'Record cache state' (skipped)
- Extra in aksh: 'Upload artifact' (skipped)
- Extra in aksh: 'Complete job' (success)

### 107 — Remote Action Resolution

- Official run: 29723785434
- Aksh run: 29753195987
- Conclusions: official=failure, aksh=failure

- Step count: official=1, aksh=7 (raw: official=1, aksh=7)
- Step 1 'Set up job': official=failure, aksh=success
- Extra in aksh: 'Checkout pinned action source' (success)
- Extra in aksh: 'Checkout explicit secondary repository' (success)
- Extra in aksh: 'Execute pinned JavaScript action' (failure)
- Extra in aksh: 'Verify downloaded trees' (skipped)
- Extra in aksh: 'Post Checkout pinned action source' (success)
- Extra in aksh: 'Complete job' (success)

### 109 — DAG Matrix Scheduler

- Official run: 29725577528
- Aksh run: 29764600981
- Conclusions: official=success, aksh=success

- Step 2 name: official='Run echo "AKSH_ORACLE: final root=success build=success test=success package=success"' vs aksh='Run ${{ format('echo "AKSH_ORACLE: final root={0} build={1} test={2} package={3}"', …'

## Issue Categories

| Issue Type | Count |
|---|---:|
| step-extra-in-aksh | 11 |
| step-count | 2 |
| step-conclusion | 2 |
| step-display-name | 1 |

## Issue Type Reference

| Issue | Severity | Description |
|---|---|---|
| conclusion-mismatch | 🔴 Critical | Job passed on one runner but failed on the other |
| job-conclusion-mismatch | 🔴 Critical | Individual job conclusion differs |
| step-conclusion | 🟠 High | Step passed/failed differently |
| step-count | 🟡 Medium | Different number of steps executed |
| step-display-name | 🔵 Low | Step name shown differently (display only) |
| step-name-mismatch | 🟡 Medium | Step name differs in a meaningful way |
| duplicate-steps | 🟡 Medium | Aksh reports duplicate step entries |
| incomplete-run | ⚪ Info | One runner did not complete the workflow |
| no-aksh-data | ⚪ Info | Aksh has no data for this scenario |
| no-aksh-steps | 🟠 High | Aksh job has no step data (runner didn't execute) |
