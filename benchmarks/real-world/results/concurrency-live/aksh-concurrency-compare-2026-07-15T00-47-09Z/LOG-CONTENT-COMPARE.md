# Concurrency Log/Step Content Compare: GitHub vs aksh

**GitHub root:** `/tmp/aksh-conformance/results/github-official`
**aksh root:** `/tmp/aksh-sync/aksh-capture`
**Score:** **21/23** scenarios with matching conclusions + content markers + step outcomes

## What is compared

1. **Run conclusion** (success/cancelled/failure)
2. **Job conclusion multiset**
3. **User step conclusions** (fuzzy name match; ignores Set up job / Complete job)
4. **Content markers** in step logs: `SCENARIO=*`, `DONE=*`, `CANCEL_ERROR`, `SHOULD_NOT_REACH`
5. Hosted-only infra lines stripped before compare

| Scenario | Result | Issues | Notes |
|---|---|---|---|
| 01 bare-string A | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=01-bare-string; DONE marker present: DONE=01 |
| 01 bare-string B | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=01-bare-string; DONE marker present: DONE=01 |
| 02 cancel-in-progress A | ✅ | — | run conclusion match: cancelled; scenario marker present: SCENARIO=02-cancel-in-progress; cancel error annotation present on both |
| 02 cancel-in-progress B | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=02-cancel-in-progress; step 'sleep-long'≈'sleep-long' conclusion=success |
| 03 fifo-pending A | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=03-fifo-pending; DONE marker present: DONE=03 |
| 03 fifo-pending B | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=03-fifo-pending; DONE marker present: DONE=03 |
| 04 cancel-expr-true A | ✅ | — | run conclusion match: cancelled; scenario marker present: SCENARIO=04-cancel-expr-true; cancel error annotation present on both |
| 04 cancel-expr-true B | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=04-cancel-expr-true; step 'sleep-long'≈'sleep-long' conclusion=success |
| 05 cancel-expr-false A | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=05-cancel-expr-false; DONE marker present: DONE=05 |
| 05 cancel-expr-false B | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=05-cancel-expr-false; DONE marker present: DONE=05 |
| 06 queue-max A | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=06-queue-max; DONE marker present: DONE=06 |
| 06 queue-max B | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=06-queue-max; DONE marker present: DONE=06 |
| 06 queue-max C | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=06-queue-max; DONE marker present: DONE=06 |
| 07a case-Prod | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=07-case-Prod; DONE marker present: DONE=07a |
| 07b case-prod | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=07-case-prod; DONE marker present: DONE=07b |
| 08 job-level | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=08-job-level; DONE marker present: DONE=one |
| 09 multi-job-hold | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=09-multi-job; DONE marker present: DONE=one |
| 10 empty-group | ✅ | — | run conclusion match: failure |
| 11 expr-group-ref | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=11-expr-group-ref; DONE marker present: DONE=11 |
| 12 matrix-same-group | ❌ | cross-run log contamination: aksh extra DONE markers ['DONE=1.0', 'DONE=2.0', 'DONE=3.0']; DONE marker missing in aksh:  | run conclusion match: success; scenario marker present: SCENARIO=12-matrix; step 'run-cell'≈'run-cell' conclusion=success |
| 13 jobset-caller-only | ❌ | run conclusion: gh=failure aksh=success; cross-run log contamination: aksh has extra SCENARIO markers ['SCENARIO=reusabl | — |
| 14 jobset-embedded-only | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=reusable-callee; DONE marker present: DONE=reusable |
| 15 jobset-different-key | ✅ | — | run conclusion match: success; scenario marker present: SCENARIO=reusable-callee; DONE marker present: DONE=reusable |

## Per-scenario detail

### 01 bare-string A
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/01-bare-A`
- aksh: `/tmp/aksh-sync/aksh-capture/01-bare-A`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=01-bare-string
- note: DONE marker present: DONE=01
- note: step 'marker'≈'marker' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=01', 'SCENARIO=01-bare-string']`
- aksh markers: `['DONE=01', 'SCENARIO=01-bare-string']`

### 01 bare-string B
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/01-bare-B`
- aksh: `/tmp/aksh-sync/aksh-capture/01-bare-B`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=01-bare-string
- note: DONE marker present: DONE=01
- note: step 'marker'≈'marker' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=01', 'SCENARIO=01-bare-string']`
- aksh markers: `['DONE=01', 'SCENARIO=01-bare-string']`

### 02 cancel-in-progress A
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/02-cancel-A`
- aksh: `/tmp/aksh-sync/aksh-capture/02-cancel-A`
- note: run conclusion match: cancelled
- note: scenario marker present: SCENARIO=02-cancel-in-progress
- note: cancel error annotation present on both
- note: step 'sleep-long'≈'sleep-long' conclusion=cancelled
- note: job conclusions match: ['cancelled']
- gh markers: `['CANCEL_ERROR', 'SCENARIO=02-cancel-in-progress']`
- aksh markers: `['CANCEL_ERROR', 'SCENARIO=02-cancel-in-progress']`

### 02 cancel-in-progress B
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/02-cancel-B`
- aksh: `/tmp/aksh-sync/aksh-capture/02-cancel-B`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=02-cancel-in-progress
- note: step 'sleep-long'≈'sleep-long' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['SCENARIO=02-cancel-in-progress', 'SHOULD_NOT_REACH_EXECUTED']`
- aksh markers: `['SCENARIO=02-cancel-in-progress', 'SHOULD_NOT_REACH_EXECUTED']`

### 03 fifo-pending A
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/03-fifo-A`
- aksh: `/tmp/aksh-sync/aksh-capture/03-fifo-A`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=03-fifo-pending
- note: DONE marker present: DONE=03
- note: step 'sleep-a-bit'≈'sleep-a-bit' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=03', 'SCENARIO=03-fifo-pending']`
- aksh markers: `['DONE=03', 'SCENARIO=03-fifo-pending']`

### 03 fifo-pending B
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/03-fifo-B`
- aksh: `/tmp/aksh-sync/aksh-capture/03-fifo-B`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=03-fifo-pending
- note: DONE marker present: DONE=03
- note: step 'sleep-a-bit'≈'sleep-a-bit' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=03', 'SCENARIO=03-fifo-pending']`
- aksh markers: `['DONE=03', 'SCENARIO=03-fifo-pending']`

### 04 cancel-expr-true A
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/04-cancel-expr-A`
- aksh: `/tmp/aksh-sync/aksh-capture/04-cancel-expr-A`
- note: run conclusion match: cancelled
- note: scenario marker present: SCENARIO=04-cancel-expr-true
- note: cancel error annotation present on both
- note: step 'sleep-long'≈'sleep-long' conclusion=cancelled
- note: job conclusions match: ['cancelled']
- gh markers: `['CANCEL_ERROR', 'SCENARIO=04-cancel-expr-true']`
- aksh markers: `['CANCEL_ERROR', 'SCENARIO=04-cancel-expr-true']`

### 04 cancel-expr-true B
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/04-cancel-expr-B`
- aksh: `/tmp/aksh-sync/aksh-capture/04-cancel-expr-B`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=04-cancel-expr-true
- note: step 'sleep-long'≈'sleep-long' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['SCENARIO=04-cancel-expr-true', 'SHOULD_NOT_REACH_EXECUTED']`
- aksh markers: `['SCENARIO=04-cancel-expr-true', 'SHOULD_NOT_REACH_EXECUTED']`

### 05 cancel-expr-false A
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/05-expr-false-A`
- aksh: `/tmp/aksh-sync/aksh-capture/05-expr-false-A`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=05-cancel-expr-false
- note: DONE marker present: DONE=05
- note: step 'sleep-a-bit'≈'sleep-a-bit' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=05', 'SCENARIO=05-cancel-expr-false']`
- aksh markers: `['DONE=05', 'SCENARIO=05-cancel-expr-false']`

### 05 cancel-expr-false B
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/05-expr-false-B`
- aksh: `/tmp/aksh-sync/aksh-capture/05-expr-false-B`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=05-cancel-expr-false
- note: DONE marker present: DONE=05
- note: step 'sleep-a-bit'≈'sleep-a-bit' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=05', 'SCENARIO=05-cancel-expr-false']`
- aksh markers: `['DONE=05', 'SCENARIO=05-cancel-expr-false']`

### 06 queue-max A
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/06-queue-max-A`
- aksh: `/tmp/aksh-sync/aksh-capture/06-queue-max-A`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=06-queue-max
- note: DONE marker present: DONE=06
- note: step 'marker'≈'marker' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=06', 'SCENARIO=06-queue-max']`
- aksh markers: `['DONE=06', 'SCENARIO=06-queue-max']`

### 06 queue-max B
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/06-queue-max-B`
- aksh: `/tmp/aksh-sync/aksh-capture/06-queue-max-B`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=06-queue-max
- note: DONE marker present: DONE=06
- note: step 'marker'≈'marker' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=06', 'SCENARIO=06-queue-max']`
- aksh markers: `['DONE=06', 'SCENARIO=06-queue-max']`

### 06 queue-max C
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/06-queue-max-C`
- aksh: `/tmp/aksh-sync/aksh-capture/06-queue-max-C`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=06-queue-max
- note: DONE marker present: DONE=06
- note: step 'marker'≈'marker' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=06', 'SCENARIO=06-queue-max']`
- aksh markers: `['DONE=06', 'SCENARIO=06-queue-max']`

### 07a case-Prod
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/07a-case-Prod`
- aksh: `/tmp/aksh-sync/aksh-capture/07a-case-Prod`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=07-case-Prod
- note: DONE marker present: DONE=07a
- note: step 'marker'≈'marker' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=07a', 'SCENARIO=07-case-Prod']`
- aksh markers: `['DONE=07a', 'SCENARIO=07-case-Prod']`

### 07b case-prod
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/07b-case-prod`
- aksh: `/tmp/aksh-sync/aksh-capture/07b-case-prod`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=07-case-prod
- note: DONE marker present: DONE=07b
- note: step 'marker'≈'marker' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=07b', 'SCENARIO=07-case-prod']`
- aksh markers: `['DONE=07b', 'SCENARIO=07-case-prod']`

### 08 job-level
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/08-job-level`
- aksh: `/tmp/aksh-sync/aksh-capture/08-job-level`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=08-job-level
- note: DONE marker present: DONE=one
- note: DONE marker present: DONE=two
- note: step 'one'≈'one' conclusion=success
- note: step 'two'≈'two' conclusion=success
- note: job conclusions match: ['success', 'success']
- gh markers: `['DONE=one', 'DONE=two', 'SCENARIO=08-job-level']`
- aksh markers: `['DONE=one', 'DONE=two', 'SCENARIO=08-job-level']`

### 09 multi-job-hold
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/09-multi-job`
- aksh: `/tmp/aksh-sync/aksh-capture/09-multi-job`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=09-multi-job
- note: DONE marker present: DONE=one
- note: DONE marker present: DONE=two
- note: step 'two'≈'two' conclusion=success
- note: step 'one'≈'one' conclusion=success
- note: job conclusions match: ['success', 'success']
- gh markers: `['DONE=one', 'DONE=two', 'SCENARIO=09-multi-job']`
- aksh markers: `['DONE=one', 'DONE=two', 'SCENARIO=09-multi-job']`

### 10 empty-group
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/10-empty`
- aksh: `/tmp/aksh-sync/aksh-capture/10-empty-group`
- note: run conclusion match: failure
- gh markers: `[]`
- aksh markers: `[]`

### 11 expr-group-ref
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/11-expr-group`
- aksh: `/tmp/aksh-sync/aksh-capture/11-expr-group`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=11-expr-group-ref
- note: DONE marker present: DONE=11
- note: step 'marker'≈'marker' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=11', 'SCENARIO=11-expr-group-ref']`
- aksh markers: `['DONE=11', 'SCENARIO=11-expr-group-ref']`

### 12 matrix-same-group
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/12-matrix`
- aksh: `/tmp/aksh-sync/aksh-capture/12-matrix`
- **issue:** cross-run log contamination: aksh extra DONE markers ['DONE=1.0', 'DONE=2.0', 'DONE=3.0']
- **issue:** DONE marker missing in aksh: DONE=1
- **issue:** DONE marker missing in aksh: DONE=2
- **issue:** DONE marker missing in aksh: DONE=3
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=12-matrix
- note: step 'run-cell'≈'run-cell' conclusion=success
- note: job conclusions match: ['success', 'success', 'success']
- gh markers: `['DONE=1', 'DONE=2', 'DONE=3', 'SCENARIO=12-matrix']`
- aksh markers: `['DONE=1.0', 'DONE=2.0', 'DONE=3.0', 'SCENARIO=12-matrix']`

### 13 jobset-caller-only
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/13-jobset-caller`
- aksh: `/tmp/aksh-sync/aksh-capture/13-jobset-caller`
- **issue:** run conclusion: gh=failure aksh=success
- **issue:** cross-run log contamination: aksh has extra SCENARIO markers ['SCENARIO=reusable-callee']
- **issue:** cross-run log contamination: aksh extra DONE markers ['DONE=reusable']
- gh markers: `[]`
- aksh markers: `['DONE=reusable', 'SCENARIO=reusable-callee']`

### 14 jobset-embedded-only
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/14-jobset-embedded`
- aksh: `/tmp/aksh-sync/aksh-capture/14-jobset-embedded`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=reusable-callee
- note: DONE marker present: DONE=reusable
- note: step 'run-inner'≈'run-inner' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=reusable', 'SCENARIO=reusable-callee']`
- aksh markers: `['DONE=reusable', 'SCENARIO=reusable-callee']`

### 15 jobset-different-key
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/15-jobset-diffkey`
- aksh: `/tmp/aksh-sync/aksh-capture/15-jobset-diffkey`
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=reusable-callee
- note: DONE marker present: DONE=reusable
- note: step 'run-inner'≈'run-inner' conclusion=success
- note: job conclusions match: ['success']
- gh markers: `['DONE=reusable', 'SCENARIO=reusable-callee']`
- aksh markers: `['DONE=reusable', 'SCENARIO=reusable-callee']`
