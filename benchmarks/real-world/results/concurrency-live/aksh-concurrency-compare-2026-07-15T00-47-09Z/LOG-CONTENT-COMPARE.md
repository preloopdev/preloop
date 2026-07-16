# Concurrency Log/Step Content Compare: GitHub vs aksh

**GitHub root:** `/tmp/aksh-conformance/results/github-official`
**aksh root:** `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh`
**Score:** **1/23** scenarios with matching conclusions + content markers + step outcomes

## What is compared

1. **Run conclusion** (success/cancelled/failure)
2. **Job conclusion multiset**
3. **User step conclusions** (fuzzy name match; ignores hosted-only Set up job / Complete job)
4. **Content markers** in step logs: `SCENARIO=*`, `DONE=*`, cancel error annotation
5. Hosted-only infra log lines (image provisioner, GITHUB_TOKEN perms, etc.) are **stripped** before compare

| Scenario | Result | Issues | Notes |
|----------|--------|--------|-------|
| 01 bare A | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'marker' (success) | run conclusion match: success; scenario marker present: SCENARIO=01-bare-string; DONE marker present: DONE=01 |
| 01 bare B | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'marker' (success) | run conclusion match: success; scenario marker present: SCENARIO=01-bare-string; DONE marker present: DONE=01 |
| 02 cancel-in-progress A | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'sleep-long' (cancelled) | run conclusion match: cancelled; scenario marker present: SCENARIO=02-cancel-in-progress; cancel error annotation present on both |
| 02 cancel-in-progress B | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'sleep-long' (success) | run conclusion match: success; scenario marker present: SCENARIO=02-cancel-in-progress; job conclusions match: ['success'] |
| 03 FIFO A | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'sleep-a-bit' (success) | run conclusion match: success; scenario marker present: SCENARIO=03-fifo-pending; DONE marker present: DONE=03 |
| 03 FIFO B | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'sleep-a-bit' (success) | run conclusion match: success; scenario marker present: SCENARIO=03-fifo-pending; DONE marker present: DONE=03 |
| 04 cancel expression A | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'sleep-long' (cancelled) | run conclusion match: cancelled; scenario marker present: SCENARIO=04-cancel-expr-true; cancel error annotation present on both |
| 04 cancel expression B | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'sleep-long' (success) | run conclusion match: success; scenario marker present: SCENARIO=04-cancel-expr-true; job conclusions match: ['success'] |
| 05 false expression A | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'sleep-a-bit' (success) | run conclusion match: success; scenario marker present: SCENARIO=05-cancel-expr-false; DONE marker present: DONE=05 |
| 05 false expression B | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'sleep-a-bit' (success) | run conclusion match: success; scenario marker present: SCENARIO=05-cancel-expr-false; DONE marker present: DONE=05 |
| 06 queue max A | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'marker' (success) | run conclusion match: success; scenario marker present: SCENARIO=06-queue-max; DONE marker present: DONE=06 |
| 06 queue max B | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'marker' (success) | run conclusion match: success; scenario marker present: SCENARIO=06-queue-max; DONE marker present: DONE=06 |
| 06 queue max C | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'marker' (success) | run conclusion match: success; scenario marker present: SCENARIO=06-queue-max; DONE marker present: DONE=06 |
| 07 case Prod | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'marker' (success) | run conclusion match: success; scenario marker present: SCENARIO=07-case-Prod; DONE marker present: DONE=07a |
| 07 case prod | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'marker' (success) | run conclusion match: success; scenario marker present: SCENARIO=07-case-prod; DONE marker present: DONE=07b |
| 08 job-level | ❌ | user step count: gh=2 aksh=0; missing aksh step matching GH step 'one' (success); missing aksh step matching GH step 'tw | run conclusion match: success; scenario marker present: SCENARIO=08-job-level; DONE marker present: DONE=one |
| 09 multi-job | ❌ | user step count: gh=2 aksh=0; missing aksh step matching GH step 'two' (success); missing aksh step matching GH step 'on | run conclusion match: success; scenario marker present: SCENARIO=09-multi-job; DONE marker present: DONE=one |
| 10 empty group | ✅ | — | run conclusion match: failure; job conclusions match: [] |
| 11 expression group | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'marker' (success) | run conclusion match: success; scenario marker present: SCENARIO=11-expr-group-ref; DONE marker present: DONE=11 |
| 12 matrix | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'run-cell' (success) | run conclusion match: success; scenario marker present: SCENARIO=12-matrix; DONE marker present: DONE=1 |
| 13 caller JobSet | ❌ | run conclusion: gh=failure aksh=success; cross-run log contamination: aksh has SCENARIO markers ['SCENARIO=reusable-call | — |
| 14 embedded JobSet | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'run-inner' (success) | run conclusion match: success; scenario marker present: SCENARIO=reusable-callee; DONE marker present: DONE=reusable |
| 15 different-key JobSet | ❌ | user step count: gh=1 aksh=0; missing aksh step matching GH step 'run-inner' (success) | run conclusion match: success; scenario marker present: SCENARIO=reusable-callee; DONE marker present: DONE=reusable |

## Per-scenario detail

### 01 bare A
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/01-bare-A`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/01-bare-A`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'marker' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=01-bare-string
- note: DONE marker present: DONE=01
- note: job conclusions match: ['success']
- gh markers: `['DONE=01', 'SCENARIO=01-bare-string']`
- aksh markers: `['DONE=01', 'SCENARIO=01-bare-string']`

### 01 bare B
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/01-bare-B`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/01-bare-B`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'marker' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=01-bare-string
- note: DONE marker present: DONE=01
- note: job conclusions match: ['success']
- gh markers: `['DONE=01', 'SCENARIO=01-bare-string']`
- aksh markers: `['DONE=01', 'SCENARIO=01-bare-string']`

### 02 cancel-in-progress A
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/02-cancel-A`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/02-cancel-A`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'sleep-long' (cancelled)
- note: run conclusion match: cancelled
- note: scenario marker present: SCENARIO=02-cancel-in-progress
- note: cancel error annotation present on both
- note: job conclusions match: ['cancelled']
- gh markers: `['CANCEL_ERROR', 'SCENARIO=02-cancel-in-progress']`
- aksh markers: `['CANCEL_ERROR', 'SCENARIO=02-cancel-in-progress']`

### 02 cancel-in-progress B
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/02-cancel-B`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/02-cancel-B`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'sleep-long' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=02-cancel-in-progress
- note: job conclusions match: ['success']
- gh markers: `['SCENARIO=02-cancel-in-progress', 'SHOULD_NOT_REACH_EXECUTED']`
- aksh markers: `['SCENARIO=02-cancel-in-progress', 'SHOULD_NOT_REACH_EXECUTED']`

### 03 FIFO A
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/03-fifo-A`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/03-fifo-A`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'sleep-a-bit' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=03-fifo-pending
- note: DONE marker present: DONE=03
- note: job conclusions match: ['success']
- gh markers: `['DONE=03', 'SCENARIO=03-fifo-pending']`
- aksh markers: `['DONE=03', 'SCENARIO=03-fifo-pending']`

### 03 FIFO B
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/03-fifo-B`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/03-fifo-B`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'sleep-a-bit' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=03-fifo-pending
- note: DONE marker present: DONE=03
- note: job conclusions match: ['success']
- gh markers: `['DONE=03', 'SCENARIO=03-fifo-pending']`
- aksh markers: `['DONE=03', 'SCENARIO=03-fifo-pending']`

### 04 cancel expression A
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/04-cancel-expr-A`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/04-cancel-expr-A`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'sleep-long' (cancelled)
- note: run conclusion match: cancelled
- note: scenario marker present: SCENARIO=04-cancel-expr-true
- note: cancel error annotation present on both
- note: job conclusions match: ['cancelled']
- gh markers: `['CANCEL_ERROR', 'SCENARIO=04-cancel-expr-true']`
- aksh markers: `['CANCEL_ERROR', 'SCENARIO=04-cancel-expr-true']`

### 04 cancel expression B
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/04-cancel-expr-B`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/04-cancel-expr-B`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'sleep-long' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=04-cancel-expr-true
- note: job conclusions match: ['success']
- gh markers: `['SCENARIO=04-cancel-expr-true', 'SHOULD_NOT_REACH_EXECUTED']`
- aksh markers: `['SCENARIO=04-cancel-expr-true', 'SHOULD_NOT_REACH_EXECUTED']`

### 05 false expression A
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/05-expr-false-A`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/05-expr-false-A`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'sleep-a-bit' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=05-cancel-expr-false
- note: DONE marker present: DONE=05
- note: job conclusions match: ['success']
- gh markers: `['DONE=05', 'SCENARIO=05-cancel-expr-false']`
- aksh markers: `['DONE=05', 'SCENARIO=05-cancel-expr-false']`

### 05 false expression B
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/05-expr-false-B`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/05-expr-false-B`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'sleep-a-bit' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=05-cancel-expr-false
- note: DONE marker present: DONE=05
- note: job conclusions match: ['success']
- gh markers: `['DONE=05', 'SCENARIO=05-cancel-expr-false']`
- aksh markers: `['DONE=05', 'SCENARIO=05-cancel-expr-false']`

### 06 queue max A
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/06-queue-max-A`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/06-queue-max-A`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'marker' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=06-queue-max
- note: DONE marker present: DONE=06
- note: job conclusions match: ['success']
- gh markers: `['DONE=06', 'SCENARIO=06-queue-max']`
- aksh markers: `['DONE=06', 'SCENARIO=06-queue-max']`

### 06 queue max B
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/06-queue-max-B`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/06-queue-max-B`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'marker' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=06-queue-max
- note: DONE marker present: DONE=06
- note: job conclusions match: ['success']
- gh markers: `['DONE=06', 'SCENARIO=06-queue-max']`
- aksh markers: `['DONE=06', 'SCENARIO=06-queue-max']`

### 06 queue max C
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/06-queue-max-C`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/06-queue-max-C`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'marker' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=06-queue-max
- note: DONE marker present: DONE=06
- note: job conclusions match: ['success']
- gh markers: `['DONE=06', 'SCENARIO=06-queue-max']`
- aksh markers: `['DONE=06', 'SCENARIO=06-queue-max']`

### 07 case Prod
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/07a-case-Prod`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/07a-case-Prod`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'marker' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=07-case-Prod
- note: DONE marker present: DONE=07a
- note: job conclusions match: ['success']
- gh markers: `['DONE=07a', 'SCENARIO=07-case-Prod']`
- aksh markers: `['DONE=07a', 'SCENARIO=07-case-Prod']`

### 07 case prod
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/07b-case-prod`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/07b-case-prod`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'marker' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=07-case-prod
- note: DONE marker present: DONE=07b
- note: job conclusions match: ['success']
- gh markers: `['DONE=07b', 'SCENARIO=07-case-prod']`
- aksh markers: `['DONE=07b', 'SCENARIO=07-case-prod']`

### 08 job-level
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/08-job-level`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/08-job-level`
- **issue:** user step count: gh=2 aksh=0
- **issue:** missing aksh step matching GH step 'one' (success)
- **issue:** missing aksh step matching GH step 'two' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=08-job-level
- note: DONE marker present: DONE=one
- note: DONE marker present: DONE=two
- note: job conclusions match: ['success', 'success']
- gh markers: `['DONE=one', 'DONE=two', 'SCENARIO=08-job-level']`
- aksh markers: `['DONE=one', 'DONE=two', 'SCENARIO=08-job-level']`

### 09 multi-job
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/09-multi-job`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/09-multi-job`
- **issue:** user step count: gh=2 aksh=0
- **issue:** missing aksh step matching GH step 'two' (success)
- **issue:** missing aksh step matching GH step 'one' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=09-multi-job
- note: DONE marker present: DONE=one
- note: DONE marker present: DONE=two
- note: job conclusions match: ['success', 'success']
- gh markers: `['DONE=one', 'DONE=two', 'SCENARIO=09-multi-job']`
- aksh markers: `['DONE=one', 'DONE=two', 'SCENARIO=09-multi-job']`

### 10 empty group
- ok: `True`
- github: `/tmp/aksh-conformance/results/github-official/10-empty`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/10-empty-group`
- note: run conclusion match: failure
- note: job conclusions match: []
- gh markers: `[]`
- aksh markers: `[]`

### 11 expression group
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/11-expr-group`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/11-expr-group`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'marker' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=11-expr-group-ref
- note: DONE marker present: DONE=11
- note: job conclusions match: ['success']
- gh markers: `['DONE=11', 'SCENARIO=11-expr-group-ref']`
- aksh markers: `['DONE=11', 'SCENARIO=11-expr-group-ref']`

### 12 matrix
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/12-matrix`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/12-matrix`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'run-cell' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=12-matrix
- note: DONE marker present: DONE=1
- note: DONE marker present: DONE=2
- note: DONE marker present: DONE=3
- note: job conclusions match: ['success', 'success', 'success']
- gh markers: `['DONE=1', 'DONE=2', 'DONE=3', 'SCENARIO=12-matrix']`
- aksh markers: `['DONE=1', 'DONE=2', 'DONE=3', 'SCENARIO=12-matrix']`

### 13 caller JobSet
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/13-jobset-caller`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/13-jobset-caller`
- **issue:** run conclusion: gh=failure aksh=success
- **issue:** cross-run log contamination: aksh has SCENARIO markers ['SCENARIO=reusable-callee'] not present in GH capture []; capture run.log was not isolated to this run
- **issue:** cross-run log contamination: aksh has DONE markers ['DONE=reusable'] not present in GH capture []; capture run.log was not isolated to this run
- **issue:** job count: gh=0 aksh=1
- **issue:** job conclusions multiset: gh=[] aksh=['success']
- gh markers: `[]`
- aksh markers: `['DONE=reusable', 'SCENARIO=reusable-callee']`

### 14 embedded JobSet
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/14-jobset-embedded`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/14-jobset-embedded`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'run-inner' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=reusable-callee
- note: DONE marker present: DONE=reusable
- note: job conclusions match: ['success']
- gh markers: `['DONE=reusable', 'SCENARIO=reusable-callee']`
- aksh markers: `['DONE=reusable', 'SCENARIO=reusable-callee']`

### 15 different-key JobSet
- ok: `False`
- github: `/tmp/aksh-conformance/results/github-official/15-jobset-diffkey`
- aksh: `/tmp/aksh-conformance/results/final-2026-07-15/aksh-aksh/15-jobset-diffkey`
- **issue:** user step count: gh=1 aksh=0
- **issue:** missing aksh step matching GH step 'run-inner' (success)
- note: run conclusion match: success
- note: scenario marker present: SCENARIO=reusable-callee
- note: DONE marker present: DONE=reusable
- note: job conclusions match: ['success']
- gh markers: `['DONE=reusable', 'SCENARIO=reusable-callee']`
- aksh markers: `['DONE=reusable', 'SCENARIO=reusable-callee']`
