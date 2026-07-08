# Capture Inventory & Conformance Status
Generated: 2026-07-08 18:26 UTC

Two separate data sources:
- **MITM flows** — raw HTTP traffic captures (flows.jsonl) from mitmproxy, recording every request/response
- **Conformance outcomes** — job conclusion + step data from GitHub's API after live workflow dispatch

## MITM Flow Captures
43 scenarios — 11 official — 4 aksh — 2 both — [diffs](runner-flow/) linked where available

| # | Scenario | Official | Aksh | Diff |
|---|---:|---:|---|
| 01 | ⚠️ register-and-idle | 68 | 3 | [1 diffs](runner-flow/01/diff.md) |
| 06 | ⚠️ multi-step | 47 | 240474 | [1 diffs](runner-flow/06/diff.md) |
| 07 | 🔵 step-failure | 50 | — | — |
| 08 | 🔵 job-outputs-needs | 59 | — | — |
| 09 | 🔵 matrix-fan-out | 73 | — | — |
| 10 | 🔵 uses-checkout | 36 | — | — |
| 11 | 🔵 cache-roundtrip | 40 | — | — |
| 12 | 🔵 artifact | 45 | — | — |
| 13 | 🔵 composite-action | 36 | — | — |
| 14 | 🔵 annotations | 33 | — | — |
| 15 | 🔵 oidc-id-token | 30 | — | — |
| 19 | ⬜ step-summary | — | — | — |
| 20 | 🟡 reusable-workflow | — | 30 | — |
| 21 | ⬜ job-timeout | — | — | — |
| 22 | ⬜ cancel-semantics | — | — | — |
| 23 | ⬜ context-fields | — | — | — |
| 24 | ⬜ problem-matcher | — | — | — |
| 30 | ⬜ container-job-basic | — | — | — |
| 31 | ⬜ container-with-services | — | — | — |
| 32 | ⬜ services-no-container | — | — | — |
| 33 | ⬜ container-env-options | — | — | — |
| 34 | ⬜ container-with-checkout | — | — | — |
| 35 | ⬜ container-lifecycle | — | — | — |
| 36 | ⬜ docker-action | — | — | — |
| 50 | ⬜ signal-sequence | — | — | — |
| 51 | ⬜ action-contexts | — | — | — |
| 52 | ⬜ expression-features | — | — | — |
| 53 | ⬜ secret-masking | — | — | — |
| 54 | ⬜ job-annotations | — | — | — |
| 55 | ⬜ proxy-injection | — | — | — |
| 56 | ⬜ problem-matcher-frompath | — | — | — |
| 57 | ⬜ runner-settings | — | — | — |
| 58 | ⬜ auth-and-diag | — | — | — |
| 60 | ⬜ hashfiles-and-fips | — | — | — |
| 61 | ⬜ cache-stress | — | — | — |
| 62 | ⬜ artifact-stress | — | — | — |
| 63 | ⬜ mega-runner-stress | — | — | — |
| 70 | ⬜ defaults-run | — | — | — |
| 71 | ⬜ composite-advanced | — | — | — |
| 72 | ⬜ label-matching | — | — | — |
| 73 | ⬜ path-env | — | — | — |
| 74 | ⬜ broker-poll-timing | — | — | — |
| 75 | 🟡 workflow-call-stress | — | 12 | — |

### Gaps
**Official only (9):** 07 step-failure, 08 job-outputs-needs, 09 matrix-fan-out, 10 uses-checkout, 11 cache-roundtrip, 12 artifact, 13 composite-action, 14 annotations, 15 oidc-id-token
**Aksh only (2):** 20 reusable-workflow, 75 workflow-call-stress
**Neither (30):** 19, 21, 22, 23, 24, 30, 31, 32, 33, 34, 35, 36, 50, 51, 52, 53, 54, 55, 56, 57, 58, 60, 61, 62, 63, 70, 71, 72, 73, 74

## Conformance Outcomes
Scenarios 80–100 dispatched against GitHub. 7 match, 3 mismatch, 9 incomplete.

| # | Scenario | Official | Aksh | Match |
|---|---|---|---|---|

| 80 | custom-shells | (empty) | failure | ⏳ (empty) / failure |
| 81 | step-timeout | (empty) | failure | ⏳ (empty) / failure |
| 82 | reusable-workflow | failure | cancelled | 🔴 failure / cancelled |
| 83 | local-node-action | (empty) | success | ⏳ (empty) / success |
| 84 | concurrency-groups | — | cancelled | — cancelled |
| 85 | permissions-scoping | (empty) | (empty) | ⏳ (empty) / (empty) |
| 86 | environment-deployments | (empty) | (empty) | ⏳ (empty) / (empty) |
| 87 | multiline-output | success | (empty) | ⏳ success / (empty) |
| 88 | state-and-post | success | (empty) | ⏳ success / (empty) |
| 89 | workflow-inputs | failure | (empty) | ⏳ failure / (empty) |
| 90 | shell-exit-behavior | failure | failure | 🟢 failure |
| 91 | large-output | failure | success | 🔴 failure / success |
| 92 | unicode-special-chars | failure | success | 🔴 failure / success |
| 93 | empty-null-values | success | success | 🟢 success |
| 94 | action-pinning | success | success | 🟢 success |
| 95 | nested-composite-outputs | success | success | 🟢 success |
| 96 | env-inheritance | success | success | 🟢 success |
| 97 | artifact-cross-job | — | (empty) | — (empty) |
| 98 | outcome-vs-conclusion | failure | failure | 🟢 failure |
| 99 | workspace-defaults | (empty) | failure | ⏳ (empty) / failure |
| 100 | tool-cache | success | success | 🟢 success |
