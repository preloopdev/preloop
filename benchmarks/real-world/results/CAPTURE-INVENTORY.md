# Capture Inventory & Conformance Status
Generated: 2026-07-08 14:55 UTC
## Legend
| Icon | Meaning |
|---|---|
| ⚠️ | Both MITM captured, flow diff available |
| 🔵 | Official MITM only (no aksh) |
| 🟡 | Aksh MITM only (no official) |
| ⬜ | Neither MITM captured |
| 🟢 | Outcome match |
| 🔴 | Outcome mismatch |
| ⏳ | One side incomplete |

## Summary
64 workflows — 15 official MITM captures — 8 aksh MITM captures — 6 both — 7 outcome matches

| # | Workflow | Official | Aksh | Flow Diff | Outcome |
|---|---:|---:|---|---|
| 01 | ⚠️ register-and-idle | 68 | 3 | [1 diffs](runner-flow/01/diff.md) | — |
| 06 | ⚠️ multi-step | 47 | 240474 | [1 diffs](runner-flow/06/diff.md) | — |
| 07 | 🔵 step-failure | 50 | — | — | — |
| 08 | 🔵 job-outputs-needs | 59 | — | — | — |
| 09 | 🔵 matrix-fan-out | 73 | — | — | — |
| 10 | 🔵 uses-checkout | 36 | — | — | — |
| 11 | 🔵 cache-roundtrip | 40 | — | — | — |
| 12 | 🔵 artifact | 45 | — | — | — |
| 13 | 🔵 composite-action | 36 | — | — | — |
| 14 | 🔵 annotations | 33 | — | — | — |
| 15 | 🔵 oidc-id-token | 30 | — | — | — |
| 19 | ⬜ step-summary | — | — | — | — |
| 20 | 🟡 reusable-workflow | — | 30 | — | — |
| 21 | ⬜ job-timeout | — | — | — | — |
| 22 | ⬜ cancel-semantics | — | — | — | — |
| 23 | ⬜ context-fields | — | — | — | — |
| 24 | ⬜ problem-matcher | — | — | — | — |
| 30 | ⬜ container-job-basic | — | — | — | — |
| 31 | ⬜ container-with-services | — | — | — | — |
| 32 | ⬜ services-no-container | — | — | — | — |
| 33 | ⬜ container-env-options | — | — | — | — |
| 34 | ⬜ container-with-checkout | — | — | — | — |
| 35 | ⬜ container-lifecycle | — | — | — | — |
| 36 | ⬜ docker-action | — | — | — | — |
| 50 | ⬜ signal-sequence | — | — | — | — |
| 51 | ⬜ action-contexts | — | — | — | — |
| 52 | ⬜ expression-features | — | — | — | — |
| 53 | ⬜ secret-masking | — | — | — | — |
| 54 | ⬜ job-annotations | — | — | — | — |
| 55 | ⬜ proxy-injection | — | — | — | — |
| 56 | ⬜ problem-matcher-frompath | — | — | — | — |
| 57 | ⬜ runner-settings | — | — | — | — |
| 58 | ⬜ auth-and-diag | — | — | — | — |
| 60 | ⬜ hashfiles-and-fips | — | — | — | — |
| 61 | ⬜ cache-stress | — | — | — | — |
| 62 | ⬜ artifact-stress | — | — | — | — |
| 63 | ⬜ mega-runner-stress | — | — | — | — |
| 70 | ⬜ defaults-run | — | — | — | — |
| 71 | ⬜ composite-advanced | — | — | — | — |
| 72 | ⬜ label-matching | — | — | — | — |
| 73 | ⬜ path-env | — | — | — | — |
| 74 | ⬜ broker-poll-timing | — | — | — | — |
| 75 | 🟡 workflow-call-stress | — | 12 | — | — |
| 80 | ⬜ custom-shells | — | — | — | ⏳ (empty)/failure |
| 81 | ⬜ step-timeout | — | — | — | ⏳ (empty)/failure |
| 82 | ⬜ reusable-workflow | — | — | — | 🔴 failure/cancelled |
| 83 | ⚠️ local-node-action | 13 | 29 | — | ⏳ (empty)/success |
| 84 | ⬜ concurrency-groups | — | — | — | cancelled |
| 85 | ⬜ permissions-scoping | — | — | — | ⏳ (empty)/(empty) |
| 86 | ⬜ environment-deployments | — | — | — | ⏳ (empty)/(empty) |
| 87 | ⬜ multiline-output | — | — | — | ⏳ success/(empty) |
| 88 | ⬜ state-and-post | — | — | — | ⏳ success/(empty) |
| 89 | ⬜ workflow-inputs | — | — | — | ⏳ failure/(empty) |
| 90 | ⬜ shell-exit-behavior | — | — | — | 🟢 failure |
| 91 | ⚠️ large-output | 47 | 49 | [23 diffs](runner-flow/91-large-output/diff.md) | 🔴 failure/success |
| 92 | ⚠️ unicode-special-chars | 52 | 54 | [27 diffs](runner-flow/92-unicode-special-chars/diff.md) | 🔴 failure/success |
| 93 | ⚠️ empty-null-values | 70 | 1205 | [32 diffs](runner-flow/93-empty-null-values/diff.md) | 🟢 success |
| 94 | ⬜ action-pinning | — | — | — | 🟢 success |
| 95 | ⬜ nested-composite-outputs | — | — | — | 🟢 success |
| 96 | ⬜ env-inheritance | — | — | — | 🟢 success |
| 97 | ⬜ artifact-cross-job | — | — | — | (empty) |
| 98 | ⬜ outcome-vs-conclusion | — | — | — | 🟢 failure |
| 99 | ⬜ workspace-defaults | — | — | — | ⏳ (empty)/failure |
| 100 | ⬜ tool-cache | — | — | — | 🟢 success |

## Gaps
### Official MITM only — need aksh recapture
- **07** step-failure — 50 official flows
- **08** job-outputs-needs — 59 official flows
- **09** matrix-fan-out — 73 official flows
- **10** uses-checkout — 36 official flows
- **11** cache-roundtrip — 40 official flows
- **12** artifact — 45 official flows
- **13** composite-action — 36 official flows
- **14** annotations — 33 official flows
- **15** oidc-id-token — 30 official flows
_9 scenarios_
### Aksh MITM only — need official recapture
- **20** reusable-workflow — 30 aksh flows
- **75** workflow-call-stress — 12 aksh flows
_2 scenarios_
### Neither MITM captured
- **19** step-summary
- **21** job-timeout
- **22** cancel-semantics
- **23** context-fields
- **24** problem-matcher
- **30** container-job-basic
- **31** container-with-services
- **32** services-no-container
- **33** container-env-options
- **34** container-with-checkout
- **35** container-lifecycle
- **36** docker-action
- **50** signal-sequence
- **51** action-contexts
- **52** expression-features
- **53** secret-masking
- **54** job-annotations
- **55** proxy-injection
- **56** problem-matcher-frompath
- **57** runner-settings
- **58** auth-and-diag
- **60** hashfiles-and-fips
- **61** cache-stress
- **62** artifact-stress
- **63** mega-runner-stress
- **70** defaults-run
- **71** composite-advanced
- **72** label-matching
- **73** path-env
- **74** broker-poll-timing
- **80** custom-shells
- **81** step-timeout
- **82** reusable-workflow
- **84** concurrency-groups
- **85** permissions-scoping
- **86** environment-deployments
- **87** multiline-output
- **88** state-and-post
- **89** workflow-inputs
- **90** shell-exit-behavior
- **94** action-pinning
- **95** nested-composite-outputs
- **96** env-inheritance
- **97** artifact-cross-job
- **98** outcome-vs-conclusion
- **99** workspace-defaults
- **100** tool-cache
_47 scenarios_
