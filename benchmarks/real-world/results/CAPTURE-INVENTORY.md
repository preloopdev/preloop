# Capture Inventory & Conformance Status

Generated: 2026-07-08 14:51 UTC

## Legend

| Icon | Meaning |
|---|---|
| ✅ | Both sides captured, flow diff PASS |
| ⚠️ | Both captured, flow diff FAIL (see diff.md) |
| 🔵 | Official MITM flows only (no aksh) |
| 🟡 | Aksh MITM flows only (no official) |
| ⬜ | Neither MITM captured |
| 🟢 | Outcome match (both same conclusion) |
| 🔴 | Outcome mismatch |
| ⏳ | One side incomplete or missing |

## Summary
| Metric | Count |
|---|---:|
| Workflows | 64 |
| Official MITM flows | 14 |
| Aksh MITM flows | 8 |
| Both captured | 5 |
| Flow diffs | 6 |
| Outcome matches | 7 |

## Per-Workflow Inventory
| # | Workflow | Official | Aksh | Flow Diff | Outcome |
|---|---:|---:|---|---|
| 01 | ⚠️ register-and-idle | 68 | 3 | ⚠️ [1 diffs](runner-flow/01/diff.md) |  |
| 06 | ⚠️ multi-step | 47 | 240474 | ⚠️ [1 diffs](runner-flow/06/diff.md) |  |
| 07 | 🔵 step-failure | 50 | 0 |  |  |
| 08 | 🔵 job-outputs-needs | 59 | 0 |  |  |
| 09 | 🔵 matrix-fan-out | 73 | 0 |  |  |
| 10 | 🔵 uses-checkout | 36 | 0 |  |  |
| 11 | 🔵 cache-roundtrip | 40 | 0 |  |  |
| 12 | 🔵 artifact | 45 | 0 |  |  |
| 13 | 🔵 composite-action | 36 | 0 |  |  |
| 14 | 🔵 annotations | 33 | 0 |  |  |
| 15 | 🔵 oidc-id-token | 30 | 0 |  |  |
| 19 | ⬜ step-summary | 0 | 0 |  |  |
| 20 | 🟡 reusable-workflow | 0 | 30 |  |  |
| 21 | ⬜ job-timeout | 0 | 0 |  |  |
| 22 | ⬜ cancel-semantics | 0 | 0 |  |  |
| 23 | ⬜ context-fields | 0 | 0 |  |  |
| 24 | ⬜ problem-matcher | 0 | 0 |  |  |
| 30 | ⬜ container-job-basic | 0 | 0 |  |  |
| 31 | ⬜ container-with-services | 0 | 0 |  |  |
| 32 | ⬜ services-no-container | 0 | 0 |  |  |
| 33 | ⬜ container-env-options | 0 | 0 |  |  |
| 34 | ⬜ container-with-checkout | 0 | 0 |  |  |
| 35 | ⬜ container-lifecycle | 0 | 0 |  |  |
| 36 | ⬜ docker-action | 0 | 0 |  |  |
| 50 | ⬜ signal-sequence | 0 | 0 |  |  |
| 51 | ⬜ action-contexts | 0 | 0 |  |  |
| 52 | ⬜ expression-features | 0 | 0 |  |  |
| 53 | ⬜ secret-masking | 0 | 0 |  |  |
| 54 | ⬜ job-annotations | 0 | 0 |  |  |
| 55 | ⬜ proxy-injection | 0 | 0 |  |  |
| 56 | ⬜ problem-matcher-frompath | 0 | 0 |  |  |
| 57 | ⬜ runner-settings | 0 | 0 |  |  |
| 58 | ⬜ auth-and-diag | 0 | 0 |  |  |
| 60 | ⬜ hashfiles-and-fips | 0 | 0 |  |  |
| 61 | ⬜ cache-stress | 0 | 0 |  |  |
| 62 | ⬜ artifact-stress | 0 | 0 |  |  |
| 63 | ⬜ mega-runner-stress | 0 | 0 |  |  |
| 70 | ⬜ defaults-run | 0 | 0 |  |  |
| 71 | ⬜ composite-advanced | 0 | 0 |  |  |
| 72 | ⬜ label-matching | 0 | 0 |  |  |
| 73 | ⬜ path-env | 0 | 0 |  |  |
| 74 | ⬜ broker-poll-timing | 0 | 0 |  |  |
| 75 | 🟡 workflow-call-stress | 0 | 12 |  |  |
| 80 | ⬜ custom-shells | 0 | 0 |  | ⏳ (empty) | failure |
| 81 | ⬜ step-timeout | 0 | 0 |  | ⏳ (empty) | failure |
| 82 | ⬜ reusable-workflow | 0 | 0 |  | 🔴 failure vs cancelled |
| 83 | 🟡 local-node-action | 0 | 27 |  | ⏳ (empty) | success |
| 84 | ⬜ concurrency-groups | 0 | 0 |  | cancelled (aksh only) |
| 85 | ⬜ permissions-scoping | 0 | 0 |  | ⏳ (empty) | (empty) |
| 86 | ⬜ environment-deployments | 0 | 0 |  | ⏳ (empty) | (empty) |
| 87 | ⬜ multiline-output | 0 | 0 |  | ⏳ success | (empty) |
| 88 | ⬜ state-and-post | 0 | 0 |  | ⏳ success | (empty) |
| 89 | ⬜ workflow-inputs | 0 | 0 |  | ⏳ failure | (empty) |
| 90 | ⬜ shell-exit-behavior | 0 | 0 |  | 🟢 failure |
| 91 | ⚠️ large-output | 47 | 49 | ⚠️ [23 diffs](runner-flow/91-large-output/diff.md) | 🔴 failure vs success |
| 92 | ⚠️ unicode-special-chars | 52 | 54 | ⚠️ [27 diffs](runner-flow/92-unicode-special-chars/diff.md) | 🔴 failure vs success |
| 93 | ⚠️ empty-null-values | 70 | 1205 | ⚠️ [32 diffs](runner-flow/93/diff.md) | 🟢 success |
| 94 | ⬜ action-pinning | 0 | 0 |  | 🟢 success |
| 95 | ⬜ nested-composite-outputs | 0 | 0 |  | 🟢 success |
| 96 | ⬜ env-inheritance | 0 | 0 |  | 🟢 success |
| 97 | ⬜ artifact-cross-job | 0 | 0 |  | (empty) (aksh only) |
| 98 | ⬜ outcome-vs-conclusion | 0 | 0 |  | 🟢 failure |
| 99 | ⬜ workspace-defaults | 0 | 0 |  | ⏳ (empty) | failure |
| 100 | ⬜ tool-cache | 0 | 0 |  | 🟢 success |

## Gaps

### Official MITM flows only — need aksh recapture
- **07** — step-failure (50 flows)
- **08** — job-outputs-needs (59 flows)
- **09** — matrix-fan-out (73 flows)
- **10** — uses-checkout (36 flows)
- **11** — cache-roundtrip (40 flows)
- **12** — artifact (45 flows)
- **13** — composite-action (36 flows)
- **14** — annotations (33 flows)
- **15** — oidc-id-token (30 flows)
_(9 scenarios)_

### Aksh MITM flows only — need official recapture
- **20** — reusable-workflow (30 flows)
- **75** — workflow-call-stress (12 flows)
- **83** — local-node-action (27 flows)
_(3 scenarios)_

### Neither captured
- **19** — step-summary
- **21** — job-timeout
- **22** — cancel-semantics
- **23** — context-fields
- **24** — problem-matcher
- **30** — container-job-basic
- **31** — container-with-services
- **32** — services-no-container
- **33** — container-env-options
- **34** — container-with-checkout
- **35** — container-lifecycle
- **36** — docker-action
- **50** — signal-sequence
- **51** — action-contexts
- **52** — expression-features
- **53** — secret-masking
- **54** — job-annotations
- **55** — proxy-injection
- **56** — problem-matcher-frompath
- **57** — runner-settings
- **58** — auth-and-diag
- **60** — hashfiles-and-fips
- **61** — cache-stress
- **62** — artifact-stress
- **63** — mega-runner-stress
- **70** — defaults-run
- **71** — composite-advanced
- **72** — label-matching
- **73** — path-env
- **74** — broker-poll-timing
- **80** — custom-shells
- **81** — step-timeout
- **82** — reusable-workflow
- **84** — concurrency-groups
- **85** — permissions-scoping
- **86** — environment-deployments
- **87** — multiline-output
- **88** — state-and-post
- **89** — workflow-inputs
- **90** — shell-exit-behavior
- **94** — action-pinning
- **95** — nested-composite-outputs
- **96** — env-inheritance
- **97** — artifact-cross-job
- **98** — outcome-vs-conclusion
- **99** — workspace-defaults
- **100** — tool-cache
_(47 scenarios)_
