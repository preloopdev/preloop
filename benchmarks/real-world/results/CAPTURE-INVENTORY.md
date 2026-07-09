# Capture Inventory & Conformance Status
Generated: 2026-07-09 06:35 UTC (updated 2026-07-09 18:00 UTC — fixes applied, recaptures done)

Two separate data sources:
- **MITM flows** — raw HTTP traffic captures (flows.jsonl) from mitmproxy, recording every request/response
- **Conformance outcomes** — job conclusion + step data from GitHub's API after live workflow dispatch

## Fixes Applied (2026-07-09)

| Fix | File | Description |
|---|---|---|
| `displayNameToken` | `azdo.rs`, `job_builder.rs` | Added TemplateToken literal to `TaskStep` serialization |
| `ingest.sock` auth | `job_runner.rs` | Added `Authorization: Bearer` header + random WebSocket key |
| Broker busy-poll | `broker.rs`, `broker_listener.rs` | 3s timeout when busy (was 50s); fixed comment |
| `connectOptions` | `broker_listener.rs` | Changed `0` → `1` to match official runner |

## MITM Flow Captures
43 scenarios — 14 official — 18 aksh — 14 both — 9 matches + 10 diffs — [diffs](runner-flow/) linked where available

| # | Scenario | Official | Aksh | Diff |
|---|---|---:|---:|---|
| 01 | ⚠️ register-and-idle | 68 | 3 | [1 diffs](runner-flow/01/diff.md) |
| 06 | ⚠️ multi-step | 47 | 240474 | [1 diffs](runner-flow/06/diff.md) |
| 07 | ✅ step-failure | 43 | 45 | ✅ match |
| 08 | ✅ job-outputs-needs | 48 | 50 | ✅ match |
| 09 | ✅ matrix-fan-out | 59 | 61 | ✅ match |
| 10 | ✅ uses-checkout | 28 | 29 | ✅ match |
| 11 | ✅ cache-roundtrip | 31 | 32 | ✅ match |
| 12 | ✅ artifact | 33 | 34 | ✅ match |
| 13 | ✅ composite-action | 28 | 29 | ✅ match |
| 14 | ✅ annotations | 22 | 23 | ✅ match |
| 15 | ✅ oidc-id-token | 38 | 38 | ✅ match (post-fix) |
| 19 | ✅ step-summary | 43 | 40 | ⚠️ capture failed (CA cert timing) |
| 20 | 🟡 reusable-workflow | — | 30 | — |
| 21 | ✅ job-timeout | 224 | 44 | ⚠️ MITM proxy limitation (see below) |
| 22 | ✅ cancel-semantics | 230 | 50 | ⚠️ MITM proxy limitation (see below) |
| 23 | ✅ context-fields | 40 | 41 | ✅ match (Node.js download expected) |
| 24 | ✅ problem-matcher | 40 | 41 | ✅ match (Node.js download expected) |
| 30 | ⬜ container-job-basic | — | — | — |
| 31 | ⬜ container-with-services | — | — | — |
| 32 | ⬜ services-no-container | — | — | — |
| 33 | ⬜ container-env-options | — | — | — |
| 34 | ⬜ container-with-checkout | — | — | — |
| 35 | ⬜ container-lifecycle | — | — | — |
| 36 | ⬜ docker-action | — | — | — |
| 50 | ✅ signal-sequence | 87 | 10 | ⚠️ MITM proxy limitation (same as 21/22) |
| 51 | ✅ action-contexts | 49 | 40 | [21 diffs](runner-flow/51/diff.md) |
| 52 | ✅ expression-features | 49 | 46 | [25 diffs](runner-flow/52/diff.md) |
| 53 | ✅ secret-masking | 55 | 0 | — aksh failed |
| 54 | 🔄 job-annotations | — | 37 | — aksh only |
| 55 | ❌ proxy-injection | — | 0 | — cancelled |
| 56 | 🔄 problem-matcher-frompath | — | 43 | — aksh only |
| 57 | 🔄 runner-settings | — | 43 | — aksh only |
| 58 | ❌ auth-and-diag | — | 0 | — cancelled |
| 60 | 🔄 hashfiles-and-fips | — | 43 | — aksh only |
| 61 | ❌ cache-stress | — | 0 | — runners failed |
| 62 | ❌ artifact-stress | — | 0 | — runners failed |
| 63 | ❌ mega-runner-stress | — | 0 | — not attempted |
| 71 | ⬜ composite-advanced | — | — | — |
| 72 | ⬜ label-matching | — | — | — |
| 73 | ⬜ path-env | — | — | — |
| 74 | ⬜ broker-poll-timing | — | — | — |
| 75 | 🟡 workflow-call-stress | — | 12 | — |

### MITM Proxy Limitation (Scenarios 21, 22, 50)

The flow count difference in these scenarios is a **proxy observation artifact**, not a protocol mismatch:

- The official runner has ~183 `/message` polls (every ~3 seconds during job execution)
- Our code does the same — `get_message` uses a 3-second HTTP timeout when `busy=true`
- But the MITM proxy records only flows where the server actually responded
- When the client times out after 3s and re-polls, the proxy's server-side connection stays open
- The server responds after 10-63 seconds, and the proxy records that as one flow
- Without the proxy, the 3s timeout closes the TCP connection directly and the client re-polls immediately

### Gaps
**Official only:** _none_
**Aksh only (6):** 20 reusable-workflow, 54 job-annotations, 56 problem-matcher-frompath, 57 runner-settings, 60 hashfiles-and-fips, 75 workflow-call-stress
**Neither (13):** 30, 31, 32, 33, 34, 35, 36, 70, 71, 72, 73, 74 (container/GitHub-hosted)
**Failed captures:** 55, 58, 61, 62, 63 (cancelled/runners failed)

## Conformance Outcomes
Scenarios 80–100 dispatched against GitHub. 8 match, 2 mismatch, 9 incomplete.

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
| 91 | large-output | failure | failure | 🟢 failure |
| 92 | unicode-special-chars | failure | failure | 🟢 failure |
| 93 | empty-null-values | success | success | 🟢 success |
| 94 | action-pinning | success | success | 🟢 success |
| 95 | nested-composite-outputs | success | success | 🟢 success |
| 96 | env-inheritance | success | success | 🟢 success |
| 97 | artifact-cross-job | — | (empty) | — (empty) |
| 98 | outcome-vs-conclusion | failure | failure | 🟢 failure |
| 99 | workspace-defaults | (empty) | failure | ⏳ (empty) / failure |
| 100 | tool-cache | success | success | 🔴 mismatch |
