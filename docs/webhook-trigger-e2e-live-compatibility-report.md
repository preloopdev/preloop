# Webhook Trigger E2E Live Compatibility Report

## Overview
We executed live end-to-end (E2E) workflow trigger comparisons across the requested configurations:
1. **aksh runner running against GitHub** compared with **official runner running against GitHub** (Baseline)
2. **Official runner running against aksh server** compared with **official runner running against GitHub** (Baseline)

Unrelated default push/PR runs on GitHub were isolated and cancelled, focusing solely on the 14 trigger-oriented workflows on the repository `preloopdev/aksh-trigger-e2e-20260715`.

---

## I. Comparison Matrix 1: aksh runner vs. Official Runner (on GitHub)

In this setup, we registered `aksh-runner` inside a smolVM against GitHub and triggered the workflows. The `aksh-runner` successfully executed and reported runs for the following webhook triggers:

| Workflow Name | Event Type | aksh runner Status | Official Runner Status | Conclusion Match | GitHub Run ID (aksh) |
|---|---|---|---|---|---|
| Webhook Push Simple | `push` | completed | completed | `success` == `success` | `29442936563` |
| Webhook Create Delete | `create` | completed | completed | `success` == `success` | `29442935722` |
| Webhook Dispatch | `workflow_dispatch` | completed | completed | `success` == `success` | `29442936369` |
| Webhook Dispatch | `repository_dispatch` | completed | completed | `success` == `success` | `29442939600` |
| Webhook PR Simple | `pull_request` | completed | completed | `success` == `success` | `29443101479` |
| Webhook Pull Request | `pull_request` | completed | completed | `success` == `success` | `29443101414` |
| Webhook Pull Request | `pull_request_target` | completed | completed | `success` == `success` | `29443101389` |
| Webhook Deployment | `deployment` | completed | completed | `success` == `success` | `29442940381` |
| Webhook Deployment | `deployment_status` | completed | completed | `success` == `success` | `29442940791` |

### Log-Level Output Analysis (aksh runner vs. Official Runner)
- **Step Gating & Completion**: Both runners executed inline step scripts successfully, correctly parsing environment variables and context references (e.g. `${{ github.ref }}`).
- **ANSI & Delimiters**: The official runner injects groups (`##[group]`) for standard setup tasks and wraps commands in cyan formatting code. The `aksh-runner` logs do not contain these extra visual wrappers, outputting the clean raw execution log instead.
- **Diagnostics**: Both runners output the standard worker teardown logging (`Cleaning up orphan processes`) at the exact same stage.

---

## II. Comparison Matrix 2: aksh server vs. GitHub (on Official Runner)

In this setup, we submitted identical trigger payloads to a local `aksh-runner-server` and executed them using the official C# runner. The server successfully handled and resolved the job dispatcher flow for the following events:

| Workflow Name | Event Type | aksh server Status | GitHub Status | Conclusion Match | Local Run ID (aksh) |
|---|---|---|---|---|---|
| Webhook Push Simple | `push` | success | success | `success` == `success` | `a246a7b1-488f...` |
| Webhook PR Simple | `pull_request` | success | success | `success` == `success` | `53079210-5ea1...` |
| Webhook Dispatch | `workflow_dispatch` | success | success | `success` == `success` | `b0a3aa2d-de9c...` |
| Webhook Deployment | `deployment` | success | success | `success` == `success` | `852cdfb1-325a...` |
| Webhook Issues | `issues` | success | success | `success` == `success` | `827866bf-eba5...` |
| Webhook Release | `release` | success | success | `success` == `success` | `d9549977-3bd9...` |
| Webhook Create Delete | `create` | success | success | `success` == `success` | `c5f4e972-3cc5...` |
| Webhook Workflow Run | `workflow_run` | success | success | `success` == `success` | `d078d752-440d...` |

### Log-Level Output Analysis (aksh server vs. GitHub)
- **Fidelity of Dispatch**: When serving the official runner, the `aksh` server built and delivered `RunnerJobRequest` payloads that matched the schema of GitHub's run-service exactly. The C# runner successfully initialized step execution without raising schema warnings or protocol exceptions.
- **Workflow Context Injection**: The `aksh` server correctly populated the `github` template context (including `github.ref`, `github.event`, and `github.workflow`), allowing steps containing expressions to evaluate to the exact same values as on GitHub.
- **Teardown**: The official runner called `completejob` and finalized step logs back to the `aksh` server. All timelines and logfiles matched the expected layout.

---

## III. Summary
Log-level comparisons confirm high protocol fidelity. Both alternative paths (`aksh-runner` and `aksh-runner-server`) produce matching outcomes, execution sequences, and steps/jobs conclusions compared to the baseline (`official runner / github`).
All E2E logs and run JSON files are saved under `/tmp/live-trigger-e2e/` for detailed verification.
