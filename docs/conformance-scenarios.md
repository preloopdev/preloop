# Runner conformance scenario catalog

This catalog defines the next runner-watcher scenarios for comparing aksh against the official
GitHub Actions runner. The goal is not to invent random workflows; every scenario must force a
specific runner protocol surface that appears in `docs/fidelity-gap.md`, upstream
`actions/runner`, or runner-watch release review notes.

## Where scenarios come from

Use these sources, in order:

1. `docs/fidelity-gap.md` scorecard rows marked partial or missing.
2. `docs/fidelity-gap.md` controller inventory for upstream `runner.server` surfaces.
3. `.runner-watch/review.toml` and `.runner-watch/reviews/v2.335.1/*.toml` feature notes.
4. Upstream `actions/runner` code in the tracked listener, worker, common, and SDK directories.
5. GitHub Actions workflow features that force runner behavior: `uses:` actions, cache, artifacts,
   matrix, `needs`, OIDC, annotations, containers, services, cancellation, and job outputs.

Rule: a new scenario should cite the protocol surface it covers. If it does not map to an aksh
compatibility gap or upstream runner behavior, it does not belong in this suite.

## Fixture vs. scenario vs. golden

- **Fixture**: the static input, usually workflow YAML and any helper files. Example:
  `11-cache-roundtrip.yml`.
- **Scenario**: the driver recipe in `scenario.toml` that submits the fixture and waits for runner
  events such as job assignment and completion.
- **Golden**: the official GitHub runner traffic captured for that scenario in `flows.jsonl`.
- **Conformance**: replaying the official golden against aksh and comparing endpoints, status
  codes, headers, and bodies.

Short version: the fixture is the question, the golden is the official answer, the scenario is the
script that asks the question, and conformance checks whether aksh gives the same answer.

## Implemented scenario definitions

The scenario definitions live in `experiments/mitm/scenarios/`. Each new scenario uses
a unique workflow fixture basename matching the directory name because the official backend runs
`gh workflow run <basename>`.

| # | Directory | Surface exercised | v2.336.0 golden |
|---|---|---|---|
| 01 | `01-register-and-idle` | registration, session creation, idle polling | yes |
| 02 | `02-trivial-job` | minimal broker job lifecycle | yes |
| 03 | `03-cancellation` | cancellation delivery and completion | yes |
| 04 | `04-request-ack` | explicit request acknowledgement | yes |
| 05 | `05-multi-job` | multiple jobs in one workflow | yes |
| 06 | `06-multi-step` | step list, env, multiline scripts, logs/timeline | yes |
| 07 | `07-step-failure` | failure conclusions and conditional execution | yes |
| 08 | `08-job-outputs-needs` | job outputs through `needs` | yes |
| 09 | `09-matrix-fan-out` | matrix expansion and fail-fast | yes |
| 10 | `10-uses-checkout` | action resolution and codeload auth | yes |
| 11 | `11-cache-roundtrip` | cache v2 restore/save and blob handoff | yes |
| 12 | `12-artifact` | artifact v4 upload/download | yes |
| 13 | `13-composite-action` | local composite action resolution | yes |
| 14 | `14-annotations` | annotations and timeline issues | yes |
| 15 | `15-oidc-id-token` | OIDC token endpoint and audience | yes |
| 16 | `16-container-job` | job container startup/protocol | yes |
| 17 | `17-service-container` | service container startup/protocol | yes |
| 30 | `30-container-job-basic` | basic container job | yes |
| 31 | `31-container-with-services` | job container with services | yes |
| 32 | `32-services-no-container` | host job with services | yes |
| 33 | `33-container-env-options` | container env and options | yes |
| 34 | `34-container-with-checkout` | checkout inside a job container | yes |
| 35 | `35-container-lifecycle` | lifecycle and continue-on-error | yes |
| 36 | `36-docker-action` | `docker://` action references | yes |
| 101 | `101-dynamic-matrix-dataflow` | dynamic matrix expansion via fromJson step outputs | pending |
| 102 | `102-mask-and-secret-propagation` | add-mask redaction and multiline env/output commands | pending |
| 103 | `103-composite-nested-post` | composite action step tree and post execution order | pending |
| 104 | `104-job-defaults-env-cascade` | defaults.run scoping and env precedence cascade | pending |
| 105 | `105-concurrency-cancellation-group` | concurrency locks and mid-flight cancellation signals | pending |
| 107 | `107-continue-on-error-status-funcs` | continue-on-error and status functions (always/failure) | pending |
| 108 | `108-workflow-dispatch-inputs` | workflow_dispatch schema defaults and input context | pending |
| 109 | `109-log-streaming-backpressure` | high-frequency streaming, ANSI codes, long lines | pending |
| 110 | `110-environment-deployment-url` | environment deployment lifecycle with dynamic URL outputs | pending |
| 111 | `111-github-state-post-execution` | GITHUB_STATE persistence across post step execution | pending |
| 112 | `112-service-container-health-ports` | service container health check polling and port bindings | pending |
| 113 | `113-artifact-v4-multi-pattern` | artifact v4 multi-pattern glob include/exclude and download | pending |
| 114 | `114-step-timeout-graceful-kill` | step timeout enforcement and signal escalation | pending |
| 115 | `115-cache-v2-restore-fallback` | cache v2 key miss and restore-keys prefix fallback | pending |
| 101 | `101-dynamic-matrix-dataflow` | dynamic matrix expansion via fromJson step outputs | yes |
| 102 | `102-mask-and-secret-propagation` | add-mask redaction and multiline env/output commands | yes |
| 103 | `103-composite-nested-post` | composite action step tree and post execution order | yes |
| 104 | `104-job-defaults-env-cascade` | defaults.run scoping and env precedence cascade | yes |
| 105 | `105-concurrency-cancellation-group` | concurrency locks and mid-flight cancellation signals | yes |
| 107 | `107-continue-on-error-status-funcs` | continue-on-error and status functions (always/failure) | yes |
| 108 | `108-workflow-dispatch-inputs` | workflow_dispatch schema defaults and input context | yes |
| 109 | `109-log-streaming-backpressure` | high-frequency streaming, ANSI codes, long lines | yes |
| 110 | `110-environment-deployment-url` | environment deployment lifecycle with dynamic URL outputs | yes |
| 111 | `111-github-state-post-execution` | GITHUB_STATE persistence across post step execution | yes |
| 112 | `112-service-container-health-ports` | service container health check polling and port bindings | yes |
| 113 | `113-artifact-v4-multi-pattern` | artifact v4 multi-pattern glob include/exclude and download | yes |
| 114 | `114-step-timeout-graceful-kill` | step timeout enforcement and signal escalation | yes |
| 115 | `115-cache-v2-restore-fallback` | cache v2 key miss and restore-keys prefix fallback | yes |
| 163 | `163-reusable-caller` | local reusable workflows, input types, output mapping | yes |

## Files generated

For each scenario `NN-name`:

- `experiments/mitm/scenarios/NN-name/scenario.toml`
- `experiments/mitm/scenarios/NN-name/NN-name.yml`

`13-composite-action` also includes `actions/greet/action.yml` as the helper composite-action
fixture to copy into the recording repository at `.github/actions/greet/action.yml` before official
recording.
`163-reusable-caller` also includes `workflows/reusable.yml` as the helper reusable workflow
fixture to copy into the recording repository at `.github/workflows/reusable.yml`.

## Recording official goldens

Before recording, commit each workflow fixture to the target GitHub repository as
`.github/workflows/<fixture-basename>`. The basename must exactly match the fixture in the scenario
directory. For `13-composite-action`, also commit the helper action to
`.github/actions/greet/action.yml`.

Required environment:

```sh
export GITHUB_OWNER=...
export GITHUB_REPO=...
export GITHUB_REF=...
export GITHUB_RUNNER_TOKEN=...
```

Record a scenario with the MITM tooling:

```sh
cd experiments/mitm
bin/record-golden.sh --scenario 06-multi-step --non-interactive
```

Container scenarios require Linux and Docker. Record them with:

```sh
experiments/mitm/bin/record-golden-linux.sh
```

The committed corpus lives at `.runner-watch/golden/v2.336.0/`. The corpus checker
requires one non-empty, version-matched capture for every scenario definition.
Capture success means the official runner completed its protocol exchange. It does
not imply every workload step succeeded; the isolated smolVM capture environment
cannot faithfully provide embedded Docker DNS/localhost or trusted proxy TLS for
all of scenarios 31, 32, 34, and 35.

## Running conformance

Replay the complete current corpus:

```sh
just conform
```

Outputs:

- `.runner-watch/conformance/v2.336.0/<scenario>.md`
- `.runner-watch/conformance/v2.336.0/conformance-report.md`

The gate fails on missing official-only endpoints, status mismatches, request body
schema mismatches, and acquirejob response schema regressions. Local URLs, tokens,
IDs, and other volatile values are normalized.

## Expansion rules

When adding another scenario:

1. Pick a fidelity gap or upstream controller surface first.
2. Give the scenario a unique two-digit directory and unique workflow basename.
3. Keep `scenario.toml` to supported primitives: `submit_workflow`, `wait_for_event`,
   `wait_seconds`, and `cancel_workflow`.
4. Record the official golden before treating the scenario as conformance evidence.
5. If the host cannot run the scenario, save the definition but mark recording deferred with the
   concrete prerequisite.
