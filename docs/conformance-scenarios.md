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

| # | Directory | Fixture | Surface exercised | Expected current aksh status | Record now? |
|---|---|---|---|---|---|
| 06 | `06-multi-step` | `06-multi-step.yml` | step list, env, multiline run scripts, logs/timeline | mostly implemented | yes |
| 07 | `07-step-failure` | `07-step-failure.yml` | step failure conclusions and conditional execution | mostly implemented | yes |
| 08 | `08-job-outputs-needs` | `08-job-outputs-needs.yml` | job outputs propagated through `needs` context | implemented | yes |
| 09 | `09-matrix-fan-out` | `09-matrix-fan-out.yml` | matrix expansion and fail-fast sibling cancellation | implemented | yes |
| 10 | `10-uses-checkout` | `10-uses-checkout.yml` | action resolution/download and codeload auth | partial/stubbed | yes |
| 11 | `11-cache-roundtrip` | `11-cache-roundtrip.yml` | modern cache restore/save paths | runner-side v2 save/restore verified against GitHub with ephemeral runners; local server v2 remains separate gap | yes |
| 12 | `12-artifact` | `12-artifact.yml` | artifact upload/download v4 paths | partial/missing v4 parity | yes |
| 13 | `13-composite-action` | `13-composite-action.yml` | local composite action resolution | partial | yes |
| 14 | `14-annotations` | `14-annotations.yml` | workflow command annotations and timeline issues | partial | yes |
| 15 | `15-oidc-id-token` | `15-oidc-id-token.yml` | OIDC token endpoint and requested audience | implemented | yes |
| 16 | `16-container-job` | `16-container-job.yml` | job container startup/protocol | missing/deferred | no, Linux+Docker required |
| 17 | `17-service-container` | `17-service-container.yml` | service container startup/protocol | missing/deferred | no, Linux+Docker required |

## Files generated

For each scenario `NN-name`:

- `experiments/mitm/scenarios/NN-name/scenario.toml`
- `experiments/mitm/scenarios/NN-name/NN-name.yml`

`13-composite-action` also includes `actions/greet/action.yml` as the helper composite-action
fixture to copy into the recording repository at `.github/actions/greet/action.yml` before official
recording.

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

Then import that golden into this repo:

```sh
cd ~/runner-watcher
cargo run -p runner-watch -- record-golden --runner v2.335.1 --scenario 06-multi-step
```

Repeat for `06-multi-step` through `15-oidc-id-token`. Defer `16-container-job` and
`17-service-container` until a Linux runner with Docker is available.

## Running conformance

Start aksh, then replay all imported goldens:

```sh
experiments/mitm/bin/up-aksh.sh
cargo run -p runner-watch -- conform --runner v2.335.1 --aksh-url http://127.0.0.1:9090
```

Outputs:

- `.runner-watch/conformance/v2.335.1/<scenario>.md`
- `.runner-watch/conformance-report.md`

Important: the current conformance gate fails only on missing official-only endpoints. Some partial
compatibility bugs show up only as body/status diffs or runner log errors, especially checkout,
annotations, cache, artifact, and composite-action behavior. Inspect each per-scenario report; do
not rely only on the rollup pass/fail.

## Expansion rules

When adding another scenario:

1. Pick a fidelity gap or upstream controller surface first.
2. Give the scenario a unique two-digit directory and unique workflow basename.
3. Keep `scenario.toml` to supported primitives: `submit_workflow`, `wait_for_event`,
   `wait_seconds`, and `cancel_workflow`.
4. Record the official golden before treating the scenario as conformance evidence.
5. If the host cannot run the scenario, save the definition but mark recording deferred with the
   concrete prerequisite.
