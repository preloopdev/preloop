# Conformance

preloop treats compatibility as a test artifact. The evidence lives in four layers, from raw wire bytes to whole-repo
behavior.

```
Layer 5  whole-repo behavior   ~28 real-world repos, 39-scenario benchmark
Layer 4  formal verification   TLA+ model checking (Specula, SANY + TLC)
Layer 3  invariants            property tests (concurrency, scheduling)
Layer 2  replayed wire         goldens: official bytes replayed at preloop
Layer 1  captured wire         MITM proxy between runner and control plane
```

The evidence lives in `benchmarks/compatibility/{runner,server}/`
(separating server fidelity — official runner against GitHub versus preloop —
from runner fidelity — official runner versus preloop-runner against GitHub)
and the machine-readable captures in `.runner-watch/`.

---

## Layer 1: captured wire using the MITM proxy

The bottom layer records the **exact HTTP traffic** between the official
`actions/runner` binary and a control plane, using a mitmproxy addon
(`experiments/mitm/addons/capture.py`):

```
runner ──→ mitmproxy ──→ GitHub      (golden capture: official bytes)
runner ──→ mitmproxy ──→ preloop        (target capture: preloop's bytes)
                    ↓
              compare                (side-by-side diff report)
```

Recording a golden against real GitHub produces `.runner-watch/golden/<v>/<scenario>/flows.jsonl`
This is the "eye-level" check: we look at the request/response bodies
directly, not at aggregate behavior.

Specifically, we check:

 - HTTP method and normalized endpoint path
 - Endpoint presence and call counts
 - HTTP status codes
 - Request header names
 - Response header names
 - Request JSON body shape and values
 - Response JSON body shape and values
 - acquirejob response schema
 - Timeline PATCH payloads
 - Job and step completion payloads
 - Job conclusions
 - Step conclusions and results
 - Job outputs
 - Annotations
 - Log-file and console-log protocol requests
 - Results-service/Twirp requests when included in the scenario
 - Cache/artifact/OIDC protocol exchanges in their scenarios

For full workflow comparisons, we compare:                                                                                                                             
 - Job count
 - Job names
 - Job order
 - Step names
 - Step order
 - Job conclusions
 - Step conclusions
 - Skipped versus executed jobs
 - Matrix expansion
 - Dependency and output behavior
 - Cancellation and failure behavior
 - Annotations where the scenario exercises them    

Dynamic values are normalized or ignored:                                                                                                                              
 - Tokens
 - GUIDs
 - Runner/session IDs
 - Random URL path segments
 - Timestamps
 - Date, Server, Content-Length, request IDs  

 The goal is generally to ensure wire-level compatibility where it makes sense.  

```sh
# Record the official runner's exchange through the proxy (needs the
# official runner binary, e.g. ~/.cache/actions-runner/current):
runner-watch record-golden --runner /path/to/actions-runner --scenario <name>

# Replay a captured scenario against a running preloop server and diff every
# request/response pair:
runner-watch conform --runner 2.336.0 --preloop-url http://127.0.0.1:9090

# The older mitm worktree variant (still used for ad-hoc captures):
experiments/mitm/bin/conform.sh --golden .runner-watch/golden/v2.336.0/01-register-and-idle \
  --target preloop --scenario 01-register-and-idle
```

The replay gate compares status codes, job and step conclusions, request-body
schemas for jobs and annotations, and
`acquirejob` response schemas byte-for-byte; anything volatile (timing,
tokens) is normalized before comparison.

## Local runner-light execution

The runner conformance PR gate is `runner-conformance.yml`'s `runner-light`
job (`python3 benchmarks/real-world/runner-conformance.py --mode light`), and
`conformance.yml` replays the goldens through `benchmarks/conformance/run.sh`.
What follows is an optional local harness (not currently wired into CI) that
does not contact GitHub: it builds the current
`preloop-runner`, `preloop-server`, and native client, submits the checked-in
scenario workflows to a throwaway local server, runs the current runner against
that server, and validates that every workflow reaches a structured terminal
workflow/job/step result:

```sh
cargo build --locked -p preloop-runner-server -p preloop-runner -p preloop-runner-client
python3 benchmarks/real-world/local-runner-conformance.py \
  --server-binary target/debug/preloop-server \
  --runner-binary target/debug/preloop-runner \
  --client-binary target/debug/preloop-runner-client \
  --official benchmarks/compatibility/runner/behavior/conformance-official.jsonl \
  --exclude-prefix 2 \
  --exclude-prefix 16-container \
  --exclude-prefix 17 \
  --exclude-prefix 30 \
  --exclude-prefix 31 \
  --exclude-prefix 32 \
  --exclude-prefix 33 \
  --exclude-prefix 34 \
  --exclude-prefix 35 \
  --exclude-prefix 36 \
  --output benchmarks/compatibility/runner/behavior/conformance-preloop.jsonl
python3 benchmarks/real-world/runner-conformance.py --mode local \
  --preloop benchmarks/compatibility/runner/behavior/conformance-preloop.jsonl \
  --output benchmarks/compatibility/runner/behavior/RUNNER-CONFORMANCE-REPORT.md
```

This catches runner execution regressions that a request replay cannot see:
step dispatch, completion reporting, matrix expansion, environment/file
commands, and local action execution. The separate `light`/`deep` profiles
remain available for comparing captured GitHub results when a live campaign
has produced records. Local execution deliberately does not treat GitHub
conclusions as an oracle: infrastructure-dependent fixtures can legitimately
change conclusion on a local host.

The v2.337.0 campaign is retained as comparison material, but is not part of
the required CI gates: its reconstructed workflows are incomplete and their
acquirejob payloads are not equivalent provenance to the v2.336.0 captures.
The server-light replay targets only the shipped runner version through
`benchmarks/conformance/targets.toml`. The local runner harness excludes the
reconstructed `2xx-*` manifests plus VM-only container/service manifests; the
latter belong to the VM-backed deep profile. All remain available for explicit
campaign runs.

## Tracking new official runner versions

The pinned target is `versions.toml` (`runner_version = "2.336.0"`), and
`runner-watch` keeps the repo from silently desyncing when upstream ships a
new release. The pipeline is watch → diff → triage → implement → review →
record → conform → PR:

```sh
runner-watch watch      # poll https://github.com/actions/runner/releases.atom
                        # for new tags; records last_known_tag in
                        # .runner-watch/state.json when one appears

runner-watch diff --from v2.322.0 --to v2.335.1
                        # clone both tags and emit .runner-watch/delta.json —
                        # every source change between the two releases

runner-watch triage     # convert delta.json into per-change TOML specs
                        # (.runner-watch/specs/<version>/), AI-triaged
                        # against the surface map (docs/preloop-surface.toml,
                        # generated by `runner-watch init`)

runner-watch implement  # Codex implements the specs (dry-run: prompts only)
runner-watch review     # Claude reviews the diffs

runner-watch record-golden --runner /path/to/actions-runner --scenario <name>
runner-watch conform --runner 2.336.0 --preloop-url http://127.0.0.1:9090
                        # record official bytes for the new version and replay
                        # them against the built server

runner-watch pr         # create tiered draft PRs from the artifacts
runner-watch run        # the whole watch→diff→triage→implement→conform loop
```

The watch scope is configured in `.runner-watch/config.toml` (also generated
by `runner-watch init`): which upstream
directories are tracked (`src/Runner.Listener`, `src/Runner.Worker`,
`src/Runner.Common`, `src/Runner.Sdk`), which paths are skipped
(`src/Test/**`, `*.md`, `*.yml`, …), and which agents drive triage,
implementation, and review. Every new release therefore lands as: a version
bump in `versions.toml`, a fresh golden capture set, a conformance run, and
specs/PRs for anything the delta changed.

### Automated path (Renovate + CI)

The manual loop above is also driven automatically. Renovate
(`renovate.json`) watches the `actions/runner` releases and opens a
`runner_version` bump PR (labeled `protocol-sync`) when a new release
appears — that PR is the tripwire. `.github/workflows/runner-sync.yml` then
runs the pipeline stages:

1. **prep** (always-on self-hosted host): `diff` + `triage --no-agents` and posts the spec
   summary on the tripwire PR.
2. **sync** (self-hosted, opt-in): `record-golden` (official bytes via
   mitmproxy), `conform` (replay against the built server), and `pr`, which
   opens the tiered draft PRs — each tagged `protocol-sync` +
   `priority:{critical,high,low}` — based on a `runner-sync/v<version>`
   branch carrying the bump, goldens, conformance report, and docs.

The sync job is inert until a `[self-hosted, runner-sync]` host exists with
mitmproxy, the official runner cache, Rust toolchain, and `gh`, and
`RUNNER_SYNC_HOST_WORKSPACE` is set on the repo. Agent stages
(`implement`/`review`) run only when `RUNNER_SYNC_AGENTS=true` and the agent
CLIs (`codex`, `claude`) are installed; otherwise the tiered PRs surface the
specs for human implementation.

## Recovered v2.337.0 MITM scenarios

The v2.337.0 capture set contains four cells under
`.runner-watch/golden/v2.337.0/`: `gh-official` (27), `gh-preloop` (11),
`pl-official` (19), and `pl-preloop` (27). `gh-official` is the cell used by
the reconstruction and replay gate: its 27 captures all contain a populated
`acquirejob` response. The other cells remain comparison material; they are
not silently presented as official-runner provenance.

`benchmarks/conformance/targets.toml` is the single CI target list. The
conformance workflow starts one replay server and currently iterates only the
shipped v2.336.0 target. The recovered v2.337.0 cell is intentionally omitted
until its reconstructed workflows have protocol-equivalent provenance. Adding
a future verified version is one target entry, not another workflow job.

`experiments/mitm/bin/reconstruct_scenario.py` decodes the first populated
`acquirejob` response and emits the workflow plus the standard submit/wait
`scenario.toml`. It recovers job/step names, conditions, environment maps,
outputs, containers/services, action references, and script bodies. Script
expressions are rebuilt from the runner's `format(...)` template, including
`{{`/`}}` brace unescaping. Source-span line numbers are honored whenever the
inferred YAML structure is still before the token; inferred `on`, job IDs,
`runs-on`, `jobs`, and `steps` keys have no acquirejob source token. When a
multiline body or inferred structure has already consumed a token's line, the
generator emits that field at the next available line and reports the shift.
Comments and original scalar/flow formatting are unavailable.

The checker accepts the recorded count either with or without the single
`mitm.it` connectivity probe; the v2.337.0 summaries are not uniform about
whether that probe is included.

Per-scenario recovery gaps:

| Scenario | Not recoverable or inferred |
|---|---|
| `201-expression-edge-cases` | `on` trigger; comments/formatting |
| `202-dynamic-matrix-dataflow` | trigger; matrix definition before expansion; seven other expanded jobs; comments/formatting |
| `203-needs-dag-deep-chain` | trigger; `needs` DAG and matrix definition; other jobs/cells; comments/formatting |
| `204-composite-with-outputs-ifs` | trigger; composite action implementation; comments/formatting |
| `205-reusable-workflow-chain` | trigger; reusable child workflow and dispatch inputs; second job; comments/formatting |
| `206-cache-artifact-roundtrip` | trigger; second consumer job; action internals; comments/formatting |
| `207-masking-secrets-vars` | trigger; secret/variable declarations and values; comments/formatting |
| `208-timeout-graceful-kill` | trigger; second cancelled job and concurrency behavior; comments/formatting |
| `209-continue-on-error-cascade` | trigger; second job's job-level `continue-on-error`; comments/formatting |
| `210-service-containers-health` | trigger; original service-map formatting and implicit service environment details; comments/formatting |
| `211-job-container-volumes` | trigger; second container job; source formatting for container/volume declarations; comments/formatting |
| `212-docker-action` | trigger; local Docker action implementation; comments/formatting |
| `213-oidc-token-claims` | trigger; permissions/id-token declaration and local runner script details; comments/formatting |
| `214-checkout-repo-inspect` | trigger; nested local actions and repository contents; comments/formatting |
| `215-concurrency-cancel-in-progress` | trigger; concurrency group declaration and any unobserved run; comments/formatting |
| `216-summaries-env-cascade` | trigger; comments/formatting |
| `217-shell-variants` | trigger; comments/formatting |
| `218-node-migration` | trigger; second job and local Node action implementation; comments/formatting |
| `219-env-state-files` | trigger; comments/formatting |
| `220-label-routing` | trigger; second job and exact runner-label/group declarations; comments/formatting |
| `221-failure-reporting` | trigger; second reporter job; comments/formatting |
| `222-dispatch-inputs-typed` | trigger; typed dispatch-input declarations and matrix definition; second expanded cell; comments/formatting |
| `223-tojson-contexts` | trigger; comments/formatting |
| `224-matrix-include-exclude` | trigger; matrix include/exclude definition; other expanded cells and fail-fast ordering; comments/formatting |
| `225-docker-build-run` | trigger; comments/formatting |
| `226-multiline-outputs` | trigger; consumer job; comments/formatting |
| `227-composite-pre-post` | trigger; second job; composite action implementation and pre/post source; comments/formatting |

`on: workflow_dispatch` is an inference from the corpus convention, not a
fact present in acquirejob. Likewise, an expanded acquirejob payload cannot
prove the pre-expansion `strategy.matrix`, jobs that never ran, permissions,
concurrency declarations, or comments.

The ownership check is intentionally limited to the first populated
`acquirejob` payload used for each reconstructed workflow; matrix and
multi-job captures contain additional expanded payloads that the one-job
reconstruction does not claim to reproduce. A reconstructed workflow agrees
with that payload by construction, so the “declared job” invariant is
circular for these 27 scenarios and does **not** establish that a capture is
uncontaminated. The plan-GUID uniqueness check is independent and remains
meaningful. These artifacts must not be described as equivalent provenance to
the 39 genuinely recorded v2.336.0 scenarios.

## Layer 2: replayed wire using the goldens

`.runner-watch/golden/v2.336.0/` holds the golden set the replay gate targets
(`benchmarks/conformance/targets.toml`, replayed by `just conform`): **36
scenario captures** from the official runner — `01-register-and-idle`,
`06-multi-step`, `07-step-failure`, `08-job-outputs-needs`, matrix fan-out,
cache round-trips, composite actions, OIDC, containers, services, and Docker
actions. The prior baseline `.runner-watch/golden/v2.335.1/` (23 captures)
remains for comparison. The v2.336.0 conformance run reports under
`.runner-watch/conformance/` are generated output (gitignored), not checked-in
evidence — regenerate them locally with `runner-watch conform`.

The `runner-watch` pipeline keeps the goldens honest across upstream
releases: it watches `actions/runner` tags, clones and diffs the upstream  
source, turns each delta into TOML specs, and re-runs the replay gate  so a new runner release cannot silently desync preloop.

```sh
just conform            # replay all goldens against the built server
runner-watch run        # watch → diff → triage → implement → conform loop
```

## Layer 3: invariants property tests

Beyond recorded bytes, the server's scheduling and concurrency behavior is
pinned by **dozens of proptest cases** in `preloop-runner-server`: queue
modes, `cancel-in-progress`, lease expiry, stale-runner reaping, assignment  
binding, and matrix/concurrency interactions. These are randomized tests  
with explicit invariants, not golden replay so  they catch the states a single  
recording never hits.

```sh
# Fast profile (CI, PRs), single-threaded:
PROPTEST_CASES=256 cargo test -p preloop-runner-server \
  'concurrency::properties' -- --test-threads=1
PROPTEST_CASES=256 cargo test -p preloop-runner-server \
  concurrency_properties::pure -- --test-threads=1
PROPTEST_CASES=256 cargo test -p preloop-runner-server \
  concurrency_properties::state_machine -- --test-threads=1
PROPTEST_CASES=64 cargo test -p preloop-runner-server \
  concurrency_http_properties -- --test-threads=1

# Intensive profile (`just test-properties-full`), including the ignored cases:
PROPTEST_CASES=10000 cargo test --locked -p preloop-runner-server --quiet
PROPTEST_CASES=10000 cargo test --locked -p preloop-runner-server --quiet -- --ignored

# Structural guards in CI: every property file must match ≥1 test, and no
# test may contain `sleep(` (flaky-time guards).
```

## Layer 4: formal verification using TLA+ model checking (Specula)

Beyond randomized invariants, the concurrency model is *model-checked*:
the [Specula](https://github.com/SpeculaIO/Specula) pipeline (code analysis
→ TLA+ spec generation → validation → bug confirmation) built a TLA+
specification of the server's scheduling/gate logic from the Rust source,
repaired it against TLC's strict typing during validation, and hunted bugs
with real SANY + TLC runs (`experiments/specula-20260804/`). Project moves fast so the model can be abit outdated. We try to run them atleast every week. For instance we found these bugs from the latest run. 

**Six findings, all fixed and reconciled into the current tree** (2026-08-06):


| Finding | Bug (model semantics)                                                                                                                              | Disposition                                                                                     |
| ------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| MC-S2   | Workflow concurrency-gate leak                                                                                                                     | Fixed; synchronized in `base.tla`                                                               |
| MC-S3   | Job-level gate bypass                                                                                                                              | Fixed; confirmed as a code bug, not a model bug                                                 |
| MC-S5   | Step-transition loss                                                                                                                               | Fixed                                                                                           |
| MC-S6   | `format` brace-escape handling                                                                                                                     | Fixed                                                                                           |
| MC-R1   | `apply_matrix_fail_fast` never released the concurrency slot of the siblings it cancelled                                                          | Fixed (2026-08-06); regression test `fail_fast_releases_the_cancelled_sibling_concurrency_slot` |
| MC-R2   | `cancel_in_progress` could cancel a predecessor of the *arriving* run, letting `release_concurrency_for_run` evict the holder it had just admitted | Fixed; regression test `same_run_cancel_in_progress_keeps_the_arriving_holder`                  |


MC-R1 and MC-R2 were each confirmed by reverting the fix and watching the  
regression test fail on the predicted symptom, then pass again with the fix  
restored. The broker `messageId` collision case was dropped — already fixed by commit `193986ce`.

Artifacts: `spec/base.tla` (single SANY-valid module), TLC configs per
scenario, four counterexample traces, `spec/bug-report.md` (per-bug Rust
source evidence), `spec/findings.json` (current status per finding), and
per-finding confirmation verdicts in `confirmation/`.

Re-running (Java 21 + `tla2tools.jar`):

```sh
cd experiments/specula-20260804/spec
java -cp /path/to/tla2tools.jar tla2sany.SANY base.tla
java -cp /path/to/tla2tools.jar tlc2.TLC -config MC_hunt_s2_concurrency.safety.cfg -workers auto -deadlock MC
```

Known TLC pitfalls from the run are documented in the experiment README
(sequential runs or separate `-metadir`s, single-name `CONSTRAINT` entries,
strict runtime typing, `\*` comments, parenthesized primed disjunctions).

## Layer 5: whole-repo behavior through differential runs

The top layer runs real workflows end to end and compares *behavior*: job
and step names, order, and conclusions.

**39-scenario benchmark.** The `experiments/mitm/scenarios/` corpus (trivial
jobs, cancellation, matrix fan-out, OIDC, container jobs, service health,
artifacts, annotations, reusable callers) is executed. Results are recorded per scenario in
`benchmarks/{act,agent_ci,preloop}_scenarios_results.json`. 

**Real-world repos.** Unmodified workflows from ~28 distinct public repos
run against the preloop stack across five campaigns, with GitHub's own run as
the oracle:

```sh
gh run view --log <run-id>        # oracle: GitHub's step names/order/conclusions
# …run the same workflow on preloop (preloop run), then diff the two:
# step names, step order, job conclusions, job count.
```

1. **2026-07-28 runner campaign** (`benchmarks/real-world/results/aksh-campaign-report.md`):
 Apache ECharts, VS Code, Angular, n8n, Apache RocketMQ, Apache Pulsar,
 Cilium, Apache Kafka — official-runner-oracle runs of the preloop runner.
2. **2026-08-05 stack campaign** (`.runner-watch/repos-conformance-20260805.md`):
 bento, caddy, tokio, uv — unmodified workflows on the engine + smolvm
 pool; the environmental findings (host-OS vs `runs-on` mismatch, pool
 labels, smolvm state pileup) are documented there.
3. **Replay campaigns** (`benchmarks/real-world/results/conformance-*`, per-repo
 reports): django, fmt, grpc, hugo, jekyll, junit, laravel, nushell, pytest,
 testcontainers, bat, nextcloud, uv, vite, deno, grafana, helm, pip,
 prometheus, rails, webpack.
4. **Earlier openclaw / preloop-trigger era** (`benchmarks/real-world/results/`):
 axum, bat, serde, buzz, nextcloud, qm, vite, agent-ci, openclaw — with
 e2e flow captures (`e2e-*.jsonl`) and comparison reports
 (`UNIFIED-COMPARISON.md`, `FLOW-DIFF-REPORT.md`).

The methodology — including known environment divergences (host OS vs
`runs-on` labels, container jobs) — is documented alongside each campaign.

**Differential probes.** The concurrency-property harness runs the same
scenario against GitHub and preloop and compares conclusions:

```sh
python3 benchmarks/real-world/run-concurrency-property-probes.py \
  --corpus benchmarks/real-world/concurrency-property-cases.json   # live probes
python3 benchmarks/real-world/run-concurrency-property-probes.py \
  --dry-run --corpus …/concurrency-property-cases.json             # CI-safe
```

## The gate

```sh
just test-ci    # fmt-check + clippy -D + zizmor + test suite + `just conform`
```

PRs touching the runner protocol interface must additionally validate wire
changes against the official runner (golden replay), per the PR template.

## Compatibility targets

- Protocol: official `actions/runner` v2.336.0, pinned in `versions.toml` and
tracked through the watch→diff→triage→conform pipeline in
[Tracking new official runner versions](#tracking-new-official-runner-versions).
- Upstream reference: `ChristopherHX/runner.server` at the pinned commit,
  per `docs/fidelity-gap.md`.
- Current status (2026-07): the official runner completes the full broker
lifecycle against preloop — configure → session → message → acquire →
execute → report. Verified live against real GitHub services (scenario 61:
three ephemeral runners, cache v2 save/restore through Azure Blob).

