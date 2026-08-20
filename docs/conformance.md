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

The complete evidence index is `benchmarks/compatibility/README.md`
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
experiments/mitm/bin/conform.sh --golden golden/v2.329.0/01-register-and-idle \
  --target preloop --scenario 01-register-and-idle
```

The replay gate compares status codes, job and step conclusions, request-body
schemas for jobs and annotations, and
`acquirejob` response schemas byte-for-byte; anything volatile (timing,
tokens) is normalized before comparison.

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
                        # against the surface map (docs/preloop-surface.toml)

runner-watch implement  # Codex implements the specs (dry-run: prompts only)
runner-watch review     # Claude reviews the diffs

runner-watch record-golden --runner /path/to/actions-runner --scenario <name>
runner-watch conform --runner 2.336.0 --preloop-url http://127.0.0.1:9090
                        # record official bytes for the new version and replay
                        # them against the built server

runner-watch pr         # create tiered draft PRs from the artifacts
runner-watch run        # the whole watch→diff→triage→implement→conform loop
```

The watch scope is configured in `.runner-watch/config.toml`: which upstream
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

1. **prep** (hosted): `diff` + `triage --no-agents` and posts the spec
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

## Layer 2: replayed wire using the goldens

`.runner-watch/golden/v2.335.1/` holds **23 scenario captures** from the
official runner: `01-register-and-idle`, `06-multi-step`, `07-step-failure`,
`08-job-outputs-needs`, matrix fan-out, cache round-trips, composite actions,
OIDC, containers, services, and Docker actions. The v2.336.0 conformance
run reports live in `.runner-watch/conformance/v2.336.0/` (one markdown
report per scenario, 79 files).

The `runner-watch` pipeline keeps the goldens honest across upstream
releases: it watches `actions/runner` tags, clones and diffs the upstream  
source, turns each delta into TOML specs, and re-runs the replay gate  so a new runner release cannot silently desync preloop.

```sh
just conform            # replay all goldens against the built server
runner-watch run        # watch → diff → triage → implement → conform loop
```

## Layer 3: invariants property tests

Beyond recorded bytes, the server's scheduling and concurrency behavior is
pinned by **91 property tests** in `preloop-runner-server` (proptest): queue
modes, `cancel-in-progress`, lease expiry, stale-runner reaping, assignment  
binding, and matrix/concurrency interactions. These are randomized tests  
with explicit invariants, not golden replay so  they catch the states a single  
recording never hits.

```sh
# Fast profile (CI, PRs):
PROPTEST_CASES=256 cargo test -p preloop-runner-server concurrency_properties
PROPTEST_CASES=256 cargo test -p preloop-runner-server concurrency_http_properties

# Intensive profile (nightly, release mode):
PROPTEST_CASES=10000 cargo test -p preloop-runner-server

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
restored. CR-1 (broker messageId collision) was dropped during confirmation  already fixed by review commit `193986ce`.

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
strict runtime typing, `\*` comments, parenthesized primed disjunctions). Project moves fast so the model might sometimes be out-of-date. I aim to run these weekly at-least to ensure an accurate model. 

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

1. **2026-07-28 runner campaign** (`benchmarks/real-world/results/preloop-campaign-report.md`):
 Apache ECharts, VS Code, Angular, n8n, Apache RocketMQ, Apache Pulsar,
 Cilium — official-runner-oracle runs of the preloop runner.
2. **2026-08-05 stack campaign** (`.runner-watch/repos-conformance-20260805.md`):
 bento, caddy, tokio, uv — unmodified workflows on the engine + smolvm
 pool; the environmental findings (host-OS vs `runs-on` mismatch, pool
 labels, smolvm state pileup) are documented there.
3. **Replay campaigns** (`docs/internal/conformance/`, per-repo reports):
 go-github, cli/cli, psf/requests, prettier, just, gin, black,
 eslint-config.
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
just test-ci    # fmt-check + clippy -D + full test suite + `just conform`
```

PRs touching the runner protocol interface must additionally validate wire
changes against the official runner (golden replay), per the PR template.

## Compatibility targets

- Protocol: official `actions/runner` v2.336.0, pinned in `versions.toml` and
tracked through the watch→diff→triage→conform pipeline in
[Tracking new official runner versions](#tracking-new-official-runner-versions).
- Upstream reference: `ChristopherHX/runner.server` at the pinned commit
(`PRELOOP_UPSTREAM_RUNNER_SERVER_REF`), per `docs/fidelity-gap.md`.
- Current status (2026-07): the official runner completes the full broker
lifecycle against preloop — configure → session → message → acquire →
execute → report. Verified live against real GitHub services (scenario 61:
three ephemeral runners, cache v2 save/restore through Azure Blob).

