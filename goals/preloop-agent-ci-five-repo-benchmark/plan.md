# Execution plan

## Solution approach

Build a reproducible benchmark harness with one manifest and two execution adapters: the current Preloop CLI/engine and Agent CI. Run identical pinned workflow revisions across five deliberately different repositories. Add structured, correlated phase events around the existing control-plane, orchestrator, runner, and result paths instead of inferring timings from terminal output. Store raw event records and resource samples per run, then generate aggregate comparisons with cold and warm populations kept separate.

Selected initial repository set:

1. `BurntSushi/ripgrep` — Rust CLI build and test workload.
2. `pallets/flask` — Python package/test workload.
3. `vitejs/vite` — JavaScript/TypeScript package and multi-step build workload.
4. `go-chi/chi` — Go module test workload.
5. `testcontainers/testcontainers-go` — Go plus Docker/container-heavy integration workload.

At execution time, verify each repository still exposes a public, runnable workflow and pin the exact commit and workflow path in the manifest. Replace only a repository that cannot run without unavailable secrets, documenting the reason.

## Ordered steps

1. **Freeze benchmark inputs and acceptance criteria.**
   - Add a benchmark manifest containing repository URL, commit SHA, workflow path, event, expected jobs, runner labels, timeout, and platform-specific adaptation notes.
   - Record Preloop commit, Agent CI version/API revision, runner image, host details, toolchain versions, and benchmark configuration.
   - Define cold-start as no reusable runner/golden VM available and warm-run as reuse of the prepared runner environment; never mix the populations in one statistic.
   - Verification: manifest validation rejects missing SHAs, floating refs, missing workflow paths, duplicate repositories, and secret-looking values.

2. **Define the Agent CI adapter and secret boundary.**
   - Implement a narrow adapter for the available Agent CI CLI/API once its endpoint and authentication surface are confirmed.
   - Keep credentials in environment/secret storage; redact headers, tokens, repository secrets, and action credentials from raw events.
   - Normalize both systems into one run record: submission ID, job IDs, phase timestamps, conclusion, failure phase, retries, cancellation, and resource samples.
   - Verification: adapter dry-run validates credentials without submitting a workload; fixture responses normalize into the common schema with secrets absent.

3. **Instrument the Preloop control-plane path.**
   - Add structured benchmark events around CLI submission and result polling in `crates/preloop-cli/src/main.rs`.
   - Add correlated timestamps around workflow loading/parsing, matrix expansion, concurrency admission, queue insertion, job dispatch, and completion in `crates/aksh-runner-server/src/runs.rs` and scheduler/bootstrap paths.
   - Reuse the run ID/job ID as correlation keys; do not alter official runner wire DTOs for benchmark-only fields.
   - Verification: a no-op or tiny local run emits a complete ordered submission-to-completion trace with monotonic timestamps.

4. **Instrument VM, runner, and action lifecycle.**
   - Add lifecycle events around golden preparation, image/container preload, VM create/fork, VM start/resume, guest readiness, runner registration, job acquisition, successor pre-provisioning, guest exit, debug preservation, and deletion in `crates/preloop-orchestrator/src/lib.rs`.
   - Capture checkout and remote-action preparation boundaries in `crates/aksh-runner/src/worker/action_preparation.rs`, the worker job/step runners, and local snapshot creation/serve paths in `crates/aksh-runner-server/src/snapshots.rs`.
   - Include VM name, generation, cold/warm class, image fingerprint, queue wait, and action/checkout result without logging source or secrets.
   - Verification: a synthetic runner-provider test covers every lifecycle event and guarantees terminal events occur once even on boot, job, and teardown failures.

5. **Add resource sampling and run artifact storage.**
   - Sample host and guest CPU, memory, disk, network bytes, active VM count, container count, and peak values at a fixed interval; record provider limitations explicitly when a metric is unavailable.
   - Store one immutable raw JSONL stream and one normalized summary per run, keyed by benchmark campaign, repository, system, and repetition.
   - Add checks for clock monotonicity, missing phase boundaries, duplicate terminal events, and secret redaction before artifacts are accepted.
   - Verification: replay a captured trace into the normalizer and assert stable summaries and redaction; intentionally malformed traces fail validation.

6. **Run the controlled matrix.**
   - For each repository, run three cold-start repetitions and five warm repetitions on Preloop and Agent CI.
   - Keep commit, workflow, host class, concurrency, timeout, network policy, cache policy, and container image policy constant where possible; record unavoidable differences rather than silently normalizing them away.
   - Retain failures, retries, cancellations, and setup failures as data. Do not retry a failed run into a pass without preserving the original attempt.
   - Verification: the harness produces exactly 80 planned system/repository/repetition records and refuses to publish a campaign with incomplete rows.

7. **Aggregate and compare results.**
   - Compute median and p95 end-to-end latency, queue/boot/resume/checkout/action/workload/result/teardown distributions, throughput, success rate, retry/cancellation rate, and peak resource use.
   - Separate controller/Agent CI overhead from workload execution and report cold and warm populations independently.
   - Compare expected workflow conclusions and step/job outcomes for fidelity; flag platform-specific adaptations and unavailable metrics beside the numbers.
   - Verification: deterministic fixture data yields known aggregates; report generation fails on incomplete or mixed-population input.

8. **Publish the benchmark report and rerun instructions.**
   - Produce a report containing the selected commits, exact commands, environment, raw-artifact locations, phase diagrams, tables, confidence/variance notes, failures, and conclusions.
   - Include a limitations section covering SmolVM startup failures, unavailable Agent CI telemetry, network variance, caches, and any repository substitutions.
   - Verification: a clean checkout can validate the manifest, run the harness in dry-run mode, and regenerate the report from stored artifacts without credentials in the repository.

## Verification commands

- `just test-ci`
- `cargo test -p preloop-orchestrator`
- `cargo test -p aksh-runner-server`
- Benchmark manifest validation and dry-run commands from the new harness.
- A one-repository end-to-end smoke run on each adapter before the full 80-run matrix.

## Risks and open questions

- The exact Agent CI CLI/API, available runner labels, and telemetry surface are not present in this repository; execution requires those access details.
- Agent CI may not expose VM or container resource counters equivalent to SmolVM. The report must mark unavailable metrics rather than compare unlike measurements as equal.
- Public repositories may change workflow policy or require external services between planning and execution; immutable commits and a preflight audit are mandatory.
- Caches can dominate warm-run results. The harness must record cache state and either disable caches or report cache-hit populations separately.
- The current local environment has demonstrated SmolVM `machine start` stalls with no runner registration. The campaign must classify infrastructure setup failures separately from workflow failures and cannot claim a complete comparison until the runner pool is healthy.
