# Conformance

aksh treats compatibility as a test artifact, not an assertion in prose.
The evidence lives in four layers, from raw wire bytes to whole-repo
behavior; the index below maps each layer to its artifacts and commands.

```
Layer 4  whole-repo behavior   real-world repos, 39-scenario benchmark
Layer 3  invariants            property tests (concurrency, scheduling)
Layer 2  replayed wire         goldens: official bytes replayed at aksh
Layer 1  captured wire         MITM proxy between runner and control plane
```

The complete evidence index is `benchmarks/compatibility/README.md`
(separating server fidelity — official runner against GitHub versus aksh —
from runner fidelity — official runner versus aksh-runner against GitHub)
and the machine-readable captures in `.runner-watch/`.

---

## Layer 1: captured wire — the MITM proxy

The bottom layer records the **exact HTTP traffic** between the official
`actions/runner` binary and a control plane, using a mitmproxy addon
(`experiments/mitm/addons/capture.py`):

```
runner ──→ mitmproxy ──→ GitHub      (golden capture: official bytes)
runner ──→ mitmproxy ──→ aksh        (target capture: aksh's bytes)
                    ↓
              compare                (side-by-side diff report)
```

Recording a golden against real GitHub produces `.runner-watch/golden/<v>/<scenario>/flows.jsonl` —
every request method, path, header, and body, plus the response, timestamped.
This is the "eye-level" check: we look at the request/response bodies
directly, not at aggregate behavior.

```sh
# Record the official runner's exchange through the proxy (needs the
# official runner binary, e.g. ~/.cache/actions-runner/current):
runner-watch record-golden --runner /path/to/actions-runner --scenario <name>

# Replay a captured scenario against a running aksh server and diff every
# request/response pair:
runner-watch conform --runner 2.336.0 --aksh-url http://127.0.0.1:9090

# The older mitm worktree variant (still used for ad-hoc captures):
experiments/mitm/bin/conform.sh --golden golden/v2.329.0/01-register-and-idle \
  --target aksh --scenario 01-register-and-idle
```

The replay gate compares status codes, request-body schemas, and
`acquirejob` response schemas byte-for-byte; anything volatile (timing,
tokens) is normalized before comparison.

## Layer 2: replayed wire — the goldens

`.runner-watch/golden/v2.335.1/` holds **23 scenario captures** from the
official runner: `01-register-and-idle`, `06-multi-step`, `07-step-failure`,
`08-job-outputs-needs`, matrix fan-out, cache round-trips, composite actions,
OIDC, containers, services, and Docker actions. The v2.336.0 conformance
run reports live in `.runner-watch/conformance/v2.336.0/` (one markdown
report per scenario, 79 files).

The `runner-watch` pipeline keeps the goldens honest across upstream
releases: it watches `actions/runner` tags, clones and diffs the upstream
source, turns each delta into TOML specs, and re-runs the replay gate —
so a new runner release cannot silently desync aksh.

```sh
just conform            # replay all goldens against the built server
runner-watch run        # watch → diff → triage → implement → conform loop
```

## Layer 3: invariants — property tests

Beyond recorded bytes, the server's scheduling and concurrency behavior is
pinned by **91 property tests** in `aksh-runner-server` (proptest): queue
modes, `cancel-in-progress`, lease expiry, stale-runner reaping, assignment
binding, and matrix/concurrency interactions. These are randomized tests
with explicit invariants, not golden replay — they catch the states a single
recording never hits.

```sh
# Fast profile (CI, PRs):
PROPTEST_CASES=256 cargo test -p aksh-runner-server concurrency_properties
PROPTEST_CASES=256 cargo test -p aksh-runner-server concurrency_http_properties

# Intensive profile (nightly, release mode):
PROPTEST_CASES=10000 cargo test -p aksh-runner-server

# Structural guards in CI: every property file must match ≥1 test, and no
# test may contain `sleep(` (flaky-time guards).
```

## Layer 4: whole-repo behavior — differential runs

The top layer runs real workflows end to end and compares *behavior*: job
and step names, order, and conclusions.

**39-scenario benchmark.** The `experiments/mitm/scenarios/` corpus (trivial
jobs, cancellation, matrix fan-out, OIDC, container jobs, service health,
artifacts, annotations, reusable callers) is executed on act, agent-ci, and
Preloop on the same host; results are recorded per scenario in
`benchmarks/{act,agent_ci,preloop}_scenarios_results.json`. Latest run:
Preloop 31/39 correct behavior, act 29/39, agent-ci 29/39 (details and
per-scenario timings in `docs/act-vs-others.md`).

**Real-world repos.** Unmodified workflows from medium-sized public repos
run against the aksh stack, with GitHub's own run as the oracle:

```sh
gh run view --log <run-id>        # oracle: GitHub's step names/order/conclusions
# …run the same workflow on aksh (preloop run), then diff the two:
# step names, step order, job conclusions, job count.
```

Eight repos covered so far (go-github, cli/cli, psf/requests, prettier,
just, gin, black, eslint-config); per-repo reports are in
`docs/internal/conformance/`, and the methodology — including known
environment divergences (host OS vs `runs-on` labels, container jobs) — is
documented alongside.

**Differential probes.** The concurrency-property harness runs the same
scenario against GitHub and aksh and compares conclusions:

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

- Protocol: official `actions/runner` v2.336.0 (`versions.toml`), tracked
  by `runner-watch` against upstream releases.
- Upstream reference: `ChristopherHX/runner.server` at the pinned commit
  (`AKSH_UPSTREAM_RUNNER_SERVER_REF`), per `docs/fidelity-gap.md`.
- Current status (2026-07): the official runner completes the full broker
  lifecycle against aksh — configure → session → message → acquire →
  execute → report. Verified live against real GitHub services (scenario 61:
  three ephemeral runners, cache v2 save/restore through Azure Blob).
