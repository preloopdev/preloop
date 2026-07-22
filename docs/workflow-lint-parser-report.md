# Real-World Workflow Lint/Parser Report

**Last updated:** 2026-07-21  
**aksh command:** `cargo run -p aksh-runner -- lint -W <workflow>`  
**Reference linter:** `actionlint`

Remote reusable workflows are resolved through GitHub's Contents API when the
server or standalone lint command encounters `owner/repo/.github/workflows/x.yml@ref`.
Set `AKSH_GITHUB_TOKEN` for private repositories; public repositories can use
unauthenticated requests subject to GitHub API rate limits.

The server also resolves each remote reference to its immutable commit SHA and
propagates the repository, ref, and SHA into expanded job plans and reusable-call
metadata.

This document tracks real public GitHub Actions workflows exercised against `aksh` and `actionlint`. Results are from the current default branches at the time of each run; workflows can change upstream.

## Current results

| Repository | Workflow | `aksh` result | `actionlint` result | Notes |
|---|---|---:|---:|---|
| [denoland/deno](https://github.com/denoland/deno/blob/main/.github/workflows/ci.generated.yml) | `ci.generated.yml` | **pass** — 134 expanded job plans, 3,041 steps | shellcheck findings | Large generated workflow; useful parser/matrix stress test. |
| [rust-lang/rust](https://github.com/rust-lang/rust/blob/master/.github/workflows/ci.yml) | `ci.yml` | **pass** — 3 job plans, 38 steps | shellcheck findings | Expression-valued strategy booleans and matrix `include` are preserved; unresolved runtime output uses a lint placeholder. |
| [microsoft/TypeScript](https://github.com/microsoft/TypeScript/blob/main/.github/workflows/ci.yml) | `ci.yml` | **pass** — 26 job plans, 135 steps | custom runner labels plus shellcheck findings | `runs-on` includes Azure/1ES-specific labels. |
| [neovim/neovim](https://github.com/neovim/neovim/blob/master/.github/workflows/test.yml) | `test.yml` | **pass** — 30 job plans, 322 steps | shellcheck findings | Requires `--workspace-root` with a checkout so local reusable workflows can be loaded. |
| [pytorch/pytorch](https://github.com/pytorch/pytorch/blob/main/.github/workflows/_linux-test.yml) | `_linux-test.yml` | **pass** — 1 deferred-input job plan, 17 steps | shellcheck findings | Standalone called workflow has no caller inputs; matrix expansion is deferred to a placeholder. |
| [preloopdev/aksh](https://github.com/preloopdev/aksh/blob/907db62fa1b294f76bb041c55586394dba517fa7/.github/workflows/ci.yml) | `ci.yml` | **pass** | not recorded | Repository's own CI workflow. |

### Apache projects

Ten Apache repositories were exercised against `aksh` lint and `actionlint` to validate
parser/evaluator coverage across diverse workflow patterns (reusable workflows, large
matrices, expression-valued fields, multi-OS matrices). All ten pass `aksh` lint.

| Repository | Workflow | `aksh` result | `actionlint` result | Notes |
|---|---:|---:|---:|---|
| [apache/superset](https://github.com/apache/superset/blob/master/.github/workflows/superset-python-unittest.yml) | `superset-python-unittest.yml` | **pass** — 3 job plans, 8 steps | exit 1 — unknown runner label `ubuntu-26.04` | Uses expression-valued `if` conditions for matrix selection. |
| [apache/echarts](https://github.com/apache/echarts/blob/master/.github/workflows/ci.yml) | `ci.yml` | **pass** — 2 job plans, 16 steps | exit 1 — shellcheck findings | Simple matrix with Node.js version strategy. |
| [apache/airflow](https://github.com/apache/airflow/blob/main/.github/workflows/basic-tests.yml) | `basic-tests.yml` | **pass** — 10 job plans, 56 steps | exit 1 — shellcheck findings | Uses `fromJSON` expressions in job names and matrix includes. |
| [apache/spark](https://github.com/apache/spark/blob/master/.github/workflows/build_main.yml) | `build_main.yml` | **pass** — 32 job plans, 450 steps | exit 0 — clean | Local reusable workflow `./.github/workflows/build_and_test.yml`; requires `--workspace-root`. |
| [apache/kafka](https://github.com/apache/kafka/blob/trunk/.github/workflows/ci.yml) | `ci.yml` | **pass** — 12 job plans, 87 steps | exit 0 — clean | Local reusable workflow `./.github/workflows/build.yml`; requires `--workspace-root`. |
| [apache/flink](https://github.com/apache/flink/blob/master/.github/workflows/ci.yml) | `ci.yml` | **pass** — 13 job plans, 189 steps | exit 0 — clean | Local reusable workflow `./.github/workflows/template.pre-compile-checks.yml`; requires `--workspace-root`. |
| [apache/dubbo](https://github.com/apache/dubbo/blob/3.3/.github/workflows/build-and-test-pr.yml) | `build-and-test-pr.yml` | **pass** — 32 job plans, 302 steps | exit 1 — shellcheck + expression type issues + old action versions | Large matrix with `include` entries; uses `actions/cache@v3`. |
| [apache/skywalking](https://github.com/apache/skywalking/blob/master/.github/workflows/skywalking.yaml) | `skywalking.yaml` | **pass** — 221 job plans, 2,760 steps | exit 1 — shellcheck + expression type issues | Largest expansion in this set; matrix `include` with complex objects (docker configs, e2e test cases). |
| [apache/rocketmq](https://github.com/apache/rocketmq/blob/develop/.github/workflows/maven.yaml) | `maven.yaml` | **pass** — 3 job plans, 15 steps | exit 1 — old action version `actions/checkout@v2` | Simple multi-OS matrix. |
| [apache/apisix](https://github.com/apache/apisix/blob/master/.github/workflows/build.yml) | `build.yml` | **pass** — 4 job plans, 68 steps | exit 1 — shellcheck findings | Matrix with glob patterns in step names. |

### Wider validation set (20 projects)

Twenty additional repositories were tested to exercise large generated workflows,
deep reusable workflow chaining, complex matrix expansion, and YAML edge cases.

| Repository | Workflow | `aksh` result | `actionlint` result | Notes |
|---|---:|---:|---:|---|
| [denoland/deno](https://github.com/denoland/deno/blob/main/.github/workflows/ci.generated.yml) | `ci.generated.yml` | **pass** — 134 job plans, 3,041 steps | shellcheck findings | 405 KB generated workflow; repeated for regression. |
| [cilium/cilium](https://github.com/cilium/cilium/blob/main/.github/workflows/tests-clustermesh-upgrade.yaml) | `tests-clustermesh-upgrade.yaml` | **pass** — 10 job plans, 137 steps | shellcheck findings + expression type issues | 43 KB; requires `--workspace-root` (101 workflow files). |
| [bytecodealliance/wasmtime](https://github.com/bytecodealliance/wasmtime/blob/main/.github/workflows/main.yml) | `main.yml` | **pass** — 44 job plans, 542 steps | shellcheck findings | 62 KB; deep reusable workflow calls. |
| [home-assistant/core](https://github.com/home-assistant/core/blob/dev/.github/workflows/ci.yaml) | `ci.yaml` | **pass** — 42 job plans, 331 steps | shellcheck findings | 53 KB; large matrix with multi-stage dependencies. |
| [astral-sh/ruff](https://github.com/astral-sh/ruff/blob/main/.github/workflows/ci.yaml) | `ci.yaml` | **pass** — 55 job plans, 507 steps | shellcheck findings + custom runner label `codspeed-macro` | 52 KB; extensive job graph. |
| [pytorch/pytorch](https://github.com/pytorch/pytorch/blob/main/.github/workflows/generated-linux-binary-manywheel-nightly.yml) | `generated-linux-binary-manywheel-nightly.yml` | **pass** — 59 job plans, 502 steps | custom runner label + shellcheck findings | 81 KB generated; some remote refs hit API rate limit. |
| [vercel/next.js](https://github.com/vercel/next.js/blob/canary/.github/workflows/build_and_test.yml) | `build_and_test.yml` | **pass** — 162 job plans | shellcheck + `if: false` warning | Coerces boolean job-level `if` conditions to strings (finding #6). |
| [hashicorp/terraform](https://github.com/hashicorp/terraform/blob/main/.github/workflows/build.yml) | `build.yml` | **pass** — 40 job plans, 241 steps | shellcheck findings | Resolves bare `needs` references to all matrix-expanded instances (finding #7). |
| [microsoft/vscode](https://github.com/microsoft/vscode/blob/main/.github/workflows/component-fixtures.yml) | `component-fixtures.yml` | **pass** — 4 job plans, 47 steps | shellcheck findings | Screenshot-based testing. |
| [angular/angular](https://github.com/angular/angular/blob/main/.github/workflows/ci.yml) | `ci.yml` | **pass** — 13 job plans, 176 steps | custom runner label + shellcheck | Multi-stage CI. |
| [godotengine/godot](https://github.com/godotengine/godot/blob/master/.github/workflows/linux_builds.yml) | `linux_builds.yml` | **pass** — 41 job plans, 154 steps | shellcheck findings | Platform matrix with feature-flags. |
| [nodejs/node](https://github.com/nodejs/node/blob/main/.github/workflows/benchmark.yml) | `benchmark.yml` | **pass** — 2 job plans, 21 steps | shellcheck findings | Benchmark comparison. |
| [apache/arrow](https://github.com/apache/arrow/blob/main/.github/workflows/cpp_extra.yml) | `cpp_extra.yml` | **pass** — 54 job plans, 485 steps | shellcheck findings | C++ cross-build matrix; requires `--workspace-root`. |
| [nixos/nixpkgs](https://github.com/nixos/nixpkgs/blob/master/.github/workflows/check.yml) | `check.yml` | **pass** — 2 job plans, 9 steps | exit 0 — clean | Ownership checker. |
| [n8n-io/n8n](https://github.com/n8n-io/n8n/blob/master/.github/workflows/docker-build-push.yml) | `docker-build-push.yml` | **pass** — 6 job plans, 19 steps | exit 0 — clean | Docker build/push. Remote ref hit rate limit. |
| [charmbracelet/gum](https://github.com/charmbracelet/gum/blob/main/.github/workflows/build.yml) | `build.yml` | **pass** — 1 job plan, 13 steps | exit 0 — clean | Goreleaser build. Remote ref hit rate limit. |
| [elastic/kibana](https://github.com/elastic/kibana/blob/main/.github/workflows/trigger-chromium-build.yml) | `trigger-chromium-build.yml` | **pass** — 1 job plan, 8 steps | shellcheck findings | Trigger workflow. |
| [mozilla/gecko-dev](https://github.com/mozilla/gecko-dev/blob/master/.github/workflows/close-pr.yml) | `close-pr.yml` | **pass** — 1 job plan, 4 steps | exit 0 — clean | Minimal (640 bytes). |
| [rails/rails](https://github.com/rails/rails/blob/main/.github/workflows/rails_releaser_tests.yml) | `rails_releaser_tests.yml` | **pass** — 1 job plan, 11 steps | exit 0 — clean | Simple test. |
| [apache/pulsar](https://github.com/apache/pulsar/blob/master/.github/workflows/ci-pulsarbot.yaml) | `ci-pulsarbot.yaml` | **pass** — 1 job plan, 10 steps | exit 0 — clean | Pulsar bot CI. |

`actionlint` exit status is nonzero for the listed shellcheck findings even when YAML and workflow schema validation succeeds. Treat those separately from parser/schema failures.

## Reproduction

Fetch individual workflows:

```sh
curl -L --fail \
  https://raw.githubusercontent.com/rust-lang/rust/master/.github/workflows/ci.yml \
  -o /tmp/rust-ci.yml
```

Run `aksh`:

```sh
cargo run -p aksh-runner -- lint -W /tmp/rust-ci.yml
```

Run `actionlint`:

```sh
actionlint /tmp/rust-ci.yml
```

For local reusable workflows, use a checkout instead of downloading one YAML file:

```sh
git clone --filter=blob:none --no-checkout https://github.com/neovim/neovim.git /tmp/neovim
cd /tmp/neovim
git sparse-checkout set .github/workflows
git checkout master
find .github/workflows -type f \( -name '*.yml' -o -name '*.yaml' \) -print
cargo run -p aksh-runner -- lint -W .github/workflows/test.yml
```

## Findings to track

### 1. Expression-valued typed scalar fields — implemented

Real workflows use expressions in fields represented by typed YAML values, for example:

```yaml
continue-on-error: ${{ matrix.continue_on_error || false }}
```

The parser now preserves these values as deferred scalars and resolves them when a matrix/input context is available. The current `JobPlan` protocol remains concrete, so unresolved runtime expressions use a conservative lint placeholder rather than pretending to produce the final matrix.

Previously, the parser attempted to deserialize that string directly into `bool`, producing errors such as:

```text
invalid type: string "${{ ... }}", expected a boolean
```

The expression-aware representation is implemented for strategy booleans/numbers and step continuation flags. Additional scalar fields can use the same pattern as they are encountered.

### 2. Expression-valued matrices — implemented with deferred lint behavior

Reusable workflows may define matrices from JSON expressions:

```yaml
strategy:
  matrix: ${{ fromJSON(inputs.test-matrix) }}
```

The parser now preserves expression-valued matrices and evaluates them with reusable-workflow inputs. A standalone called workflow without caller inputs cannot produce concrete combinations; lint preserves the workflow and emits one deferred placeholder plan. Server submissions with resolved caller inputs expand concrete combinations.

### 3. Local reusable workflow resolution — implemented for lint

A single downloaded workflow is insufficient when it references a local reusable workflow:

```yaml
uses: ./.github/workflows/test_windows.yml
```

The lint CLI now accepts `--workspace-root`, loads local workflow files, and passes them to the same reusable-workflow expander used by server submissions.

### 4. Remote reusable workflow resolution — implemented

Remote references are fetched from:

```text
https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={ref}
```

Fetched workflows are inserted into the reusable-workflow table and scanned
for nested remote references up to the same depth limit used by the parser.
The server resolves these before job expansion, matching the official runner's
server-side `ReusableWorkflowsLoader` architecture.

### 5. Custom runner labels

Some repositories use organization-specific labels, for example TypeScript's `1ES.Pool=...` and `1ES.ImageOverride=...`. These are valid in GitHub's environment but may not be known to `actionlint` or local runner configuration. They should not be treated as generic parser failures.

### 6. Boolean/numeric job/step `if` conditions — implemented

Real workflows use plain YAML booleans and numbers in `if` blocks:

```yaml
test-next-napi-bindings-wasi:
  if: false
```

The parser previously deserialized `if` as `Option<String>`, which failed on
literal YAML booleans (`false`, `true`) or numbers, raising:

```text
invalid type: boolean `false`, expected a string
```

Resolved: implemented custom deserializer coercion to accept any scalar value and deserialize it to `Option<String>` for both job-level and step-level `if` conditions. Action `with` values already deserialize to `serde_json::Value` (which natively permits booleans/numbers) and are coerced to string during job building.

### 7. Dependency on matrix-expanded reusable workflow call — implemented

When a job depends on a reusable workflow call that uses a matrix, other jobs
may reference the call by its unexpanded job id:

```yaml
build:
  uses: ./.github/workflows/build-terraform-cli.yml
  strategy:
    matrix:
      include:
        - {goos: "linux", goarch: "amd64"}
        - {goos: "darwin", goarch: "arm64"}

package-docker:
  needs: [build]
```

The expander creates suffixed job ids (`build (linux, amd64)`, `build (darwin, arm64)`)
but `package-docker` references the bare `build`. GitHub's runner interprets
`needs: [build]` as a dependency on all matrix instances of job `build`. The
parser now resolves bare needed job names to the set of all matrix-expanded instances (both reusable workflow calls and regular matrix jobs).

## Suggested next validation set

1. Add deferred handling for remaining scalar fields such as `timeout-minutes`.
2. Run Neovim and Kubernetes workflows from full sparse checkouts to exercise local reusable workflow expansion.
3. Run the full Deno generated workflow periodically; it currently provides the largest successful expansion observed here.
4. Expand the Apache validation set to include more workflows per repository (e.g. Superset's `superset-frontend.yml`, Spark's `build_branch42.yml`, Airflow's `ci-amd.yml`) to stress matrix expansion and reusable-workflow chaining further.
5. Keep this table updated with the workflow commit SHA when recording long-lived comparisons.
