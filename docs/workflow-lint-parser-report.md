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

### 4. Custom runner labels

Some repositories use organization-specific labels, for example TypeScript's `1ES.Pool=...` and `1ES.ImageOverride=...`. These are valid in GitHub's environment but may not be known to `actionlint` or local runner configuration. They should not be treated as generic parser failures.

## Suggested next validation set

1. Add deferred handling for remaining scalar fields such as `timeout-minutes`.
2. Run Neovim and Kubernetes workflows from full sparse checkouts to exercise local reusable workflow expansion.
3. Run the full Deno generated workflow periodically; it currently provides the largest successful expansion observed here.
5. Keep this table updated with the workflow commit SHA when recording long-lived comparisons.
