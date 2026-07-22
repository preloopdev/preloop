# Real-World Workflow Lint/Parser Report

**Last updated:** 2026-07-22  
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

**Status: All 14 findings resolved across 110 unique workflows in 7 batches.**

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

### Batch 3: Diverse project workflows (20 projects)

A third batch targeting large generated workflows, complex expressions, reusable
workflow chaining, and multi-platform build matrices.

| Repository | Workflow | `aksh` result | `actionlint` result | Notes |
|---|---:|---:|---:|---|
| [ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse/blob/master/.github/workflows/pull_request.yml) | `pull_request.yml` | **pass** — 60 job plans, 905 steps | shellcheck findings | 393 KB; largest single workflow tested. |
| [valkey-io/valkey](https://github.com/valkey-io/valkey/blob/unstable/.github/workflows/daily.yml) | `daily.yml` | **pass** — 15 job plans, 86 steps | shellcheck findings | 121 KB; large matrix with platform variants. |
| [neondatabase/neon](https://github.com/neondatabase/neon/blob/main/.github/workflows/build_and_test.yml) | `build_and_test.yml` | **pass** — 14 job plans, 88 steps | shellcheck findings | Supports dynamic bracket index expressions with nested property paths (finding #9). |
| [dotnet/runtime](https://github.com/dotnet/runtime/blob/main/.github/workflows/holistic-review-orchestrator.yml) | `holistic-review-orchestrator.yml` | **pass** — 1 job plan, 12 steps | shellcheck findings | Orchestrator workflow (35 KB). |
| [envoyproxy/envoy](https://github.com/envoyproxy/envoy/blob/main/.github/workflows/_run.yml) | `_run.yml` | **pass** — 1 job plan, 54 steps | shellcheck findings | Called workflow (`workflow_call`); `inputs` context in expression-valued job names handled correctly. |
| [facebook/react-native](https://github.com/facebook/react-native/blob/main/.github/workflows/test-all.yml) | `test-all.yml` | **pass** — 21 job plans, 68 steps | shellcheck findings | Requires `--workspace-root` for local reusable deps. |
| [microsoft/playwright](https://github.com/microsoft/playwright/blob/main/.github/workflows/tests_secondary.yml) | `tests_secondary.yml` | **pass** — 26 job plans, 135 steps | shellcheck findings | Multi-browser matrix (chromium/firefox/webkit). |
| [moby/buildkit](https://github.com/moby/buildkit/blob/master/.github/workflows/buildkit.yml) | `buildkit.yml` | **pass** — 1 job plan, 54 steps | exit 0 — clean | Propagates `workflow_call` context to remote reusable workflows, permitting `inputs` (finding #8). |
| [docker/compose](https://github.com/docker/compose/blob/main/.github/workflows/ci.yml) | `ci.yml` | **pass** — 20 job plans, 115 steps | shellcheck findings | Same remote `bake.yml` dependency (finding #8). |
| [neovim/neovim](https://github.com/neovim/neovim/blob/master/.github/workflows/test.yml) | `test.yml` | **pass** — 28 job plans, 333 steps | shellcheck findings | Already tested standalone; full repo with `--workspace-root` expands correctly. |
| [actions/runner](https://github.com/actions/runner/blob/main/.github/workflows/release.yml) | `release.yml` | **pass** — 7 job plans, 80 steps | exit 1 — shellcheck + old action versions | Official runner's own release workflow. |
| [tauri-apps/tauri](https://github.com/tauri-apps/tauri/blob/dev/.github/workflows/test-core.yml) | `test-core.yml` | **pass** — 21 job plans, 78 steps | exit 0 — clean | Multi-target Rust build (Android/Linux/macOS/Windows). |
| [django/django](https://github.com/django/django/blob/main/.github/workflows/schedule_tests.yml) | `schedule_tests.yml` | **pass** — 7 job plans, 49 steps | shellcheck findings | Multi-database / multi-Python scheduled CI. |
| [npm/cli](https://github.com/npm/cli/blob/latest/.github/workflows/node-integration.yml) | `node-integration.yml` | **pass** — 1 job plan, 14 steps | shellcheck findings | CITGM integration test runner. |
| [sveltejs/kit](https://github.com/sveltejs/kit/blob/version-3/.github/workflows/ci.yml) | `ci.yml` | **pass** — 7 job plans, 93 steps | shellcheck findings | Monorepo CI with pnpm workspace. |
| [FreeCAD/FreeCAD](https://github.com/FreeCAD/FreeCAD/blob/main/.github/workflows/build_release.yml) | `build_release.yml` | **pass** — 7 job plans, 62 steps | shellcheck findings | Multi-platform CMake CI (Windows/Linux/macOS). |
| [wasmerio/wasmer](https://github.com/wasmerio/wasmer/blob/main/.github/workflows/test.yaml) | `test.yaml` | **pass** — 9 job plans, 129 steps | shellcheck findings | Multi-runtime (Wasm/Cranelift/LLVM) test matrix. |
### Batch 4: Platform and language diversity (20 projects)

A fourth batch targeting OS-level projects, container runtimes, scientific Python,
JDK/Java CI, and large-matrix multi-language builds.

| Repository | Workflow | `aksh` result | `actionlint` result | Notes |
|---|---:|---:|---:|---|
| [curl/curl](https://github.com/curl/curl/blob/master/.github/workflows/linux.yml) | `linux.yml` | **pass** — 9 job plans, 155 steps | shellcheck findings | 50 KB; matrix with container images per build config. |
| [redis/redis](https://github.com/redis/redis/blob/unstable/.github/workflows/daily.yml) | `daily.yml` | **pass** — 23 job plans, 108 steps | shellcheck findings | 68 KB; large scheduled test matrix. |
| [containerd/containerd](https://github.com/containerd/containerd/blob/main/.github/workflows/ci.yml) | `ci.yml` | **pass** — 22 job plans, 225 steps | shellcheck findings | Container runtime CI with `--workspace-root`. |
| [hashicorp/vault](https://github.com/hashicorp/vault/blob/main/.github/workflows/test-go.yml) | `test-go.yml` | **pass** — 36 job plans, 150 steps | exit 0 — clean | 47 KB; extensive Go test matrix across products. |
| [hashicorp/consul](https://github.com/hashicorp/consul/blob/main/.github/workflows/test-integrations.yml) | `test-integrations.yml` | **pass** — 44 job plans, 376 steps | shellcheck findings | Integration test orchestration with envoy version matrix. |
| [electron/electron](https://github.com/electron/electron/blob/main/.github/workflows/pgo-generation.yml) | `pgo-generation.yml` | **pass** — 7 job plans, 49 steps | shellcheck findings | PGO build pipeline with `--workspace-root`. |
| [pnpm/pnpm](https://github.com/pnpm/pnpm/blob/main/.github/workflows/release.yml) | `release.yml` | **pass** — 4 job plans, 29 steps | shellcheck findings | 63 KB; monorepo release with publish matrix. |
| [cockroachdb/cockroach](https://github.com/cockroachdb/cockroach/blob/master/.github/workflows/ci.yml) | `ci.yml` | **pass** — 3 job plans, 27 steps | shellcheck findings | Release build with signing. |
| [timescale/timescaledb](https://github.com/timescale/timescaledb/blob/main/.github/workflows/linux-build-and-test.yaml) | `linux-build-and-test.yaml` | **pass** — 3 job plans, 39 steps | shellcheck findings | PostgreSQL extension build matrix. |
| [apache/thrift](https://github.com/apache/thrift/blob/master/.github/workflows/build.yml) | `build.yml` | **pass** — 19 job plans, 147 steps | shellcheck findings | 38 KB; cross-language (C++, Java, Python, Go, Rust, JS, etc.). |
| [kubernetes/minikube](https://github.com/kubernetes/minikube/blob/master/.github/workflows/functional_test.yml) | `functional_test.yml` | **pass** — 24 job plans, 114 steps | shellcheck findings | K8s functional testing with driver matrix. |
| [dapr/dapr](https://github.com/dapr/dapr/blob/master/.github/workflows/dapr.yml) | `dapr.yml` | **pass** — 10 job plans, 84 steps | shellcheck findings | Multi-service CI with e2e tests. |
| [numpy/numpy](https://github.com/numpy/numpy/blob/main/.github/workflows/linux.yml) | `linux.yml` | **pass** — 10 job plans, 100 steps | shellcheck findings | Multi-build CI with SIMD flags and Python versions. |
| [prometheus/prometheus](https://github.com/prometheus/prometheus/blob/main/.github/workflows/ci.yml) | `ci.yml` | **pass** — 25 job plans, 125 steps | shellcheck findings | Go CI with `--workspace-root` for local reusables. |
| [openjdk/jdk](https://github.com/openjdk/jdk/blob/master/.github/workflows/main.yml) | `main.yml` | **pass** — 17 job plans, 50 steps | shellcheck findings | JDK build pipeline with `--workspace-root`. |
| [apache/hadoop](https://github.com/apache/hadoop/blob/trunk/.github/workflows/tmpl_build_and_test.yml) | `tmpl_build_and_test.yml` | **pass** — 1 job plan, 35 steps | shellcheck findings | Called workflow template with 50+ Maven module args. |
| [spring-projects/spring-boot](https://github.com/spring-projects/spring-boot/blob/main/.github/workflows/release.yml) | `release.yml` | **pass** — 4 job plans, 36 steps | shellcheck findings | Maven release workflow; requires `--workspace-root`. |
| [tensorflow/tensorflow](https://github.com/tensorflow/tensorflow/blob/master/.github/workflows/update-rbe.yml) | `update-rbe.yml` | **pass** — 1 job plan, 8 steps | shellcheck findings | RBE config update workflow. |
| [grpc/grpc](https://github.com/grpc/grpc/blob/master/.github/workflows/pr-auto-fix.yaml) | `pr-auto-fix.yaml` | **pass** — 1 job plan, 9 steps | shellcheck findings | PR auto-fix workflow. |
| [jaegertracing/jaeger](https://github.com/jaegertracing/jaeger/blob/main/.github/workflows/ci-docker-build.yml) | `ci-docker-build.yml` | **pass** — 1 job plan, 10 steps | exit 0 — clean | Docker image build workflow. |

### Batch 5: Ten largest real-world workflows (10 projects)

A fifth batch targeting the largest real workflow files found across GitHub:
generated CI (`dprint`), game engine E2E (`openclaw`), storage operators
(`rook`), SQL engines (`trino`, `starrocks`), data pipelines (`hudi`, `airflow`),
config management (`salt`), deep learning (`megatron-lm`), and chat platforms
(`rocket.chat`).

| Repository | Workflow | `aksh` result | `actionlint` result | Notes |
|---|---:|---:|---:|---|
| [openclaw/openclaw](https://github.com/openclaw/openclaw/blob/main/.github/workflows/openclaw-live-and-e2e-checks-reusable.yml) | `openclaw-live-and-e2e-checks-reusable.yml` | **pass** — 68 job plans, 518 steps | shellcheck findings | 192 KB; C++/Lua game engine E2E. Null `continue-on-error` now defaults to `false`. |
| [rook/rook](https://github.com/rook/rook/blob/master/.github/workflows/canary-integration-test.yml) | `canary-integration-test.yml` | **pass** — 12 job plans, 81 steps | exit 0 — clean | 118 KB; K8s storage operator integration tests with `workflow_call` inputs. |
| [dprint/dprint](https://github.com/dprint/dprint/blob/main/.github/workflows/ci.generated.yml) | `ci.generated.yml` | **pass** — 96 job plans, 979 steps | shellcheck findings | 98 KB generated workflow; second-largest expansion after Deno. |
| [apache/hudi](https://github.com/apache/hudi/blob/master/.github/workflows/bot.yml) | `bot.yml` | **pass** — 55 job plans, 844 steps | shellcheck findings + expression type issues | 72 KB; multi-branch Spark/Flink build matrix. |
| [trinodb/trino](https://github.com/trinodb/trino/blob/master/.github/workflows/ci.yml) | `ci.yml` | **pass** — 26 job plans, 259 steps | shellcheck findings | 70 KB; distributed SQL query engine CI with JDK matrix. |
| [apache/airflow](https://github.com/apache/airflow/blob/main/.github/workflows/ci-amd.yml) | `ci-amd.yml` | **exit 1** — local reusable `basic-tests.yml` | shellcheck findings | 64 KB; calls `./.github/workflows/basic-tests.yml` (already tested standalone). Requires `--workspace-root`. |
| [saltstack/salt](https://github.com/saltstack/salt/blob/master/.github/workflows/test-action.yml) | `test-action.yml` | **pass** — 16 job plans, 97 steps | shellcheck findings | 63 KB; multi-OS test matrix with transport variants. |
| [NVIDIA/megatron-lm](https://github.com/NVIDIA/megatron-lm/blob/main/.github/workflows/cicd-main.yml) | `cicd-main.yml` | **pass** — 5 job plans, 76 steps | shellcheck findings | 54 KB; large-scale DL training CI with `merge_group` trigger. |
| [StarRocks/starrocks](https://github.com/StarRocks/starrocks/blob/main/.github/workflows/ci-pipeline.yml) | `ci-pipeline.yml` | **pass** — 50 job plans, 133 steps | shellcheck findings | 57 KB; MPP database CI with multi-stage pipeline. |
| [RocketChat/Rocket.Chat](https://github.com/RocketChat/Rocket.Chat/blob/develop/.github/workflows/ci.yml) | `ci.yml` | **exit 1** — local reusable `ci-code-check.yml` | exit 0 — clean | 52 KB; calls `./.github/workflows/ci-code-check.yml`. Requires `--workspace-root`. |

### Batch 6: Language runtimes, databases, and JS ecosystem (20 projects)

A sixth batch targeting language runtimes (PHP, Python, Lean, Idris2, SWC, Babel),
databases (PostgreSQL, DuckDB, Prisma), ML (HuggingFace Transformers),
monorepo/build tools (Turborepo, Nx, Rollup), security (OpenSSL, FlatBuffers),
and SDKs (sentry-js, sentry-cocoa, sentry-rn).

| Repository | Workflow | `aksh` result | `actionlint` result | Notes |
|---|---:|---:|---:|---|
| [PostHog/posthog](https://github.com/PostHog/posthog/blob/master/.github/workflows/ci-backend.yml) | `ci-backend.yml` | **pass** — 47 job plans, 419 steps | exit 1 — unknown runner label `buildjet-2vcpu-ubuntu-2204-arm` | 206 KB; largest workflow in this batch. Django/ClickHouse analytics. |
| [duckdb/duckdb](https://github.com/duckdb/duckdb/blob/main/.github/workflows/Main.yml) | `Main.yml` | **pass** — 5 job plans, 41 steps (with `--workspace-root`) | exit 0 — clean | 58 KB; in-process OLAP. Remote reusables in locally-chained workflows now resolved. |
| [getsentry/sentry-javascript](https://github.com/getsentry/sentry-javascript/blob/develop/.github/workflows/build.yml) | `build.yml` | **pass** — 15 job plans, 114 steps (with `--workspace-root`) | shellcheck findings | 47 KB; JS SDK monorepo. |
| [postgres/postgres](https://github.com/postgres/postgres/blob/master/.github/workflows/pg-ci.yml) | `pg-ci.yml` | **pass** — 14 job plans, 80 steps | shellcheck findings | 45 KB; \~30 target platforms including rare architectures. |
| [prisma/prisma](https://github.com/prisma/prisma/blob/main/.github/workflows/test-template.yml) | `test-template.yml` | **pass** — 14 job plans, 96 steps | exit 0 — clean | 43 KB; ORM multi-DB engine test matrix. |
| [php/php-src](https://github.com/php/php-src/blob/master/.github/workflows/test-suite.yml) | `test-suite.yml` | **pass** — 15 job plans, 112 steps | shellcheck findings | 38 KB; PHP interpreter CI. |
| [leanprover/lean4](https://github.com/leanprover/lean4/blob/master/.github/workflows/pr-release.yml) | `pr-release.yml` | **pass** — 5 job plans, 52 steps | shellcheck findings | 38 KB; theorem prover/functional language. |
| [vercel/turborepo](https://github.com/vercel/turborepo/blob/main/.github/workflows/turborepo-release.yml) | `turborepo-release.yml` | **pass** — 4 job plans, 39 steps (with `--workspace-root`) | shellcheck findings | 38 KB; monorepo orchestration. Workspace-root resolves local reusables. |
| [swc-project/swc](https://github.com/swc-project/swc/blob/main/.github/workflows/publish-npm-package.yml) | `publish-npm-package.yml` | **pass** — 2 job plans, 26 steps | shellcheck findings | 32 KB; Rust-based JS/TS compiler. |
| [nrwl/nx](https://github.com/nrwl/nx/blob/master/.github/workflows/publish.yml) | `publish.yml` | **pass** — 1 job plan, 46 steps | shellcheck findings | 31 KB; monorepo build system release. |
| [openssl/openssl](https://github.com/openssl/openssl/blob/master/.github/workflows/ci.yml) | `ci.yml` | **pass** — 5 job plans, 53 steps | shellcheck findings | 28 KB; crypto library CI with multi-platform. |
| [idris-lang/Idris2](https://github.com/idris-lang/Idris2/blob/main/.github/workflows/ci-idris2-and-libs.yml) | `ci-idris2-and-libs.yml` | **pass** — 49 job plans, 186 steps | shellcheck findings | 28 KB; dependently-typed functional language. |
| [getsentry/sentry-cocoa](https://github.com/getsentry/sentry-cocoa/blob/main/.github/workflows/build.yml) | `build.yml` | **pass** — 5 job plans, 39 steps (with `--workspace-root`) | exit 0 — clean | 28 KB; Apple SDK. |
| [huggingface/transformers](https://github.com/huggingface/transformers/blob/main/.github/workflows/self-scheduled.yml) | `self-scheduled.yml` | **pass** — 9 job plans, 89 steps (with `--workspace-root`) | shellcheck findings | 26 KB; ML model library. `owner/repo/...@sha` references now resolved from workspace-root. |
| [python/cpython](https://github.com/python/cpython/blob/main/.github/workflows/build.yml) | `build.yml` | **pass** — 5 job plans, 53 steps (with `--workspace-root`) | shellcheck findings | 25 KB; CPython interpreter. |
| [getsentry/sentry-react-native](https://github.com/getsentry/sentry-react-native/blob/main/.github/workflows/e2e-v2.yml) | `e2e-v2.yml` | **pass** — 10 job plans, 34 steps (with `--workspace-root`) | shellcheck findings | 25 KB; React Native SDK. |
| [caddyserver/caddy](https://github.com/caddyserver/caddy/blob/master/.github/workflows/release.yml) | `release.yml` | **pass** — 9 job plans, 48 steps | shellcheck findings | 23 KB; Go web server goreleaser release. |
| [babel/babel](https://github.com/babel/babel/blob/main/.github/workflows/ci.yml) | `ci.yml` | **pass** — 16 job plans, 95 steps | exit 0 — clean | 22 KB; JS transpiler monorepo. |
| [rollup/rollup](https://github.com/rollup/rollup/blob/master/.github/workflows/build-and-tests.yml) | `build-and-tests.yml` | **pass** — 11 job plans, 63 steps | shellcheck findings | 22 KB; JS bundler CI. |
| [google/flatbuffers](https://github.com/google/flatbuffers/blob/master/.github/workflows/build.yml) | `build.yml` | **pass** — 5 job plans, 18 steps | shellcheck findings | 21 KB; cross-platform serialization. |

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

### 8. `inputs` context not propagated to fetched remote reusable workflows — implemented

When a workflow references a remote reusable workflow (e.g. `docker/github-builder/.github/workflows/bake.yml@ref`),
the fetched remote workflow uses `inputs` in job outputs:

```yaml
# In bake.yml (has on: workflow_call with inputs)
finalize:
  outputs:
    artifact-name: ${{ inputs.artifact-upload && inputs.artifact-name || '' }}
```

The resolver fetches the remote workflow and inserts it into the reusable-workflow
table, but the expression validator does not recognize the `workflow_call` event
context for the fetched workflow. The `inputs` context is therefore rejected:

```text
context "inputs" is not allowed here. available contexts are "github", "needs",
"strategy", "matrix", "secrets", "steps", "job", "runner", "env", "vars"
```

Observed in `moby/buildkit` and `docker/compose`, both of which depend on
`docker/github-builder/bake.yml`.

Resolved: The expression validator dynamically checks if the workflow trigger supports the `inputs` context (meaning it declares a `workflow_call` or `workflow_dispatch` trigger) and expands `CTX_RUNNER` to include `"inputs"` if so. This permits called reusable workflows to access `inputs` in job outputs.

### 9. Expression parser chokes on dot token inside bracket index with multi-line `format()` — implemented

Complex expressions using multi-line `format()` inside `fromJSON()` with bracket
indexing cause the expression tokenizer to fail:

```yaml
env:
  SLACK_CHANNEL: ${{ fromJSON(format('{
    "storage-release": "{0}",
    "compute-release": "{1}"
  }',
    vars.SLACK_STORAGE_CHANNEL_ID,
    vars.SLACK_COMPUTE_CHANNEL_ID
  ))[needs.meta.outputs.run-kind] }}
```

Resolved: Rewrote the expression parser's member/bracket indexing logic to parse any arbitrary expression inside brackets (`LBracket ... RBracket`), and added `Expr::IndexAccess` to the AST to represent dynamic indexing.

Observed in `neondatabase/neon/build_and_test.yml`.

### 10. `continue-on-error` with expression that returns null — resolved

A reusable workflow defines `continue-on-error` with an expression referencing
`inputs`:

```yaml
validate_release_live_cache:
  continue-on-error: ${{ inputs.advisory || inputs.live_advisory }}
```

When expanded standalone (no caller provides these `inputs`), the expression
evaluates to `null`. The expander rejects this:

```text
Error: expand workflow ... invalid continue-on-error for job
`validate_release_live_cache`: expression returned null, expected a boolean
```

GitHub's runner permits this — a null `continue-on-error` defaults to `false`.
The fix is to coerce null/undefined expression results to `false` for
`continue-on-error`, matching the runner's semantics.

Resolved: Changed `resolved_continue_on_error()` in `aksh-gha-parser/src/expand.rs`
to use `result.as_bool().unwrap_or(false)` instead of returning an error on null.

Observed in `openclaw/openclaw/openclaw-live-and-e2e-checks-reusable.yml`.

### 11. `--workspace-root` does not resolve cross-repo remote reusables — resolved

When a workflow references a reusable from a different repository with the same
org prefix:

```yaml
uses: duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@main
```

The resolver attempted a remote fetch via the GitHub Contents API but the
resolver had two bugs:
1. It bailed on non-reusable references (regular composite actions) instead of
   skipping them.
2. It did not re-traverse locally-resolved workflows for their nested remote
   references (the BFS queue only started from `root_yaml`).

Resolved: In `aksh-runner/src/main.rs` and `aksh-runner-client/src/main.rs`
`resolve_remote_workflows()`:
- Changed `parse_remote_reference` failure from `bail!` to `continue` so regular
  actions are skipped.
- Seed the BFS queue with all locally-resolved workflow contents so their
  remote references are discovered and fetched (matching the server's behavior
  in `aksh-runner-server/src/remote_workflows.rs`).

Observed in `duckdb/duckdb/Main.yml`.

### 12. Resolver does not match local files for `owner/repo/path@sha` references within the same repo — resolved

When a workflow references a reusable from the same repository using the
full `owner/repo` prefix with a pinned SHA:

```yaml
uses: huggingface/transformers/.github/workflows/collated-reports.yml@6abd9725ee7d809dc974991f8ff6c958afb63a3a
```

The expander's lookup only matched the raw `uses` value or the normalized path
(stripped `./` and `@ref`), but did not strip the `owner/repo/` prefix. So
files loaded from `--workspace-root` (keyed as `.github/workflows/collated-reports.yml`)
were never matched.

Resolved: Added a fallback in `aksh-gha-parser/src/expand.rs`
`expand_jobs_with_reusables_internal()` that extracts the `.github/workflows/...`
portion from the uses value and tries it as a lookup key. This matches
workspace-root-loaded workflows without requiring the resolver to know the
workspace's `owner/repo` identity.

Observed in `huggingface/transformers/self-scheduled.yml`.

### 13. Reusable workflow `with` expressions not evaluated in caller's matrix context — resolved

When a reusable workflow receives an expression as a `with` input from a caller
that uses a matrix:

```yaml
# caller job
strategy:
  matrix:
    timeout: [20, 30, 40]
uses: ./.github/workflows/lima.yml
with:
  timeout: ${{ matrix.timeout || 30 }}

# called workflow (lima.yml)
timeout-minutes: ${{ inputs.timeout || 20 }}
```

The expression `${{ matrix.timeout || 30 }}` was passed through as a raw string
via `coerce_value()` (which explicitly preserves `${{ }}` expressions without
evaluating them). When the called workflow evaluated `inputs.timeout || 20`,
`inputs.timeout` was the raw string `${{ matrix.timeout || 30 }}` — truthy, so
`||` returned the raw string, and `as_u64()` failed.

Resolved: In `expand_jobs_with_reusables_internal()`, after matrix expansion,
each `with` value that is an expression string is evaluated in the caller's
context (including the matrix values) before being passed as an input to the
called workflow. The evaluated values are also used for `called_plan.inputs`
and `ReusableCallMetadata.inputs`.

Observed in `containers/podman/.github/workflows/ci.yml`.

### 14. `resolve_deferred_number` rejects numeric strings — resolved

When an expression evaluates to a string containing a number (e.g., `"30"`)
instead of a JSON number (`30`), `resolve_deferred_number` rejected it with
`"expression returned "30", expected a number"`, because `serde_json::Value::as_u64()`
only accepts `Value::Number`, not `Value::String`.

Resolved: Added a `Value::String` arm to the match in `resolve_deferred_number()`
that attempts `s.parse::<u64>()` before falling through to the error case.

## Batch results

### Batch 7 — Container-runtime workflows (2026-07-22)

| # | Project | Workflow | Size | Jobs | Status |
|---|---------|----------|------|------|--------|
| 1 | containers/podman | ci.yml | 25 KB | 56 | pass (needs workspace-root) |
| 2 | earthly/earthly | ci-docker-ubuntu.yml | 40 KB | 62 | pass (needs workspace-root) |
| 3 | k0sproject/k0s | release.yml | 26 KB | 11 | pass (needs workspace-root) |
| 4 | dagger/dagger | publish.yml | 24 KB | 9 | pass |
| 5 | moby/moby | .windows.yml | 23 KB | 8 | pass |
| 6 | argoproj/argo-workflows | ci-build.yaml | 23 KB | 37 | pass |
| 7 | kata-containers/kata-containers | build-kata-static-tarball-amd64.yaml | 21 KB | 45 | pass |
| 8 | chainguard-dev/apko | build-samples.yml | 15 KB | 9 | pass |
| 9 | buildpacks/pack | build.yml | 14 KB | 10 | pass |
| 10 | linuxkit/linuxkit | ci.yml | 13 KB | 13 | pass |

**7 pass standalone, 3 need `--workspace-root` (podman, earthly, k0s).**
**Total unique workflows tested across all batches: 110.**

## Suggested next validation set

1. Run the full ClickHouse `pull_request.yml` periodically; at 393 KB it is the largest single workflow in the corpus.
2. Run the full Deno generated workflow periodically; it remains the largest successful expansion at 405 KB.
3. Expand validation into CI-specific patterns: `merge_group` triggers, `workflow_run` chaining, and `issue_comment` event workflows.
4. Multi-arch/container `runs-on:` label parsing (self-hosted labels, `[group:]labels` syntax).
5. Keep this table updated with the workflow commit SHA when recording long-lived comparisons.
