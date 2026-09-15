# Five-Repository Real-World Conformance Campaign

**Date:** 2026-09-13  
**Base:** official `ubuntu24-arm64-runner-large` packed golden  
**Runtime:** matched smolvm 1.15.0 + bundled libkrun, serialized one-VM campaign  
**Scope:** five repositories not used in the preceding campaigns; real workflow YAML and push payloads

## Repository selection

| Repository | Primary ecosystem | Workflow | Rationale |
|---|---|---|---|
| prometheus/prometheus | Go | `.github/workflows/ci.yml` | Large observability project with Go-version, upgrade, UI, fuzz, CodeQL, and compliance jobs |
| helm/helm | Go | `.github/workflows/build-test.yml` | Medium-large Go package manager with build/test/lint coverage |
| pypa/pip | Python | `.github/workflows/ci.yml` | Core Python packaging project with packaging and multi-version test matrices |
| rails/rails | Ruby | `.github/workflows/devcontainer-smoke-test.yml` | Large Ruby framework; container/devcontainer smoke path |
| webpack/webpack | JavaScript | `.github/workflows/test.yml` | Large JavaScript toolchain with basic, lint, unit, and Node-version integration matrices |

## Results

| Repository | Job-level result | Findings |
|---|---:|---|
| prometheus/prometheus | 3 success / 12 failure / 20 skipped | Successful jobs ran; 20 skips and several failures are matrix/platform or workflow-environment gaps. Checkout failures were observed in multiple jobs and require a retained per-job log for exact attribution. |
| helm/helm | **1 success** | Full selected workflow passed. |
| pypa/pip | 16 success / 22 failure | Ubuntu jobs ran; macOS/Windows jobs were unavailable; aggregate `check` failed because required platform jobs failed. |
| rails/rails | **1 success** | Full selected smoke workflow passed. |
| webpack/webpack | 1 success / 15 failure / 24 skipped | Basic/lint/unit executed but failed; macOS/Windows integration jobs were unavailable; many integration cells skipped via dependency/fail-fast behavior. |

The run snapshots are in the per-repository directories beside this report.

## Interpretation

- `helm` and `rails` passed their selected workflows end to end.
- Prometheus, pip, and webpack produced useful Linux execution, but their run-level failures are not equivalent to total runner failure: platform cells are intentionally unavailable on this ARM Linux-only host, and the workflow aggregators correctly turn those required failures into a failed run.
- The campaign used one active VM to stay within local disk limits. smolvm 1.15.0 forked successfully and avoided the earlier code-133 fork-base failure. Provisioning was materially slower than a hosted fleet because each job still gets an ephemeral fork and may install a missing toolchain.
- This campaign is evidence of workflow compatibility, not a claim that the ARM64 golden is package-identical to GitHub's x86_64 `ubuntu-latest`. Platform-specific jobs, i386, external fleets, credentials, and host package inventories remain explicit dimensions.

## Harness

`benchmarks/real-world/conformance-new5repos.sh` pins the selected repositories, uses the server's `golden-path` command instead of duplicating fingerprint logic, uses matched smolvm 1.15.0 libraries, and caps local VM storage at 80 GiB by default.
