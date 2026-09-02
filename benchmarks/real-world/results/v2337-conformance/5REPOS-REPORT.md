# 5-Repo Real-World Conformance Campaign v2 (Official v2.337 & Preloop Runner)

**Date:** 2026-09-01  
**Substrate:** Isolated `smolvm` microVMs on Apple Silicon (host only orchestrates, no workflows executed on host).  
**Baseline:** Prior recorded GitHub.com runs (Linux-targeted jobs from `5repos-preloop-REPORT.md`).  
**Targets Tested:**
1. `official v2.337` → `preloop-server` (`pl-official`)
2. `preloop-runner` → `preloop-server` (`pl-preloop`)

---

## 1. Summary Comparison Table

| Repository | Workflow | Target Jobs | GitHub Baseline | `pl-official` (v2.337) | `pl-preloop` (v2.335.1 compat) | Classification / Notes |
|---|---|---|---|---|---|---|
| **cli/cli** | `.github/workflows/lint.yml` | `govulncheck`, `lint` | ✅ **PASS** (2/2) | ✅ **PASS** (2/2) | ✅ **PASS** (2/2) | **Exact Match**: Full Go toolchain & action resolution match. |
| **serde-rs/serde** | `.github/workflows/ci.yml` | `Test suite`, `Clippy` | ✅ **PASS** (2/2) | ✅ **PASS** (2/2) | ✅ **PASS** (2/2) | **Exact Match**: Rust test suites & clippy checks pass identically. |
| **tokio-rs/tokio** | `.github/workflows/ci.yml` | `basics`, `clippy`, `fmt`, `minrust` | ✅ **PASS** (4/4) | ✅ **PASS** (4/4) | ✅ **PASS** (4/4) | **Exact Match**: Multi-job cargo pipelines pass clean across both runners. |
| **valkey-io/valkey** | `.github/workflows/ci.yml` | `test-ubuntu-latest` | ⚠️ **Failure** (hung test suite) | ⚠️ **Failure** | ⚠️ **Failure** | **Environment Limitation**: Comprehensive test suite requires system privileges/tcl hooks not present in minimal container/guest setup. |
| **pydantic/pydantic** | `.github/workflows/ci.yml` | `lint` (6 Python matrix legs) | ⚠️ **Failure** (PEP 668 / toolchain) | ⚠️ **Failure** | ⚠️ **Failure** | **Environment Limitation**: Pre-commit CGo compilation & PEP 668 restrictions replicate identically across official and preloop runners. |

---

## 2. Classification of Divergences

1. **Preloop Server Gaps Identified & Addressed**:
   - **Job Env Wire Shape (P1)**: Serialized `environmentVariables` as TemplateToken mapping objects rather than raw JSON to prevent official runner template evaluator panics (`The template is not valid`).
   - **Orchestration ID Token Safety**: Sanitized job ID tokens to prevent header format exceptions when evaluating nested reusable workflow jobs (e.g., `ci/build`).
   - **Client ID GUID Format**: Formatted agent authorization client IDs as valid GUIDs for official runner compatibility.
   - **Artifact Scoping**: Resolved artifact scoping by run ID rather than per-job plan IDs.

2. **Preloop Runner Gaps**:
   - **Working Directory Pre-creation**: Fixed step runner to ensure target working directories exist before spawning script processes.
   - **OIDC Token Wiring**: Identified missing token export for GitHub-hosted execution.

3. **Environment Limitations (Not Runner Bugs)**:
   - CGo / Python virtualenv tooling (`time-machine`, `pre-commit`) requires specific compiler and header configurations.
   - Valkey full integration test suite requires extended system privileges.
