# Preloop Logical Architecture Documents

This document set breaks the updated Preloop plan into logical areas that can be read independently and used as implementation briefs.

## Reading order

1. [Product Strategy and Operating Modes](01_product_strategy_and_modes.md)
2. [Aksh Runner and Control Plane](02_aksh_runner_control_plane.md)
3. [MicroVM Isolation and the smolvm Runtime](03_microvm_isolation_and_smolvm_runtime.md)
4. [Guest Agent, Workspace, and Filesystem](04_guest_agent_workspace_filesystem.md)
5. [Docker, Services, and Container Actions](05_docker_services_container_actions.md)
6. [Security Policy and Trust Tiers](06_security_policy_trust_tiers.md)
7. [Cache, Artifacts, and Toolchains](07_cache_artifacts_toolchains.md)
8. [Conformance and Fidelity](08_conformance_and_fidelity.md)
9. [Agent Loop, Retry, and Forking](09_agent_loop_retry_and_fork.md)
10. [GitHub App, Self-Hosted, and Managed CI](10_github_app_self_hosted_managed_ci.md)
11. [Roadmap, Milestones, and Risk Register](11_roadmap_milestones_and_risks.md)
12. [Rust Engineering Standards](12_rust_engineering_standards.md)
13. [Rosetta x86_64 and CI Performance](13_rosetta_x86_64_and_ci_performance.md)
14. [Runtime Tiers and Portable Handoff](14_runtime_tiers_and_portable_handoff.md)
15. [CI Efficiency Levers](15_ci_efficiency_levers.md)

## Source context used

These documents synthesize:

- The uploaded `Preloop-Master-Plan (1).pdf`, especially its wedge around hardware isolation, private Docker-in-VM, deny-by-default egress, non-leaking secrets, clean-room fidelity, and fork-from-failure.
- The uploaded `aksh-runner-rust.zip` branch, including:
  - `docs/architecture.md`
  - `docs/local-ci-vs-self-hosted.md`
  - `docs/runner/00-architecture.md`
  - `docs/runner/microvm-isolation-research.md`
  - `docs/conformance.md`
  - `docs/github-app-webhook.md`
  - `.runner-watch/golden/v2.335.1/*`
  - the Rust workspace `Cargo.toml` and crate layout.
- The updated direction: Aksh is the Rust-native runner/control-plane core; official runner and `ChristopherHX/runner.server` remain conformance oracles and optional fallback modes.
- [smolvm](https://github.com/preloopdev/smolvm): the libkrun-backed microVM substrate Preloop consumes (not reimplements) for the Local and smolvm-KVM tiers, plus Firecracker for the scale tier.

## One-sentence architecture

Preloop is a macOS-first, Linux-capable local, self-hosted, and managed CI platform that uses Aksh as a Rust-native GitHub Actions-compatible runner/control plane over interchangeable microVM executors — **smolvm** (libkrun) for the local and smolvm-KVM tiers and **Firecracker** for the production scale tier — as the execution boundary for fast, isolated, agent-native jobs.

## Top-level components

```text
preloop CLI / MCP / API
        |
        v
preloopd
  |
  +-- aksh-control              # workflow/control-plane brain
  +-- aksh-runner               # Rust Listener/Worker execution path
  +-- official-runner adapter   # oracle/fallback
  +-- runner.server adapter     # oracle/fallback/local comparison
  +-- preloop-orchestrator      # job graph, VM lifecycle, retry/fork
  +-- preloop-policy            # trust tiers, network, secrets, actions, fs
  +-- preloop-cache             # actions, OCI, toolchain, package, artifacts
  +-- preloop-github            # GitHub App, webhooks, Checks API
  +-- preloop-conformance       # differential behavior gates
  +-- preloop-vm                # VmProvider seam over executors:
  |     +-- SmolvmProvider      #   smolvm CLI/HTTP/SDK (Local + smolvm-KVM tiers)
  |     +-- FirecrackerProvider #   Firecracker API + jailer (scale tier)
  +-- preloop-guest             # static guest agent (runtime-agnostic)
```

## Product modes

| Mode | User | Trust model | Runtime default | Main success metric |
|---|---|---|---|---|
| Local developer CI | human on macOS/Linux/Windows | mostly trusted, but unsafe code can exist | smolvm VM, ergonomic defaults | edit-to-verdict latency |
| Local agent loop | Codex/Claude/Cursor | agent-generated code is untrusted | smolvm, stricter network/secrets/cache defaults | safe red/green loop |
| Self-hosted CI | team-owned worker | untrusted PRs and internal branches | smolvm-KVM (jailed) or Firecracker worker | clean ephemeral job execution |
| Managed CI | Preloop SaaS | hostile multi-tenant code | Firecracker (jailer) primary; smolvm-KVM option | security, reliability, cost/perf |

## Non-negotiables

- One isolated Linux microVM per CI job.
- No host Docker socket as a core dependency.
- No real secrets in untrusted jobs by default.
- No silent fidelity gaps: unsupported behavior must be explicit and machine-readable.
- Conformance is a release gate, not a prose claim.
- Local fast retry must be followed by clean strict verification before a final “fixed” verdict.
