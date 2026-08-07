# aksh — GitHub Actions Control Plane

**aksh** is a faithful Rust reimplementation of the GitHub Actions control plane. It speaks
the official runner protocol, so the **unmodified `actions/runner`** can register, poll for
jobs, execute, and report — without GitHub-hosted minutes.

**[Preloop](https://github.com/preloop/preloop)** is the local CI product built on aksh:
microVM-isolated runner pools, one-command install, and CI that keeps working through GitHub
outages. aksh is Preloop's control plane, and independently usable — point any runner at it.

## Features

- **Official-runner wire fidelity** — tracked by `runner-watch` against `actions/runner`
  v2.336.0; protocol captures and a conformance gate live in `.runner-watch/`.
- **Two ways to trigger CI**:
  - **Webhooks** — GitHub App or per-repo webhook deliver push/PR events (`docs/webhooks.md`).
  - **Submit-driven** — `preloop run --push` runs CI on the server without any GitHub event,
    then pushes the *tested* commit and opens a draft PR when GitHub is reachable again
    (`docs/submit-driven-ci.md`). CI survives GitHub outages.
- **Check runs** — reported to GitHub through the Checks API, annotations included.
- **Execution-agnostic** — jobs run in microVMs (Preloop), containers, or bare processes;
  anything that speaks the runner protocol works.
- **Real workflow semantics** — `needs` DAGs, matrix expansion, trigger matching, expression
  evaluation (`${{ }}`), secrets with redaction-safe types, reusable workflows, OIDC.
- **Local cache + artifact stores** — no GitHub-hosted infrastructure required.
- **NDJSON event stream** — machine-readable run events for agents and tooling.
- **DAP debugging** — attach a debugger to a failed job (`preloop debug`).

## Quickstart (2 minutes)

```sh
# 1. Install (Linux x86_64/aarch64; see install.sh for other platforms)
curl -fsSL https://raw.githubusercontent.com/preloopdev/preloop/main/install.sh | sh

# 2. Configure GitHub access once
preloop setup github

# 3. Start the control plane + runner pool
preloop serve

# 4. Run a workflow
preloop run -f .github/workflows/ci.yml

# …or run CI first, publish later — no webhook needed:
preloop run --push --create-pr
```

`preloop doctor` checks your setup at any time. Full setup guidance:
[`docs/setup.md`](docs/setup.md) (GitHub App), [`docs/webhooks.md`](docs/webhooks.md)
(webhook-only setups), [`docs/submit-driven-ci.md`](docs/submit-driven-ci.md) (push-back).

## Install

| Path | How |
|---|---|
| **Release binary** (recommended) | `curl -fsSL …/install.sh \| sh` — downloads from GitHub Releases, verifies the sha256 |
| **From source** | `cargo build --release -p preloop-cli -p aksh-runner-server -p aksh-runner` (Rust 1.97+, see `rust-toolchain.toml`) |
| **Self-update** | `preloop update` — polls Releases and installs atomically |

`install.sh` installs three binaries: `preloop` (CLI + engine), `preloop-server` (control
plane), `preloop-runner` (runner). Flags: `--version <tag>`, `--dir <path>`, `--skip-doctor`,
`--dry-run`.

## Usage

| Command | What it does |
|---|---|
| `preloop run -f <workflow>` | Submit + stream a run (flags: `--job`, `--event`, `--secret`, `--detach`) |
| `preloop run --push [--create-pr]` | Run CI, then push the tested commit and open/update a draft PR |
| `preloop push <run_id>` | Replay the push-back for a finished run (idempotent) |
| `preloop status` / `preloop logs <run_id>` | Runs and logs, incl. per-job steps |
| `preloop plan -f <workflow>` | Expand the job DAG without executing |
| `preloop setup github` / `preloop doctor` | Configure and verify GitHub credentials (App or PAT) |
| `preloop secret set <NAME>` | Store a workflow secret |
| `preloop serve` | Control plane + microVM runner pool (self-hosted entry point) |
| `preloop shell` / `preloop debug` | Open a failed job's VM / attach a debugger |
| `preloop update` | Self-update from Releases |

## Crates

- `aksh-runner-server` — control plane: runner protocol APIs, run queue, cancellation,
  reruns, NDJSON events, webhook + checks integration. **FSL-1.1-MIT licensed.**
- `aksh-runner` / `preloop-runner` — the runner (Listener + Worker), faithful to
  `actions/runner` v2.336.0.
- `preloop-cli` — the `preloop` command: engine, pool, and client.
- `aksh-gha-parser` / `aksh-gha-expressions` — typed workflow parsing, trigger matching,
  DAGs, matrix expansion, expression evaluation.
- `aksh-gha-protocol` — wire DTOs, session crypto, secret wrappers, NDJSON events.
- `aksh-cache` / `aksh-artifacts` — local cache/artifact stores compatible with the runner
  protocols.
- `aksh-conformance` / `runner-watch` — conformance harnesses and the protocol-diff tool.

## Compatibility & contributing

Compatibility is a test artifact, not prose: `runner-watch` records the official runner's
wire behavior and replays it against aksh; the full gate is `just test-ci`
(fmt + clippy + tests + conformance). Protocol gaps live in
[`docs/fidelity-gap.md`](docs/fidelity-gap.md).

- [CONTRIBUTING.md](CONTRIBUTING.md) — dev workflow + compatibility checklist
- [docs/architecture.md](docs/architecture.md) — crate and module map
- [docs/conformance.md](docs/conformance.md) — how compatibility is measured
- [SECURITY.md](SECURITY.md) — reporting vulnerabilities

Found a divergence from the official runner? The issue template asks for a
`runner-watch` capture of the official behavior — see
[.github/ISSUE_TEMPLATE/bug_report.yml](.github/ISSUE_TEMPLATE/bug_report.yml).

## License

MIT for all crates except **`aksh-runner-server`** (the control plane), which is licensed
under **FSL-1.1-MIT** (`crates/aksh-runner-server/LICENSE`): source-available — you may use,
modify, and redistribute it for any non-competing purpose — and it converts to MIT on the
second anniversary of each release.
