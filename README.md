# preloop

![Watch the preloop demo: a failed step, a fix in another pane, and a re-run, all local, no push required](docs/demo/demo.gif)

> Full-resolution video: [debug.mp4](docs/demo/debug.mp4)

> A failed step, a fix in another pane, and a re-run, all local, with no push required.

---

preloop is a local, self-hosted equivalent of GitHub Actions. The engine (`preloop serve`) accepts workflows the same way GitHub does: `${{ }}` expressions, matrix builds, reusable workflows, concurrency groups, OIDC. It executes them on local machines (smolvm microVMs by default). Your `.github/workflows` run unmodified.

It speaks the official `actions/runner` protocol, so you can use the official runner to register, poll, execute, and report against it — without GitHub-hosted minutes. You can also use our Rust-equivalent runner with up to 10x smaller binary sizes and 10x lower memory RSS.

Runs on macOS, Linux, and Windows.

## Quick start

```sh
# Install (macOS/Linux): downloads the release binary and verifies its sha256
curl -fsSL https://raw.githubusercontent.com/preloopdev/preloop/main/install.sh | sh

preloop serve            # engine on 127.0.0.1:9090
cd my-repo
preloop run -f .github/workflows/ci.yml --event push
```

Setup, GitHub App and PAT credentials, secrets, config file, and the troubleshooting guide: [docs/setup.md](docs/setup.md)

Run it as a team server — service install, every runtime knob, and how to
expose it (tailnet only, Tailscale Funnel, Cloudflare Tunnel, or your own
domain): [docs/self-hosting.md](docs/self-hosting.md)

## What makes it different

- The real runner protocol, not a behavior approximation: the official runner binary works against it unchanged.
- Execution agnostic: smolvm microVMs, containers, or bare processes.
- GitHub App and fine-grained PAT support with per-job token minting.
- Step debugger with breakpoints, pause, and retry.
- OIDC issuer for local runs.
- Secrets with GitHub-compatible masking and global/repo tiers.
- NDJSON event output for agents and developer tooling.

## Documentation

| Topic | Doc |
|---|---|
| Setup, credentials, secrets, config | [docs/setup.md](docs/setup.md) |
| Hosting it yourself: service install, knobs, exposure | [docs/self-hosting.md](docs/self-hosting.md) |
| GitHub App webhooks and check runs | [docs/github-app-webhook.md](docs/github-app-webhook.md) |
| Job tokens, minting, OIDC | [docs/github-tokens.md](docs/github-tokens.md) |
| Debug sessions (pause, inspect, retry) | [docs/debug-sessions.md](docs/debug-sessions.md) |
| Architecture and crate map | [docs/architecture.md](docs/architecture.md) |
| Protocol conformance | [docs/conformance.md](docs/conformance.md) |
| Fidelity gaps and roadmap | [docs/fidelity-gap.md](docs/fidelity-gap.md) |
| Contributing and CI requirements | [CONTRIBUTING.md](CONTRIBUTING.md) |

## Requirements

macOS (Apple Silicon) or Linux, 64-bit, Rust 1.97+, and [smolvm] for the default VM runner pool. `preloop runner` works without smolvm. Postgres is optional.

## License

Two licenses, one project:

- **Everything except the control plane is MIT** — the CLI, the Rust runner,
  the parser/expression/protocol crates, the VM orchestrator, and the docs.
- **The control plane (`preloop serve` / `preloop-runner-server`) is
  FSL-1.1-MIT** — source-available. You may use, modify, and redistribute it
  for any non-competing purpose (internal CI, commercial products, forks),
  and it converts to MIT on the second anniversary of each release. What
  "non-competing" means: you can't offer it as a hosted CI *service* that
  competes with preloop's own offering.

Full terms: `crates/preloop-runner-server/LICENSE` (FSL-1.1-MIT) and MIT for
the rest. If that split doesn't work for you, the FSL clause is time-boxed —
the server becomes MIT two years after its release date.

## Credits

This project wouldn't be possible without:

- [smolvm] — the microVM runtime every job executes in
- [runner.server] — the protocol reverse-engineering this project builds on

[smolvm]: https://github.com/smol-machines/smolvm
[runner.server]: https://github.com/ChristopherHX/runner.server
