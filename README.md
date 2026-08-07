# preloop

<video src="docs/demo/debug.mp4" autoplay loop muted playsinline width="100%"></video>

> A failed step, a fix in another pane, and a re-run — all local, no push required.

---

preloop is a local, self-hosted equivalent of GitHub Actions. The engine (`preloop serve`) accepts workflows the same way GitHub does: `${{ }}` expressions, matrix builds, reusable workflows, concurrency groups, OIDC. It executes them on local machines (smolvm microVMs by default). Your `.github/workflows` run unmodified.

It speaks the official `actions/runner` protocol, so you can use the official runner to register, poll, execute, and report against it — without GitHub-hosted minutes. You can also use our Rust-equivalent runner with up to 10x smaller binary sizes and 10x lower memory RSS.

Runs on macOS, Linux, and Windows.

## Quick start

```sh
preloop serve            # engine on 127.0.0.1:9090
cd my-repo
preloop run -f .github/workflows/ci.yml --event push
```

Setup, GitHub App and PAT credentials, secrets, config file, and the troubleshooting guide: [docs/setup.md](docs/setup.md)

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

MIT for all crates except the control plane (`aksh-runner-server`), which is
licensed under FSL-1.1-MIT (`crates/aksh-runner-server/LICENSE`):
source-available (you may use, modify, and redistribute it for any
non-competing purpose), converting to MIT on the second anniversary of each release.

## Credits

This project wouldn't be possible without:

[smolvm]: https://github.com/preloopdev/smolvm
[runner.server]: https://github.com/ChristopherHX/runner.server
