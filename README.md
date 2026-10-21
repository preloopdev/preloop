<p align="center">
  <a href="https://preloop.dev"><img src="docs/images/preloop-logo-wide.png" width="320" alt="preloop"></a>
</p>

<p align="center">
  <b>5× faster drop-in GitHub Actions, rebuilt for agents.</b>
</p>

<p align="center">
  <img alt="Status: beta" src="https://img.shields.io/badge/status-beta-orange">
  <a href="https://docs.preloop.dev"><img alt="Docs" src="https://img.shields.io/badge/docs-docs.preloop.dev-0B5FFF"></a>
  <a href="https://github.com/preloopdev/preloop/releases/latest"><img alt="Latest release" src="https://img.shields.io/badge/dynamic/regex?url=https%3A%2F%2Fgithub.com%2Fpreloopdev%2Fpreloop%2Freleases.atom&amp;search=releases%2Ftag%2Fv%28%5B0-9%5D%2B%5C.%5B0-9%5D%2B%5C.%5B0-9%5D%2B%29&amp;replace=v%241&amp;label=release&amp;cacheSeconds=3600"></a>
  <a href="https://www.npmjs.com/package/@preloop-dev/cli"><img alt="npm version" src="https://img.shields.io/npm/v/@preloop-dev/cli?label=npm&amp;logo=npm&amp;logoColor=white"></a>
</p>

![Watch the preloop demo: a failed step, a fix in another pane, and a re-run, all local, no push required](docs/demo/demo.gif)

preloop is a local, self-hosted equivalent of GitHub Actions. The engine (`preloop serve`) accepts workflows the same way GitHub does — `${{ }}` expressions, matrices, reusable workflows, concurrency groups, OIDC — and runs them in hardware-isolated microVMs. Your `.github/workflows` run unmodified, including against uncommitted changes.

It speaks the official `actions/runner` protocol, so the official runner can register against it. Or use our Rust runner, which installs 36× smaller and peaks at 19× lower memory (see benchmarks below).

## Install

Pick one:

**Homebrew**

```sh
brew install preloopdev/tap/preloop
```

**npm**

```sh
npm install -g @preloop-dev/cli
```

**mise**

```sh
mise use -g github:preloopdev/preloop
```

**Installer script**

```sh
ver=v0.35.5   # https://github.com/preloopdev/preloop/releases
curl -fsSL "https://raw.githubusercontent.com/preloopdev/preloop/$ver/install.sh" -o install.sh
less install.sh
sh install.sh
```

Setup, credentials, golden images, `preloop init`, and first run are in the docs:

- [Install](https://docs.preloop.dev/control-plane/install)
- [Quickstart](https://docs.preloop.dev/control-plane/quickstart)

### Just the runner

`preloop-runner` is a drop-in Rust replacement for `actions/runner`. It registers against GitHub or a preloop server the same way.

```sh
ver=v0.35.5
curl -fsSL "https://raw.githubusercontent.com/preloopdev/preloop/$ver/install.sh" -o install.sh
less install.sh
sh install.sh --runner
```

Container image and full register/run steps: [runner install](https://docs.preloop.dev/runner/install).

## What makes it different

- Real runner protocol, not a behavior approximation — the official runner works against it unchanged.
- Hardware-isolated microVMs for control-plane jobs.
- GitHub App and fine-grained PAT support with per-job token minting so checks stay updated.
- Heavily tested: property tests, differential tests, formal verification, and conformance gates.
- NDJSON event output for agents and developer tooling.

Detailed comparison: [docs.preloop.dev/comparison](https://docs.preloop.dev/comparison).

## Benchmarks

Same server, same job, measured on the same machine (M1 Pro, macOS, median of 3):

![preloop-runner vs actions/runner](benchmarks/runner-compare.gif)

| Metric | actions/runner 2.336.0 | preloop-runner | |
|---|---|---|---|
| Install footprint | 434 MB · 9,301 files | 11.9 MB · 4 files | 36× |
| Runner code only | 85 MB | 11.9 MB | 7× |
| Cold start | 97 ms | 29 ms | 3.3× |
| Time to listening | 456 ms | 32 ms | 14× |
| Idle RSS | 67 MB | 9 MB | 7.5× |
| Warm pool ×10 idle | 672 MB | 90 MB | 7.5× |
| Job pickup | 268 ms | 100 ms | 2.7× |
| Peak RSS during a job | 206 MB | 11 MB | 19× |

Reproduce: `scripts/bench-runner-compare.sh`. Chart: `benchmarks/runner-compare.html`.

## Documentation

| Topic | Doc |
|---|---|
| Docs home | [docs.preloop.dev](https://docs.preloop.dev) |
| Install / quickstart | [install](https://docs.preloop.dev/control-plane/install) · [quickstart](https://docs.preloop.dev/control-plane/quickstart) |
| Configuration | [configuration](https://docs.preloop.dev/control-plane/configuration) |
| CLI reference | [CLI](https://docs.preloop.dev/control-plane/reference/cli) |
| Deploy / self-host | [deploy](https://docs.preloop.dev/control-plane/operate/deploy) |
| VM images | [vm-images](https://docs.preloop.dev/control-plane/operate/vm-images) |
| GitHub App / tokens | [app](https://docs.preloop.dev/control-plane/github/app) · [tokens](https://docs.preloop.dev/control-plane/github/tokens) |
| Debugging / agents | [debugging](https://docs.preloop.dev/control-plane/guides/debugging) · [agents](https://docs.preloop.dev/control-plane/guides/agents) |
| Runner | [overview](https://docs.preloop.dev/runner/overview) · [install](https://docs.preloop.dev/runner/install) |
| Compatibility / comparison | [compatibility](https://docs.preloop.dev/compatibility) · [comparison](https://docs.preloop.dev/comparison) |
| Architecture | [overview](https://docs.preloop.dev/control-plane/architecture/overview) |
| Contributing | [CONTRIBUTING.md](CONTRIBUTING.md) |

## License

Two licenses, one project:

- **Everything except the control plane is MIT** — the CLI, the Rust runner, the parser/expression/protocol crates, the VM orchestrator, and the docs.
- **The control plane (`preloop serve` / `preloop-runner-server`) is FSL-1.1-MIT** — source-available. You may use, modify, and redistribute it for any non-competing purpose; it converts to MIT on the second anniversary of each release. "Non-competing" means you can't offer it as a hosted CI service that competes with preloop's own offering.

Full terms: `crates/preloop-runner-server/LICENSE` (FSL-1.1-MIT) and MIT for the rest.

## Credits

- [smolvm] — the microVM runtime every job executes in
- [runner.server] — the protocol reverse-engineering this project builds on

[smolvm]: https://github.com/smol-machines/smolvm
[runner.server]: https://github.com/ChristopherHX/runner.server
