# preloop

![Watch the preloop demo: a failed step, a fix in another pane, and a re-run, all local, no push required](docs/demo/demo.gif)

![Watch a preloop debug session: a step fails and pauses, fix the file in another pane, `:sync` + `:retry` from the paused job, and it continues — no re-run of the whole workflow](demo/debug/debug.gif)

![Watch an omp agent attach to a live preloop job with DAP, inspect runtime context, diagnose the failure, and rerun successfully](docs/demo/dap/demo-highlight.gif)



> A failed step, a fix in another pane, and a re-run, all local, with no push required.


preloop is a local, self-hosted equivalent of GitHub Actions. The engine (`preloop serve`) accepts workflows the same way GitHub does: `${{ }}` expressions, matrix builds, reusable workflows, concurrency groups, OIDC etc. It executes them on hardware-isolated microvms that work on Windows/macOS or Linux, and resume in 300ms. Your `.github/workflows` run unmodified, and you can run CI against your uncommitted changes(respects .gitignored/untracked ones).


It speaks the official `actions/runner` protocol, so you can use the official runner to register, poll, execute, and report against it without GitHub-hosted minutes. You can also use our Rust runner, which installs 36× smaller and peaks at 19× lower memory (see benchmarks below).

Preloop doesn't rely only on GitHub Webhooks to update the status of the CI. We run a Webhook watchdog that detects drift, and reconciles your commits with received webhooks against the GitHub API. You can run your committed changes and run workflows locally and pass `--push` or `--create-pr` to open a PR with the checks correctly updated (new PRs open as drafts by default, see `--pr-draft`). This can be useful if/when GitHub's webhook service is down.

## Quick start

```sh
# Install (macOS/Linux), any one of:
brew install preloopdev/tap/preloop   # Homebrew
npm install -g @preloop-dev/cli       # npm, downloads the same release binaries
mise use -g github:preloopdev/preloop # mise, downloads the same release archive

# Or fetch the installer at a release tag, read it, then run it. It downloads
# the release binaries and verifies the sha256 of each one.
ver=v0.34.0   # latest release, others at https://github.com/preloopdev/preloop/releases
curl -fsSL "https://raw.githubusercontent.com/preloopdev/preloop/$ver/install.sh" -o install.sh
less install.sh
sh install.sh
```
```
preloop init              # wizard: credentials, golden image, how to run
preloop serve            # engine on 127.0.0.1:9090
```
```
cd my-repo
preloop run -f .github/workflows/ci.yml --event pull_request
```
`preloop init` is a short setup wizard. It sets up your GitHub credential,
picks the VM image your jobs run in, and chooses how the engine runs, then
writes `~/.preloop/config.toml`. Without a terminal it takes the same answers
as flags. Every flag is in the [`init` reference](docs/control-plane/reference/cli.mdx).

`preloop serve` runs in the foreground. Add `-d` to detach it.

The first run downloads and prepares the default VM image. Its size depends on
the release and host architecture; see the [VM image guide](docs/control-plane/operate/vm-images.mdx)
before provisioning a machine. Each job runs in an isolated VM. To use your
own image, see the same guide.

Prefer not to run a script? Download `preloop-cli-<target>.tar.gz` and its
`.sha256` from the [latest release](https://github.com/preloopdev/preloop/releases/latest),
check the checksum, put `preloop` on your `PATH`, and run `preloop update` to
install the pinned SmolVM runtime. Release assets also carry build provenance:
`gh attestation verify <asset> --repo preloopdev/preloop`. MicroVM jobs also
need the Linux guest runner at `<prefix>/lib/preloop/runner/<triple>/preloop-runner`.
`install.sh` installs it, and the [deployment guide](docs/control-plane/operate/deploy.mdx)
has the manual commands.

On Apple Silicon, x86_64 goldens run through Rosetta 2. Prefer arm64 goldens
when you can. Known limits are in the [compatibility guide](docs/compatibility.mdx).

You can simulate most GitHub events locally. For some events, you might need to add a payload.
See the [CLI reference](docs/control-plane/reference/cli.mdx) for more flags you can pass.

To continue with the setup, see [configuration and credentials](docs/control-plane/configuration.mdx).

Run it as a server, service install, runtime knobs, and exposure options in the
[deployment guide](docs/control-plane/operate/deploy.mdx).

### Just the runner



`preloop-runner` is a drop-in Rust replacement for `actions/runner`. It
registers against GitHub (or a preloop server) the same way. The repository's
runner comparison shows a smaller install and lower idle and peak RSS; see the
benchmark table below for the measured values.

```sh
# Fetch the installer at a release tag, read it, then run it with --runner.
ver=v0.34.4   # replace with another release tag if needed
curl -fsSL "https://raw.githubusercontent.com/preloopdev/preloop/$ver/install.sh" -o install.sh
less install.sh
sh install.sh --runner
export PATH="$HOME/.local/bin:$PATH"
preloop-runner configure --url https://github.com/owner/repo --token <registration-token>
preloop-runner run
```
Each release publishes standalone binaries for Linux (x86_64/aarch64) and
macOS (x86_64/arm64), each with a CycloneDX SBOM
(`preloop-runner_<triple>.cdx.json`) and a sha256 checksum. Windows uses the
Linux build under WSL2.


Or use the container image with a persistent volume:

```sh
docker run --rm -v preloop-runner-data:/runner \
  ghcr.io/preloopdev/preloop-runner:latest \
  --runner-root /runner configure --url … --token … --unattended
docker run --rm -v preloop-runner-data:/runner \
  ghcr.io/preloopdev/preloop-runner:latest \
  --runner-root /runner run
```

Details: [runner installation](docs/runner/install.mdx).

## What makes it different vs others.

- We use the real runner protocol, not a behavior approximation: the official runner binary works against it unchanged.
- Hardware-isolated microVMs run control-plane jobs; host requirements and startup behavior vary by substrate.
- GitHub App and fine-grained PAT support with per-job token minting so your checks get updated.
- DAP-powered job inspection: attach at entry, inspect live GitHub/job context, pause, and continue, and allow for step-level retries.
- Heavily tested with property tests, differential tests, and formal verification. Conformance is table-stakes.
- NDJSON event output for agents and developer tooling.

For a detailed comparison, see [the comparison guide](docs/comparison.mdx).


### Agent-driven debugging

When a run is submitted with the DAP debugger enabled, preloop holds the job at entry until a debugger attaches. An agent or compatible DAP client can then inspect the live `github`, `env`, `runner`, `job`, `steps`, and `secrets`
scopes before continuing the job. This is useful when the workflow YAML looks
right but the runtime event payload, matrix values, or generated context is
wrong.


```sh
preloop dap <run-id>
```

## Benchmarks

`preloop-runner` is a from-scratch Rust reimplementation of `actions/runner`
that speaks the same protocol — the official runner works against preloop
unchanged, and preloop-runner registers against GitHub the same way. Same
server, same job, measured on the same machine (M1 Pro, macOS, median of 3):

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

The official tarball bundles node20 + node24 (~350 MB) into every install;
preloop-runner ships as a single binary and mounts one shared read-only copy
of the externals into every VM (or bakes them into the golden image). A warm
pool of 10 idle runners costs ~90 MB instead of ~670 MB.


Reproduce: `scripts/bench-runner-compare.sh` (set `BENCH_RUNS=N`; needs
`target/release/preloop-runner` and the official runner in
`~/.cache/actions-runner/current`). Interactive chart:
`benchmarks/runner-compare.html`.

## Documentation on where to go to find info.

| Topic | Doc |
|---|---|
| Setup, credentials, secrets, config | [control-plane install](docs/control-plane/install.mdx) · [configuration](docs/control-plane/configuration.mdx) |
| CLI reference | [docs/control-plane/reference/cli.mdx](docs/control-plane/reference/cli.mdx) |
| Hosting it yourself: service install, knobs, exposure | [docs/control-plane/operate/deploy.mdx](docs/control-plane/operate/deploy.mdx) |
| VM images, version pins, and custom goldens | [docs/control-plane/operate/vm-images.mdx](docs/control-plane/operate/vm-images.mdx) |
| GitHub App webhooks and check runs | [docs/control-plane/github/app.mdx](docs/control-plane/github/app.mdx) |
| Job tokens, minting, OIDC | [docs/control-plane/github/tokens.mdx](docs/control-plane/github/tokens.mdx) |
| Debug sessions (pause, inspect, retry) | [docs/control-plane/guides/debugging.mdx](docs/control-plane/guides/debugging.mdx) |
| Pre-push checks for coding agents | [docs/control-plane/guides/agents.mdx](docs/control-plane/guides/agents.mdx) |
| Architecture and crate map | [docs/control-plane/architecture/overview.mdx](docs/control-plane/architecture/overview.mdx) |
| Protocol compatibility | [docs/compatibility.mdx](docs/compatibility.mdx) |
| Contributing and CI requirements | [CONTRIBUTING.md](CONTRIBUTING.md) |

## License

Two licenses, one project:

- **Everything except the control plane is MIT** — the CLI, the Rust runner,
  the parser/expression/protocol crates, the VM orchestrator, and the docs.
- **The control plane (`preloop serve` / `preloop-runner-server`) is
  FSL-1.1-MIT** so source-available. You may use, modify, and redistribute it
  for any non-competing purpose (internal CI, commercial products, forks),
  and it converts to MIT on the second anniversary of each release. What
  "non-competing" means: you can't offer it as a hosted CI *service* that
  competes with preloop's own offering.

Full terms: `crates/preloop-runner-server/LICENSE` (FSL-1.1-MIT) and MIT for
the rest.

## Credits

This project wouldn't especially be possible without:

- [smolvm] — the microVM runtime every job executes in
- [runner.server] — the protocol reverse-engineering this project builds on

[smolvm]: https://github.com/smol-machines/smolvm
[runner.server]: https://github.com/ChristopherHX/runner.server
