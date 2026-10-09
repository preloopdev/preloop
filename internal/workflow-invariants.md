# Workflow and CI invariants (maintainer notes)

Rationale that used to live as long comments in public workflow files. The
public comments keep the directive and the constraint; the "why" behind each
one is here so the detail stays reviewable without turning every workflow into
an incident report.

## Pullfrog review bot (`.github/workflows/pullfrog.yml`)

- **Fork PRs never run, through two gates.** `pull_request` events require
  `head.repo.full_name == github.repository`. `issue_comment` payloads carry
  no head repo at all, so a first step resolves the PR's head through the API
  URL named in the payload and gates every later step on it; an unresolved or
  failed lookup skips the run, so the gate fails closed.
- **The comment trigger is restricted to write-access authors**
  (`author_association` in OWNER/MEMBER/COLLABORATOR). The repository is
  public: an unrestricted `@pullfrog` trigger would let any GitHub user spend
  the model credentials the action holds.
- **`TMPDIR` is pinned to `runner.temp` and must stay pinned.** The action
  clones into a `pullfrog-XXXXXX` directory under the system temp dir and
  never cleans it up. Where the system temp dir is memory-backed, every leaked
  clone permanently consumes RAM; enough of them can push the runner host and
  the control plane into the OOM killer. `runner.temp` is disk-backed and is
  cleared between jobs. Do not "tidy" this variable away.
- **`GH_TOKEN` replaces the action's OIDC token exchange.** Pullfrog's server
  verifies OIDC tokens from GitHub's own issuer only; a token minted by a
  preloop engine fails that exchange. The external-token path uses a plain
  GitHub token for git and API work instead. That token must carry
  `pull-requests: write` (or the review POST is a 403 that still exits 0), so
  prefer an explicitly scoped credential over the job token where the App
  installation does not grant the scope.
- **One review per PR, job-level.** The concurrency group is keyed per PR and
  lives on the job so that runs skipped by the job `if:` never enter the
  group; a newer commit supersedes the running review of the same PR without
  cancelling another PR's review.
- **Model and effort pins** are deliberate and reviewed with the workflow.

## Golden bakes (`official-golden.yml`, `release-golden.yml`, `release.yml`)

- **Goldens are baked by architecture-matching hosts, never by the release
  pipeline.** A golden's content is keyed by the official-image pin, the
  toolchain pins, and the apt-index bake date — none of which move with an
  engine release — and the runner bundle is mounted from the engine at job
  time. A per-release bake would ship a byte-identical artifact for >110 GiB
  of staging. Releases therefore *seed* the newest complete golden pair
  (payload plus sidecars) so `golden_url_candidates` never 404s and never
  claims a checksum it cannot prove.
- **Disk-headroom gate: the host must have the guest's own size free before
  the bake starts.** `pack create` writes the export's layers and a template
  the size of the guest disk next to the output, on the *host*. "The file is
  sparse" only describes the guest-side view; it does not make a large guest
  disk safe when the host is nearly full. Without the gate, each attempt burns
  its full runtime before dying with `tar error: No space left on device`.
- **Never stage the base with a host `docker save` tar.** The uncompressed
  export does not fit a hosted runner's disk, and `docker save` fails with
  `no space left on device` after a successful pull. Stream the base from the
  registry inside the bake VM so no copy lands on the host filesystem.
- **`SMOLVM_MAX_IMAGE_BYTES` must stay raised for runner-scale bases.**
  Runner-large base archives exceed smolvm's 8 GiB default local-archive cap,
  and the bake fails at `smolvm create` before extracting a byte.
- **`SMOLVM_EXPORT_HELPER_STORAGE_GIB` = the template size.** The export
  helper's disk defaults to 3x the source; growing the helper's filesystem at
  boot delays its agent accept loop, whose control listener has a backlog of
  one, so early host connections get reset and libkrun exhausts its connect
  retries — the pack fails with `Connection reset by peer`. A helper disk the
  template's size boots without the grow.
- **Answer the guest directly instead of through the NAT DNS proxy.** The
  proxy has not resolved ghcr.io from a bake VM on hosted runners; the direct
  answer makes the same pull succeed. Trying the default first only costs an
  attempt and a backoff.
- **Machine names are ASCII alphanumerics and hyphens only** (smolvm
  constraint), and aarch64 bakes cannot run on hosted macOS runners
  (EINVAL), hence the manual Apple Silicon path.

## CI resource sizing (`ci.yml`)

- 4 GiB runner guests OOM (SIGKILL) when too many rustc/linker jobs run at
  once; `CARGO_BUILD_JOBS: "4"` is tuned for that ceiling (a larger box could
  carry more). The rust shard display names are load-bearing: the `main`
  ruleset gates on them, so renaming a shard requires updating the ruleset.
- The preloop-runner-server integration tests live in separate `tests/*.rs`
  crates because the single large lib unit did not fit the 4 GiB guest; they
  need the `test-support` feature, which nextest cannot scope per package in a
  workspace run, so they run under `cargo test`.
- LFS-tracked golden captures are fetched as a small set of individual flows
  rather than the complete pinned corpus (which also carries large container
  captures), and every job that needs them must authenticate: the
  unauthenticated LFS API rate limit otherwise kills the run.

## Dependency bumps (`renovate.json`, `versions.toml`)

- `smolvm_min_version` is the runtime floor compiled into the CLI and the VM
  crate; it must stay at the first release that satisfies every runtime
  invariant (fork freeze-source flag, packed-machine UID/GID restoration for
  an unprivileged smolvm). `smolvm_golden_version` is Renovate-managed and
  tracks the last release verified by `smolvm-release-verify.yml` (real-VM E2E
  on GitHub-hosted KVM). SmolVM bumps auto-merge once that check and `ci.yml`
  pass.
- The `actions/runner` bump is a tripwire for the runner-watch pipeline and
  never auto-merges: the protocol delta needs goldens and review first.
