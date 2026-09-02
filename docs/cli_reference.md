# preloop CLI reference

Every command, flag, and argument of the `preloop` CLI. Generated from the
CLI's own help output — run `preloop <command> --help` for the same text
locally.

## Global

```
Usage: preloop <COMMAND>
```

| Command | Purpose |
|---|---|
| `run` | Submit + stream a workflow run |
| `plan` | Show the expanded job DAG without executing |
| `status` | Show active and recent runs |
| `logs` | Show run logs (defaults to the most recent run) |
| `cancel` | Cancel the current run |
| `secret` | Manage the local secret store |
| `setup` | Configure GitHub credentials (App or fine-grained PAT) |
| `doctor` | Verify the GitHub credential configuration |
| `server` | Install/remove the control plane as a supervised service |
| `shell` | Open a shell in a preserved VM |
| `debug` | Attach to a job paused at a failed step |
| `update` | Poll GitHub Releases and atomically self-update |
| `serve` | Run the control plane + microVM runner pool in the foreground |

`preloop run` auto-starts the engine when no server is reachable
(`ensure_engine_running`); `serve` is the explicit foreground server.

---

## `preloop run [OPTIONS]`

Submit a workflow and stream its events until terminal.

| Flag | Description |
|---|---|
| `-f, --file <FILE>` | Workflow file path. Bare filenames resolve inside `.github/workflows/` |
| `--job <JOB>` | Run a single job by its YAML key (includes the `needs:` dependency closure) |
| `--event <EVENT>` | Simulated trigger event (`push`, `pull_request`, `merge_group`, …). Default `push` |
| `--payload <PATH>` | Event payload JSON file (the webhook body) for the simulated trigger |
| `--base <BASE>` | Base ref for `pull_request` / `merge_group` events |
| `--no-debug` | Tear down on failure instead of pausing for debugging |
| `--preserve-on-failure` | Keep the failed job VM alive when nothing can attach interactively |
| `--secret <NAME=VALUE>` | Inline secret, repeatable |
| `-d, --detach` | Submit and return immediately (run continues in the background) |

Behavior notes:
- **Pausing**: a failed step pauses by default in an interactive terminal so you
  can fix and retry. Non-interactive runs (pipes, CI, `--detach`) never pause.
- **Local workspace**: the run snapshots the current workspace (uncommitted
  changes included) — the run never depends on what was pushed.
- Local reusable workflows (`uses: ./.github/workflows/…`) are uploaded with
  the submission automatically.
- **Simulated event context**: a local run stands in for a webhook delivery, so
  the CLI fills in the parts of that delivery git can answer for. Nothing else
  is invented — see below.

### What a local run derives

GitHub sends a webhook body; `preloop run` has git instead. These are derived
automatically, and an explicit `--payload` field always wins:

| Field | Derived from | Why it is safe |
|---|---|---|
| changed files (`paths` / `paths-ignore` filters) | `git diff --name-only <base>...HEAD` plus uncommitted changes | The run tests the working tree, so the filter should judge the same files |
| PR activity type (`types:` filters) | defaults to `synchronize` | One of GitHub's default `pull_request` types |
| target branch (`branches:` filters on `pull_request`) | `--base`, else the branch's tracking ref | GitHub applies PR branch filters to the **target** branch, not the head branch |
| branch / tag | the current checkout | It is the ref being tested |

The base for the diff is `--base` when given, otherwise the branch's tracking
ref, then each remote's default branch, then local `main`/`master`. A candidate
is only used if it shares history with `HEAD`, so a fork remote with unrelated
history is skipped rather than picked and then failing to diff.

Nothing else is synthesized. PR number, actor, labels, review state, and
`workflow_run` upstream results have no local truth, and guessing them would
flip `if:` conditions on fabricated data — pass `--payload` when a workflow
needs them.

If no usable base is found, the change set stays *unknown* rather than empty:
an empty known list would make every `paths:` filter reject the run. Path-
filtered workflows then fail with an error naming what to pass.

## `preloop plan [OPTIONS]`

Show the expanded job DAG (matrix fan-out, `needs:`) without executing.

| Flag | Description |
|---|---|
| `-f, --file <FILE>` | Workflow file path |
| `--json` | Machine-readable output |

## `preloop status`

Show active and recent runs (RUN ID, number, status, event, workflow).
No flags.

## `preloop logs [RUN_ID] [OPTIONS]`

| Argument | Description |
|---|---|
| `RUN_ID` | Run ID (defaults to the **most recent** run) |

| Flag | Description |
|---|---|
| `--job <JOB>` | Narrow to one job: the workflow job key (`build`) or its agent job UUID |
| `--step <STEP>` | Narrow to one 1-based step within the job, in execution order |
| `-f`, `--follow` | Stream one job's output and exit when that job finishes |

Without flags, every job's log is merged in job-request order.

`--step` counts user-visible steps from 1, matching `preloop debug --from`. It
needs `--job` when a run has more than one job, because numbering restarts per
job. A job whose runner uploaded a single merged log has no recoverable step
boundaries; asking for a step there fails with `409` rather than returning the
whole job under a step's name.

`--follow` tracks one job's live console feed, so it needs `--job` unless the
run has exactly one job. It replays the retained buffer before going live, then
exits when that selected job completes. If the job is already complete, it
returns the available durable log instead. It cannot be combined with `--step`
(the feed carries whole steps as they stream).

```bash
preloop logs                          # whole latest run
preloop logs --job test               # just the `test` job
preloop logs --job test --step 3      # just that job's third step
preloop logs -f --job test            # tail it live
```

## `preloop cancel [RUN_ID]`

Cancel a run. `RUN_ID` defaults to the most recent active run.

## `preloop secret <COMMAND>`

Manage the secret store. Secrets are `SecretString`-typed end to end; `list`
never prints values.

### `preloop secret set [OPTIONS] <NAME>`

| Flag | Description |
|---|---|
| `--value <VALUE>` | Value. Omitted → read one line from stdin (hidden on a TTY) |
| `--repo <REPO>` | Scope to one repository (`owner/repo`) instead of global |
| `--env <ENV>` | Scope to one environment of `--repo` (requires `--repo`) |

### `preloop secret list [OPTIONS]`

| Flag | Description |
|---|---|
| `--repo <REPO>` | Only secrets scoped to this repository |
| `--env <ENV>` | Only secrets scoped to this environment (requires `--repo`) |

### `preloop secret rm [OPTIONS] <NAME>`

| Flag | Description |
|---|---|
| `--repo <REPO>` | Remove from this repository scope instead of global |
| `--env <ENV>` | Remove from this environment scope (requires `--repo`) |

## `preloop setup github [OPTIONS]`

Configure GitHub credentials.

| Flag | Description |
|---|---|
| `--via <app\|pat>` | Credential type. `app` (recommended) or `pat` (fine-grained PAT, for orgs that gate App installations) |
| *(no App flags)* | With `--via app` and no `--app-id`/`--pem-file`, creates the App through GitHub's manifest flow in your browser and stores everything |
| `--app-id <ID>` | GitHub App ID (with `--via app`) |
| `--pem-file <PATH>` | Path to the GitHub App private key PEM (with `--via app`) |
| `--org <NAME>` | Create the App under an organization instead of your account |
| `--public-url <URL>` | Enable webhook delivery to this URL. On an already-configured App, updates its webhook instead of creating a second App |
| `--app-name <NAME>` | Name of the created App (default `preloop-local`; GitHub requires global uniqueness) |
| `--port <N>` | Pin the loopback port GitHub redirects back to (default: a free port) |
| `--no-browser` | Print the URL instead of opening a browser |
| `--webhook-secret <SECRET>` | Store a webhook secret you created yourself (the App flow gets one from GitHub automatically) |
| `--token <TOKEN>` | PAT to store (with `--via pat`). Falls back to `PRELOOP_GITHUB_PAT`, then an interactive prompt |
| `--repo <REPOS>` | Repository to verify the credential against (repeatable) |
| `--workspace <WORKSPACE>` | Workspace whose workflows should drive the permission checklist |

## `preloop doctor [OPTIONS]`

Verify the GitHub credential configuration.

| Flag | Description |
|---|---|
| `--repo <REPOS>` | Repository to verify the credential against (repeatable) |

## `preloop server <COMMAND>`

Install/remove the control plane as a supervised service.

### `preloop server install [OPTIONS]`

| Flag | Description |
|---|---|
| `--listen <ADDR>` | Bind address. Defaults to the engine default (`127.0.0.1:9090`); on Linux the port publishes through socket activation |
| `--public-url <URL>` | Externally reachable base URL (webhook + Checks links). GitHub must reach it — public DNS + reverse proxy, or a tunnel (cloudflared, ngrok, Tailscale Funnel) |
| `--github-app-id <ID>` | GitHub App id |
| `--github-app-key <PATH>` | GitHub App private key PEM path |
| `--github-app-installation-id <ID>` | Installation id (skips discovery) |
| `--webhook-secret <SECRET>` | Shared secret for `X-Hub-Signature-256` |
| `--home <PATH>` | State directory for the service (default `/var/lib/preloop`) |
| `--no-update-timer` | Skip the systemd self-update timer (Linux) |
| `--systemd-credential <PATH>` | Encrypted systemd credential (`LoadCredentialEncrypted=preloop-secrets`) with `[secrets]`/`[repo_secrets]`; create with `systemd-creds encrypt --name=preloop-secrets secrets.toml PATH` (Linux only) |
| `--user` | Per-user units (systemd user units / LaunchAgent), state in `~/.preloop`; `loginctl enable-linger $USER` keeps them alive after logout |
| `--dry-run` | Print what would be written/run without touching the system |

### `preloop server uninstall [OPTIONS]`

| Flag | Description |
|---|---|
| `--home <PATH>` | State directory the service was installed with (default `/var/lib/preloop`) |
| `--purge-data` | Also delete the state directory and everything in it |
| `--user` | Uninstall the per-user service |
| `--dry-run` | Print what would be removed without touching the system |

## `preloop shell [RUN_REF]`

Open a shell in a preserved (failed) VM.

| Argument | Description |
|---|---|
| `RUN_REF` | Run reference (e.g. `last-failed`). Defaults to the last failed run |

## `preloop debug [OPTIONS] [SESSION]`

Attach to a job paused at a failed step.

| Argument | Description |
|---|---|
| `SESSION` | Session id, run id, or job name. Optional when exactly one is paused |

| Flag | Description |
|---|---|
| `--json` | Print the paused session as JSON and exit (for agents/scripts) |
| `--verdict <retry\|continue\|abort>` | Issue a verdict without attaching |
| `--sync` | With `--verdict retry`: sync host source changes into the VM first |
| `--export` | Bring source edits made inside the VM back to the host workspace |
| `--patch-only` | With `--export`: write the patch but do not apply it |
| `--force` | Overwrite VM-side edits when syncing (without it, both-sides-changed aborts) |
| `--revert <none\|untracked\|all>` | With `--verdict retry`: undo the failed attempt's workspace debris (default `none`) |
| `--from <STEP>` | With `--verdict retry`: re-run from a 1-based step number or display name |
| `--from-start` | With `--verdict retry`: re-run from the first user step in this job |

## `preloop update [OPTIONS]`

Poll GitHub Releases and atomically install the matching binary.

| Flag | Description |
|---|---|
| `--check` | Only check for a newer release; do not install |
| `--version <VERSION>` | Install a specific release tag/semver instead of the latest |
| `--repository <OWNER/NAME>` | Release source repo [env: `PRELOOP_RELEASE_REPOSITORY`] |

## `preloop serve [OPTIONS]`

Run the control plane and microVM runner pool in the foreground — the
self-hosting entry point (webhook + Checks endpoints, microVM provisioning).

| Flag | Description |
|---|---|
| `--listen <ADDR>` | Bind address. Overrides `PRELOOP_LISTEN` |
| `--public-url <URL>` | Externally reachable base URL. Overrides `PRELOOP_PUBLIC_URL`. Loopback is only correct when everything is on one host |
| `--github-app-id <ID>` | GitHub App id |
| `--github-app-key <PATH>` | GitHub App private key PEM path |
| `--github-app-installation-id <ID>` | Installation id (skips discovery) |
| `--webhook-secret <SECRET>` | Shared secret for `X-Hub-Signature-256` |
| `--save` | Persist the supplied GitHub credentials for later runs |
| `--store <URL>` | Durable-state backend: `sqlite://<path>`, a bare path, or `postgres://…` (with optional `?sslmode=require\|verify-full`). Defaults to `PRELOOP_STORE_URL`, then SQLite in the state dir |

## Golden image automation

Release automation and operators building organization-specific images can
pack a golden with:

```sh
preloop build-golden \
  --runner-bundle target/aarch64-unknown-linux-gnu/release \
  --base-image ghcr.io/acme/preloop-base@sha256:<digest> \
  --storage-gb 80 \
  --output dist/acme-ubuntu-24.04-aarch64
```

The command is hidden from the normal help because it is an operator/build
command rather than part of workflow submission. The base must currently be
Ubuntu-derived, and its architecture must match the runner bundle and host.
`--base-image` also accepts a registry snapshot of the official hosted image.
Set `--storage-gb` or `PRELOOP_RUNNER_STORAGE_GB` for large snapshots; the
default is 20 GiB.
See [VM images and version tracking](vm-images.md#building-a-golden) for the
stock build, custom OCI base, checksum, publishing, and runtime configuration
steps.

## Environment variables

| Variable | Purpose |
|---|---|
| `PRELOOP_LISTEN` | Default bind address for `serve` |
| `PRELOOP_PUBLIC_URL` | Default public base URL |
| `PRELOOP_STORE_URL` | Default durable-state backend |
| `PRELOOP_GITHUB_PAT` | PAT fallback for `setup github --via pat` |
| `PRELOOP_RELEASE_REPOSITORY` | Release source for `update` |
| `PRELOOP_HOME` | State directory (default `~/.config/preloop`) |
| `PRELOOP_RUNNER_POOL_ENABLED` | Enable the local microVM runner pool (default off) |
| `PRELOOP_RUNNER_POOL_SIZE` | Pool size (warm forks/VMs) |
| `PRELOOP_USE_FORK` | Run the pool as forked microVMs (default true with a packed golden) |
| `PRELOOP_USE_PACKED_GOLDEN` | Use a release or locally cached packed golden (default on; set `false` for cold OCI provisioning) |
| `PRELOOP_GOLDEN_URL` | Override the packed golden URL; checksum URL is this value plus `.sha256` |
| `PRELOOP_GOLDEN_OCI_REF` | Override the default public OCI packed golden reference (arm64 default: `ghcr.io/preloopdev/preloop-golden@sha256:a2f7caf367e19efa4cb2d6f32a7093db8fae79e1b1525b65ac1190c1d2b44361`) |
| `PRELOOP_RUNNER_BASE_IMAGE` | Override the digest-pinned Ubuntu base identity at serve time; set it with `PRELOOP_GOLDEN_URL` for a custom packed golden |
| `PRELOOP_VERIFY_BASE_IMAGE` / `PRELOOP_VERIFY_BASE_IMAGE_REPO` | Require a digest-pinned OCI base's GitHub attestation and Cosign signature before `build-golden` |
| `PRELOOP_REQUIRE_BASE_DIGEST` | Reject mutable registry tags during `build-golden` (used by release provenance builds) |
| `PRELOOP_RUNNER_STORAGE_GB` | Persistent guest storage per runner and golden build (default 20 GiB; use 80 or more for full hosted-image snapshots) |
| `PRELOOP_RUNNER_PACK_PROXY` | HTTP proxy for smolvm's separate registry export VM during golden packing; standard HTTP(S) proxy variables are fallbacks |
| `PRELOOP_RUNNER_PACK_NO_PROXY` | Proxy bypass list for golden packing; `NO_PROXY` and `no_proxy` are fallbacks |
| `PRELOOP_RUNNER_LABELS` | Extra `runs-on` labels the pool's runners declare |
| `PRELOOP_RUNNER_USER` / `PRELOOP_RUNNER_UID` | Guest runner account (default `runner`/1001); `root` restores root; empty disables switching |
| `PRELOOP_WORKSPACE` | Workspace context for daemon deployments; not a package or toolchain installation input |
| `AKSH_URL` | Server URL for the client commands (default `http://127.0.0.1:9090`) |
| `AKSH_SYSTEM_TOKEN` | Native API bearer token (also `AKSH_TOKEN`) |
| `AKSH_PUBLIC_URL` | Public URL used in check-run details links |
| `AKSH_GITHUB_TOKEN` | PAT fallback for GitHub API calls (check runs need the App) |
| `AKSH_GITHUB_API_URL` | Override the GitHub API base (tests, GHES) |
| `AKSH_WEBHOOK_SECRET` | Webhook signature secret (the server's only source of truth for repo hooks) |

## Quick examples

```sh
preloop run -f ci.yml                            # run the workflow, stream events
preloop run -f ci.yml --job test --secret TOKEN=x
preloop run -d -f ci.yml && preloop logs        # submit detached, watch the latest run
preloop plan -f ci.yml --json                    # inspect the expanded DAG
preloop status                                   # run list
preloop cancel <run_id>                          # stop a run
preloop secret set GH_TOKEN --repo owner/repo    # repo-scoped secret
preloop setup github --via app                   # creates the App in a browser
preloop doctor --repo owner/repo
preloop server install --public-url https://ci.example.com --webhook-secret "$(openssl rand -hex 32)"
preloop serve                                    # foreground engine + pool
preloop update --check                           # is there a newer release?
```
