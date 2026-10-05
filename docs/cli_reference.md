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
| `version` | Print the installed Preloop version |
| `run` | Submit + stream a workflow run |
| `plan` | Show the expanded job DAG without executing |
| `status` | Show active and recent runs |
| `logs` | Show run logs (defaults to the most recent run) |
| `cancel` | Cancel the current run |
| `secret` | Manage the local secret store |
| `setup` | Configure GitHub credentials (App or fine-grained PAT) |
| `doctor` | Verify the GitHub credential configuration |
| `server` | Install/remove the control plane as a supervised service |
| `store` | Control-database operations: import a legacy `preloop.db` |
| `shell` | Open a shell in a preserved VM |
| `debug` | Attach to a job paused at a failed step |
| `dap` | Attach an interactive DAP client to a debugger-enabled run |
| `push` | Publish a completed run's result to GitHub (commit, PR, check runs) |
| `webhooks` | Inspect, replay and health-check the durable webhook queue |
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
| `--debug` | Open a live debug session when a job fails |
| `--no-debug` | Tear down on failure instead of pausing for debugging (hidden compatibility flag) |
| `--preserve-on-failure` | Keep the failed job VM alive when nothing can attach interactively |
| `--secret <NAME=VALUE>` | Inline secret, repeatable |
| `--strict-secrets` | Fail instead of warning when the workflow reads secrets this engine has not stored |
| `-d, --detach` | Submit and return immediately (run continues in the background) |
| `--push` | After the run completes, push the tested commit and publish the result (pull request + check runs) |
| `--create-pr` | Create a pull request for the branch when none is open (implies `--push`) |
| `--pr-draft` | Create new pull requests as drafts (default true; `--pr-draft=false` opens them ready for review) |

Behavior notes:
- **Pausing**: pausing is the SmolVM default in an interactive terminal so you
  can fix and retry; on the AgentENV backend pass `--debug` to open a session.
  Non-interactive runs (pipes, CI, `--detach`) never pause.
- **Local workspace**: the run snapshots the current workspace (uncommitted
  changes included) — the run never depends on what was pushed.
- Local reusable workflows (`uses: ./.github/workflows/…`) are uploaded with
  the submission automatically.
- **Secret preflight**: before submitting, `preloop run` lists the secrets the
  workflow reads through `${{ secrets.NAME }}` and warns about any this engine
  has not stored, with the `preloop secret set` lines to fix them.
  `--strict-secrets` turns that warning into a failure. The check reads the
  engine's own store (global, repository, and each declared `environment:`
  tier) and never GitHub's, whose secret values are write-only. A job that
  passes `secrets: inherit` to a reusable workflow is called out, because its
  callee's names cannot be listed statically. `GITHUB_TOKEN` and the
  `ACTIONS_*` runtime tokens are ignored — the engine mints those per job.
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

## `preloop status [RUN_ID] [--json] [--limit <N>]`

Show active and recent runs (RUN ID, number, status, event, workflow) — or,
with a `RUN_ID`, a single machine-readable status word for scripts.

| Flag | Description |
|---|---|
| `--json` | Print the raw status JSON (no prose) for jq/scripting |
| `--limit <N>` | Number of recent runs to show in the table (default 20) |

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

`--step` counts the steps *declared in the workflow* from 1, matching
`preloop debug --from`. It needs `--job` when a run has more than one job,
because numbering restarts per job.

Numbering comes from the step list in the job's request message, recorded when
the job was dispatched. Steps the runner adds on its own — `Set up job`,
`Pre`/`Post` action hooks, container setup and teardown, `Complete job` — own
their logs and appear in the whole-job output, but never take a `--step`
position. Two steps sharing a `name:` stay distinct, because a step is
identified by its stable id rather than its display name.

Asking for a step fails with `409` rather than guessing when the order cannot
be recovered: a job whose runner uploaded a single merged log has no step
boundaries, and a job dispatched before its step list was recorded has no
declared order to index.

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

## `preloop-runner-client approve <RUN_ID> <JOB_ID> [--note <NOTE>]`

Approve a job waiting on its environment's required-reviewer gate (see
"Environment protection rules" in `self-hosting.md`). Records one approval;
an optional `--note` is stored with the approval for the audit trail.
Requires the server system token; preloop has no user identities, so the
approver is whoever holds the operator credential. For a single-operator
server this is a deliberate confirmation step, not a second human. These
commands live in the separate `preloop-runner-client` binary (`install.sh`
does not ship it); the stable surface is the REST API —
`POST /api/v1/runs/:run_id/jobs/:job_id/approve` and
`POST /api/v1/runs/:run_id/approve-fork`.

## `preloop-runner-client approve-fork <RUN_ID> [--note ...]`

Release a run held by the fork-PR workflow policy (`[fork_policy]
require_approval = true`). The run was created and queued, but every job held
in `Pending` until the operator confirms it. `--note` records an optional
audit note with the approval. A run not approved within 24 hours of entering
the hold fails closed.

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

## `preloop init [OPTIONS]`

One command for first-run setup: it configures GitHub credentials, the golden
image every job VM forks from, and how to run. In a terminal (stdin *and*
stdout are TTYs) it is a four-step wizard; anywhere else it takes the same
answers as flags, never prompts, and exits instead of waiting. Re-running it
reconfigures in place — each step shows the stored answer and only asks again
if you change it.

Steps: **1 credentials** (the `preloop setup github` flow, verified live like
`preloop doctor`), **2 golden**, **3 run mode**, **4 preflight** (free space on
the SmolVM data volume, host architecture, hypervisor) — then the config is
written to `$PRELOOP_CONFIG` and the chosen mode starts.

| Flag | Description |
|---|---|
| `--probe` | Print a side-effect-free capability report as JSON and exit (`arch`, `os`, `disk_free_bytes`, `data_root`, `hypervisor`, `docker`, `dockerfile_detected`, `smolvm`, `existing_config`, `credentials_configured`, `ghcr_reachable`, plus the base image `serve` would use) |
| `--auth <app\|pat\|none>` | Credential step. `none` writes no credential (local-only engine) and leaves existing ones alone |
| `--app-id <ID>`, `--pem-file <PATH>` | Existing App credentials (with `--auth app`); without them the browser flow creates one |
| `--token <TOKEN>` | PAT (with `--auth pat`); falls back to `PRELOOP_GITHUB_PAT` |
| `--org <NAME>`, `--public-url <URL>`, `--port <N>`, `--no-browser`, `--webhook-secret <SECRET>` | As in `preloop setup github` |
| `--repo <OWNER/NAME>` | Repository to verify the credential against (repeatable) |
| `--golden <official\|oci\|dockerfile\|file>` | Golden step. `official` (default) is the packed official GitHub runner golden: drop-in parity with GitHub-hosted runners, **~60 GB on disk** (≈9.6 GB download + the unpacked golden + per-job VM space) |
| `--base-image <REF>` | OCI reference (with `--golden oci`). Resolved anonymously before it is written; pin `@sha256:…` for reproducibility (unpinned only warns). Private registries answer 401/403 — add their credentials with `smolvm config registries edit` and retry; they are used for the pull only and never enter the VM |
| `--dockerfile <PATH>`, `--docker-context <DIR>` | Build a Dockerfile in this repo (with `--golden dockerfile`; defaults `./Dockerfile` and its directory). Offline-builds with `docker`/`nerdctl`, saves the result to a tar under `$PRELOOP_HOME/goldens/`, and uses that tar as the base image. The choice is only offered when a `Dockerfile` exists in the working directory and `docker`/`nerdctl` is on `PATH`; otherwise the wizard shows it disabled with the reason |
| `--path <FILE-OR-DIR>` | Local `.smolmachine` pack or rootfs directory (with `--golden file`) |
| `--mode <foreground\|service\|none>` | Run mode: `serve` here now, install as a systemd/launchd service (needs root), or write the config only |
| `--yes` | Take the suggested answer for confirmation prompts |
| `--json` | One JSON object per step (`{"step","decision","result"}`), ending with the written config path. Implies non-interactive: every step needs its flag |

Exit codes: `2` a required flag is missing or invalid (stderr names it), `3`
preflight failed (the JSON says what is short: disk, architecture, or
hypervisor), `4` resolving/pulling/building the base image failed. Anything
else — a failed credential check, a failed service install — is `1`.

The golden is **recorded, not baked**: `init` writes
`[golden] base_image = "…"` into the config file and `serve` prepares the
golden through its existing path (download the packed official artifact, or
bake a custom base on first run). The one exception is `--golden dockerfile`,
whose `docker build`/`docker save` cannot be deferred to the engine, so `init`
builds it and stores the tar. `PRELOOP_RUNNER_BASE_IMAGE` still overrides the
recorded choice, and `--mode service` writes the config into the service's own
state directory unless `PRELOOP_HOME` names one.

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


## `preloop webhooks <COMMAND>`

Inspect, replay and health-check the durable webhook queue. See
[Webhook resilience](./webhook-resilience.md).

### `preloop webhooks list [OPTIONS]`

| Flag | Description |
|---|---|
| `--state <STATE>` | Only `received`, `processing`, `done` or `failed` deliveries |
| `--limit <N>` | Rows to print (default 50, max 500) |
| `--json` | Print the raw JSON document |

### `preloop webhooks replay <DELIVERY_ID>`

Requeue a delivery from its retained local payload. Retention is 30 days —
ten times GitHub's redelivery window — and the replay needs no GitHub call.
Processing is idempotent: the run is reused and existing check runs are
patched, not duplicated.

### `preloop webhooks health [OPTIONS]`

Queue depth and oldest pending age, delivery-watchdog freshness (loudly
flagged when stale or disabled), open repairs, GitHub circuit-breaker state,
and per-App webhook configuration drift.

| Flag | Description |
|---|---|
| `--json` | Print the raw JSON document |

## `preloop store <COMMAND>`

Control-database operations. They are offline and explicit — they never
bootstrap the engine.

### `preloop store import-legacy [OPTIONS]`

One-time import of a released v11 `preloop.db` (the durable-state store the
control backend replaced) into a fresh control database. See
[Importing a legacy `preloop.db`](./setup.md#importing-a-legacy-preloopdb).

| Flag | Description |
|---|---|
| `--source <LEGACY_DB>` | Legacy `preloop.db` to read (opened read-only, never modified) |
| `--target <CONTROL_DB>` | Control database to create; must not exist |
| `--state-dir <DIR>` | Server state dir the target will live in (secret tiers, log segments). Defaults to the target's directory |
| `--key <FILE>` | Cluster key file (32 bytes). Defaults to `PRELOOP_HMAC_KEY`, then `<state-dir>/hmac-key.bin` |
| `--active <POLICY>` | In-flight work: `refuse` (default), `requeue`, or `cancel` |
| `--json` | Print the machine-readable report |

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
`--workspace <PATH>` selects the workspace whose toolchain version files
(rust-toolchain.toml, .nvmrc, …) are baked into the golden; it defaults to
the current directory.
Set `--storage-gb` or `PRELOOP_RUNNER_STORAGE_GB` for large snapshots; the
default is 80 GiB.
See [VM images and version tracking](vm-images.md#building-a-golden) for the
stock build, custom OCI base, checksum, publishing, and runtime configuration
steps.

For the everyday case — "run my workflows on something other than the official
image" — `preloop init` records the choice instead:

```toml
[golden]
kind = "oci"                                        # official | oci | dockerfile | file
base_image = "ghcr.io/acme/preloop-base@sha256:…"   # read by `serve`/`server install`
dockerfile = "ci/Dockerfile"                        # only for kind = "dockerfile"
```

`serve` reads `base_image` when `PRELOOP_RUNNER_BASE_IMAGE` is unset, and
prepares the golden on its normal path: the packed official artifact for the
stock base, or a local bake for a custom one.

## Environment variables

| Variable | Purpose |
|---|---|
| `PRELOOP_LISTEN` | Default bind address for `serve` |
| `PRELOOP_PUBLIC_URL` | Default public base URL; also used for check-run details links |
| `PRELOOP_STORE_URL` | Default durable-state backend |
| `PRELOOP_GITHUB_PAT` | PAT fallback for `setup github --via pat` |
| `PRELOOP_RELEASE_REPOSITORY` | Release source for `update` |
| `PRELOOP_HOME` | State directory (default `~/.preloop`) |
| `PRELOOP_RUNNER_POOL_ENABLED` | Enable the local microVM runner pool (default off) |
| `PRELOOP_RUNNER_POOL_SIZE` | Pool size (warm forks/VMs) |
| `PRELOOP_RUNNER_CPUS` | vCPUs allocated to each runner VM (default 8) |
| `PRELOOP_USE_FORK` | Run the pool as forked microVMs (default true with a packed golden) |
| `PRELOOP_USE_PACKED_GOLDEN` | Use a release or locally cached packed golden (default on; set `false` for cold OCI provisioning) |
| `PRELOOP_GOLDEN_URL` | Override the packed golden URL; checksum URL is this value plus `.sha256` |
| `PRELOOP_GOLDEN_OCI_REF` | Override the per-architecture packed golden OCI reference; the engine has digest-pinned defaults for both the official arm64 and x86_64 GHCR artifacts |
| `PRELOOP_RUNNER_BASE_IMAGE` | Override the digest-pinned Ubuntu base identity at serve time; set it with `PRELOOP_GOLDEN_URL` for a custom packed golden. Wins over the `[golden] base_image` that `preloop init` records |
| `PRELOOP_VERIFY_BASE_IMAGE` / `PRELOOP_VERIFY_BASE_IMAGE_REPO` | Require a digest-pinned OCI base's GitHub attestation and Cosign signature before `build-golden` |
| `PRELOOP_REQUIRE_BASE_DIGEST` | Reject mutable registry tags during `build-golden` (used by release provenance builds) |
| `PRELOOP_RUNNER_STORAGE_GB` | Persistent guest storage per runner and golden build (default 80 GiB) |
| `PRELOOP_RUNNER_MIN_FREE_DISK_GB` | Free space kept on the VM volume (`PRELOOP_RUNNER_STORAGE_GB`'s volume) before a job VM is forked or created (default 20 GiB; `0` disables). Below it the pool holds the slot and re-measures instead of starting a runner, logging `waiting for disk: …`; jobs wait, they are not failed. A volume that cannot be measured never blocks |
| `PRELOOP_SKIP_DISK_PREFLIGHT` | Proceed past the golden disk check with a warning. Without it, a golden download is refused when the artifact cannot fit on its volume, and a golden build when the SmolVM data volume has less than the builder disk (`PRELOOP_RUNNER_STORAGE_GB`, min 40) + 20 GiB of pack staging free |
| `PRELOOP_RUNNER_PACK_PROXY` | HTTP proxy for smolvm's separate registry export VM during golden packing; standard HTTP(S) proxy variables are fallbacks |
| `PRELOOP_RUNNER_PACK_NO_PROXY` | Proxy bypass list for golden packing; `NO_PROXY` and `no_proxy` are fallbacks |
| `PRELOOP_RUNNER_LABELS` | Extra `runs-on` labels the pool's runners declare |
| `PRELOOP_RUNNER_USER` / `PRELOOP_RUNNER_UID` | Guest runner account (default `runner`/1001); `root` restores root; empty disables switching |
| `PRELOOP_WORKSPACE` | Workspace whose toolchain version files (rust-toolchain.toml, .nvmrc, …) drive golden toolchain baking; overrides the current directory for daemon deployments |
| `PRELOOP_URL` | Server URL for the client commands (default `http://127.0.0.1:9090`) |
| `PRELOOP_SYSTEM_TOKEN` | Native API bearer token (also `PRELOOP_TOKEN`) |
| `PRELOOP_GITHUB_TOKEN` | PAT fallback for GitHub API calls (check runs need the App) |
| `PRELOOP_GITHUB_API_URL` | Override the GitHub API base (tests, GHES) |
| `PRELOOP_GITHUB_SKIP_WORKFLOWS` | Comma-separated GitHub-owned workflow filenames or `.github/workflows/…` paths (for example `release-runner.yml`). Preloop ignores these workflows on webhook dispatch while GitHub Actions still runs them; set on the engine and restart it |
| `PRELOOP_WEBHOOK_SECRET` | Webhook signature secret for the default App (Apps in `github.apps` may carry their own) |

### Engine token storage

The native API administrator token is generated on first engine startup when
`PRELOOP_SYSTEM_TOKEN` is not set. Preloop stores it in the operating system's
credential store, scoped to the engine home. On hosts without an available or
readable OS credential service, it uses `$PRELOOP_HOME/engine.token` with private
permissions instead. Managed CLI commands read this token automatically; do
not commit or print the file. Set `PRELOOP_SYSTEM_TOKEN` explicitly for a
separate client or service.

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
