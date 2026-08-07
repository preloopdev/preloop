# Setup guide

This page is the "what to tell users" companion to the `preloop setup github`
wizard. It covers installing the engine, connecting it to GitHub, and storing
secrets.

## What preloop is

`preloop` is a local GitHub Actions control plane. The engine (`preloop
serve`) accepts workflows the same way GitHub does — `${{ }}` expressions,
matrix builds, reusable workflows, concurrency groups, OIDC — and executes
them on local machines (smolvm microVMs by default). Your `.github/workflows`
run unmodified.

## Requirements

- macOS (Apple Silicon) or Linux, 64-bit
- [smolvm] for the default VM runner pool (`preloop runner` works without it)
- A GitHub account for credentials (see below)

## Quick start

```sh
preloop serve            # engine on 127.0.0.1:9090
cd my-repo
preloop run -f .github/workflows/ci.yml --event push
```

`preloop run` snapshots the local workspace (dirty changes included) so a run
never depends on what is pushed to GitHub.

## Connecting GitHub credentials

Workflows reference GitHub — `${{ github.repository }}`, `GITHUB_TOKEN`,
`secrets.*` — so the engine needs a credential. Two kinds are supported:

| | GitHub App (recommended) | Fine-grained PAT |
|---|---|---|
| Token scope | Per-installation, narrows to the repos you pick | Repo/org scoped, expires on a schedule |
| Token shape | `ghs_…` (installation), minted by the engine | `github_pat_…` |
| Setup effort | Create app + install once | Generate once, rotate when it expires |
| Best for | Teams, servers, anything long-running | Personal machines, quick starts |

Classic (`ghp_…`) and OAuth (`gho_…`) tokens **work but are warned against**:
they carry every scope the account has. The wizard refuses nothing but tells
you what you are doing.

### Option A — GitHub App

App creation is a browser-only step (GitHub has no API for creating user
apps):

1. Create the app at <https://github.com/settings/apps/new>. You only need a
   name; leave webhooks off. Note the **App ID** and download the **private
   key** (PEM).
2. Install the app on the account(s) whose repos you run:
   <https://github.com/apps/YOUR-APP/installations/new>. Grant it the repos
   you want to run — the engine cannot mint tokens for repos outside the
   installation.
3. Configure the engine:

   ```sh
   preloop setup github --via app --app-id 123456 --pem-file app.pem
   preloop doctor --repo owner/repo
   ```

   The engine mints a fresh installation token per job — no long-lived
   secret sits in the config.

### Option B — fine-grained PAT

```sh
preloop setup github --via pat --token github_pat_… --repo owner/repo
```

or without `--token` (prompted, hidden input):

```sh
preloop setup github --via pat --repo owner/repo
```

Create the PAT at <https://github.com/settings/personal-access-tokens/new>
with **Repository access → Only select repositories** and the permissions the
wizard prints. The checklist is derived from your own workflows' `permissions`
blocks (union across `.github/workflows/*.yml`), so you can grant exactly what
your pipelines use.

> **`id-token:` is not a PAT permission.** The engine is itself the OIDC
> issuer for local runs (`${{ steps.oidc.outputs.jwt }}` is signed by the
> engine), so workflows declaring `id-token: write` need no GitHub-side
> permission for it.

For orgs that gate app installations, a fine-grained PAT scoped to the org's
repos is the supported fallback.

## Secrets

`preloop secret` mirrors GitHub's secret model: a **global tier** (like
org-level secrets, injected into every trusted job) and a **per-repository
tier** (like repo secrets, injected only into that repository's jobs).
Per-repo secrets override the global tier per name; values a submission
passes explicitly win over both.

```sh
preloop secret set DOCKERHUB_TOKEN                     # prompts, hidden
preloop secret set AWS_CREDS --repo owner/repo --value …
preloop secret list                                   # names only, never values
preloop secret list --repo owner/repo
preloop secret rm DOCKERHUB_TOKEN
preloop secret rm AWS_CREDS --repo owner/repo
```

Names must be `UPPER_SNAKE`; values are masked in logs exactly like GitHub
(`***`).

Secrets apply **live**: when an engine is running, `set`/`rm` go through the
engine API and affect the very next submitted run. With no engine running
they are written to the config file and apply on next start.

Workflows read them the usual way:

```yaml
steps:
  - run: echo "$DOCKERHUB_TOKEN" | docker login -u user --password-stdin
    env:
      DOCKERHUB_TOKEN: ${{ secrets.DOCKERHUB_TOKEN }}
```

Trust: submissions from untrusted events (fork PRs via the webhook path) do
not receive stored secrets; native `preloop run` submissions always do.

## Config file

Everything lives in `~/.preloop/config.toml` (mode 0600; `PRELOOP_CONFIG`
overrides the path). Fields:

```toml
[github]
app_id = "123456"
app_pem = "-----BEGIN RSA PRIVATE KEY-----…"
mint_failure = "pat"        # "local" | "error" | "pat"
pat = "github_pat_…"

[secrets]
DOCKERHUB_TOKEN = "…"

[repo_secrets."owner/repo"]
AWS_CREDS = "…"
```

Environment variables override the file per field (`AKSH_GITHUB_APP_ID`,
`AKSH_GITHUB_APP_PEM`, `AKSH_GITHUB_PAT`, …) — the file is the durable store,
env vars are the escape hatch for containers. GitHub credential changes are
picked up on engine restart; secrets changes apply live.

## doctor

`preloop doctor [--repo owner/repo …]` verifies each configured credential:
it mints an App token (or uses the PAT) and probes the repository for
contents/pull-requests/actions/issues read. Run it after setup and any time a
job's `GITHUB_TOKEN` misbehaves.

## Durable state (SQLite by default, Postgres optional)

Run history, queued jobs, runners, sessions, and logs survive restarts. The
default backend is **SQLite** at `<state dir>/aksh.db` — zero configuration,
correct for a single machine, and the right choice unless you have a reason
to move off it.

To use **Postgres**, point the engine at a database with `--store` or
`AKSH_STORE_URL`:

```sh
preloop serve --store 'postgres://user:password@host:5432/aksh?sslmode=require'
# or, for systemd deployments:
# Environment=AKSH_STORE_URL=postgres://…?sslmode=require
```

- **`sqlite://<path>`**, a bare path, or nothing = SQLite (default).
- **`postgres://…`** = the Postgres backend. The schema (tables, sealed-blob
  payloads, migrations) mirrors SQLite exactly; the engine keeps a single
  writer connection, so the database must not be shared with a second engine
  process.
- **TLS**: add `?sslmode=require` (or `verify-ca` / `verify-full`) for remote
  databases — managed Postgres (Neon, RDS, Supabase, …) typically requires
  it. Verification always uses the system root store. Plaintext is the
  default for loopback databases.

Run Postgres however you like — a managed service, a `postgres` container on
the same host, or an OS package. The engine does not bundle or spawn a
database server; SQLite is the embedded option, Postgres is an external
dependency you point at.

## Troubleshooting

First stop for any failure: the engine log (`~/.preloop/engine.log`), and
`preloop doctor --repo owner/repo` for credential problems.

### Engine won't start

- **`preloop run` / `preloop serve` exits with "engine exited before
  becoming ready: exit status: 1"** — read `~/.preloop/engine.log` for the
  real error. The two common causes below produce exactly this symptom.
- **`Error: Address already in use (os error 48)` on the unix socket
  (`~/.preloop/preloop.sock`)** — a stale socket file or an orphaned engine
  from a crashed session. Find and kill the holder, then remove the file:
  `lsof ~/.preloop/preloop.sock`; kill it; `rm -f ~/.preloop/preloop.sock`.
- **`Address already in use` on TCP :9090** — another `preloop serve`/`engine`
  is already listening (or a leftover process from a previous session).
  `lsof -nP -iTCP:9090 -sTCP:LISTEN` to see who, kill the stale one, or run
  the new engine on another port with `PRELOOP_LISTEN=127.0.0.1:9091`.
- **`server returned 404: /api/v1/runs not available on this endpoint`** —
  the CLI is talking to the wrong server: `AKSH_URL` unset defaults to
  `http://127.0.0.1:9090`, and an older/different engine may be squatting
  there. Point `AKSH_URL` at your engine, or clear the stale engine.
- **`missing or invalid native API token`** — the client's token
  (`~/.preloop/engine.token`) does not match the engine's
  `AKSH_SYSTEM_TOKEN`. Restart the engine and client together, or set
  `AKSH_SYSTEM_TOKEN` to the value in `~/.preloop/engine.token`.
- **`Error: connection refused` when submitting** — no engine is listening on
  `AKSH_URL`; start one (`preloop serve`) or fix the URL.
- **`local runner provisioning unavailable … Linux runner bundle
  unavailable; run just build-preloop`** — the Linux guest runner binary is
  missing; build it with `just build-preloop` (the control plane stays up,
  but no VM jobs can start).

### Runner / VM issues

- **Job queued forever, nothing claims it** — no runner is registered for the
  job's `runs-on` labels, or the pool can't provision a VM (see the engine
  log for the slot error). Check `preloop runner list` and the label set.
- **`smolvm exec failed with exit code 1: setpriv: mutually exclusive
  arguments`** — an old engine binary with the pre-fix `as_runner_user`
  wrapper. Rebuild the engine (`cargo build -p preloop-cli`) and restart it.
- **`crane manifest failed … lookup index.docker.io on 100.96.0.1:53: i/o
  timeout` (or any in-guest DNS timeout)** — the VM's gateway forwards DNS to
  a broken host resolver (Tailscale is a common culprit). Run the engine with
  `PRELOOP_RUNNER_DNS=8.8.8.8` so the gateway uses a public resolver
  directly.
- **`TOOMANYREQUESTS: unauthenticated pull rate limit` from
  index.docker.io** — Docker Hub's anonymous rate limit on the egress IP.
  Authenticate: `docker login` on the host (the guest's `crane` reads the
  host's `~/.docker`), or use a packed golden / preloaded image so no pull is
  needed.
- **`530 / Argo Tunnel error 1033` from inside VMs, or
  `actions/upload-artifact` timeouts** — the guests reach the engine through
  the tunnel; a tunnel outage breaks local CI too. Restore `cloudflared`, or
  give guests a host-reachable URL (see
  [github-app-webhook.md](github-app-webhook.md) §8.2).
- **A workflow pins `macos-13`/`windows-*` and gets the wrong host** — image
  versions are not disambiguated yet: any `macos-*` label matches whatever
  Mac is registered. Track [issue #44] for platform-specific routing.

### Credentials & tokens

- **`doctor` says the App has no installation for a repo** — install the app
  on that account/repo, or check the installation's repository selection.
- **`GITHUB_TOKEN` 403s in a job** — the installation may not grant the
  workflow's requested permissions. The engine logs which permissions are
  ungranted; grant them on the installation page. Requesting a scope the App
  was never granted fails the mint outright (a `422`), by design — narrow
  the workflow's `permissions:` or grant the scope.
- **A job's `GITHUB_TOKEN` is the local JWT instead of a GitHub token** —
  App minting failed and `mint_failure` defaulted to `local`. Check the
  engine log for why (App not installed on that owner, expired key, PAT-only
  configuration). Set `AKSH_GITHUB_APP_MINT_FAILURE=error` to make
  misconfiguration loud (`502` at submit), or `pat` to fall back to the
  static PAT deliberately. See [github-tokens.md](github-tokens.md).
- **PAT-authenticated jobs fail with `401`/`403` at api.github.com** — the
  PAT expired or lacks a permission. Rotate it:
  `preloop setup github --via pat --token github_pat_… --repo owner/repo`.
- **`parsed PEM …` hard startup error** — a configured App PEM was invalid,
  or `AKSH_GITHUB_APP_MINT_FAILURE` had an unrecognised value; both are fatal
  at startup on purpose. Fix the config, restart.

### Workflow & event issues

- **Submit fails with `workflow does not match event pull_request`** —
  pull_request events need an activity type: the simulated payload must
  include `"action": "opened"` (or `synchronize`/`reopened`), e.g.
  `preloop run --event pull_request --payload pr.json`. The same applies to
  webhook deliveries missing the action.
- **`${{ steps.*.outputs.* }}` is empty on local runs** — the parser
  pre-evaluates expressions at submit time, before step outputs exist. Known
  gap on aksh (works on GitHub). See [issue #88].
- **A job that should run is `skipped` at submit** — non-Linux cells
  (macOS/Windows) are skipped when no runner declares the OS, a deliberate
  divergence from GitHub's queue-and-wait.
- **`system.*` / `DistributedTask.*` variables visible in steps** — a known
  divergence (GitHub exports only the step's own `env:`); harmless so far.

### Webhooks

- **`401 Unauthorized` on webhook deliveries** — `X-Hub-Signature-256`
  verification failed: the secret in GitHub's hook does not match
  `AKSH_WEBHOOK_SECRET` on the server.
- **Runs never created for push/PR events** — check the hook is pointed at
  the tunnel/`public_url` (not a LAN IP — the egress floor blackholes them),
  the tunnel is up (see Error 1033 above), and the event has the
  `action`/`ref` fields the workflow filters on.
- **No check runs on commits/PRs** — check runs require a GitHub App; a
  repository-level webhook delivers runs but cannot create check marks (see
  [github-webhooks.md](github-webhooks.md)).

### Secrets

- **A secret reads empty in a run** — check `preloop secret list` (was it
  scoped to another repo?) and whether the event was trusted (fork PRs get
  no stored secrets). Values are masked as `***` in logs by design.
- **`preloop secret set` says the name is invalid** — names must be
  `UPPER_SNAKE`.

### Store / database

- **Engine log warns about store failures but keeps running** — the store is
  best-effort on some surfaces (`delete_session` warns) and fatal on others
  (`register_runner`/`create_session` return 500). Check the SQLite file
  permissions/disk space, or the Postgres connection.
- **Postgres: credentials travel in plaintext** — any `postgres://` URL
  without explicit `sslmode` connects unencrypted with no warning. Add
  `?sslmode=require` (or `verify-ca`/`verify-full`) for remote databases.
- **Two engines sharing one Postgres database** — unsupported: the engine
  keeps a single writer connection; point each engine at its own database.
- **`conform: FAIL` when running `just test-ci`** — protocol drift against
  the official-runner goldens. See [conformance.md](conformance.md) — it is
  the compatibility contract and the index for what must not drift.

### Still stuck?

- Check `~/.preloop/engine.log` first — it carries the actual error behind
  most one-line CLI failures.
- `preloop doctor --repo owner/repo` re-verifies every credential.
- For official-runner E2E quirks (port stripping), see
  [CONTRIBUTING.md](../CONTRIBUTING.md) — `USE_DEV_ACTIONS_SERVICE_URL`
  keeps non-default ports instead of the old port-80 redirect.

[issue #44]: https://github.com/preloopdev/preloop/issues/44
[issue #88]: https://github.com/preloopdev/preloop/issues/88

[smolvm]: https://github.com/preloopdev/smolvm
