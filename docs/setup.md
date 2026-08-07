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

- **`doctor` says the App has no installation for a repo** — install the app
  on that account/repo, or check the installation's repository selection.
- **`GITHUB_TOKEN` 403s in a job** — the installation may not grant the
  workflow's requested permissions. The engine logs which permissions are
  ungranted; grant them on the installation page.
- **A secret reads empty in a run** — check `preloop secret list` (was it
  scoped to another repo?) and whether the event was trusted (fork PRs get
  no stored secrets).
- **Mint failure policy** — `mint_failure` decides what happens when App
  minting fails: `local` (fall back to the local JWT), `error` (fail the
  job), `pat` (fall back to the PAT).

[smolvm]: https://github.com/preloopdev/smolvm
