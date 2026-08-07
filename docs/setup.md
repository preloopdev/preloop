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

### Where secrets live

By default stored secrets persist in the config file (`[secrets]`, mode
0600). For deployments that must not hold plaintext at rest, two options:

- **Memory-only store** — set `secrets_store = "memory"` in the config file
  (or `PRELOOP_SECRETS_STORE=memory`): the live secrets API keeps values in
  engine memory for the process lifetime and never writes them to the file.
  `preloop secret set` then requires a running engine; after a restart you
  re-seed. Combine with the systemd credential below for a durable base set.
- **systemd credential** — install the service with
  `--systemd-credential /etc/preloop-secrets.enc` to mount an encrypted
  credential (`LoadCredentialEncrypted=preloop-secrets`); the engine reads
  `[secrets]`/`[repo_secrets]` from it at startup, overriding the config
  file per name. Create the blob with:

  ```sh
  systemd-creds encrypt --name=preloop-secrets secrets.toml /etc/preloop-secrets.enc
  ```

  At rest the blob is encrypted and bound to the host (TPM or machine key);
  systemd decrypts it into an in-memory file (memfd) the service never
  writes back. Secrets already in the config file still load and apply —
  the credential wins per name. Note: secrets backed by the credential are
  re-applied on every engine restart — `preloop secret rm` removes them from
  the running store, but to make the removal permanent, edit the credential
  file (or the config file) itself.

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
## Running as a service

For a team server that must survive reboots and restarts, install the engine
as a supervised service instead of running `preloop serve` by hand:

```sh
sudo preloop server install \
    --public-url https://ci.example.com \
    --github-app-id 123456 \
    --github-app-key /etc/preloop/app.pem \
    --webhook-secret '…'
```

What it does:

- **Linux (systemd)** — writes hardened units to
  `/etc/systemd/system/preloop.{service,socket}` plus a self-update timer
  (`preloop-update.{service,timer}`, hourly, polls GitHub Releases). The
  control plane is socket-activated on the port of `--listen` (default 9090).
- **macOS (launchd)** — writes a LaunchDaemon plist to
  `/Library/LaunchDaemons/dev.preloop.server.plist` (mode 0600).
- `--systemd-credential PATH` (Linux) — mounts an encrypted systemd
  credential (`LoadCredentialEncrypted=preloop-secrets:PATH`) so stored secrets come
  from an encrypted, host-bound blob instead of the config file; see
  "Where secrets live" in the Secrets section.
- Configuration is written to a mode-0600 environment file
  (`/var/lib/preloop/environment` on Linux) — the webhook secret never lands
  in a world-readable unit.
- State lives in `/var/lib/preloop` (mode 0700; `--home` overrides).

The `--github-app-*` / `--webhook-secret` flags are optional at install time,
but **you must define these secrets** for the service to be useful: without a
webhook secret the engine rejects every GitHub webhook delivery, and without
an App key it cannot mint `GITHUB_TOKEN`. They land in the mode-0600
environment file (never in the world-readable units); alternatively install
first, then configure credentials with
`PRELOOP_HOME=/var/lib/preloop preloop setup github --save` (writes the
mode-0600 `config.toml` — see above). `preloop server install --dry-run`
prints the full plan without touching the system, and
`sudo preloop server uninstall` removes the units while keeping
`/var/lib/preloop` data; pass `--purge-data` to delete it. Manual copies of
the units live in `contrib/systemd/`.

### Rootless option: `--user`

Don't have (or don't want) root on the box? Install a per-user service
instead — **no sudo required**, and state defaults to `~/.preloop` instead of
`/var/lib/preloop`:

```sh
preloop server install --user --public-url https://ci.example.com
```

- **Linux** — systemd *user* units in `~/.config/systemd/user/`, managed with
  `systemctl --user` (the self-update timer works the same). They stop when
  you log out; `sudo loginctl enable-linger $USER` keeps them running.
- **macOS** — a LaunchAgent at `~/Library/LaunchAgents/`, loaded into your
  GUI session. LaunchAgents only run while you're logged in.
- Everything else is identical: same flags, same 0600 config file, same
  `--dry-run`, and `preloop server uninstall --user` to remove it.

System scope is still the right default for a team server (runs before login,
accepts webhooks unattended); `--user` fits personal machines and dev boxes.

### Exposing the engine to GitHub

`--public-url` is the address GitHub uses to deliver webhooks and link check
runs. Without it (or with a loopback default), GitHub can't reach the engine —
the service runs, but nothing ever triggers. Two ways to make it reachable:

**Production — a domain.** You should definitely point a DNS record at the host and terminate TLS
in front of the engine: a Caddy/nginx reverse proxy to `127.0.0.1:9090 and do a bunch of other security hardening stuff on your server. (or
bind `0.0.0.0:9090` behind your own TLS). Register the App's webhook as
`https://ci.example.com/api/v1/github/webhooks` (gated by the webhook secret)
and install with:

```sh
sudo preloop server install \
    --public-url https://ci.example.com \
    --github-app-id 123456 --github-app-key /etc/preloop/app.pem \
    --webhook-secret '…'
```

**Trying it out — a tunnel.** No DNS record or inbound port needed:

```sh
cloudflared tunnel --url http://127.0.0.1:9090   # quick tunnel → https://xxx.trycloudflare.com
ngrok http 9090
tailscale funnel 9090
```

Re-run `preloop server install` with the tunnel URL as `--public-url` (or set
`PRELOOP_PUBLIC_URL` in the service environment file and restart). Note that a
quick tunnel's URL changes on every restart — for anything long-lived, use a
named Cloudflare tunnel or the domain path above.

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
