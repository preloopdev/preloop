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

- macOS (Apple Silicon), Windows or Linux, 64-bit
- [smolvm] for the default VM runner pool (`preloop runner` works without it)
- A GitHub account for credentials (see below)

### Windows

Windows is supported **via WSL2** for now — native Windows support is coming
(the Windows binaries and the WHP-backed VM backend already exist in the
pipeline). Inside WSL2, everything works like Linux:

```sh
curl -fsSL https://raw.githubusercontent.com/preloopdev/preloop/main/install.sh | sh
```

- For the full microVM runner pool, enable nested virtualization in
  `.wslconfig` (`[wsl2] nestedVirtualization=true`) so `/dev/kvm` is exposed.
- Without it, `preloop runner` still works (jobs run as WSL processes); only the VM pool needs KVM.

## Quick start

```sh
preloop serve            # engine on 127.0.0.1:9090
cd my-repo
preloop run -f .github/workflows/ci.yml --event push
```

`preloop run` snapshots the local workspace (dirty changes included) so a run
never depends on what is pushed to GitHub.

Running it as an always-on server for a team — service install, every runtime
knob, and how to expose it (tunnel, funnel, or your own domain) — is covered in
[self-hosting.md](self-hosting.md).

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

Decide first whether you need webhooks:

- Just running CI that talks to GitHub (checkout, `gh`, `GITHUB_TOKEN`,
  API steps): create the App and stop there. No public address is needed;
  you start runs yourself with `preloop run`.
- Check runs on GitHub (the checks on commits and pull requests): you also
  need webhooks, and webhooks need a publicly accessible HTTP address for
  the engine. This applies to laptops too, not just servers: a tunnel from
  your machine is enough.

The App and its webhook are one object; the webhook only adds GitHub's
ability to call you. You can enable it later with `--public-url`, so
starting without it is never a dead end.

### Option A — GitHub App

GitHub has no API for creating an App, but it does accept a *manifest*: a
form POST that pre-fills the creation page. `preloop setup github --via app`
uses it, so the only manual step is clicking **Create on GitHub**.

```sh
preloop setup github --via app
```

The command binds a single-use listener on loopback, opens it in your
browser, and uses it as the manifest's redirect target. You click **Create on
GitHub**; GitHub redirects back with a one-time code, and the CLI converts it
into the App id, private key, and webhook secret and writes them to
`~/.preloop/config.toml` (mode 0600). The browser then lands on the
installation page — pick the repositories you run, and the CLI reports the
installation id and exits.

The private key never leaves the machine: the redirect target is
`127.0.0.1`, not a hosted page.

| Flag | Effect |
|---|---|
| `--org NAME` | Create the App under an organization instead of your account. |
| `--public-url URL` | Also enable webhook delivery to that URL. Omitted, the App is created with webhooks off — GitHub cannot reach `localhost`. |
| `--app-name NAME` | App name (GitHub requires global uniqueness). Default `preloop-local`. |
| `--port N` | Pin the loopback port instead of taking a free one. |
| `--no-browser` | Print the URL instead of opening a browser (headless/SSH). |

#### What "webhooks off" means

Without `--public-url` the App is created with its webhook inactive, because
GitHub cannot reach `127.0.0.1`. That only removes GitHub's ability to *call
you*; everything outbound still works:

| | webhooks off (default) | `--public-url` |
|---|---|---|
| What starts a run | you do: `preloop run`, `just submit-ci` | a `push`/`pull_request` on GitHub |
| Private-repo checkout, `gh`, API steps | works — the App mints a token per job | same |
| Check runs on the commit | published (outbound to GitHub) | same |

So a laptop setup is a complete CI system you trigger yourself. When you later
get a reachable address — a tunnel is enough — point the App you already have
at it:

```sh
cloudflared tunnel --url http://127.0.0.1:9090      # → https://xxx.trycloudflare.com
preloop setup github --via app --public-url https://xxx.trycloudflare.com
```

For anything persistent, prefer a named tunnel: the `trycloudflare.com`
address above changes every restart, which would leave the webhook URL
pointing at a dead address. A named tunnel keeps a stable hostname:

```sh
cloudflared tunnel create preloop
cloudflared tunnel route dns preloop ci.example.com
cloudflared tunnel --url http://127.0.0.1:9090 run preloop
```

Point the webhook at the stable hostname once:

```sh
preloop setup github --via app --public-url https://ci.example.com
```

That updates the existing App's webhook URL and secret through GitHub's API
(`PATCH /app/hook/config`) instead of creating a second App, and stores the
secret so deliveries verify. GitHub exposes no API for the webhook **Active**
checkbox, so an App created without `--public-url` needs that ticked once in
its settings; the command says so.

Already have an App — or your org blocks manifest creation? Create it by hand
at <https://github.com/settings/apps/new> (name it, leave webhooks off,
download the PEM), install it on the accounts whose repos you run at
<https://github.com/apps/YOUR-APP/installations/new>, then:

```sh
preloop setup github --via app --app-id 123456 --pem-file app.pem
preloop doctor --repo owner/repo
```

Either way the engine mints a fresh installation token per job — no
long-lived secret sits in the config.

### Option B — fine-grained PAT

```sh
preloop setup github --via pat --token github_pat_… --repo owner/repo
```

or without `--token` (prompted, hidden input):

```sh
preloop setup github --via pat --repo owner/repo
```

Unlike Apps, PATs have no manifest flow — GitHub exposes no API for creating
one — so this path opens the creation page and waits at a hidden prompt.
`--no-browser` skips the opening; piping the token in (or setting
`PRELOOP_GITHUB_PAT`) skips the prompt entirely, so automation is unaffected.

Two things a PAT does not get you:

- **Check runs.** GitHub's checks API only accepts App installation tokens, so
  a PAT-configured engine records check runs locally instead of publishing
  them to the commit. Jobs still get the PAT as `GITHUB_TOKEN`, so checkout,
  `gh`, and API steps work normally.
- **A webhook secret.** The App flow receives one from GitHub; here you create
  the webhook yourself (repository → Settings → Webhooks, pointed at
  `<public-url>/api/v1/github/webhooks`) and store its secret:

  ```sh
  preloop setup github --via pat --webhook-secret "$(openssl rand -hex 32)"
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
webhook_secret = "…"        # written by `setup github --via app`

[secrets]
DOCKERHUB_TOKEN = "…"

[repo_secrets."owner/repo"]
AWS_CREDS = "…"
```

Environment variables override the file per field (`PRELOOP_GITHUB_APP_ID`,
`PRELOOP_GITHUB_APP_PEM`, `PRELOOP_GITHUB_PAT`, `PRELOOP_WEBHOOK_SECRET`, …) —
the file is the durable store, env vars are the escape hatch for containers.
GitHub credential changes are picked up on engine restart; secrets changes
apply live.

## doctor

`preloop doctor [--repo owner/repo …]` verifies each configured credential:
it mints an App token (or uses the PAT) and probes the repository for
contents/pull-requests/actions/issues read. Run it after setup and any time a
job's `GITHUB_TOKEN` misbehaves.

## One engine, one user (for now)

An engine is built for a single operator. Nothing stops several people from
pointing `PRELOOP_URL` at the same server, but the server does not yet model
*who* submitted a run, so treat a shared engine as unsupported:

- **One identity.** Every caller authenticates with the same native token, so
  the server cannot tell two people apart. Anyone who can reach the API can
  read every run's logs and secrets-bearing job messages.
- **`preloop push` defaults to the server's most recent run**, not to yours.
  On a shared engine that may be a colleague's run — publishing their commit
  and opening their pull request under *your* git credentials. Pass an
  explicit run id (`preloop push <run_id>`) if you share an engine anyway.
- **One credential set.** The configured App or PAT is used for every run, so
  check runs and pull requests are always attributed to that identity rather
  than to the person who submitted.

Give each person their own engine until per-user identity lands.

## Durable state (SQLite by default, Postgres optional)

Run history, queued jobs, runners, sessions, and logs survive restarts. The
default backend is **SQLite** at `<state dir>/preloop.db` — zero configuration,
correct for a single machine, and the right choice unless you have a reason
to move off it.

To use **Postgres**, point the engine at a database with `--store` or
`PRELOOP_STORE_URL`:

```sh
preloop serve --store 'postgres://user:password@host:5432/preloop?sslmode=require'
# or, for systemd deployments:
# Environment=PRELOOP_STORE_URL=postgres://…?sslmode=require
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

[smolvm]: https://github.com/smol-machines/smolvm
