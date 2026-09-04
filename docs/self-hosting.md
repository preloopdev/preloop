# Self-hosting Preloop

`preloop serve` is the whole server: the control plane (runner protocol, native
API, GitHub Checks reporting) plus the microVM runner pool that executes jobs.
One binary, one process.

This guide covers what has to be reachable, how to install it as a service,
every runtime knob, four ways to expose it — including one with no third party
involved — and how to operate it.

---

## 1. What actually needs to be reachable

This decision drives everything else, and the answer is narrower than it looks:

| Traffic | Direction | Needs a public address? |
|---|---|---|
| Job execution (pool VMs ↔ control plane) | in-host, over the mounted control socket and an in-guest loopback bridge | **No** |
| Native API (`/api/v1/*`, the `preloop` CLI) | inbound, authenticated | No — loopback or a private network is enough |
| Reporting check runs, minting tokens, pushing | **outbound** to GitHub | No |
| GitHub webhook delivery | **inbound** from GitHub | **Yes** — the only thing that forces it |
| `details_url` links on check runs | inbound, from a browser | Only for whoever clicks them |

Jobs never traverse your public endpoint. `PRELOOP_RUNNER_URL` is pinned to the
loopback listen address at startup, and in-VM runners reach the control plane
through the mounted control socket plus a loopback bridge inside the guest. So
if you can live without webhook-triggered runs, you can run with **no inbound
access at all** — see option A.

---

## 2. Requirements

- **Host**: Linux with KVM (x86_64 or arm64). macOS on Apple Silicon works for
  local development and runs x86_64 goldens through Rosetta 2 translation
  (enabled automatically per VM).
- **Memory**: `PRELOOP_RUNNER_POOL_SIZE × PRELOOP_RUNNER_MEMORY_MIB`, plus a few
  GB for the control plane. Ceilings are ballooned, so idle runners hold far
  less than their ceiling — but concurrent heavy builds really do use it.
- **CPU**: each runner VM gets `PRELOOP_RUNNER_CPUS` vCPUs (default 4).
  `pool_size × PRELOOP_RUNNER_CPUS` well above your core count means jobs
  contend and wall-clock-sensitive tests turn flaky.
- **Disk**: golden images (roughly 0.7–6 GB each, and superseded ones
  accumulate) plus the cache directory, which grows with every distinct key.
- **GitHub App**: app id, private key PEM, installation id, and a webhook secret
  if you use webhooks. A GitHub App — not a PAT — is required to report check
  runs; GitHub rejects PAT-authored check runs.

---

## 3. Install

For a Linux system service, install the CLI and SmolVM runtime in system
locations. A user-local binary under `/home` cannot be traversed by the
dedicated service account, and a user-local runtime is not visible to it:

```sh
sudo -H env PREFIX=/usr/local sh -c \
  'curl -fsSL https://raw.githubusercontent.com/preloopdev/preloop/main/install.sh | sh'
sudo /usr/local/bin/preloop setup github --via app --public-url https://ci.example.com
```

Then install it as a supervised service — systemd on Linux, launchd on macOS:

```sh
sudo /usr/local/bin/preloop server install \
  --public-url https://ci.example.com \
  --github-app-id 123456 \
  --github-app-key /etc/preloop/app.pem \
  --github-app-installation-id 7654321 \
  --webhook-secret "$WEBHOOK_SECRET" \
  --home /var/lib/preloop
```

Useful flags:

| Flag | Effect |
|---|---|
| `--dry-run` | Print every file and command without touching the system |
| `--user` | Per-user service (systemd user unit / LaunchAgent), no root, state in `~/.preloop`. On Linux pair with `sudo loginctl enable-linger $USER` so it survives logout |
| `--systemd-credential PATH` | Mount an encrypted secrets file via `LoadCredentialEncrypted`; create it with `systemd-creds encrypt --name=preloop-secrets secrets.toml PATH` |
| `--no-update-timer` | Skip the systemd self-update timer |
| `--listen ADDR` | Bind address; on Linux the port is published through socket activation |

`preloop server uninstall` removes the units and config, keeping `PRELOOP_HOME`
unless you pass `--purge-data`.

To run it in the foreground instead:

```sh
preloop serve --listen 127.0.0.1:9090 --public-url https://ci.example.com
```

---

## 4. Runtime knobs

All configuration is environment variables; CLI flags override them.

### Core

| Variable | Default | Meaning |
|---|---|---|
| `PRELOOP_LISTEN` | `127.0.0.1:9090` | Bind address. Loopback by default — expose with `--listen 0.0.0.0:9090` behind a proxy or tunnel (see §6). |
| `PRELOOP_PUBLIC_URL` | `http://127.0.0.1:<port>` | Externally reachable base URL. Used for `details_url` on check runs |
| `PRELOOP_HOME` | `$HOME/.preloop` | State directory (database, blobs, cache, credentials) |
| `PRELOOP_STORE_URL` | SQLite in the state dir | `sqlite://<path>`, a bare path, or `postgres://…?sslmode=require\|verify-full` |
| `PRELOOP_UNIX_SOCKET` | — | Control socket path; mounted into runner VMs, serves the runner surface only |
| `PRELOOP_SYSTEM_TOKEN` | generated and stored in the OS credential store; private `$PRELOOP_HOME/engine.token` fallback | Admin credential for `/api/v1/*`. Treat it as root for the control plane |
| `PRELOOP_TOKEN_TTL_SECS` | `2999` | Issued runner token lifetime |
| `PRELOOP_CONFIG` | `$PRELOOP_HOME/config.toml` | Config file path |
| `PRELOOP_SECRETS_STORE` | config file | Secrets backend selector |
| `PRELOOP_RUNNER_URL` | loopback listen address | Origin handed to runners. Set automatically; override only for remote runners |
| `PRELOOP_CONTROL_UPSTREAM` | — | LAN address remote runners use when loopback is not reachable |

Managed engines read their generated token from the OS credential store (or
`$PRELOOP_HOME/engine.token` when that store is unavailable or unreadable). For
a separate client or service, set `PRELOOP_SYSTEM_TOKEN` explicitly; never print
or commit the fallback file.

### GitHub

| Variable | Meaning |
|---|---|
| `PRELOOP_GITHUB_APP_ID` | App id |
| `PRELOOP_GITHUB_APP_PEM` / `_PEM_FILE` / `_PRIVATE_KEY` / `_PRIVATE_KEY_PATH` | Private key, inline or by path |
| `PRELOOP_GITHUB_APP_INSTALLATION_ID` | Installation id; skips discovery |
| `PRELOOP_WEBHOOK_SECRET` | Verifies `X-Hub-Signature-256` |
| `PRELOOP_GITHUB_TOKEN` | PAT fallback; also fetches **private remote reusable workflows** |
| `PRELOOP_GITHUB_APP_MINT_FAILURE` | Policy when installation-token minting fails |
| `PRELOOP_GITHUB_SERVER_URL` / `_API_URL` / `_GRAPHQL_URL` | Point at GitHub Enterprise Server |
| `PRELOOP_GITHUB_REPOSITORY` | Repository the scheduler scans for `schedule:` workflows at startup |

### Runner pool

| Variable | Default | Meaning |
|---|---|---|
| `PRELOOP_RUNNER_POOL_ENABLED` | off | Master switch for the microVM pool |
| `PRELOOP_RUNNER_POOL_SIZE` | derived from host CPU/RAM | Warm machines; `0` forks on demand |
| `PRELOOP_RUNNER_CPUS` | `4` | vCPUs allocated to each runner VM |
| `PRELOOP_RUNNER_MEMORY_MIB` | `4096` | Memory ceiling per VM. Raise it for LTO release builds — rustc is `SIGKILL`ed at 4 GiB on large workspaces |
| `PRELOOP_RUNNER_STORAGE_GB` | `20` | Writable guest disk. Raise to `80` or more for full hosted-image snapshots and large golden packs |
| `PRELOOP_RUNNER_OVERLAY_GB` | — | Per-VM writable overlay size |
| `PRELOOP_RUNNER_USER` / `PRELOOP_RUNNER_UID` | `runner` / `1001` | Guest account steps run as, for GitHub-hosted parity. `root` restores root; empty disables switching |
| `PRELOOP_USE_FORK` | — | Fork machines from a prepared golden instead of building each |
| `PRELOOP_USE_PACKED_GOLDEN` | `true` | Use a release or locally cached packed golden for on-demand and pooled runners |
| `PRELOOP_GOLDEN_URL` | release asset | Packed golden URL; the optional checksum is fetched from the same URL plus `.sha256` |
| `PRELOOP_GOLDEN_OCI_REF` | official arm64 GHCR artifact | OCI packed golden reference downloaded automatically on arm64 hosts |
| `PRELOOP_RUNNER_BUNDLE` | — | Directory of runner binaries mounted into guests |
| `PRELOOP_RUNNER_EXTERNALS` | temp dir | Host-side Node externals directory |
| `PRELOOP_RUNNER_BASE_IMAGE` | digest-pinned Ubuntu 24.04 | OCI base identity for `runs-on` resolution; set it with `PRELOOP_GOLDEN_URL` for a custom packed golden |
| `PRELOOP_RUNNER_LABELS` | — | Extra labels on every pool runner. **Jobs only dispatch to runners whose labels match `runs-on`** |
| `PRELOOP_RUNNER_NAME_PREFIX` | `preloop-runner` | Machine naming prefix |
| `PRELOOP_RUNNER_DNS` | host resolver | Force a resolver inside guests (e.g. `8.8.8.8`) when the host's is unreachable from the VM network |
| `PRELOOP_RUNNER_PACK_PROXY` / `_NO_PROXY` | — | Proxy and no-proxy values used while downloading and packing golden artifacts |
| `PRELOOP_WORKSPACE` | — | Workspace context for daemon deployments; not a package or toolchain installation input |
| `PRELOOP_REQUIRE_JOB_ASSIGNMENTS` | — | Only let a runner claim jobs explicitly assigned to it |

To add organization-wide software, derive an OCI image from one of Preloop's
digest-pinned Ubuntu bases and build a custom packed golden. For the complete
build, checksum, publishing, and runtime configuration flow, see
[VM images and version tracking](vm-images.md#adding-organization-wide-software).
Keep repository-specific software in workflow setup actions, install steps,
or a job `container:`.

---
## 5. Exposure options

Set `PRELOOP_PUBLIC_URL` to whatever address others actually reach.

### A. No public inbound access

The most locked-down option, and the only one with zero public attack surface.
Bind to loopback or a private/VPN address:

```sh
PRELOOP_LISTEN=127.0.0.1:9090
PRELOOP_PUBLIC_URL=http://127.0.0.1:9090
```

Webhooks **cannot** be delivered, so trigger CI from the CLI:

```sh
preloop run                      # run the workflow locally
preloop run --push --create-pr   # run CI first, then push and open a draft PR
```

Both are outbound-only: check runs are still reported to GitHub. On a VPN such
as Tailscale, bind the VPN address instead and restrict access with its ACLs —
then `details_url` links resolve for exactly the people allowed to read logs.
For a tailnet-only HTTPS URL while keeping Preloop on loopback, use Tailscale
Serve:

```sh
tailscale serve --bg --https=443 http://127.0.0.1:9090
PRELOOP_PUBLIC_URL=https://<host>.<tailnet>.ts.net
```

This URL is reachable only by tailnet members permitted by the Tailscale ACL.
It is not a GitHub webhook endpoint; use a separate public, path-filtering
ingress if GitHub must deliver webhooks.

### B. Tailscale Funnel

Tailscale Funnel provides public HTTPS without opening a port or running a
proxy. Do **not** point it directly at Preloop: Funnel would publish the
unauthenticated runner-registration surface described in §6. Put a
path-filtering proxy in front and publish only the webhook path.

```sh
tailscale funnel --bg 9090
PRELOOP_PUBLIC_URL=https://<host>.<tailnet>.ts.net
```

Webhook URL: `https://<host>.<tailnet>.ts.net/api/v1/github/webhooks`

### C. Cloudflare Tunnel

An outbound connector — no inbound firewall rule and no public IP. Traffic
transits Cloudflare.

```yaml
# /etc/cloudflared/config.yml
tunnel: <tunnel-id>
credentials-file: /etc/cloudflared/<tunnel-id>.json
ingress:
  # Publish ONLY the webhook path — see §6.
  - hostname: ci.example.com
    path: ^/api/v1/github/webhooks$
    service: http://127.0.0.1:9090
  - service: http_status:404
```

A catch-all rule with no `path:` publishes **the entire API**, including runner
registration. Don't do that.

### D. Your own domain, no third party

No tunnel and no external relay: DNS points at your address and you terminate
TLS yourself. The only outbound contact is ACME certificate issuance with your
CA.

1. `A`/`AAAA` record for `ci.example.com` → your public IP
2. Forward `443` to the host
3. Keep preloop on loopback and expose only the webhook path through a proxy:

```caddy
# /etc/caddy/Caddyfile
ci.example.com {
	@webhook path /api/v1/github/webhooks
	handle @webhook {
		reverse_proxy 127.0.0.1:9090
	}
	handle {
		respond 404
	}
}
```

nginx equivalent:

```nginx
server {
    listen 443 ssl;
    server_name ci.example.com;
    ssl_certificate     /etc/letsencrypt/live/ci.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/ci.example.com/privkey.pem;

    location = /api/v1/github/webhooks {
        proxy_pass http://127.0.0.1:9090;
        proxy_set_header Host $host;
    }
    location / { return 404; }
}
```

Set `PRELOOP_PUBLIC_URL=https://ci.example.com`. To make `details_url` links
work in a browser, publish `/runs/` too — behind authentication, because that
path exposes job logs.

---

## 6. Security: what must not be public

`PRELOOP_LISTEN` defaults to `127.0.0.1:9090`, so a bare `preloop serve` is
only reachable from the host. A generated native API token protects
`/api/v1/*` even when you bind a private non-loopback address, but it does not
protect the runner registration surface. Bind a private address or `0.0.0.0`
behind a proxy or tunnel, and do not publish the registration endpoint to the
internet.

**Never publish the whole API surface.** Restrict your proxy or tunnel to
`/api/v1/github/webhooks`, as every example above does.

The reason is `POST /api/v3/actions/runner-registration`. The official GitHub
runner authenticates there with a registration token **GitHub** issued and can
therefore validate. A self-hosted control plane cannot validate a third-party
credential, so it accepts any non-empty one over TCP. Anyone who can reach that
endpoint can:

1. obtain a runner-management credential,
2. register a runner with labels matching your jobs,
3. receive a job message — which carries a freshly minted GitHub App
   installation token and any secrets scoped to that job, and
4. report fabricated job conclusions.

Requests arriving on the mounted control socket are held to a stricter rule (the
system credential is required), because untrusted workflow code can reach that
socket. The TCP runner-registration surface has no equivalent gate, so
**network reachability is the control**.

Also worth knowing:

- `/api/v1/*` requires `PRELOOP_SYSTEM_TOKEN` and returns `401` without it.
- The control socket is path-restricted to the runner surface: guests cannot
  reach native management endpoints through it.
- Job messages carry live credentials by design. Anything that can claim a job
  can read them.
- Workflow code is untrusted: every dependency's `build.rs` and proc macro
  executes inside the VM. Don't run fork pull requests on a pool that shares a
  host with your App private key.

---

## 7. Operating it

### State and backups

`$PRELOOP_HOME` holds everything durable:

| Path | Contents |
|---|---|
| `state/*.db`, `-wal`, `-shm` | Runs, jobs, requests, sessions, check-run ids |
| `state/github-app.json` | GitHub App private key and installation details |
| `state/blobs/`, `state/replay/` | Step logs and job artifacts |
| `state/cache/` | Actions cache entries |
| `vms/` | Golden images and per-machine state |
| `engine.token` | Private fallback for the generated native API token when no OS credential service is available |

Back up at least the database and `github-app.json`. Losing the database strands
any check run GitHub is still waiting on; losing the key means re-keying the App.

### Restarts

A restart settles in-flight work rather than stranding it. Pool machines are
destroyed with the control plane, so their claims can never be completed;
startup fails them explicitly (`failed job claims orphaned by a control-plane
restart count=N`) so the run and its check run reach a terminal state. Those
jobs are lost, not resumed — restart during a quiet period and re-run after.

### Capacity

- Memory: `pool_size × PRELOOP_RUNNER_MEMORY_MIB` should fit with headroom.
  4 × 6 GiB on a 22 GiB host is already oversubscribed and relies on ballooning.
- CPU: `pool_size × PRELOOP_RUNNER_CPUS` vCPUs against your core count. Past roughly 2× the box
  thrashes and timing-sensitive tests flake.
- Disk: prune superseded golden directories and cap the cache directory.

### Troubleshooting

| Symptom | Check |
|---|---|
| Checks stay `queued`, pool idle | Are machines running? Do runner labels match `runs-on`? A runner with no labels is never assigned work |
| `could not compile … (signal: 9, SIGKILL)` | Out of memory — raise `PRELOOP_RUNNER_MEMORY_MIB` |
| Jobs never resume after a restart | Expected; see *Restarts* |
| Guest cannot resolve DNS | Set `PRELOOP_RUNNER_DNS` |
| Webhook delivered but nothing runs | Confirm `PRELOOP_WEBHOOK_SECRET` matches the App, and that the workflow's `on:` matches the event |
| `401` from the CLI | `PRELOOP_TOKEN` must match the server's `PRELOOP_SYSTEM_TOKEN` |

---

## See also

- [`setup.md`](setup.md) — GitHub App and credential setup
- [`github-app-webhook.md`](github-app-webhook.md) — webhook wiring in detail
- [`cli_reference.md`](cli_reference.md) — every command, flag, and variable
- [`push.md`](push.md) — CI before push, with no inbound access
- [`vm-images.md`](vm-images.md) — golden image contents and how they are baked
