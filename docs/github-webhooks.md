# Receiving webhooks without the GitHub App

The GitHub App is the recommended integration: one installation covers every
repo, it is the only way to create **check runs** (see below), and its
one-click registration lives at `/api/v1/github/register` (or `preloop setup
github --via app`). The App webhook path is documented in
[`github-app-webhook.md`](github-app-webhook.md).

Some orgs gate App installations or prefer a lighter footprint. A
repository-level webhook works for triggering runs — with two caveats that
matter:

1. **Check runs cannot be created without a GitHub App.** The Checks API
   rejects PATs (`403 You must authenticate via a GitHub App`). With a repo
   hook you get *runs*, but no check marks on commits/PRs. `--push` PR
   creation also needs `pull_requests: write` on whatever token you use.
2. The webhook delivers **per repository**, so every repo needs its own hook
   (or you automate hook creation).

## Setup

### 1. Server side

The server verifies every delivery's `X-Hub-Signature-256` against exactly
one secret: the `AKSH_WEBHOOK_SECRET` environment variable. Generate one and
start the server with it:

```sh
openssl rand -hex 32            # → e.g. 23cc9db6… (keep it)
export AKSH_WEBHOOK_SECRET="23cc9db6…"
preloop serve --listen 127.0.0.1:9090
```

The server must be reachable from GitHub: a public URL or a tunnel that maps
`https://your-host/api/v1/github/webhooks` to the server. The delivery URL is
fixed — the webhook handler lives at `/api/v1/github/webhooks`.

### 2. GitHub side

For each repository:

1. **Settings → Webhooks → Add webhook**
2. **Payload URL:** `https://your-host/api/v1/github/webhooks`
3. **Content type:** `application/json`
4. **Secret:** the exact `AKSH_WEBHOOK_SECRET` value from step 1
5. **Which events:** select *Let me select individual events* and check
   **Pushes** and **Pull requests** (or choose *Just the push event*)
6. **Active:** leave checked, **Add webhook**

### 3. Verify

- Push a commit or open a PR; the run should appear in `preloop status`.
- In the repo's **Webhooks → Recent Deliveries**, the delivery should show
  `200`. A `401` means the secret on GitHub does not match the server's
  `AKSH_WEBHOOK_SECRET`; `530`/timeouts mean the tunnel is down.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Delivery `401` | Secret mismatch (the server checks `AKSH_WEBHOOK_SECRET` only — the App's registered webhook secret is irrelevant to repo hooks) | Align the hook secret with the server env, or vice versa |
| Delivery `530` / timeout | GitHub cannot reach the server | Tunnel/port-forward down; fix and **Redeliver** from Recent Deliveries |
| Run created, no check marks | PAT cannot use the Checks API | Install the GitHub App (check runs) or accept runs-only |
| `--push` PR creation fails `403` | Token/App lacks `pull_requests: write` | Grant it (App settings → Permissions → Pull requests → Read and write) |

GitHub retries failed deliveries with backoff for up to ~24h; after that a
missed event is gone. The reconciliation sweep (periodic GitHub-API catch-up)
will close the gap once implemented — see `submit-driven-ci.md`.

## App webhook vs repo webhook

| | GitHub App | Repo webhook |
|---|---|---|
| Scope | Every repo the App is installed on | One repo per hook |
| Check runs | ✅ (required for the Checks API) | ❌ |
| Trigger runs | ✅ | ✅ |
| Setup | One-time manifest flow | Per-repo, ~1 minute each |
| Org policy | Needs App approval | Works with PATs |
