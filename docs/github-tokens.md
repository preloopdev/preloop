# GitHub Tokens in Preloop

How `GITHUB_TOKEN` is produced, what it can and cannot do, and how to upgrade it
to a real GitHub credential.

---

## 1. How `GITHUB_TOKEN` works in Preloop

Preloop runs its own control plane. `aksh-runner-server` implements the Actions
runner protocol locally, so no GitHub-hosted orchestrator is in the loop.

Each job gets **two independent credentials**, and keeping them straight is the
whole point of this document.

| Credential | Runner env | Source | Always local? |
|---|---|---|---|
| Runner/service token | `ACTIONS_RUNTIME_TOKEN`, `ACTIONS_ID_TOKEN_REQUEST_TOKEN` | `endpoint.authorization.parameters.AccessToken` | Yes — always the local HMAC JWT |
| Job token | `GITHUB_TOKEN` | `system.github.token` / `github_token` | No — becomes a real GitHub credential when an App or PAT is configured |

The **local HMAC JWT** is signed with a per-instance key persisted at
`<state-dir>/hmac-key.bin` and carries `sub: aksh-job-<job-id>` plus
`scp: Actions.Results:<plan-id>:<job-id>`. It authenticates against *the Preloop
server*, and against nothing else — `api.github.com` will reject it.

Almost everything the runner does against the control plane uses the
service token, not `GITHUB_TOKEN`: cache, artifacts, timeline records, console
logs, log files, step summaries, and OIDC id-tokens all authenticate with
`ACTIONS_RUNTIME_TOKEN`. That credential is never replaced, so **configuring a
GitHub App or PAT cannot break any of those paths**.

Checkout of a local workspace is the one case where a job-facing token talks to
the Preloop server. When `AKSH_LOCAL_WORKSPACE` is set, submission captures the
worktree as an immutable synthetic commit and rewrites the default
`actions/checkout` step to fetch it from Preloop's Git smart-HTTP endpoint.
That endpoint only accepts a local job JWT, so the server pins the local JWT
directly onto the step's `token` input rather than letting it default to
`${{ github.token }}`. Local checkout therefore always works, whatever
`GITHUB_TOKEN` happens to hold. The rewrite is skipped entirely if the step
already sets any of `repository`, `ref`, `token`, or `github-server-url`, so
explicit user intent is never overridden.

When no App and no PAT are configured, `GITHUB_TOKEN` is also a local HMAC JWT.
It is a placeholder in that case: nothing in a job needs it, and any call it
makes to `api.github.com` will fail.

---

## 2. What works without any GitHub token

Everything below runs against Preloop's own control plane and needs no GitHub
credential.

| Capability | Notes |
|---|---|
| `actions/checkout` | For a repo in `AKSH_LOCAL_WORKSPACE`, served from the run's immutable snapshot over Preloop's authenticated Git smart-HTTP endpoint. |
| `actions/cache` | Both the v1 `_apis/artifactcache` API and the v2 `CacheService` Twirp API used when `ACTIONS_CACHE_SERVICE_V2` is on. |
| `actions/upload-artifact`, `actions/download-artifact` | v1 and v4 (`ArtifactService` Twirp plus blob upload/download). |
| All `run:` steps | Shell execution is entirely runner-local. |
| Container jobs and service containers | `container:` and `services:` are resolved from the job plan; images are pulled by the runner. |
| Matrix, `needs`, `if:` conditions, expressions | Evaluated by Preloop's own planner. |
| OIDC id-tokens | Needs `permissions: id-token: write`. Signed RS256 with the server's own keypair; the issuer defaults to `<public-base-url>/oidc` and is overridable with `--oidc-issuer`. Usable only if your cloud provider is configured to trust that issuer and its JWKS. |
| Problem matchers, annotations, step summaries | Matchers run inside the runner, annotations travel as timeline issues, summaries upload to Preloop's blob store. |
| Public actions in `uses:` | The server fetches action tarballs from `https://api.github.com/repos/{owner}/{repo}/tarball/{ref}` **unauthenticated** and caches them under `<state-dir>/actions/`. This path is hardcoded and does not honor `AKSH_GITHUB_API_URL`. Private actions will not download, and you share the anonymous API rate limit. |

---

## 3. What requires a real GitHub token

These call `api.github.com` from inside the job, where a local JWT is
meaningless:

- `actions/github-script`
- `gh` CLI commands that hit the API
- Actions that create or update pull requests, issues, releases, comments, or labels
- `actions/checkout` against a private **remote** repo — one not served from the local workspace snapshot
- Any other action that calls `api.github.com`

Without a real token these fail at the API call with `401`/`403`, not at job
setup: Preloop always supplies *some* `GITHUB_TOKEN` value.

---

## 4. Setting up a GitHub App (recommended)

A GitHub App gives each job a short-lived installation token scoped to the
permissions that job declares.

1. Visit `http://<server>/api/v1/github/register` in a browser. Use the
   server's final public HTTPS URL if you also want webhook delivery — GitHub
   cannot reach `localhost`, and the manifest only includes webhook settings for
   non-local hosts.
2. Click **Register App on GitHub**. GitHub opens its App-creation flow with
   Preloop's manifest pre-filled: a private App with default permissions
   `checks: write`, `contents: read`, `metadata: read`, `pull_requests: read`.
3. GitHub redirects back to `/api/v1/github/callback`, which exchanges the
   one-time code and displays the **App ID**, **webhook secret**, and
   **private-key PEM**. Copy them now.
4. Install the App on the target account, org, or repositories from its GitHub
   settings page. No installation token can be minted until it is installed.
5. Configure the server and restart it:

   ```sh
   export AKSH_GITHUB_APP_ID="123456"
   export AKSH_GITHUB_APP_PEM_FILE="/secure/path/aksh-app.pem"
   ```

### Configuration reference

| Variable | Purpose |
|---|---|
| `AKSH_GITHUB_APP_ID` | App ID. Required to enable minting. |
| `AKSH_GITHUB_APP_PEM` | Private key, inline PEM. Highest precedence. |
| `AKSH_GITHUB_APP_PEM_FILE` | Path to a private-key PEM file. |
| `AKSH_GITHUB_APP_PRIVATE_KEY` | Inline PEM, older alias. |
| `AKSH_GITHUB_APP_PRIVATE_KEY_PATH` | PEM file path, older alias. |
| `AKSH_GITHUB_APP_INSTALLATION_ID` | Pins one installation and skips discovery. |
| `AKSH_GITHUB_API_URL` | REST API base. Defaults to `https://api.github.com`; set it for GitHub Enterprise Server. |

The four private-key variables are tried in the order listed and the first
non-blank one wins; blank and whitespace-only values count as unset. Both PKCS#1
(`BEGIN RSA PRIVATE KEY`, what GitHub hands out) and PKCS#8
(`BEGIN PRIVATE KEY`, what `openssl pkcs8` produces) are accepted, and inline
PEMs whose newlines were flattened to literal `\n` — routine in Docker and
Kubernetes env vars — are un-escaped before parsing.

Partial configuration (an App ID with no key, or a key with no App ID) logs a
warning and simply disables minting. A key that *is* set but cannot be read or
parsed is a hard startup error: the operator clearly meant to configure an App,
and booting without one would silently downgrade every job token.

### What happens per job

- The installation is resolved from the repository owner by paging
  `GET /app/installations` (100 per page, up to 10 pages) and matching
  `account.login` case-insensitively. The resulting id is cached in-process for
  the server's lifetime; `AKSH_GITHUB_APP_INSTALLATION_ID` skips discovery
  altogether.
- Only the installation id is cached — never the token, since permissions vary
  per job. Every job mints a fresh one.
- The App authenticates with an RS256 JWT that is backdated 60 seconds for clock
  skew and expires 10 minutes out, regenerated for each mint.
- Tokens expire after **1 hour** and are not refreshed in place. A job running
  longer than an hour may see `$GITHUB_TOKEN` expire mid-run.
- Minting happens for every job in a run before the dispatch lock is taken, so a
  slow GitHub round-trip cannot stall other runs.
- If minting fails for any reason — App not installed for that owner, network
  error, revoked key — the server logs a warning and falls back (see §6). A mint
  failure never fails the job. A run whose `repository` is not a real GitHub
  slug, such as a pure local-workspace submission, degrades this way by design.

### How `permissions:` maps to token scopes

Job-level `permissions:` overrides workflow-level entirely (they are not
merged), and `read-all` / `write-all` expand to every known scope. Three
spellings of the same scope names exist in this pipeline:

```
workflow YAML          kebab-case   pull-requests, security-events
runner wire variable   PascalCase   PullRequests, SecurityEvents   (system.github.token.permissions)
GitHub REST body       snake_case   pull_requests, security_events (installation token request)
```

Two rules govern what actually reaches GitHub:

- A scope set to `none` is **dropped** from the request rather than forwarded.
  The installation-token API has no `none` level and rejects unknown values with
  a `422`, which would fail the whole mint and fall the job back to the broader
  PAT — more access, not less. Omitting the key is what withholds the scope.
- `id-token` and `models` are dropped for the same reason: neither is a GitHub
  App installation permission. `permissions: id-token: write` still works
  exactly as before, because OIDC id-tokens are issued by Preloop's own
  `/oidctoken` endpoint, not by the App token.

If the workflow declares no `permissions:` at all, the request omits the
`permissions` field, which grants the installation's full permission set — the
same thing Actions does. A token can never exceed what the App was granted at
install time.

### Enterprise considerations

- The App created by the manifest flow is **private**, owned by whichever
  account completed the flow.
- Installing it on an org may require org-admin approval.
- Orgs with strict App policies should skip the manifest flow, create the App by
  hand in org settings, and set the same env vars. Grant it at least the
  permissions your workflows request.
- Store the PEM in a secrets manager and inject it as `AKSH_GITHUB_APP_PEM`.
  Do not leave a `.pem` on disk in production, and never commit one.

---

## 5. Using a PAT (fallback)

Set a Personal Access Token when starting the server:

```sh
export AKSH_GITHUB_TOKEN="github_pat_..."
```

- The PAT is injected verbatim as `GITHUB_TOKEN` for **all** jobs. It is not
  scoped per repository and it **ignores `permissions:`** entirely — a workflow
  declaring `permissions: contents: read` still receives the PAT's full rights.
  `permissions:` is only enforced on the GitHub App path.
- Fine-grained PATs are strongly preferred over classic tokens; scope them to
  the specific repositories and permissions your workflows need.
- The same `AKSH_GITHUB_TOKEN` is also used server-side for Check Run
  create/update, remote workflow fetching, and pull-request changed-file
  lookups. With it unset, check runs are simulated in-memory and logged instead.

---

## 6. Token priority

```
GitHub App installation token (scoped to `permissions:`, fresh per job, 1h TTL)
  ↓ falls back to
AKSH_GITHUB_TOKEN PAT (static, operator-provided, unscoped)
  ↓ falls back to
Local HMAC JWT (only works against the Preloop server)
```

The App path is attempted whenever App credentials are configured; a mint
failure logs a warning and falls through. Every job therefore receives some
`GITHUB_TOKEN` value.

Regardless of which branch is taken, `ACTIONS_RUNTIME_TOKEN` and the pinned
`actions/checkout` token stay local HMAC JWTs, so cache, artifacts, logs, OIDC,
and local-workspace checkout behave identically in all three cases.

---

## Related

- [GitHub App Webhook Integration](./github-app-webhook.md) — webhook delivery, Checks API reporting, App registration walkthrough
- [Fidelity Gap](./fidelity-gap.md) — what Preloop deliberately does not replicate
