# GitHub Tokens in Preloop

How `GITHUB_TOKEN` is produced, what it can and cannot do, and how to upgrade it
to a real GitHub credential.

---

## 1. How `GITHUB_TOKEN` works in Preloop

Preloop runs its own control plane. `preloop-runner-server` implements the Actions
runner protocol locally, so no GitHub-hosted orchestrator is in the loop.

Each job gets **two independent credentials**, and keeping them straight is the
whole point of this document.

| Credential | Runner env | Source | Always local? |
|---|---|---|---|
| Runner/service token | `ACTIONS_RUNTIME_TOKEN`, `ACTIONS_ID_TOKEN_REQUEST_TOKEN` | `endpoint.authorization.parameters.AccessToken` | Yes — always the local HMAC JWT |
| Job token | `GITHUB_TOKEN` | `system.github.token` / `github_token` | GitHub App token, an accepted static PAT, or empty; never the runtime JWT |

The runner message uses the official lower-case `github_token` variable. The
runner maps that built-in variable to the canonical
`${{ secrets.GITHUB_TOKEN }}` expression; an uppercase `GITHUB_TOKEN` variable
is not transmitted in the acquire response.

The **local HMAC JWT** is signed with a per-instance key persisted at
`<state-dir>/hmac-key.bin` and carries `sub: preloop-job-<job-id>` plus
`scp: Actions.Results:<plan-id>:<job-id>`. It authenticates against *the Preloop
server*, and against nothing else — `api.github.com` will reject it.

Almost everything the runner does against the control plane uses the
service token, not `GITHUB_TOKEN`: cache, artifacts, timeline records, console
logs, log files, step summaries, and OIDC id-tokens all authenticate with
`ACTIONS_RUNTIME_TOKEN`. That credential is never replaced, so **configuring a
GitHub App or PAT cannot break any of those paths**.

Checkout of a local workspace is the one case where a job-facing token talks to
the Preloop server. When `PRELOOP_LOCAL_WORKSPACE` is set, submission captures the
worktree as an immutable synthetic commit and rewrites the default
`actions/checkout` step to fetch it from Preloop's Git smart-HTTP endpoint.
That endpoint only accepts a local job JWT, so the server pins the local JWT
directly onto the step's `token` input rather than letting it default to
`${{ github.token }}`. Local checkout therefore always works, whatever
`GITHUB_TOKEN` happens to hold. The rewrite is skipped if the step sets
`repository`, `ref`, or `github-server-url` (or non-default
`fetch-depth`/`fetch-tags`); a user `token` input is replaced by the pinned
local JWT.

When no GitHub credential is available, the job's `GITHUB_TOKEN` and
`${{ github.token }}` are empty. The local HMAC JWT remains a separate runtime
credential used to authenticate to Preloop; it is never passed to GitHub as the
job token. A step that reads an empty token may make an anonymous API request;
private or write operations that need authentication will fail.

---

## 2. What works without any GitHub token

Everything below runs against Preloop's own control plane and needs no GitHub
credential.

| Capability | Notes |
|---|---|
| `actions/checkout` | For a repo in `PRELOOP_LOCAL_WORKSPACE`, served from the run's immutable snapshot over Preloop's authenticated Git smart-HTTP endpoint. |
| `actions/cache` | Both the v1 `_apis/artifactcache` API and the v2 `CacheService` Twirp API used when `ACTIONS_CACHE_SERVICE_V2` is on. |
| `actions/upload-artifact`, `actions/download-artifact` | v1 and v4 (`ArtifactService` Twirp plus blob upload/download). |
| All `run:` steps | Shell execution is entirely runner-local. |
| Container jobs and service containers | `container:` and `services:` are resolved from the job plan; images are pulled by the runner. |
| Matrix, `needs`, `if:` conditions, expressions | Evaluated by Preloop's own planner. |
| OIDC id-tokens | Needs `permissions: id-token: write`. Signed RS256 with the server's own keypair; the issuer defaults to `<runner-base-url>/oidc` (`PRELOOP_RUNNER_URL`, falling back to `PRELOOP_PUBLIC_URL`) and is overridable with `--oidc-issuer`. Usable only if your cloud provider is configured to trust that issuer and its JWKS. |
| Problem matchers, annotations, step summaries | Matchers run inside the runner, annotations travel as timeline issues, summaries upload to Preloop's blob store. |
| Public actions in `uses:` | The server fetches action tarballs from `<github-api-url>/repos/{owner}/{repo}/tarball/{ref}` (honors `PRELOOP_GITHUB_API_URL`), authenticating with `PRELOOP_GITHUB_TOKEN` when set to avoid the 60/hour anonymous limit, and caches them under `<state-dir>/actions/`. Private actions will not download without a credential. |

---

## 3. What requires a real GitHub token

These call `api.github.com` from inside the job, where a local JWT is
meaningless:

- `actions/github-script`
- `gh` CLI commands that hit the API
- Actions that create or update pull requests, issues, releases, comments, or labels
- `actions/checkout` against a private **remote** repo — one not served from the local workspace snapshot
- Any other action that calls `api.github.com`

Without a usable GitHub credential, these calls receive an empty token and may
be anonymous. Public unauthenticated reads can still work; private or
write-authorized operations require a credential with the necessary access.

---

## 4. Setting up a GitHub App (recommended)

A GitHub App gives each job an installation token for that repository,
requested with the job's effective permissions after the operator's configured
ceiling. If GitHub rejects the request with `422`, Preloop narrows it to the
installation's available grants and retries; the resulting token can therefore
carry fewer permissions than the workflow declared.

On the machine running the engine, one command does the whole thing:

```sh
preloop setup github --via app          # add --org NAME for an org App
```

It serves the manifest from a single-use loopback listener, GitHub redirects
the one-time code back to it, and the App id, private key, and webhook secret
are stored in the engine's credential store (`~/.preloop/credentials` by default, or the operating-system keychain with `credential_store = "os"`). `~/.preloop/config.toml`
(mode 0600) keeps only non-secret references. Add `--public-url https://…` to
create the App with webhook delivery enabled; without it webhooks are off,
since GitHub cannot reach `localhost`. Restart the engine to pick the
credentials up.

### From a browser, against a remote engine

When the engine already runs somewhere GitHub can reach, the same manifest is
served by the engine itself. This path *displays* the credentials rather than
storing them — you copy them to the engine yourself:

1. Visit `https://<server>/api/v1/github/register` in a browser. Use the
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
   preloop setup github --via app --app-id 123456 --pem-file /secure/path/preloop-app.pem
   export PRELOOP_WEBHOOK_SECRET="…"   # or set github.webhook_secret in config.toml
   ```

### Configuration reference

| Variable | Purpose |
|---|---|
| `PRELOOP_GITHUB_APP_ID` | App ID. Required to enable minting. |
| `PRELOOP_GITHUB_APP_PEM` | Private key, inline PEM. Highest precedence. |
| `PRELOOP_GITHUB_APP_PEM_FILE` | Path to a private-key PEM file. |
| `PRELOOP_GITHUB_APP_PRIVATE_KEY` | Inline PEM, older alias. |
| `PRELOOP_GITHUB_APP_PRIVATE_KEY_PATH` | PEM file path, older alias. |
| `PRELOOP_GITHUB_APP_INSTALLATION_ID` | Pins one installation and skips discovery. |
| `PRELOOP_GITHUB_APP_MINT_FAILURE` | What a job's `GITHUB_TOKEN` becomes if minting fails: `local` (default), `error`, or `pat`. See below. |
| `PRELOOP_GITHUB_API_URL` | REST API base. Defaults to `https://api.github.com`; set it for GitHub Enterprise Server. |

The four private-key variables are tried in the order listed and the first
non-blank one wins; blank and whitespace-only values count as unset. Both PKCS#1
(`BEGIN RSA PRIVATE KEY`, what GitHub hands out) and PKCS#8
(`BEGIN PRIVATE KEY`, what `openssl pkcs8` produces) are accepted, and inline
PEMs whose newlines were flattened to literal `\n` — routine in Docker and
Kubernetes env vars — are un-escaped before parsing.

Partial configuration (an App ID with no key, or a key with no App ID) logs a
warning and simply disables minting. A key that *is* set but cannot be read or
parsed is a hard startup error: the operator clearly meant to configure an App,
and booting without one would silently downgrade every job token. An
unrecognised `PRELOOP_GITHUB_APP_MINT_FAILURE` value is likewise fatal at startup,
so a typo cannot quietly select a different policy.

### What happens per job

- The installation is resolved from the repository owner by paging
  `GET /app/installations` (100 per page, up to 10 pages) and matching
  `account.login` case-insensitively. The resulting id is cached in-process for
  the server's lifetime. `PRELOOP_GITHUB_APP_INSTALLATION_ID` skips list
  discovery, but Preloop still verifies that the installation belongs to the
  configured App.
- The token is minted with **both** `repositories` and `permissions` always
  present in the request body. `repositories` holds the single repository from
  the run's `owner/repo` slug, and `permissions` holds the job's effective
  scopes. Omitting either field is what GitHub treats as "grant everything this
  installation can reach", so neither is ever left out.
- A run whose `repository` is not an `owner/repo` slug — a pure local-workspace
  submission, say — cannot be repository-scoped, so minting fails before any
  network call and `PRELOOP_GITHUB_APP_MINT_FAILURE` takes over.
- Only the installation id is cached — never the token, since permissions vary
  per job. Every job mints a fresh one.
- The App authenticates with an RS256 JWT that is backdated 60 seconds for clock
  skew and expires 10 minutes out, regenerated for each mint.
- Preloop uses the `expires_at` returned with the installation token and does
  not refresh that token in place. Long-running jobs should not assume the
  token remains valid indefinitely.
- Installation tokens are minted per job when the broker dispatches it — not
  at submission — so downstream jobs never wait on a short-lived token.

### When minting fails

A mint can fail for many reasons: the App is not installed on that owner, the
key was revoked, the API is rate-limited, or the run's `repository` is not a
real slug. If GitHub returns `422` for the requested permissions, Preloop fetches
the installation's grants, narrows the request, retries, and reports the
permissions actually minted. If that retry also fails, the configured
`PRELOOP_GITHUB_APP_MINT_FAILURE` policy applies:

| Value | Behavior |
|---|---|
| `local` (default) | The App mint failure does not itself fail the job. The intended result is no App-issued job token. **Current implementation caveat:** if a static `PRELOOP_GITHUB_TOKEN` is also configured, a later broker fallback can still supply it; do not rely on `local` to withhold that PAT. Use `error` or remove the static PAT if you require fail-closed behavior. |
| `error` | The mint failure is terminal: the job claim fails instead of running with fallback authority. |
| `pat` | The job receives `PRELOOP_GITHUB_TOKEN` without per-job or per-repository narrowing; fork-restricted jobs do not receive this fallback. |

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
  a `422`, which would fail the whole mint. Omitting the key is what withholds
  the scope.
- `id-token` and `models` are dropped for the same reason: neither is a GitHub
  App installation permission. `permissions: id-token: write` still works
  exactly as before, because OIDC id-tokens are issued by Preloop's own
  `/oidctoken` endpoint, not by the App token.

If the workflow declares no `permissions:` at all, the job gets GitHub's
restricted default — `contents: read`, `metadata: read`, `packages: read`, the
same three scopes the official runner prints in its setup log — stated
explicitly in the request. It does **not** get the installation's full
permission set.

If every declared scope is withheld (`permissions: {}`, or a block whose entries
are all `none` or Actions-only), the request asks for `metadata: read` alone.
That is the narrowest token GitHub will issue, and it is used instead of an empty
`permissions` object because an empty object is not a documented way to request
an empty token.

An installation token cannot exceed the App's grants. If a requested scope is
not granted, Preloop reads the installation permissions, narrows the request,
and retries. The resulting token may be weaker than the workflow's declaration;
check the reported effective permissions when a workflow depends on a write
scope. Grant the App the permissions your workflows need to avoid narrowing.

### Enterprise considerations

- The App created by the manifest flow is **private**, owned by whichever
  account completed the flow.
- Installing it on an org may require org-admin approval.
- Orgs with strict App policies should skip the manifest flow, create the App by
  hand in org settings, and set the same env vars. Grant it at least the
  permissions your workflows request.
- Store the PEM in a secrets manager and inject it as `PRELOOP_GITHUB_APP_PEM`.
  Do not leave a `.pem` on disk in production, and never commit one.

---

## 5. Using a PAT (fallback)

Set a Personal Access Token when starting the server:

```sh
export PRELOOP_GITHUB_TOKEN="github_pat_..."
```

- When no default App is configured, Preloop introspects a classic PAT's
  `X-OAuth-Scopes` and applies a coarse read/write admission check against the
  workflow's effective `permissions:`. This is not a per-scope comparison and
  does not narrow the PAT per job or repository. A fine-grained PAT normally
  has no `X-OAuth-Scopes` header, so its bounds are unverifiable and it is
  withheld from jobs in this path. It may still be used for supported
  server-side GitHub API calls.
- An explicit App-mint fallback of `pat` bypasses that normal PAT admission
  check. The static credential is not repository-scoped and may exceed a job's
  declared permissions. Fork-restricted jobs never receive it.
- `PRELOOP_GITHUB_TOKEN` is used server-side for supported GitHub API calls,
  including remote workflow fetching and pull-request changed-file lookups.
  The server may also attempt Check Run calls with a resolved static PAT, but
  GitHub may reject that credential. For reliable published Check Runs, use a
  GitHub App installation token with `checks: write`.

---

## 6. Token priority

```
GitHub App installation token
  (one repository; effective permissions after the operator ceiling and any
   install-grant narrowing; minted per job)
  ↓ no App, or an App mint returns no token
PRELOOP_GITHUB_TOKEN PAT
  (classic scopes checked coarsely when the default App is absent; a fine-grained
   PAT is normally withheld because its bounds cannot be verified)
  ↓ neither credential is usable
empty `GITHUB_TOKEN` — clients may make anonymous API calls
```

There is an implementation caveat when an App and a static PAT are both
configured: the broker's later fallback can still supply a verified PAT even
when the App policy is `local`. Use `PRELOOP_GITHUB_APP_MINT_FAILURE=error` or
remove the static PAT if a failed App mint must stop the job. The explicit
`pat` policy also supplies the static credential without repository or per-job
narrowing. Fork-restricted jobs do not receive the PAT fallback.

With no usable GitHub credential, the job receives an empty `GITHUB_TOKEN`; the
job-scoped runtime JWT is never handed out as that token. It authenticates to
Preloop only. `ACTIONS_RUNTIME_TOKEN` and the pinned `actions/checkout` token
remain separate local HMAC JWTs, so cache, artifacts, logs, OIDC, and
local-workspace checkout do not depend on the GitHub job token.

## 7. Dispatch API authentication (GitHub App API compatibility)

The github.com-compatible dispatch endpoints
(`POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches`,
`POST /repos/{owner}/{repo}/dispatches`) authenticate through a credential
chain rather than the native bearer alone:

| Credential | Verification | Works offline? |
|---|---|---|
| System token | Native bearer comparison | yes |
| PAT (`PRELOOP_GITHUB_TOKEN`) | Constant-time compare | yes |
| Own-App JWT | RS256 verified against the registered App PEM | yes |
| Own-App installation token | In-memory mint ledger (hash → installation, repo, permissions, expiry) | yes |
| Third-party App installation token | github.com round-trip: `GET /installation` + `GET /installation/repositories`; requires `actions: write`; 60s cache; **fails closed** on network error | no |

Third-party App JWTs are never accepted — there is no PEM to verify them with.
The `sender` / `github.actor` for a dispatched run is:
- own App installation token (offline mint ledger) → `{slug}[bot]` resolved via
  `GET /app` (cached); `{app_id}[bot]` when github.com is unreachable
- third-party installation token (online) → `{slug}[bot]`, falling back to
  `{account.login}[bot]` when the slug is absent
- own-App JWT → `{slug}[bot]` (or `{app_id}[bot]` offline)
- PAT → `GET /user` login when github.com is reachable; otherwise the dedicated
  placeholder `preloop-pat` (never the system-bearer identity `preloop-system`).
  Failed PAT actor lookups are not cached, so a transient outage does not pin
  the placeholder after github.com recovers.
- system token → `preloop-system`

Installation-token dispatches carry the `AppDispatch` trust tier, which allows
repository secrets (the caller has proven the endpoint's write permission,
matching github.com). System / PAT / own-App JWT dispatches stay on
`AdminManual`.

---

## Related

- [GitHub App Webhook Integration](./github-app-webhook.md) — webhook delivery, Checks API reporting, App registration walkthrough
- [Fidelity Gap](./fidelity-gap.md) — what Preloop deliberately does not replicate
