# GitHub App Webhook Integration Log &amp; User Guide

This document records the design, build log, and interaction guide for the end-to-end GitHub App Webhook receiver and Checks API status reporting system in `preloop`.

---

## 1. Webhook Architecture Overview

The webhook system enables `preloop` to receive GitHub App events, verify and
commit each raw delivery to a durable queue, and acknowledge it with HTTP 202.
An asynchronous worker fetches matching workflow files and queues jobs for
self-hosted runners. It also integrates with the GitHub Checks API to report
status back to the repository.

### Supported events

The webhook receiver understands every event the `EventAdapter` registry
supports (`src/events/mod.rs`, `all_event_names()`):


| Tier                        | Events                                                                                                                                                                                      |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A — CI-critical             | `push`, `pull_request`, `pull_request_target`, `pull_request_review`, `workflow_dispatch`, `workflow_run`, `check_run`, `check_suite`, `repository_dispatch`, `create`, `delete`, `release` |
| B — issue/PR/social         | `issues`, `issue_comment`, `discussion`, `discussion_comment`, `label`, `milestone`                                                                                                         |
| C — release/admin/fork/wiki | `watch`, `fork`, `deployment`, `deployment_status`, `member`, `public`, `gollum`, `page_build`                                                                                              |
| Internal                    | `schedule` (synthesized by the cron scheduler, never delivered as a webhook)                                                                                                                |


The App-manifest flow (`GET /api/v1/github/register`) defaults to the minimal
CI event set: `push` and `pull_request`. Override the creation-time event list
with `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` (comma-separated) when the App needs
additional or different events. GitHub cannot change an App's event
subscriptions through its API after creation; add later events manually under
the App's settings → Webhooks → Edit.

At startup preloop reads an existing App's subscription back from GitHub
(`GET /app`, App-JWT auth) and **warns loudly** when the trigger events it
turns into runs are missing (`push`, `pull_request`, `pull_request_review`,
`workflow_dispatch`, `workflow_run`, `repository_dispatch`, `issue_comment`,
`issues`, `check_run`, `check_suite`, `create`, `delete`, `release`). GitHub
cannot change an App's event subscription through the API — tick the missing
events under the App's settings → Permissions & events → Subscribe to events.

Two subtleties the warning accounts for, so it stays silent on a correctly
configured App:

- **Permissions gate the checkboxes.** GitHub only renders an event's checkbox
  once the App holds the permission that event requires (`issues` → Issues,
  `workflow_run` → Actions, `create`/`delete`/`release`/`push`/
  `workflow_dispatch`/`repository_dispatch` → Contents, `pull_request*` → Pull
  requests). The warning names the permission to grant first; the installation
  must also accept the widened permissions before delivery starts.
- **`check_run`/`check_suite` are implicit.** Apps with `checks: write` are
  auto-subscribed and GitHub never lists these in `events`, so preloop treats
  write access as satisfying them.

`pull_request_target` is deliberately not in the list: it is a workflow
trigger preloop synthesizes from the `pull_request` webhook
(`src/events/pull_request.rs`), never an event GitHub delivers, so requiring a
subscription to it warned forever.

### Data Flow diagram:

```mermaid
graph TD
    GH[GitHub Webhook / API] -->|1. Event payload & signature| Wh[Webhook Receiver]
    Wh -->|2. Verify and durable enqueue| Queue[(webhook_deliveries)]
    Queue -->|3. Lease and drain asynchronously| Worker[Webhook Worker]
    Worker -->|4. App JWT / installation token| Auth[App Auth Client]
    Auth -->|Workflow fetch| GH
    Worker -->|5. Workflow evaluation| Core[preloop Control Plane]
    Core -->|6. Job InProgress/Done| Report[Checks Reporter]
    Report -->|Checks API / Commit Status| GH
```

Delivery is only half the story: GitHub never resends a failed webhook, so
everything downstream of that `202` — the delivery watchdog, the GitHub
circuit breaker, the App health monitor, and `preloop webhooks replay` — is
documented in [Webhook resilience](./webhook-resilience.md).

---

## 2. Configuration Parameters

The system is configured using the following environment variables:


| Variable                            | Description                                                                                                                                  | Example                                    |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `PRELOOP_WEBHOOK_SECRET`            | Secret key configured on the GitHub App to verify payload signatures.                                                                        | `my-secure-webhook-secret`                 |
| `PRELOOP_LOCAL_WORKSPACE`           | Path to a local Git worktree used for offline workflow loading and immutable local-source checkouts.                                         | `/path/to/my-repo`                         |
| `PRELOOP_GITHUB_TOKEN`              | Static PAT for supported API calls, including remote workflow retrieval, private reusable-workflow resolution, changed-file lookups, and job `GITHUB_TOKEN` when no App is configured. A static PAT is not narrowed per job and is not a Check Runs credential. Prefer an App installation token with `checks: write` for Check Runs. | `github_pat_...`                           |
| `PRELOOP_GITHUB_APPS_JSON`          | JSON array of additional registered Apps overriding `github.apps`; each entry: `app_id`, `pem`, optional `webhook_secret`/`installation_id`. | `[{"app_id":12345,"pem":"-----BEGIN..."}]` |
| `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` | Comma-separated creation-time event list for the App-manifest flow; defaults to `push,pull_request`.                                         | `push,pull_request`                        |


### Security Best Practices

- **Prefer a GitHub App over `PRELOOP_GITHUB_TOKEN`**: App installation tokens can be scoped to the repository and effective job permissions. A static PAT is not narrowed per job. The implementation performs a coarse scope check for some classic PATs, but it is not an equivalent least-privilege boundary; do not configure a broad PAT as an App-mint-failure fallback.
- **Git-Ignore Credentials**: Never check `.env`, `*.pem`, or `*.key` files into Git. These files are excluded in the root `.gitignore`.
- **Production Key Management**: In production, do not write private keys or secrets to plaintext files on the server disk. Instead:
  - Load them directly into memory at runtime using a Secrets Manager (e.g. HashiCorp Vault, AWS Secrets Manager, or Kubernetes Secrets).
  - Inject the configuration as environment variables directly to the running process without file-based middleware.

---

## 3. Webhook signature verification (`X-Hub-Signature-256`)

`preloop` verifies that incoming webhooks are authentic:

- When `PRELOOP_WEBHOOK_SECRET` is set, `preloop` computes the HMAC-SHA256 signature of the raw request body and verifies it against the `x-hub-signature-256` header.
- If verification fails, the header is missing, or no secret is configured at
all, the endpoint returns `401 Unauthorized` — signature verification is
mandatory, never skipped.
- With multiple registered Apps (`github.apps`, see
[GitHub Tokens](./github-tokens.md)), the signature is verified against
**each** App's webhook secret; a payload signed by any registered App is
accepted, and one signed by none is rejected.

---

## 4. Workflow Fetching Strategies

When a webhook event is received, `preloop` resolves the event's immutable
commit SHA before retrieving workflow definitions. The branch ref remains in
the submission for GitHub event context (`github.ref` and trigger filters), but
the YAML is fetched from the commit SHA so a branch update cannot select a
different workflow during delivery.

For `pull_request` and `pull_request_review` runs the checked-out commit is the
pull request's test merge, not its head, so the workflow files come from that
merge, as they do for GitHub's own `pull_request` runs. When `preloop` builds
the merge itself, the merge exists only in its mirror, and the files are read
from there. Only if that read fails are the head's copies used, with a warning.

1. **Local Filesystem (Offline/Dev Mode)**:
   If `PRELOOP_LOCAL_WORKSPACE` is configured, `preloop` reads the
   `.github/workflows/` directory from the event's commit when that commit is
   present in the local repository. If an immutable event SHA is unavailable,
   the accepted delivery is retained in the durable queue for internal retry;
   the current worktree is never used as a substitute. If the durable enqueue
   itself fails, GitHub does not automatically retry the non-2xx response, so
   use GitHub's delivery redelivery action or API after the store recovers. For
   a default `uses: actions/checkout@v4` step, submission also captures the
   worktree as an immutable synthetic Git commit and redirects the compiled
   checkout inputs to preloop's authenticated smart-HTTP endpoint. Tracked
   modifications, deletions, and untracked non-ignored files are included
   without modifying the user's index or workflow YAML. Explicit
   `repository`, `ref`, or `github-server-url` inputs (and non-default
   `fetch-depth`/`fetch-tags`) retain their original remote behavior; an
   explicit `token` is overwritten with the job's local JWT, which the
   snapshot endpoint can verify.
2. **GitHub API (Remote/Production Mode)**:
 If `PRELOOP_LOCAL_WORKSPACE` is not configured, but
 `PRELOOP_GITHUB_TOKEN` or a configured GitHub App is available, `preloop`
 queries:
 `GET /repos/{owner}/{repo}/contents/.github/workflows?ref={commit_sha}`
 and downloads files from that immutable ref.
3. **Current Directory Fallback**:
If neither is set and no immutable workflow revision was requested, it defaults
to looking in the local `.github/workflows/` directory of the current running
workspace. A webhook delivery with an event SHA fails instead of reading the
current worktree.

---

## 5. GitHub Checks API Reporting

When a GitHub App is configured with `checks: write`, the system maps each
job's lifecycle to a GitHub Check Run:

1. **Queued**: When a run is accepted, a check run is created via `POST /repos/{owner}/{repo}/check-runs` with status `queued`. The check run ID is recorded in `RunRecord.job_check_run_ids`.
2. **In Progress**: When the runner fetches and starts the job, the status is updated to `in_progress`.
3. **Completed**: When the runner finishes (or `preloop` reaps it due to timeout/lease expiration), the check run is updated to `completed` with the corresponding conclusion (`success`, `failure`, or `cancelled`).

For reliable Check Run reporting, configure a GitHub App installation with
`checks: write`. The engine's reporting path can consider available GitHub
tokens, but a static PAT is not a substitute for the supported App-based
configuration. Without suitable credentials or permissions, jobs may still
run while Check Run publication fails or is unavailable.

---

## 6. How Users Interact with it

### Step 1: Set up Webhook in GitHub

1. Go to your GitHub App settings.
2. Set the payload URL to `https://<your-url>/api/v1/github/webhooks`.
3. Set the content type to `application/json`.
4. Enter a secure Webhook Secret (e.g. `super-secret`).
5. Select the events preloop should turn into runs. The manifest defaults to
   `push` and `pull_request`. Before registering a new App, set
   `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` to a comma-separated list when additional
   events are required. For an existing App, tick additional events manually
   under the App's settings → Webhooks → Edit.
 At minimum, tick the trigger events: `push`, `pull_request`,
 `pull_request_review`, `workflow_dispatch`, `workflow_run`,
 `repository_dispatch`, `issue_comment`, `issues`, `check_run`, `check_suite`,
 `create`, `delete`, `release`.

### Step 2: Start `preloop-runner-server`

Run the server with the environment variables set:

```sh
export PRELOOP_WEBHOOK_SECRET="super-secret"
export PRELOOP_LOCAL_WORKSPACE="/path/to/runner-watcher"
# Optional static-token fallback for supported API and job-token paths; not a Check Runs credential.
export PRELOOP_GITHUB_TOKEN="ghp_..."

just serve
```

### Step 3: Trigger workflows

Push a commit, open a pull request, dispatch a workflow through the REST API,
or trigger any other subscribed event. `preloop` will:

- Receive the webhook event.
- Fetch the workflows.
- Match filters (branches, tags, paths).
- Queue jobs for any registered runners matching `runs-on` labels.
- Create Check Runs on GitHub when the GitHub App has `checks: write`.

---

## 7. GitHub App Registration and Installation

`preloop` supports the official **GitHub App Manifest** flow to create an App. Creating the App and installing it are separate GitHub operations.

1. **Expose preloop at its final public HTTPS URL**:
 Start `preloop-runner-server` behind a publicly reachable HTTPS address, then open:
  ```text
   https://preloop.example.com/api/v1/github/register
  ```

   `localhost` is suitable for viewing the form, but GitHub cannot deliver webhook events or callback redirects to it. The manifest only includes webhook settings for non-local hosts.
2. **Create the App from the manifest**:
 Click **Register App on GitHub**. GitHub opens its App-creation flow with preloop's redirect URL, webhook URL, default events, and default permissions pre-filled.
3. **Capture the callback credentials**:
 After GitHub creates the App, it redirects to:
  ```text
   https://preloop.example.com/api/v1/github/callback?code=...
  ```

   preloop exchanges the one-time code for an App ID, webhook secret, and private-key PEM, then displays them. Treat the PEM and webhook secret as credentials: store them in a secret manager and do not commit them.
4. **Install the App**:
 In GitHub's App settings, install the newly created App on the target account or repository. Copy the installation ID from its installation settings URL, for example:
  ```text
   https://github.com/settings/installations/INSTALLATION_ID
  ```

   The App cannot mint an installation access token until this step is complete.
5. **Configure preloop and restart it**:
  ```sh
   export PRELOOP_WEBHOOK_SECRET="your-new-webhook-secret"
   export PRELOOP_GITHUB_APP_ID="your-new-app-id"
   export PRELOOP_GITHUB_APP_INSTALLATION_ID="your-installation-id"
   export PRELOOP_GITHUB_APP_PRIVATE_KEY='-----BEGIN PRIVATE KEY-----
   ...
   -----END PRIVATE KEY-----'
   # Or: export PRELOOP_GITHUB_APP_PRIVATE_KEY_PATH=/secure/path/preloop-app.pem
  ```

   When these values are set, preloop signs a GitHub App JWT and requests a per-job installation token scoped to the repository and effective `permissions:`. The granted installation permissions still bound what GitHub can mint; see [GitHub Tokens](./github-tokens.md) for permission narrowing and failure behavior. If no App is configured, `PRELOOP_GITHUB_TOKEN` may be used as the job token. If App minting fails, `PRELOOP_GITHUB_APP_MINT_FAILURE` selects the fallback: `error` fails the job and `pat` explicitly uses the static PAT. `local` is intended to leave the job without an App token, but a later broker fallback can still supply a configured PAT; use `error` or remove the PAT if you require fail-closed behavior. Private reusable-workflow references are resolved with `PRELOOP_GITHUB_TOKEN` or `github.pat`; without either, access to private reusable workflows may fail. App-only deployments do not need a PAT for server-side calls that do not require private reusable-workflow access. Prefer an App with `checks: write` for Check Run reporting, and do not treat a static PAT as a supported replacement.
6. **Confirm delivery and job credential use**:
Push a commit or open a pull request. preloop verifies the webhook and queues matching jobs. With a configured App, the runner receives the job-scoped installation token when minting succeeds. For Check Run reporting, use the App with `checks: write`; a static PAT can support other API or job-token paths but is not a Check Runs credential. Private reusable-workflow references require `PRELOOP_GITHUB_TOKEN` or `github.pat`. App-only deployments need no PAT for server-side calls that do not require private reusable-workflow access.

---

## 8. Operational Deployment &amp; Troubleshooting Guide

This section documents operational best practices, lessons learned, and real-world failure modes encountered during live deployments.

### 8.1 GitHub-facing URL

`--public-url` / `PRELOOP_PUBLIC_URL` is for GitHub: it sets the webhook
address and the base used for Check Run links. Runner and job traffic uses
`PRELOOP_RUNNER_URL` instead (loopback by default when no override is set).
A local or private `--public-url` can make Check Run links unreachable to
GitHub users, but it does not route guest traffic through that address. See
[Self-hosting](self-hosting.md#5-exposure-options) for secure ingress examples.

### 8.2 Tunnel failures

Cloudflare Error 1033 means the edge cannot reach the configured tunnel
origin. This can interrupt inbound webhook delivery and make GitHub-facing
links unavailable. With the current split between `PRELOOP_PUBLIC_URL` and
`PRELOOP_RUNNER_URL`, a public-tunnel outage does not by itself break local
runner artifact uploads. Point the tunnel at a path-filtering proxy; do not
expose the engine port directly. See the Cloudflare example in
[Self-hosting](self-hosting.md#5-exposure-options).

### 8.3 GitHub App permission narrowing

When GitHub rejects an installation-token request with `422`, Preloop reads the
installation's grants, narrows the requested permissions, retries, and reports
the permissions actually minted. This can also affect explicitly declared
workflow permissions; a job may receive a weaker token than the YAML requests.
Grant the App the permissions your workflows need, and check the reported
permission set when a job depends on a write scope. If the retry fails, the
configured `PRELOOP_GITHUB_APP_MINT_FAILURE` policy applies.

### 8.4 Cross-Compiled Runner Bundle &amp; `cargo clean`

The microVM orchestrator requires the cross-compiled Linux runner binary,
located by searching the install prefix (`lib/preloop/runner/<triple>/`),
`target/<triple>/{debug,release}`, or `PRELOOP_RUNNER_BUNDLE`:

- Running `cargo clean` removes this binary, causing `preloop serve` to log:
  ```text
  WARN preloop: no runner pool: jobs will fail after the queue grace window. Install the Linux guest runner with `preloop update`, or set PRELOOP_RUNNER_BUNDLE to a directory containing preloop-runner (see docs/vm-images.md)
  ```
- **Recovery**: `preloop update` is the recommended fix — it installs the
  Linux guest runner into the install prefix. For development builds, rebuild
  the runner bundle with `cargo zigbuild` before starting the server:
  ```sh
  cargo zigbuild -p preloop-runner --target aarch64-unknown-linux-gnu
  ```

### 8.5 Skipping CI runs

GitHub supports commit-message markers such as `[skip ci]` to skip workflows
triggered by `push` and `pull_request`. A skipped workflow creates no check run;
if that check is required for merging, the pull request can remain blocked with
the check pending. Use skip markers only when no required validation needs to
run. For documentation-only changes, prefer workflow path filters so unrelated
code jobs can be omitted while the Mintlify build, link, and accessibility
checks still execute.

### 8.6 Runner and public URL separation

`PRELOOP_PUBLIC_URL` / `--public-url` is for GitHub-facing webhooks and Check
Run links. Runner and job endpoints use `PRELOOP_RUNNER_URL` instead; the
engine supplies a loopback default when no override is configured. Do not
assume a public tunnel is part of the local runner's data path.



#### Endpoint details

`preloop serve` now publishes two origins:

- `PRELOOP_PUBLIC_URL` (`--public-url`) — GitHub-facing only. Used for Check Run `details_url` links and webhook delivery. No guest dials it.
- `PRELOOP_RUNNER_URL` — runner and job endpoints, including broker, results/cache, signed blob, OIDC, and live-log URLs. It defaults to `http://127.0.0.1:<port>` when no override is configured; local runner traffic uses the mounted control socket and guest loopback bridge.

Consequences:

- Job traffic never crosses the tunnel regardless of `--public-url`; the tunnel is load-bearing only for *GitHub→host* webhook delivery and check-run links — where it belongs.
- A tunnel outage degrades webhook delivery/check-run UX, not in-VM CI execution.
- **Remote runners** (runner on another machine, not the local smolvm pool): set `PRELOOP_RUNNER_URL=http://<host-reachable>:9090` — but note in-VM runners cannot reach LAN addresses under `PublicOnly` egress, so this override only works for runners outside the VM pool.

Keep local runner traffic on the loopback default. Set a host-reachable
`PRELOOP_RUNNER_URL` only for remote runners, and keep public tunnel ingress
path-filtered.