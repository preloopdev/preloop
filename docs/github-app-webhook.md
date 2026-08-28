# GitHub App Webhook Integration Log &amp; User Guide

This document records the design, build log, and interaction guide for the end-to-end GitHub App Webhook receiver and Checks API status reporting system in `preloop`.

---

## 1. Webhook Architecture Overview

The webhook system enables `preloop` to receive GitHub App webhook events
directly from GitHub, fetch matching workflow files, and queue jobs for
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
    GH[GitHub Webhook / API] -->|1. Event payload & Signatures| Wh[Webhook Receiver]
    Wh -->|2. Event Type & SHAs| Auth[App Auth Client]
    Auth -->|JWT / App Private Key| GH
    GH -->|Installation Token| Fetch[Workflow Fetcher]
    Fetch -->|3. Fetch .github/workflows/*.yml| AST[Workflow Evaluator]
    AST -->|Matches? -> submit_run_inner| Core[preloop Control Plane]
    Core -->|4. Job InProgress/Done| Report[Checks Reporter]
    Report -->|Checks API / Commit Status| GH
```

---

## 2. Configuration Parameters

The system is configured using the following environment variables:


| Variable                            | Description                                                                                                                                  | Example                                    |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `PRELOOP_WEBHOOK_SECRET`            | Secret key configured on the GitHub App to verify payload signatures.                                                                        | `my-secure-webhook-secret`                 |
| `PRELOOP_LOCAL_WORKSPACE`           | Path to a local Git worktree used for offline workflow loading and immutable local-source checkouts.                                         | `/path/to/my-repo`                         |
| `PRELOOP_GITHUB_TOKEN`              | Fallback GitHub Personal Access Token for workflow retrieval and Check Run updates when no configured GitHub App is available.                  | `ghp_...`                                  |
| `PRELOOP_GITHUB_APPS_JSON`          | JSON array of additional registered Apps overriding `github.apps`; each entry: `app_id`, `pem`, optional `webhook_secret`/`installation_id`. | `[{"app_id":12345,"pem":"-----BEGIN..."}]` |
| `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` | Comma-separated creation-time event list for the App-manifest flow; defaults to `push,pull_request`.                                         | `push,pull_request`                        |


### Security Best Practices

- **Git-Ignore Credentials**: Never check `.env`, `*.pem`, or `*.key` files into Git. These files are excluded in the root `.gitignore`.
- **Production Key Management**: In production, do not write private keys or secrets to plaintext files on the server disk. Instead:
  - Load them directly into memory at runtime using a Secrets Manager (e.g. HashiCorp Vault, AWS Secrets Manager, or Kubernetes Secrets).
  - Inject the configuration as environment variables directly to the running process without file-based middleware.

---

## 3. Webhook signature verification (`X-Hub-Signature-256`)

<!-- Trigger Webhook Event Test 2026-07-01 13:38 -->

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

1. **Local Filesystem (Offline/Dev Mode)**:
 If `PRELOOP_LOCAL_WORKSPACE` is configured, `preloop` reads the
 `.github/workflows/` directory from the event's commit when that commit is
 present in the local repository. If an immutable event SHA is unavailable,
 delivery fails so GitHub can redeliver it; the current worktree is never used
 as a substitute. For a default
 `uses: actions/checkout@v4` step, submission also captures the worktree as
 an immutable synthetic Git commit and redirects the compiled checkout inputs
 to preloop's authenticated smart-HTTP endpoint. Tracked modifications,
 deletions, and untracked non-ignored files are included without modifying
 the user's index or workflow YAML. Explicit repository/ref/token/server
 checkout inputs retain their original remote behavior.
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

The system maps the lifecycle of each job to a GitHub Check Run:

1. **Queued**: When a run is accepted, a check run is created via `POST /repos/{owner}/{repo}/check-runs` with status `queued`. The check run ID is recorded in `RunRecord.job_check_run_ids`.
2. **In Progress**: When the runner fetches and starts the job, the status is updated to `in_progress`.
3. **Completed**: When the runner finishes (or `preloop` reaps it due to timeout/lease expiration), the check run is updated to `completed` with the corresponding conclusion (`success`, `failure`, or `cancelled`).

If neither a configured GitHub App nor `PRELOOP_GITHUB_TOKEN` is available,
these requests are simulated in-memory and logged to the console, allowing
fully offline execution.

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
 `pull_request_target`, `pull_request_review`, `workflow_dispatch`,
 `workflow_run`, `repository_dispatch`, `issue_comment`, `issues`,
 `check_run`, `check_suite`, `create`, `delete`, `release`.

### Step 2: Start `preloop-runner-server`

Run the server with the environment variables set:

```sh
export PRELOOP_WEBHOOK_SECRET="super-secret"
export PRELOOP_LOCAL_WORKSPACE="/path/to/runner-watcher"
export PRELOOP_GITHUB_TOKEN="ghp_optional_token_for_checks"

just serve
```

### Step 3: Trigger workflows

Push a commit, open a pull request, dispatch a workflow through the REST API,
or trigger any other subscribed event. `preloop` will:

- Receive the webhook event.
- Fetch the workflows.
- Match filters (branches, tags, paths).
- Queue jobs for any registered runners matching `runs-on` labels.
- Create check runs on GitHub.

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

   When these values are set, preloop signs a GitHub App JWT and exchanges it for a per-job installation access token scoped to the run's repository and to that job's effective `permissions:`. A job declaring no `permissions:` gets GitHub's restricted default (`contents`, `metadata`, `packages` at `read`), never the installation's full grant. If no App configuration is present at all, `PRELOOP_GITHUB_TOKEN` is the job token. If an App *is* configured but minting fails, `PRELOOP_GITHUB_APP_MINT_FAILURE` decides — `local` (the default) keeps the job on the local HMAC JWT rather than silently widening its authority to the PAT. See [GitHub Tokens](./github-tokens.md).
6. **Confirm delivery and job credential use**:
Push a commit or open a pull request. preloop verifies the webhook and queues matching jobs. The installed App's scoped token is supplied to the runner job. For remote workflow retrieval and GitHub Check Run reporting, preloop selects the configured GitHub App credentials; `PRELOOP_GITHUB_TOKEN` is used only as the fallback when no App is available. App-only deployments do not need to provide a PAT for these server-side calls.

---

## 8. Operational Deployment &amp; Troubleshooting Guide

This section documents operational best practices, lessons learned, and real-world failure modes encountered during live deployments.

### 8.1 `--public-url` Dual Purpose

The `--public-url` parameter supplied to `preloop serve` serves two distinct purposes:

1. **In-VM Control Plane Endpoint**: Tells the ephemeral runner microVM inside SmolVM where to connect back to the control plane.
2. **GitHub Check Run Links**: Forms the base URL for the `details_url` field sent to GitHub when registering check runs on PRs and commits (e.g. `https://preloop.preloop.dev/runs/<run_id>`).

**Pitfall**: Setting `--public-url` to a local LAN IP (e.g. `http://192.168.1.221:9090`) during local testing will cause GitHub check runs to be registered with non-routable local IP links on GitHub PRs. Always keep `--public-url https://preloop.preloop.dev` in production deployments.

### 8.2 Cloudflare Tunnel Configuration &amp; Error 1033

When routing public webhooks and check status requests through Cloudflare Tunnels (`preloop.preloop.dev`):

- **Error 1033 (`Argo Tunnel error`)**: Occurs when Cloudflare's edge cannot communicate with the local `cloudflared` process, or when port `9090` is down or un-routable.
- **Correct Tunnel Target**: The production named tunnel `preloop-prod` (`16cc97ea-e9d5-4723-875a-6de90f880b07`) must be run with explicit local target port 9090:
  ```sh
  cloudflared tunnel --url http://127.0.0.1:9090 run 16cc97ea-e9d5-4723-875a-6de90f880b07
  ```
- **Port Mismatches**: Running an unconfigured token or pointing `cloudflared` to a different port (e.g., `8787` or `8080`) breaks webhook delivery and causes in-VM runner artifact uploads to time out with HTTP 530.

### 8.3 GitHub App Scope Clamping &amp; HTTP 422 Handling

GitHub App token minting is strictly all-or-nothing:

- Requesting any scope ungranted by the GitHub App installation causes GitHub's API to return `HTTP 422 Unprocessable Entity`.
- **Automatic Clamping**: For unrequested default scopes (e.g. `packages: read`), `preloop` automatically clamps token requests to the installation's granted intersection (`contents: read`, `metadata: read`).
- **Explicit Permissions**: Workflows declaring explicit `permissions:` blocks that exceed installation grants will intentionally fail loudly so missing permissions are never silently ignored.

### 8.4 Cross-Compiled Runner Bundle &amp; `cargo clean`

The microVM orchestrator requires the cross-compiled Linux ARM64 runner binary at `target/aarch64-unknown-linux-gnu/debug/preloop-runner`:

- Running `cargo clean` removes this binary, causing `preloop serve` to log:
  ```text
  WARN preloop: local runner provisioning unavailable; jobs queue until a runner is available error=Linux runner bundle unavailable...
  ```
- **Recovery**: Rebuild the runner bundle with `cargo zigbuild` before starting the server:
  ```sh
  cargo zigbuild -p preloop-runner --target aarch64-unknown-linux-gnu
  ```
- 

### 8.5 Skipping CI Runs

To suppress CI for a push, include any of these labels anywhere in a commit message within the push batch (not just the head commit):

- `[skip ci]`
- `[ci skip]`
- `[no ci]`
- `[skip actions]`
- `[actions skip]`
- `***NO_CI***`

Example:

```sh
git commit -m "docs: update readme [skip ci]"
git push
```

If **any** commit in a push batch contains a skip label, the entire push is suppressed — no jobs are queued and no check runs are created. This matches official GitHub Actions behavior.

### 8.6 Guest Network Isolation, Origin Routing &amp; the Tunnel Hairpin

Runner VMs run under `NetworkPolicy::PublicOnly` — guest egress can reach the public internet but the hypervisor's egress floor deliberately refuses guest→host private addresses (loopback or LAN IP; verified: guest curl domehan the host LAN URL hangs indefinitely). Guests reach the control plane through exactly one sanctioned path:

1. **Runner transport** — the runner's own control-plane HTTP (connectionData, long-poll, broker) rides the mounted unix socket `/run/preloop-control/engine.sock` when `PRELOOP_CONTROL_SOCKET`/`PRELOOP_CONTROL_ORIGIN` are set (always set by the orchestrator when a control socket is configured).
2. **Job-side programs** (`actions/checkout`'s git, `curl`, Node actions) only know URLs. The runner binds the advertised origin *inside the guest on loopback* and splices each accepted connection onto the socket (`preloop-runner/src/control_bridge.rs`). Blast radius: one host endpoint.

#### The advertised origin decides everything

The bridge only binds when the advertised origin is a loopback address the guest can bind. This makes `--public-url` carry a third, hidden role beyond in-VM runner config and GitHub check-run `details_url`: it selects the transport for *all* guest-side traffic.


| `--public-url`                       | Runner transport                                      | Job-side traffic                                | Tunnel dependency   |
| ------------------------------------ | ----------------------------------------------------- | ----------------------------------------------- | ------------------- |
| `http://127.0.0.1:9090` (dev)        | socket                                                | loopback bridge → socket                        | none                |
| `http://<lan-ip>:9090`               | socket                                                | **blackholes** (LAN IP refused by egress floor) | —                   |
| `https://preloop.preloop.dev` (prod) | **public internet → Cloudflare → argo tunnel → host** | same hairpin                                    | **hard dependency** |


Consequences:

- With the production hostname, *everything* guests do — registration, long-poll, artifact uploads, checkout fetches — physically leaves the host, traverses Cloudflare, and returns through the tunnel. A tunnel outage therefore breaks *local* CI: observed as `530 / error code: 1033` from in-VM `connectionData` fetches and as `actions/upload-artifact` timeouts (`runner-light` failure during the 2026-07-30 tunnel transition).
- The hostname also has to keep resolving publicly; `preloop.preloop.dev` TLS and DNS are load-bearing for local jobs in this mode.

#### Resolution: the runner-facing origin is split from the public URL

`preloop serve` now publishes two origins:

- `**PRELOOP_PUBLIC_URL**` (`--public-url`) — GitHub-facing only. Used for check-run `details_url` links and anything GitHub must reach. No guest ever dials it.
- `**PRELOOP_RUNNER_URL**` — every URL handed to runners and their jobs (connectionData `brokerUrl`, endpoint data `ResultsServiceUrl`/`CacheServerUrl`, Twirp signed blob/cache URLs, `system.github.launch_endpoint`, OIDC issuer, live-log ws feed). `serve` pins it to `http://127.0.0.1:<port>` by default, which routes over the mounted unix socket (`PRELOOP_CONTROL_SOCKET`) for the runner itself and via the in-guest loopback bridge (`control_bridge.rs`) for job-side TCP programs.

Consequences:

- Job traffic never crosses the tunnel regardless of `--public-url`; the tunnel is load-bearing only for *GitHub→host* webhook delivery and check-run links — where it belongs.
- A tunnel outage degrades webhook delivery/check-run UX, not in-VM CI execution.
- **Remote runners** (runner on another machine, not the local smolvm pool): set `PRELOOP_RUNNER_URL=http://<host-reachable>:9090` — but note in-VM runners cannot reach LAN addresses under `PublicOnly` egress, so this override only works for runners outside the VM pool.

A host-side reverse proxy cannot solve the hairpin by itself — the guest resolves public names over public DNS and `PublicOnly` egress means packets leave the host before a proxy could intercept them. Origin separation is the fix; DNS-pinning the public hostname to guest loopback with a local CA achieves the same effect at strictly higher complexity.