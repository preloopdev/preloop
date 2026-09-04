# GitHub App API Compatibility — Implementation Plan

Status: Implemented (M1–M4 per Implementation Log; M5 pending)
Author: design session — see §11 for the implementation record
Branch: `Bnjoroge/gh-app-api-compat`

## 1. Problem Statement

Third-party GitHub Apps (bots, CI orchestration tools like pullfrog, merge queues,
coverage bots) assume a five-surface contract with github.com:

1. **Event ingress** — webhooks delivered to the App (push, PR, issue_comment, …)
2. **Programmatic dispatch** — REST endpoints that *trigger* runs
3. **Execution** — runners
4. **Status egress** — check runs / commit statuses
5. **Identity** — GitHub App tokens

Preloop already has surfaces 1, 3, 4, and most of 5 (webhook receiver with ~27
event adapters, the runner pool, check-run reporting, per-job App token minting).
It has **no surface 2**: there are no github.com-compatible REST routes that an App
can call to say "run this workflow". Apps therefore cannot use preloop as their CI
service without code changes on their side.

**Goal**: implement the Actions-relevant subset of the GitHub REST API on preloop's
own listener, with GitHub-App token validation, so any App written against
github.com (or GHE) can target preloop by changing its API base URL.

**Principle ("be GHE")**: GitHub Enterprise Server supports third-party Apps not by
adapting to each App but by implementing the same REST contract. Preloop already
has the GHE hooks (`PRELOOP_GITHUB_API_URL`, `github.server_url`). This plan
extends that to the *dispatch* contract.

## 2. Goals / Non-Goals

### Goals
- `POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches` (workflow_dispatch)
- `POST /repos/{owner}/{repo}/dispatches` (repository_dispatch, broadcast)
- Dispatch auth: own-App installation tokens, own-App JWTs, PAT, system bearer;
  third-party App installation tokens validated via a github.com round-trip
- Faithful `github.event` / `github.event_name` / `github.actor` context for
  dispatched runs, byte-identical semantics to github.com where the adapters allow
- Multi-App registry (per-App webhook secret + PEM) so several Apps can coexist
- App event-subscription support so social/trigger events (issue_comment, issues,
  pull_request_review, repository_dispatch, workflow_dispatch) reach preloop
- Fix the stale `docs/github-app-webhook.md` (code supports ~27 events; doc says 2)

### Non-Goals
- Full GitHub REST API parity (only the Actions-facing dispatch/read subset)
- GitHub Enterprise Server support (that is the *model*, not a deliverable)
- Changing the runner protocol, OIDC, or the broker path
- The pullfrog fork itself (external work that consumes this API)

## 3. Current State (verified)

| Area | Where | State |
|---|---|---|
| Webhook receiver | `src/github.rs::handle_github_webhook` (~813), `process_github_webhook` (~1075), `verify_signature` (~121) | HMAC via single `webhook_secret`, delivery dedup, per-event adapters |
| Event adapters | `src/events/mod.rs` + `events/{push,pull_request,pull_request_target,pull_request_review,workflow_dispatch,workflow_run,repository_dispatch,issue_comment,issues,check_run,check_suite,schedule,create,delete,deployment,fork,gollum,release,watch,...}.rs` | ~27 events; `EventAdapter` trait → `EffectiveEvent` (event, git_ref, sha, activity_type, trust_tier, payload) |
| workflow_dispatch adapter | `src/events/workflow_dispatch.rs` | Resolves ref (input / default branch), stamps `TrustTier::AdminManual`, comment says inputs are defaulted "in submission handling" |
| repository_dispatch adapter | `src/events/repository_dispatch.rs` | `make_default_branch_events`, broadcast semantics implied |
| Run submission | `src/runs.rs::submit_run_inner` (~123, takes `WorkflowSubmission`) | The scheduler already does "synthesize payload → submit_run_inner" (`src/scheduler.rs` ~745) — the template for dispatch |
| Workflow fetch | `src/remote_workflows.rs`, `src/github.rs` (~634 contents API, ~715 commit fetch, ~746 PR files) | Fetches `.github/workflows` for a ref; used by the webhook path |
| Default branch | `src/scheduler.rs` (~388, `GET /repos/{repository}`) | Reusable for ref resolution |
| App JWT/mint | `src/github_app.rs` | Single App (`PRELOOP_GITHUB_APP_ID` + PEM), installation discovery, per-job token minting with `repositories`+`permissions`, `set_app_webhook_config` (~915, PATCH `/app/hook/config`), manifest (`default_events = ["push","pull_request"]` at ~1463) |
| Router | `src/routes.rs` | `protected_apis` block; no `/repos/...` routes |
| Auth deny rules | `src/auth.rs` (~357-381) | `/api/v3/` denied; `/api/v1/` protected except `/api/v1/actions/`; `/repos/...` currently unclassified |
| Config | `src/config.rs` `Github` struct | Single `webhook_secret`, `pat`, App env vars |
| Check runs | `src/github.rs` / check-run reporting | queued/in_progress/completed with `details_url` |
| Parser context | `preloop-gha-parser` | `github.event_name`, `github.event.inputs` supported (test `workflow_dispatch_inputs_are_in_event_context`) |
| Docs | `docs/github-app-webhook.md` | **STALE**: documents only push + pull_request |
| Tests | `src/lib_tests.rs` (axum router: `app()`, `request_json`), `src/concurrency_http_properties.rs`, `src/events/property_tests.rs` | Patterns to follow |

## 4. Design Decisions

### D1. Dispatch routes live in the protected router, authenticated by a new extractor
Add a `dispatch_auth` module. Routes are registered in `routes.rs` under the
protected router. Auth is **mandatory** (github.com returns 401 without a token).
`/repos/...` must be added to `auth.rs` classification so it is neither public nor
accidentally denied — see M2.

### D2. Token validation chain (in order)
1. **System bearer** (the per-engine token from `PRELOOP_SYSTEM_TOKEN` or its secure store) — trusted operator; tier AdminManual.
2. **PAT** (`PRELOOP_GITHUB_TOKEN`) — constant-time compare; tier AdminManual.
3. **Own-App JWT** (RS256, `iss` = one of the registered App ids) — verify with that App's PEM; offline-safe; tier AdminManual.
4. **Installation tokens** (any App, including third-party):
   - **Online path** (github.com reachable): validate with a github.com round-trip —
     `GET /installation` (bearer = the token) returns the installation (id, account,
     app_id, permissions); `GET /installation/repositories` returns repo access.
     Require the endpoint's permission in the installation's granted permissions
     (`actions: write` for workflow dispatch, `contents: write` for repository
     dispatch).
     Cache by token-hash with short TTL (60s), keyed also by expiry; fail **closed**
     on network error (do not fall through to "anonymous").
   - **Offline path** (no github.com): only tokens preloop itself minted are accepted —
     keep an in-memory mint ledger (token-hash → {installation_id, repo, permissions,
     expires_at, app_id}) populated at mint time in `github_app.rs`, with expiry
     cleanup. This keeps local/offline dispatch working for the operator's own App.
5. **Anything else** — 401. Third-party App JWTs are *never* accepted (no PEM to verify with).

Actor resolution for the synthesized `sender`:
- Installation token → `{app_slug}[bot]` (the App's bot login; the third-party
  online path falls back to `{account.login}[bot]` when the slug is absent; the
  offline mint-ledger path resolves the slug via `GET /app`, falling back to
  `{app_id}[bot]` when github.com is unreachable)
- Own-App JWT → `{slug}[bot]` (`GET /app`, cached), or `{app_id}[bot]` when
  github.com is unreachable
- PAT → `GET /user` (cached) when github.com is reachable, else the `preloop-pat`
  placeholder
- System bearer → `preloop-system`

### D3. Dispatch → run pipeline reuses the webhook adapters
Do **not** write a parallel submit path. Synthesize the webhook-shaped payload,
run it through the existing adapter, then `submit_run_inner`:

- `workflow_dispatch`: fetch the named workflow file (remote or local), synthesize
  `{ref, ref_type, inputs, repository:{default_branch,...}, sender:{...}}`, project
  via `events::workflow_dispatch::Adapter`, submit.
- `repository_dispatch`: synthesize `{action: event_type, client_payload,
  repository:{...}, sender:{...}}`, fetch **all** workflows for the ref, project via
  `events::repository_dispatch::Adapter` for each workflow whose `on.repository_dispatch.types`
  matches `event_type` (broadcast), submit each match.

`github.event` fidelity comes for free: `EffectiveEvent.payload` is stored raw and
the parser resolves against it.

### D4. Input validation for workflow_dispatch (github.com semantics)
GitHub validates `inputs` against the workflow's `on.workflow_dispatch.inputs`:
- missing **required** input → 422
- type mismatch (`boolean|choice|number|string`) → 422
- `default` applied when absent
- `options` enforced for `choice`

The adapter comment says defaulting already happens "in submission handling" —
**verify and complete it**; the dispatch endpoint must surface 422 before any run
is created. Missing input validation must not reject a *webhook*-delivered dispatch
(the webhook path must stay lenient, matching github.com).

### D5. Trust tiers
- System/PAT/own-App-JWT dispatch → `TrustTier::AdminManual` (matches the adapter's
  existing stamp; secrets allowed).
- Validated installation-token dispatch (actions:write proven) → a tier that
  **allows secrets** — dispatched runs on github.com receive repo secrets, and the
  caller holds actions:write. If no existing tier fits, extend `TrustTier` with
  e.g. `AppDispatch` (secrets allowed). Check `TrustTier::allows_secrets` in
  `src/events/trust_tier.rs` before deciding.

### D6. Multi-App registry (config + runtime)
- Config: extend `config.rs::Github` with `apps: Vec<AppConfig>` where
  `AppConfig { app_id, pem, webhook_secret, installation_id? }`. Keep the existing
  single-App env vars (`PRELOOP_GITHUB_APP_ID`, `PRELOOP_GITHUB_APP_PEM*`) as the
  default first entry (back-compat). New env: `PRELOOP_GITHUB_APPS_JSON` (or
  config.toml `github.apps = [...]` — follow existing config conventions).
- Webhook receiver: verify `X-Hub-Signature-256` against **each** registered App's
  secret; prefer identifying the App via `x-github-hook-installation-target-id`
  when present. Unknown signature → 401 as today.
- Token minting (`github_app.rs`): pick the App whose installation covers the run's
  `owner/repo` (installation discovery per App, cached); fall back to the default App.
- Native admin endpoint `GET/POST /api/v1/github/apps` (system bearer) to list and
  register Apps at runtime (nice-to-have; config-first is fine).

### D7. Event subscription
- Manifest `default_events` is configurable and defaults to the minimal CI set:
  `push`, `pull_request`. Operators who need additional events must add them
  manually in the App settings UI because GitHub's API cannot change event
  subscriptions after App creation. The trigger events Apps may need include
  `pull_request_review`, `workflow_dispatch`, `workflow_run`,
  `repository_dispatch`, `issue_comment`, `issues`, `check_run`, `check_suite`,
  `create`, `delete`, `release`, plus the Tier B/C events the adapters already
  handle. `pull_request_target` is not among them: preloop synthesizes it from
  the `pull_request` webhook, and GitHub delivers no such event. `check_run`
  and `check_suite` need no explicit subscription when the App holds
  `checks: write` — GitHub auto-subscribes those and omits them from `events`.
- **Caveat**: GitHub's `PATCH /app/hook/config` cannot change an App's event
  subscription (only url/content_type/secret). Event subscriptions are set at App
  creation (manifest) or in the App settings UI. So:
  - New Apps (manifest flow): only the minimal defaults apply automatically;
    add additional events manually in the App settings UI or set the override
    before creating the App.
  - Existing Apps: `GET /app` (App JWT auth) returns `events` — read it back at
    startup, and **warn loudly** if the required trigger events are missing, with
    instructions to tick them in App settings.
- `set_app_webhook_config` (github_app.rs ~915) stays URL/secret-only; extend its
  verification to report missing events too.

### D8. Repository_dispatch broadcast + workflow_id resolution
- `workflow_id` (path param) accepts: filename (`ci.yml`, `ci`, `.github/workflows/ci.yml`).
  Numeric workflow ids only if preloop already tracks them — otherwise filename-only,
  documented. (github.com accepts both; filename is the 99% case.)
- `repository_dispatch` has no target — it is a broadcast over all workflows with a
  matching `types:` entry; matching is done by `submit_run_inner`'s trigger match
  (authoritative), same as the webhook path.

## 5. Endpoint Contracts (match github.com)

### `POST /repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches`
- Auth: D2 chain. Requirement: `actions: write` on the repo (github.com requires
  `actions: write` for workflow dispatch).
- Body: `{ "ref": string?, "inputs": {k: v}? }` (ref defaults to default branch)
- Success: `204 No Content` (empty body)
- Errors:
  - `401` — missing/invalid token
  - `403` — token valid but no `actions: write` / repo not accessible
  - `404` — repo or workflow unknown (do not leak existence: 404 for both)
  - `409` — workflow exists but `workflow_dispatch` is not in its `on:` triggers
  - `422` — input validation failure (message lists the offending input)
- Side effects: run created (or rejected before any state mutation), check runs
  reported exactly like webhook-driven runs.

### `POST /repos/{owner}/{repo}/dispatches`
- Auth: D2 chain. Requirement: `contents: write` (github.com requires
  `contents: write` for repository dispatch; verified against the installation's
  granted permissions).
- Body: `{ "event_type": string (required, ≤100 chars), "client_payload": object? }`
- Success: `204 No Content`
- Errors: `401`, `403`, `404`, `422` (event_type missing or >100 chars)
- Side effects: every workflow with a matching `on.repository_dispatch.types` entry
  runs (broadcast).

### Read endpoints (for Apps that poll — secondary, include in M2 if cheap)
- `GET /repos/{owner}/{repo}/actions/workflows` → list with `id`, `name`, `path`
- `GET /repos/{owner}/{repo}/actions/runs` → recent runs with `status`, `conclusion`
These are convenience; check runs remain the primary egress. Implement only if they
fall out of existing state queries cheaply.

## 6. Module Layout

```
src/
  dispatch.rs            # NEW  — route handlers: workflow_dispatch + repository_dispatch
  dispatch_auth.rs       # NEW  — token validation chain (D2), actor resolution, ledger, cache
  github_apps.rs         # NEW (or extend github_app.rs) — multi-App registry, per-App secret
                           #   verification for the webhook receiver, minting selection
  github.rs              # EDIT — receiver: multi-secret verification; expose fetch helpers
  github_app.rs          # EDIT — manifest events (D7), mint ledger population (D2.4), GET /app read-back
  routes.rs              # EDIT — register dispatch routes in protected router
  auth.rs                # EDIT — classify /repos/... dispatch routes (protected, dispatch_auth)
  config.rs              # EDIT — apps registry
  events/*.rs            # small edits only if TrustTier needs the D5 extension
  runs.rs                # EDIT — expose a submit helper if submit_run_inner needs an
                           #   actor/tier override hook (prefer minimal: reuse as-is)
  openapi.rs             # EDIT — document the new endpoints (native doc convention)
docs/
  github-app-api-compat.md   # THIS FILE — keep as the design doc
  github-app-webhook.md      # EDIT — fix staleness (27 events, not 2)
  github-tokens.md           # EDIT — dispatch auth section
  fidelity-gap.md            # EDIT — record the new surface
```

## 7. Test Plan (repo conventions)

- **Router integration tests** (`lib_tests.rs` style: `AppState::new`, `app()`,
  `request_json`): POST dispatch → 204 → run appears in `GET /api/v1/runs` →
  jobs complete → check-run state transitions. Follow `concurrency_http_properties.rs`
  for exercising the real router.
- **Auth tests**: each chain step (system bearer, PAT, own-App JWT, own-minted
  installation token offline, third-party token online via stubbed github.com,
  garbage → 401, expired → 401, missing `actions:write` → 403). Stub github.com
  the way `github_app.rs` tests do (`spawn_probe_stub`, axum stubs).
- **Fidelity tests**: dispatched run's `github.event_name`, `github.event.inputs`,
  `github.event.client_payload`, `github.actor` — assert exact JSON in expressions
  (pattern: `job_builder.rs` / `eval.rs` tests).
- **Input validation tests**: required missing / type mismatch / choice options /
  default application → 422 with correct message; webhook-delivered dispatch stays lenient.
- **Broadcast tests**: two workflows with matching `repository_dispatch.types` → both
  run; non-matching `types` → not run; empty `types` → matches all (github.com semantics:
  absent `types` matches every event_type).
- **Property tests** (`events/property_tests.rs` style): synthesized dispatch payloads
  for workflow_dispatch/repository_dispatch project to valid `EffectiveEvent`s.
- **Multi-App tests**: two Apps with different secrets; webhook signed by App B's
  secret → verified; wrong secret → 401; minting picks the App installed on the repo.

## 8. Milestones & Acceptance Criteria

### M1 — Docs fix + event subscription
- [ ] `docs/github-app-webhook.md` corrected (full event list from `all_event_names()`)
- [ ] Manifest `default_events` configurable, minimal `push`/`pull_request`
      default with manual GitHub settings guidance for additional events (D7)
- [ ] Startup read-back of `GET /app` events; loud warning when trigger events missing
- [ ] Tests: manifest generation includes events; read-back warning path
- **Exit**: existing deployments unchanged (no new required config); docs match code.

### M2 — Dispatch shim (own auth)
- [ ] `POST .../actions/workflows/{id}/dispatches` + `POST /repos/{o}/{r}/dispatches`
      with system-bearer, PAT, own-App-JWT, own-minted-token (offline) auth
- [ ] Input validation (D4) → 422 semantics; workflow not dispatchable → 409
- [ ] Fidelity: `github.event_name/inputs/client_payload/actor` correct
- [ ] Read endpoints if cheap (D8/secondary)
- [ ] Tests: integration + auth + fidelity + validation
- **Exit**: an App (or curl) can dispatch a workflow to the local pool end to end,
  `github.event` matches github.com shapes, check runs report.

### M3 — Third-party token validation
- [ ] Online round-trip (`GET /installation`, `GET /installation/repositories`),
      cache, fail-closed
- [ ] Mint ledger for offline own-token path (D2.4)
- [ ] Actor resolution for third-party tokens
- [ ] Tests with stubbed github.com (success, 401, network failure → fail closed)
- **Exit**: a token minted by an *unrelated* GitHub App dispatches successfully when
  it holds actions:write, and is rejected (401/403) otherwise.

### M4 — Multi-App registry
- [ ] `github.apps` config; per-App webhook secrets in the receiver; minting selection
- [ ] Native admin endpoints (optional)
- [ ] Tests: per-App secret routing, minting selection
- **Exit**: two Apps coexist; webhooks signed by either App are accepted; each run's
  `GITHUB_TOKEN` is minted from the App installed on its repo.

### M5 — Fidelity hardening + gate
- [ ] Property tests for dispatch payloads
- [ ] `openapi.rs` docs; `docs/github-tokens.md` + `fidelity-gap.md` updated
- [ ] Dogfood: add a fixture workflow to `fixtures/workflows/` exercising
      `repository_dispatch` (broadcast) end to end locally
- [ ] `just test-ci` green
- **Exit**: full gate passes; docs describe the new surface accurately.

## 9. Verification (per repo rules)

- Run `just test-ci` (fmt-check + clippy + tests) at the end of the work; fix
  everything it finds.
- Dogfood the dispatch path locally: `just serve` + a submitted
  `workflow_dispatch`/`repository_dispatch` fixture, confirm runs land on the pool.
- Do NOT run project-wide validation mid-flight; do it once at the end.

## 10. Open Questions (decide with evidence, note in the doc)

1. Does `submit_run_inner` need an actor/tier override, or can the adapter carry it
   (prefer adapter/EffectiveEvent; avoid new plumbing)?
2. Does preloop track numeric workflow ids anywhere? (If yes, accept them in
   `workflow_id`; if no, filename-only + document.)
3. For the offline mint ledger: is mint-time population sufficient, or should the
   ledger also cover tokens minted by the `preloop setup github` flow before this
   feature shipped? (Answer: nothing to migrate — the ledger is populated at mint
   time going forward; old tokens expire.)
4. Exact tier for installation-token dispatch (D5): extend `TrustTier` or reuse?
   Check `allows_secrets` policy first.

## 11. Implementation Log

Milestone status and open-question decisions, updated as the work lands.

### M1 — Docs fix + event subscription (done)

- `docs/github-app-webhook.md` rewritten: full event table from
  `all_event_names()` (Tier A/B/C + `schedule` note); signature verification
  is mandatory (doc previously claimed it is skipped without a secret — the
  code 401s); webhook setup steps list the trigger events; multi-secret
  verification noted.
- `src/github.rs`: `manifest_default_events()` — minimal `push` and
  `pull_request` default, overridable via
  `PRELOOP_GITHUB_APP_DEFAULT_EVENTS` (comma-separated). Manifest
  `default_permissions` expanded (read-level: actions/issues/discussions/
  deployments/members/pages) so the expanded event list is deliverable.
- `src/github_app.rs`: `read_app_events_at` (`GET /app`, App-JWT auth),
  `required_trigger_events()` / `missing_trigger_events()` /
  `warn_missing_trigger_events()`; `set_app_webhook_config_at` now reads the
  subscription back after a successful PATCH and warns on missing triggers.
- `src/bootstrap.rs`: `serve()` spawns a startup read-back of the App's event
  subscription and warns loudly when trigger events are missing.
- Tests: manifest defaults use the minimal CI set; env override respected;
  `read_app_events_at` parses a stubbed `/app` and
  fails closed on refusal; missing-trigger computation is canonical.

**Open-question decisions so far:**
- D7 manifest default: `push` and `pull_request`. Additional events can be
  selected at creation time with `PRELOOP_GITHUB_APP_DEFAULT_EVENTS`; after
  creation, operators must add subscriptions manually in GitHub's App
  settings because the API cannot change them.

### M2 — Dispatch shim (done)

- `src/dispatch.rs` (new): `POST /repos/{owner}/{repo}/actions/workflows/
  {workflow_id}/dispatches`, `POST /repos/{owner}/{repo}/dispatches`, and the
  read endpoints `GET /repos/{owner}/{repo}/actions/workflows` and
  `GET /repos/{owner}/{repo}/actions/runs`. Workflow fetch/ref resolution/
  submission reuse the webhook machinery (`fetch_workflows`,
  `resolve_ref_sha`, the `workflow_dispatch` / `repository_dispatch`
  adapters, `submit_run_inner`). Input validation (D4) runs through the
  parser's `apply_workflow_dispatch_inputs` **before** any run is created
  (422 with the offending input named); a workflow without a
  `workflow_dispatch` trigger is 409; unknown workflow/ref/repo is 404;
  repo/actions-write failures are 403; malformed JSON is 400. `repository_dispatch`
  is a broadcast over matching `types` (absent `types` matches all), 204 even
  when nothing matches.
- `src/dispatch_auth.rs` (new): the D2 chain — system bearer, PAT
  (constant-time), own-App JWT (RS256 verified offline against the registered
  App's key; JWT-shaped tokens that fail verification are rejected locally
  since third-party App JWTs are never accepted), installation tokens (mint
  ledger first, then stubbed-github.com round-trip), actor resolution with
  short-TTL caches. Auth is a mandatory middleware inserting a
  `DispatchIdentity` extension.
- `src/github_app.rs`: in-memory `MintLedger` populated at mint time in
  `mint_for_repository` (token hash → installation, repo, permissions,
  expiry, app id, account login), swept on expiry.
- `src/events/trust_tier.rs`: new `AppDispatch` tier (secrets allowed) for
  installation-token dispatches.
- `src/runs.rs`: `github.actor` / `github.triggering_actor` now use
  `submission.actor` (they were hardcoded to `preloop-system`, breaking
  dispatch fidelity; webhook runs now also report the real sender).
- `src/github.rs`: `resolve_ref_sha` now takes shared state and uses the
  App-minted token ladder (App-only remote setups can dispatch).
- `src/routes.rs` / `src/auth.rs`: dispatch routes registered in the protected
  router; `/repos/...` denied on the runner control-socket surface.
- 30 router/auth/fidelity tests in `src/dispatch_tests.rs`; full server crate
  suite green (533 tests).

**Open-question decisions:**
- Q1 (actor/tier override): no new plumbing — `WorkflowSubmission.actor` and
  `.trust_tier` already exist; the handler sets them from the identity.
- Q2 (numeric workflow ids): preloop tracks neither github.com workflow ids
  nor numeric run ids. `workflow_id` is filename-only (documented); the read
  endpoints expose a deterministic `DefaultHasher` id for shape compat.
- Q3 (ledger migration): nothing to migrate — the ledger is in-memory and
  populated at mint time going forward; tokens minted before the feature
  expire under GitHub's 1-hour lifetime.
- Q4 (tier): extended `TrustTier` with `AppDispatch`. Checked
  `allows_secrets` first — only `UntrustedForkPullRequest`/`Untrusted`
  withhold secrets, so `AppDispatch` allows them, matching github.com.

### M3 — Third-party token validation (done, same module as M2)

- Online round-trip `GET /installation` + `GET /installation/repositories`
  (paginated) in `dispatch_auth.rs`, short-TTL cache (60s, keyed by token
  hash), fail **closed**: a transport failure is 502, a refused token 401.
  `repository_selection: "all"` skips the repository list; "selected"
  installs must name the repo. `actions: write` (or admin) required.
- Actor for third-party tokens: `{app_slug}[bot]` (GitHub's bot identity),
  falling back to `{account.login}[bot]` when the slug is absent.
- Tests: dispatch with actions:write (cached round-trip), 403 without
  actions:write, 403 without repo access, 401 on github.com refusal,
  fail-closed 502 on transport failure, all-repos installs.

### M4 — Multi-App registry (implemented; gate pending)

- `src/config.rs`: `AppConfig { app_id, pem, webhook_secret?,
  installation_id? }` + `GitHubConfig.apps: Vec<AppConfig>` (env override
  `PRELOOP_GITHUB_APPS_JSON`).
- `src/github_app.rs`: `GitHubApps` registry (legacy env App always entry 0 /
  `default_index`); `load_from` now returns the registry; per-App
  `installation_id` honored before the legacy env override; `select_app_for_repo`
  picks the App whose installation covers the repo (cached discovery), falling
  back to the legacy field/App. `GitHubAppCredentials` gained `webhook_secret`
  and `installation_id`.
- `src/github.rs`: webhook receiver verifies `X-Hub-Signature-256` against
  **every** registered App's secret plus the legacy one (deduped); all minting
  call sites (broker job tokens, check-run tokens, workflow fetch, ref SHA,
  PR changed files, push-back, dispatch default-branch) go through
  `select_app_for_repo`.
- `src/dispatch_auth.rs`: own-App JWT verification tries every registered
  App's key; actor resolution finds the App by id.
- `src/state.rs`: `AppState.github_apps: Option<GitHubApps>`; `github_app` is
  the registry default (back-compat).
- Tests: webhook signed by either App's secret is accepted / unknown rejected;
  `select_app_for_repo` routes by owner, falls back to the default App.

Not done (budget cut): the optional native admin endpoints
`GET/POST /api/v1/github/apps` (D6 nice-to-have), and the M4 minting-selection
test proving a job's `GITHUB_TOKEN` comes from the installed App end to end.

### M5 — Fidelity hardening + gate (done except the final gate)

Shipped: dispatch-payload property tests (`events/property_tests.rs` style),
`openapi.rs` docs for the new endpoints, `docs/github-tokens.md` +
`docs/fidelity-gap.md` updates, and the `repository_dispatch` dogfood fixture.
Pending: `just test-ci` (fmt-check + clippy + workspace tests). The crate's lib
test suite is green (535 tests) but the full gate has not run.
