# fix(server): close the action-download oracle (declared-actions allowlist + short ticket TTL)

Stacked on `fix/round3-github-client-unify` (PR-H). Contains H's commits; this PR's own commit is on top.

## Finding (round-3 report §8)

`POST /_apis/v1/ActionDownloadInfo/…` accepted **any** `nameWithOwner` from any valid job runtime token and minted 6-hour bearerless HMAC ticket URLs for it. The engine then fetched the tarball from github.com on the operator's PAT/identity. The endpoint never checked whether the requested action was declared in the workflow — any job token turned the engine into an arbitrary-repo fetch oracle burning operator API quota.

## Fix

- **Declared-action allowlist.** Both resolution endpoints (`/_apis/v1/ActionDownloadInfo/:scope/:hub/:plan_id` incl. the `/:org` GHES alias, and `/actions/build/:orchestration_id/jobs/:job_id/runnerresolve/actions`) now resolve the run from the URL's plan/job id and mint tickets **only** for actions in the run's declared set. Anything else fails the whole batch with **403** naming the undeclared actions. An unresolvable plan/job id fails closed (403), never open.
- **Declared set** = every `owner/repo` (normalized, lowercased) in the run's stored workflow `uses:` — step-level, job-level reusable calls (transitively through local and already-fetched remote callee YAML), plus the workflow's own repository (for `$/` self-references, which the runner resolves through the forge). Derived from the run record's stored submission: no new persisted state, no migration, restart-safe. An unparseable stored workflow yields an empty set (fail closed).
- **Transitive composite closure.** Composite actions stage nested `uses:` in later waves, after the parent's own download — those nested repos are not in the workflow's direct set. When a batch names an undeclared action, the server consults the manifests of actions the run already resolved (fetched once per `(owner, repo, ref)` via the forge contents API, memoized, through the unified breaker with `Actions` accounting) and admits nested scopes to a fixpoint. Ordinary job setups (all-direct batches) never pay for a manifest fetch. A malicious workflow gains nothing: it could declare the nested repo directly.
- **Ticket TTL 6h → 20 min** (`ACTION_TICKET_TTL_SECS`). Tickets are minted at job setup and used within minutes; a leaked bearerless URL is now a briefly-open window.

## Tradeoffs (documented)

- **Refs are not pinned, owner/repo is.** A declared `actions/checkout` requested at any ref/tag/SHA is admitted. Pinning to declared refs would break legitimate dynamic usage (`uses: actions/checkout@${{ matrix.ref }}` and the runner's own ref→SHA wave), so enforcement is owner/repo-scoped per the task.
- **Expression-driven owners fail closed.** `uses: ${{ matrix.owner }}/repo@v1` cannot be statically resolved, so it is not in the declared set and its batch 403s. Static `owner/repo` with a dynamic ref is fine.
- **Restart vs the transitive ledger.** The direct set is recomputed from the stored workflow (restart-safe). The per-run "already resolved" ledger and the manifest memo are in-memory; after a restart, nested composite actions fail closed until the parent is re-resolved (direct actions always work). Jobs mid-setup across a restart already face disruption; accepted.
- **Token↔plan binding (not done, follow-up).** The allowlist keys off the plan/job id in the URL; it does not verify the bearer belongs to that job. A job token for run A requesting run B's plan id gets tickets only for B's declared actions (still no arbitrary-repo oracle), but cross-job confusion is possible. `require_protocol_bearer` intentionally accepts any valid local JWT here. Tightening that is a separate change.

## Verification

- New unit tests (`actions.rs`): normalization cases, direct-set collection (steps/reusables/self-repo), fail-closed on corrupt YAML, reusable-callee recursion, manifest nested-uses parsing (composite vs node/docker), TTL ∈ [15,30] min.
- New integration tests (`tests/runs_api.rs`): declared→200, undeclared→403 (single + mixed batch), unknown plan→403, unknown job→403, **real job runtime token** 403 on undeclared / 200 on declared, transitive composite nested admitted while unrelated repo 403s (mock forge).
- Updated existing `ActionDownloadInfo`/`runnerresolve` tests to address real plan/job ids.
- Live repro against a patched local server on port 19107 (state dir `.repro-state` in the worktree; `preloop store migrate` first; `PRELOOP_SYSTEM_TOKEN` set for auth):
  ```bash
  # 1. submit a workflow using actions/checkout@v4
  curl -s http://127.0.0.1:19107/api/v1/runs \
    -H "Authorization: Bearer $TOK" -H "Content-Type: application/json" \
    -d '{"workflow_yaml":"name: repro\non: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n","event":"push","repository":"owner/repo"}'
  # -> {"run_id":"4d340658-...","queued_jobs":1}
  # plan id (agent job id) from the control DB job_requests table:
  # ebff7321-a2f9-4a55-bab2-a9cc9ac5c1a7
  # 2. declared action -> 200 with a signed ticket URL
  curl -s -w "\nHTTP:%{http_code}\n" \
    "http://127.0.0.1:19107/_apis/v1/ActionDownloadInfo/s/h/$PLAN" \
    -H "Authorization: Bearer $TOK" -H "Content-Type: application/json" \
    -d '{"actions":[{"nameWithOwner":"actions/checkout","ref":"<40-char-sha>"}]}'
  # -> HTTP:200, actions map contains tarballUrl "...?exp=<now+1196s>&sig=..."
  # 3. undeclared repo -> 403 (single and mixed batch)
  curl -s -w "\nHTTP:%{http_code}\n" \
    "http://127.0.0.1:19107/_apis/v1/ActionDownloadInfo/s/h/$PLAN" \
    -H "Authorization: Bearer $TOK" -H "Content-Type: application/json" \
    -d '{"actions":[{"nameWithOwner":"someother/repo","ref":"<40-char-sha>"}]}'
  # -> HTTP:403 {"message":"action download not permitted: someother/repo@<sha>
  #    not declared in this run's workflow `uses:`", ...}
  # 4. runnerresolve behaves the same:
  #    /actions/build/$ORCH/jobs/$PLAN/runnerresolve/actions
  #    declared -> 200 with tar_url; undeclared -> 403
  ```
  TTL measured on a minted ticket: `exp - now` = 1196 s (~20 min), down from 21600 s (6 h). (Bearer was the system token; enforcement keys off the plan id identically for job tokens — the job-token path is covered by the integration test with a genuinely minted `preloop-job-*` JWT.)
- `cargo fmt` clean; `cargo clippy -p preloop-runner-server --all-targets` clean; full `tests/runs_api.rs` suite: 58 passed, 0 failed.

## Risk

Behavior change: workflows relying on resolving actions **not** declared in their `uses:` (e.g. composite actions with nested remote `uses:` — now covered by the transitive closure; expression-driven owners — fails closed with a clear 403 naming the action) will see 403s at job setup instead of tickets. The 403 message names the undeclared actions.
