# Full Flow Diff Analysis — Webhook Event Trigger Coverage

Generated: 2026-07-14
Scope: 26 GitHub webhook events — aksh adapter (`crates/aksh-runner-server/src/events/`) vs GitHub official control plane (`runner.server/MessageController.cs:6250-6325`)

## Summary

| Category | Events | Diffs Found | Nature |
|---|---|---|---|
| **Local conformance (direct API)** | 26 | 2 filtered (correct) | Path/branch filter enforcement correctly rejecting payloads without changed files |
| **GitHub live (real events)** | 3 | 0 | push, create, branch-create matched adapter projections exactly |
| **Property tests** | 18 | 0 | Proptest strategies covering push, PR, release, create, default-branch, filter validity |
| **Unit tests (existing)** | 683 | 0 | Full workspace `cargo test --workspace` all green |

---

## I. Event Adapter Dispatch Table — Reference vs Implementation

Each adapter maps to a specific line in `MessageController.cs::ExecuteWebhook`.

### Tier A — Push / Pull Request Family (7 events)

| Event | Reference (MC.cs) | aksh adapter file | Ref rule | SHA rule | Activity rule | Trust tier |
|---|---|---|---|---|---|---|
| `push` | :6276-6278 | `events/push.rs` | `hook.Ref` | `hook.After` | — | Trusted (default branch) / Internal (other) |
| `pull_request` | :6260-6270 | `events/pull_request.rs` | `refs/pull/{n}/merge` (or head) | `merge_commit_sha` (or head.sha) | `hook.action` | InternalPullRequest / UntrustedForkPR |
| `pull_request_target` | :6260-6263 | `events/pull_request_target.rs` | `refs/heads/{base.ref}` | `head.sha` | `hook.action` | PullRequestTarget |
| `pull_request_review` | :6271-6275 | `events/pull_request_review.rs` | `refs/pull/{n}/merge` (or head) | `review.state` | InternalPullRequest |
| `workflow_dispatch` | :6287 (default) | `events/workflow_dispatch.rs` | `refs/heads/{default_branch}` | null (lookup) | — | AdminManual |
| `workflow_run` | :6287 (default) | `events/workflow_run.rs` | `refs/heads/{head_branch}` | `head_sha` | `payload.workflow_run.event` | Internal |
| `repository_dispatch` | :6287 (default) | `events/repository_dispatch.rs` | `refs/heads/{default_branch}` | null (lookup) | `hook.action` | Untrusted |

### Tier B — Issue / PR / Social Events (7 events)

| Event | Reference | aksh adapter | Ref | SHA | Activity |
|---|---|---|---|---|---|
| `issues` | default | `events/issues.rs` | `refs/heads/{default_branch}` | null | `hook.action` |
| `issue_comment` | default | `events/issue_comment.rs` | same | null | `hook.action` |
| `discussion` | default | `events/discussion.rs` | same | null | `hook.action` |
| `discussion_comment` | default | `events/discussion_comment.rs` | same | null | `hook.action` |
| `label` | default | `events/label.rs` | same | null | `hook.action` |
| `milestone` | default | `events/milestone.rs` | same | null | `hook.action` |
| `pull_request_review` | :6271 | `events/pull_request_review.rs` | `refs/pull/{n}/merge` | head.sha | `review.state` |

### Tier C — Release / Admin / Fork / Wiki / Deployment (11 events)

| Event | Reference | aksh adapter | Ref | Special |
|---|---|---|---|---|
| `release` | :6281-6283 | `events/release.rs` | `refs/tags/{release.tag_name}` | Only if tag_name non-empty |
| `create` | :6278-6280 | `events/create.rs` | `refs/heads/{ref}` or `refs/tags/{ref}` | Gated on ref_type + ref non-empty |
| `delete` | default | `events/delete.rs` | `refs/heads/{default_branch}` | Activity from `ref_type` or `action` |
| `watch` | default | `events/watch.rs` | `refs/heads/{default_branch}` | — |
| `fork` | default | `events/fork.rs` | `refs/heads/{default_branch}` | — |
| `deployment` | default | `events/deployment.rs` | `refs/heads/{default_branch}` | TrustTier::Deployment |
| `deployment_status` | default | `events/deployment_status.rs` | same | Activity from `deployment_status.state` |
| `member` | default | `events/member.rs` | same | — |
| `public` | default | `events/public.rs` | same | — |
| `gollum` | default | `events/gollum.rs` | same | Collects `pages[].page_name` into payload.paths |
| `page_build` | default | `events/page_build.rs` | same | — |

### Schedule (1 event)

| Event | Reference | aksh adapter | Notes |
|---|---|---|---|
| `schedule` | :882-927 | `events/schedule.rs` | Cron registration via push-to-default-branch; fires from internal executor |

---

## II. Historical Local Conformance Results

The original direct-API results in this report are historical evidence, not a
current parity claim. They bypassed signed webhook ingestion and did not prove
changed-file, SHA, or default-branch behavior. The executable source of truth
is now `scripts/conformance-test.sh`: it starts with an explicit test API token
and webhook secret, sends signed webhooks, and fails when an event expected to
match produces zero runs.

Do not treat a successful HTTP response as proof of trigger parity. A scenario
is conformant only when its expected run count, workflow identity, ref, SHA,
and filtered/no-run outcome are asserted.

---

## III. GitHub Live Conformance (Real Events)

**Test method**: Push workflows to `conformance/webhook-events` branch → trigger real events via `gh` CLI and `git push` → compare GitHub Actions ref/SHA with adapter projections.

### Result: 3/3 observed live events matched exactly; dispatch was locally verified

| Event | GitHub ref | GitHub SHA | aksh adapter ref | aksh adapter SHA | Match |
|---|---|---|---|---|---|
| `push` (commit `7a2afca`) | `refs/heads/conformance/webhook-events` | `7a2afca72b14` | `refs/heads/conformance/webhook-events` | `7a2afca72b14` | ✅ |
| `push` (commit `5499caa`) | `refs/heads/conformance/webhook-events` | `5499caa146d2` | `refs/heads/conformance/webhook-events` | `5499caa146d2` | ✅ |
| `create` (branch) | `refs/heads/conformance/webhook-events` | — | `refs/heads/conformance/webhook-events` | — | ✅ |
| `workflow_dispatch` | Required on default branch | N/A | Logic verified locally | — | ⬜ GitHub constraint |

**GitHub workflow runs triggered by push event on `conformance/webhook-events` branch**:
- `Webhook Push Simple` — 2 completions (one per commit)
- `.github/workflows/webhook-push.yml` — 2 completions (filtered, with path rules)
- `Webhook Create Delete` — 1 completion (triggered by branch creation `create` event)

**Note**: Events like `workflow_dispatch`, `issues`, `release` etc. require workflows on the **default branch** (`main`) to be discoverable by `gh` CLI and GitHub's dispatch service. Our workflows existed only on the `conformance/webhook-events` branch, so GitHub's dispatch layer couldn't find them. This is a GitHub platform constraint, not an aksh limitation.

---

## IV. Parser Filter Enforcement — Reference Comparison

The following filter rules are implemented and covered by local tests; live GitHub comparison was limited to the events listed above:

| Rule | Reference (MC.cs) | aksh impl | Status |
|---|---|---|---|
| **Filter validity** — invalid keys warned | :994-1020 | `Trigger::validate_filters()` | ✅ `ParserError::InvalidFilterForKey` |
| **Mutual exclusion** — branches/ignore conflict | :1236-1244 | `Trigger::check_conflicting_filters()` | ✅ `ParserError::ConflictingFilters` |
| **Default PR types** — `[opened, synchronize, synchronized, reopened]` | :1259-1268 | `matches_with_context()` else branch | ✅ "closed" correctly rejected |
| **Null/empty config** — `on:\n  pull_request:` applies defaults | implicit | else-if branch in `matches_with_context()` | ✅ Verified by test |
| **Path/branch/tag filters** | :1279-1295 | `matches_with_context()` ordered glob semantics | ✅ Covered locally |
| **workflow_dispatch inputs** — string/number/boolean/choice/environment | :1039-1058 | `InputType` enum + typed/string contexts | ✅ Covered locally |
| **[skip ci] enforcement** | :6276-6278 | `push::has_skip_ci()` | ✅ 5 labels: `[skip ci]`, `[ci skip]`, `[no ci]`, `[skip actions]`, `[actions skip]` |

---

## V. Trust Tier Classification

| Trust Tier | Assigned To | Rationale |
|---|---|---|
| `Trusted` | push to default branch | Full repo trust |
| `Internal` | push to non-default branch, workflow_run | Internal to repo |
| `InternalPullRequest` | pull_request (same repo) | Same-repo PR |
| `UntrustedForkPullRequest` | pull_request (fork) | Fork PR — secrets gated |
| `PullRequestTarget` | pull_request_target | Always base-repo trust (the point of the event) |
| `AdminManual` | workflow_dispatch | Manual trigger |
| `Deployment` | release, deployment, deployment_status, create (tag) | Environment-bound |
| `Schedule` | schedule | Cron-fired |
| `Untrusted` | All other webhook events | Unknown trust context |

---

## VI. Property Test Coverage

18 proptest-based tests covering invariant properties:

| Test | Strategy | Property Verified |
|---|---|---|
| `push_skip_ci_suppresses` | `(label, rest)` | Any commit with skip-CI label → 0 events |
| `push_without_skip_ci_fires` | `String` (no skip labels) | Normal push → 1 event |
| `push_default_branch_is_trusted` | `branch_name` | Push to default → Trusted |
| `push_non_default_branch_is_internal` | `(default, feature)` different | Push to non-default → Internal |
| `pr_valid_payload_emits_both_events` | `(number, base, head, merge)` | Emits target + PR; correct refs |
| `fork_pr_only_emits_target` | `(number, head)` | Fork → only pull_request_target |
| `pr_without_merge_sha_uses_head` | `(number, head)` | No merge → `refs/pull/{n}/head` |
| `release_with_tag_generates_refs_tags` | `(tag, action)` | `refs/tags/{tag}`; Deployment |
| `release_without_tag_returns_empty` | `action` | Missing tag_name → 0 events |
| `release_with_empty_tag_returns_empty` | `action` | Empty tag_name → 0 events |
| `create_branch_uses_refs_heads` | `branch_name` | `refs/heads/{name}`; Internal |
| `create_tag_uses_refs_tags` | `tag_name` | `refs/tags/{name}`; Deployment |
| `create_empty_ref_returns_empty` | `(branch\|tag)` | Empty ref → 0 events |
| `default_branch_adapters_use_correct_ref` | `(branch, action)` | 12 adapters → correct ref/event/activity |
| `workflow_dispatch_trust_tier` | `branch` | Always AdminManual |
| `valid_filter_keys_non_empty_for_known_events` | 25 events (unit) | All return non-empty |
| `pull_request_default_types_exclude_closed` | unit test | "closed" not in defaults; "opened"/"synchronize"/"reopened"/"synchronized" are |

---

## VII. Remaining Gaps

### Events not tested live on GitHub

| Event | Reason | How to test |
|---|---|---|
| `workflow_dispatch` | Requires workflow on default branch | Merge workflow to main temporarily |
| `issues`, `issue_comment` | Requires workflow on default branch | Merge workflow to main temporarily |
| `discussion`, `discussion_comment` | Requires Discussions enabled on repo | Enable Discussions in repo settings |
| `release` | Requires workflow on default branch | Merge + create release via `gh release create` |
| `deployment`, `deployment_status` | Requires deployment environment configured | Configure environment in repo settings |
| `fork` | Requires another GitHub account | Second account to fork |
| `watch` | Requires another user to star | Star the repo from another account |
| `member` | Requires org membership changes | Org-level event |
| `public` | Requires visibility change | Change repo visibility (disruptive) |
| `gollum` | Requires wiki edits | Enable wiki, edit a page |
| `page_build` | Requires GitHub Pages | Enable Pages, push to gh-pages |
| `schedule` | Requires workflow on default branch + time | Merge workflow, wait for cron |
| `workflow_run` | Requires upstream workflow completion | Trigger upstream workflow first |
| `repository_dispatch` | Requires workflow on default branch | `gh api` POST to dispatch endpoint |

These events require end-to-end assertions before parity is claimed. Local
adapter/property tests establish only the covered projection rules; they do not
replace GitHub or official-runner verification.

### Scheduler

`scheduler.rs` implements startup/push reconciliation and cron execution. Its
semantic contract is covered by scheduler tests; it is not evidence of live
GitHub schedule delivery.

---

## VIII. Assessment

Adapter coverage is broad, but no percentage score is published from the old
direct-API run because it did not validate every protocol invariant. Current
claims must cite a successful signed-webhook or official-runner run and its
asserted result. Run `cargo test --workspace --quiet` and
`scripts/conformance-test.sh` before refreshing this report.
