# Submit-driven CI with GitHub push-back

Status: implemented (feat/submit-driven-ci). Webhooks remain the primary
trigger; this document describes the webhook-independent path for users who
want CI results on GitHub without depending on GitHub delivering an event.

## Problem

Runs are created from webhook deliveries only. If GitHub's webhook pipeline
is down or our delivery path is broken (tunnel, secret), no runs are created
for hours — CI goes dark even for merged PRs. GitHub retries deliveries for a
while, but eventually stops; missed events are gone forever.

## Model

Invert the dependency: the user submits CI to the server directly (existing
`preloop run`), the server runs it, and *afterwards* — if GitHub is
reachable — the flow pushes the tested commit to GitHub, creates or updates
the pull request, and reports check runs. GitHub is the sink for results,
not the required source of triggers.

```
1. work locally on branch feat/x          (tree must be CLEAN)
2. preloop run --sync [--create-pr]
3. server runs CI on the workspace snapshot
4. run reaches a terminal state
5. CLI pins the push to the tested SHA (never the branch tip)
6. if no open PR for the branch: server creates one (draft by default)
7. server reports check runs for the tested SHA (queued/in_progress/completed)
8. GitHub unreachable? CLI retries with backoff; `preloop sync <run_id>` replays
```

## Invariants

1. **The SHA that lands on GitHub is the SHA that was tested.** Enforced by
   the clean-tree gate (refuse to submit with a dirty tree when `--sync` is
   set), by pinning the push to the recorded `HEAD` SHA, and by the server's
   tree verification: the pushed commit's tree must equal the tree the
   snapshot was taken from (`submit_tree`).
2. **No clobbering.** The push is a fast-forward or a branch creation only.
   If the remote branch diverged from the tested commit, the sync refuses
   with instructions; a force-push never happens automatically.
3. **Idempotent sync.** `preloop sync <run_id>` may be re-run freely: the
   push becomes a no-op when the remote already points at the tested SHA,
   PR creation checks for an existing open PR first, and check runs are only
   created for jobs that lack one.

## Wire protocol

`WorkflowSubmission` (aksh-gha-protocol) gains two optional fields:

- `sync: Option<SyncRequest>` — present when the user asked for push-back.
  `SyncRequest { create_pr: bool, draft_pr: bool }`.
- `sync_tree: Option<String>` — `git rev-parse HEAD^{tree}` of the tested
  tree, used for verification.

The run record gains `sync_state: Option<SyncState>`
(`pending | synced | blocked` + error + PR number), surfaced by
`preloop status`.

## Components

| Piece | Location | Notes |
|---|---|---|
| Clean-tree gate, SHA/tree capture | `preloop-cli` `cmd_run` | `--sync` requires `git status --porcelain` empty; captures `HEAD` + `HEAD^{tree}` |
| Pinned push + divergence check + backoff | `preloop-cli/src/sync.rs` | `git push <sha>:refs/heads/<branch>`; remote missing / equal / ancestor → push or skip; diverged → refuse |
| Retry loop | `preloop-cli/src/sync.rs` | transient push/sync failures retried at 1m/5m/15m; permanent failures surface immediately |
| Check reporting for native runs | `aksh-runner-server` `submit_run` | same queued/completed loop the webhook adapter uses, gated on `sync` being set |
| PR create/update + tree verify | `aksh-runner-server/src/github_sync.rs` | `POST /api/v1/runs/:run_id/sync` |
| Manifest | `github.rs` | `pull_requests: write` (was `read`); `checks: write` already granted |

## Failure modes

| Failure | Behavior |
|---|---|
| GitHub unreachable at completion | CLI retries (1m/5m/15m); then instructs `preloop sync <run_id>` |
| Remote branch diverged | Sync blocked with error; user rebases and re-submits |
| Tree mismatch (pushed ≠ tested) | Sync blocked; never reports checks against an untested SHA |
| Commit not on GitHub yet | Sync blocked with "push the branch first" (commit lookup 404s) |
| Interrupted CLI during sync | Run keeps its state; `preloop sync <run_id>` replays idempotently |
| Run failed | Sync still proceeds (draft PR + red checks — reviewable state); `--pr-draft=false` to create ready PRs |

## Scope boundaries

- **Feature branches only.** Pushing `main` (or any default branch) is
  refused: main stays webhook + reconciliation-sweep driven.
- **CLI-push, not server-push.** The server never holds push power
  (`contents` stays `read`); the user's own git credentials perform the push.
- **`preloop run` only.** `aksh-runner-client submit` is unchanged (it has
  no workspace ownership); extend later if needed.
- Check run `details_url` honors `AKSH_PUBLIC_URL`; local servers without a
  public URL get the summary in the check's text instead.

## Verification

- Server: unit tests drive the sync endpoint against a mock GitHub API
  (`AKSH_GITHUB_API_URL` + `AKSH_GITHUB_TOKEN`), covering PR creation,
  tree verification, existing-PR reuse, and blocked states.
- CLI: git-level tests against a local bare remote (create / equal /
  fast-forward / diverged).
- E2E: `preloop run --sync` against a real repository with the App
  installation, then cleanup (close PR, delete branch).
