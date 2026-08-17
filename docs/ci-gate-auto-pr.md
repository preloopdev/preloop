# CI Gate + Auto-PR

How preloop gates pushes with CI and opens pull requests after a green run.
Three complementary flows, all ending in "the tested commit is on GitHub with
a pull request when CI passed":

1. **Committed flow — push-back**: `preloop run --push [--create-pr]` runs CI,
   then the client pushes the tested commit and the server verifies
   `pushed tree == tested tree`, reuses/creates the branch's pull request, and
   reports check runs. `github_push.rs` + `preloop-cli/src/push.rs`.
2. **Seamless webhook flow — auto-PR**: a plain `git push origin feature/x`
   delivers a push webhook; when the run succeeds, the server opens the pull
   request itself per policy. No CLI step. `github_pr.rs`.
3. **Dirty-tree flow**: `preloop run --push` on uncommitted changes runs CI on
   the server's snapshot of the working tree, then on green materializes a
   real commit from the exact tested tree, pushes it, and (per the prompt or
   flags) opens the PR. The commit's author is the developer's git identity.

Plus a **pre-push hook** (`contrib/pre-push`, auto-offered by the CLI) that
makes flow 1's gate automatic for plain `git push`.

## Config

Server auto-PR policy (`config.toml` `[github.pr]` or environment):

| Key | Env | Default | Meaning |
|---|---|---|---|
| `auto` | `PRELOOP_GITHUB_PR_AUTO` | `feature` | `feature`: open PRs for non-default, non-excluded branches with no open PR. `never`: never open automatically; a `[pr]` head-commit label still opens one. |
| `draft` | `PRELOOP_GITHUB_PR_DRAFT` | `true` | Open newly-created PRs as drafts. Unknown env values warn and keep the configured value. |
| `exclude` | `PRELOOP_GITHUB_PR_EXCLUDE` | — | Comma-separated gitignore-style branch patterns never to open a PR for (`*` matches any run of characters; a trailing `/` matches everything below a prefix). |

Head-commit message labels override policy: `[no-pr]` skips (always), `[draft]`
opens as draft, `[pr]` forces an open even under `auto = never`. Labels are
read from the **head** commit only — an older commit in the same push cannot
suppress or force the current head's PR.

Only webhook-delivered runs trigger auto-PR: native `/api/v1/runs`
submissions carry no trust tier and are never auto-PR'd, and push-back runs
(`submission.push` set) are client-managed — `github_push.rs` owns their PR.
The server needs the GitHub App `pull_requests: write` permission (or a PAT)
to create PRs; without credentials the run still succeeds and the PR is
simply not opened.

## Push-back verification (`POST /api/v1/runs/:run_id/push`)

Every step is idempotent (`preloop push <run_id>` replays freely):

1. **Tree verification** — the pushed commit's tree must equal the tested
   tree. A clean submission pins `commits/{sha}` directly (no branch-tip
   fallback: a tree-only match on a different commit must not publish checks
   for untested work). A dirty submission recorded the snapshot tree at
   accept time; the client materializes its commit *after* the run, so the
   server verifies the **branch head**, which is the authoritative commit the
   client pushed.
2. **PR** — reuse an open PR for `owner:branch`, else create one when
   requested (`--create-pr`, or a `{create_pr, draft}` body override —
   non-boolean override values are rejected with a 400, never silently
   dropped).
3. **Checks** — jobs that lack a check run get queued + completed check runs
   against the *effective* head commit (the materialized commit for dirty
   runs; submit-time checks are only created for clean runs, where the head
   is known up front).
4. The published commit is recorded (`push_state.effective_sha`) so the push
   webhook echo of the materialized commit is recognized by
   `already_published` and does not re-run CI.

## Dirty-tree flow

`preloop run --push` on a dirty tree:

- The server snapshots the workspace at accept time and records the snapshot
  tree as the tested tree. If the snapshot fails, the submission is rejected
  loudly — a push-requested run without a tested tree can never be pushed.
- After CI, the CLI materializes a commit whose tree is exactly the tested
  tree (`git commit-tree`, parented on the base commit, authored by the local
  git identity). The tested tree's objects are reproduced from the working
  tree with a private index (the user's staging area is untouched); if the
  working tree changed since submission so the tested tree can no longer be
  reproduced, the push fails with a re-submit hint instead of pushing
  something untested.
- The PR decision comes after CI: explicit `--create-pr` > head-commit labels
  > interactive `[y/N/d]` prompt > safe default. A non-interactive explicit
  `--push` (without `--create-pr`) still pushes the tested tree, leaving
  `create_pr` false.

## Pre-push hook

`contrib/pre-push` (or let `preloop run` install it) is a **soft, advisory
gate**: on `git push` of the checked-out branch's current commit it holds the
push open while CI runs and aborts the push when CI fails. Other refs
(non-`HEAD` branches, tags, deletions) pass with a warning — the tree CI
would test is not the tree being pushed. The hook never pushes anything
itself.

- `[skip ci]` in any pushed commit bypasses the gate (all-zero remote SHAs on
  new branches are handled so the check works there too).
- Verdicts are cached per `(remote, server endpoint, commit sha)`, recording
  the run id; a re-push validates the recorded run's live terminal status
  (success → skip, failure → block, in progress → wait, unknown → re-run).
  An interrupted hook resumes the run it already started instead of
  duplicating CI.
- **Fail-open**: when preloop itself is unreachable the push proceeds with a
  loud warning. Unreachability is detected from the CLI's machine-readable
  `PRELOOP_UNREACHABLE` marker — never from CI step output that happens to
  mention a network error, and never from Git push errors (auth, permissions)
  that would otherwise bypass the gate.
- Install preserves an existing pre-push hook: the previous hook is backed up
  to `pre-push.preloop-prev` and chained (run first, verdict authoritative),
  and `core.hooksPath` is respected.

This is a soft gate by design: it is advisory per machine. Anyone can push
without the hook (web UI, another machine); server-side enforcement is a
separate concern.
