# Pull requests without webhooks (submit-driven CI)

Webhook-triggered CI depends on GitHub delivering the event. GitHub does not
automatically retry failed webhook deliveries; an administrator can manually
redeliver a retained delivery. The submit-driven flow lets you submit a run
directly to Preloop, then optionally publish the tested commit and create or
update a pull request. This avoids needing a webhook to start that particular
run; publication and Check Run reporting still require GitHub access and
credentials.

## The commands


| Command                            | What it does                                                                        |
| ---------------------------------- | ----------------------------------------------------------------------------------- |
| `preloop run -f <workflow>`        | Submit a run directly without a webhook trigger; GitHub access may still be needed for workflow or job operations |
| `preloop run --push -f <workflow>` | Same, then push the tested commit + create/update the PR; publishing Check Runs requires a GitHub App with `checks: write` |
| `preloop run --push --create-pr`   | Push-back **and** create a PR when the branch has none open                         |
| `preloop push`                     | Replay push-back for the **most recent** run (idempotent so safe to re-run)         |
| `preloop push <run_id>`            | Replay for a specific run                                                           |


Flags on `preloop run`:

- `--push` — publish the run's result to GitHub after it completes
- `--create-pr` — implies `--push`; create a draft PR if the branch has no
open one
- `--pr-draft=false` — create new PRs as ready instead of draft

## The flow

```
1. work locally on branch feat/x        (dirty trees are snapshotted; see below)
2. preloop run --push --create-pr
3. server runs CI on your workspace snapshot; no webhook is needed to start the run, though workflow or job operations may still need GitHub access
4. run reaches a terminal state (any conclusion)
5. CLI pushes the tested commit: git push <sha>:refs/heads/feat/x
6. no open PR? server creates one (draft) — or the push updated the PR
7. server verifies pushed tree == tested tree, reports check runs
8. GitHub unreachable? retries (1m/5m/15m), then `preloop push` replays
```

The invariants that make it honest:

- **The SHA that lands on GitHub is the SHA that was tested.** A dirty tree
is snapshotted at submit, then materialized into a commit after CI passes.
The server compares the pushed commit's tree against the tested tree and
blocks on mismatch.
- **No clobbering.** The push is a branch creation or a fast-forward only. A
diverged branch is refused with instructions — never a force-push.
- **Default branches are refused.** Pushing `main` is blocked client-side
(before the push) and server-side. Main stays webhook-driven.
- **The work is not repeated.** Landing the commit makes GitHub send a push
webhook for that very commit, which would otherwise re-run the workflow the
server just finished and report a second set of check runs over the ones
push-back published. The server recognises the echo of its own push and skips
it. The `pull_request` echo that opening the PR emits is skipped too, but only
when a `pull_request` run that tested the merge with the base already published
that commit; a run that tested the branch alone leaves the PR's merge test to
run. The match is deliberately narrow — same commit *and* same workflow
file, and only against a completed run that requested push-back — so a
workflow you never submitted still runs when the branch lands, and an
ordinary webhook redelivery is still honoured as the retry it is.

> **Single-user.** `preloop push` with no run id resolves to the server's most
> recent run, not to yours, and the server has no per-user identity. See
> [One engine, one user](setup.md#one-engine-one-user-for-now).

## The run ID

`preloop push` defaults to the most recent run, so you usually never need
it. When you do:

- the submit line: `Run 3c9759be-… created (1 jobs queued)`
- `preloop status` — the `RUN ID` column (`--json` for scripts)
- error hints print the exact command: `rerun: preloop push 3c9759be-…`

## Checks on the PR

Check runs are reported through GitHub's Checks API and appear on the
commit/PR: `queued` at submit → `in_progress` while running → `completed`
with the conclusion. Two requirements:

- **Use GitHub App credentials with `checks: write`.** This is the supported
  configuration for Check Run reporting. The implementation may have other
  token fallback paths, but do not rely on a static PAT as a substitute; a
  PAT-only setup can leave checks unpublished even if commit or PR operations
  succeed.
- The check's details link uses `PRELOOP_PUBLIC_URL` (set it to the address
GitHub viewers can reach; local-only servers show the summary text
instead).

PR creation needs the App or token to have `pull_requests: write` — grant
it only if you want the feature (the setup notes say exactly this).

## Circumstances


| Situation                          | What happens                                                                                                                                                                                         |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Local engine, same machine**     | Workspace path is shared; snapshot is instant; push runs from your checkout                                                                                                                          |
| **Remote server** (`PRELOOP_URL`)     | Same commands; the CLI only sends the local-workspace header to loopback servers, so a remote server runs against its own configured workspace, and the push still happens from your checkout |
| **Branch not on GitHub yet**       | The push creates it at the tested commit; the PR is created against the repo's default branch                                                                                                        |
| **PR already open for the branch** | The push updates the branch; the PR follows; checks report on the new head                                                                                                                           |
| **GitHub down at completion**      | Run already finished (results local). Push-back retries 1m → 5m → 15m, then tells you: `preloop push <run_id>` later                                                                                 |
| **CLI interrupted mid-sync**       | The run keeps running; `preloop push <run_id>` resumes the publish step idempotently                                                                                                                 |
| **Detached submit** (`--detach`)   | Submit returns immediately. If submitted with `--push`, the requested publish step still follows the run; otherwise publish later with `preloop push` |
| **Dirty tree**                     | Snapshotted before CI and materialized only after a successful run; re-submit if the working tree changes before push                                                                                                                 |
| **Branch diverged on GitHub**      | Refused: rebase your branch onto the remote (or reset to the tested commit) and re-submit                                                                                                            |
| **Run failed**                     | Clean-tree push-back still publishes red checks; dirty-tree snapshots are not materialized after a failed run                                                                                   |
| **New PRs as ready**               | `--pr-draft=false` — reviewers get notified on creation                                                                                                                                              |
| **PAT-only server**                | Some GitHub operations may work; configure an App with `checks: write` for reliable Check Run reporting |
| **No webhook available**           | Plain `preloop run` can submit and report results locally; workflow loading, actions, or credentials may still require GitHub/network access. Nothing is published unless you request push-back. |
| `**--sync` era flag**              | The flag was renamed: `--push` (and `preloop push`)                                                                                                                                                  |


## Troubleshooting


| Symptom                                           | Cause / fix                                                                                                         |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `run was not submitted with --push`               | The run predates the flag or was submitted without it — re-submit with `--push`                                     |
| `commit … is not present in this checkout`        | Run `preloop push` from the checkout the run was submitted from (the sha must exist locally)                        |
| `refusing to push branch main…`                   | Push-back is for feature branches — main stays webhook-driven                                                       |
| `…has commits that are not ancestors…`            | The remote branch diverged — rebase/reset and re-submit; push-back never force-pushes                               |
| `tested tree … does not match pushed commit tree` | The branch wasn't pushed from the tested commit — re-submit after pushing                                           |
| Push fails with auth errors                       | Your git credentials (the push is *your* git push; the server never holds push power)                               |
| Check runs 403                                    | The server has no GitHub App credentials — configure the App                                                        |
| PR creation 403                                   | The App/token lacks `pull_requests: write` — grant it (App settings → Permissions → Pull requests → Read and write) |
| `no pull request created`                         | `--create-pr` wasn't passed and no PR exists — pass it, or the branch's PR is closed                                |


## Verification

```sh
preloop run --push --create-pr --no-debug -f .github/workflows/ci.yml
# watch: ✓ run → "pushed <sha> to origin/<branch> (branch created)"
#        → "synced: PR https://github.com/<repo>/pull/<n>"
git ls-remote origin refs/heads/<branch>     # branch exists at the tested sha
gh pr list --head <branch>                   # draft PR open
```

The no-webhook guarantee: a plain `preloop run` completes with **zero**
GitHub API calls and nothing pushed — verify by watching the server logs
and `git ls-remote`. Design details and the reconciliation backstop are in
`docs/ci-gate-auto-pr.md`.