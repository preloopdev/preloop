# Pull requests without webhooks (submit-driven CI)

Normally a PR's CI depends on GitHub delivering a webhook: push event → server creates the run. If GitHub's webhook pipeline is down,  **no runs get created at all.** CI goes dark even for merged PRs, and GitHub stops retrying missed deliveries after ~24h.

The submit-driven flow inverts the dependency. **You submit the CI run to**  
**the server directly; the server runs it; then — if you ask — it pushes the tested commit to GitHub and opens or updates the (draft) pull request.** GitHub is the *sink* for results, not the required *source* of triggers. PRs get
created and checks get reported with **no webhook involved at any point**.

## The commands


| Command                            | What it does                                                                        |
| ---------------------------------- | ----------------------------------------------------------------------------------- |
| `preloop run -f <workflow>`        | Run CI on the server. **Zero GitHub involvement** so it works during GitHub outages |
| `preloop run --push -f <workflow>` | Same, then push the tested commit + create/update the PR + report checks            |
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
1. work locally on branch feat/x        (tree must be CLEAN — see below)
2. preloop run --push --create-pr
3. server runs CI on your workspace snapshot        ← no GitHub needed
4. run reaches a terminal state (any conclusion)
5. CLI pushes the tested commit: git push <sha>:refs/heads/feat/x
6. no open PR? server creates one (draft) — or the push updated the PR
7. server verifies pushed tree == tested tree, reports check runs
8. GitHub unreachable? retries (1m/5m/15m), then `preloop push` replays
```

The invariants that make it honest:

- **The SHA that lands on GitHub is the SHA that was tested.** A dirty tree
is refused at submit (`--push requires a clean working tree`); the push is
pinned to the recorded `HEAD`; the server compares the pushed commit's
tree against the tested tree and blocks on mismatch.
- **No clobbering.** The push is a branch creation or a fast-forward only. A
diverged branch is refused with instructions — never a force-push.
- **Default branches are refused.** Pushing `main` is blocked client-side
(before the push) and server-side. Main stays webhook-driven.

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

- **The server must have GitHub App credentials.** The Checks API rejects
PATs (`403 You must authenticate via a GitHub App`). With App creds
(`preloop setup github --via app`, or `PRELOOP_GITHUB_APP_ID` + PEM) checks
pre-fill correctly; a PAT-only server still creates the PR but shows no
checks.
- The check's details link uses `PRELOOP_PUBLIC_URL` (set it to the address
GitHub viewers can reach; local-only servers show the summary text
instead).

PR creation needs the App or token to have `pull_requests: write` — grant
it only if you want the feature (the setup notes say exactly this).

## Circumstances


| Situation                          | What happens                                                                                                                                                                                         |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Local engine, same machine**     | Workspace path is shared; snapshot is instant; push runs from your checkout                                                                                                                          |
| **Remote server** (`PRELOOP_URL`)     | Same commands; the server runs against its own configured workspace (the local-workspace header is loopback-only so the server never reads your disk), and the push still happens from your checkout |
| **Branch not on GitHub yet**       | The push creates it at the tested commit; the PR is created against the repo's default branch                                                                                                        |
| **PR already open for the branch** | The push updates the branch; the PR follows; checks report on the new head                                                                                                                           |
| **GitHub down at completion**      | Run already finished (results local). Push-back retries 1m → 5m → 15m, then tells you: `preloop push <run_id>` later                                                                                 |
| **CLI interrupted mid-sync**       | The run keeps running; `preloop push <run_id>` resumes the publish step idempotently                                                                                                                 |
| **Detached submit** (`--detach`)   | Submit returns immediately; run `preloop push` when you want to publish                                                                                                                              |
| **Dirty tree**                     | Refused before submit: `--push requires a clean working tree… commit or stash first`                                                                                                                 |
| **Branch diverged on GitHub**      | Refused: rebase your branch onto the remote (or reset to the tested commit) and re-submit                                                                                                            |
| **Run failed**                     | Push-back still runs: the tested commit lands, the draft PR shows red checks — the reviewable state                                                                                                  |
| **New PRs as ready**               | `--pr-draft=false` — reviewers get notified on creation                                                                                                                                              |
| **PAT-only server**                | Runs + PR creation work; check runs 403 (needs an App)                                                                                                                                               |
| **No GitHub at all**               | Plain `preloop run` — CI completes, results in `preloop status` / `preloop logs`, nothing ever leaves your machine                                                                                   |
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
`docs/submit-driven-ci.md`.