# CI Gate + Auto-PR — Implementation Plan

Status: Plan (implementation start)
Branch: `Bnjoroge/ci-gate-auto-pr`

## 1. Goal

Three complementary flows, all ending in "PR opened on GitHub after CI passes":

1. **Committed flow (exists)**: `preloop run --push --create-pr [--pr-draft]` — CI on a
   clean tree, client pushes the tested commit, server verifies tree == tested tree,
   creates/reuses the PR, reports check runs. `github_push.rs` + `preloop-cli/src/push.rs`.
   Missing: a **pre-push hook** so plain `git push` runs this instead of a CLI step.
2. **Seamless webhook flow (new — M1)**: plain `git push origin feature/x` → push webhook →
   CI on preloop → **server opens the PR automatically** when the run succeeds, per policy.
   No hook, no CLI step. Soft gate (branch exists on GitHub before CI — accepted).
3. **Dirty-tree flow (new — M2)**: CI on uncommitted changes (server snapshot), then on
   green the client **materializes a real commit from the tested snapshot**, pushes it,
   and opens the PR. Interactive `[y/N/d]` prompt; author = developer's git identity.

Decisions from design review (all accepted):
- The gate is **soft** (hooks are advisory; web UI / other machines bypass). No server-side
  enforcement in this milestone.
- **Author identity** = the developer's local git config (`user.name`/`user.email`), passed
  through `submission.actor` and used by `git commit-tree` for the materialized commit.
- **Retroactive check runs**: already handled — push-back reports check runs for the pushed
  SHA (github_push.rs step 4); `already_published` suppresses the webhook re-run.
- **Resumable gate**: the hook caches the run id per HEAD SHA; a re-push reuses a completed
  run instead of re-running CI.
- **Fail-open**: hook exits 0 with a loud warning when preloop is unreachable (configurable).

## 2. Current state (verified)

- `github_push.rs`: `POST /api/v1/runs/:run_id/push` — verifies tested tree == pushed tree,
  resolves default base, refuses default branch (feature-only backstop), reuses open PR by
  `head=owner:branch`, creates PR when `submission.push.create_pr` (with `draft_pr`), reports
  missing check runs, marks `PushState::Synced`. Idempotent. Helpers: `push_token`,
  `github_json`, `classify`, `validate_push_target`.
- `preloop-cli/src/main.rs` run command: `--push`, `--create-pr`, `--pr-draft`; **refuses a
  dirty tree** ("--push requires a clean working tree so the pushed commit is exactly what
  was tested"). Sets `submission.sha`, `.push_tree`, `.actor` (from `git_config_user_name`).
- `preloop-cli/src/push.rs`: `push_tested_commit` (with merge-base safety, refuses diverged
  branch, refuses default branch), `push_run` retry loop.
- Server snapshot: `runs.rs` `create_workspace_snapshot` builds a synthetic commit from the
  local workspace (respects .gitignore, includes untracked non-ignored); snapshot
  `commit_sha` exists; checkout served via preloop git smart-HTTP.
- Run completion: `broker.rs::broker_complete_job` (jobs); run reaches terminal conclusion
  when the last job completes.
- Skip labels: `[skip ci]`-family parsed from push payload commit messages
  (`events/push.rs`).

## 3. M1 — Server: webhook-driven auto-PR (new module `github_pr.rs`)

### Config (`config.rs::GithubConfig`)
```rust
pub struct PrConfig {
    pub auto: PrAuto,          // feature (default) | always | never   env PRELOOP_GITHUB_PR_AUTO
    pub draft: bool,           // default true                        env PRELOOP_GITHUB_PR_DRAFT
    pub exclude: Vec<String>,  // branch patterns (gitignore-style)   env PRELOOP_GITHUB_PR_EXCLUDE
}
```
Keep push-back's explicit `--create-pr` behavior unchanged (client-managed).

### Policy (`github_pr.rs`)
`pr_decision(shared, run, payload) -> PrDecision { Open { draft }, Skip(&'static str) }`:
- Only runs with `event == "push"` delivered by webhook (not `submission.push` set — those
  are client-managed; not local-only synthetic submissions with no real owner/repo slug —
  `validate_push_target` rules).
- Only `conclusion == success`.
- Branch = payload ref (`refs/heads/...`), not the default branch, not a tag, not excluded
  by patterns.
- No existing open PR (`GET pulls?head={owner}:{branch}&state=open`) — dedup.
- Head-commit message labels override: `[no-pr]` → Skip; `[draft]` → Open draft;
  `[pr]` → Open (even if `auto = never`). Precedence: label > prompt-equivalent > config.
- `auto = never` + no `[pr]` → Skip.

### Completion hook
In the run-completion path (where the last job's conclusion finalizes the run): if the run
matches the webhook-push criteria and succeeds → spawn a **best-effort async task** that runs
`pr_decision` then `POST pulls` with the minted token (`pull_requests: write`). PR body
mirrors push-back's (CI run details + details URL). Errors are logged, never fail the run.
Skip if no App/PAT configured.

### Tests
- `pr_decision` unit tests: feature/main/tag/excluded/`[no-pr]`/`[draft]`/`[pr]`/dedup.
- Router-level: webhook push → run completes → PR created (stubbed api.github.com, pattern
  from `dispatch_tests.rs`); 403 without `pull_requests: write`; no PR when policy skips.

## 4. M2 — CLI: dirty-tree interactive prompt flow

### Server: expose the tested snapshot tree
`submit_run_inner`: when a push-requested submission has a dirty tree (no explicit
`push_tree`) but the local-workspace snapshot exists, record the snapshot commit's tree as
`push_tree` (server knows it — snapshot is built before submit completes). Then
`POST /api/v1/runs/:run_id/push` tree-verification works for dirty runs.

### CLI (`preloop-cli/src/main.rs` run command)
- Replace the hard dirty-tree bail: when `--push`/`--create-pr` and the tree is dirty,
  proceed (CI runs on the snapshot; server records `push_tree`). Keep the clean-tree fast
  path unchanged.
- After the run reaches success:
  - Interactive (`stdin` is a TTY) and not `--create-pr`-explicit: prompt
    `Commit these changes and open a PR? [y/N/d]` (after printing the snapshot summary:
    modified/untracked counts — reuse `git_porcelain`).
  - Non-interactive: honor head-commit labels `[pr]`/`[draft]`/`[no-pr]`; else skip
    (safe default) unless `--create-pr`.
- Materialize (new fn in `preloop-cli/src/push.rs` or `commit.rs`): `git commit-tree
  <tested_tree> -p <local HEAD> -m <msg>` where `<tested_tree>` comes from the run record
  (`push_tree`), msg from policy (latest commit message + CI marker) or a flag; author from
  local git config (`git_config_user_name`/`user.email` — the developer's identity).
- Then the existing path: `push_tested_commit(new_commit, branch)` + `push_run` (POST push
  endpoint → verifies tree == tested tree, opens PR with `draft` per answer).
- Local tree stays untouched; commit exists only on the pushed branch.

### Tests
- Unit: materialize fn (bare-repo fixtures like `push.rs` tests), prompt decision table
  (label precedence), dirty-tree submission carries `push_tree` from the snapshot.

## 5. M3 — Pre-push hook (soft gate, committed flow)

`contrib/pre-push` (shell, documented): on `git push origin <branch>`:
- Skip entirely when the pushed commits' messages contain `[skip ci]`.
- Run `preloop run --push --create-pr [--pr-draft per config/label]`.
- Resumable: cache run id per HEAD SHA in `$HOME/.cache/preloop-push-<sha>`; if a cached
  terminal run exists, reuse instead of re-running.
- Fail-open: preloop unreachable → warn loudly, exit 0 (push proceeds without gate).
- CI failure → exit 1 (push aborted, branch never reaches GitHub).
- On success exit 0 — preloop already pushed the commit; git's own push no-ops
  ("Everything up-to-date").

## 6. M4 — Docs, gate, dogfood, PR

- `docs/github-tokens.md` / `docs/github-app-webhook.md`: the three flows + hook install.
- `fixtures/workflows/`: a push-PR dogfood workflow (if needed).
- `just test-ci` green in the worktree.
- Dogfood: local E2E — serve, submit dirty-tree run through the new flow against a local
  bare "origin", verify commit == tested tree and PR-open call shape.
- Open the feature PR on GitHub (this branch).

## 7. Non-goals
- Server-side enforcement of the CI-first gate (hook is advisory by decision).
- GitHub branch protection / status-check wiring for the gate.
- Webhook auto-PR for non-push events (PRs, reviews) — push only this milestone.
