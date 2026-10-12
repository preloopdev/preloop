# Pre-push checks for coding agents

A coding agent can run the check jobs of a repository's own workflows with
`preloop run` before it pushes. A failing test or lint job then stops the push
on the machine, and the agent reads the log or opens the failed VM instead of
waiting for a CI run on GitHub.

For people, [`contrib/pre-push`](../contrib/pre-push) gates `git push` with a
full `preloop run` (see [CI Gate + Auto-PR](ci-gate-auto-pr.md#pre-push-hook)).
This page covers a narrower loop for agents: run the jobs that check code, one
at a time, and leave out the jobs that deploy.

## Install

Install a tagged release and verify it before an agent uses it:

1. Download `preloop-cli-<target>.tar.gz` and its `.sha256` from a
   [release](https://github.com/preloopdev/preloop/releases) and verify the
   checksum.
2. Verify the build provenance:
   `gh attestation verify preloop-cli-<target>.tar.gz --repo preloopdev/preloop`.
3. Put `preloop` on your `PATH` and run `preloop update` to install the pinned
   SmolVM runtime.
4. On Linux, also install the guest runner every job VM needs:
   `preloop-runner-<triple>` from the same release, verified the same way, at
   `<prefix>/lib/preloop/runner/<triple>/preloop-runner`. `preloop update`
   places it on macOS only. [Self-hosting §3](self-hosting.md#3-install) has
   the commands.
5. Run `preloop init` yourself. Its first step sets up a GitHub credential,
   and the agent should never handle that credential.

## Steps

1. **Commit first.** `preloop run` snapshots the working tree, uncommitted
   changes included. Commit before the run so the tested tree is the commit
   that will be pushed.
2. **Read the job graph** of each workflow that the push or the pull request
   triggers:

   ```sh
   preloop plan -f <file> --json
   ```

3. **Pick the check jobs:** tests, lint, build, and any gate job that collects
   them. Skip every job that deploys, publishes, pushes, or reads a secret
   other than `GITHUB_TOKEN`. `environment:`, `secrets.`, `deploy`, `release`,
   and `pages` in a job are the usual signs.
4. **Run each check job:**

   ```sh
   preloop run -f <file> --job <job> --event pull_request --base origin/main --strict-secrets
   ```

   `--job` also runs the job's `needs:` closure, so check the plan for deploy
   jobs in that closure too. Use `--event push` for workflows that only
   trigger on push. `--strict-secrets` fails the run before it starts when the
   workflow reads a secret this engine has not stored. The check reads every
   job in the file, including jobs that `--job` leaves out. When it fails on a
   secret that only a skipped job reads, run the check job without
   `--strict-secrets` and read the warning it prints.
5. **On a failure:**
   - Read the failed job's log: `preloop logs --job <job>`.
   - To inspect the failed VM, run the job again with `--preserve-on-failure`
     and open the VM with `preloop shell`.
   - After a fix, commit and run the job again with `preloop run`.
     `preloop rerun <run-id> --failed` re-runs the failed jobs on the tree the
     first run captured. Use it for a flaky failure; it does not pick up a fix.
6. **Push only when every selected job passed.**

## Rules for the agent

- Never pass `--push` or `--create-pr`. Pushing and opening pull requests stay
  with the normal flow and the person who owns the change.
- Never pass a real secret as `--secret NAME=VALUE`. The value lands in shell
  history and in the agent's transcript. Let the person store it once with
  `preloop secret set NAME`, which reads the value from stdin (hidden on a
  TTY), or skip the job.
- A local pass does not replace CI. The run on the pull request stays the
  record, and the local run only stops a red push early.

Every flag above is listed in the [CLI reference](cli_reference.md). An
example of this as an agent skill:
[`pre-push`](https://github.com/uncovertechtalent/cto-powersuit/blob/main/plugins/sdlc/skills/pre-push/SKILL.md).
