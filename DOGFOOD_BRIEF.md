# Task: dogfood the preloop CLI, then run 4-repo conformance

You are working in the Orca worktree `/Users/bnjoroge/preloop-cli-dogfood`
(branch `Bnjoroge/cli-dogfood`). It already contains a round of review fixes on
top of `conformance-campaign-fixes`.

`aksh`/`preloop` is a Rust reimplementation of the GitHub Actions control plane
plus the official `actions/runner`. Read `AGENTS.md` and `REVIEW.md` first.

## Ground rules

- `just test-ci` is the gate. Run it before you claim any fix is done.
- Protocol fidelity beats everything. Never change what bytes a runner sees
  without checking the official runner behavior.
- Every bug you claim MUST have a reproduction: the exact command and its
  output, before and after. `REVIEW.md` section 1a is binding.
- Commit in small, self-describing commits on this branch. Never force-push.
- Do not touch `/Users/bnjoroge/preloop` (the parent worktree).

## Phase 1 — drive every preloop CLI feature as a human would

Build first: `cargo build -p preloop-cli -p aksh-runner-server -p aksh-runner`.

Walk the entire surface of `./target/debug/preloop` — `--help` on the root and
on every subcommand, then actually run them:

- `setup`, `doctor`, `status`, `run`, `shell`, `logs`, and anything else `--help`
  lists. Enumerate from the binary, not from memory.
- First-run experience with no config at all. Then with a partial config.
- The failure paths: no server running, wrong `AKSH_URL`, missing auth token,
  a workflow file that does not exist, a workflow with a YAML syntax error, a
  job that fails, Ctrl-C mid-run.

Known-bad starting points, already observed, confirm and fix:

1. `preloop run` against an endpoint that does not serve the native API prints
   `server returned 404 Not Found: {"error":"/api/v1/runs not available on this
   endpoint"}` without ever saying *which* URL it used or that `AKSH_URL`
   controls it.
2. Immediately after that, a bare `401 missing or invalid native API token`
   with no hint that `AKSH_SYSTEM_TOKEN` is the knob, and no pointer to
   `preloop setup`.
3. A queued job with no eligible runner sits silently forever. Nothing tells the
   user the pool is empty or that no runner matches the job's labels.

For each papercut: write down the exact command, the actual output, what a
first-time user would expect, then fix it and paste the new output.

## Phase 2 — drive the same surface as an agent would

An agent consumes this CLI non-interactively. Check specifically:

- Does every command support `--json`, and is the JSON stable and parseable?
- Are exit codes meaningful and distinct (0 success, non-zero per failure class)?
- Do errors go to stderr and data to stdout, so `... | jq` works?
- Does anything block on a TTY prompt when stdin is not a terminal? That is a
  hang for an agent. `--detach` and piped runs must never wait for input.
- Is there a way to poll a run to completion and get its conclusion without
  scraping human text?

Fix what is broken. Prefer extending existing flags over inventing new ones.

## Phase 3 — 4-repo conformance

Pick 4 real public GitHub repositories with non-trivial Actions workflows.
Choose different shapes on purpose, for example: a Node/pnpm monorepo, a Go
service, a Python package with a version matrix, and a Rust crate. Avoid repos
whose CI needs cloud credentials.

For each repo:

1. Clone it locally.
2. Fetch the real GitHub Actions logs for a recent successful run of the
   workflow you are about to replay, using `gh run list` / `gh run view --log`.
   Those logs are the oracle.
3. Start the aksh server, register the real `actions/runner` against it, and
   run the same workflow locally.
4. Diff local behavior against the GitHub logs: step order, step names, which
   steps ran versus were skipped, exit statuses, `${{ }}` values that appear in
   the log, and the job conclusion.
5. Write findings to `docs/conformance/<repo>.md`: what matched, what diverged,
   and for each divergence the root cause plus either a fix or a precise
   description of why it is not fixable yet.

Match the format of whatever already exists under `docs/` for prior conformance
runs so this slots in beside them.

## Reporting

Keep the Orca worktree comment current at real checkpoints:

```
orca worktree set --worktree active --comment "<short status>" --json
```

When each phase is done, send a summary to the coordinator:

```
orca orchestration send --run run_9f0ddd4aa14c --to term_ccc6d836-f9c1-4ebd-b638-729375e6e308 \
  --subject "phase N done" --body "<what you found, what you fixed, what is left>" --json
```

Escalate immediately rather than guessing if: a fix would change bytes on the
runner wire, a conformance divergence looks like a deliberate design decision,
or you cannot get a real runner to register.
