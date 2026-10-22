//! Push-back for submit-driven CI: after a run requested with `--push`
//! reaches a terminal state, push the tested commit to GitHub (fast-forward
//! or branch creation only — never a force), then ask the server to create
//! or update the pull request and report check runs.
//!
//! The git operations run against the current directory's `origin`, so this
//! module must be invoked from the checkout the run was submitted from. All
//! steps are idempotent: `preloop push <run_id>` may be re-run freely.

use anyhow::Context as _;
use preloop_gha_protocol::WorkflowSubmission;
use std::time::Duration;

/// Retry schedule for transient failures (GitHub unreachable, 5xx). Each
/// retry re-runs the whole sync, so a push that already landed is a no-op.
const RETRY_DELAYS_SECS: &[u64] = &[60, 300, 900];

/// One git metadata operation: `cat-file`, `merge-base`, `ls-remote`,
/// `rev-parse`, `log`, `commit-tree`.
///
/// A healthy local repository answers these in milliseconds and a healthy
/// forge's `ls-remote` in well under a second, so a minute is already generous
/// — it absorbs a cold connection to GitHub or a forge that is briefly
/// throttling. What it exists for is the other end: a git that is not
/// answering at all (a credential helper waiting on a prompt nobody will see,
/// a half-open TCP connection) must end the push, not hang it. `preloop push`
/// runs unattended by design — from a submitter's terminal or a script — so
/// there is nobody to notice a stalled child.
const GIT_CONTROL_TIMEOUT: Duration = Duration::from_secs(60);

/// `git push` of the tested commit to `origin`.
///
/// A transfer, not a metadata read: the pack for a large repository over a
/// slow uplink legitimately takes many minutes, so this bound is deliberately
/// generous — 30 minutes — rather than tight; a working push must never be
/// killed. It still ends the hang this protects against (a forge that accepts
/// the connection and then never reads, an SSH or credential helper blocked on
/// input) instead of leaving the run's push-back wedged forever.
const GIT_TRANSFER_TIMEOUT: Duration = Duration::from_secs(1800);

/// Reproducing the tested tree in the private index when its objects are not
/// already local (`git add -A` + `git write-tree` on a dirty-tree run).
///
/// Local and CPU-bound, but it hashes every modified and untracked file of the
/// checkout, so a large repository legitimately needs minutes. Five minutes
/// bounds a stale index lock or a filesystem that stopped answering without
/// tripping on repository size.
const GIT_STAGE_TIMEOUT: Duration = Duration::from_secs(300);

/// What the pinned push did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PushOutcome {
    /// Branch created at the tested commit.
    Created,
    /// Branch already pointed at the tested commit.
    AlreadyThere,
    /// Branch fast-forwarded to the tested commit.
    FastForwarded,
}

/// Per-sync override of the server's PR intent, decided after CI (the
/// dirty-tree prompt flow). `None` means "use the submission's recorded
/// `push` request".
#[derive(Debug, Clone, Copy)]
pub(crate) struct PushOpts {
    pub(crate) create_pr: bool,
    pub(crate) draft: bool,
}

/// The environment every git child of this module gets, so that no git command
/// can stop and wait for a human.
///
/// The deadline in [`preloop_observability::deadline::run_command`] bounds a
/// wait; this removes the waits that can be removed at all:
///
/// - `GIT_TERMINAL_PROMPT=0` — git must never prompt on the terminal for HTTP
///   credentials. (`preloop push` is often run from a developer's interactive
///   terminal, where that prompt is exactly the invisible hang this prevents;
///   a prompt that cannot be answered must fail instead.)
/// - `GIT_ASKPASS=false` — no askpass helper may be spawned to open a dialog.
/// - `SSH_ASKPASS=false` — same for the ssh passphrase helper.
/// - `GCM_INTERACTIVE=never` — Git Credential Manager must answer from its
///   stored credentials or fail, never open an interactive window.
///
/// `credential.helper` is deliberately NOT cleared here (unlike the engine's
/// snapshot fetches): `preloop push` pushes to the developer's own `origin`,
/// where the OS keychain / ssh-agent helper *is* the authentication path, so
/// clearing it would break every properly configured checkout. The deadline in
/// [`preloop_observability::deadline::run_command`] is what kills a helper that
/// blocks despite these settings.
///
/// Kept as a table so the test that proves the child environment is the same
/// values the production `git_command` applies.
const NON_INTERACTIVE_GIT_ENV: [(&str, &str); 4] = [
    ("GIT_TERMINAL_PROMPT", "0"),
    ("GIT_ASKPASS", "false"),
    ("SSH_ASKPASS", "false"),
    ("GCM_INTERACTIVE", "never"),
];

/// A `git` process with every interactive prompt path neutralised
/// ([`NON_INTERACTIVE_GIT_ENV`]).
fn git_command() -> tokio::process::Command {
    let mut command = tokio::process::Command::new("git");
    command.envs(NON_INTERACTIVE_GIT_ENV);
    command
}

/// Push the tested commit to `refs/heads/<branch>` on `origin`.
///
/// The remote branch must not exist, equal the tested commit, or be an
/// ancestor of it. Anything else (diverged history) is refused — a force
/// push would overwrite someone else's work, and the design never clobbers.
async fn push_tested_commit(sha: &str, branch: &str) -> anyhow::Result<PushOutcome> {
    push_tested_commit_in(
        &std::env::current_dir().context("current directory")?,
        sha,
        branch,
    )
    .await
}

async fn push_tested_commit_in(
    cwd: &std::path::Path,
    sha: &str,
    branch: &str,
) -> anyhow::Result<PushOutcome> {
    // The tested commit must exist in this checkout (it does when the push
    // runs where the run was submitted). Without it the push would fail with
    // a confusing "src refspec does not match any" error.
    if git_output(cwd, ["cat-file", "-e", &format!("{sha}^{{commit}}")])
        .await
        .is_err()
    {
        anyhow::bail!(
            "commit {sha} is not present in this checkout — run `preloop push` from the \
             checkout the run was submitted from"
        );
    }
    // One round trip tells us both the remote HEAD's default branch (via
    // `--symref`) and the branch's current position. Push-back is scoped to
    // feature branches: the default branch stays webhook/reconciliation
    // driven, and pushing it here would publish untested main.
    let remote = git_output(
        cwd,
        [
            "ls-remote",
            "--symref",
            "origin",
            "HEAD",
            &format!("refs/heads/{branch}"),
        ],
    )
    .await?;
    let default_branch = remote.lines().find_map(|line| {
        line.strip_prefix("ref: refs/heads/")
            .and_then(|rest| rest.strip_suffix("\tHEAD"))
    });
    if default_branch == Some(branch) {
        anyhow::bail!(
            "refusing to push branch {branch}: it is the repository's default branch. \
             Push-back is for feature branches; main stays webhook-driven."
        );
    }
    let remote_sha = remote
        .lines()
        .find(|line| line.ends_with(&format!("\trefs/heads/{branch}")))
        .and_then(|line| line.split_whitespace().next())
        .map(str::to_owned)
        .filter(|sha| !sha.is_empty());

    let outcome = match remote_sha {
        None => PushOutcome::Created,
        Some(remote) if remote == sha => return Ok(PushOutcome::AlreadyThere),
        Some(remote) => {
            // The remote commit is usually not in the local object store
            // (pushed by another machine); `merge-base` would only print a
            // confusing fatal. A missing object cannot be an ancestor, so
            // that case is divergence by construction.
            let remote_present =
                git_output(cwd, ["cat-file", "-e", &format!("{remote}^{{commit}}")])
                    .await
                    .is_ok();
            let is_ancestor = if remote_present {
                let mut command = git_command();
                command
                    .current_dir(cwd)
                    .args(["merge-base", "--is-ancestor", &remote, sha]);
                preloop_observability::deadline::run_command(
                    command,
                    GIT_CONTROL_TIMEOUT,
                    "git merge-base --is-ancestor",
                )
                .await
                .context("git merge-base")?
                .status
                .success()
            } else {
                false
            };
            if !is_ancestor {
                anyhow::bail!(
                    "branch {branch} on GitHub has commits that are not ancestors of the tested \
                     commit {sha} (remote {remote}). The push never force-pushes: rebase your \
                     branch onto the remote (or reset to the tested commit) and re-submit."
                );
            }
            PushOutcome::FastForwarded
        }
    };

    // A whole-pack transfer, so the transfer deadline, not the metadata one.
    let spec = format!("{sha}:refs/heads/{branch}");
    let what = format!("git push origin {spec}");
    let mut command = git_command();
    command.current_dir(cwd).args(["push", "origin", &spec]);
    let push = preloop_observability::deadline::run_command(command, GIT_TRANSFER_TIMEOUT, &what)
        .await
        .context("git push")?;
    if !push.status.success() {
        let stderr = String::from_utf8_lossy(&push.stderr);
        anyhow::bail!("git push failed: {}", stderr.trim());
    }
    Ok(outcome)
}

/// `git <args>` returning trimmed stdout; fails when git fails. `deadline`
/// bounds the command; callers that read metadata want [`GIT_CONTROL_TIMEOUT`].
async fn git_output_with<'a>(
    cwd: &std::path::Path,
    args: impl IntoIterator<Item = &'a str>,
    deadline: Duration,
) -> anyhow::Result<String> {
    let args = args.into_iter().collect::<Vec<_>>();
    let what = format!("git {}", args.join(" "));
    let mut command = git_command();
    command.current_dir(cwd).args(&args);
    let output = preloop_observability::deadline::run_command(command, deadline, &what).await?;
    if !output.status.success() {
        anyhow::bail!(
            "git {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// [`git_output_with`] under [`GIT_CONTROL_TIMEOUT`] — the deadline for every
/// metadata read in the push path.
async fn git_output<'a>(
    cwd: &std::path::Path,
    args: impl IntoIterator<Item = &'a str>,
) -> anyhow::Result<String> {
    git_output_with(cwd, args, GIT_CONTROL_TIMEOUT).await
}

/// Whether a sync failure is likely transient (GitHub unreachable, 5xx, or a
/// dependency that stalled past its deadline) rather than a permanent
/// configuration or state problem.
fn is_transient(error: &anyhow::Error) -> bool {
    // A deadline expiry is typed, so it is recognized as one instead of by its
    // text: `preloop_observability::deadline` reports its own errors as
    // `CommandError::TimedOut` ("… timed out after 1800s"), which none of the
    // phrases below match. A forge or transport that stalls is exactly what the
    // retry schedule is for, and every retry is idempotent.
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<preloop_observability::deadline::CommandError>()
            .is_some_and(preloop_observability::deadline::CommandError::timed_out)
    }) {
        return true;
    }
    let text = format!("{error:#}").to_lowercase();
    [
        "could not resolve host",
        "connection refused",
        "connection timed out",
        "operation timed out",
        "failed to connect",
        "unable to access",
        "rpc failed",
        "hung up",
        "error: 5",
        "status 5",
        "502",
        "503",
        "temporary failure",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

/// One control request against the engine's native API, with the native
/// bearer when there is one.
///
/// Every engine call in the push path is one-shot, so every one of them carries
/// [`crate::ENGINE_CONTROL_TIMEOUT`]: the client's 600-second idle read only
/// ends a connection that goes *completely* silent, so an engine that trickles
/// response bytes — or stops answering without closing — would otherwise hold
/// the sync, and the developer's terminal, open past every deadline the command
/// documents.
fn control_request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: String,
    token: Option<&str>,
) -> reqwest::RequestBuilder {
    let request = crate::engine_request(client, method, url, crate::ENGINE_CONTROL_TIMEOUT);
    match token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

/// Run the full push-back for a run: fetch it, pin the push, and ask the
/// server to publish PR + checks. Retries transient failures on a backoff
/// schedule; permanent failures surface immediately with instructions.
pub(crate) async fn push_run(
    client: &reqwest::Client,
    url: &str,
    token: Option<String>,
    run_id: &str,
    opts: Option<PushOpts>,
) -> anyhow::Result<()> {
    // Resolve the exact commit to push once, before the retry loop. Every
    // attempt must push the SAME commit: materialization stamps the current
    // time into the commit dates, so re-materializing per attempt yields a
    // divergent commit, and a transient server failure after the branch push
    // would then be misreported as "branch diverged".
    let push_sha = prepare_push_sha(client, url, token.as_deref(), run_id).await?;
    let mut attempt = 0;
    loop {
        match push_run_once(client, url, token.as_deref(), run_id, &push_sha, opts).await {
            Ok(()) => return Ok(()),
            Err(error) if is_transient(&error) && attempt < RETRY_DELAYS_SECS.len() => {
                let delay = RETRY_DELAYS_SECS[attempt];
                attempt += 1;
                eprintln!(
                    "push failed transiently: {error:#}\n\
                     retrying in {delay}s (attempt {attempt}/{}) — Ctrl-C stops, \
                     `preloop push {run_id}` resumes later",
                    RETRY_DELAYS_SECS.len()
                );
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Fetch the run and decide the exact commit to push: for a clean submission
/// that is `submission.sha` (the tested commit); for a dirty one it is a
/// materialized commit whose tree is exactly the tested snapshot tree.
async fn prepare_push_sha(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
    run_id: &str,
) -> anyhow::Result<String> {
    let request = control_request(
        client,
        reqwest::Method::GET,
        format!("{url}/api/v1/runs/{run_id}"),
        token,
    );
    let response = request
        .send()
        .await
        .with_context(|| format!("fetching run {run_id}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status} fetching run {run_id}: {body}");
    }
    let run: serde_json::Value = response.json().await?;
    let submission: WorkflowSubmission = serde_json::from_value(
        run.get("submission")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("run {run_id} has no submission record"))?,
    )
    .context("parsing run submission")?;

    submission
        .push
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("run {run_id} was not submitted with --push"))?;
    let push_tree = submission
        .push_tree
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("run {run_id} has no recorded tested tree"))?;
    let sha = &submission.sha;

    // Pin the push to the tested commit. A dirty-tree run recorded the
    // snapshot tree but its submission sha is the *base* commit — its
    // tree differs from what CI tested. Materialize a real commit whose
    // tree is exactly the tested tree (parented on the base commit, using
    // the developer's git identity), so the pushed commit is byte-identical
    // to what CI validated. If the user committed the dirty state since
    // (tree now matches), push it directly.
    let head_tree = git_output(
        &std::env::current_dir().context("current directory")?,
        ["rev-parse", &format!("{sha}^{{tree}}")],
    )
    .await?;
    if head_tree == push_tree {
        return Ok(sha.to_owned());
    }
    let materialized = materialize_tested_commit(push_tree, sha).await?;
    eprintln!(
        "materialized CI-verified commit {materialized} (tree {push_tree}) from \
         the tested snapshot"
    );
    Ok(materialized)
}

async fn push_run_once(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
    run_id: &str,
    push_sha: &str,
    opts: Option<PushOpts>,
) -> anyhow::Result<()> {
    let request = control_request(
        client,
        reqwest::Method::GET,
        format!("{url}/api/v1/runs/{run_id}"),
        token,
    );
    let response = request
        .send()
        .await
        .with_context(|| format!("fetching run {run_id}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server returned {status} fetching run {run_id}: {body}");
    }
    let run: serde_json::Value = response.json().await?;

    let submission: WorkflowSubmission = serde_json::from_value(
        run.get("submission")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("run {run_id} has no submission record"))?,
    )
    .context("parsing run submission")?;

    submission
        .push
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("run {run_id} was not submitted with --push"))?;
    let branch = submission
        .git_ref
        .strip_prefix("refs/heads/")
        .ok_or_else(|| {
            anyhow::anyhow!(
                "run {run_id} targets {} — push supports branch refs only",
                submission.git_ref
            )
        })?;

    let outcome = push_tested_commit(push_sha, branch).await?;
    match outcome {
        PushOutcome::Created => eprintln!("pushed {push_sha} to origin/{branch} (branch created)"),
        PushOutcome::AlreadyThere => eprintln!("origin/{branch} already at {push_sha}"),
        PushOutcome::FastForwarded => {
            eprintln!("pushed {push_sha} to origin/{branch} (fast-forward)")
        }
    }

    // 2. The server verifies the pushed tree, reuses or creates the PR, and
    //    reports check runs — all idempotently.
    let mut request = control_request(
        client,
        reqwest::Method::POST,
        format!("{url}/api/v1/runs/{run_id}/push"),
        token,
    );
    if let Some(opts) = opts {
        request = request.json(&serde_json::json!({
            "create_pr": opts.create_pr,
            "draft": opts.draft,
        }));
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("requesting server push for run {run_id}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("server push for run {run_id} failed with {status}: {body}");
    }
    let result: serde_json::Value = response.json().await?;

    match result.get("pr_number").and_then(serde_json::Value::as_u64) {
        Some(number) => {
            let repository = submission.repository;
            eprintln!("pushed: PR https://github.com/{repository}/pull/{number}");
        }
        None => eprintln!(
            "pushed: branch pushed, {}",
            if submission.push.as_ref().is_some_and(|s| s.create_pr) {
                "no open pull request for the branch"
            } else {
                "no pull request created (--create-pr not requested)"
            }
        ),
    }
    Ok(())
}

/// Create a real commit whose tree is exactly the tested tree, parented on
/// the run's base commit. The author/committer come from the checkout's git
/// identity (`user.name` / `user.email`), so the materialized commit is
/// attributed to the developer, not to a bot.
async fn materialize_tested_commit(tested_tree: &str, parent: &str) -> anyhow::Result<String> {
    materialize_tested_commit_in(
        &std::env::current_dir().context("current directory")?,
        tested_tree,
        parent,
    )
    .await
}

async fn materialize_tested_commit_in(
    cwd: &std::path::Path,
    tested_tree: &str,
    parent: &str,
) -> anyhow::Result<String> {
    // The tested tree is a server-computed sha (the snapshot repository's
    // tree), whose objects may not exist in this checkout. Reproduce the
    // tree from the working tree with a private index — the same inclusion
    // rules the server's snapshot applies (tracked modifications + untracked
    // non-ignored files) — so `commit-tree` can resolve it. Content
    // addressing makes a sha match proof that the exact tested objects are
    // now present. A mismatch means the working tree changed since the run
    // was submitted; materializing a *different* tree would violate
    // pushed == tested, so fail with a re-submit hint.
    ensure_tested_tree_resolvable(cwd, tested_tree).await?;
    let head_message = git_output(cwd, ["log", "-1", "--format=%B", parent])
        .await
        .unwrap_or_else(|_| "CI-verified snapshot".to_owned());
    let message = format!(
        "{head_message}\n\n[preloop] CI-verified snapshot of the working tree \
         (tree {tested_tree})"
    );
    let commit = git_output(
        cwd,
        ["commit-tree", tested_tree, "-p", parent, "-m", &message],
    )
    .await?;
    Ok(commit)
}

/// Make `tested_tree` resolvable in `cwd`'s object store.
///
/// Fast path: the tree object already exists locally. Otherwise stage the
/// working tree into a private index (never touching the user's index) and
/// write the resulting tree — this materializes every blob the tree
/// references. The private index keeps the user's staging area untouched.
async fn ensure_tested_tree_resolvable(
    cwd: &std::path::Path,
    tested_tree: &str,
) -> anyhow::Result<()> {
    if git_output(cwd, ["cat-file", "-e", tested_tree])
        .await
        .is_ok()
    {
        return Ok(());
    }
    // A private index under the repo's git dir: unique per invocation, and
    // git cleans up nothing, so remove it afterwards.
    let git_dir = git_output(cwd, ["rev-parse", "--git-dir"]).await?;
    let index =
        std::path::Path::new(&git_dir).join(format!("preloop-index-{}", std::process::id()));
    let _ = std::fs::remove_file(&index);
    let reproduced = stage_working_tree(cwd, &index).await;
    let _ = std::fs::remove_file(&index);
    let reproduced = reproduced?;
    if reproduced != tested_tree {
        anyhow::bail!(
            "tested tree {tested_tree} does not match the working tree {reproduced}; \
             the working tree changed since the run was submitted. Re-submit after \
             committing or stashing your changes so the pushed commit is exactly what CI tested"
        );
    }
    Ok(())
}

/// Stage the working tree into `index` (a private `GIT_INDEX_FILE`) and return
/// the resulting tree.
///
/// Split out of [`ensure_tested_tree_resolvable`] so that the private index is
/// removed on the failure path too; both calls share [`GIT_STAGE_TIMEOUT`]
/// because they are the same local, size-dependent working-tree operation.
async fn stage_working_tree(
    cwd: &std::path::Path,
    index: &std::path::Path,
) -> anyhow::Result<String> {
    let mut command = git_command();
    command
        .current_dir(cwd)
        .env("GIT_INDEX_FILE", index)
        .args(["add", "-A"]);
    let add = preloop_observability::deadline::run_command(
        command,
        GIT_STAGE_TIMEOUT,
        "git add -A (private index)",
    )
    .await?;
    if !add.status.success() {
        anyhow::bail!(
            "git add -A: {}",
            String::from_utf8_lossy(&add.stderr).trim()
        );
    }
    let mut command = git_command();
    command
        .current_dir(cwd)
        .env("GIT_INDEX_FILE", index)
        .args(["write-tree"]);
    let write = preloop_observability::deadline::run_command(
        command,
        GIT_STAGE_TIMEOUT,
        "git write-tree (private index)",
    )
    .await?;
    if !write.status.success() {
        anyhow::bail!(
            "git write-tree: {}",
            String::from_utf8_lossy(&write.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&write.stdout).trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    // The fixture helper below is deliberately synchronous: it drives git the
    // way a developer's shell does, independent of the code under test.
    use std::process::Command;

    fn git(cwd: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .current_dir(cwd)
            // Test fixtures must not depend on the developer's signing
            // setup — a broken or locked signing agent (e.g. 1Password)
            // would otherwise fail every commit fixture.
            .arg("-c")
            .arg("commit.gpgsign=false")
            .args(args)
            .output()
            .unwrap_or_else(|e| panic!("git {args:?}: {e}"));
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    /// Checkout with one committed file and a bare `origin`.
    fn repo_with_remote() -> (tempfile::TempDir, tempfile::TempDir, String) {
        let work = tempfile::tempdir().unwrap();
        let remote = tempfile::tempdir().unwrap();
        let remote_path = remote.path().join("origin.git");
        git(work.path(), &["init", "-b", "main"]);
        git(work.path(), &["config", "user.email", "test@example.com"]);
        git(work.path(), &["config", "user.name", "Test"]);
        fs::write(work.path().join("f.txt"), "one\n").unwrap();
        git(work.path(), &["add", "f.txt"]);
        git(work.path(), &["commit", "-m", "one"]);
        git(
            work.path(),
            &["init", "--bare", remote_path.to_str().unwrap()],
        );
        git(&remote_path, &["symbolic-ref", "HEAD", "refs/heads/main"]);
        git(
            work.path(),
            &["remote", "add", "origin", remote_path.to_str().unwrap()],
        );
        git(work.path(), &["push", "-q", "origin", "main"]);
        let head = git(work.path(), &["rev-parse", "HEAD"]);
        (work, remote, head)
    }

    #[tokio::test]
    async fn push_creates_missing_branch() {
        let (work, _remote, head) = repo_with_remote();
        assert_eq!(
            push_tested_commit_in(work.path(), &head, "feat/x")
                .await
                .unwrap(),
            PushOutcome::Created
        );
        let remote_sha = git(work.path(), &["ls-remote", "origin", "refs/heads/feat/x"]);
        assert!(remote_sha.starts_with(&head), "branch created at {head}");
    }

    #[tokio::test]
    async fn materialize_tested_commit_pins_the_exact_tree() {
        let (work, _remote, head) = repo_with_remote();

        // Dirty the tree and capture the staged tree — what CI would test.
        fs::write(work.path().join("f.txt"), "two\n").unwrap();
        git(work.path(), &["add", "f.txt"]);
        let dirty_tree = git(work.path(), &["write-tree"]);
        let head_tree = git(work.path(), &["rev-parse", "HEAD^{tree}"]);
        assert_ne!(
            dirty_tree, head_tree,
            "the dirty tree must differ from HEAD"
        );

        let commit = materialize_tested_commit_in(work.path(), &dirty_tree, &head)
            .await
            .unwrap();
        // The materialized commit's tree is exactly the tested tree…
        assert_eq!(
            git(work.path(), &["rev-parse", &format!("{commit}^{{tree}}")]),
            dirty_tree
        );
        // …parented on the base commit…
        assert_eq!(
            git(work.path(), &["rev-parse", &format!("{commit}^")]),
            head
        );
        // …and attributed to the developer's git identity, not a bot.
        let author = git(
            work.path(),
            &["log", "-1", "--format=%an <%ae>", commit.as_str()],
        );
        assert_eq!(author, "Test <test@example.com>");
    }

    #[tokio::test]
    async fn materialize_imports_tree_from_another_repository() {
        // The tested tree is created in repo A (its object store holds the
        // blobs); the push runs from repo B, whose object store never saw
        // that tree. Materialization must reproduce the tree from B's
        // working files — content-addressed, so a sha match proves the
        // exact tested objects are now local — and commit-tree must resolve.
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        git(a.path(), &["init", "-q", "-b", "main"]);
        git(a.path(), &["config", "user.email", "test@example.com"]);
        git(a.path(), &["config", "user.name", "Test"]);
        fs::write(a.path().join("f.txt"), "one\n").unwrap();
        git(a.path(), &["add", "f.txt"]);
        git(a.path(), &["commit", "-q", "-m", "one"]);
        let head = git(a.path(), &["rev-parse", "HEAD"]);

        // B is a clone of A at the same base commit.
        git(
            a.path(),
            &[
                "clone",
                "-q",
                a.path().to_str().unwrap(),
                b.path().to_str().unwrap(),
            ],
        );
        // The commit happens in B, and a clone does not inherit A's local
        // config. Without this, the test only passes on hosts whose global
        // git identity leaks into B (the CI fork has none, so it failed with
        // "Author identity unknown").
        git(b.path(), &["config", "user.email", "test@example.com"]);
        git(b.path(), &["config", "user.name", "Test"]);
        assert_eq!(
            git(b.path(), &["rev-parse", "HEAD"]),
            head,
            "B starts at the same base commit"
        );

        // A's dirty tree is the "tested" tree; its objects only exist in A.
        fs::write(a.path().join("f.txt"), "two\n").unwrap();
        git(a.path(), &["add", "f.txt"]);
        let tested_tree = git(a.path(), &["write-tree"]);
        let probe = Command::new("git")
            .current_dir(b.path())
            .args(["cat-file", "-e", &tested_tree])
            .status()
            .unwrap();
        assert!(
            !probe.success(),
            "B must not know the tested tree object before materialization"
        );

        // B has the same working-file content, unstaged.
        fs::write(b.path().join("f.txt"), "two\n").unwrap();

        let commit = materialize_tested_commit_in(b.path(), &tested_tree, &head)
            .await
            .unwrap();
        assert_eq!(
            git(b.path(), &["rev-parse", &format!("{commit}^{{tree}}")]),
            tested_tree,
            "the materialized commit carries the exact tested tree"
        );
        assert_eq!(git(b.path(), &["rev-parse", &format!("{commit}^")]), head);
    }

    #[tokio::test]
    async fn push_skips_when_remote_already_equal() {
        let (work, _remote, head) = repo_with_remote();
        push_tested_commit_in(work.path(), &head, "feat/x")
            .await
            .unwrap();
        assert_eq!(
            push_tested_commit_in(work.path(), &head, "feat/x")
                .await
                .unwrap(),
            PushOutcome::AlreadyThere
        );
    }

    #[tokio::test]
    async fn push_fast_forwards_an_ancestor() {
        let (work, _remote, head) = repo_with_remote();
        push_tested_commit_in(work.path(), &head, "feat/x")
            .await
            .unwrap();
        fs::write(work.path().join("f.txt"), "two\n").unwrap();
        git(work.path(), &["commit", "-am", "two"]);
        let second = git(work.path(), &["rev-parse", "HEAD"]);
        assert_eq!(
            push_tested_commit_in(work.path(), &second, "feat/x")
                .await
                .unwrap(),
            PushOutcome::FastForwarded
        );
        let remote_sha = git(work.path(), &["ls-remote", "origin", "refs/heads/feat/x"]);
        assert!(
            remote_sha.starts_with(&second),
            "fast-forwarded to {second}"
        );
    }

    #[tokio::test]
    async fn push_refuses_diverged_branch() {
        let (work, remote, head) = repo_with_remote();
        push_tested_commit_in(work.path(), &head, "feat/x")
            .await
            .unwrap();

        // Someone else pushed a conflicting commit to the same branch.
        let other_clone = tempfile::tempdir().unwrap();
        git(
            other_clone.path(),
            &[
                "clone",
                "-q",
                remote.path().join("origin.git").to_str().unwrap(),
                ".",
            ],
        );
        git(
            other_clone.path(),
            &["config", "user.email", "other@example.com"],
        );
        git(other_clone.path(), &["config", "user.name", "Other"]);
        fs::write(other_clone.path().join("f.txt"), "other\n").unwrap();
        git(other_clone.path(), &["commit", "-am", "other"]);
        git(
            other_clone.path(),
            &["push", "origin", "HEAD:refs/heads/feat/x"],
        );

        // Our next commit diverges from the remote branch.
        git(work.path(), &["checkout", "-q", "-b", "feat/x"]);
        fs::write(work.path().join("f.txt"), "mine\n").unwrap();
        git(work.path(), &["commit", "-am", "mine"]);
        let mine = git(work.path(), &["rev-parse", "HEAD"]);
        let error = push_tested_commit_in(work.path(), &mine, "feat/x")
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("never force-pushes"),
            "diverged branch refused: {error:#}"
        );
        let remote_sha = git(work.path(), &["ls-remote", "origin", "refs/heads/feat/x"]);
        assert!(
            !remote_sha.starts_with(&mine),
            "remote must be untouched after a refused push"
        );
    }

    #[tokio::test]
    async fn push_refuses_unknown_commit() {
        let (work, _remote, _head) = repo_with_remote();
        let error = push_tested_commit_in(
            work.path(),
            "ffffffffffffffffffffffffffffffffffffffff",
            "feat/x",
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("not present in this checkout"));
    }

    /// A deadline expiry is transient, not a permanent failure: the push path's
    /// own `run_command` deadline reports a typed `CommandError::TimedOut`
    /// ("git push origin … timed out after 1800s"), which the phrase list below
    /// does not match. Classifying it as permanent made a stalled forge end
    /// `preloop push` on the first attempt instead of using its retry schedule.
    #[tokio::test]
    async fn transient_classification() {
        let transient = [
            "git push failed: fatal: unable to access 'https://github.com/x/y.git/': Could not resolve host: github.com",
            "git push failed: fatal: The remote end hung up unexpectedly",
            "server push for run x failed with 502 Bad Gateway",
        ];
        for message in transient {
            assert!(is_transient(&anyhow::anyhow!("{message}")), "{message}");
        }
        // The real error, not a paraphrase of its text: `git_output` and the
        // push itself wrap `run_command`'s typed expiry in `anyhow` context.
        let mut command = tokio::process::Command::new("/bin/sh");
        command.args(["-c", "exec sleep 30"]);
        let expired = preloop_observability::deadline::run_command(
            command,
            Duration::from_millis(100),
            "git push origin abc:refs/heads/feat/x",
        )
        .await
        .expect_err("a stalled git child must expire its deadline");
        assert!(expired.timed_out(), "the runner must report a typed expiry");
        let wrapped = anyhow::Error::new(expired).context("git push");
        assert!(
            is_transient(&wrapped),
            "a deadline expiry must reach the retry schedule: {wrapped:#}"
        );
        let permanent = [
            "branch feat/x on GitHub has commits that are not ancestors",
            "git push failed: fatal: Authentication failed for 'https://github.com/x/y.git/'",
            "git push failed: remote: Permission to x/y.git denied to z",
            "git push failed: ! [remote rejected] feat/x -> feat/x (protected branch)",
        ];
        for message in permanent {
            assert!(!is_transient(&anyhow::anyhow!("{message}")), "{message}");
        }
    }

    #[tokio::test]
    async fn push_refuses_default_branch() {
        let (work, _remote, head) = repo_with_remote();
        let error = push_tested_commit_in(work.path(), &head, "main")
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("default branch"),
            "default-branch push refused: {error:#}"
        );
    }

    /// Every engine call in the push path carries the control total, so a
    /// wedged engine fails the sync instead of holding it until the client's
    /// 600-second idle read (or forever, if it trickles bytes).
    #[test]
    fn engine_calls_carry_the_control_deadline() {
        let client = crate::build_client();
        for (method, path) in [
            (reqwest::Method::GET, "/api/v1/runs/x"),
            (reqwest::Method::POST, "/api/v1/runs/x/push"),
        ] {
            let request = control_request(
                &client,
                method,
                format!("http://127.0.0.1:1{path}"),
                Some("token"),
            )
            .build()
            .expect("the request builds");
            assert_eq!(
                request.timeout(),
                Some(&crate::ENGINE_CONTROL_TIMEOUT),
                "{path} must carry the control total"
            );
            assert!(
                request
                    .headers()
                    .contains_key(reqwest::header::AUTHORIZATION),
                "{path} must keep the native bearer"
            );
        }
        // Without a token the request still builds and still has the total.
        let anonymous = control_request(
            &client,
            reqwest::Method::GET,
            "http://127.0.0.1:1/api/v1/runs/x".to_owned(),
            None,
        )
        .build()
        .expect("the request builds");
        assert_eq!(anonymous.timeout(), Some(&crate::ENGINE_CONTROL_TIMEOUT));
    }

    /// The deadline, not the child, must end the call: the old synchronous
    /// `wait()` returned only when the child exited, so a hung git hung the
    /// whole `preloop push` with it.
    #[tokio::test]
    async fn run_command_kills_a_child_that_never_exits() {
        let deadline = Duration::from_millis(300);
        let mut command = tokio::process::Command::new("/bin/sh");
        command.args(["-c", "exec sleep 30"]);
        let started = std::time::Instant::now();
        let error = preloop_observability::deadline::run_command(command, deadline, "git push")
            .await
            .expect_err("a child that never exits must time out");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the git runner must return on its own deadline, not after the child exits"
        );
        let text = format!("{error:#}");
        assert!(text.contains("git push"), "error names the command: {text}");
        assert!(
            text.contains(&format!("{deadline:?}")),
            "error names the deadline: {text}"
        );
    }

    /// A credential prompt is the classic way a push hangs; the child
    /// environment must leave no interactive path open. The probe runs a shell
    /// under exactly the environment `git_command` applies, so a regression in
    /// that table (or its values) fails here.
    #[tokio::test]
    async fn git_children_are_non_interactive() {
        let mut probe = tokio::process::Command::new("/bin/sh");
        probe.envs(NON_INTERACTIVE_GIT_ENV);
        // `git_command` applies this table to git itself; the shell is only
        // the vehicle that prints it back, because git would not.
        probe.args([
            "-c",
            r#"printf "%s|%s|%s|%s" "$GIT_TERMINAL_PROMPT" "$GIT_ASKPASS" "$SSH_ASKPASS" "$GCM_INTERACTIVE""#,
        ]);
        let output = preloop_observability::deadline::run_command(
            probe,
            Duration::from_secs(10),
            "git env probe",
        )
        .await
        .expect("the probe shell runs");
        assert!(
            output.status.success(),
            "probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            "0|false|false|never",
            "every interactive git path must be neutralised"
        );
        // The probe mirrors the table; this pins the production command to it.
        let command = git_command();
        for (key, value) in NON_INTERACTIVE_GIT_ENV {
            let applied = command
                .as_std()
                .get_envs()
                .find(|(name, _)| *name == std::ffi::OsStr::new(key))
                .and_then(|(_, value)| value);
            assert_eq!(
                applied,
                Some(std::ffi::OsStr::new(value)),
                "{key} must be set on every git child"
            );
        }
    }
}
