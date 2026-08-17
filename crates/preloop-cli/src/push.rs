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
use std::process::Command;
use std::time::Duration;

/// Retry schedule for transient failures (GitHub unreachable, 5xx). Each
/// retry re-runs the whole sync, so a push that already landed is a no-op.
const RETRY_DELAYS_SECS: &[u64] = &[60, 300, 900];

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

/// Push the tested commit to `refs/heads/<branch>` on `origin`.
///
/// The remote branch must not exist, equal the tested commit, or be an
/// ancestor of it. Anything else (diverged history) is refused — a force
/// push would overwrite someone else's work, and the design never clobbers.
fn push_tested_commit(sha: &str, branch: &str) -> anyhow::Result<PushOutcome> {
    push_tested_commit_in(
        &std::env::current_dir().context("current directory")?,
        sha,
        branch,
    )
}

fn push_tested_commit_in(
    cwd: &std::path::Path,
    sha: &str,
    branch: &str,
) -> anyhow::Result<PushOutcome> {
    // The tested commit must exist in this checkout (it does when the push
    // runs where the run was submitted). Without it the push would fail with
    // a confusing "src refspec does not match any" error.
    if git_output(cwd, ["cat-file", "-e", &format!("{sha}^{{commit}}")]).is_err() {
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
    )?;
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
                git_output(cwd, ["cat-file", "-e", &format!("{remote}^{{commit}}")]).is_ok();
            let is_ancestor = remote_present
                && Command::new("git")
                    .current_dir(cwd)
                    .args(["merge-base", "--is-ancestor", &remote, sha])
                    .status()
                    .context("git merge-base")?
                    .success();
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

    let push = Command::new("git")
        .current_dir(cwd)
        .args(["push", "origin", &format!("{sha}:refs/heads/{branch}")])
        .output()
        .context("git push")?;
    if !push.status.success() {
        let stderr = String::from_utf8_lossy(&push.stderr);
        anyhow::bail!("git push failed: {}", stderr.trim());
    }
    Ok(outcome)
}

/// `git <args>` returning trimmed stdout; fails when git fails.
fn git_output<'a>(
    cwd: &std::path::Path,
    args: impl IntoIterator<Item = &'a str>,
) -> anyhow::Result<String> {
    let args = args.into_iter().collect::<Vec<_>>();
    let output = Command::new("git")
        .current_dir(cwd)
        .args(&args)
        .output()
        .with_context(|| format!("git {} failed to run", args.join(" ")))?;
    if !output.status.success() {
        anyhow::bail!(
            "git {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Whether a sync failure is likely transient (GitHub unreachable, 5xx)
/// rather than a permanent configuration or state problem.
fn is_transient(error: &anyhow::Error) -> bool {
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
    let mut request = client.get(format!("{url}/api/v1/runs/{run_id}"));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
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
    )?;
    if head_tree == push_tree {
        return Ok(sha.to_owned());
    }
    let materialized = materialize_tested_commit(push_tree, sha)?;
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
    let mut request = client.get(format!("{url}/api/v1/runs/{run_id}"));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
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

    let outcome = push_tested_commit(push_sha, branch)?;
    match outcome {
        PushOutcome::Created => eprintln!("pushed {push_sha} to origin/{branch} (branch created)"),
        PushOutcome::AlreadyThere => eprintln!("origin/{branch} already at {push_sha}"),
        PushOutcome::FastForwarded => {
            eprintln!("pushed {push_sha} to origin/{branch} (fast-forward)")
        }
    }

    // 2. The server verifies the pushed tree, reuses or creates the PR, and
    //    reports check runs — all idempotently.
    let mut request = client.post(format!("{url}/api/v1/runs/{run_id}/push"));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
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
fn materialize_tested_commit(tested_tree: &str, parent: &str) -> anyhow::Result<String> {
    materialize_tested_commit_in(
        &std::env::current_dir().context("current directory")?,
        tested_tree,
        parent,
    )
}

fn materialize_tested_commit_in(
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
    ensure_tested_tree_resolvable(cwd, tested_tree)?;
    let head_message = git_output(cwd, ["log", "-1", "--format=%B", parent])
        .unwrap_or_else(|_| "CI-verified snapshot".to_owned());
    let message = format!(
        "{head_message}\n\n[preloop] CI-verified snapshot of the working tree \
         (tree {tested_tree})"
    );
    let commit = git_output(
        cwd,
        ["commit-tree", tested_tree, "-p", parent, "-m", &message],
    )?;
    Ok(commit)
}

/// Make `tested_tree` resolvable in `cwd`'s object store.
///
/// Fast path: the tree object already exists locally. Otherwise stage the
/// working tree into a private index (never touching the user's index) and
/// write the resulting tree — this materializes every blob the tree
/// references. The private index keeps the user's staging area untouched.
fn ensure_tested_tree_resolvable(cwd: &std::path::Path, tested_tree: &str) -> anyhow::Result<()> {
    if git_output(cwd, ["cat-file", "-e", tested_tree]).is_ok() {
        return Ok(());
    }
    // A private index under the repo's git dir: unique per invocation, and
    // git cleans up nothing, so remove it afterwards.
    let git_dir = git_output(cwd, ["rev-parse", "--git-dir"])?;
    let index =
        std::path::Path::new(&git_dir).join(format!("preloop-index-{}", std::process::id()));
    let _ = std::fs::remove_file(&index);
    let stage = || -> anyhow::Result<String> {
        let add = Command::new("git")
            .current_dir(cwd)
            .env("GIT_INDEX_FILE", &index)
            .args(["add", "-A"])
            .output()
            .context("git add -A (private index)")?;
        if !add.status.success() {
            anyhow::bail!(
                "git add -A: {}",
                String::from_utf8_lossy(&add.stderr).trim()
            );
        }
        let write = Command::new("git")
            .current_dir(cwd)
            .env("GIT_INDEX_FILE", &index)
            .args(["write-tree"])
            .output()
            .context("git write-tree (private index)")?;
        if !write.status.success() {
            anyhow::bail!(
                "git write-tree: {}",
                String::from_utf8_lossy(&write.stderr).trim()
            );
        }
        Ok(String::from_utf8_lossy(&write.stdout).trim().to_owned())
    };
    let reproduced = stage();
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    fn git(cwd: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .current_dir(cwd)
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

    #[test]
    fn push_creates_missing_branch() {
        let (work, _remote, head) = repo_with_remote();
        assert_eq!(
            push_tested_commit_in(work.path(), &head, "feat/x").unwrap(),
            PushOutcome::Created
        );
        let remote_sha = git(work.path(), &["ls-remote", "origin", "refs/heads/feat/x"]);
        assert!(remote_sha.starts_with(&head), "branch created at {head}");
    }

    #[test]
    fn materialize_tested_commit_pins_the_exact_tree() {
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

        let commit = materialize_tested_commit_in(work.path(), &dirty_tree, &head).unwrap();
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

    #[test]
    fn materialize_imports_tree_from_another_repository() {
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

        let commit = materialize_tested_commit_in(b.path(), &tested_tree, &head).unwrap();
        assert_eq!(
            git(b.path(), &["rev-parse", &format!("{commit}^{{tree}}")]),
            tested_tree,
            "the materialized commit carries the exact tested tree"
        );
        assert_eq!(git(b.path(), &["rev-parse", &format!("{commit}^")]), head);
    }

    #[test]
    fn push_skips_when_remote_already_equal() {
        let (work, _remote, head) = repo_with_remote();
        push_tested_commit_in(work.path(), &head, "feat/x").unwrap();
        assert_eq!(
            push_tested_commit_in(work.path(), &head, "feat/x").unwrap(),
            PushOutcome::AlreadyThere
        );
    }

    #[test]
    fn push_fast_forwards_an_ancestor() {
        let (work, _remote, head) = repo_with_remote();
        push_tested_commit_in(work.path(), &head, "feat/x").unwrap();
        fs::write(work.path().join("f.txt"), "two\n").unwrap();
        git(work.path(), &["commit", "-am", "two"]);
        let second = git(work.path(), &["rev-parse", "HEAD"]);
        assert_eq!(
            push_tested_commit_in(work.path(), &second, "feat/x").unwrap(),
            PushOutcome::FastForwarded
        );
        let remote_sha = git(work.path(), &["ls-remote", "origin", "refs/heads/feat/x"]);
        assert!(
            remote_sha.starts_with(&second),
            "fast-forwarded to {second}"
        );
    }

    #[test]
    fn push_refuses_diverged_branch() {
        let (work, remote, head) = repo_with_remote();
        push_tested_commit_in(work.path(), &head, "feat/x").unwrap();

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
        let error = push_tested_commit_in(work.path(), &mine, "feat/x").unwrap_err();
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

    #[test]
    fn push_refuses_unknown_commit() {
        let (work, _remote, _head) = repo_with_remote();
        let error = push_tested_commit_in(
            work.path(),
            "ffffffffffffffffffffffffffffffffffffffff",
            "feat/x",
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("not present in this checkout"));
    }

    #[test]
    fn transient_classification() {
        let transient = [
            "git push failed: fatal: unable to access 'https://github.com/x/y.git/': Could not resolve host: github.com",
            "git push failed: fatal: The remote end hung up unexpectedly",
            "server push for run x failed with 502 Bad Gateway",
        ];
        for message in transient {
            assert!(is_transient(&anyhow::anyhow!("{message}")), "{message}");
        }
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

    #[test]
    fn push_refuses_default_branch() {
        let (work, _remote, head) = repo_with_remote();
        let error = push_tested_commit_in(work.path(), &head, "main").unwrap_err();
        assert!(
            format!("{error:#}").contains("default branch"),
            "default-branch push refused: {error:#}"
        );
    }
}
