//! Push-back for submit-driven CI: after a run requested with `--push`
//! reaches a terminal state, push the tested commit to GitHub (fast-forward
//! or branch creation only — never a force), then ask the server to create
//! or update the pull request and report check runs.
//!
//! The git operations run against the current directory's `origin`, so this
//! module must be invoked from the checkout the run was submitted from. All
//! steps are idempotent: `preloop push <run_id>` may be re-run freely.

use aksh_gha_protocol::WorkflowSubmission;
use anyhow::Context as _;
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
) -> anyhow::Result<()> {
    let mut attempt = 0;
    loop {
        match push_run_once(client, url, token.as_deref(), run_id).await {
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

async fn push_run_once(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
    run_id: &str,
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
    let _push_tree = submission
        .push_tree
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("run {run_id} has no recorded tested tree"))?;
    let sha = &submission.sha;
    let branch = submission
        .git_ref
        .strip_prefix("refs/heads/")
        .ok_or_else(|| {
            anyhow::anyhow!(
                "run {run_id} targets {} — push supports branch refs only",
                submission.git_ref
            )
        })?;

    // 1. Pin the push to the tested commit.
    let outcome = push_tested_commit(sha, branch)?;
    match outcome {
        PushOutcome::Created => eprintln!("pushed {sha} to origin/{branch} (branch created)"),
        PushOutcome::AlreadyThere => eprintln!("origin/{branch} already at {sha}"),
        PushOutcome::FastForwarded => eprintln!("pushed {sha} to origin/{branch} (fast-forward)"),
    }

    // 2. The server verifies the pushed tree, reuses or creates the PR, and
    //    reports check runs — all idempotently.
    let mut request = client.post(format!("{url}/api/v1/runs/{run_id}/push"));
    if let Some(token) = token {
        request = request.bearer_auth(token);
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
