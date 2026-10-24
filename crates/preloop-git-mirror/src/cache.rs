//! Bare-mirror lifecycle: presence checks, provenance-tracked fetches, and
//! the lock-aware cleanup a fetch needs after a killed child.

use std::path::Path;

use crate::{GIT_CONTROL_TIMEOUT, MirrorError};

/// Where a mirror's commit came from, so callers can report whether a fetch
/// touched the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchProvenance {
    /// The object was already in the mirror; no upstream request was made.
    ServedFromCache,
    /// The object was fetched from the upstream repository.
    FetchedFromUpstream,
}

impl FetchProvenance {
    /// Whether this fetch reached the upstream.
    pub fn fetched_from_upstream(self) -> bool {
        matches!(self, Self::FetchedFromUpstream)
    }
}

/// Refs under which fetched commits are anchored, keeping them reachable for
/// `git gc` while the mirror serves them.
pub const FETCHED_REF_PREFIX: &str = "refs/preloop/commits";

/// Whether the mirror holds `sha` as a commit.
///
/// Any failure to ask — a non-zero exit, a spawn error, or an expired deadline
/// — counts as "does not hold it": the caller answers by fetching, which is
/// the safe direction.
pub async fn cache_holds_commit(repository: &Path, sha: &str) -> bool {
    if !crate::is_full_object_id(sha) {
        return false;
    }
    let mut command = crate::git_command();
    command
        .arg("--git-dir")
        .arg(repository)
        .args(["cat-file", "-e", &format!("{sha}^{{commit}}")]);
    crate::run_git_within(command, "check cached commit", GIT_CONTROL_TIMEOUT)
        .await
        .is_ok_and(|output| output.status.success())
}

/// Stage an origin-scoped credential header for a fetch.
///
/// The child *environment*, never the command line: `GIT_CONFIG_*` is readable
/// only by the process owner, while argv is world-readable through process
/// listings on the platforms engines run on — and the value staged here is a
/// live bearer token. Git reads `GIT_CONFIG_*` as `-c`, so it carries the same
/// precedence, and [`crate::git_command`] deliberately leaves these variables
/// alone for exactly this.
pub fn stage_auth_header(command: &mut tokio::process::Command, key: &str, value: &str) {
    command
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", key)
        .env("GIT_CONFIG_VALUE_0", value);
}

/// Initialize a bare mirror when it does not exist yet.
pub async fn ensure_bare_repository(repository: &Path) -> Result<(), MirrorError> {
    if repository.is_dir() {
        return Ok(());
    }
    let mut init = crate::git_command();
    init.arg("init").arg("--bare").arg(repository);
    crate::run_git_checked(init, "initialize bare mirror", GIT_CONTROL_TIMEOUT).await?;
    Ok(())
}

/// Point `ref_name` at `sha`.
pub async fn publish_ref(repository: &Path, ref_name: &str, sha: &str) -> Result<(), MirrorError> {
    if !ref_name.starts_with("refs/") {
        return Err(MirrorError::InvalidInput(format!(
            "`{ref_name}` is not a full ref name"
        )));
    }
    let sha = crate::safe_revision(sha)?;
    let mut update = crate::git_command();
    update
        .arg("--git-dir")
        .arg(repository)
        .arg("update-ref")
        .arg(ref_name)
        .arg(sha);
    crate::run_git_checked(update, "publish mirror ref", GIT_CONTROL_TIMEOUT).await?;
    Ok(())
}

/// Resolve `rev` to its tree object id.
pub async fn resolve_tree(repository: &Path, rev: &str) -> Result<String, MirrorError> {
    let rev = crate::safe_revision(rev)?;
    let mut tree = crate::git_command();
    tree.arg("--git-dir")
        .arg(repository)
        .args(["rev-parse", &format!("{rev}^{{tree}}")]);
    let output = crate::run_git_checked(tree, "resolve tree", GIT_CONTROL_TIMEOUT).await?;
    let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !crate::is_full_object_id(&value) {
        return Err(MirrorError::MissingObject(rev.to_owned()));
    }
    Ok(value)
}

/// Fetch one commit into the mirror, skipping the network when it is already
/// present.
///
/// The credential-helper reset rides on the command line
/// ([`crate::git_command`]) while the origin-scoped header is staged in the
/// child environment ([`stage_auth_header`]) — the value is a live token and
/// argv is world-readable. Nothing here needs a helper: the header is the
/// credential.
///
/// `deadline` bounds the whole fetch, and expiry kills the child, so a stalled
/// upstream can never keep writing into the mirror after the caller releases
/// the lock.
pub async fn fetch_commit(
    repository: &Path,
    upstream: &str,
    commit_sha: &str,
    auth_header: Option<(&str, &str)>,
    deadline: std::time::Duration,
) -> Result<FetchProvenance, MirrorError> {
    fetch_commit_with_program(
        "git",
        repository,
        upstream,
        commit_sha,
        auth_header,
        deadline,
    )
    .await
}

/// [`fetch_commit`] against an explicit Git binary.
///
/// A call site that substitutes the binary (a script that records argv, or one
/// that hangs to exercise the deadline) needs the same policy and provenance
/// without reimplementing the presence check.
pub async fn fetch_commit_with_program(
    program: impl AsRef<std::ffi::OsStr>,
    repository: &Path,
    upstream: &str,
    commit_sha: &str,
    auth_header: Option<(&str, &str)>,
    deadline: std::time::Duration,
) -> Result<FetchProvenance, MirrorError> {
    let commit_sha = crate::safe_revision(commit_sha)?;
    // A commit already in the mirror needs no upstream round trip. Anchoring
    // it keeps it reachable for the garbage collector while the mirror serves
    // it, which a bare `fetch` of a raw sha never did.
    if crate::is_full_object_id(commit_sha) && cache_holds_commit(repository, commit_sha).await {
        publish_ref(
            repository,
            &format!("{FETCHED_REF_PREFIX}/{commit_sha}"),
            commit_sha,
        )
        .await?;
        return Ok(FetchProvenance::ServedFromCache);
    }
    // A previous fetch in this repository may have been killed at its
    // deadline, and its lock files would fail this one. The caller holds the
    // mirror lock, so nothing else is inside the repository.
    remove_stale_git_locks(repository, std::time::Duration::ZERO);
    let mut fetch = crate::git_command_for(program);
    if let Some((key, value)) = auth_header {
        stage_auth_header(&mut fetch, key, value);
    }
    fetch
        .arg("--git-dir")
        .arg(repository)
        .arg("fetch")
        .arg("--no-tags")
        .arg("--depth=1")
        .arg(upstream)
        .arg(commit_sha);
    crate::run_git_checked(fetch, "fetch mirror commit", deadline).await?;
    if crate::is_full_object_id(commit_sha) {
        publish_ref(
            repository,
            &format!("{FETCHED_REF_PREFIX}/{commit_sha}"),
            commit_sha,
        )
        .await?;
    }
    Ok(FetchProvenance::FetchedFromUpstream)
}

/// Remove Git lock files a killed child left behind.
///
/// Expiry is a `SIGKILL`, which Git cannot catch, so a fetch killed during a
/// ref, `shallow`, or `packed-refs` update leaves its lock file behind. A
/// caller holding the mirror lock passes `older_than = ZERO` — every lock file
/// found is a corpse of a previous kill. A caller *without* the lock passes
/// the longest engine fetch deadline: no live fetch can hold a lock older than
/// that, and anything younger is left alone rather than stolen from a fetch
/// that is still running.
///
/// Best effort: a failed removal is logged and the caller proceeds, where Git
/// reports the lock itself with a better message than this can.
pub fn remove_stale_git_locks(repository: &Path, older_than: std::time::Duration) {
    let now = std::time::SystemTime::now();
    // Only the trees Git takes lock files in: walking the object fanout
    // directories holds no locks and would cost more than the fetch's own
    // control commands.
    for (directory, recursive) in [
        (repository.to_path_buf(), false),
        (repository.join("refs"), true),
        (repository.join("logs"), true),
        (repository.join("objects").join("pack"), false),
    ] {
        remove_lock_files(&directory, recursive, older_than, now);
    }
}

fn remove_lock_files(
    directory: &Path,
    recursive: bool,
    older_than: std::time::Duration,
    now: std::time::SystemTime,
) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            if recursive {
                remove_lock_files(&path, true, older_than, now);
            }
            continue;
        }
        if path.extension().is_none_or(|extension| extension != "lock") {
            continue;
        }
        if !older_than.is_zero() {
            let Ok(modified) = entry.metadata().and_then(|metadata| metadata.modified()) else {
                continue;
            };
            if now.duration_since(modified).unwrap_or_default() < older_than {
                continue;
            }
        }
        match std::fs::remove_file(&path) {
            Ok(()) => tracing::warn!(
                path = %path.display(),
                "removed a Git lock file left by a process killed at its deadline"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(
                path = %path.display(),
                %error,
                "could not remove a stale Git lock file"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn git(directory: &Path, args: &[&str]) {
        let status = tokio::process::Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(args)
            .status()
            .await
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }

    async fn commit(repository: &Path, file: &str) -> String {
        std::fs::write(repository.join(file), file).unwrap();
        git(repository, &["add", file]).await;
        git(
            repository,
            &[
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@example.com",
                "commit",
                "-q",
                "-m",
                file,
            ],
        )
        .await;
        let head = tokio::process::Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(["rev-parse", "HEAD"])
            .output()
            .await
            .unwrap();
        String::from_utf8(head.stdout).unwrap().trim().to_owned()
    }

    #[tokio::test]
    async fn a_present_commit_is_served_without_an_upstream_fetch() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir_all(&source).unwrap();
        git(&source, &["init", "-q", "-b", "main"]).await;
        let sha = commit(&source, "a.txt").await;

        let mirror = temp.path().join("mirror.git");
        ensure_bare_repository(&mirror).await.unwrap();
        // Seed the mirror from the source, as a prior fetch would have.
        let seeded = fetch_commit(
            &mirror,
            &format!("file://{}", source.display()),
            &sha,
            None,
            GIT_CONTROL_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(seeded, FetchProvenance::FetchedFromUpstream);

        // A second fetch must not touch the upstream: point it at a path that
        // cannot exist and require the cached answer anyway.
        let cached = fetch_commit(
            &mirror,
            &format!("file://{}/does-not-exist.git", temp.path().display()),
            &sha,
            None,
            GIT_CONTROL_TIMEOUT,
        )
        .await
        .unwrap();
        assert_eq!(cached, FetchProvenance::ServedFromCache);
        assert!(cache_holds_commit(&mirror, &sha).await);
        // The anchoring ref exists, so `git gc` cannot collect the commit.
        let refs = tokio::process::Command::new("git")
            .arg("--git-dir")
            .arg(&mirror)
            .args(["rev-parse", &format!("{FETCHED_REF_PREFIX}/{sha}")])
            .output()
            .await
            .unwrap();
        assert!(refs.status.success());
        assert_eq!(String::from_utf8_lossy(&refs.stdout).trim(), sha);
    }

    #[tokio::test]
    async fn a_missing_commit_is_fetched_and_anchored() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::create_dir_all(&source).unwrap();
        git(&source, &["init", "-q", "-b", "main"]).await;
        let sha = commit(&source, "b.txt").await;
        let mirror = temp.path().join("mirror.git");
        ensure_bare_repository(&mirror).await.unwrap();

        assert!(!cache_holds_commit(&mirror, &sha).await);
        let provenance = fetch_commit(
            &mirror,
            &format!("file://{}", source.display()),
            &sha,
            None,
            GIT_CONTROL_TIMEOUT,
        )
        .await
        .unwrap();
        assert!(provenance.fetched_from_upstream());
        assert!(cache_holds_commit(&mirror, &sha).await);
    }

    #[tokio::test]
    async fn a_non_object_id_is_never_treated_as_cached() {
        let temp = tempfile::tempdir().unwrap();
        let mirror = temp.path().join("mirror.git");
        ensure_bare_repository(&mirror).await.unwrap();
        assert!(!cache_holds_commit(&mirror, "main").await);
        assert!(!cache_holds_commit(&mirror, "--upload-pack=evil").await);
    }

    #[test]
    fn stale_lock_files_are_removed_on_the_held_path() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("mirror.git");
        std::fs::create_dir_all(repository.join("refs/heads")).unwrap();
        std::fs::create_dir_all(repository.join("objects/pack")).unwrap();
        std::fs::write(repository.join("refs/heads/main.lock"), "").unwrap();
        std::fs::write(repository.join("packed-refs.lock"), "").unwrap();
        std::fs::write(repository.join("objects/pack/keep.pack"), "").unwrap();

        remove_stale_git_locks(&repository, std::time::Duration::ZERO);

        assert!(!repository.join("refs/heads/main.lock").exists());
        assert!(!repository.join("packed-refs.lock").exists());
        // Only `*.lock` files are removed.
        assert!(repository.join("objects/pack/keep.pack").exists());
    }
}
