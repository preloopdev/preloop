//! Detect and revert what a failed step changed in the workspace.
//!
//! The pristine workspace snapshot the job checked out is already a complete
//! undo log for everything worth undoing, so nothing has to be recorded ahead
//! of time and no file content is ever stored:
//!
//! - **Tracked** files are restorable from the snapshot commit.
//! - **Untracked** files are removable.
//! - **Ignored** build output and caches are *deliberately never touched* —
//!   reverting `target/` would destroy the warm state that makes retrying in
//!   place worth doing at all.
//!
//! That third category is also why this is cheap: the expensive tree to walk is
//! the one we must not revert, and `git` skips it by construction.
//!
//! Detection still costs two `git` invocations per step while debugging is
//! enabled — the baseline has to be captured *before* a step runs, and no step
//! announces in advance that it is about to fail. Those invocations are
//! therefore kept off the async runtime; see [`diff_workspace_async`]. A run
//! without pause-on-failure pays nothing at all.

use std::path::{Component, Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

use aksh_gha_protocol::debug_session::{
    ChangeCategory, ChangeStatus, RevertPolicy, WorkspaceChange, WorkspaceDiff,
};

/// Diff the live workspace against the commit the job checked out.
///
/// Ignored paths are not enumerated and never appear in the result.
pub fn diff_workspace(workspace: &Path, snapshot_commit: &str) -> Result<WorkspaceDiff> {
    let mut changes = Vec::new();

    // Tracked changes relative to the pristine tree. Content-compared by git,
    // so this also catches writes that preserved mtime — `cp -p`, tar
    // extraction, ccache — which an mtime scan would miss.
    let tracked = git(
        workspace,
        &["diff", "--name-status", "-z", snapshot_commit, "--"],
    )?;
    changes.extend(parse_name_status(&tracked));

    // Untracked but not ignored. `--exclude-standard` is what keeps this from
    // descending into `target/`.
    let untracked = git(
        workspace,
        &["ls-files", "--others", "--exclude-standard", "-z"],
    )?;
    for path in untracked.split('\0').filter(|s| !s.is_empty()) {
        changes.push(WorkspaceChange {
            path: path.to_owned(),
            status: ChangeStatus::Added,
            category: ChangeCategory::Untracked,
        });
    }

    let mut counts = std::collections::BTreeMap::new();
    for change in &changes {
        let key = match change.category {
            ChangeCategory::Tracked => "tracked",
            ChangeCategory::Untracked => "untracked",
            ChangeCategory::Cache => "cache",
        };
        *counts.entry(key.to_owned()).or_insert(0) += 1;
    }

    Ok(WorkspaceDiff { changes, counts })
}

/// Parse a `git diff --name-status -z` stream into revert actions.
///
/// The stream is `status\0path\0` per entry — except renames and copies, which
/// carry `status\0old\0new\0`. Consuming one path unconditionally read the new
/// path as the *following* entry's status, so one `git mv` desynchronised every
/// change after it in the diff: paths reported as statuses, statuses as paths,
/// and the tail silently dropped.
///
/// Rename detection is parsed rather than suppressed with `--no-renames`. The
/// two-path shape has to be handled correctly regardless: copy detection is
/// reachable from a repo's own `diff.renames = copies`, which the runner does
/// not own, and a parser whose invariant depends on a flag being present breaks
/// quietly the day the flag moves.
///
/// A rename becomes the two facts a revert needs: the old path is missing and
/// restorable from the snapshot, the new path is an addition to delete. A copy
/// leaves its source alone, so only the new path is reported.
fn parse_name_status(stream: &str) -> Vec<WorkspaceChange> {
    fn tracked(path: &str, status: ChangeStatus) -> WorkspaceChange {
        WorkspaceChange {
            path: path.to_owned(),
            status,
            category: ChangeCategory::Tracked,
        }
    }

    let mut changes = Vec::new();
    let mut fields = stream.split('\0').filter(|field| !field.is_empty());
    while let Some(status) = fields.next() {
        let Some(code) = status.chars().next() else {
            continue;
        };
        match code {
            'R' | 'C' => {
                let (Some(old), Some(new)) = (fields.next(), fields.next()) else {
                    break;
                };
                // The source of a copy still holds its snapshot content; only a
                // rename leaves a hole where the old path was.
                if code == 'R' {
                    changes.push(tracked(old, ChangeStatus::Deleted));
                }
                changes.push(tracked(new, ChangeStatus::Added));
            }
            _ => {
                let Some(path) = fields.next() else { break };
                changes.push(tracked(
                    path,
                    match code {
                        'A' => ChangeStatus::Added,
                        'D' => ChangeStatus::Deleted,
                        // Type changes and unmerged entries revert the way a
                        // modification does: check the snapshot's version out.
                        _ => ChangeStatus::Modified,
                    },
                ));
            }
        }
    }
    changes
}

/// Whether reverting a change means deleting the path rather than restoring it.
///
/// True when the pristine snapshot has no version to restore: an untracked
/// file, or a tracked addition — a staged new file, or the destination half of
/// a rename or copy.
fn reverts_by_deletion(change: &WorkspaceChange) -> bool {
    match change.category {
        ChangeCategory::Untracked => true,
        ChangeCategory::Tracked => change.status == ChangeStatus::Added,
        ChangeCategory::Cache => false,
    }
}

/// Revert a selected subset of changes. Returns how many paths were reverted.
///
/// Refuses outright on a [`ChangeCategory::Cache`] entry rather than skipping
/// it. A silent skip would hide a caller bug, and the caller asking to revert
/// cache is always wrong — the build system owns that directory.
pub fn revert_paths(
    workspace: &Path,
    snapshot_commit: &str,
    paths: &[WorkspaceChange],
) -> Result<usize> {
    for change in paths {
        if change.category == ChangeCategory::Cache {
            bail!(
                "refusing to revert cached build output: {} — reverting it would \
                 discard the warm state that makes retrying in place fast",
                change.path
            );
        }
        reject_escaping_path(&change.path)?;
    }

    let mut reverted = 0usize;

    // Restore, in one call, every tracked path the pristine commit still has.
    let restore: Vec<&str> = paths
        .iter()
        .filter(|c| c.category == ChangeCategory::Tracked && !reverts_by_deletion(c))
        .map(|c| c.path.as_str())
        .collect();
    if !restore.is_empty() {
        let mut args = vec!["checkout", snapshot_commit, "--"];
        args.extend(restore.iter().copied());
        git(workspace, &args).context("restoring tracked files from the workspace snapshot")?;
        reverted += restore.len();
    }

    // A tracked addition is absent from the snapshot, so `git checkout` cannot
    // produce it — it fails the *whole* batch with "did not exist in", taking
    // the genuinely restorable paths down with it. Dropping the index entry is
    // what returns the index to the snapshot; the file itself is deleted below,
    // through the same guarded removal untracked debris goes through.
    let staged: Vec<&str> = paths
        .iter()
        .filter(|c| c.category == ChangeCategory::Tracked && reverts_by_deletion(c))
        .map(|c| c.path.as_str())
        .collect();
    if !staged.is_empty() {
        // `--force` because git guards `--cached` against dropping an index
        // entry that differs from both HEAD and the worktree — a staged edit to
        // a renamed file is exactly that, and discarding it is the whole point:
        // the file itself is deleted immediately below.
        let mut args = vec![
            "rm",
            "--cached",
            "--force",
            "--quiet",
            "--ignore-unmatch",
            "--",
        ];
        args.extend(staged.iter().copied());
        git(workspace, &args).context("unstaging paths the workspace snapshot does not have")?;
    }

    // Paths that did not exist in the pristine tree: removal is the revert.
    for change in paths.iter().filter(|c| reverts_by_deletion(c)) {
        let target = workspace.join(&change.path);
        // Lexical rejection above stops `../`, but not a symlinked parent:
        // a step that leaves `link -> /etc` behind would turn `link/passwd`
        // into a delete outside the workspace. Resolve the parent and confirm
        // it is still inside before unlinking anything.
        if !resolves_inside(workspace, &target)? {
            bail!(
                "refusing to revert {} — it resolves outside the workspace",
                change.path
            );
        }
        let outcome = if target.is_dir() {
            std::fs::remove_dir_all(&target)
        } else {
            std::fs::remove_file(&target)
        };
        match outcome {
            Ok(()) => reverted += 1,
            // Already gone is the desired end state.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("removing {}", target.display()))
            }
        }
    }

    Ok(reverted)
}

/// Whether `target`'s parent directory is still inside `workspace` once every
/// symlink on the way has been resolved.
///
/// The leaf itself is deliberately not canonicalized: a symlink is removed as
/// a link, and resolving it would ask about the wrong file.
fn resolves_inside(workspace: &Path, target: &Path) -> Result<bool> {
    let Some(parent) = target.parent() else {
        return Ok(false);
    };
    let root = workspace
        .canonicalize()
        .with_context(|| format!("resolving workspace {}", workspace.display()))?;
    match parent.canonicalize() {
        Ok(resolved) => Ok(resolved.starts_with(&root)),
        // Already gone: nothing to delete, so nothing to escape through.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error).with_context(|| format!("resolving {}", parent.display())),
    }
}

/// Reject absolute paths and `..` traversal.
///
/// Paths reach this from a controller over HTTP, so a workspace-relative path
/// is an assumption to enforce rather than trust.
fn reject_escaping_path(path: &str) -> Result<()> {
    let candidate = Path::new(path);
    if candidate.is_absolute() {
        bail!("refusing to revert an absolute path: {path}");
    }
    if candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        bail!("refusing to revert a path escaping the workspace: {path}");
    }
    Ok(())
}

fn git(workspace: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(args)
        .output()
        .with_context(|| format!("running git {}", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// [`diff_workspace`] off the async runtime.
///
/// `git diff` against a large tree is seconds of blocking work. Run inline it
/// stalls a tokio worker thread, and with it every other job this worker is
/// streaming logs for.
pub async fn diff_workspace_async(
    workspace: PathBuf,
    snapshot_commit: String,
) -> Result<WorkspaceDiff> {
    tokio::task::spawn_blocking(move || diff_workspace(&workspace, &snapshot_commit))
        .await
        .context("workspace diff task panicked")?
}

/// [`revert_paths`] off the async runtime.
pub async fn revert_paths_async(
    workspace: PathBuf,
    snapshot_commit: String,
    paths: Vec<WorkspaceChange>,
) -> Result<usize> {
    tokio::task::spawn_blocking(move || revert_paths(&workspace, &snapshot_commit, &paths))
        .await
        .context("workspace revert task panicked")?
}

/// Changes present in `current` but not in `baseline`.
///
/// Attributes debris to the attempt that produced it. Without this, a retry
/// would offer to revert files that were already dirty when the step started —
/// including anything the user edited while attached.
pub fn changes_since(baseline: &WorkspaceDiff, current: &WorkspaceDiff) -> Vec<WorkspaceChange> {
    let before: std::collections::HashSet<(&str, ChangeStatus)> = baseline
        .changes
        .iter()
        .map(|change| (change.path.as_str(), change.status))
        .collect();
    current
        .changes
        .iter()
        .filter(|change| !before.contains(&(change.path.as_str(), change.status)))
        .cloned()
        .collect()
}

/// Narrow a change set to what a revert policy permits.
pub fn select_for_policy(
    changes: &[WorkspaceChange],
    policy: RevertPolicy,
) -> Vec<WorkspaceChange> {
    changes
        .iter()
        .filter(|change| match policy {
            RevertPolicy::None => false,
            // Untracked-only can never discard an edit that predates the step:
            // these files did not exist in the pristine tree.
            RevertPolicy::Untracked => change.category == ChangeCategory::Untracked,
            RevertPolicy::All => change.category != ChangeCategory::Cache,
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        _dir: tempfile::TempDir,
        path: std::path::PathBuf,
        commit: String,
    }

    impl Fixture {
        /// A repo with a tracked source file and a gitignored `target/`,
        /// mirroring the shape of a real job workspace.
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().to_path_buf();
            let run = |args: &[&str]| {
                let status = Command::new("git")
                    .arg("-C")
                    .arg(&path)
                    .args(args)
                    .output()
                    .unwrap();
                assert!(
                    status.status.success(),
                    "git {args:?}: {}",
                    String::from_utf8_lossy(&status.stderr)
                );
            };
            run(&["init", "-q", "-b", "main"]);
            run(&["config", "user.email", "t@example.com"]);
            run(&["config", "user.name", "t"]);
            // Pin rename detection to git's default rather than inheriting the
            // developer's `diff.renames`, so the two-path entries these tests
            // exercise appear regardless of who runs them.
            run(&["config", "diff.renames", "true"]);
            std::fs::write(path.join(".gitignore"), "target/\n").unwrap();
            std::fs::write(path.join("lib.rs"), "original\n").unwrap();
            // A second tracked file, so a diff can carry an entry *after* a
            // rename — which is where a desynchronised parse shows up.
            std::fs::write(path.join("notes.md"), "notes\n").unwrap();
            std::fs::create_dir_all(path.join("target/debug")).unwrap();
            std::fs::write(path.join("target/debug/artifact"), "warm cache\n").unwrap();
            run(&["add", "."]);
            run(&["commit", "-qm", "pristine"]);
            let commit = String::from_utf8_lossy(
                &Command::new("git")
                    .arg("-C")
                    .arg(&path)
                    .args(["rev-parse", "HEAD"])
                    .output()
                    .unwrap()
                    .stdout,
            )
            .trim()
            .to_owned();
            Self {
                _dir: dir,
                path,
                commit,
            }
        }

        fn diff(&self) -> WorkspaceDiff {
            diff_workspace(&self.path, &self.commit).unwrap()
        }

        /// Run git in the fixture, asserting it succeeded.
        fn git(&self, args: &[&str]) {
            let output = Command::new("git")
                .arg("-C")
                .arg(&self.path)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        /// The raw `--name-status -z` stream `diff_workspace` parses.
        ///
        /// Asserted on directly so a test cannot pass vacuously when git
        /// declines to report the rename or copy it claims to exercise.
        fn name_status(&self) -> String {
            super::git(
                &self.path,
                &["diff", "--name-status", "-z", &self.commit, "--"],
            )
            .unwrap()
        }

        /// Every change as `(path, sigil)`, sorted — git's ordering across
        /// rename detection is not a contract worth asserting.
        fn shape(&self) -> Vec<(String, char)> {
            let mut shape: Vec<(String, char)> = self
                .diff()
                .changes
                .iter()
                .map(|change| (change.path.clone(), change.status.sigil()))
                .collect();
            shape.sort();
            shape
        }
    }

    /// The `-z` stream is parsed by entry shape, not by position: a rename or
    /// copy carries two paths, and consuming one left the second to be read as
    /// the next entry's status — mislabelling everything after it and dropping
    /// the tail.
    #[test]
    fn two_path_entries_do_not_desynchronise_the_parse() {
        let parsed = parse_name_status(
            "R096\0old.rs\0new.rs\0M\0kept.rs\0C100\0src.rs\0dup.rs\0A\0added.rs\0D\0gone.rs\0",
        );
        let shape: Vec<(&str, char)> = parsed
            .iter()
            .map(|change| (change.path.as_str(), change.status.sigil()))
            .collect();
        assert_eq!(
            shape,
            vec![
                // A rename is the two facts a revert needs.
                ("old.rs", '-'),
                ("new.rs", '+'),
                ("kept.rs", 'M'),
                // A copy's source still holds its snapshot content, so
                // `src.rs` is deliberately absent.
                ("dup.rs", '+'),
                ("added.rs", '+'),
                ("gone.rs", '-'),
            ]
        );
        assert!(parsed
            .iter()
            .all(|change| change.category == ChangeCategory::Tracked));
        // A truncated entry invents nothing.
        assert!(parse_name_status("R100\0old-only\0").is_empty());
    }

    /// A staged rename: `git mv` puts both halves in the index, so git reports
    /// one `R` entry with two paths. The old path is restorable; the new one
    /// exists nowhere in the snapshot and must be deleted instead — a
    /// `git checkout <snapshot> -- new` fails the *whole* batch, taking the
    /// restorable paths down with it.
    #[test]
    fn a_staged_rename_restores_the_old_path_and_deletes_the_new_one() {
        let fixture = Fixture::new();
        fixture.git(&["mv", "lib.rs", "renamed.rs"]);

        let raw = fixture.name_status();
        assert!(
            raw.split('\0').any(|field| field.starts_with('R')),
            "git must report a rename here, got {raw:?}"
        );
        assert_eq!(
            fixture.shape(),
            vec![("lib.rs".to_owned(), '-'), ("renamed.rs".to_owned(), '+')]
        );

        let diff = fixture.diff();
        assert!(diff
            .changes
            .iter()
            .all(|c| c.category == ChangeCategory::Tracked));
        assert_eq!(
            revert_paths(&fixture.path, &fixture.commit, &diff.changes).unwrap(),
            2
        );
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("lib.rs")).unwrap(),
            "original\n"
        );
        assert!(
            !fixture.path.join("renamed.rs").exists(),
            "the rename's destination must be gone"
        );
        // Index as well as worktree: a leftover staged addition would make the
        // retry's own diff report debris that is no longer there.
        assert_eq!(fixture.shape(), Vec::new());
    }

    /// An unstaged `mv` cannot be a rename — the destination is untracked, so
    /// git reports a tracked deletion plus untracked debris. Both halves still
    /// have to revert, through different mechanisms.
    #[test]
    fn an_unstaged_rename_restores_the_old_path_and_deletes_the_new_one() {
        let fixture = Fixture::new();
        std::fs::rename(fixture.path.join("lib.rs"), fixture.path.join("renamed.rs")).unwrap();

        let diff = fixture.diff();
        let old = diff
            .changes
            .iter()
            .find(|c| c.path == "lib.rs")
            .expect("the vacated path must be reported");
        assert_eq!(old.status, ChangeStatus::Deleted);
        assert_eq!(old.category, ChangeCategory::Tracked);
        let new = diff
            .changes
            .iter()
            .find(|c| c.path == "renamed.rs")
            .expect("the destination must be reported");
        assert_eq!(new.status, ChangeStatus::Added);
        assert_eq!(
            new.category,
            ChangeCategory::Untracked,
            "an unstaged destination is not in the index"
        );

        revert_paths(&fixture.path, &fixture.commit, &diff.changes).unwrap();
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("lib.rs")).unwrap(),
            "original\n"
        );
        assert!(!fixture.path.join("renamed.rs").exists());
        assert_eq!(fixture.shape(), Vec::new());
    }

    /// A rename that also edited the file, with an ordinary modification in the
    /// same diff. The trailing entry is the point: it is what a desynchronised
    /// parse mislabels or loses.
    #[test]
    fn a_modified_rename_keeps_the_rest_of_the_diff_intact() {
        let fixture = Fixture::new();
        fixture.git(&["mv", "lib.rs", "renamed.rs"]);
        // Edited too, but similar enough to stay above git's 50% rename
        // threshold — a heavier edit is reported as an unrelated add and delete
        // instead, which is the case the unstaged test already covers.
        std::fs::write(fixture.path.join("renamed.rs"), "original\nedit\n").unwrap();
        std::fs::write(fixture.path.join("notes.md"), "clobbered\n").unwrap();

        let raw = fixture.name_status();
        assert!(
            raw.split('\0').any(|field| field.starts_with('R')),
            "git must report a rename here, got {raw:?}"
        );
        assert_eq!(
            fixture.shape(),
            vec![
                ("lib.rs".to_owned(), '-'),
                ("notes.md".to_owned(), 'M'),
                ("renamed.rs".to_owned(), '+'),
            ]
        );

        let diff = fixture.diff();
        revert_paths(&fixture.path, &fixture.commit, &diff.changes).unwrap();
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("lib.rs")).unwrap(),
            "original\n"
        );
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("notes.md")).unwrap(),
            "notes\n",
            "the entry after the rename must be restored, not skipped"
        );
        assert!(!fixture.path.join("renamed.rs").exists());
        assert_eq!(fixture.shape(), Vec::new());
    }

    /// Copy detection is reachable from the repo's own `diff.renames`, which the
    /// runner does not own — so the parser has to handle `C` whether or not we
    /// would have asked for it. A copy leaves its source alone: only the new
    /// path is debris, and reverting it must not delete the original.
    #[test]
    fn a_detected_copy_deletes_only_the_new_path() {
        let fixture = Fixture::new();
        fixture.git(&["config", "diff.renames", "copies"]);
        // A copy is detected against a source that is itself in the diff, and
        // the destination has to be staged to be part of one at all.
        std::fs::write(fixture.path.join("lib.rs"), "regenerated\n").unwrap();
        std::fs::write(fixture.path.join("copy.rs"), "original\n").unwrap();
        fixture.git(&["add", "copy.rs"]);

        let raw = fixture.name_status();
        assert!(
            raw.split('\0').any(|field| field.starts_with('C')),
            "git must report a copy here, got {raw:?}"
        );
        assert_eq!(
            fixture.shape(),
            vec![("copy.rs".to_owned(), '+'), ("lib.rs".to_owned(), 'M')]
        );

        let diff = fixture.diff();
        revert_paths(&fixture.path, &fixture.commit, &diff.changes).unwrap();
        assert!(
            !fixture.path.join("copy.rs").exists(),
            "the copy's destination is debris"
        );
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("lib.rs")).unwrap(),
            "original\n",
            "the copy's source is restored, not deleted"
        );
        assert_eq!(fixture.shape(), Vec::new());
    }

    /// A symlinked parent must not turn a workspace-relative revert into a
    /// delete somewhere else on the filesystem.
    ///
    /// `reject_escaping_path` is lexical, so `link/victim` passes it: the
    /// escape happens when the kernel resolves `link`. A failed step can leave
    /// such a link behind, and the revert then runs with the runner's rights.
    #[test]
    fn a_symlinked_parent_cannot_smuggle_a_revert_out_of_the_workspace() {
        let fixture = Fixture::new();
        let outside = tempfile::tempdir().unwrap();
        let victim = outside.path().join("victim");
        std::fs::write(&victim, "not yours\n").unwrap();

        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), fixture.path.join("link")).unwrap();

        let error = revert_paths(
            &fixture.path,
            &fixture.commit,
            &[WorkspaceChange {
                path: "link/victim".to_owned(),
                status: ChangeStatus::Added,
                category: ChangeCategory::Untracked,
            }],
        )
        .expect_err("a revert resolving outside the workspace must be refused");
        assert!(
            error.to_string().contains("outside the workspace"),
            "unexpected error: {error}"
        );
        assert!(
            victim.exists(),
            "the file outside the workspace was deleted"
        );
    }

    /// The link itself is untracked debris and is removable — only what it
    /// points *through* is off limits.
    #[test]
    fn an_untracked_symlink_is_removed_as_a_link() {
        let fixture = Fixture::new();
        let outside = tempfile::tempdir().unwrap();
        let victim = outside.path().join("victim");
        std::fs::write(&victim, "not yours\n").unwrap();

        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), fixture.path.join("link")).unwrap();

        let reverted = revert_paths(
            &fixture.path,
            &fixture.commit,
            &[WorkspaceChange {
                path: "link".to_owned(),
                status: ChangeStatus::Added,
                category: ChangeCategory::Untracked,
            }],
        )
        .unwrap();
        assert_eq!(reverted, 1);
        assert!(!fixture.path.join("link").is_symlink());
        assert!(victim.exists(), "removing the link must not follow it");
    }

    #[test]
    fn a_clean_workspace_has_no_changes() {
        let fixture = Fixture::new();
        let diff = fixture.diff();
        assert!(diff.changes.is_empty(), "unexpected: {:?}", diff.changes);
        assert!(!diff.has_revertible());
    }

    #[test]
    fn tracked_modification_is_detected_and_restored() {
        let fixture = Fixture::new();
        std::fs::write(fixture.path.join("lib.rs"), "corrupted by failed step\n").unwrap();

        let diff = fixture.diff();
        let change = diff
            .changes
            .iter()
            .find(|c| c.path == "lib.rs")
            .expect("modified tracked file must be detected");
        assert_eq!(change.status, ChangeStatus::Modified);
        assert_eq!(change.category, ChangeCategory::Tracked);

        let reverted = revert_paths(&fixture.path, &fixture.commit, &diff.changes).unwrap();
        assert_eq!(reverted, 1);
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("lib.rs")).unwrap(),
            "original\n"
        );
        assert!(fixture.diff().changes.is_empty());
    }

    #[test]
    fn tracked_deletion_is_detected_and_restored() {
        let fixture = Fixture::new();
        std::fs::remove_file(fixture.path.join("lib.rs")).unwrap();

        let diff = fixture.diff();
        let change = diff.changes.iter().find(|c| c.path == "lib.rs").unwrap();
        assert_eq!(change.status, ChangeStatus::Deleted);

        revert_paths(&fixture.path, &fixture.commit, &diff.changes).unwrap();
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("lib.rs")).unwrap(),
            "original\n"
        );
    }

    #[test]
    fn untracked_junk_is_detected_and_deleted() {
        // The `mkdir build && cmake ..` case: the failed attempt leaves debris
        // that makes the retry fail for a different reason.
        let fixture = Fixture::new();
        std::fs::create_dir_all(fixture.path.join("build")).unwrap();
        std::fs::write(fixture.path.join("build/stale"), "junk\n").unwrap();

        let diff = fixture.diff();
        let change = diff
            .changes
            .iter()
            .find(|c| c.path.starts_with("build/"))
            .expect("untracked junk must be detected");
        assert_eq!(change.category, ChangeCategory::Untracked);

        revert_paths(&fixture.path, &fixture.commit, &diff.changes).unwrap();
        assert!(!fixture.path.join("build/stale").exists());
    }

    #[test]
    fn gitignored_cache_is_never_enumerated() {
        // The warm `target/` must not appear in the diff at all: enumerating it
        // is the expensive thing, and reverting it is always wrong.
        let fixture = Fixture::new();
        std::fs::write(fixture.path.join("target/debug/artifact"), "rebuilt\n").unwrap();
        std::fs::write(fixture.path.join("target/debug/new-file"), "fresh\n").unwrap();

        let diff = fixture.diff();
        assert!(
            diff.changes.iter().all(|c| !c.path.starts_with("target/")),
            "cache must not be enumerated, got {:?}",
            diff.changes
        );
    }

    #[test]
    fn reverting_cache_is_refused_rather_than_skipped() {
        let fixture = Fixture::new();
        let error = revert_paths(
            &fixture.path,
            &fixture.commit,
            &[WorkspaceChange {
                path: "target/debug/artifact".into(),
                status: ChangeStatus::Modified,
                category: ChangeCategory::Cache,
            }],
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("refusing to revert cached"),
            "got: {error}"
        );
        // And the cache is genuinely untouched.
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("target/debug/artifact")).unwrap(),
            "warm cache\n"
        );
    }

    #[test]
    fn revert_refuses_paths_escaping_the_workspace() {
        let fixture = Fixture::new();
        for path in ["/etc/passwd", "../outside", "a/../../b"] {
            let error = revert_paths(
                &fixture.path,
                &fixture.commit,
                &[WorkspaceChange {
                    path: path.into(),
                    status: ChangeStatus::Modified,
                    category: ChangeCategory::Tracked,
                }],
            )
            .unwrap_err();
            assert!(error.to_string().contains("refusing"), "{path}: {error}");
        }
    }

    #[test]
    fn counts_summarize_by_category() {
        let fixture = Fixture::new();
        std::fs::write(fixture.path.join("lib.rs"), "changed\n").unwrap();
        std::fs::write(fixture.path.join("stray.txt"), "junk\n").unwrap();

        let diff = fixture.diff();
        assert_eq!(diff.counts.get("tracked"), Some(&1));
        assert_eq!(diff.counts.get("untracked"), Some(&1));
        assert_eq!(diff.counts.get("cache"), None);
        assert!(diff.has_revertible());
    }

    #[test]
    fn attribution_excludes_dirt_that_predates_the_step() {
        // A workspace already dirty when the step began must not be offered up
        // for reverting — that dirt is the user's, not the attempt's.
        let fixture = Fixture::new();
        std::fs::write(fixture.path.join("lib.rs"), "user edit before the step\n").unwrap();
        let baseline = fixture.diff();

        // Now the step runs and leaves its own debris.
        std::fs::create_dir_all(fixture.path.join("build")).unwrap();
        std::fs::write(fixture.path.join("build/stale"), "junk\n").unwrap();
        let after = fixture.diff();

        let attributed = changes_since(&baseline, &after);
        let paths: Vec<&str> = attributed.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(paths, vec!["build/stale"]);
        assert!(
            !paths.contains(&"lib.rs"),
            "a pre-existing edit must never be attributed to the failed attempt"
        );
    }

    #[test]
    fn a_modification_on_top_of_a_pre_existing_one_is_not_double_counted() {
        let fixture = Fixture::new();
        std::fs::write(fixture.path.join("lib.rs"), "first\n").unwrap();
        let baseline = fixture.diff();
        std::fs::write(fixture.path.join("lib.rs"), "second\n").unwrap();
        // Same path, same status: already dirty before the step, so not the
        // attempt's to undo.
        assert!(changes_since(&baseline, &fixture.diff()).is_empty());
    }

    #[test]
    fn policy_none_selects_nothing() {
        let fixture = Fixture::new();
        std::fs::write(fixture.path.join("lib.rs"), "changed\n").unwrap();
        std::fs::write(fixture.path.join("stray.txt"), "junk\n").unwrap();
        let changes = fixture.diff().changes;
        assert!(select_for_policy(&changes, RevertPolicy::None).is_empty());
    }

    #[test]
    fn policy_untracked_never_touches_tracked_content() {
        let fixture = Fixture::new();
        std::fs::write(fixture.path.join("lib.rs"), "changed\n").unwrap();
        std::fs::write(fixture.path.join("stray.txt"), "junk\n").unwrap();
        let changes = fixture.diff().changes;

        let selected = select_for_policy(&changes, RevertPolicy::Untracked);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].path, "stray.txt");

        revert_paths(&fixture.path, &fixture.commit, &selected).unwrap();
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("lib.rs")).unwrap(),
            "changed\n",
            "untracked-only must not discard a tracked edit"
        );
    }

    #[test]
    fn policy_all_takes_everything_except_cache() {
        let fixture = Fixture::new();
        std::fs::write(fixture.path.join("lib.rs"), "changed\n").unwrap();
        std::fs::write(fixture.path.join("stray.txt"), "junk\n").unwrap();
        let mut changes = fixture.diff().changes;
        changes.push(WorkspaceChange {
            path: "target/debug/artifact".into(),
            status: ChangeStatus::Modified,
            category: ChangeCategory::Cache,
        });

        let selected = select_for_policy(&changes, RevertPolicy::All);
        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|c| c.category != ChangeCategory::Cache));

        revert_paths(&fixture.path, &fixture.commit, &selected).unwrap();
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("lib.rs")).unwrap(),
            "original\n"
        );
        assert!(!fixture.path.join("stray.txt").exists());
    }

    #[test]
    fn a_partial_revert_leaves_unselected_changes_alone() {
        // Answering "revert the untracked junk but keep my codegen" must do
        // exactly that.
        let fixture = Fixture::new();
        std::fs::write(fixture.path.join("lib.rs"), "regenerated\n").unwrap();
        std::fs::write(fixture.path.join("stray.txt"), "junk\n").unwrap();

        let diff = fixture.diff();
        let untracked: Vec<_> = diff.untracked().cloned().collect();
        revert_paths(&fixture.path, &fixture.commit, &untracked).unwrap();

        assert!(!fixture.path.join("stray.txt").exists());
        assert_eq!(
            std::fs::read_to_string(fixture.path.join("lib.rs")).unwrap(),
            "regenerated\n",
            "unselected tracked change must survive"
        );
    }
}
