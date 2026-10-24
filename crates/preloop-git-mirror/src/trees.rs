//! Commit and tree inspection over a bare mirror.
//!
//! Reuse decisions need two things a working tree cannot give: exactly which
//! paths differ between the tree a job ran on and the tree a candidate run
//! would run on, and a deterministic digest of the paths a job actually read.
//! Both are answered from object ids, so neither needs the repository checked
//! out and neither can be fooled by uncommitted files.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::{GIT_CONTROL_TIMEOUT, MirrorError};

/// Metadata for a single repository-relative path in a commit tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    /// Git mode string (`"100644"`, `"100755"`, `"120000"`, `"040000"`,
    /// `"160000"`).
    pub mode: String,
    /// Object kind (`"blob"`, `"tree"`, `"commit"`).
    pub kind: String,
    /// Full object id.
    pub oid: String,
}

impl TreeEntry {
    /// Whether this entry is a submodule (a gitlink to another repository).
    ///
    /// A gitlink's content lives in a different repository that this mirror
    /// does not track, so a job that read it cannot be fingerprinted from this
    /// tree alone.
    pub fn is_submodule(&self) -> bool {
        self.mode == "160000" || self.kind == "commit"
    }
}

/// Whether `rev` resolves to a commit present in `repository`.
pub async fn has_commit(repository: &Path, rev: &str) -> bool {
    let Ok(rev) = crate::safe_revision(rev) else {
        return false;
    };
    let mut command = crate::git_command();
    command
        .arg("--git-dir")
        .arg(repository)
        .args(["cat-file", "-e", &format!("{rev}^{{commit}}")]);
    crate::run_git_within(command, "probe commit", GIT_CONTROL_TIMEOUT)
        .await
        .is_ok_and(|output| output.status.success())
}

/// Resolve a commit or tree revision to its tree object id.
pub async fn commit_tree_sha(repository: &Path, rev: &str) -> Result<String, MirrorError> {
    let rev = crate::safe_revision(rev)?;
    let mut command = crate::git_command();
    command.arg("--git-dir").arg(repository).args([
        "rev-parse",
        "--verify",
        &format!("{rev}^{{tree}}"),
    ]);
    let output = crate::run_git_within(command, "resolve tree", GIT_CONTROL_TIMEOUT).await?;
    if !output.status.success() {
        return Err(MirrorError::MissingObject(rev.to_owned()));
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if !crate::is_full_object_id(&value) {
        return Err(MirrorError::MissingObject(rev.to_owned()));
    }
    Ok(value)
}

/// Paths that differ between two revisions' trees.
///
/// Fails closed: either revision must resolve, because "no differences" for a
/// revision the mirror cannot read would let a candidate run be treated as
/// identical to one it never compared against.
pub async fn diff_trees(
    repository: &Path,
    base_rev: &str,
    head_rev: &str,
) -> Result<BTreeSet<String>, MirrorError> {
    let base_tree = commit_tree_sha(repository, base_rev).await?;
    let head_tree = commit_tree_sha(repository, head_rev).await?;
    if base_tree == head_tree {
        return Ok(BTreeSet::new());
    }
    let mut command = crate::git_command();
    command
        .arg("--git-dir")
        .arg(repository)
        .args(["diff-tree", "-r", "--name-only", "--no-commit-id", "-z"])
        .arg(&base_tree)
        .arg(&head_tree);
    let output = crate::run_git_within(command, "diff trees", GIT_CONTROL_TIMEOUT).await?;
    if !output.status.success() {
        return Err(MirrorError::Git {
            operation: "diff trees".to_owned(),
            message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
        .map(|record| String::from_utf8_lossy(record).into_owned())
        .collect())
}

/// Look up the tree entries for `paths` in `rev`'s tree.
///
/// One `ls-tree` invocation lists a file as a blob entry and a directory as a
/// tree entry, so a single call answers both "what content did this job read"
/// and "what listing did it see". Paths absent from the tree simply do not
/// appear in the result, which is exactly what a caller needs to prove a
/// missing path was missing.
///
/// Paths are grouped by depth: `ls-tree` can omit a directory's own entry when
/// an ancestor and descendant pathspec share an invocation. Equal-depth paths
/// cannot contain one another. Deduplicating and chunking each group also bounds
/// argv without repeatedly comparing every pair of requested paths.
pub async fn ls_tree_entries(
    repository: &Path,
    rev: &str,
    paths: &[String],
) -> Result<BTreeMap<String, TreeEntry>, MirrorError> {
    const CHUNK: usize = 512;
    let root = commit_tree_sha(repository, rev).await?;
    let mut entries = BTreeMap::new();
    if paths.iter().any(|path| path.is_empty()) {
        entries.insert(
            String::new(),
            TreeEntry {
                mode: "040000".to_owned(),
                kind: "tree".to_owned(),
                oid: root.clone(),
            },
        );
    }
    let mut levels = BTreeMap::<usize, Vec<&str>>::new();
    for path in paths.iter().filter(|path| !path.is_empty()) {
        let depth = path.bytes().filter(|byte| *byte == b'/').count();
        levels.entry(depth).or_default().push(path.as_str());
    }
    for level in levels.values_mut() {
        level.sort_unstable();
        level.dedup();
        for chunk in level.chunks(CHUNK) {
            let mut command = crate::git_command();
            command
                .arg("--git-dir")
                .arg(repository)
                .arg("ls-tree")
                .arg("-z")
                .arg(&root)
                .arg("--");
            for path in chunk {
                command.arg(path);
            }
            let output =
                crate::run_git_within(command, "list tree entries", GIT_CONTROL_TIMEOUT).await?;
            if !output.status.success() {
                return Err(MirrorError::Git {
                    operation: "list tree entries".to_owned(),
                    message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
                });
            }
            collect_ls_tree(&output.stdout, &mut entries);
        }
    }
    Ok(entries)
}

/// Parse `ls-tree -z` output into `entries`.
fn collect_ls_tree(stdout: &[u8], entries: &mut BTreeMap<String, TreeEntry>) {
    for record in stdout.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        let Some(tab) = record.iter().position(|byte| *byte == b'\t') else {
            continue;
        };
        let (meta, path) = (&record[..tab], &record[tab + 1..]);
        let meta = String::from_utf8_lossy(meta);
        let mut fields = meta.split(' ');
        let (Some(mode), Some(kind), Some(oid)) = (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        if !crate::is_full_object_id(oid) {
            continue;
        }
        entries.insert(
            String::from_utf8_lossy(path).into_owned(),
            TreeEntry {
                mode: mode.to_owned(),
                kind: kind.to_owned(),
                oid: oid.to_owned(),
            },
        );
    }
}

/// Deterministic digest of a job's observed inputs against one tree.
///
/// The fingerprint answers "would this candidate tree give the job the same
/// bytes it read". It is domain-separated per category so a path cannot move
/// between `files`, `listed`, and `missing` without changing the digest.
///
/// Returns `None` — never a partial digest — when the tree cannot answer:
/// a submodule entry (content in another repository), a `files` path that is
/// not a blob, a `listed` path that is not a directory, or a `missing` path
/// that unexpectedly exists as a blob or directory. Failing closed is what
/// keeps a consumer job from being reused on evidence the fingerprint did not
/// actually cover.
pub fn compute_input_fingerprint(
    entries: &BTreeMap<String, TreeEntry>,
    files: &[String],
    listed: &[String],
    missing: &[String],
) -> Option<String> {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"preloop-input-fingerprint-v1\0");

    let mut files = files.to_vec();
    files.sort();
    files.dedup();
    for path in &files {
        let entry = entries.get(path)?;
        if entry.is_submodule() || entry.kind != "blob" {
            return None;
        }
        hasher.update(b"file\0");
        hasher.update(path.as_bytes());
        hasher.update(b"\0");
        hasher.update(entry.mode.as_bytes());
        hasher.update(b"\0");
        hasher.update(entry.oid.as_bytes());
        hasher.update(b"\0");
    }

    let mut listed = listed.to_vec();
    listed.sort();
    listed.dedup();
    for path in &listed {
        if path.is_empty() {
            // The workspace root is always a tree; its oid is the root tree.
            let entry = entries.get(path.as_str())?;
            hasher.update(b"listed\0\0tree\0");
            hasher.update(entry.oid.as_bytes());
            hasher.update(b"\0");
            continue;
        }
        let entry = entries.get(path)?;
        if entry.is_submodule() || entry.kind != "tree" {
            return None;
        }
        hasher.update(b"listed\0");
        hasher.update(path.as_bytes());
        hasher.update(b"\0tree\0");
        hasher.update(entry.oid.as_bytes());
        hasher.update(b"\0");
    }

    let mut missing = missing.to_vec();
    missing.sort();
    missing.dedup();
    for path in &missing {
        // A path the job found absent must still be absent here. If the
        // candidate tree has it, the job would have read content it never saw.
        if entries.contains_key(path) {
            return None;
        }
        hasher.update(b"missing\0");
        hasher.update(path.as_bytes());
        hasher.update(b"\0");
    }

    Some(format!("sha256:{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(directory: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }

    fn commit(repository: &Path, message: &str) -> String {
        git(repository, &["add", "-A"]);
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
                message,
            ],
        );
        String::from_utf8(
            std::process::Command::new("git")
                .arg("-C")
                .arg(repository)
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned()
    }

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, String, String) {
        let temp = tempfile::tempdir().unwrap();
        let work = temp.path().join("work");
        let mirror = temp.path().join("mirror.git");
        std::fs::create_dir_all(work.join("src/nested")).unwrap();
        git(&work, &["init", "-q", "-b", "main"]);
        git(&work, &["init", "--bare", "-q", mirror.to_str().unwrap()]);
        std::fs::write(work.join("README.md"), "readme\n").unwrap();
        std::fs::write(work.join("src/lib.rs"), "lib\n").unwrap();
        std::fs::write(work.join("src/nested/deep.rs"), "deep\n").unwrap();
        let base = commit(&work, "base");
        git(&work, &["push", "-q", mirror.to_str().unwrap(), "main"]);
        std::fs::write(work.join("src/lib.rs"), "lib v2\n").unwrap();
        std::fs::write(work.join("CHANGED.txt"), "new\n").unwrap();
        let head = commit(&work, "head");
        git(&work, &["push", "-q", mirror.to_str().unwrap(), "main"]);
        (temp, mirror, base, head)
    }

    #[tokio::test]
    async fn diffs_name_exactly_the_changed_paths() {
        let (_temp, mirror, base, head) = fixture();
        let changes = diff_trees(&mirror, &base, &head).await.unwrap();
        assert_eq!(
            changes,
            BTreeSet::from(["CHANGED.txt".to_owned(), "src/lib.rs".to_owned()])
        );
        // Comparing a revision with itself yields nothing.
        assert!(diff_trees(&mirror, &head, &head).await.unwrap().is_empty());
        // A revision the repository does not have fails closed.
        assert!(
            diff_trees(&mirror, &base, "0".repeat(40).as_str())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn ls_tree_separates_blobs_trees_and_absent_paths() {
        let (_temp, mirror, _base, head) = fixture();
        let paths = vec![
            String::new(),
            "README.md".to_owned(),
            "src".to_owned(),
            "src/nested".to_owned(),
            "nope.txt".to_owned(),
        ];
        let entries = ls_tree_entries(&mirror, &head, &paths).await.unwrap();
        assert_eq!(entries[""].kind, "tree");
        assert_eq!(entries["README.md"].kind, "blob");
        assert_eq!(entries["src"].kind, "tree");
        assert_eq!(entries["src/nested"].kind, "tree");
        assert!(!entries.contains_key("nope.txt"));
    }

    #[tokio::test]
    async fn fingerprints_change_with_content_and_are_stable_across_order() {
        let (_temp, mirror, base, head) = fixture();
        let paths = vec![String::new(), "README.md".to_owned(), "src".to_owned()];
        let entries = ls_tree_entries(&mirror, &head, &paths).await.unwrap();
        let first = compute_input_fingerprint(
            &entries,
            &["README.md".to_owned()],
            &[String::new(), "src".to_owned()],
            &["nope.txt".to_owned()],
        )
        .unwrap();
        let reordered = compute_input_fingerprint(
            &entries,
            &["README.md".to_owned(), "README.md".to_owned()],
            &["src".to_owned(), String::new()],
            &["nope.txt".to_owned()],
        )
        .unwrap();
        assert_eq!(first, reordered, "order and duplicates must not matter");

        // The same paths against a tree whose content differs must not be
        // treated as equivalent evidence.
        let work = mirror
            .parent()
            .unwrap()
            .join("work")
            .canonicalize()
            .unwrap();
        std::fs::write(work.join("README.md"), "readme v2\n").unwrap();
        let changed = commit(&work, "readme");
        git(&work, &["push", "-q", mirror.to_str().unwrap(), "main"]);
        let changed_entries = ls_tree_entries(&mirror, &changed, &paths).await.unwrap();
        let second = compute_input_fingerprint(
            &changed_entries,
            &["README.md".to_owned()],
            &[String::new(), "src".to_owned()],
            &["nope.txt".to_owned()],
        )
        .unwrap();
        assert_ne!(first, second);
        let _ = base;
    }

    #[test]
    fn a_fingerprint_refuses_evidence_it_cannot_cover() {
        let mut entries = BTreeMap::new();
        entries.insert(
            "src".to_owned(),
            TreeEntry {
                mode: "040000".to_owned(),
                kind: "tree".to_owned(),
                oid: "1".repeat(40),
            },
        );
        // A directory named as a read file is not a blob.
        assert!(compute_input_fingerprint(&entries, &["src".to_owned()], &[], &[]).is_none());
        // A submodule's content is another repository's.
        entries.insert(
            "vendor".to_owned(),
            TreeEntry {
                mode: "160000".to_owned(),
                kind: "commit".to_owned(),
                oid: "2".repeat(40),
            },
        );
        assert!(compute_input_fingerprint(&entries, &["vendor".to_owned()], &[], &[]).is_none());
        assert!(compute_input_fingerprint(&entries, &[], &["vendor".to_owned()], &[]).is_none());
        // A path the job found absent now exists.
        assert!(compute_input_fingerprint(&entries, &[], &[], &["src".to_owned()]).is_none());
    }
}
