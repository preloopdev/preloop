//! File-backed artifact storage for Preloop workflow runs.

use std::path::PathBuf;

use aksh_gha_protocol::RunId;
use tokio::fs;
use uuid::Uuid;

/// Artifact store error.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    /// I/O failed.
    #[error("artifact io error: {0}")]
    Io(#[from] std::io::Error),
    /// Artifact was not found.
    #[error("artifact `{0}` was not found")]
    NotFound(Uuid),
    /// Invalid artifact name.
    #[error("invalid artifact name: {0}")]
    InvalidName(String),
    /// Invalid file name.
    #[error("invalid file name: {0}")]
    InvalidFileName(String),
}

/// Validate an artifact name using the official toolkit's upload contract.
///
/// Reference: `actions/toolkit/packages/artifact/src/internal/upload/path-and-artifact-name-validation.ts`.
pub fn validate_artifact_name(name: &str) -> Result<(), ArtifactError> {
    if name.is_empty() {
        return Err(ArtifactError::InvalidName(
            "artifact name must not be empty".to_owned(),
        ));
    }
    if let Some(character) = name.chars().find(|character| {
        matches!(
            character,
            '"' | ':' | '<' | '>' | '|' | '*' | '?' | '\r' | '\n' | '/' | '\\'
        )
    }) {
        return Err(ArtifactError::InvalidName(format!(
            "artifact name contains invalid character: {character:?}"
        )));
    }
    Ok(())
}

fn validate_storage_relative_path(file_name: &str) -> Result<(), ArtifactError> {
    let path = std::path::Path::new(file_name);
    if file_name.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(ArtifactError::InvalidFileName(
            "artifact file path must be a non-empty relative path without `..`".to_owned(),
        ));
    }
    Ok(())
}

/// Artifact metadata.
#[derive(Debug, Clone)]
pub struct Artifact {
    /// Artifact id.
    pub id: Uuid,
    /// Run id.
    pub run_id: RunId,
    /// Artifact name.
    pub name: String,
    /// Stored file path.
    pub path: PathBuf,
    /// Size in bytes.
    pub size: u64,
}

/// File-backed artifact store.
#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    /// Create a store rooted at `root`.
    pub async fn new(root: impl Into<PathBuf>) -> Result<Self, ArtifactError> {
        let root = root.into();
        fs::create_dir_all(&root).await?;
        Ok(Self { root })
    }

    /// Save an artifact payload.
    ///
    /// Artifact-name validation exactly follows the official toolkit source:
    /// `actions/toolkit/packages/artifact/src/internal/upload/path-and-artifact-name-validation.ts`.
    /// File paths are distinct from artifact names and may contain separators;
    /// this store only rejects absolute or parent-traversal paths to keep the
    /// file-backed implementation rooted safely.
    pub async fn put(
        &self,
        run_id: RunId,
        name: &str,
        file_name: &str,
        bytes: &[u8],
    ) -> Result<Artifact, ArtifactError> {
        validate_artifact_name(name)?;
        validate_storage_relative_path(file_name)?;
        let id = Uuid::new_v4();
        let path = self
            .root
            .join(run_id.to_string())
            .join(id.to_string())
            .join(file_name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&path, bytes).await?;
        Ok(Artifact {
            id,
            run_id,
            name: name.to_owned(),
            path,
            size: bytes.len() as u64,
        })
    }

    /// Read an artifact payload.
    pub async fn get(&self, artifact: &Artifact) -> Result<Vec<u8>, ArtifactError> {
        Ok(fs::read(&artifact.path).await?)
    }

    /// List all artifact files for a run, including nested paths.
    pub async fn list_run(&self, run_id: RunId) -> Result<Vec<PathBuf>, ArtifactError> {
        let run_root = self.root.join(run_id.to_string());
        if !fs::try_exists(&run_root).await? {
            return Ok(Vec::new());
        }
        let mut paths = Vec::new();
        let mut pending = vec![run_root];
        while let Some(directory) = pending.pop() {
            let mut entries = fs::read_dir(directory).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if entry.file_type().await?.is_dir() {
                    pending.push(path);
                } else {
                    paths.push(path);
                }
            }
        }
        Ok(paths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn invalid_name() -> impl Strategy<Value = String> {
        prop_oneof![
            Just(String::new()),
            prop::string::string_regex(".*[\"/:?*|<>\\\\\r\n].*").unwrap(),
        ]
    }
    #[tokio::test]
    async fn stores_artifact_payloads() {
        let temp = tempfile::tempdir().unwrap();
        let store = ArtifactStore::new(temp.path()).await.unwrap();
        let run_id = RunId::new();

        let artifact = store
            .put(run_id, "logs", "job.txt", b"hello")
            .await
            .unwrap();
        let bytes = store.get(&artifact).await.unwrap();

        assert_eq!(bytes, b"hello");
        assert_eq!(store.list_run(run_id).await.unwrap().len(), 1);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_artifact_roundtrip(
            ref name in "[a-zA-Z0-9_-]{1,32}",
            ref file_name in "[a-zA-Z0-9_-]{1,32}",
            ref payload in prop::collection::vec(0..=255u8, 0..1024)
        ) {
            let temp = tempfile::tempdir().unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = ArtifactStore::new(temp.path()).await.unwrap();
                let run_id = RunId::new();
                let artifact = store.put(run_id, name, file_name, payload).await.unwrap();
                assert_eq!(artifact.run_id, run_id);
                assert_eq!(artifact.name, *name);

                // Assert path safety: must stay inside root
                assert!(artifact.path.starts_with(temp.path()));

                let restored = store.get(&artifact).await.unwrap();
                assert_eq!(restored, *payload);

                let list = store.list_run(run_id).await.unwrap();
                assert_eq!(list.len(), 1);
                assert_eq!(list[0], artifact.path);
            });
        }
        #[test]
        fn test_artifact_invalid_names(
            ref name in invalid_name(),
            ref file_name in "[a-zA-Z0-9_-]{1,32}"
        ) {
            let temp = tempfile::tempdir().unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let store = ArtifactStore::new(temp.path()).await.unwrap();
                let result = store.put(RunId::new(), name, file_name, b"payload").await;
                assert!(matches!(result, Err(ArtifactError::InvalidName(_))));
            });
        }
    }

    #[tokio::test]
    async fn rejects_unsafe_storage_relative_paths() {
        let temp = tempfile::tempdir().unwrap();
        let store = ArtifactStore::new(temp.path()).await.unwrap();
        for file_name in ["../escape", "/absolute"] {
            let result = store
                .put(RunId::new(), "valid-name", file_name, b"payload")
                .await;
            assert!(matches!(result, Err(ArtifactError::InvalidFileName(_))));
        }
    }
}
