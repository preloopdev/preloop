//! File-backed artifact storage for Preloop workflow runs.

use std::path::PathBuf;

use preloop_gha_protocol::RunId;
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
    pub async fn put(
        &self,
        run_id: RunId,
        name: &str,
        file_name: &str,
        bytes: &[u8],
    ) -> Result<Artifact, ArtifactError> {
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

    /// List artifacts for a run.
    pub async fn list_run(&self, run_id: RunId) -> Result<Vec<PathBuf>, ArtifactError> {
        let run_root = self.root.join(run_id.to_string());
        if !fs::try_exists(&run_root).await? {
            return Ok(Vec::new());
        }
        let mut paths = Vec::new();
        let mut artifacts = fs::read_dir(run_root).await?;
        while let Some(artifact_dir) = artifacts.next_entry().await? {
            let mut files = fs::read_dir(artifact_dir.path()).await?;
            while let Some(file) = files.next_entry().await? {
                paths.push(file.path());
            }
        }
        Ok(paths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
