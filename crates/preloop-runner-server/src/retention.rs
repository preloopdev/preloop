//! Run/check/status retention sweep, mirroring GitHub's Actions retention
//! setting (default 90 days).
//!
//! GitHub cleans up checks, workflow runs, and statuses per the Actions
//! retention setting instead of letting them live 400+ days. Preloop had no
//! such lifecycle: runs accumulated indefinitely. This module adds it — a
//! background sweep, wired into server startup, that deletes terminal runs
//! (plus their check-run records, commit statuses, artifacts, and logs)
//! once they are older than `retention_days`, from the durable control
//! backend so a restart cannot resurrect them, and from this node's caches.
//!
//! Only terminal runs are ever deleted: a run that is still queued,
//! pending, or in progress is never a candidate, no matter how old it is
//! (the backend enforces that structurally, not just in the selection). The
//! sweep is idempotent — candidates are selected from the durable rows and
//! every delete is a delete-if-present, so a repeated or interrupted pass
//! converges without double-deleting.
//!
//! Durable rows are the source of truth (`ControlBackend`), so the sweep
//! deletes through commands rather than rewriting a snapshot: a settled run
//! lives in the `runs` table until the archiver moves it to `run_history`,
//! and the selection reads both. What is left for this node is the
//! node-local cache: retained console logs, both artifact registries,
//! timeline projections, debug sessions — and the artifact bytes on disk.

use super::*;
use std::time::Duration;
use tracing::{info, warn};

/// How often the retention sweep runs. Run history is long-lived data; an
/// hourly pass matches the checkout-cache pruner and keeps each pass cheap.
const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(3_600);

/// Runs deleted per candidate batch. Bounded so one pass cannot hold the
/// state lock or a write transaction for an unbounded stretch; the sweep
/// loops until a batch comes back short.
const RETENTION_DELETE_BATCH: usize = 64;

/// Hard bound on how many runs one pass deletes. A backlog (retention
/// lowered, or a long outage) is drained hour by hour rather than in one
/// arbitrarily long request.
const RETENTION_MAX_RUNS_PER_PASS: usize = 10_000;

/// Outcome of one retention pass, for logs and tests.
#[derive(Debug, Default)]
pub struct RetentionSweepOutcome {
    /// Whether the pass ran at all (`false` when `retention_days` is 0).
    pub ran: bool,
    /// Runs deleted by this pass.
    pub deleted_runs: Vec<RunId>,
}

/// One retention pass: delete terminal runs older than the retention window
/// from the durable backend and this node's caches.
///
/// Candidates are terminal runs whose `completed_at` (falling back to
/// `created_at` when unset) is older than `retention_days`, including runs
/// the archiver has already moved to `run_history`. In-progress, queued, and
/// pending runs are never candidates. When `retention_days` is 0 the sweep
/// is disabled and this is a no-op.
pub async fn sweep_once(shared: &Arc<SharedState>) -> RetentionSweepOutcome {
    let retention_days = shared.state.retention_days;
    if retention_days == 0 {
        return RetentionSweepOutcome::default();
    }
    let days = retention_days.min(i64::MAX as u64) as i64;
    let cutoff_us = (chrono::Utc::now() - chrono::Duration::days(days)).timestamp_micros();

    let mut deleted_runs: Vec<RunId> = Vec::new();
    'pass: loop {
        let candidates = match shared
            .state
            .backend
            .expired_terminal_runs(cutoff_us, RETENTION_DELETE_BATCH)
            .await
        {
            Ok(candidates) => candidates,
            Err(error) => {
                warn!(?error, "retention sweep: candidate scan failed");
                break;
            }
        };
        let batch = candidates.len();
        if batch == 0 {
            break;
        }
        let mut progressed = false;
        for run_id in candidates {
            if deleted_runs.len() >= RETENTION_MAX_RUNS_PER_PASS {
                warn!(
                    limit = RETENTION_MAX_RUNS_PER_PASS,
                    "retention sweep: pass bound reached; the backlog continues next hour"
                );
                break 'pass;
            }
            if delete_one(shared, run_id).await {
                deleted_runs.push(run_id);
                progressed = true;
            }
        }
        // A short batch means the backlog is drained. A full batch with no
        // progress would spin on rows this node cannot delete; stop instead.
        if !progressed || batch < RETENTION_DELETE_BATCH {
            break;
        }
    }

    if !deleted_runs.is_empty() {
        // The artifact-v2 registry is a node-local file: the entries for the
        // deleted runs are gone from memory, so re-save it to match.
        if let Err(error) = crate::artifact_twirp::save_artifact_v2_registry(shared).await {
            warn!(
                ?error,
                "retention sweep: failed to re-save artifact v2 registry"
            );
        }
        info!(
            deleted = deleted_runs.len(),
            retention_days, "retention sweep deleted runs past the retention window"
        );
    }
    RetentionSweepOutcome {
        ran: true,
        deleted_runs,
    }
}

/// Delete one expired run: durable rows, node-local caches, on-disk
/// artifacts. `false` when the run could not be deleted (logged) — the next
/// pass retries it.
///
/// The node-local key set is resolved *before* the durable delete: the rows
/// that map the run to its plan/log/agent keys are exactly what goes away,
/// and the in-memory caches are keyed by those plans.
async fn delete_one(shared: &Arc<SharedState>, run_id: RunId) -> bool {
    let keys = match crate::memory_caps::RunNodeKeys::resolve(&shared.state.backend, run_id).await {
        Ok(keys) => keys,
        Err(error) => {
            warn!(?error, %run_id, "retention sweep: failed to resolve the run's node-local keys");
            return false;
        }
    };
    if let Err(error) = shared.state.backend.delete_expired_run(run_id).await {
        warn!(?error, %run_id, "retention sweep: failed to delete the run's durable rows");
        return false;
    }
    // Submission-supplied secrets live exactly as long as the run's rows.
    if let Err(error) = shared.state.secret_provider.delete_run(run_id) {
        warn!(?error, %run_id, "retention sweep: failed to drop the run's secrets");
    }
    let removal = {
        let mut inner = shared.state.inner.lock().await;
        crate::memory_caps::remove_run_everywhere(&mut inner, run_id, &keys)
    };
    for path in &removal.artifact_paths {
        if let Err(error) = tokio::fs::remove_file(path).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                ?error,
                path = %path.display(),
                "retention sweep: failed to delete artifact file"
            );
        }
    }
    for token in &removal.artifact_v2_blob_tokens {
        let dir = shared
            .state
            .state_dir
            .join("blobs")
            .join("artifact")
            .join(token);
        if let Err(error) = tokio::fs::remove_dir_all(&dir).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                ?error,
                path = %dir.display(),
                "retention sweep: failed to delete artifact blob directory"
            );
        }
    }
    true
}

/// Hourly retention sweep. Best-effort housekeeping, never on a request
/// path — the same shape as the checkout-cache pruner.
pub async fn run_retention_sweeper(shared: Arc<SharedState>) {
    let mut interval = tokio::time::interval(RETENTION_SWEEP_INTERVAL);
    // Skip the first tick: serve() already sweeps once at startup.
    interval.tick().await;
    while !shared.shutdown.is_cancelled() {
        tokio::select! {
            _ = interval.tick() => {
                sweep_once(&shared).await;
            }
            _ = shared.shutdown.cancelled() => {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ArtifactRecord;
    use crate::models::ArtifactV2Entry;

    /// Build a server against a temp state dir and a temp config file
    /// carrying `retention_days`.
    async fn test_shared(
        temp: &tempfile::TempDir,
        retention_days: u64,
    ) -> (Arc<SharedState>, tempfile::TempDir) {
        let config_dir = tempfile::tempdir().unwrap();
        let config_path = config_dir.path().join("config.toml");
        std::fs::write(&config_path, format!("retention_days = {retention_days}\n")).unwrap();
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
            .await
            .unwrap();
        (state.shared(), config_dir)
    }

    /// Plant one run row directly: no command submits a run with a
    /// backdated terminal stamp, and retention's own contract is about
    /// timestamps. `archived` moves the row to `run_history` exactly like
    /// the archiver does (which is where a settled run actually lives).
    ///
    /// `completed_days_ago` is `None` for a run whose `completed_at` is
    /// unset — the cutoff then has to fall back to `created_at`.
    async fn seed_run(
        state: &AppState,
        run_id: RunId,
        status: &str,
        created_days_ago: i64,
        completed_days_ago: Option<i64>,
        archived: bool,
    ) {
        let now = chrono::Utc::now();
        let created_us = (now - chrono::Duration::days(created_days_ago)).timestamp_micros();
        let completed_us =
            completed_days_ago.map(|days| (now - chrono::Duration::days(days)).timestamp_micros());
        let conclusion = completed_us.map(|_| {
            if status == "completed" {
                "success"
            } else {
                "failure"
            }
        });
        let key = run_id.0.to_string();
        // Unique per run without threading a counter through the test:
        // `runs_number` is unique on (namespace, repository, workflow_path,
        // run_number, run_attempt), and two seeds can share a timestamp.
        static SEEDED: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1);
        let number = SEEDED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        state
            .test_db_mutate(move |tx| {
                tx.execute(
                    "INSERT INTO namespaces (namespace_id) VALUES ('test') ON CONFLICT DO NOTHING",
                    [],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO runs (run_id, namespace_id, repository, workflow_path, \
                         run_number, run_attempt, event, ref, ref_type, head_sha, workflow_ref, \
                         status, conclusion, origin, created_at, started_at, completed_at) \
                     VALUES (?1, 'test', 'acme/widget', '.github/workflows/ci.yml', \
                         ?2, 1, 'push', 'main', 'branch', 'deadbeef', \
                         '.github/workflows/ci.yml@refs/heads/main', ?3, ?4, 'cli', ?5, ?5, ?6)",
                    rusqlite::params![key, number, status, conclusion, created_us, completed_us],
                )
                .unwrap();
                if archived {
                    tx.execute(
                        "INSERT INTO run_history (run_id, namespace_id, repository, workflow_path, \
                             run_number, run_attempt, event, ref, ref_type, head_sha, conclusion, \
                             submission, created_at, started_at, completed_at) \
                         SELECT run_id, namespace_id, repository, workflow_path, run_number, \
                             run_attempt, event, ref, ref_type, head_sha, conclusion, '{}', \
                             created_at, started_at, completed_at \
                         FROM runs WHERE run_id = ?1",
                        rusqlite::params![key],
                    )
                    .unwrap();
                    tx.execute("DELETE FROM runs WHERE run_id = ?1", rusqlite::params![key])
                        .unwrap();
                }
            })
            .await;
    }

    /// Seed one v1 artifact with a file on disk, one artifact-v2 entry with a
    /// blob directory on disk, and one timeline projection — the node-local
    /// state the sweep must purge and the bytes it must delete.
    async fn seed_run_attachments(shared: &Arc<SharedState>, run_id: RunId) {
        let artifact_path = shared
            .state
            .state_dir
            .join("artifacts")
            .join(format!("{run_id}.bin"));
        tokio::fs::create_dir_all(artifact_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&artifact_path, b"artifact bytes")
            .await
            .unwrap();
        let blob_token = format!("blob-{run_id}");
        let blob_dir = shared
            .state
            .state_dir
            .join("blobs")
            .join("artifact")
            .join(&blob_token);
        tokio::fs::create_dir_all(&blob_dir).await.unwrap();
        tokio::fs::write(blob_dir.join("data"), b"v2 bytes")
            .await
            .unwrap();
        let mut inner = shared.state.inner.lock().await;
        inner.artifacts.insert(
            format!("artifact-{run_id}"),
            ArtifactRecord {
                id: format!("artifact-{run_id}"),
                run_id,
                name: "dist".to_owned(),
                file_name: "dist.zip".to_owned(),
                path: artifact_path.to_string_lossy().into_owned(),
                size: 14,
            },
        );
        let registry_key =
            crate::artifact_twirp::artifact_v2_registry_key(&run_id.to_string(), "dist");
        inner.artifact_v2_registry.insert(
            registry_key.clone(),
            ArtifactV2Entry {
                id: 7,
                workflow_run_backend_id: format!("plan-{run_id}"),
                workflow_job_run_backend_id: "job-1".to_owned(),
                name: "dist".to_owned(),
                size: 8,
                created_at: "2026-09-20T00:00:00Z".to_owned(),
                digest: None,
                blob_token,
            },
        );
        inner.artifact_registry_order.push_back(registry_key);
        inner.timeline_events.insert(
            run_id,
            vec![NdjsonEvent::RunStatus {
                run_id,
                status: ExecutionStatus::Success,
                reason: None,
            }],
        );
    }

    /// Whether the backend still knows the run (live row or history row).
    async fn run_exists(shared: &Arc<SharedState>, run_id: RunId) -> bool {
        shared.state.backend.run_record(run_id).await.is_ok()
    }

    async fn stored_artifacts(state: &AppState, run_id: RunId) -> i64 {
        let key = run_id.0.to_string();
        state
            .test_db_mutate(move |tx| {
                tx.0.query_row(
                    "SELECT count(*) FROM artifacts WHERE run_id = ?1",
                    rusqlite::params![key],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
            })
            .await
    }

    #[tokio::test]
    async fn sweep_deletes_only_terminal_runs_older_than_window() {
        let temp = tempfile::tempdir().unwrap();
        let (shared, _config_dir) = test_shared(&temp, 1).await;
        let state = shared.state.clone();

        // Terminal, past the window, still in the live table.
        let old_terminal = RunId::new();
        seed_run(&state, old_terminal, "completed", 2, Some(2), false).await;
        // Terminal, inside the window: never a candidate.
        let recent_terminal = RunId::new();
        seed_run(&state, recent_terminal, "completed", 0, Some(0), false).await;
        // In progress and created long ago: never a candidate.
        let old_live = RunId::new();
        seed_run(&state, old_live, "in_progress", 30, None, false).await;
        // Terminal with no completed_at falls back to created_at.
        let old_no_completed = RunId::new();
        seed_run(&state, old_no_completed, "completed", 30, None, false).await;
        // Already archived: only the history row is left, and it is expired.
        let old_archived = RunId::new();
        seed_run(&state, old_archived, "completed", 5, Some(5), true).await;

        for run_id in [
            old_terminal,
            recent_terminal,
            old_no_completed,
            old_archived,
        ] {
            seed_run_attachments(&shared, run_id).await;
        }
        // Artifact metadata for the live expired run and the archived one.
        state
            .test_db_mutate(move |tx| {
                for run_id in [old_terminal, old_archived] {
                    tx.execute(
                        "INSERT INTO artifacts (namespace_id, run_id, job_backend_id, name, \
                             storage_key, state) \
                         VALUES ('test', ?1, 'job-1', 'dist', 'key', 'finalized')",
                        rusqlite::params![run_id.0.to_string()],
                    )
                    .unwrap();
                }
            })
            .await;

        let outcome = sweep_once(&shared).await;
        assert!(outcome.ran);
        assert_eq!(outcome.deleted_runs.len(), 3, "{:?}", outcome.deleted_runs);
        for run_id in [old_terminal, old_no_completed, old_archived] {
            assert!(
                outcome.deleted_runs.contains(&run_id),
                "{run_id} must be deleted"
            );
            assert!(!run_exists(&shared, run_id).await, "{run_id} must be gone");
        }
        for run_id in [recent_terminal, old_live] {
            assert!(!outcome.deleted_runs.contains(&run_id));
            assert!(run_exists(&shared, run_id).await, "{run_id} must survive");
        }
        assert_eq!(stored_artifacts(&state, old_terminal).await, 0);
        assert_eq!(stored_artifacts(&state, old_archived).await, 0);

        // Node-local attachments of the deleted runs are gone…
        let inner = shared.state.inner.lock().await;
        for run_id in [old_terminal, old_no_completed, old_archived] {
            assert!(
                !inner.artifacts.values().any(|r| r.run_id == run_id),
                "artifacts of {run_id} must be gone"
            );
            assert!(
                !inner
                    .artifact_v2_registry
                    .keys()
                    .any(|k| k.starts_with(&format!("{run_id}/"))),
                "artifact-v2 entries of {run_id} must be gone"
            );
            assert!(
                !inner.timeline_events.contains_key(&run_id),
                "timeline of {run_id} must be gone"
            );
        }
        assert!(
            inner
                .artifacts
                .values()
                .any(|r| r.run_id == recent_terminal),
            "the recent run's artifacts must survive"
        );
        drop(inner);

        // …and so are the bytes.
        for run_id in [old_terminal, old_archived] {
            assert!(
                !shared
                    .state
                    .state_dir
                    .join("artifacts")
                    .join(format!("{run_id}.bin"))
                    .exists()
            );
            assert!(
                !shared
                    .state
                    .state_dir
                    .join("blobs")
                    .join("artifact")
                    .join(format!("blob-{run_id}"))
                    .exists()
            );
        }
        assert!(
            shared
                .state
                .state_dir
                .join("artifacts")
                .join(format!("{recent_terminal}.bin"))
                .exists()
        );

        // A restart must not resurrect the deleted runs.
        drop(shared);
        let (shared2, _config_dir2) = test_shared(&temp, 1).await;
        for run_id in [old_terminal, old_no_completed, old_archived] {
            assert!(!run_exists(&shared2, run_id).await);
        }
        assert!(run_exists(&shared2, recent_terminal).await);
        assert!(run_exists(&shared2, old_live).await);
    }

    #[tokio::test]
    async fn sweep_is_disabled_when_retention_days_is_zero() {
        let temp = tempfile::tempdir().unwrap();
        let (shared, _config_dir) = test_shared(&temp, 0).await;
        let old = RunId::new();
        seed_run(&shared.state, old, "completed", 365, Some(365), false).await;
        let outcome = sweep_once(&shared).await;
        assert!(!outcome.ran);
        assert!(outcome.deleted_runs.is_empty());
        assert!(run_exists(&shared, old).await);
    }

    #[tokio::test]
    async fn sweep_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let (shared, _config_dir) = test_shared(&temp, 1).await;
        let old = RunId::new();
        seed_run(&shared.state, old, "completed", 5, Some(5), false).await;
        seed_run_attachments(&shared, old).await;

        let first = sweep_once(&shared).await;
        assert_eq!(first.deleted_runs, vec![old]);
        // A second pass finds nothing to delete and fails nothing.
        let second = sweep_once(&shared).await;
        assert!(second.ran);
        assert!(second.deleted_runs.is_empty());
    }

    #[tokio::test]
    async fn server_default_retention_is_90_days() {
        let temp = tempfile::tempdir().unwrap();
        let config_dir = tempfile::tempdir().unwrap();
        let config_path = config_dir.path().join("config.toml");
        std::fs::write(&config_path, "").unwrap();
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
            .await
            .unwrap();
        assert_eq!(state.retention_days, 90);
    }
}
