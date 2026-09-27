//! Run/check/status retention sweep, mirroring GitHub's Actions retention
//! setting (default 90 days).
//!
//! GitHub cleans up checks, workflow runs, and statuses per the Actions
//! retention setting instead of letting them live 400+ days. Preloop had no
//! such lifecycle: runs accumulated indefinitely. This module adds it — a
//! background sweep, wired into server startup, that deletes terminal runs
//! (plus their check-run records, commit statuses, artifacts, and logs)
//! once they are older than `retention_days`, from both the in-memory state
//! and the durable store backend so a restart cannot resurrect them.
//!
//! Only terminal runs are ever deleted: a run that is still queued,
//! pending, or in progress is never a candidate, no matter how old it is.
//! The sweep is idempotent — candidates are selected from current memory
//! and every durable delete is a delete-if-present, so a repeated or
//! interrupted pass converges without double-deleting.

use super::*;
use std::time::Duration;
use tracing::{info, warn};

/// How often the retention sweep runs. Run history is long-lived data; an
/// hourly pass matches the checkout-cache pruner and keeps each pass cheap.
const RETENTION_SWEEP_INTERVAL: Duration = Duration::from_secs(3_600);

/// Outcome of one retention pass, for logs and tests.
#[derive(Debug, Default)]
pub struct RetentionSweepOutcome {
    /// Whether the pass ran at all (`false` when `retention_days` is 0).
    pub ran: bool,
    /// Runs deleted by this pass.
    pub deleted_runs: Vec<RunId>,
}

/// One retention pass: delete terminal runs older than the retention window
/// from memory and from the durable store.
///
/// Selection happens under the state lock: terminal runs whose
/// `completed_at` (falling back to `created_at` when unset) is older than
/// `retention_days`. In-progress, queued, and pending runs are never
/// candidates. When `retention_days` is 0 the sweep is disabled and this is
/// a no-op.
pub async fn sweep_once(shared: &Arc<SharedState>) -> RetentionSweepOutcome {
    let retention_days = shared.state.retention_days;
    if retention_days == 0 {
        return RetentionSweepOutcome::default();
    }
    let days = retention_days.min(i64::MAX as u64) as i64;
    let cutoff = chrono::Utc::now() - chrono::Duration::days(days);
    // Collect candidates and purge them from memory under one lock
    // acquisition; the durable deletes happen after, without the lock.
    let (run_ids, removals, meta) = {
        let mut inner = shared.state.inner.lock().await;
        let candidates: Vec<RunId> = inner
            .runs
            .values()
            .filter(|run| run.status.is_terminal())
            .filter(|run| run.completed_at.unwrap_or(run.created_at) < cutoff)
            .map(|run| run.run_id)
            .collect();
        let mut removals = Vec::with_capacity(candidates.len());
        for run_id in &candidates {
            removals.push(crate::memory_caps::remove_run_everywhere(
                &mut inner, *run_id,
            ));
        }
        let meta = crate::store::build_meta_snapshot(&inner);
        (candidates, removals, meta)
    };
    for (run_id, removal) in run_ids.iter().zip(removals.iter()) {
        if let Err(error) = shared.state.store.delete_run(*run_id).await {
            warn!(?error, %run_id, "retention sweep: failed to delete run from store");
        }
        for key in &removal.log_keys {
            if let Err(error) = shared.state.store.delete_log(key).await {
                warn!(
                    ?error,
                    key, "retention sweep: failed to delete log from store"
                );
            }
        }
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
    }
    if !run_ids.is_empty() {
        // Rewrite the meta snapshot so the sealed blob no longer carries
        // the deleted runs' artifacts, log metadata, or timeline
        // projections, and re-save the artifact-v2 registry file.
        if let Err(error) = shared.state.store.store_meta_only(&meta).await {
            warn!(?error, "retention sweep: failed to persist meta snapshot");
        }
        if let Err(error) = crate::artifact_twirp::save_artifact_v2_registry(shared).await {
            warn!(
                ?error,
                "retention sweep: failed to re-save artifact v2 registry"
            );
        }
        info!(
            deleted = run_ids.len(),
            retention_days, "retention sweep deleted runs past the retention window"
        );
    }
    RetentionSweepOutcome {
        ran: true,
        deleted_runs: run_ids,
    }
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

    fn test_run(status: ExecutionStatus, completed_days_ago: Option<i64>) -> RunRecord {
        let now = chrono::Utc::now();
        RunRecord {
            run_id: RunId::new(),
            webhook_delivery_id: None,
            run_name: None,
            submission: Arc::new(WorkflowSubmission::default()),
            jobs: BTreeMap::from([(JobId("build".to_owned()), status)]),
            status,
            job_outputs: BTreeMap::new(),
            job_base_ids: BTreeMap::new(),
            job_needs: BTreeMap::new(),
            caller_plans: BTreeMap::new(),
            job_names: BTreeMap::new(),
            github: serde_json::Value::Null,
            head_sha: String::new(),
            workflow_ref: String::new(),
            workspace_snapshot: None,
            job_fail_fast: BTreeMap::new(),
            job_continue_on_error: BTreeMap::new(),
            job_check_run_ids: BTreeMap::from([(JobId("build".to_owned()), 42)]),
            reports_check_runs: false,
            reusable_calls: BTreeMap::new(),
            jobs_list: Vec::new(),
            created_at: now,
            started_at: Some(now),
            completed_at: completed_days_ago.map(|days| now - chrono::Duration::days(days)),
            run_number: 1,
            run_attempt: 1,
            workflow_path_str: ".github/workflows/ci.yml".to_owned(),
            event: "push".to_owned(),
            conclusion: None,
            push_state: None,
            snapshot_timing: None,
            fork_approval_pending: false,
            fork_approval_requested_at_unix_nanos: None,
            fork_approved_at_unix_nanos: None,
            fork_approval_note: None,
        }
    }

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

    /// Seed one job request (so the sweep can map plan -> run), one retained
    /// log, one v1 artifact with a file on disk, and one artifact-v2 entry
    /// with a blob directory on disk.
    async fn seed_run_attachments(shared: &Arc<SharedState>, run: &RunRecord, request_id: i64) {
        let run_id = run.run_id;
        let plan_id = format!("plan-{run_id}");
        let log_key = format!("{plan_id}/1");
        {
            let mut inner = shared.state.inner.lock().await;
            inner.job_requests.insert(
                request_id,
                crate::models::TaskAgentJobRequestRecord {
                    request_id,
                    run_id,
                    job_id: JobId("build".to_owned()),
                    agent_job_id: uuid::Uuid::new_v4(),
                    plan_id: plan_id.clone(),
                    plan_type: "plan".to_owned(),
                    timeline_id: uuid::Uuid::new_v4(),
                    result: Some(ExecutionStatus::Success),
                    locked_until: String::new(),
                    claimed_at: None,
                    owner_runner_id: None,
                    started_at: None,
                    last_renewed_at: None,
                    timeout_triggered: false,
                    debug_token_issued: false,
                },
            );
            inner
                .logs
                .insert(log_key.clone(), b"console output".to_vec());
            inner.log_metadata.insert(
                log_key.clone(),
                crate::models::LogMetadata {
                    byte_count: 14,
                    line_count: 1,
                },
            );
            inner.log_order.push_back(log_key.clone());
            inner.log_bytes_total += 14;
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
            let registry_key =
                crate::artifact_twirp::artifact_v2_registry_key(&run_id.to_string(), "dist");
            inner.artifact_v2_registry.insert(
                registry_key.clone(),
                ArtifactV2Entry {
                    id: 7,
                    workflow_run_backend_id: plan_id,
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
        // Persist the run + attachments durably so the test can prove the
        // sweep deletes from the store, not just from memory.
        let projection = {
            let inner = shared.state.inner.lock().await;
            crate::store::RunProjection::from_inner(
                &inner,
                run_id,
                NdjsonEvent::RunStatus {
                    run_id,
                    status: run.status,
                    reason: None,
                },
            )
            .unwrap()
        };
        shared
            .state
            .store
            .store_run_event(projection)
            .await
            .unwrap();
        let meta = {
            let inner = shared.state.inner.lock().await;
            crate::store::build_meta_snapshot(&inner)
        };
        shared.state.store.store_meta_only(&meta).await.unwrap();
        shared
            .state
            .store
            .store_log_chunk(&log_key, 0, b"console output", 14, 1)
            .await
            .unwrap();
        crate::artifact_twirp::save_artifact_v2_registry(shared)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn sweep_deletes_only_terminal_runs_older_than_window() {
        let temp = tempfile::tempdir().unwrap();
        let (shared, _config_dir) = test_shared(&temp, 1).await;

        let old_terminal = test_run(ExecutionStatus::Success, Some(2));
        let recent_terminal = test_run(ExecutionStatus::Failure, Some(0));
        // In-progress run created long ago: never a candidate.
        let mut old_live = test_run(ExecutionStatus::InProgress, None);
        old_live.created_at = chrono::Utc::now() - chrono::Duration::days(30);
        // Terminal run with no completed_at falls back to created_at.
        let mut old_no_completed = test_run(ExecutionStatus::Success, None);
        old_no_completed.created_at = chrono::Utc::now() - chrono::Duration::days(30);

        for (index, run) in [
            &old_terminal,
            &recent_terminal,
            &old_live,
            &old_no_completed,
        ]
        .into_iter()
        .enumerate()
        {
            {
                let mut inner = shared.state.inner.lock().await;
                inner.runs.insert(run.run_id, run.clone());
            }
            seed_run_attachments(&shared, run, index as i64 + 1).await;
        }

        let outcome = sweep_once(&shared).await;
        assert!(outcome.ran);
        assert_eq!(outcome.deleted_runs.len(), 2);
        assert!(outcome.deleted_runs.contains(&old_terminal.run_id));
        assert!(outcome.deleted_runs.contains(&old_no_completed.run_id));

        let inner = shared.state.inner.lock().await;
        assert!(!inner.runs.contains_key(&old_terminal.run_id));
        assert!(!inner.runs.contains_key(&old_no_completed.run_id));
        assert!(inner.runs.contains_key(&recent_terminal.run_id));
        assert!(inner.runs.contains_key(&old_live.run_id));
        // Attachments of the deleted runs are gone from memory…
        for run_id in [old_terminal.run_id, old_no_completed.run_id] {
            let plan_prefix = format!("plan-{run_id}/");
            assert!(
                !inner.logs.keys().any(|k| k.starts_with(&plan_prefix)),
                "logs of {run_id} must be gone"
            );
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
        // …while the recent run's attachments survive.
        let recent_prefix = format!("plan-{}/", recent_terminal.run_id);
        assert!(inner.logs.keys().any(|k| k.starts_with(&recent_prefix)));
        drop(inner);

        // Files on disk are gone for deleted runs, kept for the recent one.
        assert!(
            !shared
                .state
                .state_dir
                .join("artifacts")
                .join(format!("{}.bin", old_terminal.run_id))
                .exists()
        );
        assert!(
            !shared
                .state
                .state_dir
                .join("blobs")
                .join("artifact")
                .join(format!("blob-{}", old_terminal.run_id))
                .exists()
        );
        assert!(
            shared
                .state
                .state_dir
                .join("artifacts")
                .join(format!("{}.bin", recent_terminal.run_id))
                .exists()
        );

        // A restart must not resurrect the deleted runs.
        drop(shared);
        let (shared2, _config_dir2) = test_shared(&temp, 1).await;
        let inner2 = shared2.state.inner.lock().await;
        assert!(!inner2.runs.contains_key(&old_terminal.run_id));
        assert!(!inner2.runs.contains_key(&old_no_completed.run_id));
        assert!(inner2.runs.contains_key(&recent_terminal.run_id));
        assert!(inner2.runs.contains_key(&old_live.run_id));
        let recent_prefix = format!("plan-{}/", recent_terminal.run_id);
        assert!(inner2.logs.keys().any(|k| k.starts_with(&recent_prefix)));
    }

    #[tokio::test]
    async fn sweep_is_disabled_when_retention_days_is_zero() {
        let temp = tempfile::tempdir().unwrap();
        let (shared, _config_dir) = test_shared(&temp, 0).await;
        let old = test_run(ExecutionStatus::Success, Some(365));
        {
            let mut inner = shared.state.inner.lock().await;
            inner.runs.insert(old.run_id, old.clone());
        }
        let outcome = sweep_once(&shared).await;
        assert!(!outcome.ran);
        assert!(outcome.deleted_runs.is_empty());
        let inner = shared.state.inner.lock().await;
        assert!(inner.runs.contains_key(&old.run_id));
    }

    #[tokio::test]
    async fn sweep_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let (shared, _config_dir) = test_shared(&temp, 1).await;
        let old = test_run(ExecutionStatus::Success, Some(5));
        {
            let mut inner = shared.state.inner.lock().await;
            inner.runs.insert(old.run_id, old.clone());
        }
        seed_run_attachments(&shared, &old, 1).await;
        let first = sweep_once(&shared).await;
        assert_eq!(first.deleted_runs, vec![old.run_id]);
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
