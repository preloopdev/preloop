//! Fork pull-request workflow policy: operator control over whether
//! workflows run for fork pull-request events, and whether those runs need
//! explicit approval first.
//!
//! Mirrors GitHub's "Fork pull request workflows" admin settings
//! (`run_fork_workflows`, `require_approval`). Policy lives in the
//! operator's server config ([`crate::config::ForkPolicyConfig`]) — never in
//! workflow repos — so a fork author cannot weaken the policy that
//! constrains their own workflows.
//!
//! Two enforcement points, matching the two knobs:
//! - `run_fork_workflows = false`: the webhook intake loop in `github.rs`
//!   skips fork-PR effective events before any workflow is matched.
//! - `require_approval = true`: runs created from fork-PR events are stamped
//!   [`RunRecord::fork_approval_pending`](crate::models::RunRecord) and held
//!   at scheduler admission until the operator approves the run
//!   (`POST /api/v1/runs/:run_id/approve-fork` or `preloop approve-fork`).
//!   The hold lives on the run's durable row, so the reaper sweep can fail
//!   unapproved runs closed after [`FORK_APPROVAL_WINDOW_NANOS`] through the
//!   control backend, from any node.
//!
//! Deliberate non-knobs: sending secrets and write tokens to fork-PR
//! workflows stays hardcoded off (the `UntrustedForkPullRequest` trust tier
//! never receives them), so there is nothing to configure — and unlike
//! execution protections there is no evaluate mode: the two booleans are
//! the whole policy.

use crate::ControlBackend;
use crate::config::ForkPolicyConfig;
use crate::events::trust_tier::TrustTier;
use crate::state::SharedState;
use preloop_gha_protocol::RunId;
use std::sync::Arc;

/// How long a fork-PR run may wait for approval before it fails closed.
pub const FORK_APPROVAL_WINDOW_NANOS: i64 = 24 * 60 * 60 * 1_000_000_000;

/// Whether a run with this trust tier must wait for fork-PR approval before
/// any job may start. Only the fork pull-request tier is ever held: trusted
/// runs, `pull_request_target` (base-repo trust), and manual dispatches are
/// unaffected.
pub fn fork_approval_required(policy: &ForkPolicyConfig, tier: Option<TrustTier>) -> bool {
    policy.require_approval && tier == Some(TrustTier::UntrustedForkPullRequest)
}

/// Fail closed every run whose fork-approval window has expired.
///
/// The hold and its stamp are durable run fields, so the sweep is one
/// backend command ([`ControlBackend::expire_fork_approvals`]): every
/// non-terminal job of an expired run lands `Failure`, the hold clears, the
/// run finalizes as a `failure`, its scheduling rows drop and its
/// concurrency holder is released so the group is not wedged by a run that
/// will never start. Held jobs never reached a runner, so nothing has to be
/// interrupted — but the command still clears the queues, so a later
/// promotion cannot resurrect them.
///
/// Returns the failed run ids for the caller's per-job event and check-run
/// fan-out. A run whose hold stamp is missing is left held (the operator can
/// still approve or cancel it) — a bookkeeping gap must not auto-fail a run.
/// A backend failure leaves every run untouched and returns an empty list:
/// this is housekeeping, and the next tick retries.
pub async fn sweep_expired_fork_approvals(
    shared: &Arc<SharedState>,
    now_unix_nanos: i64,
) -> Vec<RunId> {
    let expired_before_unix_nanos = now_unix_nanos.saturating_sub(FORK_APPROVAL_WINDOW_NANOS);
    match shared
        .state
        .backend
        .expire_fork_approvals(expired_before_unix_nanos)
        .await
    {
        Ok(failed) => {
            for run_id in &failed {
                tracing::warn!(
                    run_id = %run_id.0,
                    "fork policy: approval window expired, run failed closed"
                );
            }
            failed
        }
        Err(error) => {
            tracing::warn!(?error, "fork policy: approval sweep failed; will retry");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(run_fork_workflows: bool, require_approval: bool) -> ForkPolicyConfig {
        ForkPolicyConfig {
            run_fork_workflows,
            require_approval,
        }
    }

    #[test]
    fn approval_required_only_for_fork_tier() {
        let p = policy(true, true);
        assert!(fork_approval_required(
            &p,
            Some(TrustTier::UntrustedForkPullRequest)
        ));
        assert!(!fork_approval_required(&p, Some(TrustTier::Trusted)));
        assert!(!fork_approval_required(
            &p,
            Some(TrustTier::PullRequestTarget)
        ));
        assert!(!fork_approval_required(
            &p,
            Some(TrustTier::InternalPullRequest)
        ));
        assert!(!fork_approval_required(&p, None));
    }

    #[test]
    fn approval_not_required_when_disabled() {
        let p = policy(true, false);
        assert!(!fork_approval_required(
            &p,
            Some(TrustTier::UntrustedForkPullRequest)
        ));
    }

    #[test]
    fn config_defaults_preserve_today() {
        let cfg: ForkPolicyConfig = toml::from_str("").expect("empty table parses");
        assert!(cfg.run_fork_workflows);
        assert!(!cfg.require_approval);
        assert_eq!(cfg, ForkPolicyConfig::default());
    }

    #[test]
    fn config_explicit_values() {
        let cfg: ForkPolicyConfig =
            toml::from_str("run_fork_workflows = false\nrequire_approval = true\n")
                .expect("explicit table parses");
        assert!(!cfg.run_fork_workflows);
        assert!(cfg.require_approval);
    }

    #[test]
    fn config_rejects_unknown_fields() {
        let err = toml::from_str::<ForkPolicyConfig>("run_fork_workflowz = false\n")
            .expect_err("typo must fail parsing, not silently weaken policy");
        assert!(err.to_string().contains("run_fork_workflowz"));
    }

    // -- expiry sweep ------------------------------------------------------

    use crate::models::RunRecord;
    use preloop_gha_protocol::{ExecutionStatus, JobId, WorkflowSubmission};
    use std::collections::BTreeMap;

    fn held_run(pending: bool, requested_at: Option<i64>, status: ExecutionStatus) -> RunRecord {
        RunRecord {
            run_id: RunId::new(),
            webhook_delivery_id: None,
            run_name: None,
            submission: Arc::new(WorkflowSubmission::default()),
            jobs: BTreeMap::from([(JobId("build".to_owned()), ExecutionStatus::Queued)]),
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
            job_check_run_ids: BTreeMap::new(),
            reports_check_runs: false,
            reusable_calls: BTreeMap::new(),
            jobs_list: Vec::new(),
            created_at: chrono::Utc::now(),
            started_at: None,
            completed_at: None,
            run_number: 1,
            run_attempt: 1,
            workflow_path_str: ".github/workflows/ci.yml".to_owned(),
            event: "pull_request".to_owned(),
            conclusion: None,
            push_state: None,
            snapshot_timing: None,
            fork_approval_pending: pending,
            fork_approval_requested_at_unix_nanos: requested_at,
            fork_approved_at_unix_nanos: None,
            fork_approval_note: None,
        }
    }

    /// A server against a temp state dir, for the backend-backed sweep tests.
    async fn test_shared(temp: &tempfile::TempDir) -> Arc<SharedState> {
        let config_dir = tempfile::tempdir().unwrap();
        let config_path = config_dir.path().join("config.toml");
        std::fs::write(&config_path, "").unwrap();
        let state = crate::state::AppState::new_with_config(temp.path().to_path_buf(), config_path)
            .await
            .unwrap();
        state.shared()
    }

    /// Plant one fork-held run and its single job row: no command stamps a
    /// held run at an arbitrary past instant, which is exactly what the
    /// window compares (and the `jobs` row is what proves the fail-closed
    /// transition ran).
    async fn seed_fork_run(
        state: &crate::state::AppState,
        run_id: RunId,
        requested_at: Option<i64>,
        status: &str,
        job_queue_state: &str,
        job_status: &str,
    ) {
        let key = run_id.0.to_string();
        let pending = requested_at.is_some();
        // `runs_number` is unique per workflow: two seeds in one test can
        // share a timestamp, so take a monotonic number instead.
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
                         status, origin, fork_approval_pending, fork_approval_requested_at) \
                     VALUES (?1, 'test', 'acme/widget', '.github/workflows/ci.yml', \
                         ?5, 1, 'pull_request', \
                         'refs/pull/1/merge', 'pull_request', 'deadbeef', \
                         '.github/workflows/ci.yml@refs/pull/1/merge', ?2, 'webhook', ?3, ?4)",
                    rusqlite::params![key, status, pending, requested_at, number],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO jobs (run_id, job_id, namespace_id, kind, base_id, status, \
                         queue_state) \
                     VALUES (?1, 'build', 'test', 'job', 'build', ?2, ?3)",
                    rusqlite::params![key, job_status, job_queue_state],
                )
                .unwrap();
            })
            .await;
    }

    /// One observable fact about a run: status, conclusion, hold, and each
    /// job's `(status, queue_state)`.
    async fn run_state(
        state: &crate::state::AppState,
        run_id: RunId,
    ) -> (String, Option<String>, bool, Vec<(String, String)>) {
        let key = run_id.0.to_string();
        state
            .test_db_mutate(move |tx| {
                let (status, conclusion, pending): (String, Option<String>, bool) =
                    tx.0.query_row(
                        "SELECT status, conclusion, fork_approval_pending FROM runs \
                         WHERE run_id = ?1",
                        rusqlite::params![key],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .unwrap();
                let mut stmt =
                    tx.0.prepare("SELECT status, queue_state FROM jobs WHERE run_id = ?1")
                        .unwrap();
                let jobs = stmt
                    .query_map(rusqlite::params![key], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                (status, conclusion, pending, jobs)
            })
            .await
    }

    #[tokio::test]
    async fn expired_hold_fails_closed() {
        let temp = tempfile::tempdir().unwrap();
        let shared = test_shared(&temp).await;
        let now = crate::models::now_unix_nanos();
        let run_id = RunId::new();
        seed_fork_run(
            &shared.state,
            run_id,
            Some(now - FORK_APPROVAL_WINDOW_NANOS - 1),
            "queued",
            "held",
            "pending",
        )
        .await;

        let expired = sweep_expired_fork_approvals(&shared, now).await;
        assert_eq!(expired, vec![run_id]);

        let (status, conclusion, pending, jobs) = run_state(&shared.state, run_id).await;
        assert_eq!(status, "completed", "run must be terminal");
        assert_eq!(conclusion.as_deref(), Some("failure"));
        assert!(!pending, "hold must be released");
        assert_eq!(
            jobs,
            vec![("failure".to_owned(), "none".to_owned())],
            "nonterminal jobs fail closed and leave every queue"
        );
    }

    #[tokio::test]
    async fn fresh_hold_survives_sweep() {
        let temp = tempfile::tempdir().unwrap();
        let shared = test_shared(&temp).await;
        let now = crate::models::now_unix_nanos();
        let run_id = RunId::new();
        seed_fork_run(
            &shared.state,
            run_id,
            Some(now),
            "queued",
            "held",
            "pending",
        )
        .await;

        let expired = sweep_expired_fork_approvals(&shared, now + 60_000_000_000).await;
        assert!(expired.is_empty());
        let (status, _, pending, _) = run_state(&shared.state, run_id).await;
        assert!(pending, "a hold inside the window must survive");
        assert_eq!(status, "queued");
    }

    #[tokio::test]
    async fn approved_and_terminal_runs_untouched() {
        let temp = tempfile::tempdir().unwrap();
        let shared = test_shared(&temp).await;
        let now = crate::models::now_unix_nanos();
        let old = now - FORK_APPROVAL_WINDOW_NANOS - 1;

        // Approved long ago: the hold is cleared, the run must not be failed.
        let approved = RunId::new();
        seed_fork_run(
            &shared.state,
            approved,
            None,
            "in_progress",
            "ready",
            "queued",
        )
        .await;
        // Terminal run still flagged pending (lost clear): must not be
        // re-failed by the sweep.
        let terminal = RunId::new();
        seed_fork_run(
            &shared.state,
            terminal,
            Some(old),
            "completed",
            "none",
            "failure",
        )
        .await;

        let expired = sweep_expired_fork_approvals(&shared, now).await;
        assert!(expired.is_empty(), "{expired:?}");
        let (status, _, pending, _) = run_state(&shared.state, approved).await;
        assert_eq!(status, "in_progress");
        assert!(!pending);
        let (status, _, _, jobs) = run_state(&shared.state, terminal).await;
        assert_eq!(status, "completed");
        assert_eq!(jobs, vec![("failure".to_owned(), "none".to_owned())]);
    }

    #[test]
    fn hold_round_trips_through_serde() {
        let run = held_run(
            true,
            Some(1_700_000_000_000_000_000),
            ExecutionStatus::Queued,
        );
        let value = serde_json::to_value(&run).expect("serialize");
        let restored: RunRecord = serde_json::from_value(value).expect("deserialize");
        assert!(restored.fork_approval_pending);
        assert_eq!(
            restored.fork_approval_requested_at_unix_nanos,
            Some(1_700_000_000_000_000_000)
        );
        // Blobs written before this field existed restore as unheld.
        let mut legacy = serde_json::to_value(&run).expect("serialize");
        legacy
            .as_object_mut()
            .unwrap()
            .remove("fork_approval_pending");
        legacy
            .as_object_mut()
            .unwrap()
            .remove("fork_approval_requested_at_unix_nanos");
        let restored: RunRecord = serde_json::from_value(legacy).expect("deserialize legacy");
        assert!(!restored.fork_approval_pending);
        assert!(restored.fork_approval_requested_at_unix_nanos.is_none());
    }

    /// An expired fork run must leave every scheduling queue and release its
    /// concurrency holder, so a later promotion cannot resurrect its jobs —
    /// and the waiter behind it inherits the group instead of staying wedged.
    #[tokio::test]
    async fn expired_sweep_releases_the_holder_and_promotes_the_waiter() {
        let temp = tempfile::tempdir().unwrap();
        let shared = test_shared(&temp).await;
        let now = crate::models::now_unix_nanos();
        let expired_run = RunId::new();
        // Jobs scattered across the queue states submit and concurrency can
        // leave them in.
        seed_fork_run(
            &shared.state,
            expired_run,
            Some(now - FORK_APPROVAL_WINDOW_NANOS - 1),
            "queued",
            "held",
            "pending",
        )
        .await;
        let blocked_job = expired_run.0.to_string();
        shared
            .state
            .test_db_mutate(move |tx| {
                tx.execute(
                    "INSERT INTO jobs (run_id, job_id, namespace_id, kind, base_id, status, \
                         queue_state) \
                     VALUES (?1, 'second', 'test', 'job', 'second', 'queued', 'ready')",
                    rusqlite::params![blocked_job],
                )
                .unwrap();
            })
            .await;

        // The expired run holds `deploy`; an ordinary run waits behind it.
        let waiter = RunId::new();
        seed_fork_run(&shared.state, waiter, None, "queued", "held", "pending").await;
        let holder = expired_run.0.to_string();
        let waiting = waiter.0.to_string();
        shared
            .state
            .test_db_mutate(move |tx| {
                tx.execute(
                    "INSERT INTO concurrency_holds (namespace_id, repository, group_name, \
                         display_name, holder_kind, holder_run_id) \
                     VALUES ('test', 'acme/widget', 'deploy', 'deploy', 'run', ?1)",
                    rusqlite::params![holder],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO concurrency_waits (namespace_id, repository, group_name, \
                         holder_kind, holder_run_id) \
                     VALUES ('test', 'acme/widget', 'deploy', 'run', ?1)",
                    rusqlite::params![waiting],
                )
                .unwrap();
            })
            .await;

        let expired = sweep_expired_fork_approvals(&shared, now).await;
        assert_eq!(expired, vec![expired_run]);

        let (status, conclusion, pending, jobs) = run_state(&shared.state, expired_run).await;
        assert_eq!(status, "completed");
        assert_eq!(conclusion.as_deref(), Some("failure"));
        assert!(!pending);
        assert!(
            jobs.iter()
                .all(|(status, queue)| status == "failure" && queue == "none"),
            "every job of the failed run must be terminal and unschedulable: {jobs:?}"
        );

        let expired_key = expired_run.0.to_string();
        let waiter_key = waiter.0.to_string();
        let (holds, waits) = shared
            .state
            .test_db_mutate(move |tx| {
                let holds: Vec<String> =
                    tx.0.prepare(
                        "SELECT holder_run_id FROM concurrency_holds WHERE group_name = 'deploy'",
                    )
                    .unwrap()
                    .query_map([], |row| row.get::<_, String>(0))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                let waits: Vec<String> =
                    tx.0.prepare(
                        "SELECT holder_run_id FROM concurrency_waits WHERE group_name = 'deploy'",
                    )
                    .unwrap()
                    .query_map([], |row| row.get::<_, String>(0))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                (holds, waits)
            })
            .await;
        assert!(
            !holds.contains(&expired_key),
            "the failed run must not keep the concurrency slot: {holds:?}"
        );
        assert!(
            !waits.contains(&expired_key),
            "the failed run must not keep a wait row: {waits:?}"
        );
        assert_eq!(
            holds,
            vec![waiter_key],
            "the waiter behind the dead run inherits the slot"
        );
        assert!(waits.is_empty(), "the waiter left the queue: {waits:?}");
    }
}
