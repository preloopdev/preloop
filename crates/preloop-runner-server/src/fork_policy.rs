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
//!   at scheduler admission in `promote_ready_jobs` until the operator
//!   approves the run (`POST /api/v1/runs/:run_id/approve-fork` or
//!   `preloop approve-fork`). The reaper sweep fails unapproved runs closed
//!   after [`FORK_APPROVAL_WINDOW_NANOS`].
//!
//! Deliberate non-knobs: sending secrets and write tokens to fork-PR
//! workflows stays hardcoded off (the `UntrustedForkPullRequest` trust tier
//! never receives them), so there is nothing to configure — and unlike
//! execution protections there is no evaluate mode: the two booleans are
//! the whole policy.

use crate::config::ForkPolicyConfig;
use crate::events::trust_tier::TrustTier;
use crate::runtime_scheduling::{finalize_run_if_complete, summarize_run};
use crate::state::InnerState;
use preloop_gha_protocol::{ExecutionStatus, RunId};

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
/// Held jobs never left `pending_jobs`, so expiry removes them there, marks
/// every non-terminal job `Failure`, clears the hold, and finalizes the run.
/// Jobs that reached other scheduling queues (the submit-time fast path or a
/// workflow-level concurrency hold in `held_runs`) are removed there too,
/// and the run's concurrency holder is released so the group is not wedged
/// by a run that will never start. Returns the failed run ids for logging.
/// A run whose hold stamp is missing is left held (the operator can still
/// approve or cancel it) — a bookkeeping gap must not auto-fail a run.
pub fn sweep_expired_fork_approvals(inner: &mut InnerState, now_unix_nanos: i64) -> Vec<RunId> {
    let expired: Vec<RunId> = inner
        .runs
        .iter()
        .filter(|(_, run)| {
            run.fork_approval_pending
                && !run.status.is_terminal()
                && run
                    .fork_approval_requested_at_unix_nanos
                    .is_some_and(|requested_at| {
                        now_unix_nanos.saturating_sub(requested_at) > FORK_APPROVAL_WINDOW_NANOS
                    })
        })
        .map(|(run_id, _)| *run_id)
        .collect();
    for run_id in &expired {
        // A fork run awaiting approval must never dispatch after expiry:
        // drop it from every scheduling queue, not just pending_jobs, so a
        // later concurrency release cannot resurrect its jobs.
        inner.pending_jobs.retain(|job| job.run_id != *run_id);
        inner.queue.retain(|job| job.run_id != *run_id);
        inner
            .concurrency_blocked
            .retain(|job| job.run_id != *run_id);
        inner.held_runs.remove(run_id);
        crate::runtime_scheduling::release_concurrency_for_run(inner, *run_id);
        if let Some(run) = inner.runs.get_mut(run_id) {
            for status in run.jobs.values_mut() {
                if !status.is_terminal() {
                    *status = ExecutionStatus::Failure;
                }
            }
            run.fork_approval_pending = false;
            run.status = summarize_run(run.jobs.values().copied());
            finalize_run_if_complete(run);
            tracing::warn!(
                run_id = %run_id.0,
                "fork policy: approval window expired, run failed closed"
            );
        }
    }
    expired
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
    use preloop_gha_protocol::{JobId, RunId, WorkflowSubmission};
    use std::collections::BTreeMap;
    use std::sync::Arc;

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

    #[test]
    fn expired_hold_fails_closed() {
        let mut inner = InnerState::default();
        let run = held_run(
            true,
            Some(1_700_000_000_000_000_000),
            ExecutionStatus::Queued,
        );
        let run_id = run.run_id;
        inner.runs.insert(run_id, run);
        inner.pending_jobs.push_back(crate::models::QueuedJob {
            run_id,
            job_id: JobId("build".to_owned()),
            base_id: "build".to_owned(),
            created_at_unix_nanos: 0,
            dependencies_ready_at_unix_nanos: None,
            concurrency_wait_started_at_unix_nanos: None,
            concurrency_acquired_at_unix_nanos: None,
            enqueued_at_unix_nanos: 0,
            needs: Vec::new(),
            if_condition: None,
            condition_context: preloop_gha_expressions::Context::default(),
            max_parallel: None,
            runs_on: vec!["self-hosted".to_owned()],
            runner_group: None,
            message: serde_json::from_value(serde_json::json!({
                "jobId": "00000000-0000-0000-0000-000000000001",
                "requestId": 1,
                "plan": {"planId": "plan", "planType": "build", "version": 1, "artifactUri": "", "artifactLocation": ""},
                "timeline": {"id": "00000000-0000-0000-0000-000000000002", "changeId": 0, "location": null},
                "jobName": "build",
                "lockedUntil": "",
                "resources": {"endpoints": []},
                "variables": {},
                "mask": [],
                "steps": []
            }))
            .unwrap(),
            environment: None,
            concurrency: None,
            matrix: BTreeMap::new(),
            deferred_matrix: None,
            reusable_call: None,
            environment_gate: None,
        });
        let expired = sweep_expired_fork_approvals(
            &mut inner,
            1_700_000_000_000_000_000 + FORK_APPROVAL_WINDOW_NANOS + 1,
        );
        assert_eq!(expired, vec![run_id]);
        let run = &inner.runs[&run_id];
        assert!(!run.fork_approval_pending, "hold must be released");
        assert!(run.status.is_terminal(), "run must be terminal");
        assert_eq!(
            run.jobs[&JobId("build".to_owned())],
            ExecutionStatus::Failure,
            "nonterminal jobs fail closed"
        );
        assert!(
            inner.pending_jobs.is_empty(),
            "held jobs must leave the pending queue"
        );
    }

    #[test]
    fn fresh_hold_survives_sweep() {
        let mut inner = InnerState::default();
        let now = 1_700_000_000_000_000_000;
        let run = held_run(true, Some(now), ExecutionStatus::Queued);
        let run_id = run.run_id;
        inner.runs.insert(run_id, run);
        let expired = sweep_expired_fork_approvals(&mut inner, now + 60_000_000_000);
        assert!(expired.is_empty());
        assert!(inner.runs[&run_id].fork_approval_pending);
    }

    #[test]
    fn approved_and_terminal_runs_untouched() {
        let mut inner = InnerState::default();
        let old = 1_700_000_000_000_000_000;
        // Approved long ago: not pending, must not be failed.
        let mut approved = held_run(false, Some(old), ExecutionStatus::InProgress);
        approved.fork_approved_at_unix_nanos = Some(old + 1);
        let approved_id = approved.run_id;
        inner.runs.insert(approved_id, approved);
        // Terminal run still flagged pending (lost stamp): must not be failed again.
        let terminal = held_run(true, Some(old), ExecutionStatus::Failure);
        let terminal_id = terminal.run_id;
        inner.runs.insert(terminal_id, terminal);
        let expired =
            sweep_expired_fork_approvals(&mut inner, old + FORK_APPROVAL_WINDOW_NANOS + 1);
        assert!(expired.is_empty());
        assert_eq!(inner.runs[&approved_id].status, ExecutionStatus::InProgress);
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

    fn test_queued_job(run_id: RunId, job_id: &str) -> crate::models::QueuedJob {
        crate::models::QueuedJob {
            run_id,
            job_id: JobId(job_id.to_owned()),
            base_id: job_id.to_owned(),
            created_at_unix_nanos: 0,
            dependencies_ready_at_unix_nanos: None,
            concurrency_wait_started_at_unix_nanos: None,
            concurrency_acquired_at_unix_nanos: None,
            enqueued_at_unix_nanos: 0,
            needs: Vec::new(),
            if_condition: None,
            condition_context: preloop_gha_expressions::Context::default(),
            max_parallel: None,
            runs_on: vec!["self-hosted".to_owned()],
            runner_group: None,
            message: serde_json::from_value(serde_json::json!({
                "jobId": "00000000-0000-0000-0000-000000000001",
                "requestId": 1,
                "plan": {"planId": "plan", "planType": "build", "version": 1, "artifactUri": "", "artifactLocation": ""},
                "timeline": {"id": "00000000-0000-0000-0000-000000000002", "changeId": 0, "location": null},
                "jobName": job_id,
                "lockedUntil": "",
                "resources": {"endpoints": []},
                "variables": {},
                "mask": [],
                "steps": []
            }))
            .unwrap(),
            environment: None,
            concurrency: None,
            matrix: BTreeMap::new(),
            deferred_matrix: None,
            reusable_call: None,
            environment_gate: None,
        }
    }

    /// An expired fork run must be scrubbed from every scheduling queue —
    /// not just `pending_jobs` — and its concurrency holder released, so a
    /// later promotion cannot resurrect jobs from a failed-closed run.
    #[test]
    fn expired_sweep_clears_all_queues_and_releases_holder() {
        use crate::concurrency::{ConcurrencyGroup, Holder};
        use std::collections::VecDeque;

        let mut inner = InnerState::default();
        let now = 1_700_000_000_000_000_000;
        let run = held_run(true, Some(now), ExecutionStatus::Queued);
        let run_id = run.run_id;
        inner.runs.insert(run_id, run);

        // Jobs scattered across every scheduling queue the submit and
        // concurrency paths can place them in.
        inner
            .pending_jobs
            .push_back(test_queued_job(run_id, "pending-job"));
        inner.queue.push_back(test_queued_job(run_id, "queued-job"));
        inner
            .concurrency_blocked
            .push_back(test_queued_job(run_id, "blocked-job"));
        inner
            .held_runs
            .insert(run_id, vec![test_queued_job(run_id, "held-job")]);

        // The run holds a workflow-level concurrency slot, with a successor
        // waiting behind it.
        let other_run_id = RunId::new();
        let key = ("owner/repo".to_owned(), "deploy".to_owned());
        inner.holder_keys.insert(run_id, vec![key.clone()]);
        inner.concurrency_groups.insert(
            key.clone(),
            ConcurrencyGroup {
                display_name: "deploy".to_owned(),
                running: Some(Holder::Run(run_id)),
                pending: VecDeque::from([Holder::Run(other_run_id)]),
            },
        );

        let expired =
            sweep_expired_fork_approvals(&mut inner, now + FORK_APPROVAL_WINDOW_NANOS + 1);
        assert_eq!(expired, vec![run_id]);

        assert!(
            inner.pending_jobs.iter().all(|j| j.run_id != run_id),
            "expired run must leave pending_jobs"
        );
        assert!(
            inner.queue.iter().all(|j| j.run_id != run_id),
            "expired run must leave the ready queue"
        );
        assert!(
            inner.concurrency_blocked.iter().all(|j| j.run_id != run_id),
            "expired run must leave concurrency_blocked"
        );
        assert!(
            !inner.held_runs.contains_key(&run_id),
            "expired run must leave held_runs"
        );

        let group = &inner.concurrency_groups[&key];
        assert!(
            !matches!(group.running, Some(Holder::Run(id)) if id == run_id),
            "expired run must release its concurrency holder"
        );
        assert!(
            matches!(group.running, Some(Holder::Run(id)) if id == other_run_id),
            "the waiting holder must be promoted"
        );

        // Even if something re-triggers promotion, the terminal run's jobs
        // are gone from every queue: nothing can dispatch.
        let job_ids: Vec<&str> = ["pending-job", "queued-job", "blocked-job", "held-job"]
            .into_iter()
            .collect();
        for queue in [
            &inner.pending_jobs,
            &inner.queue,
            &inner.concurrency_blocked,
        ] {
            for job in queue.iter() {
                assert!(
                    !job_ids.contains(&job.job_id.0.as_str()),
                    "no expired-run job may remain schedulable"
                );
            }
        }
        assert!(inner.runs[&run_id].status.is_terminal());
    }

    /// A fork run that wins a workflow-concurrency slot while still awaiting
    /// approval must not dispatch: promotion routes its jobs back to
    /// `pending_jobs` (held by the fork gate) instead of the ready queue.
    #[test]
    fn concurrency_promotion_holds_fork_pending_run() {
        use crate::concurrency::{ConcurrencyGroup, Holder};
        use crate::runtime_scheduling::promote_next_from_group;
        use std::collections::VecDeque;

        let mut inner = InnerState::default();
        let run = held_run(
            true,
            Some(1_700_000_000_000_000_000),
            ExecutionStatus::Queued,
        );
        let run_id = run.run_id;
        inner.runs.insert(run_id, run);
        inner
            .held_runs
            .insert(run_id, vec![test_queued_job(run_id, "held-job")]);

        let done_id = RunId::new();
        let key = ("owner/repo".to_owned(), "deploy".to_owned());
        inner.concurrency_groups.insert(
            key.clone(),
            ConcurrencyGroup {
                display_name: "deploy".to_owned(),
                running: Some(Holder::Run(done_id)),
                pending: VecDeque::from([Holder::Run(run_id)]),
            },
        );

        promote_next_from_group(&mut inner, &key, Holder::Run(done_id));

        // The run won the concurrency slot...
        assert!(
            matches!(
                inner.concurrency_groups[&key].running,
                Some(Holder::Run(id)) if id == run_id
            ),
            "promoted run keeps its concurrency slot"
        );
        // ...but its jobs must not dispatch before approval.
        assert!(
            inner.queue.iter().all(|j| j.run_id != run_id),
            "fork-held jobs must not reach the ready queue"
        );
        assert_eq!(
            inner
                .pending_jobs
                .iter()
                .filter(|j| j.run_id == run_id)
                .count(),
            1,
            "fork-held jobs route back to pending_jobs"
        );
        assert_eq!(
            inner.runs[&run_id].jobs[&JobId("held-job".to_owned())],
            ExecutionStatus::Pending,
            "fork-held jobs wait visibly in Pending"
        );
    }

    /// A terminal run promoted by a stale concurrency release must not have
    /// its jobs resurrected: they are dropped instead of re-queued.
    #[test]
    fn concurrency_promotion_drops_terminal_run_jobs() {
        use crate::concurrency::{ConcurrencyGroup, Holder};
        use crate::runtime_scheduling::promote_next_from_group;
        use std::collections::VecDeque;

        let mut inner = InnerState::default();
        let run = held_run(false, None, ExecutionStatus::Failure);
        let run_id = run.run_id;
        inner.runs.insert(run_id, run);
        inner
            .held_runs
            .insert(run_id, vec![test_queued_job(run_id, "held-job")]);

        let done_id = RunId::new();
        let key = ("owner/repo".to_owned(), "deploy".to_owned());
        inner.concurrency_groups.insert(
            key.clone(),
            ConcurrencyGroup {
                display_name: "deploy".to_owned(),
                running: Some(Holder::Run(done_id)),
                pending: VecDeque::from([Holder::Run(run_id)]),
            },
        );

        promote_next_from_group(&mut inner, &key, Holder::Run(done_id));

        assert!(
            inner.queue.iter().all(|j| j.run_id != run_id),
            "terminal run jobs must not reach the ready queue"
        );
        assert!(
            inner.pending_jobs.iter().all(|j| j.run_id != run_id),
            "terminal run jobs must not reach pending_jobs"
        );
        assert!(
            !inner.held_runs.contains_key(&run_id),
            "terminal run jobs must leave held_runs"
        );
    }
}
