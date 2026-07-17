//! Host-side Preloop runner control plane.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

pub mod concurrency;
pub mod events;
pub mod github;
pub mod scheduler;
mod errors;
pub use errors::ApiError;
mod actions;
use actions::*;
mod reusable_workflows;
use reusable_workflows::*;
mod runs;
use runs::*;
mod timeline_logs;
use timeline_logs::*;
mod routes;
pub use routes::{app, app_with_test_api};
use routes::build_app;
mod live_logs;
use live_logs::*;
mod debug_handlers;
use debug_handlers::*;
mod runner_lifecycle;
use runner_lifecycle::*;
mod broker;
use broker::*;
mod auth;
use auth::*;
mod oauth;
use oauth::*;
mod oidc_handlers;
use oidc_handlers::*;
mod results_twirp;
use results_twirp::*;
mod artifact_twirp;
use artifact_twirp::*;
mod compat_ghes;
use compat_ghes::*;
mod cache_artifacts;
use cache_artifacts::*;
mod recording;
use recording::*;
mod state;
pub use state::{AppState, SharedState};
use state::*;
mod models;
use models::*;
mod bootstrap;
pub use bootstrap::{generate_self_signed_cert, serve, SelfSignedCert, ServerConfig, TlsMode};
#[cfg(test)]
use bootstrap::reap_once;
mod blob_store;
use blob_store::*;
mod connection;
use connection::*;

/// Pure job-graph scheduler model and property tests.
pub mod scheduling;

#[cfg(test)]
mod concurrency_http_properties;
#[cfg(test)]
mod concurrency_properties;
/// GitHub-compatible OIDC id-token provider.
pub mod oidc;

use axum_server::{tls_rustls::RustlsConfig, Handle};
use rcgen::generate_simple_self_signed;

use aksh_artifacts::{validate_artifact_name, ArtifactStore};
use aksh_cache::CacheStore;
use aksh_gha_parser::eval::build_context;
use aksh_gha_parser::{expand_jobs_with_reusables, parse_workflow};
use aksh_gha_protocol::{
    azdo,
    crypto::{AgentRsaKeypair, AgentRsaPublicKey, SessionEncryption},
    event_to_ndjson, AnnotationLevel, ExecutionStatus, JobCompletion, JobId, NdjsonEvent,
    RegisteredRunner, RunAccepted, RunId, RunnerRegistrationRequest, RunnerSession,
    RunnerSessionRequest, WorkflowSubmission, PROTOCOL_VERSION,
};
use axum::body::{to_bytes, Body};
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::{Json, Router};
use base64::engine::general_purpose::{STANDARD as BASE64_STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use bytes::Bytes;
use futures::{stream, StreamExt};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex, Notify};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};

/// Default local token used when `AKSH_SYSTEM_TOKEN` is not configured.
const DEFAULT_AKSH_SYSTEM_TOKEN: &str = "aksh-system-token";
#[cfg(test)]
const TEST_LOCAL_JWT_KEY: &[u8] = b"aksh-test-local-jwt-signing-key";

// Re-export from protocol crate — shared wire type with the runner.
use aksh_gha_protocol::LiveLogFeedLinesWrapper;

/// Enqueue a ready job, applying job-level concurrency if present.
/// Returns Ok(true) if pushed to ready queue, Ok(false) if parked, Err if cancelled.
fn try_enqueue_with_job_concurrency(
    inner: &mut InnerState,
    github: &serde_json::Value,
    submission: &WorkflowSubmission,
    queued_job: QueuedJob,
    statuses: &mut BTreeMap<JobId, ExecutionStatus>,
) -> Result<bool, ()> {
    let Some(raw) = queued_job.concurrency.clone() else {
        statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Queued);
        inner.queue.push_back(queued_job);
        return Ok(true);
    };

    let strategy = queued_job
        .message
        .context_data
        .get("strategy")
        .map(azdo::PipelineContextData::to_json)
        .unwrap_or_else(|| json!({}));
    let eval_ctx = concurrency::ConcurrencyContext {
        scope: concurrency::ConcurrencyScope::Job,
        github,
        vars: &submission.vars,
        inputs: &submission.inputs,
        matrix: Some(&queued_job.matrix),
        strategy: Some(&strategy),
        needs: None,
    };
    let eval = concurrency::evaluate_concurrency(&raw, &eval_ctx);
    let (group, cancel, queue) = match eval {
        Ok(v) => v,
        Err(e) => {
            concurrency::log_eval_error("job concurrency", &e);
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Failure);
            return Err(());
        }
    };
    if group.trim().is_empty() {
        statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Failure);
        return Err(());
    }

    let key = concurrency::concurrency_key(&submission.repository, &group);
    let holder = concurrency::Holder::Job {
        run_id: queued_job.run_id,
        job_id: queued_job.job_id.clone(),
    };
    match try_acquire_concurrency(inner, key, group, holder, cancel, queue) {
        Ok(true) => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Queued);
            inner.queue.push_back(queued_job);
            Ok(true)
        }
        Ok(false) => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Pending);
            inner.concurrency_blocked.push_back(queued_job);
            Ok(false)
        }
        Err(e) if e == "concurrency_queue_overflow" => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Cancelled);
            let _ = queued_job;
            Err(())
        }
        Err(_) => {
            statuses.insert(queued_job.job_id.clone(), ExecutionStatus::Failure);
            Err(())
        }
    }
}
/// Resolve the agent job GUID for an in-flight job, if any.
fn agent_job_id_for(inner: &InnerState, run_id: RunId, job_id: &JobId) -> Option<uuid::Uuid> {
    inner
        .job_requests
        .values()
        .find(|r| r.run_id == run_id && r.job_id == *job_id && r.result.is_none())
        .map(|r| r.agent_job_id)
        .or_else(|| {
            // Also check via inflight_requests if result already set but still relevant.
            inner
                .job_requests
                .values()
                .find(|r| r.run_id == run_id && r.job_id == *job_id)
                .map(|r| r.agent_job_id)
        })
}

/// Cancel a run: mark non-terminal jobs Cancelled, enqueue JobCancellation for
/// in-flight jobs, remove from queues/held/blocked, and release concurrency.
/// Returns the number of cancellation messages enqueued.
fn cancel_run_inner(inner: &mut InnerState, run_id: RunId, reason: Option<&str>) -> usize {
    let mut in_progress: Vec<JobId> = Vec::new();
    {
        let Some(record) = inner.runs.get_mut(&run_id) else {
            return 0;
        };
        record.status = ExecutionStatus::Cancelled;
        for (job_id, status) in &mut record.jobs {
            if matches!(*status, ExecutionStatus::InProgress) {
                in_progress.push(job_id.clone());
            }
            if matches!(
                *status,
                ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
            ) {
                *status = ExecutionStatus::Cancelled;
            }
        }
    }

    let mut cancellations = Vec::new();
    for job_id in in_progress {
        if let Some(agent_job_id) = agent_job_id_for(inner, run_id, &job_id) {
            cancellations.push(QueuedCancellation {
                run_id,
                job_id,
                agent_job_id,
            });
        }
    }
    let count = cancellations.len();
    inner.cancellation_queue.extend(cancellations);

    inner.queue.retain(|job| job.run_id != run_id);
    inner.pending_jobs.retain(|job| job.run_id != run_id);
    inner.held_runs.remove(&run_id);
    inner.concurrency_blocked.retain(|job| job.run_id != run_id);
    inner.dap_ports.remove(&run_id);

    // Release any concurrency holders belonging to this run and promote next.
    release_concurrency_for_run(inner, run_id);
    inner.jobset_admissions.retain(|id, _| id.run_id != run_id);

    let _ = reason; // events emitted by caller when needed
    count
}

/// Cancel a single job (job-level concurrency / fail-fast style).
fn cancel_job_inner(inner: &mut InnerState, run_id: RunId, job_id: &JobId) -> usize {
    let was_in_progress = {
        let Some(record) = inner.runs.get_mut(&run_id) else {
            return 0;
        };
        let Some(status) = record.jobs.get_mut(job_id) else {
            return 0;
        };
        let in_progress = matches!(*status, ExecutionStatus::InProgress);
        if matches!(
            *status,
            ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
        ) {
            *status = ExecutionStatus::Cancelled;
        }
        record.status = summarize_run(record.jobs.values().copied());
        in_progress
    };

    let mut count = 0;
    if was_in_progress {
        if let Some(agent_job_id) = agent_job_id_for(inner, run_id, job_id) {
            inner.cancellation_queue.push_back(QueuedCancellation {
                run_id,
                job_id: job_id.clone(),
                agent_job_id,
            });
            count = 1;
        }
    }
    inner
        .queue
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    inner
        .pending_jobs
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    inner
        .concurrency_blocked
        .retain(|j| !(j.run_id == run_id && j.job_id == *job_id));
    if let Some(held) = inner.held_runs.get_mut(&run_id) {
        held.retain(|j| j.job_id != *job_id);
    }

    release_concurrency_for_job(inner, run_id, job_id);
    count
}

fn release_concurrency_for_run(inner: &mut InnerState, run_id: RunId) {
    let keys: Vec<(String, String)> = inner.holder_keys.get(&run_id).cloned().unwrap_or_default();
    for key in keys {
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            let running_match = group
                .running
                .as_ref()
                .is_some_and(|h| h.is_run_holder(run_id) || h.run_id() == run_id);
            if running_match {
                let done = group.running.take();
                if let Some(done) = done {
                    // Only release if all jobs terminal OR this was a cancel of the whole run.
                    promote_next_from_group(inner, &key, done);
                }
            } else {
                // Remove from pending queue.
                if let Some(group) = inner.concurrency_groups.get_mut(&key) {
                    group.pending.retain(|h| h.run_id() != run_id);
                    if group.running.is_none() && group.pending.is_empty() {
                        inner.concurrency_groups.remove(&key);
                    }
                }
            }
        }
    }
    // C-07: discard all key tracking for this run now that every group has been released.
    inner.holder_keys.remove(&run_id);
}

fn release_concurrency_for_job(inner: &mut InnerState, run_id: RunId, job_id: &JobId) {
    let keys: Vec<(String, String)> = inner.concurrency_groups.keys().cloned().collect();
    for key in keys {
        let should_release = {
            let Some(group) = inner.concurrency_groups.get(&key) else {
                continue;
            };
            match &group.running {
                Some(h) if h.contains_job(run_id, job_id) => {
                    // Job holders release immediately; Run/JobSet when all terminal.
                    match h {
                        concurrency::Holder::Job { .. } => true,
                        concurrency::Holder::Run(_) | concurrency::Holder::JobSet { .. } => inner
                            .runs
                            .get(&run_id)
                            .is_some_and(|r| concurrency::holder_is_terminal(h, &r.jobs)),
                    }
                }
                _ => false,
            }
        };
        // Also drop pending entries for this job.
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            group.pending.retain(|h| !h.contains_job(run_id, job_id));
        }
        if should_release {
            if let Some(group) = inner.concurrency_groups.get_mut(&key) {
                if let Some(done) = group.running.take() {
                    promote_next_from_group(inner, &key, done);
                }
            }
        } else if let Some(group) = inner.concurrency_groups.get(&key) {
            if group.running.is_none() && group.pending.is_empty() {
                inner.concurrency_groups.remove(&key);
            }
        }
        // C-07: prune this key from holder_keys when the run has no remaining
        // presence in the group (neither running nor pending).
        let run_still_present = inner.concurrency_groups.get(&key).is_some_and(|g| {
            g.running.as_ref().is_some_and(|h| h.run_id() == run_id)
                || g.pending.iter().any(|h| h.run_id() == run_id)
        });
        if !run_still_present {
            if let Some(rkeys) = inner.holder_keys.get_mut(&run_id) {
                rkeys.retain(|k| k != &key);
                if rkeys.is_empty() {
                    inner.holder_keys.remove(&run_id);
                }
            }
        }
    }
}

/// Release a single concurrency key acquired by a JobSet whose members all
/// became terminal before any could dispatch (e.g. embedded gate overflow).
/// Removes the running holder from the group and promotes the next pending.
fn merge_jobset_gate(gates: &mut Vec<JobSetGate>, mut gate: JobSetGate) {
    if let Some(existing) = gates.iter_mut().find(|existing| existing.key == gate.key) {
        existing.cancel_in_progress |= gate.cancel_in_progress;
        if gate.queue == aksh_gha_parser::ConcurrencyQueue::Single {
            existing.queue = aksh_gha_parser::ConcurrencyQueue::Single;
        }
        return;
    }
    gate.display_name = gate.display_name.trim().to_owned();
    gates.push(gate);
    gates.sort_by(|left, right| left.key.cmp(&right.key));
}

fn release_holder_key(
    inner: &mut InnerState,
    key: &(String, String),
    holder: &concurrency::Holder,
) {
    let mut promote = None;
    if let Some(group) = inner.concurrency_groups.get_mut(key) {
        if group.running.as_ref() == Some(holder) {
            promote = group.running.take();
        } else {
            group.pending.retain(|pending| pending != holder);
        }
    }
    if let Some(done) = promote {
        promote_next_from_group(inner, key, done);
    }
    if inner
        .concurrency_groups
        .get(key)
        .is_some_and(|group| group.running.is_none() && group.pending.is_empty())
    {
        inner.concurrency_groups.remove(key);
    }

    let run_id = holder.run_id();
    let run_still_present = inner.concurrency_groups.get(key).is_some_and(|group| {
        group
            .running
            .as_ref()
            .is_some_and(|candidate| candidate.run_id() == run_id)
            || group
                .pending
                .iter()
                .any(|candidate| candidate.run_id() == run_id)
    });
    if !run_still_present {
        if let Some(keys) = inner.holder_keys.get_mut(&run_id) {
            keys.retain(|candidate| candidate != key);
            if keys.is_empty() {
                inner.holder_keys.remove(&run_id);
            }
        }
    }
}

fn release_jobset_admission(inner: &mut InnerState, id: &JobSetId) {
    let Some(admission) = inner.jobset_admissions.remove(id) else {
        return;
    };
    let holder = id.holder();
    for key in admission.acquired_keys {
        release_holder_key(inner, &key, &holder);
    }
}

fn advance_jobset_admission(
    inner: &mut InnerState,
    id: &JobSetId,
    promoted_key: Option<&(String, String)>,
) -> Result<JobSetAdmissionResult, String> {
    if let Some(key) = promoted_key {
        if let Some(admission) = inner.jobset_admissions.get_mut(id) {
            admission.acquired_keys.insert(key.clone());
        }
    }

    loop {
        let next_gate = {
            let Some(admission) = inner.jobset_admissions.get(id) else {
                return Ok(JobSetAdmissionResult::Ready);
            };
            admission
                .gates
                .iter()
                .find(|gate| !admission.acquired_keys.contains(&gate.key))
                .cloned()
        };
        let Some(gate) = next_gate else {
            inner.jobset_admissions.remove(id);
            return Ok(JobSetAdmissionResult::Ready);
        };

        let holder = id.holder();
        match try_acquire_concurrency(
            inner,
            gate.key.clone(),
            gate.display_name,
            holder,
            gate.cancel_in_progress,
            gate.queue,
        ) {
            Ok(true) => {
                if let Some(admission) = inner.jobset_admissions.get_mut(id) {
                    admission.acquired_keys.insert(gate.key);
                }
            }
            Ok(false) => return Ok(JobSetAdmissionResult::Blocked),
            Err(error) => {
                release_jobset_admission(inner, id);
                return Err(error);
            }
        }
    }
}

/// After a holder finishes, promote the next pending holder for the group.
fn promote_next_from_group(
    inner: &mut InnerState,
    key: &(String, String),
    _done: concurrency::Holder,
) {
    let next = {
        let Some(group) = inner.concurrency_groups.get_mut(key) else {
            return;
        };
        group.pending.pop_front()
    };

    let Some(next) = next else {
        if let Some(group) = inner.concurrency_groups.get(key) {
            if group.running.is_none() && group.pending.is_empty() {
                inner.concurrency_groups.remove(key);
            }
        }
        return;
    };

    // Install as running immediately for Run and JobSet; for Holder::Job, defer
    // until max-parallel is confirmed free so the job cannot contend with its
    // own pending holder (C-01).
    if !matches!(&next, concurrency::Holder::Job { .. }) {
        if let Some(group) = inner.concurrency_groups.get_mut(key) {
            group.running = Some(next.clone());
        }
    }

    match next {
        concurrency::Holder::Run(run_id) => {
            if let Some(jobs) = inner.held_runs.remove(&run_id) {
                for mut job in jobs {
                    if let Some(run) = inner.runs.get_mut(&run_id) {
                        run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                    }
                    // Re-check needs/max_parallel before queueing.
                    let needs_ok = inner.runs.get(&run_id).is_some_and(|run| {
                        job.needs
                            .iter()
                            .all(|n| scheduling::need_satisfied(&run.jobs, n))
                    });
                    if needs_ok && under_max_parallel(inner, &job) {
                        if let Some(run) = inner.runs.get(&run_id) {
                            hydrate_needs_context(&mut job, run);
                        }
                        inner.queue.push_back(job);
                    } else {
                        if let Some(run) = inner.runs.get_mut(&run_id) {
                            // keep Queued status in pending_jobs path
                            run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                        }
                        inner.pending_jobs.push_back(job);
                    }
                }
                if let Some(run) = inner.runs.get_mut(&run_id) {
                    if run.status == ExecutionStatus::Pending {
                        run.status = ExecutionStatus::Queued;
                    }
                }
            }
        }
        concurrency::Holder::Job { run_id, job_id } => {
            let pos = inner
                .concurrency_blocked
                .iter()
                .position(|j| j.run_id == run_id && j.job_id == job_id);
            let Some(pos) = pos else { return };
            // Remove the job temporarily so we can call under_max_parallel
            // without a mutable/immutable borrow conflict on inner.
            let mut job = inner.concurrency_blocked.remove(pos).unwrap();
            if !under_max_parallel(inner, &job) {
                // max-parallel still full: restore the holder at the front of
                // the pending queue and put the job back where it was so the
                // next release event or promote_ready_jobs sweep can retry.
                inner.concurrency_blocked.insert(pos, job);
                if let Some(group) = inner.concurrency_groups.get_mut(key) {
                    group
                        .pending
                        .push_front(concurrency::Holder::Job { run_id, job_id });
                }
                return;
            }
            // Both gates clear: atomically install as running and dispatch.
            if let Some(group) = inner.concurrency_groups.get_mut(key) {
                group.running = Some(concurrency::Holder::Job { run_id, job_id });
            }
            if let Some(run) = inner.runs.get_mut(&run_id) {
                run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                hydrate_needs_context(&mut job, run);
            }
            inner.queue.push_back(job);
        }
        concurrency::Holder::JobSet { run_id, job_ids } => {
            let id = JobSetId {
                run_id,
                job_ids: job_ids.clone(),
            };
            match advance_jobset_admission(inner, &id, Some(key)) {
                Ok(JobSetAdmissionResult::Blocked) => return,
                Err(_) => {
                    cancel_holder(
                        inner,
                        &concurrency::Holder::JobSet { run_id, job_ids },
                        concurrency::cancelled_reason().as_deref(),
                    );
                    return;
                }
                Ok(JobSetAdmissionResult::Ready) => {}
            }

            let mut to_queue = Vec::new();
            inner.concurrency_blocked.retain(|job| {
                if job.run_id == run_id && job_ids.contains(&job.job_id) {
                    to_queue.push(job.clone());
                    false
                } else {
                    true
                }
            });
            for mut job in to_queue {
                if under_max_parallel(inner, &job) {
                    if let Some(run) = inner.runs.get_mut(&run_id) {
                        run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                        hydrate_needs_context(&mut job, run);
                    }
                    inner.queue.push_back(job);
                } else {
                    if let Some(run) = inner.runs.get_mut(&run_id) {
                        run.jobs.insert(job.job_id.clone(), ExecutionStatus::Queued);
                    }
                    inner.pending_jobs.push_back(job);
                }
            }
        }
    }
}

/// Try to acquire a concurrency slot for a holder. Returns:
/// - `Ok(true)` if the holder may proceed (slot acquired / free)
/// - `Ok(false)` if parked as pending
/// - `Err("cancelled")` if the arrival itself was cancelled (queue max overflow)
/// - `Err(msg)` for evaluation / empty-group errors
fn try_acquire_concurrency(
    inner: &mut InnerState,
    key: (String, String),
    display_name: String,
    holder: concurrency::Holder,
    cancel_in_progress: bool,
    queue: aksh_gha_parser::ConcurrencyQueue,
) -> Result<bool, String> {
    let group = inner
        .concurrency_groups
        .entry(key.clone())
        .or_insert_with(|| concurrency::ConcurrencyGroup {
            display_name: display_name.clone(),
            running: None,
            pending: VecDeque::new(),
        });
    if group.display_name.is_empty() {
        group.display_name = display_name;
    }

    if group.running.is_none() {
        group.running = Some(holder.clone());
        let _ = group;
        track_holder_key(inner, &holder, key);
        return Ok(true);
    }

    if cancel_in_progress {
        let prev = group.running.take();
        // Docs: "any existing pending job or workflow in the same concurrency
        // group will be canceled" — drain all pending holders too.
        let stale_pending: Vec<concurrency::Holder> = group.pending.drain(..).collect();
        group.running = Some(holder.clone());
        let _ = group;
        track_holder_key(inner, &holder, key.clone());
        if let Some(prev) = prev {
            cancel_holder(inner, &prev, concurrency::cancelled_reason().as_deref());
        }
        for pending in stale_pending {
            cancel_holder(inner, &pending, concurrency::cancelled_reason().as_deref());
        }
        return Ok(true);
    }
    let _ = group;

    // Contended — apply queue mode for this arrival.
    let join = {
        let group = inner.concurrency_groups.get(&key).unwrap();
        concurrency::apply_queue_mode(queue, &group.pending)
    };

    for pending_holder in join.cancel_pending {
        if pending_holder.run_id() == holder.run_id() {
            continue;
        }
        cancel_holder(
            inner,
            &pending_holder,
            concurrency::cancelled_reason().as_deref(),
        );
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            group.pending.retain(|h| h != &pending_holder);
        }
    }

    if join.cancel_arrival {
        return Err("concurrency_queue_overflow".to_owned());
    }

    if join.park_arrival {
        if let Some(group) = inner.concurrency_groups.get_mut(&key) {
            // After single-mode clears, re-push.
            group.pending.push_back(holder.clone());
        }
        track_holder_key(inner, &holder, key);
        return Ok(false);
    }

    Ok(true)
}

fn track_holder_key(inner: &mut InnerState, holder: &concurrency::Holder, key: (String, String)) {
    let run_id = holder.run_id();
    let keys = inner.holder_keys.entry(run_id).or_default();
    if !keys.contains(&key) {
        keys.push(key);
    }
}

fn cancel_holder(inner: &mut InnerState, holder: &concurrency::Holder, _reason: Option<&str>) {
    match holder {
        concurrency::Holder::Run(run_id) => {
            cancel_run_inner(inner, *run_id, Some("concurrency_cancelled"));
        }
        concurrency::Holder::Job { run_id, job_id } => {
            cancel_job_inner(inner, *run_id, job_id);
        }
        concurrency::Holder::JobSet { run_id, job_ids } => {
            inner.jobset_admissions.remove(&JobSetId {
                run_id: *run_id,
                job_ids: job_ids.clone(),
            });
            for job_id in job_ids {
                cancel_job_inner(inner, *run_id, job_id);
            }
            // If all jobs cancelled, mark run cancelled when appropriate.
            if let Some(run) = inner.runs.get_mut(run_id) {
                if run.jobs.values().all(|s| concurrency::is_terminal(*s)) {
                    run.status = summarize_run(run.jobs.values().copied());
                }
            }
        }
    }
}

async fn next_message(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Option<azdo::TaskAgentMessage>>, ApiError> {
    let session_id = params
        .get("sessionId")
        .cloned()
        .unwrap_or_else(|| "default".to_owned());

    let wait_seconds = params
        .get("waitSeconds")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(50);

    loop {
        let mut inner = shared.state.inner.lock().await;
        if let Some(message) = inner
            .inflight_messages
            .get(&session_id)
            .and_then(|messages| messages.values().next().cloned())
        {
            return Ok(Json(Some(message)));
        }

        if let Some(cancellation) = inner.cancellation_queue.pop_front() {
            let body_json = concurrency::job_cancel_body(cancellation.agent_job_id);
            let message = build_task_agent_message(
                &mut inner,
                &session_id,
                azdo::message_type::JOB_CANCELLED,
                body_json,
            )?;
            return Ok(Json(Some(message)));
        }

        if let Some(request_id) = inner.session_active_requests.get(&session_id).copied() {
            let request_finished = inner
                .job_requests
                .get(&request_id)
                .is_none_or(|request| request.result.is_some());
            if request_finished {
                inner.session_active_requests.remove(&session_id);
            } else {
                drop(inner);
                if wait_seconds == 0 {
                    return Ok(Json(None));
                }
                if tokio::time::timeout(
                    Duration::from_secs(wait_seconds),
                    shared.state.message_notify.notified(),
                )
                .await
                .is_err()
                {
                    return Ok(Json(None));
                }
                continue;
            }
        }

        let runner_labels = inner.runner_labels_for_session(&session_id);
        let Some(queued) = take_matching_job(&mut inner.queue, &runner_labels) else {
            drop(inner);
            if wait_seconds == 0 {
                return Ok(Json(None));
            }
            if tokio::time::timeout(
                Duration::from_secs(wait_seconds),
                shared.state.message_notify.notified(),
            )
            .await
            .is_err()
            {
                return Ok(Json(None));
            }
            continue;
        };

        // Update run status
        if let Some(run) = inner.runs.get_mut(&queued.run_id) {
            run.status = ExecutionStatus::InProgress;
            run.jobs
                .insert(queued.job_id.clone(), ExecutionStatus::InProgress);
        }

        // F030: inject SystemVssConnection so the worker's AzDO reporting context
        // has a server URL, access token, and ResultsServiceUrl — same as broker_acquire_job.
        let mut msg = queued.message.clone();
        for endpoint in &mut msg.resources.endpoints {
            if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
                endpoint.url = Some(runner_server_url());
                endpoint.authorization.parameters.insert(
                    "AccessToken".to_owned(),
                    shared
                        .state
                        .mint_runtime_token(&msg.plan.plan_id, &msg.job_id),
                );
                endpoint
                    .data
                    .insert("ResultsServiceUrl".to_owned(), public_base_url());
                endpoint
                    .data
                    .insert("PipelinesServiceUrl".to_owned(), runner_server_url());
                endpoint
                    .data
                    .insert("CacheServerUrl".to_owned(), public_base_url());
            }
        }
        debug!(
            endpoint_count = msg.resources.endpoints.len(),
            "F030: injected SystemVssConnection into AzDO job message"
        );
        let body_json = serde_json::to_string(&msg)
            .map_err(|e| ApiError::bad_request(format!("failed to serialize job message: {e}")))?;
        let request_id = queued.message.request_id;
        inner
            .session_active_requests
            .insert(session_id.clone(), request_id);
        if let Some(request) = inner.job_requests.get_mut(&request_id) {
            request.started_at = Some(std::time::SystemTime::now());
        }
        let message = build_task_agent_message(
            &mut inner,
            &session_id,
            azdo::message_type::PIPELINE_AGENT_JOB_REQUEST,
            body_json,
        )?;

        let run_id = queued.run_id;
        let job_id = queued.job_id.clone();
        drop(inner);

        github::report_check_run_in_progress(&shared, run_id, &job_id).await;

        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id,
                status: ExecutionStatus::InProgress,
                reason: None,
            })
            .await;

        return Ok(Json(Some(message)));
    }
}

async fn delete_session_message(
    State(shared): State<Arc<SharedState>>,
    Path((session_id, message_id)): Path<(String, i64)>,
) -> StatusCode {
    ack_message(shared, &session_id, message_id).await
}

fn build_task_agent_message(
    inner: &mut InnerState,
    session_id: &str,
    message_type: &str,
    body_json: String,
) -> Result<azdo::TaskAgentMessage, ApiError> {
    let session_key = inner
        .session_keys
        .get(session_id)
        .map(|s| s.key.clone())
        .unwrap_or_default();
    let (encrypted_body, iv) = if !session_key.is_empty() {
        let enc = SessionEncryption::from_key(session_key);
        enc.encrypt(body_json.as_bytes())
            .map_err(|e| ApiError::bad_request(format!("encryption failed: {e}")))?
    } else {
        (body_json.into_bytes(), vec![0u8; 16])
    };

    inner.next_message_id += 1;
    let message_id = inner.next_message_id;
    let message = azdo::TaskAgentMessage {
        message_id,
        message_type: message_type.to_owned(),
        body: BASE64_STANDARD.encode(&encrypted_body),
        iv: Some(BASE64_STANDARD.encode(&iv)),
    };
    inner
        .inflight_messages
        .entry(session_id.to_owned())
        .or_default()
        .insert(message_id, message.clone());
    Ok(message)
}

fn build_broker_plaintext_message(
    inner: &mut InnerState,
    session_id: &str,
    message_type: &str,
    body_json: String,
) -> azdo::TaskAgentMessage {
    inner.next_message_id += 1;
    let message_id = inner.next_message_id;
    let message = azdo::TaskAgentMessage {
        message_id,
        message_type: message_type.to_owned(),
        body: body_json,
        iv: None,
    };
    inner
        .inflight_messages
        .entry(session_id.to_owned())
        .or_default()
        .insert(message_id, message.clone());
    message
}

async fn delete_pool_message(
    State(shared): State<Arc<SharedState>>,
    Path((_pool_id, message_id)): Path<(i64, i64)>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    let session_id = params.get("sessionId").map(String::as_str).unwrap_or("");
    ack_message(shared, session_id, message_id).await
}

async fn ack_message(shared: Arc<SharedState>, session_id: &str, message_id: i64) -> StatusCode {
    let mut inner = shared.state.inner.lock().await;
    if let Some(messages) = inner.inflight_messages.get_mut(session_id) {
        messages.remove(&message_id);
        if messages.is_empty() {
            inner.inflight_messages.remove(session_id);
        }
    }
    StatusCode::NO_CONTENT
}

async fn complete_job(
    State(shared): State<Arc<SharedState>>,
    Json(completion): Json<JobCompletion>,
) -> Result<Json<RunRecord>, ApiError> {
    complete_job_inner(shared, completion).await
}

async fn complete_job_compat(
    State(shared): State<Arc<SharedState>>,
    Path((run_id, job_id)): Path<(RunId, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<RunRecord>, ApiError> {
    let status = match body.get("status").and_then(|value| value.as_str()) {
        Some("success" | "succeeded" | "completed") => ExecutionStatus::Success,
        Some("cancelled" | "canceled") => ExecutionStatus::Cancelled,
        Some("skipped") => ExecutionStatus::Skipped,
        _ => ExecutionStatus::Failure,
    };
    complete_job_inner(
        shared,
        JobCompletion {
            run_id,
            job_id: JobId(job_id),
            status,
            outputs: Default::default(),
        },
    )
    .await
}

/// GET /_apis/v1/AgentRequest/:pool_id/:request_id — query a job request lease/result.
///
/// The official listener calls this when another job arrives while the previous
/// worker process may still be unwinding. Returning a completed `result` lets it
/// safely move on; 404/405 makes it cancel the worker and can poison matrix runs.
async fn agent_request_get(
    State(shared): State<Arc<SharedState>>,
    Path((pool_id, request_id)): Path<(i64, i64)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let request = inner
        .job_requests
        .get(&request_id)
        .ok_or_else(|| ApiError::not_found("agent request not found"))?;
    Ok(Json(agent_request_json(pool_id, request)))
}

/// POST /_apis/v1/AgentRequest/:pool_id/:request_id — best-effort request ack.
async fn agent_request_ack(Path((_pool_id, _request_id)): Path<(i64, i64)>) -> StatusCode {
    StatusCode::OK
}

/// PATCH /_apis/v1/AgentRequest/:pool_id/:request_id — renew or complete job request.
/// The runner sends this to renew the job lock or report completion.
async fn agent_request_patch(
    State(shared): State<Arc<SharedState>>,
    Path((pool_id, request_id)): Path<(i64, i64)>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    info!(?body, "agent_request_patch received");
    // If this is a completion (has result), delegate to complete_job_inner
    // so summarize_run, promote_ready_jobs, and notify_waiters all fire.
    // The result field is only present on the final PATCH; renewals have no result.
    if let Some(result) = body.get("result").and_then(|v| v.as_str()) {
        let new_status = match execution_status_from_runner_result(result) {
            Some(status) => status,
            None => {
                info!(request_id, %result, "unknown agent_request_patch result; skipping completion");
                return Json(
                    json!({ "requestId": request_id, "lockedUntil": "2099-12-31T23:59:59Z" }),
                );
            }
        };
        // Look up (run_id, job_id) under the inner lock, then drop it before calling
        // complete_job_inner which acquires the lock itself.
        let completion = {
            let mut inner = shared.state.inner.lock().await;
            let mut already_completed = false;
            if let Some(request) = inner.job_requests.get_mut(&request_id) {
                already_completed = request.result.is_some();
                request.result = Some(new_status);
                request.locked_until = agent_request_locked_until();
            }
            if already_completed {
                inner.inflight_requests.remove(&request_id);
                info!(
                    request_id,
                    result, "agent request already completed; refreshing result only"
                );
                None
            } else if let Some((run_id, job_id)) = inner.inflight_requests.remove(&request_id) {
                info!(%run_id, %job_id, result, "job completed via agent_request_patch");
                Some(JobCompletion {
                    run_id,
                    job_id,
                    status: new_status,
                    outputs: Default::default(),
                })
            } else {
                info!(
                    request_id,
                    "no inflight job for request_id; ignoring result"
                );
                None
            }
        };
        if let Some(c) = completion {
            let _ = complete_job_inner(shared.clone(), c).await;
        }
        return Json(agent_request_response(&shared, pool_id, request_id).await);
    }
    // Renewal — runner is still working; just extend the lock.
    {
        let mut inner = shared.state.inner.lock().await;
        if let Some(request) = inner.job_requests.get_mut(&request_id) {
            request.locked_until = agent_request_locked_until();
            request.last_renewed_at = Some(std::time::SystemTime::now());
        }
    }
    Json(agent_request_response(&shared, pool_id, request_id).await)
}

async fn agent_request_response(
    shared: &Arc<SharedState>,
    pool_id: i64,
    request_id: i64,
) -> serde_json::Value {
    let inner = shared.state.inner.lock().await;
    inner
        .job_requests
        .get(&request_id)
        .map(|request| agent_request_json(pool_id, request))
        .unwrap_or_else(|| {
            json!({
                "requestId": request_id,
                "poolId": pool_id,
                "lockedUntil": agent_request_locked_until(),
            })
        })
}

fn agent_request_json(pool_id: i64, request: &TaskAgentJobRequestRecord) -> serde_json::Value {
    json!({
        "requestId": request.request_id,
        "poolId": pool_id,
        "jobId": request.agent_job_id,
        "jobName": request.job_id.to_string(),
        "planId": request.plan_id,
        "planType": request.plan_type,
        "lockedUntil": request.locked_until,
        "result": request.result.map(agent_request_result),
    })
}

fn agent_request_result(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Success => "succeeded",
        ExecutionStatus::Failure => "failed",
        ExecutionStatus::Cancelled => "canceled",
        ExecutionStatus::Skipped => "skipped",
        ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress => {
            "pending"
        }
    }
}

fn agent_request_locked_until() -> String {
    "2099-12-31T23:59:59Z".to_owned()
}

fn task_result_status(result: azdo::TaskResult) -> ExecutionStatus {
    match result {
        azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues => {
            ExecutionStatus::Success
        }
        azdo::TaskResult::Failed => ExecutionStatus::Failure,
        azdo::TaskResult::Cancelled => ExecutionStatus::Cancelled,
        azdo::TaskResult::Skipped => ExecutionStatus::Skipped,
    }
}

fn resolve_callback_job(
    inner: &InnerState,
    plan_id: &str,
    timeline_id: Option<uuid::Uuid>,
    agent_job_id: Option<uuid::Uuid>,
) -> Option<(i64, RunId, JobId)> {
    let request_id = inner
        .plan_requests
        .get(plan_id)
        .copied()
        .or_else(|| timeline_id.and_then(|id| inner.timeline_requests.get(&id).copied()))
        .or_else(|| agent_job_id.and_then(|id| inner.agent_job_requests.get(&id).copied()))?;
    let request = inner.job_requests.get(&request_id)?;
    Some((request_id, request.run_id, request.job_id.clone()))
}

fn sole_active_unfinished_request(inner: &InnerState) -> Option<i64> {
    let mut active = inner
        .session_active_requests
        .values()
        .copied()
        .filter(|request_id| {
            inner
                .job_requests
                .get(request_id)
                .is_some_and(|request| request.result.is_none())
        });
    let request_id = active.next()?;
    if active.next().is_none() {
        return Some(request_id);
    }
    None
}
fn job_request_tuple(inner: &InnerState, request_id: i64) -> Option<(i64, RunId, JobId)> {
    let request = inner.job_requests.get(&request_id)?;
    Some((request_id, request.run_id, request.job_id.clone()))
}

async fn complete_job_inner(
    shared: Arc<SharedState>,
    completion: JobCompletion,
) -> Result<Json<RunRecord>, ApiError> {
    if !is_terminal_status(completion.status) {
        return Err(ApiError::bad_request(
            "job completion status must be terminal",
        ));
    }
    let mut inner = shared.state.inner.lock().await;
    {
        let run = inner
            .runs
            .get_mut(&completion.run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        let prior = run
            .jobs
            .get(&completion.job_id)
            .copied()
            .ok_or_else(|| ApiError::bad_request("job does not belong to run"))?;
        if is_terminal_status(prior) && prior != ExecutionStatus::Cancelled {
            return Ok(Json(run.clone()));
        }
        let effective = match (prior, completion.status) {
            (ExecutionStatus::Cancelled, ExecutionStatus::Success)
            | (ExecutionStatus::Cancelled, ExecutionStatus::Failure) => ExecutionStatus::Cancelled,
            _ => completion.status,
        };
        run.jobs.insert(completion.job_id.clone(), effective);
        let job_name = completion.job_id.0.clone();
        if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
            run.jobs_list[pos].conclusion = format!("{:?}", effective).to_lowercase();
        } else {
            run.jobs_list.push(JobDetail {
                name: job_name,
                conclusion: format!("{:?}", effective).to_lowercase(),
                steps: Vec::new(),
            });
        }
        run.job_outputs.insert(
            completion.job_id.clone(),
            completion
                .outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        propagate_reusable_outputs(run);
        run.status = summarize_run(run.jobs.values().copied());
    }
    // Use the status actually stored (may differ from completion if terminal-locked).
    let effective_status = inner
        .runs
        .get(&completion.run_id)
        .and_then(|r| r.jobs.get(&completion.job_id).copied())
        .unwrap_or(completion.status);
    let cancelled_siblings = if effective_status == ExecutionStatus::Failure {
        apply_matrix_fail_fast(&mut inner, completion.run_id, &completion.job_id)
    } else {
        Vec::new()
    };
    // A terminal job must not remain dispatchable, including completion via
    // the native/internal API before a runner acquires it.
    inner
        .queue
        .retain(|job| !(job.run_id == completion.run_id && job.job_id == completion.job_id));
    inner
        .pending_jobs
        .retain(|job| !(job.run_id == completion.run_id && job.job_id == completion.job_id));
    inner
        .concurrency_blocked
        .retain(|job| !(job.run_id == completion.run_id && job.job_id == completion.job_id));
    if let Some(held) = inner.held_runs.get_mut(&completion.run_id) {
        held.retain(|job| job.job_id != completion.job_id);
        if held.is_empty() {
            inner.held_runs.remove(&completion.run_id);
        }
    }
    // Release concurrency for the completed job / run, which may promote held work.
    release_concurrency_for_job(&mut inner, completion.run_id, &completion.job_id);
    let scheduling = promote_ready_jobs(&mut inner);
    let record = inner
        .runs
        .get(&completion.run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    // Mark agent request finished and free the broker session slot so the
    // runner can immediately poll the next job (including concurrency successors).
    let finished_request_ids: Vec<i64> = inner
        .job_requests
        .iter()
        .filter(|(_, r)| r.run_id == completion.run_id && r.job_id == completion.job_id)
        .map(|(id, _)| *id)
        .collect();
    for request_id in &finished_request_ids {
        if let Some(req) = inner.job_requests.get_mut(request_id) {
            if req.result.is_none() {
                req.result = Some(effective_status);
            }
        }
        inner
            .session_active_requests
            .retain(|_, &mut rid| rid != *request_id);
        inner.inflight_requests.remove(request_id);
    }
    // Evict live-log state for this job to prevent unbounded memory growth.
    // The durable step-log blob has already been uploaded by the runner.
    if let Some(agent_key) = inner
        .job_requests
        .values()
        .find(|r| r.run_id == completion.run_id && r.job_id == completion.job_id)
        .map(|r| r.agent_job_id.to_string())
    {
        inner.live_log_lines.remove(&agent_key);
        inner.live_log_tx.remove(&agent_key);
    }
    inner.dap_ports.remove(&completion.run_id);
    let queue_nonempty = !inner.queue.is_empty() || !inner.cancellation_queue.is_empty();
    drop(inner);

    github::report_check_run_completed(
        &shared,
        completion.run_id,
        &completion.job_id,
        effective_status,
    )
    .await;

    if scheduling.promoted > 0 || !cancelled_siblings.is_empty() || queue_nonempty {
        shared.state.message_notify.notify_waiters();
    }

    shared
        .state
        .emit(NdjsonEvent::JobStatus {
            run_id: completion.run_id,
            job_id: completion.job_id,
            status: effective_status,
            reason: None,
        })
        .await;
    for job_id in cancelled_siblings {
        github::report_check_run_completed(
            &shared,
            completion.run_id,
            &job_id,
            ExecutionStatus::Cancelled,
        )
        .await;
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id: completion.run_id,
                job_id,
                status: ExecutionStatus::Cancelled,
                reason: None,
            })
            .await;
    }
    for (run_id, job_id) in scheduling.skipped {
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id,
                status: ExecutionStatus::Skipped,
                reason: None,
            })
            .await;
    }
    for (run_id, job_id) in scheduling.failed {
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id,
                job_id,
                status: ExecutionStatus::Failure,
                reason: None,
            })
            .await;
    }
    shared
        .state
        .emit(NdjsonEvent::RunStatus {
            run_id: completion.run_id,
            status: record.status,
            reason: None,
        })
        .await;
    Ok(Json(record))
}

#[derive(Default)]
struct SchedulingOutcome {
    promoted: usize,
    skipped: Vec<(RunId, JobId)>,
    failed: Vec<(RunId, JobId)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DependencyDecision {
    Wait,
    Run,
    Skip,
    Error,
}

/// Promote or skip pending jobs once every declared dependency is terminal.
fn promote_ready_jobs(inner: &mut InnerState) -> SchedulingOutcome {
    let mut outcome = SchedulingOutcome::default();
    loop {
        let mut promoted_by_base: BTreeMap<(RunId, String), u64> = BTreeMap::new();
        let mut promoted = Vec::new();
        let mut remaining = VecDeque::new();
        let mut settled = false;

        while let Some(mut job) = inner.pending_jobs.pop_front() {
            let decision = inner
                .runs
                .get(&job.run_id)
                .map(|run| dependency_decision(run, &job))
                .unwrap_or(DependencyDecision::Wait);
            match decision {
                DependencyDecision::Run
                    if under_max_parallel(inner, &job)
                        && promoted_by_base
                            .get(&(job.run_id, job.base_id.clone()))
                            .copied()
                            .unwrap_or(0)
                            < job.max_parallel.unwrap_or(u64::MAX) =>
                {
                    if let Some(run) = inner.runs.get(&job.run_id) {
                        hydrate_needs_context(&mut job, run);
                    }
                    *promoted_by_base
                        .entry((job.run_id, job.base_id.clone()))
                        .or_default() += 1;
                    promoted.push(job);
                }
                DependencyDecision::Skip | DependencyDecision::Error => {
                    if let Some(run) = inner.runs.get_mut(&job.run_id) {
                        let status = if decision == DependencyDecision::Skip {
                            ExecutionStatus::Skipped
                        } else {
                            ExecutionStatus::Failure
                        };
                        run.jobs.insert(job.job_id.clone(), status);
                        run.status = summarize_run(run.jobs.values().copied());
                    }
                    if decision == DependencyDecision::Skip {
                        outcome.skipped.push((job.run_id, job.job_id));
                    } else {
                        outcome.failed.push((job.run_id, job.job_id));
                    }
                    settled = true;
                }
                DependencyDecision::Wait | DependencyDecision::Run => remaining.push_back(job),
            }
        }

        outcome.promoted += promoted.len();
        inner.pending_jobs = remaining;
        inner.queue.extend(promoted);
        if !settled {
            return outcome;
        }
    }
}

fn dependency_decision(run: &RunRecord, job: &QueuedJob) -> DependencyDecision {
    if job.needs.is_empty() {
        return DependencyDecision::Run;
    }
    let direct_statuses = job
        .needs
        .iter()
        .flat_map(|need| matching_need_statuses(run, need))
        .collect::<Vec<_>>();
    if direct_statuses.is_empty()
        || direct_statuses
            .iter()
            .any(|status| !is_terminal_status(*status))
    {
        return DependencyDecision::Wait;
    }
    let statuses = ancestor_statuses(run, job);
    let aggregate = aggregate_need_status(&statuses).unwrap_or(ExecutionStatus::Skipped);
    let context = job.condition_context.clone().with_status(
        aggregate == ExecutionStatus::Success,
        aggregate == ExecutionStatus::Failure,
        aggregate == ExecutionStatus::Cancelled,
    );
    let mut context = context;
    context.insert("needs", needs_json_context(run, &job.needs));
    let condition = aksh_gha_expressions::effective_condition(job.if_condition.as_deref());
    match aksh_gha_expressions::eval_bool(&condition, &context) {
        Ok(true) => DependencyDecision::Run,
        Ok(false) => DependencyDecision::Skip,
        Err(_) => DependencyDecision::Error,
    }
}

fn matching_need_ids(run: &RunRecord, need: &JobId) -> Vec<JobId> {
    run.jobs
        .keys()
        .filter(|job_id| {
            *job_id == need
                || run
                    .job_base_ids
                    .get(*job_id)
                    .is_some_and(|base| base == &need.0)
        })
        .cloned()
        .collect()
}

fn matching_need_statuses(run: &RunRecord, need: &JobId) -> Vec<ExecutionStatus> {
    matching_need_ids(run, need)
        .iter()
        .filter_map(|job_id| run.jobs.get(job_id).copied())
        .collect()
}

fn ancestor_statuses(run: &RunRecord, job: &QueuedJob) -> Vec<ExecutionStatus> {
    let mut pending = job
        .needs
        .iter()
        .flat_map(|need| matching_need_ids(run, need))
        .collect::<Vec<_>>();
    let mut visited = std::collections::BTreeSet::new();
    let mut statuses = Vec::new();

    while let Some(job_id) = pending.pop() {
        if !visited.insert(job_id.clone()) {
            continue;
        }
        if let Some(status) = run.jobs.get(&job_id) {
            statuses.push(*status);
        }
        if let Some(needs) = run.job_needs.get(&job_id) {
            pending.extend(needs.iter().flat_map(|need| matching_need_ids(run, need)));
        }
    }
    statuses
}

fn is_terminal_status(status: ExecutionStatus) -> bool {
    matches!(
        status,
        ExecutionStatus::Success
            | ExecutionStatus::Failure
            | ExecutionStatus::Skipped
            | ExecutionStatus::Cancelled
    )
}

/// Check if a job's `runs-on` labels match a runner's registered labels.
///
/// A job matches when every label in the job's `runs-on` is present in the
/// runner's label set (case-insensitive). GitHub-hosted runner labels like
/// `ubuntu-latest` are treated as aliases for common self-hosted labels.
fn job_matches_runner(job_labels: &[String], runner_labels: &[String]) -> bool {
    // Empty runs-on matches any runner (shouldn't happen, but be safe)
    if job_labels.is_empty() {
        return true;
    }
    // Unknown runner (no session→runner mapping) matches any job.
    // This preserves backward compat for tests and legacy session paths.
    if runner_labels.is_empty() {
        return true;
    }
    let runner_set: std::collections::HashSet<String> =
        runner_labels.iter().map(|l| l.to_lowercase()).collect();
    job_labels.iter().all(|required| {
        let req = required.to_lowercase();
        // Direct match
        if runner_set.contains(&req) {
            return true;
        }
        // GitHub-hosted aliases: treat `ubuntu-latest`, `ubuntu-24.04`, etc.
        // as matching a runner with "linux" label; `macos-latest` matches "macos";
        // `windows-latest` matches "windows".
        if req.starts_with("ubuntu") && runner_set.contains("linux") {
            return true;
        }
        if req.starts_with("macos") && runner_set.contains("macos") {
            return true;
        }
        if req.starts_with("windows") && runner_set.contains("windows") {
            return true;
        }
        // Broad fallback: if the runner has "self-hosted" and the job only
        // specifies a GitHub-hosted label (e.g. "ubuntu-latest"), match it.
        // This lets single-runner local setups work without label gymnastics.
        if runner_set.contains("self-hosted")
            && (req.starts_with("ubuntu") || req.starts_with("macos") || req.starts_with("windows"))
        {
            return true;
        }
        false
    })
}

/// Find and remove the first job in the queue that matches the given runner's labels.
/// Returns `None` if no matching job is found.
fn take_matching_job(
    queue: &mut VecDeque<QueuedJob>,
    runner_labels: &[String],
) -> Option<QueuedJob> {
    let pos = queue
        .iter()
        .position(|job| job_matches_runner(&job.runs_on, runner_labels))?;
    queue.remove(pos)
}

fn under_max_parallel(inner: &InnerState, job: &QueuedJob) -> bool {
    let Some(max_parallel) = job.max_parallel else {
        return true;
    };
    let active_in_queue = inner
        .queue
        .iter()
        .filter(|queued| queued.run_id == job.run_id && queued.base_id == job.base_id)
        .count() as u64;
    let active_running = inner
        .runs
        .get(&job.run_id)
        .map(|run| {
            run.jobs
                .iter()
                .filter(|(job_id, status)| {
                    run.job_base_ids.get(*job_id) == Some(&job.base_id)
                        && matches!(status, ExecutionStatus::InProgress)
                })
                .count() as u64
        })
        .unwrap_or(0);

    active_in_queue + active_running < max_parallel
}

fn apply_matrix_fail_fast(inner: &mut InnerState, run_id: RunId, failed_job: &JobId) -> Vec<JobId> {
    let Some(run) = inner.runs.get_mut(&run_id) else {
        return Vec::new();
    };
    let Some(base_id) = run.job_base_ids.get(failed_job).cloned() else {
        return Vec::new();
    };
    if !run.job_fail_fast.get(&base_id).copied().unwrap_or(true) {
        return Vec::new();
    }

    // Track in-progress siblings: they need a JOB_CANCELLED message so the
    // runner aborts the worker. Queued siblings only need their state flipped
    // — they were never dispatched.
    let mut cancelled_jobs = Vec::new();
    let mut cancellations = Vec::new();
    for (job_id, status) in &mut run.jobs {
        if job_id != failed_job
            && run.job_base_ids.get(job_id) == Some(&base_id)
            && matches!(
                status,
                ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
            )
        {
            if matches!(status, ExecutionStatus::InProgress) {
                // Resolve agent_job_id after loop (borrow checker).
                cancellations.push(QueuedCancellation {
                    run_id,
                    job_id: job_id.clone(),
                    agent_job_id: uuid::Uuid::nil(), // filled below
                });
            }
            cancelled_jobs.push(job_id.clone());
            *status = ExecutionStatus::Cancelled;
        }
    }
    run.status = summarize_run(run.jobs.values().copied());
    inner
        .queue
        .retain(|job| !(job.run_id == run_id && job.base_id == base_id));
    inner
        .pending_jobs
        .retain(|job| !(job.run_id == run_id && job.base_id == base_id));
    // Fill real agent_job_ids; drop cancellations for jobs not in flight.
    cancellations.retain_mut(|c| {
        if let Some(id) = agent_job_id_for(inner, c.run_id, &c.job_id) {
            c.agent_job_id = id;
            true
        } else {
            false
        }
    });
    inner.cancellation_queue.extend(cancellations);
    cancelled_jobs
}

fn hydrate_needs_context(job: &mut QueuedJob, run: &RunRecord) {
    let needs = job
        .needs
        .iter()
        .filter_map(|need| need_context(run, need).map(|context| (need.0.clone(), context)))
        .collect();
    job.message
        .context_data
        .insert("needs".to_owned(), azdo::PipelineContextData::Dict(needs));
}
fn needs_json_context(run: &RunRecord, needs: &[JobId]) -> serde_json::Value {
    let values = needs
        .iter()
        .filter_map(|need| {
            let statuses = matching_need_statuses(run, need);
            let result = aggregate_need_status(&statuses)?;
            let matching_ids = matching_need_ids(run, need);
            let mut outputs = serde_json::Map::new();
            for job_id in matching_ids {
                if let Some(job_outputs) = run.job_outputs.get(&job_id) {
                    outputs.extend(job_outputs.clone());
                }
            }
            Some((
                need.0.clone(),
                json!({
                    "result": status_string(result),
                    "outputs": outputs,
                }),
            ))
        })
        .collect();
    serde_json::Value::Object(values)
}

fn aggregate_need_status(statuses: &[ExecutionStatus]) -> Option<ExecutionStatus> {
    if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Failure)
    {
        Some(ExecutionStatus::Failure)
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Cancelled)
    {
        Some(ExecutionStatus::Cancelled)
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Skipped)
    {
        Some(ExecutionStatus::Skipped)
    } else if !statuses.is_empty()
        && statuses
            .iter()
            .all(|status| *status == ExecutionStatus::Success)
    {
        Some(ExecutionStatus::Success)
    } else {
        None
    }
}

fn need_context(run: &RunRecord, need: &JobId) -> Option<azdo::PipelineContextData> {
    let statuses = matching_need_statuses(run, need);
    let result = aggregate_need_status(&statuses)?;
    let mut outputs = BTreeMap::new();
    for job_id in matching_need_ids(run, need) {
        if let Some(job_outputs) = run.job_outputs.get(&job_id) {
            for (key, value) in job_outputs {
                outputs.insert(key.clone(), azdo::PipelineContextData::from_json(value));
            }
        }
    }

    let mut context = BTreeMap::new();
    context.insert(
        "result".to_owned(),
        azdo::PipelineContextData::String(status_string(result)),
    );
    context.insert(
        "outputs".to_owned(),
        azdo::PipelineContextData::Dict(outputs),
    );
    Some(azdo::PipelineContextData::Dict(context))
}

fn status_string(status: ExecutionStatus) -> String {
    match status {
        ExecutionStatus::Queued
        | ExecutionStatus::Pending
        | ExecutionStatus::InProgress
        | ExecutionStatus::Success => "success",
        ExecutionStatus::Failure => "failure",
        ExecutionStatus::Skipped => "skipped",
        ExecutionStatus::Cancelled => "cancelled",
    }
    .to_owned()
}

fn summarize_run(statuses: impl Iterator<Item = ExecutionStatus>) -> ExecutionStatus {
    let statuses = statuses.collect::<Vec<_>>();
    if statuses.iter().any(|status| {
        matches!(
            status,
            ExecutionStatus::Queued | ExecutionStatus::Pending | ExecutionStatus::InProgress
        )
    }) {
        ExecutionStatus::InProgress
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Failure)
    {
        ExecutionStatus::Failure
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Cancelled)
    {
        ExecutionStatus::Cancelled
    } else {
        ExecutionStatus::Success
    }
}

#[cfg(test)]
/// Production-path DAG/workflow properties.
///
/// Oracle sources:
/// - `needs`, skipped dependencies, and job-level conditions:
///   <https://docs.github.com/en/actions/writing-workflows/workflow-syntax-for-github-actions#jobsjob_idneeds>.
/// - status functions: <https://docs.github.com/en/actions/learn-github-actions/expressions#status-check-functions>.
/// - runner v2.335.1: `src/Runner.Worker/StepsRunner.cs` and
///   `src/Runner.Worker/Expressions/{Success,Failure,Cancelled,Always}Function.cs`.
///
/// These tests submit YAML through the real parser/router and use only the
/// explicitly gated internal test API to simulate worker completions. The
/// oracle compares observable job/run state; it does not copy scheduler code.
#[path = "lib_tests.rs"]
mod tests;
