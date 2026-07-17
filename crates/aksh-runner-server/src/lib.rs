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
mod routes;
pub use routes::{app, app_with_test_api};
use routes::build_app;
mod live_logs;
use live_logs::*;
mod debug_handlers;
use debug_handlers::*;
mod runner_lifecycle;
use runner_lifecycle::*;
mod auth;
use auth::*;
mod oauth;
use oauth::*;
mod oidc_handlers;
use oidc_handlers::*;
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

async fn healthz(State(shared): State<Arc<SharedState>>) -> Json<serde_json::Value> {
    Json(json!({
        "ok": true,
        "protocol_version": PROTOCOL_VERSION,
        "shutdown_requested": shared.shutdown.is_cancelled(),
    }))
}

pub(crate) async fn submit_run_inner(
    shared: &Arc<SharedState>,
    mut submission: WorkflowSubmission,
) -> Result<RunAccepted, ApiError> {
    let workflow = parse_workflow(&submission.workflow_yaml)?;
    if submission.event == "workflow_dispatch" {
        workflow.apply_workflow_dispatch_inputs(&mut submission.payload)?;
        if submission.dispatch_inputs.is_empty() {
            submission.dispatch_inputs = submission
                .payload
                .get("inputs")
                .and_then(serde_json::Value::as_object)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
        }
        if submission.dispatch_inputs_stringified.is_empty() {
            submission.dispatch_inputs_stringified = submission
                .dispatch_inputs
                .iter()
                .map(|(name, value)| (name.clone(), value_to_input_string(value)))
                .collect();
        }
        if let Some(object) = submission.payload.as_object_mut() {
            object.insert(
                "inputs".to_owned(),
                serde_json::to_value(&submission.dispatch_inputs_stringified).unwrap_or_default(),
            );
        }
    }
    if let Some(tier) = submission.trust_tier.as_deref().and_then(|value| {
        serde_json::from_value::<crate::events::trust_tier::TrustTier>(json!(value)).ok()
    }) {
        if !tier.allows_secrets() {
            submission.secrets.clear();
        }
    }
    let (branch, tag) = {
        let (default_branch, default_tag) = git_ref_context(&submission.git_ref);
        let filter_branch = submission.filter_branch.clone().or_else(|| {
            if matches!(
                submission.event.as_str(),
                "pull_request" | "pull_request_target"
            ) {
                submission
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("base"))
                    .and_then(|base| base.get("ref"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            } else if submission.event == "workflow_run" {
                submission
                    .payload
                    .get("workflow_run")
                    .and_then(|run| run.get("head_branch"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            } else {
                None
            }
        });
        if filter_branch.is_some() {
            (filter_branch, None)
        } else {
            (default_branch, default_tag)
        }
    };
    let payload_has_paths =
        submission.payload.get("paths").is_some() || submission.payload.get("commits").is_some();
    let changed_paths_known = submission.changed_paths_known || payload_has_paths;
    let changed_paths = if submission.changed_paths_known {
        submission.changed_paths.clone()
    } else {
        changed_paths_from_payload(&submission.payload)
    };
    if !changed_paths_known && workflow.on.has_path_filters(&submission.event) {
        return Err(ApiError::bad_request(
            "workflow path filters require a complete changed-file list".to_owned(),
        ));
    }
    // Activity type from explicit field (set by dispatcher) or payload.action fallback.
    let activity_owned: Option<String> = submission.activity_type.clone().or_else(|| {
        submission
            .payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
    });
    let activity_type = activity_owned.as_deref();
    if !workflow.on.matches_with_context(
        &submission.event,
        branch.as_deref(),
        tag.as_deref(),
        &changed_paths,
        activity_type,
        &submission.workflow_run_upstream_names,
    ) {
        return Err(ApiError::bad_request(format!(
            "workflow does not match event `{}`",
            submission.event
        )));
    }
    let expanded = expand_jobs_with_reusables(&workflow, &submission.reusable_workflows)?;
    let mut jobs = expanded.jobs;
    if !submission.dispatch_inputs.is_empty() {
        for job in &mut jobs {
            job.inputs = submission.dispatch_inputs.clone();
        }
    }
    let reusable_calls = expanded.reusable_calls;
    let run_id = RunId::new();
    let repository_owner = submission
        .repository
        .split('/')
        .next()
        .unwrap_or("owner")
        .to_string();
    let sha = submission
        .resolved_sha
        .clone()
        .or_else(|| {
            submission
                .payload
                .get("after")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| {
            if submission.git_ref.len() == 40
                && submission
                    .git_ref
                    .chars()
                    .all(|character| character.is_ascii_hexdigit())
            {
                submission.git_ref.clone()
            } else {
                "0000000000000000000000000000000000000000".to_owned()
            }
        })
        .to_string();
    let workflow_path = submission
        .workflow_path
        .clone()
        .unwrap_or_else(|| ".github/workflows/workflow.yml".to_owned());
    let workflow_ref = format!(
        "{}/{}@{}",
        submission.repository, workflow_path, submission.git_ref
    );

    let ref_name = submission
        .git_ref
        .strip_prefix("refs/heads/")
        .or_else(|| submission.git_ref.strip_prefix("refs/tags/"))
        .unwrap_or(&submission.git_ref)
        .to_owned();
    let ref_type = if submission.git_ref.starts_with("refs/tags/") {
        "tag"
    } else {
        "branch"
    };

    let github = json!({
        "ref": submission.git_ref,
        "sha": sha,
        "repository": submission.repository,
        "repository_owner": repository_owner,
        "repository_owner_id": "0",
        "repositoryUrl": format!("git://github.com/{}.git", submission.repository),
        "run_id": run_id.to_string(),
        "run_number": "1",
        "retention_days": "90",
        "run_attempt": "1",
        "artifact_cache_size_limit": "10",
        "repository_visibility": "private",
        "actor_id": "0",
        "actor": "aksh-system",
        "workflow": workflow.name.clone().unwrap_or_default(),
        "head_ref": "",
        "base_ref": "",
        "event_name": submission.event,
        "server_url": "https://github.com",
        "api_url": "https://api.github.com",
        "graphql_url": "https://api.github.com/graphql",
        "ref_name": ref_name,
        "ref_protected": false,
        "ref_type": ref_type,
        "secret_source": "Actions",
        "event": submission.payload,
        "workflow_ref": workflow_ref,
        "workflow_sha": sha,
        "repository_id": "0",
        "triggering_actor": "aksh-system"
    });

    // Evaluate workflow-level concurrency before locking (pure).
    let workflow_concurrency = workflow.concurrency.clone();
    let mut empty_workflow_concurrency_group = false;
    let workflow_concurrency_eval = if let Some(raw) = &workflow_concurrency {
        let eval_ctx = concurrency::ConcurrencyContext {
            scope: concurrency::ConcurrencyScope::Workflow,
            github: &github,
            vars: &submission.vars,
            inputs: &submission.inputs,
            matrix: None,
            strategy: None,
            needs: None,
        };
        let (group, cancel, queue) =
            concurrency::evaluate_concurrency(raw, &eval_ctx).map_err(|error| {
                ApiError::bad_request(format!("concurrency evaluation failed: {error}"))
            })?;
        if group.trim().is_empty() {
            empty_workflow_concurrency_group = true;
            None
        } else {
            Some((group, cancel, queue, raw.clone()))
        }
    } else {
        None
    };

    {
        let mut inner = shared.state.inner.lock().await;
        let mut statuses = BTreeMap::new();
        let mut ready_jobs = 0usize;
        let mut job_base_ids = BTreeMap::new();
        let mut job_needs = BTreeMap::new();
        let mut job_fail_fast = BTreeMap::new();
        let mut ready_by_base: BTreeMap<String, u64> = BTreeMap::new();
        let mut initially_skipped = Vec::new();
        let mut built_jobs: Vec<QueuedJob> = Vec::new();
        if empty_workflow_concurrency_group {
            let queued_jobs = 0;
            inner.runs.insert(
                run_id,
                RunRecord {
                    run_id,
                    submission,
                    jobs: BTreeMap::new(),
                    job_outputs: BTreeMap::new(),
                    job_base_ids: BTreeMap::new(),
                    job_needs: BTreeMap::new(),
                    job_fail_fast: BTreeMap::new(),
                    status: ExecutionStatus::Failure,
                    job_check_run_ids: BTreeMap::new(),
                    reusable_calls,
                    jobs_list: Vec::new(),
                },
            );
            drop(inner);
            shared
                .state
                .emit(NdjsonEvent::RunAccepted {
                    run_id,
                    queued_jobs,
                })
                .await;
            shared
                .state
                .emit(NdjsonEvent::RunStatus {
                    run_id,
                    status: ExecutionStatus::Failure,
                    reason: Some("concurrency group name must not be empty".to_owned()),
                })
                .await;
            return Ok(RunAccepted {
                run_id,
                queued_jobs,
            });
        }
        for job in jobs {
            job_base_ids.insert(job.id.clone(), job.base_id.clone());
            job_needs.insert(job.id.clone(), job.needs.clone());
            job_fail_fast.insert(job.base_id.clone(), job.fail_fast);
            statuses.insert(job.id.clone(), ExecutionStatus::Queued);
            let condition_context = build_context(
                &github,
                &BTreeMap::new(),
                &submission.vars,
                &indexmap::IndexMap::new(),
                &serde_json::json!({}),
                &BTreeMap::new(),
                &job.inputs,
            );
            if job.needs.is_empty() {
                let condition =
                    aksh_gha_expressions::effective_condition(job.if_condition.as_deref());
                let should_run = aksh_gha_expressions::eval_bool(&condition, &condition_context)
                    .map_err(|error| {
                        ApiError::bad_request(format!(
                            "failed to evaluate condition for job `{}`: {error}",
                            job.id
                        ))
                    })?;
                if !should_run {
                    statuses.insert(job.id.clone(), ExecutionStatus::Skipped);
                    initially_skipped.push((run_id, job.id.clone()));
                    continue;
                }
            }
            let mut agent_msg = aksh_gha_parser::job_builder::build_agent_job_message(
                &job,
                &github,
                &job.env,
                &submission
                    .secrets
                    .iter()
                    .map(|(k, v)| (k.clone(), v.expose().to_owned()))
                    .collect(),
                &submission.vars,
            )
            .map_err(|e| ApiError::bad_request(format!("failed to build job message: {e}")))?;

            let id_token_granted = job.oidc_id_token_granted;
            inner
                .id_token_grants
                .insert((run_id, job.id.clone()), id_token_granted);
            inner.oidc_job_contexts.insert(
                (run_id, job.id.clone()),
                OidcJobContext {
                    environment: job.oidc_environment.clone(),
                    job_workflow_ref: job.oidc_job_workflow_ref.clone(),
                },
            );
            inner.next_request_id += 1;
            let request_id = inner.next_request_id;
            agent_msg.request_id = request_id;
            if id_token_granted {
                let oidc_url = format!(
                    "{}/runner/server/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken?api-version=2.0",
                    public_base_url(),
                    agent_msg.plan.plan_id,
                    agent_msg.job_id,
                );
                for endpoint in &mut agent_msg.resources.endpoints {
                    if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
                        endpoint
                            .data
                            .insert("GenerateIdTokenUrl".to_owned(), oidc_url.clone());
                    }
                }
            }
            // Mint a dynamic JWT for the job and inject it as GITHUB_TOKEN.
            let token = shared
                .state
                .mint_runtime_token(&agent_msg.plan.plan_id, &agent_msg.job_id);
            agent_msg.variables.insert(
                "system.github.token".to_owned(),
                aksh_gha_protocol::azdo::VariableValue::secret(token.clone()),
            );
            agent_msg.variables.insert(
                "github_token".to_owned(),
                aksh_gha_protocol::azdo::VariableValue::secret(token.clone()),
            );
            agent_msg.variables.insert(
                "system.github.launch_endpoint".to_owned(),
                aksh_gha_protocol::azdo::VariableValue::new(public_base_url()),
            );
            agent_msg.variables.insert(
                "system.github.results_endpoint".to_owned(),
                aksh_gha_protocol::azdo::VariableValue::new(public_base_url()),
            );
            agent_msg.variables.insert(
                "system.orchestrationId".to_owned(),
                aksh_gha_protocol::azdo::VariableValue::new(format!(
                    "{}.{}.{}",
                    agent_msg.plan.plan_id, job.base_id, agent_msg.job_name
                )),
            );
            if let Some(aksh_gha_protocol::azdo::PipelineContextData::Dict(github_dict)) =
                &mut agent_msg.context_data.get_mut("github")
            {
                github_dict.insert(
                    "token".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::String(token),
                );
                let mut perms = std::collections::BTreeMap::new();
                for perm in &[
                    "actions",
                    "contents",
                    "issues",
                    "metadata",
                    "pull-requests",
                    "statuses",
                ] {
                    perms.insert(
                        perm.to_string(),
                        aksh_gha_protocol::azdo::PipelineContextData::String("write".to_string()),
                    );
                }
                github_dict.insert(
                    "token_permissions".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::Dict(perms),
                );
            }

            agent_msg.file_table = vec![workflow_path.clone()];
            if let Some(aksh_gha_protocol::azdo::PipelineContextData::Dict(job_dict)) =
                agent_msg.context_data.get_mut("job")
            {
                job_dict.insert(
                    "check_run_id".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::Number(0.0),
                );
                job_dict.insert(
                    "workflow_ref".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::String(workflow_ref.clone()),
                );
                job_dict.insert(
                    "workflow_sha".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::String(sha.clone()),
                );
                job_dict.insert(
                    "workflow_repository".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::String(
                        submission.repository.clone(),
                    ),
                );
                job_dict.insert(
                    "workflow_file_path".to_owned(),
                    aksh_gha_protocol::azdo::PipelineContextData::String(workflow_path.clone()),
                );
            }

            agent_msg.enable_debugger = submission.enable_debugger;
            agent_msg.debugger_welcome_message = submission.debugger_welcome_message.clone();
            if submission.enable_debugger {
                agent_msg.aksh_debug_run_id = Some(run_id.to_string());
                agent_msg.aksh_debug_transport = Some("local".to_string());
            }
            inner
                .inflight_requests
                .insert(request_id, (run_id, job.id.clone()));
            let job_request = TaskAgentJobRequestRecord {
                request_id,
                run_id,
                job_id: job.id.clone(),
                agent_job_id: agent_msg.job_id,
                plan_id: agent_msg.plan.plan_id.clone(),
                plan_type: agent_msg.plan.plan_type.clone(),
                timeline_id: agent_msg.timeline.id,
                result: None,
                locked_until: agent_request_locked_until(),
                started_at: None,
                last_renewed_at: None,
                timeout_triggered: false,
            };
            inner
                .plan_requests
                .insert(job_request.plan_id.clone(), request_id);
            inner
                .agent_job_requests
                .insert(job_request.agent_job_id, request_id);
            inner
                .timeline_requests
                .insert(job_request.timeline_id, request_id);
            inner.job_requests.insert(request_id, job_request);

            let queued_job = QueuedJob {
                run_id,
                job_id: job.id.clone(),
                base_id: job.base_id.clone(),
                needs: job.needs.clone(),
                if_condition: job.if_condition.clone(),
                condition_context,
                fail_fast: job.fail_fast,
                max_parallel: job.max_parallel,
                runs_on: job.runs_on.clone(),
                message: agent_msg,
                concurrency: concurrency::concurrency_from_plan_fields(
                    job.concurrency_group.as_deref(),
                    job.concurrency_cancel_in_progress.as_deref(),
                    job.concurrency_queue.as_deref(),
                ),
                matrix: job
                    .matrix
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            };
            job_base_ids.insert(job.id.clone(), job.base_id.clone());
            job_fail_fast.insert(job.base_id.clone(), job.fail_fast);
            built_jobs.push(queued_job);
        }

        // Workflow-level concurrency gate.
        let mut hold_entire_run = false;
        if let Some((group, cancel, queue, raw)) = &workflow_concurrency_eval {
            let key = concurrency::concurrency_key(&submission.repository, group);
            match try_acquire_concurrency(
                &mut inner,
                key,
                group.clone(),
                concurrency::Holder::Run(run_id),
                *cancel,
                *queue,
            ) {
                Ok(true) => {
                    inner.run_concurrency.insert(run_id, raw.clone());
                }
                Ok(false) => {
                    hold_entire_run = true;
                    inner.run_concurrency.insert(run_id, raw.clone());
                }
                Err(e) if e == "concurrency_queue_overflow" => {
                    // Cancel this run immediately — all jobs Cancelled.
                    for job in &built_jobs {
                        statuses.insert(job.job_id.clone(), ExecutionStatus::Cancelled);
                    }
                    let queued_jobs = statuses.len();
                    inner.runs.insert(
                        run_id,
                        RunRecord {
                            run_id,
                            submission,
                            jobs: statuses,
                            job_outputs: BTreeMap::new(),
                            job_base_ids,
                            job_needs,
                            job_fail_fast,
                            status: ExecutionStatus::Cancelled,
                            job_check_run_ids: BTreeMap::new(),
                            reusable_calls,
                            jobs_list: Vec::new(),
                        },
                    );
                    drop(inner);
                    shared
                        .state
                        .emit(NdjsonEvent::RunAccepted {
                            run_id,
                            queued_jobs,
                        })
                        .await;
                    shared
                        .state
                        .emit(NdjsonEvent::RunStatus {
                            run_id,
                            status: ExecutionStatus::Cancelled,
                            reason: concurrency::cancelled_reason(),
                        })
                        .await;
                    return Ok(RunAccepted {
                        run_id,
                        queued_jobs,
                    });
                }
                Err(e) => {
                    return Err(ApiError::bad_request(e));
                }
            }
        }

        if hold_entire_run {
            for job in &built_jobs {
                statuses.insert(job.job_id.clone(), ExecutionStatus::Pending);
            }
            inner.held_runs.insert(run_id, built_jobs);
            let queued_jobs = statuses.len();
            inner.runs.insert(
                run_id,
                RunRecord {
                    run_id,
                    submission,
                    jobs: statuses,
                    job_outputs: BTreeMap::new(),
                    job_base_ids,
                    job_needs,
                    job_fail_fast,
                    status: ExecutionStatus::Pending,
                    job_check_run_ids: BTreeMap::new(),
                    reusable_calls,
                    jobs_list: Vec::new(),
                },
            );
            drop(inner);
            shared
                .state
                .emit(NdjsonEvent::RunAccepted {
                    run_id,
                    queued_jobs,
                })
                .await;
            shared
                .state
                .emit(NdjsonEvent::RunStatus {
                    run_id,
                    status: ExecutionStatus::Pending,
                    reason: concurrency::pending_reason(),
                })
                .await;
            return Ok(RunAccepted {
                run_id,
                queued_jobs,
            });
        }
        // Install a provisional run before evaluating per-job and JobSet gates.
        // Multiple holders from this same submission can cancel each other;
        // cancellation helpers need the run to exist so they can persist the
        // affected job conclusion instead of silently becoming no-ops.
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                submission: submission.clone(),
                jobs: statuses.clone(),
                job_outputs: BTreeMap::new(),
                job_base_ids: job_base_ids.clone(),
                job_needs: job_needs.clone(),
                job_fail_fast: job_fail_fast.clone(),
                status: ExecutionStatus::Queued,
                job_check_run_ids: BTreeMap::new(),
                reusable_calls: reusable_calls.clone(),
                jobs_list: Vec::new(),
            },
        );

        // Reusable workflow invocations must acquire caller and embedded
        // concurrency gates as one ordered, deduplicated admission set. A
        // partially admitted JobSet keeps its earlier keys while waiting on
        // the next key, preventing it from bypassing either scope.
        let mut jobset_blocked: std::collections::HashSet<JobId> = std::collections::HashSet::new();
        for call in reusable_calls.values() {
            let member_ids: BTreeSet<JobId> = call
                .inner_job_ids
                .iter()
                .map(|id| JobId(id.clone()))
                .collect();
            let id = JobSetId {
                run_id,
                job_ids: member_ids.clone(),
            };
            let mut gates = Vec::new();
            let mut evaluation_failed = false;

            for (raw, scope, label, inputs) in [
                (
                    call.caller_concurrency.as_ref(),
                    concurrency::ConcurrencyScope::Job,
                    "caller concurrency (JobSet)",
                    &submission.inputs,
                ),
                (
                    call.embedded_concurrency.as_ref(),
                    concurrency::ConcurrencyScope::Workflow,
                    "embedded concurrency (JobSet)",
                    &call.inputs,
                ),
            ] {
                let Some(raw) = raw else { continue };
                let eval_ctx = concurrency::ConcurrencyContext {
                    scope,
                    github: &github,
                    vars: &submission.vars,
                    inputs,
                    matrix: Some(&call.matrix),
                    strategy: None,
                    needs: None,
                };
                match concurrency::evaluate_concurrency(raw, &eval_ctx) {
                    Ok((group, cancel_in_progress, queue)) if !group.trim().is_empty() => {
                        merge_jobset_gate(
                            &mut gates,
                            JobSetGate {
                                key: concurrency::concurrency_key(&submission.repository, &group),
                                display_name: group,
                                cancel_in_progress,
                                queue,
                            },
                        );
                    }
                    Ok((_, _, _)) => {
                        evaluation_failed = true;
                    }
                    Err(error) => {
                        concurrency::log_eval_error(label, &error);
                        evaluation_failed = true;
                    }
                }
            }

            if evaluation_failed {
                for member_id in &member_ids {
                    statuses.insert(member_id.clone(), ExecutionStatus::Failure);
                }
                jobset_blocked.extend(member_ids);
                continue;
            }
            if gates.is_empty() {
                continue;
            }

            inner.jobset_admissions.insert(
                id.clone(),
                JobSetAdmission {
                    gates,
                    acquired_keys: BTreeSet::new(),
                },
            );
            match advance_jobset_admission(&mut inner, &id, None) {
                Ok(JobSetAdmissionResult::Ready) => {}
                Ok(JobSetAdmissionResult::Blocked) => {
                    jobset_blocked.extend(member_ids);
                }
                Err(error) => {
                    let status = if error == "concurrency_queue_overflow" {
                        ExecutionStatus::Cancelled
                    } else {
                        ExecutionStatus::Failure
                    };
                    for member_id in &member_ids {
                        statuses.insert(member_id.clone(), status);
                    }
                    jobset_blocked.extend(member_ids);
                }
            }
        }

        // Enqueue jobs (workflow concurrency free / acquired).
        for queued_job in built_jobs {
            let job_id = queued_job.job_id.clone();
            let base_id = queued_job.base_id.clone();

            // A blocked JobSet member must remain durably parked until every
            // required key is acquired. Terminal members are not parked.
            if jobset_blocked.contains(&job_id) {
                let status = statuses.get(&job_id).copied();
                if status.is_some_and(concurrency::is_awaiting_execution) {
                    statuses.insert(job_id, ExecutionStatus::Pending);
                    inner.concurrency_blocked.push_back(queued_job);
                }
                continue;
            }

            let needs_empty = queued_job.needs.is_empty();
            let max_parallel = queued_job.max_parallel;
            let under_mp = max_parallel
                .is_none_or(|max| ready_by_base.get(&base_id).copied().unwrap_or(0) < max);

            if needs_empty && under_mp {
                // Job-level concurrency gate (needs/max_parallel already satisfied).
                match try_enqueue_with_job_concurrency(
                    &mut inner,
                    &github,
                    &submission,
                    queued_job,
                    &mut statuses,
                ) {
                    Ok(true) => {
                        *ready_by_base.entry(base_id).or_default() += 1;
                        ready_jobs += 1;
                    }
                    Ok(false) => {
                        // parked pending
                    }
                    Err(_) => {
                        // cancelled by queue overflow or eval failure already marked
                    }
                }
            } else {
                statuses.insert(job_id, ExecutionStatus::Queued);
                inner.pending_jobs.push_back(queued_job);
            }
        }

        // Preserve terminal conclusions written through cancel_job_inner while
        // gates were evaluated. Non-terminal scheduling state remains owned by
        // the local status map and is installed below with the final record.
        if let Some(provisional) = inner.runs.get(&run_id) {
            for (job_id, status) in &provisional.jobs {
                if concurrency::is_terminal(*status) {
                    statuses.insert(job_id.clone(), *status);
                }
            }
        }

        let queued_jobs = statuses.len();
        // C-05: derive the initial run status from job statuses so that eval
        // failures (Failure) are reflected immediately rather than leaving the
        // run permanently Queued. summarize_run returns InProgress for any mix
        // of Queued/Pending jobs; map that to Queued since no job has started.
        let initial_status = {
            let s = summarize_run(statuses.values().copied());
            if s == ExecutionStatus::InProgress {
                ExecutionStatus::Queued
            } else {
                s
            }
        };
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                submission,
                jobs: statuses,
                job_outputs: BTreeMap::new(),
                job_base_ids,
                job_needs,
                job_fail_fast,
                status: initial_status,
                job_check_run_ids: BTreeMap::new(),
                reusable_calls,
                jobs_list: Vec::new(),
            },
        );
        let cancel_count = inner.cancellation_queue.len();
        drop(inner);
        if ready_jobs > 0 || cancel_count > 0 {
            shared.state.message_notify.notify_waiters();
        }
        for (event_run_id, job_id) in initially_skipped {
            shared
                .state
                .emit(NdjsonEvent::JobStatus {
                    run_id: event_run_id,
                    job_id,
                    status: ExecutionStatus::Skipped,
                    reason: None,
                })
                .await;
        }
        shared
            .state
            .emit(NdjsonEvent::RunAccepted {
                run_id,
                queued_jobs,
            })
            .await;
        Ok(RunAccepted {
            run_id,
            queued_jobs,
        })
    }
}

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
async fn submit_run(
    State(shared): State<Arc<SharedState>>,
    Json(submission): Json<WorkflowSubmission>,
) -> Result<Json<RunAccepted>, ApiError> {
    submit_run_inner(&shared, submission).await.map(Json)
}

async fn get_scheduler_history(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<Vec<crate::scheduler::ScheduleFire>>, ApiError> {
    if let Some(scheduler) = &shared.state.scheduler {
        let history = scheduler.history.lock().await.clone();
        Ok(Json(history))
    } else {
        Ok(Json(vec![]))
    }
}

fn value_to_input_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        _ => value.to_string(),
    }
}

fn git_ref_context(git_ref: &str) -> (Option<String>, Option<String>) {
    if let Some(branch) = git_ref.strip_prefix("refs/heads/") {
        (Some(branch.to_owned()), None)
    } else if let Some(tag) = git_ref.strip_prefix("refs/tags/") {
        (None, Some(tag.to_owned()))
    } else {
        (None, None)
    }
}

fn changed_paths_from_payload(payload: &serde_json::Value) -> Vec<String> {
    let mut paths = Vec::new();

    if let Some(values) = payload.get("paths").and_then(|value| value.as_array()) {
        collect_string_array(values, &mut paths);
    }

    if let Some(commits) = payload.get("commits").and_then(|value| value.as_array()) {
        for commit in commits {
            for field in ["added", "modified", "removed"] {
                if let Some(values) = commit.get(field).and_then(|value| value.as_array()) {
                    collect_string_array(values, &mut paths);
                }
            }
        }
    }

    paths.sort();
    paths.dedup();
    paths
}

fn collect_string_array(values: &[serde_json::Value], out: &mut Vec<String>) {
    out.extend(
        values
            .iter()
            .filter_map(|value| value.as_str())
            .map(str::to_owned),
    );
}

async fn get_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let mut run = inner
        .runs
        .get(&run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;

    // Results/timeline updates only create details for dispatched jobs. Keep
    // the native jobs_list a complete projection by adding jobs that were
    // cancelled or failed before dispatch with an empty step list.
    for (job_id, status) in &run.jobs {
        if !run.jobs_list.iter().any(|detail| detail.name == job_id.0) {
            run.jobs_list.push(JobDetail {
                name: job_id.0.clone(),
                conclusion: status_string(*status),
                steps: Vec::new(),
            });
        }
    }

    for job_detail in &mut run.jobs_list {
        if let Some(status) = run.jobs.get(&JobId(job_detail.name.clone())) {
            job_detail.conclusion = status_string(*status);
        }
    }

    Ok(Json(run))
}

async fn get_run_logs(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Response, ApiError> {
    let (state_dir, sources) = {
        let inner = shared.state.inner.lock().await;
        if !inner.runs.contains_key(&run_id) {
            return Err(ApiError::not_found("run not found"));
        }

        let mut requests: Vec<&TaskAgentJobRequestRecord> = inner
            .job_requests
            .values()
            .filter(|request| request.run_id == run_id)
            .collect();
        requests.sort_by_key(|request| request.request_id);
        let sources = requests
            .into_iter()
            .map(|request| {
                let prefix = format!("{}/", request.plan_id);
                let mut blocks: Vec<(&str, &[u8])> = inner
                    .logs
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix(&prefix)
                            .map(|log_id| (log_id, value.as_slice()))
                    })
                    .collect();
                blocks.sort_by(|(left, _), (right, _)| {
                    match (left.parse::<u64>(), right.parse::<u64>()) {
                        (Ok(left), Ok(right)) => left.cmp(&right),
                        (Ok(_), Err(_)) => std::cmp::Ordering::Less,
                        (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
                        (Err(_), Err(_)) => left.cmp(right),
                    }
                });
                (
                    request.plan_id.clone(),
                    request.agent_job_id.to_string(),
                    blocks
                        .into_iter()
                        .map(|(_, block)| block.to_vec())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        (shared.state.state_dir.clone(), sources)
    };

    let mut merged = Vec::new();
    for (plan_id, agent_job_id, fallback_blocks) in sources {
        let results_log = state_dir
            .join("replay")
            .join("results")
            .join(plan_id)
            .join(agent_job_id)
            .join("job-logs.txt");
        match tokio::fs::read(&results_log).await {
            Ok(contents) => merged.extend_from_slice(&contents),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                for block in fallback_blocks {
                    merged.extend_from_slice(&block);
                }
            }
            Err(error) => {
                return Err(ApiError::internal(format!(
                    "failed to read run log `{}`: {error}",
                    results_log.display()
                )));
            }
        }
    }

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(merged))
        .expect("static run log response"))
}

async fn cancel_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    if !inner.runs.contains_key(&run_id) {
        return Err(ApiError::not_found("run not found"));
    }
    let cancellation_count =
        cancel_run_inner(&mut inner, run_id, None /* no concurrency reason */);
    let record = inner
        .runs
        .get(&run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    drop(inner);
    if cancellation_count > 0 {
        shared.state.message_notify.notify_waiters();
    }
    shared
        .state
        .emit(NdjsonEvent::RunStatus {
            run_id,
            status: ExecutionStatus::Cancelled,
            reason: None,
        })
        .await;
    Ok(Json(record))
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

async fn rerun_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunAccepted>, ApiError> {
    let submission = {
        let inner = shared.state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .map(|run| run.submission.clone())
            .ok_or_else(|| ApiError::not_found("run not found"))?
    };
    submit_run(State(shared), Json(submission)).await
}

async fn run_events(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Response, ApiError> {
    let inner = shared.state.inner.lock().await;
    let run = inner
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    let mut out = event_to_ndjson(&NdjsonEvent::RunStatus {
        run_id,
        status: run.status,
        reason: None,
    })?;
    for (job_id, status) in &run.jobs {
        out.push_str(&event_to_ndjson(&NdjsonEvent::JobStatus {
            run_id,
            job_id: job_id.clone(),
            status: *status,
            reason: None,
        })?);
    }
    if let Some(events) = inner.timeline_events.get(&run_id) {
        for event in events {
            out.push_str(&event_to_ndjson(event)?);
        }
    }
    Ok(Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from(out))
        .expect("static response builder"))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrokerAcquireJobRequest {
    job_message_id: uuid::Uuid,
    #[allow(dead_code)]
    billing_owner_id: Option<String>,
    #[allow(dead_code)]
    runner_os: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrokerRenewJobRequest {
    job_id: uuid::Uuid,
    #[serde(rename = "planId")]
    _plan_id: String,
    conclusion: Option<String>,
    #[serde(default)]
    outputs: BTreeMap<String, serde_json::Value>,
}

fn execution_status_from_runner_result(result: &str) -> Option<ExecutionStatus> {
    match result.to_ascii_lowercase().as_str() {
        "success" | "succeeded" | "succeededwithissues" => Some(ExecutionStatus::Success),
        "failure" | "failed" => Some(ExecutionStatus::Failure),
        "cancelled" | "canceled" => Some(ExecutionStatus::Cancelled),
        "skipped" => Some(ExecutionStatus::Skipped),
        _ => None,
    }
}

fn broker_run_service_url(runner_id: i64) -> String {
    format!("{}/broker/{runner_id}/", public_base_url())
}

fn public_base_url() -> String {
    std::env::var("AKSH_PUBLIC_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:9090".to_owned())
        .trim_end_matches('/')
        .to_owned()
}

fn format_reusable_workflow_ref(repository: &str, workflow_ref: &str, caller_ref: &str) -> String {
    if let Some(path) = workflow_ref.strip_prefix("./") {
        let (path, git_ref) = path.split_once('@').unwrap_or((path, caller_ref));
        return format!("{repository}/{path}@{git_ref}");
    }
    workflow_ref.to_owned()
}

fn normalize_oidc_issuer(value: String) -> anyhow::Result<String> {
    let issuer = value.trim_end_matches('/').to_owned();
    if issuer.is_empty()
        || !(issuer.starts_with("https://") || issuer.starts_with("http://"))
        || issuer.contains('?')
        || issuer.contains('#')
    {
        anyhow::bail!("OIDC issuer must be an absolute HTTP(S) URL without query or fragment");
    }
    Ok(issuer)
}

/// Return the effective OIDC issuer URL, falling back to
/// `{public_base_url}/oidc` when not explicitly configured.
fn oidc_issuer_url(inner: &InnerState) -> String {
    if inner.oidc_issuer.is_empty() {
        format!("{}/oidc", public_base_url())
    } else {
        inner.oidc_issuer.clone()
    }
}

fn websocket_base_url() -> String {
    let base = public_base_url();
    if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base
    }
}

fn runner_server_url() -> String {
    format!("{}/runner/server", public_base_url())
}

fn broker_job_ref(request: &TaskAgentJobRequestRecord, runner_id: i64) -> serde_json::Value {
    json!({
        "messageId": request.agent_job_id.to_string(),
        "messageType": "RunnerJobRequest",
        "body": serde_json::to_string(&json!({
            "runner_request_id": request.agent_job_id.to_string(),
            "run_service_url": broker_run_service_url(runner_id),
            "billing_owner_id": "local",
            "should_acknowledge": true
        })).unwrap()
    })
}

fn broker_job_ref_root(request: &TaskAgentJobRequestRecord, runner_id: i64) -> serde_json::Value {
    // messageId must be unique across job + cancel messages on a session.
    // Using request_id alone collides with cancel messages that also allocate
    // from the same integer space (runner in-memory dedup then drops the job).
    json!({
        "messageId": request.request_id,
        "messageType": "RunnerJobRequest",
        "body": serde_json::to_string(&json!({
            "runner_request_id": request.agent_job_id.to_string(),
            "run_service_url": broker_run_service_url(runner_id),
            "billing_owner_id": "local",
            "should_acknowledge": true
        })).unwrap()
    })
}

/// Allocate a session-unique broker message id that cannot collide with
/// `request_id` values used as RunnerJobRequest messageIds.
fn next_broker_message_id(inner: &mut InnerState) -> i64 {
    // request_ids start at 1 and increase; keep message ids in a separate high
    // range so cancels never reuse a past/future request_id.
    const MESSAGE_ID_BASE: i64 = 1_000_000;
    if inner.next_message_id < MESSAGE_ID_BASE {
        inner.next_message_id = MESSAGE_ID_BASE;
    }
    inner.next_message_id += 1;
    inner.next_message_id
}
async fn next_message_broker_ref(
    State(shared): State<Arc<SharedState>>,
    Path(pool_id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Response, ApiError> {
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
            return Ok(Json(message).into_response());
        }

        if let Some(request_id) = inner.session_active_requests.get(&session_id).copied() {
            if let Some(request) = inner.job_requests.get(&request_id) {
                if let Some(pos) = inner
                    .cancellation_queue
                    .iter()
                    .position(|c| c.run_id == request.run_id && c.job_id == request.job_id)
                {
                    let cancellation = inner.cancellation_queue.remove(pos).unwrap();
                    let message = build_broker_plaintext_message(
                        &mut inner,
                        &session_id,
                        azdo::message_type::JOB_CANCELLED,
                        concurrency::job_cancel_body(cancellation.agent_job_id),
                    );
                    return Ok(Json(message).into_response());
                }

                if request.result.is_none() {
                    return Ok(Json(broker_job_ref(request, pool_id)).into_response());
                }
            }
            inner.session_active_requests.remove(&session_id);
        }

        let runner_labels = inner.runner_labels_for_session(&session_id);
        let Some(queued) = take_matching_job(&mut inner.queue, &runner_labels) else {
            drop(inner);
            if wait_seconds == 0 {
                return Ok((StatusCode::ACCEPTED, Json(json!({}))).into_response());
            }
            if tokio::time::timeout(
                Duration::from_secs(wait_seconds),
                shared.state.message_notify.notified(),
            )
            .await
            .is_err()
            {
                return Ok((StatusCode::ACCEPTED, Json(json!({}))).into_response());
            }
            continue;
        };

        if let Some(run) = inner.runs.get_mut(&queued.run_id) {
            run.status = ExecutionStatus::InProgress;
            run.jobs
                .insert(queued.job_id.clone(), ExecutionStatus::InProgress);
        }

        let request_id = queued.message.request_id;
        inner
            .session_active_requests
            .insert(session_id.clone(), request_id);
        if let Some(request) = inner.job_requests.get_mut(&request_id) {
            request.started_at = Some(std::time::SystemTime::now());
            request.last_renewed_at = Some(std::time::SystemTime::now());
        }
        inner
            .broker_messages
            .insert(request_id, queued.message.clone());
        let request = inner
            .job_requests
            .get(&request_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("agent request not found"))?;

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

        return Ok(Json(broker_job_ref(&request, pool_id)).into_response());
    }
}

/// GET `/_apis/distributedtask/pools/:pool_id/messages` dispatcher.
///
/// Sessions created via the AzDO path (`create_session_disttask`) are marked
/// in `azdo_sessions` and receive the full encrypted `PipelineAgentJobRequest`
/// message via `next_message_compat`.  All other sessions (broker-hybrid tests,
/// legacy broker flow) get the lightweight `RunnerJobRequest` broker ref.
async fn next_message_disttask(
    State(shared): State<Arc<SharedState>>,
    Path(pool_id): Path<i64>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let session_id = params
        .get("sessionId")
        .cloned()
        .unwrap_or_else(|| "default".to_owned());
    let is_azdo = {
        let inner = shared.state.inner.lock().await;
        inner.azdo_sessions.contains(&session_id)
    };
    if is_azdo {
        next_message_compat(State(shared), Path(pool_id), Query(params))
            .await
            .map(|r| r.into_response())
    } else {
        next_message_broker_ref(State(shared), Path(pool_id), Query(params)).await
    }
}

async fn broker_session_root(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    let session_id = uuid::Uuid::new_v4().to_string();
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .session_keys
            .insert(session_id.clone(), SessionEncryption::generate());
        inner
            .broker_session_runners
            .insert(session_id.clone(), runner_id);
    }
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "sessionId": session_id,
            "ownerName": "aksh-runner",
            "assignmentQueued": false,
            "orchestrationId": ""
        })),
    ))
}

async fn broker_delete_session_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    if let Some(session_id) = params.get("sessionId") {
        remove_broker_session(&shared, session_id, runner_id).await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn broker_delete_session_by_path(
    State(shared): State<Arc<SharedState>>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    remove_broker_session(&shared, &session_id, runner_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_broker_session(
    shared: &Arc<SharedState>,
    session_id: &str,
    runner_id: i64,
) -> Result<(), ApiError> {
    let mut inner = shared.state.inner.lock().await;
    match inner.broker_session_runners.get(session_id).copied() {
        Some(owner) if owner == runner_id => {
            inner.broker_session_runners.remove(session_id);
            inner.session_keys.remove(session_id);
            inner.session_active_requests.remove(session_id);
            Ok(())
        }
        Some(_) => Err(ApiError::forbidden(
            "broker session belongs to another runner",
        )),
        None => Err(ApiError::not_found("broker session not found")),
    }
}
fn authenticated_runner_id(
    shared: &Arc<SharedState>,
    headers: &HeaderMap,
    expected_runner_id: Option<i64>,
) -> Result<i64, ApiError> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError::unauthorized("runner listen token required"))?;
    let runner_id = shared
        .state
        .runner_id_from_token(bearer)
        .ok_or_else(|| ApiError::unauthorized("runner listen token required"))?;
    if expected_runner_id.is_some_and(|expected| expected != runner_id) {
        return Err(ApiError::forbidden(
            "runner token does not match broker path",
        ));
    }
    Ok(runner_id)
}

fn ensure_broker_request_owner(
    inner: &InnerState,
    request_id: i64,
    runner_id: i64,
) -> Result<(), ApiError> {
    let owner = inner
        .session_active_requests
        .iter()
        .find_map(|(session_id, active_request_id)| {
            (*active_request_id == request_id).then_some(session_id)
        })
        .and_then(|session_id| inner.broker_session_runners.get(session_id).copied());
    match owner {
        Some(owner) if owner == runner_id => Ok(()),
        Some(_) => Err(ApiError::forbidden(
            "broker request belongs to another runner",
        )),
        None => Err(ApiError::not_found(
            "broker request is not assigned to a session",
        )),
    }
}

async fn next_message_broker_ref_root(
    State(shared): State<Arc<SharedState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let runner_id = authenticated_runner_id(&shared, &headers, None)?;
    let session_id = params
        .get("sessionId")
        .cloned()
        .ok_or_else(|| ApiError::bad_request("broker sessionId is required"))?;
    {
        let inner = shared.state.inner.lock().await;
        if inner.broker_session_runners.get(&session_id) != Some(&runner_id) {
            return Err(ApiError::forbidden(
                "broker session belongs to another runner",
            ));
        }
    }

    // Default to 50s long-poll (golden flows show ~50s waits between jobs)
    let wait = params
        .get("waitSeconds")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(50);
    // The runner may report completion before its worker process has fully
    // exited. GitHub keeps polling with status=Busy during that drain window;
    // never dispatch a successor until the runner reports Online again.
    let runner_busy = params
        .get("status")
        .is_some_and(|status| status.eq_ignore_ascii_case("busy"));

    let deadline = std::time::Instant::now() + Duration::from_secs(wait);

    loop {
        let maybe = {
            let mut inner = shared.state.inner.lock().await;
            // Prefer delivering JobCancellation for the active request (official
            // cancel path). Without this, concurrency cancel-in-progress never
            // reaches broker-path runners.
            if let Some(request_id) = inner.session_active_requests.get(&session_id).copied() {
                if let Some(request) = inner.job_requests.get(&request_id).cloned() {
                    if let Some(pos) = inner
                        .cancellation_queue
                        .iter()
                        .position(|c| c.run_id == request.run_id && c.job_id == request.job_id)
                    {
                        let cancellation = inner.cancellation_queue.remove(pos).unwrap();
                        let message_id = next_broker_message_id(&mut inner);
                        Some(json!({
                            "messageId": message_id,
                            "messageType": azdo::message_type::JOB_CANCELLED,
                            "body": concurrency::job_cancel_body(cancellation.agent_job_id),
                        }))
                    } else if request.result.is_none() {
                        // Still running — long-poll for cancel rather than
                        // redelivering the same RunnerJobRequest (runner dedups it).
                        None
                    } else {
                        inner.session_active_requests.remove(&session_id);
                        None
                    }
                } else {
                    inner.session_active_requests.remove(&session_id);
                    None
                }
            } else if runner_busy {
                None
            } else {
                let labels = inner.runner_labels_for_session(&session_id);
                if let Some(queued) = take_matching_job(&mut inner.queue, &labels) {
                    if let Some(run) = inner.runs.get_mut(&queued.run_id) {
                        run.status = ExecutionStatus::InProgress;
                        run.jobs
                            .insert(queued.job_id.clone(), ExecutionStatus::InProgress);
                    }
                    let request_id = queued.message.request_id;
                    if let Some(request) = inner.job_requests.get_mut(&request_id) {
                        request.started_at = Some(std::time::SystemTime::now());
                        request.last_renewed_at = Some(std::time::SystemTime::now());
                    }
                    // Job messageId = request_id (low range). Cancels use 1_000_000+.
                    inner
                        .session_active_requests
                        .insert(session_id.clone(), request_id);
                    inner
                        .broker_messages
                        .insert(request_id, queued.message.clone());
                    let request = inner
                        .job_requests
                        .get(&request_id)
                        .expect("queued request must exist");
                    Some(broker_job_ref_root(request, 1))
                } else {
                    None
                }
            }
        };

        if let Some(message) = maybe {
            return Ok(Json(message));
        }
        if wait == 0 || std::time::Instant::now() >= deadline {
            return Ok(Json(serde_json::Value::Null));
        }
        // Wake promptly on cancel/enqueue rather than fixed 250ms sleep.
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let slice = remaining.min(Duration::from_secs(3));
        let _ = tokio::time::timeout(slice, shared.state.message_notify.notified()).await;
    }
}

async fn broker_acknowledge_root(
    State(_shared): State<Arc<SharedState>>,
    Query(_params): Query<std::collections::HashMap<String, String>>,
) -> StatusCode {
    // Acknowledge receipt of the message. Do NOT clear session_active_requests
    // here — the runner is still working on the job. The session's active
    // request is cleared when completejob sets the result and the next poll
    // sees result.is_some() at line 2190.
    StatusCode::OK
}

async fn broker_acquire_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<BrokerAcquireJobRequest>,
) -> Result<Json<azdo::AgentJobRequestMessage>, ApiError> {
    authenticated_runner_id(&shared, &headers, Some(runner_id))?;
    let inner = shared.state.inner.lock().await;
    let request_id = inner
        .agent_job_requests
        .get(&request.job_message_id)
        .copied()
        .ok_or_else(|| ApiError::not_found("broker job message not found"))?;
    ensure_broker_request_owner(&inner, request_id, runner_id)?;
    let mut message = inner
        .broker_messages
        .get(&request_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("broker job payload not found"))?;
    message.message_type = Some(azdo::message_type::RUNNER_JOB_REQUEST.to_owned());
    let run_service_url = broker_run_service_url(runner_id);
    for endpoint in &mut message.resources.endpoints {
        if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
            endpoint.url = Some(run_service_url.clone());
            endpoint.authorization.parameters.insert(
                "AccessToken".to_owned(),
                shared
                    .state
                    .mint_runtime_token(&message.plan.plan_id, &message.job_id),
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
            endpoint.data.insert(
                "FeedStreamUrl".to_owned(),
                format!("{}/ws/live-logs/{}", websocket_base_url(), message.job_id),
            );
        }
    }
    message.billing_owner_id = request.billing_owner_id;
    // Run-service payloads use the DTO default; internal request IDs remain in
    // `job_requests` and broker lookup maps for renew/complete bookkeeping.
    message.request_id = 0;
    Ok(Json(message))
}

async fn broker_renew_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<BrokerRenewJobRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authenticated_runner_id(&shared, &headers, Some(runner_id))?;
    let mut inner = shared.state.inner.lock().await;
    let request_id = inner
        .agent_job_requests
        .get(&request.job_id)
        .copied()
        .ok_or_else(|| ApiError::not_found("broker renew request not found"))?;
    ensure_broker_request_owner(&inner, request_id, runner_id)?;
    let record = inner
        .job_requests
        .get_mut(&request_id)
        .ok_or_else(|| ApiError::not_found("agent request not found"))?;
    record.locked_until = agent_request_locked_until();
    record.last_renewed_at = Some(std::time::SystemTime::now());
    Ok(Json(json!({"lockedUntil": record.locked_until})))
}

async fn broker_complete_job(
    State(shared): State<Arc<SharedState>>,
    Path(runner_id): Path<i64>,
    headers: HeaderMap,
    Json(request): Json<BrokerRenewJobRequest>,
) -> Result<StatusCode, ApiError> {
    authenticated_runner_id(&shared, &headers, Some(runner_id))?;
    let status = match request.conclusion.as_deref() {
        Some(conclusion) => execution_status_from_runner_result(conclusion).ok_or_else(|| {
            ApiError::bad_request(format!("unknown broker conclusion `{conclusion}`"))
        })?,
        // Older broker clients omit this field on successful completion.
        None => ExecutionStatus::Success,
    };

    // Extract outputs from the completejob body.
    // Runner sends: { "outputName": {"value": "theValue"} }
    // Server stores: { "outputName": "theValue" }
    let mut outputs = aksh_gha_protocol::OutputMap::new();
    for (key, val) in &request.outputs {
        if let Some(v) = val.get("value").and_then(|v| v.as_str()) {
            outputs.insert(key.clone(), serde_json::Value::String(v.to_owned()));
        } else if let Some(v) = val.get("value") {
            outputs.insert(key.clone(), v.clone());
        } else if let Some(s) = val.as_str() {
            outputs.insert(key.clone(), serde_json::Value::String(s.to_owned()));
        } else {
            outputs.insert(key.clone(), val.clone());
        }
    }

    let completion = {
        let mut inner = shared.state.inner.lock().await;
        let request_id = inner
            .agent_job_requests
            .get(&request.job_id)
            .copied()
            .ok_or_else(|| ApiError::not_found("broker complete request not found"))?;
        ensure_broker_request_owner(&inner, request_id, runner_id)?;
        debug!(request_id, job_id = %request.job_id, "broker complete: found request");
        if let Some(record) = inner.job_requests.get_mut(&request_id) {
            record.result = Some(status);
            record.locked_until = agent_request_locked_until();
        }
        // Free the session so the next broker poll can take a new job immediately
        // (otherwise the poll arm waits until it observes result.is_some()).
        inner
            .session_active_requests
            .retain(|_, &mut rid| rid != request_id);
        let run_job = inner.inflight_requests.remove(&request_id).or_else(|| {
            job_request_tuple(&inner, request_id).map(|(_, run_id, job_id)| (run_id, job_id))
        });
        match run_job {
            Some((run_id, job_id)) => {
                info!(%run_id, %job_id, "broker complete: completing job");
                Some(JobCompletion {
                    run_id,
                    job_id,
                    status,
                    outputs,
                })
            }
            None => {
                warn!(
                    request_id,
                    "broker complete: no inflight_requests entry found"
                );
                None
            }
        }
    };
    if let Some(completion) = completion {
        let _ = complete_job_inner(shared.clone(), completion).await?;
    }
    // Wake long-polling runners so a queued successor job is delivered promptly
    // after cancel/complete (concurrency release path).
    shared.state.message_notify.notify_waiters();
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
struct JobLogsSignedBlobUrlRequest {
    workflow_job_run_backend_id: String,
    workflow_run_backend_id: String,
}

#[derive(Debug, Deserialize)]
struct StepLogsSignedBlobUrlRequest {
    step_backend_id: String,
    workflow_job_run_backend_id: String,
    workflow_run_backend_id: String,
}

async fn twirp_workflow_steps_update(
    State(shared): State<Arc<SharedState>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let mut inner = shared.state.inner.lock().await;

    let plan_id = payload["workflow_run_backend_id"].as_str().unwrap_or("");
    let agent_job_id_str = payload["workflow_job_run_backend_id"]
        .as_str()
        .unwrap_or("");

    if let (Some(plan_uuid), Some(job_uuid)) = (
        uuid::Uuid::parse_str(plan_id).ok(),
        uuid::Uuid::parse_str(agent_job_id_str).ok(),
    ) {
        if let Some((_, run_id, job_id)) =
            resolve_callback_job(&inner, &plan_uuid.to_string(), None, Some(job_uuid))
        {
            if let Some(run) = inner.runs.get_mut(&run_id) {
                let job_name = job_id.0.clone();
                let job_detail =
                    if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
                        &mut run.jobs_list[pos]
                    } else {
                        run.jobs_list.push(JobDetail {
                            name: job_name,
                            conclusion: "success".to_owned(),
                            steps: Vec::new(),
                        });
                        run.jobs_list.last_mut().unwrap()
                    };

                if let Some(status) = run.jobs.get(&job_id) {
                    job_detail.conclusion = format!("{:?}", status).to_lowercase();
                }
                if let Some(steps) = payload["steps"].as_array() {
                    for step in steps {
                        let name = step["name"].as_str().unwrap_or("").to_owned();
                        let conclusion_num = step["conclusion"].as_u64().unwrap_or(0);
                        let status_num = step["status"].as_u64().unwrap_or(0);

                        let job_status = run.jobs.get(&job_id).copied();
                        let conclusion_str = if status_num == 6 {
                            match conclusion_num {
                                2 => "success",
                                3 => {
                                    if job_status == Some(ExecutionStatus::Cancelled) {
                                        "cancelled"
                                    } else {
                                        "failure"
                                    }
                                }
                                7 => "skipped",
                                _ => "success",
                            }
                        } else {
                            "in_progress"
                        };

                        if let Some(pos) = job_detail.steps.iter().position(|s| s.name == name) {
                            job_detail.steps[pos].conclusion = conclusion_str.to_owned();
                        } else {
                            job_detail.steps.push(StepRecord {
                                name,
                                conclusion: conclusion_str.to_owned(),
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(Json(json!({"ok": true})))
}

async fn twirp_get_job_logs_signed_blob_url(
    Json(request): Json<JobLogsSignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "logs_url": format!(
            "{}/replay/results/{}/{}/job-logs.txt",
            public_base_url(), request.workflow_run_backend_id, request.workflow_job_run_backend_id
        )
    }))
}

async fn twirp_get_step_logs_signed_blob_url(
    Json(request): Json<StepLogsSignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "logs_url": format!(
            "{}/replay/results/{}/{}/step-{}.txt",
            public_base_url(), request.workflow_run_backend_id, request.workflow_job_run_backend_id, request.step_backend_id
        ),
        "soft_size_limit": "1048576"
    }))
}

#[derive(Debug, Deserialize)]
struct StepSummarySignedBlobUrlRequest {
    step_backend_id: String,
    workflow_job_run_backend_id: String,
    workflow_run_backend_id: String,
}

async fn twirp_get_step_summary_signed_blob_url(
    Json(request): Json<StepSummarySignedBlobUrlRequest>,
) -> Json<serde_json::Value> {
    Json(json!({
        "blob_storage_type": "BLOB_STORAGE_TYPE_AZURE",
        "summary_url": format!(
            "{}/replay/results/{}/{}/step-{}-summary.md",
            public_base_url(), request.workflow_run_backend_id, request.workflow_job_run_backend_id, request.step_backend_id
        ),
        "soft_size_limit": "1048576"
    }))
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StepSummaryMetadataRequest {
    step_backend_id: String,
    workflow_job_run_backend_id: String,
    workflow_run_backend_id: String,
    size: Option<u64>,
    uploaded_at: Option<String>,
}

async fn twirp_create_step_summary_metadata(
    Json(_request): Json<StepSummaryMetadataRequest>,
) -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StepLogsMetadataRequest {
    step_backend_id: Option<String>,
    workflow_job_run_backend_id: Option<String>,
    workflow_run_backend_id: Option<String>,
    upload_url: Option<String>,
    line_count: Option<u64>,
}

/// POST CreateStepLogsMetadata — runner calls this after uploading step logs.
async fn twirp_create_step_logs_metadata(
    Json(_request): Json<StepLogsMetadataRequest>,
) -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct JobLogsMetadataRequest {
    workflow_job_run_backend_id: Option<String>,
    workflow_run_backend_id: Option<String>,
    upload_url: Option<String>,
    line_count: Option<u64>,
}

/// POST CreateJobLogsMetadata — runner calls this after uploading job logs.
async fn twirp_create_job_logs_metadata(
    Json(_request): Json<JobLogsMetadataRequest>,
) -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

// ─── Cache v2 Twirp (github.actions.results.api.v1.CacheService) ─────────────

fn scoped_cache_key(key: &str, scope: Option<&str>, repository: Option<&str>) -> String {
    format!(
        "{}:{}\0{key}",
        repository.unwrap_or("default"),
        scope.unwrap_or("default")
    )
}

#[derive(Debug, Deserialize)]
struct CacheV2CreateRequest {
    key: String,
    version: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    repository: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CacheV2FinalizeRequest {
    key: String,
    version: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    repository: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CacheV2GetDlUrlRequest {
    key: String,
    version: String,
    #[serde(default)]
    restore_keys: Vec<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    repository: Option<String>,
}

async fn twirp_cache_v2_create(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CacheV2CreateRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = scoped_cache_key(
        &request.key,
        request.scope.as_deref(),
        request.repository.as_deref(),
    );
    if shared
        .state
        .cache
        .get(&storage_key, &request.version, &[])
        .await
        .map_err(|error| ApiError::internal(format!("cache lookup error: {error}")))?
        .is_some()
    {
        return Ok(Json(json!({
            "ok": false,
            "signed_upload_url": "",
            "message": "cache already exists"
        })));
    }
    let token = uuid::Uuid::new_v4().to_string();
    let stage_dir = shared
        .state
        .state_dir
        .join("blobs")
        .join("cache")
        .join(&token);
    tokio::fs::create_dir_all(&stage_dir)
        .await
        .map_err(|e| ApiError::internal(format!("failed to create cache stage dir: {e}")))?;
    let already_reserved = {
        let mut inner = shared.state.inner.lock().await;
        if inner
            .cache_v2_pending
            .values()
            .any(|pending| pending.key == storage_key && pending.version == request.version)
        {
            true
        } else {
            inner.cache_v2_pending.insert(
                token.clone(),
                CacheV2Pending {
                    key: storage_key,
                    version: request.version,
                },
            );
            false
        }
    };
    if already_reserved {
        let _ = tokio::fs::remove_dir_all(&stage_dir).await;
        return Ok(Json(json!({
            "ok": false,
            "signed_upload_url": "",
            "message": "cache upload already reserved"
        })));
    }
    let upload_url = format!("{}/twirp-blob/cache/{token}", public_base_url());
    info!(token, "cache v2 create entry");
    Ok(Json(
        json!({ "ok": true, "signed_upload_url": upload_url, "message": "" }),
    ))
}

async fn twirp_cache_v2_finalize(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CacheV2FinalizeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = scoped_cache_key(
        &request.key,
        request.scope.as_deref(),
        request.repository.as_deref(),
    );
    // Find the pending upload token matching key+version.
    let token = {
        let inner = shared.state.inner.lock().await;
        inner
            .cache_v2_pending
            .iter()
            .find(|(_, p)| p.key == storage_key && p.version == request.version)
            .map(|(k, _)| k.clone())
    }
    .ok_or_else(|| ApiError::not_found("no pending cache upload for key+version"))?;

    let blob_path = shared
        .state
        .state_dir
        .join("blobs")
        .join("cache")
        .join(&token)
        .join("data");
    let bytes = tokio::fs::read(&blob_path).await.map_err(|e| {
        ApiError::not_found(format!("cache blob not found (not yet uploaded?): {e}"))
    })?;

    let (key, version) = {
        let inner = shared.state.inner.lock().await;
        let pending = inner
            .cache_v2_pending
            .get(&token)
            .ok_or_else(|| ApiError::internal("pending entry vanished"))?;
        (pending.key.clone(), pending.version.clone())
    };

    shared
        .state
        .cache
        .put(&key, &version, &bytes)
        .await
        .map_err(|e| ApiError::internal(format!("cache store error: {e}")))?;

    {
        let mut inner = shared.state.inner.lock().await;
        inner.cache_v2_pending.remove(&token);
    }

    // Clean up staging directory.
    let _ = tokio::fs::remove_dir_all(
        shared
            .state
            .state_dir
            .join("blobs")
            .join("cache")
            .join(&token),
    )
    .await;

    info!(key, version, size = bytes.len(), "cache v2 finalized");
    Ok(Json(json!({ "ok": true, "entry_id": "1", "message": "" })))
}

async fn twirp_cache_v2_get_dl_url(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<CacheV2GetDlUrlRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let storage_key = scoped_cache_key(
        &request.key,
        request.scope.as_deref(),
        request.repository.as_deref(),
    );
    let storage_restore_keys = request
        .restore_keys
        .iter()
        .map(|key| scoped_cache_key(key, request.scope.as_deref(), request.repository.as_deref()))
        .collect::<Vec<_>>();
    let result = shared
        .state
        .cache
        .get(&storage_key, &request.version, &storage_restore_keys)
        .await
        .map_err(|e| ApiError::internal(format!("cache lookup error: {e}")))?;

    let (entry, _bytes) = match result {
        Some(r) => r,
        None => {
            return Ok(Json(
                json!({ "ok": false, "signed_download_url": "", "matched_key": "" }),
            ))
        }
    };

    let dl_token = uuid::Uuid::new_v4().to_string();
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .cache_v2_dl_tokens
            .insert(dl_token.clone(), (entry.key.clone(), entry.version.clone()));
    }
    let download_url = format!("{}/twirp-blob/cache/{dl_token}", public_base_url());
    let matched_key = entry
        .key
        .split_once('\0')
        .map(|(_, key)| key.to_owned())
        .unwrap_or_else(|| entry.key.clone());
    info!(key = %matched_key, "cache v2 download URL issued");
    Ok(Json(json!({
        "ok": true,
        "signed_download_url": download_url,
        "matched_key": matched_key
    })))
}

// ─── Artifact v2 Twirp (github.actions.results.api.v1.ArtifactService) ────────

#[derive(Debug, Deserialize)]
struct ArtifactV2CreateRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ArtifactV2FinalizeRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
    #[serde(default)]
    size: serde_json::Value, // proto3 JSON: int64 as string
    #[serde(default)]
    hash: Option<serde_json::Value>, // StringValue: plain string or wrapped object
}

#[derive(Debug, Deserialize)]
struct ArtifactV2ListRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    #[serde(default)]
    name_filter: Option<serde_json::Value>, // StringValue: plain string in proto3 JSON
    #[serde(default)]
    id_filter: Option<serde_json::Value>, // Int64Value: string in proto3 JSON
}

#[derive(Debug, Deserialize)]
struct ArtifactV2GetSignedUrlRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ArtifactV2DeleteRequest {
    workflow_run_backend_id: String,
    workflow_job_run_backend_id: String,
    name: String,
}

fn artifact_v2_registry_key(run_id: &str, job_id: &str, name: &str) -> String {
    format!("{run_id}/{job_id}/{name}")
}

async fn save_artifact_v2_registry(shared: &Arc<SharedState>) -> Result<(), std::io::Error> {
    let registry_path = shared.state.state_dir.join("artifact_v2_registry.json");
    let serialized = {
        let inner = shared.state.inner.lock().await;
        serde_json::to_string(&inner.artifact_v2_registry)?
    };
    tokio::fs::write(&registry_path, serialized.as_bytes()).await?;
    Ok(())
}
async fn twirp_artifact_v2_create(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2CreateRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    validate_artifact_name(&request.name)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let token = uuid::Uuid::new_v4().to_string();
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let stage_dir = shared
        .state
        .state_dir
        .join("blobs")
        .join("artifact")
        .join(&token);
    tokio::fs::create_dir_all(&stage_dir)
        .await
        .map_err(|e| ApiError::internal(format!("failed to create artifact stage dir: {e}")))?;
    {
        let mut inner = shared.state.inner.lock().await;
        inner
            .artifact_v2_pending
            .insert(token.clone(), ArtifactV2Pending { registry_key });
    }
    let upload_url = format!("{}/twirp-blob/artifact/{token}", public_base_url());
    info!(token, name = request.name, "artifact v2 create");
    Ok(Json(json!({ "ok": true, "signed_upload_url": upload_url })))
}

async fn twirp_artifact_v2_finalize(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2FinalizeRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let token = {
        let inner = shared.state.inner.lock().await;
        inner
            .artifact_v2_pending
            .iter()
            .find(|(_, p)| p.registry_key == registry_key)
            .map(|(k, _)| k.clone())
    }
    .ok_or_else(|| ApiError::not_found("no pending artifact upload for this name/run/job"))?;

    // Measure actual blob size.
    let blob_path = shared
        .state
        .state_dir
        .join("blobs")
        .join("artifact")
        .join(&token)
        .join("data");
    let size = tokio::fs::metadata(&blob_path)
        .await
        .map(|m| m.len())
        .unwrap_or_else(|_| match &request.size {
            serde_json::Value::String(s) => s.parse::<u64>().unwrap_or(0),
            serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
            _ => 0,
        });

    let artifact_id;
    {
        let mut inner = shared.state.inner.lock().await;
        inner.artifact_v2_pending.remove(&token);
        inner.next_artifact_v2_id += 1;
        artifact_id = inner.next_artifact_v2_id;
        let digest = request.hash.and_then(|v| match v {
            serde_json::Value::String(s) => Some(s),
            serde_json::Value::Object(ref obj) => obj
                .get("value")
                .and_then(|val| val.as_str().map(|s| s.to_owned())),
            _ => None,
        });
        inner.artifact_v2_registry.insert(
            registry_key,
            ArtifactV2Entry {
                id: artifact_id,
                workflow_run_backend_id: request.workflow_run_backend_id,
                workflow_job_run_backend_id: request.workflow_job_run_backend_id,
                name: request.name.clone(),
                size,
                created_at: server_iso_now(),
                digest,
                blob_token: token,
            },
        );
    }
    let _ = save_artifact_v2_registry(&shared).await;
    info!(
        artifact_id,
        name = request.name,
        size,
        "artifact v2 finalized"
    );
    Ok(Json(
        json!({ "ok": true, "artifact_id": artifact_id.to_string() }),
    ))
}

async fn twirp_artifact_v2_list(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2ListRequest>,
) -> Json<serde_json::Value> {
    let inner = shared.state.inner.lock().await;

    let name_filter: Option<String> = request.name_filter.and_then(|v| match v {
        serde_json::Value::String(s) => Some(s),
        serde_json::Value::Object(ref obj) => obj
            .get("value")
            .and_then(|val| val.as_str().map(|s| s.to_owned())),
        _ => None,
    });
    let id_filter: Option<u64> = request.id_filter.and_then(|v| match v {
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::Object(ref obj) => obj.get("value").and_then(|val| match val {
            serde_json::Value::String(s) => s.parse::<u64>().ok(),
            serde_json::Value::Number(n) => n.as_u64(),
            _ => None,
        }),
        _ => None,
    });

    let artifacts: Vec<serde_json::Value> = inner
        .artifact_v2_registry
        .values()
        .filter(|e| {
            e.workflow_run_backend_id == request.workflow_run_backend_id
                && e.workflow_job_run_backend_id == request.workflow_job_run_backend_id
        })
        .filter(|e| name_filter.as_deref().map(|f| e.name == f).unwrap_or(true))
        .filter(|e| id_filter.map(|id| e.id == id).unwrap_or(true))
        .map(|e| {
            json!({
                "workflow_run_backend_id": e.workflow_run_backend_id,
                "workflow_job_run_backend_id": e.workflow_job_run_backend_id,
                "database_id": e.id.to_string(),
                "name": e.name,
                "size": e.size.to_string(),
                "created_at": e.created_at,
                "digest": e.digest.as_deref().unwrap_or("")
            })
        })
        .collect();
    Json(json!({ "artifacts": artifacts }))
}

async fn twirp_artifact_v2_get_signed_url(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2GetSignedUrlRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let blob_token = {
        let inner = shared.state.inner.lock().await;
        inner
            .artifact_v2_registry
            .get(&registry_key)
            .map(|e| e.blob_token.clone())
    }
    .ok_or_else(|| ApiError::not_found("artifact not found"))?;

    // URL must end in .zip so the toolkit's streamExtract detects it as a zip.
    let signed_url = format!("{}/twirp-blob/artifact/{blob_token}.zip", public_base_url());
    Ok(Json(json!({ "signed_url": signed_url })))
}

async fn twirp_artifact_v2_delete(
    State(shared): State<Arc<SharedState>>,
    Json(request): Json<ArtifactV2DeleteRequest>,
) -> Json<serde_json::Value> {
    let registry_key = artifact_v2_registry_key(
        &request.workflow_run_backend_id,
        &request.workflow_job_run_backend_id,
        &request.name,
    );
    let removed = {
        let mut inner = shared.state.inner.lock().await;
        inner.artifact_v2_registry.remove(&registry_key)
    };
    if let Some(e) = removed {
        let _ = save_artifact_v2_registry(&shared).await;
        let blob_dir = shared
            .state
            .state_dir
            .join("blobs")
            .join("artifact")
            .join(&e.blob_token);
        let _ = tokio::fs::remove_dir_all(blob_dir).await;
        Json(json!({ "ok": true, "artifact_id": e.id.to_string() }))
    } else {
        Json(json!({ "ok": false, "artifact_id": "0" }))
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

// ─── Phase E: Timeline, logs, completion ────────────────────────────────────

/// PATCH timeline records — runner updates step/job state.
async fn patch_timeline_records(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, timeline_id)): Path<(String, String, String, String)>,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    let records = wrapper.value;
    let count = records.len();
    let callback_job = {
        let inner = shared.state.inner.lock().await;
        resolve_callback_job(&inner, &plan_id, timeline_id.parse().ok(), None)
    };
    let run_id = callback_job
        .as_ref()
        .map(|(_, run_id, _)| *run_id)
        .or_else(|| plan_id.parse::<RunId>().ok());
    let logical_job_id = callback_job.as_ref().map(|(_, _, job_id)| job_id.clone());
    let mut projected = Vec::new();
    for record in &records {
        if let Some(state) = &record.state {
            info!(
                timeline_id = %timeline_id,
                record_id = %record.id,
                name = record.display_name.as_deref().unwrap_or(""),
                state = ?state,
                "timeline record update"
            );
        }
        if let (Some(run_id), Some(status)) = (run_id, timeline_status(record)) {
            projected.push(NdjsonEvent::JobStatus {
                run_id,
                job_id: logical_job_id
                    .clone()
                    .unwrap_or_else(|| JobId(record.id.to_string())),
                status,
                reason: None,
            });
        }
        if let Some(run_id) = run_id {
            for issue in &record.issues {
                projected.push(NdjsonEvent::Annotation {
                    run_id,
                    job_id: logical_job_id
                        .clone()
                        .unwrap_or_else(|| JobId(record.id.to_string())),
                    level: issue_level(issue.issue_type),
                    message: issue.message.clone().unwrap_or_default(),
                    file: issue.data.get("file").cloned(),
                    line: issue.data.get("line").and_then(|line| line.parse().ok()),
                });
            }
        }
    }
    if let Some(run_id) = run_id {
        let mut inner = shared.state.inner.lock().await;
        inner
            .timeline_events
            .entry(run_id)
            .or_default()
            .extend(projected.clone());

        if let Some(job_id) = logical_job_id {
            if let Some(run) = inner.runs.get_mut(&run_id) {
                let job_name = job_id.0.clone();
                let job_detail =
                    if let Some(pos) = run.jobs_list.iter().position(|j| j.name == job_name) {
                        &mut run.jobs_list[pos]
                    } else {
                        run.jobs_list.push(JobDetail {
                            name: job_name,
                            conclusion: "success".to_owned(),
                            steps: Vec::new(),
                        });
                        run.jobs_list.last_mut().unwrap()
                    };

                if let Some(status) = run.jobs.get(&job_id) {
                    job_detail.conclusion = format!("{:?}", status).to_lowercase();
                }

                for record in &records {
                    let Some(name) = &record.display_name else {
                        continue;
                    };
                    if record.id.to_string() == job_id.0 {
                        continue;
                    }

                    let conclusion_str = match record.result {
                        Some(
                            azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues,
                        ) => "success",
                        Some(azdo::TaskResult::Failed) => {
                            if run.jobs.get(&job_id) == Some(&ExecutionStatus::Cancelled) {
                                "cancelled"
                            } else {
                                "failure"
                            }
                        }
                        Some(azdo::TaskResult::Cancelled) => "cancelled",
                        Some(azdo::TaskResult::Skipped) => "skipped",
                        None if record.state == Some(azdo::TimelineRecordState::InProgress) => {
                            "in_progress"
                        }
                        _ => "success",
                    };

                    if let Some(pos) = job_detail.steps.iter().position(|s| s.name == *name) {
                        job_detail.steps[pos].conclusion = conclusion_str.to_owned();
                    } else {
                        job_detail.steps.push(StepRecord {
                            name: name.clone(),
                            conclusion: conclusion_str.to_owned(),
                        });
                    }
                }
            }
        }
    }
    for event in projected {
        shared.state.emit(event).await;
    }
    Json(json!({ "count": count, "value": records }))
}
fn timeline_status(record: &azdo::TimelineRecord) -> Option<ExecutionStatus> {
    match record.result {
        Some(azdo::TaskResult::Succeeded | azdo::TaskResult::SucceededWithIssues) => {
            Some(ExecutionStatus::Success)
        }
        Some(azdo::TaskResult::Failed) => Some(ExecutionStatus::Failure),
        Some(azdo::TaskResult::Cancelled) => Some(ExecutionStatus::Cancelled),
        Some(azdo::TaskResult::Skipped) => Some(ExecutionStatus::Skipped),
        None if record.state == Some(azdo::TimelineRecordState::InProgress) => {
            Some(ExecutionStatus::InProgress)
        }
        _ => None,
    }
}

fn issue_level(issue_type: azdo::IssueType) -> AnnotationLevel {
    match issue_type {
        azdo::IssueType::Error => AnnotationLevel::Error,
        azdo::IssueType::Warning => AnnotationLevel::Warning,
        azdo::IssueType::Info => AnnotationLevel::Notice,
    }
}

/// POST create log file — runner creates a log container.
async fn create_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id)): Path<(String, String, String)>,
    Json(mut log): Json<azdo::TaskLog>,
) -> Json<serde_json::Value> {
    let mut inner = shared.state.inner.lock().await;
    let next_id = inner.next_log_id;
    inner.next_log_id = next_id.wrapping_add(1);
    log.id = next_id as i64;
    let key = format!("{}/{}", plan_id, next_id);
    inner.logs.entry(key).or_default();
    Json(serde_json::to_value(&log).unwrap_or(json!({ "ok": true })))
}

/// POST append log — runner appends lines to a log file.
async fn append_log(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id, log_id)): Path<(String, String, String, String)>,
    body: Bytes,
) -> StatusCode {
    let key = log_key(&plan_id, &log_id);
    let mut inner = shared.state.inner.lock().await;
    let masked = mask_log_bytes(&inner, &plan_id, &body);
    inner
        .logs
        .entry(key)
        .or_default()
        .extend_from_slice(&masked);
    StatusCode::ACCEPTED
}

fn log_key(plan_id: &str, log_id: &str) -> String {
    format!("{plan_id}/{log_id}")
}

fn mask_log_bytes(inner: &InnerState, plan_id: &str, body: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(body);
    let resolved_run_id = resolve_callback_job(inner, plan_id, None, None)
        .map(|(_, run_id, _)| run_id)
        .or_else(|| plan_id.parse::<RunId>().ok());
    let run_secrets: Vec<String> = resolved_run_id
        .and_then(|run_id| inner.runs.get(&run_id))
        .map(|run| {
            run.submission
                .secrets
                .values()
                .map(|s| s.expose().to_owned())
                .collect()
        })
        .unwrap_or_else(|| {
            inner
                .runs
                .values()
                .flat_map(|run| run.submission.secrets.values())
                .map(|s| s.expose().to_owned())
                .collect()
        });

    aksh_gha_protocol::masking::mask_secrets(&text, run_secrets.iter().map(String::as_str), &[])
        .into_bytes()
}

/// POST console log — runner streams live console output.
async fn console_log(
    State(_shared): State<Arc<SharedState>>,
    Path((_scope, _hub, _plan_id, _timeline_id, _record_id)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    _body: Bytes,
) -> StatusCode {
    StatusCode::OK
}

/// POST finish job — runner reports final result + outputs.
async fn finish_job(
    State(shared): State<Arc<SharedState>>,
    Path((_scope, _hub, plan_id)): Path<(String, String, String)>,
    Json(event): Json<azdo::JobCompletedEvent>,
) -> Json<serde_json::Value> {
    let status = task_result_status(event.result);
    let outputs = event
        .outputs
        .iter()
        .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
        .collect();
    let completion = {
        let mut inner = shared.state.inner.lock().await;
        let callback_resolved = resolve_callback_job(
            &inner,
            &plan_id,
            Some(event.timeline_id),
            Some(event.job_id),
        );
        let active_resolved =
            sole_active_unfinished_request(&inner).and_then(|id| job_request_tuple(&inner, id));
        let resolved = callback_resolved.or(active_resolved).or_else(|| {
            plan_id
                .parse::<RunId>()
                .ok()
                .map(|run_id| (0, run_id, JobId(event.job_id.to_string())))
        });
        if let Some((request_id, run_id, job_id)) = resolved {
            if let Some(request) = inner.job_requests.get_mut(&request_id) {
                request.result = Some(status);
                request.locked_until = agent_request_locked_until();
            }
            Some(JobCompletion {
                run_id,
                job_id,
                status,
                outputs,
            })
        } else {
            None
        }
    };

    info!(
        job_id = %event.job_id,
        result = ?event.result,
        outputs = ?event.outputs,
        "job completed"
    );

    if let Some(completion) = completion {
        let _ = complete_job_inner(shared, completion).await;
    } else {
        warn!(
            plan_id,
            job_id = %event.job_id,
            timeline_id = %event.timeline_id,
            "finish_job could not resolve callback to a run/job"
        );
    }

    Json(json!({ "ok": true }))
}

// ── F030: standard AzDO `/_apis/v1/plans/` route handlers ────────────────────
// These use the URL pattern our AzDO client sends (`plans/{planId}/...`) rather
// than the scoped pattern (`Timeline/{scope}/{hub}/{planId}/{timelineId}`).
// The logic is identical to the existing handlers above.

/// PATCH `/_apis/v1/plans/:plan_id/timelines/:timeline_id/records`
async fn patch_timeline_records_plan(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, timeline_id)): Path<(String, String)>,
    Json(wrapper): Json<azdo::VssJsonCollectionWrapper<azdo::TimelineRecord>>,
) -> Json<serde_json::Value> {
    patch_timeline_records(
        State(shared),
        Path((String::new(), String::new(), plan_id, timeline_id)),
        Json(wrapper),
    )
    .await
}

/// POST `/_apis/v1/plans/:plan_id/logs`
async fn create_log_plan(
    State(shared): State<Arc<SharedState>>,
    Path(plan_id): Path<String>,
    Json(log): Json<azdo::TaskLog>,
) -> Json<serde_json::Value> {
    create_log(
        State(shared),
        Path((String::new(), String::new(), plan_id)),
        Json(log),
    )
    .await
}

/// PUT `/_apis/v1/plans/:plan_id/logs/:log_id`
async fn append_log_plan(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, log_id)): Path<(String, String)>,
    body: Bytes,
) -> StatusCode {
    append_log(
        State(shared),
        Path((String::new(), String::new(), plan_id, log_id)),
        body,
    )
    .await
}

/// POST `/_apis/v1/plans/:plan_id/events`
///
/// Handles the `JobCompleted` event sent by the runner's AzDO reporting path.
/// The body shape is `{name, jobId, requestId, result, outputs}` — slightly
/// different from the scoped `finish_job` path which uses `JobCompletedEvent`.
async fn finish_job_plan(
    State(shared): State<Arc<SharedState>>,
    Path(plan_id): Path<String>,
    Json(event): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let result_str = event
        .get("result")
        .and_then(|v| v.as_str())
        .unwrap_or("failed");
    let status =
        execution_status_from_runner_result(result_str).unwrap_or(ExecutionStatus::Failure);
    let job_id_str = event.get("jobId").and_then(|v| v.as_str()).unwrap_or("");
    let outputs = event
        .get("outputs")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                .collect::<std::collections::HashMap<_, _>>()
        })
        .unwrap_or_default();
    let outputs: aksh_gha_protocol::OutputMap = outputs
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect();

    info!(
        plan_id,
        job_id = job_id_str,
        result = result_str,
        "finish_job_plan"
    );

    let completion = {
        let mut inner = shared.state.inner.lock().await;
        let resolved = resolve_callback_job(&inner, &plan_id, None, None).or_else(|| {
            sole_active_unfinished_request(&inner).and_then(|id| job_request_tuple(&inner, id))
        });
        if let Some((request_id, run_id, job_id)) = resolved {
            if let Some(request) = inner.job_requests.get_mut(&request_id) {
                request.result = Some(status);
                request.locked_until = agent_request_locked_until();
            }
            Some(JobCompletion {
                run_id,
                job_id,
                status,
                outputs,
            })
        } else {
            warn!(plan_id, "finish_job_plan: could not resolve run/job");
            None
        }
    };
    if let Some(c) = completion {
        let _ = complete_job_inner(shared, c).await;
    }
    Json(json!({ "ok": true }))
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

/// Fetch a remote reusable workflow YAML from GitHub.
/// `uses` format: `owner/repo/path/.github/workflows/workflow.yml@ref`
async fn fetch_remote_workflow(uses: &str) -> Result<String, anyhow::Error> {
    let parts: Vec<&str> = uses.split('@').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("invalid uses format: {uses}"));
    }
    let path_part = parts[0];
    let git_ref = parts[1];
    let segments: Vec<&str> = path_part.splitn(3, '/').collect();
    if segments.len() < 3 {
        return Err(anyhow::anyhow!("invalid uses path: {uses}"));
    }
    let owner = segments[0];
    let repo = segments[1];
    let path = segments[2];
    let url = format!(
        "https://raw.githubusercontent.com/{}/{}/{}/{}",
        owner, repo, git_ref, path
    );
    let client = reqwest::Client::new();
    let resp = client.get(&url).send().await?.error_for_status()?;
    Ok(resp.text().await?)
}

/// Resolve a git ref (branch/tag) to a commit SHA via the GitHub API.
async fn resolve_remote_sha(owner: &str, repo: &str, git_ref: &str) -> Option<String> {
    let url = format!(
        "https://api.github.com/repos/{}/{}/git/ref/{}",
        owner, repo, git_ref
    );
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("User-Agent", "aksh-runner-server")
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        // Try tags endpoint if heads fails
        let url = format!(
            "https://api.github.com/repos/{}/{}/git/ref/tags/{}",
            owner, repo, git_ref
        );
        let resp = client
            .get(&url)
            .header("User-Agent", "aksh-runner-server")
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let json: serde_json::Value = resp.json().await.ok()?;
        return json
            .get("object")
            .and_then(|o| o.get("sha"))
            .and_then(|s| s.as_str())
            .map(String::from);
    }
    let json: serde_json::Value = resp.json().await.ok()?;
    json.get("object")
        .and_then(|o| o.get("sha"))
        .and_then(|s| s.as_str())
        .map(String::from)
}

async fn resolve_all_reusable_workflows(
    workflow: &aksh_gha_parser::Workflow,
    reusable_workflows: &mut BTreeMap<String, String>,
    reusable_shas: &mut BTreeMap<String, String>,
    depth: usize,
) -> Result<(), ApiError> {
    if depth >= 4 {
        return Ok(());
    }
    for job in workflow.jobs.values() {
        if let Some(uses) = &job.uses {
            if !uses.starts_with("./") && !uses.starts_with(".github/") {
                if !reusable_workflows.contains_key(uses) {
                    let text = fetch_remote_workflow(uses).await.map_err(|e| {
                        ApiError::bad_request(format!(
                            "failed to fetch remote workflow `{}`: {}",
                            uses, e
                        ))
                    })?;
                    reusable_workflows.insert(uses.clone(), text.clone());
                    if let Ok(called) = parse_workflow(&text) {
                        Box::pin(resolve_all_reusable_workflows(
                            &called,
                            reusable_workflows,
                            reusable_shas,
                            depth + 1,
                        ))
                        .await?;
                    }
                }
                if !reusable_shas.contains_key(uses) {
                    let parts: Vec<&str> = uses.split('@').collect();
                    if parts.len() == 2 {
                        let path_part = parts[0];
                        let git_ref = parts[1];
                        let path_segments: Vec<&str> = path_part.splitn(3, '/').collect();
                        if path_segments.len() == 3 {
                            let owner = path_segments[0];
                            let repo = path_segments[1];
                            if let Some(sha) = resolve_remote_sha(owner, repo, git_ref).await {
                                reusable_shas.insert(uses.clone(), sha);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn propagate_reusable_outputs(run: &mut RunRecord) {
    let mut outputs_to_add = Vec::new();
    for (caller_job_id, call) in &run.reusable_calls {
        let caller_job_id_typed = JobId(caller_job_id.clone());
        if run.job_outputs.contains_key(&caller_job_id_typed) {
            continue;
        }

        // Check if all inner jobs are complete
        let all_complete = !call.inner_job_ids.is_empty()
            && call.inner_job_ids.iter().all(|id| {
                run.jobs.get(&JobId(id.clone())).is_some_and(|status| {
                    matches!(
                        status,
                        ExecutionStatus::Success
                            | ExecutionStatus::Failure
                            | ExecutionStatus::Skipped
                            | ExecutionStatus::Cancelled
                    )
                })
            });

        if all_complete {
            // Build expression context
            let mut jobs_map = serde_json::Map::new();
            for inner_id in &call.inner_job_ids {
                let prefix = format!("{}/", caller_job_id);
                let inner_id_without_prefix = if inner_id.starts_with(&prefix) {
                    &inner_id[prefix.len()..]
                } else {
                    inner_id
                };

                let mut job_outputs_map = serde_json::Map::new();
                if let Some(outputs) = run.job_outputs.get(&JobId(inner_id.clone())) {
                    for (k, v) in outputs {
                        job_outputs_map.insert(k.clone(), v.clone());
                    }
                }

                let mut job_record = serde_json::Map::new();
                job_record.insert(
                    "outputs".to_owned(),
                    serde_json::Value::Object(job_outputs_map),
                );
                jobs_map.insert(
                    inner_id_without_prefix.to_owned(),
                    serde_json::Value::Object(job_record),
                );
            }

            let mut context = aksh_gha_expressions::Context::default();
            context.insert("jobs", serde_json::Value::Object(jobs_map));

            let mut inputs_map = serde_json::Map::new();
            for (k, v) in &call.inputs {
                inputs_map.insert(k.clone(), v.clone());
            }
            context.insert("inputs", serde_json::Value::Object(inputs_map));

            let mut caller_outputs = BTreeMap::new();
            for (name, expr) in &call.output_definitions {
                let resolved = aksh_gha_parser::eval::resolve_string(expr, &context)
                    .unwrap_or_else(|_| expr.clone());
                let val =
                    serde_json::from_str(&resolved).unwrap_or(serde_json::Value::String(resolved));
                caller_outputs.insert(name.clone(), val);
            }

            outputs_to_add.push((caller_job_id_typed, caller_outputs));
        }
    }

    for (job_id, outputs) in outputs_to_add {
        run.job_outputs.insert(job_id, outputs);
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
