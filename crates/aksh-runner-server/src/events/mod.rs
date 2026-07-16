//! Per-event webhook adapters.
//!
//! Each adapter computes the effective (event, ref, sha, status_check_sha)
//! tuple from a raw webhook payload, mirroring the dispatch table in
//! runner.server's MessageController.cs::ExecuteWebhook (lines 6250-6325).

pub mod pull_request;
pub mod pull_request_review;
pub mod pull_request_target;
pub mod push;
pub mod schedule;
pub mod trust_tier;
pub mod workflow_dispatch;
pub mod workflow_run;

// Tier B — issue/PR/social events
pub mod discussion;
pub mod discussion_comment;
pub mod issue_comment;
pub mod issues;
pub mod label;
pub mod milestone;

// Tier C — release/admin/fork/wiki/deployment events
pub mod create;
pub mod delete;
pub mod deployment;
pub mod deployment_status;
pub mod fork;
pub mod gollum;
pub mod member;
pub mod page_build;
pub mod public;
pub mod release;
pub mod repository_dispatch;
pub mod watch;

use serde_json::Value;

/// Projected event after adapter processing.
#[derive(Debug, Clone)]
pub struct EffectiveEvent {
    /// The effective event name (may differ from incoming, e.g.
    /// `pull_request` webhook emits `pull_request_target` AND
    /// `pull_request`).
    pub event: String,
    /// Git ref for the run.
    pub git_ref: String,
    /// SHA for checkout and check-run reporting.
    pub sha: Option<String>,
    /// SHA for status checks.
    pub status_check_sha: Option<String>,
    /// Activity type (e.g. "opened", "synchronize").
    pub activity_type: Option<String>,
    /// Trust tier classification.
    pub trust_tier: Option<trust_tier::TrustTier>,
    /// Whether this event should be skipped (e.g. [skip ci]).
    pub skip: bool,
    /// The raw payload (mutated by some adapters like workflow_dispatch).
    pub payload: Value,
    /// Upstream workflow display names for `workflow_run.workflows:` filtering.
    pub upstream_workflow_names: Vec<String>,
}

/// An adapter that projects a raw webhook payload into one or more
/// `EffectiveEvent`s.
pub trait EventAdapter: Send + Sync {
    /// The event name this adapter handles (matches `X-GitHub-Event` header).
    fn event_name(&self) -> &'static str;

    /// Project the raw payload into effective events. May return zero
    /// (e.g. [skip ci] or fork-gated), one, or multiple (pull_request
    /// fan-out).
    fn project(&self, payload: &Value) -> Vec<EffectiveEvent>;
}

/// Registry of all event adapters, indexed by event name.
pub fn adapter_for(event_name: &str) -> Option<&'static dyn EventAdapter> {
    match event_name {
        "push" => Some(&push::Adapter),
        "pull_request" => Some(&pull_request::Adapter),
        "pull_request_target" => Some(&pull_request_target::Adapter),
        "pull_request_review" => Some(&pull_request_review::Adapter),
        "workflow_dispatch" => Some(&workflow_dispatch::Adapter),
        "workflow_run" => Some(&workflow_run::Adapter),
        "repository_dispatch" => Some(&repository_dispatch::Adapter),
        "create" => Some(&create::Adapter),
        "delete" => Some(&delete::Adapter),
        "release" => Some(&release::Adapter),
        "issues" => Some(&issues::Adapter),
        "issue_comment" => Some(&issue_comment::Adapter),
        "discussion" => Some(&discussion::Adapter),
        "discussion_comment" => Some(&discussion_comment::Adapter),
        "label" => Some(&label::Adapter),
        "milestone" => Some(&milestone::Adapter),
        "watch" => Some(&watch::Adapter),
        "fork" => Some(&fork::Adapter),
        "deployment" => Some(&deployment::Adapter),
        "deployment_status" => Some(&deployment_status::Adapter),
        "member" => Some(&member::Adapter),
        "public" => Some(&public::Adapter),
        "gollum" => Some(&gollum::Adapter),
        "page_build" => Some(&page_build::Adapter),
        "schedule" => Some(&schedule::Adapter),
        _ => None,
    }
}

/// All supported event names.
pub fn all_event_names() -> &'static [&'static str] {
    &[
        "push",
        "pull_request",
        "pull_request_target",
        "pull_request_review",
        "workflow_dispatch",
        "workflow_run",
        "repository_dispatch",
        "create",
        "delete",
        "release",
        "issues",
        "issue_comment",
        "discussion",
        "discussion_comment",
        "label",
        "milestone",
        "watch",
        "fork",
        "deployment",
        "deployment_status",
        "member",
        "public",
        "gollum",
        "page_build",
        "schedule",
    ]
}

/// Build `EffectiveEvent` vec for events that use the repository default
/// branch as the ref and `payload.action` as the activity type.
/// Mirrors MessageController.cs:6287 (* default case).
pub(crate) fn make_default_branch_events(event_name: &str, payload: &Value) -> Vec<EffectiveEvent> {
    let default_branch = payload
        .get("repository")
        .and_then(|r| r.get("default_branch"))
        .and_then(|v| v.as_str())
        .unwrap_or("main");

    let activity_type = payload
        .get("action")
        .and_then(|v| v.as_str())
        .map(|s| s.to_owned());

    vec![EffectiveEvent {
        event: event_name.to_owned(),
        git_ref: format!("refs/heads/{default_branch}"),
        sha: None,
        status_check_sha: None,
        activity_type,
        trust_tier: Some(trust_tier::TrustTier::Untrusted),
        skip: false,
        payload: payload.clone(),
        upstream_workflow_names: vec![],
    }]
}

#[cfg(test)]
#[path = "property_tests.rs"]
mod property_tests;
