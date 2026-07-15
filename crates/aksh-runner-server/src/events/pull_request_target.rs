//! Pull request target standalone adapter.
//!
//! This adapter handles manual `pull_request_target` submissions (not the
//! fan-out from `pull_request` webhooks — that's in `pull_request.rs`).
//!
//! Reference: MessageController.cs:6260-6263

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "pull_request_target"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let default_branch = payload
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(|v| v.as_str())
            .unwrap_or("main");

        let sha = payload
            .get("pull_request")
            .and_then(|pr| pr.get("head"))
            .and_then(|h| h.get("sha"))
            .and_then(|v| v.as_str());

        let base_ref = payload
            .get("pull_request")
            .and_then(|pr| pr.get("base"))
            .and_then(|b| b.get("ref"))
            .and_then(|v| v.as_str())
            .unwrap_or(default_branch);

        let activity_type = payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());

        vec![EffectiveEvent {
            event: "pull_request_target".to_owned(),
            git_ref: format!("refs/heads/{base_ref}"),
            sha: sha.map(|s| s.to_owned()),
            status_check_sha: sha.map(|s| s.to_owned()),
            activity_type,
            trust_tier: Some(TrustTier::PullRequestTarget),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}
