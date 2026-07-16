//! Workflow run event adapter.
//!
//! Reference: MessageController.cs:6287 (default-branch dispatch).
//! The downstream workflow definition, `GITHUB_REF`, and `GITHUB_SHA` use the
//! repository default branch. The upstream run remains available only in the
//! event payload.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "workflow_run"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let workflow_run = payload.get("workflow_run");

        let default_branch = payload
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(|v| v.as_str())
            .unwrap_or("main");

        let activity_type = payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(str::to_owned);

        let upstream_workflow_names = workflow_run
            .and_then(|wr| wr.get("name"))
            .and_then(|v| v.as_str())
            .map(|name| vec![name.to_owned()])
            .unwrap_or_default();

        vec![EffectiveEvent {
            event: "workflow_run".to_owned(),
            git_ref: format!("refs/heads/{default_branch}"),
            sha: None,
            status_check_sha: None,
            activity_type,
            trust_tier: Some(TrustTier::Internal),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names,
        }]
    }
}
