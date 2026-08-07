//! Delete event adapter.
//!
//! Reference: MessageController.cs:6287 (* default case)
//! ref = default branch, activity = ref_type or action.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "delete"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let default_branch = payload
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(|v| v.as_str())
            .unwrap_or("main");

        let activity_type = payload
            .get("ref_type")
            .and_then(|v| v.as_str())
            .or_else(|| payload.get("action").and_then(|v| v.as_str()))
            .map(|s| s.to_owned());

        vec![EffectiveEvent {
            event: "delete".to_owned(),
            git_ref: format!("refs/heads/{default_branch}"),
            sha: None,
            status_check_sha: None,
            activity_type,
            trust_tier: Some(TrustTier::Untrusted),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}
