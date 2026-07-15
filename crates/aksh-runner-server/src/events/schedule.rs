//! Schedule event adapter.
//!
//! Only fires from the internal cron executor, never from a webhook.
//! Reference: MessageController.cs:882-927 (registration) and line 123 (firing).
//! ref = default branch, sha = head of default branch.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "schedule"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let default_branch = payload
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(|v| v.as_str())
            .unwrap_or("main");

        vec![EffectiveEvent {
            event: "schedule".to_owned(),
            git_ref: format!("refs/heads/{default_branch}"),
            sha: None,
            status_check_sha: None,
            activity_type: Some("schedule".to_owned()),
            trust_tier: Some(TrustTier::Schedule),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}
