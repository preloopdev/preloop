//! Release event adapter.
//!
//! Reference: MessageController.cs:6281-6283
//! ref = refs/tags/{release.tag_name}, sha = null.
//! activity = payload.action.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "release"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let tag_name = payload
            .get("release")
            .and_then(|r| r.get("tag_name"))
            .and_then(|v| v.as_str());

        let tag_name = match tag_name {
            Some(t) if !t.is_empty() => t,
            _ => return vec![],
        };

        let activity_type = payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());

        vec![EffectiveEvent {
            event: "release".to_owned(),
            git_ref: format!("refs/tags/{tag_name}"),
            sha: None,
            status_check_sha: None,
            activity_type,
            trust_tier: Some(TrustTier::Deployment),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}
