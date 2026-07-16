//! Create event adapter.
//!
//! Reference: MessageController.cs:6278-6280
//! ref = refs/heads/{ref} or refs/tags/{ref} based on ref_type.
//! sha = null.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "create"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let ref_type = payload.get("ref_type").and_then(|v| v.as_str());

        let ref_name = payload.get("ref").and_then(|v| v.as_str());

        let (git_ref, trust_tier) = match (ref_type, ref_name) {
            (Some("branch"), Some(name)) if !name.is_empty() => {
                (format!("refs/heads/{name}"), TrustTier::Internal)
            }
            (Some("tag"), Some(name)) if !name.is_empty() => {
                (format!("refs/tags/{name}"), TrustTier::Deployment)
            }
            _ => return vec![],
        };

        vec![EffectiveEvent {
            event: "create".to_owned(),
            git_ref,
            sha: None,
            status_check_sha: None,
            activity_type: ref_type.map(|s| s.to_owned()),
            trust_tier: Some(trust_tier),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}
