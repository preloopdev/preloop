//! Workflow dispatch event adapter.
//!
//! Reference: MessageController.cs:1022-1180 (input defaults + validation)
//! Falls into the * default case at line 6287.
//! ref = selected branch/tag when supplied, otherwise default branch.
//! Mutates payload.inputs to apply type-specific defaults in submission handling.
use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "workflow_dispatch"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let payload = payload.clone();

        let selected_ref = payload
            .get("ref")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| {
                payload
                    .get("repository")
                    .and_then(|r| r.get("default_branch"))
                    .and_then(Value::as_str)
                    .unwrap_or("main")
            });
        let ref_type = payload.get("ref_type").and_then(Value::as_str);
        let git_ref = if selected_ref.starts_with("refs/") {
            selected_ref.to_owned()
        } else if ref_type == Some("tag") {
            format!("refs/tags/{selected_ref}")
        } else {
            format!("refs/heads/{selected_ref}")
        };

        vec![EffectiveEvent {
            event: "workflow_dispatch".to_owned(),
            git_ref,
            sha: None,
            status_check_sha: None,
            activity_type: None,
            trust_tier: Some(TrustTier::AdminManual),
            skip: false,
            payload,
            upstream_workflow_names: vec![],
        }]
    }
}
