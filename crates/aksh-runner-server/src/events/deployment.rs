//! Deployment event adapter.
//!
//! Reference: MessageController.cs:6287 (* default case)
//! ref = default branch, activity = payload.action.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "deployment"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let deployment = payload.get("deployment");
        let git_ref = deployment
            .and_then(|value| value.get("ref"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| {
                if value.starts_with("refs/") {
                    value.to_owned()
                } else {
                    // GitHub deployment.ref is a branch, tag, or SHA.
                    // Without ref_type metadata, default to refs/heads/ (branch).
                    format!("refs/heads/{value}")
                }
            })
            .unwrap_or_else(|| {
                payload
                    .get("repository")
                    .and_then(|r| r.get("default_branch"))
                    .and_then(Value::as_str)
                    .map(|branch| format!("refs/heads/{branch}"))
                    .unwrap_or_else(|| "refs/heads/main".to_owned())
            });
        let sha = deployment
            .and_then(|value| value.get("sha"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let activity_type = payload
            .get("action")
            .and_then(Value::as_str)
            .map(str::to_owned);
        vec![EffectiveEvent {
            event: "deployment".to_owned(),
            git_ref,
            sha,
            status_check_sha: None,
            activity_type,
            trust_tier: Some(TrustTier::Deployment),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}
