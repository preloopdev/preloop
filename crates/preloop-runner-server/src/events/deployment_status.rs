//! Deployment status event adapter.
//!
//! Reference: MessageController.cs:6287 (* default case)
//! ref = default branch, activity = deployment_status.state.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "deployment_status"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let state = payload
            .get("deployment_status")
            .and_then(|value| value.get("state"))
            .and_then(Value::as_str);
        if state == Some("inactive") {
            return vec![];
        }
        let deployment = payload.get("deployment");
        let git_ref = deployment
            .and_then(|value| value.get("ref"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| {
                if value.starts_with("refs/") {
                    value.to_owned()
                } else if deployment
                    .and_then(|item| item.get("ref_type"))
                    .and_then(Value::as_str)
                    == Some("tag")
                {
                    format!("refs/tags/{value}")
                } else {
                    format!("refs/heads/{value}")
                }
            })
            .unwrap_or_else(|| {
                let default_branch = payload
                    .get("repository")
                    .and_then(|r| r.get("default_branch"))
                    .and_then(Value::as_str)
                    .unwrap_or("main");
                format!("refs/heads/{default_branch}")
            });
        let sha = deployment
            .and_then(|value| value.get("sha"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        vec![EffectiveEvent {
            event: "deployment_status".to_owned(),
            git_ref,
            sha,
            status_check_sha: None,
            activity_type: state
                .or_else(|| payload.get("action").and_then(Value::as_str))
                .map(str::to_owned),
            trust_tier: Some(TrustTier::Deployment),
            skip: false,
            payload: payload.clone(),
            upstream_workflow_names: vec![],
        }]
    }
}
