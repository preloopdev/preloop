//! Gollum (wiki) event adapter.
//!
//! Reference: MessageController.cs:6287 (* default case)
//! ref = default branch, collects pages[].page_name into payload.paths.

use crate::events::trust_tier::TrustTier;
use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "gollum"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let default_branch = payload
            .get("repository")
            .and_then(|r| r.get("default_branch"))
            .and_then(|v| v.as_str())
            .unwrap_or("main");

        let activity_type = payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());

        // Collect wiki page names into payload.paths so path filters work
        let mut payload = payload.clone();
        if let Some(pages) = payload.get("pages").and_then(|v| v.as_array()) {
            let page_names: Vec<Value> = pages
                .iter()
                .filter_map(|p| {
                    p.get("page_name")
                        .and_then(|v| v.as_str())
                        .map(|s| Value::String(s.to_owned()))
                })
                .collect();
            if !page_names.is_empty() {
                payload["paths"] = Value::Array(page_names);
            }
        }

        vec![EffectiveEvent {
            event: "gollum".to_owned(),
            git_ref: format!("refs/heads/{default_branch}"),
            sha: None,
            status_check_sha: None,
            activity_type,
            trust_tier: Some(TrustTier::Untrusted),
            skip: false,
            payload,
            upstream_workflow_names: vec![],
        }]
    }
}
