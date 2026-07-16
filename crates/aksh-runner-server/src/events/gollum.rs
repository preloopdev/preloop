//! Gollum (wiki) event adapter.
//!
//! Reference: MessageController.cs:6287 (* default case)
//! ref = default branch, collects pages[].page_name into payload.paths.

use crate::events::{make_default_branch_events, EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "gollum"
    }

    fn project(&self, payload: &Value) -> Vec<EffectiveEvent> {
        let mut events = make_default_branch_events("gollum", payload);
        // Collect wiki page names into payload.paths so path filters work
        if let Some(event) = events.first_mut() {
            if let Some(pages) = event.payload.get("pages").and_then(|v| v.as_array()) {
                let page_names: Vec<Value> = pages
                    .iter()
                    .filter_map(|p| {
                        p.get("page_name")
                            .and_then(|v| v.as_str())
                            .map(|s| Value::String(s.to_owned()))
                    })
                    .collect();
                if !page_names.is_empty() {
                    event.payload["paths"] = Value::Array(page_names);
                }
            }
        }
        events
    }
}
