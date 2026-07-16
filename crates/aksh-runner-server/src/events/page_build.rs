//! Page build event adapter.
//!
//! Reference: MessageController.cs:6287 (* default case)
//! ref = default branch, activity = payload.action.

use crate::events::make_default_branch_events;

/// Event adapter.
pub struct Adapter;

impl crate::events::EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "page_build"
    }

    fn project(&self, payload: &serde_json::Value) -> Vec<crate::events::EffectiveEvent> {
        make_default_branch_events("page_build", payload)
    }
}
