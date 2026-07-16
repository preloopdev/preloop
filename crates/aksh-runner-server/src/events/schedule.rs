//! Schedule event adapter.
//!
//! Only fires from the internal cron executor, never from a webhook.
//! Reference: MessageController.cs:882-927 (registration) and line 123 (firing).
//! ref = default branch, sha = head of default branch.

use crate::events::{EffectiveEvent, EventAdapter};
use serde_json::Value;

/// Event adapter.
pub struct Adapter;

impl EventAdapter for Adapter {
    fn event_name(&self) -> &'static str {
        "schedule"
    }

    fn project(&self, _payload: &Value) -> Vec<EffectiveEvent> {
        // The internal cron executor (scheduler.rs) builds EffectiveEvent
        // directly and never goes through this adapter. If we reach here,
        // it's an external webhook claiming to be a schedule event — reject it
        // to prevent untrusted sources from getting Schedule trust tier.
        tracing::warn!("Ignoring external schedule webhook (schedule events are internal-only)");
        vec![]
    }
}
