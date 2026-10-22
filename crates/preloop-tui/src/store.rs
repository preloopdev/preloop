//! Typed slots for everything the TUI has loaded.
//!
//! Each slot is a [`Loadable`]: the last good data, when it was fetched, the
//! last error, and whether a fetch is in flight. A failed refresh keeps the old
//! data and records the error, so a flaky network does not blank the screen.

use std::collections::{BTreeMap, VecDeque};

use chrono::{DateTime, Utc};

use crate::api::{
    Annotation, DebugSession, LiveLogChunk, OperationalSnapshot, PoolBindings, RunRecord,
    RunnersList, WebhookDeliveries, WebhookHealth,
};
use crate::client::sandbox::SandboxSummary;

/// How many ready-queue samples to keep (30 minutes at one sample per 5 s).
pub const READY_HISTORY_LEN: usize = 360;

/// Data that may be missing, stale, or being refreshed.
#[derive(Debug, Clone, PartialEq)]
pub struct Loadable<T> {
    /// The last successfully loaded value.
    pub data: Option<T>,
    /// When `data` was loaded.
    pub fetched_at: Option<DateTime<Utc>>,
    /// The last error, cleared by the next success.
    pub error: Option<String>,
    /// True while a fetch is in flight.
    pub loading: bool,
}

impl<T> Default for Loadable<T> {
    fn default() -> Self {
        Self {
            data: None,
            fetched_at: None,
            error: None,
            loading: false,
        }
    }
}

impl<T> Loadable<T> {
    /// Mark a fetch as started. Returns false when one is already in flight,
    /// so callers do not stack requests.
    pub fn begin(&mut self) -> bool {
        if self.loading {
            return false;
        }
        self.loading = true;
        true
    }

    /// Record a successful fetch.
    pub fn succeed(&mut self, data: T, now: DateTime<Utc>) {
        self.data = Some(data);
        self.fetched_at = Some(now);
        self.error = None;
        self.loading = false;
    }

    /// Record a failed fetch. The previous data is kept.
    pub fn fail(&mut self, error: String) {
        self.error = Some(error);
        self.loading = false;
    }

    /// The loaded value, if any.
    pub fn get(&self) -> Option<&T> {
        self.data.as_ref()
    }
}

/// Live log lines for one step, keyed by 1-based line number within the step.
///
/// Keying by line number makes replays idempotent: the engine resends the
/// retained window on every (re)connect, and lines already present are
/// overwritten with identical text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepLog {
    /// Line number to text.
    pub lines: BTreeMap<u64, String>,
}

impl StepLog {
    /// Place a chunk's lines at their stated positions.
    pub fn apply(&mut self, chunk: &LiveLogChunk) {
        for (offset, line) in chunk.value.iter().enumerate() {
            self.lines
                .insert(chunk.start_line + offset as u64, line.clone());
        }
    }

    /// True when no line has arrived.
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// `(line number, text)` pairs in order.
    pub fn numbered(&self) -> Vec<(u64, String)> {
        self.lines
            .iter()
            .map(|(number, text)| (*number, text.clone()))
            .collect()
    }
}

/// Identifies one fetched log: a job, or one step of it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogKey {
    /// Run id.
    pub run_id: String,
    /// Job id.
    pub job_id: String,
    /// 1-based step index (declared index plus one). `None` means the whole job.
    pub step: Option<usize>,
}

/// Everything loaded so far.
#[derive(Debug, Clone, Default)]
pub struct Store {
    /// Operational snapshot (`/api/v1/status`).
    pub status: Loadable<OperationalSnapshot>,
    /// Recent runs, newest first.
    pub runs: Loadable<Vec<RunRecord>>,
    /// Full run records, keyed by run id. Live events patch these in place.
    pub details: BTreeMap<String, Loadable<RunRecord>>,
    /// Logs fetched with `GET /logs`, for finished work.
    pub logs: BTreeMap<LogKey, Loadable<String>>,
    /// Live console lines, keyed by `(run id, job id)`, then step id.
    pub live: BTreeMap<(String, String), BTreeMap<String, StepLog>>,
    /// Live annotations, keyed by `(run id, job id)`.
    pub live_annotations: BTreeMap<(String, String), Vec<Annotation>>,
    /// Pool bindings (`/api/v1/pool/bindings`).
    pub bindings: Loadable<PoolBindings>,
    /// Registered runners.
    pub runners: Loadable<RunnersList>,
    /// Webhook deliveries.
    pub webhooks: Loadable<WebhookDeliveries>,
    /// Webhook queue and watchdog health.
    pub webhook_health: Loadable<WebhookHealth>,
    /// Debug sessions.
    pub debug: Loadable<Vec<DebugSession>>,
    /// Sandbox Service summary, when the context has a sandbox.
    pub sandbox: Loadable<SandboxSummary>,
    /// Ready-job counts over time, for the queue sparkline.
    pub ready_history: VecDeque<(DateTime<Utc>, u32)>,
}

impl Store {
    /// Append a ready-queue sample, dropping the oldest beyond the cap.
    pub fn push_ready(&mut self, at: DateTime<Utc>, ready: u32) {
        self.ready_history.push_back((at, ready));
        while self.ready_history.len() > READY_HISTORY_LEN {
            self.ready_history.pop_front();
        }
    }

    /// The run record for `run_id`, if one is loaded.
    pub fn run(&self, run_id: &str) -> Option<&RunRecord> {
        self.details.get(run_id)?.get()
    }

    /// Mutable run record, for applying live updates.
    pub fn run_mut(&mut self, run_id: &str) -> Option<&mut RunRecord> {
        self.details.get_mut(run_id)?.data.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::LiveLogChunk;

    fn chunk(start: u64, lines: &[&str]) -> LiveLogChunk {
        LiveLogChunk {
            step_id: "s".into(),
            start_line: start,
            count: lines.len(),
            value: lines.iter().map(|line| (*line).to_owned()).collect(),
        }
    }

    #[test]
    fn replayed_chunks_do_not_duplicate_lines() {
        let mut log = StepLog::default();
        log.apply(&chunk(1, &["a", "b"]));
        log.apply(&chunk(1, &["a", "b"]));
        log.apply(&chunk(3, &["c"]));
        assert_eq!(
            log.numbered(),
            vec![(1, "a".to_owned()), (2, "b".to_owned()), (3, "c".to_owned())]
        );
    }

    #[test]
    fn failed_refresh_keeps_previous_data() {
        let now = Utc::now();
        let mut slot: Loadable<u32> = Loadable::default();
        assert!(slot.begin());
        assert!(!slot.begin(), "no stacked requests");
        slot.succeed(7, now);
        assert!(slot.begin());
        slot.fail("boom".into());
        assert_eq!(slot.get(), Some(&7));
        assert_eq!(slot.error.as_deref(), Some("boom"));
        assert!(!slot.loading);
    }

    #[test]
    fn ready_history_is_capped() {
        let mut store = Store::default();
        for n in 0..(READY_HISTORY_LEN as u32 + 5) {
            store.push_ready(Utc::now(), n);
        }
        assert_eq!(store.ready_history.len(), READY_HISTORY_LEN);
        assert_eq!(store.ready_history.front().map(|s| s.1), Some(5));
    }
}
