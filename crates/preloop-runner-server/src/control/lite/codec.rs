//! Column codecs: timestamps are INTEGER microseconds since the Unix epoch,
//! uuids are TEXT, booleans 0/1, JSON is TEXT.

use super::super::types::ControlError;
use preloop_gha_protocol::{JobId, RunId};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Wall clock in microseconds.
pub(super) fn now_us() -> i64 {
    system_to_us(SystemTime::now())
}

pub(super) fn system_to_us(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

pub(super) fn us_to_system(us: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(us.max(0) as u64)
}

pub(super) fn us_to_utc(us: i64) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp_micros(us)
}

/// The runner-protocol lease string (`locked_until`) of a lease expiry.
pub(super) fn lease_string(expires_at_us: Option<i64>) -> String {
    expires_at_us
        .map(|us| crate::recording::server_iso_at(us_to_system(us)))
        .unwrap_or_default()
}

/// Parse a runner-protocol lease string (`server_iso_at` form, or any
/// RFC 3339 timestamp) into microseconds.
pub(super) fn parse_lease(locked_until: &str) -> Result<i64, ControlError> {
    chrono::DateTime::parse_from_rfc3339(locked_until)
        .map(|t| t.timestamp_micros())
        .map_err(|error| {
            ControlError::BadRequest(format!("invalid lease timestamp {locked_until:?}: {error}"))
        })
}

pub(super) fn run_id(s: &str) -> RunId {
    RunId(s.parse().unwrap_or_default())
}

pub(super) fn run_key(run_id: RunId) -> String {
    run_id.0.to_string()
}

pub(super) fn job_id(s: String) -> JobId {
    JobId(s)
}

pub(super) fn uuid(s: &str) -> uuid::Uuid {
    s.parse().unwrap_or_default()
}

/// `plan_id` is derived, not stored: `plan_id = agent_job_id` (string form).
pub(super) fn plan_id(agent_job_id: &uuid::Uuid) -> String {
    agent_job_id.to_string()
}

/// `plan_type` is derived: always `"actions"`.
pub(super) const PLAN_TYPE: &str = "actions";

/// A plan id is an agent job id; anything that does not parse addresses
/// no attempt.
pub(super) fn plan_agent_job_id(plan_id: &str) -> Option<uuid::Uuid> {
    plan_id.parse().ok()
}
