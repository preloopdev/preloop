//! SQL fragments and row codecs shared by every pg command.
//!
//! The driver is built without its `uuid`/`serde_json`/`chrono` features
//! (enabling them changes `Cargo.lock`, a supply-chain policy file), so
//! values cross the wire as text / `int8` and the SQL casts them:
//! - timestamps are bound as `int8` microseconds since the epoch via
//!   [`ts!`] and read back as microseconds via [`us!`];
//! - uuids are bound as `$n::text::uuid` and read as `col::text`;
//! - jsonb is bound as `$n::text::jsonb` and read as `col::text`.
//!
//! The SQLite backend stores the same values natively (INTEGER µs, TEXT
//! uuid/JSON), so both backends see identical Rust-side values.

use super::super::types::ControlError;
use preloop_gha_protocol::{ExecutionStatus, JobId, RunId};

/// `timestamptz` column → `int8` microseconds since the epoch (NULL stays
/// NULL). Exact: `extract(epoch …)` is `numeric`.
macro_rules! us {
    ($col:literal) => {
        concat!("(extract(epoch from ", $col, ") * 1000000)::int8")
    };
}
pub(super) use us;

/// `int8` microseconds parameter → `timestamptz` (NULL stays NULL). Exact:
/// interval arithmetic, not a float division.
macro_rules! ts {
    ($param:literal) => {
        concat!(
            "(timestamptz 'epoch' + ",
            $param,
            "::int8 * interval '1 microsecond')"
        )
    };
}
pub(super) use ts;

/// Current wall-clock time in microseconds since the epoch.
pub(super) fn now_us() -> i64 {
    system_to_us(std::time::SystemTime::now())
}

pub(super) fn system_to_us(t: std::time::SystemTime) -> i64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

pub(super) fn us_to_system(us: i64) -> std::time::SystemTime {
    std::time::UNIX_EPOCH + std::time::Duration::from_micros(us.max(0) as u64)
}

pub(super) fn us_to_chrono(us: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_micros(us).unwrap_or_default()
}

/// Parse a uuid read back as `col::text`. The column type guarantees the
/// shape, so a failure is a backend invariant violation.
pub(super) fn uuid(text: &str) -> Result<uuid::Uuid, ControlError> {
    text.parse()
        .map_err(|error| ControlError::backend(anyhow::anyhow!("bad uuid {text:?}: {error}")))
}

pub(super) fn run_id(text: &str) -> Result<RunId, ControlError> {
    uuid(text).map(RunId)
}

pub(super) fn job_id(text: String) -> JobId {
    JobId(text)
}

/// `ExecutionStatus` from a `jobs.status` / `job_requests.result` column.
pub(super) fn status(text: &str) -> ExecutionStatus {
    super::super::types::status_parse(text)
}

/// Serialize a value for a `$n::text::jsonb` parameter.
pub(super) fn json<T: serde::Serialize + ?Sized>(value: &T) -> Result<String, ControlError> {
    serde_json::to_string(value).map_err(ControlError::backend)
}

/// Decode a `col::text` jsonb read.
pub(super) fn from_json<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, ControlError> {
    serde_json::from_str(text).map_err(ControlError::backend)
}

/// Clamp a `usize` limit into an `int8` parameter.
pub(super) fn limit(n: usize) -> i64 {
    n.min(i64::MAX as usize) as i64
}

/// Parse an RFC 3339 `locked_until` into microseconds. An empty string
/// means "no lease".
pub(super) fn locked_until_us(locked_until: &str) -> Result<Option<i64>, ControlError> {
    if locked_until.is_empty() {
        return Ok(None);
    }
    chrono::DateTime::parse_from_rfc3339(locked_until)
        .map(|t| Some(t.timestamp_micros()))
        .map_err(|error| {
            ControlError::BadRequest(format!("invalid lockedUntil {locked_until:?}: {error}"))
        })
}

/// Render a lease expiry as the protocol `lockedUntil` string, formatted by
/// the same helper that mints it (`""` when there is no lease row).
pub(super) fn locked_until_string(expires_us: Option<i64>) -> String {
    expires_us
        .map(|us| crate::recording::server_iso_at(us_to_system(us)))
        .unwrap_or_default()
}
