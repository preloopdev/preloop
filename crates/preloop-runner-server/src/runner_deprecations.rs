//! Runner version end-of-life schedule lookup.
//!
//! Mirrors GitHub's `GET /actions/runners/deprecations/{version}` (added in
//! the September 3, 2026 changelog): given a runner version, it reports when
//! runtime support ends for that version (`runtime_deprecates_at` — the point
//! past which jobs stop being queued to runners on it) and when registration
//! ends (`registration_deprecates_at` — the point past which new runners on
//! it can no longer configure/register), so operators can plan upgrades
//! before a version is cut off.
//!
//! Where the dates come from: preloop is a local control plane, not a
//! multi-version fleet operator. It runs no brownouts and gates no runner
//! version out of registration or job queueing — the opt-in
//! `PRELOOP_RUNNER_VERSION_DEPRECATED` kill switch is a plain boolean, not a
//! schedule — and preloop tracks no end-of-life dates for runner versions.
//! (The lifecycle dates preloop does know, like the node24 default date in
//! `populate_runner_variables`, describe the JS-action runtime, not the
//! runner release.) `RUNNER_EOL_SCHEDULE` is therefore intentionally empty,
//! and both date fields come back null rather than invented. That is the one
//! deliberate data divergence from GitHub's endpoint, which returns real
//! dates for old runner versions.
//!
//! Unknown versions: GitHub answers 404 for a version it never released.
//! preloop returns 404 for a path segment that cannot be a runner version at
//! all (anything but an optional leading `v` followed by dot-separated
//! numbers, e.g. `2.336.0`); well-formed versions with no scheduled
//! end-of-life get the echoed version with null dates.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    Json,
};
use serde::Serialize;

use super::errors::ApiError;
use super::state::SharedState;

/// End-of-life schedule for runner versions.
///
/// Each entry is `(version, runtime_deprecates_at, registration_deprecates_at)`
/// with RFC 3339 timestamps. Empty by design — see the module docs.
static RUNNER_EOL_SCHEDULE: &[(&str, Option<&str>, Option<&str>)] = &[];

/// GitHub-compatible deprecation lookup response.
#[derive(Debug, Serialize, PartialEq)]
pub struct RunnerDeprecation {
    pub runner_version: String,
    pub runtime_deprecates_at: Option<String>,
    pub registration_deprecates_at: Option<String>,
}

/// True when `version` has the shape of a runner release (`2.336.0`,
/// optionally with a leading `v`).
fn is_runner_version_shape(version: &str) -> bool {
    let digits = version.strip_prefix('v').unwrap_or(version);
    !digits.is_empty()
        && digits
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
}

/// Look up the end-of-life schedule for `version`.
///
/// Returns `None` when the segment cannot be a runner version at all (the
/// handler turns that into a 404, matching GitHub's behavior for a version it
/// never released).
pub fn lookup_runner_deprecation(version: &str) -> Option<RunnerDeprecation> {
    if !is_runner_version_shape(version) {
        return None;
    }
    let normalized = version.strip_prefix('v').unwrap_or(version);
    let (runtime_deprecates_at, registration_deprecates_at) = RUNNER_EOL_SCHEDULE
        .iter()
        .find(|(scheduled, _, _)| *scheduled == normalized)
        .map(|(_, runtime, registration)| {
            (runtime.map(str::to_owned), registration.map(str::to_owned))
        })
        .unwrap_or((None, None));
    Some(RunnerDeprecation {
        runner_version: version.to_owned(),
        runtime_deprecates_at,
        registration_deprecates_at,
    })
}

/// Shared handler core: system-token gate plus schedule lookup.
fn deprecation_response(
    shared: &SharedState,
    headers: &HeaderMap,
    version: &str,
) -> Result<Json<RunnerDeprecation>, ApiError> {
    // Same credential posture as the neighboring runner-registration routes:
    // the check lives in the handler, not in a routing-layer middleware, and
    // the only accepted credential is the control plane's own system token
    // (GitHub's endpoint likewise requires an authenticated caller).
    let authorized = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("RemoteAuth "))
        })
        == Some(shared.state.system_token.as_str());
    if !authorized {
        return Err(ApiError::unauthorized(
            "system token required for runner deprecation lookup",
        ));
    }
    lookup_runner_deprecation(version)
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("unknown runner version {version:?}")))
}

/// `GET /api/v3/actions/runners/deprecations/:version` (control-plane level).
pub async fn runner_deprecation_lookup(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    Path(version): Path<String>,
) -> Result<Json<RunnerDeprecation>, ApiError> {
    deprecation_response(&shared, &headers, &version)
}

/// `GET /api/v3/orgs/:org/actions/runners/deprecations/:version`.
///
/// The org is accepted and ignored: preloop is a single-tenant local control
/// plane, so the end-of-life schedule is global, not per-org.
pub async fn runner_deprecation_lookup_org(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    Path((_org, version)): Path<(String, String)>,
) -> Result<Json<RunnerDeprecation>, ApiError> {
    deprecation_response(&shared, &headers, &version)
}

/// `GET /api/v3/repos/:owner/:repo/actions/runners/deprecations/:version`.
///
/// The repo is accepted and ignored, same single-tenant reasoning as the org
/// variant.
pub async fn runner_deprecation_lookup_repo(
    State(shared): State<Arc<SharedState>>,
    headers: HeaderMap,
    Path((_owner, _repo, version)): Path<(String, String, String)>,
) -> Result<Json<RunnerDeprecation>, ApiError> {
    deprecation_response(&shared, &headers, &version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_shape_carries_the_three_github_fields() {
        let deprecation = lookup_runner_deprecation("2.336.0").unwrap();
        let json = serde_json::to_value(&deprecation).unwrap();
        let object = json.as_object().unwrap();
        assert_eq!(
            object.keys().collect::<Vec<_>>(),
            vec![
                "runner_version",
                "runtime_deprecates_at",
                "registration_deprecates_at"
            ]
        );
        assert_eq!(json["runner_version"], "2.336.0");
        assert!(json["runtime_deprecates_at"].is_null());
        assert!(json["registration_deprecates_at"].is_null());
    }

    #[test]
    fn scheduled_versions_echo_with_nulls_when_preloop_tracks_no_dates() {
        // preloop tracks no runner end-of-life dates (see the module docs),
        // so even versions preloop has genuinely seen — its own pinned runner
        // and versions from the conformance goldens — come back with null
        // dates rather than invented ones.
        for version in ["2.336.0", "2.335.1", "2.329.0", "v2.336.0"] {
            let deprecation = lookup_runner_deprecation(version).unwrap();
            assert_eq!(deprecation.runner_version, version);
            assert_eq!(deprecation.runtime_deprecates_at, None);
            assert_eq!(deprecation.registration_deprecates_at, None);
        }
    }

    #[test]
    fn malformed_versions_are_not_runner_versions() {
        for version in ["", "latest", "node20", "2.336.0-rc1", "2..336", ".336", "v"] {
            assert!(
                lookup_runner_deprecation(version).is_none(),
                "{version:?} should not parse as a runner version"
            );
        }
    }

    #[test]
    fn scheduled_dates_must_be_rfc3339_when_the_table_grows() {
        // The table is empty today; this guards the format for future entries.
        for (_, runtime, registration) in RUNNER_EOL_SCHEDULE {
            for date in runtime.iter().chain(registration.iter()) {
                chrono::DateTime::parse_from_rfc3339(date)
                    .unwrap_or_else(|_| panic!("{date:?} is not RFC 3339"));
            }
        }
    }
}
