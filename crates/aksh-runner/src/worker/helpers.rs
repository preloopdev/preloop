//! Shared worker utility functions.

/// Extract the run-service base URL and access token from the job message.
///
/// The job message's `resources.endpoints` contains a `SystemVssConnection`
/// endpoint with the URL and OAuth AccessToken for the run-service.
pub(crate) fn extract_service_endpoint(
    job_message: &serde_json::Value,
) -> Option<(String, String)> {
    let endpoints = job_message
        .get("resources")
        .and_then(|r| r.get("endpoints"))
        .and_then(|e| e.as_array())?;

    for ep in endpoints {
        let name = ep.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if name == "SystemVssConnection" {
            let url = ep.get("url").and_then(|v| v.as_str())?.to_string();
            let token = ep
                .get("authorization")
                .and_then(|a| a.get("parameters"))
                .and_then(|p| p.get("AccessToken"))
                .and_then(|v| v.as_str())?
                .to_string();
            return Some((url.trim_end_matches('/').to_string(), token));
        }
    }
    None
}

/// Extract the results service URL from endpoint data or job message variables.
///
/// Golden 06: `system.github.results_endpoint` = `https://results-receiver.actions.githubusercontent.com/`.
/// Current acquire payloads can also carry `resources.endpoints[].data.ResultsServiceUrl`.
pub(crate) fn extract_results_url(job_message: &serde_json::Value) -> Option<String> {
    if let Some(endpoints) = job_message
        .get("resources")
        .and_then(|r| r.get("endpoints"))
        .and_then(|e| e.as_array())
    {
        for ep in endpoints {
            let name = ep.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if name.eq_ignore_ascii_case("SystemVssConnection") {
                if let Some(url) = ep
                    .get("data")
                    .and_then(|d| d.get("ResultsServiceUrl"))
                    .and_then(|v| v.as_str())
                    .filter(|url| !url.is_empty())
                {
                    return Some(url.trim_end_matches('/').to_string());
                }
            }
        }
    }

    let vars = job_message.get("variables")?.as_object()?;
    let url = vars
        .get("system.github.results_endpoint")
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_str())?;
    Some(url.trim_end_matches('/').to_string())
}
/// ISO 8601 timestamp for step timing (public so steps_runner can call it).
pub fn iso_now() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    time_to_iso8601(secs, millis)
}

/// Convert unix timestamp to ISO 8601 string (UTC).
pub(crate) fn time_to_iso8601(secs: u64, millis: u32) -> String {
    // Simple UTC ISO 8601 formatter without chrono dependency
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    // Days since epoch to y/m/d (civil_from_days algorithm)
    let (y, m, d) = civil_from_days(days as i64);

    format!("{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z")
}

/// Convert days since Unix epoch to (year, month, day).
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
