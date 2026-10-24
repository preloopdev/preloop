//! Code-scanning SARIF ingestion.
//!
//! Two surfaces share one store:
//!
//! - the GitHub code-scanning shim (`/api/v3/repos/{owner}/{repo}/…`, also
//!   aliased unprefixed for deployments whose GHES-style API base is the
//!   engine root), enough for `github/codeql-action/upload-sarif` to complete
//!   unchanged: the legacy `POST …/code-scanning/sarifs`, the current
//!   `PUT …/code-scanning/analysis`, the status read the action polls
//!   (`GET …/code-scanning/sarifs/{id}`) and the analyses list behind
//!   `analyses_url`;
//! - the native run API (`/api/v1/runs/{run_id}/sarif`), which accepts the
//!   raw `application/sarif+json` document or a JSON wrapper.
//!
//! Authentication is the job runtime token (Bearer, or Basic
//! `x-access-token:<token>` as `actions/checkout` sends) bound to the run that
//! owns the repository, or the system token. A token for another repository is
//! rejected: the repository and run are resolved from the token's job → run →
//! submission chain, never from the request body.
//!
//! The raw document is stored gzip-compressed under `<state_dir>/sarif/`,
//! addressed by the analysis id; the metadata and normalized findings live in
//! the control database (`sarif_analyses` / `sarif_findings`). Re-uploading
//! the same commit/ref/tool/findings set is idempotent on a content
//! fingerprint, not on the raw bytes.

use super::*;
use crate::control::types::{
    NewSarifAnalysis, SarifAnalysisRecord, SarifAnalysisSource, SarifFinding,
};

use sha2::Digest as _;
use std::io::{Read as _, Write as _};
use std::path::PathBuf;

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

/// Largest request body accepted on any SARIF endpoint (base64 + JSON
/// envelope included).
pub const MAX_SARIF_REQUEST_BYTES: usize = 24 * 1024 * 1024;

/// Largest compressed SARIF document accepted (GitHub's own limit for the
/// gzipped upload).
pub const MAX_SARIF_COMPRESSED_BYTES: usize = 10 * 1024 * 1024;

/// Largest decompressed SARIF document accepted. A tiny gzip can expand
/// past the compressed cap in RAM, so the decompressed size has its own
/// bound.
pub const MAX_SARIF_DECOMPRESSED_BYTES: usize = 32 * 1024 * 1024;

/// Largest number of normalized findings one analysis may carry.
pub const MAX_SARIF_FINDINGS: usize = 25_000;

/// GitHub's documentation anchor for the code-scanning upload API, carried by
/// every GitHub-shaped error.
const SARIF_DOCUMENTATION_URL: &str =
    "https://docs.github.com/rest/code-scanning/code-scanning#upload-an-analysis-as-sarif-data";

/// Storage directory (below the state directory) for raw SARIF documents.
const SARIF_STORE_DIR: &str = "sarif";

// ── Errors ───────────────────────────────────────────────────────────────

/// A SARIF rejection before it is rendered on one of the two wire surfaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SarifError {
    pub(crate) status: StatusCode,
    pub(crate) message: String,
}

impl SarifError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn unsupported_media_type(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            message: message.into(),
        }
    }

    fn payload_too_large(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: message.into(),
        }
    }

    fn unprocessable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: message.into(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("sarif store error: {error}"),
        }
    }
}

impl From<SarifError> for ApiError {
    fn from(error: SarifError) -> Self {
        match error.status {
            StatusCode::UNAUTHORIZED => ApiError::unauthorized(error.message),
            StatusCode::FORBIDDEN => ApiError::forbidden(error.message),
            StatusCode::NOT_FOUND => ApiError::not_found(error.message),
            StatusCode::PAYLOAD_TOO_LARGE => ApiError::payload_too_large(error.message),
            StatusCode::UNPROCESSABLE_ENTITY => ApiError::unprocessable(error.message),
            StatusCode::INTERNAL_SERVER_ERROR => ApiError::internal(error.message),
            _ => ApiError::bad_request(error.message),
        }
    }
}

impl From<ApiError> for SarifError {
    fn from(error: ApiError) -> Self {
        let status = error.status();
        let message = error.message().to_owned();
        match status {
            StatusCode::UNAUTHORIZED => Self::unauthorized(message),
            StatusCode::FORBIDDEN => Self::forbidden(message),
            StatusCode::NOT_FOUND => Self::not_found(message),
            StatusCode::PAYLOAD_TOO_LARGE => Self::payload_too_large(message),
            StatusCode::UNPROCESSABLE_ENTITY => Self::unprocessable(message),
            _ => Self::internal(message),
        }
    }
}

impl From<crate::control::ControlError> for SarifError {
    fn from(error: crate::control::ControlError) -> Self {
        let api: ApiError = error.into();
        let status = api.status();
        let message = api.message().to_owned();
        match status {
            StatusCode::UNAUTHORIZED => Self::unauthorized(message),
            StatusCode::FORBIDDEN => Self::forbidden(message),
            StatusCode::NOT_FOUND => Self::not_found(message),
            StatusCode::PAYLOAD_TOO_LARGE => Self::payload_too_large(message),
            StatusCode::UNPROCESSABLE_ENTITY => Self::unprocessable(message),
            _ => Self::internal(message),
        }
    }
}

/// GitHub-shaped JSON error envelope (`message` + `documentation_url`).
fn github_error_response(error: SarifError) -> Response {
    let payload = json!({
        "message": error.message,
        "documentation_url": SARIF_DOCUMENTATION_URL,
    });
    (
        error.status,
        [(header::CONTENT_TYPE, "application/json")],
        payload.to_string(),
    )
        .into_response()
}

// ── SARIF 2.1.0 model (the subset the shim reads) ────────────────────────

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifLog {
    #[serde(default)]
    pub(crate) version: Option<String>,
    /// Required by SARIF 2.1.0. `Option` so a JSON object without it is
    /// rejected instead of silently becoming an empty analysis.
    #[serde(default)]
    pub(crate) runs: Option<Vec<SarifRun>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifRun {
    #[serde(default)]
    pub(crate) tool: Option<SarifTool>,
    #[serde(default)]
    pub(crate) artifacts: Vec<SarifArtifact>,
    #[serde(default)]
    pub(crate) results: Vec<SarifResult>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifTool {
    #[serde(default)]
    pub(crate) driver: Option<SarifComponent>,
    #[serde(default)]
    pub(crate) extensions: Vec<SarifComponent>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifComponent {
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) rules: Vec<SarifRule>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifRule {
    #[serde(default)]
    pub(crate) id: Option<String>,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) short_description: Option<SarifMessage>,
    #[serde(default)]
    pub(crate) default_configuration: Option<SarifConfiguration>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifConfiguration {
    #[serde(default)]
    pub(crate) level: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifMessage {
    #[serde(default)]
    pub(crate) text: Option<String>,
    #[serde(default)]
    pub(crate) markdown: Option<String>,
    #[serde(default)]
    pub(crate) id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifArtifact {
    #[serde(default)]
    pub(crate) location: Option<SarifArtifactLocation>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifArtifactLocation {
    #[serde(default)]
    pub(crate) uri: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifResult {
    #[serde(default)]
    pub(crate) rule_id: Option<String>,
    #[serde(default)]
    pub(crate) rule_index: Option<u64>,
    #[serde(default)]
    pub(crate) level: Option<String>,
    #[serde(default)]
    pub(crate) message: Option<SarifMessage>,
    #[serde(default)]
    pub(crate) locations: Vec<SarifLocation>,
    #[serde(default)]
    pub(crate) fingerprints: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) partial_fingerprints: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) suppressions: Vec<SarifSuppression>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifLocation {
    #[serde(default)]
    pub(crate) physical_location: Option<SarifPhysicalLocation>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifPhysicalLocation {
    #[serde(default)]
    pub(crate) artifact_location: Option<SarifArtifactLocation>,
    #[serde(default)]
    pub(crate) region: Option<SarifRegion>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifRegion {
    #[serde(default)]
    pub(crate) start_line: Option<u64>,
    #[serde(default)]
    pub(crate) end_line: Option<u64>,
    #[serde(default)]
    pub(crate) start_column: Option<u64>,
    #[serde(default)]
    pub(crate) end_column: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SarifSuppression {
    #[serde(default)]
    pub(crate) kind: Option<String>,
    #[serde(default)]
    pub(crate) status: Option<String>,
}

// ── Normalization ───────────────────────────────────────────────────────

/// `(name, default level, short description)` for one SARIF rule.
type RuleMeta = (Option<String>, Option<String>, Option<String>);

/// One parsed document: the tool name and the deduplicated findings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedSarif {
    pub(crate) tool_name: String,
    pub(crate) findings: Vec<SarifFinding>,
}

/// Map a SARIF level onto the GitHub annotation vocabulary. Unknown values
/// fall back to `warning` (SARIF's own default), and `none` keeps its meaning
/// of "informational, not a problem".
fn normalize_level(level: Option<&str>) -> String {
    match level.map(str::trim) {
        Some("error") => "error",
        Some("warning") => "warning",
        Some("note") | Some("none") => "note",
        _ => "warning",
    }
    .to_owned()
}

/// A location URI as a repository-relative path. `file://` URLs and `./`
/// prefixes are the two forms SARIF producers emit for checked-out sources.
fn normalize_path(uri: &str) -> Option<String> {
    let uri = uri.trim();
    if uri.is_empty() {
        return None;
    }
    let uri = uri.strip_prefix("file://").unwrap_or(uri);
    let uri = uri.strip_prefix("./").unwrap_or(uri);
    let uri = uri.trim_start_matches('/');
    if uri.is_empty() || uri.len() > 4096 {
        return None;
    }
    Some(uri.to_owned())
}

fn message_text(message: &Option<SarifMessage>) -> Option<String> {
    let message = message.as_ref()?;
    message
        .text
        .clone()
        .or_else(|| message.markdown.clone())
        .filter(|text| !text.trim().is_empty())
}

fn line(value: Option<u64>) -> Option<i64> {
    value.and_then(|value| i64::try_from(value).ok())
}

/// Stable hash for one finding. `partialFingerprints` is SARIF's own
/// rerun-to-rerun identity, so it wins over the coarser `fingerprints` map;
/// when neither exists the identity is derived from the location and message
/// — never from the raw bytes of the document.
fn finding_fingerprint(
    tool_name: &str,
    rule_id: Option<&str>,
    level: &str,
    message: &str,
    path: Option<&str>,
    start_line: Option<i64>,
    partial: &BTreeMap<String, String>,
    fingerprints: &BTreeMap<String, String>,
) -> (String, Option<String>) {
    let mut hasher = Sha256::new();
    hasher.update(b"sarif-finding-v1");
    hasher.update([0]);
    hasher.update(tool_name.as_bytes());
    hasher.update([0]);
    hasher.update(rule_id.unwrap_or("").as_bytes());
    hasher.update([0]);
    let (kind, value) = if let Some((key, value)) = partial.iter().next() {
        ("partial", format!("{key}={value}"))
    } else if let Some((key, value)) = fingerprints.iter().next() {
        ("fingerprint", format!("{key}={value}"))
    } else {
        hasher.update(level.as_bytes());
        hasher.update([0]);
        hasher.update(path.unwrap_or("").as_bytes());
        hasher.update([0]);
        hasher.update(start_line.unwrap_or(0).to_string().as_bytes());
        hasher.update([0]);
        hasher.update(message.as_bytes());
        ("derived", String::new())
    };
    if !value.is_empty() {
        hasher.update([0]);
        hasher.update(value.as_bytes());
    }
    let partial_value = if kind == "derived" { None } else { Some(value) };
    (format!("{:x}", hasher.finalize()), partial_value)
}

/// Parse and normalize a SARIF 2.1.0 document.
///
/// `preferred_tool_name` is the upload payload's `tool_name`/`tool_names`
/// when the client sent one; otherwise the document's own driver names are
/// used. Malformed JSON, a non-2.1.0 version, oversized finding sets and
/// documents that are not SARIF at all are rejected.
pub(crate) fn parse_sarif(
    document: &[u8],
    preferred_tool_name: Option<&str>,
) -> Result<NormalizedSarif, SarifError> {
    let log: SarifLog = serde_json::from_slice(document)
        .map_err(|error| SarifError::bad_request(format!("invalid SARIF JSON: {error}")))?;
    if let Some(version) = log.version.as_deref()
        && version.trim() != "2.1.0"
    {
        return Err(SarifError::bad_request(format!(
            "unsupported SARIF version {version:?}; expected 2.1.0"
        )));
    }
    let Some(runs) = log.runs.as_deref() else {
        return Err(SarifError::bad_request(
            "SARIF document is missing the required `runs` array",
        ));
    };

    let mut driver_names: Vec<String> = Vec::new();
    let mut findings: BTreeMap<String, SarifFinding> = BTreeMap::new();
    for run in runs {
        let Some(tool) = run.tool.as_ref() else {
            continue;
        };
        let driver_name = tool
            .driver
            .as_ref()
            .and_then(|driver| driver.name.clone())
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| "unknown".to_owned());
        if !driver_names.contains(&driver_name) {
            driver_names.push(driver_name.clone());
        }
        // Rule id -> (name, default level, short description), from the
        // driver and every extension component (CodeQL ships its queries as
        // extensions).
        let mut rules: BTreeMap<String, RuleMeta> = BTreeMap::new();
        let components = tool.driver.iter().chain(tool.extensions.iter());
        for component in components {
            for rule in &component.rules {
                let Some(id) = rule.id.clone() else {
                    continue;
                };
                let level = rule
                    .default_configuration
                    .as_ref()
                    .and_then(|configuration| configuration.level.clone());
                rules.insert(
                    id,
                    (
                        rule.name.clone(),
                        level,
                        message_text(&rule.short_description),
                    ),
                );
            }
        }
        for result in &run.results {
            let rule_id = result.rule_id.clone().or_else(|| {
                result
                    .rule_index
                    .and_then(|index| {
                        tool.driver
                            .as_ref()
                            .and_then(|driver| driver.rules.get(index as usize))
                    })
                    .and_then(|rule| rule.id.clone())
            });
            let (rule_name, rule_level, rule_description) = rule_id
                .as_deref()
                .and_then(|id| rules.get(id))
                .cloned()
                .unwrap_or((None, None, None));
            let level = normalize_level(result.level.as_deref().or(rule_level.as_deref()));
            let message = message_text(&result.message)
                .or(rule_description)
                .or_else(|| rule_name.clone())
                .or_else(|| rule_id.clone())
                .unwrap_or_else(|| "SARIF result".to_owned());

            let location = result
                .locations
                .iter()
                .find_map(|location| location.physical_location.as_ref());
            let path = location.and_then(|physical| {
                let uri = physical.artifact_location.as_ref()?.uri.as_deref()?;
                normalize_path(uri)
            });
            // Fall back to the run's artifact list when the result carries no
            // artifactLocation (some producers only populate `artifacts`).
            let path = path.or_else(|| {
                if run.artifacts.len() == 1 {
                    run.artifacts[0]
                        .location
                        .as_ref()
                        .and_then(|location| location.uri.as_deref())
                        .and_then(normalize_path)
                } else {
                    None
                }
            });
            let region = location.and_then(|physical| physical.region.as_ref());
            let start_line = line(region.and_then(|region| region.start_line));
            let end_line = line(region.and_then(|region| region.end_line));
            let start_column = line(region.and_then(|region| region.start_column));
            let end_column = line(region.and_then(|region| region.end_column));
            let suppression = result.suppressions.first().map(|suppression| {
                suppression
                    .kind
                    .clone()
                    .filter(|kind| !kind.trim().is_empty())
                    .unwrap_or_else(|| "suppressed".to_owned())
            });

            let (fingerprint, partial_value) = finding_fingerprint(
                &driver_name,
                rule_id.as_deref(),
                &level,
                &message,
                path.as_deref(),
                start_line,
                &result.partial_fingerprints,
                &result.fingerprints,
            );
            findings.entry(fingerprint.clone()).or_insert(SarifFinding {
                fingerprint,
                tool_name: driver_name.clone(),
                rule_id,
                rule_name,
                level,
                message,
                path,
                start_line,
                end_line,
                start_column,
                end_column,
                partial_fingerprint: partial_value,
                suppression,
            });
            if findings.len() > MAX_SARIF_FINDINGS {
                return Err(SarifError::payload_too_large(format!(
                    "SARIF document carries more than {MAX_SARIF_FINDINGS} findings"
                )));
            }
        }
    }

    let tool_name = preferred_tool_name
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            if driver_names.is_empty() {
                None
            } else {
                Some(driver_names.join(", "))
            }
        })
        .unwrap_or_else(|| "unknown".to_owned());

    Ok(NormalizedSarif {
        tool_name,
        findings: findings.into_values().collect(),
    })
}

/// Content fingerprint that makes re-uploads idempotent: unchanged findings in
/// the same repository/commit/ref/tool set produce the same key even when the
/// raw SARIF bytes differ (timestamps, ordering, formatting).
pub(crate) fn analysis_dedupe_key(
    repository: &str,
    commit_sha: &str,
    git_ref: &str,
    tool_name: &str,
    findings: &[SarifFinding],
) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"sarif-analysis-v1");
    for part in [repository, commit_sha, git_ref, tool_name] {
        hasher.update([0]);
        hasher.update(part.as_bytes());
    }
    for finding in findings {
        hasher.update([0]);
        hasher.update(finding.fingerprint.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

// ── Decoding ────────────────────────────────────────────────────────────

/// Gunzip a document, refusing anything over [`MAX_SARIF_DECOMPRESSED_BYTES`].
pub(crate) fn gunzip_bounded(compressed: &[u8]) -> Result<Vec<u8>, SarifError> {
    let mut decoder = GzDecoder::new(compressed).take(MAX_SARIF_DECOMPRESSED_BYTES as u64 + 1);
    let mut decoded = Vec::new();
    decoder
        .read_to_end(&mut decoded)
        .map_err(|error| SarifError::bad_request(format!("invalid gzip SARIF body: {error}")))?;
    if decoded.len() > MAX_SARIF_DECOMPRESSED_BYTES {
        return Err(SarifError::payload_too_large(format!(
            "decompressed SARIF exceeds {MAX_SARIF_DECOMPRESSED_BYTES} bytes"
        )));
    }
    Ok(decoded)
}

/// The GitHub upload's `sarif` field: base64, then gzip. A base64 payload that
/// is not gzip is accepted as the plain document (some producers skip the
/// compression step), still under the decompressed bound.
pub(crate) fn decode_github_sarif(encoded: &str) -> Result<(Vec<u8>, Vec<u8>), SarifError> {
    let compressed = BASE64_STANDARD
        .decode(encoded.trim())
        .map_err(|error| SarifError::bad_request(format!("sarif is not valid base64: {error}")))?;
    if compressed.len() > MAX_SARIF_COMPRESSED_BYTES {
        return Err(SarifError::payload_too_large(format!(
            "compressed SARIF exceeds {MAX_SARIF_COMPRESSED_BYTES} bytes"
        )));
    }
    if compressed.starts_with(&[0x1f, 0x8b]) {
        let document = gunzip_bounded(&compressed)?;
        Ok((document, compressed))
    } else {
        if compressed.len() > MAX_SARIF_DECOMPRESSED_BYTES {
            return Err(SarifError::payload_too_large(format!(
                "SARIF exceeds {MAX_SARIF_DECOMPRESSED_BYTES} bytes"
            )));
        }
        Ok((compressed.clone(), compressed))
    }
}

/// Compress a document for storage.
fn gzip(document: &[u8]) -> Result<Vec<u8>, SarifError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(document).map_err(SarifError::internal)?;
    encoder.finish().map_err(SarifError::internal)
}

/// Read a request body with a hard cap. Reading manually (instead of a body
/// extractor) keeps the rejection on the caller's wire shape.
async fn read_bounded_body(body: Body, limit: usize) -> Result<Vec<u8>, SarifError> {
    let mut stream = body.into_data_stream();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(|error| SarifError::bad_request(format!("read request body: {error}")))?;
        if out.len().saturating_add(chunk.len()) > limit {
            return Err(SarifError::payload_too_large(format!(
                "request body exceeds {limit} bytes"
            )));
        }
        out.extend_from_slice(&chunk);
    }
    Ok(out)
}

/// The credential a SARIF request authenticated with.
#[derive(Debug, Clone)]
pub(crate) struct SarifCaller {
    /// `None` for the system token.
    pub(crate) job: Option<uuid::Uuid>,
    pub(crate) uploader: String,
}

/// Accept the header forms action clients send: `Bearer <token>` (Octokit),
/// `token <token>` (Octokit with a PAT-style credential) and Basic
/// `x-access-token:<token>` (`actions/checkout`).
pub(crate) fn authorization_token(value: &str) -> Option<String> {
    if let Some(rest) = value
        .strip_prefix("token ")
        .or_else(|| value.strip_prefix("Token "))
    {
        let token = rest.trim();
        if !token.is_empty() {
            return Some(token.to_owned());
        }
    }
    snapshot_authorization_token(value)
}

fn header_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(authorization_token)
}

// ── Authorization ───────────────────────────────────────────────────────

/// Resolve the caller's job and confirm it belongs to `repository`.
///
/// The repository comes from the token's job → run → submission chain, never
/// from the path or body, so a job token cannot upload findings for another
/// repository by naming it in the URL.
async fn authorize_github_caller(
    state: &AppState,
    token: &str,
    owner: &str,
    repo: &str,
    require_live: bool,
) -> Result<(SarifCaller, RunId), SarifError> {
    if token == state.system_token.as_str() {
        return Err(SarifError::forbidden(
            "code-scanning uploads require a job runtime token bound to a repository",
        ));
    }
    let job = state
        .job_uuid_from_token(token)
        .ok_or_else(|| SarifError::unauthorized("job runtime token required"))?;
    let run_id = state
        .backend
        .run_for_attempt(job)
        .await
        .map_err(SarifError::from)?
        .ok_or_else(|| {
            SarifError::forbidden("job runtime token is not bound to a live workflow run")
        })?;
    let repository = state
        .backend
        .attempt_repository(job)
        .await
        .map_err(SarifError::from)?
        .ok_or_else(|| {
            SarifError::forbidden("job runtime token is not bound to a live workflow run")
        })?;
    if !repository.eq_ignore_ascii_case(&format!("{owner}/{repo}")) {
        return Err(SarifError::forbidden(
            "job runtime token is not bound to this repository",
        ));
    }
    if require_live && !crate::auth::job_is_live(state, job).await? {
        return Err(SarifError::forbidden(
            "job is not live; uploads are rejected for completed or unknown jobs",
        ));
    }
    Ok((
        SarifCaller {
            job: Some(job),
            uploader: job.to_string(),
        },
        run_id,
    ))
}

/// Resolve the caller for a native run endpoint: the system token addresses
/// any run, a job token only the run its attempt belongs to.
async fn authorize_run_caller(
    state: &AppState,
    headers: &HeaderMap,
    run_id: RunId,
    require_live: bool,
) -> Result<SarifCaller, SarifError> {
    let token = header_token(headers)
        .ok_or_else(|| SarifError::unauthorized("job or system token required"))?;
    if token == state.system_token.as_str() {
        return Ok(SarifCaller {
            job: None,
            uploader: "system".to_owned(),
        });
    }
    let job = state
        .job_uuid_from_token(&token)
        .ok_or_else(|| SarifError::unauthorized("job or system token required"))?;
    let owned = state
        .backend
        .run_for_attempt(job)
        .await
        .map_err(SarifError::from)?;
    if owned != Some(run_id) {
        return Err(SarifError::forbidden(
            "job runtime token is not bound to this run",
        ));
    }
    if require_live && !crate::auth::job_is_live(state, job).await? {
        return Err(SarifError::forbidden(
            "job is not live; uploads are rejected for completed or unknown jobs",
        ));
    }
    Ok(SarifCaller {
        job: Some(job),
        uploader: job.to_string(),
    })
}

// ── Raw document store ──────────────────────────────────────────────────

fn sarif_store_dir(state: &AppState) -> PathBuf {
    state.state_dir.join(SARIF_STORE_DIR)
}

/// Path of one stored document. `storage_key` is the analysis id (a UUID);
/// anything else is refused before a path is built, so a corrupted row can
/// never escape the store directory.
fn sarif_document_path(state: &AppState, storage_key: &str) -> Result<PathBuf, SarifError> {
    uuid::Uuid::parse_str(storage_key)
        .map_err(|_| SarifError::internal(format!("invalid sarif storage key {storage_key:?}")))?;
    Ok(sarif_store_dir(state).join(format!("{storage_key}.sarif.gz")))
}

async fn store_document(
    state: &AppState,
    storage_key: &str,
    compressed: &[u8],
) -> Result<(), SarifError> {
    let path = sarif_document_path(state, storage_key)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(SarifError::internal)?;
    }
    tokio::fs::write(&path, compressed)
        .await
        .map_err(SarifError::internal)
}

async fn remove_document(state: &AppState, storage_key: &str) {
    if let Ok(path) = sarif_document_path(state, storage_key) {
        let _ = tokio::fs::remove_file(path).await;
    }
}

/// Read one stored document back, decompressed and bounded.
pub(crate) async fn read_document(
    state: &AppState,
    storage_key: &str,
) -> Result<Vec<u8>, SarifError> {
    let path = sarif_document_path(state, storage_key)?;
    let compressed = tokio::fs::read(&path).await.map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            SarifError::not_found("stored SARIF document is missing")
        } else {
            SarifError::internal(error)
        }
    })?;
    gunzip_bounded(&compressed)
}

// ── Ingestion ───────────────────────────────────────────────────────────

/// Everything one upload resolves before it reaches the store.
pub(crate) struct SarifUpload {
    pub(crate) run_id: RunId,
    pub(crate) repository: String,
    pub(crate) commit_sha: String,
    pub(crate) git_ref: String,
    pub(crate) tool_name: Option<String>,
    pub(crate) checkout_uri: Option<String>,
    pub(crate) started_at: Option<String>,
    pub(crate) analysis_key: Option<String>,
    pub(crate) environment: Option<String>,
    pub(crate) uploader: String,
    pub(crate) source: SarifAnalysisSource,
    /// The decompressed SARIF document.
    pub(crate) document: Vec<u8>,
    /// The upload's compressed bytes, when the client already gzipped them.
    pub(crate) compressed: Option<Vec<u8>>,
}

/// Persist one upload: normalize, dedupe by content, store the document, then
/// write the analysis and its findings. Returns the winning record and
/// whether this call inserted it.
pub(crate) async fn ingest_sarif(
    state: &AppState,
    upload: SarifUpload,
) -> Result<(SarifAnalysisRecord, bool), SarifError> {
    let normalized = parse_sarif(&upload.document, upload.tool_name.as_deref())?;
    let dedupe_key = analysis_dedupe_key(
        &upload.repository,
        &upload.commit_sha,
        &upload.git_ref,
        &normalized.tool_name,
        &normalized.findings,
    );
    if let Some(existing) = state
        .backend
        .sarif_analysis_by_dedupe(upload.run_id, &dedupe_key)
        .await?
    {
        return Ok((existing, false));
    }

    let analysis_id = uuid::Uuid::new_v4().to_string();
    let compressed = match upload.compressed {
        Some(compressed) => compressed,
        None => gzip(&upload.document)?,
    };
    let storage_key = analysis_id.clone();
    store_document(state, &storage_key, &compressed).await?;

    let analysis = NewSarifAnalysis {
        analysis_id: analysis_id.clone(),
        run_id: upload.run_id,
        repository: upload.repository,
        commit_sha: upload.commit_sha,
        git_ref: upload.git_ref,
        tool_name: normalized.tool_name.clone(),
        source: upload.source,
        uploader: upload.uploader,
        dedupe_key,
        storage_key: storage_key.clone(),
        size_bytes: compressed.len() as i64,
        raw_size_bytes: upload.document.len() as i64,
        finding_count: normalized.findings.len() as i64,
        errors: None,
        checkout_uri: upload.checkout_uri,
        analysis_key: upload.analysis_key,
        environment: upload.environment,
        started_at: upload.started_at,
    };
    match state
        .backend
        .put_sarif_analysis(analysis, normalized.findings)
        .await
    {
        Ok((record, inserted)) => {
            if !inserted {
                // A concurrent upload of the same content won the unique slot;
                // this call's document is unreachable.
                remove_document(state, &storage_key).await;
            }
            Ok((record, inserted))
        }
        Err(error) => {
            remove_document(state, &storage_key).await;
            Err(SarifError::from(error))
        }
    }
}

// ── Wire payloads ───────────────────────────────────────────────────────

/// GitHub's code-scanning upload payload (`sarif` is base64(gzip(SARIF))).
/// The legacy endpoint names the commit `commit_sha`; the current
/// `PUT …/code-scanning/analysis` names it `commit_oid`. Both are accepted on
/// either endpoint.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct GithubSarifPayload {
    #[serde(default)]
    pub(crate) commit_sha: Option<String>,
    #[serde(default)]
    pub(crate) commit_oid: Option<String>,
    #[serde(default, rename = "ref")]
    pub(crate) git_ref: Option<String>,
    #[serde(default)]
    pub(crate) sarif: Option<String>,
    #[serde(default)]
    pub(crate) tool_name: Option<String>,
    #[serde(default)]
    pub(crate) tool_names: Vec<String>,
    #[serde(default)]
    pub(crate) checkout_uri: Option<String>,
    #[serde(default)]
    pub(crate) started_at: Option<String>,
    #[serde(default)]
    pub(crate) analysis_key: Option<String>,
    #[serde(default)]
    pub(crate) environment: Option<String>,
    #[serde(default)]
    pub(crate) base_ref: Option<String>,
    #[serde(default)]
    pub(crate) base_sha: Option<String>,
}

impl GithubSarifPayload {
    fn commit(&self) -> Option<&str> {
        self.commit_sha
            .as_deref()
            .or(self.commit_oid.as_deref())
            .map(str::trim)
            .filter(|commit| !commit.is_empty())
    }

    fn tool(&self) -> Option<String> {
        let explicit = self
            .tool_name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty());
        let names: Vec<&str> = self
            .tool_names
            .iter()
            .map(String::as_str)
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .collect();
        match (explicit, names.is_empty()) {
            (Some(name), _) => Some(name.to_owned()),
            (None, false) => Some(names.join(", ")),
            (None, true) => None,
        }
    }
}

/// Native upload wrapper. `sarif` is either the SARIF object or a
/// base64(gzip) string; a body that is itself a SARIF log (no wrapper) is
/// accepted too.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct NativeSarifPayload {
    #[serde(default)]
    pub(crate) sarif: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) commit_sha: Option<String>,
    #[serde(default, rename = "ref")]
    pub(crate) git_ref: Option<String>,
    #[serde(default)]
    pub(crate) tool_name: Option<String>,
    #[serde(default)]
    pub(crate) tool_names: Vec<String>,
    #[serde(default)]
    pub(crate) checkout_uri: Option<String>,
    #[serde(default)]
    pub(crate) started_at: Option<String>,
    #[serde(default)]
    pub(crate) analysis_key: Option<String>,
    #[serde(default)]
    pub(crate) environment: Option<String>,
}

impl NativeSarifPayload {
    fn tool(&self) -> Option<String> {
        GithubSarifPayload {
            tool_name: self.tool_name.clone(),
            tool_names: self.tool_names.clone(),
            ..Default::default()
        }
        .tool()
    }
}

fn validate_commit_sha(commit: &str) -> Result<(), SarifError> {
    if !(7..=64).contains(&commit.len()) || !commit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SarifError::unprocessable(format!(
            "commit_sha {commit:?} is not a hexadecimal commit id"
        )));
    }
    Ok(())
}

fn validate_git_ref(git_ref: &str) -> Result<(), SarifError> {
    if git_ref.is_empty() || git_ref.len() > 1024 || git_ref.starts_with('-') {
        return Err(SarifError::unprocessable("invalid ref"));
    }
    Ok(())
}

fn trim_opt(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

// ── GitHub shim handlers ────────────────────────────────────────────────

/// Shared body of `POST …/code-scanning/sarifs` and
/// `PUT …/code-scanning/analysis`.
async fn github_upload(state: &AppState, owner: &str, repo: &str, request: Request) -> Response {
    let Some(token) = header_token(request.headers()) else {
        return github_error_response(SarifError::unauthorized(
            "code-scanning upload requires a job runtime token",
        ));
    };
    let (caller, run_id) = match authorize_github_caller(state, &token, owner, repo, true).await {
        Ok(caller) => caller,
        Err(error) => {
            if error.status == StatusCode::UNAUTHORIZED {
                let mut response = github_error_response(error);
                response.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    header::HeaderValue::from_static("Bearer"),
                );
                return response;
            }
            return github_error_response(error);
        }
    };
    let body = match read_bounded_body(request.into_body(), MAX_SARIF_REQUEST_BYTES).await {
        Ok(body) => body,
        Err(error) => return github_error_response(error),
    };
    let payload: GithubSarifPayload = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(error) => {
            return github_error_response(SarifError::bad_request(format!(
                "invalid JSON body: {error}"
            )));
        }
    };
    let Some(commit) = payload.commit() else {
        return github_error_response(SarifError::unprocessable("commit_sha is required"));
    };
    if let Err(error) = validate_commit_sha(commit) {
        return github_error_response(error);
    }
    let Some(git_ref) = trim_opt(payload.git_ref.clone()) else {
        return github_error_response(SarifError::unprocessable("ref is required"));
    };
    if let Err(error) = validate_git_ref(&git_ref) {
        return github_error_response(error);
    }
    let Some(encoded) = payload.sarif.as_deref() else {
        return github_error_response(SarifError::unprocessable("sarif is required"));
    };
    let (document, compressed) = match decode_github_sarif(encoded) {
        Ok(decoded) => decoded,
        Err(error) => return github_error_response(error),
    };
    let repository = match state.backend.run_record(run_id).await {
        Ok(record) => record.submission.repository.clone(),
        Err(error) => return github_error_response(SarifError::from(error)),
    };
    let upload = SarifUpload {
        run_id,
        repository: repository.clone(),
        commit_sha: commit.to_owned(),
        git_ref: git_ref.clone(),
        tool_name: payload.tool(),
        checkout_uri: trim_opt(payload.checkout_uri.clone()),
        started_at: trim_opt(payload.started_at.clone()),
        analysis_key: trim_opt(payload.analysis_key.clone()),
        environment: trim_opt(payload.environment.clone()),
        uploader: caller.uploader,
        source: SarifAnalysisSource::Github,
        document,
        compressed: Some(compressed),
    };
    match ingest_sarif(state, upload).await {
        Ok((record, _inserted)) => {
            let base = crate::broker::public_base_url();
            let url = format!(
                "{base}/api/v3/repos/{owner}/{repo}/code-scanning/sarifs/{}",
                record.analysis_id
            );
            (
                StatusCode::ACCEPTED,
                [(header::CONTENT_TYPE, "application/json")],
                json!({ "id": record.analysis_id, "url": url }).to_string(),
            )
                .into_response()
        }
        Err(error) => github_error_response(error),
    }
}

/// `POST /repos/{owner}/{repo}/code-scanning/sarifs` (the legacy upload).
pub async fn github_upload_sarif(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo)): Path<(String, String)>,
    request: Request,
) -> Response {
    github_upload(&shared.state, &owner, &repo, request).await
}

/// `PUT /repos/{owner}/{repo}/code-scanning/analysis` (the endpoint current
/// `github/codeql-action` releases upload to).
pub async fn github_create_analysis(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo)): Path<(String, String)>,
    request: Request,
) -> Response {
    github_upload(&shared.state, &owner, &repo, request).await
}

/// `GET /repos/{owner}/{repo}/code-scanning/sarifs/{sarif_id}`: the status
/// read `upload-sarif` polls after an upload.
pub async fn github_sarif_status(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo, sarif_id)): Path<(String, String, String)>,
    request: Request,
) -> Response {
    let Some(token) = header_token(request.headers()) else {
        let mut response = github_error_response(SarifError::unauthorized(
            "code-scanning status requires a job runtime token",
        ));
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            header::HeaderValue::from_static("Bearer"),
        );
        return response;
    };
    if token != shared.state.system_token.as_str()
        && let Err(error) =
            authorize_github_caller(&shared.state, &token, &owner, &repo, false).await
    {
        return github_error_response(error);
    }
    let record = match shared.state.backend.sarif_analysis(&sarif_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return github_error_response(SarifError::not_found("Not Found")),
        Err(error) => return github_error_response(SarifError::from(error)),
    };
    if !record
        .repository
        .eq_ignore_ascii_case(&format!("{owner}/{repo}"))
    {
        // Never disclose that an analysis exists under another repository.
        return github_error_response(SarifError::not_found("Not Found"));
    }
    let base = crate::broker::public_base_url();
    let analyses_url = format!(
        "{base}/api/v3/repos/{owner}/{repo}/code-scanning/analyses?run_id={}",
        record.run_id
    );
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json!({
            "processing_status": record.status.as_str(),
            "analyses_url": analyses_url,
            "sarif_id": record.analysis_id,
            "errors": [],
        })
        .to_string(),
    )
        .into_response()
}

/// `GET /repos/{owner}/{repo}/code-scanning/analyses`: the list behind the
/// `analyses_url` a status read returns. The engine's analyses are run-scoped,
/// so the caller's job token selects the run; the system token may name one
/// with `?run_id=`.
pub async fn github_list_analyses(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo)): Path<(String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    request: Request,
) -> Response {
    let Some(token) = header_token(request.headers()) else {
        return github_error_response(SarifError::unauthorized(
            "code-scanning analyses require a job runtime token",
        ));
    };
    let run_id = if token == shared.state.system_token.as_str() {
        match query
            .get("run_id")
            .and_then(|value| value.parse::<RunId>().ok())
        {
            Some(run_id) => run_id,
            None => {
                return (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/json")],
                    json!({ "analyses": [] }).to_string(),
                )
                    .into_response();
            }
        }
    } else {
        match authorize_github_caller(&shared.state, &token, &owner, &repo, false).await {
            Ok((_, run_id)) => run_id,
            Err(error) => return github_error_response(error),
        }
    };
    let analyses = match shared.state.backend.sarif_analyses(run_id).await {
        Ok(analyses) => analyses,
        Err(error) => return github_error_response(SarifError::from(error)),
    };
    let analyses: Vec<serde_json::Value> = analyses
        .iter()
        .filter(|analysis| {
            analysis
                .repository
                .eq_ignore_ascii_case(&format!("{owner}/{repo}"))
        })
        .map(analysis_view)
        .collect();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json!({ "analyses": analyses }).to_string(),
    )
        .into_response()
}

/// `GET /repos/{owner}/{repo}/actions/runs/{run_id}`: the Actions run read
/// `github/codeql-action` performs while it derives an `analysis_key`.
///
/// The action asks for `GITHUB_RUN_ID`, which the engine renders as the run
/// UUID — Octokit coerces it with `Number(...)`, so the path segment is
/// literally `NaN` (or a numeric id on a GHES-shaped deployment). The
/// credential is authoritative here: the run is resolved from the caller's
/// job token, never from the segment, and only the fields the action reads
/// are returned. `workflow_url` is `null` so the action takes its documented
/// fallback to `path` instead of fetching the workflow again.
pub async fn github_actions_run(
    State(shared): State<Arc<SharedState>>,
    Path((owner, repo, _run_ref)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let Some(token) = header_token(&headers) else {
        let mut response = github_error_response(SarifError::unauthorized(
            "actions run read requires a job runtime token",
        ));
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            header::HeaderValue::from_static("Bearer"),
        );
        return response;
    };
    let run_id = match authorize_github_caller(&shared.state, &token, &owner, &repo, false).await {
        Ok((_, run_id)) => run_id,
        Err(error) => return github_error_response(error),
    };
    let run = match shared.state.backend.run_record(run_id).await {
        Ok(run) => run,
        Err(error) => return github_error_response(SarifError::from(error)),
    };
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json!({
            "id": 0,
            "run_id": run.run_id,
            "run_number": run.run_number,
            "run_attempt": run.run_attempt,
            "event": run.event,
            "status": "in_progress",
            "head_sha": run.head_sha,
            // The action falls back to `path` when `workflow_url` is absent.
            "path": run.workflow_path_str,
            "workflow_url": serde_json::Value::Null,
        })
        .to_string(),
    )
        .into_response()
}

// ── Native API handlers ─────────────────────────────────────────────────

/// `POST /api/v1/runs/{run_id}/sarif`: raw `application/sarif+json` or a JSON
/// wrapper (`{"sarif": <object|base64-gzip string>, …}`).
pub async fn upload_run_sarif(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    let caller = authorize_run_caller(&shared.state, &headers, run_id, true).await?;
    let run = shared
        .state
        .backend
        .run_record(run_id)
        .await
        .map_err(|error| match error {
            crate::control::ControlError::NotFound(_) => ApiError::not_found("run not found"),
            other => ApiError::from(other),
        })?;

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let content_encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let body = read_bounded_body(body, MAX_SARIF_REQUEST_BYTES).await?;

    // A gzip Transfer/Content-Encoding wraps whatever the body is.
    let body = if content_encoding
        .split(',')
        .any(|value| value.trim().eq_ignore_ascii_case("gzip"))
    {
        gunzip_bounded(&body)?
    } else {
        body
    };

    // Decide what the body is. `application/sarif+json` (and any other
    // `+json` subtype) is the document itself. `application/json` may be the
    // wrapper or a bare SARIF log — a SARIF log always carries `runs`. An
    // absent/unknown content type is sniffed the same way, so a client that
    // posts a bare document without a header still works.
    let declared = content_type.as_str();
    let bare_document = declared.ends_with("+json")
        || match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(value) => value.get("runs").is_some(),
            Err(_) => false,
        };
    if !declared.is_empty()
        && !declared.ends_with("+json")
        && declared != "application/json"
        && !declared.starts_with("text/")
    {
        return Err(SarifError::unsupported_media_type(format!(
            "unsupported content type {content_type:?}; send application/sarif+json or application/json"
        ))
        .into());
    }

    let mut payload = NativeSarifPayload::default();
    let mut document: Vec<u8> = body.clone();
    let mut compressed: Option<Vec<u8>> = None;
    if !bare_document {
        let wrapper: NativeSarifPayload = serde_json::from_slice(&body)
            .map_err(|error| SarifError::bad_request(format!("invalid JSON body: {error}")))?;
        match wrapper.sarif.as_ref() {
            Some(serde_json::Value::Object(_)) | Some(serde_json::Value::Array(_)) => {
                let value = wrapper.sarif.as_ref().expect("matched Some above");
                document = serde_json::to_vec(value)
                    .map_err(|error| ApiError::internal(error.to_string()))?;
            }
            Some(serde_json::Value::String(encoded)) => {
                let (decoded, gzipped) = decode_github_sarif(encoded)?;
                document = decoded;
                compressed = Some(gzipped);
            }
            Some(_) => {
                return Err(SarifError::bad_request(
                    "wrapper field `sarif` must be an object or a base64-encoded gzip string",
                )
                .into());
            }
            None => {
                document = body.clone();
            }
        }
        payload = wrapper;
    }
    if bare_document && declared.ends_with("+json") {
        // Fail before storage if the declared SARIF document is not JSON.
        if serde_json::from_slice::<serde_json::Value>(&document).is_err() {
            return Err(SarifError::bad_request("body is not valid JSON").into());
        }
    }

    let commit = trim_opt(payload.commit_sha.clone())
        .or_else(|| {
            let sha = run.submission.sha.trim();
            (!sha.is_empty()).then(|| sha.to_owned())
        })
        .or_else(|| {
            let sha = run.head_sha.trim();
            (!sha.is_empty()).then(|| sha.to_owned())
        })
        .ok_or_else(|| {
            SarifError::unprocessable("commit_sha is required when the run carries no head SHA")
        })?;
    if let Err(error) = validate_commit_sha(&commit) {
        return Err(error.into());
    }
    let git_ref =
        trim_opt(payload.git_ref.clone()).unwrap_or_else(|| run.submission.git_ref.clone());
    if let Err(error) = validate_git_ref(&git_ref) {
        return Err(error.into());
    }
    if compressed.is_none() {
        compressed = Some(gzip(&document)?);
    }

    let upload = SarifUpload {
        run_id,
        repository: run.submission.repository.clone(),
        commit_sha: commit,
        git_ref,
        tool_name: payload.tool(),
        checkout_uri: trim_opt(payload.checkout_uri.clone()),
        started_at: trim_opt(payload.started_at.clone()),
        analysis_key: trim_opt(payload.analysis_key.clone()),
        environment: trim_opt(payload.environment.clone()),
        uploader: caller.uploader,
        source: SarifAnalysisSource::Api,
        document,
        compressed,
    };
    let (record, inserted) = ingest_sarif(&shared.state, upload).await?;
    Ok((
        StatusCode::ACCEPTED,
        [(header::CONTENT_TYPE, "application/json")],
        json!({
            "analysis_id": record.analysis_id,
            "run_id": record.run_id,
            "status": record.status.as_str(),
            "finding_count": record.finding_count,
            "deduplicated": !inserted,
            "url": format!("/api/v1/sarif/{}", record.analysis_id),
        })
        .to_string(),
    )
        .into_response())
}

fn timestamp(us: i64) -> String {
    chrono::DateTime::from_timestamp_micros(us)
        .map(|value| value.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_default()
}

fn analysis_view(record: &SarifAnalysisRecord) -> serde_json::Value {
    json!({
        "analysis_id": record.analysis_id,
        "run_id": record.run_id,
        "repository": record.repository,
        "commit_sha": record.commit_sha,
        "ref": record.git_ref,
        "tool_name": record.tool_name,
        "source": record.source.as_str(),
        "status": record.status.as_str(),
        "uploader": record.uploader,
        "finding_count": record.finding_count,
        "size_bytes": record.size_bytes,
        "raw_size_bytes": record.raw_size_bytes,
        "checkout_uri": record.checkout_uri,
        "analysis_key": record.analysis_key,
        "environment": record.environment,
        "started_at": record.started_at,
        "created_at": timestamp(record.created_at_us),
        "completed_at": record.completed_at_us.map(timestamp),
    })
}

fn finding_view(finding: &SarifFinding) -> serde_json::Value {
    json!({
        "fingerprint": finding.fingerprint,
        "tool_name": finding.tool_name,
        "rule_id": finding.rule_id,
        "rule_name": finding.rule_name,
        "level": finding.level,
        "message": finding.message,
        "path": finding.path,
        "start_line": finding.start_line,
        "end_line": finding.end_line,
        "start_column": finding.start_column,
        "end_column": finding.end_column,
        "partial_fingerprint": finding.partial_fingerprint,
        "suppression": finding.suppression,
    })
}

/// `GET /api/v1/runs/{run_id}/sarif`: the run's stored analyses.
pub async fn list_run_sarif(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_run_caller(&shared.state, &headers, run_id, false).await?;
    let analyses = shared
        .state
        .backend
        .sarif_analyses(run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({
        "run_id": run_id,
        "analyses": analyses.iter().map(analysis_view).collect::<Vec<_>>(),
    })))
}

/// Authorize a read of one analysis: the system token, or a job token whose
/// run owns it.
async fn authorize_analysis_read(
    state: &AppState,
    headers: &HeaderMap,
    record: &SarifAnalysisRecord,
) -> Result<(), SarifError> {
    let token = header_token(headers)
        .ok_or_else(|| SarifError::unauthorized("job or system token required"))?;
    if token == state.system_token.as_str() {
        return Ok(());
    }
    let job = state
        .job_uuid_from_token(&token)
        .ok_or_else(|| SarifError::unauthorized("job or system token required"))?;
    let owned = state
        .backend
        .run_for_attempt(job)
        .await
        .map_err(SarifError::from)?;
    if owned != Some(record.run_id) {
        return Err(SarifError::forbidden(
            "job runtime token is not bound to this analysis's run",
        ));
    }
    Ok(())
}

/// `GET /api/v1/sarif/{analysis_id}`: metadata plus normalized findings.
pub async fn get_sarif_analysis(
    State(shared): State<Arc<SharedState>>,
    Path(analysis_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let record = shared
        .state
        .backend
        .sarif_analysis(&analysis_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("sarif analysis not found"))?;
    authorize_analysis_read(&shared.state, &headers, &record).await?;
    let findings = shared
        .state
        .backend
        .sarif_findings(&analysis_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({
        "analysis": analysis_view(&record),
        "findings": findings.iter().map(finding_view).collect::<Vec<_>>(),
    })))
}

/// `GET /api/v1/sarif/{analysis_id}/raw`: the stored SARIF 2.1.0 document.
pub async fn get_sarif_raw(
    State(shared): State<Arc<SharedState>>,
    Path(analysis_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let record = shared
        .state
        .backend
        .sarif_analysis(&analysis_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("sarif analysis not found"))?;
    authorize_analysis_read(&shared.state, &headers, &record).await?;
    let document = read_document(&shared.state, &record.storage_key).await?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/sarif+json")],
        document,
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document() -> Vec<u8> {
        json!({
            "version": "2.1.0",
            "runs": [{
                "tool": {
                    "driver": {
                        "name": "CodeQL",
                        "rules": [{
                            "id": "js/sql-injection",
                            "name": "SQL injection",
                            "defaultConfiguration": { "level": "error" }
                        }]
                    }
                },
                "results": [
                    {
                        "ruleId": "js/sql-injection",
                        "message": { "text": "Query built from user input" },
                        "locations": [{
                            "physicalLocation": {
                                "artifactLocation": { "uri": "file:///src/db.js" },
                                "region": { "startLine": 12, "endLine": 14, "startColumn": 3 }
                            }
                        }],
                        "partialFingerprints": { "primaryLocationLineHash": "abc:1" }
                    },
                    {
                        "ruleId": "js/sql-injection",
                        "level": "note",
                        "message": { "text": "Query built from user input" },
                        "locations": [{
                            "physicalLocation": {
                                "artifactLocation": { "uri": "src/db.js" },
                                "region": { "startLine": 12 }
                            }
                        }],
                        "partialFingerprints": { "primaryLocationLineHash": "abc:1" }
                    }
                ]
            }]
        })
        .to_string()
        .into_bytes()
    }

    #[test]
    fn parses_and_deduplicates_findings() {
        let parsed = parse_sarif(&document(), None).unwrap();
        assert_eq!(parsed.tool_name, "CodeQL");
        // Both results share a partial fingerprint: one normalized finding.
        assert_eq!(parsed.findings.len(), 1);
        let finding = &parsed.findings[0];
        assert_eq!(finding.rule_id.as_deref(), Some("js/sql-injection"));
        assert_eq!(finding.level, "error", "rule default level applies");
        assert_eq!(finding.path.as_deref(), Some("src/db.js"));
        assert_eq!(finding.start_line, Some(12));
        assert_eq!(finding.end_line, Some(14));
        assert_eq!(finding.start_column, Some(3));
        assert_eq!(
            finding.partial_fingerprint.as_deref(),
            Some("primaryLocationLineHash=abc:1")
        );
    }

    #[test]
    fn dedupe_key_ignores_byte_ordering() {
        let first = parse_sarif(&document(), None).unwrap();
        let mut reversed: serde_json::Value = serde_json::from_slice(&document()).unwrap();
        reversed["runs"][0]["results"]
            .as_array_mut()
            .unwrap()
            .reverse();
        let second = parse_sarif(&serde_json::to_vec(&reversed).unwrap(), None).unwrap();
        assert_eq!(
            analysis_dedupe_key(
                "o/r",
                "abc1234",
                "refs/heads/main",
                "CodeQL",
                &first.findings
            ),
            analysis_dedupe_key(
                "o/r",
                "abc1234",
                "refs/heads/main",
                "CodeQL",
                &second.findings
            ),
        );
    }

    #[test]
    fn rejects_non_sarif_and_wrong_version() {
        assert!(parse_sarif(b"not json", None).is_err());
        let wrong = br#"{"version":"2.0.0","runs":[]}"#;
        let error = parse_sarif(wrong, None).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("2.1.0"));
    }

    #[test]
    fn decodes_base64_gzip_uploads() {
        let plain = document();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&plain).unwrap();
        let gzipped = encoder.finish().unwrap();
        let encoded = BASE64_STANDARD.encode(&gzipped);
        let (decoded, compressed) = decode_github_sarif(&encoded).unwrap();
        assert_eq!(decoded, plain);
        assert_eq!(compressed, gzipped);
        assert_eq!(
            decode_github_sarif("!!!").unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn gunzip_refuses_a_decompression_bomb() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        let chunk = [0u8; 64 * 1024];
        let mut remaining = MAX_SARIF_DECOMPRESSED_BYTES + 1;
        while remaining > 0 {
            let n = remaining.min(chunk.len());
            encoder.write_all(&chunk[..n]).unwrap();
            remaining -= n;
        }
        let gzipped = encoder.finish().unwrap();
        assert_eq!(
            gunzip_bounded(&gzipped).unwrap_err().status,
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn authorization_accepts_octokit_and_checkout_forms() {
        assert_eq!(authorization_token("Bearer abc").as_deref(), Some("abc"));
        assert_eq!(authorization_token("token abc").as_deref(), Some("abc"));
        let basic = format!("Basic {}", BASE64_STANDARD.encode("x-access-token:abc"));
        assert_eq!(authorization_token(&basic).as_deref(), Some("abc"));
        assert!(authorization_token("Digest abc").is_none());
    }

    #[test]
    fn rejects_oversized_finding_sets() {
        let results: Vec<serde_json::Value> = (0..(MAX_SARIF_FINDINGS + 1))
            .map(|index| {
                json!({
                    "ruleId": "r",
                    "message": { "text": format!("finding {index}") },
                    "locations": [{ "physicalLocation": {
                        "artifactLocation": { "uri": "src/x.rs" },
                        "region": { "startLine": index + 1 }
                    }}],
                })
            })
            .collect();
        let document = serde_json::to_vec(&json!({
            "version": "2.1.0",
            "runs": [{ "tool": { "driver": { "name": "tool" } }, "results": results }]
        }))
        .unwrap();
        let error = parse_sarif(&document, None).unwrap_err();
        assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
    }
}
