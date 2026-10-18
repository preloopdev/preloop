//! Just-in-time (JIT) runner configs — `preloop-runner run --jitconfig`.
//!
//! Mirrors `Runner.Listener/Runner.cs`: `--jitconfig` carries the
//! `encoded_jit_config` value returned by
//! `POST .../actions/runners/generate-jitconfig` (repo and org scopes).
//! The value base64-decodes to a JSON object mapping **file name →
//! base64-encoded file content**; the official runner writes each entry into
//! the runner root and then continues through the normal `run` path — no
//! `configure` POSTs ever happen because the runner was already registered
//! server-side when the config was minted.
//!
//! In practice the payload materializes `.runner`, `.credentials`, and
//! `.credentials_rsaparams`, and the three files use two different casings:
//! `.runner` and `.credentials` carry the service's PascalCase POCO keys with
//! string-typed values (`"AgentId": "1425"`, `"Ephemeral": "True"`,
//! `"Data": {"ClientId": …}`), while `.credentials_rsaparams` carries .NET's
//! web-default camelCase RSA fields (`d`, `dp`, `exponent`, `inverseQ`, …).
//! Both are normalized into our canonical JSON before anything is written, so
//! a JIT materialized root is indistinguishable from `configure` output and
//! `RunnerConfig::load` consumes it unchanged.
//!
//! JIT runners are single-use: GitHub mints them `Ephemeral: true` and the
//! broker loop exits after the first job, so `materialize` forces the flag
//! rather than trusting a payload that omits it. An idle JIT runner may also
//! be reclaimed server-side — observed removals after a few idle minutes
//! (upstream actions/runner#4726 is still open on the exact limit) — so
//! `--jitconfig` is meant for provision-then-run-once hosts, not warm pools.

use anyhow::{Context, Result, bail};
use base64::Engine;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::settings::{
    CREDENTIALS_FILE, CredentialData, RSA_PARAMS_FILE, RUNNER_FILE, RsaParameters, RunnerConfig,
    RunnerSettings, save_json, write_private,
};

/// Largest accepted `--jitconfig` argument. A real payload is a few KB (three
/// small JSON documents); the cap only exists so a hostile or corrupt value
/// cannot make the runner allocate unbounded memory before it starts.
const MAX_ENCODED_LEN: usize = 1 << 20;
/// Largest accepted decoded size for one file in the payload.
const MAX_FILE_LEN: usize = 256 << 10;
/// Largest accepted total decoded size across every file in the payload.
const MAX_TOTAL_LEN: usize = 1 << 20;

/// Decode an `encoded_jit_config` payload into its file map.
///
/// Outer layer: base64 → UTF-8 JSON object `{ "<file>": "<base64 content>" }`.
/// The official runner accepts any string-valued map and writes every entry
/// under the runner root; we additionally validate file names (relative
/// basenames only — the payload is trusted server material, but a basename
/// check costs nothing against a compromised or confused producer) and bound
/// the decoded size.
pub fn decode_file_map(encoded: &str) -> Result<BTreeMap<String, Vec<u8>>> {
    if encoded.len() > MAX_ENCODED_LEN {
        bail!(
            "JIT config is {} bytes; the limit is {MAX_ENCODED_LEN}",
            encoded.len()
        );
    }
    let decoded = decode_base64(encoded).context("JIT config is not valid base64")?;
    let text = String::from_utf8(decoded).context("JIT config payload is not UTF-8")?;
    let map: Map<String, Value> = serde_json::from_str(text.trim_start_matches('\u{FEFF}'))
        .context("JIT config payload is not a JSON object")?;

    let mut files = BTreeMap::new();
    let mut total_len = 0usize;
    for (name, value) in map {
        validate_file_name(&name)?;
        let encoded_content = value
            .as_str()
            .with_context(|| format!("JIT config entry `{name}` is not a base64 string"))?;
        let content = decode_base64(encoded_content)
            .with_context(|| format!("JIT config entry `{name}` is not valid base64"))?;
        if content.len() > MAX_FILE_LEN {
            bail!(
                "JIT config entry `{name}` decodes to {} bytes; the limit is {MAX_FILE_LEN}",
                content.len()
            );
        }
        total_len += content.len();
        if total_len > MAX_TOTAL_LEN {
            bail!("JIT config decodes to more than {MAX_TOTAL_LEN} bytes in total");
        }
        files.insert(name, content);
    }
    Ok(files)
}

/// Base64-decode as `Convert.FromBase64String` does: embedded whitespace —
/// the newlines a line-wrapped environment variable carries — is ignored.
fn decode_base64(encoded: &str) -> std::result::Result<Vec<u8>, base64::DecodeError> {
    let compact: String = encoded
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    base64::engine::general_purpose::STANDARD.decode(compact)
}

/// Materialize a JIT config into `root` and return the resulting [`RunnerConfig`].
///
/// Writes `.runner`, `.credentials`, and `.credentials_rsaparams` (plus any
/// extra files the server included, verbatim) with 0600 permissions, matching
/// the official runner's write-the-files-then-run flow.
///
/// Existing configuration in `root` is replaced — the official runner does
/// not guard on `IsConfigured()` for JIT either — but the replacement is
/// loud, and a failure while writing removes the files this call committed
/// rather than leaving a half-materialized root holding credentials.
pub fn materialize(encoded: &str, root: &Path) -> Result<RunnerConfig> {
    let files = decode_file_map(encoded)?;

    // Parse all three before writing anything: a bad payload errors cleanly
    // instead of leaving a half-materialized root.
    let mut settings = normalize_runner_settings(
        files
            .get(RUNNER_FILE)
            .context("JIT config is missing required file `.runner`")?,
    )?;
    // Single-use is the JIT contract, not a property of the payload: the
    // listener already forces `--once`, and the broker's `job-not-found` exit
    // keys on `ephemeral` alone (Runner.cs `when (settings.Ephemeral)`), so a
    // payload that omits the flag — or an older server that sends `False` —
    // must not leave this runner polling after its one job.
    if !settings.ephemeral {
        settings.ephemeral = true;
    }
    let credentials = normalize_credentials(
        files
            .get(CREDENTIALS_FILE)
            .context("JIT config is missing required file `.credentials`")?,
    )?;
    let rsa_params = parse_rsa_params(
        files
            .get(RSA_PARAMS_FILE)
            .context("JIT config is missing required file `.credentials_rsaparams`")?,
    )?;

    std::fs::create_dir_all(root)
        .with_context(|| format!("creating runner root {}", root.display()))?;
    if RunnerConfig::is_configured(root) {
        tracing::warn!(
            "JIT config replaces the runner configuration already present in {} \
             (the official runner overwrites it too); the previous registration \
             is gone when this JIT runner exits",
            root.display()
        );
    }

    // Stage every file beside its destination, then rename the whole set into
    // place. Renames are metadata-only, so the failure mode that matters —
    // running out of space while writing — leaves the root untouched.
    Stage::write(root, &files, &settings, &credentials, &rsa_params)?.commit()?;

    Ok(RunnerConfig {
        settings,
        credentials,
        rsa_params,
    })
}

/// Files written for one JIT materialization, removed again unless committed.
///
/// Every file is staged next to its destination so a failing write cannot
/// leave a partial `.runner` behind, and a failure partway through the rename
/// set removes what this call already replaced.
struct Stage {
    /// `(temp, destination)` pairs in payload order.
    staged: Vec<(PathBuf, PathBuf)>,
    /// How many leading entries of `staged` are renamed into place.
    committed: usize,
}

impl Stage {
    fn write(
        root: &Path,
        files: &BTreeMap<String, Vec<u8>>,
        settings: &RunnerSettings,
        credentials: &CredentialData,
        rsa_params: &RsaParameters,
    ) -> Result<Self> {
        let mut stage = Self {
            staged: Vec::new(),
            committed: 0,
        };
        for (name, content) in files {
            let destination = root.join(name);
            let temp =
                destination.with_file_name(format!(".{name}.jit-{}.tmp", std::process::id()));
            let result = match name.as_str() {
                RUNNER_FILE => save_json(&temp, settings),
                CREDENTIALS_FILE => save_json(&temp, credentials),
                RSA_PARAMS_FILE => save_json(&temp, rsa_params),
                // Unknown extra files are written verbatim, like the official
                // runner — forward-compatible with any additional artifacts a
                // future server version starts shipping in the payload.
                _ => write_private(&temp, content),
            };
            stage.staged.push((temp, destination));
            if let Err(error) = result {
                stage.rollback();
                return Err(error);
            }
        }
        Ok(stage)
    }

    /// Rename every staged file into place, or undo everything staged.
    fn commit(mut self) -> Result<()> {
        while self.committed < self.staged.len() {
            let (temp, destination) = &self.staged[self.committed];
            if let Err(error) = std::fs::rename(temp, destination)
                .with_context(|| format!("replacing {}", destination.display()))
            {
                self.rollback();
                return Err(error);
            }
            self.committed += 1;
        }
        Ok(())
    }

    /// Best-effort removal of every staged temp file plus the destinations
    /// this call already replaced. A renamed temp no longer exists, so
    /// removing it twice is harmless.
    fn rollback(&self) {
        for (temp, _) in &self.staged {
            remove_quietly(temp);
        }
        for (_, destination) in self.staged.iter().take(self.committed) {
            remove_quietly(destination);
        }
    }
}

fn remove_quietly(path: &Path) {
    if let Err(error) = std::fs::remove_file(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!("failed to clean up {}: {error}", path.display());
    }
}

/// Reject anything that is not a relative basename: no separators, no parent
/// traversal, no absolute paths, no drive-relative names.
fn validate_file_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.starts_with('/')
        || name.starts_with('\\')
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
        || name.chars().nth(1) == Some(':')
    {
        bail!("JIT config file name `{name}` is not a safe basename");
    }
    Ok(())
}

/// `PascalCase`/`camelCase` key → `snake_case` for our field matching.
fn to_snake_case(key: &str) -> String {
    let mut out = String::with_capacity(key.len() + 4);
    let mut prev_lower_or_digit = false;
    let chars: Vec<char> = key.chars().collect();
    for (i, ch) in chars.iter().enumerate() {
        if ch.is_uppercase() {
            let next_lower = chars.get(i + 1).is_some_and(|next| next.is_lowercase());
            if i > 0 && (prev_lower_or_digit || next_lower) {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
            prev_lower_or_digit = false;
        } else {
            out.push(*ch);
            prev_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        }
    }
    out
}

/// `snake_case` field → the `camelCase` key used by [`RunnerSettings`]' serde
/// representation (for example, `git_hub_url` → `gitHubUrl`).
fn to_camel_case(field: &str) -> String {
    let mut parts = field.split('_');
    let Some(first) = parts.next() else {
        return String::new();
    };
    let mut out = first.to_string();
    for part in parts {
        if let Some(first) = part.chars().next() {
            out.push(first.to_ascii_uppercase());
            out.extend(part.chars().skip(1));
        }
    }
    out
}

/// Coerce a JIT string-typed value to the JSON type our schema expects.
///
/// The server serializes `.runner` values through `ToString()` — numbers and
/// booleans arrive as `"1425"` / `"True"`. Booleans tolerate the .NET
/// `True`/`False` spellings and JSON `true`/`false`.
fn coerce_scalar(field: &str, value: &Value) -> Value {
    const STRING_FIELDS: &[&str] = &[
        "agent_name",
        "pool_name",
        "server_url",
        "git_hub_url",
        "work_folder",
    ];
    const BOOL_FIELDS: &[&str] = &[
        "ephemeral",
        "is_hosted",
        "is_hosted_server",
        "use_v2_flow",
        "disable_update",
        "skip_session_recover",
        "use_runner_admin_flow",
    ];
    const INT_FIELDS: &[&str] = &["agent_id", "pool_id", "runner_group_id"];

    if STRING_FIELDS.contains(&field) && value.is_null() {
        return Value::String(String::new());
    }
    if BOOL_FIELDS.contains(&field) {
        return match value {
            Value::String(s) => match s.to_ascii_lowercase().as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                _ => value.clone(),
            },
            _ => value.clone(),
        };
    }
    if INT_FIELDS.contains(&field) {
        return match value {
            Value::String(s) => s
                .parse::<i64>()
                .map(|n| Value::Number(n.into()))
                .unwrap_or_else(|_| value.clone()),
            _ => value.clone(),
        };
    }
    value.clone()
}

/// Parse the server-emitted `.runner` document into [`RunnerSettings`].
///
/// JIT `.runner` uses PascalCase keys with string-typed values. Keys are
/// normalized to snake_case (covers both `AgentId` and `agentId` spellings)
/// and scalars coerced, so a payload produced by the official
/// `generate-jitconfig` endpoint yields the same `RunnerSettings` that
/// `configure` persists.
fn normalize_runner_settings(content: &[u8]) -> Result<RunnerSettings> {
    let text = std::str::from_utf8(content)
        .context("JIT `.runner` content is not UTF-8")?
        .trim_start_matches('\u{FEFF}');
    let object: Map<String, Value> =
        serde_json::from_str(text).context("JIT `.runner` content is not a JSON object")?;

    let mut normalized = Map::new();
    for (key, value) in object {
        let field = to_snake_case(&key);
        normalized.insert(to_camel_case(&field), coerce_scalar(&field, &value));
    }
    // Older JIT payloads omit GitHubUrl; it is not needed by the listener
    // after the server has already registered the runner.
    normalized
        .entry("gitHubUrl".to_string())
        .or_insert_with(|| Value::String(String::new()));

    serde_json::from_value(Value::Object(normalized))
        .context("JIT `.runner` does not match the runner settings schema")
}

/// Parse the server-emitted `.credentials` document into [`CredentialData`].
///
/// The JIT payload carries the service's `CredentialData` POCO spelling
/// (`Scheme`, `Data`, and `Data` keys `ClientId`/`AuthorizationUrl`), while a
/// configured runner reads the camelCase spelling the official runner writes.
/// Both outer and nested keys are canonicalized to camelCase, which is what
/// the OAuth path and `requireFipsCryptography` lookups expect — upstream gets
/// the same tolerance from a `Dictionary<string,string>` built with
/// `StringComparer.OrdinalIgnoreCase`. Non-string scalars are stringified to
/// match the official `Data` map of `Dictionary<string,string>`.
fn normalize_credentials(content: &[u8]) -> Result<CredentialData> {
    let text = std::str::from_utf8(content)
        .context("JIT `.credentials` content is not UTF-8")?
        .trim_start_matches('\u{FEFF}');
    let object: Map<String, Value> =
        serde_json::from_str(text).context("JIT `.credentials` content is not a JSON object")?;

    let mut scheme = None;
    let mut data = Map::new();
    for (key, value) in object {
        match to_snake_case(&key).as_str() {
            "scheme" => {
                scheme = Some(
                    value
                        .as_str()
                        .context("JIT `.credentials` scheme is not a string")?
                        .to_string(),
                );
            }
            "data" => {
                let entries = value
                    .as_object()
                    .context("JIT `.credentials` data is not an object")?;
                for (k, v) in entries {
                    data.insert(
                        canonical_data_key(k),
                        match v {
                            Value::String(_) => v.clone(),
                            other => Value::String(other.to_string()),
                        },
                    );
                }
            }
            _ => {}
        }
    }

    Ok(CredentialData {
        scheme: scheme.context("JIT `.credentials` is missing `scheme`")?,
        data,
    })
}

/// `ClientId`/`clientId`/`client_id` → `clientId` (idempotent for keys the
/// official runner already writes in camelCase).
fn canonical_data_key(key: &str) -> String {
    to_camel_case(&to_snake_case(key))
}

fn parse_rsa_params(content: &[u8]) -> Result<RsaParameters> {
    let text = std::str::from_utf8(content)
        .context("JIT `.credentials_rsaparams` content is not UTF-8")?
        .trim_start_matches('\u{FEFF}');
    serde_json::from_str(text)
        .context("JIT `.credentials_rsaparams` does not match the RSA parameters schema")
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD;

    fn encode(content: &[u8]) -> String {
        STANDARD.encode(content)
    }

    fn sample_runner_settings() -> &'static str {
        r#"{
            "AgentId": "1425",
            "AgentName": "jit-runner-1",
            "PoolId": "1",
            "PoolName": null,
            "ServerUrl": "https://pipelines.actions.githubusercontent.com/abc/",
            "GitHubUrl": "https://github.com/org/repo",
            "WorkFolder": "_work",
            "Ephemeral": "True",
            "DisableUpdate": "True",
            "UseV2Flow": "true",
            "ServerUrlV2": "https://broker.actions.githubusercontent.com/",
            "RunnerGroupId": "1",
            "RunnerGroupName": "Default"
        }"#
    }

    /// The `.credentials` a minted JIT config carries: the service's POCO
    /// spelling (`Scheme`/`Data`, `ClientId`/`AuthorizationUrl`).
    fn sample_credentials() -> &'static str {
        r#"{
            "Scheme": "OAuth",
            "Data": {
                "ClientId": "9f8e7d6c-5b4a-3928-8170-6f5e4d3c2b1a",
                "AuthorizationUrl": "https://vssps.example.com/_apis/oauth2/token",
                "RequireFipsCryptography": "False"
            }
        }"#
    }

    /// RSA parameters in the casing the service emits: .NET's web-default
    /// camelCase (`d`, `dp`, `dq`, `exponent`, `inverseQ`, `modulus`, `p`, `q`).
    fn sample_rsa_params() -> String {
        let keypair = preloop_gha_protocol::crypto::AgentRsaKeypair::generate().unwrap();
        let lowercase = serde_json::to_value(keypair.to_rsaparams()).unwrap();
        let object = lowercase.as_object().unwrap();
        let mut renamed = Map::new();
        for (key, value) in object {
            let camel = match key.as_str() {
                "D" => "d",
                "DP" => "dp",
                "DQ" => "dq",
                "Exponent" => "exponent",
                "InverseQ" => "inverseQ",
                "Modulus" => "modulus",
                "P" => "p",
                "Q" => "q",
                other => panic!("unexpected RSA parameter {other}"),
            };
            renamed.insert(camel.to_string(), value.clone());
        }
        serde_json::to_string(&renamed).unwrap()
    }

    fn sample_jitconfig() -> String {
        let map = serde_json::json!({
            ".runner": encode(sample_runner_settings().as_bytes()),
            ".credentials": encode(sample_credentials().as_bytes()),
            ".credentials_rsaparams": encode(sample_rsa_params().as_bytes()),
        });
        STANDARD.encode(serde_json::to_vec(&map).unwrap())
    }

    #[test]
    fn decode_rejects_malformed_base64() {
        let err = decode_file_map("!!!not-base64!!!").unwrap_err();
        assert!(err.to_string().contains("base64"), "got: {err:#}");
    }

    #[test]
    fn decode_rejects_truncated_base64() {
        let err = decode_file_map("eyJ").unwrap_err();
        assert!(err.to_string().contains("base64"), "got: {err:#}");
    }

    #[test]
    fn decode_rejects_base64_of_non_json() {
        let err = decode_file_map(&encode(b"hello world")).unwrap_err();
        assert!(err.to_string().contains("JSON"), "got: {err:#}");
    }

    #[test]
    fn decode_rejects_json_non_object() {
        let err = decode_file_map(&encode(b"[1,2,3]")).unwrap_err();
        assert!(err.to_string().contains("JSON"), "got: {err:#}");
    }

    #[test]
    fn decode_rejects_non_base64_entry() {
        let map = serde_json::json!({".runner": "%%%not-base64%%%"});
        let err =
            decode_file_map(&encode(serde_json::to_vec(&map).unwrap().as_slice())).unwrap_err();
        assert!(err.to_string().contains("`.runner`"), "got: {err:#}");
    }

    #[test]
    fn decode_rejects_traversal_file_names() {
        for name in ["../evil", "/abs/path", "a/b", "..\\evil", "C:/evil"] {
            let map = serde_json::json!({name: encode(b"x")});
            let err =
                decode_file_map(&encode(serde_json::to_vec(&map).unwrap().as_slice())).unwrap_err();
            assert!(err.to_string().contains(name), "got: {err:#}");
        }
    }

    #[test]
    fn materialize_missing_runner_file_errors() {
        let dir = tempfile::TempDir::new().unwrap();
        let map = serde_json::json!({
            ".credentials": encode(sample_credentials().as_bytes()),
        });
        let encoded = STANDARD.encode(serde_json::to_vec(&map).unwrap());
        let err = materialize(&encoded, dir.path()).unwrap_err();
        assert!(err.to_string().contains("`.runner`"), "got: {err:#}");
    }

    #[test]
    fn materialize_hydrates_config_and_files() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = materialize(&sample_jitconfig(), dir.path()).unwrap();

        assert_eq!(config.settings.agent_id, 1425);
        assert_eq!(config.settings.agent_name, "jit-runner-1");
        assert_eq!(config.settings.pool_id, 1);
        assert!(config.settings.pool_name.is_empty());
        assert_eq!(
            config.settings.server_url,
            "https://pipelines.actions.githubusercontent.com/abc/"
        );
        assert_eq!(config.settings.git_hub_url, "https://github.com/org/repo");
        assert!(config.settings.ephemeral);
        assert!(config.settings.disable_update);
        assert!(config.settings.use_v2_flow);
        assert_eq!(
            config.settings.server_url_v2.as_deref(),
            Some("https://broker.actions.githubusercontent.com/")
        );
        assert_eq!(config.settings.runner_group_id, Some(1));
        assert_eq!(config.credentials.scheme, "OAuth");
        assert_eq!(
            config.credentials.client_id(),
            Some("9f8e7d6c-5b4a-3928-8170-6f5e4d3c2b1a")
        );
        assert_eq!(
            config.credentials.authorization_url(),
            Some("https://vssps.example.com/_apis/oauth2/token")
        );

        // Files on disk are canonical configure-convention JSON: the strict
        // `RunnerConfig::load` path reads them back identically.
        let reloaded = RunnerConfig::load(dir.path()).unwrap();
        assert_eq!(reloaded.settings.agent_id, 1425);
        assert_eq!(reloaded.credentials.scheme, "OAuth");
        assert_eq!(reloaded.rsa_params.exponent, config.rsa_params.exponent);

        // Credential material lands owner-only, like `configure` output.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for name in [".runner", ".credentials", ".credentials_rsaparams"] {
                let mode = std::fs::metadata(dir.path().join(name))
                    .unwrap()
                    .permissions()
                    .mode();
                assert_eq!(mode & 0o777, 0o600, "{name} mode");
            }
        }

        // The JIT RSA key is importable — the listener signs OAuth client
        // assertions with it.
        let keypair =
            preloop_gha_protocol::crypto::AgentRsaKeypair::from_rsaparams(&config.rsa_params)
                .unwrap();
        assert!(!keypair.public_key_xml().is_empty());
    }

    #[test]
    fn materialize_writes_unknown_files_verbatim() {
        let dir = tempfile::TempDir::new().unwrap();
        let map = serde_json::json!({
            ".runner": encode(sample_runner_settings().as_bytes()),
            ".credentials": encode(sample_credentials().as_bytes()),
            ".credentials_rsaparams": encode(sample_rsa_params().as_bytes()),
            ".runner_session": encode(b"raw\x00bytes\xff"),
        });
        let encoded = STANDARD.encode(serde_json::to_vec(&map).unwrap());
        materialize(&encoded, dir.path()).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join(".runner_session")).unwrap(),
            b"raw\x00bytes\xff"
        );
    }

    #[test]
    fn normalize_runner_settings_accepts_typed_camel_case() {
        // Defensive: a payload carrying our own canonical shape also parses.
        let settings = normalize_runner_settings(
            br#"{"agentId": 7, "agentName": "n", "poolId": 2, "serverUrl": "s",
                 "gitHubUrl": "g", "workFolder": "w", "ephemeral": true}"#,
        )
        .unwrap();
        assert_eq!(settings.agent_id, 7);
        assert!(settings.ephemeral);
    }

    #[test]
    fn normalize_runner_settings_rejects_missing_required() {
        let err = normalize_runner_settings(br#"{"AgentName": "only-name"}"#).unwrap_err();
        assert!(err.to_string().contains("schema"), "got: {err:#}");
    }

    #[test]
    fn normalize_credentials_canonicalizes_data_keys() {
        // The payload's PascalCase `Data` keys must not survive into the
        // credential map the OAuth path reads.
        let creds = normalize_credentials(
            br#"{"Scheme": "OAuth", "Data": {"ClientId": "abc", "AuthorizationUrl": "https://a",
                 "RequireFipsCryptography": true}}"#,
        )
        .unwrap();
        assert_eq!(creds.scheme, "OAuth");
        assert_eq!(creds.client_id(), Some("abc"));
        assert_eq!(creds.authorization_url(), Some("https://a"));
        assert_eq!(
            creds.data.get("requireFipsCryptography").unwrap(),
            &Value::String("true".into())
        );
        assert!(
            !creds.data.contains_key("ClientId") && !creds.data.contains_key("AuthorizationUrl"),
            "data keys must be canonical camelCase: {:?}",
            creds.data
        );
    }

    #[test]
    fn normalize_credentials_accepts_camel_case() {
        // Defensive: the shape our own `configure` writes also parses.
        let creds = normalize_credentials(
            br#"{"scheme": "OAuth", "data": {"clientId": "abc", "authorizationUrlV2": "https://v2"}}"#,
        )
        .unwrap();
        assert_eq!(creds.client_id(), Some("abc"));
        assert_eq!(
            creds.data.get("authorizationUrlV2"),
            Some(&Value::String("https://v2".into()))
        );
    }

    #[test]
    fn parse_rsa_params_accepts_both_casings() {
        let pascal = br#"{"D":"a","DP":"b","DQ":"c","Exponent":"d","InverseQ":"e",
                          "Modulus":"f","P":"g","Q":"h"}"#;
        let camel = br#"{"d":"a","dp":"b","dq":"c","exponent":"d","inverseQ":"e",
                          "modulus":"f","p":"g","q":"h"}"#;
        for content in [&pascal[..], &camel[..]] {
            let params = parse_rsa_params(content).unwrap();
            assert_eq!(params.d, "a");
            assert_eq!(params.dp, "b");
            assert_eq!(params.dq, "c");
            assert_eq!(params.exponent, "d");
            assert_eq!(params.inverse_q, "e");
            assert_eq!(params.modulus, "f");
            assert_eq!(params.p, "g");
            assert_eq!(params.q, "h");
        }
    }

    #[test]
    fn decode_ignores_embedded_whitespace() {
        // `Convert.FromBase64String` ignores whitespace, so a payload from a
        // line-wrapped environment variable must decode too.
        let payload = sample_jitconfig();
        let wrapped = format!("{}\n{}\n", &payload[..16], &payload[16..]);
        assert_eq!(decode_file_map(&wrapped).unwrap().len(), 3);
    }

    #[test]
    fn decode_rejects_oversized_payload() {
        let encoded = "A".repeat(MAX_ENCODED_LEN + 1);
        let err = decode_file_map(&encoded).unwrap_err();
        assert!(err.to_string().contains("limit"), "got: {err:#}");
    }

    #[test]
    fn decode_rejects_oversized_entry() {
        let map = serde_json::json!({".runner": encode(&vec![b'a'; MAX_FILE_LEN + 1])});
        let encoded = STANDARD.encode(serde_json::to_vec(&map).unwrap());
        let err = decode_file_map(&encoded).unwrap_err();
        assert!(err.to_string().contains("`.runner`"), "got: {err:#}");
        assert!(err.to_string().contains("limit"), "got: {err:#}");
    }

    #[test]
    fn materialize_forces_ephemeral() {
        let dir = tempfile::TempDir::new().unwrap();
        let runner =
            sample_runner_settings().replace("\"Ephemeral\": \"True\"", "\"Ephemeral\": \"False\"");
        let map = serde_json::json!({
            ".runner": encode(runner.as_bytes()),
            ".credentials": encode(sample_credentials().as_bytes()),
            ".credentials_rsaparams": encode(sample_rsa_params().as_bytes()),
        });
        let encoded = STANDARD.encode(serde_json::to_vec(&map).unwrap());
        let config = materialize(&encoded, dir.path()).unwrap();
        // A JIT registration is single-use whether or not the payload says so.
        assert!(config.settings.ephemeral);
        assert!(RunnerConfig::load(dir.path()).unwrap().settings.ephemeral);
    }

    #[test]
    fn materialize_replaces_existing_configuration() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join(".runner"), br#"{"agentName":"older"}"#).unwrap();
        std::fs::write(dir.path().join(".credentials"), b"stale").unwrap();
        let config = materialize(&sample_jitconfig(), dir.path()).unwrap();
        assert_eq!(config.settings.agent_name, "jit-runner-1");
        assert_eq!(
            RunnerConfig::load(dir.path()).unwrap().settings.agent_name,
            "jit-runner-1"
        );
    }

    #[test]
    fn materialize_rolls_back_on_failure() {
        let dir = tempfile::TempDir::new().unwrap();
        // A directory at a destination path makes the final rename fail after
        // the payload's earlier files were already renamed into place.
        std::fs::create_dir(dir.path().join(".credentials_rsaparams")).unwrap();
        let err = materialize(&sample_jitconfig(), dir.path()).unwrap_err();
        assert!(
            err.to_string().contains(".credentials_rsaparams"),
            "got: {err:#}"
        );
        for name in [".runner", ".credentials"] {
            assert!(
                !dir.path().join(name).exists(),
                "{name} survived a failed materialization"
            );
        }
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".jit-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "staged temps left behind: {leftovers:?}"
        );
    }

    #[test]
    fn to_snake_case_conversions() {
        for (input, expected) in [
            ("AgentId", "agent_id"),
            ("agentId", "agent_id"),
            ("ServerUrlV2", "server_url_v2"),
            ("GitHubUrl", "git_hub_url"),
            ("UseV2Flow", "use_v2_flow"),
            ("RSAParameters", "rsa_parameters"),
        ] {
            assert_eq!(to_snake_case(input), expected, "{input}");
        }
    }
}
