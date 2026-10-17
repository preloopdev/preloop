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
//! `.credentials_rsaparams`. Two quirks of the server-emitted `.runner` are
//! handled here rather than in `settings.rs`: keys are PascalCase and values
//! are string-typed (`"AgentId": "1425"`, `"Ephemeral": "True"`), so the
//! document is normalized into our canonical `RunnerSettings` JSON before it
//! is written. A JIT materialized root is therefore indistinguishable from
//! `configure` output and `RunnerConfig::load` consumes it unchanged.
//!
//! JIT runners are ephemeral: GitHub mints them `Ephemeral: true`, so the
//! broker loop exits after the first job. An idle JIT runner may also be
//! reclaimed server-side — observed removals after a few idle minutes
//! (upstream actions/runner#4726 is still open on the exact limit) — so
//! `--jitconfig` is meant for provision-then-run-once hosts, not warm pools.

use anyhow::{Context, Result, bail};
use base64::Engine;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::path::Path;

use crate::settings::{
    CREDENTIALS_FILE, CredentialData, RSA_PARAMS_FILE, RUNNER_FILE, RsaParameters, RunnerConfig,
    RunnerSettings, restrict_permissions, save_json,
};

/// Decode an `encoded_jit_config` payload into its file map.
///
/// Outer layer: base64 → UTF-8 JSON object `{ "<file>": "<base64 content>" }`.
/// The official runner accepts any string-valued map and writes every entry
/// under the runner root; we additionally validate file names (relative
/// basenames only — the payload is trusted server material, but a basename
/// check costs nothing against a compromised or confused producer).
pub fn decode_file_map(encoded: &str) -> Result<BTreeMap<String, Vec<u8>>> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .context("JIT config is not valid base64")?;
    let text = String::from_utf8(decoded).context("JIT config payload is not UTF-8")?;
    let map: Map<String, Value> = serde_json::from_str(text.trim_start_matches('\u{FEFF}'))
        .context("JIT config payload is not a JSON object")?;

    let mut files = BTreeMap::new();
    for (name, value) in map {
        validate_file_name(&name)?;
        let encoded_content = value
            .as_str()
            .with_context(|| format!("JIT config entry `{name}` is not a base64 string"))?;
        let content = base64::engine::general_purpose::STANDARD
            .decode(encoded_content)
            .with_context(|| format!("JIT config entry `{name}` is not valid base64"))?;
        files.insert(name, content);
    }
    Ok(files)
}

/// Materialize a JIT config into `root` and return the resulting [`RunnerConfig`].
///
/// Writes `.runner`, `.credentials`, and `.credentials_rsaparams` (plus any
/// extra files the server included, verbatim) with 0600 permissions, matching
/// the official runner's write-the-files-then-run flow. Existing configuration
/// in `root` is replaced — the official runner does not guard on
/// `IsConfigured()` for JIT either.
pub fn materialize(encoded: &str, root: &Path) -> Result<RunnerConfig> {
    let files = decode_file_map(encoded)?;

    // Parse all three before writing anything: a bad payload errors cleanly
    // instead of leaving a half-materialized root.
    let settings = normalize_runner_settings(
        files
            .get(RUNNER_FILE)
            .context("JIT config is missing required file `.runner`")?,
    )?;
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
    for (name, content) in &files {
        let path = root.join(name);
        match name.as_str() {
            RUNNER_FILE => save_json(&path, &settings)?,
            CREDENTIALS_FILE => save_json(&path, &credentials)?,
            RSA_PARAMS_FILE => save_json(&path, &rsa_params)?,
            // Unknown extra files are written verbatim, like the official
            // runner — forward-compatible with any additional artifacts a
            // future server version starts shipping in the payload.
            _ => std::fs::write(&path, content)
                .with_context(|| format!("writing {}", path.display()))?,
        }
        restrict_permissions(&path)?;
    }

    Ok(RunnerConfig {
        settings,
        credentials,
        rsa_params,
    })
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
/// `.credentials` is camelCase in the wild, but the payload may carry
/// `Scheme`/`Data` casing from a server-side `CredentialData` POCO — accept
/// either. `data` keys are carried through verbatim (`clientId`,
/// `authorizationUrl`, `authorizationUrlV2`, `requireFipsCryptography`, …);
/// non-string scalars are stringified to match the official `Data` map of
/// `Dictionary<string,string>`.
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
                        k.clone(),
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

    fn sample_credentials() -> &'static str {
        r#"{
            "scheme": "OAuth",
            "data": {
                "clientId": "9f8e7d6c-5b4a-3928-8170-6f5e4d3c2b1a",
                "authorizationUrl": "https://vssps.example.com/_apis/oauth2/token",
                "requireFipsCryptography": "False"
            }
        }"#
    }

    fn sample_rsa_params() -> String {
        let keypair = preloop_gha_protocol::crypto::AgentRsaKeypair::generate().unwrap();
        serde_json::to_string(&keypair.to_rsaparams()).unwrap()
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
    fn normalize_credentials_accepts_pascal_case() {
        let creds = normalize_credentials(
            br#"{"Scheme": "OAuth", "Data": {"clientId": "abc", "requireFipsCryptography": true}}"#,
        )
        .unwrap();
        assert_eq!(creds.scheme, "OAuth");
        assert_eq!(creds.client_id(), Some("abc"));
        assert_eq!(
            creds.data.get("requireFipsCryptography").unwrap(),
            &Value::String("true".into())
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
