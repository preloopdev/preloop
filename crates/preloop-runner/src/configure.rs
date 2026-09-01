//! Runner configuration and registration.
//!
//! Implements the `configure` and `remove` subcommands, handling:
//! - Registration via the GitHub runner-registration API
//! - RSA keypair generation and persistence
//! - Node.js externals download (unless --no-externals)
//! - Agent creation/deletion via the distributedtask API

use std::future::Future;

use anyhow::{bail, Context, Result};
use tracing::{info, warn};

use crate::cli::{ConfigureArgs, GlobalArgs, RemoveArgs};
use crate::client::http::HttpClient;
use crate::node_externals;
use crate::settings::{CredentialData, RsaParameters, RunnerConfig, RunnerSettings};

const DISTTASK_POOLS_ACCEPT: &str = "application/json; api-version=5.1-preview.1";
const DISTTASK_AGENT_ACCEPT: &str = "application/json; api-version=6.0-preview.2";
const DISTTASK_AGENT_CONTENT_TYPE: &str =
    "application/json; charset=utf-8; api-version=6.0-preview.2";

/// Environment variable carrying a pre-generated keypair as `RSAParameters` JSON.
///
/// Generating a 2048-bit RSA key costs 70-180 ms, and for a pool of
/// single-use runners that lands squarely on the path between a job arriving
/// and a runner being ready for the next one. A supervising orchestrator can
/// generate keys ahead of time and hand one over instead. Each runner still
/// gets its own key; only the timing changes.
pub const RSA_PARAMS_ENV: &str = "PRELOOP_RUNNER_RSA_PARAMS";

/// Read a caller-supplied keypair, if one was injected.
///
/// A malformed value is a configuration error rather than a reason to
/// silently fall back: falling back would hide the failure behind a slower
/// runner that still works.
fn supplied_keypair() -> Result<Option<preloop_gha_protocol::crypto::AgentRsaKeypair>> {
    let Ok(raw) = std::env::var(RSA_PARAMS_ENV) else {
        return Ok(None);
    };
    if raw.trim().is_empty() {
        return Ok(None);
    }
    let params: preloop_gha_protocol::crypto::RsaParametersExport =
        serde_json::from_str(raw.trim())
            .with_context(|| format!("parsing {RSA_PARAMS_ENV} as RSAParameters JSON"))?;
    let keypair = preloop_gha_protocol::crypto::AgentRsaKeypair::from_rsaparams(&params)
        .map_err(|e| anyhow::anyhow!("importing keypair from {RSA_PARAMS_ENV}: {e}"))?;
    info!("Using pre-generated RSA keypair from {RSA_PARAMS_ENV}");
    Ok(Some(keypair))
}

/// Run the `configure` subcommand.
pub async fn run_configure(args: ConfigureArgs, global: &GlobalArgs) -> Result<()> {
    let root = global.runner_root();
    info!("Configuring runner in {}", root.display());
    std::fs::create_dir_all(&root)
        .with_context(|| format!("creating runner root {}", root.display()))?;

    // Check for existing configuration
    if RunnerConfig::is_configured(&root) && !args.replace {
        bail!(
            "Runner is already configured in {}. Use --replace to overwrite.",
            root.display()
        );
    }

    let http = HttpClient::new(global.ca_bundle.as_deref())?;

    // Step 1: Runner registration — get OAuth token and service URL
    let mut registration = register_runner(&http, &args.url, &args.token).await?;
    // In TCP upstream mode the server returns loopback URLs in the service_url
    // but the runner must reach it via the upstream LAN address during
    // configure (the loopback bridge is not yet running). Rewrite transparently.
    registration.service_url = http.rewrite_url(&registration.service_url).into_owned();
    info!(
        "Registration successful, service URL: {}",
        registration.service_url
    );

    // Step 2: Fetch connection data. The official runner sends the full
    // connectOptions=1 response with cached location-service change IDs on
    // later configurations, matching the official runner's warm path.
    let cache_path = root.join(".connectionData");
    let cached_ids = std::fs::read_to_string(&cache_path)
        .ok()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        .and_then(|value| {
            Some((
                value.get("lastChangeId")?.as_i64()?,
                value.get("lastChangeId64")?.as_i64()?,
            ))
        });
    let connection_url = if let Some((last_change_id, last_change_id64)) = cached_ids {
        format!(
            "{}/_apis/connectionData?connectOptions=0&lastChangeId={last_change_id}&lastChangeId64={last_change_id64}",
            registration.service_url
        )
    } else {
        format!(
            "{}/_apis/connectionData?connectOptions=0",
            registration.service_url
        )
    };
    let conn_data = http
        .get_json::<serde_json::Value>(&connection_url)
        .await
        .context("fetching connectionData")?;
    if let Some(location) = conn_data.get("locationServiceData") {
        if let (Some(last_change_id), Some(last_change_id64)) = (
            location
                .get("lastChangeId")
                .and_then(serde_json::Value::as_i64),
            location
                .get("lastChangeId64")
                .and_then(serde_json::Value::as_i64),
        ) {
            let cache = serde_json::json!({
                "lastChangeId": last_change_id,
                "lastChangeId64": last_change_id64,
            });
            std::fs::write(&cache_path, serde_json::to_vec_pretty(&cache)?)
                .with_context(|| format!("writing {}", cache_path.display()))?;
        }
    }

    // Step 3: Obtain the RSA keypair
    let keypair = supplied_keypair()?.map_or_else(
        || {
            preloop_gha_protocol::crypto::AgentRsaKeypair::generate()
                .map_err(|e| anyhow::anyhow!("generating RSA keypair: {e}"))
        },
        Ok,
    )?;
    let (rsa_params, public_key_xml) = export_keypair(&keypair);

    // Step 4: Determine runner name
    let runner_name = args.name.clone().unwrap_or_else(|| {
        hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "preloop-runner".to_string())
    });

    // Step 5: Discover pool (F004)
    let pool_id =
        discover_pool(&http, &registration.service_url, &registration.oauth_token).await?;
    info!("Discovered pool ID: {pool_id}");

    // Step 5b: Check for existing agent
    if let Some(existing_id) = check_existing_agent(
        &http,
        &registration.service_url,
        &registration.oauth_token,
        pool_id,
        &runner_name,
    )
    .await?
    {
        if !args.replace {
            anyhow::bail!(
                "Runner '{}' already exists (agent ID: {}). Use --replace.",
                runner_name,
                existing_id
            );
        }
        info!("Replacing existing agent {existing_id}");
        let delete_url = format!(
            "{}/_apis/v1/pools/{}/agents/{}",
            registration.service_url, pool_id, existing_id
        );
        match http
            .delete_with_token(&delete_url, &registration.oauth_token)
            .await
        {
            Ok(_) => info!("Deleted existing agent {existing_id}"),
            Err(e) => warn!("Failed to delete existing agent {existing_id}: {e:#}"),
        }
    }

    // Step 6: Download Node.js externals (unless --no-externals).
    //
    // Runs before agent registration so a download failure does not orphan a
    // server-side agent entry. Registration cannot be retried without
    // --replace, and the orchestrator would have to deregister the dead agent
    // before provisioning a replacement.
    if !args.no_externals {
        download_externals(&http, &root)
            .await
            .context("downloading Node.js externals")?;
    }

    // Step 7: Register agent with the server (F003: correct endpoint)
    let agent_response = create_agent(
        &http,
        &registration,
        &runner_name,
        &public_key_xml,
        &args,
        pool_id,
    )
    .await?;

    let agent_id = agent_response
        .get("id")
        .and_then(|v| v.as_i64())
        .unwrap_or(1);

    // Step 7: Persist configuration (F006: .credentials format, F007: .runner fields)
    // Extract OAuth URL and clientId from the agent creation RESPONSE (golden flow 6)
    // — the server generates these; we never fabricate them.
    let auth_block = agent_response.get("authorization");
    let auth_url = auth_block
        .and_then(|a| a.get("authorizationUrl"))
        .and_then(|v| v.as_str())
        .map(String::from)
        .context("agent response missing authorization.authorizationUrl")?;
    let client_id = auth_block
        .and_then(|a| a.get("clientId"))
        .and_then(|v| v.as_str())
        .map(String::from)
        .context("agent response missing authorization.clientId")?;

    // F006: .credentials with data map (matching official format)
    let mut cred_data = serde_json::Map::new();
    cred_data.insert("clientId".to_string(), serde_json::json!(client_id));
    cred_data.insert("authorizationUrl".to_string(), serde_json::json!(auth_url));
    // Extract agent response properties (used for FIPS, auth migration, broker URL)
    let props = agent_response.get("properties");

    // F056: Read requireFipsCryptography from agent response properties
    // instead of hardcoding "True". Official reads properties.RequireFipsCryptography.
    // Default to "True" if not present (matches prior behavior).
    let require_fips = props
        .and_then(|p| p.get("RequireFipsCryptography"))
        .and_then(|v| v.get("$value").or(Some(v)))
        .and_then(|v| v.as_str())
        .unwrap_or("True");
    cred_data.insert(
        "requireFipsCryptography".to_string(),
        serde_json::json!(require_fips),
    );

    // F053: Extract auth migration fields from agent response properties
    let enable_auth_migration = props
        .and_then(|p| p.get("EnableAuthMigrationByDefault"))
        .and_then(|v| v.get("$value").or(Some(v)))
        .and_then(|v| {
            v.as_bool()
                .or_else(|| v.as_str().map(|s| s == "true" || s == "True"))
        })
        .unwrap_or(false);
    let auth_url_v2 = props
        .and_then(|p| p.get("AuthorizationUrlV2"))
        .and_then(|v| v.get("$value").or(Some(v)))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    if enable_auth_migration {
        cred_data.insert(
            "enableAuthMigrationByDefault".to_string(),
            serde_json::json!("true"),
        );
    }
    if let Some(url_v2) = auth_url_v2 {
        cred_data.insert("authorizationUrlV2".to_string(), serde_json::json!(url_v2));
    }
    // P1.1: Extract broker URL from agent response properties.ServerUrlV2
    // GitHub returns `properties.ServerUrlV2.$value` = "https://broker.actions.githubusercontent.com/"
    // aksh returns it set to its own server URL. Fall back to registration service URL.
    let server_url_v2 = agent_response
        .get("properties")
        .and_then(|p| p.get("ServerUrlV2"))
        .and_then(|v| v.get("$value").or(Some(v)))
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| registration.service_url.clone());

    let config = RunnerConfig {
        settings: RunnerSettings {
            agent_id,
            agent_name: runner_name.clone(),
            pool_id,
            pool_name: "Default".to_string(),
            server_url: registration.service_url.clone(),
            git_hub_url: args.url.clone(),
            work_folder: args.work.clone(),
            is_hosted: false,
            runner_group_id: None,
            runner_group_name: Some(args.runner_group.clone()),
            ephemeral: args.ephemeral,
            // F007: new fields
            is_hosted_server: false,
            use_v2_flow: true,
            server_url_v2: Some(server_url_v2),
            // F052: settings fields (defaults matching official runner)
            disable_update: false,
            skip_session_recover: false,
            monitor_socket_address: None,
            use_runner_admin_flow: false,
        },
        credentials: CredentialData {
            scheme: "OAuth".to_string(),
            data: cred_data,
        },
        rsa_params,
    };
    std::fs::create_dir_all(&root)?;
    config.save(&root)?;
    info!(
        "Runner '{}' configured successfully (agent ID: {})",
        runner_name, agent_id
    );

    Ok(())
}

/// Run the `remove` subcommand.
pub async fn run_remove(args: RemoveArgs, global: &GlobalArgs) -> Result<()> {
    let root = global.runner_root();

    if !RunnerConfig::is_configured(&root) {
        bail!("Runner is not configured in {}", root.display());
    }

    let config = RunnerConfig::load(&root)?;
    let http = HttpClient::new(global.ca_bundle.as_deref())?;

    // Deregister with the server
    let delete_url = format!(
        "{}/_apis/v1/pools/{}/agents/{}",
        config.settings.server_url, config.settings.pool_id, config.settings.agent_id
    );

    // Get a fresh token for deletion
    let registration = register_runner(&http, &config.settings.git_hub_url, &args.token).await?;

    match http
        .delete_with_token(&delete_url, &registration.oauth_token)
        .await
    {
        Ok(_) => info!("Runner deregistered from server"),
        Err(e) => warn!("Failed to deregister runner: {e:#}. Removing local files anyway."),
    }

    RunnerConfig::remove_files(&root)?;
    info!("Runner configuration removed");
    Ok(())
}

// ─── Registration helpers ────────────────────────────────────────────────

/// Result of the runner registration API call.
struct RegistrationResult {
    oauth_token: String,
    service_url: String,
    #[allow(dead_code)]
    token_url: Option<String>,
    #[allow(dead_code)]
    client_id: Option<String>,
    #[allow(dead_code)]
    pool_id: Option<i64>,
}

/// POST to the runner-registration endpoint to get OAuth token and service URL.
async fn register_runner(http: &HttpClient, url: &str, token: &str) -> Result<RegistrationResult> {
    // Determine the registration URL:
    // For GitHub: POST https://api.github.com/actions/runner-registration
    // For local aksh: POST {base}/api/v3/actions/runner-registration
    let reg_url = if url.contains("github.com") {
        "https://api.github.com/actions/runner-registration".to_string()
    } else {
        // Local server — extract base URL
        let base = url.strip_suffix('/').unwrap_or(url);
        format!("{base}/api/v3/actions/runner-registration")
    };

    let body = serde_json::json!({
        "url": url,
        "runner_event": "register"
    });

    let resp: serde_json::Value = http
        .post_json_with_auth(&reg_url, &body, &format!("RemoteAuth {token}"))
        .await
        .context("runner registration")?;

    let oauth_token = resp
        .get("token")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let service_url = resp
        .get("url")
        .and_then(|v| v.as_str())
        .map(|s| s.trim_end_matches('/').to_string())
        .unwrap_or_default();
    let token_url = resp
        .get("token_schema")
        .and_then(|v| v.get("authorization_url"))
        .and_then(|v| v.as_str())
        .or_else(|| resp.get("tokenUrl").and_then(|v| v.as_str()))
        .map(|s| s.to_string());
    let client_id = resp
        .get("token_schema")
        .and_then(|v| v.get("client_id"))
        .and_then(|v| v.as_str())
        .or_else(|| resp.get("clientId").and_then(|v| v.as_str()))
        .map(|s| s.to_string());
    let pool_id = resp
        .get("runner_pool_id")
        .and_then(|v| v.as_i64())
        .or_else(|| resp.get("poolId").and_then(|v| v.as_i64()));

    Ok(RegistrationResult {
        oauth_token,
        service_url,
        token_url,
        client_id,
        pool_id,
    })
}

/// Discover the pool ID by querying the pools endpoint.
async fn discover_pool(http: &HttpClient, service_url: &str, token: &str) -> Result<i64> {
    let url = format!(
        "{}/_apis/distributedtask/pools?poolType=Automation",
        service_url
    );
    let resp: serde_json::Value = http
        .get_json_with_auth_accept(&url, &format!("Bearer {token}"), DISTTASK_POOLS_ACCEPT)
        .await
        .context("querying pools endpoint")?;

    let pools = resp
        .get("value")
        .and_then(|v| v.as_array())
        .context("pools response missing 'value' array")?;

    let pool = pools.first().context("no pools returned by server")?;

    pool.get("id")
        .and_then(|v| v.as_i64())
        .context("pool missing 'id' field")
}

/// Check if an agent with this name already exists.
async fn check_existing_agent(
    http: &HttpClient,
    service_url: &str,
    token: &str,
    pool_id: i64,
    name: &str,
) -> Result<Option<i64>> {
    let url = format!(
        "{}/_apis/distributedtask/pools/{pool_id}/agents?agentName={name}&includeCapabilities=False",
        service_url
    );
    let resp: serde_json::Value = http
        .get_json_with_auth_accept(&url, &format!("Bearer {token}"), DISTTASK_AGENT_ACCEPT)
        .await
        .context("querying existing agents")?;

    if let Some(agents) = resp.get("value").and_then(|v| v.as_array()) {
        if let Some(agent) = agents.first() {
            return Ok(agent.get("id").and_then(|v| v.as_i64()));
        }
    }
    Ok(None)
}

/// Create the agent on the server (F003: correct endpoint, F005: full field set).
async fn create_agent(
    http: &HttpClient,
    reg: &RegistrationResult,
    name: &str,
    public_key_xml: &str,
    args: &ConfigureArgs,
    pool_id: i64,
) -> Result<serde_json::Value> {
    // F003: Use _apis/distributedtask/ (not _apis/v1/)
    let url = format!(
        "{}/_apis/distributedtask/pools/{pool_id}/agents",
        reg.service_url
    );

    let mut labels: Vec<serde_json::Value> = vec![
        serde_json::json!({"id": 0, "name": "self-hosted", "type": "system"}),
        serde_json::json!({"id": 0, "name": current_os_label(), "type": "system"}),
        serde_json::json!({"id": 0, "name": current_arch_label(), "type": "system"}),
    ];
    if let Some(user_labels) = &args.labels {
        // Dedupe case-insensitively, preserving first occurrence. A user who
        // passes `--labels self-hosted,gpu` would otherwise ship a duplicate
        // `self-hosted` that the server's `(runner_id, label)` primary key
        // rejects with HTTP 500.
        let mut seen: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(3 + user_labels.len());
        for label in &["self-hosted", current_os_label(), current_arch_label()] {
            seen.insert(label.to_lowercase());
        }
        for l in user_labels {
            if !seen.insert(l.to_lowercase()) {
                continue;
            }
            labels.push(serde_json::json!({"id": 0, "name": l, "type": "user"}));
        }
    }

    // F005: Full field set matching official runner (golden flow 6)
    let os_description = crate::os_description();
    let agent = serde_json::json!({
        "labels": labels,
        "maxParallelism": 1,
        "createdOn": "0001-01-01T00:00:00",
        "authorization": {
            "publicKey": {
                "exponent": public_key_xml.split("<Exponent>")
                    .nth(1)
                    .and_then(|s| s.split("</Exponent>").next())
                    .unwrap_or("AQAB"),
                "modulus": public_key_xml.split("<Modulus>")
                    .nth(1)
                    .and_then(|s| s.split("</Modulus>").next())
                    .unwrap_or(""),
            }
        },
        "id": 0,
        "name": name,
        "version": crate::PROTOCOL_COMPAT_VERSION,
        "osDescription": os_description,
        "ephemeral": args.ephemeral,
        "disableUpdate": true,
        "status": 0,
        "provisioningState": "Provisioned",
    });

    // The pool injects a one-time provision token per machine; forwarding it
    // lets the server pair this registration with the job the machine was
    // provisioned for. Absent outside pool provisioning.
    let provision_token = std::env::var("PRELOOP_PROVISION_TOKEN").ok();
    let extra: Vec<(&str, &str)> = provision_token
        .as_deref()
        .into_iter()
        .map(|token| ("X-Preloop-Provision-Token", token))
        .collect();
    http.post_json_with_auth_headers_extra(
        &url,
        &agent,
        &format!("Bearer {}", reg.oauth_token),
        DISTTASK_AGENT_ACCEPT,
        DISTTASK_AGENT_CONTENT_TYPE,
        &extra,
    )
    .await
    .context("creating agent")
}

/// Export keypair to RsaParameters + XML public key.
fn export_keypair(
    keypair: &preloop_gha_protocol::crypto::AgentRsaKeypair,
) -> (RsaParameters, String) {
    let export = keypair.to_rsaparams();
    let xml = keypair.public_key_xml();
    let rsa_params = RsaParameters {
        d: export.d,
        dp: export.dp,
        dq: export.dq,
        exponent: export.exponent,
        inverse_q: export.inverse_q,
        modulus: export.modulus,
        p: export.p,
        q: export.q,
    };
    (rsa_params, xml)
}

/// Download Node.js externals for running JS-based actions.
///
/// Cache validation (R2): each `externals/nodeXX` must contain `preloop-node.json`
/// with matching version and a `bin/node --version` that prints `v<version>`.
/// Stale/missing/mismatched entries are re-materialized (download into temp dir,
/// verify SHA256 via pinned table + SHASUMS256.txt, atomic rename).
async fn download_externals(http: &HttpClient, root: &std::path::Path) -> Result<()> {
    download_externals_with_fetcher(
        |url| {
            let http = http.clone();
            let url = url.to_owned();
            async move { http.get_bytes(&url).await }
        },
        root,
    )
    .await
}

/// Injectable fetcher variant for tests — `fetcher` is called for both the tarball
/// and the `SHASUMS256.txt` URL.
async fn download_externals_with_fetcher<F, Fut>(fetcher: F, root: &std::path::Path) -> Result<()>
where
    F: Fn(&str) -> Fut + Sync,
    Fut: Future<Output = Result<bytes::Bytes>> + Send,
{
    let node_versions = [
        ("node20", crate::NODE20_EXTERNALS_VERSION),
        ("node24", crate::NODE24_EXTERNALS_VERSION),
    ];
    download_externals_with_runtimes_and_fetcher(fetcher, &node_versions, root).await
}

async fn download_externals_with_runtimes_and_fetcher<F, Fut>(
    fetcher: F,
    node_versions: &[(&str, &str)],
    root: &std::path::Path,
) -> Result<()>
where
    F: Fn(&str) -> Fut + Sync,
    Fut: Future<Output = Result<bytes::Bytes>> + Send,
{
    let externals_dir = root.join("externals");
    std::fs::create_dir_all(&externals_dir)?;

    let platform = node_externals::current_platform();

    for (name, version) in node_versions {
        let version = version.trim();
        let version_v = if version.starts_with('v') {
            version.to_owned()
        } else {
            format!("v{version}")
        };
        let version_plain = version.trim_start_matches('v');
        let dest = externals_dir.join(name);
        if node_externals::is_valid_externals_dir(&dest, name, version_plain) {
            info!("Externals {name} {version_v} already valid, skipping");
            continue;
        }
        // If dest exists but is invalid, we will replace it.
        if dest.exists() {
            info!("Externals {name} stale or invalid (expected {version_v}), re-downloading");
        }

        let archive_name = node_externals::archive_name(&version_v, &platform);
        let url = node_externals::source_url(&version_v, &archive_name);
        let shasums_url = node_externals::shasums_url(&version_v);
        info!("Downloading {name} from {url}");

        let bytes = fetcher(&url)
            .await
            .with_context(|| format!("fetch {url}"))?;
        // Fetch SHASUMS for belt-and-braces verification (best-effort).
        let shasums = match fetcher(&shasums_url).await {
            Ok(b) => Some(String::from_utf8_lossy(&b).into_owned()),
            Err(e) => {
                warn!("could not fetch SHASUMS256.txt for {version_v}: {e:#}; relying on pinned SHA only");
                None
            }
        };
        let digest = node_externals::sha256_hex(bytes.as_ref());
        let pinned_key = node_externals::pinned_key(name, &version_v, &platform);
        let pinned = crate::node_externals_pinned_sha256(&pinned_key);
        node_externals::verify_digest(&digest, &archive_name, pinned, shasums.as_deref()).map_err(
            |e| {
                anyhow::anyhow!(
                    "checksum verification failed for {name} {version_v} ({archive_name}): {e}"
                )
            },
        )?;
        // Extract into a temporary directory, then publish atomically. A
        // failed download or extraction must not leave a directory that a
        // later configure mistakenly treats as a complete external.
        let temp = externals_dir.join(format!(".{name}.tmp-{}", std::process::id()));
        if temp.exists() {
            std::fs::remove_dir_all(&temp)?;
        }
        std::fs::create_dir_all(&temp)?;
        if cfg!(target_os = "windows") {
            let reader = std::io::Cursor::new(bytes.as_ref());
            let mut archive = zip::ZipArchive::new(reader)?;
            for index in 0..archive.len() {
                let mut entry = archive.by_index(index)?;
                let path = entry
                    .enclosed_name()
                    .ok_or_else(|| anyhow::anyhow!("invalid path in Node archive"))?
                    .to_owned();
                // Strip the first path component (node-v20.x.y-win-x64/)
                let stripped: std::path::PathBuf = path.components().skip(1).collect();
                if stripped.components().count() == 0 {
                    continue;
                }
                let target = temp.join(&stripped);
                if entry.is_dir() {
                    std::fs::create_dir_all(&target)?;
                } else {
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let mut output = std::fs::File::create(&target)?;
                    std::io::copy(&mut entry, &mut output)?;
                }
            }
        } else {
            let decoder = flate2::read::GzDecoder::new(bytes.as_ref());
            let mut archive = tar::Archive::new(decoder);
            for entry in archive.entries()? {
                let mut entry = entry?;
                let path = entry.path()?.into_owned();
                // Strip the first path component (node-v20.x.y-os-arch/)
                let stripped: std::path::PathBuf = path.components().skip(1).collect();
                if stripped.components().count() == 0 {
                    continue;
                }
                let target = temp.join(&stripped);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                entry.unpack(&target)?;
            }
        }
        let extracted_binary = if cfg!(target_os = "windows") {
            temp.join("node.exe")
        } else {
            temp.join("bin/node")
        };
        if !extracted_binary.is_file() {
            std::fs::remove_dir_all(&temp)?;
            anyhow::bail!("downloaded {name} archive did not contain bin/node");
        }
        // Write manifest into temp before publishing atomically.
        let manifest =
            node_externals::NodeManifest::new(name, version_plain, &platform, &digest, &url);
        node_externals::write_manifest(&temp, &manifest)
            .with_context(|| format!("writing manifest for {name}"))?;
        if dest.exists() {
            std::fs::remove_dir_all(&dest)?;
        }
        std::fs::rename(&temp, &dest)?;
        info!("Extracted {name} to {}", dest.display());
    }

    Ok(())
}

fn current_os_label() -> &'static str {
    if cfg!(target_os = "macos") {
        "macOS"
    } else if cfg!(target_os = "linux") {
        "Linux"
    } else if cfg!(target_os = "windows") {
        "Windows"
    } else {
        "Unknown"
    }
}

fn current_arch_label() -> &'static str {
    if cfg!(target_arch = "aarch64") {
        "ARM64"
    } else {
        "X64"
    }
}

#[cfg(all(test, unix))]
mod node_externals_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn fake_node_tar(version: &str, platform: &str) -> Vec<u8> {
        // Create a minimal tar.gz containing bin/node that prints v<version>
        let mut tar_data = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_data);
            let dir_name = format!("node-v{version}-{platform}/bin/");
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o755);
            header.set_size(0);
            header.set_cksum();
            builder
                .append_data(&mut header, &dir_name, &[][..])
                .unwrap();

            let script = format!("#!/bin/sh\necho v{version}\n");
            let file_name = format!("node-v{version}-{platform}/bin/node");
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o755);
            header.set_size(script.len() as u64);
            header.set_cksum();
            builder
                .append_data(&mut header, &file_name, script.as_bytes())
                .unwrap();
            builder.finish().unwrap();
        }
        let mut gz = Vec::new();
        {
            use flate2::write::GzEncoder;
            use flate2::Compression;
            use std::io::Write;
            let mut enc = GzEncoder::new(&mut gz, Compression::default());
            enc.write_all(&tar_data).unwrap();
            enc.finish().unwrap();
        }
        gz
    }

    fn shasums_for(bytes: &[u8], archive_name: &str) -> String {
        let digest = node_externals::sha256_hex(bytes);
        format!("{digest}  {archive_name}\n")
    }

    #[tokio::test]
    async fn stale_manifest_triggers_redownload() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let externals_dir = root.join("externals");
        std::fs::create_dir_all(externals_dir.join("node24/bin")).unwrap();
        // Write stale manifest with old version and a fake node that claims old version.
        let stale_version = "24.2.0";
        let expected = crate::NODE24_EXTERNALS_VERSION;
        assert_ne!(stale_version, expected, "test requires stale != expected");
        let manifest = node_externals::NodeManifest::new(
            "node24",
            stale_version,
            &node_externals::current_platform(),
            "oldsha",
            "https://example.com/old",
        );
        node_externals::write_manifest(&externals_dir.join("node24"), &manifest).unwrap();
        let fake_node = externals_dir.join("node24/bin/node");
        std::fs::write(&fake_node, format!("#!/bin/sh\necho v{stale_version}\n")).unwrap();
        std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o755)).unwrap();

        // Also need node20 dir to avoid extra download interfering; make it valid
        let platform = node_externals::current_platform();
        for name in ["node20", "node24"] {
            let ver = if name == "node20" {
                crate::NODE20_EXTERNALS_VERSION
            } else {
                crate::NODE24_EXTERNALS_VERSION
            };
            let plain = ver.trim_start_matches('v');
            // For node20 we make valid so only node24 is stale; for node24 we already made stale
            if name == "node20" {
                let dir = externals_dir.join(name);
                std::fs::create_dir_all(dir.join("bin")).unwrap();
                let m = node_externals::NodeManifest::new(
                    name,
                    plain,
                    &platform,
                    "abc",
                    "https://example.com",
                );
                node_externals::write_manifest(&dir, &m).unwrap();
                let fake = dir.join("bin/node");
                std::fs::write(&fake, format!("#!/bin/sh\necho v{plain}\n")).unwrap();
                std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
        }

        // Prepare fetcher that returns correct tar for the expected version.
        let platform = node_externals::current_platform();
        let expected_plain = "24.99.0";
        let archive_name = node_externals::archive_name(&format!("v{expected_plain}"), &platform);
        let tar_bytes = fake_node_tar(expected_plain, &platform);
        let shasums = shasums_for(&tar_bytes, &archive_name);
        let tar_bytes_clone = tar_bytes.clone();
        let archive_name_clone = archive_name.clone();
        let shasums_clone = shasums.clone();
        let fetcher = move |url: &str| {
            let url = url.to_owned();
            let tar = tar_bytes_clone.clone();
            let shas = shasums_clone.clone();
            let an = archive_name_clone.clone();
            async move {
                if url.ends_with(&an) {
                    Ok(bytes::Bytes::from(tar))
                } else if url.ends_with("SHASUMS256.txt") {
                    Ok(bytes::Bytes::from(shas))
                } else {
                    anyhow::bail!("unexpected url {url}")
                }
            }
        };

        download_externals_with_runtimes_and_fetcher(fetcher, &[("node24", "24.99.0")], root)
            .await
            .unwrap();
        // After download, manifest should be updated to expected version.
        let new_manifest = node_externals::read_manifest(&externals_dir.join("node24")).unwrap();
        assert_eq!(new_manifest.version, "24.99.0");
        // Binary should now report expected version.
        let out = std::process::Command::new(externals_dir.join("node24/bin/node"))
            .arg("--version")
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "v24.99.0");
    }

    #[tokio::test]
    async fn corrupted_tarball_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let externals_dir = root.join("externals");
        std::fs::create_dir_all(&externals_dir).unwrap();
        // Ensure no valid cache for node24 so it must download
        let platform = node_externals::current_platform();
        let expected = crate::NODE24_EXTERNALS_VERSION;
        let expected_plain = expected.trim_start_matches('v');
        let archive_name = node_externals::archive_name(&format!("v{expected_plain}"), &platform);

        // Create corrupted bytes (not a valid digest vs shasums)
        let bad_bytes = b"corrupted content".to_vec();
        let _shasums = shasums_for(&bad_bytes, &archive_name);
        // But also create a pinned mismatch: we won't have pinned, so verification is against shasums only.
        // To make it fail, provide shasums for DIFFERENT bytes than we return.
        let good_bytes = fake_node_tar(expected_plain, &platform);
        let good_shasums = shasums_for(&good_bytes, &archive_name);
        let fetcher = move |url: &str| {
            let url = url.to_owned();
            let bad = bad_bytes.clone();
            let good_shas = good_shasums.clone();
            let an = archive_name.clone();
            async move {
                if url.ends_with(&an) {
                    Ok(bytes::Bytes::from(bad))
                } else if url.ends_with("SHASUMS256.txt") {
                    Ok(bytes::Bytes::from(good_shas))
                } else {
                    // For node20, return good bytes to not fail other runtime
                    // Need to handle node20 fetch as well: we will provide valid for node20
                    // This branch for node20 archive_name will be different string, but our archive_name is for node24 only.
                    // Simplify: return good_bytes for any other tarball
                    let plat = node_externals::current_platform();
                    let v20_plain = crate::NODE20_EXTERNALS_VERSION.trim_start_matches('v');
                    let an20 = node_externals::archive_name(&format!("v{v20_plain}"), &plat);
                    let tar20 = fake_node_tar(v20_plain, &plat);
                    let shas20 = shasums_for(&tar20, &an20);
                    if url.ends_with(&an20) {
                        Ok(bytes::Bytes::from(tar20))
                    } else {
                        Ok(bytes::Bytes::from(shas20))
                    }
                }
            }
        };

        let result =
            download_externals_with_runtimes_and_fetcher(fetcher, &[("node24", "24.99.0")], root)
                .await;
        assert!(result.is_err(), "corrupted tarball should be rejected");
        // Ensure no directory was installed
        assert!(!externals_dir.join("node24/bin/node").exists());
        assert!(!externals_dir.join("node24/preloop-node.json").exists());
    }

    #[tokio::test]
    async fn fresh_correct_cache_is_not_redownloaded() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let externals_dir = root.join("externals");
        let platform = node_externals::current_platform();
        for name in ["node20", "node24"] {
            let ver = if name == "node20" {
                crate::NODE20_EXTERNALS_VERSION
            } else {
                crate::NODE24_EXTERNALS_VERSION
            };
            let plain = ver.trim_start_matches('v');
            let dir = externals_dir.join(name);
            std::fs::create_dir_all(dir.join("bin")).unwrap();
            let m = node_externals::NodeManifest::new(
                name,
                plain,
                &platform,
                "abc",
                "https://example.com",
            );
            node_externals::write_manifest(&dir, &m).unwrap();
            let fake = dir.join("bin/node");
            std::fs::write(&fake, format!("#!/bin/sh\necho v{plain}\n")).unwrap();
            std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let mtime_before = std::fs::metadata(externals_dir.join("node24/preloop-node.json"))
            .unwrap()
            .modified()
            .unwrap();
        // Fetcher that would panic if called
        let fetcher = |_url: &str| async {
            anyhow::bail!("network should not be called for fresh cache") as Result<bytes::Bytes>
        };
        download_externals_with_fetcher(fetcher, root)
            .await
            .unwrap();
        let mtime_after = std::fs::metadata(externals_dir.join("node24/preloop-node.json"))
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(
            mtime_before, mtime_after,
            "fresh cache should not be re-downloaded"
        );
    }
    #[tokio::test]
    async fn missing_all_checksums_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let externals_dir = root.join("externals");
        std::fs::create_dir_all(&externals_dir).unwrap();
        let platform = node_externals::current_platform();
        let archive_name = node_externals::archive_name("v24.99.0", &platform);
        let tar_bytes = fake_node_tar("24.99.0", &platform);
        // Fetcher provides tarball but fails SHASUMS request and version is unpinned
        let fetcher = move |url: &str| {
            let url = url.to_owned();
            let tar = tar_bytes.clone();
            let an = archive_name.clone();
            async move {
                if url.ends_with(&an) {
                    Ok(bytes::Bytes::from(tar))
                } else {
                    anyhow::bail!("SHASUMS unavailable")
                }
            }
        };
        let result =
            download_externals_with_runtimes_and_fetcher(fetcher, &[("node24", "24.99.0")], root)
                .await;
        assert!(
            result.is_err(),
            "missing all checksum sources must fail closed"
        );
        assert!(!externals_dir.join("node24/bin/node").exists());
    }
}
