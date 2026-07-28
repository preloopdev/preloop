//! Runner configuration and registration.
//!
//! Implements the `configure` and `remove` subcommands, handling:
//! - Registration via the GitHub runner-registration API
//! - RSA keypair generation and persistence
//! - Node.js externals download (unless --no-externals)
//! - Agent creation/deletion via the distributedtask API

use anyhow::{bail, Context, Result};
use tracing::{info, warn};

use crate::cli::{ConfigureArgs, GlobalArgs, RemoveArgs};
use crate::client::http::HttpClient;
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
fn supplied_keypair() -> Result<Option<aksh_gha_protocol::crypto::AgentRsaKeypair>> {
    let Ok(raw) = std::env::var(RSA_PARAMS_ENV) else {
        return Ok(None);
    };
    if raw.trim().is_empty() {
        return Ok(None);
    }
    let params: aksh_gha_protocol::crypto::RsaParametersExport =
        serde_json::from_str(raw.trim())
            .with_context(|| format!("parsing {RSA_PARAMS_ENV} as RSAParameters JSON"))?;
    let keypair = aksh_gha_protocol::crypto::AgentRsaKeypair::from_rsaparams(&params)
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
    let registration = register_runner(&http, &args.url, &args.token).await?;
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
            aksh_gha_protocol::crypto::AgentRsaKeypair::generate()
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
            .unwrap_or_else(|| "aksh-runner".to_string())
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
        for l in user_labels {
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

    http.post_json_with_auth_headers(
        &url,
        &agent,
        &format!("Bearer {}", reg.oauth_token),
        DISTTASK_AGENT_ACCEPT,
        DISTTASK_AGENT_CONTENT_TYPE,
    )
    .await
    .context("creating agent")
}

/// Export keypair to RsaParameters + XML public key.
fn export_keypair(keypair: &aksh_gha_protocol::crypto::AgentRsaKeypair) -> (RsaParameters, String) {
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
async fn download_externals(http: &HttpClient, root: &std::path::Path) -> Result<()> {
    // Node versions matching official runner v2.335.1 externals
    let node_versions = [("node20", "v20.19.0"), ("node24", "v24.3.0")];

    let externals_dir = root.join("externals");
    std::fs::create_dir_all(&externals_dir)?;

    let os = if cfg!(target_os = "macos") {
        "darwin"
    } else {
        "linux"
    };
    let arch = if cfg!(target_arch = "aarch64") {
        "arm64"
    } else {
        "x64"
    };

    for (name, version) in &node_versions {
        let dest = externals_dir.join(name);
        if dest.join("bin/node").is_file() {
            info!("Externals {name} already present, skipping");
            continue;
        }

        let tarball_name = format!("node-{version}-{os}-{arch}.tar.gz");
        let url = format!("https://nodejs.org/dist/{version}/{tarball_name}");
        info!("Downloading {name} from {url}");

        let bytes = http.get_bytes(&url).await?;
        let decoder = flate2::read::GzDecoder::new(bytes.as_ref());
        let mut archive = tar::Archive::new(decoder);

        // Extract into a temporary directory, then publish atomically. A
        // failed download or extraction must not leave a directory that a
        // later configure mistakenly treats as a complete external.
        let temp = externals_dir.join(format!(".{name}.tmp-{}", std::process::id()));
        if temp.exists() {
            std::fs::remove_dir_all(&temp)?;
        }
        std::fs::create_dir_all(&temp)?;
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
        if !temp.join("bin/node").is_file() {
            std::fs::remove_dir_all(&temp)?;
            anyhow::bail!("downloaded {name} archive did not contain bin/node");
        }
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
