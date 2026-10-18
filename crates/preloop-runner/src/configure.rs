//! Runner configuration and registration.
//!
//! Implements the `configure` and `remove` subcommands, handling:
//! - Registration via the GitHub runner-registration API
//! - RSA keypair generation and persistence
//! - Node.js externals download (unless --no-externals)
//! - Agent creation/deletion via the distributedtask API

use std::future::Future;

use anyhow::{Context, Result, bail};
use tracing::{info, warn};

use reqwest::header::ACCEPT;

use crate::cli::{ConfigureArgs, GlobalArgs, RemoveArgs};
use crate::client::http::{HttpClient, HttpError};
use crate::node_externals;
use crate::settings::{CredentialData, RsaParameters, RunnerConfig, RunnerSettings};

const DISTTASK_POOLS_ACCEPT: &str = "application/json; api-version=5.1-preview.1";
const DISTTASK_AGENT_ACCEPT: &str = "application/json; api-version=6.0-preview.2";
const DISTTASK_AGENT_CONTENT_TYPE: &str =
    "application/json; charset=utf-8; api-version=6.0-preview.2";

/// Pool ID meaning "search every pool". The official runner looks an agent
/// up by name and deletes it through `pools/0` — the pool segment is ignored
/// server-side by both GitHub and our engine (golden capture
/// `v2.336.0/01-register-and-idle` deletes `pools/0/agents/{id}`).
const ALL_POOLS: i64 = 0;
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
    let mut registration = register_runner(&http, &args.url, &args.token, "register").await?;
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
    if let Some(location) = conn_data.get("locationServiceData")
        && let (Some(last_change_id), Some(last_change_id64)) = (
            location
                .get("lastChangeId")
                .and_then(serde_json::Value::as_i64),
            location
                .get("lastChangeId64")
                .and_then(serde_json::Value::as_i64),
        )
    {
        let cache = serde_json::json!({
            "lastChangeId": last_change_id,
            "lastChangeId64": last_change_id64,
        });
        std::fs::write(&cache_path, serde_json::to_vec_pretty(&cache)?)
            .with_context(|| format!("writing {}", cache_path.display()))?;
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

    // Step 5: Resolve the runner group (--runner-group) to its agent pool,
    // matching the official runner's group-name lookup over the self-hosted
    // pools (`!IsHosted`, case-insensitive name match, `IsInternal` default).
    let (pool_id, pool_name) = discover_pool(
        &http,
        &registration.service_url,
        &registration.oauth_token,
        args.runner_group.as_deref(),
    )
    .await?;
    info!("Discovered pool '{pool_name}' (ID: {pool_id})");

    // Step 5b: Check for existing agent. The lookup happens before anything is
    // destroyed, and the deletion it authorises is deferred until every
    // fallible local step is done (step 6b): a failed download must not leave a
    // previously working runner unregistered while `.runner` still points at
    // the identity we just deleted.
    let existing_agent = check_existing_agent(
        &http,
        &registration.service_url,
        &registration.oauth_token,
        ALL_POOLS,
        &runner_name,
    )
    .await?;
    if let Some(existing_id) = existing_agent {
        if !args.replace {
            anyhow::bail!(
                "Runner '{}' already exists (agent ID: {}). Use --replace.",
                runner_name,
                existing_id
            );
        }
        info!("Replacing agent {existing_id}");
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

    // Step 6b: Retire the agent being replaced, now that the local assets it
    // needs are in place.
    if let Some(existing_id) = existing_agent {
        delete_agent(
            &http,
            &registration.service_url,
            &registration.oauth_token,
            existing_id,
        )
        .await
        .with_context(|| format!("deleting existing agent {existing_id} during --replace"))?;
        info!("Deleted existing agent {existing_id}");
    }

    // Step 7: Register agent with the server (correct endpoint)
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

    // Step 7: Persist configuration (.credentials format,.runner fields)
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

    //credentials with data map (matching official format)
    let mut cred_data = serde_json::Map::new();
    cred_data.insert("clientId".to_string(), serde_json::json!(client_id));
    cred_data.insert("authorizationUrl".to_string(), serde_json::json!(auth_url));
    // Extract agent response properties (used for FIPS, auth migration, broker URL)
    let props = agent_response.get("properties");

    // Read RequireFipsCryptography from agent response properties. Both our
    // engine and real GitHub serialize it as `{"$type":"System.Boolean",
    // "$value":<bool>}`; the official runner persists
    // `value.ToString()` — "True"/"False" — into .credentials
    // (ConfigurationManager.cs writes
    // `agent.Properties.GetValue("RequireFipsCryptography", true).ToString()`).
    let require_fips = props
        .and_then(|p| p.get("RequireFipsCryptography"))
        .and_then(|v| v.get("$value").or(Some(v)))
        .and_then(wire_bool)
        .unwrap_or(true);
    cred_data.insert(
        "requireFipsCryptography".to_string(),
        serde_json::json!(if require_fips { "True" } else { "False" }),
    );

    // Extract auth migration fields from agent response properties
    let enable_auth_migration = props
        .and_then(|p| p.get("EnableAuthMigrationByDefault"))
        .and_then(|v| v.get("$value").or(Some(v)))
        .and_then(wire_bool)
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
    // Extract broker URL from agent response properties.ServerUrlV2
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
            pool_name: pool_name.clone(),
            server_url: registration.service_url.clone(),
            git_hub_url: args.url.clone(),
            work_folder: args.work.clone(),
            is_hosted: false,
            runner_group_id: None,
            runner_group_name: Some(pool_name.clone()),
            ephemeral: args.ephemeral,
            // new fields
            is_hosted_server: false,
            use_v2_flow: true,
            server_url_v2: Some(server_url_v2),
            // settings fields (defaults matching official runner)
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
///
/// Mirrors the official `UnconfigureAsync`: request a *remove-scoped*
/// credential (`runner_event: "remove"`), look the runner up by name, and skip
/// the delete when it is already gone. Local files are removed only once the
/// server no longer has the registration — they are the retry's credentials —
/// and a delete failure is reported as an error so a ghost registration is
/// never mistaken for a clean removal.
pub async fn run_remove(args: RemoveArgs, global: &GlobalArgs) -> Result<()> {
    let root = global.runner_root();

    if !RunnerConfig::is_configured(&root) {
        bail!("Runner is not configured in {}", root.display());
    }

    let config = RunnerConfig::load(&root)?;
    let http = HttpClient::new(global.ca_bundle.as_deref())?;

    // Remove-scoped tenant credential: the official runner requests token
    // type "remove" and posts runner_event "remove"
    // (ConfigurationManager.cs GetRunnerTokenAsync + GetTenantCredential).
    let registration =
        register_runner(&http, &config.settings.git_hub_url, &args.token, "remove").await?;
    let service_url = http.rewrite_url(&registration.service_url).into_owned();

    // The official runner looks the agent up by name before deleting it and
    // prints "Does not exist. Skipping Removing runner from the server." when
    // it is gone, so a registration retired through the UI or by
    // stale-runner cleanup still cleans up locally.
    let existing_agent = check_existing_agent(
        &http,
        &service_url,
        &registration.oauth_token,
        ALL_POOLS,
        &config.settings.agent_name,
    )
    .await?;

    // A server-side failure returns before the local files are touched: the
    // official runner throws ahead of `DeleteLocalRunnerConfig()`, so the
    // agent id and repository URL stay behind for the retry instead of
    // orphaning the registration with no local identity left to remove it.
    match existing_agent {
        Some(agent_id) => {
            delete_agent(&http, &service_url, &registration.oauth_token, agent_id)
                .await
                .with_context(|| format!("deregistering agent {agent_id}"))?;
            info!("Runner deregistered from server");
        }
        None => info!(
            "Runner '{}' does not exist on the server; removing local configuration",
            config.settings.agent_name
        ),
    }

    RunnerConfig::remove_files(&root)?;
    info!("Runner removed");
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
///
/// `runner_event` selects the credential scope GitHub mints: `"register"` for
/// `configure`, `"remove"` for `remove` — the two token types are
/// distinguished on the real service (`GetTenantCredential`,
/// ConfigurationManager.cs).
async fn register_runner(
    http: &HttpClient,
    url: &str,
    token: &str,
    runner_event: &str,
) -> Result<RegistrationResult> {
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

    let body = registration_body(url, runner_event);

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

/// Body for `POST …/actions/runner-registration` (`GitHubAuthResult` request).
fn registration_body(url: &str, runner_event: &str) -> serde_json::Value {
    serde_json::json!({
        "url": url,
        "runner_event": runner_event
    })
}

/// `DELETE {service_url}/_apis/distributedtask/pools/{pool}/agents/{id}`.
///
/// The official runner passes pool `0` (`RunnerServer.DeleteAgentAsync`:
/// "agentPool is ignored server side") and the real GitHub endpoint answers
/// 204 on exactly this path. Pool `0` is used verbatim so the wire request
/// matches the captured official sequence.
fn agent_delete_url(service_url: &str, agent_id: i64) -> String {
    format!("{service_url}/_apis/distributedtask/pools/{ALL_POOLS}/agents/{agent_id}")
}

/// Delete an agent through the distributed-task endpoint.
///
/// A `404` is the desired end state already holding — the registration is
/// gone — so it is not an error, and a `remove` whose only remaining work is
/// local cleanup still finishes.
async fn delete_agent(
    http: &HttpClient,
    service_url: &str,
    token: &str,
    agent_id: i64,
) -> Result<()> {
    let url = agent_delete_url(service_url, agent_id);
    match http
        .delete_with_token_header(&url, token, ACCEPT.as_str(), DISTTASK_AGENT_ACCEPT)
        .await
    {
        Ok(()) => Ok(()),
        Err(error) if is_not_found(&error) => {
            info!("Agent {agent_id} is already gone from the server");
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// Whether an error carries an HTTP `404 Not Found`.
fn is_not_found(error: &anyhow::Error) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<HttpError>())
        .is_some_and(|error| {
            matches!(error, HttpError::Status { status, .. } if *status == reqwest::StatusCode::NOT_FOUND)
        })
}

/// Resolve the runner group (`--runner-group`) to `(pool_id, pool_name)` by
/// querying the pools endpoint.
///
/// Matches the official `ConfigureAsync` group resolution: the first internal
/// (`isInternal`) pool is the default, the requested group name is matched
/// case-insensitively among the self-hosted (`!isHosted`) pools, and an
/// omitted name falls back to the internal group's name. A name that hits no
/// group, or more than one, is an error listing what the server reported.
async fn discover_pool(
    http: &HttpClient,
    service_url: &str,
    token: &str,
    runner_group: Option<&str>,
) -> Result<(i64, String)> {
    let url = format!("{service_url}/_apis/distributedtask/pools?poolType=Automation");
    let resp: serde_json::Value = http
        .get_json_with_auth_accept(&url, &format!("Bearer {token}"), DISTTASK_POOLS_ACCEPT)
        .await
        .context("querying pools endpoint")?;

    let pools = resp
        .get("value")
        .and_then(|v| v.as_array())
        .context("pools response missing 'value' array")?;
    select_pool(pools, runner_group)
}

fn pool_is_hosted(pool: &serde_json::Value) -> bool {
    pool.get("isHosted")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn pool_is_internal(pool: &serde_json::Value) -> bool {
    pool.get("isInternal")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn pool_name(pool: &serde_json::Value) -> Option<&str> {
    pool.get("name").and_then(serde_json::Value::as_str)
}

fn pool_id(pool: &serde_json::Value) -> Option<i64> {
    pool.get("id").and_then(serde_json::Value::as_i64)
}

/// `(id, name)` of a pool, erroring when either field is absent.
fn pool_identity(pool: &serde_json::Value) -> Result<(i64, String)> {
    let id = pool_id(pool).context("pool missing 'id' field")?;
    let name = pool_name(pool).context("pool missing 'name' field")?;
    Ok((id, name.to_owned()))
}

/// Select a self-hosted pool by runner-group name.
///
/// Mirrors `ConfigurationManager.ConfigureAsync`: the first internal
/// (`isInternal`) pool is the default; a server that advertises no self-hosted
/// group at all is used through it directly, and otherwise the requested name
/// — defaulted to the internal group's name, as the official
/// `command.GetRunnerGroupName(defaultPool?.Name)` does — is matched
/// case-insensitively among the self-hosted (`!isHosted`) pools. Defaulting to
/// the *discovered* name is what keeps an omitted `--runner-group` working
/// when the default group has been renamed on the server.
fn select_pool(pools: &[serde_json::Value], runner_group: Option<&str>) -> Result<(i64, String)> {
    const NO_SELF_HOSTED_GROUPS: &str =
        "Could not find any self-hosted runner groups. Contact support.";

    let self_hosted: Vec<&serde_json::Value> =
        pools.iter().filter(|pool| !pool_is_hosted(pool)).collect();
    let internal = pools.iter().find(|pool| pool_is_internal(pool));

    if self_hosted.is_empty() {
        let default = internal.context(NO_SELF_HOSTED_GROUPS)?;
        return pool_identity(default);
    }

    let requested = match runner_group.map(str::trim).filter(|name| !name.is_empty()) {
        Some(name) => name,
        // `GetRunnerGroupName(defaultPool?.Name)`: the discovered internal
        // group's name is the argument default, or the literal `"default"`
        // when the server reports no internal pool.
        None => internal.and_then(pool_name).unwrap_or("default"),
    };

    let matches: Vec<&serde_json::Value> = self_hosted
        .iter()
        .copied()
        .filter(|pool| pool_name(pool).is_some_and(|name| name.eq_ignore_ascii_case(requested)))
        .collect();

    match matches.as_slice() {
        [] => {
            let groups = self_hosted
                .iter()
                .filter_map(|pool| pool_name(pool))
                .collect::<Vec<_>>()
                .join(", ");
            bail!(
                "Could not find any self-hosted runner group named \"{requested}\". \
                 Available self-hosted groups: {groups}"
            );
        }
        [pool] => pool_identity(pool),
        _ => {
            let groups = matches
                .iter()
                .filter_map(|pool| pool_name(pool).zip(pool_id(pool)))
                .map(|(name, id)| format!("{name} (id {id})"))
                .collect::<Vec<_>>()
                .join(", ");
            bail!("More than one self-hosted runner group is named \"{requested}\": {groups}");
        }
    }
}

/// Decode a VSS-typed JSON scalar to a bool. Agent properties arrive as
/// `{"$type": "System.Boolean", "$value": <bool>}`; accept bare bools and
/// case-insensitive "true"/"false" strings as fallbacks. Anything else is
/// `None` so the caller decides the default.
fn wire_bool(v: &serde_json::Value) -> Option<bool> {
    v.as_bool().or_else(|| match v.as_str() {
        Some(s) if s.eq_ignore_ascii_case("true") => Some(true),
        Some(s) if s.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    })
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

    if let Some(agents) = resp.get("value").and_then(|v| v.as_array())
        && let Some(agent) = agents.first()
    {
        return Ok(agent.get("id").and_then(|v| v.as_i64()));
    }
    Ok(None)
}

/// Create the agent on the server (correct endpoint, full field set).
async fn create_agent(
    http: &HttpClient,
    reg: &RegistrationResult,
    name: &str,
    public_key_xml: &str,
    args: &ConfigureArgs,
    pool_id: i64,
) -> Result<serde_json::Value> {
    // Use _apis/distributedtask/ (not _apis/v1/)
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

    // Full field set matching official runner (golden flow 6)
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
/// Cache validation : each `externals/nodeXX` must contain `preloop-node.json`
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
                warn!(
                    "could not fetch SHASUMS256.txt for {version_v}: {e:#}; relying on pinned SHA only"
                );
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
            use flate2::Compression;
            use flate2::write::GzEncoder;
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

#[cfg(test)]
mod registration_parity_tests {
    use super::*;

    #[test]
    fn registration_body_uses_remove_event() {
        assert_eq!(
            registration_body("https://github.com/preloopdev/preloop", "remove",),
            serde_json::json!({
                "url": "https://github.com/preloopdev/preloop",
                "runner_event": "remove",
            })
        );
    }

    #[test]
    fn agent_delete_url_matches_distributedtask_endpoint() {
        assert_eq!(
            agent_delete_url(
                "https://pipelinesghubeus24.actions.githubusercontent.com/tenant",
                1071,
            ),
            "https://pipelinesghubeus24.actions.githubusercontent.com/tenant/_apis/distributedtask/pools/0/agents/1071",
        );
    }

    #[test]
    fn fips_property_accepts_vss_boolean_value() {
        let property = serde_json::json!({
            "$type": "System.Boolean",
            "$value": false,
        });
        let value = property.get("$value").and_then(wire_bool).unwrap_or(true);
        assert!(!value);
        assert_eq!(if value { "True" } else { "False" }, "False",);
    }

    #[test]
    fn wire_bool_rejects_unknown_strings() {
        assert_eq!(wire_bool(&serde_json::json!("TRUE")), Some(true));
        assert_eq!(wire_bool(&serde_json::json!("false")), Some(false));
        assert_eq!(wire_bool(&serde_json::json!("not-a-bool")), None);
        assert_eq!(wire_bool(&serde_json::json!(null)), None);
    }
    #[test]
    fn runner_group_selects_matching_self_hosted_pool() {
        let pools = serde_json::json!([
            {"id": 1, "name": "Default", "isHosted": false, "isInternal": true},
            {"id": 2, "name": "GitHub Actions", "isHosted": true, "isInternal": false},
            {"id": 3, "name": "GPU", "isHosted": false, "isInternal": false},
        ]);
        assert_eq!(
            select_pool(pools.as_array().unwrap(), Some("gpu")).unwrap(),
            (3, "GPU".to_owned()),
        );
    }

    /// The pools response the official runner recorded against GitHub
    /// (`.runner-watch/golden/v2.336.0/01-register-and-idle/flows.jsonl`):
    /// `Default` (1, internal, self-hosted) and `GitHub Actions` (2, hosted).
    #[test]
    fn runner_group_defaults_to_the_discovered_internal_pool() {
        let pools = serde_json::json!([
            {"id": 1, "name": "Renamed Default", "isHosted": false, "isInternal": true},
            {"id": 2, "name": "GitHub Actions", "isHosted": true, "isInternal": false},
        ]);
        // Omitted --runner-group: `command.GetRunnerGroupName(defaultPool?.Name)`
        // supplies the name, so a renamed internal group still resolves instead
        // of failing on the literal "default".
        assert_eq!(
            select_pool(pools.as_array().unwrap(), None).unwrap(),
            (1, "Renamed Default".to_owned()),
        );
        // An explicit name is matched against the server's names only.
        assert_eq!(
            select_pool(pools.as_array().unwrap(), Some("renamed default")).unwrap(),
            (1, "Renamed Default".to_owned()),
        );
        let error = select_pool(pools.as_array().unwrap(), Some("default")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Available self-hosted groups: Renamed Default")
        );
    }

    #[test]
    fn runner_group_ambiguity_lists_matching_groups() {
        let pools = serde_json::json!([
            {"id": 1, "name": "GPU", "isHosted": false},
            {"id": 2, "name": "gpu", "isHosted": false},
        ]);
        let error = select_pool(pools.as_array().unwrap(), Some("Gpu")).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("GPU (id 1)"));
        assert!(message.contains("gpu (id 2)"));
    }

    #[test]
    fn runner_group_absent_from_the_pool_list_lists_the_candidates() {
        let pools = serde_json::json!([
            {"id": 1, "name": "Default", "isHosted": false, "isInternal": true},
            {"id": 2, "name": "GitHub Actions", "isHosted": true},
        ]);
        let error = select_pool(pools.as_array().unwrap(), Some("GPU")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Available self-hosted groups: Default")
        );

        // No self-hosted group at all: the internal pool decides, or the
        // server is reported as unusable exactly like the official runner.
        let only_hosted =
            serde_json::json!([{"id": 2, "name": "GitHub Actions", "isHosted": true}]);
        let error = select_pool(only_hosted.as_array().unwrap(), None).unwrap_err();
        assert!(error.to_string().contains("Contact support."));

        // No internal pool reported: the literal `"default"` is the argument
        // default (`defaultPoolName ?? "default"`) and is still name-matched.
        let unmarked = serde_json::json!([
            {"id": 4, "name": "Default", "isHosted": false},
            {"id": 2, "name": "GitHub Actions", "isHosted": true},
        ]);
        assert_eq!(
            select_pool(unmarked.as_array().unwrap(), None).unwrap(),
            (4, "Default".to_owned()),
        );
    }
}
