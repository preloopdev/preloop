use super::*;

const LOCAL_JWT_LIFETIME: Duration = Duration::from_secs(2999);

/// A debug credential is acquired before the first step and remains in use
/// through both the six-hour GitHub job limit and this server's four-hour
/// pause-credit window. Keep a small allowance for setup and transport around
/// those two bounded intervals.
pub(crate) const DEBUG_WORKER_TOKEN_LIFETIME: Duration =
    Duration::from_secs((6 + 4) * 60 * 60 + 5 * 60);

impl AppState {
    pub(crate) fn local_jwt(&self, claims: serde_json::Value) -> Result<String, ApiError> {
        self.local_jwt_with_lifetime(claims, LOCAL_JWT_LIFETIME)
    }

    fn local_jwt_with_lifetime(
        &self,
        mut claims: serde_json::Value,
        lifetime: Duration,
    ) -> Result<String, ApiError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| ApiError::bad_request(format!("system clock before epoch: {error}")))?
            .as_secs();
        let expires_at = now
            .checked_add(lifetime.as_secs())
            .ok_or_else(|| ApiError::bad_request("JWT expiration exceeds u64"))?;
        let claims = claims
            .as_object_mut()
            .ok_or_else(|| ApiError::bad_request("JWT claims must be an object"))?;
        claims.insert("iss".to_owned(), json!("https://aksh.local"));
        claims.insert("iat".to_owned(), json!(now));
        claims.insert("nbf".to_owned(), json!(now));
        claims.insert("exp".to_owned(), json!(expires_at));
        let header = json!({
            "alg": "HS256",
            "typ": "JWT",
            "kid": "aksh-local"
        });
        let signing_input = format!(
            "{}.{}",
            base64_url_json(&header)?,
            base64_url_json(&serde_json::Value::Object(claims.clone()))?
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.local_jwt_key)
            .map_err(|error| ApiError::bad_request(format!("invalid signing key: {error}")))?;
        mac.update(signing_input.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!("{signing_input}.{signature}"))
    }

    pub(crate) fn verify_local_jwt_claims(&self, token: &str) -> Option<serde_json::Value> {
        let parts: Vec<&str> = token.splitn(3, '.').collect();
        if parts.len() != 3 {
            return None;
        }
        let header_bytes = URL_SAFE_NO_PAD.decode(parts[0]).ok()?;
        let header: serde_json::Value = serde_json::from_slice(&header_bytes).ok()?;
        if header.get("alg").and_then(|value| value.as_str()) != Some("HS256") {
            return None;
        }
        let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
        let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        let exp = payload.get("exp").and_then(|value| value.as_u64())?;
        let nbf = payload.get("nbf").and_then(|value| value.as_u64());
        if exp <= now || nbf.is_some_and(|value| value > now + 30) {
            return None;
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.local_jwt_key).ok()?;
        mac.update(format!("{}.{}", parts[0], parts[1]).as_bytes());
        let signature = URL_SAFE_NO_PAD.decode(parts[2]).ok()?;
        mac.verify_slice(&signature).ok()?;
        Some(payload)
    }

    /// Sign an action-archive download ticket.
    ///
    /// The download route is deliberately bearerless — the official runner
    /// treats archive URLs as tickets — and it is reachable from inside every
    /// runner VM, where workflow code runs. Unsigned, it let a guest ask the
    /// engine to fetch *any* `owner/repo` tarball using the engine's own
    /// GitHub credential, which reads repositories the workflow was never
    /// granted. Signing binds each URL to the one action it was minted for.
    pub(crate) fn sign_action_ticket(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
        expires_at: u64,
    ) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.local_jwt_key)
            .expect("HMAC accepts keys of any length");
        mac.update(action_ticket_payload(owner, repo, git_ref, expires_at).as_bytes());
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    }

    /// Whether `signature` authorises this exact action at this expiry.
    pub(crate) fn verify_action_ticket(
        &self,
        owner: &str,
        repo: &str,
        git_ref: &str,
        expires_at: u64,
        signature: &str,
    ) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|value| value.as_secs())
            .unwrap_or(u64::MAX);
        if expires_at <= now {
            return false;
        }
        let Ok(provided) = URL_SAFE_NO_PAD.decode(signature) else {
            return false;
        };
        let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(&self.local_jwt_key) else {
            return false;
        };
        mac.update(action_ticket_payload(owner, repo, git_ref, expires_at).as_bytes());
        // Constant-time comparison: `verify_slice` rejects length mismatches
        // and never short-circuits on the first differing byte.
        mac.verify_slice(&provided).is_ok()
    }

    pub(crate) fn verify_local_jwt_scope(&self, token: &str, expected_scope: &str) -> bool {
        self.verify_local_jwt_claims(token)
            .and_then(|payload| {
                payload
                    .get("scp")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            })
            .as_deref()
            == Some(expected_scope)
    }

    pub(crate) fn runner_id_from_token(&self, token: &str) -> Option<i64> {
        let payload = self.verify_local_jwt_claims(token)?;
        let scope = payload.get("scp")?.as_str()?;
        if !scope
            .split_whitespace()
            .any(|value| value == "ActionsRuntime.RunnerListen")
        {
            return None;
        }
        payload
            .get("sub")?
            .as_str()?
            .strip_prefix("aksh-runner-listen-")?
            .parse()
            .ok()
    }

    /// Agent job UUID a runtime token was minted for.
    ///
    /// The counterpart to [`Self::runner_id_from_token`]: a job runtime token
    /// names exactly one job, so any surface a worker calls can authorize
    /// against the job rather than merely against token validity.
    pub(crate) fn job_uuid_from_token(&self, token: &str) -> Option<uuid::Uuid> {
        let payload = self.verify_local_jwt_claims(token)?;
        // `scp` is `Actions.Results:{plan_id}:{job_id}`; `sub` is the job on
        // its own. Require both to agree so a token minted for a different
        // surface cannot be replayed here.
        let subject_job = payload
            .get("sub")?
            .as_str()?
            .strip_prefix("aksh-job-")?
            .parse::<uuid::Uuid>()
            .ok()?;
        let scope_job = payload
            .get("scp")?
            .as_str()?
            .strip_prefix("Actions.Results:")?
            .rsplit(':')
            .next()?
            .parse::<uuid::Uuid>()
            .ok()?;
        (subject_job == scope_job).then_some(subject_job)
    }

    /// Agent job UUID a debug-worker token was minted for.
    ///
    /// Distinct from [`Self::job_uuid_from_token`]: the runtime token is
    /// handed to workflow code as `GITHUB_TOKEN`, so accepting it on debug
    /// surfaces would let an untrusted step forge session requests for its own
    /// job. This token never leaves the trusted runner process.
    pub(crate) fn job_uuid_from_debug_token(&self, token: &str) -> Option<uuid::Uuid> {
        let payload = self.verify_local_jwt_claims(token)?;
        // `scp` is `DebugWorker:{plan_id}:{job_id}`; `sub` is the job on its
        // own. Require both to agree so a token minted for a different surface
        // cannot be replayed here.
        let subject_job = payload
            .get("sub")?
            .as_str()?
            .strip_prefix("aksh-debug-worker-")?
            .parse::<uuid::Uuid>()
            .ok()?;
        let scope_job = payload
            .get("scp")?
            .as_str()?
            .strip_prefix("DebugWorker:")?
            .rsplit(':')
            .next()?
            .parse::<uuid::Uuid>()
            .ok()?;
        (subject_job == scope_job).then_some(subject_job)
    }

    pub(crate) fn mint_runtime_token(&self, plan_id: &str, job_id: &uuid::Uuid) -> String {
        self.local_jwt(json!({
            "sub": format!("aksh-job-{job_id}"),
            "scp": format!("Actions.Results:{plan_id}:{job_id}"),
        }))
        .expect("fixed local JWT claims must serialize")
    }

    /// Mint the token the runner process uses to speak for a job's debug
    /// session, kept separate from the runtime token that workflow code sees.
    ///
    /// Reached only through [`crate::debug_sessions::issue_worker_token`], and
    /// never as a job variable: the official runner copies every secret
    /// variable into the `secrets` context, so a job message is a publication
    /// channel to the workflow being debugged.
    pub(crate) fn mint_debug_worker_token(&self, plan_id: &str, job_id: &uuid::Uuid) -> String {
        self.local_jwt_with_lifetime(
            json!({
                "sub": format!("aksh-debug-worker-{job_id}"),
                "scp": format!("DebugWorker:{plan_id}:{job_id}"),
            }),
            DEBUG_WORKER_TOKEN_LIFETIME,
        )
        .expect("fixed local JWT claims must serialize")
    }
}

#[derive(Clone)]
pub struct SharedState {
    /// Inner application state.
    pub state: AppState,
    /// Cancellation token for graceful shutdown.
    pub shutdown: CancellationToken,
}

/// Application state.
#[derive(Clone)]
pub struct AppState {
    pub(crate) inner: Arc<Mutex<InnerState>>,
    pub(crate) events: broadcast::Sender<NdjsonEvent>,
    pub(crate) message_notify: Arc<Notify>,
    /// Atomic counter for pre-allocating request IDs outside the dispatch
    /// lock.  Monotonically increases; the inner counter is no longer the
    /// source of truth once this is in use.
    pub(crate) next_request_id: Arc<std::sync::atomic::AtomicI64>,
    /// Jobs accepted and still waiting for a runner, refreshed whenever one
    /// is claimed. A supervising runner pool reads it to decide whether the
    /// work already queued outruns the runners it has left.
    pub queue_depth: Arc<std::sync::atomic::AtomicUsize>,
    /// `runs-on` labels of the job at the front of the dispatch queue,
    /// refreshed after each claim so a co-hosted runner pool can select the
    /// right golden before the next fork.
    pub next_job_runs_on: Arc<std::sync::RwLock<Vec<String>>>,
    pub(crate) cache: CacheStore,
    pub(crate) artifacts: ArtifactStore,
    /// Optional GitHub App Webhook Secret for signature verification.
    pub webhook_secret: Option<String>,
    /// Optional local workspace path to load workflows from.
    pub local_workspace: Option<PathBuf>,
    /// State directory for replay/log storage.
    pub state_dir: PathBuf,
    /// Native API administrator credential for this server instance.
    pub(crate) system_token: String,
    /// Per-instance HMAC key for runner and job JWTs.
    pub(crate) local_jwt_key: Vec<u8>,
    /// When enabled, message polling returns the official AccessDenied/ErrorCode=1
    /// runner-version deprecation response. Disabled by default for local runners.
    pub runner_version_deprecated: bool,
    /// Optional cron scheduler (active when `--enable-scheduler` is set).
    pub scheduler: Option<Arc<crate::scheduler::Scheduler>>,
    /// GitHub App credentials for minting installation tokens.
    ///
    /// `None` when no App is configured, in which case job tokens fall back to
    /// `AKSH_GITHUB_TOKEN` and then to the local HMAC JWT.
    pub(crate) github_app: Option<crate::github_app::GitHubAppCredentials>,
    /// Static PAT used for job tokens when no GitHub App is configured.
    ///
    /// Sourced from `AKSH_GITHUB_TOKEN` (which wins) or, failing that, the
    /// config file's `github.pat` — the credential `preloop setup github
    /// --via pat` writes. Without this, a PAT-only setup would report success
    /// while every job silently received the local runtime token instead.
    pub(crate) github_pat: Option<aksh_gha_protocol::SecretString>,
    /// Stored job secrets from the config file (`[secrets]` + per-repo
    /// tables), injected into every job whose trust tier allows secrets
    /// (mirroring GitHub org/repo secrets). Submission-provided secrets
    /// take precedence per name. Writable at runtime by the live secrets
    /// API, which also persists the config file.
    pub(crate) secrets: Arc<parking_lot::RwLock<SecretStore>>,
    /// Serializes the live secrets API's load → mutate → persist → publish
    /// sequence. `set_secret`/`delete_secret` read the whole config file,
    /// change one entry and write the file back; without mutual exclusion
    /// two concurrent requests read the same base config and the second
    /// rename drops the first request's secret, leaving the in-memory store
    /// holding both mutations while the file holds only one.
    ///
    /// Lock ordering, to keep this deadlock-free: acquire this mutex
    /// FIRST, then (briefly, and only after the file write succeeded) the
    /// `secrets` `RwLock`. Never acquire it while holding `secrets` or the
    /// global `inner` mutex, and never acquire `inner` while holding it —
    /// the secrets handlers touch neither ordering partner.
    pub(crate) secret_mutation: Arc<Mutex<()>>,
    /// The config file this engine is pinned to, resolved once at startup.
    ///
    /// Every engine-side read and write of configuration goes through this
    /// rather than re-resolving `PRELOOP_CONFIG`, so one engine stays bound
    /// to one file for its whole life and cannot be retargeted underneath
    /// itself by a later environment change.
    pub(crate) config_path: PathBuf,
    /// One-time provision tokens issued by the embedded runner pool, one per
    /// machine provisioning event, forwarded by the runner's `configure`
    /// call inside the fresh VM. Registration presenting a matching token is
    /// trusted to be pool-originated, which is what authorizes stamping a
    /// job → runner assignment a rogue machine-side process cannot mint.
    pub pending_registrations: Arc<std::sync::RwLock<BTreeMap<String, std::time::SystemTime>>>,
}

/// The runtime secret store: a global tier injected everywhere plus
/// per-repository tiers that override the global tier by name.
#[derive(Clone, Default)]
pub(crate) struct SecretStore {
    /// Global secrets, injected into every trusted job.
    pub global: BTreeMap<String, String>,
    /// Per-repository secrets, keyed by `owner/repo`. A name here wins over
    /// the same name in `global` for jobs of that repository.
    pub repo: BTreeMap<String, BTreeMap<String, String>>,
}

/// Redacting `Debug`: the store holds plaintext secret values, so a single
/// `debug!(?store)` on the derived impl would disclose every stored secret.
/// Only counts and names are rendered.
impl std::fmt::Debug for SecretStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repo_names: BTreeMap<&String, Vec<&String>> = self
            .repo
            .iter()
            .map(|(scope, names)| (scope, names.keys().collect()))
            .collect();
        f.debug_struct("SecretStore")
            .field("global_count", &self.global.len())
            .field("global_names", &self.global.keys().collect::<Vec<_>>())
            .field("repo_names", &repo_names)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OidcJobContext {
    pub(crate) environment: Option<String>,
    pub(crate) job_workflow_ref: Option<String>,
    /// Immutable commit SHA of the called reusable workflow, when resolved.
    pub(crate) job_workflow_sha: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct JobSetId {
    pub(crate) run_id: RunId,
    pub(crate) job_ids: BTreeSet<JobId>,
}

impl JobSetId {
    pub(crate) fn holder(&self) -> concurrency::Holder {
        concurrency::Holder::JobSet {
            run_id: self.run_id,
            job_ids: self.job_ids.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JobSetGate {
    pub(crate) key: (String, String),
    pub(crate) display_name: String,
    pub(crate) cancel_in_progress: bool,
    pub(crate) queue: aksh_gha_parser::ConcurrencyQueue,
}

#[derive(Debug, Clone)]
pub(crate) struct JobSetAdmission {
    pub(crate) gates: Vec<JobSetGate>,
    pub(crate) acquired_keys: BTreeSet<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobSetAdmissionResult {
    Ready,
    Blocked,
}

impl AppState {
    pub async fn new(state_dir: PathBuf) -> anyhow::Result<Self> {
        let config_path = crate::config::config_path();
        Self::new_with_config(state_dir, config_path).await
    }

    /// [`AppState::new`] against an explicit config file.
    ///
    /// The path is resolved once, here, and then stored: every later read and
    /// write goes through the stored value rather than re-reading
    /// `PRELOOP_CONFIG`. That keeps one engine pinned to one file for its
    /// whole life, and lets tests point at a temp file without mutating
    /// process-wide environment state that other tests race against.
    pub async fn new_with_config(state_dir: PathBuf, config_path: PathBuf) -> anyhow::Result<Self> {
        let cache = CacheStore::new(state_dir.join("cache")).await?;
        let artifacts = ArtifactStore::new(state_dir.join("artifacts")).await?;
        let (events, _) = broadcast::channel(1024);
        let oidc_state_dir = state_dir.clone();
        let keypair_handle = tokio::task::spawn_blocking(AgentRsaKeypair::generate);
        let oidc_handle =
            tokio::task::spawn_blocking(move || load_or_generate_oidc_keypair(&oidc_state_dir));
        #[cfg(not(test))]
        let hmac_handle = {
            let hmac_state_dir = state_dir.clone();
            tokio::task::spawn_blocking(move || load_or_generate_hmac_key(&hmac_state_dir))
        };
        #[cfg(not(test))]
        let (keypair_result, oidc_result, hmac_result) =
            tokio::join!(keypair_handle, oidc_handle, hmac_handle);
        #[cfg(test)]
        let (keypair_result, oidc_result) = tokio::join!(keypair_handle, oidc_handle);
        let keypair = keypair_result??;
        let oidc_keypair = oidc_result??;
        let system_token =
            env::var("AKSH_SYSTEM_TOKEN").unwrap_or_else(|_| DEFAULT_AKSH_SYSTEM_TOKEN.to_owned());
        #[cfg(test)]
        let local_jwt_key = TEST_LOCAL_JWT_KEY.to_vec();
        #[cfg(not(test))]
        let local_jwt_key = hmac_result??;
        let registry_path = state_dir.join("artifact_v2_registry.json");
        let (registry, next_id) = if registry_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&registry_path) {
                if let Ok(map) = serde_json::from_str::<BTreeMap<String, ArtifactV2Entry>>(&content)
                {
                    let max_id = map.values().map(|e| e.id).max().unwrap_or(0);
                    (map, max_id)
                } else {
                    (BTreeMap::new(), 0)
                }
            } else {
                (BTreeMap::new(), 0)
            }
        } else {
            (BTreeMap::new(), 0)
        };
        let inner = InnerState {
            agent_keypair: Some(keypair),
            artifact_v2_registry: registry,
            next_artifact_v2_id: next_id,
            oidc_keypair: Some(oidc_keypair),
            ..Default::default()
        };
        let webhook_secret = std::env::var("AKSH_WEBHOOK_SECRET").ok();
        let local_workspace = std::env::var("AKSH_LOCAL_WORKSPACE")
            .ok()
            .map(PathBuf::from);
        let runner_version_deprecated = std::env::var("AKSH_RUNNER_VERSION_DEPRECATED")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes"
                )
            })
            .unwrap_or(false);
        let config = crate::config::load_config_from(&config_path)?;
        let github_app = crate::github_app::load_from(&config)?;
        // Env wins over the config file, matching every other `AKSH_GITHUB_*`
        // override. An empty value in either source counts as unset.
        let github_pat = env::var("AKSH_GITHUB_TOKEN")
            .ok()
            .filter(|pat| !pat.is_empty())
            .or_else(|| config.github.pat.clone().filter(|pat| !pat.is_empty()))
            .map(aksh_gha_protocol::SecretString::new);
        let secrets = Arc::new(parking_lot::RwLock::new(SecretStore {
            global: config.secrets,
            repo: config.repo_secrets,
        }));
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            events,
            message_notify: Arc::new(Notify::new()),
            next_request_id: Arc::new(std::sync::atomic::AtomicI64::new(1)),
            queue_depth: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            next_job_runs_on: Arc::new(std::sync::RwLock::new(Vec::new())),
            cache,
            artifacts,
            webhook_secret,
            local_workspace,
            state_dir,
            system_token,
            local_jwt_key,
            runner_version_deprecated,
            scheduler: None,
            secrets,
            secret_mutation: Arc::new(Mutex::new(())),
            github_app,
            github_pat,
            config_path,
            pending_registrations: Arc::new(std::sync::RwLock::new(BTreeMap::new())),
        })
    }

    pub(crate) async fn emit(&self, event: NdjsonEvent) {
        let _ = self.events.send(event);
    }

    /// Plaintext static PAT for job tokens, when one is configured.
    ///
    /// A job message carries the credential in the clear, so this is a genuine
    /// protocol boundary; the single `expose` below is the whole reason the
    /// field is wrapped everywhere else.
    pub(crate) fn static_github_pat(&self) -> Option<String> {
        // Deliberately `let ... else` rather than `Option::map`: the audit rule
        // `no-expose-in-loop` flags any `.expose()` inside a closure, and a
        // `match` here trips clippy::manual_map. This form satisfies both.
        let Some(pat) = &self.github_pat else {
            return None;
        };
        Some(pat.expose().to_owned())
    }
}
/// Canonical bytes covered by an action ticket signature.
///
/// The separator cannot appear in a sanitised owner/repo (both reject `/` and
/// `.`), so no two distinct actions can produce the same payload.
fn action_ticket_payload(owner: &str, repo: &str, git_ref: &str, expires_at: u64) -> String {
    format!("action-archive\n{owner}\n{repo}\n{git_ref}\n{expires_at}")
}

#[cfg(test)]
pub(crate) fn mint_runtime_token(plan_id: &str, job_id: &uuid::Uuid) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock must be after epoch")
        .as_secs();
    let header = json!({"alg": "HS256", "typ": "JWT", "kid": "aksh-local"});
    let claims = json!({
        "iss": "https://aksh.local",
        "iat": now,
        "nbf": now,
        "exp": now + 2999,
        "sub": format!("aksh-job-{job_id}"),
        "scp": format!("Actions.Results:{plan_id}:{job_id}"),
    });
    let signing_input = format!(
        "{}.{}",
        base64_url_json(&header).expect("test header serializes"),
        base64_url_json(&claims).expect("test claims serialize"),
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(TEST_LOCAL_JWT_KEY).expect("test key is valid");
    mac.update(signing_input.as_bytes());
    format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

/// Load the OIDC signing keypair from `<state_dir>/oidc-key.json` and its
/// certificate from `<state_dir>/oidc-cert.der`, generating missing material.
pub(crate) fn load_or_generate_oidc_keypair(
    state_dir: &std::path::Path,
) -> anyhow::Result<oidc::OidcKeypair> {
    let key_path = state_dir.join("oidc-key.json");
    let certificate_path = state_dir.join("oidc-cert.der");
    std::fs::create_dir_all(state_dir)?;

    if key_path.exists() {
        set_private_file_permissions(&key_path)?;
        let content = std::fs::read_to_string(&key_path).map_err(|error| {
            anyhow::anyhow!("failed to read OIDC key {}: {error}", key_path.display())
        })?;
        let params =
            serde_json::from_str::<aksh_gha_protocol::crypto::RsaParametersExport>(&content)
                .map_err(|error| {
                    anyhow::anyhow!("invalid OIDC key {}: {error}", key_path.display())
                })?;
        let kp = if certificate_path.exists() {
            set_private_file_permissions(&certificate_path)?;
            let certificate_der = std::fs::read(&certificate_path).map_err(|error| {
                anyhow::anyhow!(
                    "failed to read OIDC certificate {}: {error}",
                    certificate_path.display()
                )
            })?;
            oidc::OidcKeypair::from_params_and_certificate(&params, &certificate_der)?
        } else {
            let kp = oidc::OidcKeypair::from_params(&params)?;
            persist_private_file(&certificate_path, kp.certificate_der())?;
            kp
        };
        return Ok(kp);
    }

    let kp = oidc::OidcKeypair::generate()?;
    let json = serde_json::to_vec(&kp.params())?;
    persist_private_file(&key_path, &json)?;
    persist_private_file(&certificate_path, kp.certificate_der())?;
    Ok(kp)
}

fn persist_private_file(path: &std::path::Path, bytes: &[u8]) -> anyhow::Result<()> {
    let temp_path = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let result = (|| -> anyhow::Result<()> {
        use std::io::Write;
        let mut file = options.open(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temp_path, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

fn set_private_file_permissions(path: &std::path::Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = std::fs::metadata(path)?.mode();
        if mode & 0o077 != 0 {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
    }
    Ok(())
}

#[cfg(not(test))]
/// Load or generate a 32-byte HMAC key for local JWT signing.
///
/// Persisted to `<state_dir>/hmac-key.bin` so runtime tokens survive restarts.
pub(crate) fn load_or_generate_hmac_key(state_dir: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    let key_path = state_dir.join("hmac-key.bin");
    if key_path.exists() {
        let key = std::fs::read(&key_path).map_err(|error| {
            anyhow::anyhow!("failed to read HMAC key {}: {error}", key_path.display())
        })?;
        if key.len() == 32 {
            return Ok(key);
        }
        // Wrong size — regenerate.
    }
    let mut key = vec![0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut key);
    std::fs::create_dir_all(state_dir)?;
    let temp_path = state_dir.join(format!("hmac-key.{}.tmp", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp_path)?;
    use std::io::Write;
    file.write_all(&key)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temp_path, &key_path).map_err(|error| {
        let _ = std::fs::remove_file(&temp_path);
        anyhow::anyhow!("failed to persist HMAC key {}: {error}", key_path.display())
    })?;
    Ok(key)
}

impl InnerState {
    /// Return the runner that owns a listener session.
    pub(crate) fn runner_id_for_session(&self, session_id: &str) -> Option<i64> {
        self.broker_session_runners
            .get(session_id)
            .copied()
            .or_else(|| {
                self.sessions
                    .get(session_id)
                    .map(|session| session.runner_id)
            })
    }

    /// Look up dispatch metadata for the runner that owns a given session.
    pub(crate) fn runner_capabilities_for_session(&self, session_id: &str) -> RunnerCapabilities {
        self.runner_id_for_session(session_id)
            .and_then(|runner_id| self.runners.get(&runner_id))
            .map(|runner| RunnerCapabilities {
                known: true,
                labels: runner.labels.clone(),
                runner_group_id: runner.runner_group_id,
                runner_group_name: runner.runner_group_name.clone(),
            })
            .unwrap_or_default()
    }
}

#[derive(Default)]
pub(crate) struct InnerState {
    pub(crate) runs: BTreeMap<RunId, RunRecord>,
    pub(crate) workflow_run_counters: BTreeMap<String, u64>,
    pub(crate) queue: VecDeque<QueuedJob>,
    pub(crate) pending_jobs: VecDeque<QueuedJob>,
    /// Reusable-caller and dynamic-matrix nodes whose gates are already held
    /// and whose callee subtree still has to be built.
    ///
    /// Building a subtree parses workflow YAML, constructs one runner message
    /// per inner job and mints a runtime token for each, so it scales with the
    /// width of the callee matrix. Doing that while holding the global state
    /// mutex stalls every other request, so promotion only records the intent
    /// here; `drain_expansions` performs the work with the lock released and
    /// applies the result under a fresh one.
    pub(crate) pending_expansions: VecDeque<QueuedJob>,
    /// Nodes currently being expanded with the lock released.
    ///
    /// The entry is the reservation: it stops a second sweep from expanding
    /// the same node, and cancellation drops it so a build that finishes after
    /// the run was cancelled is discarded instead of resurrecting jobs.
    pub(crate) expanding: BTreeSet<(RunId, JobId)>,
    pub(crate) runners: BTreeMap<i64, RegisteredRunner>,
    pub(crate) sessions: BTreeMap<String, RunnerSession>,
    pub(crate) session_keys: BTreeMap<String, SessionEncryption>,
    // test-only: retained for session encryption integration coverage.
    #[allow(dead_code)]
    pub(crate) agent_keypair: Option<AgentRsaKeypair>,
    pub(crate) runner_public_keys: BTreeMap<i64, String>,
    pub(crate) runner_rsa_public_keys: BTreeMap<i64, AgentRsaPublicKey>,
    pub(crate) inflight_messages: BTreeMap<String, BTreeMap<i64, azdo::TaskAgentMessage>>,
    pub(crate) broker_messages: BTreeMap<i64, azdo::AgentJobRequestMessage>,
    /// Short-lived GitHub App credentials still to mint at broker acquisition.
    pub(crate) github_token_requests: BTreeMap<i64, GitHubTokenRequest>,
    pub(crate) runner_client_ids: BTreeMap<String, i64>,
    pub(crate) cancellation_queue: VecDeque<QueuedCancellation>,
    /// Job → runner pairings. While an entry is fresh, the job may only be
    /// claimed by sessions presenting a verified listen-token identity for
    /// that runner. Entries are consumed on successful claim and dropped on
    /// runner deregistration, requeue, or run teardown.
    pub(crate) job_assignments: BTreeMap<(RunId, JobId), AssignmentRecord>,
    /// Pool-managed jobs that are queued but not yet paired with a registered
    /// runner (a machine is being provisioned for them). While fresh, these
    /// cannot be claimed at all — the wait protects against a rogue session
    /// claiming the job before its machine registers.
    pub(crate) pool_pending: BTreeMap<(RunId, JobId), std::time::SystemTime>,
    /// Set when the embedded runner pool provisions machines for queued jobs
    /// (the `preloop serve` flow). Enables assignment enforcement for newly
    /// queued jobs.
    pub(crate) pool_assignments_enabled: bool,
    /// `PRELOOP_REQUIRE_JOB_ASSIGNMENTS`: when true, jobs may only be claimed
    /// through an assignment; unassigned jobs are never delivered, even to
    /// external runners. Default false keeps bring-your-own-runner installs
    /// working unchanged.
    pub(crate) require_job_assignments: bool,
    /// Jobs popped from the queue by a dispatch claim, keyed for requeueing:
    /// if the runner that claimed a job dies mid-execution (machine torn down,
    /// identity purged), the stashed copy is what gets the same job back into
    /// the queue intact instead of waiting for the lease reaper to fail it.
    /// Entries drop on normal completion.
    pub(crate) claimed_jobs: BTreeMap<(RunId, JobId), QueuedJob>,
    /// Recently seen GitHub webhook delivery IDs, so a redelivered (or
    /// double-fired) webhook does not create duplicate runs. An entry is
    /// `InFlight` while the handler is still processing that delivery, so a
    /// concurrent second copy of the same delivery is skipped, and
    /// `Completed` once processing succeeded, so a later redelivery inside
    /// the dedup window is skipped. A failed delivery has its entry removed
    /// entirely: GitHub redelivers after an error response and that retry is
    /// the only remaining chance to process the event, so keeping the
    /// reservation would swallow it permanently. Completed entries are pruned
    /// by the dedup window on every insert.
    pub(crate) webhook_deliveries: VecDeque<(String, WebhookDeliveryState)>,
    pub(crate) pending_caches: BTreeMap<i64, PendingCache>,
    pub(crate) artifacts: BTreeMap<String, ArtifactRecord>,
    pub(crate) logs: BTreeMap<String, Vec<u8>>,
    pub(crate) log_metadata: BTreeMap<String, LogMetadata>,
    pub(crate) timeline_events: BTreeMap<RunId, Vec<NdjsonEvent>>,
    /// Per-timeline changeId counter for timeline PATCH versioning.
    pub(crate) timeline_change_ids: BTreeMap<String, i32>,
    /// Persisted timeline records keyed by `{plan_id}/{timeline_id}`.
    /// Upserted on each PATCH; returned by GET.
    pub(crate) timeline_records: BTreeMap<String, BTreeMap<uuid::Uuid, azdo::TimelineRecord>>,
    pub(crate) live_log_lines:
        BTreeMap<String, Arc<tokio::sync::Mutex<Vec<LiveLogFeedLinesWrapper>>>>,
    pub(crate) live_log_tx: BTreeMap<String, broadcast::Sender<LiveLogFeedLinesWrapper>>,
    pub(crate) inflight_requests: BTreeMap<i64, (RunId, JobId)>,
    pub(crate) job_requests: BTreeMap<i64, TaskAgentJobRequestRecord>,
    pub(crate) plan_requests: BTreeMap<String, i64>,
    pub(crate) agent_job_requests: BTreeMap<uuid::Uuid, i64>,
    pub(crate) timeline_requests: BTreeMap<uuid::Uuid, i64>,
    pub(crate) session_active_requests: BTreeMap<String, i64>,
    /// Modern broker session owner, derived from the runner-listen JWT.
    pub(crate) broker_session_runners: BTreeMap<String, i64>,
    pub(crate) next_runner_id: i64,
    pub(crate) next_cache_id: i64,
    pub(crate) next_message_id: i64,
    pub(crate) next_log_id: usize,
    pub(crate) flows_file: Option<std::fs::File>,
    pub(crate) next_flow_index: usize,
    /// Sessions created via the AzDO distributedtask path (full encrypted message format).
    /// Sessions NOT in this set use the broker-ref (RunnerJobRequest) format.
    pub(crate) azdo_sessions: std::collections::HashSet<String>,
    /// Cache v2 Twirp pending uploads: upload_token → (key, version).
    pub(crate) cache_v2_pending: BTreeMap<String, CacheV2Pending>,
    /// Cache v2 download tokens: dl_token → (key, version).
    pub(crate) cache_v2_dl_tokens: BTreeMap<String, (String, String)>,
    /// Artifact v2 Twirp pending uploads: upload_token → registry_key.
    pub(crate) artifact_v2_pending: BTreeMap<String, ArtifactV2Pending>,
    /// Artifact v2 finalized registry: registry_key → metadata.
    pub(crate) artifact_v2_registry: BTreeMap<String, ArtifactV2Entry>,
    /// Monotonic artifact v2 ID counter.
    pub(crate) next_artifact_v2_id: u64,
    /// Per-job resolved OIDC execution context.
    pub(crate) oidc_job_contexts: BTreeMap<(RunId, JobId), OidcJobContext>,
    /// OIDC issuer URL used in the `iss` claim and discovery document.
    pub(crate) oidc_issuer: String,
    pub(crate) dap_ports: BTreeMap<RunId, DapPortRegistration>,
    /// OIDC signing keypair (RS256) for id-token minting.
    pub(crate) oidc_keypair: Option<oidc::OidcKeypair>,
    /// Per-job `id-token: write` grant, keyed by (run_id, job_id).
    pub(crate) id_token_grants: BTreeMap<(RunId, JobId), bool>,
    /// Concurrency groups keyed by (lowercased repo, lowercased group name).
    pub(crate) concurrency_groups: BTreeMap<(String, String), concurrency::ConcurrencyGroup>,
    /// Workflow-level pending runs: run_id → jobs held out of the ready queue.
    pub(crate) held_runs: BTreeMap<RunId, Vec<QueuedJob>>,
    /// Job-level concurrency-blocked jobs (FIFO).
    pub(crate) concurrency_blocked: VecDeque<QueuedJob>,
    /// Multi-key admission state for reusable workflow invocations.
    pub(crate) jobset_admissions: BTreeMap<JobSetId, JobSetAdmission>,
    /// JobSets whose gates were acquired and whose caller placeholder nodes
    /// still await callee-subtree expansion by the scheduler.
    pub(crate) jobset_ready: BTreeSet<JobSetId>,
    /// Evaluated workflow-level concurrency raw config per run (for release/debug).
    pub(crate) run_concurrency: BTreeMap<RunId, aksh_gha_parser::Concurrency>,
    /// Which concurrency key a holder currently occupies (for release).
    pub(crate) holder_keys: BTreeMap<RunId, Vec<(String, String)>>,
    /// Live debug sessions holding paused jobs open.
    pub(crate) debug_sessions: crate::debug_sessions::DebugSessionRegistry,
}
