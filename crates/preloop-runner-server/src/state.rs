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
        claims.insert("iss".to_owned(), json!("https://preloop.local"));
        claims.insert("iat".to_owned(), json!(now));
        claims.insert("nbf".to_owned(), json!(now));
        claims.insert("exp".to_owned(), json!(expires_at));
        let header = json!({
            "alg": "HS256",
            "typ": "JWT",
            "kid": "preloop-local"
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
            .strip_prefix("preloop-runner-listen-")?
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
            .strip_prefix("preloop-job-")?
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
            .strip_prefix("preloop-debug-worker-")?
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
            "sub": format!("preloop-job-{job_id}"),
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
                "sub": format!("preloop-debug-worker-{job_id}"),
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

#[cfg(test)]
impl AppState {
    /// Wrap this state in a `SharedState` for tests that call handlers taking
    /// `&Arc<SharedState>` directly. Each call makes a fresh token; tests that
    /// need shutdown coordination build the struct explicitly.
    pub(crate) fn shared(&self) -> std::sync::Arc<SharedState> {
        std::sync::Arc::new(SharedState {
            state: self.clone(),
            shutdown: CancellationToken::new(),
        })
    }
}

/// Who may register a runner with the control plane.
///
/// `Strict` is the default and the only safe choice for a deployment reachable
/// over a network: it accepts exactly the system credential. `Permissive`
/// accepts any non-empty credential — matching what GitHub itself cannot do
/// for us (validate third-party registration tokens) but recreating the
/// original "anyone who can reach the port can register a runner" hole, so it
/// exists only for the conformance harness, which replays real GitHub-issued
/// registration tokens this control plane could never have minted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationPolicy {
    /// Only the system credential may register a runner.
    Strict,
    /// Any non-empty credential may register a runner (conformance only).
    Permissive,
}

impl RegistrationPolicy {
    /// Parse the `PRELOOP_REGISTRATION_POLICY` environment variable.
    ///
    /// Unknown or missing values fall back to [`RegistrationPolicy::Strict`]:
    /// a typo must fail closed, never open.
    pub(crate) fn from_env() -> Self {
        match std::env::var("PRELOOP_REGISTRATION_POLICY")
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("permissive") => RegistrationPolicy::Permissive,
            _ => RegistrationPolicy::Strict,
        }
    }
}

/// Application state.
/// Server-visible GitHub endpoints for `github.server_url` / `api_url` /
/// `graphql_url` and their `GITHUB_*` env counterparts. GitHub's server
/// supplies these to every job; aksh exposes the real forge it fronts (env >
/// config file > github.com defaults) so actions that read the host don't
/// silently point at github.com from a GHES-style deployment.
#[derive(Debug, Clone)]
pub struct GitHubUrls {
    pub server_url: String,
    pub api_url: String,
    pub graphql_url: String,
}

/// (owner, repo, ref) → resolution outcome with the instant it was recorded.
///
/// See [`GITHUB_ENV_LOCK`] for why the tests around this are serialized.
///
/// `None` is a cached *failure*. Without it an offline or rate-limited server
/// re-attempts every unresolvable ref on every job dispatch and pays the full
/// connect timeout each time, which is the common local-CI case.
pub(crate) type ActionShaCache = std::sync::Mutex<
    std::collections::HashMap<(String, String, String), (Option<String>, std::time::Instant)>,
>;

/// Serializes tests that mutate the process-global GitHub URL environment
/// variables.
///
/// `std::env` is process-wide while `cargo test` runs the suite on a thread
/// pool, so two tests that point `PRELOOP_GITHUB_API_URL` at their own mock
/// server observe each other's value. Each such test passes in isolation and
/// they fail reliably whenever they are co-scheduled, which is what happens
/// when a developer filters the suite down to a handful of names.
#[cfg(test)]
pub(crate) static GITHUB_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Restores one process-global environment variable when a test exits,
/// including during panic unwinding.
#[cfg(test)]
pub(crate) struct TestEnvVar {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

#[cfg(test)]
impl TestEnvVar {
    pub(crate) fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        Self { key, previous }
    }

    /// Clear a variable for the duration of a test.
    ///
    /// The counterpart to [`TestEnvVar::set`] for tests whose contract is the
    /// *absence* of a value — a configured `PRELOOP_GITHUB_TOKEN` flips the
    /// check-run path from its mock to a live GitHub call, so a test asserting
    /// the mock path has to guarantee no token is visible. Restores on drop,
    /// so a panicking test cannot leak the cleared state onto the rest of the
    /// suite.
    pub(crate) fn unset(key: &'static str) -> Self {
        let previous = std::env::var_os(key);
        std::env::remove_var(key);
        Self { key, previous }
    }
}

#[cfg(test)]
impl Drop for TestEnvVar {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            std::env::set_var(self.key, previous);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub(crate) inner: Arc<Mutex<InnerState>>,
    pub(crate) store: Arc<dyn Store>,
    pub(crate) events: broadcast::Sender<NdjsonEvent>,
    pub(crate) message_notify: Arc<Notify>,
    /// Atomic counter for pre-allocating request IDs outside the dispatch
    /// lock.  Monotonically increases; the inner counter is no longer the
    /// source of truth once this is in use.
    pub(crate) next_request_id: Arc<std::sync::atomic::AtomicI64>,
    /// Observability handle (cloneable, holds heartbeat & limit registries).
    pub(crate) observability: preloop_observability::Observability,
    /// Cached operational snapshot, updated every 5s by the sampler without holding `inner`.
    pub status_snapshot:
        Arc<parking_lot::RwLock<preloop_observability::status::OperationalSnapshot>>,
    /// (run, job) pairs whose terminal transition has already been recorded
    /// (`preloop.job.completed` metric + terminal log). Seeded from the
    /// restored run record at startup; guards `emit` so a replayed terminal
    /// `JobStatus` (repeated timeline PATCH after completion) is recorded
    /// exactly once per job.
    pub(crate) terminal_jobs_recorded: Arc<std::sync::Mutex<BTreeSet<(RunId, JobId)>>>,
    /// Consolidated pool handle replacing the four ad-hoc Option<Arc<…>> fields.
    pub pool_status: Arc<preloop_observability::status::PoolStatus>,
    /// When this AppState was created (for uptime).
    pub(crate) started_at: std::time::Instant,
    /// Jobs accepted and still waiting for a runner, refreshed whenever one
    /// is claimed. A supervising runner pool reads it to decide whether the
    /// work already queued outruns the runners it has left.
    pub queue_depth: Arc<std::sync::atomic::AtomicUsize>,
    /// Raised while a co-hosted runner pool is still preparing its machine
    /// image and cannot register a runner yet; see [`ServerConfig`]. The
    /// starvation sweep pauses the queued-job grace clock while it is set.
    pub pool_preparing: Option<Arc<std::sync::atomic::AtomicBool>>,
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
    /// Registration policy for new runners; see [`RegistrationPolicy`].
    pub(crate) registration_policy: RegistrationPolicy,
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
    /// `PRELOOP_GITHUB_TOKEN` and then to the local HMAC JWT.
    pub(crate) github_app: Option<crate::github_app::GitHubAppCredentials>,
    /// The full registered GitHub App registry (D6). The legacy env-var App
    /// is always the first entry and mirrors `github_app`.
    pub(crate) github_apps: Option<crate::github_app::GitHubApps>,
    /// Short-TTL cache of installation tokens validated against github.com
    /// for the GitHub-compatible dispatch API (D2.4).
    pub(crate) dispatch_token_cache: Arc<crate::dispatch_auth::InstallationTokenCache>,
    /// Short-TTL cache of actor logins resolved for dispatch authentication
    /// (PAT `GET /user`, App `GET /app`).
    pub(crate) dispatch_actor_cache: Arc<crate::dispatch_auth::DispatchActorCache>,
    /// Static PAT used for job tokens when no GitHub App is configured.
    ///
    /// Sourced from `PRELOOP_GITHUB_TOKEN` (which wins) or, failing that, the
    /// config file's `github.pat` — the credential `preloop setup github
    /// --via pat` writes. Without this, a PAT-only setup would report success
    /// while every job silently received the local runtime token instead.
    pub(crate) github_pat: Option<preloop_gha_protocol::SecretString>,
    /// GitHub endpoints surfaced to workflows (server/api/graphql URLs).
    pub(crate) github_urls: GitHubUrls,
    /// Auto-PR policy for webhook-driven push runs (env overrides the config
    /// file, matching every other `PRELOOP_GITHUB_*` override).
    pub(crate) pr_config: crate::config::PrConfig,
    /// Short-TTL cache of resolved action refs (`owner`, `repo`, `ref`) → SHA.
    /// Keeps a matrix fan-out from re-resolving the same `uses:` ref per cell
    /// and bounds GitHub API pressure; entries expire after
    /// [`ACTION_SHA_CACHE_TTL`].
    pub(crate) action_sha_cache: Arc<ActionShaCache>,
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
/// per-repository tiers and per-repository, per-environment tiers that
/// override the coarser tiers by name — mirroring GitHub's org, repo, and
/// environment secrets.
#[derive(Clone, Default)]
pub(crate) struct SecretStore {
    /// Global secrets, injected into every trusted job.
    pub global: BTreeMap<String, String>,
    /// Per-repository secrets, keyed by `owner/repo`. A name here wins over
    /// the same name in `global` for jobs of that repository.
    pub repo: BTreeMap<String, BTreeMap<String, String>>,
    /// Per-repository, per-environment secrets, keyed by `owner/repo` then
    /// environment name. A name here wins over the same name in `repo` and
    /// `global` for jobs of that repository whose `environment:` resolves to
    /// that environment.
    pub env: BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>,
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
        let env_names: BTreeMap<&String, BTreeMap<&String, Vec<&String>>> = self
            .env
            .iter()
            .map(|(scope, envs)| {
                (
                    scope,
                    envs.iter()
                        .map(|(env, names)| (env, names.keys().collect()))
                        .collect(),
                )
            })
            .collect();
        f.debug_struct("SecretStore")
            .field("global_count", &self.global.len())
            .field("global_names", &self.global.keys().collect::<Vec<_>>())
            .field("repo_names", &repo_names)
            .field("env_names", &env_names)
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OidcJobContext {
    pub(crate) environment: Option<String>,
    pub(crate) job_workflow_ref: Option<String>,
    /// Immutable commit SHA of the called reusable workflow, when resolved.
    pub(crate) job_workflow_sha: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JobSetGate {
    pub(crate) key: (String, String),
    pub(crate) display_name: String,
    pub(crate) cancel_in_progress: bool,
    pub(crate) queue: preloop_gha_parser::ConcurrencyQueue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JobSetAdmission {
    pub(crate) gates: Vec<JobSetGate>,
    pub(crate) acquired_keys: BTreeSet<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobSetAdmissionResult {
    Ready,
    Blocked,
}

/// Bounded conclusion label for a terminal execution status.
fn execution_conclusion(status: preloop_gha_protocol::ExecutionStatus) -> &'static str {
    use preloop_gha_protocol::ExecutionStatus as S;
    match status {
        S::Success => "success",
        S::Failure => "failure",
        S::Cancelled => "cancelled",
        S::Skipped => "skipped",
        // Non-terminal statuses never reach here; the guard filters them.
        _ => "unrecognized",
    }
}

/// Classify a termination reason into a bounded code.
///
/// The control plane's `reason` is not a code — several paths build a prose
/// sentence that interpolates the job's `runs-on` labels (see the starvation
/// sweep in `bootstrap.rs`). Those values are user-controlled, so the raw
/// string must never reach a metric label: it would both explode cardinality
/// and export workflow content. Classify by the stable prefix each path
/// writes, and fall back to `unrecognized` rather than passing prose through.
///
/// The full message is still available on the structured log record; only the
/// metric dimension is bounded.
fn bounded_termination_reason(value: &str) -> &'static str {
    // Exact codes first — these come from `concurrency::*_reason()`.
    match value {
        "concurrency_pending" => return "concurrency_pending",
        "concurrency_cancelled" => return "concurrency_cancelled",
        "timeout" => return "timeout",
        "no_runner" => return "no_runner",
        "lease_expired" => return "lease_expired",
        "deaf_runner" => return "deaf_runner",
        "startup_orphan" => return "startup_orphan",
        _ => {}
    }
    // Prose paths — match on the invariant phrase, never the whole string, so
    // an interpolated label or platform cannot change the classification.
    //
    // Two distinct never-claimable conditions, and conflating them would hide
    // the difference between "wait or add capacity" and "this will never work
    // until you register that platform":
    //   - the starvation sweep, which fires after a grace window;
    //   - the external-host check, where the server has no runner of that
    //     platform class at all (`no {platform} runner is registered with
    //     this server, so `runs-on: …` cannot be scheduled`).
    // The starvation prose interpolates workflow-controlled `runs-on`
    // labels, so the anchored prefix MUST be checked before the substring:
    // a crafted label containing the platform phrase must not flip a
    // starvation reason into `no_platform_runner`.
    if value.starts_with("no runner is registered for") {
        return "no_runner";
    }
    if value.contains("runner is registered with this server") {
        return "no_platform_runner";
    }
    if value.starts_with("job exceeded its timeout")
        || value.starts_with("timed out")
        || value.contains("timeout-minutes")
    {
        return "timeout";
    }
    if value.starts_with("runner stopped polling") || value.contains("deaf") {
        return "deaf_runner";
    }
    if value.contains("lease expired") {
        return "lease_expired";
    }
    "unrecognized"
}

/// Bound free-form reason prose for export as a telemetry attribute. The
/// prose interpolates workflow input (e.g. `runs-on` labels), so one job
/// must not emit an arbitrarily large attribute. Truncation cuts on a
/// character boundary — byte slicing panics on multi-byte input.
fn bounded_reason_detail(detail: &str) -> String {
    const DETAIL_MAX: usize = 512;
    let mut detail = detail.to_string();
    if detail.len() > DETAIL_MAX {
        let cut = detail
            .char_indices()
            .map(|(i, _)| i)
            .take_while(|&i| i <= DETAIL_MAX)
            .last()
            .unwrap_or(0);
        detail.truncate(cut);
    }
    detail
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
        Self::new_with_store(state_dir, config_path, None).await
    }

    /// [`AppState::new_with_config`] against an explicit store URL.
    ///
    /// `store_url` takes precedence over the `PRELOOP_STORE_URL` environment
    /// variable; `None` falls back to the environment, then to SQLite at
    /// `<state_dir>/preloop.db`.
    pub async fn new_with_store(
        state_dir: PathBuf,
        config_path: PathBuf,
        store_url: Option<&str>,
    ) -> anyhow::Result<Self> {
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
        let system_token = env::var("PRELOOP_SYSTEM_TOKEN")
            .unwrap_or_else(|_| DEFAULT_PRELOOP_SYSTEM_TOKEN.to_owned());
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
                    let mut migrated = BTreeMap::new();
                    for (k, v) in map {
                        let parts: Vec<&str> = k.split('/').collect();
                        if parts.len() >= 3 {
                            migrated.insert(format!("{}/{}", parts[0], parts[2..].join("/")), v);
                        } else {
                            migrated.insert(k, v);
                        }
                    }
                    (migrated, max_id)
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
            session_last_seen: BTreeMap::new(),
            runner_liveness_timeout: std::time::Duration::from_secs(
                env::var("PRELOOP_RUNNER_LIVENESS_TIMEOUT_SECS")
                    .ok()
                    .and_then(|raw| raw.trim().parse().ok())
                    .unwrap_or(1800),
            ),
            ..Default::default()
        };
        let store = crate::store::open_store(store_url, &state_dir, &local_jwt_key).await?;
        let mut recovered = inner;
        store.load_into(&mut recovered).await?;
        let next_request_id = recovered
            .job_requests
            .keys()
            .copied()
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let inner = recovered;
        // Seed the terminal-transition marker from the restored run record so
        // a replayed terminal `JobStatus` after a restart cannot double-record
        // `preloop.job.completed` for a job that already completed.
        let terminal_jobs_recorded = Arc::new(std::sync::Mutex::new(
            inner
                .runs
                .iter()
                .flat_map(|(run_id, run)| {
                    run.jobs
                        .iter()
                        .filter(|(_, status)| status.is_terminal())
                        .map(move |(job_id, _)| (*run_id, job_id.clone()))
                })
                .collect::<BTreeSet<(RunId, JobId)>>(),
        ));
        // Capture queue length before moving `inner` into the Mutex so the
        // `queue_depth` atomic is set to the recovered ready-queue size.
        let recovered_queue_len = inner.queue.len();
        let local_workspace = std::env::var("PRELOOP_LOCAL_WORKSPACE")
            .ok()
            .map(PathBuf::from);
        let runner_version_deprecated = std::env::var("PRELOOP_RUNNER_VERSION_DEPRECATED")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes"
                )
            })
            .unwrap_or(false);
        let mut config = crate::config::load_config_from(&config_path)?;
        // systemd credentials (`LoadCredential=preloop-secrets:…`, encrypted
        // at rest, decrypted into a memfd by systemd) override the config
        // file's stored secrets per name. The file may hold nothing at all
        // in `secrets_store = "memory"` mode; the credential is then the
        // durable base set.
        let credential = crate::config::load_credential_secrets()?;
        crate::config::merge_secret_stores(&mut config, credential);
        let github_apps = crate::github_app::load_from(&config)?;
        let github_app = github_apps
            .as_ref()
            .map(|registry| registry.default_app().clone());
        // Env wins over the config file, matching every other credential
        // here. An empty value in either source counts as unset: a blank
        // export must not disable signature verification silently.
        let webhook_secret = env::var("PRELOOP_WEBHOOK_SECRET")
            .ok()
            .filter(|secret| !secret.is_empty())
            .or_else(|| {
                config
                    .github
                    .webhook_secret
                    .clone()
                    .filter(|secret| !secret.is_empty())
            });
        // Env wins over the config file, matching every other `PRELOOP_GITHUB_*`
        // override. An empty value in either source counts as unset.
        let github_pat = env::var("PRELOOP_GITHUB_TOKEN")
            .ok()
            .filter(|pat| !pat.is_empty())
            .or_else(|| config.github.pat.clone().filter(|pat| !pat.is_empty()))
            .map(preloop_gha_protocol::SecretString::new);
        // Env wins over the config file, matching every other `PRELOOP_GITHUB_*`
        // override; an empty value in either source counts as unset.
        let github_urls = GitHubUrls {
            server_url: env::var("PRELOOP_GITHUB_SERVER_URL")
                .ok()
                .filter(|value| !value.is_empty())
                .or_else(|| {
                    config
                        .github
                        .server_url
                        .clone()
                        .filter(|value| !value.is_empty())
                })
                .unwrap_or_else(|| "https://github.com".to_owned()),
            api_url: env::var("PRELOOP_GITHUB_API_URL")
                .ok()
                .filter(|value| !value.is_empty())
                .or_else(|| {
                    config
                        .github
                        .api_url
                        .clone()
                        .filter(|value| !value.is_empty())
                })
                .unwrap_or_else(|| "https://api.github.com".to_owned()),
            graphql_url: env::var("PRELOOP_GITHUB_GRAPHQL_URL")
                .ok()
                .filter(|value| !value.is_empty())
                .or_else(|| {
                    config
                        .github
                        .graphql_url
                        .clone()
                        .filter(|value| !value.is_empty())
                })
                .unwrap_or_else(|| "https://api.github.com/graphql".to_owned()),
        };
        let secrets = Arc::new(parking_lot::RwLock::new(SecretStore {
            global: config.secrets,
            repo: config.repo_secrets,
            env: config.env_secrets,
        }));
        // Env wins over the config file, matching every other `PRELOOP_GITHUB_*`
        // override. An empty value in either source counts as unset.
        let mut pr_config = config.github.pr.clone();
        if let Ok(value) = env::var("PRELOOP_GITHUB_PR_AUTO") {
            if !value.trim().is_empty() {
                pr_config.auto = match value.trim().to_ascii_lowercase().as_str() {
                    "feature" => crate::config::PrAuto::Feature,
                    "never" => crate::config::PrAuto::Never,
                    other => {
                        tracing::warn!(
                            value = other,
                            "unknown PRELOOP_GITHUB_PR_AUTO; expected feature|never"
                        );
                        pr_config.auto
                    }
                };
            }
        }
        if let Ok(value) = env::var("PRELOOP_GITHUB_PR_DRAFT") {
            if !value.trim().is_empty() {
                // A typo (`ture`) must not silently flip the configured
                // draft policy: unknown values keep the configured default
                // and warn, mirroring PRELOOP_GITHUB_PR_AUTO.
                pr_config.draft = match value.trim().to_ascii_lowercase().as_str() {
                    "1" | "true" | "yes" => true,
                    "0" | "false" | "no" => false,
                    other => {
                        tracing::warn!(
                            value = other,
                            "unknown PRELOOP_GITHUB_PR_DRAFT; expected 1|true|yes|0|false|no"
                        );
                        pr_config.draft
                    }
                };
            }
        }
        if let Ok(value) = env::var("PRELOOP_GITHUB_PR_EXCLUDE") {
            if !value.trim().is_empty() {
                pr_config.exclude = value
                    .split(',')
                    .map(str::trim)
                    .filter(|pattern| !pattern.is_empty())
                    .map(str::to_owned)
                    .collect();
            }
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            store,
            events,
            message_notify: Arc::new(Notify::new()),
            next_request_id: Arc::new(std::sync::atomic::AtomicI64::new(next_request_id)),
            observability: preloop_observability::Observability::noop(),
            status_snapshot: Arc::new(parking_lot::RwLock::new(
                preloop_observability::status::OperationalSnapshot::default(),
            )),
            terminal_jobs_recorded,
            pool_status: Arc::new(preloop_observability::status::PoolStatus::default()),
            started_at: std::time::Instant::now(),
            // Mirror the recovered ready-queue size so an on-demand runner
            // pool spawns against the right workload after restart.
            queue_depth: Arc::new(std::sync::atomic::AtomicUsize::new(recovered_queue_len)),
            pool_preparing: None,
            next_job_runs_on: Arc::new(std::sync::RwLock::new(Vec::new())),
            cache,
            artifacts,
            webhook_secret,
            local_workspace,
            state_dir,
            system_token,
            registration_policy: RegistrationPolicy::from_env(),
            local_jwt_key,
            runner_version_deprecated,
            scheduler: None,
            secrets,
            secret_mutation: Arc::new(Mutex::new(())),
            github_app,
            github_apps,
            dispatch_token_cache: Arc::new(crate::dispatch_auth::InstallationTokenCache::default()),
            dispatch_actor_cache: Arc::new(crate::dispatch_auth::DispatchActorCache::default()),
            github_pat,
            github_urls,
            pr_config,
            action_sha_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            config_path,
            pending_registrations: Arc::new(std::sync::RwLock::new(BTreeMap::new())),
        })
    }

    pub(crate) async fn emit(&self, event: NdjsonEvent) {
        let run_id = match &event {
            NdjsonEvent::RunAccepted { run_id, .. }
            | NdjsonEvent::JobStatus { run_id, .. }
            | NdjsonEvent::RunStatus { run_id, .. }
            | NdjsonEvent::JobCompleted { run_id, .. }
            | NdjsonEvent::CheckRunCreated { run_id } => Some(*run_id),
            _ => None,
        };
        let has_run_projection = run_id.is_some();
        if let NdjsonEvent::RunAccepted { queued_jobs, .. } = &event {
            self.observability.export_log(
                "INFO",
                "run.accepted",
                vec![
                    ("event.name".to_string(), "run.accepted".to_string()),
                    ("queued_jobs".to_string(), queued_jobs.to_string()),
                ],
            );
        }
        // Record job terminal transitions exactly once per job. `is_terminal`
        // alone is not enough: repeated timeline PATCHes (and replayed
        // completions) can re-deliver a terminal `JobStatus` after the job
        // already completed, and every delivery would inflate
        // `preloop.job.completed` and duplicate the terminal log record.
        // The first terminal event for a job wins; the marker is seeded from
        // the restored run record at startup so a post-restart replay cannot
        // double-record either. Recording here rather than at the state
        // mutation avoids double-counting on duplicate `store_run_event`
        // emits. A duplicate event is still a duplicate — drop it entirely
        // (side effects, persistence and broadcast) rather than re-append the
        // same terminal record to the timeline.
        match &event {
            NdjsonEvent::JobStatus {
                run_id,
                job_id,
                status,
                reason,
                ..
            } if status.is_terminal() => {
                let first_terminal = self
                    .terminal_jobs_recorded
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert((*run_id, job_id.clone()));
                if !first_terminal {
                    return;
                }
                let conclusion = execution_conclusion(*status);
                // `reason: None` is the common case (most terminal transitions
                // carry none) and means "no reason supplied" — not
                // "unrecognized". Only a value outside the emitted set is
                // `unrecognized`, which keeps the label bounded without
                // mislabelling the majority.
                let bounded_reason = match reason.as_deref() {
                    None => "unspecified",
                    Some(value) => bounded_termination_reason(value),
                };
                self.observability
                    .metrics()
                    .lifecycle
                    .record_job_completed(conclusion, bounded_reason);
                self.observability.export_log(
                    if *status == preloop_gha_protocol::ExecutionStatus::Success {
                        "INFO"
                    } else {
                        "WARN"
                    },
                    // A terminal JobStatus is a status transition, not the
                    // separate JobCompleted event; naming both `job.completed`
                    // conflated two distinct records in the log stream.
                    "job.status.terminal",
                    {
                        let mut attributes = vec![
                            ("event.name".to_string(), "job.status.terminal".to_string()),
                            ("conclusion".to_string(), conclusion.to_string()),
                            ("reason".to_string(), bounded_reason.to_string()),
                        ];
                        // The bounded code is the metric dimension; the prose
                        // is what an operator actually needs to act. Logs may
                        // carry it (they are not a label space), and without
                        // it an `unrecognized` classification is a dead end —
                        // you cannot tell which path produced it.
                        if let Some(detail) = reason.as_deref() {
                            attributes
                                .push(("reason.detail".to_string(), bounded_reason_detail(detail)));
                        }
                        attributes
                    },
                );
            }
            // `NdjsonEvent::JobCompleted` has no constructor anywhere in the
            // workspace — the terminal transition is reported as a terminal
            // `JobStatus`, which the arm above records. Keeping a counter
            // call here would make the record look double-sourced.
            _ => {}
        }
        // Capture the projection under the lock, then persist after releasing
        // it: a slow or unavailable backend must not stall the control plane
        // (runner polling, heartbeats, other state mutations).
        if let Some(run_id) = run_id {
            let projection = {
                let inner = self.inner.lock().await;
                crate::store::RunProjection::from_inner(&inner, run_id, event.clone())
            };
            if let Some(projection) = projection {
                if let Err(error) = self.store.store_run_event(projection).await {
                    error!(?error, %run_id, "failed to persist control-plane run event");
                }
            }
        }
        if !has_run_projection {
            if let Err(error) = self.store.append_event(&event).await {
                error!(?error, "failed to append durable control-plane event");
            }
        }
        // Always broadcast: in-memory state is the source of truth and
        // subscribers see live events. A store hiccup must never
        // freeze the SSE/UI stream for a healthy run.
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
/// Serialized as a JSON array so the encoding is injective: the download
/// route rejects `.`, `/`, and `\` in owner/repo and `..` in `git_ref`, but
/// NOT control characters, so a newline inside any accepted component could
/// splice the `\n`-joined format into a different action's payload. Two
/// distinct actions always sign distinct bytes, so a ticket minted for one
/// action can never validate for another.
fn action_ticket_payload(owner: &str, repo: &str, git_ref: &str, expires_at: u64) -> String {
    serde_json::to_string(&("action-archive", owner, repo, git_ref, expires_at))
        .expect("a tuple of strings and an integer always serializes")
}

#[cfg(test)]
pub(crate) fn mint_runtime_token(plan_id: &str, job_id: &uuid::Uuid) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock must be after epoch")
        .as_secs();
    let header = json!({"alg": "HS256", "typ": "JWT", "kid": "preloop-local"});
    let claims = json!({
        "iss": "https://preloop.local",
        "iat": now,
        "nbf": now,
        "exp": now + 2999,
        "sub": format!("preloop-job-{job_id}"),
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
            serde_json::from_str::<preloop_gha_protocol::crypto::RsaParametersExport>(&content)
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
            store_private_file(&certificate_path, kp.certificate_der())?;
            kp
        };
        return Ok(kp);
    }

    let kp = oidc::OidcKeypair::generate()?;
    let json = serde_json::to_vec(&kp.params())?;
    store_private_file(&key_path, &json)?;
    store_private_file(&certificate_path, kp.certificate_der())?;
    Ok(kp)
}

fn store_private_file(path: &std::path::Path, bytes: &[u8]) -> anyhow::Result<()> {
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

    /// Record that a runner session just polled the control plane.
    ///
    /// The liveness sweep purges runners whose sessions have not polled
    /// within [`InnerState::runner_liveness_timeout`]: a session that goes
    /// silent is a deaf runner (its in-guest control bridge died), and its
    /// unfinished job must be requeued to a fresh machine instead of sitting
    /// in_progress until the job-lease reaper fails it 45 minutes later.
    pub(crate) fn mark_session_seen(&mut self, session_id: &str) {
        self.session_last_seen
            .insert(session_id.to_owned(), std::time::Instant::now());
    }
}

#[derive(Default)]
pub(crate) struct InnerState {
    pub(crate) runs: BTreeMap<RunId, RunRecord>,
    pub(crate) workflow_run_counters: BTreeMap<String, u64>,
    pub(crate) queue: VecDeque<QueuedJob>,
    /// When each ready-queue job was first seen by the reaper, used to fail
    /// jobs no runner can ever claim. Maintained by the reaper itself, so it
    /// needs no enqueue-site coordination: entries are inserted on first
    /// observation and dropped when the job leaves the queue.
    pub(crate) queued_at: BTreeMap<(RunId, JobId), std::time::SystemTime>,
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
    /// When each runner session last polled. In-memory only: sessions are
    /// ephemeral and re-created by runners, so nothing is persisted here.
    /// Restored sessions from a restart have no entry and are left to the
    /// job-lease reaper rather than being swept immediately.
    pub(crate) session_last_seen: BTreeMap<String, std::time::Instant>,
    /// How long a session may go without polling before the liveness sweep
    /// purges its runner. Env: `PRELOOP_RUNNER_LIVENESS_TIMEOUT_SECS`
    /// (default 1800).
    pub(crate) runner_liveness_timeout: std::time::Duration,
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
    /// Runners that proved themselves with a provision token at registration,
    /// keyed by runner id. Pool-managed jobs must pair with one of these
    /// (or a machine the pool itself provisioned) rather than an external
    /// runner that registered before the job was queued.
    pub(crate) pool_proven_runners: BTreeSet<i64>,
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
    /// Sum of all retained in-memory log byte lengths (`logs` values). Kept
    /// incrementally so `trim_plan_logs` can early-return in the common case
    /// without rescanning the whole map on every append.
    pub(crate) log_bytes_total: usize,
    pub(crate) timeline_events: BTreeMap<RunId, Vec<NdjsonEvent>>,
    /// Per-timeline changeId counter for timeline PATCH versioning.
    pub(crate) timeline_change_ids: BTreeMap<String, i32>,
    /// Persisted timeline records keyed by `{plan_id}/{timeline_id}`.
    /// Upserted on each PATCH; returned by GET.
    pub(crate) timeline_records: BTreeMap<String, BTreeMap<uuid::Uuid, azdo::TimelineRecord>>,
    pub(crate) live_log_lines: BTreeMap<String, Arc<tokio::sync::Mutex<LiveLogBuffer>>>,
    pub(crate) live_log_tx: BTreeMap<String, broadcast::Sender<LiveLogFeedLinesWrapper>>,
    /// Live-log keys whose job has reached a terminal state. A follower that
    /// connects at or after completion serves the retained snapshot and then
    /// ends, instead of subscribing to a channel that will never speak again.
    /// Cleared if the same key ingests fresh lines (a retry reusing the job).
    pub(crate) live_log_closed: std::collections::BTreeSet<String>,
    pub(crate) inflight_requests: BTreeMap<i64, (RunId, JobId)>,
    pub(crate) job_requests: BTreeMap<i64, TaskAgentJobRequestRecord>,
    /// Step records per job attempt, keyed by `agent_job_id`.
    ///
    /// Authoritative for both the run record's step projection and `--step`
    /// log selection. Keyed by attempt, not by job: a re-dispatch mints fresh
    /// `TaskStep` ids, so a job-scoped map would overwrite the mapping the
    /// previous attempt's `step-<id>.txt` blobs are still named after.
    ///
    /// Seeded from the job request message at dispatch (every declared step,
    /// in workflow order); runner reports only reconcile into it.
    pub(crate) job_steps: BTreeMap<uuid::Uuid, Vec<crate::models::StepRecord>>,
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
    /// Cache v2 download-token mint order (FIFO eviction). In-memory only:
    /// restored tokens have no entry and are evicted only when the cap is
    /// exceeded, never by age.
    pub(crate) cache_v2_dl_tokens_order: VecDeque<String>,
    /// Cache v2 download-token mint time (unix seconds), for the TTL sweep.
    /// In-memory only, like `cache_v2_dl_tokens_order`.
    pub(crate) cache_v2_dl_tokens_created: BTreeMap<String, i64>,
    /// Insertion order for timeline keys (`{plan}/{timeline}`) — FIFO for
    /// global eviction. In-memory only.
    pub(crate) timeline_records_order: VecDeque<String>,
    /// Insertion order for run event buckets — FIFO for global eviction.
    pub(crate) timeline_events_order: VecDeque<RunId>,
    /// Finalization order for artifact registry — FIFO for global and
    /// per-run eviction. In-memory only.
    pub(crate) artifact_registry_order: VecDeque<String>,
    /// Insertion order for log keys — FIFO for global log eviction.
    pub(crate) log_order: VecDeque<String>,
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
    pub(crate) run_concurrency: BTreeMap<RunId, preloop_gha_parser::Concurrency>,
    /// Which concurrency key a holder currently occupies (for release).
    pub(crate) holder_keys: BTreeMap<RunId, Vec<(String, String)>>,
    /// Live debug sessions holding paused jobs open.
    pub(crate) debug_sessions: crate::debug_sessions::DebugSessionRegistry,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ticket payload must be injective even when an accepted identifier
    /// contains a newline: the download route rejects `.`, `/`, and `\` in
    /// owner/repo and `..` in `git_ref`, but never control characters, so a
    /// crafted ref could previously splice the `\n`-joined payload into a
    /// different action's bytes. Distinct actions must sign distinct bytes.
    #[test]
    fn action_ticket_payload_is_injective_across_newline_splits() {
        let expires_at = 1_800_000_000u64;
        let first = action_ticket_payload("acme", "repo", "v1\nx", expires_at);
        let second = action_ticket_payload("acme", "repo\nv1", "x", expires_at);
        assert_ne!(
            first, second,
            "two actions must never produce the same ticket payload"
        );
        // Sanity: the encoding still covers every component.
        let baseline = action_ticket_payload("acme", "repo", "v1", expires_at);
        assert_ne!(baseline, first);
        assert_ne!(baseline, second);
        assert_ne!(
            baseline,
            action_ticket_payload("acme", "repo", "v1", expires_at + 1)
        );
    }

    /// End to end: a ticket minted for one action must not authorise a
    /// different action whose identifier only differs by a newline split.
    #[tokio::test]
    async fn action_ticket_does_not_cross_validate_across_newline_splits() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let expires_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;

        let signature = state.sign_action_ticket("acme", "repo", "v1\nx", expires_at);
        assert!(
            state.verify_action_ticket("acme", "repo", "v1\nx", expires_at, &signature),
            "the minted action must verify"
        );
        assert!(
            !state.verify_action_ticket("acme", "repo\nv1", "x", expires_at, &signature),
            "a ticket for one action must not validate for a newline-split twin"
        );
    }
}

#[cfg(test)]
mod termination_reason_tests {
    use super::bounded_termination_reason;

    #[test]
    fn exact_codes_pass_through() {
        assert_eq!(
            bounded_termination_reason("concurrency_cancelled"),
            "concurrency_cancelled"
        );
        assert_eq!(bounded_termination_reason("timeout"), "timeout");
    }

    #[test]
    fn starvation_prose_classifies_to_no_runner() {
        // The starvation sweep builds this sentence with the job's runs-on
        // labels interpolated. It must classify, not pass through.
        let prose = "no runner is registered for `runs-on: self-hosted, Linux, ARM64` and none \
                     appeared within 120s, so the job cannot be scheduled";
        assert_eq!(bounded_termination_reason(prose), "no_runner");
    }

    #[test]
    fn user_controlled_labels_never_become_the_label() {
        // A hostile or merely unusual `runs-on` must not reach the metric.
        let prose = "no runner is registered for `runs-on: attacker-controlled-\u{1F4A5}-label` \
                     and none appeared within 120s, so the job cannot be scheduled";
        let bounded = bounded_termination_reason(prose);
        assert_eq!(bounded, "no_runner");
        assert!(!bounded.contains("attacker"));
    }

    #[test]
    fn external_host_prose_is_its_own_code() {
        // `{platform}` is interpolated, so match the invariant phrase.
        for platform in ["windows", "macos", "freebsd-13"] {
            let prose = format!(
                "no {platform} runner is registered with this server, so \
                 `runs-on: {platform}-latest` cannot be scheduled"
            );
            assert_eq!(
                bounded_termination_reason(&prose),
                "no_platform_runner",
                "{platform} must classify distinctly from the starvation sweep"
            );
        }
    }

    #[test]
    fn reason_detail_is_bounded_on_a_char_boundary() {
        use super::bounded_reason_detail;
        // 4-byte characters: 300 of them is 1200 bytes, way over the cap.
        let long = "w".repeat(300);
        let bounded = bounded_reason_detail(&long);
        assert!(bounded.len() <= 512);
        assert!(bounded.is_char_boundary(bounded.len()));
        // Short prose passes through untouched.
        assert_eq!(bounded_reason_detail("short"), "short");
    }

    #[test]
    fn crafted_runs_on_label_cannot_flip_the_classification() {
        // The starvation prose interpolates `runs-on` labels verbatim. A
        // label containing the platform phrase must still classify as
        // `no_runner` — the anchored prefix is checked first.
        let starved = "no runner is registered for `runs-on: self-hosted, \
                       runner is registered with this server` and none \
                       appeared within 120s, so the job cannot be scheduled";
        assert_eq!(bounded_termination_reason(starved), "no_runner");
    }

    #[test]
    fn platform_and_starvation_do_not_collide() {
        let starved = "no runner is registered for `runs-on: self-hosted, Linux, ARM64` and none \
                       appeared within 120s, so the job cannot be scheduled";
        let platform = "no windows runner is registered with this server, so \
                        `runs-on: windows-latest` cannot be scheduled";
        assert_eq!(bounded_termination_reason(starved), "no_runner");
        assert_eq!(bounded_termination_reason(platform), "no_platform_runner");
    }

    #[test]
    fn unknown_prose_is_bounded_not_passed_through() {
        let bounded = bounded_termination_reason("something entirely new happened with id-99999");
        assert_eq!(bounded, "unrecognized");
        assert!(!bounded.contains("99999"));
    }

    #[test]
    fn classification_is_a_finite_set() {
        // Drive 1,000 distinct prose strings; the label set must stay bounded.
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..1000 {
            let prose = format!(
                "no runner is registered for `runs-on: label-{i}` and none appeared within 120s"
            );
            seen.insert(bounded_termination_reason(&prose));
            seen.insert(bounded_termination_reason(&format!("novel reason {i}")));
        }
        assert_eq!(seen.len(), 2, "expected exactly no_runner + unrecognized");
    }
}
