use super::*;
use std::collections::{BTreeSet, HashMap};
use std::sync::{LazyLock, Mutex};
use std::time::Instant;
use utoipa::ToSchema;

/// A heartbeat or sampler snapshot older than this is stale: three sampler
/// intervals of 5s. Single source so `/readyz` and `/api/v1/status` cannot
/// disagree when the interval changes.
pub const STALENESS_THRESHOLD: Duration = Duration::from_secs(15);

pub async fn healthz(State(shared): State<Arc<SharedState>>) -> impl IntoResponse {
    let shutdown = shared.shutdown.is_cancelled();
    let body = json!({
        "ok": !shutdown,
        "protocol_version": PROTOCOL_VERSION,
        "shutdown_requested": shutdown,
    });
    if shutdown {
        (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
    } else {
        (StatusCode::OK, Json(body)).into_response()
    }
}

pub async fn readyz(State(shared): State<Arc<SharedState>>) -> impl IntoResponse {
    if shared.shutdown.is_cancelled() {
        let body = json!({ "ready": false, "reason": "shutting_down" });
        return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
    }
    if let Some(stale) = shared
        .state
        .observability
        .heartbeat()
        .any_critical_stale(STALENESS_THRESHOLD)
    {
        let body = json!({ "ready": false, "reason": format!("task_stale:{}", stale) });
        return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
    }
    // The snapshot is only refreshed by the `state_sampler` task. An app
    // built through `routes::app` without one (tests, embedded harnesses)
    // never promises snapshot freshness, so its `observed_at` age must not
    // turn /readyz into a permanent 503. Once a sampler has started (and
    // registered its critical heartbeat), a stale snapshot is a real
    // outage and is reported as such.
    let sampler_running = shared
        .state
        .observability
        .heartbeat()
        .snapshot()
        .iter()
        .any(|task| task.name == "state_sampler");
    if sampler_running {
        let age_secs = {
            let snap = shared.state.status_snapshot.read();
            let now = chrono::Utc::now();
            (now - snap.observed_at).num_milliseconds() as f64 / 1000.0
        };
        if age_secs > STALENESS_THRESHOLD.as_secs_f64() {
            let body = json!({ "ready": false, "reason": "state_sampler_stale" });
            return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
        }
    }
    let body = json!({ "ready": true, "reason": serde_json::Value::Null });
    (StatusCode::OK, Json(body)).into_response()
}

pub async fn status(State(shared): State<Arc<SharedState>>) -> impl IntoResponse {
    // Fail-open, no InnerState lock — clone cached snapshot and update age.
    let mut snap = shared.state.status_snapshot.read().clone();
    let now = chrono::Utc::now();
    let age = (now - snap.observed_at).num_milliseconds() as f64 / 1000.0;
    snap.snapshot_age_seconds = if age.is_finite() && age >= 0.0 {
        age
    } else {
        0.0
    };
    // Also surface current heartbeat tasks without holding InnerState
    // (best-effort: caller sees last sampler's tasks plus live heartbeat snapshot)
    // We keep sampler's tasks but also append live task snapshot if empty.
    if snap.tasks.is_empty() {
        snap.tasks = shared
            .state
            .observability
            .heartbeat()
            .snapshot()
            .into_iter()
            .map(|t| preloop_observability::status::TaskEntry {
                name: t.name.to_string(),
                critical: t.critical == preloop_observability::Criticality::Critical,
                heartbeat_age_seconds: t.heartbeat_age.as_secs_f64(),
                panicked: t.panicked,
                state: if t.panicked {
                    "failed".to_string()
                } else if t.heartbeat_age > STALENESS_THRESHOLD {
                    "stale".to_string()
                } else {
                    "running".to_string()
                },
            })
            .collect();
    }
    Json(snap).into_response()
}
/// Effective checkout-cache policy for operator UIs. The native bearer on the
/// containing router protects this deployment configuration.
pub async fn checkout_cache_config(State(shared): State<Arc<SharedState>>) -> impl IntoResponse {
    Json(shared.state.checkout_cache.clone())
}

pub async fn metrics(State(shared): State<Arc<SharedState>>) -> impl IntoResponse {
    let body = shared.state.observability.render_metrics();
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

/// GitHub's `system.orchestrationId`: `{planId}.{jobId}.{suffix}` where the
/// suffix is the 1-based matrix cell index (`build._1`) or `__default` for
/// plain jobs. The official runner emits the value as a User-Agent product
/// token, so it must not contain spaces or other invalid token characters —
/// the job display name ("Run tests with system wide configuration") would
/// crash the worker with `FormatException`.
fn orchestration_id(plan_id: &str, job_id: &str, matrix_index: Option<usize>) -> String {
    // The official runner emits this value as a User-Agent product token
    // (`ProductInfoHeaderValue("OrchestrationId", ...)`), so the whole string
    // must be token-safe. Reusable-call job ids contain '/' ("ci/build"),
    // which .NET rejects with FormatException. GitHub's own ids never carry
    // those characters; map everything outside the token alphabet to '-'.
    let job_id = sanitize_job_id_token(job_id);
    match matrix_index {
        Some(index) => format!("{plan_id}.{job_id}._{index}"),
        None => format!("{plan_id}.{job_id}.__default"),
    }
}

/// Replace every character that is not an RFC product-token character with
/// '-' so the value passes .NET's `HeaderUtilities.CheckValidToken`.
fn sanitize_job_id_token(job_id: &str) -> String {
    job_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric()
                || matches!(
                    c,
                    '!' | '#'
                        | '$'
                        | '%'
                        | '&'
                        | '\''
                        | '*'
                        | '+'
                        | '-'
                        | '.'
                        | '^'
                        | '_'
                        | '`'
                        | '|'
                        | '~'
                )
            {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Interpolate `${{ ... }}` expressions in a workflow run name.
///
/// A malformed expression is left untouched, matching GitHub's behavior of
/// retaining the configured run-name rather than rejecting the run.
fn evaluate_run_name(
    raw: &str,
    github: &serde_json::Value,
    inputs: &BTreeMap<String, serde_json::Value>,
    vars: &BTreeMap<String, String>,
) -> String {
    let mut context = preloop_gha_expressions::Context::default();
    context.insert("github", github.clone());
    context.insert(
        "inputs",
        serde_json::Value::Object(inputs.clone().into_iter().collect()),
    );
    context.insert(
        "vars",
        serde_json::Value::Object(
            vars.iter()
                .map(|(name, value)| (name.clone(), serde_json::Value::String(value.clone())))
                .collect(),
        ),
    );

    let Some(_) = raw.find("${{") else {
        return raw.to_owned();
    };
    let mut result = String::with_capacity(raw.len());
    let mut cursor = 0;
    loop {
        let Some(relative_start) = raw[cursor..].find("${{") else {
            result.push_str(&raw[cursor..]);
            break;
        };
        let start = cursor + relative_start;
        result.push_str(&raw[cursor..start]);
        let expression_start = start + 3;
        let Some(relative_end) = raw[expression_start..].find("}}") else {
            return raw.to_owned();
        };
        let expression_end = expression_start + relative_end;
        let value = match preloop_gha_expressions::eval_expression(
            &raw[expression_start..expression_end],
            &context,
        ) {
            Ok(value) => value,
            Err(_) => return raw.to_owned(),
        };
        match value {
            serde_json::Value::String(value) => result.push_str(&value),
            serde_json::Value::Null => {}
            serde_json::Value::Bool(value) => result.push_str(if value { "true" } else { "false" }),
            serde_json::Value::Number(value) => result.push_str(&value.to_string()),
            value => result.push_str(&serde_json::to_string(&value).unwrap_or_default()),
        }
        cursor = expression_end + 2;
    }
    result
}

/// Validate every `on.schedule[*].cron` expression in a submitted workflow.
/// GitHub rejects invalid cron at workflow save; aksh rejects at submit so a
/// bad schedule is a hard error instead of a cron job that never registers.
fn validate_schedule_crons(workflow: &preloop_gha_parser::Workflow) -> Result<(), ApiError> {
    let preloop_gha_parser::Trigger::Map(triggers) = &workflow.on else {
        return Ok(());
    };
    let Some(schedule) = triggers.get("schedule").and_then(|v| v.as_array()) else {
        return Ok(());
    };
    for entry in schedule {
        let Some(cron) = entry.get("cron").and_then(|v| v.as_str()) else {
            continue;
        };
        if let Err(error) = crate::scheduler::github_to_cron(cron) {
            return Err(ApiError::bad_request(format!(
                "invalid cron expression {cron:?} in on.schedule: {error}"
            )));
        }
    }
    Ok(())
}

/// Whether a submission's trust tier permits injecting stored secrets
/// (repo/global/environment tiers). Native submissions carry no trust tier —
/// `None` is therefore trusted.
fn submission_allows_secrets(submission: &WorkflowSubmission) -> bool {
    crate::events::trust_tier::tier_of(submission)
        .map(|tier| tier.allows_secrets())
        .unwrap_or(true)
}

fn existing_webhook_run(
    inner: &InnerState,
    delivery_id: &str,
    workflow_path: &str,
) -> Option<RunAccepted> {
    inner
        .runs
        .values()
        .find(|run| {
            run.webhook_delivery_id.as_deref() == Some(delivery_id)
                && run.workflow_path_str == workflow_path
        })
        .map(|run| RunAccepted {
            run_id: run.run_id,
            run_number: run.run_number,
            queued_jobs: run.jobs.len(),
        })
}

/// Static-PAT permission enforcement (H3).
///
/// When no GitHub App is configured, the operator's static PAT
/// (`PRELOOP_GITHUB_TOKEN` / `github.pat`) is embedded verbatim as every
/// non-fork job's `GITHUB_TOKEN`. A classic PAT cannot be narrowed per
/// request, so without a check here the workflow's `permissions:` block is
/// silently ignored while `system.github.token.permissions` still advertises
/// the declared set. `submit_run_inner` introspects the PAT's classic OAuth
/// scopes (`X-OAuth-Scopes`) and refuses the run when the PAT is broader than
/// a job declared; an invalid PAT is refused outright. `build_job_artifacts`
/// publishes the token's real authority in `system.github.token.pat_scopes`,
/// which the runner prints alongside the declared permission set.
///
/// What a job's `GITHUB_TOKEN` becomes in PAT mode (H3).
///
/// A static PAT cannot be narrowed per job, so it is embedded only when its
/// classic OAuth scopes were introspected and do not exceed the job's declared
/// `permissions:`. When its bounds cannot be established the credential is
/// withheld: the job keeps the job-scoped runtime token, so a step that needs
/// GitHub fails at the point of use instead of running with authority nobody
/// could bound.
#[derive(Clone, Debug)]
pub enum PatToken {
    /// Classic OAuth scopes introspected and no broader than declared.
    Embed { token: String, scopes: Vec<String> },
    /// Bounds unverifiable (no scopes header, cold scope cache, unreachable
    /// API). The PAT is not embedded.
    Withheld,
}

impl PatToken {
    /// A PAT whose classic OAuth scopes were introspected: the wire variable
    /// advertises the actual scopes.
    pub fn with_scopes(token: String, scopes: Vec<String>) -> Self {
        Self::Embed { token, scopes }
    }

    /// A PAT whose bounds could not be verified, so it is withheld.
    pub fn withheld() -> Self {
        Self::Withheld
    }
}

/// How long introspected PAT scopes are trusted: the PAT is static process
/// config, so a short TTL only bounds staleness after an out-of-band rotation.
const PAT_SCOPE_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// Introspected classic OAuth scopes by PAT hash (hash, never the token).
type PatScopeCache = Mutex<HashMap<String, (Instant, Vec<String>)>>;
static PAT_SCOPE_CACHE: LazyLock<PatScopeCache> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Synchronous read of the PAT scope cache, for the synchronous scheduler
/// expansion pipeline (`build_jobs` cannot do network I/O). Returns the
/// cached scopes once the process has introduced them already: startup warms
/// the cache and every submission refreshes it. `None` on a cold or expired
/// entry.
pub fn cached_pat_scopes(pat: &str) -> Option<Vec<String>> {
    use sha2::Digest as _;
    let cache_key = format!("{:x}", sha2::Sha256::digest(pat.as_bytes()));
    PAT_SCOPE_CACHE
        .lock()
        .ok()
        .and_then(|cache| cache.get(&cache_key).cloned())
        .filter(|(at, _)| at.elapsed() < PAT_SCOPE_CACHE_TTL)
        .map(|(_, scopes)| scopes)
}

/// Warm the process-wide PAT scope cache once at startup (H3).
///
/// The job expansion pipeline is synchronous and reads [`cached_pat_scopes`],
/// so without a warm entry the first expansions after a restart would find a
/// cold cache and have to withhold the PAT. The PAT is fixed for the process
/// lifetime (read from config/env during state construction), so one lookup
/// here covers every later expansion.
///
/// Failures are logged, never fatal: a server that cannot reach the GitHub API
/// still serves, and the run path withholds the credential instead of
/// embedding one whose bounds nobody established.
pub async fn warm_pat_scope_cache(pat: &str) {
    match pat_oauth_scopes(pat).await {
        PatScopeOutcome::Known(scopes) => {
            tracing::info!(
                scope_count = scopes.len(),
                "Warmed the static PAT OAuth scope cache"
            );
        }
        PatScopeOutcome::Unverifiable { reason } => {
            tracing::warn!(
                reason = %reason,
                "Could not introspect PRELOOP_GITHUB_TOKEN OAuth scopes at startup; jobs keep the \
                 job-scoped runtime token until an introspection succeeds, so any step that needs \
                 GitHub fails."
            );
        }
        PatScopeOutcome::Invalid(error) => {
            tracing::error!(
                error = %error,
                "PRELOOP_GITHUB_TOKEN was rejected by the GitHub API at startup; jobs keep the \
                 job-scoped runtime token and any step that needs GitHub fails."
            );
        }
    }
}

/// Outcome of PAT scope introspection.
enum PatScopeOutcome {
    /// Classic scopes positively determined: enforce declared-vs-PAT.
    Known(Vec<String>),
    /// No scopes header (fine-grained PAT), a quirky API root, or an
    /// unreachable API: bounds unverifiable. The caller withholds the PAT
    /// rather than embedding a credential whose authority nobody can bound:
    /// the job still runs, on the job-scoped runtime token, and a step that
    /// needs GitHub fails at the point of use. Refusing the whole run instead
    /// would be a self-DoS when the control plane cannot reach the API
    /// (proxies, air gaps).
    Unverifiable { reason: String },
    /// The PAT itself was rejected by the API: fail closed.
    Invalid(anyhow::Error),
}

/// Introspect the static PAT's classic OAuth scopes via the `X-OAuth-Scopes`
/// response header on an authenticated API-root request.
///
/// Returns [`PatScopeOutcome::Known`] with the scope list when GitHub answers
/// successfully with an `X-OAuth-Scopes` header. An *absent* header is
/// [`PatScopeOutcome::Unverifiable`], not an empty scope list: GitHub omits it
/// for fine-grained PATs, so absence means the token's bounds are unknown
/// rather than narrow. Results are cached per PAT (SHA-256 of the token) for
/// [`PAT_SCOPE_CACHE_TTL`].
async fn pat_oauth_scopes(pat: &str) -> PatScopeOutcome {
    use sha2::Digest as _;
    let cache_key = format!("{:x}", sha2::Sha256::digest(pat.as_bytes()));
    if let Ok(cache) = PAT_SCOPE_CACHE.lock() {
        if let Some((at, scopes)) = cache.get(&cache_key) {
            if at.elapsed() < PAT_SCOPE_CACHE_TTL {
                return PatScopeOutcome::Known(scopes.clone());
            }
        }
    }
    let url = format!("{}/", crate::github::github_api_base());
    let response = match crate::shared_http::CLIENT
        .get(&url)
        .header("Authorization", format!("Bearer {pat}"))
        .header("Accept", "application/vnd.github+json")
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return PatScopeOutcome::Unverifiable {
                reason: format!("GitHub API unreachable: {error:#}"),
            };
        }
    };
    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return PatScopeOutcome::Invalid(anyhow::anyhow!(
            "PAT scope introspection rejected with {status}"
        ));
    }
    if !status.is_success() {
        return PatScopeOutcome::Unverifiable {
            reason: format!("unexpected status {status} from the GitHub API root"),
        };
    }
    let Some(header) = response.headers().get("x-oauth-scopes") else {
        return PatScopeOutcome::Unverifiable {
            reason: "GitHub returned no X-OAuth-Scopes header (fine-grained PAT, or a proxied API \
                     root that drops it)"
                .to_owned(),
        };
    };
    let Ok(header) = header.to_str() else {
        return PatScopeOutcome::Unverifiable {
            reason: "X-OAuth-Scopes header was not valid UTF-8".to_owned(),
        };
    };
    let scopes: Vec<String> = header
        .split(',')
        .map(|scope| scope.trim().to_owned())
        .filter(|scope| !scope.is_empty())
        .collect();
    if let Ok(mut cache) = PAT_SCOPE_CACHE.lock() {
        cache.insert(cache_key, (Instant::now(), scopes.clone()));
    }
    PatScopeOutcome::Known(scopes)
}

/// Whether the PAT's classic OAuth scopes confer authority beyond the job's
/// effective `permissions:` block: any write-capable PAT scope without a
/// declared `write`/`admin` grant, or any PAT authority at all against an
/// empty (`permissions: {}`) declaration.
///
/// `pub` for the scope-matrix unit tests in `lib_tests.rs`.
pub fn pat_exceeds_declared(pat_scopes: &[String], declared: &BTreeMap<String, String>) -> bool {
    // Mirror of `github_app::effective_permissions`: `id-token` and `models`
    // are runner-authenticated and granted by the platform, not the token.
    let declared_write = declared.iter().any(|(scope, level)| {
        (level.eq_ignore_ascii_case("write") || level.eq_ignore_ascii_case("admin"))
            && !crate::github_app::ACTIONS_ONLY_SCOPES.contains(&scope.as_str())
    });
    // A classic PAT scope counts as write-capable unless provably read-only.
    // The safe direction: GitHub's classic scope list hides write grants
    // under terse names (`public_repo` pushes to public repos, `repo:status`
    // posts commit statuses, `gist` writes gists), so an unrecognized scope
    // must not be assumed narrow.
    let pat_write = pat_scopes
        .iter()
        .any(|scope| !classic_scope_is_read_only(scope));
    if pat_write && !declared_write {
        return true;
    }
    declared.is_empty() && !pat_scopes.is_empty()
}

/// Classic OAuth scopes that are provably read-only.
fn classic_scope_is_read_only(scope: &str) -> bool {
    scope.starts_with("read:") || scope == "user:email"
}

/// Reject the run when the static PAT's classic OAuth scopes confer
/// authority beyond a non-fork job's effective (declared-or-default)
/// `permissions:` block.
///
/// A PAT cannot be narrowed per request, so the only safe answers are a PAT
/// no broader than declared, a GitHub App (installation tokens are minted
/// with exactly the declared set), or no run at all. Fork-restricted jobs
/// never receive the PAT, so they are exempt.
pub fn enforce_pat_permissions(
    jobs: &[preloop_gha_protocol::JobPlan],
    submission: &preloop_gha_protocol::WorkflowSubmission,
    pat_scopes: &[String],
) -> Result<(), ApiError> {
    let tier = crate::events::trust_tier::tier_of(submission);
    let mut offenders: Vec<String> = Vec::new();
    for job in jobs {
        let policy = crate::events::trust_tier::job_authorization(
            tier,
            job.permissions.as_ref(),
            job.oidc_id_token_granted,
        );
        if policy.fork_restricted {
            continue;
        }
        if pat_exceeds_declared(pat_scopes, &policy.token_permissions) {
            offenders.push(job.id.to_string());
        }
    }
    if offenders.is_empty() {
        return Ok(());
    }
    Err(ApiError::forbidden(format!(
        "refusing run: PRELOOP_GITHUB_TOKEN is a static PAT with OAuth scopes [{}], \
         which exceed the declared `permissions:` of job(s) [{}]. A PAT cannot be \
         narrowed per job: configure a GitHub App so installation tokens are minted \
         with exactly the declared permissions, or replace the PAT with a \
         fine-grained PAT scoped no broader than the workflows' declared permissions",
        pat_scopes.join(", "),
        offenders.join(", "),
    )))
}

/// Honest `system.github.token.pat_scopes` wire value for a PAT-backed
/// `GITHUB_TOKEN`: the workflow's declared set is not honored in PAT mode, so
/// the token's actual classic OAuth scopes (or their unverifiability) are
/// stated here. `system.github.token.permissions` keeps its documented
/// `{"<Permission>": "<level>"}` map shape, so consumers that parse it are not
/// surprised by a key that is not a permission and a value that is prose.
fn pat_scopes_wire_value(scopes: &[String]) -> String {
    if scopes.is_empty() {
        // The header was present but listed nothing: the token carries no
        // classic scopes, which is narrower than any declaration. Distinct from
        // an absent header, which is unverifiable and never reaches this path.
        "no classic OAuth scopes reported; NOT the declared `permissions:` set".to_owned()
    } else {
        format!("static PAT OAuth scopes: {}", scopes.join(", "))
    }
}

pub async fn submit_run_inner(
    shared: &Arc<SharedState>,
    submission: WorkflowSubmission,
) -> Result<RunAccepted, ApiError> {
    // Workflow execution protections: every non-webhook submission path
    // funnels through here (native `POST /api/v1/runs`, REST dispatch, the
    // scheduler, native reruns), so the event/actor check lives at this
    // choke point. The webhook intake enforces separately upstream via
    // `denies_event` / `denies_workflow` and calls
    // `submit_run_inner_with_webhook_delivery` directly, so this does not
    // double-fire there.
    if let Some(hit) = crate::execution_protection::denies_submission(
        &shared.state.execution_protection,
        &submission.event,
        Some(&submission.actor),
        submission.workflow_file.as_deref(),
    ) {
        match shared.state.execution_protection.mode {
            crate::config::ProtectionMode::Enforce => {
                info!(
                    event = %submission.event,
                    actor = %submission.actor,
                    rule = %hit.describe(),
                    "execution protection denied submission"
                );
                return Err(ApiError::forbidden(format!(
                    "execution protection denied {}: {}",
                    submission.event,
                    hit.describe()
                )));
            }
            crate::config::ProtectionMode::Evaluate => {
                info!(
                    event = %submission.event,
                    actor = %submission.actor,
                    rule = %hit.describe(),
                    "execution protection would deny submission (evaluate mode)"
                );
            }
        }
    }
    submit_run_inner_with_webhook_delivery(shared, submission, None).await
}

/// Submit a run originating from one durable webhook delivery.
///
/// A replay that arrives while the first delivery is still constructing a
/// large matrix waits for that construction instead of duplicating the work.
pub async fn submit_run_inner_with_webhook_delivery(
    shared: &Arc<SharedState>,
    submission: WorkflowSubmission,
    webhook_delivery_id: Option<&str>,
) -> Result<RunAccepted, ApiError> {
    let Some(delivery_id) = webhook_delivery_id else {
        return submit_run_inner_with_webhook_delivery_unreserved(shared, submission, None).await;
    };
    let Some(workflow_path) = submission.workflow_path.as_deref() else {
        return submit_run_inner_with_webhook_delivery_unreserved(
            shared,
            submission,
            Some(delivery_id),
        )
        .await;
    };
    let key = (delivery_id.to_owned(), workflow_path.to_owned());
    loop {
        let reservation_acquired = {
            let mut inner = shared.state.inner.lock().await;
            if let Some(existing) = existing_webhook_run(&inner, delivery_id, workflow_path) {
                return Ok(existing);
            }
            inner.webhook_run_reservations.insert(key.clone())
        };
        if reservation_acquired {
            let reservation = WebhookRunReservation::new(Arc::clone(shared), key.clone());
            let result = submit_run_inner_with_webhook_delivery_unreserved(
                shared,
                submission,
                Some(delivery_id),
            )
            .await;
            reservation.release().await;
            return result;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
struct WebhookRunReservation {
    shared: Arc<SharedState>,
    key: Option<(String, String)>,
}

impl WebhookRunReservation {
    fn new(shared: Arc<SharedState>, key: (String, String)) -> Self {
        Self {
            shared,
            key: Some(key),
        }
    }

    async fn release(mut self) {
        if let Some(key) = self.key.take() {
            release_webhook_run_reservation(&self.shared, key).await;
        }
    }
}

impl Drop for WebhookRunReservation {
    fn drop(&mut self) {
        let Some(key) = self.key.take() else {
            return;
        };
        let shared = Arc::clone(&self.shared);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                release_webhook_run_reservation(&shared, key).await;
            });
        }
    }
}

async fn release_webhook_run_reservation(shared: &Arc<SharedState>, key: (String, String)) {
    let mut inner = shared.state.inner.lock().await;
    inner.webhook_run_reservations.remove(&key);
    drop(inner);
    shared.state.message_notify.notify_waiters();
}

/// Submit a run originating from one durable webhook delivery.
///
/// The delivery worker is at-least-once: a process crash after run creation
/// but before the queue row is marked done can replay the payload. Persisting
/// the delivery ID on the run lets the replay return the existing run instead
/// of creating a second one.
async fn submit_run_inner_with_webhook_delivery_unreserved(
    shared: &Arc<SharedState>,
    mut submission: WorkflowSubmission,
    webhook_delivery_id: Option<&str>,
) -> Result<RunAccepted, ApiError> {
    let webhook_delivery_id = webhook_delivery_id.map(str::to_owned);
    // Fork-PR workflow policy: runs from fork pull-request events wait for
    // explicit operator approval before any job may start. The trust tier is
    // stamped by the webhook path before submission; native submissions carry
    // no tier and are never held.
    let fork_approval_pending = crate::fork_policy::fork_approval_required(
        &shared.state.fork_policy,
        crate::events::trust_tier::tier_of(&submission),
    );
    let fork_approval_requested_at_unix_nanos =
        fork_approval_pending.then(crate::models::now_unix_nanos);
    if let (Some(delivery_id), Some(workflow_path)) = (
        webhook_delivery_id.as_deref(),
        submission.workflow_path.as_deref(),
    ) {
        let existing = {
            let inner = shared.state.inner.lock().await;
            existing_webhook_run(&inner, delivery_id, workflow_path)
        };
        if let Some(existing) = existing {
            tracing::info!(
                %delivery_id,
                %workflow_path,
                run_id = %existing.run_id,
                "reusing run for replayed webhook delivery"
            );
            return Ok(existing);
        }
    }

    let workflow = parse_workflow(&submission.workflow_yaml)?;
    // GitHub rejects workflows whose `on.schedule` cron cannot parse (save
    // time); aksh rejects them at submit so a bad schedule is a hard error
    // instead of a cron job that never registers.
    validate_schedule_crons(&workflow)?;
    // The same static credential job tokens use (env `PRELOOP_GITHUB_TOKEN`,
    // else the config file's `github.pat`) authenticates remote reusable
    // workflow fetches: private `uses: owner/repo/...` references must
    // resolve without a separately exported token.
    crate::remote_workflows::resolve_remote_workflows(
        &mut submission,
        &workflow,
        shared.state.static_github_pat().as_deref(),
    )
    .await?;
    if submission.event == "workflow_dispatch" {
        workflow.apply_workflow_dispatch_inputs(&mut submission.payload)?;
        if submission.dispatch_inputs.is_empty() {
            submission.dispatch_inputs = submission
                .payload
                .get("inputs")
                .and_then(serde_json::Value::as_object)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
        }
        if submission.dispatch_inputs_stringified.is_empty() {
            submission.dispatch_inputs_stringified = submission
                .dispatch_inputs
                .iter()
                .map(|(name, value)| (name.clone(), value_to_input_string(value)))
                .collect();
        }
        if let Some(object) = submission.payload.as_object_mut() {
            let inputs_value = if submission.dispatch_inputs_stringified.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::to_value(&submission.dispatch_inputs_stringified).unwrap_or_default()
            };
            object.insert("inputs".to_owned(), inputs_value);
        }
    }
    // Native submissions carry no trust tier (the webhook path sets it) and
    // pass secrets through unmodified — None is therefore trusted. Mirror
    // GitHub org/repo/environment secrets: stored secrets are available to
    // every trusted job, with submission-provided values winning per name.
    let allow_secrets = submission_allows_secrets(&submission);
    if !allow_secrets {
        submission.secrets.clear();
    } else {
        // Global secrets first, then per-repository secrets for the
        // submitting repository. Precedence: submission-provided secrets
        // (already in the map) > per-repo tier > global tier — mirroring
        // GitHub, where repo secrets override org secrets of the same name.
        let secret_store = shared.state.secrets.read();
        let submission_names: BTreeSet<String> = submission.secrets.keys().cloned().collect();
        // Remember the caller-provided names so per-job environment overlays
        // (applied later, in `build_job_artifacts`) keep these values
        // winning per name over the stored environment tier.
        submission.submission_names = submission_names.clone();
        for (name, value) in &secret_store.global {
            submission
                .secrets
                .entry(name.clone())
                .or_insert_with(|| preloop_gha_protocol::SecretString::new(value.clone()));
        }
        if let Some(repo_secrets) = secret_store.repo.get(&submission.repository) {
            for (name, value) in repo_secrets {
                if !submission_names.contains(name) {
                    submission.secrets.insert(
                        name.clone(),
                        preloop_gha_protocol::SecretString::new(value.clone()),
                    );
                }
            }
        }
    }
    let (branch, tag) = {
        let (default_branch, default_tag) = git_ref_context(&submission.git_ref);
        let filter_branch = submission.filter_branch.clone().or_else(|| {
            if matches!(
                submission.event.as_str(),
                "pull_request" | "pull_request_target"
            ) {
                submission
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("base"))
                    .and_then(|base| base.get("ref"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            } else if submission.event == "workflow_run" {
                submission
                    .payload
                    .get("workflow_run")
                    .and_then(|run| run.get("head_branch"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            } else {
                None
            }
        });
        if filter_branch.is_some() {
            (filter_branch, None)
        } else {
            (default_branch, default_tag)
        }
    };
    let payload_has_paths =
        submission.payload.get("paths").is_some() || submission.payload.get("commits").is_some();
    let changed_paths_known = submission.changed_paths_known || payload_has_paths;
    let changed_paths = if submission.changed_paths_known {
        submission.changed_paths.clone()
    } else {
        changed_paths_from_payload(&submission.payload)
    };
    // The reason names the axis; this names the flag that changes it, because
    // "does not match" without a next action is the whole complaint being
    // fixed here.
    fn trigger_mismatch_hint(reason: &preloop_gha_parser::TriggerMismatch) -> String {
        use preloop_gha_parser::TriggerMismatch as M;
        match reason {
            M::EventNotDeclared { declared } if !declared.is_empty() => {
                let list = declared
                    .iter()
                    .map(|event| format!("`--event {event}`"))
                    .collect::<Vec<_>>()
                    .join(" or ");
                format!(" Pass {list}.")
            }
            M::EventNotDeclared { .. } => String::new(),
            M::ActivityTypeMissing { accepted } | M::ActivityTypeRejected { accepted, .. } => {
                if accepted.is_empty() {
                    " The workflow declares an empty `types` list and accepts no activity types for this event.".to_owned()
                } else {
                    let first = accepted.first().map(String::as_str).unwrap_or("opened");
                    format!(
                        " Supply one with `--payload` containing {{\"action\": \"{first}\"}}, or pick \
                         an activity type the workflow accepts."
                    )
                }
            }
            M::RefFiltered { .. } => " Check out a matching branch or tag, or pass `--base <REF>` \
                 for pull_request events (the branch filter applies to the PR's target branch)."
                .to_owned(),
            M::PathsUnmatched { .. } | M::PathsAllIgnored { .. } => {
                " Change a file the filter selects, or pass `--base <REF>` to diff against a \
                 different base."
                    .to_owned()
            }
            M::UpstreamWorkflowUnmatched { .. } => {
                " A `workflow_run` trigger needs the upstream workflow's display name; supply it \
                 with `--payload` containing {\"workflow_run\": {\"name\": \"<NAME>\"}}."
                    .to_owned()
            }
        }
    }

    if !changed_paths_known && workflow.on.has_path_filters(&submission.event) {
        return Err(ApiError::bad_request(format!(
            "`on.{}` filters by path, so the run needs the complete list of changed files and \
             none was supplied. `preloop run` derives it from git, so this usually means the \
             workspace is not a git repository or no base ref could be resolved — pass `--base \
             <REF>` (for example `--base main`). An API caller must send `changed_paths` with \
             `changed_paths_known: true`, or a payload carrying `paths` or `commits`.",
            submission.event
        )));
    }
    // Activity type from explicit field (set by dispatcher) or payload.action fallback.
    let activity_owned: Option<String> = submission.activity_type.clone().or_else(|| {
        submission
            .payload
            .get("action")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
    });
    let activity_type = activity_owned.as_deref();
    let mut upstream_names = submission.workflow_run_upstream_names.clone();
    if upstream_names.is_empty() {
        if let Some(name) = submission
            .payload
            .get("workflow_run")
            .and_then(|wr| wr.get("name"))
            .and_then(|v| v.as_str())
        {
            upstream_names.push(name.to_owned());
        }
    }
    if let Err(reason) = workflow.on.match_event(
        &submission.event,
        branch.as_deref(),
        tag.as_deref(),
        &changed_paths,
        activity_type,
        &upstream_names,
    ) {
        return Err(ApiError::trigger_mismatch(format!(
            "workflow does not run for event `{}`: {reason}.{}",
            submission.event,
            trigger_mismatch_hint(&reason)
        )));
    }
    let dispatch_inputs_for_expand: BTreeMap<String, serde_json::Value> = submission
        .dispatch_inputs
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let expanded = preloop_gha_parser::expand_jobs_with_reusables_and_shas_and_inputs_and_event(
        &workflow,
        &submission.reusable_workflows,
        &submission.reusable_workflow_shas,
        (!dispatch_inputs_for_expand.is_empty()).then_some(&dispatch_inputs_for_expand),
        Some(submission.event.as_str()),
    )?;
    let mut jobs = expanded.jobs;
    let reusable_calls = expanded.reusable_calls;
    if !submission.dispatch_inputs.is_empty() {
        for job in &mut jobs {
            job.inputs = submission.dispatch_inputs.clone();
        }
    }

    // Filter to selected jobs and their transitive needs: closure.
    if !submission.selected_jobs.is_empty() {
        let pairs: Vec<(String, Vec<String>)> = jobs
            .iter()
            .map(|job| {
                (
                    job.base_id.clone(),
                    job.needs.iter().map(|n| n.0.clone()).collect(),
                )
            })
            .collect();
        // Reject the whole selection if any id is unknown. Silently dropping a
        // typo would run a subset of the requested jobs and report success.
        let mut selected = std::collections::BTreeSet::new();
        let mut known: std::collections::BTreeSet<&str> =
            pairs.iter().map(|(id, _)| id.as_str()).collect();
        for requested in &submission.selected_jobs {
            // A reusable caller is selected by its own (possibly
            // matrix-suffixed) node: its callee subtree is not part of the
            // plan anymore — it materializes at runtime after the gate passes.
            let matched_reusable = reusable_calls.keys().any(|caller| {
                caller.as_str() == requested
                    || caller
                        .strip_prefix(requested)
                        .is_some_and(|suffix| suffix.starts_with(" ("))
            });
            if matched_reusable {
                known.insert(requested);
            }
            selected.insert(requested.clone());
        }
        let unknown: Vec<&str> = submission
            .selected_jobs
            .iter()
            .map(String::as_str)
            .filter(|id| !known.contains(id))
            .collect();
        if !unknown.is_empty() {
            return Err(ApiError::bad_request(format!(
                "unknown job(s) in selected_jobs: {}. available jobs: {}",
                unknown.join(", "),
                known.into_iter().collect::<Vec<_>>().join(", ")
            )));
        }
        let graph = preloop_gha_parser::dag::needs_graph_from_pairs(&pairs);
        let closure = preloop_gha_parser::dag::dependency_closure(
            &graph,
            &selected.into_iter().collect::<Vec<_>>(),
        );
        let before = jobs.len();
        jobs.retain(|job| closure.contains(&job.base_id));
        tracing::info!(
            selected = ?submission.selected_jobs,
            before,
            after = jobs.len(),
            "filtered jobs to dependency closure"
        );
    }

    let run_id = RunId::new();
    let repository_owner = submission
        .repository
        .split('/')
        .next()
        .unwrap_or("owner")
        .to_string();
    let sha = submission
        .resolved_sha
        .clone()
        .or_else(|| {
            submission
                .payload
                .get("after")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
        .or_else(|| {
            // `pull_request` payloads carry no `after`; without consulting the
            // head sha the chain falls through to all-zeros and every checkout
            // asks the server for `0000…`, which fails as "not our ref" with
            // nothing pointing at the real cause.
            submission
                .payload
                .get("pull_request")
                .and_then(|pull_request| pull_request.get("head"))
                .and_then(|head| head.get("sha"))
                .and_then(|value| value.as_str())
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| {
            if preloop_gha_protocol::git_ref::is_commit_sha(&submission.git_ref) {
                submission.git_ref.clone()
            } else {
                "0000000000000000000000000000000000000000".to_owned()
            }
        })
        .to_string();
    let workflow_path = submission
        .workflow_path
        .clone()
        .unwrap_or_else(|| ".github/workflows/workflow.yml".to_owned());
    let workflow_ref = format!(
        "{}/{}@{}",
        submission.repository, workflow_path, submission.git_ref
    );

    // A pull_request submission is a synthetic PR: GitHub presents the
    // event with `github.ref = refs/pull/<number>/merge`, not the base
    // branch ref. Workflows gate on this (e.g. uv's plan computes
    // `on_main_branch` from `github.ref == refs/heads/main`); leaking the
    // base branch ref makes them treat the PR as a main-branch push and
    // enable every main-only gate.
    let github_ref = if submission.event == "pull_request" {
        let number = submission
            .payload
            .get("number")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                submission
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("number"))
                    .and_then(serde_json::Value::as_u64)
            })
            .unwrap_or(1);
        format!("refs/pull/{number}/merge")
    } else {
        submission.git_ref.clone()
    };
    // GitHub's `ref_name` is the short ref of `github.ref`: `feature-branch-1`
    // for branch events, `<tag>` for tags, and `<pr_number>/merge` for pull
    // requests (docs: "For pull requests that were not merged, the format is
    // `<pr_number>/merge`"). Deriving it from `github_ref` keeps the pair
    // consistent — previously PR events leaked the full `refs/pull/N/merge`
    // into `github.ref_name`/`GITHUB_REF_NAME`, breaking string comparisons
    // against the short form.
    let ref_name = github_ref
        .strip_prefix("refs/heads/")
        .or_else(|| github_ref.strip_prefix("refs/tags/"))
        .or_else(|| github_ref.strip_prefix("refs/pull/"))
        .unwrap_or(&github_ref)
        .to_owned();
    let ref_type = if github_ref.starts_with("refs/tags/") {
        "tag"
    } else {
        "branch"
    };
    let (pr_head_ref, pr_base_ref) = if submission.event == "pull_request" {
        let pr = submission.payload.get("pull_request");
        let head = pr
            .and_then(|pr| pr.get("head"))
            .and_then(|head| head.get("ref"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let base = pr
            .and_then(|pr| pr.get("base"))
            .and_then(|base| base.get("ref"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        (head.to_owned(), base.to_owned())
    } else {
        (
            String::new(),
            submission.base_ref.clone().unwrap_or_default(),
        )
    };

    let mut github = json!({
        "ref": github_ref,
        "sha": sha,
        "repository": submission.repository,
        "repository_owner": repository_owner,
        "repository_owner_id": "0",
        "repositoryUrl": format!("git://github.com/{}.git", submission.repository),
        "run_id": run_id.to_string(),
        "run_number": "1",
        "retention_days": "90",
        "run_attempt": "1",
        "artifact_cache_size_limit": "10",
        "repository_visibility": "private",
        "actor_id": "0",
        "actor": submission.actor.clone(),
        "workflow": workflow.name.clone().unwrap_or_default(),
        "head_ref": pr_head_ref,
        "base_ref": pr_base_ref,
        "event_name": submission.event,
        "server_url": shared.state.github_urls.server_url,
        "api_url": shared.state.github_urls.api_url,
        "graphql_url": shared.state.github_urls.graphql_url,
        "ref_name": ref_name,
        "ref_protected": false,
        "ref_type": ref_type,
        "secret_source": "Actions",
        "event": submission.payload,
        "workflow_ref": workflow_ref,
        "workflow_sha": sha,
        "repository_id": "0",
        "triggering_actor": submission.actor
    });

    let run_name = workflow.run_name.as_deref().map(|raw| {
        let inputs = if submission.dispatch_inputs.is_empty() {
            &submission.inputs
        } else {
            &submission.dispatch_inputs
        };
        evaluate_run_name(raw, &github, inputs, &submission.vars)
    });

    // Evaluate workflow-level concurrency before locking (pure).
    let workflow_concurrency = workflow.concurrency.clone();
    let mut empty_workflow_concurrency_group = false;
    let workflow_concurrency_eval = if let Some(raw) = &workflow_concurrency {
        let eval_ctx = concurrency::ConcurrencyContext {
            scope: concurrency::ConcurrencyScope::Workflow,
            github: &github,
            vars: &submission.vars,
            inputs: &submission.inputs,
            matrix: None,
            strategy: None,
            needs: None,
        };
        let (group, cancel, queue) =
            concurrency::evaluate_concurrency(raw, &eval_ctx).map_err(|error| {
                ApiError::bad_request(format!("concurrency evaluation failed: {error}"))
            })?;
        if group.trim().is_empty() {
            empty_workflow_concurrency_group = true;
            None
        } else {
            Some((group, cancel, queue, raw.clone()))
        }
    } else {
        None
    };

    // Capture one immutable source per run before any job is queued. Local
    // submissions snapshot the caller's working tree; opt-in remote modes
    // fetch the webhook commit once for every job in this run.
    let local_workspace = submission
        .local_workspace
        .as_deref()
        .map(std::path::Path::new)
        .or(shared.state.local_workspace.as_deref());
    let workspace_snapshot = if let Some(workspace) = local_workspace {
        match create_workspace_snapshot(
            &shared.state.state_dir,
            workspace,
            run_id,
            Some(shared),
            shared.state.static_github_pat().as_deref(),
        )
        .await
        {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                warn!(%run_id, error = ?error, "Failed to create workspace snapshot — falling back to normal checkout");
                None
            }
        }
    } else {
        match create_remote_checkout_snapshot(shared, &submission, run_id, &sha).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(%run_id, error = ?error, "Failed to populate remote checkout cache — falling back to normal checkout");
                None
            }
        }
    };

    // A push-requested submission from a dirty tree carries no explicit
    // tested tree (the client cannot know the snapshot tree in advance).
    // Record the snapshot's tree so push-back can verify the client's
    // materialized commit is byte-identical to what CI tested.
    if submission.push.is_some() && submission.push_tree.is_none() {
        if let Some(snapshot) = &workspace_snapshot {
            submission.push_tree = Some(snapshot.tree_sha.clone());
        }
    }
    // A push-requested run whose tree could not be captured can never be
    // pushed: the push endpoint refuses runs without a recorded tested tree.
    // Reject loudly at accept time instead of running CI on a submission
    // that is permanently unpushable (the later `/push` would fail with a
    // misleading "not submitted with --push").
    if submission.push.is_some() && submission.push_tree.is_none() {
        return Err(ApiError::bad_request(
            "push-requested run needs a tested tree, but the workspace snapshot could not be \
             created; commit or stash your changes and re-submit (or run without --push)",
        ));
    }

    // A local submission is a synthetic push/PR against the snapshot. Present
    // the same event shape GitHub would: changed-file actions
    // (`dorny/paths-filter`, `tj-actions/changed-files`) and `actions/checkout`
    // read `payload.repository.default_branch` and `payload.before` to pick
    // their diff base; without them they abort and gate the whole DAG closed.
    // The synthetic shape below is a local-submission concern: it fabricates
    // repository/before/head_commit data the forge never sent. A remote
    // snapshot carries the forge's real event payload, which must reach
    // workflows untouched — rewriting it would fake pushes, erase real
    // commit messages, and break changed-file and skip-ci gates.
    let local_snapshot = workspace_snapshot
        .as_ref()
        .filter(|snapshot| snapshot.source == crate::snapshots::SnapshotSource::LocalWorkspace);
    if let Some(snapshot) = local_snapshot {
        // Payload-less submissions (native local runs) carry `payload: null`;
        // the synthetic push/PR shape below needs an object to mutate, and
        // without it `before`/`after`/`ref`/`head_commit` were silently
        // missing from `github.event` for local runs.
        if !submission.payload.is_object() {
            submission.payload = serde_json::json!({});
        }
        if let Some(payload) = submission.payload.as_object_mut() {
            let (owner, name) = submission
                .repository
                .split_once('/')
                .map(|(owner, name)| (owner.to_owned(), name.to_owned()))
                .unwrap_or_else(|| ("local".to_owned(), submission.repository.clone()));
            payload.insert(
                "repository".to_owned(),
                serde_json::json!({
                    "name": name,
                    "full_name": submission.repository,
                    "owner": { "login": owner },
                    "default_branch": snapshot.default_branch.clone().unwrap_or_else(|| {
                        submission
                            .git_ref
                            .strip_prefix("refs/heads/")
                            .unwrap_or("main")
                            .to_owned()
                    }),
                }),
            );
            if submission.event == "push" {
                // `after` is the snapshot commit the runner checks out;
                // `before` is the base its changes are measured against (the
                // workspace HEAD when the tree is dirty, HEAD^ when clean — see
                // `WorkspaceSnapshot::before_sha`). An absent base (unborn or
                // initial-commit clean tree) is the null SHA, which GitHub
                // reports as an "initial push" and actions treat as
                // "everything changed".
                payload.insert(
                    "before".to_owned(),
                    snapshot
                        .before_sha
                        .clone()
                        .map(serde_json::Value::String)
                        .unwrap_or_else(|| {
                            serde_json::Value::String(
                                "0000000000000000000000000000000000000000".to_owned(),
                            )
                        }),
                );
                payload.insert("after".to_owned(), serde_json::json!(snapshot.commit_sha));
                payload.insert("ref".to_owned(), serde_json::json!(submission.git_ref));
                // GitHub push payloads carry `head_commit`; workflows gate on
                // `github.event.head_commit.message` (e.g. `[skip ci]`
                // markers). The snapshot knows the commit identity but not its
                // message, so the object is present with empty free-text
                // fields — `null` would make `head_commit.message` accesses
                // error out in expressions, an empty string stays falsey.
                payload.insert(
                    "head_commit".to_owned(),
                    serde_json::json!({
                        "id": snapshot.commit_sha,
                        "tree_id": "",
                        "distinct": true,
                        "message": "",
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "url": "",
                        "author": {"name": "", "email": "", "username": ""},
                        "committer": {"name": "", "email": "", "username": ""},
                        "added": [],
                        "removed": [],
                        "modified": [],
                    }),
                );
            } else if submission.event == "pull_request" {
                // Same synthetic-push shape for PR-family events: the head
                // commit the runner checks out is the snapshot commit
                // (`commit_sha` — the synthetic commit that carries the
                // dirty tree), and `base.sha` is the base its changes are
                // measured against (the workspace HEAD when the tree is
                // dirty, HEAD^ when clean — see `WorkspaceSnapshot::before_sha`).
                // Changed-file actions (`dorny/paths-filter`,
                // `tj-actions/changed-files`) diff these two SHAs; without
                // the refresh they would diff the caller-supplied head
                // against itself and see nothing, and pointing `head.sha` at
                // the real workspace HEAD diffed it against an identical
                // tree (and a sha absent from the snapshot store).
                let base_sha = snapshot
                    .before_sha
                    .clone()
                    .unwrap_or_else(|| "0000000000000000000000000000000000000000".to_owned());
                if let Some(pr) = payload
                    .get_mut("pull_request")
                    .and_then(|v| v.as_object_mut())
                {
                    if let Some(base) = pr.get_mut("base").and_then(|v| v.as_object_mut()) {
                        base.insert("sha".to_owned(), serde_json::json!(base_sha));
                    }
                    if let Some(head) = pr.get_mut("head").and_then(|v| v.as_object_mut()) {
                        head.insert("sha".to_owned(), serde_json::json!(snapshot.commit_sha));
                    }
                }
            }
        }
        // The github context was built before the snapshot existed; refresh
        // the pieces that now describe the local tree.
        if let Some(object) = github.as_object_mut() {
            object.insert("event".to_owned(), submission.payload.clone());
            // `github.sha` is the workspace's real HEAD commit, not the
            // synthetic snapshot commit: the snapshot commit exists only in
            // this engine's store, so a workflow step that fetches
            // `${{ github.sha }}` from the real remote (custom checkouts)
            // would be answered "not our ref". The workspace HEAD is the
            // identity the run is really based on.
            object.insert(
                "sha".to_owned(),
                serde_json::json!(snapshot
                    .head_sha
                    .clone()
                    .unwrap_or_else(|| snapshot.commit_sha.clone())),
            );
        }
    }

    // PATs are static and can be embedded now. GitHub App installation tokens
    // are deliberately minted later, when the broker dispatches each job, so
    // downstream jobs cannot sit in the queue until a short-lived token
    // expires.
    let mut github_tokens: BTreeMap<JobId, PatToken> = BTreeMap::new();
    if shared.state.github_app.is_none() {
        if let Some(pat) = shared.state.static_github_pat() {
            // H3: a static PAT cannot be narrowed per job, so a PAT broader
            // than a job's declared `permissions:` would silently hand every
            // non-fork job authority the workflow never claimed. Introspect
            // the PAT's classic OAuth scopes and refuse the run on mismatch;
            // an invalid PAT is refused outright, and a PAT whose bounds
            // cannot be verified is withheld rather than embedded.
            match pat_oauth_scopes(&pat).await {
                PatScopeOutcome::Known(scopes) => {
                    enforce_pat_permissions(&jobs, &submission, &scopes)?;
                    tracing::warn!(
                        %run_id,
                        pat_scopes = %scopes.join(", "),
                        "Using PRELOOP_GITHUB_TOKEN PAT for run jobs: workflow `permissions:` blocks are \
                         NOT enforced in PAT mode; the PAT above is embedded verbatim. Configure a \
                         GitHub App to mint least-privilege installation tokens."
                    );
                    github_tokens.extend(jobs.iter().map(|job| {
                        (
                            job.id.clone(),
                            PatToken::with_scopes(pat.clone(), scopes.clone()),
                        )
                    }));
                }
                PatScopeOutcome::Unverifiable { reason } => {
                    tracing::warn!(
                        %run_id,
                        reason = %reason,
                        "Withholding PRELOOP_GITHUB_TOKEN for run jobs: its OAuth scopes could not be \
                         introspected, so workflow `permissions:` blocks cannot be enforced and the \
                         PAT is not embedded. Jobs keep the job-scoped runtime token, so any step that \
                         needs GitHub fails. Configure a GitHub App to mint least-privilege installation \
                         tokens, or make the GitHub API reachable so the scopes can be verified."
                    );
                    github_tokens.extend(
                        jobs.iter()
                            .map(|job| (job.id.clone(), PatToken::withheld())),
                    );
                }
                PatScopeOutcome::Invalid(error) => {
                    return Err(ApiError::forbidden(format!(
                        "refusing run {run_id}: PRELOOP_GITHUB_TOKEN failed GitHub API \
                         authentication ({error:#}); refusing to embed an invalid PAT as \
                         job GITHUB_TOKENs"
                    )));
                }
            }
        }
    }

    // Reserve the workflow run number only after rechecking the durable
    // delivery identity under the same state lock used for run insertion.
    // A competing replay therefore returns before advancing the counter.
    let mut inner = shared.state.inner.lock().await;
    if let Some(delivery_id) = webhook_delivery_id.as_deref() {
        if let Some(existing) = existing_webhook_run(&inner, delivery_id, &workflow_path) {
            tracing::info!(
                %delivery_id,
                %workflow_path,
                run_id = %existing.run_id,
                "reusing run after webhook reservation race"
            );
            drop(inner);
            return Ok(existing);
        }
    }
    let run_number = {
        let counter = inner
            .workflow_run_counters
            .entry(workflow_path.clone())
            .or_insert(0);
        *counter += 1;
        *counter
    };
    if let Some(object) = github.as_object_mut() {
        object.insert(
            "run_number".to_owned(),
            serde_json::json!(run_number.to_string()),
        );
    }

    // Release the state lock while building messages, token material and OIDC
    // contexts for all jobs in the matrix. Holding the global lock during
    // serialization of a 100-job matrix blocks unrelated runners and polls;
    // the lock is reacquired only for atomic insertion into `inner.runs`.
    drop(inner);
    let base_url = runner_base_url();
    let normalized_github = preloop_gha_parser::job_builder::normalize_github_context(&github);
    let secrets_exposed: BTreeMap<String, String> =
        preloop_gha_protocol::masking::expose_all(&submission.secrets);

    struct PrebuiltJob {
        job: preloop_gha_protocol::JobPlan,
        agent_msg: Option<preloop_gha_protocol::azdo::AgentJobRequestMessage>,
        request_id: i64,
        condition_context: preloop_gha_expressions::Context,
        skipped: bool,
        caller: bool,
        id_token_granted: bool,
        oidc_ctx: OidcJobContext,
        job_request: Option<TaskAgentJobRequestRecord>,
        github_token_request: Option<GitHubTokenRequest>,
    }

    let mut prebuilt: Vec<PrebuiltJob> = Vec::with_capacity(jobs.len());
    let mut pre_statuses: BTreeMap<JobId, ExecutionStatus> = BTreeMap::new();
    let mut pre_job_base_ids: BTreeMap<JobId, String> = BTreeMap::new();
    let mut pre_job_needs: BTreeMap<JobId, Vec<JobId>> = BTreeMap::new();
    let mut pre_job_fail_fast: BTreeMap<String, bool> = BTreeMap::new();
    let mut pre_job_continue_on_error: BTreeMap<String, bool> = BTreeMap::new();
    let mut pre_initially_skipped: Vec<(RunId, JobId)> = Vec::new();
    let mut pre_caller_plans: BTreeMap<JobId, preloop_gha_protocol::JobPlan> = BTreeMap::new();
    let mut pre_job_names: BTreeMap<JobId, String> = BTreeMap::new();

    for job in jobs {
        pre_job_base_ids.insert(job.id.clone(), job.base_id.clone());
        pre_job_needs.insert(job.id.clone(), job.needs.clone());
        pre_job_fail_fast.insert(job.base_id.clone(), job.fail_fast);
        pre_job_continue_on_error.insert(job.id.to_string(), job.continue_on_error);
        pre_statuses.insert(job.id.clone(), ExecutionStatus::Queued);
        pre_job_names.insert(job.id.clone(), job.name.clone());
        if job.reusable_call.is_some() {
            pre_caller_plans.insert(job.id.clone(), job.clone());
        }

        let condition_context = build_context(
            &github,
            &BTreeMap::new(),
            &submission.vars,
            &indexmap::IndexMap::new(),
            &serde_json::json!({}),
            &BTreeMap::new(),
            &job.inputs,
        );

        // Evaluate condition for root jobs (no needs) outside the lock.
        let mut skipped = false;
        if job.needs.is_empty() {
            let condition =
                preloop_gha_expressions::effective_condition(job.if_condition.as_deref());
            let should_run = preloop_gha_expressions::eval_bool(&condition, &condition_context)
                .map_err(|error| {
                    ApiError::bad_request(format!(
                        "failed to evaluate condition for job `{}`: {error}",
                        job.id
                    ))
                })?;
            if !should_run {
                pre_statuses.insert(job.id.clone(), ExecutionStatus::Skipped);
                pre_initially_skipped.push((run_id, job.id.clone()));
                skipped = true;
            }
        }

        if skipped {
            // Still need to record the prebuilt entry so the index stays
            // aligned, but we skip the expensive message build.
            prebuilt.push(PrebuiltJob {
                job,
                agent_msg: None,
                request_id: 0,
                condition_context,
                skipped: true,
                caller: false,
                id_token_granted: false,
                oidc_ctx: OidcJobContext {
                    environment: None,
                    job_workflow_ref: None,
                    job_workflow_sha: None,
                },
                job_request: None,
                github_token_request: None,
            });
            continue;
        }

        let artifacts = build_job_artifacts(
            shared,
            &submission,
            run_id,
            &workflow_path,
            &workflow_ref,
            &sha,
            &normalized_github,
            &secrets_exposed,
            &base_url,
            workspace_snapshot.as_ref(),
            &job,
            github_tokens.remove(&job.id),
        )?;

        prebuilt.push(PrebuiltJob {
            caller: job.reusable_call.is_some(),
            job,
            agent_msg: Some(artifacts.agent_msg),
            request_id: artifacts.request_id,
            condition_context,
            skipped: false,
            id_token_granted: artifacts.id_token_granted,
            oidc_ctx: artifacts.oidc_ctx,
            job_request: Some(artifacts.job_request),
            github_token_request: artifacts.github_token_request,
        });
    }

    {
        let mut inner = shared.state.inner.lock().await;
        if let Some(delivery_id) = webhook_delivery_id.as_deref() {
            if let Some(existing) = existing_webhook_run(&inner, delivery_id, &workflow_path) {
                tracing::info!(
                    %delivery_id,
                    %workflow_path,
                    run_id = %existing.run_id,
                    "reusing run after webhook race during message building"
                );
                return Ok(existing);
            }
        }
        let created_at = chrono::Utc::now();
        let event = submission.event.clone();
        let github = github;
        let mut statuses = pre_statuses;
        let caller_plans = pre_caller_plans;
        let job_names = pre_job_names;
        let mut ready_jobs = 0usize;
        let job_base_ids = pre_job_base_ids;
        let job_needs = pre_job_needs;
        let job_fail_fast = pre_job_fail_fast;
        let job_continue_on_error = pre_job_continue_on_error;
        let mut ready_by_base: BTreeMap<String, u64> = BTreeMap::new();
        let initially_skipped = pre_initially_skipped;
        // Jobs concluded at submit because no runner can host their platform,
        // paired with the explanation emitted to watchers below.
        let mut unhostable_reasons: Vec<(JobId, String)> = Vec::new();
        let mut built_jobs: Vec<QueuedJob> = Vec::new();
        if empty_workflow_concurrency_group {
            let queued_jobs = 0;
            inner.runs.insert(
                run_id,
                RunRecord {
                    run_id,
                    webhook_delivery_id: webhook_delivery_id.clone(),
                    run_name,
                    submission: Arc::new(submission),
                    jobs: BTreeMap::new(),
                    job_outputs: BTreeMap::new(),
                    job_base_ids: BTreeMap::new(),
                    job_needs: BTreeMap::new(),
                    caller_plans: BTreeMap::new(),
                    job_names: BTreeMap::new(),
                    github: serde_json::Value::Null,
                    head_sha: String::new(),
                    workflow_ref: String::new(),
                    workspace_snapshot: None,
                    job_fail_fast: BTreeMap::new(),
                    job_continue_on_error: BTreeMap::new(),
                    status: ExecutionStatus::Failure,
                    job_check_run_ids: BTreeMap::new(),
                    reusable_calls,
                    jobs_list: Vec::new(),
                    created_at,
                    started_at: None,
                    completed_at: Some(created_at),
                    run_number,
                    run_attempt: 1,
                    workflow_path_str: workflow_path.clone(),
                    event: event.clone(),
                    conclusion: Some("failure".to_owned()),
                    push_state: None,
                    snapshot_timing: None,
                    fork_approval_pending,
                    fork_approval_requested_at_unix_nanos,
                    fork_approved_at_unix_nanos: None,
                    fork_approval_note: None,
                },
            );
            drop(inner);
            shared
                .state
                .emit(NdjsonEvent::RunAccepted {
                    run_id,
                    queued_jobs,
                })
                .await;
            shared
                .state
                .emit(NdjsonEvent::RunStatus {
                    run_id,
                    status: ExecutionStatus::Failure,
                    reason: Some("concurrency group name must not be empty".to_owned()),
                })
                .await;
            return Ok(RunAccepted {
                run_id,
                run_number,
                queued_jobs,
            });
        }
        // ── Install pre-built jobs under the lock (map inserts only) ────
        for pb in prebuilt {
            if pb.skipped {
                continue;
            }
            let job = &pb.job;
            let agent_msg = pb.agent_msg.expect("non-skipped job must have agent_msg");

            if !pb.caller {
                // Caller placeholders are scheduling-only: no runner ever
                // acquires them, so no request correlation records exist.
                let job_request = pb
                    .job_request
                    .expect("non-skipped job must have job_request");

                inner
                    .id_token_grants
                    .insert((run_id, job.id.clone()), pb.id_token_granted);
                inner
                    .oidc_job_contexts
                    .insert((run_id, job.id.clone()), pb.oidc_ctx);

                inner
                    .inflight_requests
                    .insert(job_request.request_id, (run_id, job.id.clone()));
                inner
                    .plan_requests
                    .insert(job_request.plan_id.clone(), pb.request_id);
                inner
                    .agent_job_requests
                    .insert(job_request.agent_job_id, pb.request_id);
                inner
                    .timeline_requests
                    .insert(job_request.timeline_id, pb.request_id);
                // Seed the attempt's step manifest before the runner can
                // report anything, so step identity and order come from the
                // message we just built rather than from whatever order the
                // runner's log blobs happen to land in.
                //
                // Not persisted here. Rows are written by a runner report, a
                // job completion, or a full snapshot that happens to flush
                // them; none of those has necessarily run when a dispatched
                // attempt is interrupted, so its manifest is rebuilt at startup
                // from the persisted request message — the same source it was
                // built from — and a restart in that window keeps its declared
                // steps.
                inner.job_steps.insert(
                    job_request.agent_job_id,
                    StepRecord::manifest(&agent_msg.steps),
                );
                inner.job_requests.insert(pb.request_id, job_request);
                if let Some(request) = pb.github_token_request {
                    inner.github_token_requests.insert(pb.request_id, request);
                    tracing::debug!(
                        request_id = pb.request_id,
                        job = %job.id,
                        "prebuild: dispatch token request inserted"
                    );
                } else {
                    // Normal whenever no GitHub App is configured; one line per
                    // job would drown the log on a wide matrix.
                    tracing::debug!(
                        request_id = pb.request_id,
                        job = %job.id,
                        "prebuild: job has no dispatch token request"
                    );
                }
            }
            let created_at_unix_nanos = crate::models::now_unix_nanos();
            let queued_job = QueuedJob {
                run_id,
                job_id: job.id.clone(),
                base_id: job.base_id.clone(),
                created_at_unix_nanos,
                dependencies_ready_at_unix_nanos: job
                    .needs
                    .is_empty()
                    .then_some(created_at_unix_nanos),
                concurrency_wait_started_at_unix_nanos: None,
                concurrency_acquired_at_unix_nanos: None,
                // Stamped when the job actually enters the ready queue (the
                // promotion sites in runtime_scheduling), never at build
                // time: dependency/concurrency delay is not queue wait.
                enqueued_at_unix_nanos: 0,
                needs: job.needs.clone(),
                if_condition: job.if_condition.clone(),
                condition_context: pb.condition_context,
                max_parallel: job.max_parallel,
                runs_on: job.runs_on.clone(),
                runner_group: job.runner_group.clone(),
                environment: job.environment.clone(),
                message: agent_msg,
                concurrency: concurrency::concurrency_from_plan_fields(
                    job.concurrency_group.as_deref(),
                    job.concurrency_cancel_in_progress.as_deref(),
                    job.concurrency_queue.as_deref(),
                ),
                matrix: job
                    .matrix
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                deferred_matrix: job.deferred_matrix.clone(),
                reusable_call: job.reusable_call.clone(),
                environment_gate: None,
            };
            built_jobs.push(queued_job);
        }

        // Workflow-level concurrency gate.
        let mut hold_entire_run = false;
        if let Some((group, cancel, queue, raw)) = &workflow_concurrency_eval {
            let key = concurrency::concurrency_key(&submission.repository, group);
            match try_acquire_concurrency(
                &mut inner,
                key,
                group.clone(),
                concurrency::Holder::Run(run_id),
                *cancel,
                *queue,
            ) {
                Ok(true) => {
                    for job in &mut built_jobs {
                        runtime_scheduling::stamp_concurrency_acquired(job);
                    }
                    shared
                        .state
                        .observability
                        .metrics()
                        .lifecycle
                        .record_concurrency_decision("workflow", "accept");
                    inner.run_concurrency.insert(run_id, raw.clone());
                }
                Ok(false) => {
                    shared
                        .state
                        .observability
                        .metrics()
                        .lifecycle
                        .record_concurrency_decision("workflow", "pending");
                    hold_entire_run = true;
                    for job in &mut built_jobs {
                        runtime_scheduling::stamp_concurrency_wait_started(job);
                    }
                    inner.run_concurrency.insert(run_id, raw.clone());
                }
                Err(e) if e == "concurrency_queue_overflow" => {
                    shared
                        .state
                        .observability
                        .metrics()
                        .lifecycle
                        .record_concurrency_decision("workflow", "reject");
                    // Cancel this run immediately — all jobs Cancelled.
                    for job in &built_jobs {
                        statuses.insert(job.job_id.clone(), ExecutionStatus::Cancelled);
                    }
                    let queued_jobs = statuses.len();
                    inner.runs.insert(
                        run_id,
                        RunRecord {
                            run_id,
                            webhook_delivery_id: webhook_delivery_id.clone(),
                            run_name,
                            submission: Arc::new(submission),
                            jobs: statuses,
                            job_outputs: BTreeMap::new(),
                            job_base_ids,
                            job_needs,
                            caller_plans: caller_plans.clone(),
                            job_names: job_names.clone(),
                            github: github.clone(),
                            head_sha: sha.clone(),
                            workflow_ref: workflow_ref.clone(),
                            workspace_snapshot: workspace_snapshot.clone(),
                            job_fail_fast,
                            job_continue_on_error,
                            status: ExecutionStatus::Cancelled,
                            job_check_run_ids: BTreeMap::new(),
                            reusable_calls,
                            jobs_list: Vec::new(),
                            created_at,
                            started_at: None,
                            completed_at: Some(created_at),
                            run_number,
                            run_attempt: 1,
                            workflow_path_str: workflow_path.clone(),
                            event: event.clone(),
                            conclusion: Some("cancelled".to_owned()),
                            push_state: None,
                            snapshot_timing: None,
                            fork_approval_pending,
                            fork_approval_requested_at_unix_nanos,
                            fork_approved_at_unix_nanos: None,
                            fork_approval_note: None,
                        },
                    );
                    // The run died on arrival: nothing will ever dispatch,
                    // so the expandable nodes' minted request correlation has
                    // to be settled here (MC-3), exactly like a cancellation.
                    for job in &built_jobs {
                        if job.deferred_matrix.is_some() || job.reusable_call.is_some() {
                            runtime_scheduling::retire_node_requests(
                                &mut inner,
                                run_id,
                                &job.job_id,
                                runtime_scheduling::RequestRetirement::Settle(
                                    ExecutionStatus::Cancelled,
                                ),
                            );
                        }
                    }
                    drop(inner);
                    shared
                        .state
                        .emit(NdjsonEvent::RunAccepted {
                            run_id,
                            queued_jobs,
                        })
                        .await;
                    shared
                        .state
                        .emit(NdjsonEvent::RunStatus {
                            run_id,
                            status: ExecutionStatus::Cancelled,
                            reason: concurrency::cancelled_reason(),
                        })
                        .await;
                    return Ok(RunAccepted {
                        run_id,
                        run_number,
                        queued_jobs,
                    });
                }
                Err(e) => {
                    shared
                        .state
                        .observability
                        .metrics()
                        .lifecycle
                        .record_concurrency_decision("workflow", "reject");
                    return Err(ApiError::bad_request(e));
                }
            }
        }

        if hold_entire_run {
            for job in &built_jobs {
                statuses.insert(job.job_id.clone(), ExecutionStatus::Pending);
            }
            inner.held_runs.insert(run_id, built_jobs);
            let queued_jobs = statuses.len();
            inner.runs.insert(
                run_id,
                RunRecord {
                    run_id,
                    webhook_delivery_id: webhook_delivery_id.clone(),
                    run_name,
                    submission: Arc::new(submission),
                    jobs: statuses,
                    job_outputs: BTreeMap::new(),
                    job_base_ids,
                    job_needs,
                    caller_plans: caller_plans.clone(),
                    job_names: job_names.clone(),
                    github: github.clone(),
                    head_sha: sha.clone(),
                    workflow_ref: workflow_ref.clone(),
                    workspace_snapshot: workspace_snapshot.clone(),
                    job_fail_fast,
                    job_continue_on_error,
                    status: ExecutionStatus::Pending,
                    job_check_run_ids: BTreeMap::new(),
                    reusable_calls,
                    jobs_list: Vec::new(),
                    created_at,
                    started_at: None,
                    completed_at: None,
                    run_number,
                    run_attempt: 1,
                    workflow_path_str: workflow_path.clone(),
                    event: event.clone(),
                    conclusion: None,
                    push_state: None,
                    snapshot_timing: None,
                    fork_approval_pending,
                    fork_approval_requested_at_unix_nanos,
                    fork_approved_at_unix_nanos: None,
                    fork_approval_note: None,
                },
            );
            drop(inner);
            shared
                .state
                .emit(NdjsonEvent::RunAccepted {
                    run_id,
                    queued_jobs,
                })
                .await;
            shared
                .state
                .emit(NdjsonEvent::RunStatus {
                    run_id,
                    status: ExecutionStatus::Pending,
                    reason: concurrency::pending_reason(),
                })
                .await;
            return Ok(RunAccepted {
                run_id,
                run_number,
                queued_jobs,
            });
        }
        // Install a provisional run before evaluating per-job and JobSet gates.
        // Multiple holders from this same submission can cancel each other;
        // cancellation helpers need the run to exist so they can persist the
        // affected job conclusion instead of silently becoming no-ops.
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                webhook_delivery_id: webhook_delivery_id.clone(),
                run_name: run_name.clone(),
                submission: Arc::new(submission.clone()),
                jobs: statuses.clone(),
                job_outputs: BTreeMap::new(),
                job_base_ids: job_base_ids.clone(),
                job_needs: job_needs.clone(),
                caller_plans: caller_plans.clone(),
                job_names: job_names.clone(),
                github: github.clone(),
                head_sha: sha.clone(),
                workflow_ref: workflow_ref.clone(),
                workspace_snapshot: workspace_snapshot.clone(),
                job_fail_fast: job_fail_fast.clone(),
                job_continue_on_error: job_continue_on_error.clone(),
                status: ExecutionStatus::Queued,
                job_check_run_ids: BTreeMap::new(),
                reusable_calls: reusable_calls.clone(),
                jobs_list: Vec::new(),
                created_at,
                started_at: None,
                completed_at: None,
                run_number,
                run_attempt: 1,
                workflow_path_str: workflow_path.clone(),
                event: event.clone(),
                conclusion: None,
                push_state: None,
                snapshot_timing: workspace_snapshot
                    .as_ref()
                    .and_then(|snapshot| snapshot.snapshot_timing),
                fork_approval_pending,
                fork_approval_requested_at_unix_nanos,
                fork_approved_at_unix_nanos: None,
                fork_approval_note: None,
            },
        );

        // Enqueue jobs (workflow concurrency free / acquired).
        for queued_job in built_jobs {
            let job_id = queued_job.job_id.clone();
            let base_id = queued_job.base_id.clone();

            // Deferred reusable-caller nodes are scheduling-only: they wait in
            // pending_jobs until their `if:` gate passes, when the scheduler
            // acquires caller/embedded JobSet concurrency gates and expands
            // the callee subtree (mirroring GitHub, which evaluates caller
            // concurrency when the caller job starts).
            if queued_job.reusable_call.is_some() {
                statuses.insert(job_id, ExecutionStatus::Pending);
                inner.pending_jobs.push_back(queued_job);
                continue;
            }

            // No runner host for this platform: conclude the job rather than
            // queue one nothing can ever claim. Checked here, before the job
            // reaches either the ready queue or `pending_jobs`, so a
            // needs-gated job on an unhostable platform concludes too and its
            // dependents see a terminal status.
            //
            // The conclusion is `Failure`, never `Skipped`. A skipped job folds
            // into `summarize_run` as success, so a workflow whose macOS leg
            // could not run anywhere would report green while its steps never
            // executed — the worst outcome available, and worse than the
            // indefinite queue GitHub would leave behind. Failing is loud,
            // and the annotation below puts the reason where the user reads it
            // rather than only in the server log.
            let platforms = runtime_scheduling::registered_runner_platforms(&inner);
            if let Some(platform) =
                runtime_scheduling::unhostable_platform(&queued_job.runs_on, platforms)
            {
                let reason = format!(
                    "no {platform} runner is registered with this server, so `runs-on: {}` \
                     cannot be scheduled",
                    queued_job.runs_on.join(", ")
                );
                tracing::warn!(
                    job = %job_id.0,
                    labels = ?queued_job.runs_on,
                    platform,
                    "no {platform} runner is registered; failing the job"
                );
                unhostable_reasons.push((job_id.clone(), reason));
                statuses.insert(job_id, ExecutionStatus::Failure);
                continue;
            }

            // Full label validation against the co-hosted pool's advertised
            // labels: when the pool has published them, a `runs-on` it can
            // never satisfy fails at enqueue rather than starving in the
            // queue. Skipped when the pool hasn't published (external-only
            // deployments, or a pool that predates the field) — the
            // starvation sweep remains the backstop there.
            let pool_labels = shared.state.pool_status.snapshot().labels;
            if !pool_labels.is_empty()
                && !crate::runtime_scheduling::job_matches_runner(&queued_job.runs_on, &pool_labels)
            {
                let reason = format!(
                    "the runner pool's advertised labels ({}) can never satisfy \
                     `runs-on: {}`, so the job cannot be scheduled",
                    pool_labels.join(", "),
                    queued_job.runs_on.join(", ")
                );
                tracing::warn!(
                    job = %job_id.0,
                    labels = ?queued_job.runs_on,
                    pool_labels = ?pool_labels,
                    "runs-on unsatisfiable by runner pool; failing the job at enqueue"
                );
                unhostable_reasons.push((job_id.clone(), reason));
                statuses.insert(job_id, ExecutionStatus::Failure);
                continue;
            }

            let needs_empty = queued_job.needs.is_empty();
            let max_parallel = queued_job.max_parallel;
            let under_mp = max_parallel
                .is_none_or(|max| ready_by_base.get(&base_id).copied().unwrap_or(0) < max);

            // Fork-PR workflow policy: a run awaiting fork approval holds
            // every job in pending_jobs until the operator approves the run.
            // The needs-empty fast path must not bypass that hold by
            // enqueueing straight to inner.queue (or parking in
            // concurrency_blocked): the fork gate in promote_ready_jobs only
            // inspects pending_jobs.
            let fork_held = inner
                .runs
                .get(&run_id)
                .is_some_and(|run| run.fork_approval_pending);
            if needs_empty && under_mp && !fork_held {
                // Job-level concurrency gate (needs/max_parallel already satisfied).
                match try_enqueue_with_job_concurrency(
                    &mut inner,
                    &github,
                    &submission,
                    queued_job,
                    &mut statuses,
                ) {
                    Ok(true) => {
                        shared
                            .state
                            .observability
                            .metrics()
                            .lifecycle
                            .record_concurrency_decision("job", "accept");
                        *ready_by_base.entry(base_id).or_default() += 1;
                        ready_jobs += 1;
                    }
                    Ok(false) => {
                        shared
                            .state
                            .observability
                            .metrics()
                            .lifecycle
                            .record_concurrency_decision("job", "pending");
                        // parked pending
                    }
                    Err(_) => {
                        shared
                            .state
                            .observability
                            .metrics()
                            .lifecycle
                            .record_concurrency_decision("job", "reject");
                        // cancelled by queue overflow or eval failure already marked
                    }
                }
            } else {
                // Fork-held jobs wait visibly in Pending until the operator
                // approves the run; everything else queues normally for the
                // scheduler.
                let status = if fork_held {
                    ExecutionStatus::Pending
                } else {
                    ExecutionStatus::Queued
                };
                statuses.insert(job_id, status);
                inner.pending_jobs.push_back(queued_job);
            }
        }

        // Preserve terminal conclusions written through cancel_job_inner while
        // gates were evaluated. Non-terminal scheduling state remains owned by
        // the local status map and is installed below with the final record.
        if let Some(provisional) = inner.runs.get(&run_id) {
            for (job_id, status) in &provisional.jobs {
                if status.is_terminal() {
                    statuses.insert(job_id.clone(), *status);
                }
            }
        }

        let queued_jobs = statuses.len();
        // C-05: derive the initial run status from job statuses so that eval
        // failures (Failure) are reflected immediately rather than leaving the
        // run permanently Queued. summarize_run returns InProgress for any mix
        // of Queued/Pending jobs; map that to Queued since no job has started.
        let initial_status = {
            let s = summarize_run(statuses.values().copied());
            if s == ExecutionStatus::InProgress {
                ExecutionStatus::Queued
            } else {
                s
            }
        };
        let snapshot_timing = workspace_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.snapshot_timing);
        inner.runs.insert(
            run_id,
            RunRecord {
                run_id,
                webhook_delivery_id: webhook_delivery_id.clone(),
                run_name,
                submission: Arc::new(submission),
                jobs: statuses,
                job_outputs: BTreeMap::new(),
                job_base_ids,
                job_needs,
                caller_plans,
                job_names,
                github,
                head_sha: sha,
                workflow_ref,
                workspace_snapshot,
                job_fail_fast,
                job_continue_on_error,
                status: initial_status,
                job_check_run_ids: BTreeMap::new(),
                reusable_calls,
                jobs_list: Vec::new(),
                created_at,
                started_at: None,
                completed_at: None,
                run_number,
                run_attempt: 1,
                workflow_path_str: workflow_path.clone(),
                event: event.clone(),
                conclusion: None,
                push_state: None,
                snapshot_timing,
                fork_approval_pending,
                fork_approval_requested_at_unix_nanos,
                fork_approved_at_unix_nanos: None,
                fork_approval_note: None,
            },
        );
        // Deferred reusable-caller nodes whose needs are already satisfied
        // (typically none) are reified by a first promote sweep: needs-free
        // callers acquire their JobSet gates and materialize their callee
        // subtree immediately.
        promote_ready_jobs(&mut inner, &shared.state.environment_rules);
        // A submission whose every job concluded before it reached the queue
        // (all skipped by `if:`, or none hostable) never passes through the
        // completion path, so nothing else would ever stamp `completed_at` and
        // `conclusion`. Without this the run reports a terminal status while
        // anything polling for completion waits forever.
        if let Some(run) = inner.runs.get_mut(&run_id) {
            runtime_scheduling::finalize_run_if_complete(run);
        }
        // The on-demand runner supervisor uses this atomic as its wake-up
        // signal. Refresh it when submission makes work runnable; updating it
        // only after a runner claims a job leaves a size-zero pool asleep
        // forever on the first webhook-created run.
        shared
            .state
            .queue_depth
            .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
        runtime_scheduling::sync_next_job_labels(&inner, &shared.state.next_job_runs_on);
        let cancel_count = inner.cancellation_queue.len();
        drop(inner);
        // The sweep above only recorded the intent to expand; the subtree build
        // runs here with the lock released.
        let expansion = drain_expansions(shared).await;
        if ready_jobs > 0 || cancel_count > 0 || expansion.promoted > 0 {
            shared.state.message_notify.notify_waiters();
        }
        for (event_run_id, job_id) in initially_skipped {
            shared
                .state
                .emit(NdjsonEvent::JobStatus {
                    run_id: event_run_id,
                    job_id,
                    status: ExecutionStatus::Skipped,
                    reason: None,
                })
                .await;
        }
        // Surface why a job could never be scheduled. Without this the only
        // record is a server-side log line the workflow author never sees.
        for (job_id, reason) in unhostable_reasons {
            shared
                .state
                .emit(NdjsonEvent::JobStatus {
                    run_id,
                    job_id,
                    status: ExecutionStatus::Failure,
                    reason: Some(reason),
                })
                .await;
        }
        shared
            .state
            .emit(NdjsonEvent::RunAccepted {
                run_id,
                queued_jobs,
            })
            .await;
        Ok(RunAccepted {
            run_id,
            run_number,
            queued_jobs,
        })
    }
}
pub async fn submit_run(
    State(shared): State<Arc<SharedState>>,
    headers: axum::http::HeaderMap,
    Json(mut submission): Json<WorkflowSubmission>,
) -> Result<Json<RunAccepted>, ApiError> {
    // Native callers cannot establish webhook provenance. Never allow a
    // request body to select the trust tier used by auto-PR and secret policy;
    // only the GitHub webhook adapters may stamp this field.
    submission.trust_tier = None;
    if let Some(encoded) = headers
        .get("x-preloop-local-workspace")
        .and_then(|value| value.to_str().ok())
    {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| ApiError::bad_request("invalid local workspace header"))?;
        submission.local_workspace = Some(
            String::from_utf8(bytes)
                .map_err(|_| ApiError::bad_request("local workspace path is not UTF-8"))?,
        );
    }

    // A run that asks for push-back must be a real GitHub branch at a real
    // commit; anything else can never produce a PR or honest checks. Refuse
    // before queueing a single job so the failure is loud and immediate.
    let push_requested = submission.push.is_some();
    if push_requested {
        crate::github_push::validate_push_target(
            &submission.repository,
            &submission.sha,
            &submission.git_ref,
            submission.push_tree.as_deref().unwrap_or_default(),
            // A dirty-tree submission cannot know the tested tree up front:
            // the server snapshots the workspace inside submit_run_inner and
            // records the snapshot tree below. The client materializes its
            // commit from that recorded tree after CI passes.
            // The snapshot uses either the client's workspace header or the
            // server-side configured workspace, so both count here.
            submission.local_workspace.is_some() || shared.state.local_workspace.is_some(),
        )?;
    }

    // A dirty-tree push has no known head commit at submit time: check runs
    // would attach to the base commit and stay pending forever. The push
    // endpoint reports them against the materialized branch head instead
    // (`github_push.rs` step 4). Clean-tree pushes know their commit up
    // front and get queued checks immediately.
    let dirty_push = submission.push.as_ref().is_some_and(|push| push.dirty);
    let clean_push_checks = push_requested && !dirty_push;
    let accepted = submit_run_inner(&shared, submission).await?;
    if push_requested {
        let run_id = accepted.run_id;
        if clean_push_checks {
            // Report queued check runs for every job, exactly like the
            // webhook adapter does for delivered events, so GitHub shows the
            // run from the moment it is accepted. Jobs resolved terminal at
            // submission (skipped, unsatisfiable needs) get their completion
            // immediately.
            let (repository, sha, jobs) = {
                let inner = shared.state.inner.lock().await;
                let Some(run) = inner.runs.get(&run_id) else {
                    return Ok(Json(accepted));
                };
                (
                    run.submission.repository.clone(),
                    run.submission.sha.clone(),
                    run.jobs.keys().cloned().collect::<Vec<_>>(),
                )
            };
            for job_id in &jobs {
                if let Err(error) = crate::github::report_check_run_queued(
                    &shared,
                    &repository,
                    &sha,
                    job_id,
                    run_id,
                )
                .await
                {
                    tracing::warn!(%run_id, %job_id, ?error, "failed to report queued GitHub check run");
                }
                let status = {
                    let inner = shared.state.inner.lock().await;
                    inner
                        .runs
                        .get(&run_id)
                        .and_then(|run| run.jobs.get(job_id).copied())
                };
                if let Some(status) = status.filter(|status| status.is_terminal()) {
                    crate::github::report_check_run_completed(&shared, run_id, job_id, status)
                        .await;
                }
            }
        }
        let mut inner = shared.state.inner.lock().await;
        if let Some(run) = inner.runs.get_mut(&run_id) {
            run.push_state = Some(PushState {
                status: PushStatus::Pending,
                error: None,
                pr_number: None,
                effective_sha: None,
            });
        }
    }
    Ok(Json(accepted))
}

pub async fn get_scheduler_history(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<Vec<crate::scheduler::ScheduleFire>>, ApiError> {
    if let Some(scheduler) = &shared.state.scheduler {
        let history = scheduler.history.lock().await.clone();
        Ok(Json(history))
    } else {
        Ok(Json(vec![]))
    }
}

pub fn value_to_input_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => value.to_string(),
        _ => value.to_string(),
    }
}

pub fn git_ref_context(git_ref: &str) -> (Option<String>, Option<String>) {
    if let Some(branch) = git_ref.strip_prefix("refs/heads/") {
        (Some(branch.to_owned()), None)
    } else if let Some(tag) = git_ref.strip_prefix("refs/tags/") {
        (None, Some(tag.to_owned()))
    } else {
        (None, None)
    }
}

pub fn changed_paths_from_payload(payload: &serde_json::Value) -> Vec<String> {
    let mut paths = Vec::new();

    if let Some(values) = payload.get("paths").and_then(|value| value.as_array()) {
        collect_string_array(values, &mut paths);
    }

    if let Some(commits) = payload.get("commits").and_then(|value| value.as_array()) {
        for commit in commits {
            for field in ["added", "modified", "removed"] {
                if let Some(values) = commit.get(field).and_then(|value| value.as_array()) {
                    collect_string_array(values, &mut paths);
                }
            }
        }
    }

    paths.sort();
    paths.dedup();
    paths
}

pub fn collect_string_array(values: &[serde_json::Value], out: &mut Vec<String>) {
    out.extend(
        values
            .iter()
            .filter_map(|value| value.as_str())
            .map(str::to_owned),
    );
}

/// Per-job runner artifacts: agent message plus the correlation records the
/// broker and results/timeline services use to track the delivered request.
pub struct BuiltJobArtifacts {
    pub agent_msg: azdo::AgentJobRequestMessage,
    pub request_id: i64,
    pub job_request: TaskAgentJobRequestRecord,
    pub id_token_granted: bool,
    pub oidc_ctx: OidcJobContext,
    pub github_token_request: Option<GitHubTokenRequest>,
}

/// The `fileTable` entry naming the reusable workflow a job was inlined from.
///
/// `None` for jobs defined in the caller, which need no second entry. Remote
/// callees render as `owner/repo/path@sha` (GitHub's shape, e.g.
/// `Bnjoroge1/conformance-v2337/.github/workflows/reusable-build.yml@eafe8a9`);
/// a local callee has no repository or sha of its own, so the path stands alone.
fn reusable_file_table_entry(job: &preloop_gha_protocol::JobPlan) -> Option<String> {
    let file = job.workflow_file.as_deref()?;
    let entry = match (&job.workflow_repository, &job.workflow_sha) {
        (Some(repository), Some(sha)) => {
            format!("{repository}/{}@{sha}", strip_repo_prefix(file, repository))
        }
        (Some(repository), None) => {
            format!("{repository}/{}", strip_repo_prefix(file, repository))
        }
        (None, _) => file.to_owned(),
    };
    Some(entry)
}

/// `expand.rs::normalize_reusable_path` stores remote `uses:` as
/// `owner/repo/path.yml`, while `workflow_repository` is `owner/repo`.
/// Concatenating them raw would emit `owner/repo/owner/repo/path.yml@sha`.
fn strip_repo_prefix<'a>(file: &'a str, repository: &str) -> &'a str {
    file.strip_prefix(repository)
        .and_then(|rest| rest.strip_prefix('/'))
        .unwrap_or(file)
}

/// Build one job's runner message and correlation records.
///
/// Pure computation shared by the submission prebuild and the scheduler's
/// runtime expansion of reusable-workflow callee subtrees (which cannot be
/// built at submission: they exist only after the caller's `if:` gate passes).
#[allow(clippy::too_many_arguments)]
pub fn build_job_artifacts(
    shared: &SharedState,
    submission: &WorkflowSubmission,
    run_id: RunId,
    workflow_path: &str,
    workflow_ref: &str,
    sha: &str,
    normalized_github: &serde_json::Value,
    secrets_exposed: &BTreeMap<String, String>,
    base_url: &str,
    workspace_snapshot: Option<&WorkspaceSnapshot>,
    job: &preloop_gha_protocol::JobPlan,
    github_token_override: Option<PatToken>,
) -> Result<BuiltJobArtifacts, ApiError> {
    // One policy drives every job-facing authority decision for this tier:
    // stored secrets, the runner-visible `system.github.token.permissions`
    // variable, the GitHub App installation-token request, the OIDC grant,
    // and which external token (if any) the job may receive. Fork-restricted
    // tiers (fork PRs, fail-closed unknown events) get GitHub's read-only
    // fork profile no matter what the workflow declared.
    let tier = crate::events::trust_tier::tier_of(submission);
    let policy = crate::events::trust_tier::job_authorization(
        tier,
        job.permissions.as_ref(),
        job.oidc_id_token_granted,
    );

    // M4: the environment registry. `environment:` names an
    // operator-registered deployment tier; a workflow claiming an
    // unregistered name gets nothing — no environment secrets, no
    // environment OIDC subject — and the job fails closed rather than
    // minting a token for a never-created, never-approved environment.
    // This check runs ahead of `policy.allows_secrets` because the OIDC
    // subject is minted for jobs even when secret injection is disabled.
    // Note: expression-based names (`${{ needs.* }}`, `${{ vars.* }}`, …)
    // arrive here as `None` — the parser only resolves `matrix.*` at build
    // time — so they are not rejected here, but they also receive no
    // environment secrets and no environment OIDC subject (both are keyed
    // off this same field). The name later resolved by
    // `hydrate_needs_context` is not re-validated against the registry;
    // it only reaches the runner's deployment record.
    if let Some(env_name) = job.oidc_environment.as_deref() {
        if !shared
            .state
            .secrets
            .read()
            .is_environment_registered(&submission.repository, env_name)
        {
            return Err(ApiError::forbidden(format!(
                "environment '{env_name}' is not registered for repository '{}'; register it under [environments]",
                submission.repository
            )));
        }
    }

    // Environment secrets are per-job: a job's `environment:` selects the
    // tier, so the overlay happens here, not in the submission-level merge.
    // Precedence per name: submission-provided > environment > repo > global,
    // mirroring GitHub's env-over-repo-over-org rule with the local
    // `--secret` escape hatch kept on top.
    // Overlay lazily: most jobs have no `environment:` tier, and the base
    // map can be large — copying it per job would be pure allocation cost.
    // The original map is borrowed directly in that case.
    let mut env_overlay: Option<BTreeMap<String, String>> = None;
    if policy.allows_secrets {
        if let Some(env_name) = job.oidc_environment.as_deref() {
            let env_secrets = shared
                .state
                .secrets
                .read()
                .env
                .get(&submission.repository)
                .and_then(|envs| envs.get(env_name))
                .cloned();
            if let Some(env_secrets) = env_secrets {
                let mut merged = secrets_exposed.clone();
                for (name, value) in env_secrets {
                    if !submission.submission_names.contains(&name) {
                        merged.insert(name, value);
                    }
                }
                env_overlay = Some(merged);
            }
        }
    }
    let merged_secrets = env_overlay.as_ref().unwrap_or(secrets_exposed);

    let mut agent_msg =
        preloop_gha_parser::job_builder::build_agent_job_message_with_normalized_context(
            job,
            normalized_github,
            &job.env,
            merged_secrets,
            &submission.vars,
        )
        .map_err(|e| ApiError::bad_request(format!("failed to build job message: {e}")))?;

    agent_msg.preloop_preserve_on_failure = submission.preserve_on_failure.then_some(true);
    agent_msg.preloop_debug_on_failure = submission.debug_on_failure.then_some(true);

    // The message builder already wrote the declared permission set into the
    // wire variable; for a fork-restricted job that set must be restated as
    // the downgraded fork profile, because the runner prints this variable
    // as its `GITHUB_TOKEN Permissions` group and it must not claim write
    // authority the job's token will not carry.
    if policy.fork_restricted {
        agent_msg.variables.insert(
            "system.github.token.permissions".to_owned(),
            preloop_gha_protocol::azdo::VariableValue::new(
                preloop_gha_parser::job_builder::token_permissions_wire_json(
                    &policy.token_permissions,
                ),
            ),
        );
    }

    // Pre-allocate request ID atomically (no lock needed).
    let request_id = shared
        .state
        .next_request_id
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    agent_msg.request_id = request_id;

    // Mint tokens outside the lock (HMAC computation).
    let runtime_token = shared
        .state
        .mint_runtime_token(&agent_msg.plan.plan_id, &agent_msg.job_id);

    if let Some(snapshot) = workspace_snapshot {
        let redirected =
            redirect_primary_checkout(&mut agent_msg, snapshot, base_url, &runtime_token);
        if redirected > 0 {
            info!(
                %run_id,
                job = %job.id,
                %redirected,
                commit = %snapshot.commit_sha,
                source = ?snapshot.source,
                "Redirected primary checkout to immutable snapshot"
            );
            agent_msg.preloop_snapshot_commit = Some(snapshot.commit_sha.clone());
        }
        // Local-workspace runs test code the forge has never seen, so anything
        // hardcoding github.com has to be rewritten or the job fails fetching
        // its own sha. A cached remote checkout is the opposite case: the
        // commit exists upstream, the cache holds only that commit, and
        // rewriting the origin would break every legitimate fetch of anything
        // else.
        let repository = normalized_github
            .get("repository")
            .and_then(|value| value.as_str())
            .map(str::to_owned);
        if let (Some(repository), crate::snapshots::SnapshotSource::LocalWorkspace) =
            (repository, snapshot.source)
        {
            use base64::Engine as _;
            let credentials = base64::engine::general_purpose::STANDARD
                .encode(format!("x-access-token:{runtime_token}"));
            agent_msg.preloop_snapshot_origin_rewrite =
                Some(preloop_gha_protocol::azdo::SnapshotOriginRewrite {
                    snapshot_url: format!("{base_url}/{}", snapshot.repository),
                    forge_url: format!("https://github.com/{repository}"),
                    auth_header: format!("AUTHORIZATION: basic {credentials}"),
                });
        }
    }

    // Fork-restricted jobs never receive an OIDC grant: no request URL is
    // emitted here (and the broker restates it only for granted jobs), and
    // the `oidctoken` endpoint refuses via `id_token_grants`.
    let id_token_granted = policy.id_token_granted;
    if id_token_granted {
        let oidc_url = format!(
            "{}/runner/server/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken?api-version=2.0",
            base_url,
            agent_msg.plan.plan_id,
            agent_msg.job_id,
        );
        for endpoint in &mut agent_msg.resources.endpoints {
            if endpoint.name.eq_ignore_ascii_case("SystemVssConnection") {
                endpoint
                    .data
                    .insert("GenerateIdTokenUrl".to_owned(), oidc_url.clone());
            }
        }
    }

    // The PAT override (used when no GitHub App is configured) is a static,
    // repository-unscoped credential: embedding it in a fork-restricted job's
    // message would hand hostile code authority GitHub would never grant the
    // fork. Such jobs keep the local job-scoped runtime token, which
    // authenticates only against this control plane.
    let github_token = if policy.fork_restricted {
        runtime_token.clone()
    } else if let Some(pat) = github_token_override {
        // PAT mode: the token carries the PAT's OAuth scopes, not the
        // workflow's declared `permissions:`. `system.github.token.permissions`
        // keeps its documented map shape (the declared set, as the message
        // builder wrote it) so consumers that parse it are not surprised; the
        // token's real authority goes in its own variable, which the runner
        // prints inside the same `GITHUB_TOKEN Permissions` group.
        let (token, authority) = match pat {
            PatToken::Embed { token, scopes } => (token, pat_scopes_wire_value(&scopes)),
            // H3: unverifiable authority means no PAT is embedded. The job
            // keeps the runtime token, which authenticates only against this
            // control plane, so a step that needs GitHub fails at the point of
            // use rather than running with authority nobody could bound.
            PatToken::Withheld => (
                runtime_token.clone(),
                "withheld: PAT authority unverifiable; NOT the declared `permissions:` set"
                    .to_owned(),
            ),
        };
        agent_msg.variables.insert(
            "system.github.token.pat_scopes".to_owned(),
            preloop_gha_protocol::azdo::VariableValue::new(authority),
        );
        token
    } else {
        runtime_token.clone()
    };
    agent_msg.variables.insert(
        "system.github.token".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::secret(github_token.clone()),
    );
    agent_msg.variables.insert(
        "github_token".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::secret(github_token.clone()),
    );
    agent_msg.variables.insert(
        "actions_runner_allow_artifacts_file".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::new("false"),
    );
    agent_msg.variables.insert(
        "actions_self_repository".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::new("true"),
    );
    // The debug-worker token is deliberately not a job variable. An
    // official runner copies every `isSecret` variable into the `secrets`
    // context, so shipping it here would publish it to workflow YAML as
    // `${{ secrets['system.preloop.debug_worker_token'] }}`. The worker
    // acquires it instead over `POST /api/v1/debug/worker-token`, which
    // authenticates against this job's runtime token.
    agent_msg.variables.insert(
        "system.github.launch_endpoint".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::new(base_url),
    );
    agent_msg.variables.insert(
        "system.github.results_endpoint".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::new(base_url),
    );
    agent_msg.variables.insert(
        "system.orchestrationId".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::new(orchestration_id(
            &agent_msg.plan.plan_id,
            &job.base_id,
            job.matrix_index,
        )),
    );
    // GitHub's dispatcher sets `github.job` from the `system.github.job`
    // variable (official runner `ExecutionContext.cs` reads exactly this key;
    // the docs say the property is "set by the Actions runner"). Without it
    // `${{ github.job }}` and `GITHUB_JOB` are empty on the official-runner
    // path and every matrix cell reports the same blank job id.
    agent_msg.variables.insert(
        "system.github.job".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::new(job.base_id.clone()),
    );
    // Cache v2 switch. GitHub's server sends the `actions_uses_cache_service_v2`
    // feature variable (golden capture: `true`); the official runner's node/
    // container handlers turn it into the `ACTIONS_CACHE_SERVICE_V2` step env
    // (`NodeScriptActionHandler.cs:76-78`). Without it, actions/cache@v4 falls
    // back to the v1 endpoints — which aksh also serves, but v2 is the modern
    // path and the one the golden exercises.
    agent_msg.variables.insert(
        "actions_uses_cache_service_v2".to_owned(),
        preloop_gha_protocol::azdo::VariableValue::new("true"),
    );
    // `fileTable` names every workflow file this job's tokens can reference,
    // caller first. A job inlined from a reusable workflow contributes a
    // second entry and its tokens carry `file: 2` (see
    // `job_builder::job_file_id`) — matching GitHub, whose callee jobs emit
    // `[caller.yml, owner/repo/path.yml@sha]` and index the callee.
    agent_msg.file_table = vec![workflow_path.to_owned()];
    if let Some(callee) = reusable_file_table_entry(job) {
        agent_msg.file_table.push(callee);
    }
    if let Some(preloop_gha_protocol::azdo::PipelineContextData::Dict(job_dict)) =
        agent_msg.context_data.get_mut("job")
    {
        job_dict.insert(
            "check_run_id".to_owned(),
            preloop_gha_protocol::azdo::PipelineContextData::Number(0.0),
        );
        job_dict.insert(
            "workflow_ref".to_owned(),
            preloop_gha_protocol::azdo::PipelineContextData::String(workflow_ref.to_owned()),
        );
        job_dict.insert(
            "workflow_sha".to_owned(),
            preloop_gha_protocol::azdo::PipelineContextData::String(sha.to_owned()),
        );
        job_dict.insert(
            "workflow_repository".to_owned(),
            preloop_gha_protocol::azdo::PipelineContextData::String(submission.repository.clone()),
        );
        job_dict.insert(
            "workflow_file_path".to_owned(),
            preloop_gha_protocol::azdo::PipelineContextData::String(workflow_path.to_owned()),
        );
    }
    agent_msg.enable_debugger = submission.enable_debugger;
    agent_msg.debugger_welcome_message = submission.debugger_welcome_message.clone();
    if submission.enable_debugger || submission.preserve_on_failure {
        agent_msg.preloop_debug_run_id = Some(run_id.to_string());
        agent_msg.preloop_debug_transport = Some("local".to_string());
        if submission.enable_debugger {
            // The runner refuses to start the debugger without a valid
            // DebuggerTunnelInfo (id/cluster/host_token non-empty, port != 0).
            // In local server-proxy mode the relay fields are placeholders;
            // only `port` is real — the runner's WebSocketDapBridge binds it
            // so the engine's `/api/v1/runs/{id}/debug` proxy can connect.
            // 4711 is preloop_dap::DAP_TUNNEL_PORT (official default).
            agent_msg.debugger_tunnel = Some(preloop_gha_protocol::DebuggerTunnelInfo {
                tunnel_id: "local".to_string(),
                cluster_id: "local".to_string(),
                host_token: "local".to_string(),
                port: 4711,
            });
        }
    }
    let oidc_ctx = OidcJobContext {
        environment: job.oidc_environment.clone(),
        job_workflow_ref: job.oidc_job_workflow_ref.clone(),
        job_workflow_sha: job.workflow_sha.clone(),
    };
    let job_request = TaskAgentJobRequestRecord {
        request_id,
        run_id,
        job_id: job.id.clone(),
        agent_job_id: agent_msg.job_id,
        plan_id: agent_msg.plan.plan_id.clone(),
        plan_type: agent_msg.plan.plan_type.clone(),
        timeline_id: agent_msg.timeline.id,
        result: None,
        locked_until: agent_request_locked_until(),
        claimed_at: None,
        owner_runner_id: None,
        started_at: None,
        last_renewed_at: None,
        timeout_triggered: false,
        debug_token_issued: false,
    };
    let github_token_request = shared
        .state
        .github_app
        .as_ref()
        .map(|_| GitHubTokenRequest {
            repository: submission.repository.clone(),
            permissions: policy.app_permissions,
            declared: job.permissions.is_some(),
            untrusted: policy.fork_restricted,
        });

    Ok(BuiltJobArtifacts {
        agent_msg,
        request_id,
        job_request,
        id_token_granted,
        oidc_ctx,
        github_token_request,
    })
}

/// The step records of a job's most recent attempt.
///
/// Attempts are ordered by `request_id`, which is a monotonic allocation, so
/// the highest one is the newest dispatch. `None` when the job was never
/// dispatched (skipped or cancelled before a request was built).
pub fn latest_attempt_steps(
    inner: &crate::state::InnerState,
    run_id: RunId,
    job_id: &JobId,
) -> Option<Vec<StepRecord>> {
    let agent_job_id = inner
        .job_requests
        .values()
        .filter(|request| request.run_id == run_id && request.job_id == *job_id)
        .max_by_key(|request| request.request_id)
        .map(|request| request.agent_job_id)?;
    let mut steps = inner.job_steps.get(&agent_job_id).cloned()?;
    // The stored vector is seeded-then-appended, so it is not execution order.
    StepRecord::sort_execution_order(&mut steps);
    Some(steps)
}

/// Project a stored run into its API shape.
///
/// Shared by the single-run and list endpoints. Step records live in the
/// attempt-scoped manifest rather than in the stored run, so a caller that
/// clones `inner.runs` directly returns empty step arrays — which is exactly
/// what the list endpoint did.
pub fn project_run(inner: &crate::state::InnerState, mut run: RunRecord) -> RunRecord {
    let run_id = run.run_id;

    // GitHub's run record shows a gate-passed reusable caller only as its
    // callee jobs: once the subtree is materialized, the caller entry leaves
    // the visible job set. Gate-failed callers never materialize and stay as
    // exactly one (skipped) entry.
    let expanded_callers: std::collections::BTreeSet<String> = run
        .reusable_calls
        .iter()
        .filter(|(_, call)| !call.inner_job_ids.is_empty())
        .map(|(caller_id, _)| caller_id.clone())
        .collect();
    run.jobs
        .retain(|job_id, _| !expanded_callers.contains(&job_id.0));

    // Project with GitHub display names (evaluated `name:`, `caller / callee`
    // separator), and hydrate each job's steps from its latest attempt's
    // manifest. A job skipped or cancelled before dispatch never got a
    // request message, so it has no manifest and shows an empty step list.
    let existing = std::mem::take(&mut run.jobs_list);
    run.jobs_list = run
        .jobs
        .iter()
        .map(|(job_id, status)| {
            let name = run
                .job_names
                .get(job_id)
                .cloned()
                .unwrap_or_else(|| job_id.0.clone());
            let mut detail = existing
                .iter()
                .find(|detail| detail.job_id == job_id.0)
                .cloned()
                .unwrap_or(JobDetail {
                    job_id: job_id.0.clone(),
                    name: name.clone(),
                    conclusion: status_string(*status),
                    steps: Vec::new(),
                    annotations: Vec::new(),
                });
            detail.job_id = job_id.0.clone();
            detail.name = name;
            detail.conclusion = status_string(*status);
            // Steps live in the attempt-scoped manifest, so the run record
            // shows the newest attempt: a retry supersedes what the previous
            // dispatch reported.
            if let Some(manifest) = latest_attempt_steps(inner, run_id, job_id) {
                detail.steps = manifest;
            }
            detail
        })
        .collect();

    run
}

pub async fn get_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let run = inner
        .runs
        .get(&run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    Ok(Json(project_run(&inner, run)))
}

/// Browser-safe status page linked from GitHub Check Runs.
///
/// This deliberately projects only execution metadata. The native run response
/// remains bearer-protected because it contains the submitted event payload and
/// secret names.
pub async fn get_public_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<axum::response::Html<String>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let run = inner
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;

    let jobs = run
        .jobs
        .iter()
        .map(|(job, status)| {
            format!(
                "<li><code>{}</code> <strong>{}</strong></li>",
                escape_html(&job.0),
                escape_html(&status_string(*status))
            )
        })
        .collect::<String>();
    let status = escape_html(&status_string(run.status));
    let workflow = escape_html(&run.workflow_path_str);
    let id = escape_html(&run.run_id.to_string());
    let html = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
         <meta name=\"robots\" content=\"noindex,nofollow\">\
         <title>Preloop run {id}</title></head><body>\
         <main><h1>Preloop run</h1><p><code>{id}</code></p>\
         <p>Workflow: <code>{workflow}</code></p>\
         <p>Status: <strong>{status}</strong></p><h2>Jobs</h2><ul>{jobs}</ul>\
         </main></body></html>"
    );
    Ok(axum::response::Html(html))
}

fn escape_html(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

#[derive(Debug, Deserialize)]
pub struct ListRunsQuery {
    #[serde(default)]
    workflow: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    event: Option<String>,
    #[serde(default, deserialize_with = "deserialize_limit")]
    limit: Option<usize>,
}

fn deserialize_limit<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<usize>, D::Error> {
    Option::<usize>::deserialize(deserializer)
}

pub async fn list_runs(
    State(shared): State<Arc<SharedState>>,
    Query(query): Query<ListRunsQuery>,
) -> Result<Json<Vec<RunRecord>>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let limit = query.limit.unwrap_or(50).min(200);

    let mut runs: Vec<RunRecord> = inner
        .runs
        .values()
        .filter(|run| {
            if let Some(workflow) = &query.workflow {
                if !run.workflow_path_str.contains(workflow) {
                    return false;
                }
            }
            if let Some(status) = &query.status {
                let run_status = serde_json::to_value(run.status)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default();
                if run_status != *status {
                    return false;
                }
            }
            if let Some(event) = &query.event {
                if run.event != *event {
                    return false;
                }
            }
            true
        })
        .cloned()
        .collect();
    runs.sort_by(|a, b| {
        a.status
            .is_terminal()
            .cmp(&b.status.is_terminal())
            .then_with(|| {
                let a_time = a.completed_at.or(a.started_at).unwrap_or(a.created_at);
                let b_time = b.completed_at.or(b.started_at).unwrap_or(b.created_at);
                b_time.cmp(&a_time)
            })
    });
    runs.truncate(limit);
    let runs = runs
        .into_iter()
        // Same projection as the single-run endpoint: steps live in the
        // attempt manifest, so cloning the stored run alone returns empty
        // step arrays.
        .map(|run| project_run(&inner, run))
        .collect();

    Ok(Json(runs))
}

/// Optional filters for `GET /api/v1/runs/:run_id/logs`.
///
/// Both are absent for the historical whole-run behavior, so an unfiltered
/// request still returns every job's log merged in request order.
#[derive(Debug, Default, Deserialize)]
pub struct RunLogsQuery {
    /// Workflow job key (`job_id`) or the agent job UUID. Matched exactly as
    /// the live-log feed matches it, so the same value works for both.
    #[serde(default)]
    job: Option<String>,
    /// 1-based index into the job's step logs, in execution order. Mirrors
    /// `preloop debug --from`, which also counts user-visible steps from 1.
    #[serde(default)]
    step: Option<usize>,
}

/// Files uploaded for one job's individual steps.
///
/// `all` is every uploaded step blob, for an unfiltered whole-job read.
/// `workflow` is the manifest's declared steps in workflow order, each mapped
/// to its blob (`None` when that step produced no log). Synthetic runner steps
/// appear in `all` but never in `workflow`, which is what keeps "Set up job"
/// and `Post <action>` out of `--step` numbering.
struct StepLogs {
    all: Vec<std::path::PathBuf>,
    workflow: Option<Vec<Option<std::path::PathBuf>>>,
}

/// One job's log material, in execution order.
///
/// The variants are the three tiers `get_run_logs` already resolved inline;
/// naming them is what lets `?step=` reject the one tier that cannot answer
/// it instead of silently returning the whole job.
enum JobLogs {
    /// The runner uploaded one merged log. Step boundaries are not recoverable
    /// from it, so `?step=` cannot be honored.
    Merged(Vec<u8>),
    /// Per-step log files, with a workflow-order view for `?step=`.
    Steps(StepLogs),
    /// Nothing on disk yet: in-memory console blocks for a job still running,
    /// already ordered by numeric console log id (one per step).
    Live(Vec<Vec<u8>>),
}

/// Resolve one job's logs through the tier fallback.
///
/// Unfiltered requests prefer the merged upload because it is the runner's
/// authoritative whole-job representation. A step-filtered request prefers
/// individual step blobs when both are present; the merged upload has no
/// recoverable boundaries and is only used to produce the explicit 409.
///
/// `workflow_step_ids` is the attempt's declared-step order, taken from the
/// manifest built out of the job request message. There is deliberately no
/// filesystem fallback: modification time records when a blob landed, not when
/// a step ran, so ordering by it returned the wrong step for out-of-order or
/// same-timestamp uploads.
async fn resolve_job_logs(
    results_dir: &std::path::Path,
    fallback_blocks: Vec<Vec<u8>>,
    workflow_step_ids: Option<&[String]>,
    // Every id the attempt's manifest knows, in execution order, used only to
    // order the whole-job concatenation.
    execution_step_ids: Option<&[String]>,
    prefer_steps: bool,
) -> Result<JobLogs, ApiError> {
    let merged = match tokio::fs::read(results_dir.join("job-logs.txt")).await {
        Ok(contents) => Some(contents),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(ApiError::internal(format!(
                "failed to read run log `{}`: {error}",
                results_dir.join("job-logs.txt").display()
            )));
        }
    };
    if !prefer_steps {
        if let Some(contents) = merged {
            return Ok(JobLogs::Merged(contents));
        }
    }

    let mut step_files: Vec<(String, std::path::PathBuf)> = Vec::new();
    match tokio::fs::read_dir(results_dir).await {
        Ok(mut entries) => {
            while let Some(entry) = entries.next_entry().await.map_err(|error| {
                ApiError::internal(format!(
                    "failed to enumerate result logs `{}`: {error}",
                    results_dir.display()
                ))
            })? {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                let Some(step_id) = name
                    .strip_prefix("step-")
                    .and_then(|name| name.strip_suffix(".txt"))
                else {
                    continue;
                };
                let step_id = step_id.to_owned();
                let metadata = entry.metadata().await.map_err(|error| {
                    ApiError::internal(format!(
                        "failed to inspect result log `{}`: {error}",
                        entry.path().display()
                    ))
                })?;
                if !metadata.is_file() {
                    continue;
                }
                step_files.push((step_id, entry.path()));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(ApiError::internal(format!(
                "failed to enumerate result logs `{}`: {error}",
                results_dir.display()
            )));
        }
    }
    // Concatenation order comes from the manifest, which knows what ran when.
    // A step id is a v4 UUID, so sorting by it emitted the whole-job log in
    // random order; blobs the manifest does not know keep a stable tail.
    let position = |id: &str| {
        execution_step_ids
            .and_then(|ids| ids.iter().position(|known| known == id))
            .unwrap_or(usize::MAX)
    };
    step_files.sort_by(|left, right| {
        position(&left.0)
            .cmp(&position(&right.0))
            .then_with(|| left.0.cmp(&right.0))
    });

    if !step_files.is_empty() {
        let workflow = workflow_step_ids.filter(|ids| !ids.is_empty()).map(|ids| {
            ids.iter()
                .map(|id| {
                    step_files
                        .iter()
                        .find(|(file_id, _)| file_id == id)
                        .map(|(_, path)| path.clone())
                })
                .collect()
        });
        return Ok(JobLogs::Steps(StepLogs {
            all: step_files.into_iter().map(|(_, path)| path).collect(),
            workflow,
        }));
    }

    // A step query with only a merged upload must be rejected by append_step;
    // do not silently turn it into a whole-job response.
    if let Some(contents) = merged {
        return Ok(JobLogs::Merged(contents));
    }
    Ok(JobLogs::Live(fallback_blocks))
}

/// Append every step of a job, in upload/execution order.
async fn append_all(logs: JobLogs, merged: &mut Vec<u8>) -> Result<(), ApiError> {
    match logs {
        JobLogs::Merged(contents) => merged.extend_from_slice(&contents),
        JobLogs::Steps(step_logs) => {
            for path in step_logs.all {
                let contents = tokio::fs::read(&path).await.map_err(|error| {
                    ApiError::internal(format!(
                        "failed to read result log `{}`: {error}",
                        path.display()
                    ))
                })?;
                merged.extend_from_slice(&contents);
            }
        }
        JobLogs::Live(blocks) => {
            for block in blocks {
                merged.extend_from_slice(&block);
            }
        }
    }
    Ok(())
}

/// Append exactly the `step`th (1-based) workflow step of a job.
async fn append_step(
    logs: JobLogs,
    step: usize,
    job_label: &str,
    merged: &mut Vec<u8>,
) -> Result<(), ApiError> {
    if step == 0 {
        return Err(ApiError::bad_request(
            "step is 1-based; use `--step 1` for the first step",
        ));
    }
    let index = step - 1;
    match logs {
        // Refusing beats guessing: the merged upload has no step boundaries,
        // so any answer here would be the whole job wearing a step's name.
        JobLogs::Merged(_) => Err(ApiError::conflict(format!(
            "job `{job_label}` reported one merged log, which carries no step \
             boundaries; re-request without `step` for the whole job log"
        ))),
        JobLogs::Steps(step_logs) => {
            // No manifest means no declared-step order to index. The uploads
            // on disk are not a substitute: they include synthetic runner
            // steps and arrive in upload order, so indexing them returned a
            // neighbouring step's log under the requested step's name.
            let Some(workflow) = step_logs.workflow else {
                return Err(ApiError::conflict(format!(
                    "job `{job_label}` has no recorded workflow-step order, so step \
                     {step} cannot be identified; re-request without `step` for the \
                     whole job log"
                )));
            };
            let total = workflow.len();
            let path = workflow.get(index).cloned().flatten();
            let Some(path) = path else {
                if index < total {
                    // A valid workflow step is allowed to have no log blob.
                    return Ok(());
                }
                return Err(ApiError::not_found(format!(
                    "job `{job_label}` has {total} steps; step {step} is out of range"
                )));
            };
            let contents = tokio::fs::read(&path).await.map_err(|error| {
                ApiError::internal(format!(
                    "failed to read result log `{}`: {error}",
                    path.display()
                ))
            })?;
            merged.extend_from_slice(&contents);
            Ok(())
        }
        // In-memory console blocks are keyed by the runner's numeric log id,
        // which counts every record it opened — `Set up job` included — so
        // indexing them returns setup output for `--step 1`. That is the
        // numbering error this whole path exists to remove, so refuse instead
        // of reproducing it for a job whose blobs have not landed yet.
        JobLogs::Live(_) => Err(ApiError::conflict(format!(
            "job `{job_label}` has not uploaded per-step logs yet, so step {step} cannot be \
             identified; re-request without `step` for the output so far"
        ))),
    }
}

pub async fn get_run_logs(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    Query(query): Query<RunLogsQuery>,
) -> Result<Response, ApiError> {
    let (state_dir, sources) = {
        let inner = shared.state.inner.lock().await;
        if !inner.runs.contains_key(&run_id) {
            return Err(ApiError::not_found("run not found"));
        }

        let mut requests: Vec<&TaskAgentJobRequestRecord> = inner
            .job_requests
            .values()
            .filter(|request| request.run_id == run_id)
            .collect();
        requests.sort_by_key(|request| request.request_id);

        if let Some(job) = &query.job {
            // Same matching rule as the live-log feed: workflow job key or
            // agent job UUID, so one value works across both surfaces.
            requests.retain(|request| {
                request.job_id.0 == *job || request.agent_job_id.to_string() == *job
            });
            if requests.is_empty() {
                return Err(ApiError::not_found(format!(
                    "job `{job}` not found in this run"
                )));
            }
        } else if query.step.is_some() && requests.len() > 1 {
            // Numbering restarts per job, so an unqualified step in a
            // multi-job run names more than one thing.
            let jobs: Vec<&str> = requests
                .iter()
                .map(|request| request.job_id.0.as_str())
                .collect();
            return Err(ApiError::bad_request(format!(
                "`step` needs `job` when a run has {} jobs: {}",
                jobs.len(),
                jobs.join(", ")
            )));
        }

        let sources = requests
            .into_iter()
            .map(|request| {
                let prefix = format!("{}/", request.plan_id);
                let mut blocks: Vec<(&str, &[u8])> = inner
                    .logs
                    .iter()
                    .filter_map(|(key, value)| {
                        key.strip_prefix(&prefix)
                            .map(|log_id| (log_id, value.as_slice()))
                    })
                    .collect();
                blocks.sort_by(|(left, _), (right, _)| {
                    match (left.parse::<u64>(), right.parse::<u64>()) {
                        (Ok(left), Ok(right)) => left.cmp(&right),
                        (Ok(_), Err(_)) => std::cmp::Ordering::Less,
                        (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
                        (Err(_), Err(_)) => left.cmp(right),
                    }
                });
                // The attempt's own manifest, keyed by the agent job id that
                // also names this request's results directory. The broker
                // message is deliberately not consulted — it is broker
                // delivery state that a restart or a retirement can drop,
                // while the manifest is run state.
                //
                // Two views: declared steps in workflow order decide `--step`,
                // because a synthetic "Set up job" record must not occupy a
                // slot; every id in execution order decides the whole-job
                // concatenation, where synthetic output belongs in place.
                let manifest = inner.job_steps.get(&request.agent_job_id);
                let workflow_step_ids = manifest
                    .map(|records| {
                        StepRecord::workflow_steps(records)
                            .into_iter()
                            .map(|step| step.id.clone())
                            .collect::<Vec<_>>()
                    })
                    .filter(|ids| !ids.is_empty());
                let execution_step_ids = manifest
                    .map(|records| {
                        let mut ordered = records.clone();
                        StepRecord::sort_execution_order(&mut ordered);
                        ordered.into_iter().map(|step| step.id).collect::<Vec<_>>()
                    })
                    .filter(|ids| !ids.is_empty());
                (
                    request.plan_id.clone(),
                    request.agent_job_id.to_string(),
                    request.job_id.0.clone(),
                    blocks
                        .into_iter()
                        .map(|(_, block)| block.to_vec())
                        .collect::<Vec<_>>(),
                    workflow_step_ids,
                    execution_step_ids,
                )
            })
            .collect::<Vec<_>>();
        (shared.state.state_dir.clone(), sources)
    };

    let mut merged = Vec::new();
    for (
        plan_id,
        agent_job_id,
        job_label,
        fallback_blocks,
        workflow_step_ids,
        execution_step_ids,
    ) in sources
    {
        let results_dir = state_dir
            .join("replay")
            .join("results")
            .join(plan_id)
            .join(agent_job_id);
        let logs = resolve_job_logs(
            &results_dir,
            fallback_blocks,
            workflow_step_ids.as_deref(),
            execution_step_ids.as_deref(),
            query.step.is_some(),
        )
        .await?;
        match query.step {
            Some(step) => append_step(logs, step, &job_label, &mut merged).await?,
            None => append_all(logs, &mut merged).await?,
        }
    }

    Ok(Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(merged))
        .expect("static run log response"))
}

pub async fn cancel_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunRecord>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    if !inner.runs.contains_key(&run_id) {
        return Err(ApiError::not_found("run not found"));
    }
    let cancellation_count =
        cancel_run_inner(&mut inner, run_id, None /* no concurrency reason */);
    let cancelled_jobs = {
        let run = inner
            .runs
            .get_mut(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        runtime_scheduling::finalize_run_if_complete(run);
        run.jobs
            .iter()
            .filter(|(_, status)| **status == ExecutionStatus::Cancelled)
            .map(|(job_id, _)| job_id.clone())
            .collect::<Vec<_>>()
    };
    let record = inner
        .runs
        .get(&run_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    shared
        .state
        .queue_depth
        .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
    runtime_scheduling::sync_next_job_labels(&inner, &shared.state.next_job_runs_on);
    drop(inner);
    if cancellation_count > 0 {
        shared.state.message_notify.notify_waiters();
    }
    shared
        .state
        .emit(NdjsonEvent::RunStatus {
            run_id,
            status: ExecutionStatus::Cancelled,
            reason: None,
        })
        .await;
    // The cancellation is already committed in memory and persisted above.
    // Reporting it to GitHub is one PATCH per job, which can take seconds
    // each behind a busy engine. Awaiting it here let a client that timed out
    // drop the handler midway: the run stayed cancelled while the remaining
    // check runs were never updated and sat `queued` on GitHub forever.
    let reporter = Arc::clone(&shared);
    tokio::spawn(async move {
        for job_id in cancelled_jobs {
            crate::github::report_check_run_completed(
                &reporter,
                run_id,
                &job_id,
                ExecutionStatus::Cancelled,
            )
            .await;
        }
    });
    Ok(Json(record))
}

/// Request body for approving a pending environment protection gate.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ApproveJobRequest {
    /// Optional operator note recorded with the approval (audit trail).
    pub note: Option<String>,
}

/// Response for an environment approval.
#[derive(Debug, Serialize, ToSchema)]
pub struct ApproveJobResponse {
    pub run_id: String,
    pub job_id: String,
    pub approvals: usize,
    pub required: u32,
    /// Whether the approval gate is now satisfied (the job may proceed).
    pub satisfied: bool,
}

/// Record one approval for a job waiting on its environment's
/// required-reviewer gate (`POST /api/v1/runs/:run_id/jobs/:job_id/approve`).
/// Requires the system (native) bearer token: preloop has no user
/// identities, so the approver is whoever holds the operator credential.
/// For a single-operator server this is a deliberate confirmation step, not
/// a second human — it stops a compromised workflow or a misclicked
/// re-run from deploying without an explicit go-ahead.
pub async fn approve_job(
    State(shared): State<Arc<SharedState>>,
    Path((run_id, job_id)): Path<(RunId, JobId)>,
    Json(body): Json<ApproveJobRequest>,
) -> Result<Json<ApproveJobResponse>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let run = inner
        .runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    if run
        .jobs
        .get(&job_id)
        .is_some_and(|status| status.is_terminal())
    {
        return Err(ApiError::conflict("job is already terminal"));
    }
    let repository = run.submission.repository.clone();
    let job = inner
        .pending_jobs
        .iter_mut()
        .find(|job| job.run_id == run_id && job.job_id == job_id)
        .ok_or_else(|| ApiError::not_found("job is not waiting in the scheduler"))?;
    let gate = job
        .environment_gate
        .as_mut()
        .filter(|gate| gate.approval_requested_at_unix_nanos.is_some())
        .ok_or_else(|| ApiError::conflict("job is not awaiting environment approval"))?;
    let env_name = match job.environment.as_ref() {
        Some(serde_json::Value::String(name)) => name.clone(),
        Some(serde_json::Value::Object(map)) => map
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        _ => String::new(),
    };
    let required = shared
        .state
        .environment_rules
        .get(&repository)
        .and_then(|envs| envs.get(&env_name))
        .map(|rule| rule.required_reviewers)
        .unwrap_or(0);
    // Fail closed on an expired window before recording anything.
    if let Some(requested_at) = gate.approval_requested_at_unix_nanos {
        if crate::models::now_unix_nanos().saturating_sub(requested_at)
            > crate::runtime_scheduling::ENVIRONMENT_APPROVAL_WINDOW_NANOS
        {
            crate::runtime_scheduling::promote_ready_jobs(
                &mut inner,
                &shared.state.environment_rules,
            );
            crate::runtime_scheduling::sync_next_job_labels(&inner, &shared.state.next_job_runs_on);
            if let Some(run) = inner.runs.get_mut(&run_id) {
                crate::runtime_scheduling::finalize_run_if_complete(run);
            }
            return Err(ApiError::conflict(
                "approval window expired; the job was failed closed",
            ));
        }
    }
    gate.approvals_unix_nanos
        .push(crate::models::now_unix_nanos());
    let approvals = gate.approvals_unix_nanos.len();
    let satisfied = required > 0 && (approvals as u32) >= required;
    tracing::info!(
        run_id = %run_id.0,
        job_id = %job_id.0,
        environment = env_name,
        approvals,
        required,
        note = body.note.as_deref().unwrap_or_default(),
        "environment approval recorded"
    );
    let outcome =
        crate::runtime_scheduling::promote_ready_jobs(&mut inner, &shared.state.environment_rules);
    shared
        .state
        .queue_depth
        .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
    crate::runtime_scheduling::sync_next_job_labels(&inner, &shared.state.next_job_runs_on);
    let promoted = outcome.promoted;
    drop(inner);
    if promoted > 0 {
        shared.state.message_notify.notify_waiters();
    }
    Ok(Json(ApproveJobResponse {
        run_id: run_id.0.to_string(),
        job_id: job_id.0.clone(),
        approvals,
        required,
        satisfied,
    }))
}
pub async fn rerun_run_inner(
    shared: &Arc<SharedState>,
    run_id: RunId,
    reused_check_run: Option<(JobId, u64)>,
) -> Result<RunAccepted, ApiError> {
    let submission = {
        let inner = shared.state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .map(|run| (*run.submission).clone())
            .ok_or_else(|| ApiError::not_found("run not found"))?
    };
    let accepted = submit_run_inner(shared, submission).await?;

    if let Some((job_id, check_run_id)) = reused_check_run.as_ref() {
        {
            let mut inner = shared.state.inner.lock().await;
            if let Some(run) = inner.runs.get_mut(&accepted.run_id) {
                if run.jobs.contains_key(job_id) {
                    run.job_check_run_ids.insert(job_id.clone(), *check_run_id);
                }
            }
        }
        // Same persistence obligation as `report_check_run_queued`: the
        // reused check id must survive a restart before the job's first
        // status event.
        shared
            .state
            .emit(preloop_gha_protocol::NdjsonEvent::CheckRunCreated {
                run_id: accepted.run_id,
            })
            .await;
    }
    crate::github::report_check_runs_for_run(shared, accepted.run_id, reused_check_run).await;
    Ok(accepted)
}

pub async fn rerun_run(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Json<RunAccepted>, ApiError> {
    rerun_run_inner(&shared, run_id, None).await.map(Json)
}

/// Request body for approving a run held by the fork-PR workflow policy.
#[derive(Debug, Deserialize, ToSchema)]
pub struct ApproveForkRequest {
    /// Optional operator note recorded with the approval (audit trail).
    #[serde(default)]
    pub note: Option<String>,
}

/// Response for a fork-PR approval.
#[derive(Debug, Serialize, ToSchema)]
pub struct ApproveForkResponse {
    pub run_id: String,
    /// Whether the run was awaiting fork approval and is now released.
    pub approved: bool,
}

/// Approve a run held by the fork-PR workflow policy
/// (`POST /api/v1/runs/:run_id/approve-fork`). Requires the system (native)
/// bearer token: preloop has no user identities, so the approver is whoever
/// holds the operator credential. For a single-operator server this is a
/// deliberate confirmation step, not a second human — it stops a fork PR
/// from executing code without an explicit go-ahead. A run not approved
/// within 24 hours of entering the hold fails closed.
pub async fn approve_fork(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
    Json(body): Json<ApproveForkRequest>,
) -> Result<Json<ApproveForkResponse>, ApiError> {
    let mut inner = shared.state.inner.lock().await;
    let run = inner
        .runs
        .get_mut(&run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    if run.status.is_terminal() {
        return Err(ApiError::conflict("run is already terminal"));
    }
    if !run.fork_approval_pending {
        return Err(ApiError::conflict("run is not awaiting fork approval"));
    }
    // Fail closed on an expired window before recording anything.
    if let Some(requested_at) = run.fork_approval_requested_at_unix_nanos {
        if crate::models::now_unix_nanos().saturating_sub(requested_at)
            > crate::fork_policy::FORK_APPROVAL_WINDOW_NANOS
        {
            let expired = crate::fork_policy::sweep_expired_fork_approvals(
                &mut inner,
                crate::models::now_unix_nanos(),
            );
            drop(inner);
            if !expired.is_empty() {
                shared.state.message_notify.notify_waiters();
            }
            return Err(ApiError::conflict(
                "approval window expired; the run was failed closed",
            ));
        }
    }
    let run = inner.runs.get_mut(&run_id).expect("run exists");
    run.fork_approval_pending = false;
    run.fork_approved_at_unix_nanos = Some(crate::models::now_unix_nanos());
    run.fork_approval_note = body.note.clone();
    tracing::info!(
        run_id = %run_id.0,
        note = body.note.as_deref().unwrap_or_default(),
        "fork-PR approval recorded; run released"
    );
    let outcome =
        crate::runtime_scheduling::promote_ready_jobs(&mut inner, &shared.state.environment_rules);
    shared
        .state
        .queue_depth
        .store(inner.queue.len(), std::sync::atomic::Ordering::Release);
    crate::runtime_scheduling::sync_next_job_labels(&inner, &shared.state.next_job_runs_on);
    let promoted = outcome.promoted;
    drop(inner);
    if promoted > 0 {
        shared.state.message_notify.notify_waiters();
    }
    Ok(Json(ApproveForkResponse {
        run_id: run_id.0.to_string(),
        approved: true,
    }))
}

/// Upper bound on how long an event stream waits for the next event.
///
/// A run that stalls must not pin a connection forever; clients reconnect and
/// re-receive the snapshot, so closing here costs only a round trip.
const EVENT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// NDJSON event feed for a run: a snapshot of everything so far, then live
/// events until the run reaches a terminal status.
///
/// Holding the response open is what keeps `preloop run` off a poll timer —
/// snapshot-and-close forced clients to re-request on an interval, which added
/// that interval to every run's wall clock.
pub async fn run_events(
    State(shared): State<Arc<SharedState>>,
    Path(run_id): Path<RunId>,
) -> Result<Response, ApiError> {
    // Subscribe before snapshotting so nothing emitted in between is lost. The
    // overlap can replay a line the snapshot already carried; clients
    // de-duplicate, and applying a status twice is idempotent.
    let receiver = shared.state.events.subscribe();

    let (snapshot, settled) = {
        let inner = shared.state.inner.lock().await;
        let run = inner
            .runs
            .get(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        let mut out = event_to_ndjson(&NdjsonEvent::RunStatus {
            run_id,
            status: run.status,
            reason: None,
        })?;
        for (job_id, status) in &run.jobs {
            out.push_str(&event_to_ndjson(&NdjsonEvent::JobStatus {
                run_id,
                job_id: job_id.clone(),
                status: *status,
                reason: None,
            })?);
        }
        if let Some(events) = inner.timeline_events.get(&run_id) {
            for event in events {
                out.push_str(&event_to_ndjson(event)?);
            }
        }
        (out, run.status.is_terminal())
    };

    let body = if settled {
        Body::from(snapshot)
    } else {
        let head = stream::once(async move { Ok::<Bytes, std::io::Error>(Bytes::from(snapshot)) });
        Body::from_stream(head.chain(live_run_events(run_id, receiver)))
    };

    Ok(Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(body)
        .expect("static response builder"))
}

/// Live tail of a run's events, ending once the run status turns terminal.
fn live_run_events(
    run_id: RunId,
    receiver: broadcast::Receiver<NdjsonEvent>,
) -> impl stream::Stream<Item = Result<Bytes, std::io::Error>> {
    stream::unfold(
        (receiver, false),
        move |(mut receiver, finished)| async move {
            if finished {
                return None;
            }
            // One deadline for the whole filtering loop, not one per `recv`.
            // The broadcast channel carries every run's events, so a per-`recv`
            // timeout is refreshed by traffic belonging to other runs and then
            // discarded by the run-id check below; on a busy server a stalled
            // run would hold its connection open forever. Anchoring the
            // deadline before the loop keeps "idle" meaning "nothing delivered
            // to *this* client", which is what the bound is for, and it reads
            // more directly than tracking whether the last event matched.
            let deadline = tokio::time::Instant::now() + EVENT_STREAM_IDLE_TIMEOUT;
            loop {
                let event = match tokio::time::timeout_at(deadline, receiver.recv()).await {
                    Ok(Ok(event)) => event,
                    // A lagging consumer has an incomplete stream. End it
                    // so the client reconnects and receives a fresh,
                    // authoritative snapshot before tailing again.
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => return None,
                    Ok(Err(broadcast::error::RecvError::Closed)) | Err(_) => return None,
                };
                if event.run_id() != run_id {
                    continue;
                }
                let Ok(line) = event_to_ndjson(&event) else {
                    continue;
                };
                let finished = event.terminal_run_status().is_some();
                return Some((Ok(Bytes::from(line)), (receiver, finished)));
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[test]
    fn orchestration_id_matches_github_format() {
        // Plain job: `{planId}.{jobId}.__default` (golden:
        // 49f720db-...hello.__default)
        assert_eq!(
            orchestration_id("49f720db-d368-4a3a-8b97-adbc8733aa79", "hello", None),
            "49f720db-d368-4a3a-8b97-adbc8733aa79.hello.__default"
        );
        // Matrix cells: 1-based index suffix (golden: build._1/_2/_3)
        assert_eq!(
            orchestration_id("37e6d806-40ab-4d76-92bd-7f6b0c91c002", "build", Some(2)),
            "37e6d806-40ab-4d76-92bd-7f6b0c91c002.build._2"
        );
        // The value must be a valid User-Agent product token: the official
        // runner inserts it into `ProductInfoHeaderValue`, which throws
        // FormatException on spaces (e.g. a display name like
        // "Run tests with system wide configuration").
        assert!(!orchestration_id("p", "j", None).contains(' '));
        assert!(!orchestration_id("p", "j", Some(1)).contains(' '));
    }

    #[tokio::test]
    async fn concurrent_webhook_replay_does_not_consume_run_number() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let shared = state.shared();
        let submission = preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n"
                .to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            workflow_path: Some(".github/workflows/build.yml".to_owned()),
            sha: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_owned(),
            ..Default::default()
        };

        let first = submit_run_inner_with_webhook_delivery(
            &shared,
            submission.clone(),
            Some("delivery-race"),
        );
        let second =
            submit_run_inner_with_webhook_delivery(&shared, submission, Some("delivery-race"));
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.run_id, second.run_id);
        assert_eq!(first.run_number, 1);
        assert_eq!(second.run_number, 1);

        let inner = state.inner.lock().await;
        assert_eq!(inner.runs.len(), 1);
        assert_eq!(
            inner
                .workflow_run_counters
                .get(".github/workflows/build.yml"),
            Some(&1),
            "a replay that reused the run must not advance the counter"
        );
    }
    /// GitHub ships `[caller.yml, owner/repo/path.yml@sha]` for a job inlined
    /// from a reusable workflow, and that job's tokens index the callee entry.
    /// Overwriting the table with the caller path alone erased the callee's
    /// provenance that `expand_reusable_call` had already resolved.
    #[test]
    fn reusable_file_table_entry_names_the_callee_workflow() {
        let mut job = preloop_gha_protocol::JobPlan {
            id: JobId("call/inner".to_owned()),
            base_id: "call/inner".to_owned(),
            name: "ci / build".to_owned(),
            runner_group: None,
            runs_on: vec!["ubuntu-latest".to_owned()],
            needs: Vec::new(),
            matrix: Default::default(),
            matrix_index: None,
            matrix_total: None,
            deferred_matrix: None,
            env: BTreeMap::new(),
            steps: Vec::new(),
            if_condition: None,
            fail_fast: true,
            continue_on_error: false,
            max_parallel: None,
            secrets_inherit: false,
            container: None,
            services: None,
            inputs: BTreeMap::new(),
            workflow_file: None,
            workflow_ref: None,
            workflow_sha: None,
            workflow_repository: None,
            secrets_map: BTreeMap::new(),
            job_outputs: BTreeMap::new(),
            oidc_id_token_granted: false,
            permissions: None,
            oidc_environment: None,
            oidc_job_workflow_ref: None,
            environment: None,
            defaults: Vec::new(),
            concurrency_group: None,
            concurrency_cancel_in_progress: None,
            concurrency_queue: None,
            reusable_call: None,
            timeout_minutes: None,
        };

        // A job defined in the caller contributes no second entry.
        assert_eq!(reusable_file_table_entry(&job), None);

        // Remote callee: `owner/repo/path@sha`, GitHub's shape.
        job.workflow_file = Some(".github/workflows/reusable-build.yml".to_owned());
        job.workflow_repository = Some("Bnjoroge1/conformance-v2337".to_owned());
        job.workflow_sha = Some("eafe8a907569a41d38fed3ffd9ed8302f247a920".to_owned());
        assert_eq!(
            reusable_file_table_entry(&job).as_deref(),
            Some(
                "Bnjoroge1/conformance-v2337/.github/workflows/reusable-build.yml@eafe8a907569a41d38fed3ffd9ed8302f247a920"
            )
        );

        // `normalize_reusable_path` stores remote uses as owner/repo/path.yml.
        job.workflow_file =
            Some("Bnjoroge1/conformance-v2337/.github/workflows/reusable-build.yml".to_owned());
        job.workflow_repository = Some("Bnjoroge1/conformance-v2337".to_owned());
        job.workflow_sha = Some("eafe8a907569a41d38fed3ffd9ed8302f247a920".to_owned());
        assert_eq!(
            reusable_file_table_entry(&job).as_deref(),
            Some(
                "Bnjoroge1/conformance-v2337/.github/workflows/reusable-build.yml@eafe8a907569a41d38fed3ffd9ed8302f247a920"
            )
        );
        // Local callee: no repository or sha of its own, so the path stands alone.
        job.workflow_file = Some(".github/workflows/reusable-build.yml".to_owned());
        job.workflow_repository = None;
        job.workflow_sha = None;
        assert_eq!(
            reusable_file_table_entry(&job).as_deref(),
            Some(".github/workflows/reusable-build.yml")
        );
    }
    /// The broadcast channel fans out every run's events, so a stalled run's
    /// stream sees — and discards — traffic it must not treat as liveness.
    /// Before the deadline was hoisted out of the filtering loop, that traffic
    /// refreshed the idle bound and the connection leaked for as long as the
    /// server stayed busy.
    #[tokio::test(start_paused = true)]
    async fn stalled_run_event_stream_ends_on_its_idle_deadline_despite_other_run_traffic() {
        let stalled = RunId::new();
        let noisy = RunId::new();
        let (sender, receiver) = broadcast::channel(64);
        let stream = live_run_events(stalled, receiver);
        tokio::pin!(stream);

        let started = tokio::time::Instant::now();
        // Emit for the other run more often than the idle bound while the clock
        // walks past it, which is the busy-server shape that hid the leak.
        let mut ended = false;
        for _ in 0..30 {
            sender
                .send(NdjsonEvent::RunStatus {
                    run_id: noisy,
                    status: ExecutionStatus::InProgress,
                    reason: None,
                })
                .expect("stream holds the receiver alive");
            // Polled rather than awaited on purpose: awaiting would let the
            // paused clock auto-advance to the next timer, which would end the
            // stream even under the per-event timeout this test rules out.
            match stream.next().now_or_never() {
                None => {}
                Some(None) => {
                    ended = true;
                    break;
                }
                Some(Some(_)) => panic!("another run's event must not be yielded to this stream"),
            }
            tokio::time::advance(EVENT_STREAM_IDLE_TIMEOUT / 3).await;
        }

        assert!(
            ended,
            "stalled stream stayed open while other runs kept the channel busy"
        );
        assert!(
            started.elapsed() < EVENT_STREAM_IDLE_TIMEOUT * 2,
            "stream outlived its idle deadline by more than a full bound"
        );
    }

    fn git_in(cwd: &std::path::Path, args: &[&str]) -> Vec<u8> {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .output()
            .expect("git runs in tests");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    /// A local PR run over a dirty tree must present the snapshot commit that
    /// carries the dirty tree as `pull_request.head.sha`, with `base.sha`
    /// pointing at the base its changes are measured against. Both SHAs live
    /// in the snapshot store, so changed-file actions
    /// (`dorny/paths-filter`, `tj-actions/changed-files`) can actually diff
    /// them; the real workspace HEAD is neither in the store nor the tree
    /// carrying the uncommitted changes.
    #[tokio::test]
    async fn local_pull_request_head_is_the_snapshot_commit_carrying_the_dirty_tree() {
        let temp = tempfile::tempdir().unwrap();
        let state_dir = temp.path().join("state");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        git_in(&workspace, &["init", "-q", "-b", "main"]);
        git_in(&workspace, &["config", "user.email", "test@example.com"]);
        git_in(&workspace, &["config", "user.name", "Test"]);
        std::fs::write(workspace.join("file.txt"), "one\n").unwrap();
        git_in(&workspace, &["add", "file.txt"]);
        git_in(&workspace, &["commit", "-qm", "initial"]);
        let workspace_head = String::from_utf8(git_in(&workspace, &["rev-parse", "HEAD"])).unwrap();
        // Dirty the tree: the uncommitted change must show up in the
        // head..base diff, which it cannot when head is the workspace HEAD.
        std::fs::write(workspace.join("file.txt"), "two (uncommitted)\n").unwrap();

        let mut state = AppState::new(state_dir.clone()).await.unwrap();
        state.local_workspace = Some(workspace.clone());
        let shared = std::sync::Arc::new(SharedState {
            state: state.clone(),
            shutdown: CancellationToken::new(),
        });

        let submission = preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: "on: pull_request\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n".to_owned(),
            event: "pull_request".to_owned(),
            payload: serde_json::json!({
                "action": "opened",
                "number": 7,
                "pull_request": {
                    "head": { "ref": "feature", "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3" },
                    "base": { "ref": "main", "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2" }
                }
            }),
            repository: "owner/repo".to_owned(),
            ..Default::default()
        };
        let accepted = submit_run_inner(&shared, submission).await.unwrap();

        let inner = state.inner.lock().await;
        let run = inner.runs.get(&accepted.run_id).expect("run is recorded");
        let snapshot = run
            .workspace_snapshot
            .as_ref()
            .expect("local runs must create a workspace snapshot");
        let pr = run.submission.payload["pull_request"].as_object().unwrap();
        let head_sha = pr["head"]["sha"].as_str().unwrap().to_owned();
        let base_sha = pr["base"]["sha"].as_str().unwrap().to_owned();

        assert_eq!(
            head_sha, snapshot.commit_sha,
            "PR head must be the snapshot commit carrying the dirty tree"
        );
        assert_ne!(
            head_sha,
            snapshot.head_sha.as_deref().unwrap(),
            "PR head must not be the real workspace HEAD"
        );
        assert_ne!(
            head_sha,
            workspace_head.trim(),
            "head must not be the workspace HEAD"
        );
        assert_eq!(
            base_sha,
            snapshot.before_sha.as_deref().unwrap(),
            "PR base must be the base the dirty tree is measured against"
        );
        assert_ne!(
            base_sha, head_sha,
            "a dirty tree must diff against its base"
        );

        // Both endpoints resolve inside the snapshot store, and the diff names
        // exactly the uncommitted change.
        let snapshot_repository = state_dir.join(&snapshot.repository);
        let changed = git_in(
            &snapshot_repository,
            &["diff", "--name-only", &base_sha, &head_sha],
        );
        assert_eq!(
            String::from_utf8(changed).unwrap().trim(),
            "file.txt",
            "changed-file actions must see the dirty tree's changes"
        );
    }

    /// A PAT stored by `preloop setup github --via pat` must authenticate
    /// remote reusable-workflow resolution, not just queued job tokens:
    /// private `uses: owner/repo/...` references fetch through the same
    /// credential the config file holds.
    #[tokio::test]
    async fn remote_reusable_workflow_resolution_uses_config_backed_pat() {
        use axum::body::Body;
        use axum::http::{header, HeaderMap, Method, Request, StatusCode};
        use axum::routing::get;
        use axum::Json;
        use tower::ServiceExt;

        // A mock GitHub API that REQUIRES the engine credential, exactly like
        // a private repository: without a bearer token it answers 404.
        let seen_auth = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
        let callee_yaml =
            "on: workflow_call\njobs:\n  callee:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo callee\n";
        let encoded = base64::engine::general_purpose::STANDARD.encode(callee_yaml.as_bytes());
        let mock = axum::Router::new()
            .route(
                "/repos/:owner/:repo/contents/*path",
                get({
                    let seen = seen_auth.clone();
                    move |headers: HeaderMap| async move {
                        let auth = headers
                            .get(header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned);
                        *seen.lock().unwrap() = auth.clone();
                        if auth.is_some() {
                            Json(serde_json::json!({
                                "content": encoded,
                                "encoding": "base64",
                            }))
                            .into_response()
                        } else {
                            (
                                StatusCode::NOT_FOUND,
                                Json(serde_json::json!({"message": "Not Found"})),
                            )
                                .into_response()
                        }
                    }
                }),
            )
            .route(
                "/repos/:owner/:repo/commits/:git_ref",
                get(|| async move {
                    Json(serde_json::json!({"sha": "c0ffee0000000000000000000000000000000000"}))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api_base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
        // Held for the whole test: `PRELOOP_GITHUB_API_URL` is process-global.
        let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
        std::env::set_var("PRELOOP_GITHUB_API_URL", api_base);

        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("config.toml");
        std::fs::write(&config_path, "[github]\npat = \"ghp_config_pat_value\"\n").unwrap();
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
            .await
            .unwrap();
        let app = crate::app(state, CancellationToken::new());

        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/runs")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {DEFAULT_PRELOOP_SYSTEM_TOKEN}"),
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "workflow_yaml": "on: push\njobs:\n  call:\n    uses: acme/private/.github/workflows/callee.yml@main\n",
                            "event": "push",
                            "repository": "owner/repo",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "submission must succeed when the config PAT authenticates the remote reusable fetch"
        );
        let received = seen_auth.lock().unwrap().clone();
        assert_eq!(
            received.as_deref(),
            Some("Bearer ghp_config_pat_value"),
            "remote reusable-workflow resolution must send the config-backed PAT"
        );

        std::env::remove_var("PRELOOP_GITHUB_API_URL");
    }

    /// Submit a run through the public API against a config-file registry.
    async fn submit_push_run(config_toml: &str, workflow_yaml: &str) -> (StatusCode, String) {
        use axum::body::Body;
        use axum::http::{header, Method, Request};
        use tower::ServiceExt;

        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("config.toml");
        std::fs::write(&config_path, config_toml).unwrap();
        let state = AppState::new_with_config(temp.path().to_path_buf(), config_path)
            .await
            .unwrap();
        let app = crate::app(state, CancellationToken::new());
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/runs")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {DEFAULT_PRELOOP_SYSTEM_TOKEN}"),
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "workflow_yaml": workflow_yaml,
                            "event": "push",
                            "repository": "owner/repo",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_default();
        (status, body)
    }

    const ENV_WORKFLOW: &str = "on: push\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    environment: production\n    steps:\n      - run: echo hi\n";

    /// M4: `environment:` is an unvalidated string. A workflow claiming an
    /// environment the operator never registered must fail closed — even
    /// when that environment has secrets configured (the pentest shape: env
    /// secret injected + OIDC `sub` asserting the unregistered environment).
    #[tokio::test]
    async fn unregistered_environment_rejects_run_submission() {
        use axum::http::StatusCode;
        let (status, body) = submit_push_run(
            "[env_secrets.\"owner/repo\".production]\nDEPLOY_KEY = \"env-secret\"\n",
            ENV_WORKFLOW,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "a job claiming an unregistered environment must fail closed, got: {body}"
        );
        assert!(
            body.contains("not registered"),
            "the rejection must name the missing registration, got: {body}"
        );
    }

    /// M4: an environment the operator registered in `[environments]` keeps
    /// working — the registry gates existence, not legitimate use.
    #[tokio::test]
    async fn registered_environment_accepts_run_submission() {
        use axum::http::StatusCode;
        let (status, body) = submit_push_run(
            "[environments]\n\"owner/repo\" = [\"production\"]\n[env_secrets.\"owner/repo\".production]\nDEPLOY_KEY = \"env-secret\"\n",
            ENV_WORKFLOW,
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "a job claiming a registered environment must be accepted, got: {body}"
        );
    }

    #[test]
    fn approve_fork_request_accepts_empty_object() {
        // The note is optional: `{}` must deserialize (the field was missing
        // #[serde(default)], so `{}` failed with "missing field `note`").
        let req: ApproveForkRequest = serde_json::from_str("{}").expect("{} must parse");
        assert!(req.note.is_none());
        let req: ApproveForkRequest =
            serde_json::from_str(r#"{"note":"lgtm"}"#).expect("note must parse");
        assert_eq!(req.note.as_deref(), Some("lgtm"));
    }

    /// The native `POST /api/v1/runs` endpoint, the scheduler, and the
    /// native rerun endpoint all call `submit_run_inner` directly, bypassing
    /// the webhook intake's per-event checks. Enforcement lives at this
    /// choke point so none of those paths can sidestep an enforced rule.
    async fn state_with_push_deny(
        temp: &std::path::Path,
        mode: crate::config::ProtectionMode,
    ) -> std::sync::Arc<crate::SharedState> {
        let mut state = crate::AppState::new(temp.to_path_buf()).await.unwrap();
        state.execution_protection = crate::config::ExecutionProtectionConfig {
            mode,
            event_rules: vec![crate::config::EventRule {
                event: "push".to_owned(),
                workflows: None,
                action: crate::config::PolicyRuleAction::Deny,
            }],
            actor_rules: vec![],
        };
        state.shared()
    }

    fn push_submission() -> preloop_gha_protocol::WorkflowSubmission {
        preloop_gha_protocol::WorkflowSubmission {
            workflow_yaml: "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hello\n"
                .to_owned(),
            event: "push".to_owned(),
            repository: "owner/repo".to_owned(),
            workflow_path: Some(".github/workflows/ci.yml".to_owned()),
            workflow_file: Some("ci.yml".to_owned()),
            actor: "alice".to_owned(),
            sha: "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_owned(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn submit_run_inner_denies_denied_event_in_enforce_mode() {
        let temp = tempfile::tempdir().unwrap();
        let shared =
            state_with_push_deny(temp.path(), crate::config::ProtectionMode::Enforce).await;
        let error = submit_run_inner(&shared, push_submission())
            .await
            .expect_err("a denied event must not submit in enforce mode");
        assert!(
            error.message().contains("execution protection"),
            "unexpected error: {}",
            error.message()
        );
        let inner = shared.state.inner.lock().await;
        assert!(
            inner.runs.is_empty(),
            "denied submission must not create a run"
        );
    }

    #[tokio::test]
    async fn submit_run_inner_allows_denied_event_in_evaluate_mode() {
        let temp = tempfile::tempdir().unwrap();
        let shared =
            state_with_push_deny(temp.path(), crate::config::ProtectionMode::Evaluate).await;
        let accepted = submit_run_inner(&shared, push_submission())
            .await
            .expect("evaluate mode must not block submission");
        let inner = shared.state.inner.lock().await;
        assert!(inner.runs.contains_key(&accepted.run_id));
    }
}
