use super::*;

/// Server configuration.
#[derive(Clone)]
pub struct ServerConfig {
    /// Address to bind.
    pub listen: SocketAddr,
    /// Consume the TCP listener passed by systemd socket activation.
    pub systemd_socket_activation: bool,
    /// Optional Unix domain socket path to bind.
    pub unix_socket: Option<PathBuf>,
    /// State directory for cache/artifacts and future durable state.
    pub state_dir: PathBuf,
    /// Durable-state backend URL (`sqlite://<path>`, a bare path, or
    /// `postgres://…`). `None` falls back to `PRELOOP_STORE_URL`, then to
    /// SQLite at `<state_dir>/preloop.db`.
    pub store_url: Option<String>,
    /// Optional file path to write recorded flows to (NDJSON format).
    pub record_flows: Option<PathBuf>,
    /// TLS mode (default: no TLS).
    pub tls: TlsMode,
    /// Shared counter published with the number of jobs still queued after
    /// each claim. Supply one to let a co-hosted runner pool scale to demand.
    pub queue_depth: Option<Arc<std::sync::atomic::AtomicUsize>>,
    /// Shared list, refreshed after each claim, of the `runs-on` labels of
    /// the job at the front of the dispatch queue. Supply one to let a
    /// co-hosted runner pool select the correct base-image golden.
    pub next_job_runs_on: Option<Arc<std::sync::RwLock<Vec<String>>>>,
    /// Raised while a co-hosted runner pool is still preparing its
    /// immutable machine image (artifact download or build, golden prep)
    /// and cannot register a runner yet. The starvation sweep pauses the
    /// queued-job grace clock while it is set.
    pub pool_preparing: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Enable privileged local/CI simulation endpoints.
    pub enable_test_api: bool,
    /// Bearer token required by privileged simulation endpoints.
    pub test_api_token: Option<String>,
    /// OIDC issuer URL. Defaults to `{public_base_url}/oidc`.
    ///
    /// This must identify an issuer controlled by the preloop deployment. Setting
    /// GitHub's hosted issuer does not make locally signed tokens GitHub-trusted.
    pub oidc_issuer: Option<String>,
    /// Enable the cron scheduler for schedule-triggered workflows.
    pub enable_scheduler: bool,
    /// Shared one-time provision-token map written by a co-hosted runner
    /// pool. Presence enables pool assignment enforcement: jobs queued while
    /// it is set may only be claimed by the runner whose registration later
    /// presents the matching provisioning token.
    pub pending_registrations:
        Option<Arc<std::sync::RwLock<std::collections::BTreeMap<String, std::time::SystemTime>>>>,
    /// `PRELOOP_REQUIRE_JOB_ASSIGNMENTS`: refuse to dispatch any job without
    /// a recorded assignment, including to external runners.
    pub require_job_assignments: bool,
}

impl std::fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print a Postgres URL verbatim: it carries the password.
        f.debug_struct("ServerConfig")
            .field("listen", &self.listen)
            .field("systemd_socket_activation", &self.systemd_socket_activation)
            .field("unix_socket", &self.unix_socket)
            .field("state_dir", &self.state_dir)
            .field(
                "store_url",
                &self.store_url.as_deref().map(redact_store_url),
            )
            .field("record_flows", &self.record_flows)
            .field("tls", &self.tls)
            .field("queue_depth", &self.queue_depth)
            .field("next_job_runs_on", &self.next_job_runs_on)
            .field("enable_test_api", &self.enable_test_api)
            .field(
                "test_api_token",
                &self.test_api_token.as_deref().map(|_| "<redacted>"),
            )
            .field("oidc_issuer", &self.oidc_issuer)
            .field("enable_scheduler", &self.enable_scheduler)
            .field("pending_registrations", &self.pending_registrations)
            .field("require_job_assignments", &self.require_job_assignments)
            .finish()
    }
}

/// Mask the password portion of a `postgres://user:pass@host/db` URL. Non-URL
/// values (bare sqlite paths, sqlite:// URLs) pass through untouched.
fn redact_store_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let Some((userinfo, hostport)) = rest.rsplit_once('@') else {
        return url.to_owned();
    };
    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:***@{hostport}"),
        None => url.to_owned(),
    }
}

/// TLS configuration.
#[derive(Debug, Clone)]
pub enum TlsMode {
    /// Plain HTTP (default).
    None,
    /// Generate an ephemeral self-signed cert at startup.
    SelfSigned,
    /// Load cert and key from PEM files.
    PemFiles { cert: PathBuf, key: PathBuf },
}

/// A self-signed TLS certificate + private key in PEM format.
pub struct SelfSignedCert {
    /// PEM-encoded certificate.
    pub cert: String,
    /// PEM-encoded private key.
    pub key: String,
}

/// Generate an ephemeral self-signed TLS certificate valid for localhost.
pub fn generate_self_signed_cert() -> anyhow::Result<SelfSignedCert> {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let rcgen::CertifiedKey { cert, key_pair } = generate_simple_self_signed(subject_alt_names)
        .map_err(|e| anyhow::anyhow!("self-signed cert generation failed: {e}"))?;
    Ok(SelfSignedCert {
        cert: cert.pem(),
        key: key_pair.serialize_pem(),
    })
}

pub(crate) async fn reap_once(shared: &Arc<SharedState>) {
    let mut inner = shared.state.inner.lock().await;
    let now = SystemTime::now();
    let mut cancellations = Vec::new();
    let mut disconnected_completions = Vec::new();
    // Jobs failed by the starvation sweep, emitted after the lock is
    // released so the event fan-out never runs under the state lock.
    let mut starved: Vec<(RunId, JobId, String)> = Vec::new();

    let mut active_reqs = Vec::new();
    for (request_id, request) in &inner.job_requests {
        if request.result.is_none() {
            active_reqs.push((
                *request_id,
                request.run_id,
                request.job_id.clone(),
                request.started_at,
                request.last_renewed_at,
                request.timeout_triggered,
            ));
        }
    }

    // Drop sessions whose worker stopped polling before reading pause credit,
    // and sessions whose job has since ended. Either way a crashed or finished
    // job must not go on suspending a timeout.
    let active_request_ids: std::collections::BTreeSet<i64> =
        active_reqs.iter().map(|(id, ..)| *id).collect();
    // Pause credit outlives the sessions that earned it: the registry retires
    // it with the job request, not with the session. The sweep below therefore
    // cannot retroactively bill a job for time it spent legitimately paused —
    // which is what used to cancel a job one tick after it resumed.
    let paused_credits: std::collections::BTreeMap<i64, Duration> = active_reqs
        .iter()
        .map(|(id, ..)| (*id, inner.debug_sessions.paused_for_request(*id, now)))
        .collect();
    crate::debug_sessions::sweep(&mut inner.debug_sessions, now, &active_request_ids);

    // Starvation sweep: a ready-queue job that no runner can ever claim must
    // not sit queued forever with no explanation. The pool is provisioned on
    // demand and external runners may register at any moment, so a job is
    // only failed after a grace window during which nothing matched its
    // labels. The `queued_at` map is maintained here, from the queue itself:
    // first observation stamps the time, a match clears it, and jobs that
    // left the queue drop their entry, so no enqueue-site coordination is
    // needed and the map cannot go stale. While a co-hosted pool is still
    // preparing its machine image (artifact download or build, golden prep)
    // it cannot register a runner no matter how long the job waits, so the
    // clock is reset for the whole warm: a job queued mid-warm gets a full
    // grace window once provisioning actually starts.
    const QUEUED_JOB_GRACE: Duration = Duration::from_secs(120);
    let pool_preparing = shared
        .state
        .pool_preparing
        .as_ref()
        .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire));
    let queued_jobs: Vec<_> = inner.queue.iter().cloned().collect();
    let in_queue: std::collections::BTreeSet<(RunId, JobId)> = queued_jobs
        .iter()
        .map(|job| (job.run_id, job.job_id.clone()))
        .collect();
    inner.queued_at.retain(|key, _| in_queue.contains(key));
    let mut starved_keys: Vec<(RunId, JobId, String)> = Vec::new();
    for job in &queued_jobs {
        let key = (job.run_id, job.job_id.clone());
        let matching = inner.runners.values().any(|runner| {
            crate::runtime_scheduling::job_matches_runner(&job.runs_on, &runner.labels)
        });
        if matching {
            inner.queued_at.remove(&key);
            continue;
        }
        // Unlike the Linux microVM pool, non-Linux jobs can only run on a
        // registered host. Keep them queued until that host appears rather
        // than converting a temporarily empty host pool into a failed job.
        let needs_external_host = job.runs_on.iter().any(|label| {
            let label = label.to_ascii_lowercase();
            label.starts_with("macos") || label.starts_with("windows")
        });
        if needs_external_host {
            inner.queued_at.remove(&key);
            continue;
        }
        if pool_preparing {
            // The pool is warming and cannot register a runner yet; do not
            // let the grace window expire while the first machine image is
            // still being produced.
            inner.queued_at.remove(&key);
            continue;
        }
        let first_seen = *inner.queued_at.entry(key.clone()).or_insert(now);
        if now.duration_since(first_seen).unwrap_or_default() < QUEUED_JOB_GRACE {
            continue;
        }
        let reason = format!(
            "no runner is registered for `runs-on: {}` and none appeared \
             within {}s, so the job cannot be scheduled",
            job.runs_on.join(", "),
            QUEUED_JOB_GRACE.as_secs()
        );
        tracing::warn!(
            run_id = %job.run_id,
            job_id = %job.job_id.0,
            labels = ?job.runs_on,
            "starving queued job failed after {}s without a matching runner",
            QUEUED_JOB_GRACE.as_secs()
        );
        starved_keys.push((job.run_id, job.job_id.clone(), reason));
    }
    for (run_id, job_id, reason) in starved_keys {
        inner.queued_at.remove(&(run_id, job_id.clone()));
        starved.push((run_id, job_id, reason));
    }

    for (request_id, run_id, job_id, started_at, last_renewed_at, timeout_triggered) in active_reqs
    {
        // 1. Check Timeout Enforcement
        if let Some(started_at) = started_at {
            if !timeout_triggered {
                // Time spent paused at a failed step is debugging, not
                // execution. Without this subtraction a `timeout-minutes: 10`
                // job is cancelled ten minutes into a debug session, through a
                // path no client can see.
                let paused = paused_credits.get(&request_id).copied().unwrap_or_default();
                let elapsed = now
                    .duration_since(started_at)
                    .unwrap_or_default()
                    .saturating_sub(paused);
                let job_timeout = inner
                    .broker_messages
                    .get(&request_id)
                    .and_then(|msg| msg.job_timeout)
                    .unwrap_or(21600); // 360 minutes in seconds

                if elapsed >= Duration::from_secs(job_timeout as u64) {
                    info!(
                        %run_id,
                        %job_id,
                        request_id,
                        "Job timed out after {}s",
                        job_timeout
                    );
                    if let Some(req) = inner.job_requests.get_mut(&request_id) {
                        req.timeout_triggered = true;
                    }
                    if let Some(agent_job_id) = agent_job_id_for(&inner, run_id, &job_id) {
                        cancellations.push(QueuedCancellation {
                            run_id,
                            job_id: job_id.clone(),
                            agent_job_id,
                        });
                    }
                }
            }
        }

        // 2. Check Lease Expiration / Disconnect Reaper
        if let Some(last_renewed_at) = last_renewed_at {
            let elapsed = now.duration_since(last_renewed_at).unwrap_or_default();
            if elapsed >= Duration::from_secs(JOB_LEASE_SECONDS) {
                info!(
                    %run_id,
                    %job_id,
                    request_id,
                    "Runner lease expired (last renewed {}s ago). Marking job as failed.",
                    elapsed.as_secs()
                );
                if let Some(req) = inner.job_requests.get_mut(&request_id) {
                    req.result = Some(ExecutionStatus::Failure);
                }
                disconnected_completions.push((
                    request_id,
                    JobCompletion {
                        run_id,
                        job_id: job_id.clone(),
                        status: ExecutionStatus::Failure,
                        outputs: Default::default(),
                        annotations: Vec::new(),
                    },
                ));
            }
        }
    }

    // Cleanup session and inflight maps for disconnected runners
    for (request_id, _) in &disconnected_completions {
        inner.inflight_requests.remove(request_id);
        inner
            .session_active_requests
            .retain(|_, &mut v| v != *request_id);
    }

    // Apply cancellations
    let cancellation_count = cancellations.len();
    if cancellation_count > 0 {
        inner.cancellation_queue.extend(cancellations);
    }

    // Apply starvation failures: remove each job from the ready queue and
    // mark it terminal in its run, so dependents unblock and a run with no
    // surviving jobs concludes. The reason is emitted after the lock.
    for (run_id, job_id, _reason) in &starved {
        inner
            .queue
            .retain(|job| job.run_id != *run_id || job.job_id != *job_id);
        if let Some(run) = inner.runs.get_mut(run_id) {
            run.jobs.insert(job_id.clone(), ExecutionStatus::Failure);
            run.status = crate::runtime_scheduling::summarize_run(run.jobs.values().copied());
            crate::runtime_scheduling::finalize_run_if_complete(run);
        }
    }

    drop(inner);

    // Liveness sweep: a session that stops polling is a deaf runner — its
    // in-guest control bridge died (e.g. the guest network was not up at
    // fork and the bridge gave up). Purge it so the unfinished job goes
    // back on the queue for a fresh machine instead of sitting in_progress
    // until the 45-minute job lease fails it, and so the pool stops handing
    // the dead machine new jobs. Restored sessions from a restart have no
    // last-seen entry and are deliberately skipped here (the runner
    // re-registers and polls, or the lease reaper bounds them).
    let stale_runners: std::collections::BTreeSet<i64> = {
        let inner = shared.state.inner.lock().await;
        let now = std::time::Instant::now();
        inner
            .session_last_seen
            .iter()
            .filter(|(_, seen)| now.duration_since(**seen) > inner.runner_liveness_timeout)
            .filter_map(|(session_id, _)| inner.runner_id_for_session(session_id))
            .collect()
    };
    for runner_id in stale_runners {
        warn!(
            runner_id,
            "liveness sweep: reaping deaf runner (no poll within timeout)"
        );
        purge_runner_identity(shared, runner_id).await;
    }

    // Notify if cancellations or starvation failures occurred
    if cancellation_count > 0 || !starved.is_empty() {
        shared.state.message_notify.notify_waiters();
    }

    // Surface why a queued job was failed. Without this the only record is a
    // server-side log line the workflow author never sees.
    for (run_id, job_id, reason) in &starved {
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id: *run_id,
                job_id: job_id.clone(),
                status: ExecutionStatus::Failure,
                reason: Some(reason.clone()),
            })
            .await;
    }

    // Process completions for disconnected runners
    for (_, completion) in disconnected_completions {
        let _ = complete_job_inner(shared.clone(), completion).await;
    }
}

async fn run_background_reaper(shared: Arc<SharedState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    // Skip the first tick
    interval.tick().await;

    while !shared.shutdown.is_cancelled() {
        tokio::select! {
            _ = interval.tick() => {
                reap_once(&shared).await;
            }
            _ = shared.shutdown.cancelled() => {
                break;
            }
        }
    }
}

fn is_routine_unix_disconnect(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<hyper::Error>()
        .is_some_and(|error| error.is_shutdown() || error.is_incomplete_message())
}

/// Start the server and block until shutdown.
pub async fn serve(config: ServerConfig) -> anyhow::Result<()> {
    let mut state = AppState::new_with_store(
        config.state_dir.clone(),
        crate::config::config_path(),
        config.store_url.as_deref(),
    )
    .await?;
    if let Some(queue_depth) = config.queue_depth.clone() {
        state.queue_depth = queue_depth;
    }
    if let Some(next_job_runs_on) = config.next_job_runs_on.clone() {
        state.next_job_runs_on = next_job_runs_on;
    }
    {
        let pool_managed = config.pending_registrations.is_some();
        if let Some(pending_registrations) = config.pending_registrations.clone() {
            state.pending_registrations = pending_registrations;
        }
        let mut inner = state.inner.lock().await;
        inner.pool_assignments_enabled = pool_managed;
        inner.require_job_assignments = config.require_job_assignments;
    }
    if !config.listen.ip().is_loopback() && state.system_token == DEFAULT_PRELOOP_SYSTEM_TOKEN {
        anyhow::bail!(
            "PRELOOP_SYSTEM_TOKEN must be explicitly configured when listening beyond loopback"
        );
    }
    let oidc_issuer = normalize_oidc_issuer(
        config
            .oidc_issuer
            .unwrap_or_else(|| format!("{}/oidc", runner_base_url())),
    )?;
    {
        let mut inner = state.inner.lock().await;
        inner.oidc_issuer = oidc_issuer;
    }
    let shutdown = CancellationToken::new();
    if config.enable_scheduler {
        let scheduler = crate::scheduler::Scheduler::new();
        state.scheduler = Some(scheduler.clone());
        let shared_for_scan = Arc::new(SharedState {
            state: state.clone(),
            shutdown: shutdown.clone(),
        });
        let scheduler_clone = scheduler.clone();
        if let Some(workspace) = state.local_workspace.clone() {
            tokio::spawn(async move {
                scheduler_clone
                    .scan_workspace(&workspace, shared_for_scan)
                    .await;
            });
        } else {
            tokio::spawn(async move {
                scheduler_clone.scan_remote(shared_for_scan).await;
            });
        }
    }
    // Read back the App's webhook event subscription at startup (D7). A new
    // App created from the manifest gets the expanded default events, but an
    // App created earlier — or narrowed by hand — may miss trigger events,
    // and GitHub cannot change a subscription through the API. Warn loudly
    // so the operator ticks the missing events in App settings.
    if let Some(app) = state.github_app.clone() {
        let app_id = app.app_id.clone();
        tokio::spawn(async move {
            match app.read_app_events().await {
                Ok(events) => crate::github_app::warn_missing_trigger_events(&app_id, &events),
                Err(error) => warn!(
                    app_id,
                    ?error,
                    "could not read back the GitHub App's event subscription at startup"
                ),
            }
        });
    }
    if let Some(path) = &config.record_flows {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let mut inner = state.inner.lock().await;
        inner.flows_file = Some(file);
    }
    let test_api_token = if config.enable_test_api {
        if !config.listen.ip().is_loopback() {
            anyhow::bail!("the test API may only be enabled on a loopback listener");
        }
        let token = config
            .test_api_token
            .clone()
            .filter(|token| !token.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("--enable-test-api requires --test-api-token"))?;
        warn!(
            listen = %config.listen,
            "PRIVILEGED TEST API ENABLED; simulated sessions and completions are accepted"
        );
        Some(token)
    } else {
        if config.test_api_token.is_some() {
            anyhow::bail!("--test-api-token requires --enable-test-api");
        }
        None
    };
    let router = build_app(state.clone(), shutdown.clone(), test_api_token);

    state.pool_preparing = config.pool_preparing.clone();
    let shared = Arc::new(SharedState {
        state,
        shutdown: shutdown.clone(),
    });

    let checker_shared = shared.clone();
    tokio::spawn(async move {
        run_background_reaper(checker_shared).await;
    });

    // Claims held by machines the restart destroyed can never be completed by
    // anyone; settle them before serving so the pool is not handed a queue of
    // jobs it is structurally unable to claim.
    crate::broker::reconcile_orphaned_claims(&shared).await;

    match config.tls {
        TlsMode::None => {
            #[cfg(unix)]
            if let Some(unix_path) = &config.unix_socket {
                use std::os::unix::fs::PermissionsExt;

                if let Some(parent) = unix_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                match std::fs::remove_file(unix_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                let unix_listener = tokio::net::UnixListener::bind(unix_path)?;
                std::fs::set_permissions(unix_path, std::fs::Permissions::from_mode(0o600))?;
                info!(path = %unix_path.display(), "preloop runner server listening on unix socket");
                // The control socket is mounted into every runner VM: guests
                // get the runner/broker protocol only. Native management and
                // test APIs stay off it — workflow code is untrusted.
                let router_unix = router
                    .clone()
                    .layer(middleware::from_fn(auth::runner_surface_only));
                let shutdown_unix = shutdown.clone();
                tokio::spawn(async move {
                    use hyper_util::rt::{TokioExecutor, TokioIo};
                    use hyper_util::server::conn::auto::Builder as AutoBuilder;
                    use hyper_util::service::TowerToHyperService;

                    loop {
                        tokio::select! {
                            _ = shutdown_unix.cancelled() => break,
                            accept_result = unix_listener.accept() => {
                                let Ok((stream, _)) = accept_result else {
                                    continue;
                                };
                                let io = TokioIo::new(stream);
                                let service = TowerToHyperService::new(router_unix.clone());
                                tokio::spawn(async move {
                                    if let Err(error) = AutoBuilder::new(TokioExecutor::new())
                                        .serve_connection_with_upgrades(io, service)
                                        .await
                                    {
                                        // Clients routinely drop the socket after
                                        // their request (the CLI's own readiness
                                        // probe included); a failed final
                                        // write-shutdown is teardown noise
                                        if is_routine_unix_disconnect(error.as_ref()) {
                                            debug!(%error, "Unix socket connection closed");
                                        } else {
                                            warn!(%error, "Unix socket HTTP connection failed");
                                        }
                                    }
                                });
                            }
                        }
                    }
                });
            }
            let listener = if config.systemd_socket_activation {
                preloop_socket_activation::take_tcp_listener()?.ok_or_else(|| {
                    anyhow::anyhow!("systemd socket activation requested but LISTEN_FDS is unset")
                })?
            } else {
                TcpListener::bind(config.listen).await?
            };
            info!(
                listen = %config.listen,
                scheme = "http",
                registration_policy = ?shared.state.registration_policy,
                "preloop runner server listening"
            );
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_signal(shutdown))
                .await?;
        }
        TlsMode::SelfSigned => {
            let cert = generate_self_signed_cert()?;
            let tls_config =
                RustlsConfig::from_pem(cert.cert.into_bytes(), cert.key.into_bytes()).await?;
            info!(listen = %config.listen, scheme = "https", self_signed = true, "preloop runner server listening");
            warn!("self-signed cert -- runner needs --ss-skip-tls-verify or GITHUB_ACTIONS_RUNNER_SKIP_TLS_VERIFY=1");
            let handle = Handle::new();
            tokio::spawn({
                let handle = handle.clone();
                async move {
                    if let Err(e) = axum_server::bind_rustls(config.listen, tls_config)
                        .handle(handle)
                        .serve(router.into_make_service())
                        .await
                    {
                        warn!(%e, "TLS server error");
                    }
                }
            });
            shutdown_signal(shutdown).await;
            handle.graceful_shutdown(Some(Duration::from_secs(5)));
        }
        TlsMode::PemFiles { cert, key } => {
            let tls_config = RustlsConfig::from_pem_file(&cert, &key).await?;
            info!(listen = %config.listen, scheme = "https", cert = %cert.display(), "preloop runner server listening");
            let handle = Handle::new();
            tokio::spawn({
                let handle = handle.clone();
                async move {
                    if let Err(e) = axum_server::bind_rustls(config.listen, tls_config)
                        .handle(handle)
                        .serve(router.into_make_service())
                        .await
                    {
                        warn!(%e, "TLS server error");
                    }
                }
            });
            shutdown_signal(shutdown).await;
            handle.graceful_shutdown(Some(Duration::from_secs(5)));
        }
    }
    Ok(())
}

async fn shutdown_signal(shutdown: CancellationToken) {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to install ctrl-c handler");
        }
    };
    ctrl_c.await;
    shutdown.cancel();
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt as _;

    #[tokio::test]
    async fn incomplete_unix_http_message_is_a_routine_disconnect() {
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto::Builder as AutoBuilder;
        use hyper_util::service::TowerToHyperService;

        let (mut client, server) = tokio::net::UnixStream::pair().unwrap();
        let connection = tokio::spawn(async move {
            let router = Router::new().route("/", get(|| async { "ok" }));
            AutoBuilder::new(TokioExecutor::new())
                .serve_connection_with_upgrades(
                    TokioIo::new(server),
                    TowerToHyperService::new(router),
                )
                .await
                .expect_err("partial request must produce an incomplete-message error")
        });

        client
            .write_all(b"GET / HTTP/1.1\r\nHost: preloop")
            .await
            .unwrap();
        drop(client);

        let error = connection.await.unwrap();
        assert!(is_routine_unix_disconnect(error.as_ref()));
    }
}
