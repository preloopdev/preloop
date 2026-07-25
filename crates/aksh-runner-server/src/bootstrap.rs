use super::*;

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind.
    pub listen: SocketAddr,
    /// Optional Unix domain socket path to bind.
    pub unix_socket: Option<PathBuf>,
    /// State directory for cache/artifacts and future durable state.
    pub state_dir: PathBuf,
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

    for (request_id, run_id, job_id, started_at, last_renewed_at, timeout_triggered) in active_reqs
    {
        // 1. Check Timeout Enforcement
        if let Some(started_at) = started_at {
            if !timeout_triggered {
                let elapsed = now.duration_since(started_at).unwrap_or_default();
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

    drop(inner);

    // Notify if cancellations occurred
    if cancellation_count > 0 {
        shared.state.message_notify.notify_waiters();
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

/// Start the server and block until shutdown.
pub async fn serve(config: ServerConfig) -> anyhow::Result<()> {
    let mut state = AppState::new(config.state_dir.clone()).await?;
    if let Some(queue_depth) = config.queue_depth.clone() {
        state.queue_depth = queue_depth;
    }
    if let Some(next_job_runs_on) = config.next_job_runs_on.clone() {
        state.next_job_runs_on = next_job_runs_on;
    }
    if !config.listen.ip().is_loopback() && state.system_token == DEFAULT_AKSH_SYSTEM_TOKEN {
        anyhow::bail!(
            "AKSH_SYSTEM_TOKEN must be explicitly configured when listening beyond loopback"
        );
    }
    let oidc_issuer = normalize_oidc_issuer(
        config
            .oidc_issuer
            .unwrap_or_else(|| format!("{}/oidc", public_base_url())),
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

    let shared = Arc::new(SharedState {
        state,
        shutdown: shutdown.clone(),
    });

    let checker_shared = shared.clone();
    tokio::spawn(async move {
        run_background_reaper(checker_shared).await;
    });

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
                info!(path = %unix_path.display(), "aksh runner server listening on unix socket");
                let router_unix = router.clone();
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
                                        warn!(%error, "Unix socket HTTP connection failed");
                                    }
                                });
                            }
                        }
                    }
                });
            }
            let listener = TcpListener::bind(config.listen).await?;
            info!(listen = %config.listen, scheme = "http", "aksh runner server listening");
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_signal(shutdown))
                .await?;
        }
        TlsMode::SelfSigned => {
            let cert = generate_self_signed_cert()?;
            let tls_config =
                RustlsConfig::from_pem(cert.cert.into_bytes(), cert.key.into_bytes()).await?;
            info!(listen = %config.listen, scheme = "https", self_signed = true, "aksh runner server listening");
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
            info!(listen = %config.listen, scheme = "https", cert = %cert.display(), "aksh runner server listening");
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
