use super::*;
use anyhow::Context;

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
    /// Shared counter with the number of jobs still queued, refreshed by the
    /// 5s state sampler. Supply one to let a co-hosted runner pool scale to
    /// demand.
    pub queue_depth: Option<Arc<std::sync::atomic::AtomicUsize>>,
    /// Shared list, refreshed after each claim, of the `runs-on` labels of
    /// the job at the front of the dispatch queue. Supply one to let a
    /// co-hosted runner pool select the correct base-image golden.
    pub next_job_runs_on: Option<Arc<std::sync::RwLock<Vec<String>>>>,
    /// Raised while a co-hosted runner pool is still preparing its
    /// immutable machine image (artifact download or build, golden prep)
    /// and cannot register a runner yet. The starvation sweep protects queued
    /// jobs during this warm, bounded by the absolute queue-age ceiling.
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
    /// Consolidated pool handle (replaces the four ad-hoc Option<Arc<…>> fields).
    /// When `Some`, the pool updates it and the sampler reads it.
    pub pool_status: Option<Arc<preloop_observability::status::PoolStatus>>,
    /// Observability handle to clone into AppState (heartbeat, limits).
    /// `None` falls back to `Observability::noop()` (tests).
    pub observability: Option<preloop_observability::Observability>,
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
            .field("pool_status", &self.pool_status)
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

pub async fn reap_once(shared: &Arc<SharedState>) {
    let (expired_cache_tokens, expired_artifact_tokens) = {
        let mut inner = shared.state.inner.lock().await;
        // Migrate legacy pending entries that restored with `created_unix == 0`
        // (pre-cap state) so they don't live forever. Give them `now` once
        // so the TTL sweeper can eventually collect them if never finalized.
        let now_u = now_unix();
        for pending in inner.cache_v2_pending.values_mut() {
            if pending.created_unix == 0 {
                pending.created_unix = now_u;
            }
        }
        for pending in inner.artifact_v2_pending.values_mut() {
            if pending.created_unix == 0 {
                pending.created_unix = now_u;
            }
        }
        // drop pending cache/artifact uploads and download tokens older than
        // PENDING_UPLOAD_TTL. Entries restored from a persisted meta have no age
        // and are left alone, so a restart never sweeps a legitimate upload.
        let expired_cache: Vec<String> = inner
            .cache_v2_pending
            .iter()
            .filter(|(_, p)| {
                p.created_unix > 0 && p.created_unix < now_u - PENDING_UPLOAD_TTL.as_secs() as i64
            })
            .map(|(k, _)| k.clone())
            .collect();
        let expired_artifact: Vec<String> = inner
            .artifact_v2_pending
            .iter()
            .filter(|(_, p)| {
                p.created_unix > 0 && p.created_unix < now_u - PENDING_UPLOAD_TTL.as_secs() as i64
            })
            .map(|(k, _)| k.clone())
            .collect();
        sweep_pending_uploads(&mut inner, now_u);
        // Release lock before doing I/O.
        (expired_cache, expired_artifact)
    };
    // Delete staging directories for expired reservations — otherwise they
    // accumulate on disk forever.
    for token in expired_cache_tokens {
        let dir = shared
            .state
            .state_dir
            .join("blobs")
            .join("cache")
            .join(token);
        let _ = tokio::fs::remove_dir_all(dir).await;
    }
    for token in expired_artifact_tokens {
        let dir = shared
            .state
            .state_dir
            .join("blobs")
            .join("artifact")
            .join(token);
        let _ = tokio::fs::remove_dir_all(dir).await;
    }
    // Fork-PR workflow policy: fail closed runs whose 24h approval window
    // expired while waiting for operator approval. The fail-closed mutation
    // is one backend transaction; the per-job events are emitted below, once
    // the run rows are final.
    let expired_fork_approvals =
        crate::fork_policy::sweep_expired_fork_approvals(shared, crate::models::now_unix_nanos())
            .await;
    // Everything this tick decides from, read directly: no working-set
    // load and no lock. Transactions below run only when something is due,
    // and only over the runs involved.
    let inputs = match shared.state.backend.reap_inputs().await {
        Ok(inputs) => inputs,
        Err(error) => {
            warn!(?error, "reaper input read failed");
            return;
        }
    };
    // Stale-binding sweep: the claim/pair path writes
    // `job_assignments`/`pool_pending`; with none present there is nothing
    // to reap (the "leaked job bindings" failure mode needs a binding).
    if inputs.has_bindings {
        match shared.state.backend.sweep_stale_bindings().await {
            Ok(0) => {}
            Ok(swept) => debug!(swept, "swept stale job bindings"),
            Err(error) => warn!(?error, "stale-binding sweep failed"),
        }
    }
    // ── Node-local inputs: pool flags + start time ──────────────────────
    let now = SystemTime::now();
    let pool_status = shared.state.pool_status.snapshot();
    let pool_preparing = shared
        .state
        .pool_preparing
        .as_ref()
        .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Acquire))
        || pool_status.preparing
        // A consolidated-handle embedding may drive only `pool_status` (no
        // legacy `preparing_signal`). Warm-slot and successor provisioning
        // raise `provisioning`, not `preparing`, so treat an in-flight
        // provision as preparing or those boots would go unprotected here.
        || pool_status.provisioning > 0;
    let started_at = shared.state.started_at;

    // ── Active attempts + the ready queue (from `inputs`) ────────────────
    let queued_jobs = inputs.ready.clone();
    // Runs with something due: a timeout or lease past its limit (a
    // superset: paused debug time only shortens the elapsed time), or a
    // ready job no runner can take (or one whose first-seen mark must clear).
    let mut due_runs: std::collections::BTreeSet<RunId> = std::collections::BTreeSet::new();
    for request in &inputs.active {
        let elapsed = |at: Option<SystemTime>| {
            at.and_then(|at| now.duration_since(at).ok())
                .unwrap_or_default()
        };
        let timed_out = request.started_at.is_some()
            && !request.timeout_triggered
            && elapsed(request.started_at)
                >= Duration::from_secs(request.job_timeout_s.unwrap_or(21600).max(0) as u64);
        let lease_expired = request.last_renewed_at.is_some()
            && elapsed(request.last_renewed_at)
                >= Duration::from_secs(if request.session_live {
                    crate::distributed_task::HUNG_WORKER_LEASE_SECONDS
                } else {
                    crate::distributed_task::DEAD_SESSION_LEASE_SECONDS
                });
        if timed_out || lease_expired {
            due_runs.insert(request.run_id);
        }
    }
    // Node-local starvation marks. The verdict here decides
    // only the mark; the backend re-evaluates the same verdict inside its
    // transaction, where the conditional ready→failure UPDATE is the
    // one-writer-wins guard across nodes.
    let first_seen = {
        let mut inner = shared.state.inner.lock().await;
        let marks = &mut inner.reaper_first_seen;
        let in_queue: std::collections::BTreeSet<(RunId, JobId)> = inputs
            .ready
            .iter()
            .map(|job| (job.run_id, job.job_id.clone()))
            .collect();
        marks.retain(|key, _| in_queue.contains(key));
        for job in &inputs.ready {
            let key = (job.run_id, job.job_id.clone());
            let any_runner_matches = inputs
                .runner_labels
                .iter()
                .any(|labels| crate::runtime_scheduling::job_matches_runner(&job.runs_on, labels));
            // An unmarked job measures from its enqueue instant (an unknown
            // age — `enqueued_at` 0 after a restore — is not granted a fresh
            // window); the verdict's `Mark` carries the instant to keep.
            let candidate = crate::control::logic::StarvationCandidate {
                runs_on: &job.runs_on,
                enqueued_at: std::time::UNIX_EPOCH
                    + Duration::from_nanos(job.enqueued_at_unix_nanos as u64),
                first_seen: marks.get(&key).copied(),
                any_runner_matches,
            };
            match crate::control::logic::starvation_verdict(
                &candidate,
                now,
                pool_preparing,
                started_at.elapsed() < crate::control::logic::MAX_QUEUED_GRACE,
                &pool_status.labels,
            ) {
                crate::control::logic::StarvationVerdict::ClearMark => {
                    marks.remove(&key);
                }
                // Provisioning protection re-stamps the observation clock;
                // ordinary unmatched jobs keep the first observation.
                crate::control::logic::StarvationVerdict::Mark { first_seen } => {
                    if pool_preparing {
                        marks.insert(key, first_seen);
                    } else {
                        marks.entry(key).or_insert(first_seen);
                    }
                }
                // The mark stays until the job leaves the ready queue
                // (`retain` above): the backend re-evaluates this verdict
                // with the same mark, and a lost race re-decides identically.
                crate::control::logic::StarvationVerdict::Starve { .. }
                | crate::control::logic::StarvationVerdict::Unschedulable { .. } => {
                    due_runs.insert(job.run_id);
                }
            }
        }
        marks.clone()
    };
    // ── Node-local: debug-session sweep + pause credits ─────────────────
    // Drop sessions whose worker stopped polling before reading pause credit,
    // and sessions whose job has since ended. Either way a crashed or finished
    // job must not go on suspending a timeout.
    let active_request_ids: std::collections::BTreeSet<i64> = inputs
        .active
        .iter()
        .map(|request| request.request_id)
        .collect();
    // Pause credit outlives the sessions that earned it: the registry retires
    // it with the job request, not with the session. The sweep below therefore
    // cannot retroactively bill a job for time it spent legitimately paused —
    // which is what used to cancel a job one tick after it resumed.
    let paused_credits: std::collections::BTreeMap<i64, Duration> = {
        let mut inner = shared.state.inner.lock().await;
        let credits = active_request_ids
            .iter()
            .map(|id| (*id, inner.debug_sessions.paused_for_request(*id, now)))
            .collect();
        crate::debug_sessions::sweep(&mut inner.debug_sessions, now, &active_request_ids);
        credits
    };

    // ── Authoritative sweep: starvation, timeout, lease-expiry ──────────
    // Starvation sweep: a ready-queue job that no runner can ever claim must
    // not sit queued forever with no explanation. The pool is provisioned on
    // demand and external runners may register at any moment, so a job is
    // only failed after a grace window during which nothing matched its
    // labels (`logic::starvation_verdict`). While a co-hosted pool is still
    // preparing its machine image or booting a runner the job stays
    // protected, bounded by the verdict's absolute ceiling measured from
    // ready-enqueue, so continuous provisioning cannot protect an
    // unschedulable job forever. Time paused at a failed debug step is not
    // execution time, so it is credited against `timeout-minutes`.
    let (cancellation_count, disconnected_completions, starved) = if due_runs.is_empty() {
        (0, Vec::new(), Vec::new())
    } else {
        let sweep = crate::control::types::ReapSweep {
            now,
            runs: due_runs,
            ready: queued_jobs,
            active: inputs.active.clone(),
            paused: paused_credits,
            pool_preparing,
            warm_window_open: started_at.elapsed() < crate::control::logic::MAX_QUEUED_GRACE,
            pool_labels: pool_status.labels.clone(),
            first_seen,
        };
        match shared.state.backend.reap_sweep(sweep).await {
            Ok(outcome) => {
                let completions: Vec<JobCompletion> = outcome
                    .expired
                    .into_iter()
                    .map(|lease| JobCompletion {
                        run_id: lease.run_id,
                        job_id: lease.job_id,
                        agent_job_id: lease.agent_job_id,
                        status: ExecutionStatus::Failure,
                        outputs: Default::default(),
                        annotations: Vec::new(),
                        step_results: Vec::new(),
                    })
                    .collect();
                (outcome.cancellations, completions, outcome.starved)
            }
            Err(error) => {
                warn!(?error, "reaper sweep failed");
                (0, Vec::new(), Vec::new())
            }
        }
    };

    // Queue-wait for every starved job, so the histogram covers every
    // terminal queue outcome, not just `claimed`: `unschedulable` = the
    // pool's advertised labels can never match; `starved` = a matching
    // runner never appeared inside the grace window.
    for job in &starved {
        if let Some(ready) = inputs
            .ready
            .iter()
            .find(|ready| ready.run_id == job.run_id && ready.job_id == job.job_id)
        {
            let enqueued_at =
                std::time::UNIX_EPOCH + Duration::from_nanos(ready.enqueued_at_unix_nanos as u64);
            if let Ok(wait) = now.duration_since(enqueued_at) {
                shared
                    .state
                    .observability
                    .metrics()
                    .lifecycle
                    .record_queue_wait(
                        if job.unschedulable {
                            "unschedulable"
                        } else {
                            "starved"
                        },
                        wait,
                    );
            }
        }
    }

    // Run statuses for the jobs the sweep just failed, read back from the
    // authoritative rows: `reap_sweep` applied the starvation failures and
    // recomputed each affected run inside its transaction, so the post-sweep
    // status is the run's conclusion. Several starved jobs can share a run,
    // and the status after the first failure is not that conclusion — the
    // reads happen after every failure was applied, not per job.
    let mut starved_run_ids: Vec<RunId> = Vec::new();
    for job in &starved {
        if !starved_run_ids.contains(&job.run_id) {
            starved_run_ids.push(job.run_id);
        }
    }
    let mut starved_runs: Vec<(RunId, ExecutionStatus)> = Vec::new();
    for run_id in starved_run_ids {
        match shared.state.backend.run_record(run_id).await {
            Ok(record) => starved_runs.push((run_id, record.status)),
            Err(error) => debug!(?error, %run_id, "starved run status read failed"),
        }
    }

    // Liveness sweep: a session that stops polling is a deaf runner — its
    // in-guest control bridge died (e.g. the guest network was not up at
    // fork and the bridge gave up). Purge it so the unfinished job goes
    // back on the queue for a fresh machine instead of sitting in_progress
    // until the lease reaper fails it, and so the pool stops handing the
    // dead machine new jobs. Restored sessions from a restart have no
    // last-seen entry and are deliberately skipped here (the runner
    // re-registers and polls, or the lease reaper bounds them).
    // Liveness sweep on the authoritative backend: `sessions`,
    // `broker_session_runners`, `runner_registered_at`, `session_last_seen`
    // and `runner_liveness_timeout` are all backend rows. Reading the dead
    // node-local maps here would mark every restored runner "phantom" (both
    // session negations unconditionally true) and purge live identities.
    let (stale_runners, phantom_runners) = (inputs.stale_runners, inputs.phantom_runners);
    for runner_id in stale_runners {
        warn!(
            runner_id,
            "liveness sweep: reaping deaf runner (no poll within timeout)"
        );
        if let Err(error) = purge_runner_identity(shared, runner_id).await {
            tracing::error!(
                ?error,
                runner_id,
                "liveness purge failed — deaf runner's listen token remains valid"
            );
        }
    }
    for runner_id in phantom_runners {
        warn!(
            runner_id,
            "liveness sweep: reaping phantom registration (no session created within timeout)"
        );
        crate::runner_lifecycle::purge_phantom_runner(shared, runner_id).await;
    }

    // Notify if cancellations or starvation failures occurred
    if cancellation_count > 0 || !starved.is_empty() || !expired_fork_approvals.is_empty() {
        shared.state.message_notify.notify_waiters();
        shared.state.sampler_notify.notify_waiters();
    }

    // Surface fork-PR runs failed closed by the expired approval window.
    // Like the starvation loop below, emit per-job status and complete the
    // GitHub check runs: without this a check run created at webhook intake
    // stays `queued` on GitHub indefinitely for a terminal run.
    for run_id in &expired_fork_approvals {
        // The run's jobs, from the authoritative rows: the sweep above
        // already failed every non-terminal job in the database.
        let failed_jobs: Vec<JobId> = match shared.state.backend.run_job_ids(*run_id).await {
            Ok(job_ids) => job_ids.into_iter().map(JobId).collect(),
            Err(error) => {
                warn!(?error, %run_id, "fork-approval job list read failed");
                Vec::new()
            }
        };
        for job_id in failed_jobs {
            shared
                .state
                .emit(NdjsonEvent::JobStatus {
                    run_id: *run_id,
                    job_id: job_id.clone(),
                    status: ExecutionStatus::Failure,
                    reason: Some("fork-PR approval window expired".to_owned()),
                })
                .await;
        }
        shared
            .state
            .emit(NdjsonEvent::RunStatus {
                run_id: *run_id,
                status: ExecutionStatus::Failure,
                reason: Some("fork-PR approval window expired".to_owned()),
            })
            .await;
    }

    // Surface why a queued job was failed. Without this the only record is a
    // server-side log line the workflow author never sees.
    for job in &starved {
        shared
            .state
            .emit(NdjsonEvent::JobStatus {
                run_id: job.run_id,
                job_id: job.job_id.clone(),
                status: ExecutionStatus::Failure,
                reason: Some(job.reason.clone()),
            })
            .await;
    }

    // Publish each affected run's updated status. A run the sweep just
    // concluded (or whose last queued job failed) reaches its terminal state
    // here; the event stream only closes a client connection on a terminal
    // `RunStatus`, so without this `preloop run` keeps waiting on a run the
    // engine has already failed instead of returning. A non-terminal status
    // (one starved job of several) is safe to publish too: it neither closes
    // the stream nor moves the watcher's conclusion.
    for (run_id, status) in &starved_runs {
        shared
            .state
            .emit(NdjsonEvent::RunStatus {
                run_id: *run_id,
                status: *status,
                reason: None,
            })
            .await;
    }

    // A completion is post-sweep housekeeping. Never let one stuck
    // completion wedge the only reaper task and prevent later lease expiry
    // attempts from being processed.
    for completion in disconnected_completions {
        match tokio::time::timeout(
            Duration::from_secs(30),
            complete_job_inner(shared.clone(), completion),
        )
        .await
        {
            Ok(Ok(_)) | Ok(Err(_)) => {}
            Err(_) => warn!("reaper completion exceeded 30s; will retry on the next tick"),
        }
    }

    // Environment protection gates: wait timers expire and approval windows
    // close on wall-clock time, not on scheduling events, so re-run admission
    // for every run parking a gate-armed job — newly-satisfied gates release
    // their jobs and expired approval windows fail closed. The command is a
    // no-op for runs with no parked gate, so the tick can always ask.
    match shared
        .state
        .backend
        .promote_ready_jobs(None, &shared.state.environment_rules)
        .await
    {
        Ok(outcome) => {
            // Post-commit: refresh the node-local mirrors the runner
            // supervisor and the pool read, then wake them if the sweep
            // changed what is schedulable. The ready-queue depth itself now
            // comes from the 5s sampler snapshot.
            if let Ok(mut guard) = shared.state.next_job_runs_on.write() {
                *guard = outcome.next_runs_on;
            }
            if outcome.promoted > 0 || outcome.failed > 0 {
                shared.state.message_notify.notify_waiters();
                shared.state.sampler_notify.notify_waiters();
            }
        }
        Err(error) => warn!(?error, "environment gate promotion sweep failed"),
    }
}

async fn run_background_reaper(shared: Arc<SharedState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    // Skip the first tick
    interval.tick().await;
    //Heartbeat for reaper (critical) — beat each interval, no cadence change.
    let heartbeat = shared.state.observability.heartbeat().clone();
    let _reaper_handle = heartbeat.register("reaper", preloop_observability::Criticality::Critical);
    heartbeat.beat("reaper");

    while !shared.shutdown.is_cancelled() {
        tokio::select! {
            _ = interval.tick() => {
                reap_once(&shared).await;
                // Beat after the sweep: a wedged or missing sweep must
                // surface as a stale critical task, not a live heartbeat.
                heartbeat.beat("reaper");
            }
            _ = shared.shutdown.cancelled() => {
                break;
            }
        }
    }
}

/// Hourly checkout-cache sweep. Retention is otherwise enforced only on run
/// completion and at startup, so a quiet server would hold expired caches
/// indefinitely. Best-effort housekeeping, never on a request path.
async fn run_checkout_cache_pruner(shared: Arc<SharedState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(3_600));
    // Skip the first tick: serve() already sweeps once at startup.
    interval.tick().await;
    while !shared.shutdown.is_cancelled() {
        tokio::select! {
            _ = interval.tick() => {
                if shared.state.checkout_cache.mode != crate::config::CheckoutCacheMode::Off {
                    let state_dir = shared.state.state_dir.clone();
                    let checkout_cache = shared.state.checkout_cache.clone();
                    crate::snapshots::prune_checkout_cache(&state_dir, &checkout_cache).await;
                }
            }
            _ = shared.shutdown.cancelled() => {
                break;
            }
        }
    }
}

/// Move settled run rows out of the hot scheduler tables. A missed wakeup or
/// process crash is repaired by the next scan; each batch commits atomically.
async fn run_history_archiver(shared: Arc<SharedState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                archive_finished_runs_once(&shared).await;
                // Timelines replay a running job to its runner; one idle for a
                // week belongs to a job long finished.
                let week_ago = (chrono::Utc::now() - chrono::Duration::days(7)).timestamp_micros();
                if let Err(error) = shared.state.backend.prune_timelines(week_ago).await {
                    tracing::warn!(?error, "timeline prune failed; will retry");
                }
                prune_outbox_once(&shared, outbox_retention()).await;
            }
            _ = shared.shutdown.cancelled() => break,
        }
    }
}

/// Outbox rows are pruned once older than the retention window *and* at or
/// below the slowest durable consumer bookmark (the `check-runs` projector);
/// rows a consumer has not read yet are retained however old they are. Env:
/// `PRELOOP_OUTBOX_RETENTION_SECONDS` (default 3600).
fn outbox_retention() -> Duration {
    Duration::from_secs(
        std::env::var("PRELOOP_OUTBOX_RETENTION_SECONDS")
            .ok()
            .and_then(|raw| raw.trim().parse().ok())
            .unwrap_or(3600),
    )
}

/// Delete expired outbox rows in batches until a short batch or an error.
/// Returns rows removed. Split out so a test can drive a pass.
pub(crate) async fn prune_outbox_once(shared: &SharedState, older_than: Duration) -> u64 {
    const BATCH: usize = 5_000;
    let mut removed_total = 0;
    loop {
        match shared.state.backend.prune_outbox(older_than, BATCH).await {
            Ok(removed) => {
                removed_total += removed;
                if removed < BATCH as u64 {
                    break;
                }
            }
            Err(error) => {
                tracing::warn!(?error, "outbox prune failed; will retry");
                break;
            }
        }
    }
    removed_total
}

/// Drain one archive pass: move settled runs to history in batches until a
/// short batch or an error, and return how many runs moved. Split out so a
/// test can drive a pass deterministically instead of racing the interval.
pub(crate) async fn archive_finished_runs_once(shared: &SharedState) -> usize {
    let mut archived_total = 0;
    loop {
        match shared.state.backend.archive_finished_runs(32).await {
            Ok(archived) => {
                archived_total += archived.len();
                // Run-tier secrets are NOT dropped here. Archiving only moves
                // settled rows into history, and a run in history can still be
                // re-run — `rerun` reads the original run tier. There is no
                // run-history retention/pruning yet, so the run tier lives as
                // long as the run's history; when retention lands, drop the
                // tier where the history row is removed.
                if archived.len() < 32 {
                    break;
                }
            }
            Err(error) => {
                tracing::warn!(?error, "run history archive failed; will retry");
                break;
            }
        }
    }
    archived_total
}

/// Everything the operational snapshot reads from durable control state
/// (`ControlBackend::status_inputs`). The 5s sampler and the startup seed
/// after a store restore both build their snapshots from this, so the two
/// cannot drift.
type SnapshotInputs = crate::control::types::StatusInputs;

/// Read the durable status inputs; a failed read degrades to an empty
/// snapshot rather than blocking the status publisher.
async fn read_snapshot_inputs(state: &AppState) -> SnapshotInputs {
    match state
        .backend
        .status_inputs(crate::runs::STALENESS_THRESHOLD)
        .await
    {
        Ok(inputs) => inputs,
        Err(error) => {
            warn!(?error, "status input read failed");
            SnapshotInputs::default()
        }
    }
}

/// `(open_session_count, oldest_session_age_seconds)` from the node-local
/// debug-session registry.
fn debug_session_scalars(inner: &InnerState) -> (u32, Option<f64>) {
    let sessions = inner.debug_sessions.list();
    let oldest_seconds = sessions
        .iter()
        .map(|session| session.created_at_ms)
        .min()
        .map(|created_at_ms| {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            now_ms.saturating_sub(created_at_ms) as f64 / 1000.0
        });
    (sessions.len() as u32, oldest_seconds)
}

/// Webhook-repair inputs for the operational snapshot.
///
/// Collected asynchronously (store + status locks) and handed to the
/// synchronous builder, so the snapshot can report repair health without
/// the builder learning how to read a database.
#[derive(Debug, Default, Clone)]
struct WebhookConditionInputs {
    stats: Option<crate::models::WebhookQueueStats>,
    watchdog: crate::webhook_status::WatchdogStatus,
    breaker: crate::github_breaker::BreakerSnapshot,
    app_config: Vec<crate::webhook_status::AppWebhookConfigStatus>,
}

/// A queue whose oldest unprocessed delivery is older than this is not
/// "busy", it is stuck: the worker retries with at most a 30s backoff.
const WEBHOOK_QUEUE_STALL_SECONDS: f64 = 900.0;
/// The watchdog polls every 5 minutes by default. Six missed polls is not a
/// blip, and a watchdog that has stopped reading GitHub's delivery history
/// looks exactly like a period with no failed deliveries.
const WEBHOOK_WATCHDOG_STALE_SECONDS: f64 = 1800.0;

/// Conditions derived from the webhook repair layers.
fn webhook_conditions(
    inputs: &WebhookConditionInputs,
    now_us: i64,
) -> Vec<preloop_observability::status::Condition> {
    use preloop_observability::status::Condition;
    let condition = |code: &str, severity: &str, message: String| Condition {
        code: code.to_owned(),
        severity: severity.to_owned(),
        message,
        exemplars: Vec::new(),
    };
    let mut conditions = Vec::new();
    match &inputs.stats {
        Some(stats) => {
            if stats.failed > 0 {
                conditions.push(condition(
                    "webhook_dead_letter",
                    "warning",
                    format!(
                        "{} webhook deliveries failed with unreportable or permanent errors",
                        stats.failed
                    ),
                ));
            }
            if let Some(oldest) = stats.oldest_pending_received_at_us {
                let age = crate::webhook_status::age_seconds(oldest, now_us);
                if age > WEBHOOK_QUEUE_STALL_SECONDS {
                    conditions.push(condition(
                        "webhook_queue_stalled",
                        "warning",
                        format!("oldest unprocessed webhook delivery is {age:.0}s old"),
                    ));
                }
            }
        }
        None => {
            conditions.push(condition(
                "webhook_queue_stats_unavailable",
                "warning",
                "webhook queue statistics have not been published by the queue worker".to_owned(),
            ));
        }
    }
    if inputs.breaker.open {
        conditions.push(condition(
            "github_unavailable",
            "warning",
            format!(
                "GitHub calls are circuit-broken for another {}s ({}): queued deliveries are parked, not failing",
                inputs.breaker.retry_in_seconds.unwrap_or_default(),
                inputs
                    .breaker
                    .last_error
                    .clone()
                    .unwrap_or_else(|| "no detail".to_owned())
            ),
        ));
    }
    if inputs.watchdog.enabled {
        let stale = match inputs.watchdog.last_success_at_us {
            Some(last) => {
                crate::webhook_status::age_seconds(last, now_us) > WEBHOOK_WATCHDOG_STALE_SECONDS
            }
            // Never succeeded: only alarming once the process has been up
            // long enough for an attempted poll to prove the watchdog is
            // actually unable to complete.
            None => inputs.watchdog.first_poll_at_us.is_some_and(|poll| {
                crate::webhook_status::age_seconds(poll, now_us) > WEBHOOK_WATCHDOG_STALE_SECONDS
            }),
        };
        if stale {
            conditions.push(condition(
                "webhook_watchdog_stale",
                "warning",
                format!(
                    "webhook delivery watchdog has not completed a poll recently ({}); \
                     lost deliveries would go unnoticed",
                    inputs
                        .watchdog
                        .last_error
                        .clone()
                        .unwrap_or_else(|| "no error recorded".to_owned())
                ),
            ));
        }
        if inputs.watchdog.open_repairs > 0 {
            conditions.push(condition(
                "webhook_repairs_pending",
                "warning",
                format!(
                    "{} GitHub deliveries have been asked for redelivery and have not arrived",
                    inputs.watchdog.open_repairs
                ),
            ));
        }
    }
    for app in inputs.app_config.iter().filter(|app| !app.healthy()) {
        let mut detail = Vec::new();
        if app.url_drifted() {
            detail.push(format!(
                "delivery URL is {} but this server expects {}",
                app.hook_url.clone().unwrap_or_else(|| "<none>".to_owned()),
                app.expected_url
                    .clone()
                    .unwrap_or_else(|| "<unknown>".to_owned())
            ));
        }
        if !app.missing_events.is_empty() {
            detail.push(format!(
                "not subscribed to {} (fix in App settings; GitHub has no API for it)",
                app.missing_events.join(", ")
            ));
        }
        if !app.missing_permissions.is_empty() {
            detail.push(format!(
                "missing permissions {}",
                app.missing_permissions.join(", ")
            ));
        }
        if let Some(error) = &app.error {
            detail.push(error.clone());
        }
        conditions.push(condition(
            "webhook_config_drift",
            "warning",
            format!("GitHub App {}: {}", app.app_id, detail.join("; ")),
        ));
    }
    conditions
}

fn build_operational_snapshot_sync(
    inputs: SnapshotInputs,
    (debug_active_sessions, debug_oldest_session_seconds): (u32, Option<f64>),
    mut pool_snapshot: preloop_observability::status::PoolSnapshot,
    observability: &preloop_observability::Observability,
    started_at: std::time::Instant,
    shutdown_requested: bool,
    scheduler_enabled: bool,
    state_dir: &std::path::Path,
    storage_components: Vec<preloop_observability::status::StorageComponent>,
    github_snapshot: preloop_observability::status::GithubSnapshot,
    store_backend: preloop_observability::status::StoreBackend,
    webhook: WebhookConditionInputs,
) -> preloop_observability::status::OperationalSnapshot {
    use chrono::Utc;
    use preloop_observability::status::*;
    let now = Utc::now();
    let uptime = started_at.elapsed().as_secs();
    // Claimability: distinguish claimable vs unclaimable using the same
    // predicates the scheduler dispatches with — label matching plus the
    // explicit runner-group check, so a job restricted to a specialized
    // group is not reported claimable by default-group runners.
    let (claimable, unclaimable) = if inputs.queue_len == 0 {
        (0, 0)
    } else if inputs.runner_caps.is_empty() {
        (0, inputs.queue_len as u32)
    } else if pool_snapshot.preparing {
        // Temporarily unclaimable while pool prepares.
        (0, inputs.queue_len as u32)
    } else {
        let mut claimable = 0u32;
        for (runs_on, runner_group) in &inputs.queue_runner_reqs {
            let matches = inputs.runner_caps.iter().any(|caps| {
                crate::runtime_scheduling::job_matches_runner(runs_on, &caps.labels)
                    && crate::runtime_scheduling::job_matches_runner_group(
                        runner_group.as_deref(),
                        caps,
                    )
            });
            if matches {
                claimable += 1;
            }
        }
        (claimable, inputs.queue_len as u32 - claimable)
    };

    let oldest_ready_seconds = inputs.oldest_ready_seconds;
    let queue_stalled = claimable > 0
        && inputs.runner_idle > 0
        && oldest_ready_seconds.is_some_and(|age| age >= 60.0);
    let check_reporting_failed = github_snapshot
        .last_check_failure_at
        .is_some_and(|failure| {
            github_snapshot
                .last_check_success_at
                .is_none_or(|success| failure > success)
        });
    let mut conditions = if queue_stalled {
        vec![Condition {
            code: "claimable_queue_stalled".to_owned(),
            severity: "error".to_owned(),
            message: format!(
                "{claimable} claimable job(s) have waited at least {:.0}s while {} runner(s) report idle",
                oldest_ready_seconds.unwrap_or_default(),
                inputs.runner_idle
            ),
            exemplars: vec![ConditionExemplar {
                run_id: inputs.oldest_ready_run_id.clone(),
                job_id: inputs.oldest_ready_job_id.clone(),
                runner_id: None,
                machine_name: None,
            }],
        }]
    } else {
        Vec::new()
    };
    if check_reporting_failed {
        conditions.push(Condition {
            code: "github_check_update_failure".to_owned(),
            severity: "error".to_owned(),
            message: "GitHub Check Run reporting failed; job results may be missing from GitHub"
                .to_owned(),
            exemplars: Vec::new(),
        });
    }
    let runs_without_execution = !inputs.orphaned_run_ids.is_empty();
    if runs_without_execution {
        conditions.push(Condition {
            code: "run_in_progress_without_execution".to_owned(),
            severity: "error".to_owned(),
            message: format!(
                "{} run(s) are in_progress with no queued, claimed, or assigned work",
                inputs.orphaned_run_ids.len()
            ),
            exemplars: inputs
                .orphaned_run_ids
                .iter()
                .take(3)
                .map(|run_id| ConditionExemplar {
                    run_id: Some(run_id.clone()),
                    job_id: None,
                    runner_id: None,
                    machine_name: None,
                })
                .collect(),
        });
    }
    pool_snapshot.released_bindings = inputs.released_bindings;
    pool_snapshot.busy = inputs.pool_busy;

    OperationalSnapshot {
        schema_version: 2,
        observed_at: now,
        snapshot_age_seconds: 0.0,
        overall: if shutdown_requested {
            Overall::ShuttingDown
        } else if queue_stalled || check_reporting_failed || runs_without_execution {
            Overall::Degraded
        } else {
            Overall::Ok
        },
        service: ServiceSnapshot {
            version: env!("CARGO_PKG_VERSION").to_string(),
            instance_id: observability.instance_id().to_string(),
            uptime_seconds: uptime,
            shutdown_requested,
        },
        runs: RunsSnapshot {
            queued: inputs.runs_queued,
            in_progress: inputs.runs_in_progress,
            completed: inputs.runs_completed,
        },
        active_runs: inputs.active_runs,
        jobs: JobsSnapshot {
            ready: inputs.queue_len as u32,
            dependency_blocked: inputs.pending_jobs_len as u32,
            concurrency_blocked: inputs.concurrency_blocked,
            pending_expansion: inputs.pending_expansions_len as u32,
            expanding: inputs.expanding_len as u32,
            claimable,
            unclaimable,
            oldest_ready_seconds,
        },
        concurrency: ConcurrencySnapshot {
            groups_active: inputs.concurrency_groups_active,
            groups_contended: inputs.concurrency_groups_contended,
            pending_holders: inputs.concurrency_pending_holders,
            deepest_group_pending: inputs.concurrency_deepest_group_pending,
            ..Default::default()
        },
        scheduler: SchedulerSnapshot {
            enabled: scheduler_enabled,
            ..Default::default()
        },
        runners: RunnersSnapshot {
            registered: inputs.registered,
            sessions: inputs.sessions,
            idle: inputs.runner_idle,
            busy: inputs.runner_busy,
            stale: inputs.runner_stale,
            max_poll_age_seconds: None,
            max_lease_age_seconds: None,
            assignments: inputs.runner_assignments,
        },
        pool: pool_snapshot,
        vms: {
            // Host sampler is stubbed until the cgroup parser lands;
            // the registry is the source of truth for configured counts
            // and will be populated by RunnerPool on create/fork.
            let caps = std::collections::HashMap::new();
            preloop_observability::vm_telemetry::build_fleet_snapshot(
                observability.vm_registry(),
                None,
                caps,
            )
        },
        store: StoreSnapshot {
            backend: store_backend,
            ..Default::default()
        },
        storage: {
            StorageSnapshot {
                state_dir: state_dir.display().to_string(),
                state_fs_free_bytes: None,
                state_fs_free_ratio: None,
                components: storage_components,
                last_gc_at: None,
            }
        },
        limits: Vec::new(),
        tasks: Vec::new(),
        github: github_snapshot,
        debug: DebugSnapshot {
            active_sessions: debug_active_sessions,
            oldest_session_seconds: debug_oldest_session_seconds,
        },
        telemetry: TelemetrySnapshot {
            otlp_enabled: observability.otlp_enabled(),
            ..Default::default()
        },
        conditions: {
            conditions.extend(webhook_conditions(
                &webhook,
                crate::webhook_status::now_us(),
            ));
            // Host memory pressure from a fresh sample (microseconds of
            // /proc reads on a path that already blocks for worse).
            // Thresholds are RAM fractions; swap counts as pressure via its
            // own warning, never as headroom against the critical line.
            let host = preloop_observability::vm_telemetry::sample_host();
            let ram_used = host.ram_used_fraction().unwrap_or(0.0);
            let swap_used = host.swap_used_fraction().unwrap_or(0.0);
            if ram_used >= 0.93 {
                conditions.push(Condition {
                    code: "host_memory_critical".to_owned(),
                    severity: "error".to_owned(),
                    message: format!(
                        "host RAM {:.0}% consumed; OOM kills are imminent, shed load",
                        ram_used * 100.0
                    ),
                    exemplars: Vec::new(),
                });
            } else if ram_used >= 0.85 || swap_used >= 0.25 {
                conditions.push(Condition {
                    code: "host_memory_pressure".to_owned(),
                    severity: "warning".to_owned(),
                    message: format!(
                        "host memory pressure: RAM {:.0}% consumed, swap {:.0}% consumed",
                        ram_used * 100.0,
                        swap_used * 100.0
                    ),
                    exemplars: Vec::new(),
                });
            }
            conditions
        },
    }
}

/// Per-component bytes for the state dir. The `metadata` reads are
/// synchronous filesystem access; the sampler runs this in
/// `spawn_blocking` so a stalled state filesystem can never stall the async
/// executor (request handling, shutdown). A recursive walk would belong on
/// the 60s cadence and must never run under the state lock.
fn collect_storage_components(
    state_dir: &std::path::Path,
) -> Vec<preloop_observability::status::StorageComponent> {
    let component =
        |name: &str, path: std::path::PathBuf| preloop_observability::status::StorageComponent {
            store: name.to_string(),
            bytes: std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0),
        };
    // Only the database is a single file. `cache` and `artifacts` are
    // directories, whose `metadata().len()` is the inode size, not the
    // contents size; they need the recursive walk on the 60s cadence.
    vec![component("database", state_dir.join("preloop.db"))]
}

/// Build and publish an operational snapshot from live state. The 5s tick and
/// the shutdown publish share this path so the final snapshot cannot drift
/// from the regular cadence; the startup seed builds the same content
/// synchronously via the same collector + builder.
async fn publish_snapshot(
    shared: &SharedState,
    store_backend: &preloop_observability::status::StoreBackend,
    shutdown_requested: bool,
) {
    let debug = {
        let inner = shared.state.inner.lock().await;
        debug_session_scalars(&inner)
    };
    let inputs = read_snapshot_inputs(&shared.state).await;
    let pool_snapshot = shared.state.pool_status.snapshot();
    let github_snapshot = shared.state.status_snapshot.read().github.clone();
    // The storage bytes are a synchronous filesystem read; run it on the
    // blocking pool so a stalled state filesystem cannot stall request
    // handling or shutdown on the executor.
    let state_dir = shared.state.state_dir.clone();
    let state_dir_for_meta = state_dir.clone();
    let storage_components =
        tokio::task::spawn_blocking(move || collect_storage_components(&state_dir_for_meta))
            .await
            .unwrap_or_default();
    let webhook = collect_webhook_condition_inputs(&shared.state);
    let snap = build_operational_snapshot_sync(
        inputs,
        debug,
        pool_snapshot,
        &shared.state.observability,
        shared.state.started_at,
        shutdown_requested,
        shared.state.scheduler.is_some(),
        &state_dir,
        storage_components,
        github_snapshot,
        store_backend.clone(),
        webhook,
    );
    *shared.state.status_snapshot.write() = snap;
}

/// Read the repair layers' published state plus the queue counters.
///
/// A publisher that has not run yet leaves the counters absent rather than
/// zero: the snapshot must not claim an empty queue (and so report no
/// dead-letter warning) merely because nothing has read the store. The queue
/// worker publishes them, so the 5s tick never takes the store's connection.
fn collect_webhook_condition_inputs(state: &AppState) -> WebhookConditionInputs {
    WebhookConditionInputs {
        stats: state.webhook_status.queue_stats(),
        watchdog: state.webhook_status.watchdog(),
        breaker: state.github_breaker.snapshot(),
        app_config: state.webhook_status.app_config_snapshot().0,
    }
}

async fn run_state_sampler(
    shared: Arc<SharedState>,
    store_backend: preloop_observability::status::StoreBackend,
) {
    let heartbeat = shared.state.observability.heartbeat().clone();
    let _handle = heartbeat.register(
        "state_sampler",
        preloop_observability::Criticality::Critical,
    );
    heartbeat.beat("state_sampler");
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    // A tick missed during a coalescing nap delays rather than bursting:
    // publishing twice back-to-back reports the same counters twice.
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Immediate sample then every 5s.
    interval.tick().await;
    // When the last tick ran: the burst-reactive early tick below must not
    // turn a submit storm back into per-operation counting.
    let mut last_tick = std::time::Instant::now();
    loop {
        let mut early = false;
        let mut shutdown = false;
        tokio::select! {
            _ = interval.tick() => {}
            // A submit/claim/complete/cancel wakes the sampler early so the
            // pool sees a burst in ~ms instead of at the next 5s tick. This
            // is the sampler's own channel: runner waiters on
            // `message_notify` are woken with per-job `notify_one` permits
            // that a parked sampler here would otherwise steal.
            _ = shared.state.sampler_notify.notified() => { early = true; }
            _ = shared.shutdown.cancelled() => { shutdown = true; }
        }
        if shutdown {
            // Publish one last snapshot with the shutdown flag set so
            // /api/v1/status reports `overall: shutting_down` and
            // `shutdown_requested: true` while /healthz//readyz already
            // 503 — without this the flag would only land on the next 5s
            // tick that never comes.
            heartbeat.beat("state_sampler");
            publish_snapshot(&shared, &store_backend, true).await;
            break;
        }
        if early
            && let Some(nap) = Duration::from_secs(1).checked_sub(last_tick.elapsed())
            && !nap.is_zero()
        {
            // Inside the 1s floor: don't drop the event — nap off the rest
            // of the floor, then sample below. A `continue` here meant a
            // change during the floor was invisible until the next 5s tick.
            // Wakes during the nap need no flag: the sample that follows
            // counts everything already.
            tokio::select! {
                _ = tokio::time::sleep(nap) => {}
                _ = shared.shutdown.cancelled() => {
                    heartbeat.beat("state_sampler");
                    publish_snapshot(&shared, &store_backend, true).await;
                    break;
                }
            }
        }
        last_tick = std::time::Instant::now();
        heartbeat.beat("state_sampler");
        sample_state_once(&shared, &store_backend).await;
    }
}

/// One sampler tick: publish the snapshot, then refresh the pool/queue
/// gauges and host-memory metrics from it. The loop calls this on every
/// tick/early-wake; tests call it directly (`test_sample_state_once`) since
/// no sampler task runs under `app()`.
pub(crate) async fn sample_state_once(
    shared: &Arc<SharedState>,
    store_backend: &preloop_observability::status::StoreBackend,
) {
    publish_snapshot(shared, store_backend, false).await;
    // Record pool/queue gauges into OTel instruments so `/metrics` has a
    // single exposition source (the SDK renderer).
    let s = shared.state.status_snapshot.read();
    // The co-hosted runner pool scales off this atomic; it used to be
    // refreshed by every submit/claim/complete (a full count(*) each). The
    // sampler's grouped count is the same number at a fixed cadence, plus a
    // burst-reactive early tick in the loop above.
    shared
        .state
        .queue_depth
        .store(s.jobs.ready as usize, std::sync::atomic::Ordering::Release);
    shared.state.pool_status.set_queue_depth(s.jobs.ready);
    shared.state.observability.metrics().pool.record(
        s.service.uptime_seconds,
        s.pool.desired as u64,
        s.pool.preparing,
        s.pool.idle as u64,
        s.pool.busy as u64,
        s.jobs.ready as u64,
        s.jobs.claimable as u64,
        s.jobs.unclaimable as u64,
        s.jobs.dependency_blocked as u64,
    );
    // Host memory reality on the same cadence: a /proc scan is microseconds
    // next to everything else on this tick. Recorded even when nothing else
    // changed so OOM proximity is a continuous signal, not a sampled one.
    shared
        .state
        .observability
        .metrics()
        .host
        .record(&preloop_observability::vm_telemetry::sample_host());
}

fn is_routine_unix_disconnect(error: &(dyn std::error::Error + 'static)) -> bool {
    error
        .downcast_ref::<hyper::Error>()
        .is_some_and(|error| error.is_shutdown() || error.is_incomplete_message())
}

/// Start the server and block until shutdown.
pub async fn serve(config: ServerConfig) -> anyhow::Result<()> {
    // Mutual exclusion: a second `preloop serve` on the same state dir must
    // fail fast, not overwrite the unix socket + PID file and orphan the
    // path the guest bridges mount. The lock file is held for the process
    // lifetime; the OS releases it on exit, so a crash leaves no stale lock.
    let _serve_lock = acquire_serve_lock(&config.state_dir)?;
    let mut state = AppState::new_with_store(
        config.state_dir.clone(),
        crate::config::config_path(),
        config.store_url.as_deref(),
    )
    .await?;
    // Wire observability if supplied (CLI/server will pass its handle).
    // Adopt the caller's handle when supplied; `AppState::new` already
    // installed a no-op one otherwise. Either way the store is instrumented,
    // so `preloop.store.operation.duration` is recorded even for the
    // standalone `preloop-server` binary, which passes no handle.
    if let Some(obs) = config.observability.clone() {
        state.observability = obs;
    }
    // Warm the static-PAT OAuth scope cache once at startup. Job expansion
    // is synchronous and cannot introspect the PAT itself, so without this the
    // first expansions after a restart would find a cold cache and withhold the
    // credential. The PAT is read from config/env during state construction and
    // is fixed for the process lifetime, so one lookup here covers every later
    // expansion. A failure is logged, not fatal: the run path withholds the PAT
    // rather than embedding one whose bounds could not be established.
    if state.github_app.is_none()
        && let Some(pat) = state.static_github_pat()
    {
        crate::runs::warm_pat_scope_cache(&pat).await;
    }
    // Resolve the effective store URL exactly once, mirroring `open_store`
    // precedence: the explicit URL wins, then the environment, then SQLite at
    // the state dir. Both the instrumentation label and the status-snapshot
    // backend are derived from this single parsed value so they cannot
    // drift — a stale `PRELOOP_STORE_URL=postgres://…` must not mislabel
    // every SQLite operation.
    let store_backend = {
        let effective_url = config
            .store_url
            .clone()
            .or_else(|| std::env::var(crate::store::STORE_URL_ENV).ok())
            .unwrap_or_default();
        let store_url = crate::store::parse_store_url(&effective_url);
        match &store_url {
            Ok(crate::store::StoreUrl::Postgres(_)) => {
                preloop_observability::status::StoreBackend::Postgres
            }
            _ => preloop_observability::status::StoreBackend::Sqlite,
        }
    };
    if let Some(ps) = config.pool_status.clone() {
        state.pool_status = ps;
        state.backend.set_pool_status((*state.pool_status).clone());
    }
    // Ensure uptime base is now (AppState::new set it, but re-arm after store load).
    state.started_at = std::time::Instant::now();
    // Retention is otherwise enforced only when a run completes; a quiet
    // server with no finishing runs would keep expired caches indefinitely.
    // Sweep once at startup so expiry does not depend on future completions.
    if state.checkout_cache.mode != crate::config::CheckoutCacheMode::Off {
        let state_dir = state.state_dir.clone();
        let checkout_cache = state.checkout_cache.clone();
        tokio::spawn(async move {
            crate::snapshots::prune_checkout_cache(&state_dir, &checkout_cache).await;
        });
    }
    // Retention sweep once at startup: runs that aged past the retention
    // window while the server was down should not wait a full interval.
    // Runs hourly afterwards (see the retention sweeper spawn below). A
    // fresh token is fine: the one-shot pass never watches for shutdown.
    {
        let startup_state = state.clone();
        tokio::spawn(async move {
            let shared = Arc::new(SharedState {
                state: startup_state,
                shutdown: CancellationToken::new(),
            });
            crate::retention::sweep_once(&shared).await;
        });
    }
    let queue = state.backend.queue_stats().await.unwrap_or_default();
    if let Some(queue_depth) = config.queue_depth.clone() {
        // A co-hosted pool shares this atomic to scale to demand. The state
        // sampler refreshes it every 5s (immediate first tick), and the
        // constructor already seeded it from the recovered store — no
        // per-operation re-arm needed.
        state.queue_depth = queue_depth;
    }
    if let Some(next_job_runs_on) = config.next_job_runs_on.clone() {
        state.next_job_runs_on = next_job_runs_on;
        if let Ok(v) = state.next_job_runs_on.read() {
            state.pool_status.set_next_job_runs_on(v.clone());
        }
    }
    {
        let labels = queue.next_runs_on;
        if let Ok(mut guard) = state.next_job_runs_on.write() {
            *guard = labels;
        }
        if state.pool_status.snapshot().next_job_runs_on.is_empty()
            && let Ok(v) = state.next_job_runs_on.read()
        {
            state.pool_status.set_next_job_runs_on(v.clone());
        }
    }
    {
        let pool_managed = config.pending_registrations.is_some();
        if let Some(pending_registrations) = config.pending_registrations.clone() {
            state.pending_registrations = pending_registrations;
            // Mirror into consolidated handle for sampler visibility
            if let Ok(map) = state.pending_registrations.read() {
                for (k, v) in map.iter() {
                    state.pool_status.insert_pending(k.clone(), *v);
                }
            }
        }
        // Read the node-local liveness timeout under the lock, then release it:
        // `rebuild_dispatch_intent` runs a transaction per ready job and must
        // not hold the global `inner` mutex while awaiting the backend.
        let runner_liveness_timeout = state.inner.lock().await.runner_liveness_timeout;
        // `pool_assignments_enabled`/`require_job_assignments`: the
        // authoritative backend reads these flags from its own config on
        // every transaction (`with_config`); mirror the effective values into
        // it now that the real server config is known. `runner_liveness_timeout`
        // is not part of the bootstrap config — keep the value `open` seeded.
        state.backend.set_config(
            pool_managed,
            config.require_job_assignments,
            runner_liveness_timeout,
        );
        // Environment protection rules are pure config the backend evaluates
        // inside its promotion and reaper transactions (where the job rows
        // live). Hand the effective rules over now that the real server
        // config is known; empty rules keep every gate open.
        state
            .backend
            .set_environment_rules(std::sync::Arc::new(state.environment_rules.clone()));
        // Imported/persisted ready jobs were enqueued under the recovered
        // (default) config, so `on_job_enqueued` may not have run for them.
        // Now that the effective config is live, rebuild dispatch intent for
        // every ready job — `on_job_enqueued` is idempotent and config-gated,
        // so this only fills assignments/pool_pending the startup config
        // actually requires.
        if let Err(error) = state.backend.rebuild_dispatch_intent().await {
            warn!(?error, "rebuilding dispatch intent at boot failed");
        }
    }
    if let Some(pool_preparing) = config.pool_preparing.clone() {
        state.pool_preparing = Some(pool_preparing.clone());
        if pool_preparing.load(std::sync::atomic::Ordering::Acquire) {
            state.pool_status.set_preparing(true);
        }
    }
    // Seed the initial snapshot from the recovered state so /readyz and
    // /api/v1/status have real data before the first 5s tick: a restart with
    // persisted queued/in-progress runs must not report an empty status for
    // the first interval. Runs after the pool-status wiring above so the
    // seeded pool section already carries the initialized queue depth,
    // next-job labels, pending registrations and preparing flag instead of
    // waiting for the first 5s tick to mirror them.
    {
        // The queue counters are read from the store once here, so the seeded
        // snapshot reports real webhook depth and the queue worker only has to
        // keep the cache fresh from then on.
        crate::github::refresh_webhook_queue_stats(&state).await;
        let debug = {
            let inner = state.inner.lock().await;
            debug_session_scalars(&inner)
        };
        let inputs = read_snapshot_inputs(&state).await;
        let init = build_operational_snapshot_sync(
            inputs,
            debug,
            state.pool_status.snapshot(),
            &state.observability,
            state.started_at,
            false,
            state.scheduler.is_some(),
            &state.state_dir,
            // Startup-time read, before the server accepts requests; the
            // 5s tick performs the same read on the blocking pool instead.
            collect_storage_components(&state.state_dir),
            preloop_observability::status::GithubSnapshot {
                configured: state.github_app.is_some(),
                ..Default::default()
            },
            store_backend.clone(),
            collect_webhook_condition_inputs(&state),
        );
        *state.status_snapshot.write() = init;
    }
    if !config.listen.ip().is_loopback()
        && state.registration_policy == RegistrationPolicy::Permissive
    {
        anyhow::bail!("PRELOOP_REGISTRATION_POLICY=permissive is only allowed on loopback");
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
    // Heartbeat for scheduler scan (critical) if enabled — beat periodically.
    let scheduler_heartbeat = state.observability.heartbeat().clone();
    if config.enable_scheduler {
        let scheduler = crate::scheduler::Scheduler::new();
        state.scheduler = Some(scheduler.clone());
        let shared_for_scan = Arc::new(SharedState {
            state: state.clone(),
            shutdown: shutdown.clone(),
        });
        let scheduler_clone = scheduler.clone();
        // The scheduler heartbeat must prove the startup scan progressed, not
        // that an unrelated timer is awake. The scan tasks beat it per
        // workflow file and deregister on completion: a scan that hangs
        // stops beating and `/readyz` goes 503; a completed scan is
        // not a stale critical task. A panic preserves the handle as failed,
        // so `/readyz` remains unhealthy instead of losing the task entry.
        let scan_hb = scheduler_heartbeat.clone();
        if let Some(workspace) = state.local_workspace.clone() {
            let shared_for_scan = shared_for_scan.clone();
            tokio::spawn(async move {
                let handle = scan_hb.register(
                    "scheduler_scan",
                    preloop_observability::Criticality::Critical,
                );
                scheduler_clone
                    .scan_workspace(&workspace, shared_for_scan, Some(handle))
                    .await;
            });
        } else {
            let shared_for_scan = shared_for_scan.clone();
            tokio::spawn(async move {
                let handle = scan_hb.register(
                    "scheduler_scan",
                    preloop_observability::Criticality::Critical,
                );
                scheduler_clone
                    .scan_remote(shared_for_scan, Some(handle))
                    .await;
            });
        }
    }
    // Read back the App's webhook event subscription at startup. A new
    // App created from the manifest gets the expanded default events, but an
    // App created earlier — or narrowed by hand — may miss trigger events,
    // and GitHub cannot change a subscription through the API. Warn loudly
    // so the operator ticks the missing events in App settings.
    if let Some(app) = state.github_app.clone() {
        let app_id = app.app_id.clone();
        tokio::spawn(async move {
            match app.read_app_subscription().await {
                Ok(subscription) => {
                    crate::github_app::warn_missing_trigger_events(&app_id, &subscription)
                }
                Err(error) => warn!(
                    app_id,
                    ?error,
                    "could not read back the GitHub App's event subscription at startup"
                ),
            }
        });
    }
    // Additional registered Apps (`github.apps` / `PRELOOP_GITHUB_APPS_JSON`)
    // get the same startup read-back. The legacy default App — always the
    // registry's `default_index` — is already covered by the branch above.
    if let Some(registry) = state.github_apps.as_ref() {
        for (index, app) in registry.apps.iter().enumerate() {
            if index == registry.default_index {
                continue;
            }
            let app = app.clone();
            tokio::spawn(async move {
                let app_id = app.app_id.clone();
                match app.read_app_subscription().await {
                    Ok(subscription) => {
                        crate::github_app::warn_missing_trigger_events(&app_id, &subscription)
                    }
                    Err(error) => warn!(
                        app_id,
                        ?error,
                        "could not read back the GitHub App's event subscription at startup"
                    ),
                }
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
        state: state.clone(),
        shutdown: shutdown.clone(),
    });
    let check_run_sender_shared = shared.clone();
    tokio::spawn(async move {
        crate::github::run_check_run_sender(check_run_sender_shared).await;
    });

    // 5s sampler — clone needed state under lock, release, then publish.
    let sampler_shared = shared.clone();
    tokio::spawn(async move {
        run_state_sampler(sampler_shared, store_backend).await;
    });

    let checker_shared = shared.clone();
    tokio::spawn(async move {
        run_background_reaper(checker_shared).await;
    });
    let cache_pruner_shared = shared.clone();
    tokio::spawn(async move {
        run_checkout_cache_pruner(cache_pruner_shared).await;
    });
    // Hourly run/check/status retention sweep (GitHub Actions retention
    // setting, default 90 days; `retention_days = 0` disables it).
    let retention_sweeper_shared = shared.clone();
    tokio::spawn(async move {
        crate::retention::run_retention_sweeper(retention_sweeper_shared).await;
    });

    // Snapshots orphaned by a restart inside their retention window are
    // otherwise never collected: the discard timer is in-process.
    let snapshot_sweep_shared = shared.clone();
    tokio::spawn(async move {
        crate::snapshots::sweep_workspace_snapshots(&snapshot_sweep_shared).await;
    });
    let archive_shared = shared.clone();
    tokio::spawn(async move { run_history_archiver(archive_shared).await });
    let flusher_shared = shared.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        interval.tick().await;
        while !flusher_shared.shutdown.is_cancelled() {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(error) = flusher_shared.state.log_segments.flush_stale().await {
                        tracing::warn!(%error, "failed to flush live-log segments");
                    }
                }
                _ = flusher_shared.shutdown.cancelled() => {
                    break;
                }
            }
        }
        if let Err(error) = flusher_shared.state.log_segments.flush_all().await {
            tracing::error!(%error, "failed to flush live-log segments on shutdown");
        }
    });

    let webhook_worker_heartbeat = state.observability.heartbeat().clone();
    let webhook_worker_handle = webhook_worker_heartbeat.register(
        "webhook_queue_worker",
        preloop_observability::Criticality::Critical,
    );
    let webhook_worker_shared = shared.clone();
    tokio::spawn(async move {
        crate::github::run_webhook_queue_worker(webhook_worker_shared, webhook_worker_handle).await;
    });

    // Repair layers. None of them is on the request path, and each is
    // independently disable-able, so they are best-effort tasks rather than
    // critical heartbeats: a stalled watchdog must not fail `/readyz` while
    // the queue itself is draining fine. Their staleness is reported through
    // the operational snapshot instead.
    let watchdog_shared = shared.clone();
    tokio::spawn(async move {
        crate::webhook_watchdog::run_webhook_watchdog(watchdog_shared).await;
    });
    let health_shared = shared.clone();
    tokio::spawn(async move {
        crate::webhook_health::run_webhook_health_monitor(health_shared).await;
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
            warn!(
                "self-signed cert -- runner needs --ss-skip-tls-verify or GITHUB_ACTIONS_RUNNER_SKIP_TLS_VERIFY=1"
            );
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

/// Exclusive `flock` on `<state_dir>/serve.lock` for the process lifetime.
///
/// A second `preloop serve` on the same state dir would overwrite the unix
/// socket and PID file, then die — leaving the socket path pointing at a
/// dead inode while the first engine keeps running on the old one. Guests
/// mount the socket by path, so every bridge connection hits the dead
/// socket and the runner goes deaf. The lock makes the second instance
/// fail fast instead.
///
/// Returns the open file so the caller holds the lock; the OS releases it
/// on process exit, so a crash leaves no stale lock behind.
#[cfg(unix)]
fn acquire_serve_lock(state_dir: &std::path::Path) -> anyhow::Result<std::fs::File> {
    let lock_path = state_dir.join("serve.lock");
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        // The lock file carries no content; opening it must never clobber it.
        .truncate(false)
        .open(&lock_path)
        .context(format!("opening serve lock {}", lock_path.display()))?;
    // try_lock: fail fast rather than queue behind a live engine — a queued
    // second serve would still overwrite the socket the moment the first
    // exits, which is exactly the race this prevents.
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            anyhow::bail!(
                "another preloop serve already holds {} — \
                 refusing to start a second engine on the same state dir",
                lock_path.display()
            );
        }
        Err(error @ std::fs::TryLockError::Error(_)) => return Err(error.into()),
    }
    Ok(file)
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
    #[test]
    fn status_degrades_when_claimable_work_waits_behind_idle_runner() {
        let temp = tempfile::tempdir().unwrap();
        let inputs = SnapshotInputs {
            queue_len: 1,
            runner_idle: 1,
            oldest_ready_seconds: Some(61.0),
            oldest_ready_run_id: Some("run-1".to_owned()),
            oldest_ready_job_id: Some("build".to_owned()),
            queue_runner_reqs: vec![(vec!["self-hosted".to_owned()], None)],
            runner_caps: vec![RunnerCapabilities {
                known: true,
                labels: vec!["self-hosted".to_owned()],
                ..Default::default()
            }],
            ..Default::default()
        };
        let snapshot = build_operational_snapshot_sync(
            inputs,
            (0, None),
            Default::default(),
            &preloop_observability::Observability::noop(),
            std::time::Instant::now(),
            false,
            true,
            temp.path(),
            Vec::new(),
            Default::default(),
            Default::default(),
            Default::default(),
        );
        assert_eq!(
            snapshot.overall,
            preloop_observability::status::Overall::Degraded
        );
        assert_eq!(snapshot.conditions[0].code, "claimable_queue_stalled");
        assert_eq!(
            snapshot.conditions[0].exemplars[0].run_id.as_deref(),
            Some("run-1")
        );
    }

    #[test]
    fn status_degrades_when_a_run_is_in_progress_with_nothing_executing_it() {
        let temp = tempfile::tempdir().unwrap();
        let inputs = SnapshotInputs {
            runs_in_progress: 1,
            orphaned_run_ids: vec!["run-orphan".to_owned()],
            ..Default::default()
        };
        let snapshot = build_operational_snapshot_sync(
            inputs,
            (0, None),
            Default::default(),
            &preloop_observability::Observability::noop(),
            std::time::Instant::now(),
            false,
            true,
            temp.path(),
            Vec::new(),
            Default::default(),
            Default::default(),
            Default::default(),
        );
        assert_eq!(
            snapshot.overall,
            preloop_observability::status::Overall::Degraded
        );
        assert_eq!(
            snapshot.conditions[0].code,
            "run_in_progress_without_execution"
        );
        assert_eq!(
            snapshot.conditions[0].exemplars[0].run_id.as_deref(),
            Some("run-orphan")
        );
    }
    #[test]
    fn status_degrades_after_latest_github_check_update_fails() {
        let temp = tempfile::tempdir().unwrap();
        let github = preloop_observability::status::GithubSnapshot {
            configured: true,
            last_check_failure_at: Some(chrono::Utc::now()),
            ..Default::default()
        };
        let snapshot = build_operational_snapshot_sync(
            Default::default(),
            (0, None),
            Default::default(),
            &preloop_observability::Observability::noop(),
            std::time::Instant::now(),
            false,
            true,
            temp.path(),
            Vec::new(),
            github,
            Default::default(),
            Default::default(),
        );
        assert_eq!(
            snapshot.overall,
            preloop_observability::status::Overall::Degraded
        );
        assert_eq!(snapshot.conditions[0].code, "github_check_update_failure");
    }
}
