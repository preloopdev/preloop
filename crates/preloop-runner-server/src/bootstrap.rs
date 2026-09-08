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

pub(crate) async fn reap_once(shared: &Arc<SharedState>) {
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
        // F7: drop pending cache/artifact uploads and download tokens older than
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
    // or booting a runner it cannot register a runner no matter how long the
    // job waits, so the clock is reset for the whole warm: a job queued
    // mid-warm gets a full grace window once provisioning actually starts.
    // The reset is bounded by MAX_QUEUED_GRACE (see below) so continuous
    // provisioning cannot pause the clock forever.
    const QUEUED_JOB_GRACE: Duration = Duration::from_secs(120);
    // Absolute backstop, measured from ready-enqueue, on how long
    // provisioning/preparing may pause a job's starvation clock. It protects
    // a job whose runner is genuinely on the way, but keeps continuous
    // successor prebuilds or a provision that fails and retries forever from
    // masking an unschedulable job (bad `runs-on`, or a persistently broken
    // provision) indefinitely.
    const MAX_QUEUED_GRACE: Duration = Duration::from_secs(600);
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
        let grace = if pool_preparing {
            // The pool is warming or booting a runner that may serve this
            // job, so hold the grace window rather than failing a job whose
            // runner is genuinely on the way. Bound the hold by
            // MAX_QUEUED_GRACE measured from ready-enqueue: once a job has
            // waited that long it starves even while the pool is still
            // preparing, so sustained provisioning cannot mask it forever.
            let enqueued = if job.enqueued_at_unix_nanos > 0 {
                SystemTime::UNIX_EPOCH + Duration::from_nanos(job.enqueued_at_unix_nanos as u64)
            } else {
                now
            };
            if now.duration_since(enqueued).unwrap_or_default() < MAX_QUEUED_GRACE {
                inner.queued_at.remove(&key);
                continue;
            }
            MAX_QUEUED_GRACE
        } else {
            let first_seen = *inner.queued_at.entry(key.clone()).or_insert(now);
            if now.duration_since(first_seen).unwrap_or_default() < QUEUED_JOB_GRACE {
                continue;
            }
            QUEUED_JOB_GRACE
        };
        let reason = format!(
            "no runner is registered for `runs-on: {}` and none appeared \
             within {}s, so the job cannot be scheduled",
            job.runs_on.join(", "),
            grace.as_secs()
        );
        tracing::warn!(
            run_id = %job.run_id,
            job_id = %job.job_id.0,
            labels = ?job.runs_on,
            "starving queued job failed after {}s without a matching runner",
            grace.as_secs()
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
                        // The lease that expired names the attempt exactly.
                        agent_job_id: inner
                            .job_requests
                            .get(&request_id)
                            .map(|record| record.agent_job_id),
                        status: ExecutionStatus::Failure,
                        outputs: Default::default(),
                        annotations: Vec::new(),
                        step_results: Vec::new(),
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
    //Heartbeat for reaper (critical) — beat each interval, no cadence change.
    let heartbeat = shared.state.observability.heartbeat().clone();
    let _reaper_handle = heartbeat.register("reaper", preloop_observability::Criticality::Critical);
    heartbeat.beat("reaper");

    while !shared.shutdown.is_cancelled() {
        tokio::select! {
            _ = interval.tick() => {
                heartbeat.beat("reaper");
                reap_once(&shared).await;
            }
            _ = shared.shutdown.cancelled() => {
                break;
            }
        }
    }
}

/// Everything the operational snapshot reads from `inner`, collected under a
/// single lock acquisition. The 5s sampler and the startup seed after a store
/// restore both build their snapshots from this, so the two cannot drift.
struct SnapshotInputs {
    queue_len: usize,
    pending_jobs_len: usize,
    pending_expansions_len: usize,
    expanding_len: usize,
    runs_queued: u32,
    runs_in_progress: u32,
    runs_completed: u32,
    registered: u32,
    /// Distinct session ids across the modern broker map and the legacy map
    /// (every session creation path inserts into both, so the sum would
    /// double count).
    sessions: u32,
    runner_idle: u32,
    runner_busy: u32,
    runner_stale: u32,
    /// Per queued job, in queue order: the `runs-on` labels and any explicit
    /// runner group. Only this label surface is needed for claimability —
    /// never the full message payload.
    queue_runner_reqs: Vec<(Vec<String>, Option<String>)>,
    /// Capabilities (labels + group identity) of every registered runner.
    runner_caps: Vec<RunnerCapabilities>,
    debug_active_sessions: u32,
    debug_oldest_session_seconds: Option<f64>,
    /// Jobs parked behind job-level concurrency gates (FIFO).
    concurrency_blocked: u32,
    /// Concurrency-group admission state, derived from
    /// `inner.concurrency_groups`. Queue-mode limits and overflow counts are
    /// not tracked in group state and stay zero.
    concurrency_groups_active: u32,
    concurrency_groups_contended: u32,
    concurrency_pending_holders: u32,
    concurrency_deepest_group_pending: u32,
}

/// Collect [`SnapshotInputs`] from `inner` while the state lock is held.
fn collect_snapshot_inputs(inner: &InnerState) -> SnapshotInputs {
    let mut runs_queued = 0u32;
    let mut runs_in_progress = 0u32;
    let mut runs_completed = 0u32;
    for run in inner.runs.values() {
        match run.status {
            ExecutionStatus::Queued => runs_queued += 1,
            ExecutionStatus::InProgress => runs_in_progress += 1,
            s if s.is_terminal() => runs_completed += 1,
            _ => {}
        }
    }
    let session_ids: std::collections::BTreeSet<&String> = inner
        .broker_session_runners
        .keys()
        .chain(inner.sessions.keys())
        .collect();

    // Runner state: busy = an owned session holds an active job assignment;
    // stale = no poll within the crate's staleness threshold; idle = neither.
    // Busy and stale are independent predicates — a runner that stopped
    // polling mid-job is both — so idle is the remainder, not the complement.
    let now = std::time::Instant::now();
    let mut runner_idle = 0u32;
    let mut runner_busy = 0u32;
    let mut runner_stale = 0u32;
    for runner_id in inner.runners.keys() {
        let mut owned: std::collections::BTreeSet<&String> = inner
            .broker_session_runners
            .iter()
            .filter(|(_, owner)| **owner == *runner_id)
            .map(|(session_id, _)| session_id)
            .collect();
        owned.extend(
            inner
                .sessions
                .iter()
                .filter(|(_, session)| session.runner_id == *runner_id)
                .map(|(session_id, _)| session_id),
        );
        let busy = owned
            .iter()
            .any(|session_id| inner.session_active_requests.contains_key(*session_id));
        // Sessions restored from a restart have no last-seen entry; like the
        // liveness sweep, treat an unknown poll time as not-yet-stale rather
        // than asserting staleness we cannot observe.
        let stale = !owned.is_empty()
            && owned.iter().all(|session_id| {
                inner
                    .session_last_seen
                    .get(*session_id)
                    .is_some_and(|seen| now.duration_since(*seen) > STALENESS_THRESHOLD)
            });
        if busy {
            runner_busy += 1;
        }
        if stale {
            runner_stale += 1;
        }
        if !busy && !stale {
            runner_idle += 1;
        }
    }

    // Live debug sessions: count open sessions and the age of the oldest.
    let debug_sessions = inner.debug_sessions.list();
    let debug_oldest_session_seconds = debug_sessions
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

    // Concurrency-group admission state: one group per (repo, group) key,
    // holding at most one running holder plus a FIFO pending queue.
    let mut concurrency_groups_active = 0u32;
    let mut concurrency_groups_contended = 0u32;
    let mut concurrency_pending_holders = 0u32;
    let mut concurrency_deepest_group_pending = 0u32;
    for group in inner.concurrency_groups.values() {
        if group.running.is_some() {
            concurrency_groups_active += 1;
        }
        if !group.pending.is_empty() {
            concurrency_groups_contended += 1;
        }
        concurrency_pending_holders += group.pending.len() as u32;
        concurrency_deepest_group_pending =
            concurrency_deepest_group_pending.max(group.pending.len() as u32);
    }

    SnapshotInputs {
        queue_len: inner.queue.len(),
        pending_jobs_len: inner.pending_jobs.len(),
        pending_expansions_len: inner.pending_expansions.len(),
        expanding_len: inner.expanding.len(),
        runs_queued,
        runs_in_progress,
        runs_completed,
        registered: inner.runners.len() as u32,
        sessions: session_ids.len() as u32,
        runner_idle,
        runner_busy,
        runner_stale,
        queue_runner_reqs: inner
            .queue
            .iter()
            .map(|job| (job.runs_on.clone(), job.runner_group.clone()))
            .collect(),
        runner_caps: inner
            .runners
            .values()
            .map(crate::runtime_scheduling::capabilities_of)
            .collect(),
        debug_active_sessions: debug_sessions.len() as u32,
        debug_oldest_session_seconds,
        concurrency_blocked: inner.concurrency_blocked.len() as u32,
        concurrency_groups_active,
        concurrency_groups_contended,
        concurrency_pending_holders,
        concurrency_deepest_group_pending,
    }
}

fn build_operational_snapshot_sync(
    inputs: SnapshotInputs,
    pool_snapshot: preloop_observability::status::PoolSnapshot,
    observability: &preloop_observability::Observability,
    started_at: std::time::Instant,
    shutdown_requested: bool,
    scheduler_enabled: bool,
    state_dir: &std::path::Path,
    storage_components: Vec<preloop_observability::status::StorageComponent>,
    github_configured: bool,
    store_backend: preloop_observability::status::StoreBackend,
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

    let oldest_ready_seconds = None; // TODO: track queued_at

    OperationalSnapshot {
        schema_version: 1,
        observed_at: now,
        snapshot_age_seconds: 0.0,
        overall: if shutdown_requested {
            Overall::ShuttingDown
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
        github: GithubSnapshot {
            configured: github_configured,
            ..Default::default()
        },
        debug: DebugSnapshot {
            active_sessions: inputs.debug_active_sessions,
            oldest_session_seconds: inputs.debug_oldest_session_seconds,
        },
        telemetry: TelemetrySnapshot {
            otlp_enabled: observability.otlp_enabled(),
            ..Default::default()
        },
        conditions: Vec::new(),
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
    // Clone needed state under lock, release, then build.
    let inputs = {
        let inner = shared.state.inner.lock().await;
        collect_snapshot_inputs(&inner)
    };
    let pool_snapshot = shared.state.pool_status.snapshot();
    // The storage bytes are a synchronous filesystem read; run it on the
    // blocking pool so a stalled state filesystem cannot stall request
    // handling or shutdown on the executor.
    let state_dir = shared.state.state_dir.clone();
    let state_dir_for_meta = state_dir.clone();
    let storage_components =
        tokio::task::spawn_blocking(move || collect_storage_components(&state_dir_for_meta))
            .await
            .unwrap_or_default();
    let snap = build_operational_snapshot_sync(
        inputs,
        pool_snapshot,
        &shared.state.observability,
        shared.state.started_at,
        shutdown_requested,
        shared.state.scheduler.is_some(),
        &state_dir,
        storage_components,
        shared.state.github_app.is_some(),
        store_backend.clone(),
    );
    *shared.state.status_snapshot.write() = snap;
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
    // Immediate sample then every 5s.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                heartbeat.beat("state_sampler");
                publish_snapshot(&shared, &store_backend, false).await;
                // Record pool/queue gauges into OTel instruments so `/metrics`
                // has a single exposition source (the SDK renderer).
                {
                    let s = shared.state.status_snapshot.read();
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
                }
            }
            _ = shared.shutdown.cancelled() => {
                // Publish one last snapshot with the shutdown flag set so
                // /api/v1/status reports `overall: shutting_down` and
                // `shutdown_requested: true` while /healthz//readyz already
                // 503 — without this the flag would only land on the next 5s
                // tick that never comes.
                heartbeat.beat("state_sampler");
                publish_snapshot(&shared, &store_backend, true).await;
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
    // Wire observability if supplied (CLI/server will pass its handle).
    // Adopt the caller's handle when supplied; `AppState::new` already
    // installed a no-op one otherwise. Either way the store is instrumented,
    // so `preloop.store.operation.duration` is recorded even for the
    // standalone `preloop-server` binary, which passes no handle.
    if let Some(obs) = config.observability.clone() {
        state.observability = obs;
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
        // One decorator around the private `Store` trait — never per-backend
        // duplication. The backend label is bounded to sqlite|postgres.
        state.store = crate::store::InstrumentedStore::wrap(
            state.store.clone(),
            state.observability.clone(),
            match &store_url {
                Ok(crate::store::StoreUrl::Postgres(_)) => "postgres",
                _ => "sqlite",
            },
        );
        match &store_url {
            Ok(crate::store::StoreUrl::Postgres(_)) => {
                preloop_observability::status::StoreBackend::Postgres
            }
            _ => preloop_observability::status::StoreBackend::Sqlite,
        }
    };
    if let Some(ps) = config.pool_status.clone() {
        state.pool_status = ps;
    }
    // Ensure uptime base is now (AppState::new set it, but re-arm after store load).
    state.started_at = std::time::Instant::now();
    if let Some(queue_depth) = config.queue_depth.clone() {
        state.queue_depth = queue_depth;
        // The pool shares this same atomic and only forks a runner while it
        // is non-zero; a freshly restarted server has no runners yet, so
        // nothing will refresh it from a broker poll. Re-arm it with the
        // ready-queue size recovered from the store, or every job queued
        // before the restart sits forever with the pool asleep.
        state.queue_depth.store(
            state.inner.lock().await.queue.len(),
            std::sync::atomic::Ordering::Release,
        );
        state
            .pool_status
            .set_queue_depth(state.queue_depth.load(std::sync::atomic::Ordering::Acquire) as u32);
    }
    if let Some(next_job_runs_on) = config.next_job_runs_on.clone() {
        state.next_job_runs_on = next_job_runs_on;
        if let Ok(v) = state.next_job_runs_on.read() {
            state.pool_status.set_next_job_runs_on(v.clone());
        }
    }
    {
        let inner = state.inner.lock().await;
        crate::runtime_scheduling::sync_next_job_labels(&inner, &state.next_job_runs_on);
        if state.pool_status.snapshot().next_job_runs_on.is_empty() {
            if let Ok(v) = state.next_job_runs_on.read() {
                state.pool_status.set_next_job_runs_on(v.clone());
            }
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
        let mut inner = state.inner.lock().await;
        inner.pool_assignments_enabled = pool_managed;
        inner.require_job_assignments = config.require_job_assignments;
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
        let inputs = {
            let inner = state.inner.lock().await;
            collect_snapshot_inputs(&inner)
        };
        let init = build_operational_snapshot_sync(
            inputs,
            state.pool_status.snapshot(),
            &state.observability,
            state.started_at,
            false,
            state.scheduler.is_some(),
            &state.state_dir,
            // Startup-time read, before the server accepts requests; the
            // 5s tick performs the same read on the blocking pool instead.
            collect_storage_components(&state.state_dir),
            state.github_app.is_some(),
            store_backend.clone(),
        );
        *state.status_snapshot.write() = init;
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
    // Read back the App's webhook event subscription at startup (D7). A new
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

    // 5s sampler — clone needed state under lock, release, then publish.
    let sampler_shared = shared.clone();
    tokio::spawn(async move {
        run_state_sampler(sampler_shared, store_backend).await;
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
