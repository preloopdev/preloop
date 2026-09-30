use super::*;
// Backend error vocabulary; not part of the crate-root prelude.
use crate::control::ControlError;

// ─── Memory-hardening caps for authenticated-runner sinks ──────────────────
//
// Every sink an authenticated runner can grow across many `200 OK` requests
// (logs, timeline records/events, blob block assembly, pending upload maps)
// is bounded here. The conventions mirror the already-shipped live-log cap
// (`live_logs.rs::LiveLogBuffer`): a documented constant per sink, retention
// of the newest data past the cap (tail-drop/truncation) or a deterministic
// eviction, and a rejection status (413) when a single payload is too large
// on its own. The complete, permanent logs are the step/job-log blobs the
// runner uploads separately; the `logs`/`log_chunks` path here is only the
// live console stream, so these caps never drop real log data.

/// F1 — per-log retained byte cap. `append_log` keeps only the newest bytes
/// within this budget in `InnerState::logs`. The durable `log_chunks` copy is
/// the live-console recovery buffer (read only by a restart to refill this
/// map) and is bounded to the SAME budget in `store_log_chunk` (D2) — keeping
/// more on disk is pointless since a restart trims it back to this cap.
pub const MAX_LOG_BYTES_PER_KEY: usize = 16 * 1024 * 1024;

/// F1 — slack above `MAX_LOG_BYTES_PER_KEY` before the in-memory retained
/// buffer is trimmed. `Vec::drain(0..excess)` front-shifts the whole tail, so
/// trimming on every append (e.g. 64 KiB appends into a 16 MiB buffer) is
/// O(n) per append. Letting the buffer grow one slack window past the cap and
/// then trimming back to the cap amortizes the shift to O(1) per byte, at the
/// cost of at most one extra slack window of memory per key.
pub const LOG_KEY_TRIM_SLACK: usize = 1024 * 1024;

/// F1 — per-plan retained byte budget across all of a plan's logs. The oldest
/// logs of the plan are evicted once the total (bytes or entry count) exceeds
/// the budget, so a flood of distinct `log_id`s cannot grow the map either.
pub const MAX_LOG_BYTES_PER_PLAN: usize = 64 * 1024 * 1024;

/// F1 — per-plan retained log entry cap. Empty logs carry no bytes, so the
/// byte budget alone would let an attacker create unbounded distinct keys.
pub const MAX_LOGS_PER_PLAN: usize = 512;

/// F1 — global retained byte budget across all plans. Prevents a runner
/// from fabricating unlimited `plan_id` values to bypass the per-plan cap.
pub const MAX_LOG_BYTES_GLOBAL: usize = 256 * 1024 * 1024;

/// F1 — global retained log entry cap.
pub const MAX_LOGS_GLOBAL: usize = 4096;

/// F2 — per-timeline record cap. PATCH upserts beyond this evict the oldest
/// (deterministically first-keyed) records. Real jobs stay far below it.
pub const MAX_TIMELINE_RECORDS: usize = 1024;

/// F2 — per-timeline byte budget for stored records. Each record's
/// `currentOperation` can be ~1 MiB; count caps alone leave an unbounded
/// byte budget (1024 × 4096 × 1 MiB). Aggregate bytes are bounded here.
pub const MAX_TIMELINE_BYTES_PER_TIMELINE: usize = 8 * 1024 * 1024;

/// F3 — per-run ring-buffer cap for projected timeline events. The oldest
/// events are drained once the retained Vec exceeds this.
pub const MAX_TIMELINE_EVENTS: usize = 2048;

/// F2 — global bound on distinct timeline keys (`{plan}/{timeline}`), which a
/// runner controls directly. Oldest-keyed timelines are evicted wholesale
/// (records and change-id counter together) past the cap.
pub const MAX_TIMELINE_KEYS: usize = 4096;

/// F3 — global bound on distinct run ids in the timeline event map. A runner
/// can PATCH for fabricated plan ids, which would otherwise mint unbounded
/// per-run event buckets (each itself capped by [`MAX_TIMELINE_EVENTS`]).
pub const MAX_TIMELINE_EVENT_KEYS: usize = 4096;

/// F5 — per-block cap for staged blob blocks. actions/upload-artifact v4
/// stages 8 MiB blocks, but @actions/cache v6 stages 64 MiB blocks
/// (`uploadChunkSize`; user-overridable up to 128 MiB via
/// `CACHE_UPLOAD_CHUNK_SIZE`) for archives over its 128 MiB single-shot
/// threshold — see issue #292. Staged blocks are streamed to disk with the
/// cap enforced mid-stream, never buffered whole in memory, so the larger
/// cap cannot exhaust server RAM; larger blocks are rejected with 413.
pub const MAX_BLOCK_BYTES: usize = 128 * 1024 * 1024;

/// F5 — cap on the number of block IDs in a blocklist commit request.
pub const MAX_BLOCKLIST_BLOCKS: usize = 10_000;

/// F5 — cap on the assembled blob size. Assembly streams block files into the
/// destination file and never materializes the whole blob in memory, but a
/// blocklist referencing more than this budget is rejected up front.
pub const MAX_ASSEMBLED_BYTES: usize = 512 * 1024 * 1024;

/// F6 — server-side cap on timeline records returned by a single GET page.
/// `?top=` larger than this is clamped, `?skip=` pages further.
pub const MAX_TOP_RECORDS: usize = 500;

/// F7 — per-job cap on in-flight pending uploads (artifact v2 and cache v2).
/// The job is taken from the signed runtime token scope, so a runner cannot
/// evade the cap by inventing other job ids in request bodies.
pub const MAX_PENDING_PER_JOB: usize = 32;

/// R1-6 — cap on a single in-flight legacy cache upload
/// (`PendingCache::bytes`). `cache_upload` appends every PATCH body with no
/// running total; without this cap a job could grow server RAM without bound
/// by PATCHing chunks forever (~500 requests/GiB at the 2 MiB default body
/// limit). Matches the 512 MiB body limit on the Twirp blob upload route.
pub const MAX_CACHE_UPLOAD_BYTES: u64 = 512 * 1024 * 1024;

/// R1-6 — per-job aggregate cap on in-flight legacy cache upload bytes.
/// `MAX_CACHE_UPLOAD_BYTES` bounds one reservation; without an aggregate
/// budget a job could still hold `MAX_PENDING_PER_JOB` × 512 MiB (~16 GiB)
/// in `pending_caches`. 1 GiB leaves headroom for two full-size uploads
/// while keeping a hostile job's worst case bounded. Enforced under the
/// `inner` lock in `cache_upload`, so concurrent chunks cannot race past
/// it; bytes are released when the reservation commits or is swept.
pub const MAX_PENDING_CACHE_BYTES_PER_JOB: u64 = 1024 * 1024 * 1024;

/// F7 — global cap on minted cache download tokens; the oldest are evicted.
pub const MAX_CACHE_DL_TOKENS: usize = 1024;

/// F7 — per-run cap on finalized artifact v2 registry entries, mirroring
/// GitHub's "500 artifacts per workflow run" limit.
pub const MAX_ARTIFACTS_PER_RUN: usize = 500;

/// F7 — global cap on the artifact v2 registry; the oldest finalized entries
/// are evicted past this so a flood of fabricated run ids stays bounded.
pub const MAX_ARTIFACT_REGISTRY_ENTRIES: usize = 10_000;

/// F8 — terminal runs whose heavy node-local runtime state (live-log buffers,
/// timeline projections) stays in memory. A finished run's live-log buffer is
/// capped at 64 MiB *per job* and its timeline projections are only dropped on
/// request purge — neither is needed once the run is old enough that nothing
/// follows it live. Without this bound, ~40 runs/hour of CI accumulated
/// ~4 GiB/hour of retained buffers on cpane and the kernel OOM killer kept
/// restarting the engine mid-run (starving every queued job).
///
/// Post-cutover there is no in-memory run-record cap to pair this with:
/// `RunRecord`s are database rows, and the durable bound on how long a settled
/// run stays in the hot scheduling tables is
/// [`ControlBackend::archive_finished_runs`](crate::control::ControlBackend::archive_finished_runs).
pub const MAX_TERMINAL_RUNS_WITH_RUNTIME_STATE: usize = 16;

/// F7 — how long a pending upload (or download token) survives without being
/// finalized/consumed before the reaper sweeps it. Jobs that never finish
/// their upload leave an entry behind; without a TTL those would accumulate.
pub const PENDING_UPLOAD_TTL: Duration = Duration::from_secs(3600);

/// Unix seconds for pending-upload timestamps.
pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

/// Job backend id proven by a request's bearer token, if it is a job runtime
/// token with an exact, self-consistent
/// `Actions.Results:{plan}:{job}` scope. The engine token and runner listen
/// tokens return `None` — those callers are not one job.
///
/// This delegates to the same verified Results parser used by route
/// authentication and job-bound URL minting. Quota identity must never be
/// recovered from only the scope suffix.
pub fn job_backend_id_from_bearer(state: &AppState, headers: &HeaderMap) -> Option<String> {
    let token = bearer_from_headers(headers)?;
    match results_identity(state, token).ok()? {
        ResultsIdentity::System => None,
        ResultsIdentity::Job(identity) => Some(identity.job_id.to_string()),
    }
}
// ─── Retention helpers ───────────────────────────────────────────────────────
//
// Called at every write site, so the maps can never drift above their caps for
// long. They are also idempotent and cheap on well-behaved state.

/// F1 — bound one plan's retained logs to `MAX_LOG_BYTES_PER_PLAN` bytes and
/// `MAX_LOGS_PER_PLAN` entries by evicting the oldest logs first. Called after
/// every append (and log creation). Returns the log keys evicted from memory
/// so the caller can delete them from the durable store too — otherwise the
/// on-disk `log_files`/`log_chunks` grow without bound even though memory is
/// capped (D2).
pub fn trim_plan_logs(inner: &mut InnerState, plan_id: &str) -> Vec<String> {
    let mut evicted = Vec::new();
    // Fast path: when the whole retained set is under the per-plan budget, no
    // single plan (a subset) can exceed it, and the larger global caps hold
    // too — so skip the O(keys) scans on the hot append path.
    if inner.log_bytes_total <= MAX_LOG_BYTES_PER_PLAN && inner.logs.len() <= MAX_LOGS_PER_PLAN {
        return evicted;
    }
    let prefix = format!("{plan_id}/");
    loop {
        let total_bytes: usize = inner
            .logs
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(_, value)| value.len())
            .sum();
        let count = inner
            .logs
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .count();
        if total_bytes <= MAX_LOG_BYTES_PER_PLAN && count <= MAX_LOGS_PER_PLAN {
            break;
        }
        // Evict the oldest log of the plan: numeric log ids sort in creation
        // order; non-numeric ids (crafted paths) fall back to string order.
        let oldest_key = inner
            .logs
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .min_by_key(|key| {
                let id = key.trim_start_matches(&prefix);
                (id.parse::<usize>().unwrap_or(usize::MAX), id.to_owned())
            })
            .cloned();
        let Some(oldest_key) = oldest_key else { break };
        if let Some(removed) = inner.logs.remove(&oldest_key) {
            inner.log_bytes_total = inner.log_bytes_total.saturating_sub(removed.len());
        }
        inner.log_metadata.remove(&oldest_key);
        inner.log_order.retain(|k| k != &oldest_key);
        evicted.push(oldest_key);
    }
    // Global caps — prevent fabricated plan_ids from bypassing per-plan limits.
    loop {
        let total_bytes: usize = inner.logs.values().map(|v| v.len()).sum();
        let count = inner.logs.len();
        if total_bytes <= MAX_LOG_BYTES_GLOBAL && count <= MAX_LOGS_GLOBAL {
            break;
        }
        // Pop oldest by insertion order; fall back to lexicographic smallest.
        let oldest = inner
            .log_order
            .pop_front()
            .filter(|k| inner.logs.contains_key(k))
            .or_else(|| inner.logs.keys().next().cloned());
        let Some(oldest) = oldest else { break };
        // Drain any stale deque entries that point to already-evicted keys.
        if !inner.logs.contains_key(&oldest) {
            continue;
        }
        if let Some(removed) = inner.logs.remove(&oldest) {
            inner.log_bytes_total = inner.log_bytes_total.saturating_sub(removed.len());
        }
        inner.log_metadata.remove(&oldest);
        evicted.push(oldest);
    }
    // Compact order deque to avoid unbounded stale entries.
    if inner.log_order.len() > inner.logs.len() + 1024 {
        let live: std::collections::HashSet<String> = inner.logs.keys().cloned().collect();
        inner.log_order.retain(|k| live.contains(k));
    }
    evicted
}

/// F2 — after a timeline PATCH upsert: bound the per-timeline record map to
/// `MAX_TIMELINE_RECORDS` (evicting the oldest keys) and the number of
/// distinct timeline keys to `MAX_TIMELINE_KEYS` (evicting whole timelines,
/// records and change-id counter together).
pub fn trim_timeline_after_patch(
    inner: &mut InnerState,
    timeline_key: &str,
    protected: &[uuid::Uuid],
) {
    // Track insertion order for global eviction.
    if !inner
        .timeline_records_order
        .iter()
        .any(|k| k == timeline_key)
    {
        inner
            .timeline_records_order
            .push_back(timeline_key.to_owned());
    }
    if let Some(records) = inner.timeline_records.get_mut(timeline_key) {
        // Evict oldest records past the count cap. The BTreeMap orders by
        // UUID, not insertion time, so prefer evicting records NOT part of
        // this PATCH — otherwise a new record whose UUID sorts first would be
        // deleted and the response/subsequent GETs would omit its update.
        // If the just-patched set alone exceeds the cap (a flood in one
        // PATCH), fall through to evicting the oldest regardless so the bound
        // always holds.
        while records.len() > MAX_TIMELINE_RECORDS {
            let victim = records
                .keys()
                .find(|id| !protected.contains(*id))
                .copied()
                .or_else(|| records.keys().next().copied());
            let Some(victim) = victim else { break };
            records.remove(&victim);
        }
        // Byte budget per timeline — evict oldest non-protected record until
        // under budget.
        loop {
            let total_bytes: usize = records
                .values()
                .map(|r| {
                    r.name.as_ref().map(|s| s.len()).unwrap_or(0)
                        + r.display_name.as_ref().map(|s| s.len()).unwrap_or(0)
                        + r.current_operation.as_ref().map(|s| s.len()).unwrap_or(0)
                        + 256 // overhead for other fields
                })
                .sum();
            if total_bytes <= MAX_TIMELINE_BYTES_PER_TIMELINE || records.len() <= 1 {
                break;
            }
            let victim = records
                .keys()
                .find(|id| !protected.contains(*id))
                .copied()
                .or_else(|| records.keys().next().copied());
            let Some(victim) = victim else { break };
            records.remove(&victim);
        }
    }
    while inner.timeline_records.len() > MAX_TIMELINE_KEYS {
        let oldest_key = inner
            .timeline_records_order
            .pop_front()
            .filter(|k| inner.timeline_records.contains_key(k))
            .or_else(|| inner.timeline_records.keys().next().cloned());
        let Some(oldest_key) = oldest_key else { break };
        if !inner.timeline_records.contains_key(&oldest_key) {
            continue;
        }
        // Never evict the timeline we just patched — otherwise a PATCH
        // whose key sorts first would return empty records.
        // Only protect if the timeline actually has a change-id counter;
        // otherwise the record and counter maps would drift by one (as in
        // the restore test where the patched timeline has no counter).
        if oldest_key == timeline_key && inner.timeline_change_ids.contains_key(timeline_key) {
            // Put it back and evict the next oldest instead.
            let next = inner
                .timeline_records_order
                .pop_front()
                .filter(|k| inner.timeline_records.contains_key(k))
                .or_else(|| {
                    inner
                        .timeline_records
                        .keys()
                        .find(|k| *k != timeline_key)
                        .cloned()
                });
            // Re-queue the protected key at the back (most recent).
            inner
                .timeline_records_order
                .push_back(timeline_key.to_owned());
            let Some(next_key) = next else { break };
            inner.timeline_records.remove(&next_key);
            inner.timeline_change_ids.remove(&next_key);
            continue;
        }
        inner.timeline_records.remove(&oldest_key);
        inner.timeline_change_ids.remove(&oldest_key);
    }
    // Ensure change_id map stays in sync with records — orphaned counters
    // are pruned so a no-op protection doesn't leave a drift.
    inner
        .timeline_change_ids
        .retain(|k, _| inner.timeline_records.contains_key(k));
}

/// F3 — after timeline events are projected: ring-buffer each run's event
/// Vec to `MAX_TIMELINE_EVENTS` and bound the number of distinct run buckets
/// to `MAX_TIMELINE_EVENT_KEYS`.
pub fn trim_timeline_events(inner: &mut InnerState, run_id: RunId) {
    if !inner.timeline_events_order.iter().any(|r| r == &run_id)
        && inner.timeline_events.contains_key(&run_id)
    {
        inner.timeline_events_order.push_back(run_id);
    }
    if let Some(events) = inner.timeline_events.get_mut(&run_id) {
        let excess = events.len().saturating_sub(MAX_TIMELINE_EVENTS);
        if excess > 0 {
            events.drain(0..excess);
        }
    }
    while inner.timeline_events.len() > MAX_TIMELINE_EVENT_KEYS {
        let oldest_run = inner
            .timeline_events_order
            .pop_front()
            .filter(|r| inner.timeline_events.contains_key(r))
            .or_else(|| inner.timeline_events.keys().next().copied());
        let Some(oldest_run) = oldest_run else { break };
        if !inner.timeline_events.contains_key(&oldest_run) {
            continue;
        }
        if oldest_run == run_id {
            // Protect the active run — evict the next oldest instead.
            let next = inner
                .timeline_events_order
                .pop_front()
                .filter(|r| inner.timeline_events.contains_key(r))
                .or_else(|| {
                    inner
                        .timeline_events
                        .keys()
                        .find(|k| **k != run_id)
                        .copied()
                });
            inner.timeline_events_order.push_back(run_id);
            let Some(next_run) = next else { break };
            inner.timeline_events.remove(&next_run);
            continue;
        }
        inner.timeline_events.remove(&oldest_run);
    }
}

/// F7 — bound the minted cache download-token map to `MAX_CACHE_DL_TOKENS`,
/// evicting the oldest minted tokens first (restored tokens with no mint
/// order fall back to map order).
pub fn trim_cache_dl_tokens(inner: &mut InnerState) {
    while inner.cache_v2_dl_tokens.len() > MAX_CACHE_DL_TOKENS {
        let oldest = inner
            .cache_v2_dl_tokens_order
            .pop_front()
            .filter(|token| inner.cache_v2_dl_tokens.contains_key(token))
            .or_else(|| inner.cache_v2_dl_tokens.keys().next().cloned());
        let Some(oldest) = oldest else { break };
        inner.cache_v2_dl_tokens.remove(&oldest);
        inner.cache_v2_dl_tokens_created.remove(&oldest);
    }
}

// ─── Node-local run keys, resolved from the control backend ─────────────────
//
// Post-cutover the run records, dispatch queue and request rows live in the
// control database; `InnerState` holds only node-local metadata. That metadata
// is still keyed by the ids those rows carry — logical job ids and agent job
// ids (live-log buffers), plan ids (retained console keys `{plan_id}/{log_id}`,
// plan caches) — so any teardown that frees one run's memory resolves those
// keys from the backend FIRST and then mutates `InnerState` with plain data.
// Keeping resolution out of the `inner` lock is the same discipline
// `retention::sweep_once` follows: no database round trip under the lock.

/// The node-local key set of one run.
pub(crate) struct RunNodeKeys {
    /// Logical job ids of the run — one live-log key per job.
    pub logical_job_ids: Vec<String>,
    /// Agent job ids of the run's attempts: per-attempt live-log keys and
    /// diagnostic-upload tokens are named by them.
    pub agent_job_ids: Vec<uuid::Uuid>,
    /// Plan ids of the run's attempts: retained console logs are keyed
    /// `{plan_id}/{log_id}` and the plan-keyed caches share that prefix.
    pub plan_ids: Vec<String>,
}

impl RunNodeKeys {
    /// Resolve `run_id`'s keys from the control backend.
    ///
    /// A run whose durable rows are gone resolves to an empty set instead of
    /// failing: at that point the record is unrecoverable, but node-local
    /// leftovers must still be droppable. `run_requests` unions the
    /// attempt-history arm, so an archived run still yields the attempt ids
    /// that named its node-local buffers.
    pub async fn resolve(
        backend: &crate::control::Backend,
        run_id: RunId,
    ) -> Result<Self, ControlError> {
        let logical_job_ids = match backend.run_job_ids(run_id).await {
            Ok(job_ids) => job_ids,
            Err(ControlError::NotFound(_)) => Vec::new(),
            Err(error) => return Err(error),
        };
        let requests = match backend.run_requests(run_id).await {
            Ok(requests) => requests,
            Err(ControlError::NotFound(_)) => Vec::new(),
            Err(error) => return Err(error),
        };
        Ok(Self {
            logical_job_ids,
            agent_job_ids: requests.iter().map(|record| record.agent_job_id).collect(),
            plan_ids: requests
                .iter()
                .map(|record| record.plan_id.clone())
                .collect(),
        })
    }
}

/// The runs that still hold node-local runtime state on this node, as the
/// keys that identify them.
///
/// Snapshot it out of [`InnerState`] while the state lock is held, then resolve
/// it with [`plan_completed_run_trim`] *after* releasing the lock: mapping a
/// live-log key back to its run needs a request lookup in the backend.
pub(crate) struct ResidentRunTraces {
    /// Runs named directly by a run-keyed node-local structure.
    pub run_ids: BTreeSet<RunId>,
    /// Live-log buffer keys: an agent job id (a dispatched attempt) or a
    /// logical job id (a job that has no request yet).
    pub live_log_keys: Vec<String>,
}

impl ResidentRunTraces {
    /// Every run that still holds node-local runtime state here.
    pub fn of(inner: &InnerState) -> Self {
        let mut run_ids: BTreeSet<RunId> = inner.timeline_events.keys().copied().collect();
        run_ids.extend(inner.dap_ports.keys().copied());
        run_ids.extend(inner.artifacts.values().map(|record| record.run_id));
        for key in inner.artifact_v2_registry.keys() {
            // Registry keys are `{run_id}/{name}`.
            if let Some(run_id) = key.split('/').next().and_then(|run| run.parse().ok()) {
                run_ids.insert(run_id);
            }
        }
        let mut live_log_keys: BTreeSet<String> = inner.live_log_lines.keys().cloned().collect();
        live_log_keys.extend(inner.live_log_tx.keys().cloned());
        Self {
            run_ids,
            live_log_keys: live_log_keys.into_iter().collect(),
        }
    }
}

/// F8 — the runs that must give up their node-local runtime state, with their
/// keys already resolved from the backend. Produced by
/// [`plan_completed_run_trim`], applied by [`trim_completed_runs`].
pub(crate) struct CompletedRunTrim {
    /// Runs whose node-local runtime state goes, with their key set.
    pub drops: Vec<(RunId, RunNodeKeys)>,
}

/// F8 — plan which runs must give up their node-local runtime state: every
/// resident run that is not among the newest
/// [`MAX_TERMINAL_RUNS_WITH_RUNTIME_STATE`] terminal runs.
///
/// The in-flight window is the whole point: a terminal run's live-log buffers
/// (up to 64 MiB per job) and timeline projections are only useful while
/// something can still follow the run live, so the newest terminal runs keep
/// theirs and everything older loses it. Runs still in flight are never
/// trimmed.
///
/// Post-cutover the run records themselves are database rows, so there is no
/// in-memory record cap to enforce alongside this: the durable bound on how
/// long a settled run stays in the hot scheduling tables is
/// [`ControlBackend::archive_finished_runs`](crate::control::ControlBackend::archive_finished_runs).
pub(crate) async fn plan_completed_run_trim(
    backend: &crate::control::Backend,
    traces: &ResidentRunTraces,
) -> Result<CompletedRunTrim, ControlError> {
    // Terminal runs ordered by completion recency, newest first.
    let newest_terminal = backend
        .list_runs(crate::control::backend::RunListFilter {
            workflow: None,
            status: Some("completed".to_owned()),
            event: None,
            limit: MAX_TERMINAL_RUNS_WITH_RUNTIME_STATE,
        })
        .await?;
    let keep: BTreeSet<RunId> = newest_terminal.iter().map(|run| run.run_id).collect();

    // Candidates: the runs holding a run-keyed structure here, plus the runs
    // behind the live-log buffers whose key is an agent job id — those buffers
    // are the dominant heap consumer and are only reachable this way.
    let mut candidates = traces.run_ids.clone();
    for key in &traces.live_log_keys {
        let Ok(agent_job_id) = key.parse::<uuid::Uuid>() else {
            continue;
        };
        if let Some((_, run_id)) = backend.find_request_by_agent_job_id(agent_job_id).await? {
            candidates.insert(run_id);
        }
    }

    let mut drops = Vec::new();
    for run_id in candidates {
        if keep.contains(&run_id) {
            continue;
        }
        match backend.run_record(run_id).await {
            // Terminal, or gone from the database entirely (its rows were
            // deleted): either way nothing follows it live any more.
            Ok(record) if record.status.is_terminal() => {}
            Ok(_) => continue,
            Err(ControlError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        drops.push((run_id, RunNodeKeys::resolve(backend, run_id).await?));
    }
    Ok(CompletedRunTrim { drops })
}

/// F8 — apply a planned trim under the state lock. Returns how many runs gave
/// up their node-local runtime state.
pub(crate) fn trim_completed_runs(inner: &mut InnerState, trim: &CompletedRunTrim) -> usize {
    for (run_id, keys) in &trim.drops {
        drop_run_runtime_state(inner, *run_id, keys);
    }
    trim.drops.len()
}

/// Everything the retention sweep must delete outside process memory when
/// it deletes a run. The durable rows are deleted through the control backend,
/// but the filesystem is not the backend's to delete: these are collected here
/// — while the state lock is held — so the sweep can delete them right after
/// releasing it.
pub struct RunRemoval {
    /// In-memory log keys (`{plan_id}/{log_id}`) that belonged to the run.
    /// The sweep deletes each from the durable log tables as well.
    pub log_keys: Vec<String>,
    /// On-disk paths of the run's v1 artifacts.
    pub artifact_paths: Vec<std::path::PathBuf>,
    /// `blob_token`s of the run's finalized artifact-v2 entries; the sweep
    /// deletes `blobs/artifact/<token>` for each.
    pub artifact_v2_blob_tokens: Vec<String>,
}

/// Delete a run from every node-local structure and collect what the caller
/// must also delete from the durable store and the filesystem.
///
/// This is the full teardown used by the retention sweep: the heavy per-job
/// runtime state, retained console logs, both artifact registries, and the
/// run-keyed node-local singletons. Everything else about a run — its record,
/// its dispatch-queue rows, its requests, its steps, its timeline — is a
/// database row now, so the caller deletes those through the backend (the live
/// tables cascade from `runs`, and the retention sweep's `DELETE` is the
/// authoritative removal). `keys` must be resolved with
/// [`RunNodeKeys::resolve`] *before* the caller deletes the run's rows.
pub(crate) fn remove_run_everywhere(
    inner: &mut InnerState,
    run_id: RunId,
    keys: &RunNodeKeys,
) -> RunRemoval {
    let agent_ids: std::collections::BTreeSet<String> = keys
        .agent_job_ids
        .iter()
        .map(uuid::Uuid::to_string)
        .collect();

    drop_run_runtime_state(inner, run_id, keys);

    // Retained console logs: keys are `{plan_id}/{log_id}`.
    let mut log_keys = Vec::new();
    for plan_id in &keys.plan_ids {
        let prefix = format!("{plan_id}/");
        let plan_log_keys: Vec<String> = inner
            .logs
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        for key in plan_log_keys {
            if let Some(bytes) = inner.logs.remove(&key) {
                inner.log_bytes_total = inner.log_bytes_total.saturating_sub(bytes.len());
            }
            inner.log_metadata.remove(&key);
            inner.log_order.retain(|ordered| ordered != &key);
            log_keys.push(key);
        }
    }

    // v1 artifacts, keyed by artifact id with the run on the record.
    let mut artifact_paths = Vec::new();
    let artifact_ids: Vec<String> = inner
        .artifacts
        .iter()
        .filter(|(_, record)| record.run_id == run_id)
        .map(|(id, _)| id.clone())
        .collect();
    for id in artifact_ids {
        if let Some(record) = inner.artifacts.remove(&id) {
            artifact_paths.push(std::path::PathBuf::from(record.path));
        }
    }

    // Artifact-v2 registry: keys are `{run_id}/{name}`.
    let prefix = format!("{run_id}/");
    let mut artifact_v2_blob_tokens = Vec::new();
    let registry_keys: Vec<String> = inner
        .artifact_v2_registry
        .keys()
        .filter(|key| key.starts_with(&prefix))
        .cloned()
        .collect();
    for key in registry_keys {
        if let Some(entry) = inner.artifact_v2_registry.remove(&key) {
            artifact_v2_blob_tokens.push(entry.blob_token);
        }
        inner
            .artifact_registry_order
            .retain(|ordered| ordered != &key);
    }

    // Debug sessions close with their jobs; drop any stray. Diag upload
    // tokens are minted per job attempt and would otherwise linger.
    inner.debug_sessions.remove_for_run(run_id);
    inner
        .diag_upload_tokens
        .retain(|_, token| !agent_ids.contains(&token.job_id));

    // Run-keyed singletons. Everything else that used to be purged here
    // (`run_concurrency`, `holder_keys`, `claimed_jobs`, `expanding`,
    // `pending_expansions`) is a scheduling table now: the backend's
    // `DELETE FROM runs` cascades it, and memory holds no copy.
    inner.dap_ports.remove(&run_id);

    // Plan-keyed secret maskers are derived from the run's own secrets; the
    // run's plans are gone, so the cached arcs would only be reclaimed by a
    // later masker-cache eviction. Drop them with the run.
    for plan_id in &keys.plan_ids {
        inner.plan_secret_masker.remove(plan_id);
        inner.plan_secret_masker_pending.remove(plan_id);
    }

    RunRemoval {
        log_keys,
        artifact_paths,
        artifact_v2_blob_tokens,
    }
}

/// Drop the heavy per-job runtime state of a run: retained live-log buffers
/// (up to 64 MiB each) and timeline projections, keyed by the run's resolved
/// [`RunNodeKeys`]. The database keeps the authoritative copies of everything
/// here (logs are re-readable through the durable `log_files`, steps and
/// timelines through the backend); this only frees the node-local projections
/// that exist to serve live followers and run-scoped reads.
pub(crate) fn drop_run_runtime_state(inner: &mut InnerState, run_id: RunId, keys: &RunNodeKeys) {
    for key in keys
        .logical_job_ids
        .iter()
        .cloned()
        .chain(keys.agent_job_ids.iter().map(uuid::Uuid::to_string))
    {
        inner.live_log_lines.remove(&key);
        inner.live_log_tx.remove(&key);
        inner.live_log_closed.remove(&key);
    }
    inner.timeline_events.remove(&run_id);
    inner.timeline_events_order.retain(|id| *id != run_id);
    for plan_id in &keys.plan_ids {
        let prefix = format!("{plan_id}/");
        inner
            .timeline_records
            .retain(|key, _| !key.starts_with(&prefix));
        inner
            .timeline_change_ids
            .retain(|key, _| !key.starts_with(&prefix));
        inner
            .timeline_records_order
            .retain(|key| !key.starts_with(&prefix));
    }
}

/// F7 — bound the finalized artifact v2 registry: `MAX_ARTIFACTS_PER_RUN` per
/// run and `MAX_ARTIFACT_REGISTRY_ENTRIES` globally, evicting oldest entries
/// by finalization order (not lexicographic key order).
pub fn trim_artifact_registry(inner: &mut InnerState) {
    // Per-run cap — enforce 500 per workflow_run_backend_id.
    {
        let mut per_run: BTreeMap<String, usize> = BTreeMap::new();
        for key in inner.artifact_v2_registry.keys() {
            if let Some(run) = key.split('/').next() {
                *per_run.entry(run.to_owned()).or_default() += 1;
            }
        }
        for (run, count) in per_run {
            if count <= MAX_ARTIFACTS_PER_RUN {
                continue;
            }
            let mut excess = count - MAX_ARTIFACTS_PER_RUN;
            // Evict oldest entries for this run first (FIFO).
            let mut to_remove: Vec<String> = Vec::new();
            for key in inner.artifact_registry_order.iter() {
                if excess == 0 {
                    break;
                }
                if key.starts_with(&format!("{run}/"))
                    && inner.artifact_v2_registry.contains_key(key)
                {
                    to_remove.push(key.clone());
                    excess -= 1;
                }
            }
            // Fallback to BTree order if order deque is incomplete (restored).
            if excess > 0 {
                for key in inner.artifact_v2_registry.keys() {
                    if excess == 0 {
                        break;
                    }
                    if key.starts_with(&format!("{run}/")) && !to_remove.contains(key) {
                        to_remove.push(key.clone());
                        excess -= 1;
                    }
                }
            }
            for key in to_remove {
                inner.artifact_v2_registry.remove(&key);
                inner.artifact_registry_order.retain(|k| k != &key);
            }
        }
    }
    while inner.artifact_v2_registry.len() > MAX_ARTIFACT_REGISTRY_ENTRIES {
        let oldest = inner
            .artifact_registry_order
            .pop_front()
            .filter(|k| inner.artifact_v2_registry.contains_key(k))
            .or_else(|| inner.artifact_v2_registry.keys().next().cloned());
        let Some(oldest_key) = oldest else { break };
        if !inner.artifact_v2_registry.contains_key(&oldest_key) {
            continue;
        }
        inner.artifact_v2_registry.remove(&oldest_key);
    }
}

/// F7 — TTL sweep for pending uploads and download tokens. Entries with
/// `created_unix == 0` (restored from a persisted meta, or engine-token
/// reservations made before timestamps existed) are left alone, matching the
/// session-liveness sweep's treatment of restored state. R1-6: the legacy
/// artifactcache reservations (`pending_caches`) are in-memory only and were
/// never swept — an abandoned reservation held its bytes forever — so they
/// are covered by the same TTL.
pub fn sweep_pending_uploads(inner: &mut InnerState, now_unix_secs: i64) {
    let cutoff = now_unix_secs.saturating_sub(PENDING_UPLOAD_TTL.as_secs() as i64);
    let stale_cache: Vec<String> = inner
        .cache_v2_pending
        .iter()
        .filter(|(_, pending)| pending.created_unix > 0 && pending.created_unix < cutoff)
        .map(|(token, _)| token.clone())
        .collect();
    for token in stale_cache {
        inner.cache_v2_pending.remove(&token);
    }
    let stale_artifact: Vec<String> = inner
        .artifact_v2_pending
        .iter()
        .filter(|(_, pending)| pending.created_unix > 0 && pending.created_unix < cutoff)
        .map(|(token, _)| token.clone())
        .collect();
    for token in stale_artifact {
        inner.artifact_v2_pending.remove(&token);
    }
    // R1-6: legacy reservations are freed too; only `cache_commit` removed
    // them before, so abandoned uploads accumulated RAM without bound.
    let stale_legacy: Vec<i64> = inner
        .pending_caches
        .iter()
        .filter(|(_, pending)| pending.created_unix > 0 && pending.created_unix < cutoff)
        .map(|(cache_id, _)| *cache_id)
        .collect();
    for cache_id in stale_legacy {
        inner.pending_caches.remove(&cache_id);
    }
    let stale_dl: Vec<String> = inner
        .cache_v2_dl_tokens_created
        .iter()
        .filter(|(_, created)| **created > 0 && **created < cutoff)
        .map(|(token, _)| token.clone())
        .collect();
    for token in stale_dl {
        inner.cache_v2_dl_tokens.remove(&token);
        inner.cache_v2_dl_tokens_created.remove(&token);
        inner
            .cache_v2_dl_tokens_order
            .retain(|queued| queued != &token);
    }
    let stale_diag: Vec<String> = inner
        .diag_upload_tokens
        .iter()
        .filter(|(_, pending)| pending.created_unix > 0 && pending.created_unix < cutoff)
        .map(|(token, _)| token.clone())
        .collect();
    for token in stale_diag {
        inner.diag_upload_tokens.remove(&token);
    }
    // Compact order deque if it grew with stale entries while under cap.
    if inner.cache_v2_dl_tokens_order.len() > inner.cache_v2_dl_tokens.len() + 1024 {
        let live: std::collections::HashSet<String> =
            inner.cache_v2_dl_tokens.keys().cloned().collect();
        inner.cache_v2_dl_tokens_order.retain(|k| live.contains(k));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_plan_logs_evicts_oldest_until_under_budget() {
        let mut inner = InnerState::default();
        // 72 MiB across 9 logs, oldest ids first.
        let chunk = vec![b'x'; 8 * 1024 * 1024];
        for id in 1..=9usize {
            inner.logs.insert(format!("plan-1/{id}"), chunk.clone());
        }
        inner.log_bytes_total = inner.logs.values().map(Vec::len).sum();
        trim_plan_logs(&mut inner, "plan-1");
        let total: usize = inner
            .logs
            .iter()
            .filter(|(key, _)| key.starts_with("plan-1/"))
            .map(|(_, value)| value.len())
            .sum();
        assert!(total <= MAX_LOG_BYTES_PER_PLAN);
        assert!(
            !inner.logs.contains_key("plan-1/1"),
            "oldest logs must be evicted first"
        );
        assert!(inner.logs.contains_key("plan-1/9"));
    }

    #[test]
    fn trim_plan_logs_bounds_empty_log_count() {
        let mut inner = InnerState::default();
        for id in 0..(MAX_LOGS_PER_PLAN + 64) {
            inner.logs.insert(format!("plan-2/{id}"), Vec::new());
        }
        inner.log_bytes_total = inner.logs.values().map(Vec::len).sum();
        trim_plan_logs(&mut inner, "plan-2");
        let count = inner
            .logs
            .keys()
            .filter(|key| key.starts_with("plan-2/"))
            .count();
        assert!(
            count <= MAX_LOGS_PER_PLAN,
            "empty logs still count against the plan budget: {count}"
        );
    }

    #[test]
    fn trim_timeline_after_patch_evicts_oldest_records_and_keys() {
        let mut inner = InnerState::default();
        for _ in 0..(MAX_TIMELINE_RECORDS + 32) {
            inner
                .timeline_records
                .entry("plan-a/0000".to_owned())
                .or_default()
                .insert(uuid::Uuid::new_v4(), minimal_record());
        }
        trim_timeline_after_patch(&mut inner, "plan-a/0000", &[]);
        assert!(inner.timeline_records["plan-a/0000"].len() <= MAX_TIMELINE_RECORDS);

        for i in 0..(MAX_TIMELINE_KEYS + 16) {
            let key = format!("plan-k/{i}");
            inner.timeline_records.entry(key.clone()).or_default();
            inner.timeline_change_ids.insert(key, i as i32);
        }
        trim_timeline_after_patch(&mut inner, "plan-a/0000", &[]);
        assert!(inner.timeline_records.len() <= MAX_TIMELINE_KEYS);
        assert_eq!(
            inner.timeline_change_ids.len(),
            inner.timeline_records.len(),
            "change-id counters must be evicted with their timelines"
        );
    }

    #[test]
    fn trim_timeline_after_patch_keeps_just_patched_low_uuid_record() {
        let mut inner = InnerState::default();
        let tl = "plan-a/0000";
        // Fill the timeline to the cap; every v4 UUID sorts above nil.
        for _ in 0..MAX_TIMELINE_RECORDS {
            inner
                .timeline_records
                .entry(tl.to_owned())
                .or_default()
                .insert(uuid::Uuid::new_v4(), minimal_record());
        }
        // A freshly patched record whose UUID sorts before every existing one.
        let patched = uuid::Uuid::nil();
        inner
            .timeline_records
            .get_mut(tl)
            .unwrap()
            .insert(patched, minimal_record());
        assert_eq!(inner.timeline_records[tl].len(), MAX_TIMELINE_RECORDS + 1);

        trim_timeline_after_patch(&mut inner, tl, &[patched]);

        assert_eq!(inner.timeline_records[tl].len(), MAX_TIMELINE_RECORDS);
        assert!(
            inner.timeline_records[tl].contains_key(&patched),
            "the just-patched low-UUID record must survive eviction"
        );
    }

    fn minimal_record() -> azdo::TimelineRecord {
        azdo::TimelineRecord {
            id: uuid::Uuid::nil(),
            change_id: None,
            parent_id: None,
            name: None,
            display_name: None,
            record_type: None,
            state: None,
            result: None,
            start_time: None,
            finish_time: None,
            issues: Vec::new(),
            variables: BTreeMap::new(),
            current_operation: None,
            percent_complete: None,
            worker_name: None,
            error_count: None,
            warning_count: None,
            is_background: None,
            background_control_type: None,
            background_control_step_ids: Vec::new(),
            parallel_group_id: None,
            steps: Vec::new(),
            last_modified: None,
            log: None,
        }
    }

    #[test]
    fn trim_timeline_events_ring_buffers_each_run_and_bounds_keys() {
        let mut inner = InnerState::default();
        let run = RunId::new();
        let event = NdjsonEvent::JobStatus {
            run_id: RunId::new(),
            job_id: JobId("j".to_owned()),
            status: ExecutionStatus::InProgress,
            reason: None,
        };
        inner
            .timeline_events
            .insert(run, vec![event; MAX_TIMELINE_EVENTS + 64]);
        for _ in 0..(MAX_TIMELINE_EVENT_KEYS + 8) {
            inner.timeline_events.insert(RunId::new(), Vec::new());
        }
        trim_timeline_events(&mut inner, run);
        assert_eq!(
            inner.timeline_events[&run].len(),
            MAX_TIMELINE_EVENTS,
            "the ring must drop the oldest events"
        );
        assert!(inner.timeline_events.len() <= MAX_TIMELINE_EVENT_KEYS);
    }

    #[test]
    fn trim_cache_dl_tokens_evicts_oldest_first() {
        let mut inner = InnerState::default();
        for i in 0..(MAX_CACHE_DL_TOKENS + 16) {
            let token = format!("tok-{i}");
            inner
                .cache_v2_dl_tokens
                .insert(token.clone(), ("k".into(), "v".into()));
            inner.cache_v2_dl_tokens_order.push_back(token.clone());
            inner.cache_v2_dl_tokens_created.insert(token, i as i64);
        }
        trim_cache_dl_tokens(&mut inner);
        assert_eq!(inner.cache_v2_dl_tokens.len(), MAX_CACHE_DL_TOKENS);
        assert!(
            !inner.cache_v2_dl_tokens.contains_key("tok-0"),
            "the oldest minted token must be evicted first"
        );
        assert!(inner.cache_v2_dl_tokens.contains_key("tok-1039"));
    }

    #[test]
    fn sweep_pending_uploads_removes_only_stale_new_entries() {
        let mut inner = InnerState::default();
        let now = now_unix();
        inner.cache_v2_pending.insert(
            "fresh".into(),
            CacheV2Pending {
                key: "k".into(),
                version: "v".into(),
                job_backend_id: "j".into(),
                created_unix: now,
            },
        );
        inner.cache_v2_pending.insert(
            "stale".into(),
            CacheV2Pending {
                key: "k".into(),
                version: "v".into(),
                job_backend_id: "j".into(),
                created_unix: now - 7200,
            },
        );
        inner.cache_v2_pending.insert(
            "restored".into(),
            CacheV2Pending {
                key: "k".into(),
                version: "v".into(),
                job_backend_id: String::new(),
                created_unix: 0,
            },
        );
        inner.artifact_v2_pending.insert(
            "stale-art".into(),
            ArtifactV2Pending {
                registry_key: "r/j/n".into(),
                job_backend_id: "j".into(),
                created_unix: now - 7200,
            },
        );
        inner
            .cache_v2_dl_tokens
            .insert("dl-old".into(), ("k".into(), "v".into()));
        inner
            .cache_v2_dl_tokens_created
            .insert("dl-old".into(), now - 7200);
        inner
            .cache_v2_dl_tokens
            .insert("dl-fresh".into(), ("k".into(), "v".into()));
        inner
            .cache_v2_dl_tokens_created
            .insert("dl-fresh".into(), now);

        sweep_pending_uploads(&mut inner, now);

        assert!(inner.cache_v2_pending.contains_key("fresh"));
        assert!(!inner.cache_v2_pending.contains_key("stale"));
        assert!(inner.cache_v2_pending.contains_key("restored"));
        assert!(!inner.artifact_v2_pending.contains_key("stale-art"));
        assert!(inner.cache_v2_dl_tokens.contains_key("dl-fresh"));
        assert!(!inner.cache_v2_dl_tokens.contains_key("dl-old"));
    }
    #[tokio::test]
    async fn job_backend_id_from_bearer_accepts_matching_runtime_token() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let job_id = uuid::Uuid::new_v4();
        let token = state
            .local_jwt(json!({
                "sub": format!("preloop-job-{job_id}"),
                "scp": format!("Actions.Results:plan-{job_id}:{job_id}"),
            }))
            .unwrap();
        let headers = HeaderMap::from_iter([(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        )]);

        assert_eq!(
            job_backend_id_from_bearer(&state, &headers),
            Some(job_id.to_string()),
            "a normal runner Results token keeps its job quota identity"
        );
    }

    #[tokio::test]
    async fn job_backend_id_from_bearer_rejects_mismatched_and_malformed_scopes() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let subject_job = uuid::Uuid::new_v4();
        let other_job = uuid::Uuid::new_v4();
        let cases = [
            (
                format!("preloop-job-{subject_job}"),
                format!("Actions.Results:plan-{other_job}:{other_job}"),
                "mismatched subject and scope job",
            ),
            (
                format!("preloop-job-{subject_job}"),
                "Actions.Results:plan".to_owned(),
                "missing scope job",
            ),
            (
                format!("preloop-job-{subject_job}"),
                "Actions.Results::".to_owned(),
                "empty plan and job",
            ),
            (
                format!("preloop-job-{subject_job}"),
                format!("Actions.Results:plan-{subject_job}:{subject_job}:extra"),
                "extra scope component",
            ),
            (
                format!("preloop-job-{subject_job}"),
                "Actions.Results:plan:not-a-uuid".to_owned(),
                "non-UUID scope job",
            ),
        ];

        for (subject, scope, reason) in cases {
            let token = state
                .local_jwt(json!({ "sub": subject, "scp": scope }))
                .unwrap();
            let headers = HeaderMap::from_iter([(
                header::AUTHORIZATION,
                format!("Bearer {token}").parse().unwrap(),
            )]);
            assert_eq!(
                job_backend_id_from_bearer(&state, &headers),
                None,
                "{reason} must not produce a quota identity"
            );
        }
    }

    #[tokio::test]
    async fn job_backend_id_from_bearer_leaves_system_credential_unscoped() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let headers = HeaderMap::from_iter([(
            header::AUTHORIZATION,
            format!("Bearer {}", state.system_token).parse().unwrap(),
        )]);

        assert_eq!(
            job_backend_id_from_bearer(&state, &headers),
            None,
            "the administrator credential is intentionally not one job"
        );
    }

    /// Seed one run row directly. `submit_run` needs a full workflow
    /// submission; these tests exercise only the read/trim paths, and
    /// `AppState::test_db_mutate` is the documented escape hatch for plants
    /// like a forced-terminal run with a fixed completion time. SQLite only.
    async fn seed_run(state: &AppState, run_id: RunId, status: &str, run_number: i64, at_us: i64) {
        let run = run_id.to_string();
        let status = status.to_owned();
        let conclusion = (status == "completed").then_some("success");
        state
            .test_db_mutate(move |db| {
                db.0.execute(
                    "INSERT OR IGNORE INTO namespaces (namespace_id) VALUES ('default')",
                    [],
                )
                .unwrap();
                db.0.execute(
                    "INSERT INTO runs (run_id, namespace_id, repository, workflow_path, \
                         run_number, event, ref, ref_type, head_sha, workflow_ref, status, \
                         conclusion, origin, created_at, started_at, completed_at) \
                     VALUES (?1, 'default', 'test/repo', '.github/workflows/ci.yml', ?2, \
                         'push', 'refs/heads/main', 'branch', 'abc123', \
                         'test/repo/.github/workflows/ci.yml@refs/heads/main', ?3, ?4, \
                         'cli', ?5, ?5, ?5)",
                    rusqlite::params![run, run_number, status, conclusion, at_us],
                )
                .unwrap();
            })
            .await;
    }

    /// Seed one dispatched attempt of a run: the `jobs` row the request FKs to,
    /// plus the request itself. Its `agent_job_id` is the runner-protocol plan
    /// id, which is what node-local buffers are named by.
    async fn seed_request(state: &AppState, run_id: RunId, job_id: &str, agent_job_id: uuid::Uuid) {
        let run = run_id.to_string();
        let job = job_id.to_owned();
        let agent = agent_job_id.to_string();
        state
            .test_db_mutate(move |db| {
                db.0.execute(
                    "INSERT INTO jobs (run_id, job_id, namespace_id, kind, base_id, status, \
                         queue_state) \
                     VALUES (?1, ?2, 'default', 'job', ?2, 'success', 'none')",
                    rusqlite::params![run, job],
                )
                .unwrap();
                db.0.execute(
                    "INSERT INTO job_requests (run_id, job_id, namespace_id, agent_job_id, \
                         timeline_id, result) \
                     VALUES (?1, ?2, 'default', ?3, ?3, 'success')",
                    rusqlite::params![run, job, agent],
                )
                .unwrap();
            })
            .await;
    }

    /// F8 — only the runs outside the newest `MAX_TERMINAL_RUNS_WITH_RUNTIME_STATE`
    /// terminal runs give up their node-local runtime state, an in-flight run is
    /// never trimmed however old it is, and applying the plan under the lock
    /// frees exactly the planned runs' state.
    #[tokio::test]
    async fn plan_completed_run_trim_keeps_the_newest_terminal_runs() {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let base = 1_700_000_000_000_000i64;
        let newest = MAX_TERMINAL_RUNS_WITH_RUNTIME_STATE;
        let mut terminal = Vec::new();
        for index in 0..newest + 2 {
            let run_id = RunId::new();
            seed_run(
                &state,
                run_id,
                "completed",
                100 + index as i64,
                base + index as i64,
            )
            .await;
            terminal.push(run_id);
        }
        let in_flight = RunId::new();
        seed_run(&state, in_flight, "in_progress", 999, base + 10_000).await;

        // The oldest run has a dispatched attempt; the plan must carry the
        // plan id / agent job id that name its node-local buffers.
        let agent_job_id = uuid::Uuid::new_v4();
        seed_request(&state, terminal[0], "build", agent_job_id).await;

        let traces = ResidentRunTraces {
            run_ids: terminal
                .iter()
                .copied()
                .chain(std::iter::once(in_flight))
                .collect(),
            live_log_keys: Vec::new(),
        };
        let trim = plan_completed_run_trim(&state.backend, &traces)
            .await
            .unwrap();
        let dropped: BTreeSet<RunId> = trim.drops.iter().map(|(run_id, _)| *run_id).collect();
        assert_eq!(
            dropped,
            terminal[..2].iter().copied().collect::<BTreeSet<_>>(),
            "only the runs outside the newest {newest} terminal runs are trimmed"
        );
        assert!(
            !dropped.contains(&in_flight),
            "an in-flight run is never trimmed, however old it is"
        );
        let (_, keys) = trim
            .drops
            .iter()
            .find(|(run_id, _)| *run_id == terminal[0])
            .expect("the oldest run is a candidate");
        assert_eq!(keys.logical_job_ids, vec!["build".to_owned()]);
        assert_eq!(keys.agent_job_ids, vec![agent_job_id]);
        assert_eq!(keys.plan_ids, vec![agent_job_id.to_string()]);

        // A run that is not in the database at all resolves to no keys rather
        // than failing: node-local leftovers must stay droppable.
        let keys = RunNodeKeys::resolve(&state.backend, RunId::new())
            .await
            .unwrap();
        assert!(keys.logical_job_ids.is_empty());
        assert!(keys.agent_job_ids.is_empty());
        assert!(keys.plan_ids.is_empty());

        let mut inner = InnerState::default();
        for run_id in terminal.iter().copied().chain(std::iter::once(in_flight)) {
            inner.timeline_events.insert(run_id, Vec::new());
            inner.timeline_events_order.push_back(run_id);
        }
        assert_eq!(trim_completed_runs(&mut inner, &trim), 2);
        assert!(!inner.timeline_events.contains_key(&terminal[0]));
        assert!(!inner.timeline_events.contains_key(&terminal[1]));
        assert!(inner.timeline_events.contains_key(&terminal[2]));
        assert!(inner.timeline_events.contains_key(&in_flight));
        // newest + 2 terminal runs plus the in-flight run, minus the two
        // trimmed ones.
        assert_eq!(inner.timeline_events_order.len(), newest + 1);
    }

    /// The F8 trim frees the live-log buffer of every logical job and attempt of
    /// the trimmed run, not only its timeline ring, and leaves other runs' buffers
    /// alone.
    #[test]
    fn trim_completed_runs_drops_live_log_buffers_by_resolved_keys() {
        let mut inner = InnerState::default();
        let run_id = RunId::new();
        let agent_job_id = uuid::Uuid::new_v4();
        let logical = "build".to_owned();
        let kept_key = "other-job".to_owned();
        for key in [logical.clone(), agent_job_id.to_string(), kept_key.clone()] {
            inner.live_log_lines.insert(
                key.clone(),
                Arc::new(tokio::sync::Mutex::new(LiveLogBuffer::new(1024))),
            );
            inner
                .live_log_tx
                .insert(key.clone(), tokio::sync::broadcast::channel(4).0);
            inner.live_log_closed.insert(key);
        }

        let traces = ResidentRunTraces::of(&inner);
        assert!(
            traces.run_ids.is_empty(),
            "no run-keyed structure was seeded"
        );
        assert_eq!(traces.live_log_keys.len(), 3);

        let trim = CompletedRunTrim {
            drops: vec![(
                run_id,
                RunNodeKeys {
                    logical_job_ids: vec![logical],
                    agent_job_ids: vec![agent_job_id],
                    plan_ids: Vec::new(),
                },
            )],
        };
        assert_eq!(trim_completed_runs(&mut inner, &trim), 1);
        // Only the other run's buffer survives.
        assert_eq!(inner.live_log_lines.len(), 1);
        assert!(inner.live_log_lines.contains_key(&kept_key));
        assert_eq!(inner.live_log_tx.len(), 1);
        assert!(inner.live_log_tx.contains_key(&kept_key));
        assert_eq!(inner.live_log_closed.len(), 1);
        assert!(inner.live_log_closed.contains(&kept_key));
    }

    /// The retention teardown drops a run's node-local attachments and reports
    /// the on-disk paths the caller must delete.
    #[test]
    fn remove_run_everywhere_drops_node_local_attachments() {
        let mut inner = InnerState::default();
        let run_id = RunId::new();
        let plan_id = "plan-1".to_owned();
        let agent_job_id = uuid::Uuid::new_v4();
        let log_key = format!("{plan_id}/log-1");
        inner.logs.insert(log_key.clone(), b"console".to_vec());
        inner.log_bytes_total = 7;
        inner.log_metadata.insert(
            log_key.clone(),
            LogMetadata {
                byte_count: 7,
                line_count: 1,
            },
        );
        inner.log_order.push_back(log_key.clone());
        inner.artifacts.insert(
            "artifact-1".to_owned(),
            ArtifactRecord {
                id: "artifact-1".to_owned(),
                run_id,
                name: "dist".to_owned(),
                file_name: "dist.zip".to_owned(),
                path: "/tmp/dist.zip".to_owned(),
                size: 7,
            },
        );
        let registry_key = format!("{run_id}/dist");
        inner.artifact_v2_registry.insert(
            registry_key.clone(),
            ArtifactV2Entry {
                id: 1,
                workflow_run_backend_id: plan_id.clone(),
                workflow_job_run_backend_id: "job-1".to_owned(),
                name: "dist".to_owned(),
                size: 7,
                created_at: "2026-09-30T00:00:00Z".to_owned(),
                digest: None,
                blob_token: "blob-token".to_owned(),
            },
        );
        inner.artifact_registry_order.push_back(registry_key);
        inner.timeline_events.insert(run_id, Vec::new());
        inner.timeline_events_order.push_back(run_id);
        inner.dap_ports.insert(
            run_id,
            DapPortRegistration {
                port: 4242,
                job_id: JobId("build".to_owned()),
            },
        );
        inner
            .plan_secret_masker
            .insert(plan_id.clone(), Arc::new(vec!["s3cret".to_owned()]));
        inner.diag_upload_tokens.insert(
            "diag-token".to_owned(),
            DiagUploadToken {
                job_id: agent_job_id.to_string(),
                created_unix: 0,
            },
        );

        // The resident-trace snapshot sees the run through its run-keyed
        // structures, so the F8 planner would consider it.
        let traces = ResidentRunTraces::of(&inner);
        assert!(traces.run_ids.contains(&run_id));

        let keys = RunNodeKeys {
            logical_job_ids: vec!["build".to_owned()],
            agent_job_ids: vec![agent_job_id],
            plan_ids: vec![plan_id.clone()],
        };
        let removal = remove_run_everywhere(&mut inner, run_id, &keys);

        assert_eq!(removal.log_keys, vec![log_key.clone()]);
        assert!(inner.logs.is_empty());
        assert_eq!(inner.log_bytes_total, 0);
        assert!(inner.log_metadata.is_empty());
        assert!(inner.log_order.is_empty());
        assert_eq!(
            removal.artifact_paths,
            vec![std::path::PathBuf::from("/tmp/dist.zip")]
        );
        assert_eq!(
            removal.artifact_v2_blob_tokens,
            vec!["blob-token".to_owned()]
        );
        assert!(inner.artifacts.is_empty());
        assert!(inner.artifact_v2_registry.is_empty());
        assert!(inner.artifact_registry_order.is_empty());
        assert!(!inner.timeline_events.contains_key(&run_id));
        assert!(inner.timeline_events_order.is_empty());
        assert!(inner.dap_ports.is_empty());
        assert!(inner.plan_secret_masker.is_empty());
        assert!(inner.diag_upload_tokens.is_empty());
    }
}
