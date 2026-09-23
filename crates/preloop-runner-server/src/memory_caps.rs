use super::*;

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

/// F5 — per-block cap for staged blob blocks. upload-artifact v4 stages
/// 8 MiB blocks (observed Content-Length 8388608 from actions/upload-artifact
/// against the nushell build); larger blocks are rejected with 413.
pub const MAX_BLOCK_BYTES: usize = 8 * 1024 * 1024;

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

/// F8 — retained *completed* run records. `inner.runs` is the source of truth
/// for run APIs, but a `RunRecord` is heavy (full `WorkflowSubmission`,
/// `github` context JSON, job details — ~1 MiB each) and completed runs are
/// never queried by the scheduler. Without a bound, every completed run
/// accumulates in heap forever and `load_into` restores all of them at boot
/// (observed: 3203 runs ≈ 4.4 GiB RSS). Live runs are never evicted; the
/// durable `runs` table keeps the full history regardless.
pub const MAX_COMPLETED_RUNS_RETAINED: usize = 256;

/// F8b — terminal runs whose heavy runtime state (live-log buffers, step
/// records, timeline projections) stays in memory. `RunRecord`s are retained
/// for `MAX_COMPLETED_RUNS_RETAINED`, but a finished run's live-log tail is
/// capped at 64 MiB *per job* and its step/timeline projections are only
/// dropped on request purge — neither is needed once the run is old enough
/// that nothing follows it live. Without this bound, ~40 runs/hour of CI
/// accumulated ~4 GiB/hour of retained buffers on cpane and the kernel OOM
/// killer kept restarting the engine mid-run (starving every queued job).
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

/// F8 — bound retained completed runs to `MAX_COMPLETED_RUNS_RETAINED`,
/// evicting the oldest `completed_at` first. Runs still in flight
/// (`completed_at.is_none()`) are never evicted. Called at boot after
/// `load_into` and from `emit` when a terminal `RunStatus` arrives, so the
/// map cannot grow past the cap between restarts. Evicted runs stay in the
/// durable `runs` table — this only drops the in-memory copy.
pub fn trim_completed_runs(inner: &mut InnerState) {
    // Collect terminal runs oldest-first (completion time, then created_at,
    // then run_id for determinism).
    let mut keyed: Vec<(
        Option<chrono::DateTime<chrono::Utc>>,
        chrono::DateTime<chrono::Utc>,
        RunId,
    )> = inner
        .runs
        .values()
        .filter(|run| run.completed_at.is_some())
        .map(|run| (run.completed_at, run.created_at, run.run_id))
        .collect();
    keyed.sort();
    let completed = keyed.len();

    // F8b — drop heavy runtime state for terminal runs outside the newest
    // `MAX_TERMINAL_RUNS_WITH_RUNTIME_STATE`. The run record itself stays
    // (subject to the 256 cap below); only the per-job live-log buffers,
    // step records, and timeline projections go. These are the dominant
    // heap consumers for finished runs and are unreachable once nothing
    // follows the run live.
    let keep_runtime = completed.saturating_sub(MAX_TERMINAL_RUNS_WITH_RUNTIME_STATE);
    for (_, _, run_id) in keyed.iter().take(keep_runtime) {
        drop_run_runtime_state(inner, *run_id);
    }

    // F8 — evict the oldest completed run records past the cap. Every
    // FK-bearing reference to the run must go with it: `store_inner` mirrors
    // memory wholesale, so a leftover `job_requests`/`session_active_requests`/
    // queued-job row for an evicted run violates `REFERENCES runs` and makes
    // EVERY subsequent full persist fail (observed: runner registration 500s
    // → provisioning retry loop → CI stall).
    let excess = completed.saturating_sub(MAX_COMPLETED_RUNS_RETAINED);
    for (_, _, run_id) in keyed.into_iter().take(excess) {
        drop_run_runtime_state(inner, run_id);
        purge_evicted_run(inner, run_id);
        inner.runs.remove(&run_id);
    }
}

/// Remove every in-memory reference to an evicted run that `store_inner`
/// would persist into an FK-constrained table. Mirrors the
/// `retire_node_requests` Purge path in `runtime_scheduling`, run-wide.
/// The durable rows stay; a restart skips them via the restore guards.
fn purge_evicted_run(inner: &mut InnerState, run_id: RunId) {
    let request_ids: Vec<i64> = inner
        .job_requests
        .iter()
        .filter(|(_, record)| record.run_id == run_id)
        .map(|(id, _)| *id)
        .collect();
    for request_id in request_ids {
        inner
            .session_active_requests
            .retain(|_, &mut rid| rid != request_id);
        inner.inflight_requests.remove(&request_id);
        inner.github_token_requests.remove(&request_id);
        inner.broker_messages.remove(&request_id);
        let Some(record) = inner.job_requests.remove(&request_id) else {
            continue;
        };
        // Sibling requests may own the current index entry; only drop one
        // that still points at this request (same guard as the Purge path).
        if inner.plan_requests.get(&record.plan_id) == Some(&request_id) {
            inner.plan_requests.remove(&record.plan_id);
        }
        if inner.agent_job_requests.get(&record.agent_job_id) == Some(&request_id) {
            inner.agent_job_requests.remove(&record.agent_job_id);
        }
        if inner.timeline_requests.get(&record.timeline_id) == Some(&request_id) {
            inner.timeline_requests.remove(&record.timeline_id);
        }
        inner.job_steps.remove(&record.agent_job_id);
        inner.job_steps_revision.remove(&record.agent_job_id);
    }
    // Queued-job rows carry `jobs.run_id REFERENCES runs`. A terminal run
    // should hold none, but a cancelled run can leave strays — purge them
    // rather than let one row poison every persist.
    inner.queue.retain(|job| job.run_id != run_id);
    inner.pending_jobs.retain(|job| job.run_id != run_id);
    inner.concurrency_blocked.retain(|job| job.run_id != run_id);
    inner.held_runs.remove(&run_id);
    inner.queued_at.retain(|(rid, _), _| *rid != run_id);
    inner.job_assignments.retain(|(rid, _), _| *rid != run_id);
    inner.pool_pending.retain(|(rid, _), _| *rid != run_id);
    inner.cancellation_queue.retain(|c| c.run_id != run_id);
    inner.id_token_grants.retain(|(rid, _), _| *rid != run_id);
    inner.oidc_job_contexts.retain(|(rid, _), _| *rid != run_id);
}

/// Drop the heavy per-job runtime state of a run: retained live-log buffers
/// (up to 64 MiB each), step records, and timeline projections. The durable
/// store keeps the authoritative copies; this only frees the in-memory
/// projections that exist to serve live followers and run-scoped reads.
fn drop_run_runtime_state(inner: &mut InnerState, run_id: RunId) {
    // Logical job ids from the run record; agent job ids and plan ids from
    // the job requests that dispatched them.
    let logical_ids: Vec<String> = inner
        .runs
        .get(&run_id)
        .map(|run| run.jobs.keys().map(|job| job.0.clone()).collect())
        .unwrap_or_default();
    let mut agent_ids: Vec<uuid::Uuid> = Vec::new();
    let mut plan_ids: Vec<String> = Vec::new();
    for record in inner.job_requests.values() {
        if record.run_id == run_id {
            agent_ids.push(record.agent_job_id);
            plan_ids.push(record.plan_id.clone());
        }
    }

    for key in logical_ids
        .iter()
        .cloned()
        .chain(agent_ids.iter().map(uuid::Uuid::to_string))
    {
        inner.live_log_lines.remove(&key);
        inner.live_log_tx.remove(&key);
        inner.live_log_closed.remove(&key);
    }
    for agent_id in &agent_ids {
        inner.job_steps.remove(agent_id);
        inner.job_steps_revision.remove(agent_id);
    }
    inner.timeline_events.remove(&run_id);
    inner.timeline_events_order.retain(|id| *id != run_id);
    for plan_id in &plan_ids {
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

    /// `trim_completed_runs` evicts a run record past the retention cap; every
    /// reference that `store_inner` persists into an FK-constrained table must
    /// go with it, or the next full snapshot fails on `REFERENCES runs` and
    /// every persist fails from then on (runner registration 500s, CI stall).
    #[test]
    fn evicted_run_purges_fk_bearing_state() {
        let mut inner = InnerState::default();
        let base = chrono::Utc::now();
        let mut run_ids = Vec::new();
        for index in 0..=MAX_COMPLETED_RUNS_RETAINED {
            let run_id = RunId::new();
            run_ids.push(run_id);
            inner.runs.insert(
                run_id,
                RunRecord {
                    run_id,
                    webhook_delivery_id: None,
                    run_name: None,
                    submission: Arc::new(WorkflowSubmission {
                        repository: "test/repo".to_owned(),
                        ..Default::default()
                    }),
                    jobs: BTreeMap::new(),
                    status: ExecutionStatus::Success,
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
                    job_check_run_ids: BTreeMap::new(),
                    reusable_calls: BTreeMap::new(),
                    jobs_list: Vec::new(),
                    created_at: base,
                    started_at: Some(base),
                    // Oldest completed_at first: index 0 is the eviction victim.
                    completed_at: Some(base + chrono::Duration::seconds(index as i64)),
                    run_number: index as u64,
                    run_attempt: 1,
                    workflow_path_str: ".github/workflows/ci.yml".to_owned(),
                    event: "push".to_owned(),
                    conclusion: Some("success".to_owned()),
                    push_state: None,
                    snapshot_timing: None,
                    fork_approval_pending: false,
                    fork_approval_requested_at_unix_nanos: None,
                    fork_approved_at_unix_nanos: None,
                    fork_approval_note: None,
                },
            );
        }
        let evicted = run_ids[0];
        let retained = run_ids[1];

        // FK-bearing state for the victim run: a settled job request plus the
        // claim/index maps that reference it.
        let request_id = 42i64;
        let agent_job_id = uuid::Uuid::new_v4();
        let plan_id = "plan-evicted".to_owned();
        let timeline_id = uuid::Uuid::new_v4();
        inner.job_requests.insert(
            request_id,
            TaskAgentJobRequestRecord {
                request_id,
                run_id: evicted,
                job_id: JobId("build".to_owned()),
                agent_job_id,
                plan_id: plan_id.clone(),
                plan_type: "build".to_owned(),
                timeline_id,
                result: Some(ExecutionStatus::Success),
                locked_until: String::new(),
                claimed_at: None,
                owner_runner_id: None,
                started_at: None,
                last_renewed_at: None,
                timeout_triggered: false,
                debug_token_issued: false,
            },
        );
        inner
            .inflight_requests
            .insert(request_id, (evicted, JobId("build".to_owned())));
        inner.plan_requests.insert(plan_id, request_id);
        inner.agent_job_requests.insert(agent_job_id, request_id);
        inner.timeline_requests.insert(timeline_id, request_id);
        inner
            .session_active_requests
            .insert("session-1".to_owned(), request_id);
        inner.broker_messages.insert(
            request_id,
            serde_json::from_value(serde_json::json!({
                "jobId": agent_job_id.to_string(),
                "requestId": request_id,
                "plan": {"planId": "plan-evicted", "planType": "build", "version": 1,
                         "artifactUri": "", "artifactLocation": ""},
                "timeline": {"id": timeline_id.to_string(), "changeId": 0, "location": null},
                "jobName": "build",
                "lockedUntil": "",
                "resources": {"endpoints": []},
                "steps": [],
                "snapshot": null
            }))
            .unwrap(),
        );
        inner.github_token_requests.insert(
            request_id,
            GitHubTokenRequest {
                repository: "test/repo".to_owned(),
                permissions: BTreeMap::new(),
                declared: false,
                untrusted: false,
            },
        );
        inner.cancellation_queue.push_back(QueuedCancellation {
            run_id: evicted,
            job_id: JobId("build".to_owned()),
            agent_job_id,
        });
        inner
            .id_token_grants
            .insert((evicted, JobId("build".to_owned())), true);
        inner.oidc_job_contexts.insert(
            (evicted, JobId("build".to_owned())),
            OidcJobContext {
                environment: None,
                job_workflow_ref: None,
                job_workflow_sha: None,
            },
        );
        inner.queued_at.insert(
            (evicted, JobId("build".to_owned())),
            std::time::SystemTime::now(),
        );
        inner.job_assignments.insert(
            (evicted, JobId("build".to_owned())),
            AssignmentRecord {
                runner_id: None,
                at: std::time::SystemTime::now(),
                first_at: std::time::SystemTime::now(),
            },
        );
        trim_completed_runs(&mut inner);

        assert!(
            !inner.runs.contains_key(&evicted),
            "the oldest completed run must be evicted"
        );
        assert!(inner.runs.contains_key(&retained));
        assert!(
            !inner.job_requests.contains_key(&request_id),
            "job_requests rows for an evicted run violate REFERENCES runs"
        );
        assert!(!inner.inflight_requests.contains_key(&request_id));
        assert!(!inner.plan_requests.values().any(|id| *id == request_id));
        assert!(!inner
            .agent_job_requests
            .values()
            .any(|id| *id == request_id));
        assert!(!inner.timeline_requests.values().any(|id| *id == request_id));
        assert!(
            !inner
                .session_active_requests
                .values()
                .any(|id| *id == request_id),
            "session_active_requests rows for a dropped request violate REFERENCES job_requests"
        );
        assert!(!inner.broker_messages.contains_key(&request_id));
        assert!(!inner.github_token_requests.contains_key(&request_id));
        assert!(!inner.job_steps.contains_key(&agent_job_id));
        assert!(inner.cancellation_queue.is_empty());
        assert!(inner.id_token_grants.is_empty());
        assert!(inner.oidc_job_contexts.is_empty());
        assert!(inner.queued_at.is_empty());
        assert!(inner.job_assignments.is_empty());
        assert!(inner.pool_pending.is_empty());
    }
}
