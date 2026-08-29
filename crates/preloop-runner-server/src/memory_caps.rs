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
pub(crate) const MAX_LOG_BYTES_PER_KEY: usize = 16 * 1024 * 1024;

/// F1 — per-plan retained byte budget across all of a plan's logs. The oldest
/// logs of the plan are evicted once the total (bytes or entry count) exceeds
/// the budget, so a flood of distinct `log_id`s cannot grow the map either.
pub(crate) const MAX_LOG_BYTES_PER_PLAN: usize = 64 * 1024 * 1024;

/// F1 — per-plan retained log entry cap. Empty logs carry no bytes, so the
/// byte budget alone would let an attacker create unbounded distinct keys.
pub(crate) const MAX_LOGS_PER_PLAN: usize = 512;

/// F1 — global retained byte budget across all plans. Prevents a runner
/// from fabricating unlimited `plan_id` values to bypass the per-plan cap.
pub(crate) const MAX_LOG_BYTES_GLOBAL: usize = 256 * 1024 * 1024;

/// F1 — global retained log entry cap.
pub(crate) const MAX_LOGS_GLOBAL: usize = 4096;

/// F2 — per-timeline record cap. PATCH upserts beyond this evict the oldest
/// (deterministically first-keyed) records. Real jobs stay far below it.
pub(crate) const MAX_TIMELINE_RECORDS: usize = 1024;

/// F2 — per-timeline byte budget for stored records. Each record's
/// `currentOperation` can be ~1 MiB; count caps alone leave an unbounded
/// byte budget (1024 × 4096 × 1 MiB). Aggregate bytes are bounded here.
pub(crate) const MAX_TIMELINE_BYTES_PER_TIMELINE: usize = 8 * 1024 * 1024;

/// F3 — per-run ring-buffer cap for projected timeline events. The oldest
/// events are drained once the retained Vec exceeds this.
pub(crate) const MAX_TIMELINE_EVENTS: usize = 2048;

/// F2 — global bound on distinct timeline keys (`{plan}/{timeline}`), which a
/// runner controls directly. Oldest-keyed timelines are evicted wholesale
/// (records and change-id counter together) past the cap.
pub(crate) const MAX_TIMELINE_KEYS: usize = 4096;

/// F3 — global bound on distinct run ids in the timeline event map. A runner
/// can PATCH for fabricated plan ids, which would otherwise mint unbounded
/// per-run event buckets (each itself capped by [`MAX_TIMELINE_EVENTS`]).
pub(crate) const MAX_TIMELINE_EVENT_KEYS: usize = 4096;

/// F5 — per-block cap for staged blob blocks (matches the official runner's
/// 4 MiB Azure SDK block size; larger blocks are rejected with 413).
pub(crate) const MAX_BLOCK_BYTES: usize = 4 * 1024 * 1024;

/// F5 — cap on the number of block IDs in a blocklist commit request.
pub(crate) const MAX_BLOCKLIST_BLOCKS: usize = 10_000;

/// F5 — cap on the assembled blob size. Assembly streams block files into the
/// destination file and never materializes the whole blob in memory, but a
/// blocklist referencing more than this budget is rejected up front.
pub(crate) const MAX_ASSEMBLED_BYTES: usize = 512 * 1024 * 1024;

/// F6 — server-side cap on timeline records returned by a single GET page.
/// `?top=` larger than this is clamped, `?skip=` pages further.
pub(crate) const MAX_TOP_RECORDS: usize = 500;

/// F7 — per-job cap on in-flight pending uploads (artifact v2 and cache v2).
/// The job is taken from the signed runtime token scope, so a runner cannot
/// evade the cap by inventing other job ids in request bodies.
pub(crate) const MAX_PENDING_PER_JOB: usize = 32;

/// F7 — global cap on minted cache download tokens; the oldest are evicted.
pub(crate) const MAX_CACHE_DL_TOKENS: usize = 1024;

/// F7 — per-run cap on finalized artifact v2 registry entries, mirroring
/// GitHub's "500 artifacts per workflow run" limit.
pub(crate) const MAX_ARTIFACTS_PER_RUN: usize = 500;

/// F7 — global cap on the artifact v2 registry; the oldest finalized entries
/// are evicted past this so a flood of fabricated run ids stays bounded.
pub(crate) const MAX_ARTIFACT_REGISTRY_ENTRIES: usize = 10_000;

/// F7 — how long a pending upload (or download token) survives without being
/// finalized/consumed before the reaper sweeps it. Jobs that never finish
/// their upload leave an entry behind; without a TTL those would accumulate.
pub(crate) const PENDING_UPLOAD_TTL: Duration = Duration::from_secs(3600);

/// Unix seconds for pending-upload timestamps.
pub(crate) fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0)
}

/// Job backend id proven by a request's bearer token, if it is a job runtime
/// token (`scp: Actions.Results:{plan}:{job}`). The engine token and runner
/// listen tokens return `None` — those callers are not one job.
pub(crate) fn job_backend_id_from_bearer(state: &AppState, headers: &HeaderMap) -> Option<String> {
    let token = bearer_from_headers(headers)?;
    if token == state.system_token {
        return None;
    }
    let claims = state.verify_local_jwt_claims(token)?;
    let scope = claims.get("scp")?.as_str()?;
    scope
        .strip_prefix("Actions.Results:")?
        .rsplit(':')
        .next()
        .map(str::to_owned)
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
pub(crate) fn trim_plan_logs(inner: &mut InnerState, plan_id: &str) -> Vec<String> {
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
pub(crate) fn trim_timeline_after_patch(
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
pub(crate) fn trim_timeline_events(inner: &mut InnerState, run_id: RunId) {
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
pub(crate) fn trim_cache_dl_tokens(inner: &mut InnerState) {
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

/// F7 — bound the finalized artifact v2 registry: `MAX_ARTIFACTS_PER_RUN` per
/// run and `MAX_ARTIFACT_REGISTRY_ENTRIES` globally, evicting oldest entries
/// by finalization order (not lexicographic key order).
pub(crate) fn trim_artifact_registry(inner: &mut InnerState) {
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
/// session-liveness sweep's treatment of restored state.
pub(crate) fn sweep_pending_uploads(inner: &mut InnerState, now_unix_secs: i64) {
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
}
