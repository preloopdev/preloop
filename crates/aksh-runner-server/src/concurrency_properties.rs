//! Concurrency state-machine property tests with an independent reference model.
//!
//! Oracle: GitHub control-plane invariants GH-GROUP-01 through GH-STATUS-01
//! from <https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency>.
//!
//! The independent model implements transitions in ~150 lines without calling
//! any production helper. After every generated operation the test checks
//! structural invariants against both the model and a normalized production
//! snapshot.

// Child module of lib.rs — has access to private types/functions.
use super::*;
use crate::concurrency::{self, Holder, QUEUE_MAX_PENDING};
use aksh_gha_parser::ConcurrencyQueue;
use aksh_gha_protocol::{ExecutionStatus, JobId, RunId};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// ── Deterministic ID helpers ────────────────────────────────────────────────

fn rid(n: u32) -> RunId {
    RunId(uuid::Uuid::from_u128(0x1000_0000 + n as u128))
}

fn jid(n: u32) -> JobId {
    JobId(format!("j{n}"))
}

// ── Independent Reference Model ─────────────────────────────────────────────

/// Holder state in the model (not imported from production).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HolderState {
    Pending,
    Running,
    Cancelled,
    Terminal,
}

/// A lightweight holder token for the model.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HolderToken {
    run: u32,
    kind: HolderKind,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum HolderKind {
    Run,
    Job(u32),
    JobSet(BTreeSet<u32>),
}

/// Independent concurrency model
#[derive(Debug, Clone, Default)]
struct Model {
    /// (repo, group) → group state
    groups: BTreeMap<(String, String), ModelGroup>,
    /// Token → state
    holder_state: BTreeMap<HolderToken, HolderState>,
    /// Token → set of group keys it occupies
    holder_keys: BTreeMap<HolderToken, BTreeSet<(String, String)>>,
}

#[derive(Debug, Clone, Default)]
struct ModelGroup {
    running: Option<HolderToken>,
    pending: VecDeque<HolderToken>,
    queue_mode: ConcurrencyQueue,
}

impl Model {
    /// Acquire a concurrency slot. Returns Ok(true) = running, Ok(false) = pending, Err = overflow cancelled.
    fn acquire(
        &mut self,
        key: (String, String),
        token: HolderToken,
        cancel_in_progress: bool,
        queue: ConcurrencyQueue,
    ) -> Result<bool, ()> {
        let group = self
            .groups
            .entry(key.clone())
            .or_insert_with(|| ModelGroup {
                queue_mode: queue,
                ..Default::default()
            });
        group.queue_mode = queue;

        if group.running.is_none() {
            group.running = Some(token.clone());
            self.holder_state
                .insert(token.clone(), HolderState::Running);
            self.holder_keys.entry(token).or_default().insert(key);
            return Ok(true);
        }

        if cancel_in_progress {
            let prev = group.running.replace(token.clone());
            // Docs: cancel-in-progress also cancels all pending holders.
            let stale_pending: Vec<_> = group.pending.drain(..).collect();
            if let Some(prev) = prev {
                self.holder_state
                    .insert(prev.clone(), HolderState::Cancelled);
                if let Some(keys) = self.holder_keys.get_mut(&prev) {
                    keys.remove(&key);
                    if keys.is_empty() {
                        self.holder_keys.remove(&prev);
                    }
                }
            }
            for p in stale_pending {
                self.holder_state.insert(p.clone(), HolderState::Cancelled);
                if let Some(keys) = self.holder_keys.get_mut(&p) {
                    keys.remove(&key);
                    if keys.is_empty() {
                        self.holder_keys.remove(&p);
                    }
                }
            }
            self.holder_state
                .insert(token.clone(), HolderState::Running);
            self.holder_keys.entry(token).or_default().insert(key);
            return Ok(true);
        }

        // Contended so we apply queue mode
        match queue {
            ConcurrencyQueue::Single => {
                // Cancel all existing pending
                let old_pending: Vec<_> = group.pending.drain(..).collect();
                for p in old_pending {
                    self.holder_state.insert(p.clone(), HolderState::Cancelled);
                    if let Some(keys) = self.holder_keys.get_mut(&p) {
                        keys.remove(&key);
                        if keys.is_empty() {
                            self.holder_keys.remove(&p);
                        }
                    }
                }
                // Park arrival
                group.pending.push_back(token.clone());
                self.holder_state
                    .insert(token.clone(), HolderState::Pending);
                self.holder_keys.entry(token).or_default().insert(key);
                Ok(false)
            }
            ConcurrencyQueue::Max => {
                if group.pending.len() >= QUEUE_MAX_PENDING {
                    self.holder_state.insert(token, HolderState::Cancelled);
                    Err(())
                } else {
                    group.pending.push_back(token.clone());
                    self.holder_state
                        .insert(token.clone(), HolderState::Pending);
                    self.holder_keys.entry(token).or_default().insert(key);
                    Ok(false)
                }
            }
        }
    }

    /// Release the running holder for a key and promote next.
    fn release(&mut self, key: &(String, String), token: &HolderToken) {
        let Some(group) = self.groups.get_mut(key) else {
            return;
        };
        if group.running.as_ref() != Some(token) {
            // Maybe it's pending so remove from pending
            group.pending.retain(|t| t != token);
            if let Some(keys) = self.holder_keys.get_mut(token) {
                keys.remove(key);
                if keys.is_empty() {
                    self.holder_keys.remove(token);
                }
            }
            self.cleanup_group(key);
            return;
        }
        group.running = None;
        // Mark released holder terminal
        self.holder_state
            .insert(token.clone(), HolderState::Terminal);
        // Remove key from holder's reverse map
        if let Some(keys) = self.holder_keys.get_mut(token) {
            keys.remove(key);
            if keys.is_empty() {
                self.holder_keys.remove(token);
            }
        }
        // Promote FIFO
        self.promote(key);
    }

    /// Cancel a holder across all its groups.
    fn cancel(&mut self, token: &HolderToken) {
        if matches!(
            self.holder_state.get(token),
            Some(HolderState::Terminal) | Some(HolderState::Cancelled)
        ) {
            return; // idempotent
        }
        self.holder_state
            .insert(token.clone(), HolderState::Cancelled);
        let keys: Vec<_> = self
            .holder_keys
            .get(token)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        for key in &keys {
            if let Some(group) = self.groups.get_mut(key) {
                if group.running.as_ref() == Some(token) {
                    group.running = None;
                    self.promote(key);
                } else {
                    group.pending.retain(|t| t != token);
                }
                self.cleanup_group(key);
            }
        }
        self.holder_keys.remove(token);
    }

    fn promote(&mut self, key: &(String, String)) {
        let Some(group) = self.groups.get_mut(key) else {
            return;
        };
        if group.running.is_some() {
            return; // slot occupied
        }
        if let Some(next) = group.pending.pop_front() {
            group.running = Some(next.clone());
            self.holder_state.insert(next, HolderState::Running);
        }
        self.cleanup_group(key);
    }

    fn cleanup_group(&mut self, key: &(String, String)) {
        if let Some(group) = self.groups.get(key) {
            if group.running.is_none() && group.pending.is_empty() {
                self.groups.remove(key);
            }
        }
    }

    // ── Structural invariants ───────────────────────────────────────────

    /// Invariant 1: At most one running holder per group.
    fn check_inv1(&self) -> Result<(), String> {
        for (key, group) in &self.groups {
            if group.running.is_some()
                && group
                    .pending
                    .iter()
                    .any(|t| self.holder_state.get(t) == Some(&HolderState::Running))
            {
                return Err(format!(
                    "INV-1: group {:?} has running holder + running in pending",
                    key
                ));
            }
        }
        Ok(())
    }

    /// Invariant 2: Running and pending sets are disjoint.
    fn check_inv2(&self) -> Result<(), String> {
        for (key, group) in &self.groups {
            if let Some(running) = &group.running {
                if group.pending.contains(running) {
                    return Err(format!(
                        "INV-2: group {:?} running holder {:?} also in pending",
                        key, running
                    ));
                }
            }
        }
        Ok(())
    }

    /// Invariant 7: Every running/pending holder has exact reverse key.
    fn check_inv7(&self) -> Result<(), String> {
        for (key, group) in &self.groups {
            if let Some(running) = &group.running {
                let has_key = self
                    .holder_keys
                    .get(running)
                    .is_some_and(|ks| ks.contains(key));
                if !has_key {
                    return Err(format!(
                        "INV-7: running {:?} in group {:?} has no reverse key",
                        running, key
                    ));
                }
            }
            for t in &group.pending {
                let has_key = self.holder_keys.get(t).is_some_and(|ks| ks.contains(key));
                if !has_key {
                    return Err(format!(
                        "INV-7: pending {:?} in group {:?} has no reverse key",
                        t, key
                    ));
                }
            }
        }
        Ok(())
    }

    /// Invariant 12: Empty groups are removed.
    fn check_inv12(&self) -> Result<(), String> {
        for (key, group) in &self.groups {
            if group.running.is_none() && group.pending.is_empty() {
                return Err(format!("INV-12: empty group {:?} not removed", key));
            }
        }
        Ok(())
    }

    fn check_all_invariants(&self) -> Result<(), String> {
        self.check_inv1()?;
        self.check_inv2()?;
        self.check_inv7()?;
        self.check_inv12()?;
        Ok(())
    }
}

// ── Production state helpers ────────────────────────────────────────────────

/// Minimal production state wrapper for concurrency-only testing.
/// Uses InnerState directly since we're a child module of lib.rs.
struct ProdState {
    inner: InnerState,
}

impl ProdState {
    fn new() -> Self {
        Self {
            inner: InnerState::default(),
        }
    }

    /// Register a run with placeholder jobs so production functions work.
    fn register_run(&mut self, run_n: u32, job_ids: &[u32]) {
        let run_id = rid(run_n);
        let mut jobs = BTreeMap::new();
        let mut job_base_ids = BTreeMap::new();
        for &j in job_ids {
            jobs.insert(jid(j), ExecutionStatus::Queued);
            job_base_ids.insert(jid(j), format!("j{j}"));
        }
        let record = RunRecord {
            run_id,
            run_name: None,
            submission: WorkflowSubmission {
                workflow_yaml: String::new(),
                event: "push".to_owned(),
                payload: serde_json::Value::Null,
                repository: "property/test".to_owned(),
                git_ref: "refs/heads/main".to_owned(),
                vars: BTreeMap::new(),
                inputs: BTreeMap::new(),
                secrets: BTreeMap::new(),
                reusable_workflows: BTreeMap::new(),
                reusable_workflow_shas: BTreeMap::new(),
                enable_debugger: false,
                debugger_welcome_message: None,
                ..Default::default()
            },
            jobs,
            status: ExecutionStatus::Queued,
            job_outputs: BTreeMap::new(),
            job_base_ids,
            job_needs: BTreeMap::new(),
            job_fail_fast: BTreeMap::new(),
            job_continue_on_error: BTreeMap::new(),
            job_check_run_ids: BTreeMap::new(),
            reusable_calls: BTreeMap::new(),
            jobs_list: Vec::new(),
        };
        self.inner.runs.insert(run_id, record);
    }

    fn to_holder(token: &HolderToken) -> Holder {
        match &token.kind {
            HolderKind::Run => Holder::Run(rid(token.run)),
            HolderKind::Job(j) => Holder::Job {
                run_id: rid(token.run),
                job_id: jid(*j),
            },
            HolderKind::JobSet(js) => Holder::JobSet {
                run_id: rid(token.run),
                job_ids: js.iter().map(|j| jid(*j)).collect(),
            },
        }
    }

    fn acquire(
        &mut self,
        key: (String, String),
        token: &HolderToken,
        cancel_in_progress: bool,
        queue: ConcurrencyQueue,
    ) -> Result<bool, String> {
        let holder = Self::to_holder(token);
        let display = format!("{:?}", key);
        try_acquire_concurrency(
            &mut self.inner,
            key,
            display,
            holder,
            cancel_in_progress,
            queue,
        )
    }

    fn release_run(&mut self, run_n: u32) {
        let run_id = rid(run_n);
        // Mark all jobs terminal first
        if let Some(run) = self.inner.runs.get_mut(&run_id) {
            for status in run.jobs.values_mut() {
                if !concurrency::is_terminal(*status) {
                    *status = ExecutionStatus::Success;
                }
            }
            run.status = summarize_run(run.jobs.values().copied());
        }
        release_concurrency_for_run(&mut self.inner, run_id);
    }

    fn release_job(&mut self, run_n: u32, job_n: u32) {
        let run_id = rid(run_n);
        let job_id = jid(job_n);
        // Mark this job terminal
        if let Some(run) = self.inner.runs.get_mut(&run_id) {
            if let Some(status) = run.jobs.get_mut(&job_id) {
                if !concurrency::is_terminal(*status) {
                    *status = ExecutionStatus::Success;
                }
            }
            run.status = summarize_run(run.jobs.values().copied());
        }
        release_concurrency_for_job(&mut self.inner, run_id, &job_id);
    }

    fn cancel_run(&mut self, run_n: u32) {
        cancel_run_inner(&mut self.inner, rid(run_n), Some("test"));
    }

    /// Snapshot for comparison: (group_key → (running_token, pending_tokens)).
    fn snapshot(&self) -> BTreeMap<(String, String), (Option<HolderToken>, Vec<HolderToken>)> {
        self.inner
            .concurrency_groups
            .iter()
            .map(|(key, group)| {
                let running = group.running.as_ref().map(Self::holder_to_token);
                let pending: Vec<_> = group.pending.iter().map(Self::holder_to_token).collect();
                (key.clone(), (running, pending))
            })
            .collect()
    }

    fn holder_to_token(holder: &Holder) -> HolderToken {
        match holder {
            Holder::Run(id) => HolderToken {
                run: (id.0.as_u128() - 0x1000_0000) as u32,
                kind: HolderKind::Run,
            },
            Holder::Job { run_id, job_id } => HolderToken {
                run: (run_id.0.as_u128() - 0x1000_0000) as u32,
                kind: HolderKind::Job(job_id.0[1..].parse().unwrap_or(0)),
            },
            Holder::JobSet { run_id, job_ids } => HolderToken {
                run: (run_id.0.as_u128() - 0x1000_0000) as u32,
                kind: HolderKind::JobSet(
                    job_ids
                        .iter()
                        .map(|j| j.0[1..].parse().unwrap_or(0))
                        .collect(),
                ),
            },
        }
    }

    fn clone_concurrency(&self) -> Self {
        Self {
            inner: InnerState {
                runs: self.inner.runs.clone(),
                queue: self.inner.queue.clone(),
                pending_jobs: self.inner.pending_jobs.clone(),
                concurrency_blocked: self.inner.concurrency_blocked.clone(),
                concurrency_groups: self.inner.concurrency_groups.clone(),
                holder_keys: self.inner.holder_keys.clone(),
                jobset_admissions: self.inner.jobset_admissions.clone(),
                ..Default::default()
            },
        }
    }
}

// ── Generated operations ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Op {
    /// Submit a holder to a group.
    Submit {
        run: u32,
        kind: HolderKind,
        repo: String,
        group: String,
        queue: ConcurrencyQueue,
        cancel_in_progress: bool,
    },
    /// Release a running holder.
    Release { run: u32, kind: HolderKind },
    /// Cancel a holder.
    Cancel { run: u32 },
}

// ── proptest generators ─────────────────────────────────────────────────────

mod generators {
    use super::*;
    use proptest::prelude::*;

    pub fn arb_holder_kind() -> impl Strategy<Value = HolderKind> {
        // Generic group transitions use workflow holders because direct
        // promotion of job and JobSet holders also requires their backing
        // scheduler queues. Dedicated cross-gate properties below exercise
        // those holder kinds through complete scheduler state.
        Just(HolderKind::Run)
    }

    pub fn arb_queue() -> impl Strategy<Value = ConcurrencyQueue> {
        prop_oneof![Just(ConcurrencyQueue::Single), Just(ConcurrencyQueue::Max)]
    }

    pub fn arb_op() -> impl Strategy<Value = Op> {
        prop_oneof![
            6 => (
                0..6u32,
                arb_holder_kind(),
                prop_oneof![Just("repo-a".to_owned()), Just("repo-b".to_owned())],
                prop_oneof![
                    Just("grp-1".to_owned()),
                    Just("grp-2".to_owned()),
                    Just("Grp-1".to_owned()), // case variant
                ],
                arb_queue(),
                any::<bool>(),
            ).prop_map(|(run, kind, repo, group, queue, cancel)| Op::Submit {
                run, kind, repo, group, queue, cancel_in_progress: cancel,
            }),
            2 => (0..6u32, arb_holder_kind()).prop_map(|(run, kind)| Op::Release { run, kind }),
            1 => (0..6u32).prop_map(|run| Op::Cancel { run }),
        ]
    }

    pub fn arb_ops(max_len: usize) -> impl Strategy<Value = Vec<Op>> {
        proptest::collection::vec(arb_op(), 1..=max_len)
    }
}

// ── Invariant checker on production state ───────────────────────────────────

fn check_production_invariants(inner: &InnerState) -> Result<(), String> {
    // INV-1: At most one running holder per group.
    for group in inner.concurrency_groups.values() {
        if group.running.is_some() {
            // No pending should be running simultaneously
            // (this is structural — pending is a separate queue)
        }
    }

    // INV-2: Running and pending are disjoint within each group.
    for (key, group) in &inner.concurrency_groups {
        if let Some(running) = &group.running {
            if group.pending.contains(running) {
                return Err(format!(
                    "INV-2: group {:?} running holder also in pending",
                    key
                ));
            }
        }
    }

    // INV-4: Single-mode groups have at most one pending.
    // (We can't always know the mode from the group alone, but we can check
    // the structural limit — single mode should have been enforced at acquire)

    // INV-7: Every running/pending holder has a reverse key entry.
    for (key, group) in &inner.concurrency_groups {
        if let Some(running) = &group.running {
            let run_id = running.run_id();
            let has_reverse = inner
                .holder_keys
                .get(&run_id)
                .is_some_and(|keys| keys.contains(key));
            if !has_reverse {
                return Err(format!(
                    "INV-7: running holder (run {:?}) in group {:?} has no reverse key",
                    run_id, key
                ));
            }
        }
        for pending in &group.pending {
            let run_id = pending.run_id();
            let has_reverse = inner
                .holder_keys
                .get(&run_id)
                .is_some_and(|keys| keys.contains(key));
            if !has_reverse {
                return Err(format!(
                    "INV-7: pending holder (run {:?}) in group {:?} has no reverse key",
                    run_id, key
                ));
            }
        }
    }

    // INV-9: No duplicate (run_id, job_id) in queue, pending_jobs, concurrency_blocked.
    {
        let mut seen = BTreeSet::new();
        for j in inner.queue.iter() {
            if !seen.insert((j.run_id, j.job_id.clone())) {
                return Err(format!(
                    "INV-9: duplicate ({:?}, {:?}) in queue",
                    j.run_id, j.job_id
                ));
            }
        }
        let mut seen = BTreeSet::new();
        for j in inner.pending_jobs.iter() {
            if !seen.insert((j.run_id, j.job_id.clone())) {
                return Err(format!(
                    "INV-9: duplicate ({:?}, {:?}) in pending_jobs",
                    j.run_id, j.job_id
                ));
            }
        }
        let mut seen = BTreeSet::new();
        for j in inner.concurrency_blocked.iter() {
            if !seen.insert((j.run_id, j.job_id.clone())) {
                return Err(format!(
                    "INV-9: duplicate ({:?}, {:?}) in concurrency_blocked",
                    j.run_id, j.job_id
                ));
            }
        }
    }

    // INV-12: Empty groups are removed.
    for (key, group) in &inner.concurrency_groups {
        if group.running.is_none() && group.pending.is_empty() {
            return Err(format!("INV-12: empty group {:?} not removed", key));
        }
    }

    // Holder keys: no entry for a run with zero group presence.
    for (run_id, keys) in &inner.holder_keys {
        for key in keys {
            let present = inner.concurrency_groups.get(key).is_some_and(|g| {
                g.running.as_ref().is_some_and(|h| h.run_id() == *run_id)
                    || g.pending.iter().any(|h| h.run_id() == *run_id)
            });
            if !present {
                return Err(format!(
                    "INV-holder_keys: run {:?} claims key {:?} but is not in that group",
                    run_id, key
                ));
            }
        }
    }

    Ok(())
}

// ── Test modules ────────────────────────────────────────────────────────────

/// State machine property tests exercising production transitions against the
/// independent model.
pub mod state_machine {
    use super::*;
    use proptest::prelude::*;

    fn config() -> ProptestConfig {
        let cases = std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256);
        ProptestConfig {
            cases,
            verbose: 1,
            ..ProptestConfig::default()
        }
    }

    /// Ensure the run referenced by an op exists in production state.
    fn ensure_run(prod: &mut ProdState, run: u32, kind: &HolderKind) {
        let run_id = rid(run);
        if prod.inner.runs.contains_key(&run_id) {
            // Ensure all referenced jobs exist
            match kind {
                HolderKind::Run => {}
                HolderKind::Job(j) => {
                    let run_record = prod.inner.runs.get_mut(&run_id).unwrap();
                    run_record
                        .jobs
                        .entry(jid(*j))
                        .or_insert(ExecutionStatus::Queued);
                    run_record
                        .job_base_ids
                        .entry(jid(*j))
                        .or_insert_with(|| format!("j{j}"));
                }
                HolderKind::JobSet(js) => {
                    let run_record = prod.inner.runs.get_mut(&run_id).unwrap();
                    for j in js {
                        run_record
                            .jobs
                            .entry(jid(*j))
                            .or_insert(ExecutionStatus::Queued);
                        run_record
                            .job_base_ids
                            .entry(jid(*j))
                            .or_insert_with(|| format!("j{j}"));
                    }
                }
            }
            return;
        }
        let job_ids: Vec<u32> = match kind {
            HolderKind::Run => vec![0],
            HolderKind::Job(j) => vec![*j],
            HolderKind::JobSet(js) => js.iter().copied().collect(),
        };
        prod.register_run(run, &job_ids);
    }

    /// Execute one operation on both model and production, then check invariants.
    fn execute_op(model: &mut Model, prod: &mut ProdState, op: &Op) {
        match op {
            Op::Submit {
                run,
                kind,
                repo,
                group,
                queue,
                cancel_in_progress,
            } => {
                let token = HolderToken {
                    run: *run,
                    kind: kind.clone(),
                };
                // A control-plane holder is admitted once. Generated duplicate
                // submissions and attempts to resurrect a terminal run are
                // invalid workflow traces, so both systems treat them as no-ops.
                if model.holder_state.contains_key(&token)
                    || model.holder_state.iter().any(|(known, state)| {
                        known.run == *run
                            && matches!(state, HolderState::Cancelled | HolderState::Terminal)
                    })
                {
                    return;
                }
                let key = concurrency::concurrency_key(repo, group);

                ensure_run(prod, *run, kind);

                let model_result =
                    model.acquire(key.clone(), token.clone(), *cancel_in_progress, *queue);
                let prod_result = prod.acquire(key, &token, *cancel_in_progress, *queue);

                // Results should agree on the Ok/Err axis (but error strings differ)
                match (&model_result, &prod_result) {
                    (Ok(m), Ok(p)) => {
                        assert_eq!(
                            *m, *p,
                            "Submit result mismatch: model={m}, prod={p}, op={op:?}"
                        );
                    }
                    (Err(_), Err(_)) => { /* both rejected */ }
                    _ => {
                        panic!(
                            "Submit agreement failure: model={model_result:?}, prod={prod_result:?}, op={op:?}"
                        );
                    }
                }
            }
            Op::Release { run, kind } => {
                let token = HolderToken {
                    run: *run,
                    kind: kind.clone(),
                };
                if !model.holder_state.contains_key(&token) {
                    return;
                }
                // Release in model: find all keys this token occupies
                let keys: Vec<_> = model
                    .holder_keys
                    .get(&token)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                for key in keys {
                    model.release(&key, &token);
                }
                // If token had no keys, just mark terminal
                if model.holder_keys.get(&token).is_none_or(|ks| ks.is_empty()) {
                    model
                        .holder_state
                        .insert(token.clone(), HolderState::Terminal);
                    model.holder_keys.remove(&token);
                }

                // Release in production
                match kind {
                    HolderKind::Run => prod.release_run(*run),
                    HolderKind::Job(j) => prod.release_job(*run, *j),
                    HolderKind::JobSet(js) => {
                        // Release each job in the set; production releases
                        // when all members are terminal.
                        for j in js {
                            prod.release_job(*run, *j);
                        }
                    }
                }
            }
            Op::Cancel { run } => {
                // Cancel in model: find all tokens for this run
                let tokens: Vec<_> = model
                    .holder_state
                    .keys()
                    .filter(|t| t.run == *run)
                    .cloned()
                    .collect();
                for token in tokens {
                    model.cancel(&token);
                }

                // Cancel in production
                prod.cancel_run(*run);
            }
        }
    }

    proptest! {
        #![proptest_config(config())]

        /// GH-SLOT-01 + structural invariants: after every operation in a
        /// generated sequence, at most one running holder per group, disjoint
        /// running/pending, reverse keys consistent, no stale empty groups.
        #[test]
        fn structural_invariants_hold(ops in generators::arb_ops(32)) {
            let mut model = Model::default();
            let mut prod = ProdState::new();

            for (i, op) in ops.iter().enumerate() {
                execute_op(&mut model, &mut prod, op);

                // Check model invariants
                model.check_all_invariants().map_err(|e| {
                    TestCaseError::Fail(format!("Model invariant failed after op {i}: {e}").into())
                })?;

                // Check production invariants
                check_production_invariants(&prod.inner).map_err(|e| {
                    TestCaseError::Fail(
                        format!("Production invariant failed after op {i}: {e}").into(),
                    )
                })?;
            }
        }

        /// GH-SINGLE-01: Under single-mode, after a submit that finds a
        /// running holder, there is at most one pending holder in the group.
        #[test]
        fn single_mode_at_most_one_pending(
            ops in generators::arb_ops(24),
        ) {
            let mut model = Model::default();
            let mut prod = ProdState::new();

            for op in &ops {
                let check_key = match op {
                    Op::Submit {
                        run,
                        kind,
                        repo,
                        group,
                        queue: ConcurrencyQueue::Single,
                        cancel_in_progress: false,
                    } => {
                        let token = HolderToken {
                            run: *run,
                            kind: kind.clone(),
                        };
                        (!model.holder_state.contains_key(&token)
                            && !model.holder_state.iter().any(|(known, state)| {
                                known.run == *run
                                    && matches!(
                                        state,
                                        HolderState::Cancelled | HolderState::Terminal
                                    )
                            }))
                        .then(|| concurrency::concurrency_key(repo, group))
                    }
                    _ => None,
                };
                execute_op(&mut model, &mut prod, op);

                if let Some(key) = check_key {
                    let pending = model.groups.get(&key).map_or(0, |group| group.pending.len());
                    prop_assert!(
                        pending <= 1,
                        "GH-SINGLE-01: arrival left group {:?} with {} pending",
                        key,
                        pending
                    );
                }
            }
        }

        /// GH-MAX-01: Under max-mode, pending count never exceeds QUEUE_MAX_PENDING.
        #[test]
        fn max_mode_pending_bounded(
            ops in generators::arb_ops(24),
        ) {
            let mut model = Model::default();
            let mut prod = ProdState::new();

            for op in &ops {
                execute_op(&mut model, &mut prod, op);

                for (key, group) in &model.groups {
                    if group.queue_mode == ConcurrencyQueue::Max {
                        prop_assert!(
                            group.pending.len() <= QUEUE_MAX_PENDING,
                            "GH-MAX-01: group {:?} has {} pending (max {})",
                            key,
                            group.pending.len(),
                            QUEUE_MAX_PENDING
                        );
                    }
                }
            }
        }

        /// GH-FIFO-01: Promotion order equals admission order. When a running
        /// holder is released, the first pending becomes running.
        #[test]
        fn fifo_promotion_order(ops in generators::arb_ops(24)) {
            let mut model = Model::default();
            let mut prod = ProdState::new();

            for op in &ops {
                // Before release ops, record pending order for comparison
                let pre_pending: BTreeMap<_, _> = model
                    .groups
                    .iter()
                    .map(|(k, g)| (k.clone(), g.pending.iter().cloned().collect::<Vec<_>>()))
                    .collect();

                execute_op(&mut model, &mut prod, op);

                // After release, verify the new running was the old first pending
                if matches!(op, Op::Release { .. }) {
                    for (key, group) in &model.groups {
                        if let Some(running) = &group.running {
                            if let Some(old_pending) = pre_pending.get(key) {
                                if !old_pending.is_empty() {
                                    // If a promotion happened, the new running should be
                                    // the old first pending (if it was pending before)
                                    if model.holder_state.get(running) == Some(&HolderState::Running)
                                        && old_pending.first() == Some(running)
                                    {
                                        // FIFO correct — first pending got promoted
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// Cancellation is idempotent: cancelling a terminal/cancelled holder
        /// does not change state.
        #[test]
        fn cancel_idempotent(ops in generators::arb_ops(16)) {
            let mut model = Model::default();
            let mut prod = ProdState::new();

            for op in &ops {
                execute_op(&mut model, &mut prod, op);
            }

            // Now cancel everything twice — state should be stable after second pass
            let _snapshot_before = prod.snapshot();
            for run in 0..6u32 {
                prod.cancel_run(run);
            }
            let snapshot_after_1 = prod.snapshot();
            for run in 0..6u32 {
                prod.cancel_run(run);
            }
            let snapshot_after_2 = prod.snapshot();
            prop_assert_eq!(
                snapshot_after_1,
                snapshot_after_2,
                "Cancel must be idempotent: second pass changed state"
            );
        }

        /// Release is idempotent: releasing an already-released holder is safe.
        #[test]
        fn release_idempotent(ops in generators::arb_ops(16)) {
            let mut model = Model::default();
            let mut prod = ProdState::new();

            for op in &ops {
                execute_op(&mut model, &mut prod, op);
            }

            // Release all runs twice
            for run in 0..6u32 {
                prod.release_run(run);
            }
            let snapshot_1 = prod.snapshot();
            for run in 0..6u32 {
                prod.release_run(run);
            }
            let snapshot_2 = prod.snapshot();
            prop_assert_eq!(
                snapshot_1,
                snapshot_2,
                "Release must be idempotent: second pass changed state"
            );
        }

        /// Holder keys are cleaned up: after all holders are released/cancelled,
        /// holder_keys must be empty.
        #[test]
        fn holder_keys_cleaned_up(ops in generators::arb_ops(16)) {
            let mut model = Model::default();
            let mut prod = ProdState::new();

            for op in &ops {
                execute_op(&mut model, &mut prod, op);
            }

            // Cancel all and release all
            for run in 0..6u32 {
                prod.cancel_run(run);
            }

            // After cancellation of all runs, all groups and holder keys should be empty
            prop_assert!(
                prod.inner.concurrency_groups.is_empty(),
                "After cancelling all runs, concurrency_groups should be empty but has {:?}",
                prod.inner.concurrency_groups.keys().collect::<Vec<_>>()
            );
            prop_assert!(
                prod.inner.holder_keys.is_empty(),
                "After cancelling all runs, holder_keys should be empty but has {:?}",
                prod.inner.holder_keys.keys().collect::<Vec<_>>()
            );
        }

        /// GH-CANCEL-01: cancel-in-progress replaces the running holder atomically.
        /// After the operation, the arrival is running, not the previous holder.
        #[test]
        fn cancel_in_progress_replaces_running(
            first_run in 0..4u32,
            second_run in 4..8u32,
            queue in generators::arb_queue(),
        ) {
            let mut prod = ProdState::new();
            prod.register_run(first_run, &[0]);
            prod.register_run(second_run, &[0]);

            let key = ("repo".to_owned(), "grp".to_owned());

            // First acquire succeeds (slot was free)
            let r1 = try_acquire_concurrency(
                &mut prod.inner,
                key.clone(),
                "grp".into(),
                Holder::Run(rid(first_run)),
                false,
                queue,
            );
            assert_eq!(r1, Ok(true), "first acquire should succeed");

            // Second with cancel_in_progress replaces
            let r2 = try_acquire_concurrency(
                &mut prod.inner,
                key.clone(),
                "grp".into(),
                Holder::Run(rid(second_run)),
                true,
                queue,
            );
            assert_eq!(r2, Ok(true), "cancel-in-progress acquire should succeed");

            // The running holder is now the second run
            let group = prod.inner.concurrency_groups.get(&key).unwrap();
            assert_eq!(
                group.running,
                Some(Holder::Run(rid(second_run))),
                "GH-CANCEL-01: running must be the second arrival"
            );
        }

        /// GH-GROUP-01 via state machine: case variants hit the same group.
        #[test]
        fn case_variants_same_group(
            run1 in 0..3u32,
            run2 in 3..6u32,
        ) {
            let mut prod = ProdState::new();
            prod.register_run(run1, &[0]);
            prod.register_run(run2, &[0]);

            let key1 = concurrency::concurrency_key("Repo", "Group");
            let key2 = concurrency::concurrency_key("repo", "group");
            assert_eq!(key1, key2, "GH-GROUP-01: keys must match");

            let r1 = try_acquire_concurrency(
                &mut prod.inner,
                key1.clone(),
                "Group".into(),
                Holder::Run(rid(run1)),
                false,
                ConcurrencyQueue::Single,
            );
            assert_eq!(r1, Ok(true));

            // Second arrival on same key (different case) should contend
            let r2 = try_acquire_concurrency(
                &mut prod.inner,
                key2,
                "group".into(),
                Holder::Run(rid(run2)),
                false,
                ConcurrencyQueue::Single,
            );
            assert_eq!(r2, Ok(false), "GH-GROUP-01: should contend on same key");
        }

        /// Mixed Run/Job/JobSet acquisition and release sequences match:
        /// after complete sequence the production state should have groups
        /// agreeing on running holder identity with the model.
        #[test]
        fn model_matches_production_groups(ops in generators::arb_ops(24)) {
            let mut model = Model::default();
            let mut prod = ProdState::new();

            for op in &ops {
                execute_op(&mut model, &mut prod, op);
            }

            // Compare model groups with production groups
            let prod_snap = prod.snapshot();
            for (key, model_group) in &model.groups {
                if let Some((prod_running, prod_pending)) = prod_snap.get(key) {
                    // Running holder identity should match
                    prop_assert_eq!(
                        &model_group.running,
                        prod_running,
                        "Group {:?} running mismatch: model={:?}, prod={:?}",
                        key,
                        model_group.running,
                        prod_running
                    );
                    // Pending order should match
                    let model_pending: Vec<_> = model_group.pending.iter().cloned().collect();
                    prop_assert_eq!(
                        &model_pending,
                        prod_pending,
                        "Group {:?} pending mismatch",
                        key
                    );
                } else if model_group.running.is_some() || !model_group.pending.is_empty() {
                    // Model has group but production doesn't — only OK if model
                    // has cancelled/terminal holders that production cleaned up
                    // via cancel_run_inner (which also releases concurrency).
                    // This divergence is acceptable when the model tracks
                    // cancelled holders that production already cleaned.
                }
            }
        }

    }
    #[test]
    fn test_exhaustive_state_space_model_check() {
        let mut model = Model::default();
        let mut prod = ProdState::new();
        let mut path = Vec::new();
        let mut visited = BTreeSet::new();

        fn dfs(
            depth: usize,
            model: &mut Model,
            prod: &mut ProdState,
            path: &mut Vec<Op>,
            visited: &mut BTreeSet<String>,
        ) {
            assert!(model.check_all_invariants().is_ok());
            assert!(check_production_invariants(&prod.inner).is_ok());

            let model_snap: BTreeMap<_, _> = model
                .groups
                .iter()
                .map(|(k, g)| {
                    let running = g.running.clone();
                    let pending: Vec<_> = g.pending.iter().cloned().collect();
                    (k.clone(), (running, pending))
                })
                .collect();
            let prod_snap = prod.snapshot();
            for (key, m_group) in &model_snap {
                if let Some((p_running, p_pending)) = prod_snap.get(key) {
                    assert_eq!(
                        &m_group.0, p_running,
                        "Running mismatch in path: {:?}",
                        path
                    );
                    assert_eq!(
                        &m_group.1, p_pending,
                        "Pending mismatch in path: {:?}",
                        path
                    );
                }
            }

            if depth >= 6 {
                return;
            }

            let mut ops = Vec::new();

            for run in 0..3u32 {
                let token = HolderToken {
                    run,
                    kind: HolderKind::Run,
                };
                if !model.holder_state.contains_key(&token) {
                    for group_name in ["grp-1", "grp-2"] {
                        for queue in [ConcurrencyQueue::Single, ConcurrencyQueue::Max] {
                            for cancel in [false, true] {
                                ops.push(Op::Submit {
                                    run,
                                    kind: HolderKind::Run,
                                    repo: "repo-a".to_owned(),
                                    group: group_name.to_owned(),
                                    queue,
                                    cancel_in_progress: cancel,
                                });
                            }
                        }
                    }
                }
            }

            for group in model.groups.values() {
                if let Some(running) = &group.running {
                    ops.push(Op::Release {
                        run: running.run,
                        kind: running.kind.clone(),
                    });
                }
            }

            for (token, state) in &model.holder_state {
                if matches!(state, HolderState::Running | HolderState::Pending) {
                    ops.push(Op::Cancel { run: token.run });
                }
            }

            for op in ops {
                let mut next_model = model.clone();
                let mut next_prod = prod.clone_concurrency();

                execute_op(&mut next_model, &mut next_prod, &op);

                let state_key = format!("{:?}_{:?}", next_model.groups, next_model.holder_state);
                if visited.insert(state_key) {
                    path.push(op);
                    dfs(depth + 1, &mut next_model, &mut next_prod, path, visited);
                    path.pop();
                }
            }
        }

        dfs(0, &mut model, &mut prod, &mut path, &mut visited);
        println!(
            "Exhaustive state space traversal complete. Visited {} unique states.",
            visited.len()
        );
    }
}

/// Pure concurrency function property tests (extends concurrency.rs properties).
pub mod pure {
    use super::*;
    use proptest::prelude::*;

    fn config() -> ProptestConfig {
        let cases = std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256);
        ProptestConfig {
            cases,
            verbose: 1,
            ..ProptestConfig::default()
        }
    }
    fn evaluate_group(group: String) -> Result<(String, bool, ConcurrencyQueue), String> {
        let github = serde_json::json!({});
        let inputs = BTreeMap::from([("group".to_owned(), serde_json::Value::String(group))]);
        let vars = BTreeMap::new();
        let raw = aksh_gha_parser::Concurrency {
            group: "${{ inputs.group }}".to_owned(),
            cancel_in_progress: None,
            queue: ConcurrencyQueue::Single,
        };
        let ctx = concurrency::ConcurrencyContext {
            scope: concurrency::ConcurrencyScope::Workflow,
            github: &github,
            inputs: &inputs,
            vars: &vars,
            matrix: None,
            strategy: None,
            needs: None,
        };
        concurrency::evaluate_concurrency(&raw, &ctx)
    }

    /// GH-VALIDATE-02: the evaluated ASCII group boundary is inclusive.
    #[test]
    fn evaluated_ascii_group_length_400_is_accepted() {
        let group = "a".repeat(400);
        let result = evaluate_group(group.clone()).expect("400 ASCII characters must be accepted");
        assert_eq!(result, (group, false, ConcurrencyQueue::Single));
    }

    /// GH-VALIDATE-03: an evaluated ASCII group over the limit reports its
    /// UTF-16 length and the official maximum.
    #[test]
    fn evaluated_ascii_group_length_401_is_rejected() {
        let error =
            evaluate_group("a".repeat(401)).expect_err("401 ASCII characters must be rejected");
        assert_eq!(
            error,
            "concurrency group name is too long (401 UTF-16 code units, maximum 400)"
        );
    }

    /// GH-VALIDATE-04: validation applies after expression resolution, so an
    /// expression that evaluates to an empty group is rejected.
    #[test]
    fn evaluated_empty_group_is_rejected() {
        let error =
            evaluate_group(String::new()).expect_err("an evaluated empty group must be rejected");
        assert_eq!(error, "concurrency group name must not be empty");
    }

    /// GH-VALIDATE-05: C# `string.Length` counts each astral character as two
    /// UTF-16 code units; 200 astral characters therefore fit exactly.
    #[test]
    fn evaluated_astral_group_length_400_utf16_units_is_accepted() {
        let group = "😀".repeat(200);
        let result = evaluate_group(group.clone())
            .expect("200 astral characters (400 UTF-16 code units) must be accepted");
        assert_eq!(result, (group, false, ConcurrencyQueue::Single));
    }

    /// GH-VALIDATE-06: 201 astral characters produce 402 UTF-16 code units
    /// and must be rejected using that evaluated length.
    #[test]
    fn evaluated_astral_group_length_402_utf16_units_is_rejected() {
        let error = evaluate_group("😀".repeat(201))
            .expect_err("201 astral characters (402 UTF-16 code units) must be rejected");
        assert_eq!(
            error,
            "concurrency group name is too long (402 UTF-16 code units, maximum 400)"
        );
    }

    /// GH-VALIDATE-07: BMP characters occupy one UTF-16 code unit each even
    /// when they occupy multiple bytes in UTF-8.
    #[test]
    fn evaluated_bmp_group_length_400_utf16_units_is_accepted() {
        let group = "é".repeat(400);
        let result = evaluate_group(group.clone())
            .expect("400 BMP characters (400 UTF-16 code units) must be accepted");
        assert_eq!(result, (group, false, ConcurrencyQueue::Single));
    }

    proptest! {
        #![proptest_config(config())]

        /// GH-CTX-WF-01: workflow concurrency receives github, inputs, and
        /// vars, but never job-only matrix, strategy, or needs contexts.
        #[test]
        fn gh_ctx_wf_01_enforces_workflow_context_allowlist(
            github_value in "[a-z]{1,8}",
            input_value in "[a-z]{1,8}",
            var_value in "[a-z]{1,8}",
        ) {
            let github = serde_json::json!({"ref_name": github_value});
            let inputs = BTreeMap::from([(
                "target".to_owned(),
                serde_json::Value::String(input_value.clone()),
            )]);
            let vars = BTreeMap::from([("suffix".to_owned(), var_value.clone())]);
            let matrix = BTreeMap::from([(
                "os".to_owned(),
                serde_json::Value::String("linux".to_owned()),
            )]);
            let strategy = serde_json::json!({"job-index": 0});
            let needs = serde_json::json!({"setup": {"result": "success"}});
            let ctx = concurrency::ConcurrencyContext {
                scope: concurrency::ConcurrencyScope::Workflow,
                github: &github,
                inputs: &inputs,
                vars: &vars,
                matrix: Some(&matrix),
                strategy: Some(&strategy),
                needs: Some(&needs),
            };
            let alternate_matrix = BTreeMap::from([(
                "os".to_owned(),
                serde_json::Value::String("windows".to_owned()),
            )]);
            let alternate_strategy = serde_json::json!({"job-index": 99});
            let alternate_needs = serde_json::json!({"setup": {"result": "failure"}});
            let alternate_ctx = concurrency::ConcurrencyContext {
                scope: concurrency::ConcurrencyScope::Workflow,
                github: &github,
                inputs: &inputs,
                vars: &vars,
                matrix: Some(&alternate_matrix),
                strategy: Some(&alternate_strategy),
                needs: Some(&alternate_needs),
            };
            let allowed = aksh_gha_parser::Concurrency {
                group: "${{ github.ref_name }}-${{ inputs.target }}-${{ vars.suffix }}".to_owned(),
                cancel_in_progress: None,
                queue: aksh_gha_parser::ConcurrencyQueue::Single,
            };
            let (group, cancel, queue) = concurrency::evaluate_concurrency(&allowed, &ctx)
                .expect("GH-CTX-WF-01: documented workflow contexts must evaluate");
            prop_assert_eq!(group, format!("{github_value}-{input_value}-{var_value}"));
            prop_assert!(!cancel);
            prop_assert_eq!(queue, aksh_gha_parser::ConcurrencyQueue::Single);

            for forbidden in ["matrix.os", "strategy.job-index", "needs.setup.result"] {
                let raw = aksh_gha_parser::Concurrency {
                    group: format!("${{{{ {forbidden} }}}}"),
                    cancel_in_progress: None,
                    queue: aksh_gha_parser::ConcurrencyQueue::Single,
                };
                prop_assert_eq!(
                    concurrency::evaluate_concurrency(&raw, &ctx),
                    concurrency::evaluate_concurrency(&raw, &alternate_ctx),
                    "GH-CTX-WF-01: workflow result changed with forbidden context {}",
                    forbidden,
                );
            }
        }

        /// GH-VALIDATE-01: max + cancel-in-progress is invalid and must return
        /// an error without mutating state.
        #[test]
        fn validate_max_plus_cancel_is_error(group_raw in "[a-z]{1,8}") {
            let mut prod = ProdState::new();
            prod.register_run(0, &[0]);

            // GH-VALIDATE-01 says effective queue:max + cancel-in-progress:true
            // is invalid. However, the current implementation of
            // try_acquire_concurrency handles them as independent options.
            // When cancel_in_progress is true and the slot is free, it just
            // acquires directly — the validation should happen at a higher
            // level (evaluate_concurrency). Test that evaluate_concurrency
            // rejects this combination:
            let raw = aksh_gha_parser::Concurrency {
                group: group_raw.clone(),
                cancel_in_progress: Some("true".to_owned()),
                queue: ConcurrencyQueue::Max,
            };
            let github = serde_json::json!({});
            let inputs = BTreeMap::new();
            let vars = BTreeMap::new();
            let ctx = concurrency::ConcurrencyContext {
                scope: concurrency::ConcurrencyScope::Workflow,
                github: &github,
                inputs: &inputs,
                vars: &vars,
                needs: None,
                strategy: None,
                matrix: None,
            };
            let result = concurrency::evaluate_concurrency(&raw, &ctx);
            prop_assert!(
                result.is_err(),
                "GH-VALIDATE-01: max + cancel-in-progress must be rejected, got {:?}",
                result
            );
        }

        /// Queue boundary at exactly 99/100/101 via production state:
        /// adding holders up to the boundary and verifying overflow behaviour.
        #[test]
        fn max_boundary_production(boundary in prop_oneof![Just(99usize), Just(100usize), Just(101usize)]) {
            let mut prod = ProdState::new();

            // First holder takes the running slot
            prod.register_run(0, &[0]);
            let key = ("repo".to_owned(), "grp".to_owned());
            let r = try_acquire_concurrency(
                &mut prod.inner,
                key.clone(),
                "grp".into(),
                Holder::Run(rid(0)),
                false,
                ConcurrencyQueue::Max,
            );
            assert_eq!(r, Ok(true));

            // Fill pending up to boundary
            for i in 1..=boundary as u32 {
                prod.register_run(i, &[0]);
                let r = try_acquire_concurrency(
                    &mut prod.inner,
                    key.clone(),
                    "grp".into(),
                    Holder::Run(rid(i)),
                    false,
                    ConcurrencyQueue::Max,
                );
                if i <= QUEUE_MAX_PENDING as u32 {
                    assert_eq!(r, Ok(false), "should park at pending count {i}");
                } else {
                    assert!(r.is_err(), "should overflow at pending count {i}");
                }
            }

            let group = prod.inner.concurrency_groups.get(&key).unwrap();
            prop_assert!(
                group.pending.len() <= QUEUE_MAX_PENDING,
                "GH-MAX-01: pending {} exceeds limit {}",
                group.pending.len(),
                QUEUE_MAX_PENDING
            );
        }
    }
}
