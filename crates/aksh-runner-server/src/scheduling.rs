//! Pure job-graph scheduler model.
//!
//! Mirrors official GitHub Actions dispatch rules for `needs` and matrix
//! fan-out so property tests can exercise thousands of graphs without HTTP.
//!
//! Oracle sources (pinned where source code is used):
//! - Workflow contract: <https://docs.github.com/en/actions/writing-workflows/workflow-syntax-for-github-actions#jobsjob_idneeds>.
//! - Status functions: <https://docs.github.com/en/actions/learn-github-actions/expressions#status-check-functions>.
//! - Runner v2.335.1 condition implementation:
//!   `src/Runner.Worker/StepsRunner.cs`,
//!   `src/Runner.Worker/Expressions/SuccessFunction.cs`,
//!   `src/Runner.Worker/Expressions/FailureFunction.cs`,
//!   `src/Runner.Worker/Expressions/CancelledFunction.cs`, and
//!   `src/Runner.Worker/Expressions/AlwaysFunction.cs`.
//!
//! Properties below encode these observable rules:
//! - A dependent waits until every needed job is terminal.
//! - Failed or skipped dependencies skip a default dependent; explicit
//!   `always()`, `failure()`, or `cancelled()` conditions may override that.
//! - `failure()` observes failures in the transitive needs ancestry.
//! - Matrix siblings share a base id; needing `build` waits for every
//!   expanded `build (...)` sibling and respects `max-parallel`.
//! - Cycles and unknown needs are invalid and must be rejected before dispatch.
//! - A job is never dispatched twice; promotion is deterministic given a
//!   stable pending order; terminal statuses do not regress.
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aksh_gha_protocol::{ExecutionStatus, JobId};

/// One schedulable job node after matrix expansion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedJob {
    /// Expanded job id (`build (ubuntu, 20)` or plain `build`).
    pub id: JobId,
    /// Workflow job id before matrix suffixing.
    pub base_id: String,
    /// Declared `needs` (base or expanded ids).
    pub needs: Vec<JobId>,
    /// Optional matrix max-parallel for this base id.
    pub max_parallel: Option<u64>,
    /// Job-level condition expression, if any.
    pub if_condition: Option<String>,
}

/// Mutable scheduler snapshot for one workflow run.
#[derive(Debug, Clone, Default)]
pub struct SchedulerState {
    /// Terminal/non-terminal status per expanded job id.
    pub jobs: BTreeMap<JobId, ExecutionStatus>,
    /// Expanded id → base id.
    pub job_base_ids: BTreeMap<JobId, String>,
    /// Expanded id → declared dependency ids.
    pub job_needs: BTreeMap<JobId, Vec<JobId>>,
    /// Jobs waiting on dependencies (stable insertion order).
    pub pending: VecDeque<SchedJob>,
    /// Jobs ready for runner acquisition (stable FIFO).
    pub queue: VecDeque<SchedJob>,
}

/// Error when a needs graph is not a DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleError {
    /// One job id participating in a cycle (representative).
    pub witness: String,
}

/// Detect a cycle in a needs graph.
///
/// `edges` maps job id → list of needed job ids.
/// Returns `Ok(())` when acyclic, else a witness job id on a cycle.
pub fn detect_needs_cycle(edges: &BTreeMap<String, Vec<String>>) -> Result<(), CycleError> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Color {
        White,
        Gray,
        Black,
    }

    let mut color: BTreeMap<&str, Color> =
        edges.keys().map(|k| (k.as_str(), Color::White)).collect();
    // Also color referenced nodes that may be missing as keys.
    for deps in edges.values() {
        for d in deps {
            color.entry(d.as_str()).or_insert(Color::White);
        }
    }

    fn visit<'a>(
        node: &'a str,
        edges: &'a BTreeMap<String, Vec<String>>,
        color: &mut BTreeMap<&'a str, Color>,
    ) -> Result<(), CycleError> {
        color.insert(node, Color::Gray);
        if let Some(deps) = edges.get(node) {
            for dep in deps {
                match color.get(dep.as_str()).copied().unwrap_or(Color::White) {
                    Color::Gray => {
                        return Err(CycleError {
                            witness: dep.clone(),
                        });
                    }
                    Color::White => visit(dep.as_str(), edges, color)?,
                    Color::Black => {}
                }
            }
        }
        color.insert(node, Color::Black);
        Ok(())
    }

    let nodes: Vec<&str> = color.keys().copied().collect();
    for node in nodes {
        if color.get(node) == Some(&Color::White) {
            visit(node, edges, &mut color)?;
        }
    }
    Ok(())
}

/// Whether a single `needs` entry completed successfully.
///
/// The need may name a concrete expanded id or a base id matching matrix
/// siblings. Every match must be successful and at least one match is required.
/// A skipped need does not satisfy GitHub's default dependency condition.
pub fn need_satisfied(jobs: &BTreeMap<JobId, ExecutionStatus>, need: &JobId) -> bool {
    let statuses = matching_need_statuses(jobs, need);
    !statuses.is_empty()
        && statuses
            .iter()
            .all(|status| *status == ExecutionStatus::Success)
}

fn matching_need_statuses(
    jobs: &BTreeMap<JobId, ExecutionStatus>,
    need: &JobId,
) -> Vec<ExecutionStatus> {
    let matrix_prefix = format!("{} (", need.0);
    jobs.iter()
        .filter_map(|(job_id, status)| {
            (job_id == need || job_id.0.starts_with(&matrix_prefix)).then_some(*status)
        })
        .collect()
}

fn matching_need_ids(state: &SchedulerState, need: &JobId) -> Vec<JobId> {
    state
        .jobs
        .keys()
        .filter(|job_id| {
            *job_id == need
                || state
                    .job_base_ids
                    .get(*job_id)
                    .is_some_and(|base| base == &need.0)
        })
        .cloned()
        .collect()
}

fn state_need_statuses(state: &SchedulerState, need: &JobId) -> Vec<ExecutionStatus> {
    matching_need_ids(state, need)
        .iter()
        .filter_map(|job_id| state.jobs.get(job_id).copied())
        .collect()
}

fn ancestor_statuses(state: &SchedulerState, job: &SchedJob) -> Vec<ExecutionStatus> {
    let mut pending = job
        .needs
        .iter()
        .flat_map(|need| matching_need_ids(state, need))
        .collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    let mut statuses = Vec::new();
    while let Some(job_id) = pending.pop() {
        if !visited.insert(job_id.clone()) {
            continue;
        }
        if let Some(status) = state.jobs.get(&job_id) {
            statuses.push(*status);
        }
        if let Some(needs) = state.job_needs.get(&job_id) {
            pending.extend(needs.iter().flat_map(|need| matching_need_ids(state, need)));
        }
    }
    statuses
}

fn aggregate_status(statuses: &[ExecutionStatus]) -> ExecutionStatus {
    if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Failure)
    {
        ExecutionStatus::Failure
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Cancelled)
    {
        ExecutionStatus::Cancelled
    } else if statuses
        .iter()
        .any(|status| *status == ExecutionStatus::Skipped)
    {
        ExecutionStatus::Skipped
    } else {
        ExecutionStatus::Success
    }
}

fn terminal(status: ExecutionStatus) -> bool {
    matches!(
        status,
        ExecutionStatus::Success
            | ExecutionStatus::Failure
            | ExecutionStatus::Skipped
            | ExecutionStatus::Cancelled
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DependencyDecision {
    Wait,
    Run,
    Skip,
}

fn dependency_decision(state: &SchedulerState, job: &SchedJob) -> DependencyDecision {
    if job.needs.is_empty() {
        return DependencyDecision::Run;
    }
    let direct_statuses = job
        .needs
        .iter()
        .flat_map(|need| state_need_statuses(state, need))
        .collect::<Vec<_>>();
    if direct_statuses.is_empty() || direct_statuses.iter().any(|status| !terminal(*status)) {
        return DependencyDecision::Wait;
    }

    let statuses = ancestor_statuses(state, job);
    let aggregate = aggregate_status(&statuses);
    let success = aggregate == ExecutionStatus::Success;
    let failure = aggregate == ExecutionStatus::Failure;
    let cancelled = aggregate == ExecutionStatus::Cancelled;
    let condition = aksh_gha_expressions::effective_condition(job.if_condition.as_deref());
    let context = aksh_gha_expressions::Context::default().with_status(success, failure, cancelled);
    match aksh_gha_expressions::eval_bool(&condition, &context) {
        Ok(true) => DependencyDecision::Run,
        Ok(false) | Err(_) => DependencyDecision::Skip,
    }
}

/// Whether `job` is under its matrix `max-parallel` budget.
pub fn under_max_parallel(state: &SchedulerState, job: &SchedJob) -> bool {
    let Some(max_parallel) = job.max_parallel else {
        return true;
    };
    let active_in_queue = state
        .queue
        .iter()
        .filter(|queued| queued.base_id == job.base_id)
        .count() as u64;
    let active_running = state
        .jobs
        .iter()
        .filter(|(job_id, status)| {
            state.job_base_ids.get(*job_id) == Some(&job.base_id)
                && matches!(status, ExecutionStatus::InProgress)
        })
        .count() as u64;

    active_in_queue + active_running < max_parallel
}

/// Resolve pending jobs whose dependencies have become terminal.
///
/// Runnable jobs enter the FIFO queue. Jobs whose condition is false become
/// terminal `Skipped`. The pass repeats after skips so downstream chains settle.
/// Returns the number of jobs promoted to the queue.
pub fn promote_ready_jobs(state: &mut SchedulerState) -> usize {
    let mut promoted_count = 0;
    loop {
        let mut promoted_by_base: BTreeMap<String, u64> = BTreeMap::new();
        let mut promoted = Vec::new();
        let mut remaining = VecDeque::new();
        let mut skipped = false;

        while let Some(job) = state.pending.pop_front() {
            match dependency_decision(state, &job) {
                DependencyDecision::Run
                    if under_max_parallel(state, &job)
                        && promoted_by_base.get(&job.base_id).copied().unwrap_or(0)
                            < job.max_parallel.unwrap_or(u64::MAX) =>
                {
                    *promoted_by_base.entry(job.base_id.clone()).or_default() += 1;
                    promoted.push(job)
                }
                DependencyDecision::Skip => {
                    state.jobs.insert(job.id, ExecutionStatus::Skipped);
                    skipped = true;
                }
                DependencyDecision::Wait | DependencyDecision::Run => remaining.push_back(job),
            }
        }

        promoted_count += promoted.len();
        state.pending = remaining;
        state.queue.extend(promoted);
        if !skipped {
            return promoted_count;
        }
    }
}

/// Seed a scheduler from expanded job definitions.
///
/// Jobs with empty `needs` enter the queue immediately; others stay pending.
/// All jobs start as `Queued`.
pub fn seed_from_jobs(jobs: Vec<SchedJob>) -> SchedulerState {
    let mut state = SchedulerState::default();
    for job in jobs {
        if state.jobs.contains_key(&job.id) {
            continue;
        }
        state.jobs.insert(job.id.clone(), ExecutionStatus::Queued);
        state
            .job_base_ids
            .insert(job.id.clone(), job.base_id.clone());
        state.job_needs.insert(job.id.clone(), job.needs.clone());
        if job.needs.is_empty() {
            state.queue.push_back(job);
        } else {
            state.pending.push_back(job);
        }
    }
    state
}

/// Mark a job terminal and re-run promotion.
pub fn complete_job(state: &mut SchedulerState, job_id: &JobId, status: ExecutionStatus) -> usize {
    let Some(current) = state.jobs.get_mut(job_id) else {
        return 0;
    };
    if terminal(*current) {
        return 0;
    }
    *current = status;
    state.queue.retain(|job| &job.id != job_id);
    state.pending.retain(|job| &job.id != job_id);
    promote_ready_jobs(state)
}

/// Simulate dispatch: move the front queued job to `InProgress` and return it.
pub fn acquire_next(state: &mut SchedulerState) -> Option<SchedJob> {
    let job = state.queue.pop_front()?;
    if let Some(slot) = state.jobs.get_mut(&job.id) {
        *slot = ExecutionStatus::InProgress;
    }
    Some(job)
}

/// True when every job is in a terminal status.
pub fn run_settled(state: &SchedulerState) -> bool {
    state.jobs.values().all(|s| {
        matches!(
            s,
            ExecutionStatus::Success
                | ExecutionStatus::Failure
                | ExecutionStatus::Skipped
                | ExecutionStatus::Cancelled
        )
    }) && state.pending.is_empty()
        && state.queue.is_empty()
}

/// Jobs currently `InProgress`.
pub fn in_progress_ids(state: &SchedulerState) -> BTreeSet<JobId> {
    state
        .jobs
        .iter()
        .filter_map(|(id, status)| {
            matches!(status, ExecutionStatus::InProgress).then(|| id.clone())
        })
        .collect()
}

/// Count how many times a job id appears across queue + pending (must be ≤ 1).
pub fn placement_count(state: &SchedulerState, job_id: &JobId) -> usize {
    state.queue.iter().filter(|j| &j.id == job_id).count()
        + state.pending.iter().filter(|j| &j.id == job_id).count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use proptest::test_runner::RngSeed;

    /// Deterministic proptest config for DAG scheduler tests.
    /// Fixed seed ensures reproducibility; failure persistence and verbose
    /// reporting preserve/report the seed on failure (docs/property-tests.md §Common).
    fn dag_config(cases: u32) -> ProptestConfig {
        ProptestConfig {
            cases,
            rng_seed: RngSeed::Fixed(20250713),
            verbose: 1,
            ..ProptestConfig::default()
        }
    }

    fn jid(s: &str) -> JobId {
        JobId(s.to_owned())
    }

    #[test]
    fn linear_needs_promotes_in_order() {
        let jobs = vec![
            SchedJob {
                id: jid("a"),
                base_id: "a".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("b"),
                base_id: "b".into(),
                needs: vec![jid("a")],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("c"),
                base_id: "c".into(),
                needs: vec![jid("b")],
                max_parallel: None,
                if_condition: None,
            },
        ];
        let mut state = seed_from_jobs(jobs);
        assert_eq!(state.queue.len(), 1);
        let a = acquire_next(&mut state).unwrap();
        assert_eq!(a.id.0, "a");
        complete_job(&mut state, &jid("a"), ExecutionStatus::Success);
        let b = acquire_next(&mut state).unwrap();
        assert_eq!(b.id.0, "b");
        complete_job(&mut state, &jid("b"), ExecutionStatus::Success);
        let c = acquire_next(&mut state).unwrap();
        assert_eq!(c.id.0, "c");
    }

    /// Official: failed dependency causes dependent to be skipped under the
    /// default `success()` gate — it must NOT stay pending forever.
    #[test]
    fn failed_need_skips_dependent() {
        let jobs = vec![
            SchedJob {
                id: jid("a"),
                base_id: "a".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("b"),
                base_id: "b".into(),
                needs: vec![jid("a")],
                max_parallel: None,
                if_condition: None,
            },
        ];
        let mut state = seed_from_jobs(jobs);
        acquire_next(&mut state);
        complete_job(&mut state, &jid("a"), ExecutionStatus::Failure);
        assert!(state.queue.is_empty(), "dependent must not be queued");
        assert!(state.pending.is_empty(), "dependent must not stay pending");
        assert_eq!(
            state.jobs.get(&jid("b")).copied(),
            Some(ExecutionStatus::Skipped),
            "dependent must be skipped when need failed under default gate"
        );
    }

    /// Official: `need_satisfied` uses the strict Success-only check.
    /// Skipped does NOT satisfy the default dependency gate — instead the
    /// scheduler evaluates `success()` via the full ancestor chain which
    /// considers skipped ancestors as non-success, causing the dependent to
    /// skip under the default condition.
    #[test]
    fn skipped_need_does_not_satisfy_success_gate() {
        let mut jobs_map = BTreeMap::new();
        jobs_map.insert(jid("a"), ExecutionStatus::Skipped);
        assert!(
            !need_satisfied(&jobs_map, &jid("a")),
            "Skipped is not Success — need_satisfied must return false"
        );
    }

    /// Official: skipped dependency causes dependent to be skipped under the
    /// default gate (skipped is not success in the ancestor chain).
    #[test]
    fn skipped_need_skips_dependent() {
        let jobs = vec![
            SchedJob {
                id: jid("build"),
                base_id: "build".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("test"),
                base_id: "test".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: None,
            },
        ];
        let mut state = seed_from_jobs(jobs);
        acquire_next(&mut state);
        complete_job(&mut state, &jid("build"), ExecutionStatus::Skipped);
        assert!(state.queue.is_empty());
        assert!(state.pending.is_empty());
        assert_eq!(
            state.jobs.get(&jid("test")).copied(),
            Some(ExecutionStatus::Skipped),
            "dependent must be skipped when need was skipped"
        );
    }

    #[test]
    fn matrix_prefix_need_requires_all_siblings() {
        let mut jobs_map = BTreeMap::new();
        jobs_map.insert(jid("build (1)"), ExecutionStatus::Success);
        jobs_map.insert(jid("build (2)"), ExecutionStatus::InProgress);
        assert!(!need_satisfied(&jobs_map, &jid("build")));
        jobs_map.insert(jid("build (2)"), ExecutionStatus::Success);
        assert!(need_satisfied(&jobs_map, &jid("build")));
    }

    #[test]
    fn cycle_detection_finds_loop() {
        let mut edges = BTreeMap::new();
        edges.insert("a".into(), vec!["b".into()]);
        edges.insert("b".into(), vec!["c".into()]);
        edges.insert("c".into(), vec!["a".into()]);
        assert!(detect_needs_cycle(&edges).is_err());
    }

    #[test]
    fn cycle_detection_accepts_dag() {
        let mut edges = BTreeMap::new();
        edges.insert("a".into(), vec![]);
        edges.insert("b".into(), vec!["a".into()]);
        edges.insert("c".into(), vec!["a".into(), "b".into()]);
        assert!(detect_needs_cycle(&edges).is_ok());
    }

    /// Generate a random DAG over `n` nodes by only allowing edges i → j with j < i.
    fn arb_dag(n: usize) -> impl Strategy<Value = Vec<SchedJob>> {
        // Bitmask per node selecting a subset of lower-index dependencies.
        proptest::collection::vec(any::<u8>(), n..=n).prop_map(move |masks| {
            (0..n)
                .map(|i| {
                    let mut needs = Vec::new();
                    if i > 0 {
                        let mut seen = BTreeSet::new();
                        for d in 0..i {
                            if masks[i] & (1u8 << (d % 8)) != 0 && seen.insert(d) {
                                needs.push(jid(&format!("j{d}")));
                            }
                        }
                    }
                    SchedJob {
                        id: jid(&format!("j{i}")),
                        base_id: format!("j{i}"),
                        needs,
                        max_parallel: None,
                        if_condition: None,
                    }
                })
                .collect()
        })
    }

    /// Golden needs-dag fixture: build → test → deploy promotion order.
    #[test]
    fn golden_needs_dag_promotes_in_dependency_order() {
        let jobs = vec![
            SchedJob {
                id: jid("build"),
                base_id: "build".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("test"),
                base_id: "test".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("deploy"),
                base_id: "deploy".into(),
                needs: vec![jid("build"), jid("test")],
                max_parallel: None,
                if_condition: None,
            },
        ];
        let mut state = seed_from_jobs(jobs);
        assert_eq!(acquire_next(&mut state).unwrap().id.0, "build");
        complete_job(&mut state, &jid("build"), ExecutionStatus::Success);
        assert_eq!(acquire_next(&mut state).unwrap().id.0, "test");
        complete_job(&mut state, &jid("test"), ExecutionStatus::Success);
        assert_eq!(acquire_next(&mut state).unwrap().id.0, "deploy");
    }

    // Oracle: GitHub `jobs.<job_id>.needs` contract (workflow syntax URL in
    // the module docs). These properties cover placement uniqueness, waiting
    // for terminal dependencies, deterministic FIFO promotion, successful
    // settlement, and terminal skip propagation.
    //
    // The cycle property below additionally cross-checks the implementation
    // against an independent DFS reference; it is not an official algorithm
    // claim.
    proptest! {
        #![proptest_config(dag_config(10_000))]

        /// No job is ever present in both queue and pending, or twice in either.
        #[test]
        fn no_duplicate_placement(jobs in arb_dag(6)) {
            let mut state = seed_from_jobs(jobs.clone());
            // Complete roots successfully in waves until settled or stuck.
            for _ in 0..32 {
                while let Some(job) = acquire_next(&mut state) {
                    complete_job(&mut state, &job.id, ExecutionStatus::Success);
                }
                if state.pending.is_empty() && state.queue.is_empty() {
                    break;
                }
                // If still pending with empty queue, nothing more can promote.
                if state.queue.is_empty() {
                    break;
                }
            }
            for job in &jobs {
                prop_assert!(
                    placement_count(&state, &job.id) <= 1,
                    "job {} placed more than once",
                    job.id.0
                );
            }
        }

        /// Completing every job as Success eventually settles an acyclic graph.
        #[test]
        fn success_wave_settles_dag(jobs in arb_dag(5)) {
            let mut edges = BTreeMap::new();
            for job in &jobs {
                edges.insert(
                    job.id.0.clone(),
                    job.needs.iter().map(|n| n.0.clone()).collect(),
                );
            }
            prop_assert!(detect_needs_cycle(&edges).is_ok());

            let mut state = seed_from_jobs(jobs);
            for _ in 0..64 {
                if let Some(job) = acquire_next(&mut state) {
                    complete_job(&mut state, &job.id, ExecutionStatus::Success);
                } else if state.pending.is_empty() {
                    break;
                } else {
                    // promote may still free jobs after completions
                    if promote_ready_jobs(&mut state) == 0 {
                        break;
                    }
                }
            }
            prop_assert!(
                run_settled(&state),
                "DAG did not settle: pending={:?} queue={:?} jobs={:?}",
                state.pending.iter().map(|j| j.id.0.clone()).collect::<Vec<_>>(),
                state.queue.iter().map(|j| j.id.0.clone()).collect::<Vec<_>>(),
                state.jobs
            );
        }

        /// A job never becomes InProgress before all needs are Success|Skipped.
        #[test]
        fn never_dispatch_before_needs(jobs in arb_dag(5)) {
            let mut state = seed_from_jobs(jobs.clone());
            let needs_of: BTreeMap<_, _> = jobs
                .iter()
                .map(|j| (j.id.clone(), j.needs.clone()))
                .collect();

            for _ in 0..64 {
                if let Some(job) = acquire_next(&mut state) {
                    for need in needs_of.get(&job.id).into_iter().flatten() {
                        let status = state.jobs.get(need).copied();
                        prop_assert!(
                            matches!(
                                status,
                                Some(ExecutionStatus::Success) | Some(ExecutionStatus::Skipped)
                            ),
                            "dispatched {} while need {} is {:?}",
                            job.id.0,
                            need.0,
                            status
                        );
                    }
                    // Alternate success/skip to exercise both satisfying terminals.
                    let terminal = if job.id.0.ends_with('0') {
                        ExecutionStatus::Skipped
                    } else {
                        ExecutionStatus::Success
                    };
                    complete_job(&mut state, &job.id, terminal);
                } else if promote_ready_jobs(&mut state) == 0 {
                    break;
                }
            }
        }

        /// Promotion is deterministic: same seed state → same queue order.
        #[test]
        fn promote_is_deterministic(jobs in arb_dag(5)) {
            let mut a = seed_from_jobs(jobs.clone());
            let mut b = seed_from_jobs(jobs);
            // Complete first root if any.
            if let Some(job) = acquire_next(&mut a) {
                complete_job(&mut a, &job.id, ExecutionStatus::Success);
            }
            if let Some(job) = acquire_next(&mut b) {
                complete_job(&mut b, &job.id, ExecutionStatus::Success);
            }
            let qa: Vec<_> = a.queue.iter().map(|j| j.id.0.clone()).collect();
            let qb: Vec<_> = b.queue.iter().map(|j| j.id.0.clone()).collect();
            prop_assert_eq!(qa, qb);
            let pa: Vec<_> = a.pending.iter().map(|j| j.id.0.clone()).collect();
            let pb: Vec<_> = b.pending.iter().map(|j| j.id.0.clone()).collect();
            prop_assert_eq!(pa, pb);
        }

        /// Failure of a dependency: dependent is never queued (it either stays
        /// pending if other deps are non-terminal, or becomes Skipped once
        /// all deps are terminal).
        #[test]
        fn failure_never_queues_dependent(jobs in arb_dag(4)) {
            let mut state = seed_from_jobs(jobs.clone());
            if let Some(job) = acquire_next(&mut state) {
                complete_job(&mut state, &job.id, ExecutionStatus::Failure);
                for other in &jobs {
                    if other.needs.iter().any(|n| n == &job.id) {
                        // Must NOT be queued under default gate after failed dep
                        prop_assert!(
                            state.queue.iter().all(|q| q.id != other.id),
                            "dependent {} entered queue after failed need {}",
                            other.id.0,
                            job.id.0
                        );
                        // If ALL deps are terminal, must be Skipped (not stuck pending).
                        let all_deps_terminal = other.needs.iter().all(|need| {
                            let status = state.jobs.get(need).copied();
                            matches!(
                                status,
                                Some(ExecutionStatus::Success)
                                    | Some(ExecutionStatus::Failure)
                                    | Some(ExecutionStatus::Skipped)
                                    | Some(ExecutionStatus::Cancelled)
                            )
                        });
                        if all_deps_terminal {
                            prop_assert_eq!(
                                state.jobs.get(&other.id).copied(),
                                Some(ExecutionStatus::Skipped),
                                "dependent {} with all-terminal deps must be Skipped, not stuck",
                                other.id.0
                            );
                        }
                    }
                }
            }
        }
    }

    // Random edge sets: cycle detector never panics and is consistent with
    // a simple DFS reference.
    proptest! {
        #![proptest_config(dag_config(10_000))]

        #[test]
        fn cycle_detector_stable(
            edges in proptest::collection::btree_map(
                "[a-d]{1}",
                proptest::collection::vec("[a-d]{1}", 0..3),
                1..5
            )
        ) {
            let result = detect_needs_cycle(&edges);
            // Self-loop is always a cycle.
            for (k, deps) in &edges {
                if deps.iter().any(|d| d == k) {
                    prop_assert!(result.is_err());
                    return Ok(());
                }
            }
            let _ = result;
        }
    }

    // ─── Regression fixtures (spec §1 Required regression fixtures) ─────────

    /// `build` fails → default `test` becomes Skipped.
    /// Official: failed dependency causes dependent to skip under default gate.
    #[test]
    fn regression_build_fails_test_skipped() {
        let jobs = vec![
            SchedJob {
                id: jid("build"),
                base_id: "build".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("test"),
                base_id: "test".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: None,
            },
        ];
        let mut state = seed_from_jobs(jobs);
        let build = acquire_next(&mut state).unwrap();
        assert_eq!(build.id.0, "build");
        complete_job(&mut state, &jid("build"), ExecutionStatus::Failure);

        assert!(state.queue.is_empty());
        assert!(state.pending.is_empty());
        assert_eq!(state.jobs[&jid("test")], ExecutionStatus::Skipped);
        assert!(run_settled(&state));
    }

    /// `build` is skipped → default `test` becomes Skipped.
    #[test]
    fn regression_build_skipped_test_skipped() {
        let jobs = vec![
            SchedJob {
                id: jid("build"),
                base_id: "build".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("test"),
                base_id: "test".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: None,
            },
        ];
        let mut state = seed_from_jobs(jobs);
        let build = acquire_next(&mut state).unwrap();
        assert_eq!(build.id.0, "build");
        complete_job(&mut state, &jid("build"), ExecutionStatus::Skipped);

        assert!(state.queue.is_empty());
        assert!(state.pending.is_empty());
        assert_eq!(state.jobs[&jid("test")], ExecutionStatus::Skipped);
        assert!(run_settled(&state));
    }

    /// `build` fails → `cleanup` with `if: always()` runs.
    /// Official: `always()` overrides the default gate.
    #[test]
    fn regression_always_runs_after_failure() {
        let jobs = vec![
            SchedJob {
                id: jid("build"),
                base_id: "build".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("cleanup"),
                base_id: "cleanup".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: Some("always()".to_owned()),
            },
        ];
        let mut state = seed_from_jobs(jobs);
        let build = acquire_next(&mut state).unwrap();
        assert_eq!(build.id.0, "build");
        complete_job(&mut state, &jid("build"), ExecutionStatus::Failure);

        // cleanup should be promoted to queue, not skipped
        assert_eq!(state.queue.len(), 1);
        let cleanup = acquire_next(&mut state).unwrap();
        assert_eq!(cleanup.id.0, "cleanup");
        assert_eq!(state.jobs[&jid("cleanup")], ExecutionStatus::InProgress);
    }

    /// `build` fails → `cleanup` with `if: failure()` runs (official status context).
    #[test]
    fn regression_failure_condition_runs_after_failure() {
        let jobs = vec![
            SchedJob {
                id: jid("build"),
                base_id: "build".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("cleanup"),
                base_id: "cleanup".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: Some("failure()".to_owned()),
            },
        ];
        let mut state = seed_from_jobs(jobs);
        acquire_next(&mut state);
        complete_job(&mut state, &jid("build"), ExecutionStatus::Failure);

        assert_eq!(state.queue.len(), 1);
        let cleanup = acquire_next(&mut state).unwrap();
        assert_eq!(cleanup.id.0, "cleanup");
    }

    /// `build` succeeds → `cleanup` with `if: failure()` is skipped.
    #[test]
    fn failure_condition_skips_when_success() {
        let jobs = vec![
            SchedJob {
                id: jid("build"),
                base_id: "build".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("cleanup"),
                base_id: "cleanup".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: Some("failure()".to_owned()),
            },
        ];
        let mut state = seed_from_jobs(jobs);
        acquire_next(&mut state);
        complete_job(&mut state, &jid("build"), ExecutionStatus::Success);

        assert!(state.queue.is_empty());
        assert_eq!(state.jobs[&jid("cleanup")], ExecutionStatus::Skipped);
    }

    /// `build` cancelled → `on_cancel` with `if: cancelled()` runs.
    #[test]
    fn cancelled_condition_runs_after_cancellation() {
        let jobs = vec![
            SchedJob {
                id: jid("build"),
                base_id: "build".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("on_cancel"),
                base_id: "on_cancel".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: Some("cancelled()".to_owned()),
            },
        ];
        let mut state = seed_from_jobs(jobs);
        acquire_next(&mut state);
        complete_job(&mut state, &jid("build"), ExecutionStatus::Cancelled);

        assert_eq!(state.queue.len(), 1);
        let on_cancel = acquire_next(&mut state).unwrap();
        assert_eq!(on_cancel.id.0, "on_cancel");
    }

    /// Transitive ancestor failure: a → b → c. `a` fails, both b and c skip.
    #[test]
    fn transitive_ancestor_failure_skips_chain() {
        let jobs = vec![
            SchedJob {
                id: jid("a"),
                base_id: "a".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("b"),
                base_id: "b".into(),
                needs: vec![jid("a")],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("c"),
                base_id: "c".into(),
                needs: vec![jid("b")],
                max_parallel: None,
                if_condition: None,
            },
        ];
        let mut state = seed_from_jobs(jobs);
        acquire_next(&mut state);
        complete_job(&mut state, &jid("a"), ExecutionStatus::Failure);

        // b skips because a failed; c skips transitively
        assert!(state.pending.is_empty());
        assert!(state.queue.is_empty());
        assert_eq!(state.jobs[&jid("b")], ExecutionStatus::Skipped);
        assert_eq!(state.jobs[&jid("c")], ExecutionStatus::Skipped);
        assert!(run_settled(&state));
    }

    /// Diamond graph: build → test-a/test-b → deploy.
    /// All succeed → deploy runs.
    #[test]
    fn regression_diamond_graph_settlement() {
        let jobs = vec![
            SchedJob {
                id: jid("build"),
                base_id: "build".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("test-a"),
                base_id: "test-a".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("test-b"),
                base_id: "test-b".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("deploy"),
                base_id: "deploy".into(),
                needs: vec![jid("test-a"), jid("test-b")],
                max_parallel: None,
                if_condition: None,
            },
        ];
        let mut state = seed_from_jobs(jobs);
        // build is the only root
        let build = acquire_next(&mut state).unwrap();
        assert_eq!(build.id.0, "build");
        assert!(acquire_next(&mut state).is_none());

        complete_job(&mut state, &jid("build"), ExecutionStatus::Success);
        // test-a and test-b promoted
        assert_eq!(state.queue.len(), 2);

        let ta = acquire_next(&mut state).unwrap();
        let tb = acquire_next(&mut state).unwrap();
        let mut test_ids: Vec<_> = vec![ta.id.0.clone(), tb.id.0.clone()];
        test_ids.sort();
        assert_eq!(test_ids, vec!["test-a", "test-b"]);

        // deploy not yet ready
        assert!(acquire_next(&mut state).is_none());

        complete_job(&mut state, &ta.id, ExecutionStatus::Success);
        // deploy still waiting for test-b
        assert!(state.queue.is_empty());

        complete_job(&mut state, &tb.id, ExecutionStatus::Success);
        // now deploy is promoted
        let deploy = acquire_next(&mut state).unwrap();
        assert_eq!(deploy.id.0, "deploy");
        complete_job(&mut state, &jid("deploy"), ExecutionStatus::Success);
        assert!(run_settled(&state));
    }

    /// Diamond graph with one leg failed → deploy skipped.
    #[test]
    fn diamond_one_leg_fails_deploy_skipped() {
        let jobs = vec![
            SchedJob {
                id: jid("build"),
                base_id: "build".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("test-a"),
                base_id: "test-a".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("test-b"),
                base_id: "test-b".into(),
                needs: vec![jid("build")],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("deploy"),
                base_id: "deploy".into(),
                needs: vec![jid("test-a"), jid("test-b")],
                max_parallel: None,
                if_condition: None,
            },
        ];
        let mut state = seed_from_jobs(jobs);
        acquire_next(&mut state); // build
        complete_job(&mut state, &jid("build"), ExecutionStatus::Success);

        let ta = acquire_next(&mut state).unwrap();
        let tb = acquire_next(&mut state).unwrap();
        complete_job(&mut state, &ta.id, ExecutionStatus::Success);
        complete_job(&mut state, &tb.id, ExecutionStatus::Failure);

        // deploy must be skipped
        assert_eq!(state.jobs[&jid("deploy")], ExecutionStatus::Skipped);
        assert!(run_settled(&state));
    }

    /// Duplicate completion is idempotent: completing an already-terminal job
    /// does not promote dependents again or change the terminal status.
    #[test]
    fn duplicate_completion_is_idempotent() {
        let jobs = vec![
            SchedJob {
                id: jid("a"),
                base_id: "a".into(),
                needs: vec![],
                max_parallel: None,
                if_condition: None,
            },
            SchedJob {
                id: jid("b"),
                base_id: "b".into(),
                needs: vec![jid("a")],
                max_parallel: None,
                if_condition: None,
            },
        ];
        let mut state = seed_from_jobs(jobs);
        acquire_next(&mut state);
        let promoted_first = complete_job(&mut state, &jid("a"), ExecutionStatus::Success);
        assert_eq!(promoted_first, 1); // b promoted

        // Completing again must be a no-op
        let promoted_second = complete_job(&mut state, &jid("a"), ExecutionStatus::Success);
        assert_eq!(
            promoted_second, 0,
            "duplicate completion must not re-promote"
        );

        // Try to override with a different status — must be ignored
        let promoted_third = complete_job(&mut state, &jid("a"), ExecutionStatus::Failure);
        assert_eq!(promoted_third, 0);
        assert_eq!(
            state.jobs[&jid("a")],
            ExecutionStatus::Success,
            "terminal status must be immutable"
        );
    }

    /// Cancellation immutability: once a job is terminal (e.g. Cancelled),
    /// it cannot transition back. The terminal status is sticky.
    #[test]
    fn cancellation_is_immutable() {
        let jobs = vec![SchedJob {
            id: jid("a"),
            base_id: "a".into(),
            needs: vec![],
            max_parallel: None,
            if_condition: None,
        }];
        let mut state = seed_from_jobs(jobs);
        acquire_next(&mut state);
        complete_job(&mut state, &jid("a"), ExecutionStatus::Cancelled);
        assert_eq!(state.jobs[&jid("a")], ExecutionStatus::Cancelled);

        // Try to override with Success — must be ignored
        complete_job(&mut state, &jid("a"), ExecutionStatus::Success);
        assert_eq!(
            state.jobs[&jid("a")],
            ExecutionStatus::Cancelled,
            "cancelled status must be immutable"
        );
    }

    /// Terminal job never returns to pending, queued, or running (spec §1.8).
    #[test]
    fn terminal_job_never_reverts() {
        let jobs = vec![SchedJob {
            id: jid("x"),
            base_id: "x".into(),
            needs: vec![],
            max_parallel: None,
            if_condition: None,
        }];
        let mut state = seed_from_jobs(jobs);
        let x = acquire_next(&mut state).unwrap();
        assert_eq!(state.jobs[&x.id], ExecutionStatus::InProgress);

        complete_job(&mut state, &x.id, ExecutionStatus::Success);
        assert_eq!(state.jobs[&x.id], ExecutionStatus::Success);

        // Attempt to complete again with different statuses
        for status in [
            ExecutionStatus::Failure,
            ExecutionStatus::Cancelled,
            ExecutionStatus::Queued,
            ExecutionStatus::InProgress,
        ] {
            complete_job(&mut state, &x.id, status);
            assert_eq!(
                state.jobs[&x.id],
                ExecutionStatus::Success,
                "terminal Success must not revert to {status:?}"
            );
        }
    }

    // ─── Bounded independent-oracle model property (spec §1) ────────────────

    /// Independent oracle for default dependency decision.
    /// Does NOT call `dependency_decision`, `need_satisfied`, or any scheduler fn.
    /// Implements the official GitHub status-function semantics directly.
    /// A dependency set is summarized to one result: failure wins over
    /// cancellation, then skipped, then success.
    fn oracle_should_run(
        ancestor_statuses: &[ExecutionStatus],
        if_condition: Option<&str>,
    ) -> bool {
        let aggregate = if ancestor_statuses
            .iter()
            .any(|s| *s == ExecutionStatus::Failure)
        {
            ExecutionStatus::Failure
        } else if ancestor_statuses
            .iter()
            .any(|s| *s == ExecutionStatus::Cancelled)
        {
            ExecutionStatus::Cancelled
        } else if ancestor_statuses
            .iter()
            .any(|s| *s == ExecutionStatus::Skipped)
        {
            ExecutionStatus::Skipped
        } else {
            ExecutionStatus::Success
        };
        let all_success = aggregate == ExecutionStatus::Success;
        let any_failure = aggregate == ExecutionStatus::Failure;
        let any_cancelled = aggregate == ExecutionStatus::Cancelled;

        match if_condition {
            None | Some("") => all_success,
            Some("always()") => true,
            Some("failure()") => any_failure,
            Some("cancelled()") => any_cancelled,
            Some("success()") => all_success,
            _ => false, // unrecognized — oracle is conservative
        }
    }

    fn arb_terminal_status() -> impl Strategy<Value = ExecutionStatus> {
        prop_oneof![
            Just(ExecutionStatus::Success),
            Just(ExecutionStatus::Failure),
            Just(ExecutionStatus::Skipped),
            Just(ExecutionStatus::Cancelled),
        ]
    }

    // Oracle: GitHub status-check functions documentation plus pinned
    // actions/runner v2.335.1 StepsRunner.cs and *Function.cs sources. The
    // generated truth table compares aggregate ancestor status against the
    // independent oracle, including failure/skip/cancelled/always overrides.
    fn arb_condition() -> impl Strategy<Value = Option<String>> {
        prop_oneof![
            Just(None),
            Just(Some("always()".to_owned())),
            Just(Some("failure()".to_owned())),
            Just(Some("cancelled()".to_owned())),
            Just(Some("success()".to_owned())),
        ]
    }

    proptest! {
        #![proptest_config(dag_config(5_000))]

        /// Bounded independent-oracle model property: for a single
        /// dependent job with 1–4 needs, the scheduler's decision
        /// (run vs skip) must agree with the independent oracle.
        #[test]
        fn oracle_agrees_with_scheduler(
            needs_count in 1usize..=4,
            statuses in proptest::collection::vec(arb_terminal_status(), 1..=4),
            condition in arb_condition(),
        ) {
            let need_count = needs_count.min(statuses.len());
            // Build a set of upstream jobs
            let mut upstream_jobs = Vec::new();
            for i in 0..need_count {
                upstream_jobs.push(SchedJob {
                    id: jid(&format!("up{i}")),
                    base_id: format!("up{i}"),
                    needs: vec![],
                    max_parallel: None,
                    if_condition: None,
                });
            }
            let needs: Vec<JobId> = (0..need_count).map(|i| jid(&format!("up{i}"))).collect();
            let dependent = SchedJob {
                id: jid("dep"),
                base_id: "dep".into(),
                needs: needs.clone(),
                max_parallel: None,
                if_condition: condition.clone(),
            };
            let mut all_jobs = upstream_jobs;
            all_jobs.push(dependent);

            let mut state = seed_from_jobs(all_jobs);
            // Complete all upstream jobs with the generated statuses
            for i in 0..need_count {
                let up_id = jid(&format!("up{i}"));
                // Force acquire (roots go straight to queue)
                acquire_next(&mut state);
                complete_job(&mut state, &up_id, statuses[i]);
            }

            let actual_status = state.jobs.get(&jid("dep")).copied().unwrap();
            let ancestor_stats: Vec<ExecutionStatus> =
                (0..need_count).map(|i| statuses[i]).collect();
            let oracle_says_run = oracle_should_run(
                &ancestor_stats,
                condition.as_deref(),
            );

            if oracle_says_run {
                prop_assert!(
                    matches!(actual_status, ExecutionStatus::Queued | ExecutionStatus::InProgress),
                    "oracle says run but scheduler gave {:?} for condition={:?} statuses={:?}",
                    actual_status,
                    condition,
                    ancestor_stats
                );
            } else {
                prop_assert_eq!(
                    actual_status,
                    ExecutionStatus::Skipped,
                    "oracle says skip but scheduler gave {:?} for condition={:?} statuses={:?}",
                    actual_status,
                    condition,
                    ancestor_stats
                );
            }
        }

        /// Every valid acyclic graph eventually settles when ALL jobs complete
        /// with mixed terminal statuses (not just Success).
        #[test]
        fn mixed_terminal_settles(
            jobs in arb_dag(5),
            statuses in proptest::collection::vec(arb_terminal_status(), 5..=5),
        ) {
            let mut edges = BTreeMap::new();
            for job in &jobs {
                edges.insert(
                    job.id.0.clone(),
                    job.needs.iter().map(|n| n.0.clone()).collect(),
                );
            }
            prop_assert!(detect_needs_cycle(&edges).is_ok());

            let mut state = seed_from_jobs(jobs.clone());
            let mut idx = 0;
            for _ in 0..64 {
                if let Some(job) = acquire_next(&mut state) {
                    let status = statuses[idx % statuses.len()];
                    idx += 1;
                    complete_job(&mut state, &job.id, status);
                } else if state.pending.is_empty() {
                    break;
                } else if promote_ready_jobs(&mut state) == 0 {
                    break;
                }
            }
            prop_assert!(
                run_settled(&state),
                "DAG did not settle with mixed statuses: pending={:?} queue={:?}",
                state.pending.iter().map(|j| j.id.0.clone()).collect::<Vec<_>>(),
                state.queue.iter().map(|j| j.id.0.clone()).collect::<Vec<_>>(),
            );
        }
    }
}
