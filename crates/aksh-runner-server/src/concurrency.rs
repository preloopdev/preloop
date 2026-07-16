//! GitHub Actions `concurrency:` group enforcement (control-plane only).
//
//! Runners never learn about concurrency groups; they only observe
//! `JobCancellation` when a holder is cancelled.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aksh_gha_expressions::{eval_bool, Context};
use aksh_gha_parser::eval::{build_context, resolve_string};
use aksh_gha_parser::{Concurrency, ConcurrencyQueue};
use aksh_gha_protocol::{azdo, ExecutionStatus, JobId, RunId};
use serde_json::{json, Value};
use tracing::warn;

/// Official cancel grace period body value (TimeSpan).
pub const CANCEL_TIMEOUT: &str = "00:05:00";

/// A concurrency-group holder (workflow run, single job, or reusable JobSet).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Holder {
    /// Workflow-level concurrency covers an entire run.
    Run(RunId),
    /// Job-level concurrency covers one job.
    Job {
        /// Workflow run containing the job.
        run_id: RunId,
        /// Logical job identifier.
        job_id: JobId,
    },
    /// Reusable workflow invocation (caller/embedded) covers a set of jobs.
    JobSet {
        /// Workflow run containing the reusable invocation.
        run_id: RunId,
        /// Jobs covered by the reusable invocation.
        job_ids: BTreeSet<JobId>,
    },
}

impl Holder {
    /// Return the workflow run that owns this holder.
    pub fn run_id(&self) -> RunId {
        match self {
            Self::Run(id) => *id,
            Self::Job { run_id, .. } | Self::JobSet { run_id, .. } => *run_id,
        }
    }

    /// Whether this holder covers the specified logical job.
    pub fn contains_job(&self, run_id: RunId, job_id: &JobId) -> bool {
        match self {
            Self::Run(id) => *id == run_id,
            Self::Job {
                run_id: r,
                job_id: j,
            } => *r == run_id && j == job_id,
            Self::JobSet { run_id: r, job_ids } => *r == run_id && job_ids.contains(job_id),
        }
    }

    /// Whether this holder represents the whole specified run.
    pub fn is_run_holder(&self, run_id: RunId) -> bool {
        matches!(self, Self::Run(id) if *id == run_id)
            || matches!(self, Self::JobSet { run_id: r, .. } if *r == run_id)
    }
}

/// One concurrency group (repo + group name, case-insensitive key).
#[derive(Debug, Clone, Default)]
pub struct ConcurrencyGroup {
    /// Display-case group name as first evaluated.
    pub display_name: String,
    /// Holder currently owning the group slot.
    pub running: Option<Holder>,
    /// Holders waiting for the group slot in admission order.
    pub pending: VecDeque<Holder>,
}

/// Which GitHub Actions scope a concurrency expression is evaluated in.
///
/// Workflow scope allows `github`, `inputs`, `vars` only.
/// Job scope additionally allows `needs`, `strategy`, `matrix`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyScope {
    /// Workflow-level concurrency expression scope.
    Workflow,
    /// Job-level concurrency expression scope.
    Job,
}

/// Typed input for concurrency expression evaluation.
pub struct ConcurrencyContext<'a> {
    /// Expression scope controlling allowed contexts.
    pub scope: ConcurrencyScope,
    /// GitHub event/workflow context.
    pub github: &'a Value,
    /// Repository or organization configuration variables.
    pub vars: &'a BTreeMap<String, String>,
    /// Workflow dispatch or reusable-workflow inputs.
    pub inputs: &'a BTreeMap<String, Value>,
    /// Job scope only — `matrix.*` values.
    pub matrix: Option<&'a BTreeMap<String, Value>>,
    /// Job scope only — `strategy` context object.
    pub strategy: Option<&'a Value>,
    /// Job scope only — `needs.<job>.result/outputs`.
    pub needs: Option<&'a Value>,
}

/// Evaluate a raw concurrency config against a typed scope-aware context.
///
/// Returns `(group_name, cancel_in_progress, queue_mode)` or an error string.
/// Errors include expression evaluation failures, empty group names, and the
/// `queue: max` + `cancel-in-progress: true` incompatibility (GitHub invariant 3).
pub fn evaluate_concurrency(
    raw: &Concurrency,
    ctx: &ConcurrencyContext<'_>,
) -> Result<(String, bool, ConcurrencyQueue), String> {
    let expr_ctx = build_eval_context(ctx);
    let group = resolve_string(&raw.group, &expr_ctx)?;
    let cancel = match &raw.cancel_in_progress {
        None => false,
        Some(expr) => eval_bool(expr, &expr_ctx).map_err(|e| format!("{e}"))?,
    };
    if cancel && raw.queue == ConcurrencyQueue::Max {
        return Err("queue: max and cancel-in-progress: true are incompatible".to_owned());
    }
    Ok((group, cancel, raw.queue))
}

/// Build the expression `Context` for a concurrency evaluation, enforcing the
/// GitHub scope allowlist: workflow scope never receives `matrix`, `strategy`,
/// or `needs`; job scope receives all allowed contexts.
fn build_eval_context(ctx: &ConcurrencyContext<'_>) -> Context {
    match ctx.scope {
        ConcurrencyScope::Workflow => {
            // Only github, inputs, vars are valid at workflow scope.
            let mut c = Context::default();
            c.insert("github", ctx.github.clone());
            let vars_val = Value::Object(
                ctx.vars
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                    .collect(),
            );
            c.insert("vars", vars_val);
            let inputs_val = Value::Object(
                ctx.inputs
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            );
            c.insert("inputs", inputs_val);
            c
        }
        ConcurrencyScope::Job => {
            let empty_matrix = indexmap::IndexMap::new();
            let matrix_im: indexmap::IndexMap<String, Value> = ctx
                .matrix
                .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or(empty_matrix);
            let empty_strategy = json!({});
            let strategy = ctx.strategy.unwrap_or(&empty_strategy);
            let mut c = build_context(
                ctx.github,
                &BTreeMap::new(),
                ctx.vars,
                &matrix_im,
                strategy,
                &BTreeMap::new(),
                ctx.inputs,
            );
            if let Some(needs) = ctx.needs {
                c.insert("needs", needs.clone());
            }
            c
        }
    }
}

/// Lowercased (repo, group) key.
pub fn concurrency_key(repo: &str, group: &str) -> (String, String) {
    (repo.to_ascii_lowercase(), group.to_ascii_lowercase())
}

/// Parse concurrency fields stored on a queued job plan.
pub fn concurrency_from_plan_fields(
    group: Option<&str>,
    cancel: Option<&str>,
    queue: Option<&str>,
) -> Option<Concurrency> {
    let group = group?.to_owned();
    let queue = match queue.unwrap_or("single") {
        "max" => ConcurrencyQueue::Max,
        _ => ConcurrencyQueue::Single,
    };
    Some(Concurrency {
        group,
        cancel_in_progress: cancel.map(|s| s.to_owned()),
        queue,
    })
}

/// Official JobCancellation body.
pub fn job_cancel_body(agent_job_id: uuid::Uuid) -> String {
    json!({
        "jobId": agent_job_id,
        "timeout": CANCEL_TIMEOUT,
    })
    .to_string()
}

/// Build a cancellation NDJSON reason helper.
pub fn pending_reason() -> Option<String> {
    Some("concurrency_pending".to_owned())
}

/// Build the terminal reason for a holder cancelled by concurrency.
pub fn cancelled_reason() -> Option<String> {
    Some("concurrency_cancelled".to_owned())
}

/// Whether a status is non-terminal for concurrency release purposes.
pub fn is_terminal(status: ExecutionStatus) -> bool {
    matches!(
        status,
        ExecutionStatus::Success
            | ExecutionStatus::Failure
            | ExecutionStatus::Cancelled
            | ExecutionStatus::Skipped
    )
}

/// Whether a job status is still awaiting assignment (queued or concurrency-pending).
pub fn is_awaiting_execution(status: ExecutionStatus) -> bool {
    matches!(status, ExecutionStatus::Queued | ExecutionStatus::Pending)
}

/// Helper: all jobs of a holder are terminal?
pub fn holder_is_terminal(holder: &Holder, jobs: &BTreeMap<JobId, ExecutionStatus>) -> bool {
    match holder {
        Holder::Run(_) => jobs.values().all(|s| is_terminal(*s)),
        Holder::Job { job_id, .. } => jobs.get(job_id).copied().is_some_and(is_terminal),
        Holder::JobSet { job_ids, .. } => job_ids
            .iter()
            .all(|id| jobs.get(id).copied().is_some_and(is_terminal)),
    }
}

/// Max pending holders for `queue: max`.
pub const QUEUE_MAX_PENDING: usize = 100;

/// Decide how an arrival joins a contended group under its own queue mode.
/// Returns holders that should be cancelled, and whether the arrival itself is cancelled.
#[derive(Debug)]
pub struct QueueJoinResult {
    /// Existing pending holders to cancel (queue: single replacement).
    pub cancel_pending: Vec<Holder>,
    /// If true, cancel the arrival instead of parking it (queue: max overflow).
    pub cancel_arrival: bool,
    /// If true, park the arrival as pending.
    pub park_arrival: bool,
}

/// Compute the queue action for an arrival without mutating group state.
pub fn apply_queue_mode(
    queue: ConcurrencyQueue,
    existing_pending: &VecDeque<Holder>,
) -> QueueJoinResult {
    match queue {
        ConcurrencyQueue::Single => QueueJoinResult {
            cancel_pending: existing_pending.iter().cloned().collect(),
            cancel_arrival: false,
            park_arrival: true,
        },
        ConcurrencyQueue::Max => {
            if existing_pending.len() >= QUEUE_MAX_PENDING {
                QueueJoinResult {
                    cancel_pending: Vec::new(),
                    cancel_arrival: true,
                    park_arrival: false,
                }
            } else {
                QueueJoinResult {
                    cancel_pending: Vec::new(),
                    cancel_arrival: false,
                    park_arrival: true,
                }
            }
        }
    }
}

/// Build a needs context JSON object from run job outputs for expression eval.
pub fn needs_json_from_context_data(needs: &BTreeMap<String, azdo::PipelineContextData>) -> Value {
    // Best-effort: expose result/outputs as JSON for expression evaluation.
    let mut map = serde_json::Map::new();
    for (k, v) in needs {
        map.insert(k.clone(), context_data_to_json(v));
    }
    Value::Object(map)
}

/// Convert runner protocol context data into expression JSON recursively.
pub fn context_data_to_json(data: &azdo::PipelineContextData) -> Value {
    match data {
        azdo::PipelineContextData::String(s) => Value::String(s.clone()),
        azdo::PipelineContextData::Bool(b) => Value::Bool(*b),
        azdo::PipelineContextData::Number(n) => json!(n),
        azdo::PipelineContextData::Array(items) => {
            Value::Array(items.iter().map(context_data_to_json).collect())
        }
        azdo::PipelineContextData::Dict(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), context_data_to_json(v));
            }
            Value::Object(obj)
        }
    }
}

/// Log and ignore evaluation failures (job-level marks failure at call site).
pub fn log_eval_error(context: &str, err: &str) {
    warn!(%context, %err, "concurrency expression evaluation failed");
}

#[cfg(test)]
mod properties {
    use super::*;
    use proptest::collection::{btree_map, btree_set, vec as pvec};
    use proptest::prelude::*;
    use uuid::Uuid;

    // ---- deterministic ID helpers ----

    fn run_id_from(n: u32) -> RunId {
        RunId(Uuid::from_u128(n as u128))
    }

    fn job_id_from(n: u32) -> JobId {
        JobId(format!("job-{n}"))
    }

    // ---- generators ----

    /// Repository and group names with deliberate upper/lowercase pairs.
    fn arb_name() -> impl Strategy<Value = String> {
        "[A-Za-z0-9_./-]{1,32}"
    }

    /// A holder with deterministic IDs.
    fn arb_holder() -> impl Strategy<Value = Holder> {
        prop_oneof![
            (0..100u32).prop_map(|n| Holder::Run(run_id_from(n))),
            (0..100u32, 0..20u32).prop_map(|(r, j)| Holder::Job {
                run_id: run_id_from(r),
                job_id: job_id_from(j),
            }),
            (0..100u32, btree_set(0..20u32, 1..=8)).prop_map(|(r, js)| Holder::JobSet {
                run_id: run_id_from(r),
                job_ids: js.into_iter().map(job_id_from).collect(),
            }),
        ]
    }

    /// Generate a pending deque of distinct holders with controlled length.
    fn arb_pending(max_len: usize) -> impl Strategy<Value = VecDeque<Holder>> {
        pvec(arb_holder(), 0..=max_len).prop_map(|v| v.into_iter().collect())
    }

    /// Generate a terminal or non-terminal ExecutionStatus.
    fn arb_status() -> impl Strategy<Value = ExecutionStatus> {
        prop_oneof![
            Just(ExecutionStatus::Queued),
            Just(ExecutionStatus::Pending),
            Just(ExecutionStatus::InProgress),
            Just(ExecutionStatus::Success),
            Just(ExecutionStatus::Failure),
            Just(ExecutionStatus::Cancelled),
            Just(ExecutionStatus::Skipped),
        ]
    }

    /// Generate PipelineContextData at bounded depth.
    fn arb_context_data(depth: u32) -> impl Strategy<Value = azdo::PipelineContextData> {
        let leaf = prop_oneof![
            "[a-zA-Z0-9 _-]{0,16}".prop_map(azdo::PipelineContextData::String),
            any::<bool>().prop_map(azdo::PipelineContextData::Bool),
            any::<f64>()
                .prop_filter("finite", |f| f.is_finite())
                .prop_map(azdo::PipelineContextData::Number),
        ];
        if depth == 0 {
            leaf.boxed()
        } else {
            prop_oneof![
                leaf.clone(),
                pvec(arb_context_data(depth - 1), 0..=4).prop_map(azdo::PipelineContextData::Array),
                btree_map("[a-z]{1,6}".boxed(), arb_context_data(depth - 1), 0..=4)
                    .prop_map(azdo::PipelineContextData::Dict),
            ]
            .boxed()
        }
    }

    // ---- property tests ----

    proptest! {
        /// GH-GROUP-01: Case variants produce equal keys.
        /// concurrency_key lowercases both repo and group, so any casing of the
        /// same ASCII string must map to the same key.
        #[test]
        fn gh_group_01_case_insensitive(
            repo in arb_name(),
            group in arb_name(),
        ) {
            let key_lower = concurrency_key(&repo, &group);
            let key_upper = concurrency_key(&repo.to_uppercase(), &group.to_uppercase());
            let key_mixed = concurrency_key(
                &repo.chars().enumerate().map(|(i, c)| {
                    if i % 2 == 0 { c.to_uppercase().next().unwrap() }
                    else { c.to_lowercase().next().unwrap() }
                }).collect::<String>(),
                &group,
            );
            prop_assert_eq!(&key_lower, &key_upper,
                "GH-GROUP-01: upper/lower must produce equal keys");
            prop_assert_eq!(&key_lower, &key_mixed,
                "GH-GROUP-01: mixed case must produce equal key");
        }

        /// GH-GROUP-01: Normalization is idempotent — applying concurrency_key
        /// to already-lowered output returns the same pair.
        #[test]
        fn gh_group_01_idempotent(
            repo in arb_name(),
            group in arb_name(),
        ) {
            let (r1, g1) = concurrency_key(&repo, &group);
            let (r2, g2) = concurrency_key(&r1, &g1);
            prop_assert_eq!((&r1, &g1), (&r2, &g2),
                "GH-GROUP-01: normalization must be idempotent");
        }

        /// GH-GROUP-01: Different repositories keep keys distinct even when
        /// group names match (case-insensitively).
        #[test]
        fn gh_group_01_repo_isolation(
            repo_a in arb_name(),
            repo_b in arb_name(),
            group in arb_name(),
        ) {
            let key_a = concurrency_key(&repo_a, &group);
            let key_b = concurrency_key(&repo_b, &group);
            // Keys equal iff repos are equal after lowering.
            let repos_equal = repo_a.to_ascii_lowercase() == repo_b.to_ascii_lowercase();
            prop_assert_eq!(key_a == key_b, repos_equal,
                "GH-GROUP-01: keys must differ iff repos differ (case-insensitive)");
        }

        /// GH-SINGLE-01: Single mode cancels every existing pending holder,
        /// never cancels the arrival, and parks the arrival.
        #[test]
        fn gh_single_01_cancel_all_pending_park_arrival(
            pending in arb_pending(10),
        ) {
            let result = apply_queue_mode(ConcurrencyQueue::Single, &pending);

            // Never cancels arrival
            prop_assert!(!result.cancel_arrival,
                "GH-SINGLE-01: arrival must never be cancelled in single mode");

            // Always parks arrival
            prop_assert!(result.park_arrival,
                "GH-SINGLE-01: arrival must always be parked in single mode");

            // Cancels exactly all existing pending holders
            prop_assert_eq!(result.cancel_pending.len(), pending.len(),
                "GH-SINGLE-01: must cancel all {} existing pending holders", pending.len());

            // Cancel set matches pending contents and order
            let expected: Vec<Holder> = pending.iter().cloned().collect();
            prop_assert_eq!(&result.cancel_pending, &expected,
                "GH-SINGLE-01: cancelled holders must match existing pending in order");
        }

        /// GH-MAX-01: Lengths 0..99 park the arrival without cancellation.
        #[test]
        fn gh_max_01_under_limit_parks(
            len in 0..100usize,
        ) {
            let pending: VecDeque<Holder> = (0..len as u32)
                .map(|n| Holder::Run(run_id_from(n)))
                .collect();
            let result = apply_queue_mode(ConcurrencyQueue::Max, &pending);

            prop_assert!(!result.cancel_arrival,
                "GH-MAX-01: arrival must not be cancelled at len={len}");
            prop_assert!(result.park_arrival,
                "GH-MAX-01: arrival must be parked at len={len}");
            prop_assert!(result.cancel_pending.is_empty(),
                "GH-MAX-01: no existing pending should be cancelled at len={len}");
        }

        /// GH-MAX-01: At exactly 100 pending, arrival is cancelled (overflow).
        #[test]
        fn gh_max_01_at_limit_cancels_arrival(
            extra in 0..6usize,
        ) {
            let len = QUEUE_MAX_PENDING + extra; // 100, 101, 102, 103, 104, 105
            let pending: VecDeque<Holder> = (0..len as u32)
                .map(|n| Holder::Run(run_id_from(n)))
                .collect();
            let result = apply_queue_mode(ConcurrencyQueue::Max, &pending);

            prop_assert!(result.cancel_arrival,
                "GH-MAX-01: arrival must be cancelled at len={len}");
            prop_assert!(!result.park_arrival,
                "GH-MAX-01: arrival must not be parked at len={len}");
            prop_assert!(result.cancel_pending.is_empty(),
                "GH-MAX-01: existing queue must not be mutated at len={len}");
        }

        /// GH-MAX-01: Boundary test at exactly 99/100/101 — the three critical
        /// values around QUEUE_MAX_PENDING.
        #[test]
        fn gh_max_01_boundary_99_100_101(
            boundary in prop_oneof![Just(99usize), Just(100usize), Just(101usize)],
        ) {
            let pending: VecDeque<Holder> = (0..boundary as u32)
                .map(|n| Holder::Run(run_id_from(n)))
                .collect();
            let result = apply_queue_mode(ConcurrencyQueue::Max, &pending);

            match boundary {
                99 => {
                    prop_assert!(!result.cancel_arrival,
                        "GH-MAX-01: 99 pending must park arrival");
                    prop_assert!(result.park_arrival);
                }
                100 | 101 => {
                    prop_assert!(result.cancel_arrival,
                        "GH-MAX-01: {boundary} pending must cancel arrival");
                    prop_assert!(!result.park_arrival);
                }
                _ => unreachable!(),
            }
        }

        /// Holder terminality: holder_is_terminal agrees with an independently
        /// computed all-terminal check over the holder's job members.
        #[test]
        fn holder_terminality_agrees_with_manual_check(
            run_n in 0..50u32,
            job_statuses in btree_map(0..10u32, arb_status(), 1..=8),
        ) {
            let jobs: BTreeMap<JobId, ExecutionStatus> = job_statuses
                .iter()
                .map(|(k, v)| (job_id_from(*k), *v))
                .collect();

            // Test Run holder — terminal iff ALL jobs in the map are terminal
            let run_holder = Holder::Run(run_id_from(run_n));
            let run_expected = jobs.values().all(|s| is_terminal(*s));
            prop_assert_eq!(
                holder_is_terminal(&run_holder, &jobs),
                run_expected,
                "Run holder terminality must equal all-jobs-terminal"
            );

            // Test Job holder — terminal iff that specific job is terminal
            if let Some((&k, &status)) = job_statuses.iter().next() {
                let jid = job_id_from(k);
                let job_holder = Holder::Job {
                    run_id: run_id_from(run_n),
                    job_id: jid.clone(),
                };
                let job_expected = is_terminal(status);
                prop_assert_eq!(
                    holder_is_terminal(&job_holder, &jobs),
                    job_expected,
                    "Job holder terminality must equal is_terminal(job_status)"
                );
            }

            // Test JobSet holder — terminal iff all members are terminal
            let member_ids: BTreeSet<JobId> = job_statuses.keys().map(|k| job_id_from(*k)).collect();
            let jobset_holder = Holder::JobSet {
                run_id: run_id_from(run_n),
                job_ids: member_ids.clone(),
            };
            let jobset_expected = member_ids.iter().all(|id| {
                jobs.get(id).copied().is_some_and(|s| is_terminal(s))
            });
            prop_assert_eq!(
                holder_is_terminal(&jobset_holder, &jobs),
                jobset_expected,
                "JobSet holder terminality must equal all-members-terminal"
            );
        }

        /// Holder terminality: a missing job in the status map makes the holder
        /// non-terminal (the is_some_and check returns false).
        #[test]
        fn holder_terminality_missing_job_is_non_terminal(
            run_n in 0..50u32,
            present_id in 0..10u32,
            missing_id in 10..20u32,
        ) {
            let mut jobs = BTreeMap::new();
            jobs.insert(job_id_from(present_id), ExecutionStatus::Success);

            // Single Job holder referencing the missing ID
            let holder = Holder::Job {
                run_id: run_id_from(run_n),
                job_id: job_id_from(missing_id),
            };
            prop_assert!(!holder_is_terminal(&holder, &jobs),
                "A Job holder for a missing job ID must be non-terminal");

            // JobSet containing a missing member
            let mut member_ids = BTreeSet::new();
            member_ids.insert(job_id_from(present_id));
            member_ids.insert(job_id_from(missing_id));
            let jobset = Holder::JobSet {
                run_id: run_id_from(run_n),
                job_ids: member_ids,
            };
            prop_assert!(!holder_is_terminal(&jobset, &jobs),
                "A JobSet with a missing member must be non-terminal");
        }

        /// context_data_to_json preserves shape: scalar/list/dict structure
        /// round-trips through the conversion with the correct JSON type.
        #[test]
        fn context_data_shape_preserved(data in arb_context_data(3)) {
            let json = context_data_to_json(&data);
            assert_shape_matches(&data, &json);
        }

        /// context_data_to_json: String values are preserved exactly.
        #[test]
        fn context_data_string_exact(s in "[a-zA-Z0-9 _-]{0,32}") {
            let data = azdo::PipelineContextData::String(s.clone());
            let json = context_data_to_json(&data);
            prop_assert_eq!(json, Value::String(s));
        }

        /// context_data_to_json: Bool values are preserved.
        #[test]
        fn context_data_bool_exact(b in any::<bool>()) {
            let data = azdo::PipelineContextData::Bool(b);
            let json = context_data_to_json(&data);
            prop_assert_eq!(json, Value::Bool(b));
        }

        /// context_data_to_json: Dict key set is preserved — every key in the
        /// input appears in the output object and vice versa.
        #[test]
        fn context_data_dict_keys_preserved(
            map in btree_map(
                "[a-z]{1,8}",
                "[a-zA-Z0-9]{0,8}".prop_map(azdo::PipelineContextData::String),
                0..=6
            )
        ) {
            let data = azdo::PipelineContextData::Dict(map.clone());
            let json = context_data_to_json(&data);
            if let Value::Object(obj) = json {
                let input_keys: BTreeSet<&String> = map.keys().collect();
                let output_keys: BTreeSet<&String> = obj.keys().collect();
                prop_assert_eq!(input_keys, output_keys,
                    "Dict key set must be preserved exactly");
            } else {
                prop_assert!(false, "Dict must produce JSON Object");
            }
        }

        /// context_data_to_json: Array length is preserved.
        #[test]
        fn context_data_array_len_preserved(
            items in pvec(
                any::<bool>().prop_map(azdo::PipelineContextData::Bool),
                0..=10
            )
        ) {
            let data = azdo::PipelineContextData::Array(items.clone());
            let json = context_data_to_json(&data);
            if let Value::Array(arr) = json {
                prop_assert_eq!(arr.len(), items.len(),
                    "Array length must be preserved");
            } else {
                prop_assert!(false, "Array must produce JSON Array");
            }
        }
    }

    /// Recursively verify that PipelineContextData shape matches JSON shape.
    fn assert_shape_matches(data: &azdo::PipelineContextData, json: &Value) {
        match data {
            azdo::PipelineContextData::String(s) => {
                assert_eq!(json, &Value::String(s.clone()), "String value mismatch");
            }
            azdo::PipelineContextData::Bool(b) => {
                assert_eq!(json, &Value::Bool(*b), "Bool value mismatch");
            }
            azdo::PipelineContextData::Number(n) => {
                assert!(json.is_number(), "Number must produce JSON number");
                assert_eq!(json.as_f64().unwrap(), *n, "Number value mismatch");
            }
            azdo::PipelineContextData::Array(items) => {
                let arr = json.as_array().expect("Array must produce JSON Array");
                assert_eq!(arr.len(), items.len(), "Array length mismatch");
                for (item, jval) in items.iter().zip(arr.iter()) {
                    assert_shape_matches(item, jval);
                }
            }
            azdo::PipelineContextData::Dict(map) => {
                let obj = json.as_object().expect("Dict must produce JSON Object");
                assert_eq!(obj.len(), map.len(), "Dict size mismatch");
                for (k, v) in map {
                    let jval = obj.get(k).unwrap_or_else(|| panic!("missing key {k}"));
                    assert_shape_matches(v, jval);
                }
            }
        }
    }
}
