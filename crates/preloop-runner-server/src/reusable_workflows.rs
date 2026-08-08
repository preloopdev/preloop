use super::*;

/// Fold completed callee subtrees into their caller nodes: compute the
/// caller's recorded `workflow_call` outputs and flip the caller node to its
/// aggregate status. Returns the caller ids that became terminal in this pass
/// so `complete_job_inner` can release their JobSet concurrency gates.
pub(crate) fn propagate_reusable_outputs(run: &mut RunRecord) -> Vec<JobId> {
    let mut finalized = Vec::new();
    // A single pass can only fold one nesting level: an ancestor caller's
    // `all_complete` check reads the *recorded* status of its callee, and a
    // callee that is itself a caller only becomes terminal in the same pass it
    // is folded (the status writes land at the end of the pass). Nested
    // reusable workflows (outer -> mid -> leaf) therefore need repeated passes:
    // without them the ancestor stays InProgress forever after the deepest job
    // finishes, because no further job completion ever re-triggers the fold,
    // and its JobSet gate never releases. Repeat until a pass changes nothing —
    // each pass terminalizes at least one caller, so this terminates.
    loop {
        let pass = propagate_reusable_outputs_once(run);
        let changed = !pass.is_empty();
        finalized.extend(pass);
        if !changed {
            return finalized;
        }
    }
}

fn propagate_reusable_outputs_once(run: &mut RunRecord) -> Vec<JobId> {
    let mut outputs_to_add = Vec::new();
    let mut statuses_to_set = Vec::new();
    for (caller_job_id, call) in &run.reusable_calls {
        let caller_job_id_typed = JobId(caller_job_id.clone());
        if run.job_outputs.contains_key(&caller_job_id_typed) {
            continue;
        }

        // Check if all inner jobs are complete
        let all_complete = !call.inner_job_ids.is_empty()
            && call.inner_job_ids.iter().all(|id| {
                run.jobs.get(&JobId(id.clone())).is_some_and(|status| {
                    matches!(
                        status,
                        ExecutionStatus::Success
                            | ExecutionStatus::Failure
                            | ExecutionStatus::Skipped
                            | ExecutionStatus::Cancelled
                    )
                })
            });

        if all_complete {
            // Build expression context
            let mut jobs_map = serde_json::Map::new();
            for inner_id in &call.inner_job_ids {
                let prefix = format!("{}/", caller_job_id);
                let inner_id_without_prefix = if inner_id.starts_with(&prefix) {
                    &inner_id[prefix.len()..]
                } else {
                    inner_id
                };

                let mut job_outputs_map = serde_json::Map::new();
                if let Some(outputs) = run.job_outputs.get(&JobId(inner_id.clone())) {
                    for (k, v) in outputs {
                        job_outputs_map.insert(k.clone(), v.clone());
                    }
                }

                let mut job_record = serde_json::Map::new();
                job_record.insert(
                    "outputs".to_owned(),
                    serde_json::Value::Object(job_outputs_map),
                );
                jobs_map.insert(
                    inner_id_without_prefix.to_owned(),
                    serde_json::Value::Object(job_record),
                );
            }

            let mut context = preloop_gha_expressions::Context::default();
            context.insert("jobs", serde_json::Value::Object(jobs_map));

            let mut inputs_map = serde_json::Map::new();
            for (k, v) in &call.inputs {
                inputs_map.insert(k.clone(), v.clone());
            }
            context.insert("inputs", serde_json::Value::Object(inputs_map));

            let mut caller_outputs = BTreeMap::new();
            for (name, expr) in &call.output_definitions {
                let resolved = preloop_gha_parser::eval::resolve_string(expr, &context)
                    .unwrap_or_else(|_| expr.clone());
                // Outputs are strings on GitHub: every `GITHUB_OUTPUT` value
                // is text, and `needs.<caller>.outputs.<name>` comparisons
                // (`== 'true'`) are string comparisons. JSON-parsing here
                // would turn "true" into a boolean and silently flip gates
                // that compare against string literals.
                caller_outputs.insert(name.clone(), serde_json::Value::String(resolved));
            }

            outputs_to_add.push((caller_job_id_typed, caller_outputs));

            // The caller entry itself becomes the aggregate of its subtree
            // once GitHub would have materialized it. A caller already
            // terminal (cancelled mid-flight) keeps its conclusion.
            let caller_status = run.jobs.get(&JobId(caller_job_id.clone())).copied();
            if caller_status.is_some_and(|status| !status.is_terminal()) {
                let statuses: Vec<ExecutionStatus> = call
                    .inner_job_ids
                    .iter()
                    .filter_map(|id| run.jobs.get(&JobId(id.clone())).copied())
                    .collect();
                let aggregate =
                    aggregate_need_status(&statuses).unwrap_or(ExecutionStatus::Skipped);
                statuses_to_set.push((JobId(caller_job_id.clone()), aggregate));
            }
        }
    }

    let mut finalized = Vec::new();
    for (job_id, outputs) in outputs_to_add {
        run.job_outputs.insert(job_id, outputs);
    }
    for (job_id, status) in statuses_to_set {
        run.jobs.insert(job_id.clone(), status);
        finalized.push(job_id);
    }
    finalized
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caller_meta(
        caller: &str,
        inner_job_ids: Vec<&str>,
    ) -> preloop_gha_parser::ReusableCallMetadata {
        preloop_gha_parser::ReusableCallMetadata {
            caller_job_id: caller.to_owned(),
            output_definitions: BTreeMap::new(),
            inner_job_ids: inner_job_ids.into_iter().map(str::to_owned).collect(),
            inputs: BTreeMap::new(),
            caller_concurrency: None,
            embedded_concurrency: None,
            matrix: BTreeMap::new(),
            if_condition: None,
            workflow_sha: None,
            workflow_repository: None,
        }
    }

    fn nested_run() -> RunRecord {
        let mut jobs = BTreeMap::new();
        jobs.insert(
            JobId("outer/call/inner/leaf".to_owned()),
            ExecutionStatus::Success,
        );
        jobs.insert(
            JobId("outer/call/inner".to_owned()),
            ExecutionStatus::InProgress,
        );
        jobs.insert(JobId("outer/call".to_owned()), ExecutionStatus::InProgress);
        RunRecord {
            run_id: RunId::new(),
            run_name: None,
            submission: Arc::new(WorkflowSubmission::default()),
            jobs,
            status: ExecutionStatus::InProgress,
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
            reusable_calls: BTreeMap::from([
                (
                    "outer/call".to_owned(),
                    caller_meta("outer/call", vec!["outer/call/inner"]),
                ),
                (
                    "outer/call/inner".to_owned(),
                    caller_meta("outer/call/inner", vec!["outer/call/inner/leaf"]),
                ),
            ]),
            jobs_list: Vec::new(),
            created_at: chrono::Utc::now(),
            started_at: None,
            completed_at: None,
            run_number: 1,
            run_attempt: 1,
            workflow_path_str: ".github/workflows/workflow.yml".to_owned(),
            event: "push".to_owned(),
            conclusion: None,
            push_state: None,
        }
    }

    #[test]
    fn nested_reusable_callers_all_terminalize_in_one_fold() {
        // outer -> mid -> leaf: the leaf is the only real job, and it completes
        // while both callers are still InProgress. GitHub's fold must terminalize
        // BOTH callers from that single completion — the mid caller only becomes
        // terminal during the same pass, so the outer caller's all_complete check
        // cannot see it without a second pass. A one-pass fold leaves the outer
        // caller (and therefore the whole run) InProgress forever.
        let mut run = nested_run();
        let finalized = propagate_reusable_outputs(&mut run);

        assert!(
            finalized.contains(&JobId("outer/call/inner".to_owned())),
            "mid caller must terminalize: {finalized:?}"
        );
        assert!(
            finalized.contains(&JobId("outer/call".to_owned())),
            "ancestor caller must terminalize in the same fold: {finalized:?}"
        );
        assert_eq!(
            run.jobs[&JobId("outer/call".to_owned())],
            ExecutionStatus::Success
        );
        assert!(
            run.job_outputs
                .contains_key(&JobId("outer/call".to_owned())),
            "ancestor outputs must be recorded"
        );
    }
}
