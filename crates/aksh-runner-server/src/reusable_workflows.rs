use super::*;

/// Fold completed callee subtrees into their caller nodes: compute the
/// caller's recorded `workflow_call` outputs and flip the caller node to its
/// aggregate status. Returns the caller ids that became terminal in this pass
/// so `complete_job_inner` can release their JobSet concurrency gates.
pub(crate) fn propagate_reusable_outputs(run: &mut RunRecord) -> Vec<JobId> {
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

            let mut context = aksh_gha_expressions::Context::default();
            context.insert("jobs", serde_json::Value::Object(jobs_map));

            let mut inputs_map = serde_json::Map::new();
            for (k, v) in &call.inputs {
                inputs_map.insert(k.clone(), v.clone());
            }
            context.insert("inputs", serde_json::Value::Object(inputs_map));

            let mut caller_outputs = BTreeMap::new();
            for (name, expr) in &call.output_definitions {
                let resolved = aksh_gha_parser::eval::resolve_string(expr, &context)
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
