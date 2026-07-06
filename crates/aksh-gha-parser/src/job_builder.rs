//! Build `AgentJobRequestMessage` from parsed workflow data.
//!
//! This module converts a parsed `JobPlan` + context data into the
//! `AgentJobRequestMessage` that the runner receives after decryption.

use std::collections::BTreeMap;

use aksh_gha_protocol::azdo::{
    AgentJobRequestMessage, EndpointAuthorization, MaskHint, MaskType, PipelineContextData,
    PlanReference, ServiceEndpoint, TaskResources, TaskStep, TimelineReference, VariableValue,
};

use crate::eval::{build_context, resolve_map, resolve_string};
use crate::JobPlan;
use aksh_gha_expressions::Context;
use serde_json::Value;

/// Build a full `AgentJobRequestMessage` from a job plan and context.
///
/// This resolves `${{ }}` in env/with/run fields, builds contextData,
/// materializes variables and maskHints, and produces the complete
/// job message the runner expects.
pub fn build_agent_job_message(
    plan: &JobPlan,
    github: &Value,
    global_env: &BTreeMap<String, String>,
    secrets: &BTreeMap<String, String>,
    vars: &BTreeMap<String, String>,
) -> Result<AgentJobRequestMessage, String> {
    let timeline_id = uuid::Uuid::new_v4();
    let job_id = uuid::Uuid::new_v4();

    // Build expression evaluation context
    let strategy = plan
        .matrix
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<BTreeMap<String, Value>>();
    let strategy_value = Value::Object(
        strategy
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    );

    let expr_context = build_context(
        github,
        &plan.env,
        vars,
        &plan.matrix,
        &strategy_value,
        secrets,
    );

    // Resolve expressions in step env and with.
    // Generate contextName for each step matching GitHub's wire format:
    //   - Script steps: __run, __run_2, __run_3 (separate counter)
    //   - Action steps: __<sanitized_action>, __<sanitized_action>_2 (per-action counter)
    //   - User `id:` is used verbatim when present
    let mut run_counter: usize = 0;
    let mut action_counters: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut used_names: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut steps: Vec<TaskStep> = Vec::new();
    for step in &plan.steps {
        let is_script = step.uses.is_none() && step.run.is_some();
        let context_name = if let Some(ref user_id) = step.id {
            user_id.clone()
        } else if is_script {
            run_counter += 1;
            if run_counter == 1 {
                "__run".to_string()
            } else {
                format!("__run_{run_counter}")
            }
        } else if let Some(ref uses) = step.uses {
            // Action steps: __<sanitized_action>, __<sanitized_action>_2, etc.
            let base = uses.split('@').next().unwrap_or(uses).replace('/', "_");
            let counter = action_counters.entry(base.clone()).or_insert(0);
            *counter += 1;
            if *counter == 1 {
                format!("__{base}")
            } else {
                format!("__{base}_{counter}")
            }
        } else {
            format!("__step_{}", steps.len())
        };
        // Dedupe: if a user-supplied id collides with an auto-generated name,
        // the auto-generated one gets a suffix (shouldn't happen in practice).
        let final_name = if used_names.contains(&context_name) {
            format!("{context_name}_{}", steps.len())
        } else {
            context_name
        };
        used_names.insert(final_name.clone());
        let mut task_step = build_task_step(step, &expr_context);
        task_step.context_name = Some(final_name);
        steps.push(task_step);
    }

    // Materialize variables
    let mut variables = BTreeMap::new();
    for (k, v) in &plan.env {
        variables.insert(k.clone(), VariableValue::new(v));
    }
    for (k, v) in global_env {
        variables
            .entry(k.clone())
            .or_insert_with(|| VariableValue::new(v));
    }
    for (k, v) in vars {
        variables.insert(k.clone(), VariableValue::new(v));
    }
    for (k, v) in secrets {
        variables.insert(k.clone(), VariableValue::secret(v));
    }

    // System variables
    variables.insert(
        "system.pullRequestTargetBranch".to_owned(),
        VariableValue::new(""),
    );

    // Mask hints for secrets
    let mask_hints: Vec<MaskHint> = secrets
        .values()
        .filter(|v| !v.is_empty())
        .map(|v| MaskHint {
            hint_type: MaskType::Hash,
            value: v.clone(),
        })
        .collect();

    // Service endpoints (SystemVssConnection)
    let endpoints = vec![ServiceEndpoint {
        data: BTreeMap::new(),
        name: "SystemVssConnection".to_owned(),
        endpoint_type: Some("azdoserver".to_owned()),
        url: Some("http://localhost".to_owned()),
        authorization: EndpointAuthorization {
            parameters: BTreeMap::from([(
                "AccessToken".to_owned(),
                "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.e30.ZmFrZXNpZw".to_owned(),
            )]),
            scheme: Some("OAuth".to_owned()),
        },
        is_shared: Some(false),
        service_owner: Some("github".to_owned()),
    }];

    let resources = TaskResources {
        endpoints,
        repositories: Vec::new(),
    };

    // System context — runner needs these for job tracking
    let mut system_ctx = BTreeMap::new();
    system_ctx.insert(
        "jobId".to_owned(),
        PipelineContextData::String(job_id.to_string()),
    );
    system_ctx.insert(
        "timelineId".to_owned(),
        PipelineContextData::String(timeline_id.to_string()),
    );
    system_ctx.insert(
        "planId".to_owned(),
        PipelineContextData::String(job_id.to_string()),
    );
    system_ctx.insert(
        "jobDisplayName".to_owned(),
        PipelineContextData::String(plan.name.clone()),
    );
    system_ctx.insert(
        "orchestrationId".to_owned(),
        PipelineContextData::String(job_id.to_string()),
    );

    // Context data
    let mut context_data = BTreeMap::new();
    context_data.insert("system".to_owned(), PipelineContextData::Dict(system_ctx));
    context_data.insert("github".to_owned(), to_context_data(github));

    let env_ctx: Map<String, Value> = plan
        .env
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    context_data.insert(
        "env".to_owned(),
        PipelineContextData::Dict(
            env_ctx
                .into_iter()
                .map(|(k, v)| (k, to_context_data(&v)))
                .collect(),
        ),
    );

    let matrix_ctx: Map<String, Value> = plan
        .matrix
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    context_data.insert(
        "matrix".to_owned(),
        PipelineContextData::Dict(
            matrix_ctx
                .into_iter()
                .map(|(k, v)| (k, to_context_data(&v)))
                .collect(),
        ),
    );

    context_data.insert(
        "needs".to_owned(),
        PipelineContextData::Dict(BTreeMap::new()),
    );
    context_data.insert("strategy".to_owned(), to_context_data(&strategy_value));
    context_data.insert(
        "vars".to_owned(),
        PipelineContextData::Dict(
            vars.iter()
                .map(|(k, v)| (k.clone(), PipelineContextData::String(v.clone())))
                .collect(),
        ),
    );

    // Actions download info
    let actions_download_info = BTreeMap::new();

    let request_id: i64 = 1;

    Ok(AgentJobRequestMessage {
        message_type: None,
        job_id,
        request_id,
        plan: PlanReference {
            plan_id: job_id.to_string(),
            plan_type: Some("Job".to_owned()),
        },
        timeline: TimelineReference { id: timeline_id },
        display_name: Some(plan.name.clone()),
        condition: plan.if_condition.clone(),
        variables,
        mask_hints,
        resources,
        context_data,
        steps,
        actions_download_info,
        job_display_name: Some(plan.name.clone()),
        retry_count: None,
        pre_job_timeout: None,
        job_timeout: None,
        job_container: plan.container.clone(),
        job_service_containers: non_empty_services(plan.services.clone()),
    })
}

/// Omit empty `services: {}` to match `EmitDefaultValue=false` behavior.
fn non_empty_services(services: Option<serde_json::Value>) -> Option<serde_json::Value> {
    match &services {
        Some(serde_json::Value::Object(m)) if m.is_empty() => None,
        _ => services,
    }
}

/// Build a `TaskStep` from a `StepPlan`.
fn build_task_step(step: &crate::StepPlan, context: &Context) -> TaskStep {
    let step_id = uuid::Uuid::new_v4();

    // Resolve expressions in env and with
    let env = resolve_map(&step.env, context).unwrap_or_else(|_| step.env.clone());
    let with: BTreeMap<String, String> = step
        .with
        .iter()
        .map(|(k, v)| {
            let input = step_input_to_string(v);
            let resolved = resolve_string(&input, context).unwrap_or(input);
            (k.clone(), resolved)
        })
        .collect();

    // Resolve expressions in run script
    let run = step
        .run
        .as_ref()
        .map(|r| resolve_string(r, context).unwrap_or_else(|_| r.clone()));

    // The runner always evaluates a step condition. Omitted conditions are
    // the same as GitHub's default `success()`.
    let condition = Some(
        step.if_condition
            .clone()
            .unwrap_or_else(|| "success()".to_owned()),
    );

    TaskStep {
        id: step_id,
        name: step.name.clone(),
        context_name: None, // Set by caller after construction
        display_name: step.name.clone(),
        condition,
        script: run,
        reference: step
            .uses
            .as_ref()
            .map(|uses| aksh_gha_protocol::azdo::TaskReference {
                id: None,
                name: Some(uses.clone()),
                version: None,
                reference_type: None,
            }),
        inputs: with,
        env,
        continue_on_error: step.continue_on_error,
        working_directory: step
            .working_directory
            .as_ref()
            .map(|wd| resolve_string(wd, context).unwrap_or_else(|_| wd.clone())),
        timeout_in_minutes: None,
    }
}

fn step_input_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

/// Convert a `serde_json::Value` to `PipelineContextData`.
fn to_context_data(value: &Value) -> PipelineContextData {
    match value {
        Value::String(s) => PipelineContextData::String(s.clone()),
        Value::Bool(b) => PipelineContextData::Bool(*b),
        Value::Number(n) => PipelineContextData::Number(n.as_f64().unwrap_or(0.0)),
        Value::Array(arr) => PipelineContextData::Array(arr.iter().map(to_context_data).collect()),
        Value::Object(map) => PipelineContextData::Dict(
            map.iter()
                .map(|(k, v)| (k.clone(), to_context_data(v)))
                .collect(),
        ),
        Value::Null => PipelineContextData::String(String::new()),
    }
}

use serde_json::Map;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_workflow, Workflow};

    fn simple_workflow() -> Workflow {
        let yaml = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hello
"#;
        parse_workflow(yaml).unwrap()
    }

    #[test]
    fn build_message_from_simple_workflow() {
        let workflow = simple_workflow();
        let plans = crate::expand_jobs(&workflow).unwrap();
        let plan = &plans[0];

        let github = serde_json::json!({
            "event_name": "push",
            "ref": "refs/heads/main",
            "sha": "abc123"
        });

        let msg = build_agent_job_message(
            plan,
            &github,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();

        assert!(!msg.steps.is_empty());
        assert_eq!(msg.steps[0].condition.as_deref(), Some("success()"));
        assert!(msg.timeline.id != uuid::Uuid::nil());
        assert!(msg.job_id != uuid::Uuid::nil());
        assert!(msg
            .resources
            .endpoints
            .iter()
            .any(|e| e.name == "SystemVssConnection"));
    }

    #[test]
    fn build_message_with_matrix() {
        let yaml = r#"
on: push
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
    steps:
      - run: echo ${{ matrix.os }}
"#;
        let workflow = parse_workflow(yaml).unwrap();
        let plans = crate::expand_jobs(&workflow).unwrap();
        assert_eq!(plans.len(), 2);

        let github = serde_json::json!({"event_name": "push"});
        let msg = build_agent_job_message(
            &plans[0],
            &github,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();

        // Matrix should be in context data
        assert!(msg.context_data.contains_key("matrix"));
    }

    #[test]
    fn string_with_inputs_are_not_json_quoted() {
        let yaml = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/cache@v4
        with:
          path: target
          fail-on-cache-miss: true
"#;
        let workflow = parse_workflow(yaml).unwrap();
        let plans = crate::expand_jobs(&workflow).unwrap();
        let github = serde_json::json!({"event_name": "push"});

        let msg = build_agent_job_message(
            &plans[0],
            &github,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(msg.steps[0].inputs.get("path"), Some(&"target".to_owned()));
        assert_eq!(
            msg.steps[0].inputs.get("fail-on-cache-miss"),
            Some(&"true".to_owned())
        );
    }

    #[test]
    fn secrets_become_variables_and_mask_hints() {
        let yaml = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo secret
"#;
        let workflow = parse_workflow(yaml).unwrap();
        let plans = crate::expand_jobs(&workflow).unwrap();

        let mut secrets = BTreeMap::new();
        secrets.insert("MY_SECRET".to_owned(), "s3cr3t".to_owned());

        let github = serde_json::json!({"event_name": "push"});
        let msg = build_agent_job_message(
            &plans[0],
            &github,
            &BTreeMap::new(),
            &secrets,
            &BTreeMap::new(),
        )
        .unwrap();

        assert!(!msg.mask_hints.is_empty());
        assert_eq!(msg.mask_hints[0].value, "s3cr3t");
        let secret = msg.variables.get("MY_SECRET").unwrap();
        assert_eq!(secret.value.as_deref(), Some("s3cr3t"));
        assert_eq!(secret.is_secret, Some(true));
    }
    #[test]
    fn workflow_dispatch_inputs_are_in_event_context() {
        let yaml = r#"
on: workflow_dispatch
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo ${{ github.event.inputs.greeting }}
"#;
        let workflow = parse_workflow(yaml).unwrap();
        let plans = crate::expand_jobs(&workflow).unwrap();

        let github = serde_json::json!({
            "event_name": "workflow_dispatch",
            "event": {
                "inputs": {
                    "greeting": "hello world"
                }
            },
            "ref": "refs/heads/main"
        });

        let msg = build_agent_job_message(
            &plans[0],
            &github,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();

        assert!(!msg.steps.is_empty());
    }
}
