//! Build `AgentJobRequestMessage` from parsed workflow data.
//!
//! This module converts a parsed `JobPlan` + context data into the
//! `AgentJobRequestMessage` that the runner receives after decryption.

use std::collections::BTreeMap;

use aksh_gha_protocol::azdo::{
    AgentJobRequestMessage, EndpointAuthorization, MaskHint, MaskType,
    PipelineContextData, ServiceEndpoint, TaskResources, TaskStep,
    TimelineReference, VariableValue,
};

use crate::JobPlan;
use crate::eval::{build_context, resolve_map, resolve_string};
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

    // Resolve expressions in step env and with
    let steps: Vec<TaskStep> = plan
        .steps
        .iter()
        .map(|step| build_task_step(step, &expr_context))
        .collect();

    // Materialize variables
    let mut variables = BTreeMap::new();
    for (k, v) in &plan.env {
        variables.insert(k.clone(), VariableValue::new(v));
    }
    for (k, v) in global_env {
        variables.entry(k.clone()).or_insert_with(|| VariableValue::new(v));
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
    let mut endpoints = Vec::new();
    endpoints.push(ServiceEndpoint {
        data: BTreeMap::new(),
        name: "SystemVssConnection".to_owned(),
        endpoint_type: Some("azdoserver".to_owned()),
        url: Some("http://localhost".to_owned()),
        authorization: EndpointAuthorization {
            parameters: BTreeMap::from([(
                "AccessToken".to_owned(),
                "aksh-system-token".to_owned(),
            )]),
            scheme: Some("OAuth".to_owned()),
        },
        is_shared: Some(false),
        service_owner: Some("github".to_owned()),
    });

    let resources = TaskResources {
        endpoints,
        repositories: Vec::new(),
    };

    // System context — runner needs these for job tracking
    let mut system_ctx = BTreeMap::new();
    system_ctx.insert("jobId".to_owned(), PipelineContextData::String(job_id.to_string()));
    system_ctx.insert("timelineId".to_owned(), PipelineContextData::String(timeline_id.to_string()));
    system_ctx.insert("planId".to_owned(), PipelineContextData::String(job_id.to_string()));
    system_ctx.insert("jobDisplayName".to_owned(), PipelineContextData::String(plan.name.clone()));
    system_ctx.insert("orchestrationId".to_owned(), PipelineContextData::String(job_id.to_string()));

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
    context_data.insert(
        "strategy".to_owned(),
        to_context_data(&strategy_value),
    );

    // Actions download info
    let actions_download_info = BTreeMap::new();

    Ok(AgentJobRequestMessage {
        job_id,
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
    })
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

    // Resolve if condition (pass through as expression string)
    let condition = step.if_condition.clone();

    TaskStep {
        id: step_id,
        name: step.name.clone(),
        display_name: step.name.clone(),
        condition,
        script: run,
        reference: step.uses.as_ref().map(|uses| {
            aksh_gha_protocol::azdo::TaskReference {
                id: None,
                name: Some(uses.clone()),
                version: None,
                reference_type: None,
            }
        }),
        inputs: with,
        env,
        continue_on_error: None,
        working_directory: None,
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

        let msg = build_agent_job_message(plan, &github, &BTreeMap::new(), &BTreeMap::new(), &BTreeMap::new())
            .unwrap();

        assert!(!msg.steps.is_empty());
        assert!(msg.timeline.id != uuid::Uuid::nil());
        assert!(msg.job_id != uuid::Uuid::nil());
        assert!(msg.resources.endpoints.iter().any(|e| e.name == "SystemVssConnection"));
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
        let msg = build_agent_job_message(&plans[0], &github, &BTreeMap::new(), &BTreeMap::new(), &BTreeMap::new())
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
        let msg = build_agent_job_message(&plans[0], &github, &BTreeMap::new(), &secrets, &BTreeMap::new())
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
