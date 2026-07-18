use super::*;
use base64::Engine;
use proptest::prelude::*;
use proptest::test_runner::{FileFailurePersistence, RngSeed};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

fn codec_config() -> ProptestConfig {
    let mut config = ProptestConfig::with_failure_persistence(
        FileFailurePersistence::SourceParallel("proptest-regressions"),
    );
    config.cases = 1_000;
    config.rng_seed = RngSeed::Fixed(0xA2D0_2026);
    config.verbose = 1;
    config
}

fn arb_key() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[a-z][a-z0-9_]{0,7}").unwrap()
}

fn arb_text() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[A-Za-z0-9 _./:'-]{0,16}").unwrap()
}
fn arb_expression() -> impl Strategy<Value = String> {
    proptest::string::string_regex("[A-Za-z0-9_. ]{0,16}").unwrap()
}

fn arb_non_script_inputs() -> impl Strategy<Value = BTreeMap<String, String>> {
    proptest::collection::btree_map(
        prop::sample::select(vec![
            "input_a".to_owned(),
            "input_b".to_owned(),
            "shell".to_owned(),
        ]),
        arb_text(),
        0..=3,
    )
}

fn expected_template_token(value: &str) -> Value {
    let mut token = if let Some(expression) = value
        .strip_prefix("${{")
        .and_then(|rest| rest.strip_suffix("}}"))
    {
        json!({"type": 3, "expr": expression.trim()})
    } else {
        json!({"type": 0, "lit": value})
    };
    token["file"] = json!(1);
    token["line"] = json!(0);
    token["col"] = json!(0);
    token
}

fn expected_template_map(values: &BTreeMap<String, String>) -> Value {
    let pairs: Vec<Value> = values
        .iter()
        .map(|(key, value)| {
            json!({
                "Key": {"type": 0, "lit": key},
                "Value": expected_template_token(value),
            })
        })
        .collect();
    if pairs.is_empty() {
        json!({"type": 2})
    } else {
        json!({"type": 2, "map": pairs})
    }
}
// Independent AgentJobRequest oracle derived from the official v2.335.1
// wire contract and the fields modeled by this DTO.
fn expected_variable_wire(value: &VariableValue) -> Value {
    let mut object = Map::new();
    if let Some(value) = &value.value {
        object.insert("value".to_owned(), json!(value));
    }
    if let Some(is_secret) = value.is_secret {
        object.insert("isSecret".to_owned(), json!(is_secret));
    }
    Value::Object(object)
}

fn expected_endpoint_wire(endpoint: &ServiceEndpoint) -> Value {
    let mut object = Map::new();
    object.insert("data".to_owned(), json!(endpoint.data));
    object.insert("name".to_owned(), json!(endpoint.name));
    if let Some(value) = &endpoint.endpoint_type {
        object.insert("type".to_owned(), json!(value));
    }
    if let Some(value) = &endpoint.service_owner {
        object.insert("serviceOwner".to_owned(), json!(value));
    }
    if let Some(value) = &endpoint.url {
        object.insert("url".to_owned(), json!(value));
    }
    let mut authorization = Map::new();
    authorization.insert(
        "parameters".to_owned(),
        json!(endpoint.authorization.parameters),
    );
    if let Some(value) = &endpoint.authorization.scheme {
        authorization.insert("scheme".to_owned(), json!(value));
    }
    object.insert("authorization".to_owned(), Value::Object(authorization));
    if let Some(value) = endpoint.is_shared {
        object.insert("isShared".to_owned(), json!(value));
    }
    if let Some(value) = endpoint.is_ready {
        object.insert("isReady".to_owned(), json!(value));
    }
    Value::Object(object)
}

fn expected_repository_wire(repository: &RepositoryReference) -> Value {
    let mut object = Map::new();
    if let Some(value) = &repository.repository {
        object.insert("repository".to_owned(), json!(value));
    }
    if let Some(value) = &repository.git_ref {
        object.insert("ref".to_owned(), json!(value));
    }
    if let Some(connector) = &repository.connector {
        let mut connector_wire = Map::new();
        if let Some(value) = &connector.id {
            connector_wire.insert("id".to_owned(), json!(value));
        }
        if let Some(value) = &connector.name {
            connector_wire.insert("name".to_owned(), json!(value));
        }
        object.insert("connector".to_owned(), Value::Object(connector_wire));
    }
    Value::Object(object)
}

fn expected_step_wire(step: &TaskStep) -> Value {
    let mut inputs = step.inputs.clone();
    if let Some(script) = &step.script {
        inputs.insert("script".to_owned(), script.clone());
    }
    let reference = match &step.reference {
        None => json!({"type": "script"}),
        Some(reference) => {
            let mut object = Map::new();
            object.insert(
                "type".to_owned(),
                json!(reference.reference_type.as_deref().unwrap_or("repository")),
            );
            if let Some(value) = &reference.name {
                object.insert("name".to_owned(), json!(value));
            }
            if let Some(value) = &reference.version {
                object.insert("ref".to_owned(), json!(value));
            }
            if reference.reference_type.is_none() {
                object.insert("repositoryType".to_owned(), json!("GitHub"));
            }
            Value::Object(object)
        }
    };
    let mut object = Map::new();
    object.insert("type".to_owned(), json!("action"));
    object.insert("reference".to_owned(), reference);
    if !step.env.is_empty() {
        object.insert("environment".to_owned(), expected_template_map(&step.env));
    }
    object.insert("inputs".to_owned(), expected_template_map(&inputs));
    object.insert("id".to_owned(), json!(step.id));
    object.insert(
        "name".to_owned(),
        json!(step.context_name.as_ref().or(step.name.as_ref())),
    );
    if let Some(value) = &step.context_name {
        object.insert("contextName".to_owned(), json!(value));
    }
    if let Some(value) = &step.display_name_token {
        object.insert("displayNameToken".to_owned(), value.clone());
    }
    if let Some(value) = &step.condition {
        object.insert("condition".to_owned(), json!(value));
    }
    object.insert("continueOnError".to_owned(), json!(step.continue_on_error));
    if let Some(value) = &step.working_directory {
        object.insert("workingDirectory".to_owned(), json!(value));
    }
    object.insert(
        "timeoutInMinutes".to_owned(),
        json!(step.timeout_in_minutes),
    );
    Value::Object(object)
}

fn expected_job_wire(job: &AgentJobRequestMessage) -> Value {
    let mut object = Map::new();
    if let Some(value) = &job.message_type {
        object.insert("messageType".to_owned(), json!(value));
    }
    object.insert("jobId".to_owned(), json!(job.job_id));
    object.insert("requestId".to_owned(), json!(job.request_id));
    object.insert(
        "plan".to_owned(),
        json!({
            "scopeIdentifier": job.plan.scope_identifier,
            "planId": job.plan.plan_id,
            "planType": job.plan.plan_type,
            "version": job.plan.version,
            "artifactUri": job.plan.artifact_uri,
            "artifactLocation": job.plan.artifact_location,
        }),
    );
    object.insert(
        "timeline".to_owned(),
        json!({
            "id": job.timeline.id,
            "changeId": job.timeline.change_id,
            "location": job.timeline.location,
        }),
    );
    if let Some(value) = &job.job_display_name {
        object.insert("jobDisplayName".to_owned(), json!(value));
    }
    object.insert("jobName".to_owned(), json!(job.job_name));
    object.insert("lockedUntil".to_owned(), json!(job.locked_until));
    if let Some(value) = &job.billing_owner_id {
        object.insert("billingOwnerId".to_owned(), json!(value));
    }
    object.insert("fileTable".to_owned(), json!(job.file_table));
    object.insert("defaults".to_owned(), json!(job.defaults));
    object.insert(
        "environmentVariables".to_owned(),
        json!(job.environment_variables),
    );
    object.insert("snapshot".to_owned(), json!(job.snapshot));
    if let Some(value) = &job.condition {
        object.insert("condition".to_owned(), json!(value));
    }
    object.insert(
        "variables".to_owned(),
        Value::Object(
            job.variables
                .iter()
                .map(|(key, value)| (key.clone(), expected_variable_wire(value)))
                .collect(),
        ),
    );
    object.insert(
        "mask".to_owned(),
        Value::Array(
            job.mask_hints
                .iter()
                .map(|hint| json!({"type": hint.hint_type, "value": hint.value}))
                .collect(),
        ),
    );
    let mut resources = Map::new();
    resources.insert(
        "endpoints".to_owned(),
        Value::Array(
            job.resources
                .endpoints
                .iter()
                .map(expected_endpoint_wire)
                .collect(),
        ),
    );
    if !job.resources.repositories.is_empty() {
        resources.insert("repositories".to_owned(), json!(job.resources.repositories));
    }
    object.insert("resources".to_owned(), Value::Object(resources));
    object.insert(
        "contextData".to_owned(),
        Value::Object(
            job.context_data
                .iter()
                .map(|(key, value)| (key.clone(), expected_context_wire(value)))
                .collect(),
        ),
    );
    object.insert(
        "steps".to_owned(),
        Value::Array(job.steps.iter().map(expected_step_wire).collect()),
    );
    if let Some(value) = job.retry_count {
        object.insert("retryCount".to_owned(), json!(value));
    }
    if let Some(value) = job.pre_job_timeout {
        object.insert("preJobTimeout".to_owned(), json!(value));
    }
    if let Some(value) = job.job_timeout {
        object.insert("jobTimeout".to_owned(), json!(value));
    }
    object.insert("jobContainer".to_owned(), json!(job.job_container));
    object.insert(
        "jobServiceContainers".to_owned(),
        json!(job.job_service_containers),
    );
    object.insert("jobOutputs".to_owned(), json!(job.job_outputs));
    if job.enable_debugger {
        object.insert("enableDebugger".to_owned(), json!(true));
    }
    if let Some(value) = &job.debugger_tunnel {
        object.insert("debuggerTunnel".to_owned(), json!(value));
    }
    if let Some(value) = &job.debugger_welcome_message {
        object.insert("debuggerWelcomeMessage".to_owned(), json!(value));
    }
    Value::Object(object)
}

fn arb_variables() -> impl Strategy<Value = BTreeMap<String, VariableValue>> {
    proptest::collection::btree_map(arb_key(), arb_variable_value(), 0..=3)
}

fn arb_resources() -> impl Strategy<Value = TaskResources> {
    (
        proptest::collection::vec(
            (
                proptest::collection::btree_map(arb_key(), arb_text(), 0..=2),
                arb_text(),
                prop::option::of(arb_text()),
                prop::option::of(arb_text()),
                proptest::collection::btree_map(arb_key(), arb_text(), 0..=2),
                prop::option::of(arb_text()),
                prop::option::of(arb_text()),
            )
                .prop_map(
                    |(data, name, endpoint_type, url, parameters, scheme, service_owner)| {
                        ServiceEndpoint {
                            data,
                            name,
                            endpoint_type,
                            url,
                            authorization: EndpointAuthorization { parameters, scheme },
                            is_shared: Some(false),
                            is_ready: None,
                            service_owner,
                        }
                    },
                ),
            0..=2,
        ),
        proptest::collection::vec(
            (
                prop::option::of(arb_text()),
                prop::option::of(arb_text()),
                prop::option::of(
                    (prop::option::of(arb_text()), prop::option::of(arb_text()))
                        .prop_map(|(id, name)| RepositoryConnector { id, name }),
                ),
            )
                .prop_map(|(repository, git_ref, connector)| RepositoryReference {
                    repository,
                    git_ref,
                    connector,
                }),
            0..=2,
        ),
    )
        .prop_map(|(endpoints, repositories)| TaskResources {
            endpoints,
            repositories,
        })
}

fn arb_job() -> impl Strategy<Value = AgentJobRequestMessage> {
    (
        (
            proptest::array::uniform16(any::<u8>()),
            proptest::array::uniform16(any::<u8>()),
            -100_000i64..=100_000,
            arb_text(),
            arb_text(),
            prop::option::of(arb_text()),
            prop::option::of(arb_text()),
            prop::option::of(-10i32..=10),
            prop::option::of(-100_000i64..=100_000),
            prop::option::of(-100_000i64..=100_000),
        ),
        (
            arb_variables(),
            arb_mask_hints(),
            proptest::collection::vec(arb_literal_step(), 0..=2),
            proptest::collection::btree_map(arb_key(), arb_context_data(), 0..=2),
            arb_resources(),
            prop::option::of(arb_text().prop_map(|value| json!({"type": 0, "lit": value}))),
            prop::option::of(arb_text().prop_map(|value| json!({"type": 2, "lit": value}))),
            prop::option::of(arb_text().prop_map(|value| json!({"type": 2, "lit": value}))),
        ),
        (
            any::<bool>(),
            prop::option::of(arb_text()),
            any::<bool>(),
            proptest::collection::vec(any::<u8>(), 0..=12),
        ),
    )
        .prop_map(
            |(
                (
                    job_id,
                    timeline_id,
                    request_id,
                    plan_id,
                    plan_type,
                    condition,
                    job_display_name,
                    retry_count,
                    pre_job_timeout,
                    job_timeout,
                ),
                (
                    variables,
                    mask_hints,
                    steps,
                    context_data,
                    resources,
                    job_container,
                    job_service_containers,
                    job_outputs,
                ),
                (enable_debugger, debugger_welcome_message, has_tunnel, key_bytes),
            )| AgentJobRequestMessage {
                message_type: Some("PipelineAgentJobRequest".to_owned()),
                job_id: uuid::Uuid::from_bytes(job_id),
                request_id,
                plan: PlanReference {
                    scope_identifier: plan_id.clone(),
                    plan_id,
                    plan_type,
                    version: 0,
                    artifact_uri: String::new(),
                    artifact_location: String::new(),
                },
                timeline: TimelineReference {
                    id: uuid::Uuid::from_bytes(timeline_id),
                    change_id: 0,
                    location: None,
                },
                job_display_name,
                job_name: "__job".to_owned(),
                locked_until: "0001-01-01T00:00:00".to_owned(),
                billing_owner_id: None,
                file_table: Vec::new(),
                defaults: Vec::new(),
                environment_variables: Vec::new(),
                snapshot: None,
                condition,
                variables,
                mask_hints,
                resources,
                context_data,
                steps,
                retry_count,
                pre_job_timeout,
                job_timeout,
                job_container,
                job_service_containers,
                job_outputs,
                enable_debugger,
                debugger_tunnel: has_tunnel.then_some(DebuggerTunnelInfo {
                    tunnel_id: "tunnel".to_owned(),
                    cluster_id: "cluster".to_owned(),
                    host_token: base64::engine::general_purpose::STANDARD.encode(key_bytes),
                    port: 443,
                }),
                debugger_welcome_message,
                aksh_debug_run_id: None,
                aksh_debug_transport: None,
            },
        )
}

// Variables, steps, and tagged context collections are compared against
// independent wire-shape oracles so DTO field renames cannot hide drift.

fn assert_context_semantics(left: &PipelineContextData, right: &PipelineContextData) {
    match (left, right) {
        (PipelineContextData::Null, PipelineContextData::Null) => {}
        (PipelineContextData::String(a), PipelineContextData::String(b)) => assert_eq!(a, b),
        (PipelineContextData::Bool(a), PipelineContextData::Bool(b)) => assert_eq!(a, b),
        (PipelineContextData::Number(a), PipelineContextData::Number(b)) => {
            assert_eq!(a.to_bits(), b.to_bits())
        }
        (PipelineContextData::Array(a), PipelineContextData::Array(b)) => {
            assert_eq!(a.len(), b.len());
            for (a, b) in a.iter().zip(b) {
                assert_context_semantics(a, b);
            }
        }
        (PipelineContextData::Dict(a), PipelineContextData::Dict(b)) => {
            assert_eq!(a.keys().collect::<Vec<_>>(), b.keys().collect::<Vec<_>>());
            for (key, value) in a {
                assert_context_semantics(value, &b[key]);
            }
        }
        _ => panic!("context variant changed: {left:?} vs {right:?}"),
    }
}

fn expected_context_wire(value: &PipelineContextData) -> Value {
    match value {
        PipelineContextData::Null => Value::Null,
        PipelineContextData::String(value) => Value::String(value.clone()),
        PipelineContextData::Bool(value) => Value::Bool(*value),
        PipelineContextData::Number(value) => json!(value),
        PipelineContextData::Array(values) => {
            let mut object = Map::new();
            object.insert("t".to_owned(), json!(1));
            object.insert(
                "a".to_owned(),
                Value::Array(values.iter().map(expected_context_wire).collect()),
            );
            Value::Object(object)
        }
        PipelineContextData::Dict(values) => {
            let mut object = Map::new();
            object.insert("t".to_owned(), json!(2));
            object.insert(
                "d".to_owned(),
                Value::Array(
                    values
                        .iter()
                        .map(|(key, value)| json!({"k": key, "v": expected_context_wire(value)}))
                        .collect(),
                ),
            );
            Value::Object(object)
        }
    }
}

fn arb_context_data() -> impl Strategy<Value = PipelineContextData> {
    let leaf = prop_oneof![
        arb_text().prop_map(PipelineContextData::String),
        any::<bool>().prop_map(PipelineContextData::Bool),
        (-1000.0f64..1000.0).prop_map(PipelineContextData::Number),
    ];
    leaf.prop_recursive(3, 32, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..=3).prop_map(PipelineContextData::Array),
            proptest::collection::btree_map(arb_key(), inner, 0..=3)
                .prop_map(PipelineContextData::Dict),
        ]
    })
}

fn arb_variable_value() -> impl Strategy<Value = VariableValue> {
    (
        prop_oneof![Just(None), arb_text().prop_map(Some)],
        prop_oneof![Just(None), Just(Some(false)), Just(Some(true))],
    )
        .prop_map(|(value, is_secret)| VariableValue { value, is_secret })
}

fn arb_mask_hints() -> impl Strategy<Value = Vec<MaskHint>> {
    proptest::collection::vec(arb_text(), 0..=3).prop_map(|values| {
        values
            .into_iter()
            .map(|value| MaskHint {
                hint_type: MaskType::Regex,
                value,
            })
            .collect()
    })
}

fn arb_literal_step() -> impl Strategy<Value = TaskStep> {
    (
        any::<bool>(),
        arb_text(),
        arb_non_script_inputs(),
        proptest::collection::btree_map(arb_key(), arb_text(), 0..=2),
        prop::option::of(arb_text()),
        prop::option::of(arb_text()),
        prop::option::of(arb_text()),
        prop::option::of(arb_text()),
    )
        .prop_map(
            |(has_script, script, inputs, env, name, context_name, display_name, condition)| {
                let display_name_token = Some(json!({
                    "type": 1,
                    "lit": display_name.clone().unwrap_or_default()
                }));
                TaskStep {
                    id: uuid::Uuid::nil(),
                    name,
                    context_name,
                    display_name,
                    display_name_token,
                    condition,
                    script: has_script.then_some(script),
                    reference: None,
                    inputs,
                    env,
                    continue_on_error: Some(false),
                    working_directory: None,
                    timeout_in_minutes: None,
                }
            },
        )
}

#[test]
fn task_agent_message_roundtrip() {
    let msg = TaskAgentMessage {
        message_id: 1,
        message_type: "PipelineAgentJobRequest".to_owned(),
        body: "aGVsbG8=".to_owned(),
        iv: Some("AQID".to_owned()),
    };
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("\"messageId\":1"));
    assert!(json.contains("\"messageType\":\"PipelineAgentJobRequest\""));
    let back: TaskAgentMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(back.message_id, 1);
    assert_eq!(back.body, "aGVsbG8=");
}

#[test]
fn task_agent_message_no_iv() {
    let json = r#"{"messageId":42,"messageType":"Test","body":"dGVzdA=="}"#;
    let msg: TaskAgentMessage = serde_json::from_str(json).unwrap();
    assert_eq!(msg.message_id, 42);
    assert!(msg.iv.is_none());
}

#[test]
fn variable_value_secret_roundtrip() {
    let v = VariableValue::secret("my-secret-val");
    let json = serde_json::to_string(&v).unwrap();
    assert!(json.contains("\"isSecret\":true"));
    let back: VariableValue = serde_json::from_str(&json).unwrap();
    assert_eq!(back.value.unwrap(), "my-secret-val");
    assert_eq!(back.is_secret, Some(true));
}

#[test]
fn timeline_record_state_serialization() {
    let record = TimelineRecord {
        id: uuid::Uuid::nil(),
        change_id: None,
        parent_id: None,
        name: None,
        display_name: None,
        record_type: Some(TimelineRecordType::Job),
        state: Some(TimelineRecordState::InProgress),
        result: None,
        start_time: None,
        finish_time: None,
        issues: vec![],
        variables: BTreeMap::new(),
        current_operation: None,
        percent_complete: Some(50),
        worker_name: None,
        error_count: None,
        warning_count: None,
        is_background: None,
        background_control_type: None,
        background_control_step_ids: vec![],
        parallel_group_id: None,
        steps: vec![],
    };
    let json = serde_json::to_string(&record).unwrap();
    assert!(json.contains("\"state\":\"inProgress\""));
    assert!(json.contains("\"type\":\"job\""));
}

#[test]
fn timeline_record_background_fields_roundtrip() {
    let step_id = uuid::Uuid::new_v4();
    let record: TimelineRecord = serde_json::from_value(serde_json::json!({
        "id": uuid::Uuid::nil(),
        "isBackground": true,
        "backgroundControlType": "wait",
        "backgroundControlStepIds": [step_id],
        "parallelGroupId": "group-1"
    }))
    .unwrap();

    assert_eq!(record.is_background, Some(true));
    assert_eq!(record.background_control_type.as_deref(), Some("wait"));
    assert_eq!(record.background_control_step_ids, vec![step_id]);
    assert_eq!(record.parallel_group_id.as_deref(), Some("group-1"));

    let json = serde_json::to_string(&record).unwrap();
    assert!(json.contains("\"isBackground\":true"));
    assert!(json.contains("\"backgroundControlType\":\"wait\""));
    assert!(json.contains("\"backgroundControlStepIds\""));
    assert!(json.contains("\"parallelGroupId\":\"group-1\""));
}

#[test]
fn task_result_serialization() {
    assert_eq!(
        serde_json::to_string(&TaskResult::Succeeded).unwrap(),
        "\"succeeded\""
    );
    assert_eq!(
        serde_json::to_string(&TaskResult::Failed).unwrap(),
        "\"failed\""
    );
    assert_eq!(
        serde_json::to_string(&TaskResult::Cancelled).unwrap(),
        "\"canceled\""
    );
    assert_eq!(
        serde_json::from_str::<TaskResult>("\"cancelled\"").unwrap(),
        TaskResult::Cancelled
    );
}

#[test]
fn task_step_serializes_as_runner_action_step() {
    let step = TaskStep {
        id: uuid::Uuid::nil(),
        name: None,
        context_name: None,
        display_name: None,
        display_name_token: None,
        condition: None,
        script: Some("echo hi".to_owned()),
        reference: None,
        inputs: BTreeMap::new(),
        env: BTreeMap::new(),
        continue_on_error: None,
        working_directory: None,
        timeout_in_minutes: None,
    };

    let json = serde_json::to_value(&step).unwrap();

    assert_eq!(json["type"], "action");
    assert_eq!(json["reference"]["type"], "script");
    assert!(json.get("environment").is_none());
    assert_eq!(json["name"], Value::Null);
    assert_eq!(json["continueOnError"], Value::Null);
    assert_eq!(json["timeoutInMinutes"], Value::Null);
    assert_eq!(json["inputs"]["type"], 2);
    assert_eq!(json["inputs"]["map"][0]["Key"]["type"], 0);
    assert_eq!(json["inputs"]["map"][0]["Key"]["lit"], "script");
    assert_eq!(json["inputs"]["map"][0]["Value"]["type"], 0);
    assert_eq!(json["inputs"]["map"][0]["Value"]["lit"], "echo hi");
}

#[test]
fn task_step_serializes_expression_as_format_token() {
    let mut inputs = BTreeMap::new();
    inputs.insert(
        "script".to_owned(),
        "OUTPUT='${{ steps.make.outputs.value }}'".to_owned(),
    );
    let step = TaskStep {
        id: uuid::Uuid::nil(),
        name: None,
        context_name: None,
        display_name: None,
        display_name_token: None,
        condition: None,
        script: None,
        reference: None,
        inputs,
        env: BTreeMap::new(),
        continue_on_error: None,
        working_directory: None,
        timeout_in_minutes: None,
    };
    let value = serde_json::to_value(step).unwrap();
    let token = &value["inputs"]["map"][0]["Value"];
    assert_eq!(token["type"], 3);
    assert_eq!(
        token["expr"],
        "format('OUTPUT=''{0}''', steps.make.outputs.value)"
    );
}

#[test]
fn template_string_token_handles_braces_inside_string_literals() {
    // Expression containing }} inside a single-quoted JSON string
    let token =
        template_string_token(r#"${{ fromJSON('{"a":{"b":{"c":"deep"}}}')['a']['b']['c'] }}"#);
    assert_eq!(token["type"], 3);
    let expr = token["expr"].as_str().unwrap();
    // Should preserve the full expression, not truncate at the first }}
    assert!(
        expr.contains("fromJSON") && expr.contains("deep"),
        "expression was truncated: {expr}"
    );
}

#[test]
fn find_expression_end_skips_braces_in_strings() {
    // }} inside a string should be skipped
    assert_eq!(find_expression_end(" fromJSON('{}}')'a' }}"), Some(20));
    // Plain expression
    assert_eq!(find_expression_end(" x }}"), Some(3));
    // No closing
    assert_eq!(find_expression_end(" x "), None);
}

#[test]
fn pipeline_context_data_variants() {
    let json = r#""hello""#;
    let data: PipelineContextData = serde_json::from_str(json).unwrap();
    assert!(matches!(data, PipelineContextData::String(_)));

    let json = "42";
    let data: PipelineContextData = serde_json::from_str(json).unwrap();
    assert!(matches!(data, PipelineContextData::Number(_)));

    let json = "true";
    let data: PipelineContextData = serde_json::from_str(json).unwrap();
    assert!(matches!(data, PipelineContextData::Bool(_)));

    let json = r#"["a","b"]"#;
    let data: PipelineContextData = serde_json::from_str(json).unwrap();
    assert!(matches!(data, PipelineContextData::Array(_)));
}

#[test]
fn pipeline_context_data_uses_runner_wire_shape_for_collections() {
    let mut github = BTreeMap::new();
    github.insert(
        "event_name".to_owned(),
        PipelineContextData::String("push".to_owned()),
    );
    github.insert("run_id".to_owned(), PipelineContextData::Number(42.0));

    let json = serde_json::to_value(PipelineContextData::Dict(github)).unwrap();

    assert_eq!(json["t"], 2);
    assert_eq!(json["d"][0]["k"], "event_name");
    assert_eq!(json["d"][0]["v"], "push");
    assert_eq!(json["d"][1]["k"], "run_id");
    assert_eq!(json["d"][1]["v"], 42.0);

    let roundtrip: PipelineContextData = serde_json::from_value(json).unwrap();
    let PipelineContextData::Dict(roundtrip) = roundtrip else {
        panic!("expected dictionary context");
    };
    assert!(matches!(
        roundtrip.get("event_name"),
        Some(PipelineContextData::String(value)) if value == "push"
    ));
}

#[test]
fn issue_roundtrip() {
    let issue = Issue {
        issue_type: IssueType::Error,
        category: Some("LoggingCommand".to_owned()),
        message: Some("::error::something broke".to_owned()),
        data: BTreeMap::new(),
        is_infrastructure_issue: None,
    };
    let json = serde_json::to_string(&issue).unwrap();
    assert!(json.contains("\"type\":\"error\""));
    let back: Issue = serde_json::from_str(&json).unwrap();
    assert_eq!(back.issue_type, IssueType::Error);
}
// Tier 2 authority (actions/runner v2.335.1, commit 7d737449ef346f6524f75688d0c9c95fa10ba10a):
// VariableValue: https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTWebApi/WebApi/VariableValue.cs#L8-L38
// ActionStep/JobStep wire fields: https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTPipelines/Pipelines/ActionStep.cs#L9-L46
// PipelineContextData converter and tagged collection shapes: https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTPipelines/Pipelines/ContextData/PipelineContextDataJsonConverter.cs#L20-L151
// TaskAgentSessionKey bytes/encrypted flag: https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTWebApi/WebApi/TaskAgentSessionKey.cs#L8-L32
// AgentJobRequestMessage core/debugger wire members: https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTPipelines/Pipelines/AgentJobRequestMessage.cs#L15-L220 and https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Sdk/DTPipelines/Pipelines/AgentJobRequestMessage.cs#L221-L267
proptest! {
    #![proptest_config(codec_config())]

    #[test]
    fn tier2_codec_variable_value_tristate(value in arb_variable_value()) {
        let encoded = serde_json::to_value(&value).unwrap();
        let decoded: VariableValue = serde_json::from_value(encoded.clone()).unwrap();
        prop_assert_eq!(serde_json::to_value(&decoded).unwrap(), encoded);
        prop_assert_eq!(&value.value, &decoded.value);
        prop_assert_eq!(&value.is_secret, &decoded.is_secret);

        let omitted: BTreeMap<String, VariableValue> = serde_json::from_value(json!({})).unwrap();
        let explicit_null: BTreeMap<String, VariableValue> =
            serde_json::from_value(json!({"VAR": {"value": null}})).unwrap();
        let empty: BTreeMap<String, VariableValue> =
            serde_json::from_value(json!({"VAR": {"value": ""}})).unwrap();
        prop_assert!(!omitted.contains_key("VAR"));
        prop_assert_eq!(explicit_null.get("VAR").and_then(|v| v.value.as_deref()), None);
        prop_assert_eq!(empty.get("VAR").and_then(|v| v.value.as_deref()), Some(""));
        prop_assert_eq!(serde_json::to_value(omitted).unwrap(), json!({}));
        prop_assert_eq!(serde_json::to_value(explicit_null).unwrap(), json!({"VAR": {}}));
        prop_assert_eq!(serde_json::to_value(empty).unwrap(), json!({"VAR": {"value": ""}}));
    }

    #[test]
    fn tier2_codec_task_step_canonical_roundtrip(step in arb_literal_step(), expression in arb_expression()) {
        let mut expected_inputs = step.inputs.clone();
        if let Some(script) = &step.script {
            expected_inputs.insert("script".to_owned(), script.clone());
        }
        let encoded = serde_json::to_value(&step).unwrap();
        prop_assert_eq!(&encoded["type"], &json!("action"));
        prop_assert_eq!(&encoded["reference"], &json!({"type": "script"}));
        if step.env.is_empty() {
            prop_assert!(encoded.get("environment").is_none());
        } else {
            prop_assert_eq!(&encoded["environment"], &expected_template_map(&step.env));
        }
        prop_assert_eq!(&encoded["inputs"], &expected_template_map(&expected_inputs));
        prop_assert_eq!(&encoded["id"], &json!(step.id));
        prop_assert_eq!(encoded.get("contextName").is_some(), step.context_name.is_some());
        prop_assert!(encoded.get("displayName").is_none());
        prop_assert_eq!(encoded.get("displayNameToken").is_some(), step.display_name_token.is_some());
        if let Some(context_name) = &step.context_name {
            prop_assert_eq!(&encoded["contextName"], &json!(context_name));
        }
        if let Some(display_name_token) = &step.display_name_token {
            prop_assert_eq!(&encoded["displayNameToken"], display_name_token);
        }

        let decoded: TaskStep = serde_json::from_value(encoded.clone()).unwrap();
        prop_assert_eq!(&decoded.id, &step.id);
        let expected_name = step.context_name.clone().or_else(|| step.name.clone());
        prop_assert_eq!(&decoded.name, &expected_name);
        prop_assert_eq!(&decoded.context_name, &step.context_name);
        prop_assert_eq!(&decoded.display_name, &None);
        prop_assert_eq!(&decoded.display_name_token, &step.display_name_token);
        prop_assert_eq!(&decoded.condition, &step.condition);
        prop_assert_eq!(&decoded.script, &step.script);
        prop_assert_eq!(&decoded.inputs, &expected_inputs);
        prop_assert_eq!(&decoded.env, &step.env);
        prop_assert_eq!(&decoded.continue_on_error, &step.continue_on_error);
        prop_assert_eq!(&decoded.working_directory, &step.working_directory);
        prop_assert_eq!(&decoded.timeout_in_minutes, &step.timeout_in_minutes);
        prop_assert_eq!(serde_json::to_value(&decoded).unwrap(), encoded);

        let mut expression_step = step.clone();
        expression_step.script = None;
        expression_step.inputs.insert("script".to_owned(), format!("${{{{ {expression} }}}}"));
        let expression_wire = serde_json::to_value(expression_step).unwrap();
        let script_token = expression_wire["inputs"]["map"]
            .as_array()
            .unwrap()
            .iter()
            .find(|pair| pair["Key"]["lit"] == "script")
            .map(|pair| pair["Value"].clone())
            .unwrap();
        prop_assert_eq!(&script_token, &expected_template_token(&format!("${{{{ {expression} }}}}")));
    }

    #[test]
    fn tier2_codec_pipeline_context_data_roundtrip(value in arb_context_data()) {
        let encoded = serde_json::to_value(&value).unwrap();
        prop_assert_eq!(&encoded, &expected_context_wire(&value));
        let decoded: PipelineContextData = serde_json::from_value(encoded.clone()).unwrap();
        assert_context_semantics(&value, &decoded);
        prop_assert_eq!(serde_json::to_value(&decoded).unwrap(), encoded);
    }

    #[test]
    fn tier2_codec_encryption_key_base64_and_flag(bytes in proptest::collection::vec(any::<u8>(), 0..=64), encrypted in any::<bool>()) {
        let key = EncryptionKey { value: bytes.clone(), encrypted };
        let encoded = serde_json::to_value(&key).unwrap();
        prop_assert_eq!(&encoded["value"], &json!(base64::engine::general_purpose::STANDARD.encode(&bytes)));
        prop_assert_eq!(&encoded["encrypted"], &json!(encrypted));
        let decoded: EncryptionKey = serde_json::from_value(encoded.clone()).unwrap();
        prop_assert_eq!(&decoded.value, &bytes);
        prop_assert_eq!(&decoded.encrypted, &encrypted);
        prop_assert_eq!(serde_json::to_value(&decoded).unwrap(), encoded);
    }

    #[test]
    fn tier2_codec_agent_job_request_canonical_roundtrip(job in arb_job()) {
        // Compare against a field-by-field oracle derived from the semantic
        // values and the official AgentJobRequestMessage DataMembers; this
        // must remain independent of AgentJobRequestMessage::serialize.
        let expected = expected_job_wire(&job);
        let encoded = serde_json::to_value(&job).unwrap();
        prop_assert_eq!(&encoded, &expected);
        prop_assert!(encoded.get("env").is_none());
        prop_assert_eq!(encoded["steps"].as_array().unwrap().len(), job.steps.len());
        let decoded: AgentJobRequestMessage = serde_json::from_value(encoded.clone()).unwrap();
        prop_assert_eq!(serde_json::to_value(&decoded).unwrap(), encoded);
    }

}
#[test]
fn tier2_codec_task_step_environment_aliases() {
    let empty_cases = ["environment", "env"];
    for field in empty_cases {
        let wire = json!({
            "type": "action",
            "reference": {"type": "script"},
            "id": uuid::Uuid::nil(),
            field: {"type": 2},
            "inputs": {"type": 2},
        });
        let decoded: TaskStep = serde_json::from_value(wire).unwrap();
        assert!(
            decoded.env.is_empty(),
            "empty {field} token must decode as empty env"
        );
        let encoded = serde_json::to_value(&decoded).unwrap();
        assert!(encoded.get("environment").is_none());
        assert!(encoded.get("env").is_none());
    }

    // Construct this TemplateToken map directly rather than through
    // TemplateStringMap, proving the decoder accepts official token
    // members independently of this DTO's serializer.
    let token_map = json!({
        "type": 2,
        "map": [
            {"Key": {"type": 0, "lit": "ALPHA"}, "Value": {"type": 0, "lit": "one"}},
            {"key": {"type": 0, "lit": "BETA"}, "value": {"type": 0, "lit": "two"}},
        ],
    });
    for field in ["environment", "env"] {
        let mut wire = json!({
            "type": "action",
            "reference": {"type": "script"},
            "id": uuid::Uuid::nil(),
            "inputs": {"type": 2},
        });
        wire[field] = token_map.clone();
        let decoded: TaskStep = serde_json::from_value(wire).unwrap();
        assert_eq!(
            decoded.env,
            BTreeMap::from([
                ("ALPHA".to_owned(), "one".to_owned()),
                ("BETA".to_owned(), "two".to_owned()),
            ])
        );
        let encoded = serde_json::to_value(&decoded).unwrap();
        assert_eq!(encoded["environment"], expected_template_map(&decoded.env));
        assert!(encoded.get("env").is_none());
    }
}

#[test]
fn context_data_from_json_null_round_trips() {
    let null_json = serde_json::Value::Null;
    let ctx = PipelineContextData::from_json(&null_json);
    assert!(matches!(ctx, PipelineContextData::Null));
    let back = ctx.to_json();
    assert_eq!(back, serde_json::Value::Null);
}

#[test]
fn context_data_from_json_string_round_trips() {
    let json = serde_json::json!("hello");
    let ctx = PipelineContextData::from_json(&json);
    assert!(matches!(&ctx, PipelineContextData::String(s) if s == "hello"));
    assert_eq!(ctx.to_json(), json);
}

#[test]
fn context_data_from_json_bool_round_trips() {
    let json = serde_json::json!(true);
    let ctx = PipelineContextData::from_json(&json);
    assert!(matches!(ctx, PipelineContextData::Bool(true)));
    assert_eq!(ctx.to_json(), json);
}

#[test]
fn context_data_from_json_number_round_trips() {
    let json = serde_json::json!(42.5);
    let ctx = PipelineContextData::from_json(&json);
    assert!(matches!(ctx, PipelineContextData::Number(n) if (n - 42.5).abs() < f64::EPSILON));
    assert_eq!(ctx.to_json(), json);
}

#[test]
fn context_data_from_json_nested_round_trips() {
    let json = serde_json::json!({
        "key": "value",
        "nested": {
            "arr": [1.0, null, true, "s"],
            "empty": null
        }
    });
    let ctx = PipelineContextData::from_json(&json);
    let back = ctx.to_json();
    assert_eq!(back, json, "nested structure must round-trip exactly");
}

#[test]
fn context_data_from_json_empty_array_and_dict() {
    let arr_json = serde_json::json!([]);
    let ctx = PipelineContextData::from_json(&arr_json);
    assert!(matches!(&ctx, PipelineContextData::Array(v) if v.is_empty()));
    assert_eq!(ctx.to_json(), arr_json);

    let dict_json = serde_json::json!({});
    let ctx = PipelineContextData::from_json(&dict_json);
    assert!(matches!(&ctx, PipelineContextData::Dict(m) if m.is_empty()));
    assert_eq!(ctx.to_json(), dict_json);
}
