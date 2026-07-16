use super::*;
use proptest::prelude::*;

#[test]
fn inject_github_env_sets_core_vars() {
    let mut job = JobContext::new(
        "j1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({
            "github": {
                "repository": "owner/repo",
                "sha": "abc123",
                "ref": "refs/heads/main",
                "actor": "user1"
            }
        }),
    );
    job.workspace = Some("_work/repo/repo".into());

    let msg = serde_json::json!({
        "contextData": {
            "github": {
                "repository": "owner/repo",
                "sha": "abc123",
                "ref": "refs/heads/main",
                "actor": "user1"
            }
        }
    });

    inject_github_env(&mut job, &msg);

    assert_eq!(job.env.get("CI").unwrap(), "true");
    assert_eq!(job.env.get("GITHUB_ACTIONS").unwrap(), "true");
    assert_eq!(job.env.get("GITHUB_REPOSITORY").unwrap(), "owner/repo");
    assert_eq!(job.env.get("GITHUB_SHA").unwrap(), "abc123");
    assert_eq!(job.env.get("GITHUB_REF").unwrap(), "refs/heads/main");
    assert_eq!(job.env.get("GITHUB_ACTOR").unwrap(), "user1");
    assert!(job.env.contains_key("RUNNER_OS"));
    assert!(job.env.contains_key("RUNNER_ARCH"));
}

#[test]
fn build_step_list_parses_script_reference() {
    let steps = vec![serde_json::json!({
        "id": "step1",
        "displayName": "Run echo",
        "reference": {
            "type": "script"
        },
        "inputs": {
            "script": "echo hello"
        },
        "condition": "success()"
    })];

    let msg = serde_json::json!({});
    let result = build_step_list(&steps, &msg);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, "step1");
    assert_eq!(result[0].display_name, "Run echo");
    assert!(
        matches!(&result[0].step_type, StepType::Script { script, .. } if script == "echo hello")
    );
}

#[test]
fn build_step_list_parses_action_reference() {
    let steps = vec![serde_json::json!({
        "id": "checkout",
        "displayName": "Checkout",
        "reference": {
            "type": "repository",
            "name": "actions/checkout@v4"
        },
        "inputs": {
            "fetch-depth": "1"
        }
    })];

    let msg = serde_json::json!({});
    let result = build_step_list(&steps, &msg);

    assert_eq!(result.len(), 1);
    assert!(
        matches!(&result[0].step_type, StepType::Action { uses, .. } if uses == "actions/checkout@v4")
    );
}

#[test]
fn build_step_list_parses_github_template_token_maps() {
    let steps = vec![serde_json::json!({
        "id": "step1",
        "reference": {
            "type": "script"
        },
        "inputs": {
            "type": 2,
            "map": [
                {
                    "Key": { "type": 0, "lit": "script" },
                    "Value": { "type": 0, "lit": "echo first" }
                }
            ]
        },
        "environment": {
            "type": 2,
            "map": [
                {
                    "Key": { "type": 0, "lit": "VAL" },
                    "Value": { "type": 0, "lit": "hello" }
                }
            ]
        }
    })];

    let result = build_step_list(&steps, &serde_json::json!({}));

    assert_eq!(result[0].display_name, "Run echo first");
    assert_eq!(result[0].env.get("VAL").map(String::as_str), Some("hello"));
    assert!(
        matches!(&result[0].step_type, StepType::Script { script, .. } if script == "echo first")
    );
}

#[test]
fn build_step_list_parses_aksh_template_string_maps() {
    let steps = vec![serde_json::json!({
        "id": "step1",
        "reference": {
            "type": "script"
        },
        "inputs": {
            "type": 2,
            "map": [
                {
                    "key": "script",
                    "value": "echo line1\necho line2\n"
                }
            ]
        },
        "environment": {
            "type": 2,
            "map": [
                {
                    "key": "VAL",
                    "value": "hello"
                }
            ]
        }
    })];

    let result = build_step_list(&steps, &serde_json::json!({}));

    assert_eq!(result[0].display_name, "Run echo line1");
    assert_eq!(result[0].env.get("VAL").map(String::as_str), Some("hello"));
    assert!(
        matches!(&result[0].step_type, StepType::Script { script, .. } if script == "echo line1\necho line2\n")
    );
}

#[test]
fn build_step_list_handles_continue_on_error() {
    let steps = vec![serde_json::json!({
        "id": "risky",
        "displayName": "Risky step",
        "continueOnError": true,
        "run": "exit 1"
    })];

    let result = build_step_list(&steps, &serde_json::json!({}));
    assert!(result[0].continue_on_error);
}

#[test]
fn build_step_list_handles_template_continue_on_error() {
    let steps = vec![
        serde_json::json!({
            "id": "lit",
            "displayName": "Template literal",
            "continueOnError": {
                "type": 0,
                "lit": "true"
            },
            "run": "exit 1"
        }),
        serde_json::json!({
            "id": "expr",
            "displayName": "Template expression",
            "continueOnError": {
                "expr": "true"
            },
            "run": "exit 1"
        }),
    ];

    let result = build_step_list(&steps, &serde_json::json!({}));
    assert!(result[0].continue_on_error);
    assert!(result[1].continue_on_error);
}

#[test]
fn inject_actions_env_from_system_vss_endpoint_data() {
    let mut job = JobContext::new(
        "j1".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    job.workspace = Some("_work/repo/repo".into());

    let msg = serde_json::json!({
        "resources": {
            "endpoints": [{
                "name": "SystemVssConnection",
                "url": "https://run-actions.example/45/",
                "authorization": {
                    "parameters": {
                        "AccessToken": "runtime-token"
                    }
                },
                "data": {
                    "ResultsServiceUrl": "https://results.example/",
                    "CacheServerUrl": "https://cache.example/",
                    "GenerateIdTokenUrl": "https://run-actions.example/idtoken"
                }
            }]
        }
    });

    inject_github_env(&mut job, &msg);

    assert_eq!(
        job.env.get("ACTIONS_RUNTIME_URL").map(String::as_str),
        Some("https://run-actions.example/45/")
    );
    assert_eq!(
        job.env.get("ACTIONS_RUNTIME_TOKEN").map(String::as_str),
        Some("runtime-token")
    );
    assert_eq!(
        job.env
            .get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
            .map(String::as_str),
        Some("runtime-token")
    );
    assert_eq!(
        job.env.get("ACTIONS_RESULTS_URL").map(String::as_str),
        Some("https://results.example/")
    );
    assert_eq!(
        job.env.get("ACTIONS_CACHE_URL").map(String::as_str),
        Some("https://cache.example")
    );
    assert_eq!(
        job.env.get("ACTIONS_CACHE_SERVICE_V2").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        job.env
            .get("ACTIONS_ID_TOKEN_REQUEST_URL")
            .map(String::as_str),
        Some("https://run-actions.example/idtoken")
    );
    assert!(job.mask_secrets("runtime-token").contains("***"));
}

#[test]
fn injects_job_environment_variables_from_acquire_payload() {
    let mut job = JobContext::new(
        "j1".into(),
        "Job".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    job.workspace = Some("_work/repo/repo".into());

    let msg = serde_json::json!({
        "environmentVariables": [{
            "type": 2,
            "map": [{
                "Key": { "type": 0, "lit": "MEGA_GLOBAL_ENV" },
                "Value": { "type": 0, "lit": "global-env-ok" }
            }]
        }]
    });

    inject_github_env(&mut job, &msg);

    assert_eq!(
        job.env.get("MEGA_GLOBAL_ENV").map(String::as_str),
        Some("global-env-ok")
    );
}
#[test]
fn lifecycle_uses_resolved_action_path_and_entry_overrides() {
    let temp = tempfile::TempDir::new().unwrap();
    let workspace = temp.path().join("_work/repo/repo");
    let action_dir = temp
        .path()
        .join("_work/_actions/actions/example/0123456789abcdef");
    std::fs::create_dir_all(&action_dir).unwrap();
    std::fs::write(
        action_dir.join("action.yml"),
        r#"
name: example
runs:
  using: node20
  main: main.js
  pre: pre.js
  post: cleanup.js
"#,
    )
    .unwrap();

    let main_steps = vec![Step {
        id: "main-action".into(),
        context_name: "main-action".into(),
        display_name: "Example".into(),
        step_type: StepType::Action {
            uses: "actions/example@v1".into(),
            with: serde_json::json!({"token": "x"}),
        },
        condition: Some("success()".into()),
        continue_on_error: false,
        timeout_minutes: None,
        env: std::collections::HashMap::new(),
        raw: serde_json::json!({}),
        is_background: false,
    }];
    let mut action_paths = std::collections::HashMap::new();
    action_paths.insert(
        "actions/example@v1".to_string(),
        action_dir.to_string_lossy().to_string(),
    );

    let ordered =
        build_step_list_with_lifecycle(main_steps, workspace.to_str().unwrap(), &action_paths);

    assert_eq!(ordered.len(), 3);
    assert_eq!(ordered[0].id, "__pre_main-action");
    assert_eq!(ordered[1].id, "main-action");
    assert_eq!(ordered[2].id, "__post_main-action");
    assert!(matches!(
        &ordered[0].step_type,
        StepType::Action { with, .. }
            if with.get("__aksh_entry").and_then(|v| v.as_str()) == Some("pre.js")
    ));
    assert!(matches!(
        &ordered[2].step_type,
        StepType::Action { with, .. }
            if with.get("__aksh_entry").and_then(|v| v.as_str()) == Some("cleanup.js")
    ));
    assert_eq!(ordered[2].condition.as_deref(), Some("always()"));
}

#[test]
fn lifecycle_local_actions_skip_pre_but_retain_main_and_post() {
    let temp = tempfile::TempDir::new().unwrap();
    let workspace = temp.path().join("_work/repo/repo");
    let local_dir = workspace.join(".github/actions/local");
    let remote_dir = temp
        .path()
        .join("_work/_actions/actions/remote/0123456789abcdef");
    std::fs::create_dir_all(&local_dir).unwrap();
    std::fs::create_dir_all(&remote_dir).unwrap();
    let manifest = "name: lifecycle\nruns:\n  using: node20\n  main: main.js\n  pre: pre.js\n  post: post.js\n";
    std::fs::write(local_dir.join("action.yml"), manifest).unwrap();
    std::fs::write(remote_dir.join("action.yml"), manifest).unwrap();

    let action_step = |id: &str, uses: &str| Step {
        id: id.into(),
        context_name: id.into(),
        display_name: id.into(),
        step_type: StepType::Action {
            uses: uses.into(),
            with: serde_json::json!({}),
        },
        condition: Some("success()".into()),
        continue_on_error: false,
        timeout_minutes: None,
        env: std::collections::HashMap::new(),
        raw: serde_json::json!({}),
        is_background: false,
    };
    let main_steps = vec![
        action_step("local", "./.github/actions/local"),
        action_step("remote", "actions/remote@v1"),
    ];
    let mut action_paths = std::collections::HashMap::new();
    action_paths.insert(
        "actions/remote@v1".to_string(),
        remote_dir.to_string_lossy().to_string(),
    );

    let ordered =
        build_step_list_with_lifecycle(main_steps, workspace.to_str().unwrap(), &action_paths);

    assert_eq!(
        ordered
            .iter()
            .map(|step| step.id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "__pre_remote",
            "local",
            "remote",
            "__post_remote",
            "__post_local",
        ]
    );
    assert!(matches!(
        &ordered[0].step_type,
        StepType::Action { with, .. }
            if with.get("__aksh_entry").and_then(|v| v.as_str()) == Some("pre.js")
    ));
    assert!(matches!(
        &ordered[4].step_type,
        StepType::Action { with, .. }
            if with.get("__aksh_entry").and_then(|v| v.as_str()) == Some("post.js")
    ));
}

#[test]
fn lifecycle_registers_docker_action_pre_and_post() {
    let temp = tempfile::TempDir::new().unwrap();
    let workspace = temp.path().join("_work/repo/repo");
    let action_dir = temp
        .path()
        .join("_work/_actions/actions/docker-action/0123456789abcdef");
    std::fs::create_dir_all(&action_dir).unwrap();
    std::fs::write(
        action_dir.join("action.yml"),
        r#"
name: docker action
runs:
  using: docker
  image: Dockerfile
  pre-entrypoint: pre-entrypoint.sh
  post-entrypoint: post-entrypoint.sh
  post-if: always()
"#,
    )
    .unwrap();

    let main_steps = vec![Step {
        id: "docker-action".into(),
        context_name: "docker-action".into(),
        display_name: "Docker Action".into(),
        step_type: StepType::Action {
            uses: "actions/docker-action@v1".into(),
            with: serde_json::json!({}),
        },
        condition: Some("success()".into()),
        continue_on_error: false,
        timeout_minutes: None,
        env: std::collections::HashMap::new(),
        raw: serde_json::json!({}),
        is_background: false,
    }];
    let mut action_paths = std::collections::HashMap::new();
    action_paths.insert(
        "actions/docker-action@v1".to_string(),
        action_dir.to_string_lossy().to_string(),
    );

    let ordered =
        build_step_list_with_lifecycle(main_steps, workspace.to_str().unwrap(), &action_paths);

    assert_eq!(ordered.len(), 3);
    assert_eq!(ordered[0].id, "__pre_docker-action");
    assert_eq!(ordered[1].id, "docker-action");
    assert_eq!(ordered[2].id, "__post_docker-action");
    assert!(matches!(
        &ordered[0].step_type,
        StepType::Action { with, .. }
            if with.get("__aksh_entry").and_then(|v| v.as_str()) == Some("pre-entrypoint.sh")
    ));
    assert!(matches!(
        &ordered[2].step_type,
        StepType::Action { with, .. }
            if with.get("__aksh_entry").and_then(|v| v.as_str()) == Some("post-entrypoint.sh")
    ));
    assert_eq!(ordered[2].condition.as_deref(), Some("always()"));
}

#[derive(Clone, Debug)]
struct LifecycleSpec {
    has_pre: bool,
    has_post: bool,
    explicit_pre_if: bool,
    explicit_post_if: bool,
    supported: bool,
    manifest_present: bool,
    local: bool,
    metadata: String,
    continue_on_error: bool,
    timeout_minutes: Option<u64>,
}

fn lifecycle_config() -> ProptestConfig {
    use proptest::test_runner::{FileFailurePersistence, RngSeed};
    let mut config = ProptestConfig::with_failure_persistence(FileFailurePersistence::default());
    config.cases = 1_000;
    config.rng_seed = RngSeed::Fixed(20260714);
    config.verbose = 1;
    config
}

fn arb_lifecycle_specs() -> impl Strategy<Value = Vec<LifecycleSpec>> {
    proptest::collection::vec(
        (
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            "[a-zA-Z0-9_-]{1,16}",
            any::<bool>(),
            prop::option::of(1u64..=30),
        ),
        0..=5,
    )
    .prop_map(
        |items: Vec<(
            bool,
            bool,
            bool,
            bool,
            bool,
            bool,
            bool,
            String,
            bool,
            Option<u64>,
        )>| {
            items
                .into_iter()
                .map(
                    |(
                        has_pre,
                        has_post,
                        explicit_pre_if,
                        explicit_post_if,
                        supported,
                        manifest_present,
                        local,
                        metadata,
                        continue_on_error,
                        timeout_minutes,
                    )| LifecycleSpec {
                        has_pre,
                        has_post,
                        explicit_pre_if,
                        explicit_post_if,
                        supported,
                        manifest_present,
                        local,
                        metadata,
                        continue_on_error,
                        timeout_minutes,
                    },
                )
                .collect()
        },
    )
}

fn arb_no_lifecycle_specs() -> impl Strategy<Value = Vec<LifecycleSpec>> {
    proptest::collection::vec(
        (
            any::<bool>(),
            any::<bool>(),
            "[a-zA-Z0-9_-]{1,16}",
            any::<bool>(),
            prop::option::of(1u64..=30),
        ),
        0..=5,
    )
    .prop_map(|items: Vec<(bool, bool, String, bool, Option<u64>)>| {
        items
            .into_iter()
            .map(
                |(supported, manifest_present, metadata, continue_on_error, timeout_minutes)| {
                    LifecycleSpec {
                        has_pre: false,
                        has_post: false,
                        explicit_pre_if: false,
                        explicit_post_if: false,
                        supported,
                        manifest_present,
                        local: false,
                        metadata,
                        continue_on_error,
                        timeout_minutes,
                    }
                },
            )
            .collect()
    })
}

fn lifecycle_fixture(
    specs: &[LifecycleSpec],
) -> (
    tempfile::TempDir,
    String,
    std::collections::HashMap<String, String>,
    Vec<Step>,
) {
    let temp = tempfile::TempDir::new().unwrap();
    let workspace = temp.path().join("workspace");
    let mut action_paths = std::collections::HashMap::new();
    let mut main_steps = Vec::with_capacity(specs.len());

    for (index, spec) in specs.iter().enumerate() {
        let uses = if spec.local {
            format!("./.github/actions/generated-{index}")
        } else {
            format!("actions/generated-{index}@v1")
        };
        let action_dir = temp.path().join(format!("action-{index}"));
        if spec.manifest_present {
            std::fs::create_dir_all(&action_dir).unwrap();
            let using = if spec.supported { "node20" } else { "node6" };
            let mut manifest =
                format!("name: generated-{index}\nruns:\n  using: {using}\n  main: main.js\n");
            if spec.has_pre {
                manifest.push_str("  pre: pre.js\n");
                if spec.explicit_pre_if {
                    manifest.push_str("  pre-if: failure()\n");
                }
            }
            if spec.has_post {
                manifest.push_str("  post: post.js\n");
                if spec.explicit_post_if {
                    manifest.push_str("  post-if: cancelled()\n");
                }
            }
            std::fs::write(action_dir.join("action.yml"), manifest).unwrap();
            action_paths.insert(uses.clone(), action_dir.to_string_lossy().to_string());
        } else if spec.supported {
            // An explicit, nonexistent path exercises the missing-manifest branch.
            action_paths.insert(
                uses.clone(),
                temp.path()
                    .join(format!("missing-{index}"))
                    .to_string_lossy()
                    .to_string(),
            );
        }

        let mut env = std::collections::HashMap::new();
        env.insert("META".to_string(), spec.metadata.clone());
        main_steps.push(Step {
            id: format!("step-{index}"),
            context_name: format!("context-{index}"),
            display_name: format!("Generated {index}"),
            step_type: StepType::Action {
                uses,
                with: serde_json::json!({"input": spec.metadata}),
            },
            condition: Some(if spec.continue_on_error {
                "always()".to_string()
            } else {
                "success()".to_string()
            }),
            continue_on_error: spec.continue_on_error,
            timeout_minutes: spec.timeout_minutes,
            env,
            raw: serde_json::json!({"generated": index, "metadata": spec.metadata}),
            is_background: index % 2 == 0,
        });
    }

    (
        temp,
        workspace.to_string_lossy().to_string(),
        action_paths,
        main_steps,
    )
}

fn lifecycle_is_materialized(spec: &LifecycleSpec) -> bool {
    spec.manifest_present && spec.supported
}

fn lifecycle_has_pre(spec: &LifecycleSpec) -> bool {
    lifecycle_is_materialized(spec) && spec.has_pre && !spec.local
}

fn assert_metadata_preserved(source: &Step, generated: &Step, post: bool) {
    assert_eq!(
        generated.context_name,
        format!(
            "__{}{}",
            if post { "post_" } else { "pre_" },
            source.context_name
        )
    );
    assert_eq!(
        generated.display_name,
        format!(
            "{} {}",
            if post { "Post" } else { "Pre" },
            source.display_name
        )
    );
    assert_eq!(generated.timeout_minutes, source.timeout_minutes);
    assert_eq!(generated.env, source.env);
    assert_eq!(generated.is_background, false);
    assert_eq!(
        generated.continue_on_error,
        if post { true } else { source.continue_on_error }
    );
    assert_eq!(
        generated.raw.get(if post { "__post" } else { "__pre" }),
        Some(&serde_json::Value::Bool(true))
    );
    match (&source.step_type, &generated.step_type) {
        (
            StepType::Action {
                uses: source_uses,
                with: source_with,
            },
            StepType::Action {
                uses: generated_uses,
                with: generated_with,
            },
        ) => {
            assert_eq!(generated_uses, source_uses);
            assert_eq!(generated_with.get("input"), source_with.get("input"));
            assert_eq!(
                generated_with
                    .get("__aksh_entry")
                    .and_then(|value| value.as_str()),
                Some(if post { "post.js" } else { "pre.js" })
            );
        }
        _ => panic!("lifecycle step changed action type"),
    }
}

// Oracle: ActionManager.PrepareActionsRecursiveAsync pre/post registration and stack order
// (lines 301-360), plus ActionRunner.RunAsync lifecycle stage registration (lines 79-105).
// https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionManager.cs#L301-L360
// https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionRunner.cs#L79-L110
proptest! {
    #![proptest_config(lifecycle_config())]

    #[test]
    fn lifecycle_order_conditions_and_unique_ids(specs in arb_lifecycle_specs()) {
        let (_temp, workspace, action_paths, main_steps) = lifecycle_fixture(&specs);
        let result = build_step_list_with_lifecycle(main_steps.clone(), &workspace, &action_paths);

        let expected_pre: Vec<String> = specs
            .iter()
            .enumerate()
            .filter(|(_, spec)| lifecycle_has_pre(spec))
            .map(|(i, _)| format!("__pre_step-{i}"))
            .collect();
        let expected_post: Vec<String> = specs
            .iter()
            .enumerate()
            .filter(|(_, spec)| lifecycle_is_materialized(spec) && spec.has_post)
            .map(|(i, _)| format!("__post_step-{i}"))
            .rev()
            .collect();
        let pre_count = expected_pre.len();
        let post_count = expected_post.len();
        prop_assert_eq!(result.len(), pre_count + main_steps.len() + post_count);
        prop_assert_eq!(
            result[..pre_count].iter().map(|step| step.id.clone()).collect::<Vec<_>>(),
            expected_pre,
        );
        prop_assert_eq!(
            result[pre_count..pre_count + main_steps.len()]
                .iter()
                .map(|step| step.id.clone())
                .collect::<Vec<_>>(),
            main_steps.iter().map(|step| step.id.clone()).collect::<Vec<_>>(),
        );
        prop_assert_eq!(
            result[pre_count + main_steps.len()..]
                .iter()
                .map(|step| step.id.clone())
                .collect::<Vec<_>>(),
            expected_post,
        );

        for (position, step) in result.iter().enumerate() {
            if let Some(index) = step.id.strip_prefix("__pre_step-").and_then(|s| s.parse::<usize>().ok()) {
                let spec = &specs[index];
                prop_assert_eq!(step.condition.as_deref(), Some(if spec.explicit_pre_if { "failure()" } else { "always()" }));
            } else if let Some(index) = step.id.strip_prefix("__post_step-").and_then(|s| s.parse::<usize>().ok()) {
                let spec = &specs[index];
                prop_assert_eq!(step.condition.as_deref(), Some(if spec.explicit_post_if { "cancelled()" } else { "always()" }));
            }
            prop_assert!(result[..position].iter().all(|previous| previous.id != step.id));
        }
    }
}

// Oracle: ActionManager.PrepareActionsAsync only tracks resolved actions with lifecycle
// definitions (lines 301-360); ActionRunner.RunAsync preserves action-step metadata while
// selecting a lifecycle Stage (lines 79-105).
// https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionManager.cs#L301-L360
// https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionRunner.cs#L79-L110
proptest! {
    #![proptest_config(lifecycle_config())]

    #[test]
    fn lifecycle_steps_preserve_main_metadata(specs in arb_lifecycle_specs()) {
        let (_temp, workspace, action_paths, main_steps) = lifecycle_fixture(&specs);
        let result = build_step_list_with_lifecycle(main_steps.clone(), &workspace, &action_paths);
        for (index, step) in main_steps.iter().enumerate() {
            if lifecycle_has_pre(&specs[index]) {
                let generated = result.iter().find(|candidate| candidate.id == format!("__pre_step-{index}")).unwrap();
                assert_metadata_preserved(step, generated, false);
            }
            if lifecycle_is_materialized(&specs[index]) && specs[index].has_post {
                let generated = result.iter().find(|candidate| candidate.id == format!("__post_step-{index}")).unwrap();
                assert_metadata_preserved(step, generated, true);
            }
        }
    }

    #[test]
    fn no_lifecycle_is_identity(specs in arb_no_lifecycle_specs()) {
        let (_temp, workspace, action_paths, main_steps) = lifecycle_fixture(&specs);
        let result = build_step_list_with_lifecycle(main_steps.clone(), &workspace, &action_paths);
        prop_assert_eq!(result.len(), main_steps.len());
        for (actual, expected) in result.iter().zip(main_steps.iter()) {
            prop_assert_eq!(&actual.id, &expected.id);
            prop_assert_eq!(&actual.context_name, &expected.context_name);
            prop_assert_eq!(&actual.display_name, &expected.display_name);
            prop_assert_eq!(&actual.condition, &expected.condition);
            prop_assert_eq!(actual.continue_on_error, expected.continue_on_error);
            prop_assert_eq!(actual.timeout_minutes, expected.timeout_minutes);
            prop_assert_eq!(&actual.env, &expected.env);
            prop_assert_eq!(&actual.raw, &expected.raw);
            match (&actual.step_type, &expected.step_type) {
                (StepType::Action { uses: actual_uses, with: actual_with }, StepType::Action { uses: expected_uses, with: expected_with }) => {
                    prop_assert_eq!(actual_uses, expected_uses);
                    prop_assert_eq!(actual_with, expected_with);
                }
                _ => prop_assert!(false, "identity changed action type"),
            }
            prop_assert_eq!(actual.is_background, expected.is_background);
        }
    }
}

// Oracle: ActionManager.PrepareActionsAsync resolves action manifests before lifecycle
// registration and skips actions that cannot provide a definition (lines 301-360).
// https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionManager.cs#L301-L360
#[test]
fn mixed_lifecycle_regression_does_not_invent_steps() {
    let specs = vec![
        LifecycleSpec {
            has_pre: true,
            has_post: true,
            explicit_pre_if: false,
            explicit_post_if: false,
            supported: true,
            manifest_present: true,
            local: false,
            metadata: "supported".into(),
            continue_on_error: false,
            timeout_minutes: Some(7),
        },
        LifecycleSpec {
            has_pre: true,
            has_post: true,
            explicit_pre_if: true,
            explicit_post_if: true,
            supported: false,
            manifest_present: true,
            metadata: "unsupported".into(),
            local: false,
            continue_on_error: true,
            timeout_minutes: None,
        },
        LifecycleSpec {
            has_pre: true,
            has_post: true,
            explicit_pre_if: true,
            explicit_post_if: true,
            supported: true,
            manifest_present: false,
            metadata: "missing".into(),
            local: false,
            continue_on_error: false,
            timeout_minutes: Some(1),
        },
    ];
    let (_temp, workspace, action_paths, main_steps) = lifecycle_fixture(&specs);
    let result = build_step_list_with_lifecycle(main_steps, &workspace, &action_paths);
    assert_eq!(
        result
            .iter()
            .map(|step| step.id.as_str())
            .collect::<Vec<_>>(),
        vec![
            "__pre_step-0",
            "step-0",
            "step-1",
            "step-2",
            "__post_step-0",
        ]
    );
    assert_eq!(result[0].condition.as_deref(), Some("always()"));
    assert_eq!(result[4].condition.as_deref(), Some("always()"));
}

#[test]
fn test_golden_acquirejob_payloads_parsing() {
    let scenarios = &[
        "06-multi-step",
        "08-job-outputs-needs",
        "10-uses-checkout",
        "11-cache-roundtrip",
        "12-artifact",
        "13-composite-action",
        "14-annotations",
        "15-oidc-id-token",
    ];

    for scenario in scenarios {
        let msg = load_golden_acquirejob(scenario)
            .unwrap_or_else(|| panic!("failed to load golden acquirejob for {scenario}"));

        // 1. Build step list from raw steps
        let steps = msg
            .get("steps")
            .and_then(|v| v.as_array())
            .expect("missing steps in golden");
        let parsed_steps = build_step_list(steps, &msg);
        assert!(
            !parsed_steps.is_empty(),
            "parsed steps must not be empty for {scenario}"
        );

        // 2. Inject environment and verify GITHUB_REPOSITORY is parsed
        let mut job = JobContext::new(
            "job1".into(),
            "test-job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        job.workspace = Some("_work/repo/repo".into());
        inject_github_env(&mut job, &msg);

        // GITHUB_REPOSITORY must be set and not empty (from contextData.github.repository)
        let repo = job.env.get("GITHUB_REPOSITORY").map(|s| s.as_str());
        assert_eq!(
            repo,
            Some("preloopdev/aksh-conformance-sample"),
            "mismatched GITHUB_REPOSITORY in {scenario}"
        );

        // GITHUB_TOKEN must be set and not empty
        let token = job.env.get("GITHUB_TOKEN").map(|s| s.as_str());
        assert!(
            token.is_some() && !token.unwrap().is_empty(),
            "GITHUB_TOKEN must not be empty in {scenario}"
        );

        // 3. Scenario-specific checks
        if *scenario == "10-uses-checkout" {
            // Verify actions/checkout has @v4 ref combined
            let checkout_step = parsed_steps
                .iter()
                .find(|s| match &s.step_type {
                    StepType::Action { uses, .. } => uses.starts_with("actions/checkout"),
                    _ => false,
                })
                .expect("missing checkout step");
            if let StepType::Action { uses, .. } = &checkout_step.step_type {
                assert_eq!(uses, "actions/checkout@v4");
            }
        } else if *scenario == "13-composite-action" {
            // Verify local action has repositoryType=self path
            let composite_step = parsed_steps
                .iter()
                .find(|s| match &s.step_type {
                    StepType::Action { uses, .. } => uses.starts_with("./"),
                    _ => false,
                })
                .expect("missing composite step");
            if let StepType::Action { uses, .. } = &composite_step.step_type {
                assert_eq!(uses, "./.github/actions/greet");
            }
        }
    }
}

fn load_golden_acquirejob(scenario: &str) -> Option<serde_json::Value> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    let path = format!("../../.runner-watch/golden/v2.335.1/{scenario}/flows.jsonl");
    let file = File::open(path).ok()?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line.ok()?;
        let d: serde_json::Value = serde_json::from_str(&line).ok()?;
        if d.get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.contains("acquirejob"))
            .unwrap_or(false)
        {
            return d.get("response_body_json").cloned();
        }
    }
    None
}
