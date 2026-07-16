use super::*;
use base64::Engine as _;
use proptest::prelude::*;
use proptest::test_runner::{FileFailurePersistence, RngSeed};

fn masking_config() -> ProptestConfig {
    let mut config = ProptestConfig::with_failure_persistence(
        FileFailurePersistence::SourceParallel("proptest-regressions"),
    );
    config.cases = 1_000;
    config.rng_seed = RngSeed::Fixed(0x5EC2_0260);
    config.verbose = 1;
    config
}

fn arb_secret_core() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            Just('a'),
            Just('b'),
            Just('c'),
            Just('x'),
            Just('y'),
            Just('z'),
            Just('0'),
            Just('7'),
            Just('9'),
        ],
        1..=48,
    )
    .prop_map(|chars| chars.into_iter().collect())
}

fn arb_secret_padding() -> impl Strategy<Value = String> {
    prop::collection::vec(prop_oneof![Just(' '), Just('\t')], 0..=4)
        .prop_map(|chars| chars.into_iter().collect())
}

// JobContext::new registers the raw secret, its trimmed form, and the
// standard/url-safe padded and unpadded Base64 encodings.
proptest! {
    #![proptest_config(masking_config())]

    #[test]
    fn masking_secret_variable_variants_are_redacted(
        core in arb_secret_core(),
        leading in arb_secret_padding(),
        trailing in arb_secret_padding(),
    ) {
        let raw = format!("{leading}{core}{trailing}");
        let trimmed = raw.trim().to_owned();
        let standard = base64::engine::general_purpose::STANDARD.encode(raw.as_bytes());
        let standard_no_pad =
            base64::engine::general_purpose::STANDARD_NO_PAD.encode(raw.as_bytes());
        let url_safe = base64::engine::general_purpose::URL_SAFE.encode(raw.as_bytes());
        let url_safe_no_pad =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes());
        let ctx = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({
                "SECRET": {"value": raw.clone(), "isSecret": true},
                "PUBLIC": {"value": "PUBLIC_VALUE", "isSecret": false},
            }),
            serde_json::json!({}),
        );

        let input = format!(
            "LEFT::{raw}::{trimmed}::{standard}::{standard_no_pad}::{url_safe}::{url_safe_no_pad}::PUBLIC_MARKER"
        );
        let masked = ctx.mask_secrets(&input);
        prop_assert_eq!(
            masked.as_str(),
            "LEFT::***::***::***::***::***::***::PUBLIC_MARKER"
        );
        prop_assert_eq!(ctx.mask_secrets(&masked), masked.as_str());
    }
}

// Replacement order must prevent a shorter mask from exposing a longer
// overlapping secret; masking an already masked string is idempotent.
proptest! {
    #![proptest_config(masking_config())]

    #[test]
    fn masking_overlapping_masks_do_not_leak_and_are_idempotent(
        short in arb_secret_core(),
        suffix in arb_secret_core(),
    ) {
        let long = format!("{short}{suffix}");
        let mut ctx = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({
                "LONG_SECRET": {"value": long.clone(), "isSecret": true},
            }),
            serde_json::json!({}),
        );
        ctx.add_mask(&short);

        let input = format!(
            "LEFT::{short}::MIDDLE::{long}::PUBLIC_MARKER::{short}::{long}::RIGHT"
        );
        let masked = ctx.mask_secrets(&input);
        prop_assert_eq!(
            masked.as_str(),
            "LEFT::***::MIDDLE::***::PUBLIC_MARKER::***::***::RIGHT"
        );
        prop_assert_eq!(ctx.mask_secrets(&masked), masked.as_str());
    }
}

// AddMaskCommandExtension ignores whitespace-only data, matching the
// pinned actions/runner v2.335.1 implementation:
// https://github.com/actions/runner/blob/7d737449ef346f6524f75688d0c9c95fa10ba10a/src/Runner.Worker/ActionCommandManager.cs#L419-L448
proptest! {
    #![proptest_config(masking_config())]

    #[test]
    fn masking_empty_add_masks_are_ignored(
        empty in prop::collection::vec(
            prop_oneof![Just(' '), Just('\t'), Just('\r'), Just('\n')],
            0..=32,
        ).prop_map(|chars| chars.into_iter().collect::<String>()),
    ) {
        let mut ctx = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        ctx.add_mask(&empty);
        let input = format!("LEFT{empty}RIGHT");
        prop_assert_eq!(ctx.mask_secrets(&input), input);
        prop_assert!(ctx.live_masks.read().map(|m| m.is_empty()).unwrap_or(false));
    }
}

// AddMaskCommandExtension registers the complete command data and every
// non-empty CR/LF-delimited, trimmed line (same pinned source as above).
proptest! {
    #![proptest_config(masking_config())]

    #[test]
    fn masking_multiline_add_masks_update_live_masks_and_redact_each_line(
        first in arb_secret_core(),
        second in arb_secret_core(),
    ) {
        let raw = format!(" \t{first}\r\n\n\t{second} \r");
        let mut ctx = JobContext::new(
            "job".into(),
            "Job".into(),
            serde_json::json!({}),
            serde_json::json!({}),
        );
        ctx.add_mask(&raw);

        let live_has_masks = ctx.live_masks.read().map(|live| {
            live.contains(&raw) && live.contains(&first) && live.contains(&second)
        }).unwrap_or(false);
        prop_assert!(live_has_masks);

        let input = format!("LEFT::{raw}::{first}::{second}::PUBLIC_MARKER");
        prop_assert_eq!(
            ctx.mask_secrets(&input),
            "LEFT::***::***::***::PUBLIC_MARKER"
        );
    }
}

fn make_variables() -> serde_json::Value {
    serde_json::json!({
        "system.github.token": {
            "value": "ghp_secret123",
            "isSecret": true
        },
        "ACTIONS_RUNTIME_URL": {
            "value": "https://results.actions.githubusercontent.com",
            "isSecret": false
        }
    })
}

#[test]
fn new_extracts_masks_from_secret_variables() {
    let ctx = JobContext::new(
        "job1".into(),
        "Test Job".into(),
        make_variables(),
        serde_json::json!({}),
    );
    assert!(ctx.masks.contains("ghp_secret123"));
    assert!(!ctx
        .masks
        .contains("https://results.actions.githubusercontent.com"));
}

#[test]
fn mask_secrets_replaces_with_stars() {
    let ctx = JobContext::new(
        "job1".into(),
        "Test Job".into(),
        make_variables(),
        serde_json::json!({}),
    );
    let masked = ctx.mask_secrets("Token is ghp_secret123 here");
    assert_eq!(masked, "Token is *** here");
    assert!(!masked.contains("ghp_secret123"));
}

#[test]
fn add_mask_adds_new_secret() {
    let mut ctx = JobContext::new(
        "job1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    ctx.add_mask("my-password");
    assert!(ctx.masks.contains("my-password"));
    assert_eq!(ctx.mask_secrets("my-password is set"), "*** is set");
}

#[test]
fn add_mask_ignores_empty() {
    let mut ctx = JobContext::new(
        "job1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    ctx.add_mask("");
    assert!(ctx.masks.is_empty());
}

#[test]
fn get_variable_returns_value() {
    let ctx = JobContext::new(
        "job1".into(),
        "Test".into(),
        make_variables(),
        serde_json::json!({}),
    );
    assert_eq!(
        ctx.get_variable("ACTIONS_RUNTIME_URL"),
        Some("https://results.actions.githubusercontent.com")
    );
    assert_eq!(ctx.get_variable("nonexistent"), None);
}

#[test]
fn build_expression_context_has_required_roots() {
    let mut ctx = JobContext::new(
        "job1".into(),
        "Test Job".into(),
        serde_json::json!({}),
        serde_json::json!({
            "github": {
                "repository": "test/repo",
                "ref": "refs/heads/main"
            }
        }),
    );
    ctx.env.insert("MY_VAR".into(), "hello".into());
    ctx.steps.insert(
        "step1".into(),
        StepResult {
            outcome: "Success".into(),
            conclusion: "Success".into(),
            outputs: HashMap::from([("result".into(), "42".into())]),
        },
    );

    let expr_ctx = ctx.build_expression_context();
    // Verify github context resolves
    let val = aksh_gha_expressions::eval_expression("github.repository", &expr_ctx);
    assert!(val.is_ok());

    // Verify steps context resolves
    let steps_val = aksh_gha_expressions::eval_expression("steps.step1.conclusion", &expr_ctx);
    assert!(steps_val.is_ok());

    // Verify success() evaluates correctly
    let success = aksh_gha_expressions::eval_bool("success()", &expr_ctx);
    assert!(success.unwrap());
}

#[test]
fn job_status_failure_reflects_in_context() {
    let mut ctx = JobContext::new(
        "job1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    ctx.job_status = JobStatus::Failure;

    let expr_ctx = ctx.build_expression_context();
    let success = aksh_gha_expressions::eval_bool("success()", &expr_ctx).unwrap();
    assert!(!success);
    let failure = aksh_gha_expressions::eval_bool("failure()", &expr_ctx).unwrap();
    assert!(failure);
}

#[test]
fn set_github_context_value_updates_context_and_env() {
    let mut job = JobContext::new(
        "j1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({"github": {"repository": "owner/repo"}}),
    );

    job.set_github_context_value(
        "action_repository",
        Some(serde_json::json!("actions/checkout")),
    );
    job.set_github_context_value("action_ref", Some(serde_json::json!("v4")));

    let expr = job.build_expression_context();
    assert_eq!(
        expr.resolve(&["github".to_string(), "action_repository".to_string()])
            .as_str(),
        Some("actions/checkout")
    );
    assert_eq!(
        expr.resolve(&["github".to_string(), "action_ref".to_string()])
            .as_str(),
        Some("v4")
    );
    assert_eq!(
        job.env.get("GITHUB_ACTION_REPOSITORY").map(String::as_str),
        Some("actions/checkout")
    );
    assert_eq!(
        job.env.get("GITHUB_ACTION_REF").map(String::as_str),
        Some("v4")
    );

    job.set_github_context_value("action_repository", Some(serde_json::Value::Null));
    assert!(!job.env.contains_key("GITHUB_ACTION_REPOSITORY"));
}

#[test]
fn vars_context_decodes_typed_dict_format() {
    // GitHub sends contextData.vars in Azure DevOps typed-dictionary format:
    // {"t": 2, "d": [{"k": "AKSH_REPO_ROOT", "v": {"t": 1, "d": "/workspace"}}]}
    let ctx = JobContext::new(
        "j1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({
            "github": {"repository": "owner/repo"},
            "vars": {
                "t": 2,
                "d": [
                    {"k": "AKSH_REPO_ROOT", "v": {"t": 1, "d": "/workspace"}},
                    {"k": "OTHER_VAR", "v": {"t": 1, "d": "hello"}}
                ]
            }
        }),
    );

    let expr = ctx.build_expression_context();
    let val = aksh_gha_expressions::eval_expression("vars.AKSH_REPO_ROOT", &expr);
    assert_eq!(val.unwrap().as_str(), Some("/workspace"));
    let val2 = aksh_gha_expressions::eval_expression("vars.OTHER_VAR", &expr);
    assert_eq!(val2.unwrap().as_str(), Some("hello"));
}

#[test]
fn variables_case_insensitive_and_edge_cases() {
    let variables = serde_json::json!({
        "MY_VAR": {"value": "hello", "isSecret": false},
        "secret_var": {"value": "secret123", "isSecret": true},
        "NULL_VAR": {"value": null, "isSecret": false},
        "": {"value": "skipped", "isSecret": false}
    });

    let ctx = JobContext::new(
        "job1".into(),
        "Test Job".into(),
        variables,
        serde_json::json!({}),
    );

    // Case-insensitive lookup
    assert_eq!(ctx.get_variable("MY_VAR"), Some("hello"));
    assert_eq!(ctx.get_variable("my_var"), Some("hello"));
    assert_eq!(ctx.get_variable("My_Var"), Some("hello"));

    // Null variable sets null/empty as empty string ""
    assert_eq!(ctx.get_variable("NULL_VAR"), Some(""));
    assert_eq!(ctx.get_variable("null_var"), Some(""));

    // Empty name is ignored/skipped (or at least cannot be looked up)
    assert_eq!(ctx.get_variable(""), None);

    // Missing returns None
    assert_eq!(ctx.get_variable("MISSING_VAR"), None);
}

#[test]
fn variables_get_boolean_does_not_throw_when_null() {
    let variables = serde_json::json!({
        "TRUE_VAR": {"value": "true", "isSecret": false},
        "FALSE_VAR": {"value": "false", "isSecret": false},
        "NULL_VAR": {"value": null, "isSecret": false}
    });

    let ctx = JobContext::new(
        "job1".into(),
        "Test Job".into(),
        variables,
        serde_json::json!({}),
    );

    assert!(ctx.get_variable_bool("TRUE_VAR"));
    assert!(ctx.get_variable_bool("true_var"));
    assert!(!ctx.get_variable_bool("FALSE_VAR"));
    assert!(!ctx.get_variable_bool("NULL_VAR"));
    assert!(!ctx.get_variable_bool("MISSING_VAR"));
}

// --- JobContextL0 gap coverage ---

#[test]
fn set_github_context_value_clears_on_none() {
    let mut job = JobContext::new(
        "j1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({"github": {"repository": "owner/repo"}}),
    );

    // Set workflow_ref
    job.set_github_context_value(
        "workflow_ref",
        Some(serde_json::json!(
            "owner/repo/.github/workflows/ci.yml@refs/heads/main"
        )),
    );
    assert_eq!(
        job.github_context_value("workflow_ref")
            .and_then(|v| v.as_str().map(String::from)),
        Some("owner/repo/.github/workflows/ci.yml@refs/heads/main".to_string())
    );
    assert_eq!(
        job.env.get("GITHUB_WORKFLOW_REF").map(String::as_str),
        Some("owner/repo/.github/workflows/ci.yml@refs/heads/main")
    );

    // Clear it
    job.set_github_context_value("workflow_ref", None);
    assert!(job.github_context_value("workflow_ref").is_none());
    assert!(!job.env.contains_key("GITHUB_WORKFLOW_REF"));
}

#[test]
fn set_github_context_value_workflow_identity_fields() {
    let mut job = JobContext::new(
        "j1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({"github": {"repository": "owner/repo"}}),
    );

    // Set all workflow identity fields
    job.set_github_context_value(
        "workflow_ref",
        Some(serde_json::json!(
            "owner/repo/.github/workflows/ci.yml@refs/heads/main"
        )),
    );
    job.set_github_context_value("workflow_sha", Some(serde_json::json!("abc123def456")));
    job.set_github_context_value("workflow", Some(serde_json::json!("CI")));

    // Verify all set
    assert_eq!(
        job.github_context_value("workflow_ref")
            .and_then(|v| v.as_str().map(String::from)),
        Some("owner/repo/.github/workflows/ci.yml@refs/heads/main".to_string())
    );
    assert_eq!(
        job.github_context_value("workflow_sha")
            .and_then(|v| v.as_str().map(String::from)),
        Some("abc123def456".to_string())
    );
    assert_eq!(
        job.github_context_value("workflow")
            .and_then(|v| v.as_str().map(String::from)),
        Some("CI".to_string())
    );

    // Verify env synced
    assert_eq!(
        job.env.get("GITHUB_WORKFLOW_REF").map(String::as_str),
        Some("owner/repo/.github/workflows/ci.yml@refs/heads/main")
    );
    assert_eq!(
        job.env.get("GITHUB_WORKFLOW_SHA").map(String::as_str),
        Some("abc123def456")
    );
    assert_eq!(
        job.env.get("GITHUB_WORKFLOW").map(String::as_str),
        Some("CI")
    );

    // Clear all
    job.set_github_context_value("workflow_ref", None);
    job.set_github_context_value("workflow_sha", None);
    job.set_github_context_value("workflow", None);

    assert!(job.github_context_value("workflow_ref").is_none());
    assert!(job.github_context_value("workflow_sha").is_none());
    assert!(job.github_context_value("workflow").is_none());
    assert!(!job.env.contains_key("GITHUB_WORKFLOW_REF"));
    assert!(!job.env.contains_key("GITHUB_WORKFLOW_SHA"));
    assert!(!job.env.contains_key("GITHUB_WORKFLOW"));
}

#[test]
fn cancelled_status_reflects_in_context() {
    let mut ctx = JobContext::new(
        "job1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    ctx.job_status = JobStatus::Cancelled;

    let expr_ctx = ctx.build_expression_context();
    assert!(!aksh_gha_expressions::eval_bool("success()", &expr_ctx).unwrap());
    assert!(!aksh_gha_expressions::eval_bool("failure()", &expr_ctx).unwrap());
    assert!(aksh_gha_expressions::eval_bool("cancelled()", &expr_ctx).unwrap());
    assert!(aksh_gha_expressions::eval_bool("always()", &expr_ctx).unwrap());
}

// --- P1 expressions/templates gap coverage ---

#[test]
fn matrix_context_resolves_in_expressions() {
    let ctx = JobContext::new(
        "job1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({
            "github": {"repository": "owner/repo"},
            "matrix": {"os": "ubuntu-latest", "node": "20"}
        }),
    );

    let expr_ctx = ctx.build_expression_context();
    assert_eq!(
        aksh_gha_expressions::eval_expression("matrix.os", &expr_ctx)
            .unwrap()
            .as_str(),
        Some("ubuntu-latest")
    );
    assert_eq!(
        aksh_gha_expressions::eval_expression("matrix.node", &expr_ctx)
            .unwrap()
            .as_str(),
        Some("20")
    );
}

#[test]
fn needs_context_resolves_in_expressions() {
    let ctx = JobContext::new(
        "job1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({
            "github": {"repository": "owner/repo"},
            "needs": {
                "build": {
                    "result": "success",
                    "outputs": {"sha": "abc123", "version": "1.2.3"}
                }
            }
        }),
    );

    let expr_ctx = ctx.build_expression_context();
    assert_eq!(
        aksh_gha_expressions::eval_expression("needs.build.result", &expr_ctx)
            .unwrap()
            .as_str(),
        Some("success")
    );
    assert_eq!(
        aksh_gha_expressions::eval_expression("needs.build.outputs.sha", &expr_ctx)
            .unwrap()
            .as_str(),
        Some("abc123")
    );
}

#[test]
fn strategy_context_resolves_in_expressions() {
    let ctx = JobContext::new(
        "job1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({
            "github": {"repository": "owner/repo"},
            "strategy": {"fail-fast": true, "max-parallel": 2}
        }),
    );

    let expr_ctx = ctx.build_expression_context();
    assert!(aksh_gha_expressions::eval_bool("strategy.fail-fast", &expr_ctx).unwrap());
}

#[test]
fn env_context_resolves_in_expressions() {
    let mut ctx = JobContext::new(
        "job1".into(),
        "Test".into(),
        serde_json::json!({}),
        serde_json::json!({}),
    );
    ctx.env.insert("MY_VAR".into(), "hello".into());
    ctx.env.insert("OTHER".into(), "world".into());

    let expr_ctx = ctx.build_expression_context();
    assert_eq!(
        aksh_gha_expressions::eval_expression("env.MY_VAR", &expr_ctx)
            .unwrap()
            .as_str(),
        Some("hello")
    );
    assert_eq!(
        aksh_gha_expressions::eval_expression("env.OTHER", &expr_ctx)
            .unwrap()
            .as_str(),
        Some("world")
    );
}

#[test]
fn secrets_context_resolves_in_expressions() {
    let ctx = JobContext::new(
        "job1".into(),
        "Test".into(),
        serde_json::json!({
            "system.github.token": {"value": "ghp_tok", "isSecret": true},
            "MY_SECRET": {"value": "s3cr3t", "isSecret": true}
        }),
        serde_json::json!({}),
    );

    let expr_ctx = ctx.build_expression_context();
    assert_eq!(
        aksh_gha_expressions::eval_expression("secrets.MY_SECRET", &expr_ctx)
            .unwrap()
            .as_str(),
        Some("s3cr3t")
    );
}
