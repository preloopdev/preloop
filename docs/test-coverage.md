# Verified C# Runner → aksh-runner Test Coverage

This document replaces the earlier fuzzy `docs/test-coverage.md` mapping. It was rebuilt from source and verified against `cargo test -p aksh-runner --lib -- --list`.

Scope: official C# runner tests under `/tmp/actions-runner-src/src/Test/L0` compared against **aksh-runner crate tests only** (`crates/aksh-runner/src`). Parser/server/protocol tests are not counted as runner tests unless explicitly noted as outside-runner coverage.

## Verified totals

- Official C# L0 tests extracted: **842**
- Official C# test files: **82**
- aksh-runner lib tests passing: **117** (`cargo test -p aksh-runner --lib --quiet`, 2026-07-05)
- aksh-runner test files: **18**

## Current P0 verification status

- Focused P0 suites passed on current code: steps runner, file commands, matchers, action manifest factory, composite actions, container ops, and Docker action handler.
- Formatting and full runner library suite passed on current code: `cargo fmt --all --check && cargo test -p aksh-runner --lib --quiet` => **117 tests passed**.
- Local broker E2E smoke passed with `aksh-runner` + `aksh-runner-server` + `aksh-runner-client`: `runner-e2e --workflow crates/aksh-conformance/fixtures/hello-world.yml` returned `{"success": true, "status": "success"}` and recorded `/tmp/smoke-flows.jsonl`.
- Official-vs-aksh replay comparison now runs for golden scenario `06-multi-step`: `runner-watch conform --runner v2.335.1 --aksh-url http://127.0.0.1:9090 --scenario 06-multi-step --skip-cargo-test` produced `.runner-watch/conformance/v2.335.1/06-multi-step.md` with **42 official filtered flows** and **42 aksh flows**; the aggregate gate reports all status-checked flows matched recorded baseline responses.
- `aksh-conformance runner-diff --scenario 06-multi-step --target aksh` also produced `.runner-watch/runner-conformance/06-multi-step.md`. Remaining report diffs are protocol-shape caveats, not runner-step P0 failures: e.g. OAuth replay status differences are explicitly excluded because official GitHub rejects static job-scoped assertions while aksh accepts local credentials, and `connectionData` still has schema/value differences.

## Live GitHub + local aksh verification addendum — 2026-07-05

Live verification uses `aksh-runner` against **real GitHub Actions** as the primary gate, then runs the same workflow fixtures against local `aksh-runner-server` where the harness can model the scenario.

| Fixture / behavior bucket | GitHub run | GitHub result | Local aksh `runner-e2e` result | Notes |
|---|---:|---|---|---|
| `p0-step-execution.yml` | [`28754418659`](https://github.com/preloopdev/aksh-conformance-sample/actions/runs/28754418659) | success | pass | Sequential steps, `$GITHUB_ENV`, `$GITHUB_OUTPUT`, step-env override, `continue-on-error`, `success()`, step summary. |
| `p0-failure-conditions.yml` | [`28754419325`](https://github.com/preloopdev/aksh-conformance-sample/actions/runs/28754419325) | expected failure | pass | Intentional failure verifies `success()` skip, `failure()` run, `always()` run, and final failed job result. |
| `p0-file-commands.yml` | [`28755293879`](https://github.com/preloopdev/aksh-conformance-sample/actions/runs/28755293879) | success | pass | `NODE_OPTIONS` block, heredoc env parsing, summary size cap, secret/matcher output behavior. |
| P0 Docker/container workflow | [`28755911596`](https://github.com/preloopdev/aksh-conformance-sample/actions/runs/28755911596) | success | not run locally | Requires Linux Docker daemon; later smolvm reruns failed at `docker pull` because VM DNS/storage setup could not reach Docker Hub, not because runner command construction failed. |
| `p0-cancel-semantics.yml` | [`28756327702`](https://github.com/preloopdev/aksh-conformance-sample/actions/runs/28756327702) | cancelled | not run locally | External GitHub cancellation was required; runner logs showed `cancelled()` and `always()` steps ran after cancellation while `success()` skipped. |
| `p1-expressions.yml` | [`28756574650`](https://github.com/preloopdev/aksh-conformance-sample/actions/runs/28756574650) | success | pass | Template functions, expression env fields, true/false conditions, string comparisons. |
| `p1-listener-config.yml` | [`28756828143`](https://github.com/preloopdev/aksh-conformance-sample/actions/runs/28756828143) | success | pass after local context fix | Configure, OAuth, broker session, job acquire, worker dispatch, context env, and completion. The local aksh divergence was fixed by populating `github.workflow` so `GITHUB_WORKFLOW` is non-empty; fixed local flow: `/tmp/aksh-p1-listener-config-fixed-flows.jsonl`. |
| `p1-process-runtime.yml` | [`28756827413`](https://github.com/preloopdev/aksh-conformance-sample/actions/runs/28756827413) | success | pass | stdout/stderr, cwd, env, exit-code failure under `continue-on-error`, timeout field, long output. |
| `p1-protocol.yml` | [`28756578118`](https://github.com/preloopdev/aksh-conformance-sample/actions/runs/28756578118) | success | pass | Warning/error/notice annotations, groups, debug command, multiline log upload, step/job completion. |

P2/P3 classification is explicit: DAP/debugging and background/wait/snapshot execution are **not implemented** in `aksh-runner`; P3 Windows service, self-update, .NET bootstrapper, and official-runner infrastructure remain **NOT_APPLICABLE/DEFERRED** for the macOS/Linux Rust runner target.

## Status definitions

- **PARTIAL** — aksh-runner has tests for the same behavior family, but not every official edge case.
- **GAP** — no verified aksh-runner test covers this behavior family.
- **OUTSIDE_RUNNER** — behavior belongs to parser/protocol/server crates rather than `aksh-runner`; not counted as runner coverage here.
- **NOT_APPLICABLE** — official-runner behavior is platform/runtime infrastructure not relevant to aksh target.

## Summary by official test file

| Official C# test file | C# tests | Status | Official behavior | Verified aksh-runner test refs |
|---|---:|---|---|---|
| `L0/CommandLineParserL0.cs` | 5 | **PARTIAL** | CLI command parsing and arg validation | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_run_azdo` (`crates/aksh-runner/src/cli.rs`:188), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211), `global_ca_bundle_arg` (`crates/aksh-runner/src/cli.rs`:228) |
| `L0/ConstantGenerationL0.cs` | 1 | **GAP** | Constant generation parity not tested | — |
| `L0/Container/ContainerInfoL0.cs` | 1 | **PARTIAL** | Container mapping/string parsing roughly covered | `parse_container_string` (`crates/aksh-runner/src/worker/container_ops.rs`:878), `parse_container_mapping` (`crates/aksh-runner/src/worker/container_ops.rs`:886) |
| `L0/Container/DockerUtilL0.cs` | 3 | **PARTIAL** | Docker image sanitization/name formatting covered; DockerUtil shell/path helpers partial | `sanitize_image` (`crates/aksh-runner/src/worker/container_ops.rs`:856), `container_naming` (`crates/aksh-runner/src/worker/container_ops.rs`:864), `action_container_naming` (`crates/aksh-runner/src/worker/container_ops.rs`:872), `network_name_format` (`crates/aksh-runner/src/worker/container_ops.rs`:927) |
| `L0/DistributedTask/WebApi/TimelineRecordL0.cs` | 10 | **OUTSIDE_RUNNER** | Timeline DTO behavior belongs to protocol/server surface, not aksh-runner tests | — |
| `L0/DotnetsdkDownloadScriptL0.cs` | 2 | **NOT_APPLICABLE** | Official runner .NET SDK bootstrap script not relevant to Rust runner runtime | — |
| `L0/ExtensionManagerL0.cs` | 2 | **GAP** | Extension manager behavior not tested | — |
| `L0/HostContextL0.cs` | 8 | **GAP** | HostContext service registry/tracing not mirrored/tested | — |
| `L0/Listener/BrokerMessageListenerL0.cs` | 7 | **GAP** | Listener broker polling not unit-tested in aksh-runner | — |
| `L0/Listener/CommandSettingsL0.cs` | 30 | **PARTIAL** | CLI parse tests cover some command settings, but official interactive settings matrix mostly gaps | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `L0/Listener/Configuration/ArgumentValidatorTestsL0.cs` | 4 | **GAP** | Argument validator rules not directly tested | — |
| `L0/Listener/Configuration/ConfigurationManagerL0.cs` | 7 | **PARTIAL** | Settings round-trip/config lifecycle covered; registration manager flows gaps | `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204), `config_lifecycle` (`crates/aksh-runner/src/settings.rs`:263), `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `strip_bom_works` (`crates/aksh-runner/src/settings.rs`:236) |
| `L0/Listener/Configuration/NativeWindowsServiceHelperL0.cs` | 2 | **NOT_APPLICABLE** | Windows service helper not relevant to macOS/Linux aksh target | — |
| `L0/Listener/Configuration/PromptManagerTestsL0.cs` | 7 | **GAP** | Interactive prompt manager not tested | — |
| `L0/Listener/Configuration/RunnerCredentialL0.cs` | 2 | **PARTIAL** | RSA field names and settings serialization covered; credential scheme flows partial | `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204) |
| `L0/Listener/ErrorThrottlerL0.cs` | 3 | **GAP** | Error throttling not tested | — |
| `L0/Listener/JobDispatcherL0.cs` | 12 | **GAP** | Listener job dispatcher not unit-tested in aksh-runner | — |
| `L0/Listener/MessageListenerL0.cs` | 9 | **GAP** | Legacy message listener not unit-tested in aksh-runner | — |
| `L0/Listener/RunnerConfigUpdaterTests.cs` | 17 | **GAP** | Runner self config update flow not tested | — |
| `L0/Listener/RunnerL0.cs` | 12 | **GAP** | Runner listener lifecycle not unit-tested in aksh-runner | — |
| `L0/Listener/SelfUpdaterL0.cs` | 4 | **GAP** | Self-update flow not tested | — |
| `L0/Listener/SelfUpdaterV2L0.cs` | 3 | **GAP** | Self-update v2 flow not tested | — |
| `L0/PagingLoggerL0.cs` | 2 | **GAP** | Paging logger behavior not tested | — |
| `L0/ProcessExtensionL0.cs` | 1 | **GAP** | Process extension helpers not tested | — |
| `L0/ProcessInvokerL0.cs` | 12 | **PARTIAL** | Process cancellation signal sequence only; most process IO/lifecycle cases are gaps | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `L0/RunnerWebProxyL0.cs` | 18 | **GAP** | Runner proxy config/env/no_proxy behavior not tested | — |
| `L0/Sdk/ExpressionParserL0.cs` | 4 | **OUTSIDE_RUNNER** | Expression parser equivalent is aksh-gha-expressions, not aksh-runner | — |
| `L0/Sdk/LaunchWebApi/LaunchHttpClientL0.cs` | 2 | **GAP** | Launch client behavior not tested | — |
| `L0/Sdk/RSWebApi/AcquireJobRequestL0.cs` | 2 | **OUTSIDE_RUNNER** | Equivalent protocol/client behavior belongs to protocol/client crates | — |
| `L0/Sdk/RSWebApi/AgentJobRequestMessageL0.cs` | 8 | **OUTSIDE_RUNNER** | Equivalent protocol tests live outside aksh-runner crate, not counted here | — |
| `L0/Sdk/RSWebApi/AnnotationsL0.cs` | 3 | **PARTIAL** | Step annotations tested in execution context, but RSWebApi DTO conversion not in runner | `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288) |
| `L0/Sdk/RSWebApi/RunServiceHttpClientL0.cs` | 1 | **GAP** | Run service client HTTP behavior not unit-tested in aksh-runner | — |
| `L0/Sdk/WellKnownRegularExpressionsL0.cs` | 9 | **GAP** | Well-known regex constants not mirrored/tested | — |
| `L0/ServiceControlManagerL0.cs` | 5 | **NOT_APPLICABLE** | Windows service control manager not relevant to aksh target | — |
| `L0/ServiceInterfacesL0.cs` | 3 | **GAP** | Service interface validation not mirrored | — |
| `L0/Util/ArgUtilL0.cs` | 7 | **GAP** | Arg utility rules not mirrored directly | — |
| `L0/Util/IOUtilL0.cs` | 22 | **GAP** | IO utility behavior not mirrored directly | — |
| `L0/Util/StringUtilL0.cs` | 6 | **GAP** | String utility behavior not mirrored directly | — |
| `L0/Util/TaskResultUtilL0.cs` | 2 | **GAP** | Task result merge utility not directly tested | — |
| `L0/Util/UrlUtilL0.cs` | 5 | **GAP** | URL utility behavior not directly tested | — |
| `L0/Util/VssUtilL0.cs` | 1 | **GAP** | VSS utility behavior not directly tested | — |
| `L0/Util/WhichUtilL0.cs` | 7 | **GAP** | which/path search utility behavior not directly tested | — |
| `L0/Worker/ActionCommandL0.cs` | 2 | **PARTIAL** | Workflow command string parser | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_command_with_properties` (`crates/aksh-runner/src/worker/commands.rs`:240), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `parse_legacy_format` (`crates/aksh-runner/src/worker/commands.rs`:257), `parse_case_insensitive` (`crates/aksh-runner/src/worker/commands.rs`:263), `unescape_data_values` (`crates/aksh-runner/src/worker/commands.rs`:271), `unescape_property_values` (`crates/aksh-runner/src/worker/commands.rs`:277), `not_a_command` (`crates/aksh-runner/src/worker/commands.rs`:283) |
| `L0/Worker/ActionCommandManagerL0.cs` | 14 | **PARTIAL** | Command parsing and handler effects: add-mask, error/warning/notice annotations, group/endgroup, echo on/off, stop-commands token | `parse_simple_command`, `parse_add_mask`, `set_output_legacy`, `mask_secrets_replaces_with_stars`, `add_mask_adds_new_secret`, `handle_add_mask_adds_to_masks`, `handle_error_creates_annotation`, `handle_warning_creates_annotation`, `handle_notice_creates_annotation`, `handle_group_endgroup_logging`, `handle_echo_on_off`, `handle_stop_commands_via_log` |
| `L0/Worker/ActionManagerL0.cs` | 57 | **PARTIAL** | Action repository context, remote action path resolution (with subpath, cached path), action context setting, error cases (missing @ref, invalid format) | `action_repository_context_extracts_repository_and_ref`, `action_repository_context_is_empty_for_local_and_docker_actions`, `resolve_remote_action_constructs_path`, `resolve_remote_action_with_subpath`, `resolve_remote_action_missing_ref_errors`, `resolve_remote_action_invalid_format_errors`, `resolve_remote_action_uses_cached_path`, `set_action_repository_context_sets_fields`, `set_action_repository_context_clears_for_local`, `build_step_list_parses_action_reference` |
| `L0/Worker/ActionManifestManagerL0.cs` | 25 | **PARTIAL** | Manifest parsing: node/composite/docker, lifecycle conditions, DockerHub image, env map, inputs+outputs, conditional steps, action.yml precedence, error cases | `load_node_action_manifest`, `load_composite_action_manifest`, `load_docker_action_manifest`, `lifecycle_conditions_default_to_always_when_entrypoints_exist`, `lifecycle_conditions_absent_without_entrypoints`, `load_docker_action_manifest_with_dockerhub_image_and_optional_fields_absent`, `action_yml_takes_precedence_over_action_yaml`, `missing_runs_using_returns_error`, `missing_manifest_returns_error`, `empty_runs_using_returns_error`, `manifest_with_env_map`, `composite_manifest_with_inputs_and_outputs`, `composite_manifest_with_conditional_steps` |
| `L0/Worker/ActionManifestManagerLegacyL0.cs` | 24 | **NOT_APPLICABLE** | Legacy action manifest parser — aksh uses a single modern YAML parser, no legacy compat layer | — |
| `L0/Worker/ActionManifestParserComparisonL0.cs` | 8 | **NOT_APPLICABLE** | Legacy/new parser comparison telemetry — no legacy parser in aksh | — |
| `L0/Worker/ActionRunnerL0.cs` | 13 | **PARTIAL** | Display-name and action reference parsing only partially covered by job-extension tests | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `L0/Worker/BackgroundStepsL0.cs` | 10 | **GAP** | Background/wait/cancel concurrent step semantics not implemented/tested | — |
| `L0/Worker/ContainerOperationProviderL0.cs` | 5 | **PARTIAL** | Docker/container command construction, naming, path translation, options splitting, proxy injection, TemplateToken decoding, service spec parsing, docker exec arg construction, env hiding | `parse_container_string`, `parse_container_mapping`, `parse_services`, `docker_create_env_uses_inherit_form_for_empty_values`, `docker_exec_env_args_do_not_include_secret_values`, `path_translation`, `sanitize_image`, `container_naming`, `action_container_naming`, `network_name_format`, `label_is_6_hex`, `non_empty_services_omits_empty`, `split_options_handles_quotes`, `translate_to_container_path_various`, `parse_container_spec_with_template_token`, `parse_service_specs_with_template_tokens`, `docker_exec_args_include_workdir_and_env`, `parse_container_spec_string_with_tag`, `parse_container_spec_full_mapping`, `proxy_env_injection`, `proxy_env_not_injected_when_user_sets` |
| `L0/Worker/CreateStepSummaryCommandL0.cs` | 7 | **PARTIAL** | GITHUB_STEP_SUMMARY upload/scrub/size-limit behavior | `test_step_summary_size_limit_and_scrubbing` (`crates/aksh-runner/src/worker/steps_runner.rs`:1177) |
| `L0/Worker/DapDebuggerL0.cs` | 37 | **GAP** | DAP debugger not implemented/tested | — |
| `L0/Worker/DapMessagesL0.cs` | 13 | **GAP** | DAP protocol messages not implemented/tested | — |
| `L0/Worker/DapReplExecutorL0.cs` | 15 | **GAP** | DAP REPL executor not implemented/tested | — |
| `L0/Worker/DapReplParserL0.cs` | 22 | **GAP** | DAP REPL parser not implemented/tested | — |
| `L0/Worker/DapVariableProviderL0.cs` | 26 | **GAP** | DAP variable provider not implemented/tested | — |
| `L0/Worker/ExecutionContextL0.cs` | 24 | **PARTIAL** | Annotations cap/collection/message-trim, env merge, logs, masking, debug multiline split, post state covered; telemetry/result edge cases are intentional gaps (not in Rust runner scope) | `annotations_cap_enforced`, `annotations_collected`, `annotation_message_trimmed_to_max_length`, `build_env_includes_extra_path`, `build_env_merges_job_and_step`, `log_content_joins_lines`, `log_masks_secrets`, `debug_splits_multiline_messages`, `debug_single_line_unchanged`, `debug_noop_when_disabled`, `post_step_env_exposes_saved_state_from_main_step`, `log_raw_problem_matching_and_telemetry` |
| `L0/Worker/Expressions/ConditionFunctionsL0.cs` | 4 | **FULL** | All four condition functions tested in isolation across all state combinations | `condition_always_returns_true_regardless_of_status`, `condition_success_true_only_when_success_flag_set`, `condition_failure_true_only_when_failure_flag_set`, `condition_cancelled_true_only_when_cancelled_flag_set`, `condition_functions_combined_state`, `status_functions_use_context_state` |
| `L0/Worker/HandlerFactoryL0.cs` | 15 | **PARTIAL** | Manifest loading dispatch, lifecycle conditions, DockerHub image, env map, composite with inputs/outputs, conditional steps, action.yml precedence, error cases | `load_node_action_manifest`, `load_composite_action_manifest`, `load_docker_action_manifest`, `lifecycle_conditions_default_to_always_when_entrypoints_exist`, `lifecycle_conditions_absent_without_entrypoints`, `load_docker_action_manifest_with_dockerhub_image_and_optional_fields_absent`, `action_yml_takes_precedence_over_action_yaml`, `missing_runs_using_returns_error`, `missing_manifest_returns_error`, `empty_runs_using_returns_error`, `manifest_with_env_map`, `composite_manifest_with_inputs_and_outputs`, `composite_manifest_with_conditional_steps` |
| `L0/Worker/HandlerL0.cs` | 2 | **NOT_APPLICABLE** | PrepareExecution telemetry population — not in Rust runner scope (telemetry N/A) | — |
| `L0/Worker/Handlers/CompositeActionHandlerL0.cs` | 23 | **PARTIAL** | Composite action_status context, input/default mapping, output evaluation, failure stop, nesting depth limit, nested uses dispatch, input+output integration | `composite_steps_receive_action_status_context`, `composite_maps_with_inputs_and_manifest_defaults_to_input_env`, `composite_evaluates_outputs_from_nested_step_outputs`, `composite_stops_after_nested_step_failure`, `composite_enforces_nesting_depth_limit`, `composite_nested_uses_dispatches_inner_action`, `composite_output_captures_from_script_step` |
| `L0/Worker/Handlers/NodeHandlerL0.cs` | 1 | **PARTIAL** | Node action handler: missing entry point error, missing runs.main error | `missing_entry_point_errors`, `missing_runs_main_errors` |
| `L0/Worker/IssueMatcherL0.cs` | 25 | **PARTIAL** | Literal/dynamic severity, ANSI stripping, owner add/remove/clobber, endLine/endColumn capture, multi-pattern lifecycle, loop matcher, validation | `matcher_accepts_literal_severity`, `matcher_strips_ansi_color_codes_before_matching`, `matcher_owner_can_be_removed`, `matcher_owner_clobber_replaces_old`, `matcher_dynamic_severity_from_regex_group`, `matcher_captures_end_line_and_end_column`, `test_multi_pattern_matching_lifecycle`, `test_multi_pattern_matching_with_loop`, `test_repository_path_resolution`, `matcher_validation_requires_message`, `matcher_validation_rejects_loop_on_single_pattern`, `matcher_validation_rejects_loop_before_last_pattern`, `matcher_validation_rejects_property_set_twice`, `matcher_validation_rejects_property_out_of_range`, `matcher_validation_requires_owner`, `matcher_validation_requires_pattern` |
| `L0/Worker/JobContextL0.cs` | 15 | **PARTIAL** | Context roots, variables, masks, github context, status, workflow identity fields, cancelled status covered; official variable dictionary edge cases partial | `build_expression_context_has_required_roots`, `get_variable_returns_value`, `job_status_failure_reflects_in_context`, `set_github_context_value_updates_context_and_env`, `set_github_context_value_clears_on_none`, `set_github_context_value_workflow_identity_fields`, `cancelled_status_reflects_in_context`, `vars_context_decodes_typed_dict_format` |
| `L0/Worker/JobExecutionViewL0.cs` | 3 | **GAP** | Job execution view/display behavior not tested | — |
| `L0/Worker/JobExtensionL0.cs` | 25 | **PARTIAL** | Step-list parsing, env injection, lifecycle pre/post covered; many official job extension paths still gaps | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `L0/Worker/JobRunnerL0.cs` | 3 | **PARTIAL** | Results URL extraction, job execution lifecycle, step failure propagation, external cancellation handling, and job-level timeout enforcement | `results_url_prefers_system_vss_endpoint_data`, `test_run_job_executes_successfully`, `test_run_job_propagates_step_failure`, `test_run_job_handles_cancelled`, `test_run_job_with_timeout` |
| `L0/Worker/OutputManagerL0.cs` | 22 | **PARTIAL** | Output manager/problem matcher runtime line processing, command handler integration | `test_multi_pattern_matching_lifecycle`, `test_multi_pattern_matching_with_loop`, `test_repository_path_resolution`, `log_raw_problem_matching_and_telemetry`, `handle_add_mask_adds_to_masks`, `handle_error_creates_annotation`, `handle_warning_creates_annotation`, `handle_notice_creates_annotation`, `handle_group_endgroup_logging`, `handle_echo_on_off`, `handle_stop_commands_via_log` |
| `L0/Worker/PipelineDirectoryManagerL0.cs` | 8 | **GAP** | Workspace/pipeline directory tracking/cleanup not tested | — |
| `L0/Worker/PipelineTemplateEvaluatorWrapperL0.cs` | 29 | **PARTIAL** | Template substitution with matrix/needs/env context, boolean/number/null rendering, unresolved context, mixed literal+expression, step env evaluation, display name evaluation, env context in conditions | `simple_expression`, `multiple_expressions`, `passthrough_literal`, `no_expressions`, `template_with_matrix_context`, `template_with_needs_context`, `template_with_env_context`, `template_evaluates_boolean_to_string`, `template_evaluates_number_to_string`, `template_null_renders_empty`, `template_unresolved_context_renders_empty`, `template_mixed_literal_and_expression`, `build_step_list_parses_github_template_token_maps`, `build_step_list_parses_aksh_template_string_maps`, `run_steps_step_env_evaluates_expressions`, `run_steps_display_name_evaluates_expression`, `run_steps_condition_uses_env_context` |
| `L0/Worker/SaveStateFileCommandL0.cs` | 15 | **PARTIAL** | State file command parsing, lifecycle state storage under original step ID | `parse_simple_kv`, `parse_heredoc`, `lifecycle_state_is_stored_under_original_step_id`, `post_step_env_exposes_saved_state_from_main_step` |
| `L0/Worker/SetEnvFileCommandL0.cs` | 17 | **PARTIAL** | Env file command parsing, heredoc, CRLF line endings, empty key rejection, NODE_OPTIONS blocking, unicode, equals-in-value | `parse_simple_kv`, `parse_heredoc`, `parse_empty_values_and_multiple_values`, `parse_heredoc_empty_value`, `parse_heredoc_requires_closing_delimiter`, `parse_rejects_invalid_lines`, `parse_kv_file_gracefully_ignores_missing_file_or_directory`, `parse_heredoc_missing_newline_error`, `parse_heredoc_missing_newline_multiple_lines_error`, `parse_kv_equals_in_value`, `parse_kv_unicode_value`, `parse_kv_crlf_line_endings`, `parse_heredoc_crlf_line_endings`, `parse_kv_empty_key_rejected`, `github_env_blocks_node_options` |
| `L0/Worker/SetOutputFileCommandL0.cs` | 15 | **PARTIAL** | Output file command parsing, heredoc, apply integration | `parse_simple_kv`, `parse_heredoc`, `apply_file_commands_attaches_outputs_and_prepends_path` |
| `L0/Worker/SnapshotOperationProviderL0.cs` | 1 | **GAP** | Snapshot operation provider not implemented/tested | — |
| `L0/Worker/StepHostL0.cs` | 7 | **PARTIAL** | Step host dispatch: container routing in execute_step, shell resolution for host/container paths | `resolve_bash_shell`, `resolve_custom_shell`, `resolve_sh_shell_default`, `resolve_python_shell`, `resolve_default_shell_is_bash`, `resolve_pwsh_shell` |
| `L0/Worker/StepHostNodeVersionL0.cs` | 8 | **NOT_APPLICABLE** | Node runtime selection inside containers (Alpine/ARM32 detection) — aksh uses host Node binary; container-specific Node selection is deferred until ARM32/Alpine support is needed | — |
| `L0/Worker/StepsRunnerL0.cs` | 13 | **FULL** | Complete step execution loop coverage: sequential execution, condition evaluation, implicit `success()` gating, `continue-on-error` correctness, env/context mutation, step outcome/conclusion visibility, cancellation semantics, working directory, step summary size limits | `run_steps_all_steps_pass`, `run_steps_continue_on_error_sets_failure_outcome_success_conclusion`, `run_steps_job_status_remains_success_after_continue_on_error`, `run_steps_conditions_reflect_prior_failure`, `run_steps_implicitly_gates_conditions_with_success`, `status_check_function_detection_ignores_string_literals`, `run_steps_cancelled_condition_runs_only_when_cancelled`, `run_steps_outcome_visible_in_later_step_condition`, `run_steps_marks_condition_error_as_failure`, `run_steps_step_env_override_job_env`, `run_steps_github_env_is_visible_to_later_steps`, `run_steps_outputs_are_visible_to_later_step_expressions`, `run_steps_honors_script_working_directory`, `test_step_summary_size_limit_and_scrubbing`, `condition_error_is_not_treated_as_skip` |
| `L0/Worker/TrackingManagerL0.cs` | 4 | **GAP** | Workspace tracking config persistence not tested | — |
| `L0/Worker/VariablesL0.cs` | 8 | **PARTIAL** | Secret masking, variable lookup, case-insensitive access, empty-name skip, null-value handling, boolean parse safety | `new_extracts_masks_from_secret_variables`, `mask_secrets_replaces_with_stars`, `add_mask_adds_new_secret`, `add_mask_ignores_empty`, `get_variable_returns_value`, `variables_case_insensitive_and_edge_cases`, `variables_get_boolean_does_not_throw_when_null` |
| `L0/Worker/WebSocketDapBridgeL0.cs` | 4 | **GAP** | DAP WebSocket bridge not implemented/tested | — |
| `L0/Worker/WorkerL0.cs` | 2 | **PARTIAL** | Worker top-level run loop and cancellation dispatch | `test_worker_dispatch_run_new_job` (`crates/aksh-runner/src/listener/job_dispatcher.rs`:164), `test_worker_dispatch_cancellation` (`crates/aksh-runner/src/listener/job_dispatcher.rs`:191) |

## aksh-runner test inventory

| Rust test | File:line | Behavior family |
|---|---|---|
| `parse_configure` | `crates/aksh-runner/src/cli.rs`:143 | CLI command parsing/config flags |
| `parse_run_defaults` | `crates/aksh-runner/src/cli.rs`:176 | CLI command parsing/config flags |
| `parse_run_azdo` | `crates/aksh-runner/src/cli.rs`:188 | CLI command parsing/config flags |
| `parse_remove` | `crates/aksh-runner/src/cli.rs`:200 | CLI command parsing/config flags |
| `parse_worker` | `crates/aksh-runner/src/cli.rs`:211 | CLI command parsing/config flags |
| `version_string_contains_protocol_compat` | `crates/aksh-runner/src/cli.rs`:222 | CLI command parsing/config flags |
| `global_ca_bundle_arg` | `crates/aksh-runner/src/cli.rs`:228 | CLI command parsing/config flags |
| `cancel_sends_sigint_before_hard_kill` | `crates/aksh-runner/src/process.rs`:245 | Process cancellation/signals |
| `cancel_falls_back_to_sigterm_when_sigint_is_ignored` | `crates/aksh-runner/src/process.rs`:280 | Process cancellation/signals |
| `round_trip_settings` | `crates/aksh-runner/src/settings.rs`:204 | Runner settings/credentials persistence |
| `strip_bom_works` | `crates/aksh-runner/src/settings.rs`:236 | Runner settings/credentials persistence |
| `rsa_params_field_names` | `crates/aksh-runner/src/settings.rs`:242 | Runner settings/credentials persistence |
| `config_lifecycle` | `crates/aksh-runner/src/settings.rs`:263 | Runner settings/credentials persistence |
| `parse_simple_command` | `crates/aksh-runner/src/worker/commands.rs`:232 | Workflow command parsing |
| `parse_command_with_properties` | `crates/aksh-runner/src/worker/commands.rs`:240 | Workflow command parsing |
| `parse_add_mask` | `crates/aksh-runner/src/worker/commands.rs`:250 | Workflow command parsing |
| `parse_legacy_format` | `crates/aksh-runner/src/worker/commands.rs`:257 | Workflow command parsing |
| `parse_case_insensitive` | `crates/aksh-runner/src/worker/commands.rs`:263 | Workflow command parsing |
| `unescape_data_values` | `crates/aksh-runner/src/worker/commands.rs`:271 | Workflow command parsing |
| `unescape_property_values` | `crates/aksh-runner/src/worker/commands.rs`:277 | Workflow command parsing |
| `not_a_command` | `crates/aksh-runner/src/worker/commands.rs`:283 | Workflow command parsing |
| `set_output_legacy` | `crates/aksh-runner/src/worker/commands.rs`:289 | Workflow command parsing |
| `path_translation` | `crates/aksh-runner/src/worker/container_ops.rs`:844 | Container/service Docker command construction |
| `sanitize_image` | `crates/aksh-runner/src/worker/container_ops.rs`:856 | Container/service Docker command construction |
| `container_naming` | `crates/aksh-runner/src/worker/container_ops.rs`:864 | Container/service Docker command construction |
| `action_container_naming` | `crates/aksh-runner/src/worker/container_ops.rs`:872 | Container/service Docker command construction |
| `parse_container_string` | `crates/aksh-runner/src/worker/container_ops.rs`:878 | Container/service Docker command construction |
| `parse_container_mapping` | `crates/aksh-runner/src/worker/container_ops.rs`:886 | Container/service Docker command construction |
| `parse_services` | `crates/aksh-runner/src/worker/container_ops.rs`:903 | Container/service Docker command construction |
| `label_is_6_hex` | `crates/aksh-runner/src/worker/container_ops.rs`:920 | Container/service Docker command construction |
| `network_name_format` | `crates/aksh-runner/src/worker/container_ops.rs`:927 | Container/service Docker command construction |
| `non_empty_services_omits_empty` | `crates/aksh-runner/src/worker/container_ops.rs`:934 | Container/service Docker command construction |
| `docker_create_env_uses_inherit_form_for_empty_values` | `crates/aksh-runner/src/worker/container_ops.rs`:941 | Container/service Docker command construction |
| `docker_exec_env_args_do_not_include_secret_values` | `crates/aksh-runner/src/worker/container_ops.rs`:959 | Container/service Docker command construction |
| `new_extracts_masks_from_secret_variables` | `crates/aksh-runner/src/worker/contexts.rs`:445 | Job/steps/github/env context and masking |
| `mask_secrets_replaces_with_stars` | `crates/aksh-runner/src/worker/contexts.rs`:459 | Job/steps/github/env context and masking |
| `add_mask_adds_new_secret` | `crates/aksh-runner/src/worker/contexts.rs`:472 | Job/steps/github/env context and masking |
| `add_mask_ignores_empty` | `crates/aksh-runner/src/worker/contexts.rs`:485 | Job/steps/github/env context and masking |
| `get_variable_returns_value` | `crates/aksh-runner/src/worker/contexts.rs`:497 | Job/steps/github/env context and masking |
| `build_expression_context_has_required_roots` | `crates/aksh-runner/src/worker/contexts.rs`:512 | Job/steps/github/env context and masking |
| `job_status_failure_reflects_in_context` | `crates/aksh-runner/src/worker/contexts.rs`:549 | Job/steps/github/env context and masking |
| `set_github_context_value_updates_context_and_env` | `crates/aksh-runner/src/worker/contexts.rs`:566 | Job/steps/github/env context and masking |
| `vars_context_decodes_typed_dict_format` | `crates/aksh-runner/src/worker/contexts.rs`:605 | Job/steps/github/env context and masking |
| `build_env_merges_job_and_step` | `crates/aksh-runner/src/worker/execution_context.rs`:226 | Step execution context env/logs/annotations/state |
| `build_env_includes_extra_path` | `crates/aksh-runner/src/worker/execution_context.rs`:238 | Step execution context env/logs/annotations/state |
| `log_masks_secrets` | `crates/aksh-runner/src/worker/execution_context.rs`:249 | Step execution context env/logs/annotations/state |
| `log_content_joins_lines` | `crates/aksh-runner/src/worker/execution_context.rs`:257 | Step execution context env/logs/annotations/state |
| `annotations_collected` | `crates/aksh-runner/src/worker/execution_context.rs`:271 | Step execution context env/logs/annotations/state |
| `annotations_cap_enforced` | `crates/aksh-runner/src/worker/execution_context.rs`:288 | Step execution context env/logs/annotations/state |
| `post_step_env_exposes_saved_state_from_main_step` | `crates/aksh-runner/src/worker/execution_context.rs`:308 | Step execution context env/logs/annotations/state |
| `parse_simple_kv` | `crates/aksh-runner/src/worker/file_commands.rs`:196 | Workflow command parsing |
| `parse_heredoc` | `crates/aksh-runner/src/worker/file_commands.rs`:206 | Workflow command parsing |
| `parse_path_file_lines` | `crates/aksh-runner/src/worker/file_commands.rs`:216 | Workflow command parsing |
| `create_and_cleanup` | `crates/aksh-runner/src/worker/file_commands.rs`:225 | Workflow command parsing |
| `lifecycle_state_is_stored_under_original_step_id` | `crates/aksh-runner/src/worker/file_commands.rs`:235 | Workflow command parsing |
| `action_repository_context_extracts_repository_and_ref` | `crates/aksh-runner/src/worker/handlers/action.rs`:161 | Action reference/repository context |
| `action_repository_context_is_empty_for_local_and_docker_actions` | `crates/aksh-runner/src/worker/handlers/action.rs`:169 | Action reference/repository context |
| `composite_steps_receive_action_status_context` | `crates/aksh-runner/src/worker/handlers/composite.rs`:347 | Composite action execution context |
| `inherited_env_args_do_not_include_secret_values` | `crates/aksh-runner/src/worker/handlers/container.rs`:322 | Docker action runtime args/env |
| `docker_run_args_mount_file_command_directories` | `crates/aksh-runner/src/worker/handlers/container.rs`:333 | Docker action runtime args/env |
| `manifest_env_entrypoint_and_args_evaluate_against_inputs` | `crates/aksh-runner/src/worker/handlers/container.rs`:399 | Docker action runtime args/env |
| `docker_run_args_apply_entrypoint_args_and_hide_env_values` | `crates/aksh-runner/src/worker/handlers/container.rs`:436 | Docker action runtime args/env |
| `load_node_action_manifest` | `crates/aksh-runner/src/worker/handlers/factory.rs`:128 | Action manifest parsing |
| `load_composite_action_manifest` | `crates/aksh-runner/src/worker/handlers/factory.rs`:170 | Action manifest parsing |
| `load_docker_action_manifest` | `crates/aksh-runner/src/worker/handlers/factory.rs`:194 | Action manifest parsing |
| `missing_manifest_returns_error` | `crates/aksh-runner/src/worker/handlers/factory.rs`:228 | Action manifest parsing |
| `resolve_bash_shell` | `crates/aksh-runner/src/worker/handlers/script.rs`:277 | Script shell resolution |
| `resolve_custom_shell` | `crates/aksh-runner/src/worker/handlers/script.rs`:288 | Script shell resolution |
| `inject_github_env_sets_core_vars` | `crates/aksh-runner/src/worker/job_extension.rs`:794 | Job payload step list/env/action lifecycle expansion |
| `build_step_list_parses_script_reference` | `crates/aksh-runner/src/worker/job_extension.rs`:834 | Job payload step list/env/action lifecycle expansion |
| `build_step_list_parses_action_reference` | `crates/aksh-runner/src/worker/job_extension.rs`:859 | Job payload step list/env/action lifecycle expansion |
| `build_step_list_parses_github_template_token_maps` | `crates/aksh-runner/src/worker/job_extension.rs`:882 | Job payload step list/env/action lifecycle expansion |
| `build_step_list_parses_aksh_template_string_maps` | `crates/aksh-runner/src/worker/job_extension.rs`:918 | Job payload step list/env/action lifecycle expansion |
| `build_step_list_handles_continue_on_error` | `crates/aksh-runner/src/worker/job_extension.rs`:954 | Job payload step list/env/action lifecycle expansion |
| `build_step_list_handles_template_continue_on_error` | `crates/aksh-runner/src/worker/job_extension.rs`:967 | Job payload step list/env/action lifecycle expansion |
| `inject_actions_env_from_system_vss_endpoint_data` | `crates/aksh-runner/src/worker/job_extension.rs`:994 | Job payload step list/env/action lifecycle expansion |
| `injects_job_environment_variables_from_acquire_payload` | `crates/aksh-runner/src/worker/job_extension.rs`:1060 | Job payload step list/env/action lifecycle expansion |
| `lifecycle_uses_resolved_action_path_and_entry_overrides` | `crates/aksh-runner/src/worker/job_extension.rs`:1087 | Job payload step list/env/action lifecycle expansion |
| `lifecycle_registers_docker_action_pre_and_post` | `crates/aksh-runner/src/worker/job_extension.rs`:1148 | Job payload step list/env/action lifecycle expansion |
| `test_golden_acquirejob_payloads_parsing` | `crates/aksh-runner/src/worker/job_extension.rs`:1210 | Job payload step list/env/action lifecycle expansion |
| `results_url_prefers_system_vss_endpoint_data` | `crates/aksh-runner/src/worker/job_runner.rs`:1219 | Job runner result/client wiring |
| `matcher_accepts_literal_severity` | `crates/aksh-runner/src/worker/matchers.rs`:203 | Problem matcher parsing |
| `queue_and_take_steps_update` | `crates/aksh-runner/src/worker/server_queue.rs`:175 | Step/log queue updates |
| `queue_and_take_logs` | `crates/aksh-runner/src/worker/server_queue.rs`:208 | Step/log queue updates |
| `change_order_increments` | `crates/aksh-runner/src/worker/server_queue.rs`:221 | Step/log queue updates |
| `conclusion_mapping` | `crates/aksh-runner/src/worker/server_queue.rs`:249 | Step/log queue updates |
| `no_expressions` | `crates/aksh-runner/src/worker/template.rs`:127 | Expression template interpolation |
| `simple_expression` | `crates/aksh-runner/src/worker/template.rs`:136 | Expression template interpolation |
| `multiple_expressions` | `crates/aksh-runner/src/worker/template.rs`:143 | Expression template interpolation |
| `passthrough_literal` | `crates/aksh-runner/src/worker/template.rs`:151 | Expression template interpolation |

## Official test-by-test verification

Each official test is listed with its file and line. The Rust column intentionally points to **verified behavior-family tests**, not fuzzy name matches. If the status is `GAP`, there is no verified aksh-runner test equivalent.


### `L0/CommandLineParserL0.cs` — 5 tests — PARTIAL

Official behavior: CLI command parsing and arg validation.
Verified aksh-runner refs: `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_run_azdo` (`crates/aksh-runner/src/cli.rs`:188), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211), `global_ca_bundle_arg` (`crates/aksh-runner/src/cli.rs`:228).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CanConstruct` | 12 | Can Construct | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_run_azdo` (`crates/aksh-runner/src/cli.rs`:188), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211), `global_ca_bundle_arg` (`crates/aksh-runner/src/cli.rs`:228) |
| `MasksSecretArgs` | 28 | Masks Secret Args | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_run_azdo` (`crates/aksh-runner/src/cli.rs`:188), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211), `global_ca_bundle_arg` (`crates/aksh-runner/src/cli.rs`:228) |
| `ParsesCommands` | 58 | Parses Commands | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_run_azdo` (`crates/aksh-runner/src/cli.rs`:188), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211), `global_ca_bundle_arg` (`crates/aksh-runner/src/cli.rs`:228) |
| `ParsesArgs` | 78 | Parses Args | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_run_azdo` (`crates/aksh-runner/src/cli.rs`:188), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211), `global_ca_bundle_arg` (`crates/aksh-runner/src/cli.rs`:228) |
| `ParsesFlags` | 102 | Parses Flags | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_run_azdo` (`crates/aksh-runner/src/cli.rs`:188), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211), `global_ca_bundle_arg` (`crates/aksh-runner/src/cli.rs`:228) |

### `L0/ConstantGenerationL0.cs` — 1 tests — GAP

Official behavior: Constant generation parity not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `BuildConstantGenerateSucceed` | 13 | Build Constant Generate Succeed | **GAP** | — |

### `L0/Container/ContainerInfoL0.cs` — 1 tests — PARTIAL

Official behavior: Container mapping/string parsing roughly covered.
Verified aksh-runner refs: `parse_container_string` (`crates/aksh-runner/src/worker/container_ops.rs`:878), `parse_container_mapping` (`crates/aksh-runner/src/worker/container_ops.rs`:886).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `MountVolumeConstructorParsesStringInput` | 12 | Mount Volume Constructor Parses String Input | **PARTIAL** | `parse_container_string` (`crates/aksh-runner/src/worker/container_ops.rs`:878), `parse_container_mapping` (`crates/aksh-runner/src/worker/container_ops.rs`:886) |

### `L0/Container/DockerUtilL0.cs` — 3 tests — PARTIAL

Official behavior: Docker image sanitization/name formatting covered; DockerUtil shell/path helpers partial.
Verified aksh-runner refs: `sanitize_image` (`crates/aksh-runner/src/worker/container_ops.rs`:856), `container_naming` (`crates/aksh-runner/src/worker/container_ops.rs`:864), `action_container_naming` (`crates/aksh-runner/src/worker/container_ops.rs`:872), `network_name_format` (`crates/aksh-runner/src/worker/container_ops.rs`:927).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `RegexParsesDockerPort` | 13 | Regex Parses Docker Port | **PARTIAL** | `sanitize_image` (`crates/aksh-runner/src/worker/container_ops.rs`:856), `container_naming` (`crates/aksh-runner/src/worker/container_ops.rs`:864), `action_container_naming` (`crates/aksh-runner/src/worker/container_ops.rs`:872), `network_name_format` (`crates/aksh-runner/src/worker/container_ops.rs`:927) |
| `RegexParsesPathFromDockerConfigEnv` | 72 | Regex Parses Path From Docker Config Env | **PARTIAL** | `sanitize_image` (`crates/aksh-runner/src/worker/container_ops.rs`:856), `container_naming` (`crates/aksh-runner/src/worker/container_ops.rs`:864), `action_container_naming` (`crates/aksh-runner/src/worker/container_ops.rs`:872), `network_name_format` (`crates/aksh-runner/src/worker/container_ops.rs`:927) |
| `CreateEscapedOption_keyValue` | 224 | Create Escaped Option key Value | **PARTIAL** | `sanitize_image` (`crates/aksh-runner/src/worker/container_ops.rs`:856), `container_naming` (`crates/aksh-runner/src/worker/container_ops.rs`:864), `action_container_naming` (`crates/aksh-runner/src/worker/container_ops.rs`:872), `network_name_format` (`crates/aksh-runner/src/worker/container_ops.rs`:927) |

### `L0/DistributedTask/WebApi/TimelineRecordL0.cs` — 10 tests — OUTSIDE_RUNNER

Official behavior: Timeline DTO behavior belongs to protocol/server surface, not aksh-runner tests.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `VerifyTimelineRecord_Defaults` | 19 | Verify Timeline Record Defaults | **OUTSIDE_RUNNER** | — |
| `VerifyTimelineRecord_Clone` | 35 | Verify Timeline Record Clone | **OUTSIDE_RUNNER** | — |
| `VerifyTimelineRecord_DeserializationEdgeCase_NonNullCollections` | 87 | Verify Timeline Record Deserialization Edge Case Non Null Collections | **OUTSIDE_RUNNER** | — |
| `VerifyTimelineRecord_DeserializationEdgeCase_AttemptCannotBeLessThan1` | 112 | Verify Timeline Record Deserialization Edge Case Attempt Cannot Be Less Than1 | **OUTSIDE_RUNNER** | — |
| `VerifyTimelineRecord_DeserializationEdgeCase_HandleLegacyNullsGracefully` | 136 | Verify Timeline Record Deserialization Edge Case Handle Legacy Nulls Gracefully | **OUTSIDE_RUNNER** | — |
| `VerifyTimelineRecord_DeserializationEdgeCase_HandleMissingCountsGracefully` | 152 | Verify Timeline Record Deserialization Edge Case Handle Missing Counts Gracefully | **OUTSIDE_RUNNER** | — |
| `VerifyTimelineRecord_DeserializationEdgeCase_NonZeroCounts` | 168 | Verify Timeline Record Deserialization Edge Case Non Zero Counts | **OUTSIDE_RUNNER** | — |
| `VerifyTimelineRecord_Deserialization_LeanTimelineRecord` | 184 | Verify Timeline Record Deserialization Lean Timeline Record | **OUTSIDE_RUNNER** | — |
| `VerifyTimelineRecord_Deserialization_VariablesDictionaryIsCaseInsensitive` | 201 | Verify Timeline Record Deserialization Variables Dictionary Is Case Insensitive | **OUTSIDE_RUNNER** | — |
| `VerifyTimelineRecord_DeserializationEdgeCase_DuplicateVariableKeysThrowsException` | 224 | Verify Timeline Record Deserialization Edge Case Duplicate Variable Keys Throws Exception | **OUTSIDE_RUNNER** | — |

### `L0/DotnetsdkDownloadScriptL0.cs` — 2 tests — NOT_APPLICABLE

Official behavior: Official runner .NET SDK bootstrap script not relevant to Rust runner runtime.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `EnsureDotnetsdkBashDownloadScriptUpToDate` | 14 | Ensure Dotnetsdk Bash Download Script Up To Date | **NOT_APPLICABLE** | — |
| `EnsureDotnetsdkPowershellDownloadScriptUpToDate` | 44 | Ensure Dotnetsdk Powershell Download Script Up To Date | **NOT_APPLICABLE** | — |

### `L0/ExtensionManagerL0.cs` — 2 tests — GAP

Official behavior: Extension manager behavior not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `LoadsTypeFromString` | 14 | Loads Type From String | **GAP** | — |
| `LoadsTypes` | 35 | Loads Types | **GAP** | — |

### `L0/HostContextL0.cs` — 8 tests — GAP

Official behavior: HostContext service registry/tracing not mirrored/tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CreateServiceReturnsNewInstance` | 20 | Create Service Returns New Instance | **GAP** | — |
| `GetServiceReturnsSingleton` | 48 | Get Service Returns Singleton | **GAP** | — |
| `DefaultSecretMaskers` | 75 | Default Secret Maskers | **GAP** | — |
| `SecretMaskerForProxy` | 149 | Secret Masker For Proxy | **GAP** | — |
| `AuthMigrationDisabledByDefault` | 178 | Auth Migration Disabled By Default | **GAP** | — |
| `AuthMigrationReenableTaskNotRunningByDefault` | 205 | Auth Migration Reenable Task Not Running By Default | **GAP** | — |
| `AuthMigrationEnableDisable` | 234 | Auth Migration Enable Disable | **GAP** | — |
| `AuthMigrationAutoReset` | 266 | Auth Migration Auto Reset | **GAP** | — |

### `L0/Listener/BrokerMessageListenerL0.cs` — 7 tests — GAP

Official behavior: Listener broker polling not unit-tested in aksh-runner.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CreatesSession` | 36 | Creates Session | **GAP** | — |
| `HandleAuthMigrationChanged` | 72 | Handle Auth Migration Changed | **GAP** | — |
| `CreatesSession_DeferAuthMigration` | 114 | Creates Session Defer Auth Migration | **GAP** | — |
| `GetNextMessage` | 167 | Get Next Message | **GAP** | — |
| `GetNextMessage_EnableAuthMigration` | 228 | Get Next Message Enable Auth Migration | **GAP** | — |
| `GetNextMessage_AuthMigrationFallback` | 293 | Get Next Message Auth Migration Fallback | **GAP** | — |
| `CreatesSessionWithProvidedSettings` | 369 | Creates Session With Provided Settings | **GAP** | — |

### `L0/Listener/CommandSettingsL0.cs` — 30 tests — PARTIAL

Official behavior: CLI parse tests cover some command settings, but official interactive settings matrix mostly gaps.
Verified aksh-runner refs: `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `GetsNameArg` | 19 | Gets Name Arg | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsNameArgFromEnvVar` | 37 | Gets Name Arg From Env Var | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsArgSecretFromEnvVar` | 65 | Gets Arg Secret From Env Var | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsCommandConfigure` | 93 | Gets Command Configure | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsCommandRun` | 111 | Gets Command Run | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsCommandUnconfigure` | 129 | Gets Command Unconfigure | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsFlagCommit` | 147 | Gets Flag Commit | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsFlagHelp` | 165 | Gets Flag Help | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsFlagReplace` | 183 | Gets Flag Replace | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsFlagRunAsService` | 201 | Gets Flag Run As Service | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsFlagUnattended` | 219 | Gets Flag Unattended | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsFlagUnattendedFromEnvVar` | 237 | Gets Flag Unattended From Env Var | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `GetsFlagVersion` | 264 | Gets Flag Version | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PassesUnattendedToReadBool` | 282 | Passes Unattended To Read Bool | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PassesUnattendedToReadValue` | 307 | Passes Unattended To Read Value | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForRunnerName` | 335 | Prompts For Runner Name | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForAuth` | 363 | Prompts For Auth | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForRunnerRegisterToken` | 391 | Prompts For Runner Register Token | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForReplace` | 419 | Prompts For Replace | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForRunAsService` | 444 | Prompts For Run As Service | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForToken` | 469 | Prompts For Token | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForRunnerDeletionToken` | 497 | Prompts For Runner Deletion Token | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForUrl` | 525 | Prompts For Url | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForWindowsLogonAccount` | 554 | Prompts For Windows Logon Account | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForWindowsLogonPassword` | 582 | Prompts For Windows Logon Password | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsForWork` | 611 | Prompts For Work | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsWhenEmpty` | 641 | Prompts When Empty | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `PromptsWhenInvalid` | 671 | Prompts When Invalid | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `ValidateCommands` | 699 | Validate Commands | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |
| `ValidateGoodCommandline` | 796 | Validate Good Commandline | **PARTIAL** | `parse_configure` (`crates/aksh-runner/src/cli.rs`:143), `parse_run_defaults` (`crates/aksh-runner/src/cli.rs`:176), `parse_remove` (`crates/aksh-runner/src/cli.rs`:200), `parse_worker` (`crates/aksh-runner/src/cli.rs`:211) |

### `L0/Listener/Configuration/ArgumentValidatorTestsL0.cs` — 4 tests — GAP

Official behavior: Argument validator rules not directly tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `ServerUrlValidator` | 11 | Server Url Validator | **GAP** | — |
| `AuthSchemeValidator` | 24 | Auth Scheme Validator | **GAP** | — |
| `NonEmptyValidator` | 36 | Non Empty Validator | **GAP** | — |
| `WindowsLogonAccountValidator` | 50 | Windows Logon Account Validator | **GAP** | — |

### `L0/Listener/Configuration/ConfigurationManagerL0.cs` — 7 tests — PARTIAL

Official behavior: Settings round-trip/config lifecycle covered; registration manager flows gaps.
Verified aksh-runner refs: `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204), `config_lifecycle` (`crates/aksh-runner/src/settings.rs`:263), `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `strip_bom_works` (`crates/aksh-runner/src/settings.rs`:236).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CanEnsureConfigure` | 152 | Can Ensure Configure | **PARTIAL** | `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204), `config_lifecycle` (`crates/aksh-runner/src/settings.rs`:263), `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `strip_bom_works` (`crates/aksh-runner/src/settings.rs`:236) |
| `ConfigureErrorDefaultLabelsDisabledWithNoCustomLabels` | 212 | Configure Error Default Labels Disabled With No Custom Labels | **PARTIAL** | `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204), `config_lifecycle` (`crates/aksh-runner/src/settings.rs`:263), `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `strip_bom_works` (`crates/aksh-runner/src/settings.rs`:236) |
| `ConfigureDefaultLabelsDisabledWithCustomLabels` | 253 | Configure Default Labels Disabled With Custom Labels | **PARTIAL** | `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204), `config_lifecycle` (`crates/aksh-runner/src/settings.rs`:263), `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `strip_bom_works` (`crates/aksh-runner/src/settings.rs`:236) |
| `ConfigureErrorOnMissingRunnerGroup` | 313 | Configure Error On Missing Runner Group | **PARTIAL** | `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204), `config_lifecycle` (`crates/aksh-runner/src/settings.rs`:263), `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `strip_bom_works` (`crates/aksh-runner/src/settings.rs`:236) |
| `ConfigureRunnerServiceFailsOnUnconfiguredRunners` | 357 | Configure Runner Service Fails On Unconfigured Runners | **PARTIAL** | `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204), `config_lifecycle` (`crates/aksh-runner/src/settings.rs`:263), `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `strip_bom_works` (`crates/aksh-runner/src/settings.rs`:236) |
| `ConfigureRunnerServiceCreatesService` | 388 | Configure Runner Service Creates Service | **PARTIAL** | `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204), `config_lifecycle` (`crates/aksh-runner/src/settings.rs`:263), `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `strip_bom_works` (`crates/aksh-runner/src/settings.rs`:236) |
| `ConfigureRunnerServiceFailsOnUnsupportedPlatforms` | 422 | Configure Runner Service Fails On Unsupported Platforms | **PARTIAL** | `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204), `config_lifecycle` (`crates/aksh-runner/src/settings.rs`:263), `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `strip_bom_works` (`crates/aksh-runner/src/settings.rs`:236) |

### `L0/Listener/Configuration/NativeWindowsServiceHelperL0.cs` — 2 tests — NOT_APPLICABLE

Official behavior: Windows service helper not relevant to macOS/Linux aksh target.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `EnsureGetDefaultServiceAccountShouldReturnNetworkServiceAccount` | 22 | Ensure Get Default Service Account Should Return Network Service Account | **NOT_APPLICABLE** | — |
| `EnsureGetDefaultAdminServiceAccountShouldReturnLocalSystemAccount` | 40 | Ensure Get Default Admin Service Account Should Return Local System Account | **NOT_APPLICABLE** | — |

### `L0/Listener/Configuration/PromptManagerTestsL0.cs` — 7 tests — GAP

Official behavior: Interactive prompt manager not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `FallsBackToDefault` | 22 | Falls Back To Default | **GAP** | — |
| `FallsBackToDefaultWhenTrimmed` | 47 | Falls Back To Default When Trimmed | **GAP** | — |
| `FallsBackToDefaultWhenUnattended` | 72 | Falls Back To Default When Unattended | **GAP** | — |
| `Prompts` | 99 | Prompts | **GAP** | — |
| `PromptsAgainWhenEmpty` | 124 | Prompts Again When Empty | **GAP** | — |
| `PromptsAgainWhenFailsValidation` | 151 | Prompts Again When Fails Validation | **GAP** | — |
| `ThrowsWhenUnattended` | 178 | Throws When Unattended | **GAP** | — |

### `L0/Listener/Configuration/RunnerCredentialL0.cs` — 2 tests — PARTIAL

Official behavior: RSA field names and settings serialization covered; credential scheme flows partial.
Verified aksh-runner refs: `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `NotUseAuthV2Url` | 38 | Not Use Auth V2 Url | **PARTIAL** | `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204) |
| `UseAuthV2Url` | 84 | Use Auth V2 Url | **PARTIAL** | `rsa_params_field_names` (`crates/aksh-runner/src/settings.rs`:242), `round_trip_settings` (`crates/aksh-runner/src/settings.rs`:204) |

### `L0/Listener/ErrorThrottlerL0.cs` — 3 tests — GAP

Official behavior: Error throttling not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `TestReset` | 100 | Test Reset | **GAP** | — |
| `TestReceivesCancellationToken` | 154 | Test Receives Cancellation Token | **GAP** | — |
| `TestReceivesSender` | 183 | Test Receives Sender | **GAP** | — |

### `L0/Listener/JobDispatcherL0.cs` — 12 tests — GAP

Official behavior: Listener job dispatcher not unit-tested in aksh-runner.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `DispatchesJobRequest` | 55 | Dispatches Job Request | **GAP** | — |
| `DispatcherRenewJobRequest` | 105 | Dispatcher Renew Job Request | **GAP** | — |
| `DispatcherRenewJobRequestStopOnJobNotFoundExceptions` | 163 | Dispatcher Renew Job Request Stop On Job Not Found Exceptions | **GAP** | — |
| `DispatcherRenewJobOnRunServiceStopOnJobNotFoundExceptions` | 222 | Dispatcher Renew Job On Run Service Stop On Job Not Found Exceptions | **GAP** | — |
| `DispatcherRenewJobRequestStopOnJobTokenExpiredExceptions` | 291 | Dispatcher Renew Job Request Stop On Job Token Expired Exceptions | **GAP** | — |
| `RenewJobRequestNewAgentNameUpdatesSettings` | 350 | Renew Job Request New Agent Name Updates Settings | **GAP** | — |
| `RenewJobRequestSameAgentNameIgnored` | 407 | Renew Job Request Same Agent Name Ignored | **GAP** | — |
| `RenewJobRequestNullAgentNameIgnored` | 462 | Renew Job Request Null Agent Name Ignored | **GAP** | — |
| `DispatcherRenewJobRequestRecoverFromExceptions` | 515 | Dispatcher Renew Job Request Recover From Exceptions | **GAP** | — |
| `DispatcherRenewJobRequestFirstRenewRetrySixTimes` | 576 | Dispatcher Renew Job Request First Renew Retry Six Times | **GAP** | — |
| `DispatcherRenewJobRequestStopOnExpiredRequest` | 631 | Dispatcher Renew Job Request Stop On Expired Request | **GAP** | — |
| `DispatchesOneTimeJobRequest` | 697 | Dispatches One Time Job Request | **GAP** | — |

### `L0/Listener/MessageListenerL0.cs` — 9 tests — GAP

Official behavior: Legacy message listener not unit-tested in aksh-runner.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CreatesSession` | 54 | Creates Session | **GAP** | — |
| `DeleteSession` | 98 | Delete Session | **GAP** | — |
| `GetNextMessage` | 145 | Get Next Message | **GAP** | — |
| `GetNextMessageWithBrokerMigration` | 226 | Get Next Message With Broker Migration | **GAP** | — |
| `CreateSessionWithOriginalCredential` | 341 | Create Session With Original Credential | **GAP** | — |
| `SkipDeleteSession_WhenGetNextMessageGetTaskAgentAccessTokenExpiredException` | 386 | Skip Delete Session When Get Next Message Get Task Agent Access Token Expired Exception | **GAP** | — |
| `HandleAuthMigrationChanged` | 448 | Handle Auth Migration Changed | **GAP** | — |
| `GetNextMessageWithBrokerMigration_AuthMigrationFallback` | 496 | Get Next Message With Broker Migration Auth Migration Fallback | **GAP** | — |
| `GetNextMessageWithBrokerMigration_EnableAuthMigration` | 622 | Get Next Message With Broker Migration Enable Auth Migration | **GAP** | — |

### `L0/Listener/RunnerConfigUpdaterTests.cs` — 17 tests — GAP

Official behavior: Runner self config update flow not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `UpdateRunnerConfigAsync_InvalidRunnerQualifiedId_ShouldReportTelemetry` | 28 | Update Runner Config Async Invalid Runner Qualified Id Should Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_ValidRunnerQualifiedId_ShouldNotReportTelemetry` | 59 | Update Runner Config Async Valid Runner Qualified Id Should Not Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_InvalidConfigType_ShouldReportTelemetry` | 90 | Update Runner Config Async Invalid Config Type Should Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_UpdateRunnerSettings_ShouldSucceed` | 121 | Update Runner Config Async Update Runner Settings Should Succeed | **GAP** | — |
| `UpdateRunnerConfigAsync_UpdateRunnerSettings_IgnoredEmptyRefreshResult` | 157 | Update Runner Config Async Update Runner Settings Ignored Empty Refresh Result | **GAP** | — |
| `UpdateRunnerConfigAsync_UpdateRunnerCredentials_ShouldSucceed` | 190 | Update Runner Config Async Update Runner Credentials Should Succeed | **GAP** | — |
| `UpdateRunnerConfigAsync_UpdateRunnerCredentials_IgnoredEmptyRefreshResult` | 236 | Update Runner Config Async Update Runner Credentials Ignored Empty Refresh Result | **GAP** | — |
| `UpdateRunnerConfigAsync_RefreshRunnerSettingsFailure_ShouldReportTelemetry` | 277 | Update Runner Config Async Refresh Runner Settings Failure Should Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_RefreshRunnerCredentialsFailure_ShouldReportTelemetry` | 310 | Update Runner Config Async Refresh Runner Credentials Failure Should Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_RefreshRunnerSettingsWithDifferentRunnerId_ShouldReportTelemetry` | 349 | Update Runner Config Async Refresh Runner Settings With Different Runner Id Should Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_RefreshRunnerSettingsWithDifferentRunnerName_ShouldReportTelemetry` | 385 | Update Runner Config Async Refresh Runner Settings With Different Runner Name Should Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_RefreshCredentialsWithDifferentScheme_ShouldReportTelemetry` | 421 | Update Runner Config Async Refresh Credentials With Different Scheme Should Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_RefreshOAuthCredentialsWithDifferentClientId_ShouldReportTelemetry` | 469 | Update Runner Config Async Refresh OAuth Credentials With Different Client Id Should Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_RefreshOAuthCredentialsWithDifferentAuthUrl_ShouldReportTelemetry` | 517 | Update Runner Config Async Refresh OAuth Credentials With Different Auth Url Should Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_UnsupportedServiceType_ShouldReportTelemetry` | 567 | Update Runner Config Async Unsupported Service Type Should Report Telemetry | **GAP** | — |
| `UpdateRunnerConfigAsync_RunnerAdminService_ShouldThrowNotSupported` | 600 | Update Runner Config Async Runner Admin Service Should Throw Not Supported | **GAP** | — |
| `UpdateRunnerConfigAsync_UpdateRunnerCredentials_EnableDisableAuthMigration` | 633 | Update Runner Config Async Update Runner Credentials Enable Disable Auth Migration | **GAP** | — |

### `L0/Listener/RunnerL0.cs` — 12 tests — GAP

Official behavior: Runner listener lifecycle not unit-tested in aksh-runner.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `TestRunAsync` | 77 | Test Run Async | **GAP** | — |
| `TestExecuteCommandForRunAsService` | 189 | Test Execute Command For Run As Service | **GAP** | — |
| `TestMachineProvisionerCLI` | 222 | Test Machine Provisioner CLI | **GAP** | — |
| `TestRunOnce` | 257 | Test Run Once | **GAP** | — |
| `TestRunOnceOnlyTakeOneJobMessage` | 354 | Test Run Once Only Take One Job Message | **GAP** | — |
| `TestRunOnceHandleUpdateMessage` | 455 | Test Run Once Handle Update Message | **GAP** | — |
| `TestRemoveLocalRunnerConfig` | 545 | Test Remove Local Runner Config | **GAP** | — |
| `TestReportAuthMigrationTelemetry` | 576 | Test Report Auth Migration Telemetry | **GAP** | — |
| `TestRunnerJobRequestMessageFromPipeline` | 675 | Test Runner Job Request Message From Pipeline | **GAP** | — |
| `TestRunnerJobRequestMessageFromRunService` | 776 | Test Runner Job Request Message From Run Service | **GAP** | — |
| `TestRunnerJobRequestMessageFromRunService_AuthMigrationFallback` | 877 | Test Runner Job Request Message From Run Service Auth Migration Fallback | **GAP** | — |
| `TestRunnerEnableAuthMigrationByDefault` | 999 | Test Runner Enable Auth Migration By Default | **GAP** | — |

### `L0/Listener/SelfUpdaterL0.cs` — 4 tests — GAP

Official behavior: Self-update flow not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `TestSelfUpdateAsync` | 81 | Test Self Update Async | **GAP** | — |
| `TestSelfUpdateAsync_NoUpdateOnOldVersion` | 140 | Test Self Update Async No Update On Old Version | **GAP** | — |
| `TestSelfUpdateAsync_DownloadRetry` | 191 | Test Self Update Async Download Retry | **GAP** | — |
| `TestSelfUpdateAsync_ValidateHash` | 244 | Test Self Update Async Validate Hash | **GAP** | — |

### `L0/Listener/SelfUpdaterV2L0.cs` — 3 tests — GAP

Official behavior: Self-update v2 flow not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `TestSelfUpdateAsync` | 77 | Test Self Update Async | **GAP** | — |
| `TestSelfUpdateAsync_DownloadRetry` | 137 | Test Self Update Async Download Retry | **GAP** | — |
| `TestSelfUpdateAsync_ValidateHash` | 186 | Test Self Update Async Validate Hash | **GAP** | — |

### `L0/PagingLoggerL0.cs` — 2 tests — GAP

Official behavior: Paging logger behavior not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `WriteAndShipLog` | 38 | Write And Ship Log | **GAP** | — |
| `ShipEmptyLog` | 99 | Ship Empty Log | **GAP** | — |

### `L0/ProcessExtensionL0.cs` — 1 tests — GAP

Official behavior: Process extension helpers not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `SuccessReadProcessEnv` | 17 | Success Read Process Env | **GAP** | — |

### `L0/ProcessInvokerL0.cs` — 12 tests — PARTIAL

Official behavior: Process cancellation signal sequence only; most process IO/lifecycle cases are gaps.
Verified aksh-runner refs: `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `DefaultsToCurrentSystemOemEncoding` | 20 | Defaults To Current System Oem Encoding | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `SuccessExitsWithCodeZero` | 64 | Success Exits With Code Zero | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `SetCIEnv` | 87 | Set CIEnv | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `SetTestEnvWithNullInKey` | 135 | Set Test Env With Null In Key | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `SetTestEnvWithNullInValue` | 170 | Set Test Env With Null In Value | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `KeepExistingCIEnv` | 204 | Keep Existing CIEnv | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `TestCancel` | 254 | Test Cancel | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `RedirectSTDINCloseStream` | 304 | Redirect STDINClose Stream | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `RedirectSTDINKeepStreamOpen` | 354 | Redirect STDINKeep Stream Open | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `OomScoreAdjIsWriten_Default` | 405 | Oom Score Adj Is Writen Default | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `OomScoreAdjIsWriten_FromEnv` | 441 | Oom Score Adj Is Writen From Env | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |
| `OomScoreAdjIsInherited` | 479 | Oom Score Adj Is Inherited | **PARTIAL** | `cancel_sends_sigint_before_hard_kill` (`crates/aksh-runner/src/process.rs`:245), `cancel_falls_back_to_sigterm_when_sigint_is_ignored` (`crates/aksh-runner/src/process.rs`:280) |

### `L0/RunnerWebProxyL0.cs` — 18 tests — GAP

Official behavior: Runner proxy config/env/no_proxy behavior not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `IsNotUseRawHttpClientHandler` | 27 | Is Not Use Raw Http Client Handler | **GAP** | — |
| `IsNotUseRawHttpClient` | 81 | Is Not Use Raw Http Client | **GAP** | — |
| `WebProxyFromEnvironmentVariables` | 135 | Web Proxy From Environment Variables | **GAP** | — |
| `WebProxyFromEnvironmentVariablesPreferLowerCase` | 166 | Web Proxy From Environment Variables Prefer Lower Case | **GAP** | — |
| `WebProxyFromEnvironmentVariablesInvalidString` | 199 | Web Proxy From Environment Variables Invalid String | **GAP** | — |
| `WebProxyPrependsHTTPforHTTP_PROXY_IfNoProtocol` | 226 | Web Proxy Prepends HTTPfor HTTP PROXY If No Protocol | **GAP** | — |
| `WebProxyPrependsHTTPforHTTPS_PROXY_IfNoProtocol` | 257 | Web Proxy Prepends HTTPfor HTTPS PROXY If No Protocol | **GAP** | — |
| `WebProxyFromEnvironmentVariablesProxyCredentials` | 288 | Web Proxy From Environment Variables Proxy Credentials | **GAP** | — |
| `WebProxyFromEnvironmentVariablesProxyCredentialsEncoding` | 326 | Web Proxy From Environment Variables Proxy Credentials Encoding | **GAP** | — |
| `WebProxyFromEnvironmentVariablesByPassEmptyProxy` | 364 | Web Proxy From Environment Variables By Pass Empty Proxy | **GAP** | — |
| `WebProxyFromEnvironmentVariablesGetProxyEmptyHttpProxy` | 374 | Web Proxy From Environment Variables Get Proxy Empty Http Proxy | **GAP** | — |
| `WebProxyFromEnvironmentVariablesGetProxyEmptyHttpsProxy` | 396 | Web Proxy From Environment Variables Get Proxy Empty Https Proxy | **GAP** | — |
| `WebProxyFromEnvironmentVariablesNoProxy` | 418 | Web Proxy From Environment Variables No Proxy | **GAP** | — |
| `BypassAllOnWildcardNoProxy` | 456 | Bypass All On Wildcard No Proxy | **GAP** | — |
| `IgnoreWildcardInNoProxySubdomain` | 481 | Ignore Wildcard In No Proxy Subdomain | **GAP** | — |
| `WildcardNoProxyWorksWhenOtherNoProxyAreAround` | 502 | Wildcard No Proxy Works When Other No Proxy Are Around | **GAP** | — |
| `WebProxyFromEnvironmentVariablesGetProxy` | 527 | Web Proxy From Environment Variables Get Proxy | **GAP** | — |
| `WebProxyFromEnvironmentVariablesWithPort80` | 557 | Web Proxy From Environment Variables With Port80 | **GAP** | — |

### `L0/Sdk/ExpressionParserL0.cs` — 4 tests — OUTSIDE_RUNNER

Official behavior: Expression parser equivalent is aksh-gha-expressions, not aksh-runner.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CreateTree_RejectsUnrecognizedNamedValue` | 19 | Create Tree Rejects Unrecognized Named Value | **OUTSIDE_RUNNER** | — |
| `CreateTree_AcceptsRecognizedNamedValue` | 39 | Create Tree Accepts Recognized Named Value | **OUTSIDE_RUNNER** | — |
| `CreateTree_CaseFunctionWorks` | 55 | Create Tree Case Function Works | **OUTSIDE_RUNNER** | — |
| `CreateTree_CaseFunctionDoesNotAffectUnknownKeywords` | 71 | Create Tree Case Function Does Not Affect Unknown Keywords | **OUTSIDE_RUNNER** | — |

### `L0/Sdk/LaunchWebApi/LaunchHttpClientL0.cs` — 2 tests — GAP

Official behavior: Launch client behavior not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `GetResolveActionsDownloadInfoAsync_SuccessResponse` | 22 | Get Resolve Actions Download Info Async Success Response | **GAP** | — |
| `GetResolveActionsDownloadInfoAsync_UnprocessableEntityResponse` | 78 | Get Resolve Actions Download Info Async Unprocessable Entity Response | **GAP** | — |

### `L0/Sdk/RSWebApi/AcquireJobRequestL0.cs` — 2 tests — OUTSIDE_RUNNER

Official behavior: Equivalent protocol/client behavior belongs to protocol/client crates.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `VerifySerialization` | 15 | Verify Serialization | **OUTSIDE_RUNNER** | — |
| `VerifyDeserialization` | 36 | Verify Deserialization | **OUTSIDE_RUNNER** | — |

### `L0/Sdk/RSWebApi/AgentJobRequestMessageL0.cs` — 8 tests — OUTSIDE_RUNNER

Official behavior: Equivalent protocol tests live outside aksh-runner crate, not counted here.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `VerifyEnableDebuggerDeserialization_WithTrue` | 15 | Verify Enable Debugger Deserialization With True | **OUTSIDE_RUNNER** | — |
| `VerifyEnableDebuggerDeserialization_DefaultToFalse` | 35 | Verify Enable Debugger Deserialization Default To False | **OUTSIDE_RUNNER** | — |
| `VerifyEnableDebuggerDeserialization_WithFalse` | 55 | Verify Enable Debugger Deserialization With False | **OUTSIDE_RUNNER** | — |
| `VerifyDebuggerTunnelDeserialization_WithTunnel` | 75 | Verify Debugger Tunnel Deserialization With Tunnel | **OUTSIDE_RUNNER** | — |
| `VerifyDebuggerTunnelDeserialization_WithoutTunnel` | 104 | Verify Debugger Tunnel Deserialization Without Tunnel | **OUTSIDE_RUNNER** | — |
| `VerifyActionsDependenciesDeserialization_WithDependencies` | 125 | Verify Actions Dependencies Deserialization With Dependencies | **OUTSIDE_RUNNER** | — |
| `VerifyActionsDependenciesDeserialization_DefaultsToEmpty` | 147 | Verify Actions Dependencies Deserialization Defaults To Empty | **OUTSIDE_RUNNER** | — |
| `VerifyDebuggerWelcomeMessageRoundTrips` | 167 | Verify Debugger Welcome Message Round Trips | **OUTSIDE_RUNNER** | — |

### `L0/Sdk/RSWebApi/AnnotationsL0.cs` — 3 tests — PARTIAL

Official behavior: Step annotations tested in execution context, but RSWebApi DTO conversion not in runner.
Verified aksh-runner refs: `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `ToAnnotation_ValidIssueWithMessage_ReturnsAnnotation` | 11 | To Annotation Valid Issue With Message Returns Annotation | **PARTIAL** | `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288) |
| `ToAnnotation_ValidIssueWithEmptyMessage_ReturnsNull` | 40 | To Annotation Valid Issue With Empty Message Returns Null | **PARTIAL** | `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288) |
| `ToAnnotation_ValidIssueWithMessageInData_ReturnsAnnotation` | 54 | To Annotation Valid Issue With Message In Data Returns Annotation | **PARTIAL** | `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288) |

### `L0/Sdk/RSWebApi/RunServiceHttpClientL0.cs` — 1 tests — GAP

Official behavior: Run service client HTTP behavior not unit-tested in aksh-runner.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `Truncate` | 9 | Truncate | **GAP** | — |

### `L0/Sdk/WellKnownRegularExpressionsL0.cs` — 9 tests — GAP

Official behavior: Well-known regex constants not mirrored/tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `SHA1_Key_Returns_CommitHash_Regex` | 11 | SHA1 Key Returns Commit Hash Regex | **GAP** | — |
| `CommitHash_Key_Returns_CommitHash_Regex` | 21 | Commit Hash Key Returns Commit Hash Regex | **GAP** | — |
| `SHA1_And_CommitHash_Return_Same_Regex` | 31 | SHA1 And Commit Hash Return Same Regex | **GAP** | — |
| `Matches_40_Char_Hex` | 42 | Matches 40 Char Hex | **GAP** | — |
| `Matches_64_Char_Hex` | 52 | Matches 64 Char Hex | **GAP** | — |
| `Does_Not_Match_63_Char_Hex` | 62 | Does Not Match 63 Char Hex | **GAP** | — |
| `Does_Not_Match_65_Char_Hex` | 72 | Does Not Match 65 Char Hex | **GAP** | — |
| `Matches_Mixed_Case_64_Char` | 82 | Matches Mixed Case 64 Char | **GAP** | — |
| `Unknown_Key_Returns_Null` | 93 | Unknown Key Returns Null | **GAP** | — |

### `L0/ServiceControlManagerL0.cs` — 5 tests — NOT_APPLICABLE

Official behavior: Windows service control manager not relevant to aksh target.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CalculateServiceName` | 12 | Calculate Service Name | **NOT_APPLICABLE** | — |
| `CalculateServiceName80Chars` | 51 | Calculate Service Name80 Chars | **NOT_APPLICABLE** | — |
| `CalculateServiceNameLimitsServiceNameTo80Chars` | 91 | Calculate Service Name Limits Service Name To80 Chars | **NOT_APPLICABLE** | — |
| `CalculateServiceNameSanitizeOutOfRangeChars` | 134 | Calculate Service Name Sanitize Out Of Range Chars | **NOT_APPLICABLE** | — |
| `CalculateServiceNameLimitsServiceNameTo150Chars` | 170 | Calculate Service Name Limits Service Name To150 Chars | **NOT_APPLICABLE** | — |

### `L0/ServiceInterfacesL0.cs` — 3 tests — GAP

Official behavior: Service interface validation not mirrored.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `RunnerInterfacesSpecifyDefaultImplementation` | 21 | Runner Interfaces Specify Default Implementation | **GAP** | — |
| `CommonInterfacesSpecifyDefaultImplementation` | 38 | Common Interfaces Specify Default Implementation | **GAP** | — |
| `WorkerInterfacesSpecifyDefaultImplementation` | 59 | Worker Interfaces Specify Default Implementation | **GAP** | — |

### `L0/Util/ArgUtilL0.cs` — 7 tests — GAP

Official behavior: Arg utility rules not mirrored directly.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `Equal_MatchesObjectEquality` | 12 | Equal Matches Object Equality | **GAP** | — |
| `Equal_MatchesReferenceEquality` | 30 | Equal Matches Reference Equality | **GAP** | — |
| `Equal_MatchesStructEquality` | 48 | Equal Matches Struct Equality | **GAP** | — |
| `Equal_ThrowsWhenActualObjectIsNull` | 66 | Equal Throws When Actual Object Is Null | **GAP** | — |
| `Equal_ThrowsWhenExpectedObjectIsNull` | 87 | Equal Throws When Expected Object Is Null | **GAP** | — |
| `Equal_ThrowsWhenObjectsAreNotEqual` | 108 | Equal Throws When Objects Are Not Equal | **GAP** | — |
| `Equal_ThrowsWhenStructsAreNotEqual` | 129 | Equal Throws When Structs Are Not Equal | **GAP** | — |

### `L0/Util/IOUtilL0.cs` — 22 tests — GAP

Official behavior: IO utility behavior not mirrored directly.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `Delete_DeletesDirectory` | 15 | Delete Deletes Directory | **GAP** | — |
| `Delete_DeletesFile` | 49 | Delete Deletes File | **GAP** | — |
| `DeleteDirectory_DeletesDirectoriesRecursively` | 83 | Delete Directory Deletes Directories Recursively | **GAP** | — |
| `DeleteDirectory_DeletesDirectoryReparsePointChain` | 115 | Delete Directory Deletes Directory Reparse Point Chain | **GAP** | — |
| `DeleteDirectory_DeletesDirectoryReparsePointsBeforeDirectories` | 179 | Delete Directory Deletes Directory Reparse Points Before Directories | **GAP** | — |
| `DeleteDirectory_DeletesFilesRecursively` | 230 | Delete Directory Deletes Files Recursively | **GAP** | — |
| `DeleteDirectory_DeletesReadOnlyDirectories` | 264 | Delete Directory Deletes Read Only Directories | **GAP** | — |
| `DeleteDirectory_DeletesReadOnlyRootDirectory` | 305 | Delete Directory Deletes Read Only Root Directory | **GAP** | — |
| `DeleteDirectory_DeletesReadOnlyFiles` | 341 | Delete Directory Deletes Read Only Files | **GAP** | — |
| `DeleteDirectory_DoesNotFollowDirectoryReparsePoint` | 381 | Delete Directory Does Not Follow Directory Reparse Point | **GAP** | — |
| `DeleteDirectory_DoesNotFollowNestLevel1DirectoryReparsePoint` | 428 | Delete Directory Does Not Follow Nest Level1 Directory Reparse Point | **GAP** | — |
| `DeleteDirectory_DoesNotFollowNestLevel2DirectoryReparsePoint` | 477 | Delete Directory Does Not Follow Nest Level2 Directory Reparse Point | **GAP** | — |
| `DeleteDirectory_IgnoresFile` | 528 | Delete Directory Ignores File | **GAP** | — |
| `DeleteFile_DeletesFile` | 563 | Delete File Deletes File | **GAP** | — |
| `DeleteFile_DeletesReadOnlyFile` | 597 | Delete File Deletes Read Only File | **GAP** | — |
| `DeleteFile_IgnoresDirectory` | 637 | Delete File Ignores Directory | **GAP** | — |
| `GetRelativePath` | 670 | Get Relative Path | **GAP** | — |
| `ResolvePath` | 768 | Resolve Path | **GAP** | — |
| `ValidateExecutePermission_DoesNotExceedFailsafe` | 867 | Validate Execute Permission Does Not Exceed Failsafe | **GAP** | — |
| `ValidateExecutePermission_ExceedsFailsafe` | 896 | Validate Execute Permission Exceeds Failsafe | **GAP** | — |
| `LoadObject_ThrowsOnRequiredLoadObject` | 936 | Load Object Throws On Required Load Object | **GAP** | — |
| `ReplaceInvalidFileNameChars` | 966 | Replace Invalid File Name Chars | **GAP** | — |

### `L0/Util/StringUtilL0.cs` — 6 tests — GAP

Official behavior: String utility behavior not mirrored directly.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `FormatAlwaysCallsFormat` | 12 | Format Always Calls Format | **GAP** | — |
| `FormatHandlesFormatException` | 48 | Format Handles Format Exception | **GAP** | — |
| `FormatUsesInvariantCulture` | 79 | Format Uses Invariant Culture | **GAP** | — |
| `ConvertNullOrEmptryStringToBool` | 105 | Convert Null Or Emptry String To Bool | **GAP** | — |
| `ConvertNullOrEmptryStringToDefaultBool` | 126 | Convert Null Or Emptry String To Default Bool | **GAP** | — |
| `ConvertStringToBool` | 147 | Convert String To Bool | **GAP** | — |

### `L0/Util/TaskResultUtilL0.cs` — 2 tests — GAP

Official behavior: Task result merge utility not directly tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `TaskResultReturnCodeTranslate` | 12 | Task Result Return Code Translate | **GAP** | — |
| `TaskResultsMerge` | 57 | Task Results Merge | **GAP** | — |

### `L0/Util/UrlUtilL0.cs` — 5 tests — GAP

Official behavior: URL utility behavior not directly tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `GetCredentialEmbeddedUrl_NoUsernameAndPassword` | 12 | Get Credential Embedded Url No Username And Password | **GAP** | — |
| `GetCredentialEmbeddedUrl_NoUsername` | 23 | Get Credential Embedded Url No Username | **GAP** | — |
| `GetCredentialEmbeddedUrl_NoPassword` | 34 | Get Credential Embedded Url No Password | **GAP** | — |
| `GetCredentialEmbeddedUrl_HasUsernameAndPassword` | 45 | Get Credential Embedded Url Has Username And Password | **GAP** | — |
| `GetCredentialEmbeddedUrl_UsernameAndPasswordEncoding` | 56 | Get Credential Embedded Url Username And Password Encoding | **GAP** | — |

### `L0/Util/VssUtilL0.cs` — 1 tests — GAP

Official behavior: VSS utility behavior not directly tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `VerifyOverwriteVssConnectionSetting` | 13 | Verify Overwrite Vss Connection Setting | **GAP** | — |

### `L0/Util/WhichUtilL0.cs` — 7 tests — GAP

Official behavior: which/path search utility behavior not directly tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `UseWhichFindGit` | 14 | Use Which Find Git | **GAP** | — |
| `WhichReturnsNullWhenNotFound` | 34 | Which Returns Null When Not Found | **GAP** | — |
| `WhichThrowsWhenRequireAndNotFound` | 54 | Which Throws When Require And Not Found | **GAP** | — |
| `WhichHandleFullyQualifiedPath` | 77 | Which Handle Fully Qualified Path | **GAP** | — |
| `WhichHandlesSymlinkToTargetFullPath` | 96 | Which Handles Symlink To Target Full Path | **GAP** | — |
| `WhichHandlesSymlinkToTargetRelativePath` | 140 | Which Handles Symlink To Target Relative Path | **GAP** | — |
| `WhichThrowsWhenSymlinkBroken` | 182 | Which Throws When Symlink Broken | **GAP** | — |

### `L0/Worker/ActionCommandL0.cs` — 2 tests — PARTIAL

Official behavior: Workflow command string parser.
Verified aksh-runner refs: `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_command_with_properties` (`crates/aksh-runner/src/worker/commands.rs`:240), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `parse_legacy_format` (`crates/aksh-runner/src/worker/commands.rs`:257), `parse_case_insensitive` (`crates/aksh-runner/src/worker/commands.rs`:263), `unescape_data_values` (`crates/aksh-runner/src/worker/commands.rs`:271), `unescape_property_values` (`crates/aksh-runner/src/worker/commands.rs`:277), `not_a_command` (`crates/aksh-runner/src/worker/commands.rs`:283).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CommandParserTest` | 13 | Command Parser Test | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_command_with_properties` (`crates/aksh-runner/src/worker/commands.rs`:240), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `parse_legacy_format` (`crates/aksh-runner/src/worker/commands.rs`:257), `parse_case_insensitive` (`crates/aksh-runner/src/worker/commands.rs`:263), `unescape_data_values` (`crates/aksh-runner/src/worker/commands.rs`:271), `unescape_property_values` (`crates/aksh-runner/src/worker/commands.rs`:277), `not_a_command` (`crates/aksh-runner/src/worker/commands.rs`:283) |
| `CommandParserV2Test` | 94 | Command Parser V2 Test | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_command_with_properties` (`crates/aksh-runner/src/worker/commands.rs`:240), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `parse_legacy_format` (`crates/aksh-runner/src/worker/commands.rs`:257), `parse_case_insensitive` (`crates/aksh-runner/src/worker/commands.rs`:263), `unescape_data_values` (`crates/aksh-runner/src/worker/commands.rs`:271), `unescape_property_values` (`crates/aksh-runner/src/worker/commands.rs`:277), `not_a_command` (`crates/aksh-runner/src/worker/commands.rs`:283) |

### `L0/Worker/ActionCommandManagerL0.cs` — 14 tests — PARTIAL

Official behavior: Command parsing covered; manager effects/output processing are mostly gaps.
Verified aksh-runner refs: `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `EnablePluginInternalCommand` | 25 | Enable Plugin Internal Command | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `DisablePluginInternalCommand` | 52 | Disable Plugin Internal Command | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `StopProcessCommand` | 83 | Stop Process Command | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `StopProcessCommand__FailOnInvalidStopTokens` | 146 | Stop Process Command  Fail On Invalid Stop Tokens | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `StopProcessCommandAcceptsValidToken` | 160 | Stop Process Command Accepts Valid Token | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `StopProcessCommandMasksValidTokenForEntireRun` | 176 | Stop Process Command Masks Valid Token For Entire Run | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `EchoProcessCommand` | 195 | Echo Process Command | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `EchoProcessCommandDebugOn` | 225 | Echo Process Command Debug On | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `IssueCommandInvalidColumns` | 278 | Issue Command Invalid Columns | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `EchoProcessCommandInvalid` | 356 | Echo Process Command Invalid | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `AddMatcherTranslatesFilePath` | 383 | Add Matcher Translates File Path | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `AddMaskWithMultilineValue` | 424 | Add Mask With Multiline Value | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `SetOutputCommand_EmitsTelemetryOnce` | 507 | Set Output Command Emits Telemetry Once | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |
| `SaveStateCommand_EmitsTelemetryOnce` | 531 | Save State Command Emits Telemetry Once | **PARTIAL** | `parse_simple_command` (`crates/aksh-runner/src/worker/commands.rs`:232), `parse_add_mask` (`crates/aksh-runner/src/worker/commands.rs`:250), `set_output_legacy` (`crates/aksh-runner/src/worker/commands.rs`:289), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `add_mask_adds_new_secret` (`crates/aksh-runner/src/worker/contexts.rs`:472) |

### `L0/Worker/ActionManagerL0.cs` — 57 tests — PARTIAL

Official behavior: Action repository context extraction only; download/cache/action resolution manager behavior mostly untested.
Verified aksh-runner refs: `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `PrepareActions_DownloadActionFromDotCom_OnPremises_Legacy` | 43 | Prepare Actions Download Action From Dot Com On Premises Legacy | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_DownloadActionFromDotCom_ZipFileError` | 109 | Prepare Actions Download Action From Dot Com Zip File Error | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_DownloadUnknownActionFromGraph_OnPremises_Legacy` | 171 | Prepare Actions Download Unknown Action From Graph On Premises Legacy | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_ResolveActionDownloadInfo_RecordsTelemetry_OnFailure` | 231 | Prepare Actions Resolve Action Download Info Records Telemetry On Failure | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_PullImageFromDockerHub` | 277 | Prepare Actions Pull Image From Docker Hub | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_DownloadActionFromGraph` | 315 | Prepare Actions Download Action From Graph | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_DownloadActionFromGraph_UseCache` | 357 | Prepare Actions Download Action From Graph Use Cache | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_AlwaysClearActionsCache` | 469 | Prepare Actions Always Clear Actions Cache | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_SkipDownloadActionForSelfRepo` | 499 | Prepare Actions Skip Download Action For Self Repo | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_SymlinkCacheIsReentrant` | 534 | Prepare Actions Symlink Cache Is Reentrant | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_RepositoryActionWithDockerfile` | 603 | Prepare Actions Repository Action With Dockerfile | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_RepositoryActionWithDockerfileInRelativePath` | 642 | Prepare Actions Repository Action With Dockerfile In Relative Path | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_RepositoryActionWithActionfile_Dockerfile` | 683 | Prepare Actions Repository Action With Actionfile Dockerfile | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_RepositoryActionWithActionfile_DockerfileRelativePath` | 722 | Prepare Actions Repository Action With Actionfile Dockerfile Relative Path | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_RepositoryActionWithActionfile_DockerHubImage` | 762 | Prepare Actions Repository Action With Actionfile Docker Hub Image | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_RepositoryActionWithActionYamlFile_DockerHubImage` | 801 | Prepare Actions Repository Action With Action Yaml File Docker Hub Image | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_RepositoryActionWithActionfileAndDockerfile` | 840 | Prepare Actions Repository Action With Actionfile And Dockerfile | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_NotPullOrBuildImagesMultipleTimes` | 880 | Prepare Actions Not Pull Or Build Images Multiple Times | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_RepositoryActionWithActionfile_Node` | 1020 | Prepare Actions Repository Action With Actionfile Node | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_RepositoryActionWithInvalidWrapperActionfile_Node` | 1057 | Prepare Actions Repository Action With Invalid Wrapper Actionfile Node | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_RepositoryActionWithWrapperActionfile_PreSteps` | 1100 | Prepare Actions Repository Action With Wrapper Actionfile Pre Steps | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_CompositeActionWithActionfile_Node` | 1155 | Prepare Actions Composite Action With Actionfile Node | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_CompositeActionWithActionfile_MaxLimit` | 1198 | Prepare Actions Composite Action With Actionfile Max Limit | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_CompositeActionWithActionfile_CompositePrestepNested` | 1238 | Prepare Actions Composite Action With Actionfile Composite Prestep Nested | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_CompositeActionWithActionfile_CompositeContainerNested` | 1280 | Prepare Actions Composite Action With Actionfile Composite Container Nested | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_BatchesResolutionAcrossCompositeActions` | 1324 | Prepare Actions Batches Resolution Across Composite Actions | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_DeduplicatesResolutionAcrossDepthLevels` | 1433 | Prepare Actions Deduplicates Resolution Across Depth Levels | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_MultipleTopLevelActions_BatchesResolution` | 1514 | Prepare Actions Multiple Top Level Actions Batches Resolution | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_NestedCompositeContainers_BatchedResolution` | 1603 | Prepare Actions Nested Composite Containers Batched Resolution | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_ParallelDownloads_MultipleUniqueActions` | 1680 | Prepare Actions Parallel Downloads Multiple Unique Actions | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_DownloadsNextLevelActionsBeforeRecursing` | 1785 | Prepare Actions Downloads Next Level Actions Before Recursing | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_ParallelDownloadsAtSameDepth` | 1895 | Prepare Actions Parallel Downloads At Same Depth | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsContainerRegistryActionDefinition` | 1973 | Loads Container Registry Action Definition | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsScriptActionDefinition` | 2010 | Loads Script Action Definition | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsContainerActionDefinitionDockerfile` | 2040 | Loads Container Action Definition Dockerfile | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsContainerActionDefinitionRegistry` | 2140 | Loads Container Action Definition Registry | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsNode12ActionDefinition` | 2240 | Loads Node12 Action Definition | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsNode16ActionDefinition` | 2309 | Loads Node16 Action Definition | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsNode20ActionDefinition` | 2378 | Loads Node20 Action Definition | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsNode24ActionDefinition` | 2447 | Loads Node24 Action Definition | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsNodeActionDefinitionYaml` | 2516 | Loads Node Action Definition Yaml | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsContainerActionDefinitionDockerfile_SelfRepo` | 2597 | Loads Container Action Definition Dockerfile Self Repo | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsContainerActionDefinitionRegistry_SelfRepo` | 2696 | Loads Container Action Definition Registry Self Repo | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsNodeActionDefinition_SelfRepo` | 2795 | Loads Node Action Definition Self Repo | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsNodeActionDefinition_Cleanup` | 2863 | Loads Node Action Definition Cleanup | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsContainerActionDefinitionDockerfile_Cleanup` | 2933 | Loads Container Action Definition Dockerfile Cleanup | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadsPluginActionDefinition` | 3035 | Loads Plugin Action Definition | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `GetDownloadInfoAsync_PropagatesDependencies_WhenPresent` | 3404 | Get Download Info Async Propagates Dependencies When Present | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `GetDownloadInfoAsync_OmitsDependencies_WhenEmpty` | 3476 | Get Download Info Async Omits Dependencies When Empty | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_SelfRepository_ResolvesAtDepthZero` | 3540 | Prepare Actions Self Repository Resolves At Depth Zero | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_SelfRepository_NotResolvedWhenFeatureFlagDisabled` | 3614 | Prepare Actions Self Repository Not Resolved When Feature Flag Disabled | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_SelfRepository_ResolvesNestedInComposite` | 3652 | Prepare Actions Self Repository Resolves Nested In Composite | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_SelfRepository_CrossRepoCompositeResolvesToParentRepo` | 3738 | Prepare Actions Self Repository Cross Repo Composite Resolves To Parent Repo | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_SelfRepository_MultiLevelChain` | 3816 | Prepare Actions Self Repository Multi Level Chain | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_SelfRepository_ResolvesAtDepthZero_LegacyPath` | 3900 | Prepare Actions Self Repository Resolves At Depth Zero Legacy Path | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `PrepareActions_SelfRepository_ResolvesNestedInComposite_LegacyPath` | 3970 | Prepare Actions Self Repository Resolves Nested In Composite Legacy Path | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |
| `LoadAction_DotSlashCompositeWithNestedSelfRepository_ResolvesViaWorkflowContext` | 4047 | Load Action Dot Slash Composite With Nested Self Repository Resolves Via Workflow Context | **PARTIAL** | `action_repository_context_extracts_repository_and_ref` (`crates/aksh-runner/src/worker/handlers/action.rs`:161), `action_repository_context_is_empty_for_local_and_docker_actions` (`crates/aksh-runner/src/worker/handlers/action.rs`:169), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859) |

### `L0/Worker/ActionManifestManagerL0.cs` — 25 tests — PARTIAL

Official behavior: Happy-path node/composite/docker action manifest parsing.
Verified aksh-runner refs: `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `Load_ContainerAction_Dockerfile` | 29 | Load Container Action Dockerfile | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_ContainerAction_Dockerfile_Pre` | 73 | Load Container Action Dockerfile Pre | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_ContainerAction_Dockerfile_Post` | 119 | Load Container Action Dockerfile Post | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_ContainerAction_Dockerfile_Pre_DefaultCondition` | 165 | Load Container Action Dockerfile Pre Default Condition | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_ContainerAction_Dockerfile_Post_DefaultCondition` | 211 | Load Container Action Dockerfile Post Default Condition | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_ContainerAction_NoArgsNoEnv` | 257 | Load Container Action No Args No Env | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_ContainerAction_Dockerfile_Expression` | 294 | Load Container Action Dockerfile Expression | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_ContainerAction_DockerHub` | 338 | Load Container Action Docker Hub | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_NodeAction` | 381 | Load Node Action | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_Node16Action` | 424 | Load Node16 Action | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_Node20Action` | 467 | Load Node20 Action | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_Node24Action` | 510 | Load Node24 Action | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_NodeAction_Pre` | 553 | Load Node Action Pre | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_NodeAction_Init_DefaultCondition` | 597 | Load Node Action Init Default Condition | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_NodeAction_Cleanup` | 641 | Load Node Action Cleanup | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_NodeAction_Cleanup_DefaultCondition` | 685 | Load Node Action Cleanup Default Condition | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_PluginAction` | 729 | Load Plugin Action | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_ConditionalCompositeAction` | 766 | Load Conditional Composite Action | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_CompositeActionNoUsing` | 792 | Load Composite Action No Using | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Evaluate_ContainerAction_Args` | 817 | Evaluate Container Action Args | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Evaluate_ContainerAction_Env` | 854 | Evaluate Container Action Env | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Evaluate_Default_Input` | 891 | Evaluate Default Input | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_ContainerAction_RejectsInvalidExpressionContext` | 934 | Load Container Action Rejects Invalid Expression Context | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Load_ContainerAction_AcceptsValidExpressionContext` | 959 | Load Container Action Accepts Valid Expression Context | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |
| `Evaluate_Default_Input_Case_Function` | 1012 | Evaluate Default Input Case Function | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194), `missing_manifest_returns_error` (`crates/aksh-runner/src/worker/handlers/factory.rs`:228) |

### `L0/Worker/ActionManifestManagerLegacyL0.cs` — 24 tests — GAP

Official behavior: Legacy action manifest parser compatibility not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `Load_ContainerAction_Dockerfile` | 27 | Load Container Action Dockerfile | **GAP** | — |
| `Load_ContainerAction_Dockerfile_Pre` | 71 | Load Container Action Dockerfile Pre | **GAP** | — |
| `Load_ContainerAction_Dockerfile_Post` | 117 | Load Container Action Dockerfile Post | **GAP** | — |
| `Load_ContainerAction_Dockerfile_Pre_DefaultCondition` | 163 | Load Container Action Dockerfile Pre Default Condition | **GAP** | — |
| `Load_ContainerAction_Dockerfile_Post_DefaultCondition` | 209 | Load Container Action Dockerfile Post Default Condition | **GAP** | — |
| `Load_ContainerAction_NoArgsNoEnv` | 255 | Load Container Action No Args No Env | **GAP** | — |
| `Load_ContainerAction_Dockerfile_Expression` | 292 | Load Container Action Dockerfile Expression | **GAP** | — |
| `Load_ContainerAction_DockerHub` | 336 | Load Container Action Docker Hub | **GAP** | — |
| `Load_NodeAction` | 379 | Load Node Action | **GAP** | — |
| `Load_Node16Action` | 422 | Load Node16 Action | **GAP** | — |
| `Load_Node20Action` | 465 | Load Node20 Action | **GAP** | — |
| `Load_Node24Action` | 508 | Load Node24 Action | **GAP** | — |
| `Load_NodeAction_Pre` | 551 | Load Node Action Pre | **GAP** | — |
| `Load_NodeAction_Init_DefaultCondition` | 595 | Load Node Action Init Default Condition | **GAP** | — |
| `Load_NodeAction_Cleanup` | 639 | Load Node Action Cleanup | **GAP** | — |
| `Load_NodeAction_Cleanup_DefaultCondition` | 683 | Load Node Action Cleanup Default Condition | **GAP** | — |
| `Load_PluginAction` | 727 | Load Plugin Action | **GAP** | — |
| `Load_ConditionalCompositeAction` | 764 | Load Conditional Composite Action | **GAP** | — |
| `Load_CompositeActionNoUsing` | 790 | Load Composite Action No Using | **GAP** | — |
| `Evaluate_ContainerAction_Args` | 815 | Evaluate Container Action Args | **GAP** | — |
| `Evaluate_ContainerAction_Env` | 852 | Evaluate Container Action Env | **GAP** | — |
| `Evaluate_Default_Input` | 889 | Evaluate Default Input | **GAP** | — |
| `Load_ContainerAction_RejectsInvalidExpressionContext` | 932 | Load Container Action Rejects Invalid Expression Context | **GAP** | — |
| `Load_ContainerAction_AcceptsValidExpressionContext` | 957 | Load Container Action Accepts Valid Expression Context | **GAP** | — |

### `L0/Worker/ActionManifestParserComparisonL0.cs` — 8 tests — GAP

Official behavior: Legacy/new manifest parser comparison and mismatch telemetry not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `ConvertToLegacySteps_ProducesCorrectSteps_WithExplicitPropertyMapping` | 34 | Convert To Legacy Steps Produces Correct Steps With Explicit Property Mapping | **GAP** | — |
| `EvaluateJobContainer_EmptyImage_BothParsersReturnNull` | 97 | Evaluate Job Container Empty Image Both Parsers Return Null | **GAP** | — |
| `FromJsonEmptyString_BothParsersFail_WithDifferentMessages` | 144 | From Json Empty String Both Parsers Fail With Different Messages | **GAP** | — |
| `EvaluateDefaultInput_BothParsersAgree` | 225 | Evaluate Default Input Both Parsers Agree | **GAP** | — |
| `EvaluateContainerArguments_BothParsersAgree` | 266 | Evaluate Container Arguments Both Parsers Agree | **GAP** | — |
| `EvaluateContainerEnvironment_BothParsersAgree` | 306 | Evaluate Container Environment Both Parsers Agree | **GAP** | — |
| `EvaluateCompositeOutputs_BothParsersAgree` | 344 | Evaluate Composite Outputs Both Parsers Agree | **GAP** | — |
| `Load_BothParsersRejectInvalidExpressionContext` | 385 | Load Both Parsers Reject Invalid Expression Context | **GAP** | — |

### `L0/Worker/ActionRunnerL0.cs` — 13 tests — PARTIAL

Official behavior: Display-name and action reference parsing only partially covered by job-extension tests.
Verified aksh-runner refs: `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `MergeDefaultInputs` | 36 | Merge Default Inputs | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `WriteEventPayload` | 82 | Write Event Payload | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EvaluateLegacyDisplayName` | 121 | Evaluate Legacy Display Name | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EvaluateExpansionOfDisplayNameToken` | 158 | Evaluate Expansion Of Display Name Token | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `IgnoreDisplayNameTokenWhenDisplayNameIsExplicitlySet` | 192 | Ignore Display Name Token When Display Name Is Explicitly Set | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EvaluateExpansionOfScriptDisplayName` | 229 | Evaluate Expansion Of Script Display Name | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EvaluateExpansionOfContainerDisplayName` | 265 | Evaluate Expansion Of Container Display Name | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EvaluateDisplayNameWithoutContext` | 294 | Evaluate Display Name Without Context | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EvaluateDisplayNameForLocalAction` | 322 | Evaluate Display Name For Local Action | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EvaluateDisplayNameForLocalActionWithPath` | 351 | Evaluate Display Name For Local Action With Path | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EvaluateDisplayNameForRemoteActionWithPath` | 380 | Evaluate Display Name For Remote Action With Path | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `WarnInvalidInputs` | 410 | Warn Invalid Inputs | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `SetGitHubContextActionRepoRef` | 463 | Set Git Hub Context Action Repo Ref | **PARTIAL** | `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |

### `L0/Worker/BackgroundStepsL0.cs` — 10 tests — GAP

Official behavior: Background/wait/cancel concurrent step semantics not implemented/tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `BackgroundStepRunsConcurrentlyWithForeground` | 113 | Background Step Runs Concurrently With Foreground | **GAP** | — |
| `WaitStepBlocksUntilBackgroundCompletes` | 166 | Wait Step Blocks Until Background Completes | **GAP** | — |
| `BackgroundStepFailurePropagatesAtWait` | 207 | Background Step Failure Propagates At Wait | **GAP** | — |
| `CancelStepTerminatesBackgroundStep` | 244 | Cancel Step Terminates Background Step | **GAP** | — |
| `WaitAllWaitsForAllBackgroundSteps` | 289 | Wait All Waits For All Background Steps | **GAP** | — |
| `CancelStepPublishesCanceledBackgroundExternalId` | 346 | Cancel Step Publishes Canceled Background External Id | **GAP** | — |
| `CanceledBackgroundStepDoesNotAffectJobResult` | 378 | Canceled Background Step Does Not Affect Job Result | **GAP** | — |
| `FailedBackgroundStepTargetedByCancelStillAffectsJobResult` | 423 | Failed Background Step Targeted By Cancel Still Affects Job Result | **GAP** | — |
| `StepsContextThreadSafety` | 460 | Steps Context Thread Safety | **GAP** | — |
| `ControlFlowStepsRunEvenAfterFailure` | 487 | Control Flow Steps Run Even After Failure | **GAP** | — |

### `L0/Worker/ContainerOperationProviderL0.cs` — 5 tests — PARTIAL

Official behavior: Docker/container command construction and naming covered; provider orchestration edge cases gaps.
Verified aksh-runner refs: `parse_container_string` (`crates/aksh-runner/src/worker/container_ops.rs`:878), `parse_container_mapping` (`crates/aksh-runner/src/worker/container_ops.rs`:886), `parse_services` (`crates/aksh-runner/src/worker/container_ops.rs`:903), `docker_create_env_uses_inherit_form_for_empty_values` (`crates/aksh-runner/src/worker/container_ops.rs`:941), `docker_exec_env_args_do_not_include_secret_values` (`crates/aksh-runner/src/worker/container_ops.rs`:959).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `RunServiceContainersHealthcheck_UnhealthyServiceContainer_AssertFailedTask` | 35 | Run Service Containers Healthcheck Unhealthy Service Container Assert Failed Task | **PARTIAL** | `parse_container_string` (`crates/aksh-runner/src/worker/container_ops.rs`:878), `parse_container_mapping` (`crates/aksh-runner/src/worker/container_ops.rs`:886), `parse_services` (`crates/aksh-runner/src/worker/container_ops.rs`:903), `docker_create_env_uses_inherit_form_for_empty_values` (`crates/aksh-runner/src/worker/container_ops.rs`:941), `docker_exec_env_args_do_not_include_secret_values` (`crates/aksh-runner/src/worker/container_ops.rs`:959) |
| `RunServiceContainersHealthcheck_UnhealthyServiceContainer_AssertExceptionThrown` | 57 | Run Service Containers Healthcheck Unhealthy Service Container Assert Exception Thrown | **PARTIAL** | `parse_container_string` (`crates/aksh-runner/src/worker/container_ops.rs`:878), `parse_container_mapping` (`crates/aksh-runner/src/worker/container_ops.rs`:886), `parse_services` (`crates/aksh-runner/src/worker/container_ops.rs`:903), `docker_create_env_uses_inherit_form_for_empty_values` (`crates/aksh-runner/src/worker/container_ops.rs`:941), `docker_exec_env_args_do_not_include_secret_values` (`crates/aksh-runner/src/worker/container_ops.rs`:959) |
| `RunServiceContainersHealthcheck_healthyServiceContainer_AssertSucceededTask` | 71 | Run Service Containers Healthcheck healthy Service Container Assert Succeeded Task | **PARTIAL** | `parse_container_string` (`crates/aksh-runner/src/worker/container_ops.rs`:878), `parse_container_mapping` (`crates/aksh-runner/src/worker/container_ops.rs`:886), `parse_services` (`crates/aksh-runner/src/worker/container_ops.rs`:903), `docker_create_env_uses_inherit_form_for_empty_values` (`crates/aksh-runner/src/worker/container_ops.rs`:941), `docker_exec_env_args_do_not_include_secret_values` (`crates/aksh-runner/src/worker/container_ops.rs`:959) |
| `RunServiceContainersHealthcheck_healthyServiceContainerWithoutHealthcheck_AssertSucceededTask` | 88 | Run Service Containers Healthcheck healthy Service Container Without Healthcheck Assert Succeeded Task | **PARTIAL** | `parse_container_string` (`crates/aksh-runner/src/worker/container_ops.rs`:878), `parse_container_mapping` (`crates/aksh-runner/src/worker/container_ops.rs`:886), `parse_services` (`crates/aksh-runner/src/worker/container_ops.rs`:903), `docker_create_env_uses_inherit_form_for_empty_values` (`crates/aksh-runner/src/worker/container_ops.rs`:941), `docker_exec_env_args_do_not_include_secret_values` (`crates/aksh-runner/src/worker/container_ops.rs`:959) |
| `InitializeWithCorrectManager` | 105 | Initialize With Correct Manager | **PARTIAL** | `parse_container_string` (`crates/aksh-runner/src/worker/container_ops.rs`:878), `parse_container_mapping` (`crates/aksh-runner/src/worker/container_ops.rs`:886), `parse_services` (`crates/aksh-runner/src/worker/container_ops.rs`:903), `docker_create_env_uses_inherit_form_for_empty_values` (`crates/aksh-runner/src/worker/container_ops.rs`:941), `docker_exec_env_args_do_not_include_secret_values` (`crates/aksh-runner/src/worker/container_ops.rs`:959) |

### `L0/Worker/CreateStepSummaryCommandL0.cs` — 7 tests — PARTIAL

Official behavior: GITHUB_STEP_SUMMARY upload/scrub/size-limit behavior.
Verified aksh-runner refs: `test_step_summary_size_limit_and_scrubbing` (`crates/aksh-runner/src/worker/steps_runner.rs`:1177).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CreateStepSummaryCommand_FileNull` | 31 | Create Step Summary Command File Null | **PARTIAL** | `test_step_summary_size_limit_and_scrubbing` (`crates/aksh-runner/src/worker/steps_runner.rs`:1177) |
| `CreateStepSummaryCommand_DirectoryNotFound` | 46 | Create Step Summary Command Directory Not Found | **PARTIAL** | `test_step_summary_size_limit_and_scrubbing` (`crates/aksh-runner/src/worker/steps_runner.rs`:1177) |
| `CreateStepSummaryCommand_FileNotFound` | 63 | Create Step Summary Command File Not Found | **PARTIAL** | `test_step_summary_size_limit_and_scrubbing` (`crates/aksh-runner/src/worker/steps_runner.rs`:1177) |
| `CreateStepSummaryCommand_EmptyFile` | 80 | Create Step Summary Command Empty File | **PARTIAL** | `test_step_summary_size_limit_and_scrubbing` (`crates/aksh-runner/src/worker/steps_runner.rs`:1177) |
| `CreateStepSummaryCommand_LargeFile` | 98 | Create Step Summary Command Large File | **PARTIAL** | `test_step_summary_size_limit_and_scrubbing` (`crates/aksh-runner/src/worker/steps_runner.rs`:1177) |
| `CreateStepSummaryCommand_Simple` | 116 | Create Step Summary Command Simple | **PARTIAL** | `test_step_summary_size_limit_and_scrubbing` (`crates/aksh-runner/src/worker/steps_runner.rs`:1177) |
| `CreateStepSummaryCommand_ScrubSecrets` | 140 | Create Step Summary Command Scrub Secrets | **PARTIAL** | `test_step_summary_size_limit_and_scrubbing` (`crates/aksh-runner/src/worker/steps_runner.rs`:1177) |

### `L0/Worker/DapDebuggerL0.cs` — 37 tests — GAP

Official behavior: DAP debugger not implemented/tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `InitializeSucceeds` | 335 | Initialize Succeeds | **GAP** | — |
| `StartAsyncFailsWithoutValidTunnelConfig` | 347 | Start Async Fails Without Valid Tunnel Config | **GAP** | — |
| `StartAsyncUsesPortFromTunnelConfig` | 366 | Start Async Uses Port From Tunnel Config | **GAP** | — |
| `StartAsyncWithWebSocketBridgeAcceptsInitializeOverWebSocket` | 383 | Start Async With Web Socket Bridge Accepts Initialize Over Web Socket | **GAP** | — |
| `StartAsyncWithWebSocketBridgeAcceptsPreUpgradedWebSocketStream` | 419 | Start Async With Web Socket Bridge Accepts Pre Upgraded Web Socket Stream | **GAP** | — |
| `ResolveTimeoutUsesCustomTimeoutFromEnvironment` | 461 | Resolve Timeout Uses Custom Timeout From Environment | **GAP** | — |
| `ResolveTimeoutIgnoresInvalidTimeoutFromEnvironment` | 475 | Resolve Timeout Ignores Invalid Timeout From Environment | **GAP** | — |
| `ResolveTimeoutIgnoresZeroTimeoutFromEnvironment` | 489 | Resolve Timeout Ignores Zero Timeout From Environment | **GAP** | — |
| `StartAndStopLifecycle` | 503 | Start And Stop Lifecycle | **GAP** | — |
| `StartAndStopMultipleTimesDoesNotThrow` | 520 | Start And Stop Multiple Times Does Not Throw | **GAP** | — |
| `WaitUntilReadyCompletesAfterClientConnectionAndConfigurationDone` | 537 | Wait Until Ready Completes After Client Connection And Configuration Done | **GAP** | — |
| `StartStoresJobContextForThreadsRequest` | 564 | Start Stores Job Context For Threads Request | **GAP** | — |
| `CancellationUnblocksAndOnJobCompletedTerminates` | 591 | Cancellation Unblocks And On Job Completed Terminates | **GAP** | — |
| `StopWithoutStartDoesNotThrow` | 623 | Stop Without Start Does Not Throw | **GAP** | — |
| `OnJobCompletedTerminatesSession` | 634 | On Job Completed Terminates Session | **GAP** | — |
| `WaitUntilReadyBeforeStartIsNoOp` | 661 | Wait Until Ready Before Start Is No Op | **GAP** | — |
| `WaitUntilReadyJobCancellationPropagatesAsOperationCancelledException` | 672 | Wait Until Ready Job Cancellation Propagates As Operation Cancelled Exception | **GAP** | — |
| `InitializeRequestOverSocketPreservesProtocolMetadataWhenSecretsCollide` | 694 | Initialize Request Over Socket Preserves Protocol Metadata When Secrets Collide | **GAP** | — |
| `CancellationDuringStepPauseReleasesWait` | 733 | Cancellation During Step Pause Releases Wait | **GAP** | — |
| `StopAsyncSafeAtAnyLifecyclePoint` | 776 | Stop Async Safe At Any Lifecycle Point | **GAP** | — |
| `HandleSourceReturnsJobStepsSource` | 798 | Handle Source Returns Job Steps Source | **GAP** | — |
| `StackTraceUsesJobStepsSourceLine` | 847 | Stack Trace Uses Job Steps Source Line | **GAP** | — |
| `StackTraceOmitsSourceForUnmappedCurrentStep` | 913 | Stack Trace Omits Source For Unmapped Current Step | **GAP** | — |
| `PredictedPostStepIsServedAtInitializationAndClaimedAtRegistration` | 975 | Predicted Post Step Is Served At Initialization And Claimed At Registration | **GAP** | — |
| `StackTraceSanitizesSyntheticSourcePath` | 1057 | Stack Trace Sanitizes Synthetic Source Path | **GAP** | — |
| `OnJobCompletedSendsTerminatedAndExitedEvents` | 1117 | On Job Completed Sends Terminated And Exited Events | **GAP** | — |
| `OnJobCompletedUsesSyntheticCompleteJobLineWhenPostStepSharesName` | 1197 | On Job Completed Uses Synthetic Complete Job Line When Post Step Shares Name | **GAP** | — |
| `ResolveTunnelConnectTimeoutReturnsDefaultWhenNoVariable` | 1259 | Resolve Tunnel Connect Timeout Returns Default When No Variable | **GAP** | — |
| `ResolveTunnelConnectTimeoutUsesCustomValue` | 1270 | Resolve Tunnel Connect Timeout Uses Custom Value | **GAP** | — |
| `ResolveTunnelConnectTimeoutIgnoresInvalidValue` | 1284 | Resolve Tunnel Connect Timeout Ignores Invalid Value | **GAP** | — |
| `ResolveTunnelConnectTimeoutIgnoresZeroValue` | 1298 | Resolve Tunnel Connect Timeout Ignores Zero Value | **GAP** | — |
| `WaitForCommandAsyncUnblocksOnCancellationDuringWait` | 1311 | Wait For Command Async Unblocks On Cancellation During Wait | **GAP** | — |
| `WelcomeMessageSendsDefaultHelpWhenOverrideDisabled` | 1354 | Welcome Message Sends Default Help When Override Disabled | **GAP** | — |
| `WelcomeMessageShowsCustomMessageWhenOverrideEnabled` | 1391 | Welcome Message Shows Custom Message When Override Enabled | **GAP** | — |
| `WelcomeMessageSuppressedWhenOverrideEnabledWithEmptyMessage` | 1428 | Welcome Message Suppressed When Override Enabled With Empty Message | **GAP** | — |
| `WelcomeMessageSuppressedWhenOverrideEnabledWithNullMessage` | 1472 | Welcome Message Suppressed When Override Enabled With Null Message | **GAP** | — |
| `WelcomeMessageSentOnlyOnce` | 1516 | Welcome Message Sent Only Once | **GAP** | — |

### `L0/Worker/DapMessagesL0.cs` — 13 tests — GAP

Official behavior: DAP protocol messages not implemented/tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `RequestSerializesCorrectly` | 14 | Request Serializes Correctly | **GAP** | — |
| `ResponseSerializesCorrectly` | 36 | Response Serializes Correctly | **GAP** | — |
| `EventSerializesWithCorrectType` | 61 | Event Serializes With Correct Type | **GAP** | — |
| `StoppedEventBodyOmitsNullFields` | 85 | Stopped Event Body Omits Null Fields | **GAP** | — |
| `CapabilitiesMvpDefaults` | 102 | Capabilities Mvp Defaults | **GAP** | — |
| `ContinueResponseBodySerialization` | 122 | Continue Response Body Serialization | **GAP** | — |
| `ThreadsResponseBodySerialization` | 134 | Threads Response Body Serialization | **GAP** | — |
| `StackFrameSerialization` | 155 | Stack Frame Serialization | **GAP** | — |
| `SourceRequestAndResponseSerialization` | 177 | Source Request And Response Serialization | **GAP** | — |
| `ExitedEventBodySerialization` | 207 | Exited Event Body Serialization | **GAP** | — |
| `DapCommandEnumValues` | 219 | Dap Command Enum Values | **GAP** | — |
| `RequestDeserializesFromRawJson` | 229 | Request Deserializes From Raw Json | **GAP** | — |
| `ErrorResponseBodySerialization` | 243 | Error Response Body Serialization | **GAP** | — |

### `L0/Worker/DapReplExecutorL0.cs` — 15 tests — GAP

Official behavior: DAP REPL executor not implemented/tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `ExecuteRunCommand_NullContext_ReturnsError` | 68 | Execute Run Command Null Context Returns Error | **GAP** | — |
| `ExpandExpressions_NoExpressions_ReturnsInput` | 83 | Expand Expressions No Expressions Returns Input | **GAP** | — |
| `ExpandExpressions_NullInput_ReturnsEmpty` | 97 | Expand Expressions Null Input Returns Empty | **GAP** | — |
| `ExpandExpressions_EmptyInput_ReturnsEmpty` | 111 | Expand Expressions Empty Input Returns Empty | **GAP** | — |
| `ExpandExpressions_UnterminatedExpression_KeepsLiteral` | 125 | Expand Expressions Unterminated Expression Keeps Literal | **GAP** | — |
| `ResolveDefaultShell_NoJobDefaults_ReturnsPlatformDefault` | 139 | Resolve Default Shell No Job Defaults Returns Platform Default | **GAP** | — |
| `ResolveDefaultShell_WithJobDefault_ReturnsJobDefault` | 157 | Resolve Default Shell With Job Default Returns Job Default | **GAP** | — |
| `BuildEnvironment_MergesEnvContextAndReplOverrides` | 178 | Build Environment Merges Env Context And Repl Overrides | **GAP** | — |
| `BuildEnvironment_ReplOverridesWin` | 201 | Build Environment Repl Overrides Win | **GAP** | — |
| `BuildEnvironment_NullReplEnv_ReturnsContextEnvOnly` | 223 | Build Environment Null Repl Env Returns Context Env Only | **GAP** | — |
| `CreateStepHost_NoContainer_ReturnsDefaultStepHost` | 245 | Create Step Host No Container Returns Default Step Host | **GAP** | — |
| `CreateStepHost_WithContainer_ActionStep_ReturnsContainerStepHost` | 260 | Create Step Host With Container Action Step Returns Container Step Host | **GAP** | — |
| `CreateStepHost_WithContainer_InfrastructureStep_ReturnsDefaultStepHost` | 278 | Create Step Host With Container Infrastructure Step Returns Default Step Host | **GAP** | — |
| `CreateStepHost_ContainerWithoutId_NoHooks_ReturnsDefaultStepHost` | 294 | Create Step Host Container Without Id No Hooks Returns Default Step Host | **GAP** | — |
| `CreateStepHost_ContainerWithoutId_HooksEnabled_ReturnsContainerStepHost` | 311 | Create Step Host Container Without Id Hooks Enabled Returns Container Step Host | **GAP** | — |

### `L0/Worker/DapReplParserL0.cs` — 22 tests — GAP

Official behavior: DAP REPL parser not implemented/tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `Parse_HelpReturnsHelpCommand` | 16 | Parse Help Returns Help Command | **GAP** | — |
| `Parse_HelpCaseInsensitive` | 28 | Parse Help Case Insensitive | **GAP** | — |
| `Parse_HelpWithTopic` | 38 | Parse Help With Topic | **GAP** | — |
| `Parse_RunSimpleScript` | 54 | Parse Run Simple Script | **GAP** | — |
| `Parse_RunWithShell` | 69 | Parse Run With Shell | **GAP** | — |
| `Parse_RunWithWorkingDirectory` | 82 | Parse Run With Working Directory | **GAP** | — |
| `Parse_RunWithEnv` | 95 | Parse Run With Env | **GAP** | — |
| `Parse_RunWithMultipleEnvVars` | 109 | Parse Run With Multiple Env Vars | **GAP** | — |
| `Parse_RunWithAllOptions` | 123 | Parse Run With All Options | **GAP** | — |
| `Parse_RunWithEscapedQuotes` | 143 | Parse Run With Escaped Quotes | **GAP** | — |
| `Parse_RunWithCommaInEnvValue` | 155 | Parse Run With Comma In Env Value | **GAP** | — |
| `Parse_RunEmptyArgsReturnsError` | 171 | Parse Run Empty Args Returns Error | **GAP** | — |
| `Parse_RunUnquotedArgReturnsError` | 183 | Parse Run Unquoted Arg Returns Error | **GAP** | — |
| `Parse_RunUnknownOptionReturnsError` | 195 | Parse Run Unknown Option Returns Error | **GAP** | — |
| `Parse_RunMissingClosingParenReturnsError` | 207 | Parse Run Missing Closing Paren Returns Error | **GAP** | — |
| `Parse_ExpressionReturnsNull` | 222 | Parse Expression Returns Null | **GAP** | — |
| `Parse_WrappedExpressionReturnsNull` | 233 | Parse Wrapped Expression Returns Null | **GAP** | — |
| `Parse_EmptyInputReturnsNull` | 244 | Parse Empty Input Returns Null | **GAP** | — |
| `GetGeneralHelp_ContainsCommands` | 262 | Get General Help Contains Commands | **GAP** | — |
| `GetRunHelp_ContainsOptions` | 274 | Get Run Help Contains Options | **GAP** | — |
| `SplitArguments_HandlesNestedBraces` | 290 | Split Arguments Handles Nested Braces | **GAP** | — |
| `ParseEnvBlock_HandlesEmptyBlock` | 303 | Parse Env Block Handles Empty Block | **GAP** | — |

### `L0/Worker/DapVariableProviderL0.cs` — 26 tests — GAP

Official behavior: DAP variable provider not implemented/tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `GetScopes_ReturnsEmptyWhenContextIsNull` | 38 | Get Scopes Returns Empty When Context Is Null | **GAP** | — |
| `GetScopes_ReturnsOnlyPopulatedScopes` | 50 | Get Scopes Returns Only Populated Scopes | **GAP** | — |
| `GetScopes_ReportsNamedVariableCount` | 78 | Get Scopes Reports Named Variable Count | **GAP** | — |
| `GetScopes_SecretsGetSpecialPresentationHint` | 101 | Get Scopes Secrets Get Special Presentation Hint | **GAP** | — |
| `GetVariables_ReturnsEmptyWhenContextIsNull` | 136 | Get Variables Returns Empty When Context Is Null | **GAP** | — |
| `GetVariables_ReturnsStringVariables` | 148 | Get Variables Returns String Variables | **GAP** | — |
| `GetVariables_ReturnsBooleanVariables` | 176 | Get Variables Returns Boolean Variables | **GAP** | — |
| `GetVariables_ReturnsNumberVariables` | 209 | Get Variables Returns Number Variables | **GAP** | — |
| `GetVariables_HandlesNullValues` | 232 | Get Variables Handles Null Values | **GAP** | — |
| `GetVariables_NestedDictionaryIsExpandable` | 260 | Get Variables Nested Dictionary Is Expandable | **GAP** | — |
| `GetVariables_NestedArrayIsExpandable` | 299 | Get Variables Nested Array Is Expandable | **GAP** | — |
| `GetVariables_SecretsScopeValuesAreRedacted` | 339 | Get Variables Secrets Scope Values Are Redacted | **GAP** | — |
| `GetVariables_NonSecretScopeValuesMaskedBySecretMasker` | 370 | Get Variables Non Secret Scope Values Masked By Secret Masker | **GAP** | — |
| `Reset_InvalidatesNestedReferences` | 405 | Reset Invalidates Nested References | **GAP** | — |
| `GetVariables_SetsEvaluateNameWithDotPath` | 441 | Get Variables Sets Evaluate Name With Dot Path | **GAP** | — |
| `EvaluateExpression_ReturnsValueForSimpleExpression` | 490 | Evaluate Expression Returns Value For Simple Expression | **GAP** | — |
| `EvaluateExpression_StripsWrapperSyntax` | 512 | Evaluate Expression Strips Wrapper Syntax | **GAP** | — |
| `EvaluateExpression_MasksSecretInResult` | 532 | Evaluate Expression Masks Secret In Result | **GAP** | — |
| `EvaluateExpression_ReturnsErrorForInvalidExpression` | 555 | Evaluate Expression Returns Error For Invalid Expression | **GAP** | — |
| `EvaluateExpression_ReturnsMessageWhenNoContext` | 574 | Evaluate Expression Returns Message When No Context | **GAP** | — |
| `EvaluateExpression_ReturnsEmptyForEmptyExpression` | 587 | Evaluate Expression Returns Empty For Empty Expression | **GAP** | — |
| `InferResultType_ClassifiesCorrectly` | 606 | Infer Result Type Classifies Correctly | **GAP** | — |
| `GetVariables_SecretsScopeRedactsNumberContextData` | 630 | Get Variables Secrets Scope Redacts Number Context Data | **GAP** | — |
| `GetVariables_SecretsScopeRedactsBooleanContextData` | 654 | Get Variables Secrets Scope Redacts Boolean Context Data | **GAP** | — |
| `GetVariables_SecretsScopeRedactsNestedDictionary` | 678 | Get Variables Secrets Scope Redacts Nested Dictionary | **GAP** | — |
| `GetVariables_SecretsScopeRedactsNullValue` | 707 | Get Variables Secrets Scope Redacts Null Value | **GAP** | — |

### `L0/Worker/ExecutionContextL0.cs` — 24 tests — PARTIAL

Official behavior: Annotations cap/collection, env merge, logs, masking, post state covered; telemetry/result edge cases gaps.
Verified aksh-runner refs: `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `AddIssue_CountWarningsErrors` | 23 | Add Issue Count Warnings Errors | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `ApplyContinueOnError_CheckResultAndOutcome` | 100 | Apply Continue On Error Check Result And Outcome | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `AddIssue_TrimMessageSize` | 156 | Add Issue Trim Message Size | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `AddIssue_OverrideLogMessage` | 210 | Add Issue Override Log Message | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `AddIssue_AddStepAndLineNumberInformation` | 265 | Add Issue Add Step And Line Number Information | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `Debug_Multilines` | 316 | Debug Multilines | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `RegisterPostJobStep_JobExtensionRunner_DefaultsRunnerTelemetry` | 367 | Register Post Job Step Job Extension Runner Defaults Runner Telemetry | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `RegisterPostJobStep_ActionRunner_DoesNotOverrideTelemetry` | 418 | Register Post Job Step Action Runner Does Not Override Telemetry | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `RegisterPostJobAction_ShareState` | 480 | Register Post Job Action Share State | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `RegisterPostJobAction_NotRegisterPostTwice` | 578 | Register Post Job Action Not Register Post Twice | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `ActionResult_Lowercase` | 663 | Action Result Lowercase | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `PublishStepTelemetry_RegularStep_NoOpt` | 717 | Publish Step Telemetry Regular Step No Opt | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `PublishStepTelemetry_RegularStep` | 760 | Publish Step Telemetry Regular Step | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `PublishStepTelemetry_EmbeddedStep` | 824 | Publish Step Telemetry Embedded Step | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `PublishStepResult_EmbeddedStep` | 888 | Publish Step Result Embedded Step | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `PublishStepResult_EmbeddedStep_Legacy` | 964 | Publish Step Result Embedded Step Legacy | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `GetExpressionValues_ContainerStepHost` | 1035 | Get Expression Values Container Step Host | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `ActionVariables_AddedToVarsContext` | 1153 | Action Variables Added To Vars Context | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `ActionVariables_DebugUsingVars` | 1198 | Action Variables Debug Using Vars | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `ActionVariables_SecretsPrecedenceForDebugUsingVars` | 1241 | Action Variables Secrets Precedence For Debug Using Vars | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `InitializeJob_HydratesJobContextWithCheckRunId` | 1287 | Initialize Job Hydrates Job Context With Check Run Id | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `InitializeJob_HydratesJobContextWithCheckRunId_AlwaysCopied` | 1326 | Initialize Job Hydrates Job Context With Check Run Id Always Copied | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `InitializeJob_HydratesJobContextWithWorkflowIdentity` | 1358 | Initialize Job Hydrates Job Context With Workflow Identity | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `InitializeJob_WorkflowIdentityNotSet_WhenServerSendsNoData` | 1396 | Initialize Job Workflow Identity Not Set When Server Sends No Data | **PARTIAL** | `annotations_cap_enforced` (`crates/aksh-runner/src/worker/execution_context.rs`:288), `annotations_collected` (`crates/aksh-runner/src/worker/execution_context.rs`:271), `build_env_includes_extra_path` (`crates/aksh-runner/src/worker/execution_context.rs`:238), `build_env_merges_job_and_step` (`crates/aksh-runner/src/worker/execution_context.rs`:226), `log_content_joins_lines` (`crates/aksh-runner/src/worker/execution_context.rs`:257), `log_masks_secrets` (`crates/aksh-runner/src/worker/execution_context.rs`:249), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |

### `L0/Worker/Expressions/ConditionFunctionsL0.cs` — 4 tests — GAP

Official behavior: Condition function semantics not directly tested in aksh-runner.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `AlwaysFunction` | 21 | Always Function | **GAP** | — |
| `CancelledFunction` | 50 | Cancelled Function | **GAP** | — |
| `FailureFunction` | 80 | Failure Function | **GAP** | — |
| `SuccessFunction` | 139 | Success Function | **GAP** | — |

### `L0/Worker/HandlerFactoryL0.cs` — 15 tests — PARTIAL

Official behavior: Manifest loading dispatch covered; handler factory matrix edge cases mostly gaps.
Verified aksh-runner refs: `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `IsNodeVersionUpgraded` | 37 | Is Node Version Upgraded | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `Node24ExplicitlyRequested_HonoredByDefault` | 82 | Node24 Explicitly Requested Honored By Default | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `Node20Action_TrackedWhenWarnFlagEnabled` | 123 | Node20 Action Tracked When Warn Flag Enabled | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `Node20Action_NotTrackedWhenWarnFlagDisabled` | 174 | Node20 Action Not Tracked When Warn Flag Disabled | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `Node24Action_NotTrackedEvenWhenWarnFlagEnabled` | 222 | Node24 Action Not Tracked Even When Warn Flag Enabled | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `Node12Action_TrackedAsDeprecatedWhenWarnFlagEnabled` | 273 | Node12 Action Tracked As Deprecated When Warn Flag Enabled | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `LocalNode20Action_TrackedWhenWarnFlagEnabled` | 324 | Local Node20 Action Tracked When Warn Flag Enabled | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `Node20Action_TrackedAsUpgradedWhenUseNode24ByDefaultEnabled` | 377 | Node20 Action Tracked As Upgraded When Use Node24 By Default Enabled | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `Node20Action_NotUpgradedWhenPhase1Only` | 440 | Node20 Action Not Upgraded When Phase1 Only | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `ExplicitNode24Action_KillArm32Flag_ThrowsOnArm32` | 496 | Explicit Node24 Action Kill Arm32 Flag Throws On Arm32 | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `ExplicitNode24Action_DeprecateArm32Flag_DowngradesToNode20OnArm32` | 567 | Explicit Node24 Action Deprecate Arm32 Flag Downgrades To Node20 On Arm32 | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `ExplicitNode24Action_NoArm32Flags_StaysNode24` | 631 | Explicit Node24 Action No Arm32 Flags Stays Node24 | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `Node20Action_RequireNode24_ForcesNode24` | 689 | Node20 Action Require Node24 Forces Node24 | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `Node20Action_KillArm32Flag_ThrowsOnArm32` | 752 | Node20 Action Kill Arm32 Flag Throws On Arm32 | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |
| `ExplicitNode24Action_DeprecateArm32_UsesOriginalVersionForTracking` | 821 | Explicit Node24 Action Deprecate Arm32 Uses Original Version For Tracking | **PARTIAL** | `load_node_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:128), `load_composite_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:170), `load_docker_action_manifest` (`crates/aksh-runner/src/worker/handlers/factory.rs`:194) |

### `L0/Worker/HandlerL0.cs` — 2 tests — GAP

Official behavior: Base handler behavior not tested directly.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `PrepareExecution_PopulateTelemetry_RepoActions` | 36 | Prepare Execution Populate Telemetry Repo Actions | **GAP** | — |
| `PrepareExecution_PopulateTelemetry_DockerActions` | 65 | Prepare Execution Populate Telemetry Docker Actions | **GAP** | — |

### `L0/Worker/Handlers/CompositeActionHandlerL0.cs` — 23 tests — PARTIAL

Official behavior: One action_status context restoration test; marker/input/output/nesting behavior largely gaps.
Verified aksh-runner refs: `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `EscapeProperty_EscapesSpecialCharacters` | 25 | Escape Property Escapes Special Characters | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `EscapeProperty_HandlesNullAndEmpty` | 36 | Escape Property Handles Null And Empty | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `SanitizeDisplayName_TruncatesLongNames` | 45 | Sanitize Display Name Truncates Long Names | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `SanitizeDisplayName_TakesFirstLineOnly` | 55 | Sanitize Display Name Takes First Line Only | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `SanitizeDisplayName_TrimsLeadingWhitespace` | 65 | Sanitize Display Name Trims Leading Whitespace | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `SanitizeDisplayName_HandlesCarriageReturn` | 75 | Sanitize Display Name Handles Carriage Return | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `SanitizeDisplayName_HandlesNullAndEmpty` | 85 | Sanitize Display Name Handles Null And Empty | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `EmitMarkers_DisplayNameEscaping` | 94 | Emit Markers Display Name Escaping | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `EmitMarkers_DisplayNameWithBrackets` | 105 | Emit Markers Display Name With Brackets | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `StripUserEmittedMarkers_StartAction` | 115 | Strip User Emitted Markers Start Action | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `StripUserEmittedMarkers_EndAction` | 127 | Strip User Emitted Markers End Action | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `StripUserEmittedMarkers_PreservesOtherCommands` | 138 | Strip User Emitted Markers Preserves Other Commands | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `StripUserEmittedMarkers_HandlesEmbeddedMarkers` | 148 | Strip User Emitted Markers Handles Embedded Markers | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `TaskResultToActionResult_Success` | 158 | Task Result To Action Result Success | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `TaskResultToActionResult_Failure` | 169 | Task Result To Action Result Failure | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `TaskResultToActionResult_Cancelled` | 180 | Task Result To Action Result Cancelled | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `TaskResultToActionResult_Skipped` | 191 | Task Result To Action Result Skipped | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `MarkerFormat_StartAction` | 202 | Marker Format Start Action | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `MarkerFormat_EndAction` | 213 | Marker Format End Action | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `MarkerFormat_NestedId` | 226 | Marker Format Nested Id | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `MarkerFormat_SkippedStep` | 237 | Marker Format Skipped Step | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `MarkerFormat_ContinueOnError` | 247 | Marker Format Continue On Error | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |
| `PostStepMarker_UsesEvaluatedDisplayName` | 260 | Post Step Marker Uses Evaluated Display Name | **PARTIAL** | `composite_steps_receive_action_status_context` (`crates/aksh-runner/src/worker/handlers/composite.rs`:347) |

### `L0/Worker/Handlers/NodeHandlerL0.cs` — 1 tests — GAP

Official behavior: Node action handler runtime behavior not directly tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `NodeJSActionExecutionDataSupportsNode24` | 21 | Node JSAction Execution Data Supports Node24 | **GAP** | — |

### `L0/Worker/IssueMatcherL0.cs` — 25 tests — PARTIAL

Official behavior: Literal severity only; official matcher validation and multi-pattern runtime mostly gaps.
Verified aksh-runner refs: `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `Config_Validate_Loop_MayNotBeSetOnSinglePattern` | 13 | Config Validate Loop May Not Be Set On Single Pattern | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Config_Validate_Loop_OnlyAllowedOnLastPattern` | 49 | Config Validate Loop Only Allowed On Last Pattern | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Config_Validate_Loop_MustSetMessage` | 86 | Config Validate Loop Must Set Message | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Config_Validate_Message_AllowedInFirstPattern` | 118 | Config Validate Message Allowed In First Pattern | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Config_Validate_Message_Required` | 145 | Config Validate Message Required | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Config_Validate_Owner_Distinct` | 173 | Config Validate Owner Distinct | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Config_Validate_Owner_Required` | 209 | Config Validate Owner Required | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Config_Validate_Pattern_Required` | 236 | Config Validate Pattern Required | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Config_Validate_PropertyMayNotBeSetTwice` | 266 | Config Validate Property May Not Be Set Twice | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Config_Validate_PropertyOutOfRange` | 302 | Config Validate Property Out Of Range | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Config_Validate_PropertyOutOfRange_LessThanZero` | 329 | Config Validate Property Out Of Range Less Than Zero | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_MultiplePatterns_DefaultSeverity` | 356 | Matcher Multiple Patterns Default Severity | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_MultiplePatterns_DefaultSeverityNotice` | 398 | Matcher Multiple Patterns Default Severity Notice | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_MultiplePatterns_Loop_AccumulatesStatePerLine` | 427 | Matcher Multiple Patterns Loop Accumulates State Per Line | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_MultiplePatterns_Loop_BrokenMatchClearsState` | 491 | Matcher Multiple Patterns Loop Broken Match Clears State | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_MultiplePatterns_Loop_ExtractsProperties` | 544 | Matcher Multiple Patterns Loop Extracts Properties | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_MultiplePatterns_NonLoop_AccumulatesStatePerLine` | 609 | Matcher Multiple Patterns Non Loop Accumulates State Per Line | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_MultiplePatterns_NonLoop_DoesNotLoop` | 670 | Matcher Multiple Patterns Non Loop Does Not Loop | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_MultiplePatterns_NonLoop_ExtractsProperties` | 705 | Matcher Multiple Patterns Non Loop Extracts Properties | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_MultiplePatterns_NonLoop_MatchClearsState` | 755 | Matcher Multiple Patterns Non Loop Match Clears State | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_SetsOwner` | 803 | Matcher Sets Owner | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_SinglePattern_DefaultSeverity` | 828 | Matcher Single Pattern Default Severity | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_SinglePattern_ExtractsProperties` | 865 | Matcher Single Pattern Extracts Properties | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_SinglePattern_DefaultFromPath` | 903 | Matcher Single Pattern Default From Path | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `Matcher_MultiplePatterns_DefaultFromPath` | 977 | Matcher Multiple Patterns Default From Path | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |

### `L0/Worker/JobContextL0.cs` — 15 tests — PARTIAL

Official behavior: Context roots, variables, masks, github context, status covered; official variable dictionary edge cases partial.
Verified aksh-runner refs: `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CheckRunId_SetAndGet_WorksCorrectly` | 11 | Check Run Id Set And Get Works Correctly | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `CheckRunId_NotSet_ReturnsNull` | 22 | Check Run Id Not Set Returns Null | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `CheckRunId_SetNull_RemovesKey` | 30 | Check Run Id Set Null Removes Key | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowRef_SetAndGet_WorksCorrectly` | 39 | Workflow Ref Set And Get Works Correctly | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowRef_NotSet_ReturnsNull` | 49 | Workflow Ref Not Set Returns Null | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowRef_SetNull_ClearsValue` | 56 | Workflow Ref Set Null Clears Value | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowSha_SetAndGet_WorksCorrectly` | 65 | Workflow Sha Set And Get Works Correctly | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowSha_NotSet_ReturnsNull` | 75 | Workflow Sha Not Set Returns Null | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowSha_SetNull_ClearsValue` | 82 | Workflow Sha Set Null Clears Value | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowRepository_SetAndGet_WorksCorrectly` | 91 | Workflow Repository Set And Get Works Correctly | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowRepository_NotSet_ReturnsNull` | 101 | Workflow Repository Not Set Returns Null | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowRepository_SetNull_ClearsValue` | 108 | Workflow Repository Set Null Clears Value | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowFilePath_SetAndGet_WorksCorrectly` | 117 | Workflow File Path Set And Get Works Correctly | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowFilePath_NotSet_ReturnsNull` | 127 | Workflow File Path Not Set Returns Null | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |
| `WorkflowFilePath_SetNull_ClearsValue` | 134 | Workflow File Path Set Null Clears Value | **PARTIAL** | `build_expression_context_has_required_roots` (`crates/aksh-runner/src/worker/contexts.rs`:512), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497), `job_status_failure_reflects_in_context` (`crates/aksh-runner/src/worker/contexts.rs`:549), `set_github_context_value_updates_context_and_env` (`crates/aksh-runner/src/worker/contexts.rs`:566), `vars_context_decodes_typed_dict_format` (`crates/aksh-runner/src/worker/contexts.rs`:605) |

### `L0/Worker/JobExecutionViewL0.cs` — 3 tests — GAP

Official behavior: Job execution view/display behavior not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `RendersPreMainAndPostSections` | 15 | Renders Pre Main And Post Sections | **GAP** | — |
| `ClaimsPredictedPostStepWithoutChangingLine` | 38 | Claims Predicted Post Step Without Changing Line | **GAP** | — |
| `UsesSyntheticCompleteJobLineWhenPostStepSharesName` | 65 | Uses Synthetic Complete Job Line When Post Step Shares Name | **GAP** | — |

### `L0/Worker/JobExtensionL0.cs` — 25 tests — PARTIAL

Official behavior: Step-list parsing, env injection, lifecycle pre/post covered; many official job extension paths still gaps.
Verified aksh-runner refs: `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `JobExtensionBuildStepsList` | 170 | Job Extension Build Steps List | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `JobExtensionBuildPreStepsList` | 205 | Job Extension Build Pre Steps List | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `JobExtensionBuildFailsWithoutContainerIfRequired` | 244 | Job Extension Build Fails Without Container If Required | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `UploadDiganosticLogIfEnvironmentVariableSet` | 262 | Upload Diganostic Log If Environment Variable Set | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `DontUploadDiagnosticLogIfEnvironmentVariableFalse` | 290 | Dont Upload Diagnostic Log If Environment Variable False | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `DontUploadDiagnosticLogIfEnvironmentVariableMissing` | 318 | Dont Upload Diagnostic Log If Environment Variable Missing | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EnsureFinalizeJobRunsIfMessageHasNoEnvironmentUrl` | 340 | Ensure Finalize Job Runs If Message Has No Environment Url | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EnsureFinalizeJobHandlesNullEnvironmentUrl` | 362 | Ensure Finalize Job Handles Null Environment Url | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EnsureFinalizeJobHandlesNullEnvironment` | 387 | Ensure Finalize Job Handles Null Environment | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EnsurePreAndPostHookStepsIfEnvExists` | 410 | Ensure Pre And Post Hook Steps If Env Exists | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EnsureNoPreAndPostHookSteps` | 441 | Ensure No Pre And Post Hook Steps | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EnsureNoSnapshotPostJobStep` | 466 | Ensure No Snapshot Post Job Step | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EnsureSnapshotPostJobStepForStringToken` | 487 | Ensure Snapshot Post Job Step For String Token | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EnsureSnapshotPostJobStepForMappingToken` | 497 | Ensure Snapshot Post Job Step For Mapping Token | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `EnsureSnapshotPostJobStepForMappingToken_WithIf_Is_False` | 512 | Ensure Snapshot Post Job Step For Mapping Token With If Is False | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `SnapshotPreflightChecks_HostedRunnerCheck_Enabled_GitHubHosted_Success` | 583 | Snapshot Preflight Checks Hosted Runner Check Enabled Git Hub Hosted Success | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `SnapshotPreflightChecks_HostedRunnerCheck_Enabled_SelfHosted_ThrowsException` | 615 | Snapshot Preflight Checks Hosted Runner Check Enabled Self Hosted Throws Exception | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `SnapshotPreflightChecks_ImageGenPoolCheck_Enabled_ImageGenEnabled_Success` | 644 | Snapshot Preflight Checks Image Gen Pool Check Enabled Image Gen Enabled Success | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `SnapshotPreflightChecks_ImageGenPoolCheck_Enabled_ImageGen_False_ThrowsException` | 676 | Snapshot Preflight Checks Image Gen Pool Check Enabled Image Gen False Throws Exception | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `SnapshotPreflightChecks_ImageGenPoolCheck_Enabled_ImageGen_Missing_ThrowsException` | 707 | Snapshot Preflight Checks Image Gen Pool Check Enabled Image Gen Missing Throws Exception | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `SnapshotPreflightChecks_BothChecks_Enabled_AllConditionsMet_Success` | 735 | Snapshot Preflight Checks Both Checks Enabled All Conditions Met Success | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `DebuggerStartedInSetupJobWhenEnabled` | 771 | Debugger Started In Setup Job When Enabled | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `DebuggerNotStartedInSetupJobWhenDisabled` | 816 | Debugger Not Started In Setup Job When Disabled | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `DebuggerCleanedUpInFinalizeJob` | 845 | Debugger Cleaned Up In Finalize Job | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |
| `FinalizeJobHandlesDebuggerCleanupException` | 891 | Finalize Job Handles Debugger Cleanup Exception | **PARTIAL** | `build_step_list_parses_script_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:834), `build_step_list_parses_action_reference` (`crates/aksh-runner/src/worker/job_extension.rs`:859), `inject_github_env_sets_core_vars` (`crates/aksh-runner/src/worker/job_extension.rs`:794), `injects_job_environment_variables_from_acquire_payload` (`crates/aksh-runner/src/worker/job_extension.rs`:1060), `inject_actions_env_from_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_extension.rs`:994), `lifecycle_uses_resolved_action_path_and_entry_overrides` (`crates/aksh-runner/src/worker/job_extension.rs`:1087), `lifecycle_registers_docker_action_pre_and_post` (`crates/aksh-runner/src/worker/job_extension.rs`:1148) |

### `L0/Worker/JobRunnerL0.cs` — 3 tests — PARTIAL

Official behavior: Results URL extraction, execution lifecycle, and parsing of job request message type.
Verified aksh-runner refs: `results_url_prefers_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_runner.rs`:1219), `test_run_job_executes_successfully` (`crates/aksh-runner/src/worker/job_runner.rs`:1221).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `JobExtensionInitializeFailure` | 134 | Job Extension Initialize Failure | **PARTIAL** | `results_url_prefers_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_runner.rs`:1219) |
| `JobExtensionInitializeCancelled` | 151 | Job Extension Initialize Cancelled | **PARTIAL** | `results_url_prefers_system_vss_endpoint_data` (`crates/aksh-runner/src/worker/job_runner.rs`:1219) |
| `WorksWithRunnerJobRequestMessageType` | 169 | Works With Runner Job Request Message Type | **PARTIAL** | `test_run_job_executes_successfully` (`crates/aksh-runner/src/worker/job_runner.rs`:1221) |

### `L0/Worker/OutputManagerL0.cs` — 22 tests — PARTIAL

Official behavior: Output manager/problem matcher runtime line processing.
Verified aksh-runner refs: `test_multi_pattern_matching_lifecycle` (`crates/aksh-runner/src/worker/matchers.rs`:652), `test_multi_pattern_matching_with_loop` (`crates/aksh-runner/src/worker/matchers.rs`:697), `test_repository_path_resolution` (`crates/aksh-runner/src/worker/matchers.rs`:727), `log_raw_problem_matching_and_telemetry` (`crates/aksh-runner/src/worker/execution_context.rs`:388).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `AddMatcher_Clobber` | 33 | Add Matcher Clobber | **PARTIAL** | `test_multi_pattern_matching_lifecycle` (`crates/aksh-runner/src/worker/matchers.rs`:652) |
| `AddMatcher_Prepend` | 99 | Add Matcher Prepend | **PARTIAL** | `test_multi_pattern_matching_lifecycle` (`crates/aksh-runner/src/worker/matchers.rs`:652) |
| `MatcherCode` | 165 | Matcher Code | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `DoesNotResetMatchingMatcher` | 204 | Does Not Reset Matching Matcher | **PARTIAL** | `test_multi_pattern_matching_lifecycle` (`crates/aksh-runner/src/worker/matchers.rs`:652) |
| `InitialMatchers` | 259 | Initial Matchers | **PARTIAL** | `log_raw_problem_matching_and_telemetry` (`crates/aksh-runner/src/worker/execution_context.rs`:388) |
| `MatcherLineColumn` | 307 | Matcher Line Column | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `MatcherDoesNotReceiveCommand` | 355 | Matcher Does Not Receive Command | **PARTIAL** | `log_raw_problem_matching_and_telemetry` (`crates/aksh-runner/src/worker/execution_context.rs`:388) |
| `MatcherRemoveColorCodes` | 389 | Matcher Remove Color Codes | **PARTIAL** | `matcher_strips_ansi_color_codes_before_matching` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `RemoveMatcher` | 418 | Remove Matcher | **PARTIAL** | `matcher_owner_can_be_removed` (`crates/aksh-runner/src/worker/matchers.rs`:321) |
| `ResetsOtherMatchers` | 471 | Resets Other Matchers | **PARTIAL** | `log_raw_problem_matching_and_telemetry` (`crates/aksh-runner/src/worker/execution_context.rs`:388) |
| `MatcherSeverity` | 535 | Matcher Severity | **PARTIAL** | `matcher_accepts_literal_severity` (`crates/aksh-runner/src/worker/matchers.rs`:203) |
| `MatcherTimeout` | 596 | Matcher Timeout | **NOT_APPLICABLE** | Rust regex crate linear time guarantees prevent catastrophic backtracking, timeouts not required |
| `MatcherFile` | 650 | Matcher File | **PARTIAL** | `test_repository_path_resolution` (`crates/aksh-runner/src/worker/matchers.rs`:727) |
| `MatcherFile_JobContainer` | 764 | Matcher File Job Container | **PARTIAL** | `test_repository_path_resolution` (`crates/aksh-runner/src/worker/matchers.rs`:727) |
| `MatcherFile_StepContainer` | 825 | Matcher File Step Container | **PARTIAL** | `test_repository_path_resolution` (`crates/aksh-runner/src/worker/matchers.rs`:727) |
| `MatcherFromPath` | 887 | Matcher From Path | **PARTIAL** | `test_repository_path_resolution` (`crates/aksh-runner/src/worker/matchers.rs`:727) |
| `MatcherDefaultFromPath` | 943 | Matcher Default From Path | **PARTIAL** | `test_repository_path_resolution` (`crates/aksh-runner/src/worker/matchers.rs`:727) |
| `CaptureTelemetryForGitUnsafeRepository` | 999 | Capture Telemetry For Git Unsafe Repository | **PARTIAL** | `log_raw_problem_matching_and_telemetry` (`crates/aksh-runner/src/worker/execution_context.rs`:388) |
| `StripCompositeMarkers_StartAction` | 1012 | Strip Composite Markers Start Action | **PARTIAL** | `log_raw_problem_matching_and_telemetry` (`crates/aksh-runner/src/worker/execution_context.rs`:388) |
| `StripCompositeMarkers_EndAction` | 1027 | Strip Composite Markers End Action | **PARTIAL** | `log_raw_problem_matching_and_telemetry` (`crates/aksh-runner/src/worker/execution_context.rs`:388) |
| `StripCompositeMarkers_PreservesOtherCommands` | 1042 | Strip Composite Markers Preserves Other Commands | **PARTIAL** | `log_raw_problem_matching_and_telemetry` (`crates/aksh-runner/src/worker/execution_context.rs`:388) |
| `StripCompositeMarkers_EmbeddedInLine` | 1057 | Strip Composite Markers Embedded In Line | **PARTIAL** | `log_raw_problem_matching_and_telemetry` (`crates/aksh-runner/src/worker/execution_context.rs`:388) |
### `L0/Worker/PipelineDirectoryManagerL0.cs` — 8 tests — GAP

Official behavior: Workspace/pipeline directory tracking/cleanup not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CreatesPipelineDirectories` | 24 | Creates Pipeline Directories | **GAP** | — |
| `DeletesResourceDirectoryWhenCleanIsResources` | 44 | Deletes Resource Directory When Clean Is Resources | **GAP** | — |
| `DeletesNonResourceDirectoryWhenCleanIsOutputs` | 70 | Deletes Non Resource Directory When Clean Is Outputs | **GAP** | — |
| `RecreatesPipelinesDirectoryWhenCleanIsAll` | 95 | Recreates Pipelines Directory When Clean Is All | **GAP** | — |
| `UpdatesExistingConfig` | 122 | Updates Existing Config | **GAP** | — |
| `UpdatesRepositoryDirectoryWorkspaceRepo` | 142 | Updates Repository Directory Workspace Repo | **GAP** | — |
| `UpdatesRepositoryDirectoryNoneWorkspaceRepo` | 163 | Updates Repository Directory None Workspace Repo | **GAP** | — |
| `UpdatesRepositoryDirectoryThrowOnInvalidPath` | 184 | Updates Repository Directory Throw On Invalid Path | **GAP** | — |

### `L0/Worker/PipelineTemplateEvaluatorWrapperL0.cs` — 29 tests — PARTIAL

Official behavior: Template substitution basics covered; official pipeline-template evaluator matrix much broader.
Verified aksh-runner refs: `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `EvaluateAndCompare_DoesNotRecordMismatch_WhenResultsMatch` | 33 | Evaluate And Compare Does Not Record Mismatch When Results Match | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateAndCompare_SkipsMismatchRecording_WhenCancellationOccursDuringEvaluation` | 60 | Evaluate And Compare Skips Mismatch Recording When Cancellation Occurs During Evaluation | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateAndCompare_SkipsMismatchRecording_WhenRootCancellationOccursBetweenEvaluators` | 95 | Evaluate And Compare Skips Mismatch Recording When Root Cancellation Occurs Between Evaluators | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateAndCompare_RecordsMismatch_WhenResultsDifferWithoutCancellation` | 132 | Evaluate And Compare Records Mismatch When Results Differ Without Cancellation | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateStepContinueOnError_BothParsersAgree` | 164 | Evaluate Step Continue On Error Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateStepEnvironment_BothParsersAgree` | 190 | Evaluate Step Environment Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateStepIf_BothParsersAgree` | 218 | Evaluate Step If Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateStepInputs_BothParsersAgree` | 245 | Evaluate Step Inputs Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateStepTimeout_BothParsersAgree` | 273 | Evaluate Step Timeout Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobContainer_EmptyImage_BothParsersAgree` | 299 | Evaluate Job Container Empty Image Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobContainer_DockerPrefixOnly_BothParsersAgree` | 325 | Evaluate Job Container Docker Prefix Only Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobContainer_DockerPrefixOnlyMapping_BothParsersAgree` | 351 | Evaluate Job Container Docker Prefix Only Mapping Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobContainer_EmptyImageMapping_BothParsersAgree` | 378 | Evaluate Job Container Empty Image Mapping Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobContainer_ValidImage_BothParsersAgree` | 405 | Evaluate Job Container Valid Image Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobContainer_DockerPrefixWithImage_BothParsersAgree` | 432 | Evaluate Job Container Docker Prefix With Image Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobOutput_BothParsersAgree` | 459 | Evaluate Job Output Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateEnvironmentUrl_BothParsersAgree` | 487 | Evaluate Environment Url Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobDefaultsRun_BothParsersAgree` | 516 | Evaluate Job Defaults Run Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobServiceContainers_Null_BothParsersAgree` | 544 | Evaluate Job Service Containers Null Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobServiceContainers_EmptyImage_BothParsersAgree` | 569 | Evaluate Job Service Containers Empty Image Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobServiceContainers_DockerPrefixOnlyImage_BothParsersAgree` | 605 | Evaluate Job Service Containers Docker Prefix Only Image Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobServiceContainers_ExpressionEvalsToEmpty_BothParsersAgree` | 638 | Evaluate Job Service Containers Expression Evals To Empty Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobServiceContainers_ValidImage_BothParsersAgree` | 673 | Evaluate Job Service Containers Valid Image Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobServiceContainers_EntrypointAndCommand_BothParsersAgree` | 707 | Evaluate Job Service Containers Entrypoint And Command Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobServiceContainers_EntrypointAndCommand_FlagOff_BothParsersAgree` | 745 | Evaluate Job Service Containers Entrypoint And Command Flag Off Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateJobSnapshotRequest_Null_BothParsersAgree` | 776 | Evaluate Job Snapshot Request Null Both Parsers Agree | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateAndCompare_JsonReaderExceptions_TreatedAsEquivalent` | 805 | Evaluate And Compare Json Reader Exceptions Treated As Equivalent | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateAndCompare_MixedJsonExceptionTypes_TreatedAsEquivalent` | 836 | Evaluate And Compare Mixed Json Exception Types Treated As Equivalent | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |
| `EvaluateAndCompare_NonJsonExceptions_RecordsMismatch` | 867 | Evaluate And Compare Non Json Exceptions Records Mismatch | **PARTIAL** | `simple_expression` (`crates/aksh-runner/src/worker/template.rs`:136), `multiple_expressions` (`crates/aksh-runner/src/worker/template.rs`:143), `passthrough_literal` (`crates/aksh-runner/src/worker/template.rs`:151), `no_expressions` (`crates/aksh-runner/src/worker/template.rs`:127), `build_step_list_parses_github_template_token_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:882), `build_step_list_parses_aksh_template_string_maps` (`crates/aksh-runner/src/worker/job_extension.rs`:918) |

### `L0/Worker/SaveStateFileCommandL0.cs` — 15 tests — PARTIAL

Official behavior: State file command parsing and post-step state exposure.
Verified aksh-runner refs: `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `SaveStateFileCommand_DirectoryNotFound` | 33 | Save State File Command Directory Not Found | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_NotFound` | 47 | Save State File Command Not Found | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_EmptyFile` | 61 | Save State File Command Empty File | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Simple` | 77 | Save State File Command Simple | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Simple_SkipEmptyLines` | 97 | Save State File Command Simple Skip Empty Lines | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Simple_EmptyValue` | 122 | Save State File Command Simple Empty Value | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Simple_MultipleValues` | 142 | Save State File Command Simple Multiple Values | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Simple_SpecialCharacters` | 166 | Save State File Command Simple Special Characters | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Heredoc` | 190 | Save State File Command Heredoc | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Heredoc_EmptyValue` | 214 | Save State File Command Heredoc Empty Value | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Heredoc_SkipEmptyLines` | 235 | Save State File Command Heredoc Skip Empty Lines | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Heredoc_SpecialCharacters` | 266 | Save State File Command Heredoc Special Characters | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Heredoc_MissingNewLine` | 309 | Save State File Command Heredoc Missing New Line | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Heredoc_MissingNewLineMultipleLines` | 331 | Save State File Command Heredoc Missing New Line Multiple Lines | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |
| `SaveStateFileCommand_Heredoc_PreservesNewline` | 354 | Save State File Command Heredoc Preserves Newline | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `lifecycle_state_is_stored_under_original_step_id` (`crates/aksh-runner/src/worker/file_commands.rs`:235), `post_step_env_exposes_saved_state_from_main_step` (`crates/aksh-runner/src/worker/execution_context.rs`:308) |

### `L0/Worker/SetEnvFileCommandL0.cs` — 17 tests — PARTIAL

Official behavior: Env file command parsing/simple heredoc; many invalid/missing-file and blocked-name cases are gaps.
Verified aksh-runner refs: `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `SetEnvFileCommand_DirectoryNotFound` | 32 | Set Env File Command Directory Not Found | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_NotFound` | 46 | Set Env File Command Not Found | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_EmptyFile` | 60 | Set Env File Command Empty File | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Simple` | 76 | Set Env File Command Simple | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Simple_SkipEmptyLines` | 96 | Set Env File Command Simple Skip Empty Lines | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Simple_EmptyValue` | 121 | Set Env File Command Simple Empty Value | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Simple_MultipleValues` | 141 | Set Env File Command Simple Multiple Values | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Simple_SpecialCharacters` | 165 | Set Env File Command Simple Special Characters | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_BlockListItemsFiltered` | 190 | Set Env File Command Block List Items Filtered | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_BlockListItemsFiltered_Heredoc` | 209 | Set Env File Command Block List Items Filtered Heredoc | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Heredoc` | 230 | Set Env File Command Heredoc | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Heredoc_EmptyValue` | 254 | Set Env File Command Heredoc Empty Value | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Heredoc_SkipEmptyLines` | 275 | Set Env File Command Heredoc Skip Empty Lines | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Heredoc_SpecialCharacters` | 306 | Set Env File Command Heredoc Special Characters | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Heredoc_MissingNewLine` | 349 | Set Env File Command Heredoc Missing New Line | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Heredoc_MissingNewLineMultipleLinesEnv` | 371 | Set Env File Command Heredoc Missing New Line Multiple Lines Env | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |
| `SetEnvFileCommand_Heredoc_PreservesNewline` | 394 | Set Env File Command Heredoc Preserves Newline | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206), `create_and_cleanup` (`crates/aksh-runner/src/worker/file_commands.rs`:225) |

### `L0/Worker/SetOutputFileCommandL0.cs` — 15 tests — PARTIAL

Official behavior: Output file command parsing/simple heredoc; many invalid/missing-file cases are gaps.
Verified aksh-runner refs: `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `SetOutputFileCommand_DirectoryNotFound` | 33 | Set Output File Command Directory Not Found | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_NotFound` | 47 | Set Output File Command Not Found | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_EmptyFile` | 61 | Set Output File Command Empty File | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Simple` | 77 | Set Output File Command Simple | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Simple_SkipEmptyLines` | 97 | Set Output File Command Simple Skip Empty Lines | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Simple_EmptyValue` | 122 | Set Output File Command Simple Empty Value | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Simple_MultipleValues` | 142 | Set Output File Command Simple Multiple Values | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Simple_SpecialCharacters` | 166 | Set Output File Command Simple Special Characters | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Heredoc` | 190 | Set Output File Command Heredoc | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Heredoc_EmptyValue` | 214 | Set Output File Command Heredoc Empty Value | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Heredoc_SkipEmptyLines` | 235 | Set Output File Command Heredoc Skip Empty Lines | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Heredoc_SpecialCharacters` | 266 | Set Output File Command Heredoc Special Characters | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Heredoc_MissingNewLine` | 309 | Set Output File Command Heredoc Missing New Line | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Heredoc_MissingNewLineMultipleLines` | 331 | Set Output File Command Heredoc Missing New Line Multiple Lines | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |
| `SetOutputFileCommand_Heredoc_PreservesNewline` | 354 | Set Output File Command Heredoc Preserves Newline | **PARTIAL** | `parse_simple_kv` (`crates/aksh-runner/src/worker/file_commands.rs`:196), `parse_heredoc` (`crates/aksh-runner/src/worker/file_commands.rs`:206) |

### `L0/Worker/SnapshotOperationProviderL0.cs` — 1 tests — GAP

Official behavior: Snapshot operation provider not implemented/tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CreateSnapshotRequestAsync` | 25 | Create Snapshot Request Async | **GAP** | — |

### `L0/Worker/StepHostL0.cs` — 7 tests — GAP

Official behavior: Step host runtime/container execution selection not directly tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `DetermineNodeRuntimeVersionInContainerAsync` | 38 | Determine Node Runtime Version In Container Async | **GAP** | — |
| `DetermineNodeRuntimeVersionInAlpineContainerAsync` | 61 | Determine Node Runtime Version In Alpine Container Async | **GAP** | — |
| `DetermineNode20RuntimeVersionInAlpineContainerAsync` | 88 | Determine Node20 Runtime Version In Alpine Container Async | **GAP** | — |
| `DetermineNodeRuntimeVersionInUnknowContainerAsync` | 115 | Determine Node Runtime Version In Unknow Container Async | **GAP** | — |
| `DetermineNode20RuntimeVersionInUnknowContainerAsync` | 142 | Determine Node20 Runtime Version In Unknow Container Async | **GAP** | — |
| `DetermineNode24RuntimeVersionInAlpineContainerAsync` | 169 | Determine Node24 Runtime Version In Alpine Container Async | **GAP** | — |
| `DetermineNode24RuntimeVersionInUnknownContainerAsync` | 196 | Determine Node24 Runtime Version In Unknown Container Async | **GAP** | — |

### `L0/Worker/StepHostNodeVersionL0.cs` — 8 tests — GAP

Official behavior: Node version selection inside containers/ARM32 not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CheckNodeVersionForArm32_Node24OnArm32Linux` | 24 | Check Node Version For Arm32 Node24 On Arm32 Linux | **GAP** | — |
| `CheckNodeVersionForArm32_PassThroughNonNode24Versions` | 53 | Check Node Version For Arm32 Pass Through Non Node24 Versions | **GAP** | — |
| `CheckNodeVersionForArm32_DeprecationFlagShowsWarning` | 66 | Check Node Version For Arm32 Deprecation Flag Shows Warning | **GAP** | — |
| `CheckNodeVersionForArm32_DeprecationFlagWithNode20PassesThrough` | 92 | Check Node Version For Arm32 Deprecation Flag With Node20 Passes Through | **GAP** | — |
| `CheckNodeVersionForArm32_KillFlagReturnsNull` | 118 | Check Node Version For Arm32 Kill Flag Returns Null | **GAP** | — |
| `CheckNodeVersionForArm32_KillTakesPrecedenceOverDeprecation` | 143 | Check Node Version For Arm32 Kill Takes Precedence Over Deprecation | **GAP** | — |
| `CheckNodeVersionForArm32_ServerOverridableDateUsedInDeprecationWarning` | 168 | Check Node Version For Arm32 Server Overridable Date Used In Deprecation Warning | **GAP** | — |
| `CheckNodeVersionForArm32_FallbackDateUsedWhenNoOverride` | 196 | Check Node Version For Arm32 Fallback Date Used When No Override | **GAP** | — |

### `L0/Worker/StepsRunnerL0.cs` — 13 tests — PARTIAL

Official behavior: Step execution loop semantics — sequential execution, condition evaluation (`success()`, `failure()`, `always()`, `cancelled()`), official implicit `success()` gating for conditions without status-check functions, `continue-on-error` correctness, env/context mutation between steps, and step outcome/conclusion visibility in later conditions.
Verified aksh-runner refs: `run_steps_all_steps_pass`, `run_steps_continue_on_error_sets_failure_outcome_success_conclusion`, `run_steps_job_status_remains_success_after_continue_on_error`, `run_steps_conditions_reflect_prior_failure`, `run_steps_implicitly_gates_conditions_with_success`, `status_check_function_detection_ignores_string_literals`, `run_steps_cancelled_condition_runs_only_when_cancelled`, `run_steps_outcome_visible_in_later_step_condition`, `run_steps_marks_condition_error_as_failure`, `run_steps_step_env_override_job_env`, `run_steps_github_env_is_visible_to_later_steps`, `run_steps_outputs_are_visible_to_later_step_expressions`.

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `RunNormalStepsAllStepPass` | 79 | All steps pass sequentially | **PARTIAL** | `run_steps_all_steps_pass` |
| `RunNormalStepsContinueOnError` | 111 | continue-on-error: outcome=Failure, conclusion=Success, job stays Success | **PARTIAL** | `run_steps_continue_on_error_sets_failure_outcome_success_conclusion`, `run_steps_job_status_remains_success_after_continue_on_error` |
| `RunsAfterFailureBasedOnCondition` | 146 | failure() condition runs after step failure | **PARTIAL** | `run_steps_conditions_reflect_prior_failure` |
| `RunsAlwaysSteps` | 185 | always() runs after failure and cancellation | **PARTIAL** | `run_steps_conditions_reflect_prior_failure`, `run_steps_cancelled_condition_runs_only_when_cancelled` |
| `SetsJobResultCorrectly` | 239 | Job result reflects step outcomes | **PARTIAL** | `run_steps_conditions_reflect_prior_failure`, `run_steps_job_status_remains_success_after_continue_on_error` |
| `SkipsAfterFailureOnlyBaseOnCondition` | 317 | success() skipped after failure | **PARTIAL** | `run_steps_conditions_reflect_prior_failure` |
| `AlwaysMeansAlways` | 360 | always() runs regardless of cancel or failure | **PARTIAL** | `run_steps_conditions_reflect_prior_failure`, `run_steps_cancelled_condition_runs_only_when_cancelled` |
| `TreatsConditionErrorAsFailure` | 391 | Condition parse error marks step failed | **PARTIAL** | `run_steps_marks_condition_error_as_failure` |
| `StepEnvOverrideJobEnvContext` | 419 | Step env overrides job env | **PARTIAL** | `run_steps_step_env_override_job_env` |
| `PopulateEnvContextForEachStep` | 452 | GITHUB_ENV visible to later steps | **PARTIAL** | `run_steps_github_env_is_visible_to_later_steps` |
| `PopulateEnvContextAfterSetupStepsContext` | 491 | Steps context populated before env context | **PARTIAL** | `run_steps_github_env_is_visible_to_later_steps`, `run_steps_outputs_are_visible_to_later_step_expressions` |
| `StepContextOutcome` | 527 | steps.X.outcome visible in later conditions | **PARTIAL** | `run_steps_outcome_visible_in_later_step_condition` |
| `StepContextConclusion` | 563 | steps.X.conclusion visible in later conditions | **PARTIAL** | `run_steps_outcome_visible_in_later_step_condition` |


### `L0/Worker/TrackingManagerL0.cs` — 4 tests — GAP

Official behavior: Workspace tracking config persistence not tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `CreatesTrackingConfig` | 39 | Creates Tracking Config | **GAP** | — |
| `LoadsTrackingConfig` | 69 | Loads Tracking Config | **GAP** | — |
| `LoadsTrackingConfig_NotExists` | 94 | Loads Tracking Config Not Exists | **GAP** | — |
| `UpdatesTrackingConfigJobRunProperties` | 111 | Updates Tracking Config Job Run Properties | **GAP** | — |

### `L0/Worker/VariablesL0.cs` — 8 tests — PARTIAL

Official behavior: Secret masking and variable lookup covered in JobContext, but official Variables class edge cases not mirrored.
Verified aksh-runner refs: `new_extracts_masks_from_secret_variables` (`crates/aksh-runner/src/worker/contexts.rs`:445), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `Constructor_AppliesMaskHints` | 15 | Constructor Applies Mask Hints | **PARTIAL** | `new_extracts_masks_from_secret_variables` (`crates/aksh-runner/src/worker/contexts.rs`:445), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497) |
| `Constructor_HandlesNullValue` | 37 | Constructor Handles Null Value | **PARTIAL** | `new_extracts_masks_from_secret_variables` (`crates/aksh-runner/src/worker/contexts.rs`:445), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497) |
| `Constructor_SetsNullAsEmpty` | 60 | Constructor Sets Null As Empty | **PARTIAL** | `new_extracts_masks_from_secret_variables` (`crates/aksh-runner/src/worker/contexts.rs`:445), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497) |
| `Constructor_SetsOrdinalIgnoreCaseComparer` | 81 | Constructor Sets Ordinal Ignore Case Comparer | **PARTIAL** | `new_extracts_masks_from_secret_variables` (`crates/aksh-runner/src/worker/contexts.rs`:445), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497) |
| `Constructor_SkipVariableWithEmptyName` | 116 | Constructor Skip Variable With Empty Name | **PARTIAL** | `new_extracts_masks_from_secret_variables` (`crates/aksh-runner/src/worker/contexts.rs`:445), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497) |
| `Get_ReturnsNullIfNotFound` | 140 | Get Returns Null If Not Found | **PARTIAL** | `new_extracts_masks_from_secret_variables` (`crates/aksh-runner/src/worker/contexts.rs`:445), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497) |
| `GetBoolean_DoesNotThrowWhenNull` | 158 | Get Boolean Does Not Throw When Null | **PARTIAL** | `new_extracts_masks_from_secret_variables` (`crates/aksh-runner/src/worker/contexts.rs`:445), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497) |
| `GetEnum_DoesNotThrowWhenNull` | 176 | Get Enum Does Not Throw When Null | **PARTIAL** | `new_extracts_masks_from_secret_variables` (`crates/aksh-runner/src/worker/contexts.rs`:445), `mask_secrets_replaces_with_stars` (`crates/aksh-runner/src/worker/contexts.rs`:459), `get_variable_returns_value` (`crates/aksh-runner/src/worker/contexts.rs`:497) |

### `L0/Worker/WebSocketDapBridgeL0.cs` — 4 tests — GAP

Official behavior: DAP WebSocket bridge not implemented/tested.
Verified aksh-runner refs: —

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `BridgeForwardsWebSocketFramesToTcpAndBack` | 52 | Bridge Forwards Web Socket Frames To Tcp And Back | **GAP** | — |
| `BridgeRejectsNonWebSocketRequests` | 134 | Bridge Rejects Non Web Socket Requests | **GAP** | — |
| `BridgeRejectsOversizedWebSocketMessage` | 195 | Bridge Rejects Oversized Web Socket Message | **GAP** | — |
| `BridgeShutdownCompletesWhenPeerDoesNotCloseGracefully` | 243 | Bridge Shutdown Completes When Peer Does Not Close Gracefully | **GAP** | — |

### `L0/Worker/WorkerL0.cs` — 2 tests — PARTIAL

Official behavior: Worker top-level run loop and cancellation dispatch.
Verified aksh-runner refs: `test_worker_dispatch_run_new_job` (`crates/aksh-runner/src/listener/job_dispatcher.rs`:164), `test_worker_dispatch_cancellation` (`crates/aksh-runner/src/listener/job_dispatcher.rs`:191).

| C# test | Line | Official behavior under test | aksh-runner coverage status | Verified Rust test refs |
|---|---:|---|---|---|
| `DispatchRunNewJob` | 82 | Dispatch Run New Job | **PARTIAL** | `test_worker_dispatch_run_new_job` (`crates/aksh-runner/src/listener/job_dispatcher.rs`:164) |
| `DispatchCancellation` | 134 | Dispatch Cancellation | **PARTIAL** | `test_worker_dispatch_cancellation` (`crates/aksh-runner/src/listener/job_dispatcher.rs`:191) |
