# Runner Test Compatibility Plan

Bucketed implementation plan for closing aksh-runner test gaps against the official C# runner. Ordered by correctness priority.

This plan is based on the verified comparison in `docs/test-coverage.md`.

## High-level status summary

| Priority | Bucket / Area | C# Tests | Status (2026-07-05) | Description of Coverage / Gaps |
|---|---|---:|---|---|
| **P0** | **Step execution semantics** | 90 | **LIVE_VERIFIED / PARTIAL** | Rust unit coverage plus live GitHub verification for sequential steps, env/output propagation, `continue-on-error`, condition functions, failure propagation, and external cancellation. |
| **P0** | **File commands, outputs, matchers** | 117 | **LIVE_VERIFIED / PARTIAL** | File commands, summaries, NODE_OPTIONS blocking, ANSI matcher stripping, add/remove matcher, and problem matcher annotations verified live; deeper OutputManager parity remains partial. |
| **P0** | **Actions, manifests, composite execution** | 168 | **PARTIAL** | Manifest evaluation, basic composite, and Docker action args/env covered by unit tests; live P0 workflows avoid remote action download and nested composite edge cases. |
| **P0** | **Containers and step host** | 24 | **LIVE_VERIFIED_WITH_CAVEAT / PARTIAL** | Linux smolvm runner successfully ran Docker/container verification once; later fresh `docker pull` attempts exposed smolvm DNS/storage setup limits, not runner-command construction failures. |
| **P1** | **Expressions and templates** | 37 | **LIVE_VERIFIED / PARTIAL** | Live GitHub and local aksh E2E cover env/template functions, conditions, and step fields; evaluator cancellation and matrix/needs breadth remain gaps. |
| **P1** | **Listener / configuration lifecycle** | 115 | **LIVE_VERIFIED / PARTIAL** | Live GitHub and local aksh E2E cover configure, OAuth, broker session, job acquire, worker dispatch, and completion; reconnect/backoff/interactive config remain gaps. |
| **P1** | **Process / runtime environment** | 93 | **LIVE_VERIFIED / PARTIAL** | Live GitHub and local aksh E2E cover stdout/stderr, cwd, env, exit-code propagation, `continue-on-error`, timeout field parsing, and long output; proxy/process-tree/workspace cleanup remain gaps. |
| **P1** | **Protocol / client DTO behavior** | 35 | **LIVE_VERIFIED / PARTIAL** | Live GitHub and local aksh E2E cover Twirp step updates, log uploads, annotations, grouping/debug commands, and completion; client HTTP error behavior remains gap. |
| **P2** | **DAP / debugging** | 117 | **NOT_IMPLEMENTED** | No runner DAP server, message framing, WebSocket bridge, REPL, breakpoints, variables, step pausing, or debugger cancellation path exists. |
| **P2** | **Background / snapshot / aux features** | 14 | **NOT_IMPLEMENTED** | Protocol DTOs include background timeline fields, but runner execution does not implement background/wait steps, snapshot provider, or job execution view semantics. |
| **P3** | **Official runner infrastructure** | 32 | **NOT_APPLICABLE / DEFERRED** | Windows service control, self-update, official constant generation, paging logger, and .NET bootstrapper are official-runner infrastructure, not current aksh macOS/Linux runtime correctness. |

## Current implementation status — 2026-07-05

The P0/P1 runner slice from this plan has been implemented or classified through three gates:

- **Live GitHub primary gate using `aksh-runner` against real GitHub Actions:** P0 step execution (`28754418659`, success), P0 failure conditions (`28754419325`, expected failure), P0 file commands (`28755293879`, success), P0 Docker/container verification (`28755911596`, success on Linux smolvm), P0 cancellation (`28756327702`, cancelled with `cancelled()`/`always()` markers observed in runner logs), P1 expressions (`28756574650`, success), P1 listener/config (`28756828143`, success), P1 process/runtime (`28756827413`, success), and P1 protocol/logging (`28756578118`, success).
- **Local aksh control-plane gate using `aksh-runner` + `aksh-runner-server`:** `aksh-conformance runner-e2e` passed for `p0-step-execution.yml`, `p0-failure-conditions.yml`, `p0-file-commands.yml`, `p1-expressions.yml`, `p1-process-runtime.yml`, and `p1-protocol.yml`, recording flows under `/tmp/aksh-*-flows.jsonl`. The same gate exposed one local control-plane divergence: `p1-listener-config.yml` completed with `conclusion:"failed"` because the local submitted job did not populate all GitHub context env values asserted by the workflow. The cancel workflow needs an external GitHub cancellation signal and the Docker workflow needs a Linux Docker daemon, so those remain live-GitHub/Linux-smolvm-only.
- **Unit and focused runner coverage:** step execution semantics, file commands, matchers, action manifest factory, composite actions, Docker action handler, container ops, process cancellation, config/settings, and protocol DTO surfaces have Rust coverage mapped in `docs/test-coverage.md`.

The detailed baseline sections below are retained as the original gap-analysis input. They are not a live list of remaining P0/P1 work after the 2026-07-05 implementation and live-verification pass.

## Compatibility scoring

This is **test-compatibility**, not proof of implementation compatibility.

- **100%** — behavior family fully mirrored by Rust tests. None of the large buckets currently reach this.
- **50%** — Rust has verified tests in the same behavior family, but official edge cases remain.
- **0%** — no verified `aksh-runner` test equivalent.
- `OUTSIDE_RUNNER` / `NOT_APPLICABLE` are called out separately.

Verified baseline:

| Inventory | Count |
|---|---:|
| Official C# L0 tests extracted | 842 |
| Official C# test files | 82 |
| `aksh-runner` lib tests | 90 |
| `aksh-runner` test files | 18 |

Overall verified classification:

| Status | Official tests |
|---|---:|
| PARTIAL | 406 |
| GAP | 403 |
| OUTSIDE_RUNNER | 24 |
| NOT_APPLICABLE | 9 |

---

## P0 — Step execution semantics

**Why P0:** Every workflow depends on correct step ordering, conditions, result propagation, context mutation, cancellation, and post-step behavior.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 90 |
| Rust coverage status | 88 partial / 2 gap |
| Test-compatibility | ~49% |

### Official areas included

- `StepsRunnerL0.cs` — 13 tests
- `JobRunnerL0.cs` — 3 tests
- `ExecutionContextL0.cs` — 24 tests
- `JobContextL0.cs` — 15 tests
- `JobExtensionL0.cs` — 25 tests
- `VariablesL0.cs` — 8 tests
- `WorkerL0.cs` — 2 tests

### Rust coverage that exists

Step-list parsing:

- `build_step_list_parses_script_reference`
- `build_step_list_parses_action_reference`
- `build_step_list_handles_continue_on_error`
- `build_step_list_handles_template_continue_on_error`

Env/context setup:

- `inject_github_env_sets_core_vars`
- `injects_job_environment_variables_from_acquire_payload`
- `build_expression_context_has_required_roots`
- `vars_context_decodes_typed_dict_format`
- `set_github_context_value_updates_context_and_env`

Execution context:

- `annotations_collected`
- `annotations_cap_enforced`
- `build_env_merges_job_and_step`
- `build_env_includes_extra_path`
- `log_masks_secrets`
- `post_step_env_exposes_saved_state_from_main_step`

Lifecycle:

- `lifecycle_uses_resolved_action_path_and_entry_overrides`
- `lifecycle_registers_docker_action_pre_and_post`

### What is missing

All originally identified gaps have been closed with verified Rust tests:

1. ~~Actual step loop semantics~~ — **DONE**: `run_steps_all_steps_pass`, `run_steps_conditions_reflect_prior_failure` (covers fail-fast, `success()`, `failure()`, `always()`, skip-after-failure), `run_steps_cancelled_condition_runs_only_when_cancelled` (covers `cancelled()`), `run_steps_marks_condition_error_as_failure`.

2. ~~`continue-on-error` correctness~~ — **DONE**: `run_steps_continue_on_error_sets_failure_outcome_success_conclusion` (outcome/conclusion), `run_steps_job_status_remains_success_after_continue_on_error` (job stays success, later step runs), `run_steps_outcome_visible_in_later_step_condition` (steps.X.outcome/conclusion in conditions).

3. ~~Context mutation between steps~~ — **DONE**: `run_steps_github_env_is_visible_to_later_steps` (env updates), `run_steps_outputs_are_visible_to_later_step_expressions` (steps context), `run_steps_step_env_override_job_env` (step env overrides job env).

4. ~~Worker top-level loop~~ — **DONE**: `test_worker_dispatch_run_new_job`, `test_worker_dispatch_cancellation` (job dispatch + cancel via subprocess), `test_run_job_executes_successfully`, `test_run_job_propagates_step_failure` (failure propagation).

### Tests written (supersedes "First tests to write")

All suggested tests have been implemented and verified:

- ✅ `run_steps_all_steps_pass` (was: `steps_runner_runs_all_steps_when_successful`)
- ✅ `run_steps_conditions_reflect_prior_failure` (was: `steps_runner_skips_success_condition_after_failure` + `steps_runner_runs_always_after_failure`)
- ✅ `run_steps_continue_on_error_sets_failure_outcome_success_conclusion` (was: `steps_runner_continue_on_error_sets_outcome_failure_conclusion_success`)
- ✅ `run_steps_outputs_are_visible_to_later_step_expressions` (was: `steps_context_is_populated_after_each_step`)
- ✅ `run_steps_marks_condition_error_as_failure` (was: `condition_error_marks_step_failed`)
- ✅ `run_steps_cancelled_condition_runs_only_when_cancelled` (new — covers `cancelled()` gap)
- ✅ `run_steps_job_status_remains_success_after_continue_on_error` (new — covers job-status-after-continue-on-error gap)
- ✅ `run_steps_outcome_visible_in_later_step_condition` (new — covers steps.X.outcome in conditions gap)
- ✅ `test_run_job_propagates_step_failure` (new — covers failure propagation to complete-job)
---

## P0 — File commands, outputs, logs, problem matchers

**Why P0:** This is how actions communicate outputs/env/state to later steps. Silent bugs here corrupt workflows without obvious failure.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 117 |
| Rust coverage status | 88 partial / 29 gap |
| Test-compatibility | ~38% |

### Official areas included

- `SaveStateFileCommandL0.cs` — 15 tests
- `SetEnvFileCommandL0.cs` — 17 tests
- `SetOutputFileCommandL0.cs` — 15 tests
- `CreateStepSummaryCommandL0.cs` — 7 tests
- `OutputManagerL0.cs` — 22 tests
- `IssueMatcherL0.cs` — 25 tests
- `ActionCommandL0.cs` — 2 tests
- `ActionCommandManagerL0.cs` — 14 tests

### Rust coverage that exists

File commands:

- `parse_simple_kv`
- `parse_heredoc`
- `parse_path_file_lines`
- `lifecycle_state_is_stored_under_original_step_id`

Workflow command parser:

- `parse_simple_command`
- `parse_command_with_properties`
- `parse_add_mask`
- `parse_legacy_format`
- `parse_case_insensitive`
- `unescape_data_values`
- `unescape_property_values`
- `set_output_legacy`

Masking:

- `mask_secrets_replaces_with_stars`
- `add_mask_adds_new_secret`

Problem matcher:

- `matcher_accepts_literal_severity`

### What is missing

1. File command edge cases:
   - missing file path
   - missing directory
   - empty file
   - empty value
   - multiple values
   - special characters
   - heredoc empty value
   - heredoc delimiter edge cases
   - malformed heredoc
   - blocked env names like `NODE_OPTIONS`

2. Step summary:
   - null file
   - missing file
   - empty file no-op
   - large file limit
   - secret scrubbing before upload

3. Problem matcher runtime:
   - add/remove matcher
   - clobber owner
   - prepend order
   - multi-pattern matcher
   - loop matcher
   - timeout behavior
   - ANSI color stripping
   - severity capture
   - line/column/code capture
   - matcher does not consume workflow commands

4. Output manager:
   - line-by-line processing
   - matcher reset behavior
   - command passthrough
   - log masking order

### First tests to write

- `file_commands_parse_empty_values`
- `file_commands_parse_multiple_values`
- `file_commands_reject_malformed_heredoc`
- `file_commands_skip_empty_lines_like_official_runner`
- `set_env_blocks_node_options`
- `step_summary_scrubs_secrets`
- `problem_matcher_captures_file_line_column_message`
- `problem_matcher_remove_by_owner`
- `output_manager_strips_ansi_before_matching`

---

## P0 — Actions, manifests, composite execution

**Why P0:** Most real workflows use actions. Bugs here break `actions/checkout`, `actions/cache`, composite actions, Docker actions, and pre/post cleanup.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 168 |
| Rust coverage status | 133 partial / 35 gap |
| Test-compatibility | ~40% |

### Official areas included

- `ActionManagerL0.cs` — 57 tests
- `ActionManifestManagerL0.cs` — 25 tests
- `ActionManifestManagerLegacyL0.cs` — 24 tests
- `ActionManifestParserComparisonL0.cs` — 8 tests
- `ActionRunnerL0.cs` — 13 tests
- `HandlerFactoryL0.cs` — 15 tests
- `HandlerL0.cs` — 2 tests
- `CompositeActionHandlerL0.cs` — 23 tests
- `NodeHandlerL0.cs` — 1 test

### Rust coverage that exists

Manifest parsing:

- `load_node_action_manifest`
- `load_composite_action_manifest`
- `load_docker_action_manifest`
- `missing_manifest_returns_error`

Action references:

- `action_repository_context_extracts_repository_and_ref`
- `action_repository_context_is_empty_for_local_and_docker_actions`
- `build_step_list_parses_action_reference`

Lifecycle:

- `lifecycle_uses_resolved_action_path_and_entry_overrides`
- `lifecycle_registers_docker_action_pre_and_post`

Docker action runtime:

- `manifest_env_entrypoint_and_args_evaluate_against_inputs`
- `docker_run_args_apply_entrypoint_args_and_hide_env_values`

Composite:

- `composite_steps_receive_action_status_context`

### What is missing

1. Action manifest edge cases:
   - DockerHub image refs
   - `Dockerfile` vs `docker://...`
   - no args/no env/no entrypoint
   - default `pre-if`
   - default `post-if`
   - invalid expression contexts
   - valid expression contexts
   - plugin action variants
   - `action.yaml` vs `action.yml` precedence
   - missing `runs.using` error quality

2. Legacy manifest compatibility:
   - entire `ActionManifestManagerLegacyL0.cs` surface
   - legacy/new parser comparison
   - mismatch telemetry

3. Composite action execution:
   - input default evaluation
   - `INPUT_*` env mapping
   - nested step output collection
   - `outputs.<name>.value`
   - nested `uses:` actions
   - recursion/nesting depth limit
   - failure stops remaining composite steps
   - `continue-on-error` inside composite

4. Composite markers:
   - `##[start-action]`
   - `##[end-action]`
   - marker escaping
   - stripping user-emitted markers
   - display-name truncation/sanitization

5. ActionRunner display names:
   - script action display name
   - container action display name
   - local action display name
   - remote action with path
   - expression expansion
   - explicit `name:` overrides generated display

6. Action download/cache manager:
   - archive download
   - cache reuse
   - action resolution
   - action package layout
   - auth headers/token behavior

### First tests to write

- `manifest_loads_dockerhub_image_ref`
- `manifest_defaults_post_if_to_always`
- `manifest_defaults_pre_if_to_always`
- `composite_maps_inputs_to_input_env`
- `composite_evaluates_output_values_from_nested_steps`
- `composite_stops_after_nested_step_failure`
- `composite_enforces_nesting_depth_limit`
- `action_display_name_remote_with_path_matches_official`
- `action_manager_reuses_downloaded_action`

---

## P0 — Containers and step host

**Why P0:** Local preloop/smolvm workflows depend on container behavior. Container semantics are often where runner compatibility diverges.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 24 |
| Rust coverage status | 9 partial / 15 gap |
| Test-compatibility | ~19% |

### Official areas included

- `ContainerOperationProviderL0.cs` — 5 tests
- `ContainerInfoL0.cs` — 1 test
- `DockerUtilL0.cs` — 3 tests
- `StepHostL0.cs` — 7 tests
- `StepHostNodeVersionL0.cs` — 8 tests

### Rust coverage that exists

- `parse_container_string`
- `parse_container_mapping`
- `parse_services`
- `path_translation`
- `sanitize_image`
- `container_naming`
- `action_container_naming`
- `network_name_format`
- `docker_create_env_uses_inherit_form_for_empty_values`
- `docker_exec_env_args_do_not_include_secret_values`

### What is missing

1. StepHost selection:
   - host step vs container step
   - job container vs service container
   - command execution inside container

2. Node runtime selection inside containers:
   - Alpine detection
   - unknown distro detection
   - Node 20 vs Node 24
   - ARM32 fallback
   - deprecation warning flags
   - kill flag behavior

3. Container operation provider:
   - create/start/stop/remove lifecycle
   - service health waiting
   - network attach/detach
   - cleanup on cancellation

### First tests to write

- `step_host_executes_in_job_container_when_job_container_present`
- `step_host_executes_on_host_without_job_container`
- `node_runtime_detects_alpine_container`
- `node_runtime_falls_back_for_unknown_container`
- `container_cleanup_runs_after_cancel`

---

## P1 — Expressions and templates

**Why P1:** Expressions affect conditions, env, matrix, inputs, outputs. Many expression tests live in parser/expression crates, but runner integration still matters.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 37 |
| Rust coverage status | 29 partial / 4 gap / 4 outside-runner |
| Test-compatibility | ~39% runner-local |

### Official areas included

- `PipelineTemplateEvaluatorWrapperL0.cs` — 29 tests
- `ConditionFunctionsL0.cs` — 4 tests
- `ExpressionParserL0.cs` — 4 tests outside runner

### Rust coverage that exists

Runner-local:

- `simple_expression`
- `multiple_expressions`
- `passthrough_literal`
- `no_expressions`
- `build_step_list_parses_github_template_token_maps`
- `build_step_list_parses_aksh_template_string_maps`

Outside runner:

- `aksh-gha-expressions` has parser/evaluator tests, but those are not counted as `aksh-runner` tests.

### What is missing

1. Condition functions in runner context:
   - `success()`
   - `failure()`
   - `always()`
   - `cancelled()`
   - interaction with prior steps/job status

2. Pipeline template evaluator wrapper:
   - `continue-on-error`
   - `timeout-minutes`
   - `env`
   - `with`
   - container image fields
   - matrix/needs context
   - cancellation during evaluation
   - parser mismatch recording

3. Runner integration:
   - expression values become actual step fields correctly
   - errors fail/skips as official runner does

### First tests to write

- `condition_success_reflects_prior_step_success`
- `condition_failure_reflects_prior_step_failure`
- `condition_always_runs_after_failure`
- `evaluate_step_timeout_minutes_template`
- `evaluate_step_env_template_against_matrix`
- `evaluate_container_image_template`

---

## P1 — Listener/configuration lifecycle

**Why P1:** Needed for real hosted runners, registration, broker polling, and job dispatch. Less important than per-job correctness, but critical for cloud hosted runners.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 115 |
| Rust coverage status | 44 partial / 71 gap |
| Test-compatibility | ~19% |

### Official areas included

- `CommandLineParserL0.cs`
- `CommandSettingsL0.cs`
- `ConfigurationManagerL0.cs`
- `RunnerCredentialL0.cs`
- `ArgumentValidatorTestsL0.cs`
- `PromptManagerTestsL0.cs`
- `BrokerMessageListenerL0.cs`
- `MessageListenerL0.cs`
- `JobDispatcherL0.cs`
- `RunnerL0.cs`
- `ErrorThrottlerL0.cs`
- `RunnerConfigUpdaterTests.cs`

### Rust coverage that exists

CLI parsing:

- `parse_configure`
- `parse_run_defaults`
- `parse_run_azdo`
- `parse_remove`
- `parse_worker`
- `global_ca_bundle_arg`

Settings:

- `round_trip_settings`
- `config_lifecycle`
- `rsa_params_field_names`
- `strip_bom_works`

### What is missing

1. Broker listener:
   - poll loop
   - reconnect/backoff
   - broker migration URL
   - message ack
   - job acquire flow
   - cancellation/shutdown

2. Job dispatcher:
   - parallel jobs
   - duplicate job IDs
   - cancel while running
   - worker process spawning
   - worker completion/failure propagation

3. Configuration manager:
   - interactive configure prompts
   - replace existing runner
   - remove/unregister
   - ephemeral runner config
   - credential scheme variants
   - invalid URL/token handling

4. Error throttling:
   - repeated errors dampened
   - reset after success/time

### First tests to write

- `broker_listener_reconnects_after_transient_failure`
- `broker_listener_acks_after_job_dispatch`
- `job_dispatcher_cancels_running_worker`
- `job_dispatcher_rejects_duplicate_job`
- `configure_replace_deletes_existing_agent`
- `remove_unregisters_runner`

---

## P1 — Process/runtime environment

**Why P1:** Directly affects script behavior and portability, but many official utility tests are implementation-detail helpers.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 93 |
| Rust coverage status | 12 partial / 81 gap |
| Test-compatibility | ~6% |

### Official areas included

- `ProcessInvokerL0.cs`
- `ProcessExtensionL0.cs`
- `RunnerWebProxyL0.cs`
- `PipelineDirectoryManagerL0.cs`
- `TrackingManagerL0.cs`
- utility classes:
  - `WhichUtilL0.cs`
  - `IOUtilL0.cs`
  - `StringUtilL0.cs`
  - `ArgUtilL0.cs`
  - `TaskResultUtilL0.cs`
  - `UrlUtilL0.cs`
  - `VssUtilL0.cs`

### Rust coverage that exists

Process cancellation:

- `cancel_sends_sigint_before_hard_kill`
- `cancel_falls_back_to_sigterm_when_sigint_is_ignored`

### What is missing

1. Process invocation:
   - stdout/stderr streaming
   - working directory
   - env propagation
   - exit code mapping
   - process tree kill
   - cancellation race conditions
   - long-running process behavior

2. Proxy:
   - `HTTP_PROXY`
   - `HTTPS_PROXY`
   - `NO_PROXY`
   - proxy credential masking
   - proxy bypass matching

3. Workspace layout/tracking:
   - clean modes
   - repository directory layout
   - `_temp`, `_tool`, `_actions`
   - tracking config persistence

4. Utility behavior:
   - path search
   - URL normalization
   - filesystem deletion/retry
   - task result merging

### First tests to write

- `process_invoker_streams_stdout_and_stderr_in_order`
- `process_invoker_sets_working_directory`
- `process_invoker_kills_process_tree_on_cancel`
- `proxy_no_proxy_matches_suffix_and_exact_host`
- `pipeline_directory_clean_all_recreates_workspace`
- `which_finds_executable_on_path`

---

## P1 — Protocol/client DTO behavior

**Why P1:** Some of this belongs in protocol/server crates, not runner. Still important for wire compatibility.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 35 |
| Rust coverage status | 3 partial / 12 gap / 20 outside-runner |
| Test-compatibility | ~4% runner-local |

### Official areas included

- `AcquireJobRequestL0.cs`
- `AgentJobRequestMessageL0.cs`
- `AnnotationsL0.cs`
- `RunServiceHttpClientL0.cs`
- `LaunchHttpClientL0.cs`
- `TimelineRecordL0.cs`
- `WellKnownRegularExpressionsL0.cs`

### Rust coverage that exists

Runner-local:

- `annotations_collected`
- `annotations_cap_enforced`

Outside runner:

- `aksh-gha-protocol` has DTO/protocol tests.
- `aksh-runner-server` has server-side API tests.

### What is missing in runner-local tests

- Run service client HTTP error behavior
- Launch client behavior
- annotation DTO conversion parity
- well-known regex constants
- acquire job request edge cases

### First tests to write

Likely better placed in `aksh-gha-protocol` / client crates:

- `agent_job_request_message_deserializes_timeline_and_variables`
- `annotation_empty_message_is_not_emitted`
- `run_service_client_handles_empty_success_response`
- `run_service_client_preserves_error_body`

---

## P2 — DAP/debugging

**Why P2:** Product-important for a debugger experience, but not required for basic workflow correctness. It becomes P0 if debugger is the current product milestone.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 117 |
| Rust coverage status | 117 gap |
| Test-compatibility | 0% |

### Official areas included

- `DapDebuggerL0.cs` — 37
- `DapMessagesL0.cs` — 13
- `DapReplExecutorL0.cs` — 15
- `DapReplParserL0.cs` — 22
- `DapVariableProviderL0.cs` — 26
- `WebSocketDapBridgeL0.cs` — 4

### What is missing

Everything:

- DAP message framing
- TCP DAP server
- WebSocket bridge
- pause/resume
- breakpoints
- source view
- stack trace
- variables
- REPL parser
- expression evaluator
- remote tunnel/relay
- cancellation during pause
- terminated/exited events

### First tests to write

- `dap_message_round_trips_content_length_frame`
- `dap_initialize_returns_capabilities_and_initialized_event`
- `dap_configuration_done_marks_session_ready`
- `dap_on_step_starting_emits_stopped_and_blocks`
- `dap_continue_unblocks_paused_step`
- `dap_cancellation_unblocks_paused_step`
- `dap_stack_trace_maps_current_step_to_execution_yml`

---

## P2 — Background/snapshot/aux runner features

**Why P2:** Useful feature parity, not the core happy path.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 14 |
| Rust coverage status | 14 gap |
| Test-compatibility | 0% |

### Official areas included

- `BackgroundStepsL0.cs` — 10
- `SnapshotOperationProviderL0.cs` — 1
- `JobExecutionViewL0.cs` — 3

### What is missing

- background steps
- wait steps
- cancellation of background tasks
- background failure propagation
- steps context thread safety
- job execution view/display behavior
- snapshot operation provider

### First tests to write

- `background_step_runs_concurrently_with_foreground`
- `wait_step_blocks_until_background_completes`
- `background_step_failure_propagates_at_wait`
- `cancel_step_terminates_background_step`

---

## P3 — Official runner infrastructure not core aksh correctness

**Why P3:** Mostly official runner implementation infrastructure, Windows service support, self-update, and .NET bootstrap. Not core to aksh protocol/runtime correctness.

| Metric | Value |
|---|---:|
| Official C# tests in bucket | 32 |
| Rust coverage status | 9 not applicable / 23 gap |
| Test-compatibility | low / intentionally deprioritized |

### Official areas included

- `NativeWindowsServiceHelperL0.cs`
- `SelfUpdaterL0.cs`
- `SelfUpdaterV2L0.cs`
- `ServiceControlManagerL0.cs`
- `ServiceInterfacesL0.cs`
- `ExtensionManagerL0.cs`
- `HostContextL0.cs`
- `PagingLoggerL0.cs`
- `ConstantGenerationL0.cs`
- `DotnetsdkDownloadScriptL0.cs`

### What is missing

- self-update
- Windows service control
- service registry
- host context service lifecycle
- paging logger
- official runner constant generation
- .NET SDK bootstrap script

### Recommendation

Do not spend correctness budget here until P0/P1 is closed. Some should stay intentionally unsupported.

---

## Priority order

1. **Step execution semantics** — ~49% compatibility. Breaks every workflow if wrong.
2. **File commands / outputs / matchers** — ~38%. High silent-corruption risk.
3. **Actions / composite / manifests** — ~40%. Most workflows use actions.
4. **Containers / step host** — ~19%. Critical for smolvm/container workflows.
5. **Expressions / templates** — ~39%. Integration gaps remain despite parser tests elsewhere.
6. **Listener / config lifecycle** — ~19%. Cloud hosted runners depend on this.
7. **Process / runtime env** — ~6%. Important after core step/action semantics.
8. **Protocol / client DTOs** — ~4% runner-local. Mostly belongs in protocol/client crates.
9. **DAP / debugging** — 0%. Product-critical if debugger becomes the milestone.
10. **Official infra/self-update/Windows** — defer or explicitly unsupported.
