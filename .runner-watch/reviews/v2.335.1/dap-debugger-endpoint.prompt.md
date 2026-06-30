Adversarially review this aksh implementation against the spec. Return review.toml exactly as in docs/runner-watch-plan.md. You have independent cargo-test evidence from the orchestrator below; do not run formatters or project-wide lint.

Cargo test evidence:
```text
cargo test --workspace: exit status: 0

running 1 test
test tests::stores_artifact_payloads ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 1 test
test tests::stores_and_restores_exact_cache ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 6 tests
test tests::short_circuits ... ok
test tests::status_functions_use_context_state ... ok
test tests::evaluates_context_and_functions ... ok
test tests::evaluates_json_join_and_comparisons ... ok
test tests::non_empty_strings_are_truthy ... ok
test tests::string_equality_is_case_insensitive ... ok

test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.02s


running 22 tests
test eval::tests::expression_end_treats_backslash_as_literal_in_single_quotes ... ok
test eval::tests::expression_end_handles_doubled_quote_escape ... ok
test eval::tests::expression_end_ignores_braces_inside_string_literals ... ok
test eval::tests::resolve_literal_string ... ok
test eval::tests::unclosed_expression_returns_error ... ok
test eval::tests::resolve_env_value ... ok
test eval::tests::resolve_single_expression ... ok
test eval::tests::resolve_matrix_value ... ok
test eval::tests::resolve_map_expressions ... ok
test eval::tests::resolve_mixed_literal_and_expression ... ok
test tests::glob_match_handles_multiple_wildcards ... ok
test job_builder::tests::build_message_from_simple_workflow ... ok
test job_builder::tests::secrets_become_variables_and_mask_hints ... ok
test job_builder::tests::string_with_inputs_are_not_json_quoted ... ok
test tests::parses_local_action_metadata ... ok
test tests::schedule_trigger_matches_event_name ... ok
test tests::expands_local_reusable_workflow_call_jobs ... ok
test job_builder::tests::workflow_dispatch_inputs_are_in_event_context ... ok
test job_builder::tests::build_message_with_matrix ... ok
test tests::trigger_context_matches_activity_types ... ok
test tests::reusable_workflow_secrets_inherit_flag ... ok
test tests::parses_and_expands_matrix ... ok

test result: ok. 22 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 18 tests
test azdo::tests::pipeline_context_data_variants ... ok
test azdo::tests::task_agent_message_no_iv ... ok
test azdo::tests::issue_roundtrip ... ok
test azdo::tests::task_agent_message_roundtrip ... ok
test azdo::tests::timeline_record_state_serialization ... ok
test azdo::tests::task_result_serialization ... ok
test azdo::tests::variable_value_secret_roundtrip ... ok
test azdo::tests::pipeline_context_data_uses_runner_wire_shape_for_collections ... ok
test azdo::tests::task_step_serializes_as_runner_action_step ... ok
test crypto::tests::aes_invalid_iv_rejected ... ok
test crypto::tests::aes_different_iv_produces_different_ciphertext ... ok
test crypto::tests::aes_encrypt_decrypt_roundtrip ... ok
test tests::secret_debug_display_and_json_are_redacted ... ok
test crypto::tests::parses_jwk_public_key_for_wrapping ... ok
test crypto::tests::rsa_keypair_generation ... ok
test crypto::tests::parses_xml_public_key_for_wrapping ... ok
test crypto::tests::rsa_wrap_unwrap_roundtrip ... ok
test crypto::tests::full_session_roundtrip ... ok

test result: ok. 18 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 4.94s


running 1 test
test ndjson_event_shape_is_stable ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 24 tests
test tests::matrix_max_parallel_and_fail_fast_are_enforced ... ok
test tests::full_runner_lifecycle_register_session_poll_complete ... ok
test tests::artifact_endpoint_stores_and_downloads_payload ... ok
test tests::needs_context_includes_completed_job_outputs ... ok
test tests::cancel_run_delivers_cancellation_message ... ok
test tests::finish_job_resolves_plan_timeline_and_agent_job_ids ... ok
test tests::log_append_persists_payload_bytes ... ok
test tests::agent_request_get_reports_completion_result ... ok
test tests::log_append_masks_submitted_secrets ... ok
test tests::runner_server_v1_sensitive_routes_require_bearer ... ok
test tests::finish_job_falls_back_to_the_single_active_request_when_unresolved ... ok
test tests::matrix_fail_fast_cancels_in_progress_siblings_via_message ... ok
test tests::agent_request_patch_targets_only_the_request_id ... ok
test tests::submit_run_uses_branch_and_path_filters ... ok
test tests::message_poll_waits_until_work_is_enqueued ... ok
test tests::protected_apis_require_bearer_token ... ok
test tests::session_message_flow_encrypts_decryptable_job_body ... ok
test tests::messages_redeliver_until_delete_ack ... ok
test tests::same_session_waits_for_active_request_before_next_job ... ok
test tests::oidc_endpoint_mints_jwt_with_requested_audience ... ok
test tests::registration_persists_runner_public_key_material ... ok
test tests::cache_protocol_reserves_uploads_commits_and_restores ... ok
test tests::timeline_patch_projects_annotations_to_run_events ... ok
test tests::session_key_uses_registered_runner_public_key ... ok

test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 9.94s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 4 tests
test tests::atom_tag_extraction_finds_release_link ... ok
test tests::path_normalization_strips_org_prefix ... ok
test tests::field_extraction_detects_added_property ... ok
test tests::deterministic_specs_cover_fidelity_gap_items ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s


```

Spec:
```toml
change_id = "dap-debugger-endpoint"
upstream_version = "v2.335.1"
category = "feature"
tags = ["debugger", "websocket"]
ai_status = "deterministic-known-fidelity-gap"

[description]
what = '''
Runner can expose a DAP debugger integration.
'''
why = '''
v2.335.0 added debugger hooks around worker step execution.
'''
runner_behavior = '''
Debugger-enabled runs use websocket/control endpoints for DAP traffic.
'''
failure_mode = '''
Non-blocking unless debugging is requested.
'''

[feature_flag]
name = ""
where = ""

[wire]
request = '''
WebSocket debugger endpoint when debug feature is active
'''
expected_response = "DAP frames proxied/stubbed according to runner expectation"

[aksh_targets]
files = [
  { crate = "aksh-runner-server", path = "crates/aksh-runner-server/src/lib.rs", area = "broker/admin flow" },
]

[implementation]
approach = '''
Add explicit unsupported/stub behavior first; implement full proxy when debug scenarios are captured.
'''
test = "Debugger route returns expected upgrade/error semantics."

[[source_entries]]
file = "src/Runner.Common/Constants.cs"
change_type = "env_var_added"
fields = ["ReturnVersionDeprecatedExitCode", "ACTIONS_RUNNER_RETURN_VERSION_DEPRECATED_EXIT_CODE"]
snippet = '''
                public static readonly string AllowUnsupportedCommands = "ACTIONS_ALLOW_UNSECURE_COMMANDS";
                public static readonly string AllowUnsupportedStopCommandTokens = "ACTIONS_ALLOW_UNSECURE_STOPCOMMAND_TOKENS";
                public static readonly string RequireJobContainer = "ACTIONS_RUNNER_REQUIRE_JOB_CONTAINER";
                public static readonly string ReturnVersionDeprecatedExitCode = "ACTIONS_RUNNER_RETURN_VERSION_DEPRECATED_EXIT_CODE";
                public static readonly string RunnerDebug = "ACTIONS_RUNNER_DEBUG";
                public static readonly string StepDebug = "ACTIONS_STEP_DEBUG";
            }
'''

[[source_entries]]
file = "src/Runner.Common/Constants.cs"
change_type = "protocol_keyword_added"
fields = ["BatchActionResolution", "actions_batch_action_resolution"]
snippet = '''
                public static readonly string SetOrchestrationIdEnvForActions = "actions_set_orchestration_id_env_for_actions";
                public static readonly string SendJobLevelAnnotations = "actions_send_job_level_annotations";
                public static readonly string EmitCompositeMarkers = "actions_runner_emit_composite_markers";
                public static readonly string BatchActionResolution = "actions_batch_action_resolution";
                public static readonly string UseBearerTokenForCodeload = "actions_use_bearer_token_for_codeload";
                public static readonly string OverrideDebuggerWelcomeMessage = "actions_runner_override_debugger_welcome_message";
            }
'''

[[source_entries]]
file = "src/Runner.Common/Constants.cs"
change_type = "protocol_keyword_added"
fields = ["OverrideDebuggerWelcomeMessage", "actions_runner_override_debugger_welcome_message"]
snippet = '''
                public static readonly string EmitCompositeMarkers = "actions_runner_emit_composite_markers";
                public static readonly string BatchActionResolution = "actions_batch_action_resolution";
                public static readonly string UseBearerTokenForCodeload = "actions_use_bearer_token_for_codeload";
                public static readonly string OverrideDebuggerWelcomeMessage = "actions_runner_override_debugger_welcome_message";
            }

            // Node version migration related constants
'''

[[source_entries]]
file = "src/Runner.Common/Constants.cs"
change_type = "protocol_keyword_added"
fields = ["UseBearerTokenForCodeload", "actions_use_bearer_token_for_codeload"]
snippet = '''
                public static readonly string SendJobLevelAnnotations = "actions_send_job_level_annotations";
                public static readonly string EmitCompositeMarkers = "actions_runner_emit_composite_markers";
                public static readonly string BatchActionResolution = "actions_batch_action_resolution";
                public static readonly string UseBearerTokenForCodeload = "actions_use_bearer_token_for_codeload";
                public static readonly string OverrideDebuggerWelcomeMessage = "actions_runner_override_debugger_welcome_message";
            }

'''

[[source_entries]]
file = "src/Runner.Common/HostContext.cs"
change_type = "env_var_added"
fields = ["IsNullOrEmpty", "Environment", "GetEnvironmentVariable", "_GITHUB_ACTION_AUTH_MIGRATION_REFRESH_INTERVAL"]
snippet = '''
                    var refreshIntervalInMS = 60 * 1000;
#if DEBUG
                    // For L0, we will refresh faster
                    if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("_GITHUB_ACTION_AUTH_MIGRATION_REFRESH_INTERVAL")))
                    {
                        refreshIntervalInMS = int.Parse(Environment.GetEnvironmentVariable("_GITHUB_ACTION_AUTH_MIGRATION_REFRESH_INTERVAL"));
                    }
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapDebugger.cs"
change_type = "env_var_added"
fields = ["Environment", "GetEnvironmentVariable", "_tunnelConnectTimeoutSeconds"]
snippet = '''

        internal int ResolveTunnelConnectTimeout()
        {
            var raw = Environment.GetEnvironmentVariable(_tunnelConnectTimeoutSeconds);
            if (!string.IsNullOrEmpty(raw) && int.TryParse(raw, out var customTimeout) && customTimeout > 0)
            {
                Trace.Info($"Using custom tunnel connect timeout {customTimeout}s from {_tunnelConnectTimeoutSeconds}");
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapDebugger.cs"
change_type = "env_var_added"
fields = ["_timeoutEnvironmentVariable", "ACTIONS_RUNNER_DAP_CONNECTION_TIMEOUT"]
snippet = '''
    public sealed class DapDebugger : RunnerService, IDapDebugger
    {
        private const int _defaultTimeoutMinutes = 15;
        private const string _timeoutEnvironmentVariable = "ACTIONS_RUNNER_DAP_CONNECTION_TIMEOUT";
        private const int _defaultTunnelConnectTimeoutSeconds = 30;
        private const string _tunnelConnectTimeoutSeconds = "ACTIONS_RUNNER_DAP_TUNNEL_CONNECT_TIMEOUT_SECONDS";
        private const string _contentLengthHeader = "Content-Length: ";
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapDebugger.cs"
change_type = "env_var_added"
fields = ["_tunnelConnectTimeoutSeconds", "ACTIONS_RUNNER_DAP_TUNNEL_CONNECT_TIMEOUT_SECONDS"]
snippet = '''
        private const int _defaultTimeoutMinutes = 15;
        private const string _timeoutEnvironmentVariable = "ACTIONS_RUNNER_DAP_CONNECTION_TIMEOUT";
        private const int _defaultTunnelConnectTimeoutSeconds = 30;
        private const string _tunnelConnectTimeoutSeconds = "ACTIONS_RUNNER_DAP_TUNNEL_CONNECT_TIMEOUT_SECONDS";
        private const string _contentLengthHeader = "Content-Length: ";
        private const int _maxMessageSize = 10 * 1024 * 1024; // 10 MB
        private const int _maxHeaderLineLength = 8192; // 8 KB
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapDebugger.cs"
change_type = "env_var_added"
fields = ["timeoutEnv", "Environment", "GetEnvironmentVariable", "_timeoutEnvironmentVariable"]
snippet = '''

        internal int ResolveTimeout()
        {
            var timeoutEnv = Environment.GetEnvironmentVariable(_timeoutEnvironmentVariable);
            if (!string.IsNullOrEmpty(timeoutEnv) && int.TryParse(timeoutEnv, out var customTimeout) && customTimeout > 0)
            {
                Trace.Info($"Using custom DAP timeout {customTimeout} minutes from {_timeoutEnvironmentVariable}");
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapDebugger.cs"
change_type = "file_added"
fields = []
snippet = '''
﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GitHub.DistributedTask.WebApi;
using GitHub.Runner.Common;
using GitHub.Runner.Sdk;
using Microsoft.DevTunnels.Connections;
using Microsoft.DevTunnels.Contracts;
using Microsoft.DevTunnels.Management;
using Newtonsoft.Json;
using Pipelines = GitHub.DistributedTask.Pipelines;

namespace GitHub.Runner.Worker.Dap
{
    /// <summary>
    /// Stores information about a completed step for stack trace display.
    /// </summary>
    internal sealed class CompletedStepInfo
    {
        public string DisplayName { get; set; }
        public TaskResult? Result { get; set; }
        public int FrameId { get; set; }
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapMessages.cs"
change_type = "file_added"
fields = []
snippet = '''
﻿using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GitHub.Runner.Worker.Dap
{
    public enum DapCommand
    {
        Continue,
        Next,
        StepIn,
        StepOut,
        Disconnect
    }

    /// <summary>
    /// Base class of requests, responses, and events per DAP specification.
    /// </summary>
    public class ProtocolMessage
    {
        /// <summary>
        /// Sequence number of the message (also known as message ID).
        /// The seq for the first message sent by a client or debug adapter is 1,
        /// and for each subsequent message is 1 greater than the previous message.
        /// </summary>
        [JsonProperty("seq")]
        public int Seq { get; set; }

        /// <summary>
        /// Message type: 'request', 'response', 'event'
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapMessages.cs"
change_type = "message_type_added"
struct = "Message"
fields = ["Message"]
snippet = '''
    /// <summary>
    /// A structured error message.
    /// </summary>
    public class Message
    {
        /// <summary>
        /// Unique identifier for the message.
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapMessages.cs"
change_type = "message_type_added"
struct = "ProtocolMessage"
fields = ["ProtocolMessage"]
snippet = '''
    /// <summary>
    /// Base class of requests, responses, and events per DAP specification.
    /// </summary>
    public class ProtocolMessage
    {
        /// <summary>
        /// Sequence number of the message (also known as message ID).
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapReplExecutor.cs"
change_type = "env_var_added"
fields = ["Expose", "GITHUB_", "RUNNER_"]
snippet = '''
                }
            }

            // Expose runtime context variables to the environment (GITHUB_*, RUNNER_*, etc.)
            foreach (var ctxPair in context.ExpressionValues)
            {
                if (ctxPair.Value is IEnvironmentContextData runtimeContext && runtimeContext != null)
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapReplExecutor.cs"
change_type = "env_var_added"
fields = ["System", "Environment", "GetEnvironmentVariable", "Constants", "PathVariable", "Then"]
snippet = '''
                        environment.TryGetValue(Constants.PathVariable, out taskEnvPATH);
                        string originalPath = context.Global.Variables?.Get(Constants.PathVariable) ?? // Prefer a job variable.
                            taskEnvPATH ?? // Then a task-environment variable.
                            System.Environment.GetEnvironmentVariable(Constants.PathVariable) ?? // Then an environment variable.
                            string.Empty;
                        environment[Constants.PathVariable] = PathUtil.PrependPath(prependPath, originalPath);
                    }
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapReplExecutor.cs"
change_type = "file_added"
fields = []
snippet = '''
﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GitHub.DistributedTask.Pipelines.ContextData;
using GitHub.Runner.Common;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Sdk;
using GitHub.Runner.Worker.Container;
using GitHub.Runner.Worker.Handlers;

namespace GitHub.Runner.Worker.Dap
{
    /// <summary>
    /// Executes <see cref="RunCommand"/> objects in the job's runtime context.
    ///
    /// Mirrors the behavior of a normal workflow <c>run:</c> step as closely
    /// as possible by reusing the runner's existing shell-resolution logic,
    /// script fixup helpers, and process execution infrastructure.
    ///
    /// Output is streamed to the debugger via DAP <c>output</c> events with
    /// secrets masked before emission.
    /// </summary>
    internal sealed class DapReplExecutor
    {
        private readonly IHostContext _hostContext;
        private readonly Action<string, string> _sendOutput;
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapReplParser.cs"
change_type = "file_added"
fields = []
snippet = '''
using System;
using System.Collections.Generic;
using System.Text;

namespace GitHub.Runner.Worker.Dap
{
    /// <summary>
    /// Base type for all REPL DSL commands.
    /// </summary>
    internal abstract class DapReplCommand
    {
    }

    /// <summary>
    /// <c>help</c> or <c>help("run")</c>
    /// </summary>
    internal sealed class HelpCommand : DapReplCommand
    {
        public string Topic { get; set; }
    }

    /// <summary>
    /// <c>run("echo hello")</c> or
    /// <c>run("echo hello", shell: "bash", env: { FOO: "bar" }, working_directory: "/tmp")</c>
    /// </summary>
    internal sealed class RunCommand : DapReplCommand
    {
        public string Script { get; set; }
        public string Shell { get; set; }
        public Dictionary<string, string> Env { get; set; }
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DapVariableProvider.cs"
change_type = "file_added"
fields = []
snippet = '''
﻿using System;
using System.Collections.Generic;
using System.Globalization;
using GitHub.DistributedTask.Logging;
using GitHub.DistributedTask.ObjectTemplating.Tokens;
using GitHub.DistributedTask.Pipelines.ContextData;

namespace GitHub.Runner.Worker.Dap
{
    /// <summary>
    /// Maps runner execution context data to DAP scopes and variables.
    ///
    /// This is the single point where runner context values are materialized
    /// for the debugger. All values pass through the runner's existing
    /// <see cref="GitHub.DistributedTask.Logging.ISecretMasker"/> so the DAP
    /// surface never exposes anything beyond what a normal CI log would show.
    ///
    /// The secrets scope is intentionally opaque: keys are visible but every
    /// value is replaced with a constant redaction marker.
    ///
    /// Designed to be reusable by future DAP features (evaluate, hover, REPL)
    /// so that masking policy is never duplicated.
    /// </summary>
    internal sealed class DapVariableProvider
    {
        // Well-known scope names that map to top-level expression contexts.
        // Order matters: the index determines the stable variablesReference ID.
        private static readonly string[] _scopeNames =
        {
            "github", "env", "runner", "job", "steps",
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/DebuggerConfig.cs"
change_type = "file_added"
fields = []
snippet = '''
﻿using GitHub.DistributedTask.Pipelines;

namespace GitHub.Runner.Worker.Dap
{
    /// <summary>
    /// Consolidated runtime configuration for the job debugger.
    /// Populated once from the acquire response and owned by <see cref="GlobalContext"/>.
    /// </summary>
    public sealed class DebuggerConfig
    {
        public DebuggerConfig(bool enabled, DebuggerTunnelInfo tunnel, bool overrideWelcomeMessage = false, string welcomeMessage = null)
        {
            Enabled = enabled;
            Tunnel = tunnel;
            OverrideWelcomeMessage = overrideWelcomeMessage;
            WelcomeMessage = welcomeMessage;
        }

        /// <summary>Whether the debugger is enabled for this job.</summary>
        public bool Enabled { get; }

        /// <summary>
        /// Dev Tunnel details for remote debugging.
        /// Required when <see cref="Enabled"/> is true.
        /// </summary>
        public DebuggerTunnelInfo Tunnel { get; }

        /// <summary>
        /// When true, the runner overrides the default welcome message with
        /// <see cref="WelcomeMessage"/>. A null or empty <see cref="WelcomeMessage"/>
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/IDapDebugger.cs"
change_type = "file_added"
fields = []
snippet = '''
﻿using System.Collections.Generic;
using System.Threading.Tasks;
using GitHub.Runner.Common;

namespace GitHub.Runner.Worker.Dap
{
    public enum DapSessionState
    {
        NotStarted,
        WaitingForConnection,
        Initializing,
        Ready,
        Paused,
        Running,
        Terminated
    }

    [ServiceLocator(Default = typeof(DapDebugger))]
    public interface IDapDebugger : IRunnerService
    {
        Task StartAsync(IExecutionContext jobContext);
        Task WaitUntilReadyAsync();
        Task OnJobStepsInitializedAsync(IEnumerable<IStep> steps, IEnumerable<IStep> initialPostSteps);
        void OnPostStepRegistered(IStep step);
        Task OnStepStartingAsync(IStep step);
        void OnStepCompleted(IStep step);
        Task OnJobCompletedAsync();
        Task StopAsync();
    }
}
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/IWebSocketDapBridge.cs"
change_type = "file_added"
fields = []
snippet = '''
﻿using System.Threading.Tasks;
using GitHub.Runner.Common;

namespace GitHub.Runner.Worker.Dap
{
    [ServiceLocator(Default = typeof(WebSocketDapBridge))]
    public interface IWebSocketDapBridge : IRunnerService
    {
        void Start(int listenPort, int targetPort);
        Task ShutdownAsync();
    }
}
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/JobExecutionView.cs"
change_type = "file_added"
fields = []
snippet = '''
﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace GitHub.Runner.Worker.Dap
{
    internal sealed class JobExecutionView
    {
        private const string _sourceFileName = "execution.yml";

        private readonly object _lock = new object();
        private readonly List<SourceEntry> _preEntries = new List<SourceEntry>();
        private readonly List<SourceEntry> _mainEntries = new List<SourceEntry>();
        private readonly List<SourceEntry> _postEntries = new List<SourceEntry>();
        private readonly List<StepLine> _lineByStep = new List<StepLine>();
        private string _content;
        private int _completeJobLine;

        public JobExecutionView(
            string jobId,
            IEnumerable<IStep> steps,
            IEnumerable<IStep> initialPostSteps,
            IEnumerable<PredictedPostStep> predictedPostSteps = null)
        {
            JobId = string.IsNullOrWhiteSpace(jobId) ? "job" : jobId;

            _preEntries.Add(new SourceEntry("Set up job"));
            AddSteps(steps);
            AddPredictedPostSteps(predictedPostSteps);
'''

[[source_entries]]
file = "src/Runner.Worker/Dap/WebSocketDapBridge.cs"
change_type = "file_added"
fields = []
snippet = '''
﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GitHub.Runner.Common;

namespace GitHub.Runner.Worker.Dap
{
    internal sealed class WebSocketDapBridge : RunnerService, IWebSocketDapBridge
    {
        internal enum IncomingStreamPrefixKind
        {
            Unknown,
            HttpWebSocketUpgrade,
            PreUpgradedWebSocket,
            WebSocketReservedBits,
            Http2Preface,
            TlsClientHello,
        }

        private const int _bufferSize = 32 * 1024;
        private const int _maxHeaderLineLength = 8 * 1024;
        private const int _defaultMaxInboundMessageSize = 10 * 1024 * 1024; // 10 MB
'''

[[source_entries]]
file = "src/Runner.Worker/ExecutionContext.cs"
change_type = "feature_flag_added"
fields = ["overrideDebuggerWelcomeMessage", "Global", "Variables", "GetBoolean", "Constants", "Runner", "Features", "OverrideDebuggerWelcomeMessage"]
snippet = '''
            Global.WriteDebug = Global.Variables.Step_Debug ?? false;

            // Debugger enabled flag (from acquire response).
            var overrideDebuggerWelcomeMessage = Global.Variables.GetBoolean(Constants.Runner.Features.OverrideDebuggerWelcomeMessage) ?? false;
            Global.Debugger = new Dap.DebuggerConfig(message.EnableDebugger, message.DebuggerTunnel, overrideDebuggerWelcomeMessage, message.DebuggerWelcomeMessage);

            // Hook up JobServerQueueThrottling event, we will log warning on server tarpit.
'''

[[source_entries]]
file = "src/Runner.Worker/ExecutionContext.cs"
change_type = "protocol_keyword_added"
fields = ["Debugger"]
snippet = '''
            // Verbosity (from GitHub.Step_Debug).
            Global.WriteDebug = Global.Variables.Step_Debug ?? false;

            // Debugger enabled flag (from acquire response).
            var overrideDebuggerWelcomeMessage = Global.Variables.GetBoolean(Constants.Runner.Features.OverrideDebuggerWelcomeMessage) ?? false;
            Global.Debugger = new Dap.DebuggerConfig(message.EnableDebugger, message.DebuggerTunnel, overrideDebuggerWelcomeMessage, message.DebuggerWelcomeMessage);

'''

[[source_entries]]
file = "src/Runner.Worker/ExecutionContext.cs"
change_type = "protocol_keyword_added"
fields = ["Global", "Debugger", "Dap", "DebuggerConfig", "EnableDebugger", "DebuggerTunnel", "overrideDebuggerWelcomeMessage", "DebuggerWelcomeMessage"]
snippet = '''

            // Debugger enabled flag (from acquire response).
            var overrideDebuggerWelcomeMessage = Global.Variables.GetBoolean(Constants.Runner.Features.OverrideDebuggerWelcomeMessage) ?? false;
            Global.Debugger = new Dap.DebuggerConfig(message.EnableDebugger, message.DebuggerTunnel, overrideDebuggerWelcomeMessage, message.DebuggerWelcomeMessage);

            // Hook up JobServerQueueThrottling event, we will log warning on server tarpit.
            _jobServerQueue.JobServerQueueThrottling += JobServerQueueThrottling_EventReceived;
'''

[[source_entries]]
file = "src/Runner.Worker/ExecutionContext.cs"
change_type = "protocol_keyword_added"
fields = ["HostContext", "GetService", "Dap", "IDapDebugger", "OnPostStepRegistered"]
snippet = '''
            {
                try
                {
                    HostContext.GetService<Dap.IDapDebugger>().OnPostStepRegistered(step);
                }
                catch (Exception ex)
                {
'''

[[source_entries]]
file = "src/Runner.Worker/ExecutionContext.cs"
change_type = "protocol_keyword_added"
fields = ["Root", "Global", "Debugger", "Enabled"]
snippet = '''
            }
            Root.PostJobSteps.Push(step);

            if (Root.Global.Debugger?.Enabled == true)
            {
                try
                {
'''

[[source_entries]]
file = "src/Runner.Worker/ExecutionContext.cs"
change_type = "protocol_keyword_added"
fields = ["Trace", "Warning", "Failed", "DAP"]
snippet = '''
                }
                catch (Exception ex)
                {
                    Trace.Warning("Failed to notify DAP debugger about registered post job step.");
                    Trace.Error(ex);
                }
            }
'''

[[source_entries]]
file = "src/Runner.Worker/ExecutionContext.cs"
change_type = "protocol_keyword_added"
fields = ["overrideDebuggerWelcomeMessage", "Global", "Variables", "GetBoolean", "Constants", "Runner", "Features", "OverrideDebuggerWelcomeMessage"]
snippet = '''
            Global.WriteDebug = Global.Variables.Step_Debug ?? false;

            // Debugger enabled flag (from acquire response).
            var overrideDebuggerWelcomeMessage = Global.Variables.GetBoolean(Constants.Runner.Features.OverrideDebuggerWelcomeMessage) ?? false;
            Global.Debugger = new Dap.DebuggerConfig(message.EnableDebugger, message.DebuggerTunnel, overrideDebuggerWelcomeMessage, message.DebuggerWelcomeMessage);

            // Hook up JobServerQueueThrottling event, we will log warning on server tarpit.
'''

[[source_entries]]
file = "src/Runner.Worker/GlobalContext.cs"
change_type = "field_added"
struct = "GlobalContext"
fields = ["Debugger"]
snippet = '''
        public StepsContext StepsContext { get; set; }
        public Variables Variables { get; set; }
        public bool WriteDebug { get; set; }
        public DebuggerConfig Debugger { get; set; }
        public string InfrastructureFailureCategory { get; set; }
        public JObject ContainerHookState { get; set; }
        public bool HasTemplateEvaluatorMismatch { get; set; }
'''

[[source_entries]]
file = "src/Runner.Worker/GlobalContext.cs"
change_type = "field_added"
struct = "GlobalContext"
fields = ["HasTemplateEvaluatorMismatch"]
snippet = '''
        public DebuggerConfig Debugger { get; set; }
        public string InfrastructureFailureCategory { get; set; }
        public JObject ContainerHookState { get; set; }
        public bool HasTemplateEvaluatorMismatch { get; set; }
        public bool HasActionManifestMismatch { get; set; }
        public bool HasDeprecatedSetOutput { get; set; }
        public bool HasDeprecatedSaveState { get; set; }
'''

[[source_entries]]
file = "src/Runner.Worker/GlobalContext.cs"
change_type = "field_added"
struct = "GlobalContext"
fields = ["InfrastructureFailureCategory"]
snippet = '''
        public Variables Variables { get; set; }
        public bool WriteDebug { get; set; }
        public DebuggerConfig Debugger { get; set; }
        public string InfrastructureFailureCategory { get; set; }
        public JObject ContainerHookState { get; set; }
        public bool HasTemplateEvaluatorMismatch { get; set; }
        public bool HasActionManifestMismatch { get; set; }
'''

[[source_entries]]
file = "src/Runner.Worker/GlobalContext.cs"
change_type = "protocol_keyword_added"
fields = ["DebuggerConfig", "Debugger"]
snippet = '''
        public StepsContext StepsContext { get; set; }
        public Variables Variables { get; set; }
        public bool WriteDebug { get; set; }
        public DebuggerConfig Debugger { get; set; }
        public string InfrastructureFailureCategory { get; set; }
        public JObject ContainerHookState { get; set; }
        public bool HasTemplateEvaluatorMismatch { get; set; }
'''

[[source_entries]]
file = "src/Runner.Worker/GlobalContext.cs"
change_type = "protocol_keyword_added"
fields = ["GitHub", "Runner", "Worker", "Dap"]
snippet = '''
using GitHub.DistributedTask.WebApi;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Worker.Container;
using GitHub.Runner.Worker.Dap;
using Newtonsoft.Json.Linq;
using Sdk.RSWebApi.Contracts;

'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["AddDebuggerConnectionTelemetry", "IExecutionContext", "jobContext"]
snippet = '''
            }
        }

        private static void AddDebuggerConnectionTelemetry(IExecutionContext jobContext, string result)
        {
            jobContext.Global.JobTelemetry.Add(new JobTelemetry
            {
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["AddDebuggerConnectionTelemetry", "jobContext", "Canceled"]
snippet = '''
                        catch (OperationCanceledException) when (jobContext.CancellationToken.IsCancellationRequested)
                        {
                            Trace.Info("Job was cancelled before debugger client connected.");
                            AddDebuggerConnectionTelemetry(jobContext, "Canceled");
                            context.Error("Job was cancelled before debugger client connected.");
                            throw;
                        }
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["AddDebuggerConnectionTelemetry", "jobContext", "Connected"]
snippet = '''

                            await _dapDebugger.WaitUntilReadyAsync();
                            context.Output("Debugger connected.");
                            AddDebuggerConnectionTelemetry(jobContext, "Connected");
                        }
                        catch (OperationCanceledException) when (jobContext.CancellationToken.IsCancellationRequested)
                        {
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["AddDebuggerConnectionTelemetry", "jobContext", "Failed", "GetType", "Name"]
snippet = '''
                        catch (Exception ex)
                        {
                            Trace.Error($"DAP debugger failed: {ex.Message}");
                            AddDebuggerConnectionTelemetry(jobContext, $"Failed: {ex.GetType().Name}");
                            context.Error("The debugger failed to start or no debugger client connected in time.");
                            throw;
                        }
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Error", "Job"]
snippet = '''
                        {
                            Trace.Info("Job was cancelled before debugger client connected.");
                            AddDebuggerConnectionTelemetry(jobContext, "Canceled");
                            context.Error("Job was cancelled before debugger client connected.");
                            throw;
                        }
                        catch (Exception ex)
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Error", "The"]
snippet = '''
                        {
                            Trace.Error($"DAP debugger failed: {ex.Message}");
                            AddDebuggerConnectionTelemetry(jobContext, $"Failed: {ex.GetType().Name}");
                            context.Error("The debugger failed to start or no debugger client connected in time.");
                            throw;
                        }
                    }
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["GitHub", "Runner", "Worker", "Dap"]
snippet = '''
using GitHub.Runner.Common;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Sdk;
using GitHub.Runner.Worker.Dap;
using GitHub.Services.Common;
using Newtonsoft.Json;
using Pipelines = GitHub.DistributedTask.Pipelines;
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["IDapDebugger", "_dapDebugger"]
snippet = '''
        private Task _diskSpaceCheckTask = null;
        private CancellationTokenSource _serviceConnectivityCheckToken = new();
        private Task _serviceConnectivityCheckTask = null;
        private IDapDebugger _dapDebugger;

        // Download all required actions.
        // Make sure all condition inputs are valid.
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["InitializeJob"]
snippet = '''
                }
                finally
                {
                    // If InitializeJob failed after the debugger was started,
                    // tear down the transport here since FinalizeJob won't run.
                    if (!initSucceeded && _dapDebugger != null)
                    {
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Message", "DebuggerConnectionResult"]
snippet = '''
            jobContext.Global.JobTelemetry.Add(new JobTelemetry
            {
                Type = JobTelemetryType.General,
                Message = $"DebuggerConnectionResult: {result}"
            });
        }

'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Output", "Debugger"]
snippet = '''
                            context.Output("Waiting for debugger client to connect…");

                            await _dapDebugger.WaitUntilReadyAsync();
                            context.Output("Debugger connected.");
                            AddDebuggerConnectionTelemetry(jobContext, "Connected");
                        }
                        catch (OperationCanceledException) when (jobContext.CancellationToken.IsCancellationRequested)
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Output", "Job", "Press"]
snippet = '''
                    // events and stops the transport.
                    if (_dapDebugger != null)
                    {
                        context.Output("Job completed — pausing for debugger inspection. Press continue to finish.");
                        try
                        {
                            await _dapDebugger.OnJobCompletedAsync();
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Output", "Starting"]
snippet = '''
                    if (jobContext.Global.Debugger?.Enabled == true)
                    {
                        Trace.Info("Debugger enabled — starting inside Set up job");
                        context.Output("Starting debugger…");

                        try
                        {
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Output", "Waiting"]
snippet = '''
                            _dapDebugger = HostContext.GetService<IDapDebugger>();
                            await _dapDebugger.StartAsync(jobContext);

                            context.Output("Waiting for debugger client to connect…");

                            await _dapDebugger.WaitUntilReadyAsync();
                            context.Output("Debugger connected.");
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Pause", "DAP"]
snippet = '''
                }
                finally
                {
                    // Pause for debugger inspection, then tear down the DAP session.
                    // OnJobCompletedAsync pauses first, then sends terminated/exited
                    // events and stops the transport.
                    if (_dapDebugger != null)
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Start", "DAP"]
snippet = '''
                    Trace.Info($"Start checking service connectivity in background.");
                    _serviceConnectivityCheckTask = CheckServiceConnectivityAsync(context, _serviceConnectivityCheckToken.Token);

                    // Start the DAP debugger and wait for a client connection inside
                    // "Set up job" so the step stays in-progress while we wait.
                    if (jobContext.Global.Debugger?.Enabled == true)
                    {
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Trace", "Error", "DAP", "Message"]
snippet = '''
                        }
                        catch (Exception ex)
                        {
                            Trace.Error($"DAP debugger failed: {ex.Message}");
                            AddDebuggerConnectionTelemetry(jobContext, $"Failed: {ex.GetType().Name}");
                            context.Error("The debugger failed to start or no debugger client connected in time.");
                            throw;
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Trace", "Info", "Debugger", "Set"]
snippet = '''
                    // "Set up job" so the step stays in-progress while we wait.
                    if (jobContext.Global.Debugger?.Enabled == true)
                    {
                        Trace.Info("Debugger enabled — starting inside Set up job");
                        context.Output("Starting debugger…");

                        try
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Trace", "Info", "Job"]
snippet = '''
                        }
                        catch (OperationCanceledException) when (jobContext.CancellationToken.IsCancellationRequested)
                        {
                            Trace.Info("Job was cancelled before debugger client connected.");
                            AddDebuggerConnectionTelemetry(jobContext, "Canceled");
                            context.Error("Job was cancelled before debugger client connected.");
                            throw;
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Trace", "Warning", "DAP", "Message"]
snippet = '''
                        }
                        catch (Exception ex)
                        {
                            Trace.Warning($"DAP debugger cleanup during failed init: {ex.Message}");
                        }
                        _dapDebugger = null;
                    }
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Trace", "Warning", "DAP", "Message"]
snippet = '''
                        }
                        catch (Exception ex)
                        {
                            Trace.Warning($"DAP debugger completion error: {ex.Message}");
                        }
                        finally
                        {
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["Trace", "Warning", "DAP", "Message"]
snippet = '''
                            }
                            catch (Exception ex)
                            {
                                Trace.Warning($"DAP debugger stop error: {ex.Message}");
                            }
                        }
                        _dapDebugger = null;
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["_dapDebugger"]
snippet = '''
                        {
                            Trace.Warning($"DAP debugger cleanup during failed init: {ex.Message}");
                        }
                        _dapDebugger = null;
                    }

                    context.Debug("Finishing: Set up job");
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["_dapDebugger"]
snippet = '''
                    // Pause for debugger inspection, then tear down the DAP session.
                    // OnJobCompletedAsync pauses first, then sends terminated/exited
                    // events and stops the transport.
                    if (_dapDebugger != null)
                    {
                        context.Output("Job completed — pausing for debugger inspection. Press continue to finish.");
                        try
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["_dapDebugger"]
snippet = '''
                                Trace.Warning($"DAP debugger stop error: {ex.Message}");
                            }
                        }
                        _dapDebugger = null;
                    }

                    context.Debug("Finishing: Complete job");
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["_dapDebugger", "HostContext", "GetService", "IDapDebugger"]
snippet = '''

                        try
                        {
                            _dapDebugger = HostContext.GetService<IDapDebugger>();
                            await _dapDebugger.StartAsync(jobContext);

                            context.Output("Waiting for debugger client to connect…");
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["_dapDebugger", "OnJobCompletedAsync"]
snippet = '''
                        context.Output("Job completed — pausing for debugger inspection. Press continue to finish.");
                        try
                        {
                            await _dapDebugger.OnJobCompletedAsync();
                        }
                        catch (Exception ex)
                        {
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["_dapDebugger", "StartAsync", "jobContext"]
snippet = '''
                        try
                        {
                            _dapDebugger = HostContext.GetService<IDapDebugger>();
                            await _dapDebugger.StartAsync(jobContext);

                            context.Output("Waiting for debugger client to connect…");

'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["_dapDebugger", "StopAsync"]
snippet = '''
                    {
                        try
                        {
                            await _dapDebugger.StopAsync();
                        }
                        catch (Exception ex)
                        {
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["_dapDebugger", "StopAsync"]
snippet = '''
                        {
                            try
                            {
                                await _dapDebugger.StopAsync();
                            }
                            catch (Exception ex)
                            {
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["_dapDebugger", "WaitUntilReadyAsync"]
snippet = '''

                            context.Output("Waiting for debugger client to connect…");

                            await _dapDebugger.WaitUntilReadyAsync();
                            context.Output("Debugger connected.");
                            AddDebuggerConnectionTelemetry(jobContext, "Connected");
                        }
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["initSucceeded", "_dapDebugger"]
snippet = '''
                {
                    // If InitializeJob failed after the debugger was started,
                    // tear down the transport here since FinalizeJob won't run.
                    if (!initSucceeded && _dapDebugger != null)
                    {
                        try
                        {
'''

[[source_entries]]
file = "src/Runner.Worker/JobExtension.cs"
change_type = "protocol_keyword_added"
fields = ["jobContext", "Global", "Debugger", "Enabled"]
snippet = '''

                    // Start the DAP debugger and wait for a client connection inside
                    // "Set up job" so the step stays in-progress while we wait.
                    if (jobContext.Global.Debugger?.Enabled == true)
                    {
                        Trace.Info("Debugger enabled — starting inside Set up job");
                        context.Output("Starting debugger…");
'''

[[source_entries]]
file = "src/Runner.Worker/JobRunner.cs"
change_type = "protocol_keyword_added"
fields = ["GitHub", "Runner", "Worker", "Dap"]
snippet = '''
using GitHub.Runner.Common;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Sdk;
using GitHub.Runner.Worker.Dap;
using GitHub.Services.Common;
using GitHub.Services.WebApi;
using Sdk.RSWebApi.Contracts;
'''

[[source_entries]]
file = "src/Runner.Worker/JobRunner.cs"
change_type = "protocol_keyword_added"
fields = ["dapDebugger", "HostContext", "GetService", "IDapDebugger"]
snippet = '''

                    if (jobContext.Global.Debugger?.Enabled == true)
                    {
                        var dapDebugger = HostContext.GetService<IDapDebugger>();
                        await dapDebugger.OnJobStepsInitializedAsync(jobContext.JobSteps, jobContext.PostJobSteps);
                    }

'''

[[source_entries]]
file = "src/Runner.Worker/JobRunner.cs"
change_type = "protocol_keyword_added"
fields = ["dapDebugger", "OnJobStepsInitializedAsync", "jobContext", "JobSteps", "jobContext", "PostJobSteps"]
snippet = '''
                    if (jobContext.Global.Debugger?.Enabled == true)
                    {
                        var dapDebugger = HostContext.GetService<IDapDebugger>();
                        await dapDebugger.OnJobStepsInitializedAsync(jobContext.JobSteps, jobContext.PostJobSteps);
                    }

                    await stepsRunner.RunAsync(jobContext);
'''

[[source_entries]]
file = "src/Runner.Worker/JobRunner.cs"
change_type = "protocol_keyword_added"
fields = ["jobContext", "Global", "Debugger", "Enabled"]
snippet = '''
                        jobContext.JobSteps.Enqueue(step);
                    }

                    if (jobContext.Global.Debugger?.Enabled == true)
                    {
                        var dapDebugger = HostContext.GetService<IDapDebugger>();
                        await dapDebugger.OnJobStepsInitializedAsync(jobContext.JobSteps, jobContext.PostJobSteps);
'''

[[source_entries]]
file = "src/Runner.Worker/SnapshotOperationProvider.cs"
change_type = "env_var_added"
fields = ["Debug", "Snapshot", "GITHUB_ACTIONS_IMAGE_GEN_ENABLED", "imageGenEnabled"]
snippet = '''
            }
        }
        var imageGenEnabled = StringUtil.ConvertToBoolean(Environment.GetEnvironmentVariable("GITHUB_ACTIONS_IMAGE_GEN_ENABLED"));
        context.Debug($"Snapshot: GITHUB_ACTIONS_IMAGE_GEN_ENABLED={imageGenEnabled}");
        var shouldCheckImageGenPool = context.Global.Variables.GetBoolean(Constants.Runner.Features.SnapshotPreflightImageGenPoolCheck) ?? false;
        if (shouldCheckImageGenPool && !imageGenEnabled)
        {
'''

[[source_entries]]
file = "src/Runner.Worker/SnapshotOperationProvider.cs"
change_type = "env_var_added"
fields = ["Debug", "Snapshot", "RUNNER_ENVIRONMENT", "runnerEnvironment"]
snippet = '''
             context.Global.Variables.TryGetValue(WellKnownDistributedTaskVariables.RunnerEnvironment, out var runnerEnvironment) &&
             !string.IsNullOrEmpty(runnerEnvironment))
        {
            context.Debug($"Snapshot: RUNNER_ENVIRONMENT={runnerEnvironment}");
            if (!string.Equals(runnerEnvironment, "github-hosted", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("Snapshot workflows must be run on a GitHub Hosted Runner");
'''

[[source_entries]]
file = "src/Runner.Worker/SnapshotOperationProvider.cs"
change_type = "env_var_added"
fields = ["imageGenEnabled", "StringUtil", "ConvertToBoolean", "Environment", "GetEnvironmentVariable", "GITHUB_ACTIONS_IMAGE_GEN_ENABLED"]
snippet = '''
                throw new ArgumentException("Snapshot workflows must be run on a GitHub Hosted Runner");
            }
        }
        var imageGenEnabled = StringUtil.ConvertToBoolean(Environment.GetEnvironmentVariable("GITHUB_ACTIONS_IMAGE_GEN_ENABLED"));
        context.Debug($"Snapshot: GITHUB_ACTIONS_IMAGE_GEN_ENABLED={imageGenEnabled}");
        var shouldCheckImageGenPool = context.Global.Variables.GetBoolean(Constants.Runner.Features.SnapshotPreflightImageGenPoolCheck) ?? false;
        if (shouldCheckImageGenPool && !imageGenEnabled)
'''

[[source_entries]]
file = "src/Runner.Worker/SnapshotOperationProvider.cs"
change_type = "feature_flag_added"
fields = ["shouldCheckImageGenPool", "Global", "Variables", "GetBoolean", "Constants", "Runner", "Features", "SnapshotPreflightImageGenPoolCheck"]
snippet = '''
        }
        var imageGenEnabled = StringUtil.ConvertToBoolean(Environment.GetEnvironmentVariable("GITHUB_ACTIONS_IMAGE_GEN_ENABLED"));
        context.Debug($"Snapshot: GITHUB_ACTIONS_IMAGE_GEN_ENABLED={imageGenEnabled}");
        var shouldCheckImageGenPool = context.Global.Variables.GetBoolean(Constants.Runner.Features.SnapshotPreflightImageGenPoolCheck) ?? false;
        if (shouldCheckImageGenPool && !imageGenEnabled)
        {
            throw new ArgumentException("Snapshot workflows must be run a hosted runner with Image Generation enabled");
'''

[[source_entries]]
file = "src/Runner.Worker/StepsRunner.cs"
change_type = "protocol_keyword_added"
fields = ["GitHub", "Runner", "Worker", "Dap"]
snippet = '''
using GitHub.Runner.Common;
using GitHub.Runner.Common.Util;
using GitHub.Runner.Sdk;
using GitHub.Runner.Worker.Dap;
using GitHub.Runner.Worker.Expressions;

namespace GitHub.Runner.Worker
'''

[[source_entries]]
file = "src/Runner.Worker/StepsRunner.cs"
change_type = "protocol_keyword_added"
fields = ["Pause", "DAP"]
snippet = '''
                            }
                            else
                            {
                                // Pause for DAP debugger before step execution
                                await dapDebugger?.OnStepStartingAsync(step);

                                // Run the step synchronously (normal behavior)
'''

[[source_entries]]
file = "src/Runner.Worker/StepsRunner.cs"
change_type = "protocol_keyword_added"
fields = ["dapDebugger", "HostContext", "GetService", "IDapDebugger"]
snippet = '''
            jobContext.JobContext.Status = (jobContext.Result ?? TaskResult.Succeeded).ToActionResult();
            var scopeInputs = new Dictionary<string, PipelineContextData>(StringComparer.OrdinalIgnoreCase);
            bool checkPostJobActions = false;
            var dapDebugger = HostContext.GetService<IDapDebugger>();
            while (jobContext.JobSteps.Count > 0 || !checkPostJobActions)
            {
                if (jobContext.JobSteps.Count == 0 && !checkPostJobActions)
'''

[[source_entries]]
file = "src/Runner.Worker/StepsRunner.cs"
change_type = "protocol_keyword_added"
fields = ["dapDebugger", "OnStepCompleted"]
snippet = '''
                                await RunStepAsync(step, jobContext.CancellationToken);
                                CompleteStep(step);

                                dapDebugger?.OnStepCompleted(step);
                            }
                        }
                    }
'''

[[source_entries]]
file = "src/Runner.Worker/StepsRunner.cs"
change_type = "protocol_keyword_added"
fields = ["dapDebugger", "OnStepStartingAsync"]
snippet = '''
                            else
                            {
                                // Pause for DAP debugger before step execution
                                await dapDebugger?.OnStepStartingAsync(step);

                                // Run the step synchronously (normal behavior)
                                await RunStepAsync(step, jobContext.CancellationToken);
'''

```

Diff:
```diff
diff --git a/Cargo.toml b/Cargo.toml
index 42f796a..4585489 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -8,6 +8,7 @@ members = [
     "crates/aksh-gha-protocol",
     "crates/aksh-runner-client",
     "crates/aksh-runner-server",
+    "crates/runner-watch",
 ]
 resolver = "2"
 
@@ -53,3 +54,4 @@ cbc = { version = "0.1", features = ["block-padding"] }
 cipher = { version = "0.4", features = ["block-padding", "alloc"] }
 rsa = "0.9"
 walkdir = "2.5"
+toml = "0.8"
diff --git a/docs/fidelity-gap.md b/docs/fidelity-gap.md
index 79015a6..9113b0f 100644
--- a/docs/fidelity-gap.md
+++ b/docs/fidelity-gap.md
@@ -965,4 +965,24 @@ Phases A–E are the critical path to "a real runner runs one job." F–H reach
 
 "a real runner runs *any* in-scope workflow exactly like GitHub." Provider integration
 
-(step 10) closes the loop for Preloop and every other host.
\ No newline at end of file
+(step 10) closes the loop for Preloop and every other host.
+
+<!-- runner-watch-sync -->
+## runner-watch generated scorecard for v2.335.1
+
+| Change | Category | Spec |
+|---|---|---|
+| background-step-timeline-fields | blocker | `.runner-watch/specs/v2.335.1/background-step-timeline-fields.toml` |
+| batch-action-resolution | feature | `.runner-watch/specs/v2.335.1/batch-action-resolution.toml` |
+| dap-debugger-endpoint | feature | `.runner-watch/specs/v2.335.1/dap-debugger-endpoint.toml` |
+| disable-stdout-multiline-log-prefixing | nit | `.runner-watch/specs/v2.335.1/disable-stdout-multiline-log-prefixing.toml` |
+| node20-deprecation-warning | nit | `.runner-watch/specs/v2.335.1/node20-deprecation-warning.toml` |
+| request-ack | concern | `.runner-watch/specs/v2.335.1/request-ack.toml` |
+| runner-version-deprecated | concern | `.runner-watch/specs/v2.335.1/runner-version-deprecated.toml` |
+| send-job-level-annotations | feature | `.runner-watch/specs/v2.335.1/send-job-level-annotations.toml` |
+| server-enforced-runner-settings | nit | `.runner-watch/specs/v2.335.1/server-enforced-runner-settings.toml` |
+| use-bearer-token-for-codeload | feature | `.runner-watch/specs/v2.335.1/use-bearer-token-for-codeload.toml` |
+| use-runner-admin-flow | concern | `.runner-watch/specs/v2.335.1/use-runner-admin-flow.toml` |
+| v2-admin-broker-connection | concern | `.runner-watch/specs/v2.335.1/v2-admin-broker-connection.toml` |
+
+Generated by `runner-watch pr`; review the TOML specs for source snippets and implementation guidance.
diff --git a/docs/runner-watch-plan.md b/docs/runner-watch-plan.md
index 34d6571..1d8b4db 100644
--- a/docs/runner-watch-plan.md
+++ b/docs/runner-watch-plan.md
@@ -8,7 +8,7 @@ aksh must stay compatible with the official `actions/runner` binary. Upstream re
 
 flags, new crypto. Today this is tracked manually via hand-written analysis in
 
-`fidelity-gap.md §1a`. That analysis took hours and is already stale (aksh targets v2.322.0;
+`fidelity-gap.md §1a`. That analysis took hours and is already stale (aksh targets v2.335.0;
 
 GitHub enforces v2.329.0+ since March 2026).
 
@@ -22,18 +22,18 @@ draft PRs — with no human intervention until the final PR review.
 
 1. **Deterministic where possible, AI where necessary.** Git diff, path filtering, struct
 
-   extraction, conformance replay, and PR creation are all mechanical. AI handles protocol
+  extraction, conformance replay, and PR creation are all mechanical. AI handles protocol
 
    semantics (what does this change mean?) and code generation (write the Rust).
 2. **Two-agent adversarial pattern.** Claude fills semantic specs and reviews code. Codex
 
-   implements Rust. Different training distributions catch different blind spots.
+  implements Rust. Different training distributions catch different blind spots.
 3. **Neither agent grades their own homework.** The conformance gate runs recorded mitm
 
-   replay bytes that neither agent can modify. The orchestrator owns the golden capture.
+  replay bytes that neither agent can modify. The orchestrator owns the golden capture.
 4. **Everything is inspectable.** Every phase produces an artifact (JSON, TOML, markdown)
 
-   that a human can read. No black boxes.
+  that a human can read. No black boxes.
 5. **Draft PRs, never auto-merge.** Even nits. C#-to-Rust translation is non-mechanical.
 
 ## Agent assignment
@@ -198,10 +198,10 @@ For each entry in `delta.json`:
 
 1. **Path filter:** skip entries in `.github/`, `Test/`, `Misc/`, `dev/`, dependency files,
 
-   CI config, README changes
+  CI config, README changes
 2. **Surface map:** map C# struct/file → aksh file via `aksh-surface.toml` (static mapping
 
-   table). If the entry touches a mapped aksh surface → keep. If purely runner-internal
+  table). If the entry touches a mapped aksh surface → keep. If purely runner-internal
 
    (Worker execution logic, CLI args, dotnet SDK bumps) → tag `skip` without AI.
 3. **Feature flag detection:** extract flag name from enum if present.
@@ -304,7 +304,7 @@ For each spec, in priority order (blocker → concern → feature → nit):
 
 1. Invoke Codex with the spec TOML, relevant aksh source files (from `aksh_targets`),
 
-   and existing patterns (serde attribute conventions, handler shape, test patterns).
+  and existing patterns (serde attribute conventions, handler shape, test patterns).
 2. Codex writes Rust code following existing patterns exactly.
 3. Codex runs `cargo check`. If errors, feeds them back and retries.
 4. Codex runs `cargo test --workspace`. If failures, feeds them back and retries.
@@ -353,13 +353,13 @@ Claude reads the spec (what was requested) and the diff (what Codex wrote). Chec
 
 1. **Spec conformance:** Does the code implement exactly what the spec describes?
 
-   Wire shapes, field names, endpoint paths.
+  Wire shapes, field names, endpoint paths.
 2. **Pattern compliance:** Does it follow existing aksh patterns? Serde attributes,
 
-   handler structure, error handling.
+  handler structure, error handling.
 3. **Edge cases:** Missing null checks, wrong defaults, missing `skip_serializing_if`,
 
-   incorrect `Option` vs non-optional.
+  incorrect `Option` vs non-optional.
 4. **Security:** Crypto changes reviewed carefully. Auth fields not leaked in logs.
 5. **Cargo test:** Claude runs `cargo test --workspace` independently to verify.
 
@@ -430,10 +430,10 @@ comparison script. The orchestrator owns both.
 2. Start aksh server on localhost
 3. `mitmdump --server-replay .runner-watch/golden/v{N}/flows.mitm` — replay recorded
 
-   official requests against aksh
+  official requests against aksh
 4. `_compare.py` — diff aksh responses against recorded official responses using the
 
-   existing normalizer (GUID replacement, path normalization, volatile field stripping)
+  existing normalizer (GUID replacement, path normalization, volatile field stripping)
 5. Report: which endpoints match, which diverge, with JSON diffs
 
 ### Gate
@@ -692,7 +692,7 @@ docs/
 
 1. **Live capture is a baseline refresh, not a per-release gate.** The runner contacts
 
-   `api.github.com` and `pipelinesghub…` even when pointed at another control plane
+  `api.github.com` and `pipelinesghub…` even when pointed at another control plane
 
    (discovered in mitm live capture report, finding #6). Registration tokens are
 
@@ -701,54 +701,56 @@ docs/
    bumping runner version.
 2. **Source diff discovers, wire diff validates.** Feature-flag-gated behavior is
 
-   invisible on the wire until the control plane advertises the capability. Source diff
+  invisible on the wire until the control plane advertises the capability. Source diff
 
    catches those (it found the §1a.4 table). Wire replay validates that implemented
 
    changes actually work. Different tools for different jobs.
 3. **Spec before code, not diff-to-code.** `aksh-gha-protocol` already has structural
 
-   divergences from C# (`EndpointAuthorization` is a direct field, `TaskResources.repositories`
+  divergences from C# (`EndpointAuthorization` is a direct field, `TaskResources.repositories`
 
    is `Vec` not `BTreeMap`). An AI translating C# diffs directly will fight existing
 
    conventions. The spec is the guard rail.
 4. **Conformance baseline drifts with runner version.** Bumping runner version invalidates
 
-   the golden capture. The tool must re-record the golden when it bumps, or the gate
+  the golden capture. The tool must re-record the golden when it bumps, or the gate
 
    tests against dead bytes.
 5. **Neither agent can modify the golden capture.** This is the single most important
 
-   constraint. The orchestrator owns the golden bytes and `_compare.py`. The implementing
+  constraint. The orchestrator owns the golden bytes and `_compare.py`. The implementing
 
    agent can iterate on `cargo test` freely, but the conformance replay is read-only
 
    with respect to the test data.
 6. **C#-to-Rust translation is non-mechanical.** Nullable reference types, `Task[[ORCA_RAW_HTML_INLINE:%3CT%3E]]`,
 
-   Newtonsoft attrs → `Option`, `async fn`, serde. Even "nit" renames can shift wire
+  Newtonsoft attrs → `Option`, `async fn`, serde. Even "nit" renames can shift wire
 
    field names. Nothing auto-merges.
 7. **Two upstreams, two roles.** `actions/runner` = the contract (what the runner sends
 
-   and requires). `ChristopherHX/runner.server` = the reference implementation (how
+  and requires). `ChristopherHX/runner.server` = the reference implementation (how
 
    someone else built the server). Watch `actions/runner` for obligations; use
 
    `runner.server` diffs only as implementation hints.
 
-
 ## Known upstream defects (actions/runner)
 
 These are bugs in the official `actions/runner` binary that we work around in aksh
+
 rather than fixing upstream (issue creation is restricted on the repo).
 
 ### Port stripped from HTTP URLs (ConfigurationManager.cs)
 
 **File:** `Runner.Listener/Configuration/ConfigurationManager.cs`  
 **Root cause:** Token-fetch URL constructions use `gitHubUrlBuilder.Host` which
+
 drops non-default ports. `UriBuilder.Host` returns only the hostname, discarding
+
 `:port` for any port that isn't the scheme default.
 
 ```
@@ -759,18 +761,24 @@ Line 768: $"...://api.{gitHubUrlBuilder.Host}/..."       ← port dropped
 ```
 
 Compare with `GetTenantCredential` (lines 835, 840) which correctly uses
+
 `gitHubUrlBuilder.ToString()` — preserving the port. The worker side
+
 (`JobExtension.cs:204-208`) also handles ports explicitly with `IsDefaultPort`.
 
 **Impact:** `--url http://example.com:9090` results in the runner dialing
+
 `example.com:80` for token-fetch endpoints. HTTPS paths preserve the port.
 
 **Fix (6 lines in C#):**
+
 ```csharp
 var port = gitHubUrlBuilder.Uri.IsDefaultPort ? "" : $":{gitHubUrlBuilder.Port}";
 githubApiUrl = $"{gitHubUrlBuilder.Scheme}://{gitHubUrlBuilder.Host}{port}/api/v3/...";
 ```
 
 **aksh workaround (see `scripts/e2e-setup.sh`):** macOS `pfctl` port 80→9090
+
 redirect, Linux `iptables` redirect, or `setcap cap_net_bind_service` on aksh.
-Alternate: use HTTPS (runner preserves the port for HTTPS URLs).
+
+Alternate: use HTTPS (runner preserves the port for HTTPS URLs).
\ No newline at end of file

```
