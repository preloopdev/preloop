You are implementing an aksh protocol-sync spec. Follow existing Rust patterns exactly. Run cargo check and relevant tests, but do not run formatters or project-wide lint.

Spec:
```toml
change_id = "batch-action-resolution"
upstream_version = "v2.335.1"
category = "feature"
tags = ["actions", "download"]
ai_status = "deterministic-known-fidelity-gap"

[description]
what = '''
Runner can resolve action downloads in batches and optionally use bearer tokens for codeload.
'''
why = '''
v2.328.0 optimized action download resolution and codeload authentication.
'''
runner_behavior = '''
Calls ActionDownloadInfo with batch requests and may attach bearer token semantics to tarball URLs.
'''
failure_mode = '''
Existing action download stubs work for simple cases but miss newer auth/download behavior.
'''

[feature_flag]
name = "BatchActionResolution"
where = "action download feature flags"
default = false

[wire]
request = '''
POST /_apis/v1/ActionDownloadInfo/{scope}/{hub}/{planId}
'''
expected_response = "JSON action download info"

[aksh_targets]
files = [
  { crate = "aksh-runner-server", path = "crates/aksh-runner-server/src/lib.rs", area = "action download handler" },
]

[implementation]
approach = '''
Extend action download handler to accept batch wire shape and token mode.
'''
test = "Batch ActionDownloadInfo request returns per-action entries."

[[source_entries]]
file = "src/Runner.Worker/ActionManager.cs"
change_type = "env_var_added"
fields = ["StringUtil", "ConvertToBoolean", "Environment", "GetEnvironmentVariable", "ACTIONS_BATCH_ACTION_RESOLUTION"]
snippet = '''
            };
            var containerSetupSteps = new List<JobExtensionRunner>();
            var batchActionResolution = (executionContext.Global.Variables.GetBoolean(Constants.Runner.Features.BatchActionResolution) ?? false)
                || StringUtil.ConvertToBoolean(Environment.GetEnvironmentVariable("ACTIONS_BATCH_ACTION_RESOLUTION"));
            // Stack-local cache: same action (owner/repo@ref) is resolved only once,
            // even if it appears at multiple depths in a composite tree.
            var resolvedDownloadInfos = batchActionResolution
'''

[[source_entries]]
file = "src/Runner.Worker/ActionManager.cs"
change_type = "feature_flag_added"
fields = ["Used", "BatchActionResolution"]
snippet = '''
        /// <summary>
        /// Legacy (non-batched) action resolution. Each composite resolves its
        /// sub-actions individually, with no cross-depth deduplication.
        /// Used when the BatchActionResolution feature flag is disabled.
        /// </summary>
        private async Task<PrepareActionsState> PrepareActionsRecursiveLegacyAsync(IExecutionContext executionContext, PrepareActionsState state, IEnumerable<Pipelines.ActionStep> actions, Int32 depth = 0, Guid parentStepId = default(Guid))
        {
'''

[[source_entries]]
file = "src/Runner.Worker/ActionManager.cs"
change_type = "feature_flag_added"
fields = ["batchActionResolution", "executionContext", "Global", "Variables", "GetBoolean", "Constants", "Runner", "Features"]
snippet = '''
                PreStepTracker = new Dictionary<Guid, IActionRunner>()
            };
            var containerSetupSteps = new List<JobExtensionRunner>();
            var batchActionResolution = (executionContext.Global.Variables.GetBoolean(Constants.Runner.Features.BatchActionResolution) ?? false)
                || StringUtil.ConvertToBoolean(Environment.GetEnvironmentVariable("ACTIONS_BATCH_ACTION_RESOLUTION"));
            // Stack-local cache: same action (owner/repo@ref) is resolved only once,
            // even if it appears at multiple depths in a composite tree.
'''

[[source_entries]]
file = "src/Runner.Worker/ActionManager.cs"
change_type = "protocol_keyword_added"
fields = ["Used", "BatchActionResolution"]
snippet = '''
        /// <summary>
        /// Legacy (non-batched) action resolution. Each composite resolves its
        /// sub-actions individually, with no cross-depth deduplication.
        /// Used when the BatchActionResolution feature flag is disabled.
        /// </summary>
        private async Task<PrepareActionsState> PrepareActionsRecursiveLegacyAsync(IExecutionContext executionContext, PrepareActionsState state, IEnumerable<Pipelines.ActionStep> actions, Int32 depth = 0, Guid parentStepId = default(Guid))
        {
'''

[[source_entries]]
file = "src/Runner.Worker/ActionManager.cs"
change_type = "protocol_keyword_added"
fields = ["batchActionResolution"]
snippet = '''
            PrepareActionsState result = new PrepareActionsState();
            try
            {
                result = batchActionResolution
                    ? await PrepareActionsRecursiveAsync(executionContext, state, actions, resolvedDownloadInfos, depth, rootStepId)
                    : await PrepareActionsRecursiveLegacyAsync(executionContext, state, actions, depth, rootStepId);
            }
'''

[[source_entries]]
file = "src/Runner.Worker/ActionManager.cs"
change_type = "protocol_keyword_added"
fields = ["batchActionResolution", "executionContext", "Global", "Variables", "GetBoolean", "Constants", "Runner", "Features"]
snippet = '''
                PreStepTracker = new Dictionary<Guid, IActionRunner>()
            };
            var containerSetupSteps = new List<JobExtensionRunner>();
            var batchActionResolution = (executionContext.Global.Variables.GetBoolean(Constants.Runner.Features.BatchActionResolution) ?? false)
                || StringUtil.ConvertToBoolean(Environment.GetEnvironmentVariable("ACTIONS_BATCH_ACTION_RESOLUTION"));
            // Stack-local cache: same action (owner/repo@ref) is resolved only once,
            // even if it appears at multiple depths in a composite tree.
'''

[[source_entries]]
file = "src/Runner.Worker/ActionManager.cs"
change_type = "protocol_keyword_added"
fields = ["resolvedDownloadInfos", "batchActionResolution"]
snippet = '''
                || StringUtil.ConvertToBoolean(Environment.GetEnvironmentVariable("ACTIONS_BATCH_ACTION_RESOLUTION"));
            // Stack-local cache: same action (owner/repo@ref) is resolved only once,
            // even if it appears at multiple depths in a composite tree.
            var resolvedDownloadInfos = batchActionResolution
                ? new Dictionary<string, WebApi.ActionDownloadInfo>(StringComparer.Ordinal)
                : null;
            var depth = 0;
'''

```
