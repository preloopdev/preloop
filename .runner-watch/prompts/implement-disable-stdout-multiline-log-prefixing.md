You are implementing an aksh protocol-sync spec. Follow existing Rust patterns exactly. Run cargo check and relevant tests, but do not run formatters or project-wide lint.

Spec:
```toml
change_id = "disable-stdout-multiline-log-prefixing"
upstream_version = "v2.335.1"
category = "nit"
tags = ["env", "logs"]
ai_status = "deterministic-known-fidelity-gap"

[description]
what = '''
Runner reads an env var controlling multiline stdout log prefixing.
'''
why = '''
v2.335.0 added a logging behavior switch.
'''
runner_behavior = '''
Worker reads DisableStdoutMultilineLogPrefixing from environment/configuration.
'''
failure_mode = '''
Runner-side cosmetic behavior; aksh control plane usually need not act.
'''

[feature_flag]
name = "DisableStdoutMultilineLogPrefixing"
where = "environment"
default = false

[wire]
request = '''
N/A
'''
expected_response = "N/A"

[aksh_targets]
files = [
]

[implementation]
approach = '''
No control-plane change unless aksh injects runner environment.
'''
test = "No server test required; document skip."

[[source_entries]]
file = "src/Runner.Common/Constants.cs"
change_type = "env_var_added"
fields = ["DisableStdoutMultilineLogPrefixing", "ACTIONS_RUNNER_DISABLE_STDOUT_MULTILINE_LOG_PREFIXING"]
snippet = '''
                public static readonly string ForcedInternalNodeVersion = "ACTIONS_RUNNER_FORCED_INTERNAL_NODE_VERSION";
                public static readonly string ForcedActionsNodeVersion = "ACTIONS_RUNNER_FORCE_ACTIONS_NODE_VERSION";
                public static readonly string PrintLogToStdout = "ACTIONS_RUNNER_PRINT_LOG_TO_STDOUT";
                public static readonly string DisableStdoutMultilineLogPrefixing = "ACTIONS_RUNNER_DISABLE_STDOUT_MULTILINE_LOG_PREFIXING";
                public static readonly string ActionArchiveCacheDirectory = "ACTIONS_RUNNER_ACTION_ARCHIVE_CACHE";
                public static readonly string SymlinkCachedActions = "ACTIONS_RUNNER_SYMLINK_CACHED_ACTIONS";
                public static readonly string EmitCompositeMarkers = "ACTIONS_RUNNER_EMIT_COMPOSITE_MARKERS";
'''

[[source_entries]]
file = "src/Runner.Common/Constants.cs"
change_type = "env_var_added"
fields = ["EmitCompositeMarkers", "ACTIONS_RUNNER_EMIT_COMPOSITE_MARKERS"]
snippet = '''
                public static readonly string DisableStdoutMultilineLogPrefixing = "ACTIONS_RUNNER_DISABLE_STDOUT_MULTILINE_LOG_PREFIXING";
                public static readonly string ActionArchiveCacheDirectory = "ACTIONS_RUNNER_ACTION_ARCHIVE_CACHE";
                public static readonly string SymlinkCachedActions = "ACTIONS_RUNNER_SYMLINK_CACHED_ACTIONS";
                public static readonly string EmitCompositeMarkers = "ACTIONS_RUNNER_EMIT_COMPOSITE_MARKERS";
            }

            public static class System
'''

[[source_entries]]
file = "src/Runner.Common/Constants.cs"
change_type = "env_var_added"
fields = ["SymlinkCachedActions", "ACTIONS_RUNNER_SYMLINK_CACHED_ACTIONS"]
snippet = '''
                public static readonly string PrintLogToStdout = "ACTIONS_RUNNER_PRINT_LOG_TO_STDOUT";
                public static readonly string DisableStdoutMultilineLogPrefixing = "ACTIONS_RUNNER_DISABLE_STDOUT_MULTILINE_LOG_PREFIXING";
                public static readonly string ActionArchiveCacheDirectory = "ACTIONS_RUNNER_ACTION_ARCHIVE_CACHE";
                public static readonly string SymlinkCachedActions = "ACTIONS_RUNNER_SYMLINK_CACHED_ACTIONS";
                public static readonly string EmitCompositeMarkers = "ACTIONS_RUNNER_EMIT_COMPOSITE_MARKERS";
            }

'''

[[source_entries]]
file = "src/Runner.Common/Constants.cs"
change_type = "protocol_keyword_added"
fields = ["DisableStdoutMultilineLogPrefixing", "ACTIONS_RUNNER_DISABLE_STDOUT_MULTILINE_LOG_PREFIXING"]
snippet = '''
                public static readonly string ForcedInternalNodeVersion = "ACTIONS_RUNNER_FORCED_INTERNAL_NODE_VERSION";
                public static readonly string ForcedActionsNodeVersion = "ACTIONS_RUNNER_FORCE_ACTIONS_NODE_VERSION";
                public static readonly string PrintLogToStdout = "ACTIONS_RUNNER_PRINT_LOG_TO_STDOUT";
                public static readonly string DisableStdoutMultilineLogPrefixing = "ACTIONS_RUNNER_DISABLE_STDOUT_MULTILINE_LOG_PREFIXING";
                public static readonly string ActionArchiveCacheDirectory = "ACTIONS_RUNNER_ACTION_ARCHIVE_CACHE";
                public static readonly string SymlinkCachedActions = "ACTIONS_RUNNER_SYMLINK_CACHED_ACTIONS";
                public static readonly string EmitCompositeMarkers = "ACTIONS_RUNNER_EMIT_COMPOSITE_MARKERS";
'''

[[source_entries]]
file = "src/Runner.Common/StdoutTraceListener.cs"
change_type = "env_var_added"
fields = ["_disablePrefixMultilineLogs", "StringUtil", "ConvertToBoolean", "Environment", "GetEnvironmentVariable", "Constants", "Variables", "Agent"]
snippet = '''
        public StdoutTraceListener(string hostType)
        {
            this._hostType = hostType;
            this._disablePrefixMultilineLogs = StringUtil.ConvertToBoolean(Environment.GetEnvironmentVariable(Constants.Variables.Agent.DisableStdoutMultilineLogPrefixing));
        }

        // Copied and modified slightly from .Net Core source code. Modification was required to make it compile.
'''

[[source_entries]]
file = "src/Runner.Common/StdoutTraceListener.cs"
change_type = "protocol_keyword_added"
fields = ["_disablePrefixMultilineLogs", "StringUtil", "ConvertToBoolean", "Environment", "GetEnvironmentVariable", "Constants", "Variables", "Agent"]
snippet = '''
        public StdoutTraceListener(string hostType)
        {
            this._hostType = hostType;
            this._disablePrefixMultilineLogs = StringUtil.ConvertToBoolean(Environment.GetEnvironmentVariable(Constants.Variables.Agent.DisableStdoutMultilineLogPrefixing));
        }

        // Copied and modified slightly from .Net Core source code. Modification was required to make it compile.
'''

```
