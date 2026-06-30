You are implementing an aksh protocol-sync spec. Follow existing Rust patterns exactly. Run cargo check and relevant tests, but do not run formatters or project-wide lint.

Spec:
```toml
change_id = "server-enforced-runner-settings"
upstream_version = "v2.335.1"
category = "nit"
tags = ["settings"]
ai_status = "deterministic-known-fidelity-gap"

[description]
what = '''
Server can enforce selected runner settings.
'''
why = '''
v2.323.0 added server-provided settings hooks.
'''
runner_behavior = '''
Runner reads settings from server responses and applies them locally.
'''
failure_mode = '''
Defaults continue to work for local control plane usage.
'''

[feature_flag]
name = ""
where = ""

[wire]
request = '''
GET settings/capability endpoint
'''
expected_response = "JSON settings"

[aksh_targets]
files = [
  { crate = "aksh-runner-server", path = "crates/aksh-runner-server/src/lib.rs", area = "connectionData payload" },
]

[implementation]
approach = '''
Return explicit defaults for any setting endpoint discovered in captures.
'''
test = "Settings response serializes default values."

[[source_entries]]
file = "src/Runner.Common/ConfigurationStore.cs"
change_type = "protocol_keyword_added"
fields = ["RunnerSettings", "GetMigratedSettings"]
snippet = '''
        CredentialData GetCredentials();
        CredentialData GetMigratedCredentials();
        RunnerSettings GetSettings();
        RunnerSettings GetMigratedSettings();
        void SaveCredential(CredentialData credential);
        void SaveMigratedCredential(CredentialData credential);
        void SaveSettings(RunnerSettings settings);
'''

[[source_entries]]
file = "src/Runner.Common/ConfigurationStore.cs"
change_type = "protocol_keyword_added"
fields = ["RunnerSettings", "GetMigratedSettings"]
snippet = '''
            return _settings;
        }

        public RunnerSettings GetMigratedSettings()
        {
            if (_migratedSettings == null)
            {
'''

[[source_entries]]
file = "src/Runner.Common/ConfigurationStore.cs"
change_type = "protocol_keyword_added"
fields = ["RunnerSettings", "_migratedSettings"]
snippet = '''
        private CredentialData _creds;
        private CredentialData _migratedCreds;
        private RunnerSettings _settings;
        private RunnerSettings _migratedSettings;

        public override void Initialize(IHostContext hostContext)
        {
'''

[[source_entries]]
file = "src/Runner.Common/ConfigurationStore.cs"
change_type = "protocol_keyword_added"
fields = ["SaveMigratedSettings", "RunnerSettings"]
snippet = '''
        void SaveCredential(CredentialData credential);
        void SaveMigratedCredential(CredentialData credential);
        void SaveSettings(RunnerSettings settings);
        void SaveMigratedSettings(RunnerSettings settings);
        void DeleteCredential();
        void DeleteMigratedCredential();
        void DeleteSettings();
'''

[[source_entries]]
file = "src/Runner.Common/ConfigurationStore.cs"
change_type = "protocol_keyword_added"
fields = ["SaveMigratedSettings", "RunnerSettings"]
snippet = '''
            File.SetAttributes(_configFilePath, File.GetAttributes(_configFilePath) | FileAttributes.Hidden);
        }

        public void SaveMigratedSettings(RunnerSettings settings)
        {
            Trace.Info("Saving runner migrated settings");
            if (File.Exists(_migratedConfigFilePath))
'''

[[source_entries]]
file = "src/Runner.Listener/BrokerMessageListener.cs"
change_type = "protocol_keyword_added"
fields = ["BrokerMessageListener", "RunnerSettings", "isMigratedSettings"]
snippet = '''
        {
        }

        public BrokerMessageListener(RunnerSettings settings, bool isMigratedSettings = false)
        {
            _settings = settings;
            _isMigratedSettings = isMigratedSettings;
'''

[[source_entries]]
file = "src/Runner.Listener/Configuration/ConfigurationManager.cs"
change_type = "protocol_keyword_added"
fields = ["RunnerSettings", "LoadMigratedSettings"]
snippet = '''
        Task UnconfigureAsync(CommandSettings command);
        void DeleteLocalRunnerConfig();
        RunnerSettings LoadSettings();
        RunnerSettings LoadMigratedSettings();
    }

    public sealed class ConfigurationManager : RunnerService, IConfigurationManager
'''

[[source_entries]]
file = "src/Runner.Listener/Configuration/ConfigurationManager.cs"
change_type = "protocol_keyword_added"
fields = ["RunnerSettings", "LoadMigratedSettings"]
snippet = '''
            return settings;
        }

        public RunnerSettings LoadMigratedSettings()
        {
            Trace.Info(nameof(LoadMigratedSettings));

'''

[[source_entries]]
file = "src/Runner.Listener/Configuration/ConfigurationManager.cs"
change_type = "protocol_keyword_added"
fields = ["RunnerSettings", "_store", "GetMigratedSettings"]
snippet = '''
                throw new NonRetryableException("No migrated configuration found.");
            }

            RunnerSettings settings = _store.GetMigratedSettings();
            Trace.Info("Migrated Settings Loaded");

            return settings;
'''

[[source_entries]]
file = "src/Runner.Listener/Configuration/ConfigurationManager.cs"
change_type = "protocol_keyword_added"
fields = ["runnerSettings", "UseV2Flow", "useV2Flow"]
snippet = '''
            if (agent.Properties.TryGetValue("UseV2Flow", out bool useV2Flow) && useV2Flow)
            {
                Trace.Info($"Service enforced useV2Flow: {useV2Flow}");
                runnerSettings.UseV2Flow = useV2Flow;
            }

            // Testing agent connection, detect any potential connection issue, like local clock skew that cause OAuth token expired.
'''

[[source_entries]]
file = "src/Runner.Listener/Runner.cs"
change_type = "protocol_keyword_added"
fields = ["IMessageListener", "GetMessageListener", "RunnerSettings", "isMigratedSettings"]
snippet = '''
            }
        }

        private IMessageListener GetMessageListener(RunnerSettings settings, bool isMigratedSettings = false)
        {
            if (settings.UseV2Flow)
            {
'''

[[source_entries]]
file = "src/Runner.Listener/Runner.cs"
change_type = "protocol_keyword_added"
fields = ["RunnerSettings", "migratedSettings"]
snippet = '''

                // First try using migrated settings if available
                var configManager = HostContext.GetService<IConfigurationManager>();
                RunnerSettings migratedSettings = null;

                try
                {
'''

[[source_entries]]
file = "src/Runner.Listener/Runner.cs"
change_type = "protocol_keyword_added"
fields = ["Task", "ExecuteRunnerAsync", "RunnerSettings", "runOnce", "returnRunOnceJobResult"]
snippet = '''
            return Constants.Runner.ReturnCode.Success;
        }

        private async Task<int> ExecuteRunnerAsync(RunnerSettings settings, bool runOnce, bool returnRunOnceJobResult)
        {
            int returnCode = Constants.Runner.ReturnCode.Success;
            bool restart = false;
'''

[[source_entries]]
file = "src/Runner.Listener/Runner.cs"
change_type = "protocol_keyword_added"
fields = ["Task", "RunAsync", "RunnerSettings", "runOnce", "returnRunOnceJobResult"]
snippet = '''
        }

        //create worker manager, create message listener and start listening to the queue
        private async Task<int> RunAsync(RunnerSettings settings, bool runOnce = false, bool returnRunOnceJobResult = false)
        {
            try
            {
'''

[[source_entries]]
file = "src/Runner.Listener/Runner.cs"
change_type = "protocol_keyword_added"
fields = ["_runnerServer", "UpdateAgentUpdateStateAsync", "runnerSettings", "PoolId", "runnerSettings", "AgentId", "RefreshConfig", "tokenSource"]
snippet = '''
                        {
                            using (var tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                            {
                                await _runnerServer.UpdateAgentUpdateStateAsync(runnerSettings.PoolId, runnerSettings.AgentId, "RefreshConfig", telemetry, tokenSource.Token);
                            }
                        }
                        catch (Exception ex)
'''

[[source_entries]]
file = "src/Runner.Listener/Runner.cs"
change_type = "protocol_keyword_added"
fields = ["runnerSettings"]
snippet = '''
                while (_authMigrationTelemetries.TryDequeue(out var telemetry))
                {
                    Trace.Verbose($"Reporting auth migration telemetry: {telemetry}");
                    if (runnerSettings != null)
                    {
                        try
                        {
'''

[[source_entries]]
file = "src/Runner.Listener/Runner.cs"
change_type = "protocol_keyword_added"
fields = ["runnerSettings", "configManager", "LoadSettings"]
snippet = '''
        private async Task ReportAuthMigrationTelemetryAsync(CancellationToken token)
        {
            var configManager = HostContext.GetService<IConfigurationManager>();
            var runnerSettings = configManager.LoadSettings();

            while (!token.IsCancellationRequested)
            {
'''

[[source_entries]]
file = "src/Runner.Listener/RunnerConfigUpdater.cs"
change_type = "file_added"
fields = []
snippet = '''
﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using GitHub.Runner.Common;
using GitHub.Runner.Sdk;
using GitHub.Services.Common;

namespace GitHub.Runner.Listener
{
    [ServiceLocator(Default = typeof(RunnerConfigUpdater))]
    public interface IRunnerConfigUpdater : IRunnerService
    {
        Task UpdateRunnerConfigAsync(string runnerQualifiedId, string configType, string serviceType, string configRefreshUrl);
    }

    public sealed class RunnerConfigUpdater : RunnerService, IRunnerConfigUpdater
    {
        private RunnerSettings _settings;
        private CredentialData _credData;
        private IRunnerServer _runnerServer;
        private IConfigurationStore _store;

        public override void Initialize(IHostContext hostContext)
        {
            base.Initialize(hostContext);
            _store = hostContext.GetService<IConfigurationStore>();
            _settings = _store.GetSettings();
'''

```
