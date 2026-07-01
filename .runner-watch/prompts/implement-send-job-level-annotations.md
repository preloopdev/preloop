You are implementing an aksh protocol-sync spec. Follow existing Rust patterns exactly. Run cargo check and relevant tests, but do not run formatters or project-wide lint.

Spec:
```toml
change_id = "send-job-level-annotations"
upstream_version = "v2.335.1"
category = "feature"
tags = ["timeline", "annotations"]
ai_status = "deterministic-known-fidelity-gap"

[description]
what = '''
Runner can send job-level annotations in timeline updates.
'''
why = '''
Newer runners aggregate annotations beyond individual step records.
'''
runner_behavior = '''
Timeline PATCH includes issue/annotation payloads that apply at job level.
'''
failure_mode = '''
Annotations may be missing from UI; job execution continues.
'''

[feature_flag]
name = "SendJobLevelAnnotations"
where = "timeline/feature flag response"
default = false

[wire]
request = '''
PATCH /_apis/v1/Timeline/... with issues[]
'''
expected_response = "200 JSON timeline collection"

[aksh_targets]
files = [
  { crate = "aksh-gha-protocol", path = "crates/aksh-gha-protocol/src/azdo.rs", area = "TimelineRecord DTO" },
  { crate = "aksh-runner-server", path = "crates/aksh-runner-server/src/lib.rs", area = "timeline handlers" },
]

[implementation]
approach = '''
Preserve issues[] on job records and project them to annotations.
'''
test = "Timeline PATCH with job issues stores annotations."

[[source_entries]]
file = "src/Runner.Common/Constants.cs"
change_type = "protocol_keyword_added"
fields = ["SendJobLevelAnnotations", "actions_send_job_level_annotations"]
snippet = '''
                public static readonly string CompareWorkflowParser = "actions_runner_compare_workflow_parser";
                public static readonly string ServiceContainerCommand = "actions_service_container_command";
                public static readonly string SetOrchestrationIdEnvForActions = "actions_set_orchestration_id_env_for_actions";
                public static readonly string SendJobLevelAnnotations = "actions_send_job_level_annotations";
                public static readonly string EmitCompositeMarkers = "actions_runner_emit_composite_markers";
                public static readonly string BatchActionResolution = "actions_batch_action_resolution";
                public static readonly string UseBearerTokenForCodeload = "actions_use_bearer_token_for_codeload";
'''

[[source_entries]]
file = "src/Runner.Worker/ExecutionContext.cs"
change_type = "feature_flag_added"
fields = ["Global", "Variables", "GetBoolean", "Constants", "Runner", "Features", "SendJobLevelAnnotations"]
snippet = '''
                Global.StepsResult.Add(stepResult);
            }

            if (Global.Variables.GetBoolean(Constants.Runner.Features.SendJobLevelAnnotations) ?? false)
            {
                if (_record.RecordType == ExecutionContextType.Job)
                {
'''

[[source_entries]]
file = "src/Runner.Worker/ExecutionContext.cs"
change_type = "protocol_keyword_added"
fields = ["Global", "Variables", "GetBoolean", "Constants", "Runner", "Features", "SendJobLevelAnnotations"]
snippet = '''
                Global.StepsResult.Add(stepResult);
            }

            if (Global.Variables.GetBoolean(Constants.Runner.Features.SendJobLevelAnnotations) ?? false)
            {
                if (_record.RecordType == ExecutionContextType.Job)
                {
'''

```
