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
