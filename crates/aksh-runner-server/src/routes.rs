use super::*;

/// Build the production server router without simulation endpoints.
pub fn app(state: AppState, shutdown: CancellationToken) -> Router {
    build_app(state, shutdown, None)
}

/// Build an in-process router with privileged local/CI simulation endpoints.
///
/// Network servers should use [`serve`], which additionally enforces a
/// loopback-only listener when this API is enabled.
pub fn app_with_test_api(
    state: AppState,
    shutdown: CancellationToken,
    token: impl Into<String>,
) -> Router {
    build_app(state, shutdown, Some(token.into()))
}

pub(crate) fn build_app(
    state: AppState,
    shutdown: CancellationToken,
    test_api_token: Option<String>,
) -> Router {
    let shared = Arc::new(SharedState {
        state: state.clone(),
        shutdown: shutdown.clone(),
    });
    let protected_apis = Router::new()
        .route("/_apis/artifactcache/cache", post(cache_reserve))
        .route("/_apis/artifactcache/cache", get(cache_lookup))
        .route("/_apis/artifactcache/cache/:cache_id", patch(cache_upload))
        .route("/_apis/artifactcache/cache/:cache_id", post(cache_commit))
        .route(
            "/_apis/pipelines/workflows/:run_id/artifacts",
            post(artifact_create),
        )
        .route(
            "/_apis/pipelines/workflows/:run_id/artifacts",
            get(artifact_list),
        )
        .route(
            "/_apis/pipelines/workflows/:run_id/artifacts/:artifact_id",
            get(artifact_get_compat),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools",
            get(runner_pools),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/agents",
            get(agent_lookup).post(register_runner_compat_pool_only),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/agents/:agent_id",
            delete(delete_agent),
        )
        .route(
            "/_apis/distributedtask/pools/:pool_id/agents/:agent_id",
            delete(delete_agent),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/sessions",
            post(create_session_disttask).delete(delete_sessions_for_pool),
        )
        .route(
            "/_apis/distributedtask/pools/:pool_id/sessions",
            post(create_session_disttask).delete(delete_sessions_for_pool),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/sessions/:session_id",
            delete(delete_session),
        )
        .route(
            "/_apis/distributedtask/pools/:pool_id/sessions/:session_id",
            delete(delete_session),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/messages",
            get(next_message_disttask),
        )
        .route(
            "/runner/server/_apis/distributedtask/pools/:pool_id/messages/:message_id",
            delete(delete_pool_message),
        )
        .route(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/:run_id/jobs/:job_id",
            patch(complete_job_compat),
        )
        .route("/ws/live-logs/:job_id", get(ws_live_logs))
        .route("/broker/:runner_id/acquirejob", post(broker_acquire_job))
        .route("/broker/:runner_id/renewjob", post(broker_renew_job))
        .route("/broker/:runner_id/completejob", post(broker_complete_job))
        .route(
            "/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id",
            patch(patch_timeline_records).get(get_timeline_records),
        )
        .route(
            "/_apis/v1/Logfiles/:scope/:hub/:plan_id",
            post(create_log),
        )
        .route(
            "/_apis/v1/Logfiles/:scope/:hub/:plan_id/:log_id",
            post(append_log),
        )
        .route(
            "/_apis/v1/TimeLineWebConsoleLog/:scope/:hub/:plan_id/:timeline_id/:record_id",
            post(console_log),
        )
        .route(
            "/_apis/v1/FinishJob/:scope/:hub/:plan_id",
            post(finish_job),
        )
        .route(
            "/runner/server/_apis/distributedtask/hubs/actions/plans/:plan_id/jobs/:job_id/oidctoken",
            get(oidc_token),
        )
        .route(
            "/:orchestration_id//idtoken/:plan_id/:job_id",
            get(oidc_token_run_service),
        )
        .route(
            "/_apis/v1/ActionDownloadInfo/:scope/:hub/:plan_id",
            post(action_download_info),
        )
        .route(
            "/actions/build/:orchestration_id/jobs/:job_id/runnerresolve/actions",
            post(runnerresolve_actions),
        )
        .route(
            "/runner/server/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id",
            patch(patch_timeline_records).get(get_timeline_records),
        )
        .route(
            "/runner/server/_apis/v1/Logfiles/:scope/:hub/:plan_id",
            post(create_log),
        )
        .route(
            "/runner/server/_apis/v1/Logfiles/:scope/:hub/:plan_id/:log_id",
            post(append_log),
        )
        .route(
            "/runner/server/_apis/v1/TimeLineWebConsoleLog/:scope/:hub/:plan_id/:timeline_id/:record_id",
            post(console_log),
        )
        .route(
            "/runner/server/_apis/v1/FinishJob/:scope/:hub/:plan_id",
            post(finish_job),
        )
        .route(
            "/runner/server/_apis/v1/ActionDownloadInfo/:scope/:hub/:plan_id",
            post(action_download_info),
        )
        .route(
            "/api/v1/runs/:run_id/jobs/:job_id/logs/live",
            get(live_logs_sse),
        )
        // F030: standard AzDO API URL pattern used by the aksh-runner AzDO client.
        // These alias the scope/hub-prefixed handlers above so both URL forms work.
        .route(
            "/_apis/v1/plans/:plan_id/timelines/:timeline_id/records",
            patch(patch_timeline_records_plan).get(get_timeline_records_plan),
        )
        .route(
            "/_apis/v1/plans/:plan_id/logs",
            post(create_log_plan),
        )
        .route(
            "/_apis/v1/plans/:plan_id/logs/:log_id",
            put(append_log_plan),
        )
        .route(
            "/_apis/v1/plans/:plan_id/events",
            post(finish_job_plan),
        )
        // F030: /runner/server/ aliases — runner uses the SystemVssConnection URL
        // which is http://…/runner/server so all plan-level AzDO calls land here.
        .route(
            "/runner/server/_apis/v1/plans/:plan_id/timelines/:timeline_id/records",
            patch(patch_timeline_records_plan).get(get_timeline_records_plan),
        )
        .route(
            "/runner/server/_apis/v1/plans/:plan_id/logs",
            post(create_log_plan),
        )
        .route(
            "/runner/server/_apis/v1/plans/:plan_id/logs/:log_id",
            put(append_log_plan),
        )
        .route(
            "/runner/server/_apis/v1/plans/:plan_id/events",
            post(finish_job_plan),
        )
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_protocol_bearer,
        ));

    let results_metadata = Router::new()
        .route(
            "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
            post(twirp_create_step_summary_metadata),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
            post(twirp_create_step_logs_metadata),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
            post(twirp_create_job_logs_metadata),
        )
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_results_bearer,
        ))
        .with_state(shared.clone());

    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/.well-known/openid-configuration", get(oidc_discovery))
        .route("/.well-known/jwks", get(oidc_jwks))
        .route("/.well-known/jwks.json", get(oidc_jwks))
        .route(
            "/oidc/.well-known/openid-configuration",
            get(oidc_discovery),
        )
        .route("/oidc/.well-known/jwks", get(oidc_jwks))
        .route("/oidc/.well-known/jwks.json", get(oidc_jwks))
        // GHES-style org-prefixed routes
        .route("/:org/_apis/connectionData", get(connection_data))
        .route("/:org/_apis/v1/oauth2/token", post(oauth2_token))
        .route("/:org/_apis/v1/AgentPools", get(runner_pools))
        .route("/:org/_apis/v1/settings/runner", get(runner_settings))
        .route(
            "/:org/_apis/v1/Agent/:pool_id/:agent_id",
            get(agent_lookup_by_id_org).post(register_runner_compat_org_2),
        )
        .route(
            "/:org/_apis/v1/Agent/:pool_id",
            get(agent_lookup_org).post(register_runner_compat_org),
        )
        .route(
            "/:org/_apis/v1/AgentSession/:pool_id/:session_id",
            post(create_session_compat_org),
        )
        .route(
            "/:org/_apis/v1/AgentSession/:pool_id",
            post(create_session_compat_org_pool_only),
        )
        .route(
            "/:org/_apis/v1/AgentSession/:pool_id/:session_id",
            delete(delete_session_org),
        )
        .route(
            "/:org/_apis/v1/Message/:pool_id",
            get(next_message_compat_org),
        )
        .route(
            "/:org/_apis/v1/Message/:pool_id/:message_id",
            delete(delete_pool_message_org),
        )
        .route(
            "/:org/_apis/v1/AgentRequest/:pool_id/:request_id",
            get(agent_request_get_org)
                .post(agent_request_ack_org)
                .patch(agent_request_patch_org),
        )
        .route(
            "/:org/_apis/v1/Timeline/:scope/:hub/:plan_id/:timeline_id",
            patch(patch_timeline_records_org).get(get_timeline_records_org),
        )
        .route(
            "/:org/_apis/v1/Logfiles/:scope/:hub/:plan_id",
            post(create_log_org),
        )
        .route(
            "/:org/_apis/v1/Logfiles/:scope/:hub/:plan_id/:log_id",
            post(append_log_org),
        )
        .route(
            "/:org/_apis/v1/TimeLineWebConsoleLog/:scope/:hub/:plan_id/:timeline_id/:record_id",
            post(console_log_org),
        )
        .route(
            "/:org/_apis/v1/FinishJob/:scope/:hub/:plan_id",
            post(finish_job_org),
        )
        .route(
            "/:org/_apis/v1/ActionDownloadInfo/:scope/:hub/:plan_id",
            post(action_download_info_org),
        )
        .route("/_apis/v1/oauth2/token", post(oauth2_token))
        .route(
            "/api/v3/actions/runner-registration",
            post(github_registration_token),
        )
        .route(
            "/api/v3/orgs/:org/actions/runners/registration-token",
            post(github_registration_token),
        )
        .route(
            "/api/v3/repos/:owner/:repo/actions/runners/registration-token",
            post(github_registration_token),
        )
        .route("/runner/server/_apis/connectionData", get(connection_data))
        .route("/runner/server/_apis/v1/oauth2/token", post(oauth2_token))
        .route("/runner/server/_apis/v1/AgentPools", get(runner_pools))
        .route(
            "/runner/server/_apis/v1/Agent/:pool_id/:agent_id",
            get(agent_lookup_by_id)
                .post(register_runner_compat)
                .put(register_runner_compat),
        )
        .route(
            "/runner/server/_apis/v1/Agent/:pool_id",
            get(agent_lookup).post(register_runner_compat_pool_only),
        )
        .route(
            "/runner/server/_apis/v1/AgentSession/:pool_id/:session_id",
            post(create_session_compat),
        )
        .route(
            "/runner/server/_apis/v1/AgentSession/:pool_id",
            post(create_session_compat_pool_only),
        )
        .route(
            "/runner/server/_apis/v1/AgentSession/:pool_id/:session_id",
            delete(delete_session),
        )
        .route(
            "/runner/server/_apis/v1/Message/:pool_id",
            get(next_message_compat),
        )
        .route(
            "/runner/server/_apis/v1/Message/:pool_id/:message_id",
            delete(delete_pool_message),
        )
        .route(
            "/runner/server/_apis/v1/AgentRequest/:pool_id/:request_id",
            get(agent_request_get)
                .post(agent_request_ack)
                .patch(agent_request_patch),
        )
        .route("/_apis/connectionData", get(connection_data))
        .route(
            "/api/v1/runs",
            post(submit_run)
                .get(list_runs)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_native_bearer,
                )),
        )
        .route("/api/v1/scheduler/history", get(get_scheduler_history))
        .route(
            "/api/v1/github/webhooks",
            post(github::handle_github_webhook),
        )
        .route("/api/v1/github/register", get(github::github_register))
        .route("/api/v1/github/callback", get(github::github_callback))
        .route(
            "/api/v1/runs/:run_id",
            get(get_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/logs",
            get(get_run_logs).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/cancel",
            post(cancel_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/rerun",
            post(rerun_run).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/events.ndjson",
            get(run_events).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/debug",
            get(ws_dap_debug).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/runs/:run_id/debug",
            post(register_dap_port).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        // Live debug sessions. Worker-facing routes are runner-authenticated;
        // controller-facing routes are native-authenticated. Both live on the
        // native surface so `/_apis/...` stays byte-identical.
        //
        // The credential those worker routes need is not in the job message —
        // an official runner would republish it as `${{ secrets[...] }}` — so
        // the worker exchanges its job runtime token for it here first.
        .route(
            "/api/v1/debug/worker-token",
            post(crate::debug_sessions::issue_worker_token).route_layer(
                middleware::from_fn_with_state(shared.clone(), require_job_runtime_bearer),
            ),
        )
        .route(
            "/api/v1/debug/sessions",
            post(crate::debug_sessions::open_session).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_worker_bearer,
            )),
        )
        .route(
            "/api/v1/debug/sessions/:session_id/verdict",
            get(crate::debug_sessions::poll_verdict).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_worker_bearer,
            )),
        )
        .route(
            "/api/v1/debug/sessions/:session_id/close",
            post(crate::debug_sessions::close_session).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_worker_bearer,
            )),
        )
        .route(
            "/api/v1/debug/sessions",
            get(crate::debug_sessions::list_sessions).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/debug/sessions/:session_id",
            get(crate::debug_sessions::get_session).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/debug/sessions/:session_id/verdict",
            post(crate::debug_sessions::post_verdict).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        // Structured agent debugging surface. It is deliberately separate
        // from the human CLI verbs, but both mutate the same session state.
        .route(
            "/api/v1/agent/debug/sessions/:session_id/lease",
            post(crate::debug_sessions::agent_acquire_lease)
                .delete(crate::debug_sessions::agent_release_lease)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_native_bearer,
                )),
        )
        .route(
            "/api/v1/agent/debug/sessions/:session_id/events",
            get(crate::debug_sessions::agent_events).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/agent/debug/sessions/:session_id/operations",
            post(crate::debug_sessions::agent_operation).route_layer(
                middleware::from_fn_with_state(shared.clone(), require_native_bearer),
            ),
        )
        .route(
            "/api/v1/agent/debug/sessions/:session_id/audit",
            get(crate::debug_sessions::agent_audit).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        // Archive tickets are bearerless in the official runner protocol.
        .route(
            "/api/v1/actions/download/:owner/:repo/*git_ref",
            get(download_action_tarball),
        )
        // Read-only Git smart HTTP for immutable local-workspace snapshots.
        .route(
            "/snapshots/:run_id/*path",
            get(snapshot_git_http).post(snapshot_git_http),
        )
        .route(
            "/api/v1/runners",
            post(register_runner).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/runner/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/runner/session/:session_id",
            delete(broker_delete_session_by_path),
        )
        .route(
            "/runner/message",
            get(next_message_broker_ref_root).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        .route("/runner/acknowledge", post(broker_acknowledge_root))
        .route(
            "/runner/server/runner/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/runner/server/runner/session/:session_id",
            delete(broker_delete_session_by_path),
        )
        .route(
            "/runner/server/runner/message",
            get(next_message_broker_ref_root).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        .route(
            "/runner/server/runner/acknowledge",
            post(broker_acknowledge_root),
        )
        .route(
            "/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/session/:session_id",
            delete(broker_delete_session_by_path),
        )
        .route(
            "/message",
            get(next_message_broker_ref_root).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        .route("/acknowledge", post(broker_acknowledge_root))
        .route(
            "/runner/server/session",
            post(broker_session_root).delete(broker_delete_session_root),
        )
        .route(
            "/runner/server/session/:session_id",
            delete(broker_delete_session_by_path),
        )
        .route(
            "/runner/server/message",
            get(next_message_broker_ref_root).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_runner_bearer,
            )),
        )
        .route("/runner/server/acknowledge", post(broker_acknowledge_root))
        .route(
            "/api/v1/cache",
            get(cache_get)
                .post(cache_put)
                .route_layer(middleware::from_fn_with_state(
                    shared.clone(),
                    require_native_bearer,
                )),
        )
        .route(
            "/api/v1/artifacts",
            post(artifact_put).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route(
            "/api/v1/artifacts/:artifact_id",
            get(artifact_get).route_layer(middleware::from_fn_with_state(
                shared.clone(),
                require_native_bearer,
            )),
        )
        .route("/_apis/v1/settings/runner", get(runner_settings))
        // Runner lifecycle endpoints — public before the runner receives its token.
        .route("/_apis/v1/AgentPools", get(runner_pools))
        .route(
            "/_apis/v1/Agent/:pool_id/:agent_id",
            get(agent_lookup_by_id).post(register_runner_compat),
        )
        .route(
            "/_apis/v1/Agent/:pool_id",
            get(agent_lookup).post(register_runner_compat_pool_only),
        )
        .route(
            "/_apis/v1/AgentSession/:pool_id/:session_id",
            post(create_session_compat),
        )
        .route(
            "/_apis/v1/AgentSession/:pool_id",
            post(create_session_compat_pool_only),
        )
        .route(
            "/_apis/v1/AgentSession/:pool_id/:session_id",
            delete(delete_session),
        )
        .route("/_apis/v1/Message/:pool_id", get(next_message_compat))
        .route(
            "/_apis/v1/Message/:pool_id/:message_id",
            delete(delete_pool_message),
        )
        .route(
            "/_apis/v1/AgentRequest/:pool_id/:request_id",
            get(agent_request_get)
                .post(agent_request_ack)
                .patch(agent_request_patch),
        )
        // P1.10: Accept blob uploads at the signed-URL paths minted by the Twirp handlers.
        // The runner PUTs logs/summaries here; we store them in the state directory.
        .route("/replay/results/*path", put(replay_results_put))
        // Twirp APIs accept only the system token or a locally signed Actions.Results job token.
        .route(
            "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
            post(twirp_workflow_steps_update),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
            post(twirp_get_job_logs_signed_blob_url),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
            post(twirp_get_step_logs_signed_blob_url),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/GetStepSummarySignedBlobURL",
            post(twirp_get_step_summary_signed_blob_url),
        )
        .route(
            "/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL",
            post(twirp_get_job_diag_logs_signed_blob_url),
        )
        // Cache v2 Twirp (CacheService) — used by actions/cache@v4 when ACTIONS_CACHE_SERVICE_V2=true.
        // The shared Twirp middleware below validates the job token before body extraction.
        .route(
            "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
            post(twirp_cache_v2_create),
        )
        .route(
            "/twirp/github.actions.results.api.v1.CacheService/FinalizeCacheEntryUpload",
            post(twirp_cache_v2_finalize),
        )
        .route(
            "/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL",
            post(twirp_cache_v2_get_dl_url),
        )
        // Artifact v2 Twirp (ArtifactService) — used by actions/upload-artifact@v4 and download-artifact@v4.
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
            post(twirp_artifact_v2_create),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/FinalizeArtifact",
            post(twirp_artifact_v2_finalize),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/ListArtifacts",
            post(twirp_artifact_v2_list),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/GetSignedArtifactURL",
            post(twirp_artifact_v2_get_signed_url),
        )
        .route(
            "/twirp/github.actions.results.api.v1.ArtifactService/DeleteArtifact",
            post(twirp_artifact_v2_delete),
        )
        // Azure Block Blob compat blob store — upload (PUT) and download (GET).
        // Cache: /twirp-blob/cache/{token}
        // Artifact: /twirp-blob/artifact/{token}  (download URL appends .zip for content-type detection)
        .route(
            "/twirp-blob/:kind/:token",
            put(blob_put)
                .layer(DefaultBodyLimit::max(512 * 1024 * 1024))
                .get(blob_get),
        )
        .route_layer(middleware::from_fn_with_state(
            shared.clone(),
            require_results_bearer,
        ))
        .merge(protected_apis)
        .with_state(shared.clone())
        .merge(results_metadata)
        .fallback(errors::protocol_not_found)
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn(errors::protocol_error_envelope))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            record_flows_middleware,
        ));

    match test_api_token {
        Some(token) => router.merge(
            Router::new()
                .route(
                    "/internal/test/runners/sessions/:session_id/messages",
                    get(next_message),
                )
                .route(
                    "/internal/test/runners/sessions/:session_id/messages/:message_id",
                    delete(delete_session_message),
                )
                .route("/internal/test/runners/sessions", post(create_session))
                .route("/internal/test/jobs/complete", post(complete_job))
                .route_layer(middleware::from_fn_with_state(
                    Arc::<str>::from(token),
                    require_test_api_token,
                ))
                .with_state(shared),
        ),
        None => router,
    }
}
