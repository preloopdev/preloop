-- Populated control-store fixture (SQLite), at the v1 baseline schema.
--
-- This is the "previous release" data an upgrade must preserve: representative
-- rows for the persisted work a real store holds (runs, jobs, specs, needs,
-- requests, leases, steps, timelines, logs, runners, sessions and messages,
-- webhook queues, check-run updates, outbox rows and the terminal history
-- tables). The migration gate applies it to a baseline-migration database,
-- then runs the pending migrations and asserts these rows survive unchanged.
--
-- Values are synthetic: no secrets, no real user data. Timestamps are fixed
-- microseconds (`1759276800000000` = 2025-10-01T00:00:00Z) so invariants can
-- assert exact values after the upgrade.

INSERT INTO namespaces (namespace_id, state, cell_generation, config_version)
VALUES ('fixture-ns', 'active', 1, 0);

INSERT INTO workflow_run_numbers (namespace_id, repository, workflow_path, last_run_number)
VALUES ('fixture-ns', 'octo/fixture', '.github/workflows/ci.yml', 2);

-- r-completed: a terminal webhook run whose results moved to history below.
-- r-queued: a live fork-approval hold (the 0002 sweep index's predicate).
INSERT INTO runs (
    run_id, namespace_id, repository, workflow_path, run_number, run_attempt,
    run_name, event, ref, ref_type, head_ref, base_ref, head_sha, workflow_ref,
    status, conclusion, webhook_delivery_id, origin, actor, tree_digest,
    fork_approval_pending, fork_approval_requested_at, reports_check_runs,
    created_at, started_at, completed_at
) VALUES
(
    '11111111-1111-4111-8111-111111111111', 'fixture-ns', 'octo/fixture',
    '.github/workflows/ci.yml', 1, 1, 'CI', 'push', 'refs/heads/main', 'branch',
    NULL, NULL, 'abc123', 'octo/fixture/.github/workflows/ci.yml@refs/heads/main',
    'completed', 'success', 'd-fixture-1', 'webhook', 'octocat', 'tree-deadbeef',
    0, NULL, 1, 1759276800000000, 1759276801000000, 1759276860000000
),
(
    '22222222-2222-4222-8222-222222222222', 'fixture-ns', 'octo/fixture',
    '.github/workflows/ci.yml', 2, 1, 'CI', 'pull_request', 'refs/pull/7/merge',
    'pull_request', 'feature', 'main', 'def456', 'octo/fixture/.github/workflows/ci.yml@refs/pull/7/merge',
    'queued', NULL, NULL, 'cli', NULL, NULL,
    1, 1759277000000000, 0, 1759277000000000, NULL, NULL
);

INSERT INTO jobs (
    run_id, job_id, namespace_id, kind, base_id, status, queue_state,
    remaining_needs, pool_key, runs_on, priority, run_order, job_order,
    not_before, enqueued_at, claimed_by_runner_id, claimed_at, version,
    outputs, created_at, started_at, completed_at
) VALUES
(
    '11111111-1111-4111-8111-111111111111', 'build', 'fixture-ns', 'job', 'build',
    'success', 'none', 0, 'default', '["ubuntu-latest"]', 0, 0, 0,
    NULL, 1759276800000000, 42, 1759276801000000, 3,
    '{"artifact":"a1"}', 1759276800000000, 1759276801000000, 1759276860000000
),
(
    '22222222-2222-4222-8222-222222222222', 'build', 'fixture-ns', 'job', 'build',
    'success', 'none', 0, 'default', '["ubuntu-latest"]', 0, 0, 0,
    NULL, 1759277000000000, 42, 1759277001000000, 1,
    NULL, 1759277000000000, 1759277001000000, 1759277010000000
),
(
    '22222222-2222-4222-8222-222222222222', 'deploy', 'fixture-ns', 'job', 'deploy',
    'queued', 'ready', 1, 'default', '["ubuntu-latest"]', 5, 0, 1,
    NULL, 1759277000000000, NULL, NULL, 0,
    NULL, 1759277000000000, NULL, NULL
);

INSERT INTO job_specs (
    run_id, job_id, display_name, display_order, if_condition, matrix,
    deferred_matrix, max_parallel, environment, concurrency, fail_fast,
    continue_on_error, id_token_granted
) VALUES
(
    '11111111-1111-4111-8111-111111111111', 'build', 'Build', 0, NULL, '{}',
    NULL, NULL, NULL, NULL, NULL, NULL, 1
),
(
    '22222222-2222-4222-8222-222222222222', 'build', 'Build', 0, NULL, '{}',
    NULL, NULL, NULL, NULL, NULL, NULL, 0
),
(
    '22222222-2222-4222-8222-222222222222', 'deploy', 'Deploy', 1, NULL, '{}',
    NULL, NULL, 'production', 'deploy-${{ github.ref }}', 1, 0, 0
);

INSERT INTO job_needs (run_id, job_id, needs_job_id, position) VALUES
('22222222-2222-4222-8222-222222222222', 'deploy', 'build', 0);

INSERT INTO runners (
    runner_id, namespace_id, name, labels, ephemeral, client_id, public_key,
    rsa_public_key, pool_proven, registered_at, last_seen_at
) VALUES
(
    42, 'fixture-ns', 'runner-fixture', '["self-hosted","linux","x64"]', 0,
    'client-fixture-1', 'ssh-rsa AAAA', X'0102030405', 1,
    1759276700000000, 1759276860000000
);

INSERT INTO runner_sessions (
    session_id, runner_id, protocol, client_id, verified, engine_node_id,
    created_at, last_seen_at
) VALUES
(
    '33333333-3333-4333-8333-333333333333', 42, 'broker', 'client-fixture-1', 1,
    'node-fixture', 1759276800000000, 1759276860000000
);

INSERT INTO job_requests (
    request_id, run_id, job_id, namespace_id, agent_job_id, timeline_id,
    runner_id, session_id, result, claimed_at, started_at, finished_at
) VALUES
(
    1000001, '11111111-1111-4111-8111-111111111111', 'build', 'fixture-ns',
    '44444444-4444-4444-8444-444444444444',
    '55555555-5555-4555-8555-555555555555', 42,
    '33333333-3333-4333-8333-333333333333', 'success',
    1759276801000000, 1759276802000000, 1759276860000000
);

INSERT INTO job_leases (request_id, runner_id, expires_at, renewed_at)
VALUES (1000001, 42, 1759276920000000, 1759276850000000);

INSERT INTO job_steps (
    agent_job_id, step_id, position, kind, workflow_index, runner_number,
    context_name, name, conclusion, started_at, finished_at
) VALUES
(
    '44444444-4444-4444-8444-444444444444', '66666666-6666-4666-8666-666666666666',
    0, 'workflow', 1, 1, 'build', 'Run tests', 'succeeded',
    1759276802000000, 1759276850000000
);

INSERT INTO timelines (timeline_id, change_id)
VALUES ('55555555-5555-4555-8555-555555555555', 2);

INSERT INTO timeline_records (timeline_id, record_id, change_id, record)
VALUES
(
    '55555555-5555-4555-8555-555555555555',
    '66666666-6666-4666-8666-666666666666', 1,
    '{"id":"66666666-6666-4666-8666-666666666666","type":"Task","name":"Run tests"}'
);

INSERT INTO log_files (log_key, run_id, plan_id, log_id, byte_count, line_count, updated_at)
VALUES
(
    'log-fixture-build-1', '11111111-1111-4111-8111-111111111111',
    '44444444-4444-4444-8444-444444444444', 1, 128, 4, 1759276860000000
);

INSERT INTO session_messages (message_id, session_id, message_type, request_id, body, created_at)
VALUES
(
    1000001, '33333333-3333-4333-8333-333333333333', 'JobRequest', 1000001,
    '{"messageId":1000001}', 1759276801000000
);

INSERT INTO webhook_deliveries (
    delivery_id, installation_id, event, payload, received_at, state,
    attempts, lease_until, lease_token, last_error
) VALUES
(
    'd-fixture-1', 7, 'workflow_run', '{"action":"completed","run_id":1}',
    1759276799000000, 'done', 1, NULL, NULL, NULL
),
(
    'd-fixture-2', 7, 'workflow_run', '{"action":"requested","run_id":2}',
    1759277000000000, 'received', 0, NULL, NULL, NULL
);

INSERT INTO webhook_watchdog (
    scope, cursor_delivered_at, cursor_delivery_guid, scan_cursor,
    last_poll_at, last_success_at
) VALUES
('installation:7', 1759276795000000, 'gh-guid-1', 'cursor-fixture',
 1759277010000000, 1759277005000000);

INSERT INTO webhook_redeliveries (
    delivery_guid, github_delivery_id, app_id, reason, attempts,
    first_seen_at, last_attempt_at, resolved_at, last_error
) VALUES
(
    'redeliver-fixture-1', 987654, '12345', 'phantom_ack', 2,
    1759276000000000, 1759276300000000, NULL, 'delivery not acknowledged'
);

INSERT INTO check_run_updates (
    run_id, job_id, installation_id, check_run_id, payload, not_before,
    attempts, leased_until
) VALUES
(
    '22222222-2222-4222-8222-222222222222', 'deploy', 7, NULL,
    '{"status":"queued"}', 1759277000000000, 0, NULL
);

INSERT INTO outbox_events (
    event_id, namespace_id, run_id, job_id, version, topic, payload, created_at
) VALUES
(
    1, 'fixture-ns', '11111111-1111-4111-8111-111111111111', 'build', 3,
    'run.completed.v1', '{"conclusion":"success"}', 1759276860000000
);

INSERT INTO run_history (
    run_id, namespace_id, repository, workflow_path, run_number, run_attempt,
    run_name, event, ref, ref_type, head_ref, base_ref, head_sha, conclusion,
    submission, record_details, fork_approval_pending, reports_check_runs,
    created_at, started_at, completed_at
) VALUES
(
    '11111111-1111-4111-8111-111111111111', 'fixture-ns', 'octo/fixture',
    '.github/workflows/ci.yml', 1, 1, 'CI', 'push', 'refs/heads/main', 'branch',
    NULL, NULL, 'abc123', 'success', '{"run_id":"11111111-1111-4111-8111-111111111111"}',
    '{"jobs":2}', 0, 1, 1759276800000000, 1759276801000000, 1759276860000000
);

INSERT INTO job_history (
    run_id, run_created_at, job_id, namespace_id, kind, base_id, display_name,
    status, pool_key, outputs, created_at, started_at, completed_at
) VALUES
(
    '11111111-1111-4111-8111-111111111111', 1759276800000000, 'build',
    'fixture-ns', 'job', 'build', 'Build', 'success', 'default',
    '{"artifact":"a1"}', 1759276800000000, 1759276801000000, 1759276860000000
);

INSERT INTO attempt_history (
    request_id, run_id, run_created_at, job_id, namespace_id, agent_job_id,
    timeline_id, runner_id, result, claimed_at, started_at, finished_at
) VALUES
(
    1000001, '11111111-1111-4111-8111-111111111111', 1759276800000000, 'build',
    'fixture-ns', '44444444-4444-4444-8444-444444444444',
    '55555555-5555-4555-8555-555555555555', 42, 'success',
    1759276801000000, 1759276802000000, 1759276860000000
);

INSERT INTO step_history (
    agent_job_id, step_id, run_id, run_created_at, namespace_id, position, kind,
    workflow_index, runner_number, context_name, name, conclusion, started_at,
    finished_at
) VALUES
(
    '44444444-4444-4444-8444-444444444444', '66666666-6666-4666-8666-666666666666',
    '11111111-1111-4111-8111-111111111111', 1759276800000000, 'fixture-ns', 0,
    'workflow', 1, 1, 'build', 'Run tests', 'succeeded',
    1759276802000000, 1759276850000000
);
