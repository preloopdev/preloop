//! preloop-runner-server integration tests — webhooks group.
//! Split from the former `lib_tests.rs` unit; see `tests/common/mod.rs`.

mod common;

use common::*;

#[tokio::test]
async fn snapshot_before_sha_tracks_working_tree_state() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    fs::create_dir_all(&state_dir).unwrap();
    git_fixture_command(&workspace, &["init", "-b", "main"]);
    git_fixture_command(&workspace, &["config", "user.name", "Snapshot Test"]);
    git_fixture_command(
        &workspace,
        &["config", "user.email", "snapshot@example.test"],
    );
    fs::write(workspace.join("file.txt"), "one\n").unwrap();
    git_fixture_command(&workspace, &["add", "file.txt"]);
    git_fixture_command(&workspace, &["commit", "-m", "c0"]);
    fs::write(workspace.join("file.txt"), "two\n").unwrap();
    git_fixture_command(&workspace, &["add", "file.txt"]);
    git_fixture_command(&workspace, &["commit", "-m", "c1"]);

    let head = String::from_utf8(git_fixture_output(&workspace, &["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned();
    let head_parent = String::from_utf8(git_fixture_output(&workspace, &["rev-parse", "HEAD^"]))
        .unwrap()
        .trim()
        .to_owned();

    // Clean tree: the change under test is the last commit, so the diff base
    // is HEAD^ (an equal-tree HEAD..S would be empty).
    let clean_run: RunId = "66666666-6666-4666-8666-666666666666".parse().unwrap();
    let clean = create_workspace_snapshot(&state_dir, &workspace, clean_run, None, None)
        .await
        .expect("clean-tree snapshot should succeed");
    assert_eq!(
        clean.before_sha.as_deref(),
        Some(head_parent.as_str()),
        "clean tree must diff against HEAD^"
    );

    // Dirty tree: the change under test is the uncommitted edit, so the diff
    // base is HEAD itself.
    fs::write(workspace.join("file.txt"), "three (uncommitted)\n").unwrap();
    let dirty_run: RunId = "77777777-7777-4777-8777-777777777777".parse().unwrap();
    let dirty = create_workspace_snapshot(&state_dir, &workspace, dirty_run, None, None)
        .await
        .expect("dirty-tree snapshot should succeed");
    assert_ne!(
        dirty.commit_sha, clean.commit_sha,
        "uncommitted edit must produce a distinct snapshot commit"
    );
    assert_eq!(
        dirty.before_sha.as_deref(),
        Some(head.as_str()),
        "dirty tree must diff against HEAD"
    );
}

#[tokio::test]
async fn terminal_run_discards_workspace_snapshot_but_preserves_object_cache() {
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());
    let mut state = AppState::new(state_dir.clone()).await.unwrap();
    state.local_workspace = Some(workspace);
    state.snapshot_retention_seconds = 0;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo snapshot
"#,
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let repository = state_dir.join("snapshots").join(run_id.to_string());
    let object_cache = state_dir.join("snapshot-object-cache");
    assert!(
        repository.is_dir(),
        "submission should create the run snapshot"
    );
    assert!(
        object_cache.is_dir(),
        "submission should create the shared object cache"
    );

    let job_id = {
        let inner = state.inner.lock().await;
        inner
            .runs
            .get(&run_id)
            .and_then(|run| run.jobs.keys().next())
            .cloned()
            .expect("submitted run should have one dispatchable job")
    };
    // Synthetic push payloads carry a `head_commit` object (GitHub shape):
    // workflows gate on `github.event.head_commit.message` and must not see a
    // null that makes property access error out.
    {
        let inner = state.inner.lock().await;
        let run = inner.runs.get(&run_id).unwrap();
        let head_commit = &run.github["event"]["head_commit"];
        let id = head_commit["id"].as_str().unwrap();
        assert_eq!(id.len(), 40, "head_commit.id must be the snapshot commit");
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(head_commit["distinct"], true);
        assert_eq!(head_commit["message"], "");
        assert_eq!(run.github["event"]["before"].as_str().unwrap().len(), 40);
        assert_eq!(run.github["event"]["after"], json!(id));
    }
    complete_via_api(&app, &run_id.to_string(), &job_id.to_string()).await;

    assert_eq!(
        get_run_json(&app, &run_id.to_string()).await["status"],
        "success"
    );
    assert!(
        !repository.exists(),
        "terminal completion should remove the run's workspace snapshot"
    );
    assert!(
        object_cache.is_dir(),
        "terminal completion must preserve the shared snapshot object cache"
    );
}

#[tokio::test]
async fn submit_rejects_invalid_schedule_cron() {
    // GitHub rejects an unparsable `on.schedule` cron at workflow save; aksh
    // rejects it at submit instead of registering a cron job that never fires.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let response = request_json_status(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on:\n  push:\n  schedule:\n    - cron: 'not a cron'\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    assert_eq!(response.0, StatusCode::BAD_REQUEST);

    // Valid cron still submits.
    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on:\n  push:\n  schedule:\n    - cron: '0 0 * * *'\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
            "event": "push",
            "repository": "owner/repo",
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 1);
}

#[tokio::test]
async fn discard_workspace_snapshot_is_idempotent_when_repository_absent() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let snapshots = state_dir.join("snapshots");
    let object_cache = state_dir.join("snapshot-object-cache");
    fs::create_dir_all(&snapshots).unwrap();
    fs::create_dir_all(&object_cache).unwrap();
    let run_id: RunId = "55555555-5555-4555-8555-555555555555".parse().unwrap();

    discard_workspace_snapshot(&state_dir, run_id).await;
    discard_workspace_snapshot(&state_dir, run_id).await;

    assert!(!snapshots.join(run_id.to_string()).exists());
    assert!(object_cache.is_dir());
}

#[tokio::test]
async fn workspace_snapshots_reuse_large_base_objects_and_materialize_changes() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    git_fixture_command(&workspace, &["init", "-b", "main"]);
    git_fixture_command(&workspace, &["config", "user.name", "Snapshot Test"]);
    git_fixture_command(
        &workspace,
        &["config", "user.email", "snapshot@example.test"],
    );
    fs::write(workspace.join(".gitignore"), "*.ignored\nignored-dir/\n").unwrap();

    // A packed, multi-megabyte base makes accidentally copying every reachable
    // object into each run repository observable rather than a tiny-fixture
    // optimization detail.
    for file in 0..64u8 {
        let mut state = 0x9e37_79b9u32 ^ u32::from(file).wrapping_mul(0x045d_9f3b);
        let contents: Vec<u8> = (0..65_536)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (state >> 24) as u8
            })
            .collect();
        fs::write(workspace.join(format!("base-{file:02}.bin")), contents).unwrap();
    }
    git_fixture_command(&workspace, &["add", "."]);
    git_fixture_command(&workspace, &["commit", "-m", "large base"]);
    git_fixture_command(&workspace, &["gc", "--aggressive", "--prune=now"]);
    let source_pack_bytes = git_pack_bytes(&workspace.join(".git"));
    assert!(source_pack_bytes > 1_000_000);

    fs::create_dir_all(&state_dir).unwrap();
    let first_run: RunId = "22222222-2222-4222-8222-222222222222".parse().unwrap();
    let second_run: RunId = "33333333-3333-4333-8333-333333333333".parse().unwrap();
    let changed_run: RunId = "44444444-4444-4444-8444-444444444444".parse().unwrap();

    let first = create_workspace_snapshot(&state_dir, &workspace, first_run, None, None)
        .await
        .expect("first snapshot should succeed");
    let second = create_workspace_snapshot(&state_dir, &workspace, second_run, None, None)
        .await
        .expect("second unchanged snapshot should succeed");
    let first_repository = state_dir.join(&first.repository);
    let second_repository = state_dir.join(&second.repository);
    assert_eq!(first.commit_sha, second.commit_sha);

    let state_dir = std::fs::canonicalize(&state_dir).unwrap();
    let first_alternates = git_alternate_object_directories(&first_repository);
    let second_alternates = git_alternate_object_directories(&second_repository);
    assert_eq!(first_alternates, second_alternates);
    assert!(first_alternates
        .iter()
        .all(|alternate| { alternate.starts_with(&state_dir) && alternate != &state_dir }));
    assert!(
        git_pack_bytes(&first_repository) < source_pack_bytes / 2,
        "first run repository contains a full-base pack"
    );
    assert!(
        git_pack_bytes(&second_repository) < source_pack_bytes / 2,
        "second run repository contains a full-base pack instead of reusing the cache"
    );

    fs::write(workspace.join("base-00.bin"), b"changed unstaged base\n").unwrap();
    fs::write(workspace.join("base-01.bin"), b"changed staged base\n").unwrap();
    git_fixture_command(&workspace, &["add", "base-01.bin"]);
    fs::remove_file(workspace.join("base-02.bin")).unwrap();
    fs::write(workspace.join("new.txt"), b"new untracked file\n").unwrap();
    fs::write(workspace.join("not-in-snapshot.ignored"), b"ignored\n").unwrap();
    fs::create_dir_all(workspace.join("ignored-dir")).unwrap();
    fs::write(workspace.join("ignored-dir/hidden.txt"), b"ignored\n").unwrap();

    let changed = create_workspace_snapshot(&state_dir, &workspace, changed_run, None, None)
        .await
        .expect("changed snapshot should succeed");
    let changed_repository = state_dir.join(&changed.repository);
    let commit = changed.commit_sha.as_str();
    assert_eq!(
        git_fixture_output(
            &changed_repository,
            &["show", &format!("{commit}:base-00.bin")]
        ),
        b"changed unstaged base\n"
    );
    assert_eq!(
        git_fixture_output(
            &changed_repository,
            &["show", &format!("{commit}:base-01.bin")]
        ),
        b"changed staged base\n"
    );
    assert_eq!(
        git_fixture_output(&changed_repository, &["show", &format!("{commit}:new.txt")]),
        b"new untracked file\n"
    );
    assert!(
        !git_fixture_output_allow_failure(
            &changed_repository,
            &["cat-file", "-e", &format!("{commit}:base-02.bin")]
        )
        .0
    );
    assert!(
        !git_fixture_output_allow_failure(
            &changed_repository,
            &[
                "cat-file",
                "-e",
                &format!("{commit}:not-in-snapshot.ignored")
            ]
        )
        .0
    );
    assert!(
        !git_fixture_output_allow_failure(
            &changed_repository,
            &[
                "cat-file",
                "-e",
                &format!("{commit}:ignored-dir/hidden.txt")
            ]
        )
        .0
    );
    assert_eq!(
        git_alternate_object_directories(&changed_repository),
        first_alternates
    );
}

#[test]
fn redirect_primary_checkout_rewrites_only_default_checkout_inputs() {
    let mut message = checkout_test_message(json!([
        {
            "id": "00000000-0000-0000-0000-000000000010",
            "name": "checkout",
            "reference": {"name": "Actions/Checkout", "version": "v4", "type": "repository"},
            "inputs": {"path": "source", "fetch-depth": "1"},
            "continueOnError": false,
            "timeoutInMinutes": null
        },
        {
            "id": "00000000-0000-0000-0000-000000000011",
            "name": "explicit checkout",
            "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
            "inputs": {
                "repository": "octo/other",
                "ref": "refs/heads/release",
                "token": "secret-token",
                "github-server-url": "https://github.example",
                "path": "other"
            },
            "continueOnError": false,
            "timeoutInMinutes": null
        },
        {
            "id": "00000000-0000-0000-0000-000000000012",
            "name": "run",
            "reference": {"name": "actions/setup-node", "version": "v4", "type": "repository"},
            "inputs": {"node-version": "22"},
            "continueOnError": false,
            "timeoutInMinutes": null
        }
    ]));
    let mut token_only = checkout_test_message(json!([{
        "id": "00000000-0000-0000-0000-000000000013",
        "name": "token-only checkout",
        "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
            "inputs": {"token": "submodule-token", "fetch-depth": "1"},
        "continueOnError": false,
        "timeoutInMinutes": null
    }]));
    let mut empty_ref = checkout_test_message(json!([{
        "id": "00000000-0000-0000-0000-000000000014",
        "name": "empty-ref checkout",
        "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
        // An expression that resolved to nothing means "default branch" —
        // the local snapshot IS the default, so the redirect must apply.
        "inputs": {"ref": "", "fetch-depth": "1"},
        "continueOnError": false,
        "timeoutInMinutes": null
    }]));
    let mut expr_ref = checkout_test_message(json!([{
        "id": "00000000-0000-0000-0000-000000000015",
        "name": "expression-ref checkout",
        "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
        // Template refs are never evaluated server-side, and one that is not
        // provably the action's declared default selects a target the
        // workflow controls at runtime. Redirecting it would hijack that
        // target once the runner evaluates the expression, so it must be
        // treated as explicitly set.
        "inputs": {"ref": "${{ inputs.head-sha }}", "fetch-depth": "0"},
        "continueOnError": false,
        "timeoutInMinutes": null
    }]));
    let original_explicit = message.steps[1].inputs.clone();
    let original_non_checkout = message.steps[2].inputs.clone();
    assert!(message.snapshot.is_none());

    let redirected = redirect_primary_checkout(
        &mut message,
        &WorkspaceSnapshot {
            head_sha: Some("f000000000000000000000000000000000000000".to_owned()),
            commit_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            tree_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            repository: "snapshots/11111111-1111-4111-8111-111111111111".to_owned(),
            default_branch: Some("main".to_owned()),
            before_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
            snapshot_timing: None,
            ..Default::default()
        },
        "http://127.0.0.1:9090",
        "local-runtime-jwt",
    );

    assert_eq!(redirected, 1);
    assert_eq!(
        message.preloop_snapshot_token_steps,
        Some(vec!["00000000-0000-0000-0000-000000000010".to_owned()]),
        "the pinned checkout step must be recorded by id so claim and retry can re-mint it"
    );
    let primary = &message.steps[0].inputs;
    assert_eq!(
        primary.get("repository"),
        Some(&"snapshots/11111111-1111-4111-8111-111111111111".to_owned())
    );
    assert_eq!(
        primary.get("ref"),
        Some(&"0123456789abcdef0123456789abcdef01234567".to_owned())
    );
    assert_eq!(
        primary.get("github-server-url"),
        Some(&"http://127.0.0.1:9090".to_owned())
    );
    // Pinned so snapshot checkout keeps working when GITHUB_TOKEN carries a
    // GitHub App installation token or PAT the snapshot endpoint cannot verify.
    assert_eq!(primary.get("token"), Some(&"local-runtime-jwt".to_owned()));
    assert_eq!(primary.get("path"), Some(&"source".to_owned()));
    assert_eq!(primary.get("fetch-depth"), Some(&"1".to_owned()));
    assert_eq!(message.steps[1].inputs, original_explicit);
    assert_eq!(message.steps[2].inputs, original_non_checkout);
    assert!(
        message.snapshot.is_none(),
        "snapshot wire field must remain untouched"
    );

    assert_eq!(
        redirect_primary_checkout(
            &mut token_only,
            &WorkspaceSnapshot {
                head_sha: Some("f000000000000000000000000000000000000000".to_owned()),
                commit_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                tree_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                repository: "snapshots/22222222-2222-4222-8222-222222222222".to_owned(),
                default_branch: Some("main".to_owned()),
                before_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
                snapshot_timing: None,
                ..Default::default()
            },
            "http://127.0.0.1:9090",
            "local-runtime-jwt",
        ),
        1,
        "a token-only primary checkout still targets the local snapshot"
    );
    assert_eq!(
        token_only.steps[0].inputs.get("token"),
        Some(&"local-runtime-jwt".to_owned())
    );
    assert_eq!(
        redirect_primary_checkout(
            &mut empty_ref,
            &WorkspaceSnapshot {
            head_sha: Some("f000000000000000000000000000000000000000".to_owned()),
                commit_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                tree_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                repository: "snapshots/33333333-3333-4333-8333-333333333333".to_owned(),
                default_branch: Some("main".to_owned()),
                before_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
                snapshot_timing: None,
                ..Default::default()
            },
            "http://127.0.0.1:9090",
            "local-runtime-jwt",
        ),
        1,
        "an empty `ref` input is GitHub's default-branch semantics and must be redirected to the snapshot"
    );
    assert_eq!(
        empty_ref.steps[0].inputs.get("ref"),
        Some(&"0123456789abcdef0123456789abcdef01234567".to_owned())
    );
    assert_eq!(
        redirect_primary_checkout(
            &mut expr_ref,
            &WorkspaceSnapshot {
                head_sha: Some("f000000000000000000000000000000000000000".to_owned()),
                commit_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                tree_sha: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                repository: "snapshots/44444444-4444-4444-8444-444444444444".to_owned(),
                default_branch: Some("main".to_owned()),
                before_sha: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
                snapshot_timing: None,
                ..Default::default()
            },
            "http://127.0.0.1:9090",
            "local-runtime-jwt",
        ),
        0,
        "a template `ref` input selects the workflow's own target and must not be redirected to the snapshot"
    );
    assert_eq!(
        expr_ref.steps[0].inputs.get("ref"),
        Some(&"${{ inputs.head-sha }}".to_owned()),
        "an expression ref must survive the redirect pass untouched"
    );
}

/// A job that sat queued past the pinned token's lifetime must get a fresh
/// credential at claim, scoped to itself, and unpinned steps must be
/// untouched.

/// A job that sat queued past the pinned token's lifetime must get a fresh
/// credential at claim, scoped to itself, and unpinned steps must be
/// untouched.
#[tokio::test]
async fn claim_remints_expired_snapshot_checkout_tokens() {
    let mut message = checkout_test_message(json!([
        {
            "id": "00000000-0000-0000-0000-000000000020",
            "name": "checkout",
            "reference": {"name": "actions/checkout", "version": "v4", "type": "repository"},
            "inputs": {"token": "expired-pinned-token", "fetch-depth": "0"},
            "continueOnError": false,
            "timeoutInMinutes": null
        },
        {
            "id": "00000000-0000-0000-0000-000000000021",
            "name": "run",
            "reference": {"name": "actions/setup-node", "version": "v4", "type": "repository"},
            "inputs": {"node-version": "22"},
            "continueOnError": false,
            "timeoutInMinutes": null
        }
    ]));
    message.preloop_snapshot_token_steps =
        Some(vec!["00000000-0000-0000-0000-000000000020".to_owned()]);

    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();

    let refreshed = crate::broker::re_mint_snapshot_tokens(&mut message, &state);
    assert_eq!(refreshed, 1);

    let token = message.steps[0].inputs.get("token").unwrap();
    assert_ne!(token, "expired-pinned-token");
    let claims = state
        .verify_local_jwt_claims(token)
        .expect("re-minted token must verify");
    assert_eq!(
        claims["sub"],
        format!("preloop-job-{}", message.job_id),
        "the fresh token must be scoped to this job"
    );
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        claims["exp"].as_u64().unwrap() > now,
        "the re-minted token must not be already expired"
    );
    assert_eq!(
        message.steps[1].inputs.get("token"),
        None,
        "unpinned steps keep their inputs untouched"
    );

    // Without the pinned-step marker nothing is refreshed.
    message.preloop_snapshot_token_steps = None;
    assert_eq!(
        crate::broker::re_mint_snapshot_tokens(&mut message, &state),
        0
    );
}

/// The claim-time re-mint must actually run on the real claim path: a queued
/// redirected checkout carries the submission-time pinned token, and the job
/// the runner acquires must carry a freshly minted one.

/// The claim-time re-mint must actually run on the real claim path: a queued
/// redirected checkout carries the submission-time pinned token, and the job
/// the runner acquires must carry a freshly minted one.
#[tokio::test]
async fn claim_remints_snapshot_tokens_on_the_real_claim_path() {
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());
    let mut state = AppState::new(state_dir.clone()).await.unwrap();
    state.local_workspace = Some(workspace.clone());
    let app = app(state.clone(), CancellationToken::new());

    submit_yaml(
        &app,
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#,
        "owner/repo",
    )
    .await;
    // The re-mint produces a fresh JWT; within the same second it is
    // byte-identical to the pinned one, so give the clock room to move.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    // The pinned submission-time token, as it sits on the queued message.
    let pinned_token = {
        let inner = state.inner.lock().await;
        let queued = inner.queue.front().expect("job should be queued");
        let checkout = queued
            .message
            .steps
            .iter()
            .find(|step| {
                step.reference
                    .as_ref()
                    .and_then(|reference| reference.name.as_deref())
                    .is_some_and(|name| name.eq_ignore_ascii_case("actions/checkout"))
            })
            .expect("queued job should contain the redirected checkout step");
        checkout.inputs.get("token").cloned().expect("pinned token")
    };
    assert!(
        state.verify_local_jwt_claims(&pinned_token).is_some(),
        "the queued token must be a valid local JWT"
    );

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": 1, "name": "remint-runner"},
            "ownerName": "remint test",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let broker_message = request_json(
        &app,
        Method::GET,
        &format!(
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
        ),
        Value::Null,
    )
    .await;
    let broker_body: Value =
        serde_json::from_str(broker_message["body"].as_str().unwrap()).unwrap();
    let runner_request_id = broker_body["runner_request_id"]
        .as_str()
        .expect("broker message should identify the queued request");
    let (_runner_id, runner_token) =
        register_runner_with_token(&app, "remint-runner", &["self-hosted"], None).await;

    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/acquirejob",
        json!({
            "jobMessageId": runner_request_id,
            "billingOwnerId": "local",
            "runnerOS": "linux"
        }),
        &runner_token,
    )
    .await;

    let checkout = acquired["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["reference"]["name"].as_str() == Some("actions/checkout"))
        .expect("the acquired job should contain the checkout step");
    fn acquired_input<'a>(step: &'a Value, name: &str) -> Option<&'a str> {
        step["inputs"]
            .get(name)
            .and_then(Value::as_str)
            .or_else(|| {
                let found = step["inputs"]["map"].as_array()?.iter().find(|entry| {
                    entry
                        .get("Key")
                        .or_else(|| entry.get("key"))
                        .and_then(|key| key.get("lit"))
                        .and_then(Value::as_str)
                        .is_some_and(|key| key == name)
                })?;
                found
                    .get("Value")
                    .or_else(|| found.get("value"))
                    .and_then(|value| value.get("lit"))
                    .and_then(Value::as_str)
            })
    }
    let claimed_token = acquired_input(checkout, "token").expect("claimed pinned token");
    assert_ne!(
        claimed_token, pinned_token,
        "claim must replace the submission-time token with a fresh one"
    );
    let claims = state
        .verify_local_jwt_claims(claimed_token)
        .expect("the claimed token must verify as a local JWT");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        claims["exp"].as_u64().unwrap() > now,
        "the claimed token must not be expired"
    );
}

/// A retry verdict must carry a freshly minted snapshot credential: the
/// worker replays the failed step from the message it already holds, whose
/// pinned token may be long expired.

/// A retry verdict must carry a freshly minted snapshot credential: the
/// worker replays the failed step from the message it already holds, whose
/// pinned token may be long expired.
#[tokio::test]
async fn retry_verdict_carries_a_fresh_snapshot_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: \"false\"\n",
            "event": "push",
            "repository": "owner/repo",
            "preserve_on_failure": true
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    request_json(
        &app,
        Method::GET,
        "/runner/server/_apis/v1/Message/1?sessionId=default",
        Value::Null,
    )
    .await;

    let (agent_job_id, worker_token) = {
        let inner = state.inner.lock().await;
        let record = inner.job_requests.iter().next().unwrap().1;
        (
            record.agent_job_id,
            state.mint_debug_worker_token(&record.plan_id, &record.agent_job_id),
        )
    };

    let opened = request_json_with_bearer(
        &app,
        Method::POST,
        "/api/v1/debug/sessions",
        json!({
            "run_id": run_id,
            "job_id": "build",
            "agent_job_id": agent_job_id,
            "job_name": "build",
            "step": {
                "index": 0,
                "total": 1,
                "context_name": "__run",
                "display_name": "Run false",
                "command": "false",
                "exit_code": 1,
                "elapsed_ms": 20,
                "diagnostics": []
            }
        }),
        &worker_token,
    )
    .await;
    let session_id = opened["session_id"].as_str().unwrap().to_owned();

    request_json(
        &app,
        Method::POST,
        &format!("/api/v1/debug/sessions/{session_id}/verdict"),
        json!({ "verdict": "retry", "controller": "test" }),
    )
    .await;

    let polled = request_json_with_bearer(
        &app,
        Method::GET,
        &format!("/api/v1/debug/sessions/{session_id}/verdict?wait=0"),
        Value::Null,
        &worker_token,
    )
    .await;
    assert_eq!(polled["verdict"], "retry");
    let token = polled["snapshot_token"]
        .as_str()
        .expect("retry verdict must carry a fresh snapshot credential");
    let claims = state
        .verify_local_jwt_claims(token)
        .expect("verdict-supplied token must verify");
    assert_eq!(claims["sub"], format!("preloop-job-{agent_job_id}"));
}

/// The snapshot surface must reject bad credentials with a Bearer challenge:
/// a bare 401 makes git fall back to Basic semantics and prompt for a
/// username no job can answer.

/// The snapshot surface must reject bad credentials with a Bearer challenge:
/// a bare 401 makes git fall back to Basic semantics and prompt for a
/// username no job can answer.
#[tokio::test]
async fn snapshot_401_advertises_a_bearer_challenge() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(
                    "/snapshots/00000000-0000-0000-0000-000000000001/info/refs?service=git-upload-pack",
                )
                .header(header::AUTHORIZATION, "Bearer not-a-real-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok()),
        Some("Bearer realm=\"preloop-snapshot\"")
    );
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error"], "invalid snapshot Git token");
}

#[tokio::test]
async fn local_workspace_checkout_acquires_synthetic_repository_and_serves_git_http() {
    // `AppState::new` captures `PRELOOP_GITHUB_TOKEN` / `PRELOOP_GITHUB_APP_*`
    // from the process env, and the claim path reads `PRELOOP_GITHUB_API_URL`
    // live. Hold the env lock so a concurrent env-mutating test cannot make
    // this job carry a foreign PAT (which would 401 the snapshot Git auth
    // instead of 403 on the wrong-run probe).
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let (state_dir, workspace) = create_snapshot_fixture(temp.path());
    let mut state = AppState::new(state_dir.clone()).await.unwrap();
    state.local_workspace = Some(workspace.clone());
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#,
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    // `github.sha` must be the workspace's real HEAD commit, not the
    // synthetic snapshot commit: a workflow step that fetches
    // `${{ github.sha }}` from the real remote (custom checkouts) must
    // receive a sha the upstream host can actually resolve, and the
    // snapshot commit exists only in this engine's store.
    let inner = state.inner.lock().await;
    let run = inner.runs.get(&run_id).unwrap();
    let context_sha = run.github["sha"].as_str().unwrap().to_owned();
    let snapshot_sha = run.workspace_snapshot.as_ref().unwrap().commit_sha.clone();
    let workspace_head =
        String::from_utf8(git_fixture_output(&workspace, &["rev-parse", "HEAD"])).unwrap();
    drop(inner);
    assert_eq!(
        context_sha,
        workspace_head.trim(),
        "github.sha must be the real workspace HEAD"
    );
    assert_ne!(
        context_sha, snapshot_sha,
        "the synthetic snapshot sha must not leak into the github context"
    );

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": 1, "name": "snapshot-runner"},
            "ownerName": "snapshot test",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let broker_message = request_json(
        &app,
        Method::GET,
        &format!(
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
        ),
        Value::Null,
    )
    .await;
    let broker_body: Value = serde_json::from_str(broker_message["body"].as_str().unwrap())
        .expect("broker message body should be JSON");
    let runner_request_id = broker_body["runner_request_id"]
        .as_str()
        .expect("broker message should identify the queued request");
    let (_runner_id, runner_token) =
        register_runner_with_token(&app, "snapshot-runner", &["self-hosted"], None).await;
    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/acquirejob",
        json!({
            "jobMessageId": runner_request_id,
            "billingOwnerId": "local",
            "runnerOS": "linux"
        }),
        &runner_token,
    )
    .await;

    let checkout = acquired["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["reference"]["name"].as_str() == Some("actions/checkout"))
        .expect("the acquired job should contain the checkout step");
    fn checkout_input<'a>(step: &'a Value, name: &str) -> Option<&'a str> {
        step["inputs"]
            .get(name)
            .and_then(Value::as_str)
            .or_else(|| {
                step["inputs"]["map"]
                    .as_array()?
                    .iter()
                    .find(|entry| {
                        entry
                            .get("Key")
                            .or_else(|| entry.get("key"))
                            .and_then(|key| key.get("lit"))
                            .and_then(Value::as_str)
                            == Some(name)
                    })
                    .and_then(|entry| entry.get("Value").or_else(|| entry.get("value")))
                    .and_then(|value| value.get("lit"))
                    .and_then(Value::as_str)
            })
    }
    let repository = checkout_input(checkout, "repository")
        .unwrap_or_else(|| panic!("checkout repository should be rewritten: {checkout}"));
    let commit = checkout_input(checkout, "ref").expect("checkout ref should be rewritten");
    let server_url = checkout_input(checkout, "github-server-url")
        .expect("checkout server URL should be rewritten");
    assert_eq!(repository, format!("snapshots/{run_id}"));
    assert_eq!(server_url, public_base_url());
    assert_eq!(commit.len(), 40);
    assert!(commit.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(acquired["snapshot"], Value::Null);

    let runtime_token = acquired["variables"]["system.github.token"]["value"]
        .as_str()
        .expect("acquired job should expose its runtime token");
    let unauthenticated = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/snapshots/{run_id}/info/refs?service=git-upload-pack"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let wrong_run = "22222222-2222-4222-8222-222222222222";
    let wrong_binding = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/snapshots/{wrong_run}/info/refs?service=git-upload-pack"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_binding.status(), StatusCode::FORBIDDEN);

    let advertisement = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/snapshots/{run_id}/info/refs?service=git-upload-pack"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(advertisement.status(), StatusCode::OK);
    let advertisement_body = to_bytes(advertisement.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(advertisement_body
        .windows(commit.len())
        .any(|window| window == commit.as_bytes()));

    fn pkt_line(payload: &[u8]) -> Vec<u8> {
        let mut line = format!("{:04x}", payload.len() + 4).into_bytes();
        line.extend_from_slice(payload);
        line
    }
    let want = format!("want {commit} multi_ack_detailed side-band-64k thin-pack ofs-delta\n");
    let mut upload_request = pkt_line(want.as_bytes());
    upload_request.extend_from_slice(b"0000");
    upload_request.extend_from_slice(&pkt_line(b"done\n"));
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("/snapshots/{run_id}/git-upload-pack"))
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .header(
                    header::CONTENT_TYPE,
                    "application/x-git-upload-pack-request",
                )
                .body(Body::from(upload_request))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::OK);
    let upload_body = to_bytes(upload.into_body(), usize::MAX).await.unwrap();

    // Decode side-band channel 1 from the actual git-http-backend response,
    // then let Git validate the fetched pack's checksum and object framing.
    let mut pack = Vec::new();
    let mut offset = 0;
    while offset + 4 <= upload_body.len() {
        let length = usize::from_str_radix(
            std::str::from_utf8(&upload_body[offset..offset + 4]).unwrap(),
            16,
        )
        .unwrap();
        offset += 4;
        if length == 0 {
            continue;
        }
        let payload_len = length - 4;
        assert!(offset + payload_len <= upload_body.len());
        let payload = &upload_body[offset..offset + payload_len];
        if payload.first() == Some(&1) {
            pack.extend_from_slice(&payload[1..]);
        } else if payload.starts_with(b"PACK") {
            pack.extend_from_slice(payload);
        }
        offset += payload_len;
    }
    assert!(
        pack.starts_with(b"PACK"),
        "upload-pack response should contain a pack"
    );
    let mut index_pack = Command::new("git")
        .args(["index-pack", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    index_pack.stdin.take().unwrap().write_all(&pack).unwrap();
    let index_result = index_pack.wait_with_output().unwrap();
    assert!(
        index_result.status.success(),
        "Git rejected the route's fetched pack: {}",
        String::from_utf8_lossy(&index_result.stderr)
    );

    for filter in ["blob:none", "tree:0", "combine:blob:none+tree:0"] {
        let mut protocol_v2_request = pkt_line(b"command=fetch\n");
        protocol_v2_request.extend_from_slice(&pkt_line(b"agent=git/2.43.0\n"));
        protocol_v2_request.extend_from_slice(&pkt_line(b"object-format=sha1\n"));
        protocol_v2_request.extend_from_slice(b"0001");
        protocol_v2_request.extend_from_slice(&pkt_line(format!("want {commit}\n").as_bytes()));
        protocol_v2_request.extend_from_slice(&pkt_line(format!("filter {filter}\n").as_bytes()));
        protocol_v2_request.extend_from_slice(&pkt_line(b"done\n"));
        protocol_v2_request.extend_from_slice(b"0000");
        let protocol_v2_upload = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/snapshots/{run_id}/git-upload-pack"))
                    .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                    .header(
                        header::CONTENT_TYPE,
                        "application/x-git-upload-pack-request",
                    )
                    .header("Git-Protocol", "version=2")
                    .body(Body::from(protocol_v2_request))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(protocol_v2_upload.status(), StatusCode::OK);
        let protocol_v2_body = to_bytes(protocol_v2_upload.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(
            protocol_v2_body.starts_with(b"0008NAK\n")
                || protocol_v2_body.windows(4).any(|window| window == b"PACK"),
            "protocol v2 {filter} response should remain pkt-line or pack framed: {:?}",
            &protocol_v2_body[..protocol_v2_body.len().min(128)]
        );
    }

    let bare_repository = state_dir.join(repository);
    assert!(
        git_fixture_output_allow_failure(
            &bare_repository,
            &["cat-file", "-e", &format!("{commit}^{{commit}}")]
        )
        .0
    );
    assert_eq!(
        git_fixture_output(
            &bare_repository,
            &["show", &format!("{commit}:tracked.txt")]
        ),
        b"tracked unstaged change\n"
    );
}

/// With `checkout_cache.mode = "run-scoped"`, a webhook-shaped run fetches its
/// commit once and every job in that run checks it out from the engine with its
/// own Actions runtime token. A token bound to a different run must not reach
/// those objects.

/// With `checkout_cache.mode = "run-scoped"`, a webhook-shaped run fetches its
/// commit once and every job in that run checks it out from the engine with its
/// own Actions runtime token. A token bound to a different run must not reach
/// those objects.
#[tokio::test]
async fn run_scoped_checkout_cache_serves_the_run_commit_to_its_job_token() {
    let _env = crate::state::GITHUB_ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let upstream_root = temp.path().join("upstream");
    let work = upstream_root.join("work");
    fs::create_dir_all(&work).unwrap();
    git_fixture_command(&work, &["init", "-b", "main"]);
    fs::write(work.join("tracked.txt"), "cached upstream\n").unwrap();
    git_fixture_command(&work, &["add", "."]);
    git_fixture_command(&work, &["commit", "-m", "upstream commit"]);
    let bare = upstream_root.join("owner/repo.git");
    fs::create_dir_all(bare.parent().unwrap()).unwrap();
    git_fixture_command(
        &upstream_root,
        &[
            "clone",
            "--bare",
            work.to_str().unwrap(),
            bare.to_str().unwrap(),
        ],
    );
    let commit = String::from_utf8(git_fixture_output(&work, &["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned();

    let mut state = AppState::new(state_dir.clone()).await.unwrap();
    state.checkout_cache = crate::config::CheckoutCacheConfig {
        mode: crate::config::CheckoutCacheMode::RunScoped,
        ..Default::default()
    };
    state.github_urls.server_url = upstream_root.to_string_lossy().to_string();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n",
            "event": "push",
            "repository": "owner/repo",
            "payload": {
                "after": commit,
                "ref": "refs/heads/main",
                "repository": {
                    "id": 4242,
                    "private": false,
                    "default_branch": "main"
                }
            }
        }),
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();

    let session = request_json(
        &app,
        Method::POST,
        "/runner/server/_apis/distributedtask/pools/1/sessions",
        json!({
            "agent": {"id": 1, "name": "cache-runner"},
            "ownerName": "checkout cache test",
            "sessionId": "00000000-0000-0000-0000-000000000000",
            "useFipsEncryption": false
        }),
    )
    .await;
    let session_id = session["sessionId"].as_str().unwrap();
    let broker_message = request_json(
        &app,
        Method::GET,
        &format!(
            "/runner/server/_apis/distributedtask/pools/1/messages?sessionId={session_id}&waitSeconds=0"
        ),
        Value::Null,
    )
    .await;
    let broker_body: Value =
        serde_json::from_str(broker_message["body"].as_str().unwrap()).unwrap();
    let runner_request_id = broker_body["runner_request_id"].as_str().unwrap();
    let (_runner_id, runner_token) =
        register_runner_with_token(&app, "cache-runner", &["self-hosted"], None).await;
    let acquired = request_json_with_bearer(
        &app,
        Method::POST,
        "/broker/1/acquirejob",
        json!({
            "jobMessageId": runner_request_id,
            "billingOwnerId": "local",
            "runnerOS": "linux"
        }),
        &runner_token,
    )
    .await;

    // The default checkout is redirected at the engine, and the commit it asks
    // for is the run's own immutable sha — not a rewritten synthetic one.
    let checkout = acquired["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["reference"]["name"].as_str() == Some("actions/checkout"))
        .expect("the acquired job carries a checkout step");
    fn cached_checkout_input<'a>(step: &'a Value, name: &str) -> Option<&'a str> {
        step["inputs"]
            .get(name)
            .and_then(Value::as_str)
            .or_else(|| {
                step["inputs"]["map"]
                    .as_array()?
                    .iter()
                    .find(|entry| {
                        entry
                            .get("Key")
                            .or_else(|| entry.get("key"))
                            .and_then(|key| key.get("lit"))
                            .and_then(Value::as_str)
                            == Some(name)
                    })
                    .and_then(|entry| entry.get("Value").or_else(|| entry.get("value")))
                    .and_then(|value| value.get("lit"))
                    .and_then(Value::as_str)
            })
    }
    let inputs =
        |name: &str| -> Option<String> { cached_checkout_input(checkout, name).map(str::to_owned) };
    assert_eq!(inputs("ref").as_deref(), Some(commit.as_str()));
    assert_eq!(inputs("repository"), Some(format!("snapshots/{run_id}")));
    assert_eq!(inputs("github-server-url"), Some(public_base_url()));
    // The forge origin is left alone: the commit exists upstream, and the cache
    // holds only that commit.
    assert_eq!(acquired["snapshotOriginRewrite"], Value::Null);

    let runtime_token = acquired["variables"]["system.github.token"]["value"]
        .as_str()
        .expect("the job exposes its runtime token");
    let advertisement = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/snapshots/{run_id}/info/refs?service=git-upload-pack"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(advertisement.status(), StatusCode::OK);
    // A real git client only proceeds on the upload-pack advertisement. A
    // missing query passthrough would answer the dumb text/plain listing
    // instead, which still names the commit but no client can fetch from.
    assert_eq!(
        advertisement
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-git-upload-pack-advertisement"),
        "the engine must answer smart-HTTP discovery, not the dumb listing"
    );
    let advertised = to_bytes(advertisement.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        advertised
            .windows(commit.len())
            .any(|window| window == commit.as_bytes()),
        "the cached commit must be fetchable by the job"
    );

    let foreign_run = "99999999-9999-4999-8999-999999999999";
    let foreign = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!(
                    "/snapshots/{foreign_run}/info/refs?service=git-upload-pack"
                ))
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(foreign.status(), StatusCode::FORBIDDEN);

    // The objects live in the checkout cache, not in a per-run copy of the
    // repository.
    assert!(state_dir
        .join(format!("checkout-cache/runs/{run_id}.git"))
        .is_dir());
    assert!(!state_dir
        .join("snapshots")
        .join(run_id.to_string())
        .exists());
}

/// Uploaded job logs must stay bounded, and pruning must not cost a run its
/// logs: `get_run_logs` prefers the blob and falls back to the in-memory
/// blocks, so an evicted plan degrades instead of disappearing.

/// Uploaded job logs must stay bounded, and pruning must not cost a run its
/// logs: `get_run_logs` prefers the blob and falls back to the in-memory
/// blocks, so an evicted plan degrades instead of disappearing.
#[tokio::test]
async fn replay_results_are_pruned_to_the_retention_window() {
    let temp = tempfile::tempdir().unwrap();
    let results = temp.path().join("replay").join("results");

    // One directory per execution plan, oldest first so mtime ordering is
    // unambiguous rather than dependent on filesystem timestamp resolution.
    let total = crate::blob_store::REPLAY_PLANS_RETAINED + 8;
    let mut plans = Vec::new();
    for index in 0..total {
        let plan = results.join(format!("plan-{index:03}"));
        std::fs::create_dir_all(&plan).unwrap();
        std::fs::write(plan.join("job-logs.txt"), format!("log {index}")).unwrap();
        filetime::set_file_mtime(
            &plan,
            filetime::FileTime::from_unix_time(1_700_000_000 + index as i64, 0),
        )
        .unwrap();
        plans.push(plan);
    }

    crate::blob_store::prune_replay_results(temp.path(), &std::collections::BTreeSet::new()).await;

    let surviving: Vec<_> = plans.iter().filter(|plan| plan.exists()).collect();
    assert_eq!(
        surviving.len(),
        crate::blob_store::REPLAY_PLANS_RETAINED,
        "retention window must bound the directory"
    );
    assert!(
        plans[total - 1].exists(),
        "the most recent plan must survive"
    );
    assert!(!plans[0].exists(), "the oldest plan must be evicted");
    assert_eq!(
        std::fs::read_to_string(plans[total - 1].join("job-logs.txt")).unwrap(),
        format!("log {}", total - 1),
        "surviving logs must be intact"
    );
}

#[tokio::test]
async fn replay_result_pruning_preserves_active_plans() {
    let temp = tempfile::tempdir().unwrap();
    let results = temp.path().join("replay").join("results");
    for index in 0..=crate::blob_store::REPLAY_PLANS_RETAINED {
        let plan = results.join(format!("plan-{index:03}"));
        std::fs::create_dir_all(&plan).unwrap();
        filetime::set_file_mtime(
            &plan,
            filetime::FileTime::from_unix_time(1_700_000_000 + index as i64, 0),
        )
        .unwrap();
    }
    let active = BTreeSet::from(["plan-000".to_owned()]);

    crate::blob_store::prune_replay_results(temp.path(), &active).await;

    assert!(results.join("plan-000").exists());
}

#[tokio::test]
async fn pruning_replay_results_is_a_no_op_without_a_replay_directory() {
    let temp = tempfile::tempdir().unwrap();
    crate::blob_store::prune_replay_results(temp.path(), &std::collections::BTreeSet::new()).await;
    assert!(!temp.path().join("replay").exists());
}

// ─── Guest-side impersonation hardening ──────────────────────────────────
//
// The pool's control socket is reachable from untrusted workflow code inside
// every runner VM, and the runner identity material on the guest disk is
// readable by any step. These tests pin the server-side contract that makes
// that exposure non-transitive: a guest can act as its own machine's runner
// (self-disclosure, same as GitHub-hosted runners) but can never pull a job
// assigned to another machine, even with a fully stolen runner identity.

/// Register a runner through the AzDO compat path and mint its listen token
/// through the mock OAuth flow with the clientId the server assigned.
#[tokio::test]
async fn pull_request_submission_uses_head_sha_not_zeros() {
    // A pull_request payload has no `after`, and a submission that does not
    // pre-resolve a sha used to fall through to all-zeros. The job then asks
    // its remote for `0000…` and dies with "not our ref 0000…", which points
    // nowhere near the real cause.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: pull_request\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
            "event": "pull_request",
            "repository": "owner/repo",
            "payload": {
                "action": "opened",
                "number": 7,
                "pull_request": {
                    "head": { "ref": "feature", "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3" },
                    "base": { "ref": "main", "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2" }
                }
            }
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 1);

    let inner = state.inner.lock().await;
    let (_, run_record) = inner.runs.iter().next().unwrap();
    assert_eq!(
        run_record.head_sha, "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3",
        "pull_request head sha must drive github.sha instead of the zero sha"
    );
}

#[tokio::test]
async fn pull_request_submission_uses_short_ref_name_and_job_id() {
    // GitHub presents PR events with `github.ref = refs/pull/<n>/merge` and
    // `github.ref_name = <n>/merge` (short form), and supplies the job id via
    // the `system.github.job` variable. Previously `ref_name` leaked the full
    // `refs/pull/7/merge` and `github.job`/`GITHUB_JOB` were empty.
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = request_json(
        &app,
        Method::POST,
        "/api/v1/runs",
        json!({
            "workflow_yaml": "on: pull_request\njobs:\n  build:\n    runs-on: self-hosted\n    strategy:\n      matrix:\n        os: [a, b]\n    steps:\n      - run: echo hi\n",
            "event": "pull_request",
            "repository": "owner/repo",
            "payload": {
                "action": "opened",
                "number": 7,
                "pull_request": {
                    "head": { "ref": "feature", "sha": "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3" },
                    "base": { "ref": "main", "sha": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3" }
                }
            }
        }),
    )
    .await;
    assert_eq!(accepted["queued_jobs"], 2);

    let inner = state.inner.lock().await;
    let queued: Vec<_> = inner.queue.iter().collect();
    assert_eq!(queued.len(), 2);
    let github = &queued[0].message.context_data["github"].to_json();
    assert_eq!(github["ref"], "refs/pull/7/merge");
    assert_eq!(github["ref_name"], "7/merge");
    assert_eq!(github["ref_type"], "branch");
    assert_eq!(github["head_ref"], "feature");
    assert_eq!(github["base_ref"], "main");
    assert_eq!(
        queued[0].message.variables["system.github.job"]
            .value
            .as_deref(),
        Some("build")
    );
    // GitHub's context carries no `job` key — the runner reads the variable.
    assert!(github.get("job").is_none());

    // Both matrix cells carry the same job id; strategy indices are per-cell.
    for request in &queued {
        assert_eq!(
            request.message.variables["system.github.job"]
                .value
                .as_deref(),
            Some("build")
        );
    }
    let strategy = queued[0].message.context_data["strategy"].to_json();
    assert_eq!(strategy["job-index"], 0.0);
    assert_eq!(strategy["job-total"], 2.0);
    let strategy1 = queued[1].message.context_data["strategy"].to_json();
    assert_eq!(strategy1["job-index"], 1.0);
}

#[tokio::test]
async fn stolen_identity_cannot_pull_another_machines_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    // Machine A is provisioned for the queued job: host-side the pool staged
    // one provision token, and the guest's configure presents it.
    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    {
        let inner = state.inner.lock().await;
        assert_eq!(inner.pool_pending.len(), 1, "job waits for its machine");
        assert!(inner.job_assignments.is_empty());
    }
    stage_provision_token(&state, "token-a");
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner
                .job_assignments
                .values()
                .next()
                .and_then(|r| r.runner_id),
            Some(runner_a),
            "registration pairing bound the job to machine A"
        );
    }

    // A second machine + a rogue process on it: a valid identity for another
    // runner, plus a raw bearer-free session impersonating runner A.
    stage_provision_token(&state, "token-b");
    let (runner_b, token_b) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    let (_, session_b) = create_disttask_session(&app, &token_b, runner_b).await;
    let session_b_id = session_b["sessionId"].as_str().unwrap();
    let stolen = poll_message(&app, &token_b, session_b_id).await;
    assert!(
        stolen.is_null(),
        "other runner's identity must not receive the assigned job: {stolen}"
    );

    // Bearer-free impersonation (the test harness attaches the system token,
    // which is not a runner token — exactly what untrusted guest code has).
    let (_, rogue_session) = create_disttask_session(&app, "preloop-system-token", runner_a).await;
    let rogue_id = rogue_session["sessionId"].as_str().unwrap();
    let stolen = poll_message(&app, "preloop-system-token", rogue_id).await;
    assert!(
        stolen.is_null(),
        "unverified session impersonating runner A must not claim: {stolen}"
    );

    // The legitimately paired machine's runner receives its job.
    let (_, session_a) = create_disttask_session(&app, &token_a, runner_a).await;
    let session_a_id = session_a["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, &token_a, session_a_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "paired runner must receive its assigned job: {delivered}"
    );
}

#[tokio::test]
async fn failing_provisioning_cannot_starve_a_healthy_runner() {
    // A pool that keeps provisioning machines and losing them (broken image,
    // host that cannot start VMs) re-binds the job to each short-lived
    // machine. Every rebind refreshed the binding window, so an established,
    // capable, verified runner was refused forever — observed as "jobs
    // queued, never promoted" behind a doomed on-demand loop.
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    // A healthy runner is registered and polling before the job lands.
    stage_provision_token(&state, "token-healthy");
    let (healthy_id, healthy_token) = register_runner_with_token(
        &app,
        "healthy-host",
        &["self-hosted"],
        Some("token-healthy"),
    )
    .await;
    let (_, healthy_session) = create_disttask_session(&app, &healthy_token, healthy_id).await;
    let healthy_session_id = healthy_session["sessionId"].as_str().unwrap().to_owned();

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);

    // Phantom machines: each registers, takes the pairing, then dies without
    // claiming. Age the binding between rounds so every phantom adopts a
    // "stale" pairing exactly as the real churn does.
    for round in 0..3 {
        {
            let mut inner = state.inner.lock().await;
            let keys: Vec<_> = inner.job_assignments.keys().cloned().collect();
            for key in keys {
                if let Some(record) = inner.job_assignments.get_mut(&key) {
                    record.at = std::time::SystemTime::now()
                        - crate::runtime_scheduling::CLAIM_BINDING_TTL
                        - std::time::Duration::from_secs(1);
                    record.first_at = record.at;
                }
            }
        }
        let token_name = format!("token-phantom-{round}");
        stage_provision_token(&state, &token_name);
        let (phantom_id, _) = register_runner_with_token(
            &app,
            &format!("phantom-{round}"),
            &["self-hosted"],
            Some(&token_name),
        )
        .await;
        // The machine dies before claiming anything.
        let shared = std::sync::Arc::new(crate::state::SharedState {
            state: state.clone(),
            shutdown: CancellationToken::new(),
        });
        crate::runner_lifecycle::purge_runner_identity(&shared, phantom_id).await;
    }

    // The established runner must now be able to claim: the job has been
    // bound-and-abandoned for longer than the binding window.
    {
        let mut inner = state.inner.lock().await;
        let keys: Vec<_> = inner.job_assignments.keys().cloned().collect();
        for key in keys {
            if let Some(record) = inner.job_assignments.get_mut(&key) {
                record.first_at = std::time::SystemTime::now()
                    - crate::runtime_scheduling::CLAIM_BINDING_TTL
                    - std::time::Duration::from_secs(1);
            }
        }
        let pending: Vec<_> = inner.pool_pending.keys().cloned().collect();
        for key in pending {
            inner.pool_pending.insert(
                key,
                std::time::SystemTime::now()
                    - crate::runtime_scheduling::CLAIM_BINDING_TTL
                    - std::time::Duration::from_secs(1),
            );
        }
    }

    let delivered = poll_message(&app, &healthy_token, &healthy_session_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "established runner must rescue a job abandoned by churning machines: {delivered}"
    );
}

#[tokio::test]
async fn rebinding_churn_cannot_starve_an_established_runner() {
    // Machines that register and then go silent without claiming keep
    // adopting the stale pairing, and each adoption refreshed the binding
    // window. An established, capable, verified runner was refused for as
    // long as the churn continued.
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);

    // The established runner registers after the job queued, so the job is
    // pool-pending and it is not the paired machine.
    stage_provision_token(&state, "token-established");
    let (established_id, established_token) = register_runner_with_token(
        &app,
        "established-host",
        &["self-hosted"],
        Some("token-established"),
    )
    .await;
    let (_, established_session) =
        create_disttask_session(&app, &established_token, established_id).await;
    let established_session_id = established_session["sessionId"]
        .as_str()
        .unwrap()
        .to_owned();

    // Churn: each new machine adopts the pairing once the previous one's
    // binding looks stale, refreshing `at` every round.
    for round in 0..3 {
        {
            let mut inner = state.inner.lock().await;
            let keys: Vec<_> = inner.job_assignments.keys().cloned().collect();
            for key in keys {
                if let Some(record) = inner.job_assignments.get_mut(&key) {
                    record.at = std::time::SystemTime::now()
                        - crate::runtime_scheduling::CLAIM_BINDING_TTL
                        - std::time::Duration::from_secs(1);
                }
            }
        }
        let token_name = format!("token-churn-{round}");
        stage_provision_token(&state, &token_name);
        register_runner_with_token(
            &app,
            &format!("churn-{round}"),
            &["self-hosted"],
            Some(&token_name),
        )
        .await;
    }

    // Every round refreshed `at`, so the binding still looks fresh — but the
    // job has been bound-and-unclaimed since the first round.
    {
        let mut inner = state.inner.lock().await;
        let keys: Vec<_> = inner.job_assignments.keys().cloned().collect();
        assert!(!keys.is_empty(), "churn must leave the job bound");
        for key in keys {
            if let Some(record) = inner.job_assignments.get_mut(&key) {
                assert!(
                    record.runner_id != Some(established_id),
                    "churned machine, not the established runner, holds the pairing"
                );
                record.first_at = std::time::SystemTime::now()
                    - crate::runtime_scheduling::CLAIM_BINDING_TTL
                    - std::time::Duration::from_secs(1);
                record.at = std::time::SystemTime::now();
            }
        }
    }

    let delivered = poll_message(&app, &established_token, &established_session_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "established runner must rescue a job held by churning machines: {delivered}"
    );
}

#[tokio::test]
async fn stale_binding_requeues_behind_newer_waits() {
    // A job whose machine died before claiming must not monopolize the pool:
    // its stale binding is released and the job re-enters the waitlist at the
    // *back*, so a job that has been waiting with no machine gets the next
    // one. The released job is still served afterwards — nothing is dropped.
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    // Job A is paired to machine-a, which dies before claiming.
    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    stage_provision_token(&state, "token-a");
    let (runner_a, _) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let key_a = {
        let inner = state.inner.lock().await;
        let key = inner.job_assignments.keys().next().cloned().unwrap();
        assert_eq!(
            inner.job_assignments.get(&key).and_then(|r| r.runner_id),
            Some(runner_a),
            "registration pairing bound job A to machine-a"
        );
        key
    };

    // Job B arrives afterwards and waits for its machine (machine-a has no
    // session, so queue-time binding cannot take it).
    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    let key_b = {
        let inner = state.inner.lock().await;
        let key = inner.pool_pending.keys().next().cloned().unwrap();
        assert!(key.0 != key_a.0, "second submission is a distinct run");
        key
    };

    // Age machine-a's binding past the claim window, then machine-b
    // registers: the stale binding is released, and the earlier wait (job B)
    // is paired — not the dying job re-adopted with priority.
    {
        let mut inner = state.inner.lock().await;
        let keys: Vec<_> = inner.job_assignments.keys().cloned().collect();
        for key in keys {
            if let Some(record) = inner.job_assignments.get_mut(&key) {
                record.at = std::time::SystemTime::now()
                    - crate::runtime_scheduling::CLAIM_BINDING_TTL
                    - std::time::Duration::from_secs(1);
                record.first_at = record.at;
            }
        }
    }
    let key_a_first_at = {
        let inner = state.inner.lock().await;
        inner
            .job_assignments
            .get(&key_a)
            .map(|r| r.first_at)
            .unwrap()
    };
    stage_provision_token(&state, "token-b");
    let (runner_b, _) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.job_assignments.len(),
            2,
            "the stale record is kept (its first_at rides the requeue), the newer wait still gets the machine"
        );
        assert_eq!(
            inner.job_assignments.get(&key_b).and_then(|r| r.runner_id),
            Some(runner_b),
            "the newer wait gets the machine before the re-queued dying job"
        );
        assert!(
            inner.pool_pending.contains_key(&key_a),
            "released job is back in the waitlist, not dropped"
        );
        assert_eq!(
            inner.job_assignments.get(&key_a).map(|r| r.first_at),
            Some(key_a_first_at),
            "the requeue must not reset the bounded claim window"
        );
    }

    // The released job is still served by the next machine: it re-entered at
    // the back, not into invisibility.
    stage_provision_token(&state, "token-c");
    let (runner_c, _) =
        register_runner_with_token(&app, "machine-c", &["self-hosted"], Some("token-c")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner.job_assignments.get(&key_a).and_then(|r| r.runner_id),
            Some(runner_c),
            "released job is paired once it reaches the front of the waitlist"
        );
        assert_eq!(
            inner.job_assignments.get(&key_a).map(|r| r.first_at),
            Some(key_a_first_at),
            "the replacement pairing keeps the original first-bound stamp"
        );
    }
}

#[tokio::test]
async fn stale_pending_mark_is_still_offered_to_a_registering_runner() {
    // A job whose machine never landed (pool-pending mark older than the
    // assignment TTL) used to be filtered out of the pairing offer set and
    // left invisible: never paired, and nothing re-armed it. A registering
    // machine must still take it over.
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    {
        let mut inner = state.inner.lock().await;
        let pending: Vec<_> = inner.pool_pending.keys().cloned().collect();
        for key in pending {
            inner.pool_pending.insert(
                key,
                std::time::SystemTime::now()
                    - crate::runtime_scheduling::ASSIGNMENT_TTL
                    - std::time::Duration::from_secs(1),
            );
        }
    }
    stage_provision_token(&state, "token-a");
    let (runner_a, _) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner
                .job_assignments
                .values()
                .next()
                .and_then(|r| r.runner_id),
            Some(runner_a),
            "stale pool-pending mark must still be offered to a registering runner"
        );
    }
}

#[tokio::test]
async fn queue_time_assignment_prefers_idle_registered_runner() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    // Runner exists and is idle before the job lands: queue-time binding.
    stage_provision_token(&state, "token-a");
    let (runner_id, token) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let (_, session) = create_disttask_session(&app, &token, runner_id).await;
    let session_id = session["sessionId"].as_str().unwrap().to_owned();

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner
                .job_assignments
                .values()
                .next()
                .and_then(|r| r.runner_id),
            Some(runner_id),
            "queued job should bind immediately to the idle runner"
        );
        assert!(inner.pool_pending.is_empty());
    }

    // Unverified fabrication still cannot claim it.
    let (_, rogue_session) = create_disttask_session(&app, "preloop-system-token", runner_id).await;
    let rogue_id = rogue_session["sessionId"].as_str().unwrap();
    let stolen = poll_message(&app, "preloop-system-token", rogue_id).await;
    assert!(
        stolen.is_null(),
        "unverified claim must stay empty: {stolen}"
    );

    let delivered = poll_message(&app, &token, &session_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "verified idle runner receives its job: {delivered}"
    );
}

#[tokio::test]
async fn provision_pairing_requires_the_token() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    // Host side staged a token for the next provisioning event.
    stage_provision_token(&state, "token-a");
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);

    // Registration without the token: runner works, but no pairing.
    let (runner_plain, _) =
        register_runner_with_token(&app, "external", &["self-hosted"], None).await;
    {
        let inner = state.inner.lock().await;
        assert!(
            inner.job_assignments.is_empty(),
            "no provisioning proof, no pairing"
        );
        assert_eq!(inner.pool_pending.len(), 1);
        let _ = runner_plain;
    }

    // Forged token value: no pairing either.
    let (_runner_forged, _) =
        register_runner_with_token(&app, "forger", &["self-hosted"], Some("wrong-token")).await;
    {
        let inner = state.inner.lock().await;
        assert!(inner.job_assignments.is_empty());
    }

    // With the token: pairing stamps.
    let (runner_a, _) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    {
        let inner = state.inner.lock().await;
        assert_eq!(
            inner
                .job_assignments
                .values()
                .next()
                .and_then(|r| r.runner_id),
            Some(runner_a)
        );
        // One-time: the token is consumed.
        assert!(
            state.pending_registrations.read().unwrap().is_empty(),
            "provision token must be single-use"
        );
    }
}

#[tokio::test]
async fn strict_mode_refuses_unassigned_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    state.inner.lock().await.require_job_assignments = true;
    let app = app(state.clone(), CancellationToken::new());
    // Strict-only engine: no provisioning channel, so nothing ever pairs.
    state.inner.lock().await.pool_assignments_enabled = false;

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    let (runner_id, token) =
        register_runner_with_token(&app, "external", &["self-hosted"], None).await;
    let (_, session) = create_disttask_session(&app, &token, runner_id).await;
    let session_id = session["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, &token, session_id).await;
    assert!(
        delivered.is_null(),
        "strict mode: unassigned job is never dispatched: {delivered}"
    );
}

#[tokio::test]
async fn strict_non_pool_mode_keeps_a_stale_binding_claimable() {
    // Strict mode without an embedded pool has no waitlist to re-mark a
    // released job on. Clearing a stale binding there used to leave the job
    // with neither a binding nor a mark, and strict claim_permitted requires
    // one — the job stranded forever while its machine had died. The stale
    // record must survive so a verified replacement runner can take over.
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    state.inner.lock().await.require_job_assignments = true;
    state.inner.lock().await.pool_assignments_enabled = false;
    let app = app(state.clone(), CancellationToken::new());

    // A pre-registered idle runner gets the queue-time binding.
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], None).await;
    let (_, session) = create_disttask_session(&app, &token_a, runner_a).await;
    let session_id = session["sessionId"].as_str().unwrap();

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    {
        let inner = state.inner.lock().await;
        let key = inner.job_assignments.keys().next().cloned().unwrap();
        assert_eq!(
            inner.job_assignments.get(&key).and_then(|r| r.runner_id),
            Some(runner_a),
            "queue-time binding assigned the job to the idle runner"
        );
    }
    // machine-a dies without claiming; the binding goes stale.
    {
        let mut inner = state.inner.lock().await;
        for record in inner.job_assignments.values_mut() {
            record.at = std::time::SystemTime::now()
                - crate::runtime_scheduling::CLAIM_BINDING_TTL
                - std::time::Duration::from_secs(1);
            record.first_at = record.at;
        }
    }

    // A fresh pool-authorized runner registers (provision token, so the
    // pairing path runs): the stale binding must survive and still let it
    // claim (kept, not cleared-and-stranded).
    stage_provision_token(&state, "token-b");
    let (runner_b, token_b) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    let (_, session_b) = create_disttask_session(&app, &token_b, runner_b).await;
    let session_b_id = session_b["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, &token_b, session_b_id).await;
    assert!(
        !delivered.is_null(),
        "a verified runner must be able to take over the stale binding: {delivered}"
    );

    // The original runner's session is not the beneficiary.
    let original = poll_message(&app, &token_a, session_id).await;
    assert!(
        original.is_null(),
        "the dead machine's session must not receive the job"
    );
}

#[tokio::test]
async fn permissive_default_keeps_unverified_claims_working() {
    let temp = tempfile::tempdir().unwrap();
    // Pool-management flags stay off — the external-runner default.
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);

    let (_status, session) = create_disttask_session(&app, "preloop-system-token", 1).await;
    let session_id = session["sessionId"].as_str().unwrap();
    let delivered = poll_message(&app, "preloop-system-token", session_id).await;
    assert!(
        delivered["messageType"].as_str().is_some(),
        "unverified legacy session must keep claiming without a pool: {delivered}"
    );
}

#[tokio::test]
async fn session_create_rejects_cross_runner_body() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());
    stage_provision_token(&state, "token-a");
    stage_provision_token(&state, "token-b");
    let (runner_a, token_a) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let (runner_b, _) =
        register_runner_with_token(&app, "machine-b", &["self-hosted"], Some("token-b")).await;
    let _ = runner_b;

    // Token for A, body asking for B: rejected.
    let (status, _) = create_disttask_session(&app, &token_a, runner_b).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Token for A, body asking for A: created and bound to A.
    let (status, session) = create_disttask_session(&app, &token_a, runner_a).await;
    assert_eq!(status, StatusCode::CREATED);
    let session_id = session["sessionId"].as_str().unwrap();
    let inner = state.inner.lock().await;
    assert_eq!(inner.runner_id_for_session(session_id), Some(runner_a));
}

#[tokio::test]
async fn delete_agent_purges_identity_and_requeues_assignment() {
    let temp = tempfile::tempdir().unwrap();
    let state = pool_managed_state(&temp).await;
    let app = app(state.clone(), CancellationToken::new());

    let accepted = submit_simple_run(&app).await;
    assert_eq!(accepted["queued_jobs"], 1);
    stage_provision_token(&state, "token-a");
    let (runner_a, _token) =
        register_runner_with_token(&app, "machine-a", &["self-hosted"], Some("token-a")).await;
    let (run_id, job_id) = {
        let inner = state.inner.lock().await;
        inner.job_assignments.keys().next().unwrap().clone()
    };
    let _ = (run_id, job_id);

    let (status, _) = try_req(
        &app,
        Method::DELETE,
        &format!("/runner/server/_apis/distributedtask/pools/1/agents/{runner_a}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let inner = state.inner.lock().await;
    assert!(!inner.runner_rsa_public_keys.contains_key(&runner_a));
    assert!(!inner.runner_public_keys.contains_key(&runner_a));
    assert!(!inner.runners.contains_key(&runner_a));
    assert!(inner.runner_client_ids.values().all(|id| *id != runner_a));
    assert!(
        inner
            .job_assignments
            .values()
            .all(|r| r.runner_id != Some(runner_a)),
        "purge drops the dead runner's assignment"
    );
    assert_eq!(
        inner.pool_pending.len(),
        1,
        "unclaimed job returns to pool-pending for re-provisioning"
    );
    drop(inner);

    // A restart must not resurrect the deleted identity from a stale snapshot.
    let recovered = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let recovered_inner = recovered.inner.lock().await;
    assert!(!recovered_inner.runners.contains_key(&runner_a));
    assert!(recovered_inner
        .runner_client_ids
        .values()
        .all(|id| *id != runner_a));
}

#[tokio::test]
async fn control_socket_surface_denies_native_and_test_apis() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    // Mirror the bootstrap wiring: the socket router is the full router plus
    // the runner-surface guard.
    let socket_app = app(state.clone(), CancellationToken::new())
        .layer(middleware::from_fn(crate::auth::runner_surface_only));

    for denied in [
        "/api/v1/secrets/owner/repo",
        "/api/v1/runs",
        "/api/v1/debug/sessions",
        "/api/v1/debug/sessions/dbg-controller-only",
        "/api/v1/agent/debug/sessions/dbg-controller-only/events",
        "/internal/test/jobs/complete",
    ] {
        let response = socket_app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(denied)
                    .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "socket must not expose {denied}"
        );
    }
    let controller_verdict = socket_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/debug/sessions/dbg-controller-only/verdict")
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"verdict":"abort"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        controller_verdict.status(),
        StatusCode::NOT_FOUND,
        "the controller verdict API must stay off the guest socket"
    );

    // The v3 registration-token endpoints mint runner-management JWTs
    // (`RunnerManage` scope) for the GitHub-compatible registration flow.
    // They are engine-facing: untrusted workflow code inside the VM must not
    // be able to mint runner-management credentials through the socket. The
    // one exception is the runner's own registration path, whose handler
    // requires the system credential — a wrong one is refused, and the mint
    // itself is tested separately.
    for v3 in [
        "/api/v3/orgs/acme/actions/runners/registration-token",
        "/api/v3/repos/acme/repo/actions/runners/registration-token",
    ] {
        let response = socket_app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(v3)
                    .header(
                        header::AUTHORIZATION,
                        "RemoteAuth preloop-registration-token",
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"url":"https://github.com/acme/repo"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "socket must not expose {v3}"
        );
    }
    let response = socket_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v3/actions/runner-registration")
                .header(
                    header::AUTHORIZATION,
                    "RemoteAuth preloop-registration-token",
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"url":"https://github.com/acme/repo"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "the carved-out registration path still requires the system credential"
    );
    let minted = request_json_with_bearer(
        &socket_app,
        Method::POST,
        "/api/v3/actions/runner-registration",
        json!({"url": "https://github.com/acme/repo", "runner_event": "register"}),
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    assert_eq!(
        minted["token_schema"], "OAuthAccessToken",
        "the runner's own registration must work through the socket"
    );

    // The runner's own log-blob uploads go through the same surface: the
    // in-VM runner PUTs step logs to the signed `/replay/results/*` URLs its
    // Twirp handlers minted, so the guard must not turn them into 404s — the
    // URL ticket (not the surface) is what authorises the write.
    let replay_path = "/replay/results/plan/job/step-1.txt";
    let unsigned = socket_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(replay_path)
                .body(Body::from("log bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        unsigned.status(),
        StatusCode::UNAUTHORIZED,
        "an unsigned upload must be refused once it reaches the auth layer"
    );
    let expires_at = crate::auth::replay_ticket_expiry();
    let sig = crate::auth::sign_replay_upload_ticket(&state, replay_path, expires_at);
    let replay = socket_app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!(
                    "{replay_path}?sv=2021-08-06&se={expires_at}&sr=c&sp=rw&sig={sig}"
                ))
                .body(Body::from("log bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        replay.status(),
        StatusCode::CREATED,
        "the runner's own signed upload must land through the socket"
    );

    // The runner surface stays reachable through the same guard.
    let response = socket_app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/runner/server/_apis/v1/AgentPools")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn replay_blob_uploads_require_a_ticket_bound_to_the_exact_path() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan = uuid::Uuid::new_v4().to_string();
    let job = uuid::Uuid::new_v4().to_string();
    let path = format!("/replay/results/{plan}/{job}/step-1.txt");

    // No credential at all: previously the blob was written; the ticket check
    // must refuse it.
    let unsigned = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(&path)
                .body(Body::from("overwrite attempt"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unsigned.status(), StatusCode::UNAUTHORIZED);

    // A ticket minted for a different path must not authorise this one —
    // this is the cross-job overwrite the signature binds away.
    let other_path = format!("/replay/results/{plan}/{job}/job-logs.txt");
    let expires_at = crate::auth::replay_ticket_expiry();
    let other_sig = crate::auth::sign_replay_upload_ticket(&state, &other_path, expires_at);
    let mismatched = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!(
                    "{path}?sv=2021-08-06&se={expires_at}&sr=c&sp=rw&sig={other_sig}"
                ))
                .body(Body::from("overwrite attempt"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(mismatched.status(), StatusCode::UNAUTHORIZED);
    let expired_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .saturating_sub(1);
    let expired_sig = crate::auth::sign_replay_upload_ticket(&state, &path, expired_at);
    let expired = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!(
                    "{path}?sv=2021-08-06&se={expired_at}&sr=c&sp=rw&sig={expired_sig}"
                ))
                .body(Body::from("expired upload"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(expired.status(), StatusCode::UNAUTHORIZED);

    // The runner's own flow: a ticket for the exact path lands the blob.
    let expires_at = crate::auth::replay_ticket_expiry();
    let sig = crate::auth::sign_replay_upload_ticket(&state, &path, expires_at);
    let uploaded = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!(
                    "{path}?sv=2021-08-06&se={expires_at}&sr=c&sp=rw&sig={sig}"
                ))
                .body(Body::from("log bytes"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::CREATED);
    let stored = tokio::fs::read_to_string(
        temp.path()
            .join("replay")
            .join("results")
            .join(&plan)
            .join(&job)
            .join("step-1.txt"),
    )
    .await
    .unwrap();
    assert_eq!(stored, "log bytes");

    // A tampered signature must not authorise anything.
    let forged_sig = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0u8; 32]);
    let forged_expires_at = crate::auth::replay_ticket_expiry();
    let forged = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!(
                    "{path}?sv=2021-08-06&se={forged_expires_at}&sr=c&sp=rw&sig={forged_sig}"
                ))
                .body(Body::from("overwrite attempt"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(forged.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn replay_job_log_upload_is_published_as_one_complete_file() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let accepted = submit_yaml(
        &app,
        r#"
on: push
jobs:
  first:
    runs-on: ubuntu-latest
    steps:
      - run: echo first
  second:
    runs-on: ubuntu-latest
    steps:
      - run: echo second
"#,
        "owner/repo",
    )
    .await;
    let run_id: RunId = accepted["run_id"].as_str().unwrap().parse().unwrap();
    let requests = {
        let inner = state.inner.lock().await;
        let mut requests = inner
            .job_requests
            .values()
            .filter(|request| request.run_id == run_id)
            .map(|request| {
                (
                    request.request_id,
                    request.plan_id.clone(),
                    request.agent_job_id.to_string(),
                )
            })
            .collect::<Vec<_>>();
        requests.sort_by_key(|(request_id, _, _)| *request_id);
        requests
    };
    assert_eq!(requests.len(), 2);

    let first_log = "first job line\n".repeat(4096);
    let second_log = "second job line\n".repeat(4096);
    for ((_, plan, job), body) in requests
        .iter()
        .zip([first_log.as_str(), second_log.as_str()])
    {
        let path = format!("/replay/results/{plan}/{job}/job-logs.txt");
        let expires_at = crate::auth::replay_ticket_expiry();
        let sig = crate::auth::sign_replay_upload_ticket(&state, &path, expires_at);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!(
                        "{path}?sv=2021-08-06&se={expires_at}&sr=c&sp=rw&sig={sig}"
                    ))
                    .body(Body::from(body.to_owned()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/runs/{run_id}/logs"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let expected = [first_log.as_bytes(), second_log.as_bytes()].concat();
    assert_eq!(body.as_ref(), expected);

    for (_, plan, job) in &requests {
        let results_dir = temp
            .path()
            .join("replay")
            .join("results")
            .join(plan)
            .join(job);
        let mut entries = tokio::fs::read_dir(results_dir).await.unwrap();
        let mut names = Vec::new();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            names.push(entry.file_name());
        }
        assert_eq!(names, vec![std::ffi::OsString::from("job-logs.txt")]);
    }
}

#[tokio::test]
async fn replay_blob_urls_are_minted_only_for_the_callers_own_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan = uuid::Uuid::new_v4().to_string();
    let my_job = uuid::Uuid::new_v4();
    let other_job = uuid::Uuid::new_v4();
    // The runtime token is exported to steps as ACTIONS_RUNTIME_TOKEN, so it
    // is exactly the credential untrusted workflow code holds.
    let runtime_token = state.mint_runtime_token(&plan, &my_job);
    // R1-10: URL-minting writes require a live job record.
    r1_10_register_live_job(&state, my_job, &plan).await;
    let mint_url = "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL";

    // Minting a signed URL for *another* job's backend ids is refused.
    let refused = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(mint_url)
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "workflow_run_backend_id": plan,
                        "workflow_job_run_backend_id": other_job.to_string(),
                        "step_backend_id": "step-1",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);

    // Minting for the caller's own job succeeds, and the returned URL is a
    // real ticket: uploading to it lands the blob.
    let minted = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(mint_url)
                .header(header::AUTHORIZATION, format!("Bearer {runtime_token}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "workflow_run_backend_id": plan,
                        "workflow_job_run_backend_id": my_job.to_string(),
                        "step_backend_id": "step-1",
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(minted.status(), StatusCode::OK);
    let payload: Value =
        serde_json::from_slice(&to_bytes(minted.into_body(), usize::MAX).await.unwrap()).unwrap();
    let upload_url = payload["logs_url"].as_str().unwrap().to_owned();
    assert!(upload_url.contains("/replay/results/"));

    let uploaded = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(upload_url)
                .body(Body::from("step one log"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::CREATED);
    let stored = tokio::fs::read_to_string(
        temp.path()
            .join("replay")
            .join("results")
            .join(&plan)
            .join(my_job.to_string())
            .join("step-step-1.txt"),
    )
    .await
    .unwrap();
    assert_eq!(stored, "step one log");
}

#[tokio::test]
async fn results_uuid_spellings_use_canonical_paths_and_metadata_keys() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan = uuid::Uuid::parse_str("fedcba98-7654-4321-89ab-cdef01234567")
        .unwrap()
        .to_string();
    let job = uuid::Uuid::parse_str("01234567-89ab-4cde-8fab-cdef01234567").unwrap();
    let canonical = job.to_string();
    let forms = [
        canonical.clone(),
        canonical.to_ascii_uppercase(),
        format!("{{{canonical}}}"),
        canonical.replace('-', ""),
        format!("urn:uuid:{canonical}"),
    ];
    let token = state.mint_runtime_token(&plan, &job);
    // R1-10: results writes require a live job record.
    r1_10_register_live_job(&state, job, &plan).await;

    for form in &forms {
        let requests = [
            (
                "/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
                "logs_url",
                format!("/replay/results/{plan}/{canonical}/job-logs.txt"),
            ),
            (
                "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
                "logs_url",
                format!("/replay/results/{plan}/{canonical}/step-step-1.txt"),
            ),
            (
                "/twirp/results.services.receiver.Receiver/GetStepSummarySignedBlobURL",
                "summary_url",
                format!("/replay/results/{plan}/{canonical}/step-step-1-summary.md"),
            ),
        ];
        for (uri, field, expected_path) in requests {
            let payload = request_json_with_bearer(
                &app,
                Method::POST,
                uri,
                json!({
                    "workflow_run_backend_id": plan,
                    "workflow_job_run_backend_id": form,
                    "step_backend_id": "step-1",
                }),
                &token,
            )
            .await;
            let url = payload[field].as_str().unwrap();
            assert!(
                url.contains(&expected_path),
                "equivalent job spelling must use the canonical path: {url}"
            );
        }

        let diag = request_json_with_bearer(
            &app,
            Method::POST,
            "/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL",
            json!({
                "workflow_run_backend_id": plan,
                "workflow_job_run_backend_id": form,
            }),
            &token,
        )
        .await;
        assert!(
            diag["diag_logs_url"]
                .as_str()
                .is_some_and(|url| url.contains("/twirp-blob/diag/")),
            "diagnostic URL must remain a token-only path"
        );

        let update = request_json_with_bearer(
            &app,
            Method::POST,
            "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate",
            json!({
                "workflow_run_backend_id": plan,
                "workflow_job_run_backend_id": form,
                "steps": [],
            }),
            &token,
        )
        .await;
        assert_eq!(update["ok"], true);

        request_json_with_bearer(
            &app,
            Method::POST,
            "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
            json!({
                "workflow_run_backend_id": plan,
                "workflow_job_run_backend_id": form,
                "step_backend_id": "step-1",
                "size": 17,
            }),
            &token,
        )
        .await;
        request_json_with_bearer(
            &app,
            Method::POST,
            "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
            json!({
                "workflow_run_backend_id": plan,
                "workflow_job_run_backend_id": form,
                "step_backend_id": "step-1",
                "line_count": 3,
            }),
            &token,
        )
        .await;
        request_json_with_bearer(
            &app,
            Method::POST,
            "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
            json!({
                "workflow_run_backend_id": plan,
                "workflow_job_run_backend_id": form,
                "line_count": 5,
            }),
            &token,
        )
        .await;
    }

    let step_payload = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
        json!({
            "workflow_run_backend_id": plan,
            "workflow_job_run_backend_id": format!("{{{canonical}}}"),
            "step_backend_id": "step-1",
        }),
        &token,
    )
    .await;
    let uploaded = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(step_payload["logs_url"].as_str().unwrap())
                .body(Body::from("canonical path"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::CREATED);
    assert_eq!(
        tokio::fs::read_to_string(
            temp.path()
                .join("replay")
                .join("results")
                .join(&plan)
                .join(&canonical)
                .join("step-step-1.txt"),
        )
        .await
        .unwrap(),
        "canonical path"
    );

    let inner = state.inner.lock().await;
    let expected_keys = [
        format!("results:{plan}:{canonical}:summary:step-1"),
        format!("results:{plan}:{canonical}:step:step-1"),
        format!("results:{plan}:{canonical}:job:{canonical}"),
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    let actual_keys = inner.log_metadata.keys().cloned().collect::<BTreeSet<_>>();
    assert_eq!(
        actual_keys, expected_keys,
        "all accepted UUID spellings must share one metadata namespace"
    );
    assert_eq!(
        inner
            .log_metadata
            .get(&format!("results:{plan}:{canonical}:summary:step-1"))
            .map(|meta| (meta.byte_count, meta.line_count)),
        Some((17, 0))
    );
    assert_eq!(
        inner
            .log_metadata
            .get(&format!("results:{plan}:{canonical}:step:step-1"))
            .map(|meta| (meta.byte_count, meta.line_count)),
        Some((240, 3))
    );
    assert_eq!(
        inner
            .log_metadata
            .get(&format!("results:{plan}:{canonical}:job:{canonical}"))
            .map(|meta| (meta.byte_count, meta.line_count)),
        Some((400, 5))
    );
}

#[tokio::test]
async fn alternate_results_job_spelling_preserves_canonical_log_lookup() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let (_, plan, canonical) = jobs
        .iter()
        .find(|(job, _, _)| job == "build")
        .cloned()
        .expect("build job must be present");
    let job = canonical.parse::<uuid::Uuid>().unwrap();
    let token = state.mint_runtime_token(&plan, &job);
    let alternate = format!("{{{canonical}}}");

    let payload = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
        json!({
            "workflow_run_backend_id": plan,
            "workflow_job_run_backend_id": alternate,
        }),
        &token,
    )
    .await;
    let uploaded = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(payload["logs_url"].as_str().unwrap())
                .body(Body::from("lookup survives"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::CREATED);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("/api/v1/runs/{run_id}/logs?job={canonical}"))
                .header(header::AUTHORIZATION, "Bearer preloop-system-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .as_ref(),
        b"lookup survives"
    );
    assert!(
        !temp
            .path()
            .join("replay")
            .join("results")
            .join(&plan)
            .join(&alternate)
            .exists(),
        "alternate spelling must not create a second lookup directory"
    );
}

#[tokio::test]
async fn results_reject_cross_job_and_malformed_uuid_targets() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let plan = "plan-1";
    let own_job = uuid::Uuid::parse_str("01234567-89ab-4cde-8fab-cdef01234567").unwrap();
    let other_job = uuid::Uuid::parse_str("fedcba98-7654-4321-89ab-cdef01234567").unwrap();
    let token = state.mint_runtime_token(plan, &own_job);
    let targets = [
        format!("{{{other_job}}}"),
        other_job.to_string().to_ascii_uppercase(),
        "not-a-uuid".to_owned(),
    ];

    for target in &targets {
        for uri in [
            "/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
            "/twirp/results.services.receiver.Receiver/GetStepLogsSignedBlobURL",
            "/twirp/results.services.receiver.Receiver/GetStepSummarySignedBlobURL",
            "/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL",
        ] {
            assert_eq!(
                status_with_bearer(
                    &app,
                    &token,
                    Method::POST,
                    uri,
                    json!({
                        "workflow_run_backend_id": plan,
                        "workflow_job_run_backend_id": target,
                        "step_backend_id": "step-1",
                    }),
                )
                .await,
                StatusCode::FORBIDDEN,
                "Results target must stay bound to the token's job: {target}"
            );
        }

        for (uri, body) in [
            (
                "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
                json!({
                    "workflow_run_backend_id": plan,
                    "workflow_job_run_backend_id": target,
                    "step_backend_id": "step-1",
                    "size": 99,
                }),
            ),
            (
                "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
                json!({
                    "workflow_run_backend_id": plan,
                    "workflow_job_run_backend_id": target,
                    "step_backend_id": "step-1",
                    "line_count": 99,
                }),
            ),
            (
                "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
                json!({
                    "workflow_run_backend_id": plan,
                    "workflow_job_run_backend_id": target,
                    "line_count": 99,
                }),
            ),
        ] {
            assert_eq!(
                status_with_bearer(&app, &token, Method::POST, uri, body).await,
                StatusCode::FORBIDDEN,
                "metadata target must stay bound to the token's job: {target}"
            );
        }
    }

    assert_eq!(
        status_with_bearer(
            &app,
            &token,
            Method::POST,
            "/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
            json!({
                "workflow_run_backend_id": "different-plan",
                "workflow_job_run_backend_id": own_job.to_string(),
            }),
        )
        .await,
        StatusCode::FORBIDDEN,
        "a matching UUID under another plan is still a different Results target"
    );

    let inner = state.inner.lock().await;
    assert!(
        inner.log_metadata.is_empty(),
        "rejected Results targets must not create metadata"
    );
}

#[tokio::test]
async fn system_results_token_preserves_opaque_identifier_compatibility() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let payload = request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/results.services.receiver.Receiver/GetJobLogsSignedBlobURL",
        json!({
            "workflow_run_backend_id": "plan-opaque",
            "workflow_job_run_backend_id": "job-opaque",
        }),
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    assert!(
        payload["logs_url"]
            .as_str()
            .is_some_and(|url| url.contains("/replay/results/plan-opaque/job-opaque/job-logs.txt")),
        "system callers may continue to address opaque backend ids"
    );

    request_json_with_bearer(
        &app,
        Method::POST,
        "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
        json!({
            "workflow_run_backend_id": "plan-opaque",
            "workflow_job_run_backend_id": "job-opaque",
            "line_count": 2,
        }),
        DEFAULT_PRELOOP_SYSTEM_TOKEN,
    )
    .await;
    let inner = state.inner.lock().await;
    assert!(
        inner
            .log_metadata
            .contains_key("results:plan-opaque:job-opaque:job:job-opaque"),
        "opaque system-token metadata ids must retain their existing spelling"
    );
}

#[tokio::test]
async fn results_workflow_steps_require_the_calling_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let (_, plan_a, agent_a) = jobs
        .iter()
        .find(|(job, _, _)| job == "build")
        .cloned()
        .expect("build job must be present");
    let (_, plan_b, agent_b) = jobs
        .iter()
        .find(|(job, _, _)| job == "test")
        .cloned()
        .expect("test job must be present");
    let a_job = agent_a.parse::<uuid::Uuid>().unwrap();
    let b_job = agent_b.parse::<uuid::Uuid>().unwrap();
    let a_step = workflow_step_ids(&state, run_id, "build")
        .await
        .into_iter()
        .next()
        .expect("job A must have a workflow step");
    let b_step = workflow_step_ids(&state, run_id, "test")
        .await
        .into_iter()
        .next()
        .expect("job B must have a workflow step");
    let token_a = state.mint_runtime_token(&plan_a, &a_job);
    let token_b = state.mint_runtime_token(&plan_b, &b_job);
    let uri = "/twirp/github.actions.results.api.v1.WorkflowStepUpdateService/WorkflowStepsUpdate";

    let before = get_run_json(&app, &run_id.to_string()).await;
    let job_steps = |run: &Value, name: &str| {
        run["jobs_list"]
            .as_array()
            .unwrap()
            .iter()
            .find(|job| job["job_id"] == name)
            .unwrap()["steps"]
            .clone()
    };
    let build_before = job_steps(&before, "build");
    let test_before = job_steps(&before, "test");

    assert_eq!(
        status_with_bearer(
            &app,
            &token_a,
            Method::POST,
            uri,
            json!({
                "workflow_run_backend_id": plan_b,
                "workflow_job_run_backend_id": agent_b,
                "steps": [{
                    "external_id": b_step,
                    "number": 1,
                    "name": "A must not rewrite B",
                    "status": 6,
                    "conclusion": 3
                }]
            }),
        )
        .await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        status_with_bearer(
            &app,
            &token_b,
            Method::POST,
            uri,
            json!({
                "workflow_run_backend_id": plan_a,
                "workflow_job_run_backend_id": agent_a,
                "steps": [{
                    "external_id": a_step,
                    "number": 1,
                    "name": "B must not rewrite A",
                    "status": 6,
                    "conclusion": 3
                }]
            }),
        )
        .await,
        StatusCode::FORBIDDEN
    );

    let after_refused = get_run_json(&app, &run_id.to_string()).await;
    assert_eq!(job_steps(&after_refused, "build"), build_before);
    assert_eq!(job_steps(&after_refused, "test"), test_before);

    assert_eq!(
        status_with_bearer(
            &app,
            &token_a,
            Method::POST,
            uri,
            json!({
                "workflow_run_backend_id": plan_a,
                "workflow_job_run_backend_id": agent_a,
                "steps": [{
                    "external_id": a_step,
                    "number": 1,
                    "name": "A owns this update",
                    "status": 6,
                    "conclusion": 2
                }]
            }),
        )
        .await,
        StatusCode::OK
    );
    let after_own = get_run_json(&app, &run_id.to_string()).await;
    assert_eq!(job_steps(&after_own, "test"), test_before);
    assert_eq!(
        job_steps(&after_own, "build")[0]["name"],
        "A owns this update"
    );
}

#[tokio::test]
async fn results_metadata_requires_the_calling_job() {
    let temp = tempfile::tempdir().unwrap();
    let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
    let app = app(state.clone(), CancellationToken::new());
    let (run_id, jobs) = two_job_run_for_log_filters(&app, &state).await;
    let (_, plan_a, agent_a) = jobs
        .iter()
        .find(|(job, _, _)| job == "build")
        .cloned()
        .expect("build job must be present");
    let (_, plan_b, agent_b) = jobs
        .iter()
        .find(|(job, _, _)| job == "test")
        .cloned()
        .expect("test job must be present");
    let a_job = agent_a.parse::<uuid::Uuid>().unwrap();
    let b_step = workflow_step_ids(&state, run_id, "test")
        .await
        .into_iter()
        .next()
        .expect("job B must have a workflow step");
    let token_a = state.mint_runtime_token(&plan_a, &a_job);
    let keys = [
        format!("results:{plan_b}:{agent_b}:summary:{b_step}"),
        format!("results:{plan_b}:{agent_b}:step:{b_step}"),
        format!("results:{plan_b}:{agent_b}:job:{agent_b}"),
    ];
    {
        let mut inner = state.inner.lock().await;
        for (index, key) in keys.iter().enumerate() {
            inner.log_metadata.insert(
                key.clone(),
                LogMetadata {
                    byte_count: index + 1,
                    line_count: index + 10,
                },
            );
        }
    }
    let metadata_before = {
        let inner = state.inner.lock().await;
        keys.iter()
            .map(|key| {
                inner
                    .log_metadata
                    .get(key)
                    .map(|meta| (meta.byte_count, meta.line_count))
            })
            .collect::<Vec<_>>()
    };

    let requests = [
        (
            "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
            json!({
                "step_backend_id": b_step,
                "workflow_job_run_backend_id": agent_b,
                "workflow_run_backend_id": plan_b,
                "size": 999
            }),
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
            json!({
                "step_backend_id": b_step,
                "workflow_job_run_backend_id": agent_b,
                "workflow_run_backend_id": plan_b,
                "line_count": 999
            }),
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
            json!({
                "workflow_job_run_backend_id": agent_b,
                "workflow_run_backend_id": plan_b,
                "line_count": 999
            }),
        ),
    ];
    for (uri, body) in requests {
        assert_eq!(
            status_with_bearer(&app, &token_a, Method::POST, uri, body).await,
            StatusCode::FORBIDDEN,
            "{uri}"
        );
    }

    let metadata_after = {
        let inner = state.inner.lock().await;
        keys.iter()
            .map(|key| {
                inner
                    .log_metadata
                    .get(key)
                    .map(|meta| (meta.byte_count, meta.line_count))
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(metadata_after, metadata_before);

    let own_requests = [
        (
            "/twirp/results.services.receiver.Receiver/CreateStepSummaryMetadata",
            json!({
                "step_backend_id": b_step,
                "workflow_job_run_backend_id": agent_a,
                "workflow_run_backend_id": plan_a,
                "size": 10
            }),
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateStepLogsMetadata",
            json!({
                "step_backend_id": b_step,
                "workflow_job_run_backend_id": agent_a,
                "workflow_run_backend_id": plan_a,
                "line_count": 2
            }),
        ),
        (
            "/twirp/results.services.receiver.Receiver/CreateJobLogsMetadata",
            json!({
                "workflow_job_run_backend_id": agent_a,
                "workflow_run_backend_id": plan_a,
                "line_count": 3
            }),
        ),
    ];
    for (uri, body) in own_requests {
        assert_eq!(
            status_with_bearer(&app, &token_a, Method::POST, uri, body).await,
            StatusCode::OK,
            "{uri}"
        );
    }

    let inner = state.inner.lock().await;
    assert_eq!(
        inner
            .log_metadata
            .get(&keys[0])
            .map(|meta| (meta.byte_count, meta.line_count)),
        Some((1, 10))
    );
    assert_eq!(
        inner
            .log_metadata
            .get(&keys[1])
            .map(|meta| (meta.byte_count, meta.line_count)),
        Some((2, 11))
    );
    assert_eq!(
        inner
            .log_metadata
            .get(&format!("results:{plan_a}:{agent_a}:summary:{b_step}"))
            .map(|meta| (meta.byte_count, meta.line_count)),
        Some((10, 0))
    );
    assert_eq!(
        inner
            .log_metadata
            .get(&format!("results:{plan_a}:{agent_a}:step:{b_step}"))
            .map(|meta| (meta.byte_count, meta.line_count)),
        Some((160, 2))
    );
    assert_eq!(
        inner
            .log_metadata
            .get(&format!("results:{plan_a}:{agent_a}:job:{agent_a}"))
            .map(|meta| (meta.byte_count, meta.line_count)),
        Some((240, 3))
    );
}
