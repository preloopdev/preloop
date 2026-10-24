//! Code-scanning SARIF ingestion: the GitHub upload shim and the native run
//! API.
//!
//! The tests drive the real router with the same credentials a job holds:
//! a runtime token minted for an attempt of the run, bound server-side to the
//! run's repository. That binding is what stops one repository's job from
//! uploading findings for another.

mod common;

use common::*;

use flate2::Compression;
use flate2::write::GzEncoder;

const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
const REF: &str = "refs/heads/feat/sarif";

fn sarif_document() -> Value {
    json!({
        "version": "2.1.0",
        "runs": [{
            "tool": {
                "driver": {
                    "name": "CodeQL",
                    "rules": [
                        {
                            "id": "js/sql-injection",
                            "name": "Database query built from user-controlled sources",
                            "shortDescription": { "text": "SQL injection" },
                            "defaultConfiguration": { "level": "error" }
                        },
                        {
                            "id": "js/unused-variable",
                            "defaultConfiguration": { "level": "note" }
                        }
                    ]
                }
            },
            "results": [
                {
                    "ruleId": "js/sql-injection",
                    "message": { "text": "Query built from user input" },
                    "locations": [{
                        "physicalLocation": {
                            "artifactLocation": { "uri": "file:///home/runner/work/repo/src/db.js" },
                            "region": { "startLine": 12, "endLine": 14, "startColumn": 3, "endColumn": 20 }
                        }
                    }],
                    "partialFingerprints": { "primaryLocationLineHash": "deadbeef:1" }
                },
                {
                    "ruleId": "js/unused-variable",
                    "message": { "text": "Unused variable" },
                    "locations": [{
                        "physicalLocation": {
                            "artifactLocation": { "uri": "src/util.js" },
                            "region": { "startLine": 4 }
                        }
                    }],
                    "suppressions": [{ "kind": "inSource" }]
                }
            ]
        }]
    })
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn encoded_document(document: &Value) -> String {
    BASE64_STANDARD.encode(gzip(&serde_json::to_vec(document).unwrap()))
}

fn github_payload(commit: &str, git_ref: &str, document: &Value) -> Value {
    json!({
        "commit_sha": commit,
        "ref": git_ref,
        "sarif": encoded_document(document),
        "tool_name": "CodeQL",
        "checkout_uri": "file:///home/runner/work/repo",
        "started_at": "2026-10-21T09:00:00Z",
        "analysis_key": ".github/workflows/codeql.yml:analyze",
        "environment": r#"{"language":"javascript"}"#,
    })
}

async fn send(
    app: &axum::Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if !body.is_empty() {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

async fn send_bytes(
    app: &axum::Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if !body.is_empty() {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, bytes.to_vec())
}

struct Fixture {
    _temp: tempfile::TempDir,
    state: AppState,
    app: axum::Router,
}

impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
        let app = app(state.clone(), CancellationToken::new());
        Self {
            _temp: temp,
            state,
            app,
        }
    }

    /// Submit a run with a real head SHA and return `(run_id, plan_id,
    /// agent_job_id)` for its first job.
    async fn submit_run(&self, repository: &str) -> (String, String, uuid::Uuid) {
        let accepted = request_json(
            &self.app,
            Method::POST,
            "/api/v1/runs",
            json!({
                "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: self-hosted\n    steps:\n      - run: echo hi\n",
                "event": "push",
                "repository": repository,
                "git_ref": REF,
                "sha": COMMIT,
                "workflow_path": ".github/workflows/codeql.yml",
            }),
        )
        .await;
        let run_id = accepted["run_id"].as_str().unwrap().to_owned();
        let parsed: RunId = run_id.parse().unwrap();
        let inner = self.state.test_tx().await;
        let request = inner
            .job_requests
            .values()
            .find(|request| request.run_id == parsed)
            .expect("submit must create a job request");
        (run_id, request.plan_id.clone(), request.agent_job_id)
    }

    async fn token_for(&self, plan_id: &str, job_id: &uuid::Uuid) -> String {
        self.state.mint_runtime_token(plan_id, job_id)
    }
}

fn upload_uri(owner: &str, repo: &str) -> String {
    format!("/api/v3/repos/{owner}/{repo}/code-scanning/sarifs")
}

#[tokio::test]
async fn github_upload_requires_a_job_token() {
    let fixture = Fixture::new().await;
    let (_run_id, plan_id, job_id) = fixture.submit_run("owner/repo").await;
    let token = fixture.token_for(&plan_id, &job_id).await;
    let payload = github_payload(COMMIT, REF, &sarif_document());

    // No credential: GitHub-shaped 401 with the Bearer challenge.
    let (status, body) = send(
        &fixture.app,
        Method::POST,
        &upload_uri("owner", "repo"),
        None,
        "application/json",
        serde_json::to_vec(&payload).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(
        body["message"].as_str().is_some(),
        "GitHub-shaped error expected: {body}"
    );

    // A token that names no live attempt is not a job credential.
    let stranger = fixture
        .state
        .mint_runtime_token("plan-x", &uuid::Uuid::new_v4());
    let (status, _) = send(
        &fixture.app,
        Method::POST,
        &upload_uri("owner", "repo"),
        Some(&stranger),
        "application/json",
        serde_json::to_vec(&payload).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // The system token is not repository-bound, so it cannot upload here.
    let (status, _) = send(
        &fixture.app,
        Method::POST,
        &upload_uri("owner", "repo"),
        Some("preloop-system-token"),
        "application/json",
        serde_json::to_vec(&payload).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // The job's own token is accepted.
    let (status, body) = send(
        &fixture.app,
        Method::POST,
        &upload_uri("owner", "repo"),
        Some(&token),
        "application/json",
        serde_json::to_vec(&payload).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
    assert!(body["id"].as_str().is_some());
    assert!(
        body["url"]
            .as_str()
            .unwrap()
            .contains("/code-scanning/sarifs/")
    );
}

#[tokio::test]
async fn github_upload_rejects_another_repository() {
    let fixture = Fixture::new().await;
    let (_run_id, plan_id, job_id) = fixture.submit_run("owner/repo").await;
    let token = fixture.token_for(&plan_id, &job_id).await;
    let payload = github_payload(COMMIT, REF, &sarif_document());
    let (status, body) = send(
        &fixture.app,
        Method::POST,
        &upload_uri("someone-else", "repo"),
        Some(&token),
        "application/json",
        serde_json::to_vec(&payload).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");
    assert!(body["message"].as_str().unwrap().contains("repository"));
}

#[tokio::test]
async fn github_upload_status_and_analyses_roundtrip() {
    let fixture = Fixture::new().await;
    let (_run_id, plan_id, job_id) = fixture.submit_run("owner/repo").await;
    let token = fixture.token_for(&plan_id, &job_id).await;
    let payload = github_payload(COMMIT, REF, &sarif_document());
    let (status, body) = send(
        &fixture.app,
        Method::POST,
        &upload_uri("owner", "repo"),
        Some(&token),
        "application/json",
        serde_json::to_vec(&payload).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let analysis_id = body["id"].as_str().unwrap().to_owned();

    // The action polls this until `processing_status` leaves `pending`.
    let status_uri = format!("/api/v3/repos/owner/repo/code-scanning/sarifs/{analysis_id}");
    let (status, body) = send(
        &fixture.app,
        Method::GET,
        &status_uri,
        Some(&token),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["processing_status"], "complete");
    assert_eq!(body["sarif_id"], analysis_id);
    assert!(
        body["analyses_url"]
            .as_str()
            .unwrap()
            .contains("code-scanning/analyses")
    );

    // `analyses_url` is a real endpoint for the caller's run.
    let (status, body) = send(
        &fixture.app,
        Method::GET,
        "/api/v3/repos/owner/repo/code-scanning/analyses",
        Some(&token),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    let analyses = body["analyses"].as_array().unwrap();
    assert_eq!(analyses.len(), 1);
    assert_eq!(analyses[0]["analysis_id"], analysis_id);
    assert_eq!(analyses[0]["tool_name"], "CodeQL");
    assert_eq!(analyses[0]["finding_count"], 2);
    assert_eq!(analyses[0]["commit_sha"], COMMIT);
    assert_eq!(analyses[0]["ref"], REF);

    // Unknown id and another repository both read as 404 (no existence leak).
    let (status, _) = send(
        &fixture.app,
        Method::GET,
        &format!(
            "/api/v3/repos/owner/repo/code-scanning/sarifs/{}",
            uuid::Uuid::new_v4()
        ),
        Some(&token),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(
        &fixture.app,
        Method::GET,
        &format!("/api/v3/repos/other/repo/code-scanning/sarifs/{analysis_id}"),
        Some("preloop-system-token"),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn github_put_analysis_accepts_commit_oid() {
    let fixture = Fixture::new().await;
    let (_run_id, plan_id, job_id) = fixture.submit_run("owner/repo").await;
    let token = fixture.token_for(&plan_id, &job_id).await;
    let mut payload = github_payload(COMMIT, REF, &sarif_document());
    let object = payload.as_object_mut().unwrap();
    // The current codeql-action endpoint sends `commit_oid`, not `commit_sha`.
    let commit = object.remove("commit_sha").unwrap();
    object.insert("commit_oid".to_owned(), commit);

    let (status, body) = send(
        &fixture.app,
        Method::PUT,
        "/api/v3/repos/owner/repo/code-scanning/analysis",
        Some(&token),
        "application/json",
        serde_json::to_vec(&payload).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
    assert!(body["id"].as_str().is_some());
}

#[tokio::test]
async fn github_upload_rejects_malformed_payloads() {
    let fixture = Fixture::new().await;
    let (_run_id, plan_id, job_id) = fixture.submit_run("owner/repo").await;
    let token = fixture.token_for(&plan_id, &job_id).await;

    let cases = [
        // `sarif` is not base64.
        json!({ "commit_sha": COMMIT, "ref": REF, "sarif": "!!!not-base64!!!" }),
        // Valid base64, not JSON after decompression.
        json!({ "commit_sha": COMMIT, "ref": REF, "sarif": BASE64_STANDARD.encode(gzip(b"not json")) }),
        // SARIF 2.1.0 requires `runs`.
        json!({ "commit_sha": COMMIT, "ref": REF, "sarif": BASE64_STANDARD.encode(gzip(br#"{"version":"2.1.0"}"#)) }),
        // Unsupported version.
        json!({ "commit_sha": COMMIT, "ref": REF, "sarif": BASE64_STANDARD.encode(gzip(br#"{"version":"1.0","runs":[]}"#)) }),
        // Missing commit.
        json!({ "ref": REF, "sarif": encoded_document(&sarif_document()) }),
        // Missing ref.
        json!({ "commit_sha": COMMIT, "sarif": encoded_document(&sarif_document()) }),
        // Commit that is not a hex id.
        json!({ "commit_sha": "not-a-sha", "ref": REF, "sarif": encoded_document(&sarif_document()) }),
    ];
    for case in cases {
        let (status, body) = send(
            &fixture.app,
            Method::POST,
            &upload_uri("owner", "repo"),
            Some(&token),
            "application/json",
            serde_json::to_vec(&case).unwrap(),
        )
        .await;
        assert!(
            status.is_client_error(),
            "expected a 4xx for {case}, got {status} {body}"
        );
        assert!(
            body["message"].as_str().is_some(),
            "GitHub-shaped error expected for {case}: {body}"
        );
    }

    // Invalid JSON body.
    let (status, _) = send(
        &fixture.app,
        Method::POST,
        &upload_uri("owner", "repo"),
        Some(&token),
        "application/json",
        b"{".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn github_upload_enforces_the_decompressed_limit() {
    let fixture = Fixture::new().await;
    let (_run_id, plan_id, job_id) = fixture.submit_run("owner/repo").await;
    let token = fixture.token_for(&plan_id, &job_id).await;
    // A tiny gzip that expands past the decompressed bound.
    let bomb = vec![b'{'; MAX_SARIF_DECOMPRESSED_BYTES + 1024];
    let payload = json!({
        "commit_sha": COMMIT,
        "ref": REF,
        "sarif": BASE64_STANDARD.encode(gzip(&bomb)),
    });
    let (status, body) = send(
        &fixture.app,
        Method::POST,
        &upload_uri("owner", "repo"),
        Some(&token),
        "application/json",
        serde_json::to_vec(&payload).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "body: {body}");
}

#[tokio::test]
async fn github_upload_is_idempotent_on_the_findings_fingerprint() {
    let fixture = Fixture::new().await;
    let (_run_id, plan_id, job_id) = fixture.submit_run("owner/repo").await;
    let token = fixture.token_for(&plan_id, &job_id).await;
    let payload = github_payload(COMMIT, REF, &sarif_document());

    let mut ids = Vec::new();
    for _ in 0..2 {
        let (status, body) = send(
            &fixture.app,
            Method::POST,
            &upload_uri("owner", "repo"),
            Some(&token),
            "application/json",
            serde_json::to_vec(&payload).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
        ids.push(body["id"].as_str().unwrap().to_owned());
    }
    assert_eq!(ids[0], ids[1], "a re-upload must reuse the analysis");

    let (_, body) = send(
        &fixture.app,
        Method::GET,
        "/api/v3/repos/owner/repo/code-scanning/analyses",
        Some(&token),
        "",
        vec![],
    )
    .await;
    assert_eq!(body["analyses"].as_array().unwrap().len(), 1);

    // A different commit is a different analysis.
    let other = github_payload(
        "fedcba9876543210fedcba9876543210fedcba98",
        REF,
        &sarif_document(),
    );
    let (status, body) = send(
        &fixture.app,
        Method::POST,
        &upload_uri("owner", "repo"),
        Some(&token),
        "application/json",
        serde_json::to_vec(&other).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let other_id = body["id"].as_str().unwrap().to_owned();
    assert_ne!(ids[0], other_id);
}

#[tokio::test]
async fn native_upload_accepts_raw_and_wrapper_documents() {
    let fixture = Fixture::new().await;
    let (run_id, plan_id, job_id) = fixture.submit_run("owner/repo").await;
    let token = fixture.token_for(&plan_id, &job_id).await;
    let uri = format!("/api/v1/runs/{run_id}/sarif");

    // Raw `application/sarif+json`: commit/ref come from the run.
    let (status, body) = send(
        &fixture.app,
        Method::POST,
        &uri,
        Some(&token),
        "application/sarif+json",
        serde_json::to_vec(&sarif_document()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
    let raw_id = body["analysis_id"].as_str().unwrap().to_owned();
    assert_eq!(body["run_id"], run_id);
    assert_eq!(body["finding_count"], 2);
    assert_eq!(body["deduplicated"], false);

    // The same findings re-uploaded through the wrapper dedupe.
    let (status, body) = send(
        &fixture.app,
        Method::POST,
        &uri,
        Some(&token),
        "application/json",
        serde_json::to_vec(&json!({
            "sarif": sarif_document(),
            "commit_sha": COMMIT,
            "ref": REF,
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "body: {body}");
    assert_eq!(body["analysis_id"], raw_id);
    assert_eq!(body["deduplicated"], true);

    // List, read and raw round-trip through the native surface.
    let (status, body) = send(
        &fixture.app,
        Method::GET,
        &uri,
        Some("preloop-system-token"),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["analyses"].as_array().unwrap().len(), 1);
    assert_eq!(body["analyses"][0]["source"], "api");

    let (status, body) = send(
        &fixture.app,
        Method::GET,
        &format!("/api/v1/sarif/{raw_id}"),
        Some(&token),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let findings = body["findings"].as_array().unwrap();
    assert_eq!(findings.len(), 2);
    let injection = findings
        .iter()
        .find(|finding| finding["rule_id"] == "js/sql-injection")
        .expect("rule finding present");
    assert_eq!(injection["level"], "error");
    assert_eq!(injection["path"], "home/runner/work/repo/src/db.js");
    assert_eq!(injection["start_line"], 12);
    assert_eq!(injection["end_line"], 14);
    let suppressed = findings
        .iter()
        .find(|finding| finding["rule_id"] == "js/unused-variable")
        .expect("suppressed finding present");
    assert_eq!(suppressed["suppression"], "inSource");

    let (status, raw) = send_bytes(
        &fixture.app,
        Method::GET,
        &format!("/api/v1/sarif/{raw_id}/raw"),
        Some(&token),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let stored: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(stored["version"], "2.1.0");
}

#[tokio::test]
async fn native_upload_requires_ownership_of_the_run() {
    let fixture = Fixture::new().await;
    let (run_a, plan_a, job_a) = fixture.submit_run("owner/repo").await;
    let (run_b, plan_b, job_b) = fixture.submit_run("owner/repo").await;
    let token_a = fixture.token_for(&plan_a, &job_a).await;

    // A job of run A cannot write to run B.
    let (status, body) = send(
        &fixture.app,
        Method::POST,
        &format!("/api/v1/runs/{run_b}/sarif"),
        Some(&token_a),
        "application/sarif+json",
        serde_json::to_vec(&sarif_document()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "body: {body}");

    // Listing run B with run A's token is refused too.
    let (status, _) = send(
        &fixture.app,
        Method::GET,
        &format!("/api/v1/runs/{run_b}/sarif"),
        Some(&token_a),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Upload as A, then read it back with B's token: forbidden. The system
    // token may read it, and unknown ids are 404.
    let (status, body) = send(
        &fixture.app,
        Method::POST,
        &format!("/api/v1/runs/{run_a}/sarif"),
        Some(&token_a),
        "application/sarif+json",
        serde_json::to_vec(&sarif_document()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let analysis_id = body["analysis_id"].as_str().unwrap().to_owned();
    let token_b = fixture.token_for(&plan_b, &job_b).await;
    let (status, _) = send(
        &fixture.app,
        Method::GET,
        &format!("/api/v1/sarif/{analysis_id}"),
        Some(&token_b),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(
        &fixture.app,
        Method::GET,
        &format!("/api/v1/sarif/{analysis_id}"),
        Some("preloop-system-token"),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &fixture.app,
        Method::GET,
        &format!("/api/v1/sarif/{}", uuid::Uuid::new_v4()),
        Some("preloop-system-token"),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // A job token for a settled attempt cannot upload.
    complete_live_job(&fixture.state, job_a).await;
    let (status, _) = send(
        &fixture.app,
        Method::POST,
        &format!("/api/v1/runs/{run_a}/sarif"),
        Some(&token_a),
        "application/sarif+json",
        serde_json::to_vec(&json!({
            "version": "2.1.0",
            "runs": [{ "tool": { "driver": { "name": "other" } }, "results": [] }]
        }))
        .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn github_actions_run_read_backs_the_analysis_key() {
    let fixture = Fixture::new().await;
    let (run_id, plan_id, job_id) = fixture.submit_run("owner/repo").await;
    let token = fixture.token_for(&plan_id, &job_id).await;

    // Octokit coerces the engine's UUID run id with `Number(...)`, so the
    // segment the action requests is literally `NaN`; the credential decides
    // the run.
    for uri in [
        "/api/v3/repos/owner/repo/actions/runs/NaN".to_owned(),
        "/repos/owner/repo/actions/runs/NaN".to_owned(),
        format!("/api/v3/repos/owner/repo/actions/runs/{run_id}"),
    ] {
        let (status, body) = send(&fixture.app, Method::GET, &uri, Some(&token), "", vec![]).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        assert_eq!(body["path"], ".github/workflows/codeql.yml", "{uri}");
        assert!(body["workflow_url"].is_null(), "path fallback must be used");
    }

    let (status, _) = send(
        &fixture.app,
        Method::GET,
        "/api/v3/repos/other/repo/actions/runs/NaN",
        Some(&token),
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = send(
        &fixture.app,
        Method::GET,
        "/api/v3/repos/owner/repo/actions/runs/NaN",
        None,
        "",
        vec![],
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
