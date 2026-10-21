use super::*;

#[derive(Debug, Deserialize)]
pub struct OidcTokenQuery {
    pub audience: Option<String>,
    #[serde(rename = "api-version")]
    pub _api_version: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct OidcTokenResponse {
    pub value: String,
}

/// `GET /runner/server/_apis/distributedtask/hubs/actions/plans/:plan_id/jobs/:job_id/oidctoken`
///
/// Mints a GitHub-compatible RS256-signed OIDC id-token JWT. Looks up the
/// originating workflow run to populate claims, and enforces `id-token: write`.
pub async fn oidc_token_run_service(
    State(shared): State<Arc<SharedState>>,
    Path((_orchestration_id, plan_id, job_id)): Path<(String, String, String)>,
    Query(query): Query<OidcTokenQuery>,
    headers: HeaderMap,
) -> Result<Json<OidcTokenResponse>, ApiError> {
    oidc_token(
        State(shared),
        Path((plan_id, job_id)),
        Query(query),
        headers,
    )
    .await
}

pub async fn oidc_token(
    State(shared): State<Arc<SharedState>>,
    Path((plan_id, job_id)): Path<(String, String)>,
    Query(query): Query<OidcTokenQuery>,
    headers: HeaderMap,
) -> Result<Json<OidcTokenResponse>, ApiError> {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError::unauthorized("OIDC bearer token required"))?;
    let identity = crate::auth::results_identity(&shared.state, bearer)
        .map_err(|_| ApiError::forbidden("OIDC runtime token is not bound to this job"))?;
    if !crate::auth::results_identity_binds_job(&identity, &plan_id, &job_id) {
        return Err(ApiError::forbidden(
            "OIDC runtime token is not bound to this job",
        ));
    }
    let requested_job_id = job_id
        .parse::<uuid::Uuid>()
        .map_err(|_| ApiError::not_found("OIDC: plan and job do not match"))?;
    crate::auth::require_live_results_job(&shared.state, &identity).await?;
    // Authoritative lookups (plan binding, job request, id-token grant, OIDC
    // context, run) come from the backend; the signing keypair and issuer are
    // node-local.
    let crate::control::types::OidcGrant {
        run,
        job_id: resolved_job_id,
        granted,
        context: oidc_context,
    } = shared
        .state
        .backend
        .oidc_grant(&plan_id, requested_job_id)
        .await?;
    let run_id = run.run_id;
    if !granted {
        return Err(ApiError::forbidden(
            "id-token: write permission is required to request an OIDC token",
        ));
    }

    // Get the OIDC signing keypair (node-local crypto).
    let (oidc_kp, issuer) = {
        let inner = shared.state.inner.lock().await;
        let kp = inner
            .oidc_keypair
            .as_ref()
            .ok_or_else(|| ApiError::internal("OIDC signing keypair not available"))?
            .clone();
        (kp, oidc_issuer_url(&inner))
    };

    // Build claims from the run's submission and parser-resolved job context.
    let submission = &run.submission;
    let repository_owner = submission
        .repository
        .split('/')
        .next()
        .unwrap_or("owner")
        .to_string();

    // Use sha from submission first-class field, fallback to payload extraction.
    let sha = if submission.sha != "0000000000000000000000000000000000000000" {
        submission.sha.clone()
    } else {
        submission
            .payload
            .get("after")
            .and_then(|v| v.as_str())
            .or_else(|| {
                submission
                    .payload
                    .get("pull_request")
                    .and_then(|pr| pr.get("head"))
                    .and_then(|h| h.get("sha"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("0000000000000000000000000000000000000000")
            .to_string()
    };

    // Use actor from submission first-class field, fallback to payload extraction.
    let actor = if submission.actor != "preloop-system" {
        submission.actor.clone()
    } else {
        submission
            .payload
            .get("pusher")
            .and_then(|p| p.get("name"))
            .and_then(|v| v.as_str())
            .or_else(|| {
                submission
                    .payload
                    .get("sender")
                    .and_then(|s| s.get("login"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("preloop-system")
            .to_string()
    };

    // Extract actor_id from payload if available.
    let actor_id = submission
        .payload
        .get("sender")
        .and_then(|s| s.get("id"))
        .and_then(|v| v.as_u64())
        .map(|id| id.to_string())
        .unwrap_or_default();

    // Extract repository_id and repository_owner_id from payload.
    let repository_id = submission
        .payload
        .get("repository")
        .and_then(|r| r.get("id"))
        .and_then(|v| v.as_u64())
        .map(|id| id.to_string())
        .unwrap_or_default();
    let repository_owner_id = submission
        .payload
        .get("repository")
        .and_then(|r| r.get("owner"))
        .and_then(|o| o.get("id"))
        .and_then(|v| v.as_u64())
        .map(|id| id.to_string())
        .unwrap_or_default();
    let repository_visibility = submission
        .payload
        .get("repository")
        .and_then(|repository| repository.get("visibility"))
        .and_then(|value| value.as_str())
        .unwrap_or("private")
        .to_owned();

    let workflow_name = parse_workflow(&submission.workflow_yaml)
        .ok()
        .and_then(|w| w.name)
        .unwrap_or_default();

    // Derive the workflow filename: explicit > parsed from YAML > "workflow.yml"
    let workflow_file = submission
        .workflow_file
        .clone()
        .unwrap_or_else(|| "workflow.yml".to_owned());

    let head_ref = submission
        .payload
        .get("pull_request")
        .and_then(|pr| pr.get("head"))
        .and_then(|h| h.get("ref"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let base_ref = submission
        .payload
        .get("pull_request")
        .and_then(|pr| pr.get("base"))
        .and_then(|b| b.get("ref"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let check_run_id = run.job_check_run_ids.get(&resolved_job_id).copied();
    let workflow_ref = format!(
        "{}/.github/workflows/{}@{}",
        submission.repository, workflow_file, submission.git_ref
    );

    let job_workflow_ref = oidc_context.job_workflow_ref.as_deref().map(|reference| {
        format_reusable_workflow_ref(&submission.repository, reference, &submission.git_ref)
    });
    let job_workflow_sha = oidc_context.job_workflow_sha.clone().or_else(|| {
        job_workflow_ref
            .as_ref()
            .and_then(|reference| {
                reference
                    .rsplit_once('@')
                    .map(|(_, git_ref)| git_ref)
                    .filter(|git_ref| preloop_gha_protocol::git_ref::is_commit_sha(git_ref))
                    .map(str::to_owned)
            })
            .or_else(|| job_workflow_ref.as_ref().map(|_| sha.clone()))
    });

    let claims_input = oidc::OidcClaimsInput {
        repository: submission.repository.clone(),
        repository_owner,
        git_ref: submission.git_ref.clone(),
        event_name: submission.event.clone(),
        sha: sha.clone(),
        actor,
        actor_id,
        workflow: workflow_name,
        run_id: run_id.to_string(),
        run_number: run.run_number.to_string(),
        run_attempt: run.run_attempt.to_string(),
        head_ref,
        base_ref,
        ref_protected: submission.ref_protected,
        environment: oidc_context.environment,
        repository_visibility,
        repository_id,
        repository_owner_id,
        workflow_ref: Some(workflow_ref),
        workflow_sha: Some(sha),
        job_workflow_ref,
        job_workflow_sha,
    };

    let audience = query
        .audience
        .unwrap_or_else(|| oidc::default_audience(&claims_input.repository_owner));

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ApiError::bad_request(format!("system clock before epoch: {error}")))?
        .as_secs();

    let mut claims = oidc::build_claims(&claims_input, &audience, &issuer, now);
    if let Some(check_run_id) = check_run_id {
        claims["check_run_id"] = json!(check_run_id.to_string());
    }

    let jwt = oidc_kp
        .sign_jwt(&claims)
        .map_err(|e| ApiError::internal(format!("OIDC token signing failed: {e}")))?;

    // The id_token is a bearer credential: a step that echoes it into the job
    // log discloses it just as surely as a raw secret value. Register it into
    // the job's server-side mask scope before the response leaves this
    // handler — once the job can see the token, every mask pass over its log
    // output (AzDO appends, live-feed batches, persisted log uploads) already
    // redacts it.
    crate::timeline_logs::register_runtime_mask_value(&shared, &plan_id, &run_id.to_string(), &jwt)
        .await;

    Ok(Json(OidcTokenResponse { value: jwt }))
}

/// `GET /.well-known/openid-configuration` — OIDC discovery document.
pub async fn oidc_discovery(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let issuer = oidc_issuer_url(&inner);
    let jwks_uri = format!("{issuer}/.well-known/jwks");
    Ok(Json(oidc::discovery_document(&issuer, &jwks_uri)))
}

/// `GET /.well-known/jwks` — JSON Web Key Set for OIDC token verification.
pub async fn oidc_jwks(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let inner = shared.state.inner.lock().await;
    let kp = inner
        .oidc_keypair
        .as_ref()
        .ok_or_else(|| ApiError::internal("OIDC keypair not available"))?;
    Ok(Json(kp.jwks()))
}

pub fn base64_url_json(value: &serde_json::Value) -> Result<String, ApiError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| ApiError::bad_request(format!("failed to encode jwt json: {error}")))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::{Method, StatusCode, header};
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt as _;

    const OIDC_WORKFLOW: &str = "on: push\npermissions:\n  id-token: write\n  contents: read\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n";

    /// A submitted run with one `id-token: write` job, driven through the real
    /// HTTP surface: the AzDO log append path and the signed replay-log
    /// upload path, plus the OIDC mint endpoint.
    struct OidcJob {
        app: axum::Router,
        state: AppState,
        temp: tempfile::TempDir,
        plan_id: String,
        agent_job_id: uuid::Uuid,
        runtime_token: String,
    }

    impl OidcJob {
        async fn submit() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let state = AppState::new(temp.path().to_path_buf()).await.unwrap();
            let app = crate::routes::app(state.clone(), CancellationToken::new());
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/api/v1/runs")
                        .header(
                            header::AUTHORIZATION,
                            format!("Bearer {DEFAULT_PRELOOP_SYSTEM_TOKEN}"),
                        )
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            serde_json::json!({
                                "workflow_yaml": OIDC_WORKFLOW,
                                "event": "push",
                                "repository": "owner/repo",
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(
                response.status().is_success(),
                "submitting the run must succeed: {}",
                response.status()
            );

            let (plan_id, agent_job_id) = {
                let tx = state.test_tx().await;
                tx.ready()
                    .next()
                    .or_else(|| tx.pending_jobs.front())
                    .map(|job| (job.message.plan.plan_id.clone(), job.message.job_id))
                    .expect("the submitted job must be queued")
            };
            let runtime_token = state.mint_runtime_token(&plan_id, &agent_job_id);
            Self {
                app,
                state,
                temp,
                plan_id,
                agent_job_id,
                runtime_token,
            }
        }

        async fn send(&self, request: Request<Body>) -> axum::response::Response {
            self.app.clone().oneshot(request).await.unwrap()
        }

        /// GET the OIDC token endpoint for this job, as the runner's step
        /// would via `ACTIONS_ID_TOKEN_REQUEST_URL`.
        async fn mint(&self) -> String {
            let response = self
                .send(
                    Request::builder()
                        .method(Method::GET)
                        .uri(format!(
                            "/runner/server/_apis/distributedtask/hubs/actions/plans/{}/jobs/{}/oidctoken?audience=api://mask-test",
                            self.plan_id, self.agent_job_id
                        ))
                        .header(
                            header::AUTHORIZATION,
                            format!("Bearer {}", self.runtime_token),
                        )
                        .body(Body::empty())
                        .unwrap(),
                )
                .await;
            assert_eq!(response.status(), StatusCode::OK, "the mint must succeed");
            json_body(response).await["value"]
                .as_str()
                .expect("the mint response carries the token")
                .to_owned()
        }

        /// Create an AzDO log for the job; returns the log id.
        async fn create_log(&self) -> i64 {
            let response = self
                .send(
                    Request::builder()
                        .method(Method::POST)
                        .uri(format!("/_apis/v1/Logfiles/s/h/{}", self.plan_id))
                        .header(
                            header::AUTHORIZATION,
                            format!("Bearer {DEFAULT_PRELOOP_SYSTEM_TOKEN}"),
                        )
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            serde_json::json!({"path": "logs/step"}).to_string(),
                        ))
                        .unwrap(),
                )
                .await;
            assert_eq!(response.status(), StatusCode::OK);
            json_body(response).await["id"]
                .as_i64()
                .expect("created log id")
        }

        /// Append one chunk the way the runner streams a step's log.
        async fn append(&self, log_id: i64, chunk: &str) {
            let response = self
                .send(
                    Request::builder()
                        .method(Method::POST)
                        .uri(format!(
                            "/_apis/v1/Logfiles/s/h/{}/{}",
                            self.plan_id, log_id
                        ))
                        .header(
                            header::AUTHORIZATION,
                            format!("Bearer {DEFAULT_PRELOOP_SYSTEM_TOKEN}"),
                        )
                        .body(Body::from(chunk.to_owned()))
                        .unwrap(),
                )
                .await;
            assert_eq!(response.status(), StatusCode::ACCEPTED);
        }

        /// The server-side mask pass over a log is exactly what the persisted
        /// blob sees (`mask_secrets_transform_aware` against the resolved
        /// scope) — staging an upload is the faithful way to drive it.
        async fn upload_step_log(&self, name: &str, body: &str) -> String {
            let path = format!("{}/{}/{}", self.plan_id, self.agent_job_id, name);
            let full = format!("/replay/results/{path}");
            let expires_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600;
            let sig = crate::auth::sign_replay_upload_ticket(&self.state, &full, expires_at);
            let response = self
                .send(
                    Request::builder()
                        .method(Method::PUT)
                        .uri(format!("{full}?se={expires_at}&sig={sig}"))
                        .body(Body::from(body.to_owned()))
                        .unwrap(),
                )
                .await;
            assert_eq!(
                response.status(),
                StatusCode::CREATED,
                "the signed log upload must be accepted"
            );
            let stored = tokio::fs::read(self.temp.path().join("replay/results").join(&path))
                .await
                .expect("uploaded step log must be persisted");
            String::from_utf8(stored).unwrap()
        }

        /// The node-local preview of the streamed log, i.e. what
        /// `append_log` stored after masking.
        async fn streamed_log(&self, log_id: i64) -> String {
            let inner = self.state.inner.lock().await;
            let stored = inner
                .logs
                .get(&format!("{}/{}", self.plan_id, log_id))
                .cloned()
                .expect("the appended log must be retained node-locally");
            drop(inner);
            String::from_utf8(stored).unwrap()
        }
    }

    async fn json_body(response: axum::response::Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    }

    /// A minted id_token is a bearer credential; once it can reach the job,
    /// every server-side mask pass over that job's log output must redact it.
    /// The log stream is already running when the mint happens, so this also
    /// covers widening the in-flight streaming masker.
    #[tokio::test]
    async fn minted_id_token_is_redacted_by_the_server_side_log_masks() {
        let job = OidcJob::submit().await;
        let log_id = job.create_log().await;
        job.append(log_id, "warmup\n").await;

        // The mint happens mid-stream: the log's streaming masker already
        // exists and predates the token.
        let jwt = job.mint().await;
        assert!(
            jwt.starts_with("eyJ"),
            "expected a JWS compact token: {jwt}"
        );

        // The step echoes the token into its log; a masking-buggy uploader
        // persists it to the replay blob.
        job.append(log_id, &format!("echo {jwt}\n")).await;
        assert_eq!(
            job.streamed_log(log_id).await,
            "warmup\necho ***\n",
            "the streamed log must redact the id_token"
        );

        let persisted = job
            .upload_step_log("step-abc.txt", &format!("log {jwt}\n"))
            .await;
        assert_eq!(
            persisted, "log ***\n",
            "the persisted log must redact the id_token"
        );
    }

    /// Registering the id_token must widen the mask scope, not blanket it:
    /// unrelated log content passes both server-side mask passes unchanged.
    #[tokio::test]
    async fn mask_registration_leaves_unrelated_content_unchanged() {
        let job = OidcJob::submit().await;
        let log_id = job.create_log().await;
        job.append(log_id, "before\n").await;

        let jwt = job.mint().await;
        assert!(!jwt.is_empty());

        job.append(log_id, "hello world\nplain=42\n").await;
        assert_eq!(
            job.streamed_log(log_id).await,
            "before\nhello world\nplain=42\n"
        );

        let persisted = job
            .upload_step_log("plain.txt", "unrelated content\n")
            .await;
        assert_eq!(persisted, "unrelated content\n");
    }
}
