//! A mock Actions runner speaking the real broker protocol: register,
//! OAuth (PS256 client assertion), session, long-poll, acquire, timeline
//! updates per step, lease renewal, completion. No VM, no job execution:
//! each job "runs" for a log-normal duration.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use preloop_gha_protocol::crypto::{sign_jwt_ps256, AgentRsaKeypair};
use rand::Rng;
use serde_json::{json, Value};

use crate::metrics::Metrics;

/// How mock jobs behave.
#[derive(Clone)]
pub struct JobModel {
    /// Median simulated job duration.
    pub median: Duration,
    /// Log-normal sigma (spread).
    pub sigma: f64,
    /// Timeline PATCHes per job (step start/finish batches).
    pub timeline_updates: u32,
    /// Lease renewal interval.
    pub renew_every: Duration,
    /// Probability a job reports failure.
    pub failure_rate: f64,
}

/// A log-normal draw with parameters `mu` (log-median) and `sigma`: a
/// standard normal sample by Box-Muller, exponentiated. `rand_distr`'s
/// `LogNormal` is not used because that crate is unvetted in this workspace.
fn log_normal_seconds(rng: &mut impl Rng, mu: f64, sigma: f64) -> f64 {
    let u1: f64 = rng.gen::<f64>().max(f64::MIN_POSITIVE);
    let u2: f64 = rng.gen();
    let normal = (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos();
    (mu + sigma * normal).exp()
}

pub struct Runner {
    pub http: reqwest::Client,
    pub base: String,
    pub system_token: String,
    pub name: String,
    pub metrics: Arc<Metrics>,
    pub model: JobModel,
}

struct Identity {
    runner_id: i64,
    listen_token: String,
    session_id: String,
}

impl Runner {
    /// Run until `deadline`, re-registering after any protocol failure
    /// (a restarted engine node answers "session expired").
    pub async fn run(self, deadline: Instant) {
        let mut identity: Option<Identity> = None;
        while Instant::now() < deadline {
            // Reconnect with the existing registration first; register anew
            // only when the server no longer accepts it (runner purged).
            if let Some(current) = identity.as_mut() {
                match self
                    .open_session(current.runner_id, &current.listen_token)
                    .await
                {
                    Ok(session_id) => {
                        current.session_id = session_id;
                        self.metrics.incr("runner.reconnected");
                    }
                    Err(_) => identity = None,
                }
            }
            if identity.is_none() {
                match self.register().await {
                    Ok(registered) => {
                        self.metrics.incr("runner.registered");
                        identity = Some(registered);
                    }
                    Err(error) => {
                        self.metrics.incr("runner.register_error");
                        eprintln!("[{}] register: {error:#}", self.name);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
            let current = identity.as_ref().expect("identity set above");
            if let Err(error) = self.serve(current, deadline).await {
                self.metrics.incr("runner.session_lost");
                eprintln!("[{}] session: {error:#}", self.name);
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }

    async fn register(&self) -> Result<Identity> {
        let keypair = AgentRsaKeypair::generate()?;
        let params = keypair.to_rsaparams();
        let agent: Value = self
            .http
            .post(format!(
                "{}/runner/server/_apis/distributedtask/pools/1/agents",
                self.base
            ))
            .bearer_auth(&self.system_token)
            .json(&json!({
                "name": self.name,
                "version": "2.335.1",
                "osDescription": "Linux load",
                "labels": [
                    {"name": "self-hosted", "type": "system"},
                    {"name": "Linux", "type": "system"},
                    {"name": "X64", "type": "system"}
                ],
                "authorization": {"publicKey": {"modulus": params.modulus, "exponent": params.exponent}}
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let runner_id = agent["id"].as_i64().context("agent id")?;
        let client_id = agent["authorization"]["clientId"]
            .as_str()
            .context("clientId")?
            .to_owned();

        let now = chrono::Utc::now().timestamp();
        let assertion = sign_jwt_ps256(
            &json!({"typ": "JWT", "alg": "PS256"}),
            &json!({
                "sub": client_id, "iss": client_id,
                "aud": "https://preloop.local/oauth",
                "jti": uuid::Uuid::new_v4().to_string(),
                "nbf": now, "exp": now + 300,
            }),
            &params,
        )?;
        let token: Value = self
            .http
            .post(format!("{}/runner/server/_apis/v1/oauth2/token", self.base))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(serde_urlencoded::to_string([
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", assertion.as_str()),
                ("grant_type", "client_credentials"),
            ])?)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let listen_token = token["access_token"]
            .as_str()
            .context("access_token")?
            .to_owned();
        let session_id = self.open_session(runner_id, &listen_token).await?;
        Ok(Identity {
            runner_id,
            listen_token,
            session_id,
        })
    }

    /// Open a session for an existing registration. What a real runner does
    /// after its session is lost: it keeps its credentials.
    async fn open_session(&self, runner_id: i64, listen_token: &str) -> Result<String> {
        let session: Value = self
            .http
            .post(format!(
                "{}/runner/server/_apis/distributedtask/pools/1/sessions",
                self.base
            ))
            .bearer_auth(listen_token)
            .json(&json!({
                "agent": {"id": runner_id, "name": self.name, "version": "2.335.1"},
                "ownerName": self.name,
                "sessionId": "00000000-0000-0000-0000-000000000000",
                "useFipsEncryption": false
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(session["sessionId"]
            .as_str()
            .context("sessionId")?
            .to_owned())
    }

    async fn serve(&self, id: &Identity, deadline: Instant) -> Result<()> {
        while Instant::now() < deadline {
            let polled = Instant::now();
            let response = self
                .http
                .get(format!(
                    "{}/runner/server/_apis/distributedtask/pools/1/messages?sessionId={}&status=Online&runnerVersion=2.335.1&os=Linux&architecture=X64&waitSeconds=20",
                    self.base, id.session_id
                ))
                .bearer_auth(&id.listen_token)
                .timeout(Duration::from_secs(40))
                .send()
                .await?;
            let status = response.status();
            if !status.is_success() {
                bail!("poll status {status}");
            }
            let text = response.text().await?;
            let message: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            if message["messageType"] != "RunnerJobRequest" {
                self.metrics.incr("runner.empty_poll");
                continue;
            }
            self.metrics.latency("poll_with_job", polled);
            let body: Value = serde_json::from_str(message["body"].as_str().unwrap_or("{}"))?;
            let request_id = body["runner_request_id"]
                .as_str()
                .context("runner_request_id")?
                .to_owned();
            self.execute(id, &request_id).await?;
        }
        Ok(())
    }

    async fn execute(&self, id: &Identity, request_id: &str) -> Result<()> {
        let started = Instant::now();
        let response = self
            .http
            .post(format!("{}/broker/{}/acquirejob", self.base, id.runner_id))
            .bearer_auth(&id.listen_token)
            .json(
                &json!({"jobMessageId": request_id, "billingOwnerId": "load", "runnerOS": "Linux"}),
            )
            .send()
            .await?;
        if !response.status().is_success() {
            self.metrics.incr("job.acquire_error");
            self.metrics
                .incr(&format!("job.acquire_error.{}", response.status().as_u16()));
            return Ok(());
        }
        let job: Value = response.json().await?;
        self.metrics.latency("acquire", started);
        self.metrics.incr("job.acquired");
        let agent_job_id = job["jobId"].as_str().context("jobId")?.to_owned();
        let plan_id = job["plan"]["planId"].as_str().context("planId")?.to_owned();
        let timeline_id = job["timeline"]["id"]
            .as_str()
            .context("timeline id")?
            .to_owned();
        let runtime = job["resources"]["endpoints"][0]["authorization"]["parameters"]
            ["AccessToken"]
            .as_str()
            .context("runtime token")?
            .to_owned();

        let duration = {
            let mut rng = rand::thread_rng();
            let median = self.model.median.as_secs_f64();
            Duration::from_secs_f64(
                log_normal_seconds(&mut rng, median.ln(), self.model.sigma)
                    .clamp(0.05, median * 20.0),
            )
        };
        let updates = self.model.timeline_updates.max(1);
        let tick = duration / updates;
        let mut last_renew = Instant::now();
        for step in 0..updates {
            tokio::time::sleep(tick).await;
            let patched = Instant::now();
            let now = chrono::Utc::now().to_rfc3339();
            let record = json!({"count": 1, "value": [{
                "id": uuid::Uuid::from_u128(step as u128 + 1).to_string(),
                "name": format!("step {step}"),
                "type": "Task",
                "order": step + 1,
                "state": "completed",
                "result": "succeeded",
                "startTime": now,
                "finishTime": now,
            }]});
            match self
                .http
                .patch(format!(
                    "{}/_apis/v1/Timeline/scope/actions/{plan_id}/{timeline_id}",
                    self.base
                ))
                .bearer_auth(&runtime)
                .json(&record)
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => {
                    self.metrics.latency("timeline_patch", patched);
                    self.metrics.incr("job.timeline_patch");
                }
                Ok(r) => {
                    self.metrics.incr("job.timeline_error");
                    self.metrics
                        .incr(&format!("job.timeline_error.{}", r.status().as_u16()));
                }
                Err(_) => {
                    self.metrics.incr("job.timeline_error");
                    self.metrics.incr("job.timeline_error.transport");
                }
            }
            if last_renew.elapsed() >= self.model.renew_every {
                last_renew = Instant::now();
                let renewed = Instant::now();
                match self
                    .http
                    .post(format!("{}/broker/{}/renewjob", self.base, id.runner_id))
                    .bearer_auth(&runtime)
                    .json(&json!({"jobId": agent_job_id, "planId": plan_id}))
                    .send()
                    .await
                {
                    Ok(r) if r.status().is_success() => {
                        self.metrics.latency("renew", renewed);
                        self.metrics.incr("job.renewed");
                    }
                    _ => self.metrics.incr("job.renew_error"),
                }
            }
        }
        let conclusion = if rand::thread_rng().gen_bool(self.model.failure_rate) {
            "failed"
        } else {
            "succeeded"
        };
        let completed = Instant::now();
        let response = self
            .http
            .post(format!("{}/broker/{}/completejob", self.base, id.runner_id))
            .bearer_auth(&runtime)
            .json(&json!({"jobId": agent_job_id, "planId": plan_id, "conclusion": conclusion}))
            .send()
            .await?;
        if response.status().is_success() {
            self.metrics.latency("complete", completed);
            self.metrics.incr("job.completed");
        } else {
            self.metrics.incr("job.complete_error");
            self.metrics.incr(&format!(
                "job.complete_error.{}",
                response.status().as_u16()
            ));
        }
        self.metrics.latency("job_wall", started);
        Ok(())
    }
}
