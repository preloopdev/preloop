//! HTTP client for the engine's native API, plus the NDJSON and SSE framing the
//! live feeds use.
//!
//! Every request carries the context's bearer token. The engine answers errors
//! with `{"error": "..."}`, which [`EngineError::Status`] surfaces as the message.
//! The streaming calls set no total deadline, because a run can last hours; the
//! client's idle read timeout is the only bound on them.

use std::collections::VecDeque;
use std::pin::Pin;
use std::time::Duration;

use futures::stream::{self, Stream};
use reqwest::{Client, RequestBuilder, Response, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::api::{
    AuditEntry, DebugSession, DebugSessions, FeedEvent, LiveLogChunk, OperationalSnapshot,
    PoolBindings, RunAccepted, RunRecord, RunnersList, SecretEntry, SecretList, WebhookDeliveries,
    WebhookHealth, parse_feed_line,
};
use crate::options::Secret;

/// How long a one-shot request may take in total.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
/// Idle bound on every connection, streams included. Matches the CLI's engine client.
const READ_TIMEOUT: Duration = Duration::from_secs(600);

/// Why an engine call failed.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The engine URL is not a usable http(s) base.
    #[error("invalid engine URL {url}: {reason}")]
    Url {
        /// The URL as given.
        url: String,
        /// What is wrong with it.
        reason: String,
    },
    /// The request never completed: refused, timed out, or reset.
    #[error("no engine at {url} ({reason})")]
    Unreachable {
        /// The engine base URL.
        url: String,
        /// The transport error.
        reason: String,
    },
    /// The engine answered with an error status.
    #[error("engine answered {status}: {message}")]
    Status {
        /// HTTP status code.
        status: u16,
        /// The engine's `error` message, or the start of the body.
        message: String,
    },
    /// The engine answered, but the body was not what this build expects.
    #[error("engine response this build cannot read: {0}")]
    Decode(String),
}

/// Filters for `GET /api/v1/runs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunsQuery {
    /// Workflow file name filter.
    pub workflow: Option<String>,
    /// Status filter, lowercase.
    pub status: Option<String>,
    /// Event filter.
    pub event: Option<String>,
    /// Maximum number of runs (the engine caps it at 200).
    pub limit: Option<usize>,
}

/// Engine client bound to one base URL and one token.
#[derive(Debug, Clone)]
pub struct EngineClient {
    base: Url,
    token: Option<Secret>,
    http: Client,
}

impl EngineClient {
    /// Build a client for `base` (for example `http://127.0.0.1:9090`).
    pub fn new(base: &str, token: Option<Secret>) -> Result<Self, EngineError> {
        let parsed = Url::parse(base).map_err(|error| EngineError::Url {
            url: base.to_owned(),
            reason: error.to_string(),
        })?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.cannot_be_a_base() {
            return Err(EngineError::Url {
                url: base.to_owned(),
                reason: "expected an http or https URL".to_owned(),
            });
        }
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .read_timeout(READ_TIMEOUT)
            .user_agent(concat!("preloop-tui/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|error| EngineError::Decode(error.to_string()))?;
        Ok(Self {
            base: parsed,
            token,
            http,
        })
    }

    /// The base URL as given at construction (normalised).
    pub fn base_url(&self) -> &str {
        self.base.as_str().trim_end_matches('/')
    }

    /// `GET /healthz`. Succeeds when the engine answers at all.
    pub async fn health(&self) -> Result<(), EngineError> {
        let url = self.url(&["healthz"]);
        let response = self.get(url).timeout(REQUEST_TIMEOUT).send().await;
        check(self.transport(response)?).await.map(|_| ())
    }

    /// `GET /api/v1/status`.
    pub async fn status(&self) -> Result<OperationalSnapshot, EngineError> {
        self.get_json(self.url(&["api", "v1", "status"]), &[]).await
    }

    /// `GET /api/v1/runs`, newest first.
    pub async fn runs(&self, query: &RunsQuery) -> Result<Vec<RunRecord>, EngineError> {
        let mut pairs: Vec<(&str, String)> = Vec::new();
        if let Some(value) = &query.workflow {
            pairs.push(("workflow", value.clone()));
        }
        if let Some(value) = &query.status {
            pairs.push(("status", value.clone()));
        }
        if let Some(value) = &query.event {
            pairs.push(("event", value.clone()));
        }
        if let Some(limit) = query.limit {
            pairs.push(("limit", limit.to_string()));
        }
        self.get_json(self.url(&["api", "v1", "runs"]), &pairs)
            .await
    }

    /// `GET /api/v1/runs/{id}`: the full record, with job details and steps.
    pub async fn run(&self, run_id: &str) -> Result<RunRecord, EngineError> {
        self.get_json(self.url(&["api", "v1", "runs", run_id]), &[])
            .await
    }

    /// `GET /api/v1/runs/{id}/logs`. Plain text. `step` is 1-based; it is the
    /// declared workflow index plus one.
    pub async fn run_logs(
        &self,
        run_id: &str,
        job: Option<&str>,
        step: Option<usize>,
    ) -> Result<String, EngineError> {
        let mut pairs: Vec<(&str, String)> = Vec::new();
        if let Some(job) = job {
            pairs.push(("job", job.to_owned()));
        }
        if let Some(step) = step {
            pairs.push(("step", step.to_string()));
        }
        let url = self.url(&["api", "v1", "runs", run_id, "logs"]);
        let response = self
            .get(url)
            .query(&pairs)
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await;
        let response = check(self.transport(response)?).await?;
        response
            .text()
            .await
            .map_err(|error| EngineError::Decode(error.to_string()))
    }

    /// `GET /api/v1/runs/{id}/events.ndjson`. The engine sends a snapshot first,
    /// then live events. The stream ends when the engine closes it.
    pub async fn events(&self, run_id: &str) -> Result<FeedStream, EngineError> {
        let url = self.url(&["api", "v1", "runs", run_id, "events.ndjson"]);
        let response = self.get(url).send().await;
        let response = check(self.transport(response)?).await?;
        Ok(ndjson_events(response))
    }

    /// `GET /api/v1/runs/{id}/logs/live?job=`: SSE `live-log` chunks for one job.
    pub async fn live_logs(&self, run_id: &str, job: &str) -> Result<LiveStream, EngineError> {
        let url = self.url(&["api", "v1", "runs", run_id, "logs", "live"]);
        let response = self
            .get(url)
            .query(&[("job", job)])
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send()
            .await;
        let response = check(self.transport(response)?).await?;
        Ok(sse_live_logs(response))
    }

    /// `POST /api/v1/runs/{id}/cancel`.
    pub async fn cancel_run(&self, run_id: &str) -> Result<RunRecord, EngineError> {
        let url = self.url(&["api", "v1", "runs", run_id, "cancel"]);
        self.send_json(self.post(url).timeout(REQUEST_TIMEOUT))
            .await
    }

    /// `POST /api/v1/runs/{id}/rerun`: a new attempt of every job.
    pub async fn rerun(&self, run_id: &str) -> Result<RunAccepted, EngineError> {
        let url = self.url(&["api", "v1", "runs", run_id, "rerun"]);
        self.send_json(self.post(url).timeout(REQUEST_TIMEOUT))
            .await
    }

    /// `POST /api/v1/runs/{id}/jobs/{job}/approve`.
    pub async fn approve_job(
        &self,
        run_id: &str,
        job_id: &str,
        note: Option<&str>,
    ) -> Result<Value, EngineError> {
        let url = self.url(&["api", "v1", "runs", run_id, "jobs", job_id, "approve"]);
        self.send_json(
            self.post(url)
                .json(&json!({ "note": note }))
                .timeout(REQUEST_TIMEOUT),
        )
        .await
    }

    /// `POST /api/v1/runs/{id}/approve-fork`: release a fork run held for approval.
    pub async fn approve_fork(
        &self,
        run_id: &str,
        note: Option<&str>,
    ) -> Result<Value, EngineError> {
        let url = self.url(&["api", "v1", "runs", run_id, "approve-fork"]);
        self.send_json(
            self.post(url)
                .json(&json!({ "note": note }))
                .timeout(REQUEST_TIMEOUT),
        )
        .await
    }

    /// `GET /api/v1/runners`, optionally only the runners serving one run.
    pub async fn runners(&self, run_id: Option<&str>) -> Result<RunnersList, EngineError> {
        let pairs: Vec<(&str, String)> = run_id
            .map(|id| vec![("run_id", id.to_owned())])
            .unwrap_or_default();
        self.get_json(self.url(&["api", "v1", "runners"]), &pairs)
            .await
    }

    /// `POST /api/v1/runners/purge` with `{"name": ...}`.
    pub async fn purge_runner(&self, name: &str) -> Result<Value, EngineError> {
        let url = self.url(&["api", "v1", "runners", "purge"]);
        self.send_json(
            self.post(url)
                .json(&json!({ "name": name }))
                .timeout(REQUEST_TIMEOUT),
        )
        .await
    }

    /// `GET /api/v1/pool/bindings`: which job holds which runner, or waits.
    pub async fn pool_bindings(&self) -> Result<PoolBindings, EngineError> {
        self.get_json(self.url(&["api", "v1", "pool", "bindings"]), &[])
            .await
    }

    /// `GET /api/v1/webhooks/deliveries`. `state` is `received`, `processing`,
    /// `done` or `failed`.
    pub async fn webhook_deliveries(
        &self,
        state: Option<&str>,
        limit: usize,
    ) -> Result<WebhookDeliveries, EngineError> {
        let mut pairs: Vec<(&str, String)> = vec![("limit", limit.to_string())];
        if let Some(state) = state {
            pairs.push(("state", state.to_owned()));
        }
        self.get_json(self.url(&["api", "v1", "webhooks", "deliveries"]), &pairs)
            .await
    }

    /// `POST /api/v1/webhooks/deliveries/{id}/replay`.
    pub async fn replay_delivery(&self, delivery_id: &str) -> Result<Value, EngineError> {
        let url = self.url(&["api", "v1", "webhooks", "deliveries", delivery_id, "replay"]);
        self.send_json(self.post(url).timeout(REQUEST_TIMEOUT))
            .await
    }

    /// `GET /api/v1/webhooks/health`.
    pub async fn webhook_health(&self) -> Result<WebhookHealth, EngineError> {
        self.get_json(self.url(&["api", "v1", "webhooks", "health"]), &[])
            .await
    }

    /// `GET /api/v1/debug/sessions`.
    pub async fn debug_sessions(&self) -> Result<Vec<DebugSession>, EngineError> {
        let list: DebugSessions = self
            .get_json(self.url(&["api", "v1", "debug", "sessions"]), &[])
            .await?;
        Ok(list.sessions)
    }

    /// `GET /api/v1/debug/sessions/{id}`. The id may be a unique prefix.
    pub async fn debug_session(&self, session_id: &str) -> Result<DebugSession, EngineError> {
        self.get_json(
            self.url(&["api", "v1", "debug", "sessions", session_id]),
            &[],
        )
        .await
    }

    /// `GET /api/v1/debug/sessions/{id}/audit`.
    pub async fn debug_audit(&self, session_id: &str) -> Result<Vec<AuditEntry>, EngineError> {
        self.get_json(
            self.url(&["api", "v1", "debug", "sessions", session_id, "audit"]),
            &[],
        )
        .await
    }

    /// Execute one controller operation through the lease-gated debug API.
    pub async fn debug_operation(
        &self,
        session_id: &str,
        operation: &str,
        step_index: Option<usize>,
    ) -> Result<Value, EngineError> {
        let lease_url = self.url(&["api", "v1", "debug", "sessions", session_id, "lease"]);
        let operations_url =
            self.url(&["api", "v1", "debug", "sessions", session_id, "operations"]);
        let lease: Value = self
            .send_json(
                self.post(lease_url.clone())
                .json(&json!({
                    "controller": "preloop-tui",
                    "capabilities": ["step.retry", "job.retry_from", "job.continue", "job.abort"]
                }))
                .timeout(REQUEST_TIMEOUT),
            )
            .await?;
        let lease_id = lease
            .get("lease_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let version = lease
            .get("session_version")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let mut body = json!({
            "request_id": format!("tui-{}-{}", session_id, version),
            "expected_version": version,
            "lease_id": lease_id,
            "operation": operation,
        });
        if let Some(index) = step_index {
            body["step_index"] = json!(index);
        }
        let response = self
            .send_json(
                self.post(operations_url)
                    .json(&body)
                    .timeout(REQUEST_TIMEOUT),
            )
            .await;
        let _ = self
            .send_json::<Value>(
                self.authorize(self.http.delete(lease_url))
                    .json(&json!({"lease_id": lease_id}))
                    .timeout(REQUEST_TIMEOUT),
            )
            .await;
        response
    }

    /// `GET /api/v1/secrets`, optionally for one repository. Names only.
    pub async fn secrets(&self, repo: Option<&str>) -> Result<Vec<SecretEntry>, EngineError> {
        let pairs: Vec<(&str, String)> = repo
            .map(|r| vec![("repo", r.to_owned())])
            .unwrap_or_default();
        let list: SecretList = self
            .get_json(self.url(&["api", "v1", "secrets"]), &pairs)
            .await?;
        Ok(list.secrets)
    }

    /// `PUT /api/v1/secrets/{name}`. The value is sent once and never returned.
    pub async fn set_secret(
        &self,
        name: &str,
        value: &str,
        repo: Option<&str>,
        env: Option<&str>,
    ) -> Result<(), EngineError> {
        #[derive(Serialize)]
        struct Body<'a> {
            value: &'a str,
            repo: Option<&'a str>,
            env: Option<&'a str>,
        }
        let url = self.url(&["api", "v1", "secrets", name]);
        let body = Body { value, repo, env };
        let request = self.put(url).json(&body).timeout(REQUEST_TIMEOUT);
        let _: Value = self.send_json(request).await?;
        Ok(())
    }

    /// `DELETE /api/v1/secrets/{name}`.
    pub async fn delete_secret(
        &self,
        name: &str,
        repo: Option<&str>,
        env: Option<&str>,
    ) -> Result<(), EngineError> {
        let mut pairs: Vec<(&str, String)> = Vec::new();
        if let Some(repo) = repo {
            pairs.push(("repo", repo.to_owned()));
        }
        if let Some(env) = env {
            pairs.push(("env", env.to_owned()));
        }
        let url = self.url(&["api", "v1", "secrets", name]);
        let request = self.http.delete(url).query(&pairs).timeout(REQUEST_TIMEOUT);
        let request = self.authorize(request);
        let _: Value = self.send_json(request).await?;
        Ok(())
    }

    fn url(&self, segments: &[&str]) -> Url {
        let mut url = self.base.clone();
        if let Ok(mut path) = url.path_segments_mut() {
            path.pop_if_empty().extend(segments);
        }
        url
    }

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => request.bearer_auth(token.expose()),
            None => request,
        }
    }

    fn get(&self, url: Url) -> RequestBuilder {
        self.authorize(self.http.get(url))
    }

    fn post(&self, url: Url) -> RequestBuilder {
        self.authorize(self.http.post(url))
    }

    fn put(&self, url: Url) -> RequestBuilder {
        self.authorize(self.http.put(url))
    }

    fn transport(
        &self,
        response: Result<Response, reqwest::Error>,
    ) -> Result<Response, EngineError> {
        response.map_err(|error| EngineError::Unreachable {
            url: self.base_url().to_owned(),
            reason: describe_transport(&error),
        })
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        url: Url,
        query: &[(&str, String)],
    ) -> Result<T, EngineError> {
        let request = self.get(url).query(query).timeout(REQUEST_TIMEOUT);
        self.send_json(request).await
    }

    async fn send_json<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
    ) -> Result<T, EngineError> {
        let response = self.transport(request.send().await)?;
        let response = check(response).await?;
        let bytes = response
            .bytes()
            .await
            .map_err(|error| EngineError::Decode(error.to_string()))?;
        if bytes.is_empty() {
            return serde_json::from_value(Value::Null)
                .map_err(|error| EngineError::Decode(error.to_string()));
        }
        serde_json::from_slice(&bytes).map_err(|error| EngineError::Decode(error.to_string()))
    }
}

/// Describe a transport error without the URL, which the caller already has.
fn describe_transport(error: &reqwest::Error) -> String {
    if error.is_connect() {
        "connection refused".to_owned()
    } else if error.is_timeout() {
        "timed out".to_owned()
    } else {
        error.to_string()
    }
}

/// Turn a non-2xx response into [`EngineError::Status`], keeping the engine's
/// `error` message when the body has one.
async fn check(response: Response) -> Result<Response, EngineError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response.text().await.unwrap_or_default();
    let message = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| body.trim().chars().take(200).collect());
    Err(EngineError::Status {
        status: status.as_u16(),
        message,
    })
}

/// A stream of parsed `events.ndjson` records.
pub type FeedStream = Pin<Box<dyn Stream<Item = Result<FeedEvent, EngineError>> + Send>>;

/// A stream of SSE `live-log` chunks.
pub type LiveStream = Pin<Box<dyn Stream<Item = Result<LiveLogChunk, EngineError>> + Send>>;

/// Splits a byte stream into NDJSON lines. Chunks may end anywhere, including
/// inside a multi-byte character; a line is only decoded once its newline arrives.
#[derive(Debug, Default)]
pub struct NdjsonDecoder {
    buffer: Vec<u8>,
}

impl NdjsonDecoder {
    /// Feed bytes; return every line they complete. Blank lines are dropped.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<String> {
        self.buffer.extend_from_slice(bytes);
        let mut lines = Vec::new();
        let mut start = 0;
        while let Some(offset) = self.buffer[start..].iter().position(|byte| *byte == b'\n') {
            let end = start + offset;
            let line = String::from_utf8_lossy(&self.buffer[start..end]);
            let line = line.trim();
            if !line.is_empty() {
                lines.push(line.to_owned());
            }
            start = end + 1;
        }
        self.buffer.drain(..start);
        lines
    }

    /// The unterminated tail at end of stream, if any.
    pub fn finish(&mut self) -> Option<String> {
        let tail = String::from_utf8_lossy(&self.buffer).trim().to_owned();
        self.buffer.clear();
        (!tail.is_empty()).then_some(tail)
    }
}

/// One Server-Sent Event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// The `event:` field, `message` when absent.
    pub event: String,
    /// The `data:` lines joined with `\n`.
    pub data: String,
}

/// Splits a byte stream into Server-Sent Events. Handles chunk boundaries
/// anywhere, `\r\n` endings, comment lines, and multi-line `data:` fields.
#[derive(Debug, Default)]
pub struct SseDecoder {
    buffer: Vec<u8>,
    event: Option<String>,
    data: Vec<String>,
}

impl SseDecoder {
    /// Feed bytes; return every event they complete.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<SseEvent> {
        self.buffer.extend_from_slice(bytes);
        let mut events = Vec::new();
        let mut start = 0;
        while let Some(offset) = self.buffer[start..].iter().position(|byte| *byte == b'\n') {
            let end = start + offset;
            let raw = String::from_utf8_lossy(&self.buffer[start..end]).into_owned();
            start = end + 1;
            let line = raw.strip_suffix('\r').unwrap_or(&raw);
            if line.is_empty() {
                if self.event.is_some() || !self.data.is_empty() {
                    events.push(SseEvent {
                        event: self.event.take().unwrap_or_else(|| "message".to_owned()),
                        data: std::mem::take(&mut self.data).join("\n"),
                    });
                }
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            let (field, value) = match line.split_once(':') {
                Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
                None => (line, ""),
            };
            match field {
                "event" => self.event = Some(value.to_owned()),
                "data" => self.data.push(value.to_owned()),
                _ => {}
            }
        }
        self.buffer.drain(..start);
        events
    }
}

struct LineState {
    response: Response,
    decoder: NdjsonDecoder,
    pending: VecDeque<String>,
    finished: bool,
}

fn ndjson_events(response: Response) -> FeedStream {
    let state = LineState {
        response,
        decoder: NdjsonDecoder::default(),
        pending: VecDeque::new(),
        finished: false,
    };
    Box::pin(stream::unfold(state, |mut state| async move {
        loop {
            if let Some(line) = state.pending.pop_front() {
                match parse_feed_line(&line) {
                    Ok(event) => return Some((Ok(event), state)),
                    Err(error) => {
                        tracing::warn!(%error, "skipping malformed events.ndjson line");
                        continue;
                    }
                }
            }
            if state.finished {
                return None;
            }
            match state.response.chunk().await {
                Ok(Some(bytes)) => state.pending.extend(state.decoder.push(&bytes)),
                Ok(None) => {
                    state.finished = true;
                    state.pending.extend(state.decoder.finish());
                }
                Err(error) => {
                    state.finished = true;
                    let failure = EngineError::Unreachable {
                        url: "events.ndjson".to_owned(),
                        reason: describe_transport(&error),
                    };
                    return Some((Err(failure), state));
                }
            }
        }
    }))
}

struct SseState {
    response: Response,
    decoder: SseDecoder,
    pending: VecDeque<SseEvent>,
    finished: bool,
}

fn sse_live_logs(response: Response) -> LiveStream {
    let state = SseState {
        response,
        decoder: SseDecoder::default(),
        pending: VecDeque::new(),
        finished: false,
    };
    Box::pin(stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                if event.event != "live-log" {
                    continue;
                }
                match serde_json::from_str::<LiveLogChunk>(&event.data) {
                    Ok(chunk) => return Some((Ok(chunk), state)),
                    Err(error) => {
                        tracing::warn!(%error, "skipping malformed live-log event");
                        continue;
                    }
                }
            }
            if state.finished {
                return None;
            }
            match state.response.chunk().await {
                Ok(Some(bytes)) => state.pending.extend(state.decoder.push(&bytes)),
                Ok(None) => state.finished = true,
                Err(error) => {
                    state.finished = true;
                    let failure = EngineError::Unreachable {
                        url: "live-log".to_owned(),
                        reason: describe_transport(&error),
                    };
                    return Some((Err(failure), state));
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed_one_byte_at_a_time(text: &str) -> Vec<String> {
        let mut decoder = NdjsonDecoder::default();
        let mut lines = Vec::new();
        for byte in text.as_bytes() {
            lines.extend(decoder.push(std::slice::from_ref(byte)));
        }
        lines.extend(decoder.finish());
        lines
    }

    #[test]
    fn ndjson_lines_split_across_chunks() {
        let mut decoder = NdjsonDecoder::default();
        assert!(decoder.push(br#"{"type":"run_st"#).is_empty());
        let lines = decoder.push(b"atus\"}\n{\"type\":\"job_status\"}\n{\"ty");
        assert_eq!(
            lines,
            vec![r#"{"type":"run_status"}"#, r#"{"type":"job_status"}"#]
        );
        assert_eq!(decoder.finish(), Some(r#"{"ty"#.to_owned()));
    }

    #[test]
    fn ndjson_survives_multibyte_characters_split_across_chunks() {
        let text = "{\"type\":\"log\",\"line\":\"✓ done ⊘\"}\n";
        let lines = feed_one_byte_at_a_time(text);
        assert_eq!(
            lines,
            vec!["{\"type\":\"log\",\"line\":\"✓ done ⊘\"}".to_owned()]
        );
    }

    #[test]
    fn ndjson_skips_blank_lines() {
        let mut decoder = NdjsonDecoder::default();
        assert_eq!(decoder.push(b"\n\n{\"a\":1}\r\n\n"), vec![r#"{"a":1}"#]);
    }

    fn sse_all(chunks: &[&[u8]]) -> Vec<SseEvent> {
        let mut decoder = SseDecoder::default();
        chunks
            .iter()
            .flat_map(|chunk| decoder.push(chunk))
            .collect()
    }

    #[test]
    fn sse_event_split_mid_field_is_reassembled() {
        let events = sse_all(&[b"event: live-", b"log\ndata: {\"stepId\":\"s\"", b"}\n\n"]);
        assert_eq!(
            events,
            vec![SseEvent {
                event: "live-log".to_owned(),
                data: "{\"stepId\":\"s\"}".to_owned(),
            }]
        );
    }

    #[test]
    fn sse_one_byte_chunks_match_whole_input() {
        let text = b": keepalive\n\nevent: live-log\r\ndata: {\"a\":1}\r\n\r\n";
        let whole = sse_all(&[text]);
        let bytewise: Vec<SseEvent> = {
            let mut decoder = SseDecoder::default();
            text.iter()
                .flat_map(|byte| decoder.push(std::slice::from_ref(byte)))
                .collect()
        };
        assert_eq!(whole, bytewise);
        assert_eq!(whole.len(), 1);
        assert_eq!(whole[0].data, "{\"a\":1}");
    }

    #[test]
    fn sse_joins_multiline_data_and_defaults_event_name() {
        let events = sse_all(&[b"data: one\ndata: two\n\n"]);
        assert_eq!(events[0].event, "message");
        assert_eq!(events[0].data, "one\ntwo");
    }

    #[test]
    fn client_rejects_non_http_base() {
        let error = EngineClient::new("file:///tmp/engine", None).expect_err("file scheme");
        assert!(error.to_string().contains("expected an http or https URL"));
    }

    #[test]
    fn client_builds_percent_encoded_paths() {
        let client = EngineClient::new("http://127.0.0.1:9090/", None).expect("valid base");
        let url = client.url(&[
            "api",
            "v1",
            "runs",
            "8f3a",
            "jobs",
            "test (sandbox)",
            "approve",
        ]);
        assert_eq!(
            url.as_str(),
            "http://127.0.0.1:9090/api/v1/runs/8f3a/jobs/test%20(sandbox)/approve"
        );
    }
}
