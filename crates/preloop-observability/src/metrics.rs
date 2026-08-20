//! Metrics registry for HTTP and store — Step 4.
//!
//! In-memory, bounded, no network I/O. Instruments and attribute arrays are
//! prebuilt where static; gauges are updated from the cached snapshot.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

// ---------------------------------------------------------------------------
// Histogram buckets
// ---------------------------------------------------------------------------

pub const HTTP_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
pub const STORE_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
pub const QUEUE_BUCKETS: &[f64] = &[
    0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 900.0,
];

#[derive(Debug, Default, Clone)]
struct Histogram {
    buckets: Vec<(f64, u64)>, // (le, count)
    count: u64,
    sum: f64,
}

impl Histogram {
    fn new(buckets: &[f64]) -> Self {
        Self {
            buckets: buckets.iter().map(|&le| (le, 0)).collect(),
            count: 0,
            sum: 0.0,
        }
    }

    fn observe(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        for (le, cnt) in &mut self.buckets {
            if value <= *le {
                *cnt += 1;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Http metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HttpLabels {
    pub method: String,
    pub route: String,
    pub surface: String,
    pub status_class: String,
}

#[derive(Debug, Default)]
pub struct HttpMetrics {
    active: RwLock<HashMap<HttpLabels, i64>>,
    durations: RwLock<HashMap<HttpLabels, Histogram>>,
}

impl HttpMetrics {
    pub fn inc_active(&self, labels: &HttpLabels) {
        *self.active.write().entry(labels.clone()).or_insert(0) += 1;
    }

    pub fn dec_active(&self, labels: &HttpLabels) {
        let mut g = self.active.write();
        if let Some(v) = g.get_mut(labels) {
            *v -= 1;
            if *v <= 0 {
                g.remove(labels);
            }
        }
    }

    pub fn observe_duration(&self, labels: HttpLabels, duration: Duration) {
        let secs = duration.as_secs_f64();
        let mut g = self.durations.write();
        let hist = g
            .entry(labels)
            .or_insert_with(|| Histogram::new(HTTP_BUCKETS));
        hist.observe(secs);
    }

    pub fn render(&self, out: &mut String) {
        out.push_str("# HELP http_server_request_duration_seconds API latency — matched route, never raw URI\n");
        out.push_str("# TYPE http_server_request_duration_seconds histogram\n");
        let g = self.durations.read();
        for (labels, hist) in g.iter() {
            for (le, cnt) in &hist.buckets {
                out.push_str(&format!(
                    "http_server_request_duration_seconds_bucket{{method=\"{}\",route=\"{}\",surface=\"{}\",status_class=\"{}\",le=\"{}\"}} {}\n",
                    labels.method, labels.route, labels.surface, labels.status_class, le, cnt
                ));
            }
            out.push_str(&format!(
                "http_server_request_duration_seconds_bucket{{method=\"{}\",route=\"{}\",surface=\"{}\",status_class=\"{}\",le=\"+Inf\"}} {}\n",
                labels.method, labels.route, labels.surface, labels.status_class, hist.count
            ));
            out.push_str(&format!(
                "http_server_request_duration_seconds_sum{{method=\"{}\",route=\"{}\",surface=\"{}\",status_class=\"{}\"}} {}\n",
                labels.method, labels.route, labels.surface, labels.status_class, hist.sum
            ));
            out.push_str(&format!(
                "http_server_request_duration_seconds_count{{method=\"{}\",route=\"{}\",surface=\"{}\",status_class=\"{}\"}} {}\n",
                labels.method, labels.route, labels.surface, labels.status_class, hist.count
            ));
        }
        out.push_str("# HELP http_server_active_requests Current HTTP concurrency\n");
        out.push_str("# TYPE http_server_active_requests gauge\n");
        let g2 = self.active.read();
        for (labels, v) in g2.iter() {
            out.push_str(&format!(
                "http_server_active_requests{{method=\"{}\",route=\"{}\",surface=\"{}\"}} {}\n",
                labels.method, labels.route, labels.surface, v
            ));
        }
    }

    #[cfg(test)]
    pub fn clear(&self) {
        self.active.write().clear();
        self.durations.write().clear();
    }

    #[cfg(test)]
    pub fn series_count(&self) -> usize {
        self.durations.read().len()
    }
}

// ---------------------------------------------------------------------------
// Store metrics
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StoreLabels {
    pub backend: String,
    pub operation: String,
    pub outcome: String,
}

#[derive(Debug, Default)]
pub struct StoreMetrics {
    durations: RwLock<HashMap<StoreLabels, Histogram>>,
    consecutive_failures: RwLock<HashMap<String, u64>>, // backend -> count
}

impl StoreMetrics {
    pub fn observe(&self, backend: &str, operation: &str, outcome: &str, duration: Duration) {
        let labels = StoreLabels {
            backend: backend.to_string(),
            operation: operation.to_string(),
            outcome: outcome.to_string(),
        };
        let mut g = self.durations.write();
        let hist = g
            .entry(labels)
            .or_insert_with(|| Histogram::new(STORE_BUCKETS));
        hist.observe(duration.as_secs_f64());

        // Update consecutive failures
        let mut cf = self.consecutive_failures.write();
        if outcome == "error" {
            *cf.entry(backend.to_string()).or_insert(0) += 1;
        } else {
            cf.insert(backend.to_string(), 0);
        }
    }

    pub fn render(&self, out: &mut String) {
        out.push_str("# HELP preloop_store_operation_duration_seconds Store operation latency\n");
        out.push_str("# TYPE preloop_store_operation_duration_seconds histogram\n");
        let g = self.durations.read();
        for (labels, hist) in g.iter() {
            for (le, cnt) in &hist.buckets {
                out.push_str(&format!(
                    "preloop_store_operation_duration_seconds_bucket{{backend=\"{}\",operation=\"{}\",outcome=\"{}\",le=\"{}\"}} {}\n",
                    labels.backend, labels.operation, labels.outcome, le, cnt
                ));
            }
            out.push_str(&format!(
                "preloop_store_operation_duration_seconds_bucket{{backend=\"{}\",operation=\"{}\",outcome=\"{}\",le=\"+Inf\"}} {}\n",
                labels.backend, labels.operation, labels.outcome, hist.count
            ));
            out.push_str(&format!(
                "preloop_store_operation_duration_seconds_sum{{backend=\"{}\",operation=\"{}\",outcome=\"{}\"}} {}\n",
                labels.backend, labels.operation, labels.outcome, hist.sum
            ));
            out.push_str(&format!(
                "preloop_store_operation_duration_seconds_count{{backend=\"{}\",operation=\"{}\",outcome=\"{}\"}} {}\n",
                labels.backend, labels.operation, labels.outcome, hist.count
            ));
        }
        out.push_str("# HELP preloop_store_consecutive_failures Restart-durability risk\n");
        out.push_str("# TYPE preloop_store_consecutive_failures gauge\n");
        let g2 = self.consecutive_failures.read();
        for (backend, v) in g2.iter() {
            out.push_str(&format!(
                "preloop_store_consecutive_failures{{backend=\"{}\"}} {}\n",
                backend, v
            ));
        }
    }

    #[cfg(test)]
    pub fn clear(&self) {
        self.durations.write().clear();
        self.consecutive_failures.write().clear();
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct MetricsRegistry {
    pub http: HttpMetrics,
    pub store: StoreMetrics,
    pub lifecycle: LifecycleMetrics,
}

impl MetricsRegistry {
    pub fn render(&self) -> String {
        let mut out = String::new();
        self.http.render(&mut out);
        self.store.render(&mut out);
        self.lifecycle.render(&mut out);
        out
    }

    #[cfg(test)]
    pub fn clear(&self) {
        self.http.clear();
        self.store.clear();
        self.lifecycle.clear();
    }
}

// ---------------------------------------------------------------------------
// Lifecycle metrics — run/job, queue, broker, runner
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobCompletedLabels {
    pub conclusion: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueWaitLabels {
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BrokerPollLabels {
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionTransitionLabels {
    pub operation: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConcurrencyDecisionLabels {
    pub queue_mode: String,
    pub action: String,
}

#[derive(Debug, Default)]
pub struct LifecycleMetrics {
    job_completed: RwLock<HashMap<JobCompletedLabels, u64>>,
    queue_wait: RwLock<HashMap<QueueWaitLabels, Histogram>>,
    broker_poll: RwLock<HashMap<BrokerPollLabels, u64>>,
    session_transition: RwLock<HashMap<SessionTransitionLabels, u64>>,
    concurrency_decision: RwLock<HashMap<ConcurrencyDecisionLabels, u64>>,
}

impl LifecycleMetrics {
    pub fn record_job_completed(&self, conclusion: &str, reason: &str) {
        let labels = JobCompletedLabels {
            conclusion: conclusion.to_string(),
            reason: reason.to_string(),
        };
        *self.job_completed.write().entry(labels).or_insert(0) += 1;
    }

    pub fn record_queue_wait(&self, outcome: &str, wait: Duration) {
        let labels = QueueWaitLabels {
            outcome: outcome.to_string(),
        };
        let mut g = self.queue_wait.write();
        let hist = g
            .entry(labels)
            .or_insert_with(|| Histogram::new(QUEUE_BUCKETS));
        hist.observe(wait.as_secs_f64());
    }

    pub fn record_broker_poll(&self, outcome: &str) {
        let labels = BrokerPollLabels {
            outcome: outcome.to_string(),
        };
        *self.broker_poll.write().entry(labels).or_insert(0) += 1;
    }

    pub fn record_session_transition(&self, operation: &str, reason: &str) {
        let labels = SessionTransitionLabels {
            operation: operation.to_string(),
            reason: reason.to_string(),
        };
        *self.session_transition.write().entry(labels).or_insert(0) += 1;
    }

    pub fn record_concurrency_decision(&self, queue_mode: &str, action: &str) {
        let labels = ConcurrencyDecisionLabels {
            queue_mode: queue_mode.to_string(),
            action: action.to_string(),
        };
        *self.concurrency_decision.write().entry(labels).or_insert(0) += 1;
    }

    pub fn render(&self, out: &mut String) {
        out.push_str("# HELP preloop_job_completed Terminal jobs by conclusion and reason\n");
        out.push_str("# TYPE preloop_job_completed counter\n");
        for (labels, cnt) in self.job_completed.read().iter() {
            out.push_str(&format!(
                "preloop_job_completed{{conclusion=\"{}\",reason=\"{}\"}} {}\n",
                labels.conclusion, labels.reason, cnt
            ));
        }
        out.push_str("# HELP preloop_job_queue_wait_seconds Queue wait until claim or terminal\n");
        out.push_str("# TYPE preloop_job_queue_wait_seconds histogram\n");
        for (labels, hist) in self.queue_wait.read().iter() {
            for (le, cnt) in &hist.buckets {
                out.push_str(&format!(
                    "preloop_job_queue_wait_seconds_bucket{{outcome=\"{}\",le=\"{}\"}} {}\n",
                    labels.outcome, le, cnt
                ));
            }
            out.push_str(&format!(
                "preloop_job_queue_wait_seconds_bucket{{outcome=\"{}\",le=\"+Inf\"}} {}\n",
                labels.outcome, hist.count
            ));
            out.push_str(&format!(
                "preloop_job_queue_wait_seconds_sum{{outcome=\"{}\"}} {}\n",
                labels.outcome, hist.sum
            ));
            out.push_str(&format!(
                "preloop_job_queue_wait_seconds_count{{outcome=\"{}\"}} {}\n",
                labels.outcome, hist.count
            ));
        }
        out.push_str("# HELP preloop_broker_poll_total Broker poll outcomes\n");
        out.push_str("# TYPE preloop_broker_poll_total counter\n");
        for (labels, cnt) in self.broker_poll.read().iter() {
            out.push_str(&format!(
                "preloop_broker_poll_total{{outcome=\"{}\"}} {}\n",
                labels.outcome, cnt
            ));
        }
        out.push_str("# HELP preloop_runner_session_transition_total Session lifecycle\n");
        out.push_str("# TYPE preloop_runner_session_transition_total counter\n");
        for (labels, cnt) in self.session_transition.read().iter() {
            out.push_str(&format!(
                "preloop_runner_session_transition_total{{operation=\"{}\",reason=\"{}\"}} {}\n",
                labels.operation, labels.reason, cnt
            ));
        }
        out.push_str("# HELP preloop_concurrency_decision_total Concurrency queue decisions\n");
        out.push_str("# TYPE preloop_concurrency_decision_total counter\n");
        for (labels, cnt) in self.concurrency_decision.read().iter() {
            out.push_str(&format!(
                "preloop_concurrency_decision_total{{queue_mode=\"{}\",action=\"{}\"}} {}\n",
                labels.queue_mode, labels.action, cnt
            ));
        }
    }

    #[cfg(test)]
    pub fn clear(&self) {
        self.job_completed.write().clear();
        self.queue_wait.write().clear();
        self.broker_poll.write().clear();
        self.session_transition.write().clear();
        self.concurrency_decision.write().clear();
    }

    #[cfg(test)]
    pub fn job_completed_count(&self, conclusion: &str, reason: &str) -> u64 {
        let labels = JobCompletedLabels {
            conclusion: conclusion.to_string(),
            reason: reason.to_string(),
        };
        *self.job_completed.read().get(&labels).unwrap_or(&0)
    }
}

// ---------------------------------------------------------------------------
// Helpers — surface classification and route normalization
// ---------------------------------------------------------------------------

/// Classify a normalized route template into a finite surface.
pub fn classify_surface(route: &str) -> &'static str {
    if route == "/healthz" || route == "/readyz" || route == "/metrics" {
        return "public";
    }
    if route.starts_with("/api/v1") {
        return "native";
    }
    if route.starts_with("/_apis") || route.starts_with("/runner") {
        return "runner";
    }
    if route.starts_with("/broker") {
        return "broker";
    }
    if route.starts_with("/twirp") || route.starts_with("/twirp-blob") {
        return "results";
    }
    if route.starts_with("/ws/live-logs") {
        return "live_logs";
    }
    if route.starts_with("/snapshots") || route.starts_with("/repos") {
        return "git";
    }
    if route.starts_with("/oidc") || route.starts_with("/.well-known") {
        return "oidc";
    }
    if route == "/webhook" || route.starts_with("/webhook") {
        return "webhook";
    }
    if route.starts_with("/internal/test") {
        return "test";
    }
    "unknown"
}

/// Normalize a raw path (with concrete IDs) to a bounded route template.
///
/// Uses Axum's matched templates where available; otherwise falls back to
/// prefix matching for the known route set. Query strings are stripped.
pub fn normalize_route(raw: &str) -> String {
    // Strip query
    let path = raw.split('?').next().unwrap_or(raw);
    // Already a template? (contains ':')
    if path.contains(':') {
        return path.to_string();
    }
    // Known templates — longest prefix first
    const TEMPLATES: &[&str] = &[
        "/api/v1/runs/:run_id",
        "/api/v1/runs",
        "/api/v1/status",
        "/api/v1/scheduler/history",
        "/api/v1/debug/sessions",
        "/api/v1/github/register",
        "/api/v1/github/callback",
        "/_apis/artifactcache/cache/:cache_id",
        "/_apis/artifactcache/cache",
        "/_apis/pipelines/workflows/:run_id/artifacts/:artifact_id",
        "/_apis/pipelines/workflows/:run_id/artifacts",
        "/runner/server/_apis/distributedtask/pools/:pool_id/agents",
        "/runner/server/_apis/distributedtask/pools",
        "/broker/:runner_id/acquirejob",
        "/broker/:runner_id/renewjob",
        "/broker/:runner_id/completejob",
        "/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact",
        "/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry",
        "/twirp-blob/:kind/:token",
        "/ws/live-logs/:job_id",
        "/snapshots",
        "/repos",
        "/oidc",
        "/.well-known",
        "/webhook",
        "/healthz",
        "/readyz",
        "/metrics",
    ];
    for tmpl in TEMPLATES {
        // Template without params matches exactly
        if !tmpl.contains(':') && path == *tmpl {
            return tmpl.to_string();
        }
        // Template with params: match prefix up to first ':'
        if let Some(colon) = tmpl.find(':') {
            let prefix = &tmpl[..colon - 1]; // up to '/' before ':'
            if path.starts_with(prefix) {
                // Ensure it's a segment boundary: /api/v1/runs/abc should match /api/v1/runs/:run_id
                // but /api/v1/runsXYZ should not.
                let rest = &path[prefix.len()..];
                if rest.is_empty() || rest.starts_with('/') {
                    return tmpl.to_string();
                }
            }
        }
    }
    // Unknown — constant label, never raw path
    "/unknown".to_string()
}

pub fn status_class(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_concrete_id_to_template() {
        assert_eq!(
            normalize_route("/api/v1/runs/abc123"),
            "/api/v1/runs/:run_id"
        );
        assert_eq!(
            normalize_route("/api/v1/runs/abc123?foo=bar"),
            "/api/v1/runs/:run_id"
        );
        assert_eq!(normalize_route("/api/v1/status"), "/api/v1/status");
        assert_eq!(normalize_route("/unknown/path/xyz"), "/unknown");
    }

    #[test]
    fn classify() {
        assert_eq!(classify_surface("/api/v1/runs"), "native");
        assert_eq!(classify_surface("/_apis/artifactcache/cache"), "runner");
        assert_eq!(classify_surface("/broker/42/acquirejob"), "broker");
        assert_eq!(classify_surface("/ws/live-logs/123"), "live_logs");
        assert_eq!(classify_surface("/healthz"), "public");
        assert_eq!(classify_surface("/unknown"), "unknown");
    }

    #[test]
    fn http_series_bounded() {
        let m = HttpMetrics::default();
        for i in 0..1000 {
            let route = format!("/api/v1/runs/{}", i);
            let tmpl = normalize_route(&route);
            let labels = HttpLabels {
                method: "GET".to_string(),
                route: tmpl,
                surface: "native".to_string(),
                status_class: "2xx".to_string(),
            };
            m.observe_duration(labels, Duration::from_millis(10));
        }
        assert_eq!(m.series_count(), 1, "1000 distinct IDs must be 1 series");
    }
}
