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

/// Identity of an in-flight request. Deliberately carries no
/// `status_class`: the status is unknown until the response completes, so any
/// status in the key would let the increment (pre-response) and decrement
/// (post-response) disagree and permanently leak the gauge.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActiveLabels {
    pub method: String,
    pub route: String,
    pub surface: String,
}

#[derive(Debug, Default)]
pub struct HttpMetrics {
    active: RwLock<HashMap<ActiveLabels, i64>>,
    durations: RwLock<HashMap<HttpLabels, Histogram>>,
}

impl HttpMetrics {
    pub fn inc_active(&self, labels: &ActiveLabels) {
        *self.active.write().entry(labels.clone()).or_insert(0) += 1;
    }

    pub fn dec_active(&self, labels: &ActiveLabels) {
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
            let method = escape_label(&labels.method);
            let route = escape_label(&labels.route);
            let surface = escape_label(&labels.surface);
            let status = escape_label(&labels.status_class);
            for (le, cnt) in &hist.buckets {
                out.push_str(&format!(
                    "http_server_request_duration_seconds_bucket{{method=\"{method}\",route=\"{route}\",surface=\"{surface}\",status_class=\"{status}\",le=\"{le}\"}} {cnt}\n"
                ));
            }
            out.push_str(&format!(
                "http_server_request_duration_seconds_bucket{{method=\"{method}\",route=\"{route}\",surface=\"{surface}\",status_class=\"{status}\",le=\"+Inf\"}} {}\n",
                hist.count
            ));
            out.push_str(&format!(
                "http_server_request_duration_seconds_sum{{method=\"{method}\",route=\"{route}\",surface=\"{surface}\",status_class=\"{status}\"}} {}\n",
                hist.sum
            ));
            out.push_str(&format!(
                "http_server_request_duration_seconds_count{{method=\"{method}\",route=\"{route}\",surface=\"{surface}\",status_class=\"{status}\"}} {}\n",
                hist.count
            ));
        }
        out.push_str("# HELP http_server_active_requests Current HTTP concurrency\n");
        out.push_str("# TYPE http_server_active_requests gauge\n");
        let g2 = self.active.read();
        for (labels, v) in g2.iter() {
            out.push_str(&format!(
                "http_server_active_requests{{method=\"{}\",route=\"{}\",surface=\"{}\"}} {}\n",
                escape_label(&labels.method),
                escape_label(&labels.route),
                escape_label(&labels.surface),
                v
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
                // A parameterized template needs a non-empty child segment;
                // the bare collection path is matched by its own entry.
                if rest.len() > 1 && rest.starts_with('/') {
                    return tmpl.to_string();
                }
            }
        }
    }
    // Unknown — constant label, never raw path
    "/unknown".to_string()
}

/// Escape a label value for Prometheus exposition text. The label set is
/// bounded, but a quote, backslash, or newline would corrupt the entire
/// scrape; defense in depth on top of the bounded construction.
fn escape_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out
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
        // A bare collection path resolves to its own template, not the
        // single-item one — otherwise list latency is reported under the
        // item route.
        assert_eq!(normalize_route("/api/v1/runs"), "/api/v1/runs");
        assert_eq!(
            normalize_route("/_apis/artifactcache/cache"),
            "/_apis/artifactcache/cache"
        );
        assert_eq!(
            normalize_route("/runner/server/_apis/distributedtask/pools"),
            "/runner/server/_apis/distributedtask/pools"
        );
    }

    #[test]
    fn colon_in_path_cannot_escape_the_template_set() {
        // A colon is legal inside a path segment; an unauthenticated 404 can
        // hit /evil:anything and must still land on the constant label.
        assert_eq!(normalize_route("/evil:1234/path"), "/unknown");
        assert_eq!(normalize_route("/api/v1/runs:junk"), "/unknown");
        assert_eq!(normalize_route("/:colon"), "/unknown");
    }

    #[test]
    fn escape_label_keeps_scrape_parseable() {
        // A quote and a backslash must be escaped so the exposition stays
        // parseable; a real newline must become the two-character escape.
        assert_eq!(escape_label("a\"b\\c"), "a\\\"b\\\\c");
        assert_eq!(escape_label("line\nbreak"), "line\\nbreak");
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
    fn otlp_bucket_counts_convert_cumulative_to_disjoint() {
        let mut hist = Histogram::new(&[0.005, 0.01]);
        // observe() bumps every bucket where value <= le, so buckets are
        // cumulative: after these three, [3, 2, 0] with count 3.
        hist.observe(0.004);
        hist.observe(0.007);
        hist.observe(0.02);
        let counts = hist.otlp_bucket_counts();
        // Disjoint: 1 under 0.005, 1 between 0.005 and 0.01, 1 above.
        assert_eq!(counts, vec![1, 1, 1]);
        assert_eq!(counts.iter().sum::<u64>(), hist.count);
    }

    #[test]
    fn active_gauge_increment_and_decrement_are_idempotent() {
        let m = HttpMetrics::default();
        let labels = ActiveLabels {
            method: "GET".to_string(),
            route: "/api/v1/runs".to_string(),
            surface: "native".to_string(),
        };
        m.inc_active(&labels);
        m.inc_active(&labels);
        m.dec_active(&labels);
        // One still in flight; the gauge key carries no status_class, so a
        // caller cannot increment under one key and decrement under another.
        let g = m.active.read();
        assert_eq!(g.get(&labels), Some(&1));
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

// ---------------------------------------------------------------------------
// Structured collection for OTLP export
// ---------------------------------------------------------------------------

/// One data point, already reduced to bounded attributes.
#[derive(Debug, Clone)]
pub enum MetricPoint {
    /// Monotonic counter (OTLP `sum`, cumulative, `isMonotonic: true`).
    Sum {
        value: f64,
        attributes: Vec<(String, String)>,
    },
    /// Instantaneous value (OTLP `gauge`).
    Gauge {
        value: f64,
        attributes: Vec<(String, String)>,
    },
    /// Explicit-bucket histogram (OTLP `histogram`, cumulative).
    ///
    /// `bucket_counts` is one longer than `bounds`: OTLP requires the
    /// implicit `+Inf` bucket to be present as the final entry.
    Histogram {
        count: u64,
        sum: f64,
        bounds: Vec<f64>,
        bucket_counts: Vec<u64>,
        attributes: Vec<(String, String)>,
    },
}

#[derive(Debug, Clone)]
pub struct MetricFamily {
    pub name: String,
    pub unit: &'static str,
    pub points: Vec<MetricPoint>,
}

impl Histogram {
    /// Convert the cumulative (Prometheus `le`) buckets to the disjoint
    /// per-bucket counts OTLP requires, where every observation lands in
    /// exactly one bucket and the counts sum to `count`. Emitting the
    /// cumulative values as-if-disjoint would count each observation once
    /// per bucket and blow the total past `count`.
    fn otlp_bucket_counts(&self) -> Vec<u64> {
        let mut deltas = Vec::with_capacity(self.buckets.len() + 1);
        let mut previous = 0;
        for (_, c) in &self.buckets {
            deltas.push(c - previous);
            previous = *c;
        }
        deltas.push(self.count - previous);
        deltas
    }

    fn bounds(&self) -> Vec<f64> {
        self.buckets.iter().map(|(le, _)| *le).collect()
    }
}

impl HttpMetrics {
    fn collect(&self, out: &mut Vec<MetricFamily>) {
        let durations = self.durations.read();
        if !durations.is_empty() {
            out.push(MetricFamily {
                name: "http.server.request.duration".to_string(),
                unit: "s",
                points: durations
                    .iter()
                    .map(|(labels, hist)| MetricPoint::Histogram {
                        count: hist.count,
                        sum: hist.sum,
                        bounds: hist.bounds(),
                        bucket_counts: hist.otlp_bucket_counts(),
                        attributes: vec![
                            ("http.request.method".to_string(), labels.method.clone()),
                            ("http.route".to_string(), labels.route.clone()),
                            ("preloop.surface".to_string(), labels.surface.clone()),
                            (
                                "http.response.status_class".to_string(),
                                labels.status_class.clone(),
                            ),
                        ],
                    })
                    .collect(),
            });
        }
        let active = self.active.read();
        if !active.is_empty() {
            out.push(MetricFamily {
                name: "http.server.active_requests".to_string(),
                unit: "{request}",
                points: active
                    .iter()
                    .map(|(labels, value)| MetricPoint::Gauge {
                        value: *value as f64,
                        attributes: vec![
                            ("http.request.method".to_string(), labels.method.clone()),
                            ("http.route".to_string(), labels.route.clone()),
                            ("preloop.surface".to_string(), labels.surface.clone()),
                        ],
                    })
                    .collect(),
            });
        }
    }
}

impl StoreMetrics {
    fn collect(&self, out: &mut Vec<MetricFamily>) {
        let durations = self.durations.read();
        if !durations.is_empty() {
            out.push(MetricFamily {
                name: "preloop.store.operation.duration".to_string(),
                unit: "s",
                points: durations
                    .iter()
                    .map(|(labels, hist)| MetricPoint::Histogram {
                        count: hist.count,
                        sum: hist.sum,
                        bounds: hist.bounds(),
                        bucket_counts: hist.otlp_bucket_counts(),
                        attributes: vec![
                            ("db.system".to_string(), labels.backend.clone()),
                            ("preloop.operation".to_string(), labels.operation.clone()),
                            ("preloop.outcome".to_string(), labels.outcome.clone()),
                        ],
                    })
                    .collect(),
            });
        }
        let failures = self.consecutive_failures.read();
        if !failures.is_empty() {
            out.push(MetricFamily {
                name: "preloop.store.consecutive_failures".to_string(),
                unit: "{failure}",
                points: failures
                    .iter()
                    .map(|(backend, value)| MetricPoint::Gauge {
                        value: *value as f64,
                        attributes: vec![("db.system".to_string(), backend.clone())],
                    })
                    .collect(),
            });
        }
    }
}

impl LifecycleMetrics {
    fn collect(&self, out: &mut Vec<MetricFamily>) {
        let completed = self.job_completed.read();
        if !completed.is_empty() {
            out.push(MetricFamily {
                name: "preloop.job.completed".to_string(),
                unit: "{job}",
                points: completed
                    .iter()
                    .map(|(labels, value)| MetricPoint::Sum {
                        value: *value as f64,
                        attributes: vec![
                            ("preloop.conclusion".to_string(), labels.conclusion.clone()),
                            ("preloop.reason".to_string(), labels.reason.clone()),
                        ],
                    })
                    .collect(),
            });
        }
        let wait = self.queue_wait.read();
        if !wait.is_empty() {
            out.push(MetricFamily {
                name: "preloop.job.queue.wait".to_string(),
                unit: "s",
                points: wait
                    .iter()
                    .map(|(labels, hist)| MetricPoint::Histogram {
                        count: hist.count,
                        sum: hist.sum,
                        bounds: hist.bounds(),
                        bucket_counts: hist.otlp_bucket_counts(),
                        attributes: vec![("preloop.outcome".to_string(), labels.outcome.clone())],
                    })
                    .collect(),
            });
        }
        let poll = self.broker_poll.read();
        if !poll.is_empty() {
            out.push(MetricFamily {
                name: "preloop.broker.poll".to_string(),
                unit: "{poll}",
                points: poll
                    .iter()
                    .map(|(labels, value)| MetricPoint::Sum {
                        value: *value as f64,
                        attributes: vec![("preloop.outcome".to_string(), labels.outcome.clone())],
                    })
                    .collect(),
            });
        }
        let sessions = self.session_transition.read();
        if !sessions.is_empty() {
            out.push(MetricFamily {
                name: "preloop.runner.session.transition".to_string(),
                unit: "{transition}",
                points: sessions
                    .iter()
                    .map(|(labels, value)| MetricPoint::Sum {
                        value: *value as f64,
                        attributes: vec![
                            ("preloop.operation".to_string(), labels.operation.clone()),
                            ("preloop.reason".to_string(), labels.reason.clone()),
                        ],
                    })
                    .collect(),
            });
        }
        let concurrency = self.concurrency_decision.read();
        if !concurrency.is_empty() {
            out.push(MetricFamily {
                name: "preloop.concurrency.decision".to_string(),
                unit: "{decision}",
                points: concurrency
                    .iter()
                    .map(|(labels, value)| MetricPoint::Sum {
                        value: *value as f64,
                        attributes: vec![
                            ("preloop.queue_mode".to_string(), labels.queue_mode.clone()),
                            ("preloop.action".to_string(), labels.action.clone()),
                        ],
                    })
                    .collect(),
            });
        }
    }
}

impl MetricsRegistry {
    /// Snapshot every instrument as OTLP-ready families.
    ///
    /// Read-only: takes each sub-registry's read lock in turn and never holds
    /// two at once, so a scrape cannot deadlock against a recording caller.
    pub fn collect(&self) -> Vec<MetricFamily> {
        let mut families = Vec::new();
        self.http.collect(&mut families);
        self.store.collect(&mut families);
        self.lifecycle.collect(&mut families);
        families
    }
}
