//! Metrics registry for HTTP, store, and lifecycle — Step 4.
//!
//! Instruments are created from the shared `Meter` so the OTLP periodic reader
//! and the Prometheus reader scrape the same instruments — one source, never
//! two. The label machinery (route normalization, surface classification,
//! bounded termination reasons) is unchanged; only the storage moved behind
//! the OpenTelemetry SDK.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::MeterProvider;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};
use opentelemetry::KeyValue;
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

// ---------------------------------------------------------------------------
// Http metrics
// ---------------------------------------------------------------------------

/// Labels for a completed HTTP request duration observation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HttpLabels {
    pub method: String,
    pub route: String,
    pub surface: String,
    pub status_class: String,
}

impl HttpLabels {
    fn as_attrs(&self) -> [KeyValue; 4] {
        [
            KeyValue::new("method", self.method.clone()),
            KeyValue::new("route", self.route.clone()),
            KeyValue::new("surface", self.surface.clone()),
            KeyValue::new("status_class", self.status_class.clone()),
        ]
    }
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

impl ActiveLabels {
    fn as_attrs(&self) -> [KeyValue; 3] {
        [
            KeyValue::new("method", self.method.clone()),
            KeyValue::new("route", self.route.clone()),
            KeyValue::new("surface", self.surface.clone()),
        ]
    }
}

/// HTTP instruments. `active` is an up-down counter so the pre-response
/// increment and post-response decrement can disagree only transiently — the
/// SDK keeps a signed value, unlike a Prometheus-style gauge that would go
/// negative and be dropped.
#[derive(Clone)]
pub struct HttpMetrics {
    active: UpDownCounter<i64>,
    durations: Histogram<f64>,
}

impl std::fmt::Debug for HttpMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpMetrics").finish_non_exhaustive()
    }
}

impl HttpMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            active: meter
                .i64_up_down_counter("http.server.active_requests")
                .with_description("Current HTTP concurrency")
                .with_unit("{request}")
                .build(),
            durations: meter
                .f64_histogram("http.server.request.duration")
                .with_description("API latency — matched route, never raw URI")
                .with_unit("s")
                .build(),
        }
    }

    pub fn inc_active(&self, labels: &ActiveLabels) {
        self.active.add(1, &labels.as_attrs());
    }

    pub fn dec_active(&self, labels: &ActiveLabels) {
        self.active.add(-1, &labels.as_attrs());
    }

    pub fn observe_duration(&self, labels: HttpLabels, duration: Duration) {
        self.durations
            .record(duration.as_secs_f64(), &labels.as_attrs());
    }
}

// ---------------------------------------------------------------------------
// Store metrics
// ---------------------------------------------------------------------------

/// Labels for a store operation duration observation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StoreLabels {
    pub backend: String,
    pub operation: String,
    pub outcome: String,
}

impl StoreLabels {
    fn as_attrs(&self) -> [KeyValue; 3] {
        [
            KeyValue::new("backend", self.backend.clone()),
            KeyValue::new("operation", self.operation.clone()),
            KeyValue::new("outcome", self.outcome.clone()),
        ]
    }
}

/// Store instruments: one latency histogram plus a consecutive-failures gauge
/// per backend (restart-durability risk).
#[derive(Clone)]
pub struct StoreMetrics {
    durations: Histogram<f64>,
    consecutive_failures: Gauge<u64>,
    /// Per-backend streak, authoritative for the gauge. Kept separately from
    /// the SDK gauge because a gauge has no "increment" — the value must be
    /// recomputed and set, and we want the streak to survive a partial
    /// observation race (error then success in the same window).
    streaks: Arc<RwLock<HashMap<String, u64>>>,
}

impl std::fmt::Debug for StoreMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreMetrics").finish_non_exhaustive()
    }
}

impl StoreMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            durations: meter
                .f64_histogram("preloop.store.operation.duration")
                .with_description("Store operation latency")
                .with_unit("s")
                .build(),
            consecutive_failures: meter
                .u64_gauge("preloop.store.consecutive_failures")
                .with_description("Restart-durability risk: consecutive store failures")
                .with_unit("{failure}")
                .build(),
            streaks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn observe(&self, backend: &str, operation: &str, outcome: &str, duration: Duration) {
        let labels = StoreLabels {
            backend: backend.to_string(),
            operation: operation.to_string(),
            outcome: outcome.to_string(),
        };
        self.durations
            .record(duration.as_secs_f64(), &labels.as_attrs());

        // Consecutive failures: +1 on error, reset to 0 on success. Update the
        // streak under the lock, then publish the current value to the gauge
        // so a scrape sees the latest streak even if observations race.
        let mut streaks = self.streaks.write();
        let streak = if outcome == "error" {
            let entry = streaks.entry(backend.to_string()).or_insert(0);
            *entry += 1;
            *entry
        } else {
            streaks.insert(backend.to_string(), 0);
            0
        };
        drop(streaks);
        let attrs = [KeyValue::new("backend", backend.to_string())];
        self.consecutive_failures.record(streak, &attrs);
    }
}

// ---------------------------------------------------------------------------
// Lifecycle metrics — run/job, queue, broker, runner
// ---------------------------------------------------------------------------

/// Bounded conclusion + reason for a terminal job. The reason is already
/// classified by `bounded_termination_reason` in the server, so these labels
/// are a finite set.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobCompletedLabels {
    pub conclusion: String,
    pub reason: String,
}

/// Queue-wait outcome: claimed, terminal-while-queued, reaped, etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueWaitLabels {
    pub outcome: String,
}

/// Broker poll outcome: job, no-job, error, etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BrokerPollLabels {
    pub outcome: String,
}

/// Runner session lifecycle transition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionTransitionLabels {
    pub operation: String,
    pub reason: String,
}

/// Concurrency queue decision.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConcurrencyDecisionLabels {
    pub queue_mode: String,
    pub action: String,
}

/// Lifecycle instruments — counters for terminal jobs, session transitions,
/// broker polls, and concurrency decisions; a histogram for queue wait.
#[derive(Clone)]
pub struct LifecycleMetrics {
    job_completed: Counter<u64>,
    queue_wait: Histogram<f64>,
    broker_poll: Counter<u64>,
    session_transition: Counter<u64>,
    concurrency_decision: Counter<u64>,
}

impl std::fmt::Debug for LifecycleMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LifecycleMetrics").finish_non_exhaustive()
    }
}

impl LifecycleMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            job_completed: meter
                .u64_counter("preloop.job.completed")
                .with_description("Terminal jobs by conclusion and reason")
                .with_unit("{job}")
                .build(),
            queue_wait: meter
                .f64_histogram("preloop.job.queue.wait")
                .with_description("Queue wait until claim or terminal")
                .with_unit("s")
                .build(),
            broker_poll: meter
                .u64_counter("preloop.broker.poll")
                .with_description("Broker poll outcomes")
                .with_unit("{poll}")
                .build(),
            session_transition: meter
                .u64_counter("preloop.runner.session.transition")
                .with_description("Session lifecycle transitions")
                .with_unit("{transition}")
                .build(),
            concurrency_decision: meter
                .u64_counter("preloop.concurrency.decision")
                .with_description("Concurrency queue decisions")
                .with_unit("{decision}")
                .build(),
        }
    }

    pub fn record_job_completed(&self, conclusion: &str, reason: &str) {
        let attrs = [
            KeyValue::new("conclusion", conclusion.to_string()),
            KeyValue::new("reason", reason.to_string()),
        ];
        self.job_completed.add(1, &attrs);
    }

    pub fn record_queue_wait(&self, outcome: &str, wait: Duration) {
        self.queue_wait.record(
            wait.as_secs_f64(),
            &[KeyValue::new("outcome", outcome.to_string())],
        );
    }

    pub fn record_broker_poll(&self, outcome: &str) {
        self.broker_poll
            .add(1, &[KeyValue::new("outcome", outcome.to_string())]);
    }

    pub fn record_session_transition(&self, operation: &str, reason: &str) {
        let attrs = [
            KeyValue::new("operation", operation.to_string()),
            KeyValue::new("reason", reason.to_string()),
        ];
        self.session_transition.add(1, &attrs);
    }

    pub fn record_concurrency_decision(&self, queue_mode: &str, action: &str) {
        let attrs = [
            KeyValue::new("queue_mode", queue_mode.to_string()),
            KeyValue::new("action", action.to_string()),
        ];
        self.concurrency_decision.add(1, &attrs);
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Instrument holder. `default()` is a no-op registry for tests and
/// library-only consumers — instruments created from the noop meter record
/// nothing.
#[derive(Clone)]
pub struct MetricsRegistry {
    pub http: HttpMetrics,
    pub store: StoreMetrics,
    pub lifecycle: LifecycleMetrics,
}

impl std::fmt::Debug for MetricsRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsRegistry").finish_non_exhaustive()
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        let provider = opentelemetry::metrics::noop::NoopMeterProvider::new();
        let meter = provider.meter("preloop-observability-noop");
        Self::from_meter(meter)
    }
}

impl MetricsRegistry {
    /// Create instruments from a real meter (from the shared `SdkMeterProvider`).
    pub fn from_meter(meter: Meter) -> Self {
        Self {
            http: HttpMetrics::new(&meter),
            store: StoreMetrics::new(&meter),
            lifecycle: LifecycleMetrics::new(&meter),
        }
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
            if let Some(rest) = path.strip_prefix(prefix) {
                // Ensure it's a segment boundary: /api/v1/runs/abc should match /api/v1/runs/:run_id
                // but /api/v1/runsXYZ should not.
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
    fn classify() {
        assert_eq!(classify_surface("/api/v1/runs"), "native");
        assert_eq!(classify_surface("/_apis/artifactcache/cache"), "runner");
        assert_eq!(classify_surface("/broker/42/acquirejob"), "broker");
        assert_eq!(classify_surface("/ws/live-logs/123"), "live_logs");
        assert_eq!(classify_surface("/healthz"), "public");
        assert_eq!(classify_surface("/unknown"), "unknown");
    }

    #[test]
    fn default_registry_is_noop() {
        // Instruments created from the noop meter must record without
        // panicking and without any storage behind them.
        let registry = MetricsRegistry::default();
        registry.http.inc_active(&ActiveLabels {
            method: "GET".to_string(),
            route: "/api/v1/runs".to_string(),
            surface: "native".to_string(),
        });
        registry.http.observe_duration(
            HttpLabels {
                method: "GET".to_string(),
                route: "/api/v1/runs".to_string(),
                surface: "native".to_string(),
                status_class: "2xx".to_string(),
            },
            Duration::from_millis(10),
        );
        registry
            .store
            .observe("sqlite", "store_inner", "ok", Duration::from_millis(1));
        registry
            .lifecycle
            .record_job_completed("success", "unspecified");
        registry
            .lifecycle
            .record_queue_wait("claimed", Duration::from_secs(2));
        registry.lifecycle.record_broker_poll("job");
        registry.lifecycle.record_session_transition("create", "ok");
        registry
            .lifecycle
            .record_concurrency_decision("queue", "accept");
    }

    #[test]
    fn label_attrs_are_bounded() {
        // The four label structs used by the middleware are exactly the four
        // that appear in the instrument attribute sets — a new label must be
        // added to both or the series set drifts.
        let labels = HttpLabels {
            method: "GET".to_string(),
            route: "/api/v1/runs".to_string(),
            surface: "native".to_string(),
            status_class: "2xx".to_string(),
        };
        assert_eq!(labels.as_attrs().len(), 4);
        let active = ActiveLabels {
            method: "GET".to_string(),
            route: "/api/v1/runs".to_string(),
            surface: "native".to_string(),
        };
        assert_eq!(active.as_attrs().len(), 3);
    }
}
