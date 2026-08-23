#![allow(missing_docs)]

//! `preloop-observability` — observability handle for Preloop.
//!
//! Small, explicit API with no dependency on server/orchestrator internals. Both
//! `preloop` and `preloop-server` construct one handle/runtime before building
//! `ServerConfig`; the handle is cloned into `AppState` and `RunnerPoolConfig`.
//! Tests use `Observability::noop()` which performs no network I/O.
//!
//! The transport is the OpenTelemetry SDK: `SdkTracerProvider`,
//! `SdkMeterProvider`, and `SdkLoggerProvider`, each with OTLP/HTTP batch
//! exporters (no tonic). A Prometheus reader always backs the meter provider so
//! `/metrics` works with no backend at all.
//!
//! Invariants from the plan:
//! - Fail open: export failure never rejects a workflow.
//! - Bounded queues, 2s flush, no backend by default (absent `OTEL_EXPORTER_OTLP_*` = disabled, not `localhost:4318`).
//! - Always retain `stderr`/`journald` even when OTLP is configured.
//! - `Debug` on config never reveals headers or credential-bearing endpoint parts.

pub mod metrics;
pub mod status;
pub mod vm_telemetry;

/// Read `traceparent` from an `http::HeaderMap`. Re-exported for the
/// middleware's span parent extraction.
pub use opentelemetry_http::HeaderExtractor;
/// W3C `traceparent` propagation. Re-exported so the server middleware can
/// adopt an inbound trace without depending on the SDK crate directly.
pub use opentelemetry_sdk::propagation::TraceContextPropagator;

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use opentelemetry::logs::{AnyValue, LogRecord, Logger, LoggerProvider, Severity};
use opentelemetry::metrics::MeterProvider;
use opentelemetry::trace::TracerProvider;
use opentelemetry::{Key, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use parking_lot::RwLock;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;
use uuid::Uuid;

use crate::metrics::{HTTP_BUCKETS, QUEUE_BUCKETS, STORE_BUCKETS};

// ---------------------------------------------------------------------------
// Log format
// ---------------------------------------------------------------------------

/// How `tracing_subscriber::fmt` should render.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Pretty on TTY, JSON when piped / in journald.
    Auto,
    Pretty,
    Json,
}

impl LogFormat {
    fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "pretty" => Some(Self::Pretty),
            "json" => Some(Self::Json),
            _ => None,
        }
    }

    /// Resolve `Auto` to a concrete format for the current stderr.
    pub fn resolve(self) -> Self {
        if self != Self::Auto {
            return self;
        }
        if std::io::IsTerminal::is_terminal(&std::io::stderr()) {
            Self::Pretty
        } else {
            Self::Json
        }
    }
}

// ---------------------------------------------------------------------------
// ObservabilityConfig
// ---------------------------------------------------------------------------

/// Fully-resolved destination for one signal: URL plus signal-scoped headers.
#[derive(Debug, Clone)]
pub struct SignalTarget {
    pub url: String,
    pub headers: Vec<(String, String)>,
}

/// Per-signal export destinations. Resolution (which env var wins, whether
/// the generic base needs the `/v1/<signal>` suffix) happens here; the SDK
/// exporters never touch the URL shape again.
#[derive(Debug, Clone, Default)]
pub struct ExportTargets {
    pub logs: Option<SignalTarget>,
    pub traces: Option<SignalTarget>,
    pub metrics: Option<SignalTarget>,
}

impl ExportTargets {
    pub fn is_empty(&self) -> bool {
        self.logs.is_none() && self.traces.is_none() && self.metrics.is_none()
    }
}

/// Parse `OTEL_EXPORTER_OTLP_HEADERS` (`k1=v1,k2=v2`).
pub fn parse_headers(raw: Option<&str>) -> Vec<(String, String)> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    raw.split(',')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            let k = k.trim();
            let v = v.trim();
            if k.is_empty() || v.is_empty() {
                None
            } else {
                Some((k.to_string(), v.to_string()))
            }
        })
        .collect()
}

/// Parsed logging + OTel configuration. `Debug` is redacted.
pub struct ObservabilityConfig {
    /// `PRELOOP_LOG_FORMAT` resolved to concrete `LogFormat` (but `Auto` is kept for display).
    pub log_format: LogFormat,
    /// Effective `RUST_LOG` filter string (default `info` when unset, matching CLI behaviour).
    pub rust_log: String,
    /// `service.name` — `preloop` or `OTEL_SERVICE_NAME`.
    pub service_name: String,
    /// `service.version` — set by the host binary via `with_service_version`.
    /// Defaults to this crate's version only until the binary overrides it.
    pub service_version: String,
    /// Per-process instance ID (UUID v4).
    pub instance_id: String,
    /// Per-signal OTLP endpoints, each fully resolved: a signal-specific
    /// variable wins and is used as-is; the generic base gets the
    /// `/v1/<signal>` suffix appended. Kept raw for transport, but `Debug`
    /// redacts userinfo/query.
    otel_logs_endpoint: Option<String>,
    otel_traces_endpoint: Option<String>,
    otel_metrics_endpoint: Option<String>,
    /// Per-signal `OTEL_EXPORTER_OTLP_*_HEADERS`, signal-specific first,
    /// generic fallback applied per signal. Never shown in `Debug` or errors.
    otel_logs_headers: Option<String>,
    otel_traces_headers: Option<String>,
    otel_metrics_headers: Option<String>,
    /// Whether any OTLP endpoint is present (i.e. export enabled).
    pub otlp_enabled: bool,
}

impl ObservabilityConfig {
    /// Read `PRELOOP_LOG_FORMAT`, `RUST_LOG`, and standard `OTEL_*` vars.
    ///
    /// `Debug` and error paths never expose `OTEL_EXPORTER_OTLP_HEADERS` values
    /// or credential-bearing endpoint components (userinfo/query).
    pub fn from_env() -> Self {
        let log_format = std::env::var("PRELOOP_LOG_FORMAT")
            .ok()
            .and_then(|v| LogFormat::parse(&v))
            .unwrap_or(LogFormat::Auto);

        // CLI defaults to `info` when unset; the standalone server historically
        // used `EnvFilter::from_default_env()` with no fallback (silent when
        // unset). We unify on `info`.
        let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

        let service_name =
            std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "preloop".to_string());

        // Presence — not value — enables export. This is a deliberate
        // deviation from the OTel spec default `http://localhost:4318`.
        let generic = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .filter(|v| !v.trim().is_empty() && v.trim() != "none")
            .map(|v| v.trim_end_matches('/').to_string());

        // Signal-specific endpoint is a complete URL used as-is; the generic
        // base needs the /v1/<signal> suffix. Appending the suffix to a
        // signal-specific URL would produce `/v1/traces/v1/traces`, and
        // sending every signal to one signal-specific URL misroutes the rest.
        let resolve = |var: &str, suffix: &str| match std::env::var(var) {
            Ok(value) => {
                let value = value.trim();
                if value.eq_ignore_ascii_case("none") {
                    // An explicit per-signal disable must not fall back to
                    // the generic endpoint.
                    None
                } else if value.is_empty() {
                    generic.as_ref().map(|g| format!("{g}{suffix}"))
                } else {
                    Some(value.trim_end_matches('/').to_string())
                }
            }
            Err(_) => generic.as_ref().map(|g| format!("{g}{suffix}")),
        };
        let otel_logs_endpoint = resolve("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "/v1/logs");
        let otel_traces_endpoint = resolve("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "/v1/traces");
        let otel_metrics_endpoint = resolve("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "/v1/metrics");

        let generic_headers = std::env::var("OTEL_EXPORTER_OTLP_HEADERS")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let resolve_headers = |var: &str| {
            std::env::var(var)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .or_else(|| generic_headers.clone())
        };
        let otel_logs_headers = resolve_headers("OTEL_EXPORTER_OTLP_LOGS_HEADERS");
        let otel_traces_headers = resolve_headers("OTEL_EXPORTER_OTLP_TRACES_HEADERS");
        let otel_metrics_headers = resolve_headers("OTEL_EXPORTER_OTLP_METRICS_HEADERS");

        let otlp_enabled = otel_logs_endpoint.is_some()
            || otel_traces_endpoint.is_some()
            || otel_metrics_endpoint.is_some();
        let instance_id = Uuid::new_v4().to_string();

        Self {
            log_format,
            rust_log,
            service_name,
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            instance_id,
            otel_logs_endpoint,
            otel_traces_endpoint,
            otel_metrics_endpoint,
            otel_logs_headers,
            otel_traces_headers,
            otel_metrics_headers,
            otlp_enabled,
        }
    }

    /// Fully-resolved per-signal destinations for the SDK exporters.
    pub fn export_targets(&self) -> ExportTargets {
        let signal = |endpoint: &Option<String>, headers: &Option<String>| {
            endpoint.as_ref().map(|url| SignalTarget {
                url: url.clone(),
                headers: parse_headers(headers.as_deref()),
            })
        };
        ExportTargets {
            logs: signal(&self.otel_logs_endpoint, &self.otel_logs_headers),
            traces: signal(&self.otel_traces_endpoint, &self.otel_traces_headers),
            metrics: signal(&self.otel_metrics_endpoint, &self.otel_metrics_headers),
        }
    }

    /// Whether any `OTEL_EXPORTER_OTLP_HEADERS` was supplied (for health reporting).
    pub fn has_otel_headers(&self) -> bool {
        self.otel_logs_headers.is_some()
            || self.otel_traces_headers.is_some()
            || self.otel_metrics_headers.is_some()
    }

    /// Override `service.version` with the host binary's version. The crate's
    /// own version is meaningless to an operator reading telemetry.
    pub fn with_service_version(mut self, version: &str) -> Self {
        self.service_version = version.to_string();
        self
    }

    /// Sanitized endpoint for `Debug`/errors: strips userinfo and query.
    fn sanitized_endpoint(&self) -> Option<String> {
        self.otel_logs_endpoint.as_ref().map(|raw| {
            // Best-effort: hide `user:pass@` and `?...` without a URL parser dep.
            let without_query = raw.split('?').next().unwrap_or(raw);
            if let Some(at) = without_query.rfind('@') {
                // Keep scheme + host/path, hide userinfo.
                if let Some(scheme_end) = without_query.find("://") {
                    let scheme = &without_query[..scheme_end + 3];
                    return format!("{scheme}***@{}", &without_query[at + 1..]);
                }
                return format!("***@{}", &without_query[at + 1..]);
            }
            without_query.to_string()
        })
    }
}

impl fmt::Debug for ObservabilityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservabilityConfig")
            .field("log_format", &self.log_format)
            .field("rust_log", &self.rust_log)
            .field("service_name", &self.service_name)
            .field("service_version", &self.service_version)
            .field("instance_id", &self.instance_id)
            .field("otel_endpoint", &self.sanitized_endpoint())
            .field(
                "otel_headers",
                &(self.has_otel_headers().then_some("<redacted>")),
            )
            .field("otlp_enabled", &self.otlp_enabled)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// TaskHeartbeat registry
// ---------------------------------------------------------------------------

/// How a task gates `/readyz`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Criticality {
    /// Staleness returns 503 from `/readyz`.
    Critical,
    /// Staleness surfaces only in `/api/v1/status` and metrics.
    NonCritical,
}

/// Registry of long-lived background tasks. Generic replacement for per-task
/// `AtomicU64` timestamps; every `tokio::spawn` that outlives a request
/// registers here per invariant 15.
#[derive(Debug, Clone, Default)]
pub struct TaskHeartbeat {
    inner: Arc<RwLock<HashMap<u64, HeartbeatEntry>>>,
    next_id: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
struct HeartbeatEntry {
    name: &'static str,
    critical: Criticality,
    last_beat: Instant,
    panicked: bool,
}

impl TaskHeartbeat {
    /// Register a task. Returns a guard — normal `Drop` deregisters, while
    /// panic unwinding preserves the entry as failed for readiness checks.
    pub fn register(&self, name: &'static str, critical: Criticality) -> HeartbeatHandle {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner.write().insert(
            id,
            HeartbeatEntry {
                name,
                critical,
                last_beat: Instant::now(),
                panicked: false,
            },
        );
        HeartbeatHandle {
            registry: self.clone(),
            id,
            name,
        }
    }

    /// Record a beat for `name`. No-op if not registered (so tests can `noop()` without registering).
    pub fn beat(&self, name: &'static str) {
        for entry in self
            .inner
            .write()
            .values_mut()
            .filter(|entry| entry.name == name)
        {
            entry.last_beat = Instant::now();
        }
    }

    fn beat_id(&self, id: u64) {
        if let Some(entry) = self.inner.write().get_mut(&id) {
            entry.last_beat = Instant::now();
        }
    }

    fn deregister(&self, id: u64) {
        self.inner.write().remove(&id);
    }

    fn mark_panicked(&self, id: u64) {
        if let Some(entry) = self.inner.write().get_mut(&id) {
            entry.panicked = true;
        }
    }

    /// Snapshot for `/readyz` and `/api/v1/status`.
    pub fn snapshot(&self) -> Vec<TaskSnapshot> {
        self.inner
            .read()
            .values()
            .map(|e| TaskSnapshot {
                name: e.name,
                critical: e.critical,
                heartbeat_age: e.last_beat.elapsed(),
            })
            .collect()
    }

    /// Whether any critical task is stale beyond `threshold`.
    pub fn any_critical_stale(&self, threshold: Duration) -> Option<&'static str> {
        // Hold read lock across iteration to avoid TOCTOU.
        let guard = self.inner.read();
        for e in guard.values() {
            if e.critical == Criticality::Critical
                && (e.panicked || e.last_beat.elapsed() > threshold)
            {
                return Some(e.name);
            }
        }
        None
    }

    /// Number of registered tasks (for tests).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Whether any task is registered (for tests).
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

/// Guard — `beat()` updates, normal `Drop` deregisters, and panic unwinding
/// preserves the task as failed.
pub struct HeartbeatHandle {
    registry: TaskHeartbeat,
    id: u64,
    name: &'static str,
}

impl HeartbeatHandle {
    pub fn beat(&self) {
        self.registry.beat_id(self.id);
    }
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.registry.mark_panicked(self.id);
        } else {
            self.registry.deregister(self.id);
        }
    }
}

impl fmt::Debug for HeartbeatHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeartbeatHandle")
            .field("name", &self.name)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub name: &'static str,
    pub critical: Criticality,
    pub heartbeat_age: Duration,
}

// ---------------------------------------------------------------------------
// LimitRegistry
// ---------------------------------------------------------------------------

/// Bounded-cap registry. `limit` is a `&'static str` constant name (finite set), never a value.
#[derive(Debug, Clone, Default)]
pub struct LimitRegistry {
    inner: Arc<RwLock<HashMap<&'static str, LimitEntry>>>,
}

#[derive(Debug, Clone, Default)]
struct LimitEntry {
    value: usize,
    dropped: u64,
    rejected: u64,
}

impl LimitRegistry {
    /// Register a cap with its configured ceiling. Re-registration updates
    /// the ceiling and keeps the counters. One lock acquisition so the write
    /// is atomic against a concurrent `record_drop`/`record_reject`.
    pub fn register(&self, limit: &'static str, value: usize) {
        self.inner.write().entry(limit).or_default().value = value;
    }

    pub fn record_drop(&self, limit: &'static str, n: u64) {
        if let Some(entry) = self.inner.write().get_mut(limit) {
            entry.dropped = entry.dropped.saturating_add(n);
        }
    }

    pub fn record_reject(&self, limit: &'static str) {
        if let Some(entry) = self.inner.write().get_mut(limit) {
            entry.rejected = entry.rejected.saturating_add(1);
        }
    }

    pub fn snapshot(&self) -> Vec<LimitSnapshot> {
        self.inner
            .read()
            .iter()
            .map(|(limit, e)| LimitSnapshot {
                limit,
                value: e.value,
                dropped: e.dropped,
                rejected: e.rejected,
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct LimitSnapshot {
    pub limit: &'static str,
    pub value: usize,
    pub dropped: u64,
    pub rejected: u64,
}

// ---------------------------------------------------------------------------
// Observability handle + Runtime
// ---------------------------------------------------------------------------

/// Cloneable handle — cheap to clone into `AppState` and `RunnerPoolConfig`.
#[derive(Clone)]
pub struct Observability {
    inner: Arc<Inner>,
}

struct Inner {
    config: Arc<ObservabilityConfig>,
    heartbeat: TaskHeartbeat,
    limits: LimitRegistry,
    metrics: Arc<metrics::MetricsRegistry>,
    vm_registry: Arc<vm_telemetry::VmTelemetryRegistry>,
    tracer: Option<opentelemetry_sdk::trace::Tracer>,
    logger: Option<opentelemetry_sdk::logs::SdkLogger>,
    prometheus_registry: Option<prometheus::Registry>,
    is_noop: bool,
}

impl fmt::Debug for Observability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Observability").finish_non_exhaustive()
    }
}

impl Observability {
    /// Allocation-light handle for tests and library-only consumers. Performs no I/O, no socket.
    pub fn noop() -> Self {
        let config = ObservabilityConfig {
            log_format: LogFormat::Auto,
            rust_log: "info".to_string(),
            service_name: "preloop".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            instance_id: Uuid::new_v4().to_string(),
            otel_logs_endpoint: None,
            otel_traces_endpoint: None,
            otel_metrics_endpoint: None,
            otel_logs_headers: None,
            otel_traces_headers: None,
            otel_metrics_headers: None,
            otlp_enabled: false,
        };
        Self {
            inner: Arc::new(Inner {
                config: Arc::new(config),
                heartbeat: TaskHeartbeat::default(),
                limits: LimitRegistry::default(),
                metrics: Arc::new(metrics::MetricsRegistry::default()),
                vm_registry: Arc::new(vm_telemetry::VmTelemetryRegistry::default()),
                tracer: None,
                logger: None,
                prometheus_registry: None,
                is_noop: true,
            }),
        }
    }

    /// Real handle from `ObservabilityConfig`. Does not install the global subscriber — pair with `ObservabilityRuntime`.
    pub fn from_config(config: ObservabilityConfig) -> (Self, ObservabilityRuntime) {
        let is_noop = !config.otlp_enabled;
        let targets = config.export_targets();
        // One rustls client for every signal — the same stack the rest of the
        // workspace uses (reqwest 0.13).
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .ok();
        let resource = Resource::builder()
            .with_attributes(vec![
                KeyValue::new("service.name", config.service_name.clone()),
                KeyValue::new("service.version", config.service_version.clone()),
                KeyValue::new("service.instance.id", config.instance_id.clone()),
            ])
            .build();

        // ---- Traces: OTLP/HTTP batch exporter when a traces endpoint exists.
        let tracer_provider = targets.traces.as_ref().and_then(|target| {
            let exporter = build_span_exporter(target, http_client.as_ref())?;
            let provider = SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource.clone())
                .build();
            Some(provider)
        });

        // ---- Logs: OTLP/HTTP batch exporter when a logs endpoint exists.
        let logger_provider = targets.logs.as_ref().and_then(|target| {
            let exporter = build_log_exporter(target, http_client.as_ref())?;
            let provider = SdkLoggerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource.clone())
                .build();
            Some(provider)
        });

        // ---- Metrics: Prometheus reader always (local /metrics), OTLP
        // periodic reader when a metrics endpoint exists. One instrument set,
        // two readers — the plan's multi-reader requirement.
        let prometheus_registry = prometheus::Registry::new();
        let prometheus_exporter = opentelemetry_prometheus::exporter()
            .with_registry(prometheus_registry.clone())
            .without_scope_info()
            .build()
            .ok();
        let mut meter_builder = SdkMeterProvider::builder()
            .with_resource(resource.clone())
            .with_view(histogram_view("http.server.request.duration", HTTP_BUCKETS))
            .with_view(histogram_view(
                "preloop.store.operation.duration",
                STORE_BUCKETS,
            ))
            .with_view(histogram_view("preloop.job.queue.wait", QUEUE_BUCKETS));
        if let Some(exporter) = prometheus_exporter {
            meter_builder = meter_builder.with_reader(exporter);
        }
        if let Some(target) = targets.metrics.as_ref() {
            if let Some(exporter) = build_metric_exporter(target, http_client.as_ref()) {
                meter_builder = meter_builder.with_reader(
                    PeriodicReader::builder(exporter)
                        .with_interval(Duration::from_secs(60))
                        .build(),
                );
            }
        }
        let meter_provider = meter_builder.build();
        let meter = meter_provider.meter("preloop");
        let metrics = Arc::new(metrics::MetricsRegistry::from_meter(meter));

        let tracer = tracer_provider
            .as_ref()
            .map(|provider| provider.tracer("preloop"));
        let logger = logger_provider
            .as_ref()
            .map(|provider| provider.logger("preloop"));

        let handle = Self {
            inner: Arc::new(Inner {
                config: Arc::new(config),
                heartbeat: TaskHeartbeat::default(),
                limits: LimitRegistry::default(),
                metrics,
                vm_registry: Arc::new(vm_telemetry::VmTelemetryRegistry::default()),
                tracer,
                logger,
                prometheus_registry: Some(prometheus_registry),
                is_noop,
            }),
        };
        let runtime = ObservabilityRuntime {
            _handle: handle.clone(),
            tracer_provider,
            meter_provider: Some(meter_provider),
            logger_provider,
        };
        (handle, runtime)
    }

    pub fn is_noop(&self) -> bool {
        self.inner.is_noop
    }

    pub fn otlp_enabled(&self) -> bool {
        self.inner.config.otlp_enabled
    }

    pub fn instance_id(&self) -> &str {
        &self.inner.config.instance_id
    }

    pub fn service_name(&self) -> &str {
        &self.inner.config.service_name
    }

    pub fn heartbeat(&self) -> &TaskHeartbeat {
        &self.inner.heartbeat
    }

    pub fn limits(&self) -> &LimitRegistry {
        &self.inner.limits
    }

    pub fn metrics(&self) -> &metrics::MetricsRegistry {
        &self.inner.metrics
    }

    pub fn vm_registry(&self) -> &vm_telemetry::VmTelemetryRegistry {
        &self.inner.vm_registry
    }

    /// The SDK tracer, when a traces endpoint is configured. `None` for
    /// no-op handles — callers skip span work entirely.
    pub fn tracer(&self) -> Option<opentelemetry_sdk::trace::Tracer> {
        self.inner.tracer.clone()
    }

    /// Whether spans are worth building. Lets a caller skip id and timestamp
    /// work entirely when nothing would consume the result.
    pub fn tracing_enabled(&self) -> bool {
        self.inner.tracer.is_some()
    }

    /// Enqueue a log record for OTLP export. No-op when export is disabled.
    pub fn export_log(
        &self,
        severity: &'static str,
        body: impl Into<String>,
        attributes: Vec<(String, String)>,
    ) {
        self.export_log_in_span(severity, body, attributes, None);
    }

    /// As [`Observability::export_log`], correlated with a span so a backend
    /// can pivot from a log line to the request that produced it.
    pub fn export_log_in_span(
        &self,
        severity: &'static str,
        body: impl Into<String>,
        attributes: Vec<(String, String)>,
        context: Option<&opentelemetry::trace::SpanContext>,
    ) {
        let Some(logger) = &self.inner.logger else {
            return;
        };
        let mut record = logger.create_log_record();
        record.set_severity_text(severity);
        record.set_severity_number(severity_number(severity));
        record.set_body(AnyValue::String(body.into().into()));
        record.add_attributes(
            attributes
                .into_iter()
                .map(|(k, v)| (Key::new(k), AnyValue::String(v.into()))),
        );
        if let Some(ctx) = context {
            record.set_trace_context(ctx.trace_id(), ctx.span_id(), Some(ctx.trace_flags()));
        }
        logger.emit(record);
    }

    pub fn config(&self) -> &ObservabilityConfig {
        &self.inner.config
    }

    /// Render the Prometheus registry text for `/metrics`. Empty when the
    /// registry is absent (noop handles).
    pub fn render_metrics(&self) -> String {
        let Some(registry) = &self.inner.prometheus_registry else {
            return String::new();
        };
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let families = registry.gather();
        let mut out = Vec::new();
        if encoder.encode(&families, &mut out).is_ok() {
            String::from_utf8_lossy(&out).into_owned()
        } else {
            String::new()
        }
    }
}

fn severity_number(severity: &str) -> Severity {
    match severity {
        "TRACE" => Severity::Trace,
        "DEBUG" => Severity::Debug,
        "INFO" => Severity::Info,
        "WARN" => Severity::Warn,
        "ERROR" => Severity::Error,
        _ => Severity::Info,
    }
}

/// Build an OTLP/HTTP span exporter from a resolved signal target, reusing
/// the shared rustls reqwest client. `None` on build failure — fail open.
fn build_span_exporter(
    target: &SignalTarget,
    http_client: Option<&reqwest::Client>,
) -> Option<opentelemetry_otlp::SpanExporter> {
    use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithHttpConfig};
    let mut builder = SpanExporter::builder()
        .with_http()
        .with_endpoint(&target.url);
    if !target.headers.is_empty() {
        builder = builder.with_headers(
            target
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashMap<_, _>>(),
        );
    }
    if let Some(client) = http_client {
        builder = builder.with_http_client(client.clone());
    }
    builder.with_timeout(Duration::from_secs(10)).build().ok()
}

/// Build an OTLP/HTTP log exporter. `None` on build failure — fail open.
fn build_log_exporter(
    target: &SignalTarget,
    http_client: Option<&reqwest::Client>,
) -> Option<opentelemetry_otlp::LogExporter> {
    use opentelemetry_otlp::{LogExporter, WithExportConfig, WithHttpConfig};
    let mut builder = LogExporter::builder()
        .with_http()
        .with_endpoint(&target.url);
    if !target.headers.is_empty() {
        builder = builder.with_headers(
            target
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashMap<_, _>>(),
        );
    }
    if let Some(client) = http_client {
        builder = builder.with_http_client(client.clone());
    }
    builder.with_timeout(Duration::from_secs(10)).build().ok()
}

/// Build an OTLP/HTTP metric exporter. `None` on build failure — fail open.
fn build_metric_exporter(
    target: &SignalTarget,
    http_client: Option<&reqwest::Client>,
) -> Option<opentelemetry_otlp::MetricExporter> {
    use opentelemetry_otlp::{MetricExporter, WithExportConfig, WithHttpConfig};
    let mut builder = MetricExporter::builder()
        .with_http()
        .with_endpoint(&target.url)
        .with_temporality(Temporality::Cumulative);
    if !target.headers.is_empty() {
        builder = builder.with_headers(
            target
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashMap<_, _>>(),
        );
    }
    if let Some(client) = http_client {
        builder = builder.with_http_client(client.clone());
    }
    builder.with_timeout(Duration::from_secs(10)).build().ok()
}

/// A view that pins a histogram's explicit bucket boundaries by instrument
/// name. Without it the SDK default boundaries would apply to every
/// histogram and the latency percentiles would be wrong.
fn histogram_view(
    name: &'static str,
    boundaries: &'static [f64],
) -> impl Fn(&opentelemetry_sdk::metrics::Instrument) -> Option<opentelemetry_sdk::metrics::Stream>
       + Send
       + Sync
       + 'static {
    move |instrument: &opentelemetry_sdk::metrics::Instrument| {
        if instrument.name() != name {
            return None;
        }
        opentelemetry_sdk::metrics::Stream::builder()
            .with_aggregation(
                opentelemetry_sdk::metrics::Aggregation::ExplicitBucketHistogram {
                    boundaries: boundaries.to_vec(),
                    record_min_max: true,
                },
            )
            .build()
            .ok()
    }
}

/// Owns subscriber/provider guards and performs bounded shutdown/flush.
///
/// On `shutdown`, flushes and shuts down each provider with a 2s bound
/// (invariant 3), then exits. Tests use scoped subscribers and never install
/// the global one twice.
pub struct ObservabilityRuntime {
    _handle: Observability,
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
}

impl ObservabilityRuntime {
    /// Install the global `tracing_subscriber` once: stderr fmt/JSON layer,
    /// the OTel trace layer, and the OTel log bridge. Safe to call at most
    /// once per process; tests use `install_fmt_subscriber_for_test` instead.
    pub fn install_fmt_subscriber(&self) {
        let config = self._handle.config();
        let filter =
            EnvFilter::try_new(&config.rust_log).unwrap_or_else(|_| EnvFilter::new("info"));
        let fmt = config.log_format.resolve();

        let fmt_layer = match fmt {
            LogFormat::Json => tracing_subscriber::fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .boxed(),
            LogFormat::Pretty | LogFormat::Auto => tracing_subscriber::fmt::layer()
                .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
                .boxed(),
        };
        // Build the layer chain as a boxed `Layer` so each signal's layer can
        // be appended without naming the concrete `Layered` type, then wrap it
        // in the registry. The tracing-opentelemetry layer requires
        // `LookupSpan`, which the registry provides.
        let mut chain: Box<
            dyn tracing_subscriber::Layer<tracing_subscriber::registry::Registry> + Send + Sync,
        > = Box::new(fmt_layer.with_filter(filter));
        if let Some(provider) = &self.tracer_provider {
            let tracer = provider.tracer("preloop");
            let layer = tracing_opentelemetry::layer().with_tracer(tracer);
            chain = Box::new(chain.and_then(layer));
        }
        if let Some(provider) = &self.logger_provider {
            let layer = OpenTelemetryTracingBridge::new(provider);
            chain = Box::new(chain.and_then(layer));
        }
        let _ = tracing::subscriber::set_global_default(Registry::default().with(chain));
    }

    /// Flush exporters for at most 2s, then exit. Each provider's bounded
    /// queue is drained; a hung backend drops the remainder rather than
    /// delaying shutdown. Export failure is logged by the SDK, never
    /// propagated.
    pub async fn shutdown(mut self) {
        let timeout = Duration::from_secs(2);
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown_with_timeout(timeout);
        }
        if let Some(provider) = self.meter_provider.take() {
            let _ = provider.shutdown_with_timeout(timeout);
        }
        if let Some(provider) = self.logger_provider.take() {
            let _ = provider.shutdown_with_timeout(timeout);
        }
    }
}

impl fmt::Debug for ObservabilityRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservabilityRuntime").finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// `ObservabilityConfig::from_env` reads process-global `OTEL_*`
    /// variables, and several tests mutate them. Cargo runs unit tests on
    /// many threads inside one process, so these tests race on the
    /// environment and fail intermittently unless serialized.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Guard that serializes the env-mutating tests.
    fn env_guard() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn noop_performs_no_network_io() {
        let obs = Observability::noop();
        assert!(obs.is_noop());
        assert!(!obs.otlp_enabled());
        assert!(!obs.instance_id().is_empty());
        assert!(obs.tracer().is_none());
        assert!(!obs.tracing_enabled());
    }

    #[test]
    fn absent_endpoint_means_disabled_not_localhost() {
        let _guard = env_guard();
        // Ensure no ambient OTEL vars leak into the test.
        for k in [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        ] {
            std::env::remove_var(k);
        }
        // Also headers, so `has_otel_headers` is false.
        for k in [
            "OTEL_EXPORTER_OTLP_HEADERS",
            "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
            "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
            "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
        ] {
            std::env::remove_var(k);
        }
        let cfg = ObservabilityConfig::from_env();
        assert!(
            !cfg.otlp_enabled,
            "absent endpoint must be disabled, not localhost:4318"
        );
        assert!(!cfg.otlp_enabled);
        let (obs, _rt) = Observability::from_config(cfg);
        assert!(!obs.otlp_enabled());
        assert!(obs.is_noop());
        // No exporter workers: a no-op handle has no tracer, no logger.
        assert!(obs.tracer().is_none());
    }

    #[test]
    fn none_disables_signal() {
        let _guard = env_guard();
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "none");
        let cfg = ObservabilityConfig::from_env();
        assert!(!cfg.otlp_enabled, "`none` must disable export");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    }

    #[test]
    fn signal_specific_endpoint_is_used_as_is() {
        let _guard = env_guard();
        for k in [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        ] {
            std::env::remove_var(k);
        }
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "http://collector:4318/v1/traces",
        );
        let cfg = ObservabilityConfig::from_env();
        let targets = cfg.export_targets();
        assert!(cfg.otlp_enabled);
        // The signal-specific URL must be used verbatim — appending the
        // suffix would produce /v1/traces/v1/traces.
        assert_eq!(
            targets.traces.as_ref().unwrap().url,
            "http://collector:4318/v1/traces"
        );
        // And it must not hijack the other signals.
        assert!(targets.logs.is_none());
        assert!(targets.metrics.is_none());
        std::env::remove_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    }

    #[test]
    fn generic_endpoint_gets_the_signal_suffix() {
        let _guard = env_guard();
        for k in [
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        ] {
            std::env::remove_var(k);
        }
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318");
        let cfg = ObservabilityConfig::from_env();
        let targets = cfg.export_targets();
        assert_eq!(
            targets.logs.as_ref().unwrap().url,
            "http://collector:4318/v1/logs"
        );
        assert_eq!(
            targets.traces.as_ref().unwrap().url,
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            targets.metrics.as_ref().unwrap().url,
            "http://collector:4318/v1/metrics"
        );
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    }

    #[test]
    fn signal_specific_none_disables_only_that_signal() {
        let _guard = env_guard();
        for k in [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        ] {
            std::env::remove_var(k);
        }
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318");
        std::env::set_var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "none");

        let targets = ObservabilityConfig::from_env().export_targets();
        assert!(targets.traces.is_none());
        assert_eq!(
            targets.logs.as_ref().unwrap().url,
            "http://collector:4318/v1/logs"
        );
        assert_eq!(
            targets.metrics.as_ref().unwrap().url,
            "http://collector:4318/v1/metrics"
        );

        for k in [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        ] {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn heartbeat_register_beat_deregister() {
        let obs = Observability::noop();
        assert_eq!(obs.heartbeat().len(), 0);
        {
            let h = obs.heartbeat().register("reaper", Criticality::Critical);
            assert_eq!(obs.heartbeat().len(), 1);
            h.beat();
            assert_eq!(obs.heartbeat().len(), 1);
            // staleness: threshold 50ms, just-beat handle is fresh.
            assert!(obs
                .heartbeat()
                .any_critical_stale(Duration::from_millis(50))
                .is_none());
        }
        assert_eq!(obs.heartbeat().len(), 0, "Drop must deregister");
    }

    #[test]
    fn heartbeat_duplicate_handles_are_independent() {
        let obs = Observability::noop();
        let first = obs.heartbeat().register("same_name", Criticality::Critical);
        let second = obs.heartbeat().register("same_name", Criticality::Critical);
        assert_eq!(obs.heartbeat().len(), 2);

        drop(first);
        assert_eq!(obs.heartbeat().len(), 1);
        second.beat();
        assert!(obs
            .heartbeat()
            .any_critical_stale(Duration::from_secs(1))
            .is_none());
    }

    #[test]
    fn critical_heartbeat_survives_panic_as_stale() {
        let obs = Observability::noop();
        let registry = obs.heartbeat().clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _handle = registry.register("panicked_task", Criticality::Critical);
            panic!("test critical task failure");
        }));

        assert!(result.is_err());
        assert_eq!(registry.len(), 1);
        assert_eq!(
            registry.any_critical_stale(Duration::from_secs(1)),
            Some("panicked_task")
        );
    }

    #[test]
    fn critical_stale_detection() {
        let obs = Observability::noop();
        let _h = obs
            .heartbeat()
            .register("scheduler_scan", Criticality::Critical);
        // Sleep past threshold — stale.
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(
            obs.heartbeat().any_critical_stale(Duration::from_millis(5)),
            Some("scheduler_scan")
        );
        // Non-critical with same age must not gate readiness.
        let obs2 = Observability::noop();
        let _h2 = obs2
            .heartbeat()
            .register("snapshot_gc", Criticality::NonCritical);
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(
            obs2.heartbeat()
                .any_critical_stale(Duration::from_millis(5)),
            None
        );
    }

    #[test]
    fn limit_registry_counts() {
        let obs = Observability::noop();
        obs.limits().register("QUEUE_MAX_PENDING", 100);
        obs.limits()
            .register("LIVE_LOG_MAX_BYTES", 64 * 1024 * 1024);
        obs.limits().record_reject("QUEUE_MAX_PENDING");
        obs.limits().record_drop("LIVE_LOG_MAX_BYTES", 3);
        let snap = obs.limits().snapshot();
        let q = snap
            .iter()
            .find(|s| s.limit == "QUEUE_MAX_PENDING")
            .unwrap();
        assert_eq!(q.value, 100);
        assert_eq!(q.rejected, 1);
        let l = snap
            .iter()
            .find(|s| s.limit == "LIVE_LOG_MAX_BYTES")
            .unwrap();
        assert_eq!(l.dropped, 3);
    }

    #[test]
    fn debug_redacts_headers_and_endpoint_userinfo() {
        let _guard = env_guard();
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "https://user:secret@example.com:4318/v1/traces?token=abc",
        );
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_HEADERS",
            "Authorization=Bearer secret123",
        );
        let cfg = ObservabilityConfig::from_env();
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains("secret"),
            "Debug must not contain secret material: {dbg}"
        );
        assert!(
            !dbg.contains("Authorization"),
            "Debug must not contain header values: {dbg}"
        );
        assert!(
            !dbg.contains("user:"),
            "Debug must not contain userinfo: {dbg}"
        );
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_HEADERS");
    }

    #[test]
    fn log_format_auto_resolves() {
        assert_eq!(LogFormat::parse("auto"), Some(LogFormat::Auto));
        assert_eq!(LogFormat::parse("PRETTY"), Some(LogFormat::Pretty));
        assert_eq!(LogFormat::parse("json"), Some(LogFormat::Json));
        assert_eq!(LogFormat::parse("bogus"), None);
    }

    #[tokio::test]
    async fn shutdown_is_bounded() {
        let (obs, rt) = Observability::from_config(ObservabilityConfig::from_env());
        // Must not hang even though there are no exporters.
        tokio::time::timeout(Duration::from_secs(3), rt.shutdown())
            .await
            .expect("shutdown must be bounded to 2s");
        drop(obs);
    }
}
