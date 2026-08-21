//! `preloop-observability` — observability handle for Preloop.
//!
//! Small, explicit API with no dependency on server/orchestrator internals. Both
//! `preloop` and `preloop-server` construct one handle/runtime before building
//! `ServerConfig`; the handle is cloned into `AppState` and `RunnerPoolConfig`.
//! Tests use `Observability::noop()` which performs no network I/O.
//!
//! Invariants from the plan:
//! - Fail open: export failure never rejects a workflow.
//! - Bounded queues, 2s flush, no backend by default (absent `OTEL_EXPORTER_OTLP_*` = disabled, not `localhost:4318`).
//! - Always retain `stderr`/`journald` even when OTLP is configured.
//! - `Debug` on config never reveals headers or credential-bearing endpoint parts.

pub mod export;
pub mod metrics;
pub mod status;
pub mod vm_telemetry;

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use uuid::Uuid;

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
        let resolve = |var: &str, suffix: &str| {
            std::env::var(var)
                .ok()
                .filter(|v| !v.trim().is_empty() && v.trim() != "none")
                .map(|v| v.trim_end_matches('/').to_string())
                .or_else(|| generic.as_ref().map(|g| format!("{g}{suffix}")))
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

    /// Fully-resolved per-signal destinations for the export worker.
    pub fn export_targets(&self) -> export::ExportTargets {
        let signal = |endpoint: &Option<String>, headers: &Option<String>| {
            endpoint.as_ref().map(|url| export::SignalTarget {
                url: url.clone(),
                headers: export::parse_headers(headers.as_deref()),
            })
        };
        export::ExportTargets {
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
                &(self.has_otel_headers().then(|| "<redacted>")),
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
    inner: Arc<RwLock<HashMap<&'static str, HeartbeatEntry>>>,
}

#[derive(Debug, Clone)]
struct HeartbeatEntry {
    critical: Criticality,
    last_beat: Instant,
}

impl TaskHeartbeat {
    /// Register a task. Returns a guard — `Drop` deregisters.
    pub fn register(&self, name: &'static str, critical: Criticality) -> HeartbeatHandle {
        self.inner.write().insert(
            name,
            HeartbeatEntry {
                critical,
                last_beat: Instant::now(),
            },
        );
        HeartbeatHandle {
            registry: self.clone(),
            name,
        }
    }

    /// Record a beat for `name`. No-op if not registered (so tests can `noop()` without registering).
    pub fn beat(&self, name: &'static str) {
        if let Some(entry) = self.inner.write().get_mut(name) {
            entry.last_beat = Instant::now();
        }
    }

    pub(crate) fn deregister(&self, name: &'static str) {
        self.inner.write().remove(name);
    }

    /// Snapshot for `/readyz` and `/api/v1/status`.
    pub fn snapshot(&self) -> Vec<TaskSnapshot> {
        self.inner
            .read()
            .iter()
            .map(|(name, e)| TaskSnapshot {
                name,
                critical: e.critical,
                heartbeat_age: e.last_beat.elapsed(),
            })
            .collect()
    }

    /// Whether any critical task is stale beyond `threshold`.
    pub fn any_critical_stale(&self, threshold: Duration) -> Option<&'static str> {
        // Hold read lock across iteration to avoid TOCTOU.
        let guard = self.inner.read();
        for (name, e) in guard.iter() {
            if e.critical == Criticality::Critical && e.last_beat.elapsed() > threshold {
                return Some(*name);
            }
        }
        None
    }

    /// Number of registered tasks (for tests).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }
}

/// Guard — `beat()` updates, `Drop` deregisters.
pub struct HeartbeatHandle {
    registry: TaskHeartbeat,
    name: &'static str,
}

impl HeartbeatHandle {
    pub fn beat(&self) {
        self.registry.beat(self.name);
    }
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        self.registry.deregister(self.name);
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

#[derive(Debug, Clone)]
struct Inner {
    config: Arc<ObservabilityConfig>,
    heartbeat: TaskHeartbeat,
    limits: LimitRegistry,
    metrics: Arc<metrics::MetricsRegistry>,
    vm_registry: Arc<vm_telemetry::VmTelemetryRegistry>,
    exporter: Option<export::Exporter>,
    is_noop: bool,
}

/// Cloneable handle — cheap to clone into `AppState` and `RunnerPoolConfig`.
#[derive(Debug, Clone)]
pub struct Observability {
    inner: Arc<Inner>,
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
                exporter: None,
                is_noop: true,
            }),
        }
    }

    /// Real handle from `ObservabilityConfig`. Does not install the global subscriber — pair with `ObservabilityRuntime`.
    pub fn from_config(config: ObservabilityConfig) -> (Self, ObservabilityRuntime) {
        let is_noop = !config.otlp_enabled;
        // Absent endpoint spawns nothing at all — no worker, no socket.
        // The registry is shared with the worker so metric export scrapes the
        // same instruments `/metrics` renders — one source, never two.
        let metrics = Arc::new(metrics::MetricsRegistry::default());
        let spawned = export::spawn(
            &config.export_targets(),
            &config.service_name,
            &config.instance_id,
            &config.service_version,
            Some(metrics.clone()),
        );
        let exporter = spawned.as_ref().map(|(exporter, _, _)| exporter.clone());
        let worker = spawned.map(|(exporter, _, join)| (exporter, join));
        let handle = Self {
            inner: Arc::new(Inner {
                config: Arc::new(config),
                heartbeat: TaskHeartbeat::default(),
                limits: LimitRegistry::default(),
                metrics,
                vm_registry: Arc::new(vm_telemetry::VmTelemetryRegistry::default()),
                exporter,
                is_noop,
            }),
        };
        let runtime = ObservabilityRuntime::new(handle.clone(), worker);
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
        context: Option<&export::SpanContext>,
    ) {
        if let Some(exporter) = &self.inner.exporter {
            exporter.log(export::LogRecord {
                severity,
                body: body.into(),
                attributes,
                trace_id: context.map(|c| c.trace_id.clone()),
                span_id: context.map(|c| c.span_id.clone()),
                // Stamp at enqueue, not at flush: a batch-level timestamp
                // would collapse a flush window's records to one instant.
                observed_unix_nanos: export::now_nanos(),
            });
        }
    }

    /// Enqueue a completed span. No-op when export is disabled.
    pub fn export_span(&self, record: export::SpanRecord) {
        if let Some(exporter) = &self.inner.exporter {
            exporter.span(record);
        }
    }

    /// Whether spans are worth building. Lets a caller skip id and timestamp
    /// work entirely when nothing would consume the result.
    pub fn tracing_enabled(&self) -> bool {
        self.inner.exporter.is_some()
    }

    /// Export health for `/api/v1/status` and `preloop.telemetry.export`.
    pub fn export_health(&self) -> Option<&Arc<export::ExportHealth>> {
        self.inner.exporter.as_ref().map(|e| e.health())
    }

    pub fn config(&self) -> &ObservabilityConfig {
        &self.inner.config
    }
}

/// Owns subscriber/provider guards and performs bounded shutdown/flush.
///
/// On `Drop`, attempts to flush for at most 2s per invariant 3, then exits.
/// Tests use scoped subscribers and never install the global one twice.
pub struct ObservabilityRuntime {
    _handle: Observability,
    // The export worker plus its completion handle, taken when the worker
    // exists. `shutdown` drops the sender so the worker drains, then awaits
    // the join inside the 2s bound.
    worker: Option<(export::Exporter, tokio::task::JoinHandle<()>)>,
    // Hold the tracing guard so it isn't dropped early when we use a
    // non-global dispatcher in tests. For the global install, this is `None`
    // and the global dispatcher owns the guard.
    _guard: Option<
        tracing_subscriber::reload::Handle<
            tracing_subscriber::EnvFilter,
            tracing_subscriber::Registry,
        >,
    >,
}

impl ObservabilityRuntime {
    fn new(
        handle: Observability,
        worker: Option<(export::Exporter, tokio::task::JoinHandle<()>)>,
    ) -> Self {
        // Does not install the global subscriber here — the binaries do
        // that via `install_fmt_subscriber`.
        Self {
            _handle: handle,
            worker,
            _guard: None,
        }
    }

    /// Install the global `tracing_subscriber::fmt` layer once, respecting
    /// `PRELOOP_LOG_FORMAT` and `RUST_LOG` from `config`. Safe to call at most
    /// once per process; tests use `install_fmt_subscriber_for_test` instead.
    pub fn install_fmt_subscriber(config: &ObservabilityConfig) {
        let filter = tracing_subscriber::EnvFilter::try_new(&config.rust_log)
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        let fmt = config.log_format.resolve();
        match fmt {
            LogFormat::Json => {
                let subscriber = tracing_subscriber::fmt()
                    .with_env_filter(filter)
                    .json()
                    .with_current_span(true)
                    .with_span_list(true)
                    .finish();
                let _ = tracing::subscriber::set_global_default(subscriber);
            }
            LogFormat::Pretty | LogFormat::Auto => {
                let subscriber = tracing_subscriber::fmt()
                    .with_env_filter(filter)
                    .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
                    .finish();
                let _ = tracing::subscriber::set_global_default(subscriber);
            }
        }
    }

    /// Flush exporters for at most 2s, then exit. The worker is signalled to
    /// drain its buffers; if it cannot finish within the bound the remaining
    /// records are dropped rather than delaying shutdown. Export failure is
    /// logged by the worker, never propagated.
    pub async fn shutdown(mut self) {
        let Some((exporter, join)) = self.worker.take() else {
            return;
        };
        exporter.request_shutdown();
        tokio::time::timeout(Duration::from_secs(2), join)
            .await
            .ok();
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
        // Must not hang even though there's no exporter.
        tokio::time::timeout(Duration::from_secs(3), rt.shutdown())
            .await
            .expect("shutdown must be bounded to 2s");
        drop(obs);
    }
}
