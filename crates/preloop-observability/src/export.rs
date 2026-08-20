//! Bounded OTLP/HTTP exporter using the existing reqwest+rustls stack.
//!
//! OTLP JSON encoding (`application/json`) per the OTLP/HTTP spec, so no
//! protobuf or tonic dependency. Invariants:
//! - Fail open: an export error is logged once per failure class and never
//!   propagates to a caller.
//! - No request-path export: callers push into a bounded channel; a single
//!   background worker drains it. Overflow drops and counts.
//! - No backend by default: constructed only when an endpoint is configured.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use tokio::sync::mpsc;

/// Bounded queue depth. Overflow drops the newest record and increments
/// `dropped`, which surfaces as `preloop.telemetry.export{outcome="dropped"}`.
const QUEUE_CAPACITY: usize = 2048;
const BATCH_MAX: usize = 256;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
pub struct ExportHealth {
    pub sent: AtomicU64,
    pub failed: AtomicU64,
    pub dropped: AtomicU64,
    pub last_success_unix: AtomicU64,
    pub last_failure_unix: AtomicU64,
}

impl ExportHealth {
    fn record_success(&self, n: u64) {
        self.sent.fetch_add(n, Ordering::Relaxed);
        self.last_success_unix.store(now_secs(), Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.last_failure_unix.store(now_secs(), Ordering::Relaxed);
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Wall-clock nanoseconds since the epoch, for OTLP timestamps.
pub fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// W3C Trace Context: 16-byte trace id / 8-byte span id, lowercase hex.
///
/// Generated locally when a request arrives without a `traceparent`, or
/// adopted from the incoming header so a caller's trace continues through
/// the control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanContext {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
}

impl SpanContext {
    /// New root context with a random trace and span id.
    pub fn root() -> Self {
        Self {
            trace_id: random_hex(16),
            span_id: random_hex(8),
            parent_span_id: None,
        }
    }

    /// Child of an incoming `traceparent`, or a new root when absent/invalid.
    ///
    /// Format: `00-<32 hex trace>-<16 hex span>-<2 hex flags>`. A malformed
    /// header starts a new trace rather than failing the request — telemetry
    /// must never reject traffic.
    pub fn from_traceparent(header: Option<&str>) -> Self {
        let Some(raw) = header else {
            return Self::root();
        };
        let parts: Vec<&str> = raw.trim().split('-').collect();
        if parts.len() != 4 {
            return Self::root();
        }
        let (version, trace_id, parent_span_id) = (parts[0], parts[1], parts[2]);
        let valid = version.len() == 2
            && trace_id.len() == 32
            && parent_span_id.len() == 16
            && trace_id.chars().all(|c| c.is_ascii_hexdigit())
            && parent_span_id.chars().all(|c| c.is_ascii_hexdigit())
            // All-zero ids are explicitly invalid per the spec.
            && trace_id.chars().any(|c| c != '0')
            && parent_span_id.chars().any(|c| c != '0');
        if !valid {
            return Self::root();
        }
        Self {
            trace_id: trace_id.to_ascii_lowercase(),
            span_id: random_hex(8),
            parent_span_id: Some(parent_span_id.to_ascii_lowercase()),
        }
    }
}

fn random_hex(bytes: usize) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes * 2);
    for _ in 0..bytes {
        let byte: u8 = rand::random();
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// OTLP span status. `Unset` is the default for a successful server span;
/// only an actual error sets `Error`, per the OTLP spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatus {
    Unset,
    Error,
}

/// One completed span queued for export.
#[derive(Debug, Clone)]
pub struct SpanRecord {
    pub context: SpanContext,
    pub name: String,
    pub start_nanos: u128,
    pub end_nanos: u128,
    pub status: SpanStatus,
    pub attributes: Vec<(String, String)>,
}

/// One log record queued for export.
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub severity: &'static str,
    pub body: String,
    pub attributes: Vec<(String, String)>,
    /// Correlates this record with a span. OTLP carries these as first-class
    /// fields, not attributes, so a backend can pivot log <-> trace.
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

/// Either signal, multiplexed over one bounded channel so a burst of one
/// cannot starve the other beyond the shared capacity.
#[derive(Debug, Clone)]
pub enum Item {
    Log(LogRecord),
    Span(SpanRecord),
}

/// Handle used by the rest of the process to enqueue telemetry.
#[derive(Debug, Clone)]
pub struct Exporter {
    tx: mpsc::Sender<Item>,
    health: Arc<ExportHealth>,
}

impl Exporter {
    /// Enqueue a log record. Never blocks; drops on a full queue.
    pub fn log(&self, record: LogRecord) {
        if self.tx.try_send(Item::Log(record)).is_err() {
            self.health.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Enqueue a completed span. Never blocks; drops on a full queue.
    pub fn span(&self, record: SpanRecord) {
        if self.tx.try_send(Item::Span(record)).is_err() {
            self.health.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn health(&self) -> &Arc<ExportHealth> {
        &self.health
    }
}

/// Spawn the export worker. Returns `None` when no endpoint is configured,
/// so the absent-endpoint path opens no socket at all.
///
/// One worker drains logs and spans from the shared queue and scrapes the
/// metrics registry on each tick, so all three signals share one batching
/// cadence and one client.
pub fn spawn(
    endpoint: Option<&str>,
    headers: Option<&str>,
    service_name: &str,
    instance_id: &str,
    service_version: &str,
    metrics: Option<Arc<crate::metrics::MetricsRegistry>>,
) -> Option<(Exporter, Arc<ExportHealth>)> {
    let endpoint = endpoint?.trim_end_matches('/').to_string();
    let header_pairs = parse_headers(headers);
    let resource = Resource {
        service_name: service_name.to_string(),
        instance_id: instance_id.to_string(),
        service_version: service_version.to_string(),
    };
    let health = Arc::new(ExportHealth::default());
    let (tx, mut rx) = mpsc::channel::<Item>(QUEUE_CAPACITY);

    let worker_health = health.clone();
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                // Sanitized: never the endpoint or headers.
                tracing::warn!(failure = "client_build", %error, "telemetry export disabled");
                return;
            }
        };
        // Cumulative temporality needs a fixed start for every point, or a
        // backend cannot tell a restart from a counter reset.
        let start_nanos = now_nanos();
        let urls = Urls {
            logs: format!("{endpoint}/v1/logs"),
            traces: format!("{endpoint}/v1/traces"),
            metrics: format!("{endpoint}/v1/metrics"),
        };
        let mut logs: Vec<LogRecord> = Vec::with_capacity(BATCH_MAX);
        let mut spans: Vec<SpanRecord> = Vec::with_capacity(BATCH_MAX);
        let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some(Item::Log(record)) => {
                            logs.push(record);
                            if logs.len() >= BATCH_MAX {
                                flush_logs(&client, &urls, &header_pairs, &resource, &mut logs, &worker_health).await;
                            }
                        }
                        Some(Item::Span(record)) => {
                            spans.push(record);
                            if spans.len() >= BATCH_MAX {
                                flush_spans(&client, &urls, &header_pairs, &resource, &mut spans, &worker_health).await;
                            }
                        }
                        None => {
                            // Channel closed: final drain, then exit.
                            flush_logs(&client, &urls, &header_pairs, &resource, &mut logs, &worker_health).await;
                            flush_spans(&client, &urls, &header_pairs, &resource, &mut spans, &worker_health).await;
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    flush_logs(&client, &urls, &header_pairs, &resource, &mut logs, &worker_health).await;
                    flush_spans(&client, &urls, &header_pairs, &resource, &mut spans, &worker_health).await;
                    if let Some(registry) = &metrics {
                        flush_metrics(&client, &urls, &header_pairs, &resource, registry, start_nanos, &worker_health).await;
                    }
                }
            }
        }
    });

    Some((
        Exporter {
            tx,
            health: health.clone(),
        },
        health,
    ))
}

#[derive(Debug, Clone)]
struct Urls {
    logs: String,
    traces: String,
    metrics: String,
}

#[derive(Debug, Clone)]
struct Resource {
    service_name: String,
    instance_id: String,
    service_version: String,
}

/// POST one payload, recording health. Never propagates an error.
async fn post(
    client: &reqwest::Client,
    url: &str,
    headers: &[(String, String)],
    payload: &Value,
    signal: &'static str,
    count: u64,
    health: &Arc<ExportHealth>,
) {
    let mut req = client.post(url).json(payload);
    for (name, value) in headers {
        req = req.header(name.as_str(), value.as_str());
    }
    match req.send().await {
        Ok(response) if response.status().is_success() => health.record_success(count),
        Ok(response) => {
            // Status class only — never the body, which can echo credentials.
            tracing::warn!(
                failure = "http_status",
                signal,
                status = response.status().as_u16(),
                "telemetry export failed"
            );
            health.record_failure();
        }
        Err(_) => {
            // No error text: reqwest errors embed the URL, which may carry
            // credentials in userinfo.
            tracing::warn!(failure = "transport", signal, "telemetry export failed");
            health.record_failure();
        }
    }
}

async fn flush_logs(
    client: &reqwest::Client,
    urls: &Urls,
    headers: &[(String, String)],
    resource: &Resource,
    buffer: &mut Vec<LogRecord>,
    health: &Arc<ExportHealth>,
) {
    if buffer.is_empty() {
        return;
    }
    let batch = std::mem::take(buffer);
    let count = batch.len() as u64;
    let payload = encode_logs(
        &batch,
        &resource.service_name,
        &resource.instance_id,
        &resource.service_version,
    );
    post(client, &urls.logs, headers, &payload, "logs", count, health).await;
}

async fn flush_spans(
    client: &reqwest::Client,
    urls: &Urls,
    headers: &[(String, String)],
    resource: &Resource,
    buffer: &mut Vec<SpanRecord>,
    health: &Arc<ExportHealth>,
) {
    if buffer.is_empty() {
        return;
    }
    let batch = std::mem::take(buffer);
    let count = batch.len() as u64;
    let payload = encode_spans(
        &batch,
        &resource.service_name,
        &resource.instance_id,
        &resource.service_version,
    );
    post(
        client,
        &urls.traces,
        headers,
        &payload,
        "traces",
        count,
        health,
    )
    .await;
}

async fn flush_metrics(
    client: &reqwest::Client,
    urls: &Urls,
    headers: &[(String, String)],
    resource: &Resource,
    registry: &Arc<crate::metrics::MetricsRegistry>,
    start_nanos: u128,
    health: &Arc<ExportHealth>,
) {
    let families = registry.collect();
    if families.is_empty() {
        return;
    }
    let count = families.len() as u64;
    let payload = encode_metrics(
        &families,
        &resource.service_name,
        &resource.instance_id,
        &resource.service_version,
        start_nanos,
    );
    post(
        client,
        &urls.metrics,
        headers,
        &payload,
        "metrics",
        count,
        health,
    )
    .await;
}

const SCOPE_NAME: &str = "preloop-observability";

fn attr(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

fn resource_attributes(service_name: &str, instance_id: &str, service_version: &str) -> Vec<Value> {
    vec![
        attr("service.name", service_name),
        attr("service.instance.id", instance_id),
        attr("service.version", service_version),
    ]
}

fn severity_number(severity: &str) -> u8 {
    match severity {
        "TRACE" => 1,
        "DEBUG" => 5,
        "INFO" => 9,
        "WARN" => 13,
        "ERROR" => 17,
        _ => 9,
    }
}

/// Encode a batch into the OTLP/HTTP JSON `ExportLogsServiceRequest` shape.
pub fn encode_logs(
    batch: &[LogRecord],
    service_name: &str,
    instance_id: &str,
    service_version: &str,
) -> Value {
    let ts = now_nanos().to_string();
    let records: Vec<Value> = batch
        .iter()
        .map(|record| {
            let attributes: Vec<Value> =
                record.attributes.iter().map(|(k, v)| attr(k, v)).collect();
            let mut obj = json!({
                "timeUnixNano": ts,
                "observedTimeUnixNano": ts,
                "severityNumber": severity_number(record.severity),
                "severityText": record.severity,
                "body": {"stringValue": record.body},
                "attributes": attributes,
            });
            // OTLP carries correlation as first-class fields, not attributes.
            if let (Some(trace_id), Some(span_id)) = (&record.trace_id, &record.span_id) {
                obj["traceId"] = json!(trace_id);
                obj["spanId"] = json!(span_id);
            }
            obj
        })
        .collect();
    json!({
        "resourceLogs": [{
            "resource": {"attributes": resource_attributes(service_name, instance_id, service_version)},
            "scopeLogs": [{
                "scope": {"name": "preloop-observability"},
                "logRecords": records,
            }]
        }]
    })
}

/// Parse `OTEL_EXPORTER_OTLP_HEADERS` (`k1=v1,k2=v2`).
fn parse_headers(raw: Option<&str>) -> Vec<(String, String)> {
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

/// Encode a batch into the OTLP/HTTP JSON `ExportTraceServiceRequest` shape.
pub fn encode_spans(
    batch: &[SpanRecord],
    service_name: &str,
    instance_id: &str,
    service_version: &str,
) -> Value {
    let spans: Vec<Value> = batch
        .iter()
        .map(|record| {
            let attributes: Vec<Value> =
                record.attributes.iter().map(|(k, v)| attr(k, v)).collect();
            let mut obj = json!({
                "traceId": record.context.trace_id,
                "spanId": record.context.span_id,
                "name": record.name,
                // 2 = SPAN_KIND_SERVER: every span we emit today is an
                // inbound request handled by the control plane.
                "kind": 2,
                "startTimeUnixNano": record.start_nanos.to_string(),
                "endTimeUnixNano": record.end_nanos.to_string(),
                "attributes": attributes,
                "status": {"code": match record.status {
                    SpanStatus::Unset => 0,
                    SpanStatus::Error => 2,
                }},
            });
            if let Some(parent) = &record.context.parent_span_id {
                obj["parentSpanId"] = json!(parent);
            }
            obj
        })
        .collect();
    json!({
        "resourceSpans": [{
            "resource": {"attributes": resource_attributes(service_name, instance_id, service_version)},
            "scopeSpans": [{
                "scope": {"name": SCOPE_NAME},
                "spans": spans,
            }]
        }]
    })
}

/// Encode collected families into the OTLP/HTTP JSON `ExportMetricsServiceRequest`.
///
/// All points are cumulative (`aggregationTemporality: 2`) and share the
/// process start as `startTimeUnixNano`, so a backend can distinguish a
/// restart from a counter reset.
pub fn encode_metrics(
    families: &[crate::metrics::MetricFamily],
    service_name: &str,
    instance_id: &str,
    service_version: &str,
    start_nanos: u128,
) -> Value {
    use crate::metrics::MetricPoint;
    const CUMULATIVE: u8 = 2;
    let now = now_nanos().to_string();
    let start = start_nanos.to_string();

    let metrics: Vec<Value> = families
        .iter()
        .map(|family| {
            let mut metric = json!({"name": family.name, "unit": family.unit});
            match family.points.first() {
                Some(MetricPoint::Sum { .. }) => {
                    let points: Vec<Value> = family
                        .points
                        .iter()
                        .filter_map(|point| match point {
                            MetricPoint::Sum { value, attributes } => Some(json!({
                                "asDouble": value,
                                "startTimeUnixNano": start,
                                "timeUnixNano": now,
                                "attributes": encode_attrs(attributes),
                            })),
                            _ => None,
                        })
                        .collect();
                    metric["sum"] = json!({
                        "dataPoints": points,
                        "aggregationTemporality": CUMULATIVE,
                        "isMonotonic": true,
                    });
                }
                Some(MetricPoint::Gauge { .. }) => {
                    let points: Vec<Value> = family
                        .points
                        .iter()
                        .filter_map(|point| match point {
                            MetricPoint::Gauge { value, attributes } => Some(json!({
                                "asDouble": value,
                                "timeUnixNano": now,
                                "attributes": encode_attrs(attributes),
                            })),
                            _ => None,
                        })
                        .collect();
                    metric["gauge"] = json!({"dataPoints": points});
                }
                Some(MetricPoint::Histogram { .. }) => {
                    let points: Vec<Value> = family
                        .points
                        .iter()
                        .filter_map(|point| match point {
                            MetricPoint::Histogram {
                                count,
                                sum,
                                bounds,
                                bucket_counts,
                                attributes,
                            } => Some(json!({
                                "count": count.to_string(),
                                "sum": sum,
                                "explicitBounds": bounds,
                                "bucketCounts": bucket_counts
                                    .iter()
                                    .map(|c| c.to_string())
                                    .collect::<Vec<_>>(),
                                "startTimeUnixNano": start,
                                "timeUnixNano": now,
                                "attributes": encode_attrs(attributes),
                            })),
                            _ => None,
                        })
                        .collect();
                    metric["histogram"] = json!({
                        "dataPoints": points,
                        "aggregationTemporality": CUMULATIVE,
                    });
                }
                None => {}
            }
            metric
        })
        .collect();

    json!({
        "resourceMetrics": [{
            "resource": {"attributes": resource_attributes(service_name, instance_id, service_version)},
            "scopeMetrics": [{
                "scope": {"name": SCOPE_NAME},
                "metrics": metrics,
            }]
        }]
    })
}

fn encode_attrs(attributes: &[(String, String)]) -> Vec<Value> {
    attributes.iter().map(|(k, v)| attr(k, v)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_endpoint_spawns_nothing() {
        assert!(spawn(None, None, "preloop", "abc", "9.9.9", None).is_none());
    }

    #[test]
    fn headers_parse_pairs() {
        let parsed = parse_headers(Some("Authorization=Basic abc,stream-name=default"));
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, "Authorization");
        assert_eq!(parsed[1].1, "default");
    }

    #[test]
    fn headers_ignore_malformed() {
        assert!(parse_headers(Some("novalue,=empty,k=")).is_empty());
    }

    #[test]
    fn encodes_otlp_log_shape() {
        let batch = vec![LogRecord {
            severity: "WARN",
            body: "pool provisioning failed".to_string(),
            attributes: vec![("event.name".to_string(), "pool.provision".to_string())],
            trace_id: None,
            span_id: None,
        }];
        let payload = encode_logs(&batch, "preloop", "inst-1", "9.9.9");
        let record = &payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
        assert_eq!(record["severityText"], "WARN");
        assert_eq!(record["severityNumber"], 13);
        assert_eq!(record["body"]["stringValue"], "pool provisioning failed");
        let resource = &payload["resourceLogs"][0]["resource"]["attributes"];
        assert_eq!(resource[0]["key"], "service.name");
        assert_eq!(resource[0]["value"]["stringValue"], "preloop");
        assert_eq!(resource[2]["key"], "service.version");
        assert_eq!(
            resource[2]["value"]["stringValue"], "9.9.9",
            "service.version must come from the host binary, not this crate"
        );
    }
}

#[cfg(test)]
mod signal_tests {
    use super::*;
    use crate::metrics::{MetricFamily, MetricPoint};

    #[test]
    fn traceparent_is_adopted_as_parent() {
        let ctx = SpanContext::from_traceparent(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ));
        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.parent_span_id.as_deref(), Some("00f067aa0ba902b7"));
        // A child must get its own span id, never reuse the parent's.
        assert_ne!(ctx.span_id, "00f067aa0ba902b7");
        assert_eq!(ctx.span_id.len(), 16);
    }

    #[test]
    fn malformed_traceparent_starts_a_new_root() {
        for bad in [
            "garbage",
            "00-tooshort-00f067aa0ba902b7-01",
            // All-zero ids are invalid per the spec.
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
        ] {
            let ctx = SpanContext::from_traceparent(Some(bad));
            assert!(
                ctx.parent_span_id.is_none(),
                "{bad} must not adopt a parent"
            );
            assert_eq!(ctx.trace_id.len(), 32);
        }
    }

    #[test]
    fn generated_ids_are_unique_and_well_formed() {
        let a = SpanContext::root();
        let b = SpanContext::root();
        assert_ne!(a.trace_id, b.trace_id);
        assert_eq!(a.trace_id.len(), 32);
        assert_eq!(a.span_id.len(), 8 * 2);
        assert!(a.trace_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn spans_encode_to_otlp_shape() {
        let ctx = SpanContext::from_traceparent(Some(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ));
        let batch = vec![SpanRecord {
            context: ctx,
            name: "GET /api/v1/runs/:run_id".to_string(),
            start_nanos: 1_000,
            end_nanos: 2_000,
            status: SpanStatus::Error,
            attributes: vec![("http.route".to_string(), "/api/v1/runs/:run_id".to_string())],
        }];
        let payload = encode_spans(&batch, "preloop", "inst", "0.2.0");
        let span = &payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert_eq!(span["traceId"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(span["parentSpanId"], "00f067aa0ba902b7");
        assert_eq!(span["kind"], 2, "server span");
        assert_eq!(span["status"]["code"], 2, "error");
        assert_eq!(span["startTimeUnixNano"], "1000");
    }

    #[test]
    fn logs_carry_trace_correlation_as_fields() {
        let batch = vec![LogRecord {
            severity: "WARN",
            body: "job.status.terminal".to_string(),
            attributes: vec![],
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            span_id: Some("00f067aa0ba902b7".to_string()),
        }];
        let payload = encode_logs(&batch, "preloop", "inst", "0.2.0");
        let record = &payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
        // OTLP requires these as fields, not attributes, for log<->trace pivot.
        assert_eq!(record["traceId"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(record["spanId"], "00f067aa0ba902b7");
    }

    #[test]
    fn counters_encode_as_cumulative_monotonic_sums() {
        let families = vec![MetricFamily {
            name: "preloop.job.completed".to_string(),
            unit: "{job}",
            points: vec![MetricPoint::Sum {
                value: 7.0,
                attributes: vec![("preloop.conclusion".to_string(), "failure".to_string())],
            }],
        }];
        let payload = encode_metrics(&families, "preloop", "inst", "0.2.0", 500);
        let metric = &payload["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0];
        assert_eq!(metric["name"], "preloop.job.completed");
        assert_eq!(metric["sum"]["isMonotonic"], true);
        assert_eq!(metric["sum"]["aggregationTemporality"], 2, "cumulative");
        let point = &metric["sum"]["dataPoints"][0];
        assert_eq!(point["asDouble"], 7.0);
        assert_eq!(
            point["startTimeUnixNano"], "500",
            "cumulative points need a fixed start or a restart reads as a reset"
        );
    }

    #[test]
    fn histograms_encode_with_the_implicit_inf_bucket() {
        let families = vec![MetricFamily {
            name: "http.server.request.duration".to_string(),
            unit: "s",
            points: vec![MetricPoint::Histogram {
                count: 5,
                sum: 0.25,
                bounds: vec![0.005, 0.01],
                bucket_counts: vec![1, 3, 5],
                attributes: vec![("http.route".to_string(), "/healthz".to_string())],
            }],
        }];
        let payload = encode_metrics(&families, "preloop", "inst", "0.2.0", 0);
        let point = &payload["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["histogram"]
            ["dataPoints"][0];
        let bounds = point["explicitBounds"].as_array().unwrap();
        let counts = point["bucketCounts"].as_array().unwrap();
        assert_eq!(
            counts.len(),
            bounds.len() + 1,
            "OTLP requires one more bucket count than bounds (+Inf)"
        );
        assert_eq!(point["count"], "5");
    }

    #[test]
    fn gauges_have_no_temporality_or_start_time() {
        let families = vec![MetricFamily {
            name: "http.server.active_requests".to_string(),
            unit: "{request}",
            points: vec![MetricPoint::Gauge {
                value: 3.0,
                attributes: vec![],
            }],
        }];
        let payload = encode_metrics(&families, "preloop", "inst", "0.2.0", 0);
        let metric = &payload["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0];
        assert!(metric["gauge"]["dataPoints"][0]["asDouble"] == 3.0);
        assert!(metric["gauge"]["aggregationTemporality"].is_null());
    }
}
