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

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// One log record queued for export.
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub severity: &'static str,
    pub body: String,
    pub attributes: Vec<(String, String)>,
}

/// Handle used by the rest of the process to enqueue telemetry.
#[derive(Debug, Clone)]
pub struct Exporter {
    tx: mpsc::Sender<LogRecord>,
    health: Arc<ExportHealth>,
}

impl Exporter {
    /// Enqueue a log record. Never blocks; drops on a full queue.
    pub fn log(&self, record: LogRecord) {
        if self.tx.try_send(record).is_err() {
            self.health.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn health(&self) -> &Arc<ExportHealth> {
        &self.health
    }
}

/// Spawn the export worker. Returns `None` when no endpoint is configured,
/// so the absent-endpoint path opens no socket at all.
pub fn spawn(
    endpoint: Option<&str>,
    headers: Option<&str>,
    service_name: &str,
    instance_id: &str,
) -> Option<(Exporter, Arc<ExportHealth>)> {
    let endpoint = endpoint?.trim_end_matches('/').to_string();
    let header_pairs = parse_headers(headers);
    let service_name = service_name.to_string();
    let instance_id = instance_id.to_string();
    let health = Arc::new(ExportHealth::default());
    let (tx, mut rx) = mpsc::channel::<LogRecord>(QUEUE_CAPACITY);

    let worker_health = health.clone();
    tokio::spawn(async move {
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                // Sanitized: never the endpoint or headers.
                tracing::warn!(
                    failure = "client_build",
                    %error,
                    "telemetry export disabled"
                );
                return;
            }
        };
        let logs_url = format!("{endpoint}/v1/logs");
        let mut buffer: Vec<LogRecord> = Vec::with_capacity(BATCH_MAX);
        let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
        loop {
            tokio::select! {
                maybe = rx.recv() => {
                    match maybe {
                        Some(record) => {
                            buffer.push(record);
                            if buffer.len() >= BATCH_MAX {
                                flush(&client, &logs_url, &header_pairs, &service_name, &instance_id, &mut buffer, &worker_health).await;
                            }
                        }
                        None => {
                            flush(&client, &logs_url, &header_pairs, &service_name, &instance_id, &mut buffer, &worker_health).await;
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    flush(&client, &logs_url, &header_pairs, &service_name, &instance_id, &mut buffer, &worker_health).await;
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

async fn flush(
    client: &reqwest::Client,
    url: &str,
    headers: &[(String, String)],
    service_name: &str,
    instance_id: &str,
    buffer: &mut Vec<LogRecord>,
    health: &Arc<ExportHealth>,
) {
    if buffer.is_empty() {
        return;
    }
    let batch = std::mem::take(buffer);
    let count = batch.len() as u64;
    let payload = encode_logs(&batch, service_name, instance_id);
    let mut req = client.post(url).json(&payload);
    for (name, value) in headers {
        req = req.header(name.as_str(), value.as_str());
    }
    match req.send().await {
        Ok(response) if response.status().is_success() => health.record_success(count),
        Ok(response) => {
            // Status class only — never the body, which can echo credentials.
            tracing::warn!(
                failure = "http_status",
                status = response.status().as_u16(),
                "telemetry export failed"
            );
            health.record_failure();
        }
        Err(_) => {
            // No error text: reqwest errors embed the URL, which may carry
            // credentials in userinfo.
            tracing::warn!(failure = "transport", "telemetry export failed");
            health.record_failure();
        }
    }
}

fn attr(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
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
pub fn encode_logs(batch: &[LogRecord], service_name: &str, instance_id: &str) -> Value {
    let ts = now_nanos().to_string();
    let records: Vec<Value> = batch
        .iter()
        .map(|record| {
            let attributes: Vec<Value> =
                record.attributes.iter().map(|(k, v)| attr(k, v)).collect();
            json!({
                "timeUnixNano": ts,
                "observedTimeUnixNano": ts,
                "severityNumber": severity_number(record.severity),
                "severityText": record.severity,
                "body": {"stringValue": record.body},
                "attributes": attributes,
            })
        })
        .collect();
    json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [
                    attr("service.name", service_name),
                    attr("service.instance.id", instance_id),
                    attr("service.version", env!("CARGO_PKG_VERSION")),
                ]
            },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_endpoint_spawns_nothing() {
        assert!(spawn(None, None, "preloop", "abc").is_none());
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
        }];
        let payload = encode_logs(&batch, "preloop", "inst-1");
        let record = &payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
        assert_eq!(record["severityText"], "WARN");
        assert_eq!(record["severityNumber"], 13);
        assert_eq!(record["body"]["stringValue"], "pool provisioning failed");
        let resource = &payload["resourceLogs"][0]["resource"]["attributes"];
        assert_eq!(resource[0]["key"], "service.name");
        assert_eq!(resource[0]["value"]["stringValue"], "preloop");
    }
}
