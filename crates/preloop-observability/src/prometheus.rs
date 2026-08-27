#![allow(missing_docs)]
//! Prometheus text exposition renderer for OTel SDK `ResourceMetrics`.
//!
//! Replaces the deprecated `opentelemetry-prometheus` crate. A `ManualReader`
//! is wrapped in a `PrometheusHandle` that implements `MetricReader` (so it
//! can be registered with the `SdkMeterProvider`) and also exposes `render()`
//! for the `/metrics` endpoint.
//!
//! Name mapping follows the [OTel → Prometheus spec][spec]:
//!
//! - Dots and hyphens become underscores.
//! - Time unit `s` → suffix `_seconds`, `ms` → `_milliseconds`.
//! - Monotonic `Sum` → counter → `_total` suffix.
//! - Non-monotonic `Sum` / `Gauge` → gauge.
//! - `Histogram` → `_bucket` / `_sum` / `_count`.
//!
//! [spec]: https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_opentelemetry/

use std::fmt::Write;

use std::collections::HashSet;
use std::sync::{Arc, Weak};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, Gauge, Histogram, MetricData, ResourceMetrics, Sum,
};
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::{InstrumentKind, ManualReader, Pipeline, Temporality};

/// Shared handle to the manual reader.  Implements `MetricReader` so it can
/// be given to `SdkMeterProvider::builder().with_reader(handle.clone())` and
/// also kept for later `render()` calls.
#[derive(Clone, Debug)]
pub struct PrometheusHandle {
    reader: Arc<ManualReader>,
}

impl PrometheusHandle {
    pub fn new(reader: ManualReader) -> Self {
        Self {
            reader: Arc::new(reader),
        }
    }

    /// Collect current metrics and render as Prometheus 0.0.4 text.
    pub fn render(&self) -> String {
        let mut rm = ResourceMetrics::default();
        if self.reader.collect(&mut rm).is_err() {
            return String::new();
        }
        render_resource_metrics(&rm)
    }
}

// ---------------------------------------------------------------------------
// MetricReader delegation is the provider owns one clone, we keep another
// ---------------------------------------------------------------------------

impl MetricReader for PrometheusHandle {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.reader.register_pipeline(pipeline);
    }

    fn collect(&self, rm: &mut ResourceMetrics) -> OTelSdkResult {
        self.reader.collect(rm)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.reader.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.reader.shutdown_with_timeout(timeout)
    }

    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.reader.temporality(kind)
    }
}

// ---------------------------------------------------------------------------
// Prometheus text renderer
// ---------------------------------------------------------------------------

/// Render a `ResourceMetrics` snapshot as Prometheus exposition text.
fn render_resource_metrics(rm: &ResourceMetrics) -> String {
    let mut out = String::with_capacity(4096);
    // Prometheus rejects a scrape with duplicate #HELP/#TYPE lines. Two
    // scopes may publish the same instrument name, so emit the metadata
    // lines once per emitted name while still rendering every series.
    let mut emitted: HashSet<String> = HashSet::new();
    for scope in rm.scope_metrics() {
        for metric in scope.metrics() {
            let raw_name = metric.name();
            let unit = metric.unit();
            let description = metric.description();
            render_metric(
                &mut out,
                &mut emitted,
                raw_name,
                unit,
                description,
                metric.data(),
            );
        }
    }
    out
}

fn render_metric(
    out: &mut String,
    emitted: &mut HashSet<String>,
    raw_name: &str,
    unit: &str,
    description: &str,
    data: &AggregatedMetrics,
) {
    match data {
        AggregatedMetrics::F64(md) => {
            render_metric_data(out, emitted, raw_name, unit, description, md)
        }
        AggregatedMetrics::U64(md) => {
            render_metric_data(out, emitted, raw_name, unit, description, md)
        }
        AggregatedMetrics::I64(md) => {
            render_metric_data(out, emitted, raw_name, unit, description, md)
        }
    }
}

fn render_metric_data<T: NumericValue>(
    out: &mut String,
    emitted: &mut HashSet<String>,
    raw_name: &str,
    unit: &str,
    description: &str,
    data: &MetricData<T>,
) {
    match data {
        MetricData::Sum(sum) => render_sum(out, emitted, raw_name, unit, description, sum),
        MetricData::Gauge(gauge) => render_gauge(out, emitted, raw_name, unit, description, gauge),
        MetricData::Histogram(hist) => {
            render_histogram(out, emitted, raw_name, unit, description, hist)
        }
        MetricData::ExponentialHistogram(_) => {
            // Exponential histograms have no standard Prometheus text mapping.
        }
    }
}

fn render_sum<T: NumericValue>(
    out: &mut String,
    emitted: &mut HashSet<String>,
    raw_name: &str,
    unit: &str,
    description: &str,
    sum: &Sum<T>,
) {
    let (prom_type, suffix) = if sum.is_monotonic() {
        ("counter", "_total")
    } else {
        ("gauge", "")
    };
    let base = prom_name(raw_name, unit);
    let full = format!("{base}{suffix}");
    if emitted.insert(full.clone()) {
        if !description.is_empty() {
            let _ = writeln!(out, "# HELP {full} {description}");
        }
        let _ = writeln!(out, "# TYPE {full} {prom_type}");
    }
    for dp in sum.data_points() {
        write_sample(out, &full, dp.attributes(), dp.value());
    }
}

fn render_gauge<T: NumericValue>(
    out: &mut String,
    emitted: &mut HashSet<String>,
    raw_name: &str,
    unit: &str,
    description: &str,
    gauge: &Gauge<T>,
) {
    let name = prom_name(raw_name, unit);
    if emitted.insert(name.clone()) {
        if !description.is_empty() {
            let _ = writeln!(out, "# HELP {name} {description}");
        }
        let _ = writeln!(out, "# TYPE {name} gauge");
    }
    for dp in gauge.data_points() {
        write_sample(out, &name, dp.attributes(), dp.value());
    }
}

fn render_histogram<T: NumericValue>(
    out: &mut String,
    emitted: &mut HashSet<String>,
    raw_name: &str,
    unit: &str,
    description: &str,
    hist: &Histogram<T>,
) {
    let base = prom_name(raw_name, unit);
    if emitted.insert(base.clone()) {
        if !description.is_empty() {
            let _ = writeln!(out, "# HELP {base} {description}");
        }
        let _ = writeln!(out, "# TYPE {base} histogram");
    }
    for dp in hist.data_points() {
        let attrs: Vec<KeyValue> = dp.attributes().cloned().collect();
        // Cumulative bucket counts for Prometheus' cumulative histogram.
        let mut cumulative: u64 = 0;
        let bounds: Vec<f64> = dp.bounds().collect();
        let counts: Vec<u64> = dp.bucket_counts().collect();
        for (i, count) in counts.iter().enumerate() {
            cumulative += count;
            let le = if i < bounds.len() {
                format!("{}", bounds[i])
            } else {
                "+Inf".to_string()
            };
            let mut bucket_attrs = attrs.clone();
            bucket_attrs.push(KeyValue::new("le", le));
            write_sample(
                out,
                &format!("{base}_bucket"),
                bucket_attrs.iter(),
                cumulative,
            );
        }
        write_sample(out, &format!("{base}_sum"), attrs.iter(), dp.sum());
        write_sample(out, &format!("{base}_count"), attrs.iter(), dp.count());
    }
}

/// OTel instrument name → Prometheus metric name.
///
/// 1. Replace `.` and `-` with `_`.
/// 2. Append unit suffix: `s` → `_seconds`, `ms` → `_milliseconds`.
///    Custom units in braces `{request}` are omitted per spec.
fn prom_name(raw: &str, unit: &str) -> String {
    let mut name: String = raw
        .chars()
        .map(|c| if c == '.' || c == '-' { '_' } else { c })
        .collect();
    match unit {
        "s" => name.push_str("_seconds"),
        "ms" => name.push_str("_milliseconds"),
        "By" => name.push_str("_bytes"),
        u if u.starts_with('{') => { /* custom unit — no suffix */ }
        "" => {}
        _ => {
            // Unknown unit: append as-is (sanitized).
            name.push('_');
            name.extend(
                unit.chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' }),
            );
        }
    }
    name
}

/// Write one sample line: `name{labels} value\n`.
fn write_sample<'a, T: NumericValue>(
    out: &mut String,
    name: &str,
    attrs: impl Iterator<Item = &'a KeyValue>,
    value: T,
) {
    out.push_str(name);
    let mut first = true;
    for kv in attrs {
        if first {
            out.push('{');
            first = false;
        } else {
            out.push(',');
        }
        let _ = write!(
            out,
            "{}=\"{}\"",
            kv.key,
            escape_label_value(kv.value.as_str().as_ref())
        );
    }
    if !first {
        out.push('}');
    }
    out.push(' ');
    value.write_to(out);
    out.push('\n');
}

/// Escape a Prometheus label value: `\`, `"`, `\n`.
fn escape_label_value(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(c),
        }
    }
    escaped
}

/// Trait to abstract over f64/u64/i64 value rendering.
trait NumericValue: Copy + std::fmt::Debug {
    fn write_to(self, out: &mut String);
}

impl NumericValue for f64 {
    fn write_to(self, out: &mut String) {
        if self.is_nan() {
            out.push_str("NaN");
        } else if self.is_infinite() {
            if self.is_sign_positive() {
                out.push_str("+Inf");
            } else {
                out.push_str("-Inf");
            }
        } else if self == 0.0 {
            out.push('0');
        } else {
            let _ = write!(out, "{self}");
        }
    }
}

impl NumericValue for u64 {
    fn write_to(self, out: &mut String) {
        let _ = write!(out, "{self}");
    }
}

impl NumericValue for i64 {
    fn write_to(self, out: &mut String) {
        let _ = write!(out, "{self}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prom_name_mapping() {
        assert_eq!(
            prom_name("http.server.request.duration", "s"),
            "http_server_request_duration_seconds"
        );
        assert_eq!(
            prom_name("preloop.job.completed", "{job}"),
            "preloop_job_completed"
        );
        assert_eq!(
            prom_name("preloop.store.consecutive_failures", "{failure}"),
            "preloop_store_consecutive_failures"
        );
        assert_eq!(
            prom_name("preloop.job.queue.wait", "s"),
            "preloop_job_queue_wait_seconds"
        );
        assert_eq!(
            prom_name("http.server.active_requests", "{request}"),
            "http_server_active_requests"
        );
    }

    #[test]
    fn escape_label_value_works() {
        assert_eq!(escape_label_value("simple"), "simple");
        assert_eq!(escape_label_value("has\"quote"), "has\\\"quote");
        assert_eq!(escape_label_value("has\nnewline"), "has\\nnewline");
        assert_eq!(escape_label_value("back\\slash"), "back\\\\slash");
    }

    #[test]
    fn empty_resource_metrics_renders_empty() {
        let rm = ResourceMetrics::default();
        assert_eq!(render_resource_metrics(&rm), "");
    }

    #[test]
    fn manual_reader_round_trip() {
        use opentelemetry::metrics::MeterProvider;
        use opentelemetry_sdk::metrics::SdkMeterProvider;

        let handle = PrometheusHandle::new(ManualReader::default());
        let provider = SdkMeterProvider::builder()
            .with_reader(handle.clone())
            .build();
        let meter = provider.meter("test");
        let counter = meter
            .u64_counter("test.requests")
            .with_unit("{request}")
            .build();
        counter.add(42, &[KeyValue::new("method", "GET")]);

        let text = handle.render();
        assert!(
            text.contains("test_requests_total"),
            "missing counter name in: {text}"
        );
        assert!(text.contains("method=\"GET\""), "missing label in: {text}");
        assert!(text.contains(" 42"), "missing value in: {text}");
    }

    #[test]
    fn duplicate_instrument_across_scopes_emits_one_type_line() {
        use opentelemetry::metrics::MeterProvider;
        use opentelemetry_sdk::metrics::SdkMeterProvider;

        let handle = PrometheusHandle::new(ManualReader::default());
        let provider = SdkMeterProvider::builder()
            .with_reader(handle.clone())
            .build();
        // Two scopes publishing the same instrument name must not emit
        // duplicate #HELP/#TYPE lines (Prometheus rejects the whole scrape);
        // every series must still be rendered.
        provider
            .meter("scope-a")
            .u64_counter("test.requests")
            .with_unit("{request}")
            .build()
            .add(1, &[KeyValue::new("scope", "a")]);
        provider
            .meter("scope-b")
            .u64_counter("test.requests")
            .with_unit("{request}")
            .build()
            .add(2, &[KeyValue::new("scope", "b")]);

        let text = handle.render();
        let type_lines = text
            .lines()
            .filter(|l| l.starts_with("# TYPE test_requests_total"))
            .count();
        let sample_lines = text
            .lines()
            .filter(|l| l.starts_with("test_requests_total{"))
            .count();
        assert_eq!(type_lines, 1, "duplicate #TYPE line in: {text}");
        assert_eq!(sample_lines, 2, "series dropped in: {text}");
    }
}
