//! Counters, latency distributions and a per-second timeline.
//!
//! Latencies are measured from the *intended* start time of an open-loop
//! operation, so a stalled server shows up as queueing delay instead of
//! vanishing (no coordinated omission).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Everything a round records.
pub struct Metrics {
    started: Instant,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// name → count
    counters: BTreeMap<String, u64>,
    /// name → latency samples in microseconds
    latencies: BTreeMap<String, Vec<u64>>,
    /// second → name → count
    per_second: BTreeMap<u64, BTreeMap<String, u64>>,
    /// second → db sample
    db_samples: Vec<serde_json::Value>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    pub fn incr(&self, name: &str) {
        self.add(name, 1);
    }

    pub fn add(&self, name: &str, value: u64) {
        let second = self.started.elapsed().as_secs();
        let mut inner = self.inner.lock().unwrap();
        *inner.counters.entry(name.to_owned()).or_default() += value;
        *inner
            .per_second
            .entry(second)
            .or_default()
            .entry(name.to_owned())
            .or_default() += value;
    }

    pub fn latency(&self, name: &str, since: Instant) {
        let micros = since.elapsed().as_micros() as u64;
        self.inner
            .lock()
            .unwrap()
            .latencies
            .entry(name.to_owned())
            .or_default()
            .push(micros);
    }

    pub fn db_sample(&self, sample: serde_json::Value) {
        self.inner.lock().unwrap().db_samples.push(sample);
    }

    pub fn counter(&self, name: &str) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .counters
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    /// The round summary: totals, rates, percentiles, peaks, db samples.
    pub fn summary(&self, label: &str, config: serde_json::Value) -> serde_json::Value {
        let inner = self.inner.lock().unwrap();
        let secs = self.started.elapsed().as_secs_f64().max(1.0);
        let rates: serde_json::Map<_, _> = inner
            .counters
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::json!(*v as f64 / secs)))
            .collect();
        let peaks: serde_json::Map<_, _> = inner
            .counters
            .keys()
            .map(|name| {
                let peak = inner
                    .per_second
                    .values()
                    .filter_map(|m| m.get(name))
                    .max()
                    .copied()
                    .unwrap_or(0);
                (name.clone(), serde_json::json!(peak))
            })
            .collect();
        let latencies: serde_json::Map<_, _> = inner
            .latencies
            .iter()
            .map(|(k, v)| (k.clone(), percentiles(v)))
            .collect();
        serde_json::json!({
            "label": label,
            "config": config,
            "duration_secs": secs,
            "totals": inner.counters,
            "rate_per_sec": rates,
            "peak_per_sec": peaks,
            "latency_ms": latencies,
            "db_samples": inner.db_samples,
        })
    }

    /// Per-second timeline as CSV (one column per counter).
    pub fn timeline_csv(&self) -> String {
        let inner = self.inner.lock().unwrap();
        let names: Vec<&String> = inner.counters.keys().collect();
        let mut out = String::from("second");
        for name in &names {
            out.push(',');
            out.push_str(name);
        }
        out.push('\n');
        let last = inner.per_second.keys().max().copied().unwrap_or(0);
        for second in 0..=last {
            out.push_str(&second.to_string());
            let row = inner.per_second.get(&second);
            for name in &names {
                out.push(',');
                out.push_str(
                    &row.and_then(|r| r.get(*name))
                        .copied()
                        .unwrap_or(0)
                        .to_string(),
                );
            }
            out.push('\n');
        }
        out
    }
}

fn percentiles(samples: &[u64]) -> serde_json::Value {
    if samples.is_empty() {
        return serde_json::json!({"count": 0});
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let at = |p: f64| {
        let index = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[index] as f64 / 1000.0
    };
    serde_json::json!({
        "count": sorted.len(),
        "p50": at(50.0),
        "p90": at(90.0),
        "p99": at(99.0),
        "p999": at(99.9),
        "max": *sorted.last().unwrap() as f64 / 1000.0,
    })
}
