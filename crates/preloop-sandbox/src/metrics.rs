//! `preloop.sandbox.*` instruments.
//!
//! Built from the shared `Meter` exposed by `preloop_observability::Observability`
//! (not from this crate's own provider): the sandbox service never depends on
//! the CI control plane, but it shares the process's single meter so the
//! OTLP/Prometheus readers scrape one instrument set, never two.
//!
//! Every instrument is recorded from the service's own state transitions —
//! never by callers (the CI pool, the CLI) pushing numbers in — so a
//! standalone `preloop sandbox serve` with no caller at all still reports
//! accurate gauges and counters.

use std::collections::HashSet;
use std::sync::Arc;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

/// Sandbox service instruments.
#[derive(Clone)]
pub struct SandboxMetrics {
    sandboxes: Gauge<u64>,
    acquire_duration: Histogram<f64>,
    acquire_failures: Counter<u64>,
    pool_warm_depth: Gauge<u64>,
    pool_refill_failures: Counter<u64>,
    lease_transitions: Counter<u64>,
    reconcile_lost: Counter<u64>,
    reconcile_orphans: Counter<u64>,
    exec_duration: Histogram<f64>,
    exec_exit: Counter<u64>,
    create_failures: Counter<u64>,
    heartbeat_truncated: Counter<u64>,
    /// `(state, provider, template)` combinations the last
    /// [`Self::record_sandbox_counts`] tick recorded. Shared across clones so
    /// a config clone cannot forget which series are live. A plain gauge keeps
    /// its last value until something records over it, so the next tick must
    /// write zero for every combination that disappeared instead of omitting
    /// it.
    live_counts: Arc<parking_lot::Mutex<HashSet<(String, String, String)>>>,
}

impl std::fmt::Debug for SandboxMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SandboxMetrics").finish_non_exhaustive()
    }
}

impl Default for SandboxMetrics {
    fn default() -> Self {
        let provider = opentelemetry::metrics::noop::NoopMeterProvider::new();
        let meter = opentelemetry::metrics::MeterProvider::meter(&provider, "preloop-sandbox-noop");
        Self::from_meter(&meter)
    }
}

impl SandboxMetrics {
    /// Build instruments from the process's shared meter
    /// (`Observability::meter()`).
    pub fn from_meter(meter: &Meter) -> Self {
        Self {
            sandboxes: meter
                .u64_gauge("preloop.sandbox.sandboxes")
                .with_description("Live sandboxes by state, provider and template")
                .with_unit("{sandbox}")
                .build(),
            acquire_duration: meter
                .f64_histogram("preloop.sandbox.acquire.duration")
                .with_description("Acquire latency from request to a ready sandbox or failure")
                .with_unit("s")
                .build(),
            acquire_failures: meter
                .u64_counter("preloop.sandbox.acquire.failures")
                .with_description(
                    "Acquire failures by reason (connect error code; failed_precondition is an \
                     unmet placement requirement)",
                )
                .with_unit("{failure}")
                .build(),
            pool_warm_depth: meter
                .u64_gauge("preloop.sandbox.pool.warm_depth")
                .with_description("Idle, unleased sandboxes currently held for a pool")
                .with_unit("{sandbox}")
                .build(),
            pool_refill_failures: meter
                .u64_counter("preloop.sandbox.pool.refill_failures")
                .with_description("Warm-pool refill attempts that failed to place a sandbox")
                .with_unit("{failure}")
                .build(),
            lease_transitions: meter
                .u64_counter("preloop.sandbox.lease.transitions")
                .with_description(
                    "Lease lifecycle transitions by outcome (renewed, expired, preserved)",
                )
                .with_unit("{lease}")
                .build(),
            reconcile_lost: meter
                .u64_counter("preloop.sandbox.reconcile.lost")
                .with_description(
                    "Sandboxes a provider's inventory no longer reports (failed as lost)",
                )
                .with_unit("{sandbox}")
                .build(),
            reconcile_orphans: meter
                .u64_counter("preloop.sandbox.reconcile.orphans")
                .with_description(
                    "Provider-observed machines no record owns, sent back for destruction",
                )
                .with_unit("{sandbox}")
                .build(),
            exec_duration: meter
                .f64_histogram("preloop.sandbox.exec.duration")
                .with_description("Exec duration from request to the terminal exit event")
                .with_unit("s")
                .build(),
            exec_exit: meter
                .u64_counter("preloop.sandbox.exec.exit")
                .with_description("Exec completions by outcome class (success, failure, error)")
                .with_unit("{exec}")
                .build(),
            create_failures: meter
                .u64_counter("preloop.sandbox.create.failures")
                .with_description("Sandbox creations that failed, by reason (timeout, reported)")
                .with_unit("{failure}")
                .build(),
            heartbeat_truncated: meter
                .u64_counter("preloop.sandbox.heartbeat.truncated")
                .with_description(
                    "Per-sandbox runtime entries dropped from a heartbeat because one report \
                     exceeded the per-heartbeat cap",
                )
                .with_unit("{sandbox}")
                .build(),
            live_counts: Arc::new(parking_lot::Mutex::new(HashSet::new())),
        }
    }

    /// Record the live sandboxes for one maintenance tick, keyed by
    /// `(state, provider, template)`.
    ///
    /// Every combination recorded by the previous tick but absent from this
    /// one is recorded as zero first: the OTel last-value gauge would
    /// otherwise keep the drained combination's old count forever, so a
    /// scrape after the last sandbox of a pool is destroyed would show it
    /// still live.
    pub fn record_sandbox_counts<I>(&self, counts: I)
    where
        I: IntoIterator<Item = (String, String, String, u64)>,
    {
        let mut current = HashSet::new();
        for (state, provider, template, count) in counts {
            self.record_sandbox_count(&state, &provider, &template, count);
            current.insert((state, provider, template));
        }
        let mut seen = self.live_counts.lock();
        for (state, provider, template) in seen.difference(&current) {
            self.record_sandbox_count(state, provider, template, 0);
        }
        *seen = current;
    }

    /// Record one `(state, provider, template)` combination's live count.
    fn record_sandbox_count(&self, state: &str, provider: &str, template: &str, count: u64) {
        self.sandboxes.record(
            count,
            &[
                KeyValue::new("state", state.to_owned()),
                KeyValue::new("provider", provider.to_owned()),
                KeyValue::new("template", template.to_owned()),
            ],
        );
    }

    /// Record per-sandbox runtime entries a heartbeat had to drop because
    /// the host runs more measured sandboxes than one report carries.
    pub fn record_heartbeat_truncation(&self, dropped: u64) {
        if dropped > 0 {
            self.heartbeat_truncated.add(dropped, &[]);
        }
    }

    /// Record one acquire's outcome. `reason` is `None` on success.
    pub fn record_acquire(&self, duration: std::time::Duration, reason: Option<&str>) {
        let outcome = if reason.is_some() { "error" } else { "ready" };
        self.acquire_duration
            .record(duration.as_secs_f64(), &[KeyValue::new("outcome", outcome)]);
        if let Some(reason) = reason {
            self.acquire_failures
                .add(1, &[KeyValue::new("reason", reason.to_owned())]);
        }
    }

    /// Record a pool's current warm depth.
    pub fn record_pool_warm_depth(&self, pool: &str, depth: u64) {
        self.pool_warm_depth
            .record(depth, &[KeyValue::new("pool", pool.to_owned())]);
    }

    /// Record a warm-pool refill that could not place a sandbox.
    pub fn record_pool_refill_failure(&self, pool: &str) {
        self.pool_refill_failures
            .add(1, &[KeyValue::new("pool", pool.to_owned())]);
    }

    /// Record a lease transition: `renewed`, `expired`, or `preserved`.
    pub fn record_lease_transition(&self, outcome: &str) {
        self.lease_transitions
            .add(1, &[KeyValue::new("outcome", outcome.to_owned())]);
    }

    /// Record sandboxes a `Reconcile` found missing from the provider's
    /// inventory (failed as lost).
    pub fn record_reconcile_lost(&self, count: u64) {
        if count > 0 {
            self.reconcile_lost.add(count, &[]);
        }
    }

    /// Record machines a `Reconcile` found no record owns (sent back for
    /// destruction as orphans or stale incarnations).
    pub fn record_reconcile_orphans(&self, count: u64) {
        if count > 0 {
            self.reconcile_orphans.add(count, &[]);
        }
    }

    /// Record one exec's duration and exit class once its terminal event
    /// (an exit code, or a stream error) is observed.
    pub fn record_exec(&self, duration: std::time::Duration, class: &str) {
        self.exec_duration.record(duration.as_secs_f64(), &[]);
        self.exec_exit
            .add(1, &[KeyValue::new("class", class.to_owned())]);
    }

    /// Record a sandbox creation failure. `reason` is `timeout` (the
    /// provider never reported) or `reported` (the provider reported one).
    pub fn record_create_failure(&self, reason: &str) {
        self.create_failures
            .add(1, &[KeyValue::new("reason", reason.to_owned())]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a meter the test can scrape synchronously. `PrometheusHandle`
    /// deduplicates series, so each render sees the latest value for every
    /// label set.
    fn test_meter() -> (
        SandboxMetrics,
        preloop_observability::prometheus::PrometheusHandle,
        opentelemetry_sdk::metrics::SdkMeterProvider,
    ) {
        use opentelemetry::metrics::MeterProvider as _;

        let handle = preloop_observability::prometheus::PrometheusHandle::new(
            opentelemetry_sdk::metrics::ManualReader::default(),
        );
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(handle.clone())
            .build();
        let metrics = SandboxMetrics::from_meter(&provider.meter("test"));
        (metrics, handle, provider)
    }

    #[test]
    fn default_registry_is_noop() {
        let metrics = SandboxMetrics::default();
        metrics.record_sandbox_counts(vec![(
            "ready".to_owned(),
            "prov-1".to_owned(),
            "alpine".to_owned(),
            2,
        )]);
        metrics.record_acquire(std::time::Duration::from_millis(5), None);
        metrics.record_acquire(
            std::time::Duration::from_millis(5),
            Some("failed_precondition"),
        );
        metrics.record_pool_warm_depth("alpine", 1);
        metrics.record_pool_refill_failure("alpine");
        metrics.record_lease_transition("renewed");
        metrics.record_reconcile_lost(1);
        metrics.record_reconcile_orphans(1);
        metrics.record_exec(std::time::Duration::from_millis(5), "success");
        metrics.record_create_failure("timeout");
        metrics.record_heartbeat_truncation(3);
    }

    #[test]
    fn drained_combinations_record_zero() {
        let (metrics, handle, provider) = test_meter();
        let series = |text: &str| {
            text.lines()
                .find(|line| line.starts_with("preloop_sandbox_sandboxes{"))
                .map(str::to_owned)
                .unwrap_or_default()
        };

        metrics.record_sandbox_counts(vec![
            (
                "ready".to_owned(),
                "prov-1".to_owned(),
                "alpine".to_owned(),
                3,
            ),
            (
                "leased".to_owned(),
                "prov-1".to_owned(),
                "alpine".to_owned(),
                1,
            ),
        ]);
        let text = handle.render();
        assert!(
            text.contains(
                "preloop_sandbox_sandboxes{provider=\"prov-1\",state=\"ready\",template=\"alpine\"} 3"
            ),
            "missing the live ready series in: {text}"
        );

        // One pool drains, the other stays: the gone combination must be
        // written back as zero, not left at its last value.
        metrics.record_sandbox_counts(vec![(
            "leased".to_owned(),
            "prov-1".to_owned(),
            "alpine".to_owned(),
            1,
        )]);
        let text = handle.render();
        assert!(
            text.contains(
                "preloop_sandbox_sandboxes{provider=\"prov-1\",state=\"ready\",template=\"alpine\"} 0"
            ),
            "drained combination kept its last value in: {text}"
        );
        assert!(
            text.contains(
                "preloop_sandbox_sandboxes{provider=\"prov-1\",state=\"leased\",template=\"alpine\"} 1"
            ),
            "live combination lost in: {text}"
        );

        // An empty tick zeroes everything currently known.
        metrics.record_sandbox_counts(Vec::new());
        let text = handle.render();
        assert!(
            text.contains(
                "preloop_sandbox_sandboxes{provider=\"prov-1\",state=\"leased\",template=\"alpine\"} 0"
            ),
            "empty tick did not zero the last combination in: {text}"
        );
        assert!(
            series(&text).contains(" 0"),
            "expected zeroed series: {text}"
        );
        drop(provider);
    }
}
