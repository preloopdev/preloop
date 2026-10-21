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
        }
    }

    /// Record one `(state, provider, template)` combination's live count.
    /// Called every maintenance tick with every combination currently
    /// present; a combination that drops to zero live sandboxes simply
    /// stops being recorded; the OTel SDK reader keeps its own meter has
    /// no way to see stale low-cardinality combinations — this is the same
    /// convention as other gauges in the workspace, which skip rather than
    /// fabricate stale zeros.
    pub fn record_sandbox_count(&self, state: &str, provider: &str, template: &str, count: u64) {
        self.sandboxes.record(
            count,
            &[
                KeyValue::new("state", state.to_owned()),
                KeyValue::new("provider", provider.to_owned()),
                KeyValue::new("template", template.to_owned()),
            ],
        );
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

    #[test]
    fn default_registry_is_noop() {
        let metrics = SandboxMetrics::default();
        metrics.record_sandbox_count("ready", "prov-1", "alpine", 2);
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
    }
}
