//! Cumulative timing of control transactions by phase (lock wait, load,
//! decide, write-back, commit), exported on `/api/v1/debug/txn-stats` for
//! load tests and operators.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

struct Phase {
    count: AtomicU64,
    lock_us: AtomicU64,
    load_us: AtomicU64,
    decide_us: AtomicU64,
    write_us: AtomicU64,
    commit_us: AtomicU64,
    max_total_us: AtomicU64,
}

impl Phase {
    const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            lock_us: AtomicU64::new(0),
            load_us: AtomicU64::new(0),
            decide_us: AtomicU64::new(0),
            write_us: AtomicU64::new(0),
            commit_us: AtomicU64::new(0),
            max_total_us: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> serde_json::Value {
        let count = self.count.load(Ordering::Relaxed).max(1) as f64;
        let mean = |v: &AtomicU64| v.load(Ordering::Relaxed) as f64 / count / 1000.0;
        serde_json::json!({
            "count": self.count.load(Ordering::Relaxed),
            "mean_lock_ms": mean(&self.lock_us),
            "mean_load_ms": mean(&self.load_us),
            "mean_decide_ms": mean(&self.decide_us),
            "mean_write_ms": mean(&self.write_us),
            "mean_commit_ms": mean(&self.commit_us),
            "max_total_ms": self.max_total_us.load(Ordering::Relaxed) as f64 / 1000.0,
        })
    }
}

static RUN_SCOPED: Phase = Phase::new();
static GLOBAL: Phase = Phase::new();

pub(crate) fn record(
    run_scoped: bool,
    lock: Duration,
    load: Duration,
    decide: Duration,
    write: Duration,
    commit: Duration,
) {
    let phase = if run_scoped { &RUN_SCOPED } else { &GLOBAL };
    let us = |d: Duration| d.as_micros() as u64;
    phase.count.fetch_add(1, Ordering::Relaxed);
    phase.lock_us.fetch_add(us(lock), Ordering::Relaxed);
    phase.load_us.fetch_add(us(load), Ordering::Relaxed);
    phase.decide_us.fetch_add(us(decide), Ordering::Relaxed);
    phase.write_us.fetch_add(us(write), Ordering::Relaxed);
    phase.commit_us.fetch_add(us(commit), Ordering::Relaxed);
    let total = us(lock + load + decide + write + commit);
    phase.max_total_us.fetch_max(total, Ordering::Relaxed);
}

/// Current cumulative stats.
pub(crate) fn snapshot() -> serde_json::Value {
    serde_json::json!({
        "run_scoped": RUN_SCOPED.snapshot(),
        "global": GLOBAL.snapshot(),
    })
}
