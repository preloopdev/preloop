//! Cumulative timing of the statements that take a run's `runs` row lock,
//! labelled by call path and exported on `/api/v1/debug/txn-stats` for load
//! tests and operators. A statement's time includes waiting for the row
//! lock, so the totals show where per-run serialization is spent.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// A statement that locks or writes a `runs` row.
#[derive(Clone, Copy)]
pub(crate) enum RunRowOp {
    /// `SELECT .. FROM runs .. FOR NO KEY UPDATE` (the per-run mutex).
    LockRun,
    /// `UPDATE runs SET status ..` from a loaded run graph.
    FlushRun,
}

impl RunRowOp {
    const ALL: [Self; 2] = [Self::LockRun, Self::FlushRun];

    fn label(self) -> &'static str {
        match self {
            Self::LockRun => "lock_run",
            Self::FlushRun => "flush_run",
        }
    }
}

struct Totals {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl Totals {
    const fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }
}

static TOTALS: [Totals; RunRowOp::ALL.len()] = [Totals::new(), Totals::new()];

pub(crate) fn record(op: RunRowOp, elapsed: Duration) {
    let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    let totals = &TOTALS[op as usize];
    totals.count.fetch_add(1, Ordering::Relaxed);
    totals.total_us.fetch_add(us, Ordering::Relaxed);
    totals.max_us.fetch_max(us, Ordering::Relaxed);
}

/// Current cumulative stats.
pub(crate) fn snapshot() -> serde_json::Value {
    let run_row: Vec<serde_json::Value> = RunRowOp::ALL
        .iter()
        .map(|&op| {
            let totals = &TOTALS[op as usize];
            serde_json::json!({
                "op": op.label(),
                "count": totals.count.load(Ordering::Relaxed),
                "total_ms": totals.total_us.load(Ordering::Relaxed) as f64 / 1000.0,
                "max_ms": totals.max_us.load(Ordering::Relaxed) as f64 / 1000.0,
            })
        })
        .collect();
    serde_json::json!({ "run_row": run_row })
}
