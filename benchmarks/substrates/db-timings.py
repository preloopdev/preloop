#!/usr/bin/env python3
"""Per-run and per-step timings straight from each engine's state DB.

The driver's wall clock includes queue waiting, which on an on-demand pool is
dominated by runner provisioning — and, when a job fails, by the 600 s debug
preserve that parks its VM and holds a concurrency permit. Splitting run
duration from step duration separates "what the substrate executed" from "what
the pool made the run wait for".
"""

import sqlite3
import sys
from collections import defaultdict

HOMES = {
    "aenv": "/home/bnjoroge/.preloop-bench-aenv",
    "smolvm": "/home/bnjoroge/.preloop-bench-smolvm",
}


def report(label: str, home: str) -> None:
    db = sqlite3.connect(f"{home}/state/preloop.db")
    print(f"\n=== {label}")
    print(f"{'workflow':32} {'run wall':>9} {'steps':>8} {'status':>10}")
    steps = defaultdict(float)
    for run_id, started, finished in db.execute(
        "select run_id, started_at_us, finished_at_us from job_steps "
        "where started_at_us is not null and finished_at_us is not null"
    ):
        steps[run_id] += (finished - started) / 1e6
    for run_id, path, status, created, completed in db.execute(
        "select run_id, workflow_path, status, created_at_us, completed_at_us from runs order by created_at_us"
    ):
        wall = (completed - created) / 1e6 if created and completed else None
        workflow = (path or "?").rsplit("/", 1)[-1]
        wall_text = f"{wall:9.1f}" if wall is not None else f"{'-':>9}"
        print(f"{workflow:32} {wall_text} {steps.get(run_id, 0.0):8.2f} {status:>10}")


if __name__ == "__main__":
    for label in sys.argv[1:] or HOMES:
        report(label, HOMES[label])
