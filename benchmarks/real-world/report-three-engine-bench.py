#!/usr/bin/env python3
"""Aggregate run-three-engine-bench.py JSON records into Markdown and JSON."""
from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * p
    low = int(index)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def load_records(root: Path) -> list[dict[str, Any]]:
    result_file = root / "results.json"
    if result_file.exists():
        return json.loads(result_file.read_text())
    records = []
    for path in root.glob("*/**/result.json"):
        records.append(json.loads(path.read_text()))
    return records


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[(record["engine"], record["workload"], record["cache_mode"])].append(record)
    summary = []
    for (engine, workload, cache_mode), rows in sorted(groups.items()):
        durations = [float(row["elapsed_s"]) for row in rows]
        summary.append({
            "engine": engine,
            "workload": workload,
            "cache_mode": cache_mode,
            "runs": len(rows),
            "successes": sum(row["status"] == "success" for row in rows),
            "failures": sum(row["status"] != "success" for row in rows),
            "median_elapsed_s": statistics.median(durations),
            "min_elapsed_s": min(durations),
            "max_elapsed_s": max(durations),
            "p95_elapsed_s": percentile(durations, 0.95),
            "peak_host_rss_mib": max(float(row.get("peak_host_rss_bytes", 0)) for row in rows) / 1024**2,
            "peak_docker_rss_mib": max(float(row.get("peak_docker_rss_bytes", 0)) for row in rows) / 1024**2,
            "peak_vm_rss_mib": max(float(row.get("peak_vm_rss_bytes", 0)) for row in rows) / 1024**2,
            "peak_host_cpu_percent": max(float(row.get("peak_host_cpu_percent", 0)) for row in rows),
            "peak_docker_cpu_percent": max(float(row.get("peak_docker_cpu_percent", 0)) for row in rows),
            "peak_vm_cpu_percent": max(float(row.get("peak_vm_cpu_percent", 0)) for row in rows),
        })
    return {"records": len(records), "summary": summary}


def markdown(data: dict[str, Any]) -> str:
    lines = [
        "# Three-Engine Benchmark Report",
        "",
        f"Runs recorded: **{data['records']}**",
        "",
        "Durations are wall-clock seconds. Cold and warm cache modes are reported separately.",
        "",
        "| Engine | Workload | Mode | Runs | Pass | Median | Min | Max | P95 | Host RSS MiB | Docker RSS MiB | VM RSS MiB | Host peak CPU % | Docker peak CPU % | VM peak CPU % |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in data["summary"]:
        lines.append(
            "| {engine} | {workload} | {cache_mode} | {runs} | {successes}/{runs} | {median_elapsed_s:.2f} | {min_elapsed_s:.2f} | {max_elapsed_s:.2f} | {p95_elapsed_s:.2f} | {peak_host_rss_mib:.1f} | {peak_docker_rss_mib:.1f} | {peak_vm_rss_mib:.1f} | {peak_host_cpu_percent:.1f} | {peak_docker_cpu_percent:.1f} | {peak_vm_cpu_percent:.1f} |".format(**row)
        )
    failures = [record for record in data.get("records_detail", []) if record["status"] != "success"]
    if failures:
        lines.extend(["", "## Failures", ""])
        for record in failures:
            lines.append(f"- `{record['engine']}/{record['workload']}/run-{record['rep']}`: {record['status']} (log: `{record['log']}`)")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    records = load_records(args.root)
    data = summarize(records)
    data["records_detail"] = records
    (args.root / "summary.json").write_text(json.dumps(data, indent=2) + "\n")
    (args.root / "REPORT.md").write_text(markdown(data))
    print((args.root / "REPORT.md").read_text())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
