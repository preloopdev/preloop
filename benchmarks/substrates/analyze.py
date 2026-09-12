#!/usr/bin/env python3
"""Summarize substrate benchmark samples into comparison tables.

Reads the JSONL emitted by `micro-bench.sh` (one sample per VM operation) and
`e2e-bench.sh` (one sample per workflow run) and prints medians, p90s, and the
SmolVM/AgentENV ratio per row. `bc` writes bare fractions like `.037`, which is
not valid JSON, so numbers are normalized on the way in.

Usage:
  analyze.py micro <micro-bench.jsonl>
  analyze.py e2e <aenv-results.jsonl> <smolvm-results.jsonl>
"""

from __future__ import annotations

import json
import re
import statistics as st
import sys
from collections import defaultdict

MICRO_ORDER = [
    "cold_boot",
    "first_exec",
    "exec_x10",
    "snapshot",
    "fork",
    "fork_first_exec",
    "fork_x8_concurrent",
    "delete_clone",
    "pause",
    "resume",
    "exec_after_resume",
    "guest_cpu_loop",
    "guest_dd_512m",
    "guest_2000_files",
    "delete",
]


def load(path: str) -> list[dict]:
    rows = []
    for line in open(path):
        line = re.sub(r'":\s*\.', '": 0.', line)
        line = re.sub(r'":\s*-\.', '": -0.', line)
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def p(values: list[float], q: float) -> float:
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def table(rows: dict[tuple[str, str], list[float]], keys: list[str], label: str) -> None:
    print(
        f"| {label} | AgentENV median | AgentENV p90 | SmolVM median | SmolVM p90 | SmolVM/AgentENV | n (a/s) |"
    )
    print("|---|---|---|---|---|---|---|")
    for key in keys:
        a = rows.get((key, "aenv"), [])
        s = rows.get((key, "smolvm"), [])
        if not a or not s:
            am = f"{st.median(a):.3f}" if a else "-"
            sm = f"{st.median(s):.3f}" if s else "-"
            print(f"| {key} | {am} | - | {sm} | - | n/a | {len(a)}/{len(s)} |")
            continue
        am, sm = st.median(a), st.median(s)
        ratio = sm / am if am else float("inf")
        print(
            f"| {key} | {am:.3f} | {p(a, .9):.3f} | {sm:.3f} | {p(s, .9):.3f} | {ratio:.2f}x | {len(a)}/{len(s)} |"
        )


def micro(path: str) -> None:
    samples: dict[tuple[str, str], list[float]] = defaultdict(list)
    failures: dict[tuple[str, str], int] = defaultdict(int)
    for row in load(path):
        key = (row["op"], row["substrate"])
        if row["ok"]:
            samples[key].append(row["seconds"])
        else:
            failures[key] += 1
    observed = [op for op in MICRO_ORDER if any(k[0] == op for k in samples)]
    extra = sorted({k[0] for k in samples} - set(observed))
    table(samples, observed + extra, "operation (seconds)")
    if failures:
        print("\nfailed samples:", {f"{k[1]}:{k[0]}": v for k, v in failures.items()})


def e2e(paths: list[str]) -> None:
    samples: dict[tuple[str, str], list[float]] = defaultdict(list)
    outcomes: dict[tuple[str, str], list[str]] = defaultdict(list)
    for path in paths:
        for row in load(path):
            key = (row["workflow"], row["substrate"])
            samples[key].append(row["seconds"])
            outcomes[key].append(row["conclusion"])
    order = sorted({k[0] for k in samples})
    table(samples, order, "workflow (seconds)")
    print("\nconclusions:")
    for key in sorted(outcomes):
        counts = defaultdict(int)
        for outcome in outcomes[key]:
            counts[outcome] += 1
        print(f"  {key[1]:7} {key[0]:36} {dict(counts)}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        raise SystemExit(2)
    if sys.argv[1] == "micro":
        micro(sys.argv[2])
    elif sys.argv[1] == "e2e":
        e2e(sys.argv[2:])
    else:
        print(__doc__)
        raise SystemExit(2)
