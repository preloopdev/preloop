#!/usr/bin/env python3
"""Fail unless every MITM scenario has a usable pinned runner flow capture."""

from __future__ import annotations

import json
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = ROOT / "experiments" / "mitm" / "scenarios"
GOLDENS = ROOT / ".runner-watch" / "golden"


def main() -> int:
    version = tomllib.loads((ROOT / "versions.toml").read_text())["runner_version"]
    mitm_version = tomllib.loads(
        (ROOT / "experiments" / "mitm" / "versions.toml").read_text()
    )["runner_version"]
    if version != mitm_version:
        raise SystemExit(
            f"runner pins differ: versions.toml={version}, experiments/mitm={mitm_version}"
        )

    expected = {
        path.parent.name for path in SCENARIOS.glob("*/scenario.toml")
    }
    golden_root = GOLDENS / f"v{version}"
    replayable = {
        path.parent.name
        for path in golden_root.glob("*/flows.jsonl")
        if path.stat().st_size > 0
    }
    missing = sorted(expected - replayable)
    if missing:
        raise SystemExit(
            f"missing v{version} MITM flows ({len(missing)}): " + ", ".join(missing)
        )

    invalid = []
    for scenario in sorted(expected):
        flows_path = golden_root / scenario / "flows.jsonl"
        summary_path = golden_root / scenario / "summary.json"
        try:
            summary = json.loads(summary_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            invalid.append(f"{scenario}: invalid summary")
            continue
        with flows_path.open("rb") as flows:
            actual_flows = sum(1 for line in flows if line.strip())
        if (
            summary.get("status") != "ok"
            or summary.get("runner_version") != version
            or summary.get("flows_count") != actual_flows
        ):
            invalid.append(
                f"{scenario}: status={summary.get('status')!r} "
                f"version={summary.get('runner_version')!r} "
                f"flows={summary.get('flows_count')!r}/{actual_flows}"
            )
    if invalid:
        raise SystemExit("unusable MITM captures: " + "; ".join(invalid))

    print(f"MITM corpus: {len(expected)}/{len(expected)} scenarios at runner v{version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
