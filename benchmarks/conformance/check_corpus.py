#!/usr/bin/env python3
"""Fail unless every MITM scenario has a usable, uncontaminated flow capture.

Two classes of defect are checked, because both have shipped silently before:

* A capture is *missing or unusable* — no flows, wrong runner pin, summary
  disagreeing with the file. This was the original purpose of this script.

* A capture is *of the wrong job*. The recorder dispatches a workflow to
  GitHub, then starts a generic `self-hosted` runner in a shared repo. That
  runner takes whatever is at the head of the queue, which is not necessarily
  the job just dispatched — a `needs:`-gated job from the previous scenario
  queues only after that scenario stopped recording, so the next scenario's
  runner picks it up. Three of 39 captures were contaminated this way and the
  conformance gate reported them as preloop divergences for months.

  Two invariants catch it:
    1. A GitHub orchestration plan GUID identifies exactly one workflow run,
       so no GUID may appear in two scenario captures.
    2. A capture's acquired job must be one its own workflow declares.

Known-bad captures are declared in `.runner-watch/quarantine.toml` with a
reason and evidence, and excluded from the gate rather than silently skipped.
"""

from __future__ import annotations

import json
import re
import sys
import tomllib
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = ROOT / "experiments" / "mitm" / "scenarios"
GOLDENS = ROOT / ".runner-watch" / "golden"
QUARANTINE_FILE = ROOT / ".runner-watch" / "quarantine.toml"
QUARANTINE_DIR = ROOT / ".runner-watch" / "quarantine"

# `jobs:` block entries: two-space indented `id:` at the top of a job body.
JOB_ID = re.compile(r"^  ([A-Za-z_][A-Za-z0-9_-]*):\s*$")


def load_quarantine(version: str) -> dict[str, str]:
    """Scenario -> reason, for captures deliberately excluded at this version."""
    if not QUARANTINE_FILE.exists():
        return {}
    raw = tomllib.loads(QUARANTINE_FILE.read_text())
    out = {}
    for scenario, entries in raw.items():
        for entry in entries:
            if entry.get("runner_version") != version:
                continue
            reason = (entry.get("reason") or "").strip()
            if not reason:
                raise SystemExit(f"quarantine entry for {scenario} has no reason")
            if not (QUARANTINE_DIR / f"v{version}" / scenario / "flows.jsonl").exists():
                raise SystemExit(
                    f"quarantined capture {scenario} is not preserved under "
                    f".runner-watch/quarantine/v{version}/ — keep the evidence"
                )
            out[scenario] = reason
    return out


def acquirejob_response(flows_path: Path) -> dict | None:
    with flows_path.open() as flows:
        for line in flows:
            if not line.strip():
                continue
            flow = json.loads(line)
            if "acquirejob" in (flow.get("path") or ""):
                return flow.get("response_body_json") or {}
    return None


def declared_jobs(scenario: str) -> tuple[set[str], str]:
    """Declared job ids plus the raw workflow text, for this scenario."""
    directory = SCENARIOS / scenario
    ids: set[str] = set()
    text = ""
    for path in sorted(directory.rglob("*.y*ml")):
        body = path.read_text()
        text += body
        in_jobs = False
        for line in body.splitlines():
            if line.startswith("jobs:"):
                in_jobs = True
                continue
            if in_jobs and line and not line.startswith((" ", "\t", "#")):
                in_jobs = False
            if in_jobs:
                match = JOB_ID.match(line)
                if match:
                    ids.add(match.group(1))
    return ids, text


def check_capture_identity(scenarios: list[str], golden_root: Path) -> list[str]:
    """Enforce the two anti-contamination invariants."""
    problems: list[str] = []
    plan_owners: dict[str, list[str]] = defaultdict(list)

    for scenario in scenarios:
        response = acquirejob_response(golden_root / scenario / "flows.jsonl")
        if response is None:
            # Idle scenarios (01-register-and-idle) never acquire a job.
            continue

        plan_id = (response.get("plan") or {}).get("planId")
        if plan_id:
            plan_owners[plan_id].append(scenario)

        display = response.get("jobDisplayName") or ""
        ids, text = declared_jobs(scenario)
        if not ids or not display:
            # Nothing to compare against; the usability checks still apply.
            continue
        # A matrix cell renders as `build (ubuntu-latest, 18)`. A job with an
        # explicit `name:` renders as that name, which appears in the source.
        known = any(display == i or display.startswith(f"{i} (") for i in ids)
        if not known and display not in text:
            problems.append(
                f"{scenario}: captured job {display!r} is not declared by this "
                f"scenario (declares {', '.join(sorted(ids))}) — the recorder "
                f"acquired another scenario's job"
            )

    for plan_id, owners in sorted(plan_owners.items()):
        if len(owners) > 1:
            problems.append(
                f"plan {plan_id} appears in {len(owners)} captures "
                f"({', '.join(sorted(owners))}) — one workflow run cannot span "
                f"two scenarios, so at least one capture is contaminated"
            )
    return problems


def main() -> int:
    version = tomllib.loads((ROOT / "versions.toml").read_text())["runner_version"]
    mitm_version = tomllib.loads(
        (ROOT / "experiments" / "mitm" / "versions.toml").read_text()
    )["runner_version"]
    if version != mitm_version:
        raise SystemExit(
            f"runner pins differ: versions.toml={version}, experiments/mitm={mitm_version}"
        )

    quarantined = load_quarantine(version)
    expected = {path.parent.name for path in SCENARIOS.glob("*/scenario.toml")}
    unknown = sorted(set(quarantined) - expected)
    if unknown:
        raise SystemExit(
            "quarantine names scenarios that do not exist: " + ", ".join(unknown)
        )
    expected -= set(quarantined)

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
    still_present = sorted(set(quarantined) & replayable)
    if still_present:
        raise SystemExit(
            "quarantined captures are still in the gated corpus: "
            + ", ".join(still_present)
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

    contaminated = check_capture_identity(sorted(expected), golden_root)
    if contaminated:
        raise SystemExit("contaminated MITM captures:\n  " + "\n  ".join(contaminated))

    print(f"MITM corpus: {len(expected)}/{len(expected)} scenarios at runner v{version}")
    for scenario, reason in sorted(quarantined.items()):
        print(f"  quarantined: {scenario} — {reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
