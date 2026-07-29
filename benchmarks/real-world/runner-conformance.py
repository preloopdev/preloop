#!/usr/bin/env python3
"""Validate the checked-in runner compatibility corpus.

The light profile checks workflow and job outcomes.  The deep profile also
checks every reported step name, conclusion, and order.  Matrix job order is
intentionally ignored because GitHub schedules matrix children nondeterministically.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


EXPECTED = {
    "101": "101-dynamic-matrix-dataflow",
    "102": "102-mask-and-secret-propagation",
    "103": "103-composite-nested-post",
    "104": "104-job-defaults-env-cascade",
    "105": "105-concurrency-cancellation-group",
    "107": "107-continue-on-error-status-funcs",
    "108": "108-workflow-dispatch-inputs",
    "109": "109-log-streaming-backpressure",
    "110": "110-environment-deployment-url",
    "111": "111-github-state-post-execution",
    "112": "112-service-container-health-ports",
    "113": "113-artifact-v4-multi-pattern",
    "114": "114-step-timeout-graceful-kill",
    "115": "115-cache-v2-restore-fallback",
}


def workflow_number(name: Any) -> str | None:
    match = re.match(r"(\d+)", str(name or "").removesuffix(".yml"))
    return match.group(1) if match else None


def load_latest(path: Path) -> tuple[dict[str, dict[str, Any]], list[str]]:
    records: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    if not path.exists():
        return records, [f"{path}: file does not exist"]

    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"{path}:{line_number}: invalid JSON: {exc}")
            continue
        if not isinstance(record, dict):
            errors.append(f"{path}:{line_number}: record is not an object")
            continue
        number = workflow_number(record.get("workflow"))
        if number in EXPECTED:
            # Batch captures append records.  The newest record is authoritative:
            # a failed dispatch must not silently fall back to an older run.
            records[number] = record
    return records, errors


def jobs(record: dict[str, Any]) -> list[dict[str, Any]]:
    result = record.get("result")
    if not isinstance(result, dict):
        return []
    value = result.get("jobs")
    return value if isinstance(value, list) else []


def outcome_summary(record: dict[str, Any]) -> tuple[str, list[tuple[str, str]]]:
    conclusion = str(record.get("conclusion") or "")
    summary = []
    for job in jobs(record):
        if isinstance(job, dict):
            summary.append((str(job.get("name") or ""), str(job.get("conclusion") or "")))
    return conclusion, sorted(summary)


def validate_response(record: dict[str, Any], side: str, number: str) -> list[str]:
    result = record.get("result")
    if not isinstance(result, dict):
        return [f"{number}: {side} response has no result object"]
    raw_jobs = result.get("jobs")
    if not isinstance(raw_jobs, list) or not raw_jobs:
        return [f"{number}: {side} response has no jobs"]

    issues = []
    for index, job in enumerate(raw_jobs):
        if not isinstance(job, dict):
            issues.append(f"{number}: {side} job {index} is not an object")
            continue
        if not job.get("name"):
            issues.append(f"{number}: {side} job {index} has no name")
        if not job.get("conclusion"):
            issues.append(f"{number}: {side} job {index} has no conclusion")
    return issues


def compare(
    official: dict[str, dict[str, Any]],
    aksh: dict[str, dict[str, Any]],
    mode: str,
) -> list[str]:
    issues: list[str] = []
    for number, description in EXPECTED.items():
        off = official.get(number)
        local = aksh.get(number)
        if off is None:
            issues.append(f"{number} {description}: missing official result")
            continue
        if local is None:
            issues.append(f"{number} {description}: missing aksh result")
            continue

        issues.extend(validate_response(off, "official", number))
        issues.extend(validate_response(local, "aksh", number))
        off_conclusion, off_jobs = outcome_summary(off)
        aksh_conclusion, aksh_jobs = outcome_summary(local)
        if not off_conclusion or off_conclusion == "unknown":
            issues.append(f"{number} {description}: official result is incomplete")
        if not aksh_conclusion or aksh_conclusion == "unknown":
            issues.append(f"{number} {description}: aksh result is incomplete")
        if off_conclusion != aksh_conclusion:
            issues.append(
                f"{number} {description}: workflow conclusion "
                f"official={off_conclusion or '(empty)'} "
                f"aksh={aksh_conclusion or '(empty)'}"
            )
        if off_jobs != aksh_jobs:
            issues.append(
                f"{number} {description}: job outcomes differ "
                f"official={off_jobs!r} aksh={aksh_jobs!r}"
            )

        if mode != "deep":
            continue

        off_by_name = {str(job.get("name") or ""): job for job in jobs(off)}
        aksh_by_name = {str(job.get("name") or ""): job for job in jobs(local)}
        for job_name in sorted(set(off_by_name) | set(aksh_by_name)):
            off_job = off_by_name.get(job_name)
            aksh_job = aksh_by_name.get(job_name)
            if off_job is None or aksh_job is None:
                continue  # already reported by the light job comparison

            off_steps = [
                (str(step.get("name") or ""), str(step.get("conclusion") or ""))
                for step in off_job.get("steps", [])
                if isinstance(step, dict)
            ]
            aksh_steps = [
                (str(step.get("name") or ""), str(step.get("conclusion") or ""))
                for step in aksh_job.get("steps", [])
                if isinstance(step, dict)
            ]
            if off_steps != aksh_steps:
                issues.append(
                    f"{number} {description} job {job_name!r}: steps differ "
                    f"official={off_steps!r} aksh={aksh_steps!r}"
                )

    return issues


def write_report(
    output: Path,
    mode: str,
    official: dict[str, dict[str, Any]],
    aksh: dict[str, dict[str, Any]],
    issues: list[str],
    loader_errors: list[str],
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Runner Compatibility Conformance",
        "",
        f"Profile: **{mode}**",
        "",
        "The official runner and `preloop-runner` are compared against the same "
        "GitHub workflow runs.",
        "",
        f"- Expected workflows: {len(EXPECTED)}",
        f"- Official records: {len(official)}",
        f"- Aksh records: {len(aksh)}",
        f"- Verdict: **{'PASS' if not issues and not loader_errors else 'FAIL'}**",
        "",
    ]
    if loader_errors:
        lines += ["## Capture errors", "", *[f"- {error}" for error in loader_errors], ""]
    if issues:
        lines += ["## Differences", "", *[f"- {issue}" for issue in issues], ""]
    else:
        lines += ["No differences found.", ""]
    output.write_text("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("light", "deep"), required=True)
    parser.add_argument(
        "--official",
        type=Path,
        default=Path("benchmarks/compatibility/runner/behavior/conformance-official.jsonl"),
    )
    parser.add_argument(
        "--aksh",
        type=Path,
        default=Path("benchmarks/compatibility/runner/behavior/conformance-aksh.jsonl"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmarks/compatibility/runner/behavior/RUNNER-CONFORMANCE-REPORT.md"),
    )
    args = parser.parse_args()

    official, official_errors = load_latest(args.official)
    aksh, aksh_errors = load_latest(args.aksh)
    loader_errors = official_errors + aksh_errors
    issues = compare(official, aksh, args.mode)
    write_report(args.output, args.mode, official, aksh, issues, loader_errors)

    print(
        f"runner-{args.mode}: "
        f"{'PASS' if not issues and not loader_errors else 'FAIL'} "
        f"({len(issues) + len(loader_errors)} issue(s)); report={args.output}"
    )
    for issue in loader_errors + issues:
        print(f"  - {issue}", file=sys.stderr)
    return 0 if not issues and not loader_errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
