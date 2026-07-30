#!/usr/bin/env python3
"""Strictly compare captured official-runner GitHub and aksh-server results."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_SCENARIOS = (
    "200-v2336-combined",
    "201-v2336-background-cancel",
    "202-v2336-file-commands",
)


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def github_jobs(path: Path) -> list[dict[str, Any]]:
    value = read_json(path)
    jobs = value.get("jobs") if value else None
    return jobs if isinstance(jobs, list) else []


def aksh_jobs(path: Path) -> list[dict[str, Any]]:
    value = read_json(path)
    jobs = value.get("jobs_list") if value else None
    if isinstance(jobs, list):
        return jobs
    status_jobs = value.get("jobs") if value else None
    if isinstance(status_jobs, dict):
        return [
            {"name": name, "conclusion": conclusion, "steps": []}
            for name, conclusion in status_jobs.items()
        ]
    return []


def job_signature(job: dict[str, Any]) -> tuple[str, str]:
    return str(job.get("name") or ""), str(job.get("conclusion") or "")


def step_signature(job: dict[str, Any]) -> list[tuple[str, str]]:
    steps = job.get("steps")
    if not isinstance(steps, list):
        return []
    return [
        (str(step.get("name") or ""), str(step.get("conclusion") or ""))
        for step in steps
        if isinstance(step, dict)
    ]


def compare_scenario(root: Path, scenario: str) -> list[str]:
    base = root / scenario
    github_dir = base / "github"
    aksh_dir = base / "aksh-server"
    github_summary = read_json(github_dir / "summary.json")
    aksh_summary = read_json(aksh_dir / "summary.json")
    issues: list[str] = []

    if github_summary is None:
        issues.append(f"{scenario}: missing GitHub summary")
    if aksh_summary is None:
        issues.append(f"{scenario}: missing aksh-server summary")
    if github_summary is None or aksh_summary is None:
        return issues

    github_conclusion = str(github_summary.get("conclusion") or "")
    aksh_conclusion = str(aksh_summary.get("conclusion") or "")
    if not github_conclusion or not aksh_conclusion:
        issues.append(f"{scenario}: incomplete conclusion")
    elif github_conclusion != aksh_conclusion:
        issues.append(
            f"{scenario}: conclusion differs "
            f"GitHub={github_conclusion} aksh={aksh_conclusion}"
        )

    official_jobs = github_jobs(github_dir / "jobs.json")
    local_jobs = aksh_jobs(aksh_dir / "run.json")
    official_by_name = {job_signature(job)[0]: job for job in official_jobs}
    local_by_name = {job_signature(job)[0]: job for job in local_jobs}
    if set(official_by_name) != set(local_by_name):
        issues.append(
            f"{scenario}: job names differ "
            f"GitHub={sorted(official_by_name)} aksh={sorted(local_by_name)}"
        )
    for name in sorted(set(official_by_name) & set(local_by_name)):
        official = official_by_name[name]
        local = local_by_name[name]
        if job_signature(official)[1] != job_signature(local)[1]:
            issues.append(
                f"{scenario} job {name!r}: conclusion differs "
                f"GitHub={job_signature(official)[1]} "
                f"aksh={job_signature(local)[1]}"
            )
        if step_signature(official) != step_signature(local):
            issues.append(
                f"{scenario} job {name!r}: steps differ "
                f"GitHub={step_signature(official)!r} "
                f"aksh={step_signature(local)!r}"
            )

    github_flows = github_dir / "flows.jsonl"
    aksh_flows = aksh_dir / "flows.jsonl"
    if not github_flows.exists() or not aksh_flows.exists():
        issues.append(f"{scenario}: missing flow capture")
    else:
        github_count = sum(1 for line in github_flows.read_text().splitlines() if line.strip())
        aksh_count = sum(1 for line in aksh_flows.read_text().splitlines() if line.strip())
        if not github_count or not aksh_count:
            issues.append(f"{scenario}: empty flow capture")
        elif github_count != aksh_count:
            issues.append(
                f"{scenario}: flow counts differ GitHub={github_count} aksh={aksh_count}"
            )
    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("benchmarks/compatibility/server/behavior"),
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("scenarios", nargs="*", default=list(DEFAULT_SCENARIOS))
    args = parser.parse_args()

    issues = [
        issue
        for scenario in args.scenarios
        for issue in compare_scenario(args.root, scenario)
    ]
    report = [
        "# Server Compatibility Conformance",
        "",
        f"- Scenarios: {len(args.scenarios)}",
        f"- Verdict: **{'PASS' if not issues else 'FAIL'}**",
        "",
    ]
    report += ["No differences found.", ""] if not issues else [
        "## Differences",
        "",
        *[f"- {issue}" for issue in issues],
        "",
    ]
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text("\n".join(report))
    print(f"server-deep: {'PASS' if not issues else 'FAIL'} ({len(issues)} issue(s))")
    for issue in issues:
        print(f"  - {issue}")
    return 0 if not issues else 1


if __name__ == "__main__":
    raise SystemExit(main())
