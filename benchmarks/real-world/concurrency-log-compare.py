#!/usr/bin/env python3
"""Compare concurrency scenario outcomes + step log content: GitHub vs aksh.

Captures aksh runs (status/jobs/events + step log blobs from state dir), then
diffs against live GitHub captures under concurrency-live/.

Usage:
  python3 benchmarks/real-world/concurrency-log-compare.py \
    --github-root benchmarks/real-world/results/concurrency-live/2026-07-13T13-19-42Z \
    --aksh-root benchmarks/real-world/results/concurrency-live/aksh-compare-<ts>
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?")
BOM = "\ufeff"


def strip_noise(line: str) -> str:
    line = line.replace(BOM, "")
    line = ANSI_RE.sub("", line)
    line = TS_RE.sub("<TS>", line)
    return line.strip()


def parse_gh_run_log(text: str) -> dict[str, list[str]]:
    """job\\tstep\\tcontent -> {step: [normalized content lines]}"""
    steps: dict[str, list[str]] = defaultdict(list)
    for raw in text.splitlines():
        parts = raw.split("\t", 2)
        if len(parts) < 3:
            continue
        _job, step, content = parts
        c = strip_noise(content)
        if not c:
            continue
        # Drop pure infrastructure noise unique to hosted runners
        if any(
            x in c
            for x in (
                "Current runner version",
                "Runner Image Provisioner",
                "Hosted Compute Agent",
                "Azure Region",
                "Included Software",
                "Image Release",
                "GITHUB_TOKEN Permissions",
                "Secret source:",
                "Prepare workflow directory",
                "Prepare all required actions",
                "Complete job name:",
                "Operating System",
                "Runner Image",
                "Image: ubuntu",
                "Version: 2026",
                "Ubuntu",
                "LTS",
                "Contents: read",
                "Metadata: read",
                "Packages: read",
                "shell: /usr/bin/bash",
            )
        ):
            continue
        steps[step].append(c)
    return dict(steps)


def extract_markers(lines: list[str]) -> set[str]:
    markers = set()
    for line in lines:
        # executed output (not ##[group] Run echo ...)
        if line.startswith("##["):
            if "error" in line.lower() and "cancel" in line.lower():
                markers.add("CANCEL_ERROR")
            continue
        if "SCENARIO=" in line and "echo" not in line:
            m = re.search(r"SCENARIO=([^\s]+)", line)
            if m:
                markers.add(f"SCENARIO={m.group(1)}")
        if re.search(r"\bDONE=([^\s]+)", line) and "echo" not in line:
            m = re.search(r"\bDONE=([^\s]+)", line)
            if m:
                markers.add(f"DONE={m.group(1)}")
        if "SHOULD_NOT_REACH" in line and "echo" not in line and "36;1m" not in line:
            # only count if it looks like executed output
            if line.strip() == "SHOULD_NOT_REACH" or line.endswith("SHOULD_NOT_REACH"):
                markers.add("SHOULD_NOT_REACH_EXECUTED")
        if "The operation was canceled" in line or "operation was cancelled" in line.lower():
            markers.add("CANCEL_ERROR")
    return markers


@dataclass
class SideCapture:
    name: str
    conclusion: str | None
    jobs: dict[str, str] = field(default_factory=dict)  # job_id -> conclusion
    step_conclusions: dict[str, str] = field(default_factory=dict)  # step name -> conclusion
    markers: set[str] = field(default_factory=set)
    step_logs: dict[str, list[str]] = field(default_factory=dict)
    raw_log: str = ""


def load_github_capture(dir_path: Path) -> SideCapture:
    summary = json.loads((dir_path / "summary.json").read_text())
    log = (dir_path / "run.log").read_text(errors="replace") if (dir_path / "run.log").exists() else ""
    steps = parse_gh_run_log(log)
    markers: set[str] = set()
    for lines in steps.values():
        markers |= extract_markers(lines)
    markers |= extract_markers([strip_noise(l) for l in log.splitlines()])

    step_conc: dict[str, str] = {}
    jobs: dict[str, str] = {}
    for j in summary.get("jobs") or []:
        jobs[j.get("name") or ""] = j.get("conclusion") or ""
        for s in j.get("steps") or []:
            if s.get("name"):
                step_conc[s["name"]] = s.get("conclusion") or ""

    return SideCapture(
        name=dir_path.name,
        conclusion=summary.get("conclusion"),
        jobs=jobs,
        step_conclusions=step_conc,
        markers=markers,
        step_logs=steps,
        raw_log=log,
    )


def load_aksh_capture(dir_path: Path) -> SideCapture:
    summary = json.loads((dir_path / "summary.json").read_text())
    log = (dir_path / "run.log").read_text(errors="replace") if (dir_path / "run.log").exists() else ""
    # aksh run.log may be synthetic: step\tline or plain lines
    steps: dict[str, list[str]] = defaultdict(list)
    for raw in log.splitlines():
        parts = raw.split("\t", 2)
        if len(parts) == 3:
            _j, step, content = parts
            steps[step].append(strip_noise(content))
        elif len(parts) == 2:
            step, content = parts
            steps[step].append(strip_noise(content))
        else:
            steps["_"].append(strip_noise(raw))
    markers: set[str] = set()
    for lines in steps.values():
        markers |= extract_markers(lines)
    markers |= extract_markers([strip_noise(l) for l in log.splitlines()])

    raw_jobs = summary.get("jobs_list") or summary.get("jobs") or []
    if isinstance(raw_jobs, dict):
        jobs = {str(job_id): str(status or "") for job_id, status in raw_jobs.items()}
        step_conc = {}
    else:
        jobs = {
            job.get("name") or job.get("id") or "":
            job.get("conclusion") or job.get("status") or ""
            for job in raw_jobs
        }
        step_conc = {}
        for job in raw_jobs:
            for step in job.get("steps") or []:
                if step.get("name"):
                    step_conc[step["name"]] = (
                        step.get("conclusion") or step.get("status") or ""
                    )

    return SideCapture(
        name=dir_path.name,
        conclusion=summary.get("conclusion") or summary.get("status"),
        jobs=jobs,
        step_conclusions=step_conc,
        markers=markers,
        step_logs=dict(steps),
        raw_log=log,
    )


def compare(gh: SideCapture, aksh: SideCapture) -> dict:
    issues: list[str] = []
    notes: list[str] = []

    # Normalize conclusions
    def norm(c: str | None) -> str:
        if not c:
            return ""
        c = c.lower()
        if c in ("canceled",):
            return "cancelled"
        if c in ("succeeded",):
            return "success"
        if c in ("failed",):
            return "failure"
        return c

    if norm(gh.conclusion) != norm(aksh.conclusion):
        issues.append(f"run conclusion: gh={gh.conclusion} aksh={aksh.conclusion}")
    else:
        notes.append(f"run conclusion match: {norm(gh.conclusion)}")

    # Cross-run contamination: aksh capture must not contain SCENARIO= or DONE= markers
    # from runs other than the one being compared. Extra markers indicate the run.log
    # accumulated output from previous scenarios, making the capture untrustworthy.
    gh_scenarios = {m for m in gh.markers if m.startswith("SCENARIO=")}
    ak_scenarios = {m for m in aksh.markers if m.startswith("SCENARIO=")}
    extra_scenarios = ak_scenarios - gh_scenarios
    if extra_scenarios:
        issues.append(
            f"cross-run log contamination: aksh has SCENARIO markers {sorted(extra_scenarios)} "
            f"not present in GH capture {sorted(gh_scenarios)}; "
            "capture run.log was not isolated to this run"
        )
    for sm in sorted(gh_scenarios):
        if sm not in ak_scenarios:
            issues.append(f"scenario marker missing in aksh: {sm}")
        else:
            notes.append(f"scenario marker present: {sm}")

    gh_dones = {m for m in gh.markers if m.startswith("DONE=")}
    ak_dones = {m for m in aksh.markers if m.startswith("DONE=")}
    extra_dones = ak_dones - gh_dones
    if extra_dones:
        issues.append(
            f"cross-run log contamination: aksh has DONE markers {sorted(extra_dones)} "
            f"not present in GH capture {sorted(gh_dones)}; "
            "capture run.log was not isolated to this run"
        )
    for dm in sorted(gh_dones):
        if dm not in ak_dones:
            # cancelled runs often lack DONE=
            if norm(gh.conclusion) != "cancelled":
                issues.append(f"DONE marker missing in aksh: {dm}")
        else:
            notes.append(f"DONE marker present: {dm}")

    # Cancellation annotations are part of step-log fidelity. Presence must
    # match in both directions; conclusions do not substitute for log parity.
    gh_cancel_error = "CANCEL_ERROR" in gh.markers
    aksh_cancel_error = "CANCEL_ERROR" in aksh.markers
    if gh_cancel_error != aksh_cancel_error:
        issues.append(
            "cancel error annotation presence differs: "
            f"gh={gh_cancel_error} aksh={aksh_cancel_error}"
        )
    elif gh_cancel_error:
        notes.append("cancel error annotation present on both")

    # SHOULD_NOT_REACH executed in aksh capture is always a hard failure when GH did not
    # execute it. This marker appears in steps that must not run (e.g., a sleep after
    # cancel-in-progress). Its presence either means a real concurrency bug (the step was
    # not cancelled) or cross-run log contamination. Both conditions make the capture invalid.
    if "SHOULD_NOT_REACH_EXECUTED" in aksh.markers and "SHOULD_NOT_REACH_EXECUTED" not in gh.markers:
        issues.append(
            "SHOULD_NOT_REACH was executed in aksh capture but not in GH capture — "
            "real concurrency bug or cross-run log contamination; capture is invalid"
        )

    # Step conclusions: compare user steps one-to-one. Missing or additional
    # steps are structural mismatches, not fidelity notes.
    gh_user = {
        key: value
        for key, value in gh.step_conclusions.items()
        if key not in ("Set up job", "Complete job")
    }
    ak_user = {
        key: value
        for key, value in aksh.step_conclusions.items()
        if key not in ("Set up job", "Complete job", "Set up runner", "Complete runner")
    }
    if len(gh_user) != len(ak_user):
        issues.append(f"user step count: gh={len(gh_user)} aksh={len(ak_user)}")

    unmatched_aksh = set(ak_user)
    for gh_name, gh_conclusion in gh_user.items():
        candidates = [name for name in unmatched_aksh if name == gh_name]
        if not candidates:
            candidates = [
                name
                for name in unmatched_aksh
                if gh_name in name or name in gh_name
            ]
        if not candidates and len(gh_user) == 1 and len(unmatched_aksh) == 1:
            candidates = list(unmatched_aksh)
        if not candidates:
            issues.append(
                f"missing aksh step matching GH step '{gh_name}' ({gh_conclusion})"
            )
            continue
        aksh_name = sorted(candidates)[0]
        unmatched_aksh.remove(aksh_name)
        aksh_conclusion = ak_user[aksh_name]
        if norm(gh_conclusion) != norm(aksh_conclusion):
            issues.append(
                f"step '{gh_name}' conclusion: "
                f"gh={gh_conclusion} aksh={aksh_name}/{aksh_conclusion}"
            )
        else:
            notes.append(
                f"step '{gh_name}'≈'{aksh_name}' conclusion={norm(gh_conclusion)}"
            )
    for aksh_name in sorted(unmatched_aksh):
        issues.append(
            f"unexpected aksh step '{aksh_name}' ({ak_user[aksh_name]})"
        )

    # Job conclusions are always compared, including zero-job captures.
    gh_values = sorted(norm(value) for value in gh.jobs.values())
    aksh_values = sorted(norm(value) for value in aksh.jobs.values())
    if len(gh.jobs) != len(aksh.jobs):
        issues.append(f"job count: gh={len(gh.jobs)} aksh={len(aksh.jobs)}")
    if norm(aksh.conclusion) == "cancelled" and aksh_values and all(
        value == "success" for value in aksh_values
    ):
        issues.append(
            f"contradictory aksh capture: job conclusions={aksh_values} "
            "but run conclusion=cancelled; job conclusion must come from run API, "
            "not from heuristic runner log"
        )
    elif gh_values != aksh_values:
        issues.append(
            f"job conclusions multiset: gh={gh_values} aksh={aksh_values}"
        )
    else:
        notes.append(f"job conclusions match: {gh_values}")

    return {
        "ok": len(issues) == 0,
        "issues": issues,
        "notes": notes,
        "gh": {"name": gh.name, "conclusion": gh.conclusion, "markers": sorted(gh.markers)},
        "aksh": {"name": aksh.name, "conclusion": aksh.conclusion, "markers": sorted(aksh.markers)},
    }


CAPTURE_PAIRS = [
    ("01 bare A", "01-bare-A", "01-bare-A"),
    ("01 bare B", "01-bare-B", "01-bare-B"),
    ("02 cancel-in-progress A", "02-cancel-A", "02-cancel-A"),
    ("02 cancel-in-progress B", "02-cancel-B", "02-cancel-B"),
    ("03 FIFO A", "03-fifo-A", "03-fifo-A"),
    ("03 FIFO B", "03-fifo-B", "03-fifo-B"),
    ("04 cancel expression A", "04-cancel-expr-A", "04-cancel-expr-A"),
    ("04 cancel expression B", "04-cancel-expr-B", "04-cancel-expr-B"),
    ("05 false expression A", "05-expr-false-A", "05-expr-false-A"),
    ("05 false expression B", "05-expr-false-B", "05-expr-false-B"),
    ("06 queue max A", "06-queue-max-A", "06-queue-max-A"),
    ("06 queue max B", "06-queue-max-B", "06-queue-max-B"),
    ("06 queue max C", "06-queue-max-C", "06-queue-max-C"),
    ("07 case Prod", "07a-case-Prod", "07a-case-Prod"),
    ("07 case prod", "07b-case-prod", "07b-case-prod"),
    ("08 job-level", "08-job-level", "08-job-level"),
    ("09 multi-job", "09-multi-job", "09-multi-job"),
    ("10 empty group", "10-empty", "10-empty-group"),
    ("11 expression group", "11-expr-group", "11-expr-group"),
    ("12 matrix", "12-matrix", "12-matrix"),
    ("13 caller JobSet", "13-jobset-caller", "13-jobset-caller"),
    ("14 embedded JobSet", "14-jobset-embedded", "14-jobset-embedded"),
    ("15 different-key JobSet", "15-jobset-diffkey", "15-jobset-diffkey"),
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--github-root", type=Path, required=True)
    ap.add_argument("--aksh-root", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()
    out = args.out or (args.aksh_root / "LOG-CONTENT-COMPARE.md")

    comparisons = []

    pairs = [
        (name, args.github_root / github_dir, args.aksh_root / aksh_dir)
        for name, github_dir, aksh_dir in CAPTURE_PAIRS
    ]

    for name, gh_path, ak_path in pairs:
        if gh_path is None or not Path(gh_path).exists():
            comparisons.append({"name": name, "ok": False, "issues": ["missing github capture"], "notes": []})
            continue
        if not ak_path.exists():
            comparisons.append({"name": name, "ok": False, "issues": [f"missing aksh capture {ak_path}"], "notes": []})
            continue
        gh = load_github_capture(Path(gh_path))
        ak = load_aksh_capture(ak_path)
        result = compare(gh, ak)
        result["name"] = name
        result["gh_path"] = str(gh_path)
        result["aksh_path"] = str(ak_path)
        comparisons.append(result)

    passed = sum(1 for c in comparisons if c.get("ok"))
    total = len(comparisons)

    lines = [
        "# Concurrency Log/Step Content Compare: GitHub vs aksh",
        "",
        f"**GitHub root:** `{args.github_root}`",
        f"**aksh root:** `{args.aksh_root}`",
        f"**Score:** **{passed}/{total}** scenarios with matching conclusions + content markers + step outcomes",
        "",
        "## What is compared",
        "",
        "1. **Run conclusion** (success/cancelled/failure)",
        "2. **Job conclusion multiset**",
        "3. **User step conclusions** (fuzzy name match; ignores hosted-only Set up job / Complete job)",
        "4. **Content markers** in step logs: `SCENARIO=*`, `DONE=*`, cancel error annotation",
        "5. Hosted-only infra log lines (image provisioner, GITHUB_TOKEN perms, etc.) are **stripped** before compare",
        "",
        "| Scenario | Result | Issues | Notes |",
        "|----------|--------|--------|-------|",
    ]
    for c in comparisons:
        issues = "; ".join(c.get("issues") or []) or "—"
        notes = "; ".join((c.get("notes") or [])[:3]) or "—"
        lines.append(
            f"| {c['name']} | {'✅' if c.get('ok') else '❌'} | {issues[:120]} | {notes[:140]} |"
        )

    lines.append("")
    lines.append("## Per-scenario detail")
    lines.append("")
    for c in comparisons:
        lines.append(f"### {c['name']}")
        lines.append(f"- ok: `{c.get('ok')}`")
        if c.get("gh_path"):
            lines.append(f"- github: `{c['gh_path']}`")
        if c.get("aksh_path"):
            lines.append(f"- aksh: `{c['aksh_path']}`")
        for i in c.get("issues") or []:
            lines.append(f"- **issue:** {i}")
        for n in c.get("notes") or []:
            lines.append(f"- note: {n}")
        if c.get("gh"):
            lines.append(f"- gh markers: `{c['gh'].get('markers')}`")
        if c.get("aksh"):
            lines.append(f"- aksh markers: `{c['aksh'].get('markers')}`")
        lines.append("")

    out.write_text("\n".join(lines))
    comparison_json = json.dumps(comparisons, indent=2)
    out.with_suffix(".json").write_text(comparison_json)
    default_json = args.aksh_root / "LOG-CONTENT-COMPARE.json"
    if default_json != out.with_suffix(".json"):
        default_json.write_text(comparison_json)
    print(f"{passed}/{total} passed")
    print(out)
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
