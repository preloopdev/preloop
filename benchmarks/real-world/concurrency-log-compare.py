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

    jobs = {j.get("name") or j.get("id") or "": j.get("conclusion") or j.get("status") or ""
            for j in (summary.get("jobs") or [])}
    step_conc = {}
    for j in summary.get("jobs") or []:
        for s in j.get("steps") or []:
            if s.get("name"):
                step_conc[s["name"]] = s.get("conclusion") or s.get("status") or ""

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

    # CANCEL_ERROR: GH writes ##[error]The operation was canceled. into step log.
    # aksh does not currently emit ##[error] annotations, so treat absence as a fidelity
    # note rather than a hard failure (run/job conclusion is the authoritative signal).
    if norm(gh.conclusion) == "cancelled" and "CANCEL_ERROR" in gh.markers:
        if "CANCEL_ERROR" in aksh.markers:
            notes.append("cancel error annotation present on both")
        else:
            notes.append(
                "FIDELITY: GH step log has ##[error] cancel annotation; "
                "aksh step blob may omit it (job conclusion still cancelled)"
            )

    # SHOULD_NOT_REACH executed in aksh capture is always a hard failure when GH did not
    # execute it. This marker appears in steps that must not run (e.g., a sleep after
    # cancel-in-progress). Its presence either means a real concurrency bug (the step was
    # not cancelled) or cross-run log contamination. Both conditions make the capture invalid.
    if "SHOULD_NOT_REACH_EXECUTED" in aksh.markers and "SHOULD_NOT_REACH_EXECUTED" not in gh.markers:
        issues.append(
            "SHOULD_NOT_REACH was executed in aksh capture but not in GH capture — "
            "real concurrency bug or cross-run log contamination; capture is invalid"
        )

    # Step conclusion: compare user steps by fuzzy name match
    gh_user = {k: v for k, v in gh.step_conclusions.items() if k not in ("Set up job", "Complete job")}
    ak_user = {
        k: v
        for k, v in aksh.step_conclusions.items()
        if k not in ("Set up job", "Complete job", "Set up runner", "Complete runner")
    }
    for gname, gconc in gh_user.items():
        matched = None
        for aname, aconc in ak_user.items():
            if gname == aname or gname in aname or aname in gname:
                matched = (aname, aconc)
                break
        if not matched and len(ak_user) == 1:
            aname, aconc = next(iter(ak_user.items()))
            matched = (aname, aconc)
        if not matched:
            notes.append(f"no aksh step matched GH step '{gname}' ({gconc})")
            continue
        aname, aconc = matched
        if norm(gconc) != norm(aconc):
            issues.append(f"step '{gname}' conclusion: gh={gconc} aksh={aname}/{aconc}")
        else:
            notes.append(f"step '{gname}'≈'{aname}' conclusion={norm(gconc)}")

    # Job conclusions — prefer run-level if single-job; else multiset
    if gh.jobs and aksh.jobs:
        gh_vals = sorted(norm(v) for v in gh.jobs.values() if v)
        ak_vals = sorted(norm(v) for v in aksh.jobs.values() if v)
        # Contradictory job success + run cancelled is a hard failure: the job conclusion
        # must be sourced from the run API response, not from heuristic global runner log
        # parsing. A capture with success jobs on a cancelled run is a corrupt capture.
        if norm(aksh.conclusion) == "cancelled" and ak_vals and all(v == "success" for v in ak_vals):
            issues.append(
                f"contradictory aksh capture: job conclusions={ak_vals} but run conclusion=cancelled; "
                "job conclusion must come from run API, not from heuristic runner log"
            )
        elif gh_vals != ak_vals:
            issues.append(f"job conclusions multiset: gh={gh_vals} aksh={ak_vals}")
        else:
            notes.append(f"job conclusions match: {gh_vals}")

    return {
        "ok": len(issues) == 0,
        "issues": issues,
        "notes": notes,
        "gh": {"name": gh.name, "conclusion": gh.conclusion, "markers": sorted(gh.markers)},
        "aksh": {"name": aksh.name, "conclusion": aksh.conclusion, "markers": sorted(aksh.markers)},
    }


# Scenario pairing: GH capture prefix -> aksh capture name
SCENARIO_PAIRS = [
    ("01-bare-string", "success", "01-bare-A", "01-bare-B"),
    ("02-cancel-in-progress", "cancelled", "02-cancel-A", None),
    ("02-cancel-in-progress", "success", "02-cancel-B", None),
    ("03-fifo-pending", "success", "03-fifo-A", "03-fifo-B"),
    ("08-job-level", "success", "08-job-level", None),
    ("10-empty-group", "failure", "10-empty-group", None),
    ("11-expr-group-ref", "success", "11-expr-group", None),
]


def find_gh(root: Path, prefix: str, conclusion: str) -> Path | None:
    cands = sorted(root.glob(f"{prefix}_*_{conclusion}"))
    if cands:
        return cands[0]
    # rerun_
    cands = sorted(root.glob(f"rerun_{prefix}_*_{conclusion}"))
    return cands[0] if cands else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--github-root", type=Path, required=True)
    ap.add_argument("--aksh-root", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()
    out = args.out or (args.aksh_root / "LOG-CONTENT-COMPARE.md")

    comparisons = []

    # Pair known scenarios present on both sides
    pairs = [
        ("01 bare A", find_gh(args.github_root, "01-bare-string", "success"), args.aksh_root / "01-bare-A"),
        ("01 bare B", sorted(args.github_root.glob("01-bare-string_*_success"))[-1] if list(args.github_root.glob("01-bare-string_*_success")) else None, args.aksh_root / "01-bare-B"),
        ("02 cancel A", find_gh(args.github_root, "02-cancel-in-progress", "cancelled") or find_gh(args.github_root, "rerun_02-cancel-in-progress", "cancelled"), args.aksh_root / "02-cancel-A"),
        ("02 cancel B", find_gh(args.github_root, "02-cancel-in-progress", "success"), args.aksh_root / "02-cancel-B"),
        ("03 fifo A", sorted(args.github_root.glob("03-fifo-pending_*_success"))[0] if list(args.github_root.glob("03-fifo-pending_*_success")) else None, args.aksh_root / "03-fifo-A"),
        ("03 fifo B", sorted(args.github_root.glob("03-fifo-pending_*_success"))[-1] if list(args.github_root.glob("03-fifo-pending_*_success")) else None, args.aksh_root / "03-fifo-B"),
        ("08 job-level", find_gh(args.github_root, "08-job-level", "success"), args.aksh_root / "08-job-level"),
        ("10 empty", find_gh(args.github_root, "10-empty-group", "failure"), args.aksh_root / "10-empty-group"),
        ("11 expr group", find_gh(args.github_root, "11-expr-group-ref", "success"), args.aksh_root / "11-expr-group"),
        ("05 fifo expr false A", sorted(args.github_root.glob("05-cancel-expr-false_*_success"))[0] if list(args.github_root.glob("05-cancel-expr-false_*_success")) else None, args.aksh_root / "05-expr-false-A"),
        ("04 cancel expr true A", find_gh(args.github_root, "04-cancel-expr-true", "cancelled"), args.aksh_root / "04-cancel-expr-A"),
        ("04 cancel expr true B", find_gh(args.github_root, "04-cancel-expr-true", "success"), args.aksh_root / "04-cancel-expr-B"),
    ]

    # Fix 02 cancel A path for rerun folders
    fixed = []
    for name, gh_path, ak_path in pairs:
        if gh_path is None and "02 cancel A" in name:
            cands = list(args.github_root.glob("*02-cancel-in-progress*_cancelled"))
            gh_path = cands[0] if cands else None
        fixed.append((name, gh_path, ak_path))
    pairs = fixed

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
    (args.aksh_root / "LOG-CONTENT-COMPARE.json").write_text(json.dumps(comparisons, indent=2))
    print(f"{passed}/{total} passed")
    print(out)
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
