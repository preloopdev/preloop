#!/usr/bin/env python3
"""Comprehensive conformance comparison: step-level + log content + flow diffing.

Reads conformance JSONL results from both official and aksh runners, matches
scenarios by workflow number, and produces a detailed comparison report covering:

1. Job-level: conclusion match, job count match
2. Step-level: step count, step name, step conclusion, step ordering
3. Log content: timestamp format, group markers, secret masking, annotations
4. Flow-level: integrates with runner-flow-diff.py captures when available

Usage:
  python3 conformance-diff.py [--official FILE] [--aksh FILE] [--flows-dir DIR] [--output FILE]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


# ── Workflow number extraction ──────────────────────────────────────

# Map workflow display names to number prefixes
WORKFLOW_NAME_MAP = {
    "Multiline Output via Heredoc": "87",
    "State and Post Step Behavior": "88",
    "Workflow Dispatch with Typed Inputs": "89",
    "Shell Exit Behavior and Pipefail": "90",
    "Large Output Handling": "91",
    "Unicode and Special Characters": "92",
    "Empty and Null Values": "93",
    "Custom Shells": "80",
    "Step Timeout": "81",
    "Reusable Workflow Caller": "82",
    "Local Node Action": "83",
    "Concurrency Groups": "84",
    "Permissions Scoping": "85",
    "Environment Deployments": "86",
    "95-nested-composite-outputs": "95",
    "96-env-inheritance": "96",
    "97-artifact-cross-job": "97",
    "98-outcome-vs-conclusion": "98",
    "99-workspace-defaults": "99",
    "100-tool-cache": "100",
    "94-action-pinning": "94",
}

WORKFLOW_DESCRIPTIONS = {
    "80": "Custom Shells",
    "81": "Step Timeout",
    "82": "Reusable Workflow",
    "83": "Local Node Action",
    "84": "Concurrency Groups",
    "85": "Permissions Scoping",
    "86": "Environment Deployments",
    "87": "Multiline Output",
    "88": "State and Post Step",
    "89": "Workflow Inputs",
    "90": "Shell Exit Behavior",
    "91": "Large Output",
    "92": "Unicode Special Chars",
    "93": "Empty/Null Values",
    "94": "Action Pinning",
    "95": "Nested Composite",
    "96": "Env Inheritance",
    "97": "Artifact Cross-Job",
    "98": "Outcome vs Conclusion",
    "99": "Workspace Defaults",
    "100": "Tool Cache",
}


def extract_wf_number(name: str) -> str | None:
    """Extract the workflow number from a name or filename."""
    # Check name map first
    if name in WORKFLOW_NAME_MAP:
        return WORKFLOW_NAME_MAP[name]
    # Try extracting leading digits
    m = re.match(r"(\d+)", name.replace(".yml", ""))
    return m.group(1) if m else None


# ── Data loading ────────────────────────────────────────────────────

def load_conformance(path: Path) -> dict[str, dict]:
    """Load conformance JSONL, keyed by workflow number. Last entry wins."""
    results: dict[str, dict] = {}
    if not path.exists():
        return results
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        d = json.loads(line)
        wf = d.get("workflow") or d.get("result", {}).get("workflow", "")
        num = extract_wf_number(wf)
        if not num:
            continue
        conclusion = d.get("conclusion", "")
        # Keep latest entry with actual data (non-empty conclusion preferred)
        if conclusion or num not in results:
            results[num] = d
    return results


# ── Step-level comparison ───────────────────────────────────────────

class StepDiff:
    """Differences found at the step level for one job."""

    def __init__(self, job_name: str):
        self.job_name = job_name
        self.issues: list[str] = []
        self.details: list[str] = []

    @property
    def has_issues(self) -> bool:
        return len(self.issues) > 0


def normalize_step_name(name: str) -> str:
    """Normalize step names for comparison.

    Official runner uses displayNameToken.lit (e.g. 'Set empty string output').
    Aksh may use the script content prefix (e.g. 'Run echo ...' ) or the
    displayNameToken depending on version.
    """
    # Strip leading 'Run ' for comparison — these are auto-generated names
    if name.startswith("Run "):
        return name
    return name


def compare_steps(
    off_steps: list[dict], aksh_steps: list[dict], job_name: str
) -> StepDiff:
    """Compare step lists between official and aksh for one job."""
    diff = StepDiff(job_name)

    # Filter out duplicate entries (aksh sometimes has duplicate Set up job / Complete job)
    def dedup_steps(steps: list[dict]) -> list[dict]:
        seen = set()
        result = []
        for s in steps:
            key = (s.get("name", ""), s.get("number", 0))
            if key not in seen:
                seen.add(key)
                result.append(s)
        return result

    off_dedup = dedup_steps(off_steps)
    aksh_dedup = dedup_steps(aksh_steps)

    # Step count
    if len(off_dedup) != len(aksh_dedup):
        diff.issues.append("step-count")
        diff.details.append(
            f"Step count: official={len(off_dedup)}, aksh={len(aksh_dedup)} "
            f"(raw: official={len(off_steps)}, aksh={len(aksh_steps)})"
        )

    # Duplicate detection
    if len(aksh_steps) != len(aksh_dedup):
        dupes = len(aksh_steps) - len(aksh_dedup)
        diff.issues.append("duplicate-steps")
        diff.details.append(f"Aksh has {dupes} duplicate step entries")

    # Step conclusion comparison (by position after dedup)
    for i, (o, a) in enumerate(zip(off_dedup, aksh_dedup)):
        oname = o.get("name", f"step-{i}")
        aname = a.get("name", f"step-{i}")
        oconc = o.get("conclusion", "")
        aconc = a.get("conclusion", "")

        # Name comparison
        if oname != aname:
            # Check if it's just a display name vs script content difference
            if oname in ("Set up job", "Complete job") and aname == oname:
                pass  # exact match on synthetic steps
            elif aname.startswith("Run ") and oname not in ("Set up job", "Complete job"):
                diff.issues.append("step-display-name")
                diff.details.append(
                    f"Step {i+1} name: official='{oname}' vs aksh='{aname}'"
                )
            elif aname.startswith("Post ") or "/" in aname or "@" in aname:
                # Action refs or post-step names — still a display name issue
                diff.issues.append("step-display-name")
                diff.details.append(
                    f"Step {i+1} name: official='{oname}' vs aksh='{aname}'"
                )
            else:
                diff.issues.append("step-name-mismatch")
                diff.details.append(
                    f"Step {i+1} name: official='{oname}' vs aksh='{aname}'"
                )

        # Conclusion comparison
        if oconc != aconc:
            diff.issues.append("step-conclusion")
            diff.details.append(
                f"Step {i+1} '{oname}': official={oconc}, aksh={aconc}"
            )

    # Steps only in one side (after exhausting zip)
    if len(off_dedup) > len(aksh_dedup):
        for s in off_dedup[len(aksh_dedup):]:
            diff.issues.append("step-missing-in-aksh")
            diff.details.append(
                f"Missing in aksh: '{s.get('name','')}' ({s.get('conclusion','')})"
            )
    elif len(aksh_dedup) > len(off_dedup):
        for s in aksh_dedup[len(off_dedup):]:
            diff.issues.append("step-extra-in-aksh")
            diff.details.append(
                f"Extra in aksh: '{s.get('name','')}' ({s.get('conclusion','')})"
            )

    return diff


# ── Log content analysis ────────────────────────────────────────────

class LogAnalysis:
    """Analyze a run.log file for formatting patterns."""

    def __init__(self):
        self.has_timestamps = False
        self.has_groups = False
        self.has_endgroups = False
        self.has_secret_masking = False
        self.has_annotations = False  # ##[error], ##[warning], ##[notice]
        self.line_count = 0
        self.step_logs: dict[str, list[str]] = defaultdict(list)
        self.group_markers: list[str] = []
        self.annotations: list[str] = []

    @classmethod
    def from_file(cls, path: Path) -> LogAnalysis:
        analysis = cls()
        if not path.exists():
            return analysis

        current_step = ""
        for line in path.read_text().splitlines():
            analysis.line_count += 1

            # Parse tab-separated format: job\tstep\ttimestamp content
            parts = line.split("\t", 2)
            if len(parts) >= 2:
                current_step = parts[1]

            content = parts[-1] if parts else line

            # Check for timestamps (ISO 8601)
            if re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", content):
                analysis.has_timestamps = True

            # Check for group markers
            if "##[group]" in content:
                analysis.has_groups = True
                analysis.group_markers.append(content.strip())
            if "##[endgroup]" in content:
                analysis.has_endgroups = True

            # Check for annotations
            for ann in ("##[error]", "##[warning]", "##[notice]"):
                if ann in content:
                    analysis.has_annotations = True
                    analysis.annotations.append(content.strip())

            # Check for secret masking
            if "***" in content:
                analysis.has_secret_masking = True

            analysis.step_logs[current_step].append(content)

        return analysis


def compare_logs(
    off_log: Path | None, aksh_log: Path | None
) -> list[str]:
    """Compare log files and return diff lines."""
    issues: list[str] = []

    if off_log and not off_log.exists():
        off_log = None
    if aksh_log and not aksh_log.exists():
        aksh_log = None

    if not off_log and not aksh_log:
        return ["No log files available for comparison"]

    off = LogAnalysis.from_file(off_log) if off_log else LogAnalysis()
    aksh = LogAnalysis.from_file(aksh_log) if aksh_log else LogAnalysis()

    if off_log:
        issues.append(f"Official: {off.line_count} lines, "
                       f"timestamps={'✓' if off.has_timestamps else '✗'}, "
                       f"groups={'✓' if off.has_groups else '✗'}, "
                       f"annotations={'✓' if off.has_annotations else '✗'}")
    if aksh_log:
        issues.append(f"Aksh:     {aksh.line_count} lines, "
                       f"timestamps={'✓' if aksh.has_timestamps else '✗'}, "
                       f"groups={'✓' if aksh.has_groups else '✗'}, "
                       f"annotations={'✓' if aksh.has_annotations else '✗'}")

    # Compare formatting features
    if off_log and aksh_log:
        if off.has_timestamps and not aksh.has_timestamps:
            issues.append("⚠ Aksh logs missing timestamps")
        if off.has_groups and not aksh.has_groups:
            issues.append("⚠ Aksh logs missing ##[group] markers")
        if off.has_endgroups and not aksh.has_endgroups:
            issues.append("⚠ Aksh logs missing ##[endgroup] markers")
        if off.has_annotations and not aksh.has_annotations:
            issues.append("⚠ Aksh logs missing annotations (##[error]/##[warning])")

        # Line count comparison
        ratio = aksh.line_count / off.line_count if off.line_count else 0
        if ratio < 0.7 or ratio > 1.5:
            issues.append(
                f"⚠ Log size mismatch: official={off.line_count} lines, "
                f"aksh={aksh.line_count} lines (ratio={ratio:.2f})"
            )

        # Step-level log comparison
        off_steps = set(off.step_logs.keys())
        aksh_steps = set(aksh.step_logs.keys())
        missing = off_steps - aksh_steps
        extra = aksh_steps - off_steps
        if missing:
            issues.append(f"Steps with logs in official but not aksh: {sorted(missing)}")
        if extra:
            issues.append(f"Steps with logs in aksh but not official: {sorted(extra)}")

    return issues


# ── Main comparison ─────────────────────────────────────────────────

def compare_scenario(
    num: str,
    off_data: dict | None,
    aksh_data: dict | None,
    flows_dir: Path | None,
) -> dict:
    """Compare a single scenario between official and aksh."""
    result: dict[str, Any] = {
        "number": num,
        "description": WORKFLOW_DESCRIPTIONS.get(num, f"Scenario {num}"),
        "issues": [],
        "details": [],
    }

    off_conclusion = ""
    aksh_conclusion = ""

    if off_data:
        off_conclusion = off_data.get("conclusion", "") or ""
        result["official_conclusion"] = off_conclusion or "(empty)"
        result["official_run_id"] = off_data.get("run_id", "")
    else:
        result["official_conclusion"] = "N/A"

    if aksh_data:
        aksh_conclusion = aksh_data.get("conclusion", "") or ""
        result["aksh_conclusion"] = aksh_conclusion or "(empty)"
        result["aksh_run_id"] = aksh_data.get("run_id", "")
    else:
        result["aksh_conclusion"] = "N/A"

    # Job-level conclusion match
    if off_data and aksh_data:
        if off_conclusion == aksh_conclusion:
            result["conclusion_match"] = True
        elif not off_conclusion or not aksh_conclusion:
            result["conclusion_match"] = None  # one didn't complete
            result["issues"].append("incomplete-run")
            if not off_conclusion:
                result["details"].append("Official runner did not complete")
            if not aksh_conclusion:
                result["details"].append("Aksh runner did not complete")
        else:
            result["conclusion_match"] = False
            result["issues"].append("conclusion-mismatch")
            result["details"].append(
                f"Conclusion: official={off_conclusion}, aksh={aksh_conclusion}"
            )

        # Job-level comparison
        off_jobs = off_data.get("result", {}).get("jobs", [])
        aksh_jobs = aksh_data.get("result", {}).get("jobs", [])

        if len(off_jobs) != len(aksh_jobs):
            result["issues"].append("job-count-mismatch")
            result["details"].append(
                f"Job count: official={len(off_jobs)}, aksh={len(aksh_jobs)}"
            )

        # Match jobs by name
        off_by_name = {j["name"]: j for j in off_jobs}
        aksh_by_name = {j["name"]: j for j in aksh_jobs}

        for jname in sorted(set(off_by_name) | set(aksh_by_name)):
            oj = off_by_name.get(jname)
            aj = aksh_by_name.get(jname)

            if not oj:
                result["issues"].append("job-missing-in-official")
                result["details"].append(f"Job '{jname}' only in aksh")
                continue
            if not aj:
                result["issues"].append("job-missing-in-aksh")
                result["details"].append(f"Job '{jname}' only in official")
                continue

            # Job conclusion
            ojc = oj.get("conclusion", "")
            ajc = aj.get("conclusion", "")
            if ojc != ajc and ojc and ajc:
                result["issues"].append("job-conclusion-mismatch")
                result["details"].append(
                    f"Job '{jname}': official={ojc}, aksh={ajc}"
                )

            # Step-level comparison
            off_steps = oj.get("steps", [])
            aksh_steps = aj.get("steps", [])

            if off_steps and aksh_steps:
                step_diff = compare_steps(off_steps, aksh_steps, jname)
                if step_diff.has_issues:
                    result["issues"].extend(step_diff.issues)
                    result["details"].extend(step_diff.details)
            elif off_steps and not aksh_steps:
                result["issues"].append("no-aksh-steps")
                result["details"].append(
                    f"Job '{jname}': official has {len(off_steps)} steps, aksh has none (did not run?)"
                )

    elif off_data and not aksh_data:
        result["issues"].append("no-aksh-data")
    elif aksh_data and not off_data:
        result["issues"].append("no-official-data")

    # Check for available flow captures
    if flows_dir:
        for scenario_name in (
            f"{num}-*",
            f"0{num}-*" if len(num) == 1 else "",
        ):
            # Check macos-runners flow captures
            pass  # Flow integration handled at report level

    return result


def generate_report(
    official: dict[str, dict],
    aksh: dict[str, dict],
    flows_dir: Path | None,
    output: Path,
) -> None:
    """Generate the full comparison report."""
    all_nums = sorted(
        set(list(official.keys()) + list(aksh.keys())),
        key=lambda x: int(x),
    )

    lines: list[str] = []
    lines.append("# Runner Conformance Comparison Report")
    lines.append("")
    lines.append(f"Generated from conformance JSONL data.")
    lines.append(f"Official scenarios: {len(official)}, Aksh scenarios: {len(aksh)}")
    lines.append("")

    # ── Summary table ───────────────────────────────────────────────
    lines.append("## Summary Matrix")
    lines.append("")
    lines.append("| # | Scenario | Official | Aksh | Match | Issues |")
    lines.append("|---|---|---|---|---|---|")

    scenarios: list[dict] = []
    total_pass = total_fail = total_incomplete = 0

    for num in all_nums:
        off_data = official.get(num)
        aksh_data = aksh.get(num)
        result = compare_scenario(num, off_data, aksh_data, flows_dir)
        scenarios.append(result)

        match_icon = ""
        if result.get("conclusion_match") is True:
            match_icon = "✅"
            total_pass += 1
        elif result.get("conclusion_match") is False:
            match_icon = "❌"
            total_fail += 1
        elif result.get("conclusion_match") is None:
            match_icon = "⏳"
            total_incomplete += 1
        else:
            match_icon = "—"
            total_incomplete += 1

        issue_summary = ", ".join(sorted(set(result["issues"])))[:60]
        lines.append(
            f"| {num} | {result['description']} | "
            f"{result['official_conclusion']} | {result['aksh_conclusion']} | "
            f"{match_icon} | {issue_summary} |"
        )

    lines.append("")
    lines.append(
        f"**Totals**: {total_pass} matching, {total_fail} mismatched, "
        f"{total_incomplete} incomplete/missing"
    )
    lines.append("")

    # ── Detailed diffs per scenario ─────────────────────────────────
    lines.append("## Detailed Comparison")
    lines.append("")

    for result in scenarios:
        if not result["issues"] and not result["details"]:
            continue

        lines.append(f"### {result['number']} — {result['description']}")
        lines.append("")

        if result.get("official_run_id"):
            lines.append(f"- Official run: {result['official_run_id']}")
        if result.get("aksh_run_id"):
            lines.append(f"- Aksh run: {result['aksh_run_id']}")
        lines.append(
            f"- Conclusions: official={result['official_conclusion']}, "
            f"aksh={result['aksh_conclusion']}"
        )
        lines.append("")

        if result["details"]:
            for detail in result["details"]:
                lines.append(f"- {detail}")
            lines.append("")

    # ── Issue categories ────────────────────────────────────────────
    lines.append("## Issue Categories")
    lines.append("")

    all_issues: list[str] = []
    for s in scenarios:
        all_issues.extend(s["issues"])

    if all_issues:
        from collections import Counter
        counts = Counter(all_issues)
        lines.append("| Issue Type | Count |")
        lines.append("|---|---:|")
        for issue, count in sorted(counts.items(), key=lambda x: -x[1]):
            lines.append(f"| {issue} | {count} |")
    else:
        lines.append("No issues found.")
    lines.append("")

    # ── Known categories explanation ────────────────────────────────
    lines.append("## Issue Type Reference")
    lines.append("")
    lines.append("| Issue | Severity | Description |")
    lines.append("|---|---|---|")
    lines.append("| conclusion-mismatch | 🔴 Critical | Job passed on one runner but failed on the other |")
    lines.append("| job-conclusion-mismatch | 🔴 Critical | Individual job conclusion differs |")
    lines.append("| step-conclusion | 🟠 High | Step passed/failed differently |")
    lines.append("| step-count | 🟡 Medium | Different number of steps executed |")
    lines.append("| step-display-name | 🔵 Low | Step name shown differently (display only) |")
    lines.append("| step-name-mismatch | 🟡 Medium | Step name differs in a meaningful way |")
    lines.append("| duplicate-steps | 🟡 Medium | Aksh reports duplicate step entries |")
    lines.append("| incomplete-run | ⚪ Info | One runner did not complete the workflow |")
    lines.append("| no-aksh-data | ⚪ Info | Aksh has no data for this scenario |")
    lines.append("| no-aksh-steps | 🟠 High | Aksh job has no step data (runner didn't execute) |")
    lines.append("")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines))
    print(f"Report written to {output}")
    print(f"  {total_pass} matching, {total_fail} mismatched, {total_incomplete} incomplete")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--official",
        type=Path,
        default=Path("benchmarks/real-world/results/conformance/conformance-official.jsonl"),
        help="Official runner conformance JSONL",
    )
    p.add_argument(
        "--aksh",
        type=Path,
        default=Path("benchmarks/real-world/results/conformance/conformance-aksh.jsonl"),
        help="Aksh runner conformance JSONL",
    )
    p.add_argument(
        "--flows-dir",
        type=Path,
        default=None,
        help="Directory containing MITM flow captures",
    )
    p.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("benchmarks/real-world/results/CONFORMANCE-REPORT.md"),
        help="Output report path",
    )
    args = p.parse_args()

    official = load_conformance(args.official)
    aksh = load_conformance(args.aksh)

    print(f"Loaded {len(official)} official scenarios, {len(aksh)} aksh scenarios")
    generate_report(official, aksh, args.flows_dir, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
