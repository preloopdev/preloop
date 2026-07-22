#!/usr/bin/env python3
"""Compare log content between official and aksh runner captures.

Reads run.log files or step log uploads from MITM captures and produces a
detailed comparison of formatting, content, and structure.

Usage:
  # Compare two run.log files directly
  python3 log-content-diff.py --official path/to/official/run.log --aksh path/to/aksh/run.log

  # Compare from MITM flow captures (extracts log uploads from flows.jsonl)
  python3 log-content-diff.py --official-flows path/to/official/latest --aksh-flows path/to/aksh/latest

  # Batch compare all scenarios with flow captures
  python3 log-content-diff.py --batch --flows-root benchmarks/compatibility/runner/protocol
"""
from __future__ import annotations

import argparse
import base64
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


# Log line parsing 

TIMESTAMP_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z?)")
GROUP_RE = re.compile(r"##\[group\](.*)")
ENDGROUP_RE = re.compile(r"##\[endgroup\]")
ANNOTATION_RE = re.compile(r"##\[(error|warning|notice)\](.*)")
COMMAND_RE = re.compile(r"##\[([a-z_]+)\s*(.*?)\](.*)")
SECRET_MASK_RE = re.compile(r"\*{3}")


class LogLine:
    """Parsed log line with metadata."""

    def __init__(self, raw: str, job: str = "", step: str = ""):
        self.raw = raw
        self.job = job
        self.step = step
        self.timestamp: str | None = None
        self.content = raw
        self.is_group = False
        self.is_endgroup = False
        self.annotation_type: str | None = None
        self.annotation_msg: str = ""
        self.command: str | None = None
        self.has_secret_mask = False

        # Parse tab-separated format: job\tstep\tcontent
        parts = raw.split("\t", 2)
        if len(parts) >= 3 and not TIMESTAMP_RE.match(parts[0]):
            self.job = parts[0]
            self.step = parts[1]
            self.content = parts[2]
        elif len(parts) == 2 and not TIMESTAMP_RE.match(parts[0]):
            self.step = parts[0]
            self.content = parts[1]

        # Extract timestamp
        m = TIMESTAMP_RE.search(self.content)
        if m:
            self.timestamp = m.group(1)

        # Check for markers
        if GROUP_RE.search(self.content):
            self.is_group = True
        if ENDGROUP_RE.search(self.content):
            self.is_endgroup = True

        m = ANNOTATION_RE.search(self.content)
        if m:
            self.annotation_type = m.group(1)
            self.annotation_msg = m.group(2).strip()

        m = COMMAND_RE.search(self.content)
        if m:
            self.command = m.group(1)

        if SECRET_MASK_RE.search(self.content):
            self.has_secret_mask = True


class StepLog:
    """Aggregated log data for one step."""

    def __init__(self, name: str):
        self.name = name
        self.lines: list[LogLine] = []

    @property
    def line_count(self) -> int:
        return len(self.lines)

    @property
    def has_timestamps(self) -> bool:
        return any(l.timestamp for l in self.lines)

    @property
    def has_groups(self) -> bool:
        return any(l.is_group for l in self.lines)

    @property
    def has_endgroups(self) -> bool:
        return any(l.is_endgroup for l in self.lines)

    @property
    def annotations(self) -> list[LogLine]:
        return [l for l in self.lines if l.annotation_type]

    @property
    def commands(self) -> list[str]:
        return [l.command for l in self.lines if l.command]

    @property
    def content_lines(self) -> list[str]:
        """Non-metadata content lines."""
        return [
            l.content for l in self.lines
            if not l.is_group and not l.is_endgroup and not l.command
        ]


class RunLog:
    """Complete parsed run log."""

    def __init__(self, path: Path | None = None, text: str = ""):
        self.path = path
        self.lines: list[LogLine] = []
        self.steps: dict[str, StepLog] = {}
        self._step_order: list[str] = []

        content = text or (path.read_text() if path and path.exists() else "")
        current_step = ""

        for raw_line in content.splitlines():
            ll = LogLine(raw_line)
            self.lines.append(ll)

            step_name = ll.step or current_step
            if step_name != current_step:
                current_step = step_name
                if step_name not in self.steps:
                    self.steps[step_name] = StepLog(step_name)
                    self._step_order.append(step_name)

            if current_step and current_step in self.steps:
                self.steps[current_step].lines.append(ll)

    @property
    def line_count(self) -> int:
        return len(self.lines)

    @property
    def step_names(self) -> list[str]:
        return list(self._step_order)

    @property
    def has_timestamps(self) -> bool:
        return any(l.timestamp for l in self.lines)

    @property
    def has_groups(self) -> bool:
        return any(l.is_group for l in self.lines)

    @property
    def has_endgroups(self) -> bool:
        return any(l.is_endgroup for l in self.lines)

    @property
    def has_annotations(self) -> bool:
        return any(l.annotation_type for l in self.lines)

    @property
    def has_secret_masking(self) -> bool:
        return any(l.has_secret_mask for l in self.lines)

    @property
    def all_annotations(self) -> list[LogLine]:
        return [l for l in self.lines if l.annotation_type]

    @property
    def all_commands(self) -> Counter:
        return Counter(l.command for l in self.lines if l.command)


# ── Comparison engine ───────────────────────────────────────────────

class LogDiff:
    """Result of comparing two run logs."""

    def __init__(self, official: RunLog, aksh: RunLog, scenario: str = ""):
        self.official = official
        self.aksh = aksh
        self.scenario = scenario
        self.issues: list[tuple[str, str]] = []  # (severity, message)

    def run(self) -> "LogDiff":
        """Execute all comparisons."""
        self._compare_structure()
        self._compare_formatting()
        self._compare_step_content()
        self._compare_annotations()
        return self

    def _compare_structure(self) -> None:
        """Compare overall structure."""
        o, a = self.official, self.aksh

        # Line counts
        if o.line_count and a.line_count:
            ratio = a.line_count / o.line_count
            if ratio < 0.6:
                self.issues.append(("high",
                    f"Aksh log significantly smaller: {a.line_count} vs {o.line_count} lines "
                    f"(ratio={ratio:.2f})"))
            elif ratio > 1.8:
                self.issues.append(("medium",
                    f"Aksh log significantly larger: {a.line_count} vs {o.line_count} lines "
                    f"(ratio={ratio:.2f})"))

        # Step count
        if len(o.step_names) != len(a.step_names):
            self.issues.append(("medium",
                f"Step count: official={len(o.step_names)}, aksh={len(a.step_names)}"))

        # Steps only in one side
        off_steps = set(o.step_names)
        aksh_steps = set(a.step_names)
        for s in sorted(off_steps - aksh_steps):
            self.issues.append(("medium", f"Step only in official: '{s}'"))
        for s in sorted(aksh_steps - off_steps):
            self.issues.append(("low", f"Step only in aksh: '{s}'"))

    def _compare_formatting(self) -> None:
        """Compare log formatting features."""
        o, a = self.official, self.aksh

        if o.has_timestamps and not a.has_timestamps:
            self.issues.append(("high", "Aksh logs missing timestamps"))
        if o.has_groups and not a.has_groups:
            self.issues.append(("high", "Aksh logs missing ##[group] markers"))
        if o.has_endgroups and not a.has_endgroups:
            self.issues.append(("high", "Aksh logs missing ##[endgroup] markers"))
        if o.has_secret_masking and not a.has_secret_masking:
            self.issues.append(("high", "Aksh logs missing secret masking (***) "))
        if o.has_annotations and not a.has_annotations:
            self.issues.append(("medium", "Aksh logs missing annotations"))

        # Compare command usage
        o_cmds = o.all_commands
        a_cmds = a.all_commands
        for cmd in sorted(set(o_cmds) | set(a_cmds)):
            oc = o_cmds.get(cmd, 0)
            ac = a_cmds.get(cmd, 0)
            if oc > 0 and ac == 0:
                self.issues.append(("medium",
                    f"Command ##[{cmd}] used {oc}x in official, 0x in aksh"))

    def _compare_step_content(self) -> None:
        """Compare per-step log content."""
        o, a = self.official, self.aksh

        for step_name in o.step_names:
            if step_name not in a.steps:
                continue

            os = o.steps[step_name]
            az = a.steps[step_name]

            # Check if "Set up job" has expected content
            if step_name == "Set up job":
                o_content = "\n".join(os.content_lines)
                a_content = "\n".join(az.content_lines)

                # Official "Set up job" includes: runner version, runner name,
                # machine name, secret source, etc.
                for marker in [
                    "Current runner version",
                    "Runner name",
                    "Machine name",
                    "Prepare workflow directory",
                ]:
                    if marker in o_content and marker not in a_content:
                        self.issues.append(("low",
                            f"'Set up job' missing '{marker}' line in aksh"))

            # Check if "Complete job" content matches
            if step_name == "Complete job":
                o_content = "\n".join(os.content_lines)
                a_content = "\n".join(az.content_lines)
                if "Cleaning up orphan processes" in o_content and "Cleaning up" not in a_content:
                    self.issues.append(("low",
                        "'Complete job' missing cleanup message in aksh"))

            # Per-step line count comparison
            if os.line_count > 0 and az.line_count > 0:
                ratio = az.line_count / os.line_count
                if ratio < 0.3 and os.line_count > 3:
                    self.issues.append(("medium",
                        f"Step '{step_name}': aksh has {az.line_count} lines vs "
                        f"official {os.line_count} (ratio={ratio:.2f})"))

    def _compare_annotations(self) -> None:
        """Compare error/warning/notice annotations."""
        o_ann = self.official.all_annotations
        a_ann = self.aksh.all_annotations

        o_types = Counter(a.annotation_type for a in o_ann)
        a_types = Counter(a.annotation_type for a in a_ann)

        for atype in ("error", "warning", "notice"):
            oc = o_types.get(atype, 0)
            ac = a_types.get(atype, 0)
            if oc != ac:
                self.issues.append(("medium",
                    f"##[{atype}] count: official={oc}, aksh={ac}"))

    def to_markdown(self) -> str:
        """Generate markdown report."""
        lines = []
        lines.append(f"### Log Content Comparison{f': {self.scenario}' if self.scenario else ''}")
        lines.append("")
        lines.append(f"| Metric | Official | Aksh |")
        lines.append(f"|---|---|---|")
        lines.append(f"| Lines | {self.official.line_count} | {self.aksh.line_count} |")
        lines.append(f"| Steps | {len(self.official.step_names)} | {len(self.aksh.step_names)} |")
        lines.append(f"| Timestamps | {'✓' if self.official.has_timestamps else '✗'} | {'✓' if self.aksh.has_timestamps else '✗'} |")
        lines.append(f"| Groups | {'✓' if self.official.has_groups else '✗'} | {'✓' if self.aksh.has_groups else '✗'} |")
        lines.append(f"| Annotations | {'✓' if self.official.has_annotations else '✗'} | {'✓' if self.aksh.has_annotations else '✗'} |")
        lines.append(f"| Secret masking | {'✓' if self.official.has_secret_masking else '✗'} | {'✓' if self.aksh.has_secret_masking else '✗'} |")
        lines.append("")

        if self.issues:
            lines.append("**Issues:**")
            lines.append("")
            for sev, msg in self.issues:
                icon = {"high": "🔴", "medium": "🟡", "low": "🔵"}.get(sev, "⚪")
                lines.append(f"- {icon} {msg}")
            lines.append("")
        else:
            lines.append("✅ No log content issues found.")
            lines.append("")

        return "\n".join(lines)


# ── Flow capture log extraction ─────────────────────────────────────

def extract_logs_from_flows(flows_dir: Path) -> str | None:
    """Extract concatenated log content from MITM flow captures.

    Looks for run.log first, then tries to extract from PUT blob uploads
    in the flows.jsonl.
    """
    # Direct run.log file
    run_log = flows_dir / "run.log"
    if run_log.exists() and run_log.stat().st_size > 0:
        return run_log.read_text()

    # Try extracting from flows
    flows_file = flows_dir / "flows.jsonl"
    if not flows_file.exists():
        # Check vm-mitm subdirectory
        flows_file = flows_dir / "vm-mitm" / "flows.jsonl"
    if not flows_file.exists():
        return None

    log_parts = []
    for line in flows_file.read_text().splitlines():
        if not line.strip():
            continue
        try:
            flow = json.loads(line)
        except json.JSONDecodeError:
            continue

        path = flow.get("path", "")
        method = flow.get("method", "")

        # Look for step log uploads (PUT to blob storage)
        if method == "PUT" and "step-logs" in path:
            body = flow.get("request_body", "")
            if body:
                try:
                    decoded = base64.b64decode(body).decode("utf-8", errors="replace")
                    log_parts.append(decoded)
                except Exception:
                    pass

        # Look for job log uploads
        if method == "PUT" and "job-logs" in path:
            body = flow.get("request_body", "")
            if body:
                try:
                    decoded = base64.b64decode(body).decode("utf-8", errors="replace")
                    log_parts.append(decoded)
                except Exception:
                    pass

    return "\n".join(log_parts) if log_parts else None


# ── Batch comparison ────────────────────────────────────────────────

def batch_compare(flows_root: Path, output: Path) -> None:
    """Compare all scenarios found under flows_root."""
    lines = ["# Log Content Comparison Report", ""]

    scenarios = sorted(
        [d for d in flows_root.iterdir() if d.is_dir()],
        key=lambda d: d.name,
    )

    total_issues = 0
    scenario_count = 0

    for scenario_dir in scenarios:
        off_dir = scenario_dir / "official" / "latest"
        aksh_dir = scenario_dir / "aksh" / "latest"

        if not off_dir.exists() and not aksh_dir.exists():
            continue

        scenario_count += 1

        # Try run.log files first
        off_log = off_dir / "run.log" if off_dir.exists() else None
        aksh_log = aksh_dir / "run.log" if aksh_dir.exists() else None

        if off_log and off_log.exists() and aksh_log and aksh_log.exists():
            off_run = RunLog(off_log)
            aksh_run = RunLog(aksh_log)
            diff = LogDiff(off_run, aksh_run, scenario_dir.name).run()
            total_issues += len(diff.issues)
            lines.append(diff.to_markdown())

    lines.append("---")
    lines.append(f"**Total**: {scenario_count} scenarios compared, {total_issues} issues found")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines))
    print(f"Report: {output}")
    print(f"  {scenario_count} scenarios, {total_issues} issues")


# ── CLI ─────────────────────────────────────────────────────────────

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--official", type=Path, help="Official run.log file")
    p.add_argument("--aksh", type=Path, help="Aksh run.log file")
    p.add_argument("--official-flows", type=Path, help="Official MITM capture directory")
    p.add_argument("--aksh-flows", type=Path, help="Aksh MITM capture directory")
    p.add_argument("--batch", action="store_true", help="Batch compare all scenarios")
    p.add_argument("--flows-root", type=Path,
                   default=Path("benchmarks/compatibility/runner/protocol"),
                   help="Root directory for flow captures")
    p.add_argument("-o", "--output", type=Path,
                   default=Path("benchmarks/compatibility/runner/protocol/LOG-CONTENT-REPORT.md"))
    args = p.parse_args()

    if args.batch:
        batch_compare(args.flows_root, args.output)
        return 0

    # Single comparison mode
    if args.official and args.aksh:
        off_run = RunLog(args.official)
        aksh_run = RunLog(args.aksh)
    elif args.official_flows and args.aksh_flows:
        off_text = extract_logs_from_flows(args.official_flows)
        aksh_text = extract_logs_from_flows(args.aksh_flows)
        if not off_text:
            print(f"No log content found in {args.official_flows}", file=sys.stderr)
            return 1
        if not aksh_text:
            print(f"No log content found in {args.aksh_flows}", file=sys.stderr)
            return 1
        off_run = RunLog(text=off_text)
        aksh_run = RunLog(text=aksh_text)
    else:
        p.error("Provide --official/--aksh or --official-flows/--aksh-flows or --batch")
        return 1

    diff = LogDiff(off_run, aksh_run).run()
    print(diff.to_markdown())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
