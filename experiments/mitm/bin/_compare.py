#!/usr/bin/env python3
"""Compare two MITM captures and generate a markdown report."""

import difflib
import json
import re
import statistics
import sys
from pathlib import Path
from typing import Any


def normalize_path(path: str) -> str:
    """Normalize volatile parts of a URL path + query for comparison."""
    # Strip '/runner/server' prefix that runner.server prepends to its routes.
    path = re.sub(r"^/runner/server(?=/)", "", path)

    # Strip aksh's '/runner/' prefix for broker endpoints so they match the
    # official broker.actions.githubusercontent.com paths.
    path = re.sub(r"^/runner/(session|message|acknowledge)", r"/\1", path)

    # Normalize aksh's /broker/{n}/ run-service prefix to the official
    # /{n}/ form (run-actions-*.actions.githubusercontent.com/{n}/...).
    path = re.sub(r"^/broker/(\d+)/", r"/\1/", path)

    # Strip a single-segment random base path prefix before /_apis/
    # (e.g. /abc123/_apis/... → /_apis/..., or /my-org/_apis/... → /_apis/...).
    # Must be a single alphanumeric/hyphen segment (never multi-segment like /runner/server/).
    path = re.sub(r"^/([a-zA-Z0-9-]+)/_apis/", "/_apis/", path, count=1)
    # Replace GUIDs.
    path = re.sub(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "{guid}", path)

    if "?" in path:
        base, qs = path.split("?", 1)
    else:
        base, qs = path, ""

    parts = []
    for p in base.split("/"):
        if p.isdigit():
            parts.append("{n}")
        else:
            parts.append(p)
    base = "/".join(parts)

    if qs:
        params = []
        for part in qs.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                if re.match(r"^\d+$", v):
                    v = "{n}"
                elif re.match(r"^[0-9a-fA-F]{8}-", v):
                    v = "{guid}"
                params.append((k, v))
            else:
                params.append((part, ""))
        qs = "&".join(f"{k}={v}" for k, v in params)

    return f"{base}{'?' + qs if qs else ''}"


def redact_report(s: str) -> str:
    s = re.sub(r"eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}", "***REDACTED***", s)
    s = re.sub(r"[A-Za-z0-9_]{30,}", lambda m: "***REDACTED***" if len(set(m.group())) > 6 else m.group(), s)
    return s


def load_flows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _short_label(label: str) -> str:
    """Derive a short table-column abbreviation from a backend label."""
    parts = label.lower().replace("-", " ").replace("_", " ").split()
    if len(parts) == 1:
        return parts[0][:4]
    return "".join(p[0] for p in parts)


def json_diff(ja: Any, jb: Any, left_label: str = "left", right_label: str = "right") -> str:
    a_lines = json.dumps(ja, indent=2, sort_keys=True, ensure_ascii=False).splitlines(keepends=True)
    b_lines = json.dumps(jb, indent=2, sort_keys=True, ensure_ascii=False).splitlines(keepends=True)
    return "".join(difflib.unified_diff(a_lines, b_lines, fromfile=left_label, tofile=right_label))


def header_keys(flows: list[dict]) -> set[str]:
    ignored = {"date", "server", "content-length", "x-request-id", "x-vss-e2eid", "x-msedge-ref"}
    keys: set[str] = set()
    for f in flows:
        for pair in f.get("request_headers", []):
            if len(pair) == 2 and pair[0].lower() not in ignored:
                keys.add(pair[0].lower())
        for pair in f.get("response_headers", []):
            if len(pair) == 2 and pair[0].lower() not in ignored:
                keys.add(pair[0].lower())
    return keys


def render_report(
    scenario_name: str,
    left_dir: Path,
    right_dir: Path,
    output_path: Path,
    left_label: str = "official",
    right_label: str = "runner-server",
):
    left_flows = load_flows(left_dir / "flows.jsonl")
    right_flows = load_flows(right_dir / "flows.jsonl")

    # Guard: fail if one capture is empty while the other has data.
    if left_flows and not right_flows:
        print(
            f"ERROR: {left_label} has {len(left_flows)} flows but {right_label} has none — "
            "cannot compare. Record the {right_label} capture first.",
            file=sys.stderr,
        )
        sys.exit(5)
    if right_flows and not left_flows:
        print(
            f"ERROR: {right_label} has {len(right_flows)} flows but {left_label} has none — "
            "cannot compare. Record the {left_label} capture first.",
            file=sys.stderr,
        )
        sys.exit(5)

    left_summary = {}
    right_summary = {}
    if (left_dir / "summary.json").exists():
        left_summary = json.loads((left_dir / "summary.json").read_text())
    if (right_dir / "summary.json").exists():
        right_summary = json.loads((right_dir / "summary.json").read_text())

    def group(flows: list[dict]) -> dict[str, list[dict]]:
        g: dict[str, list[dict]] = {}
        for f in flows:
            key = f"{f.get('method', '?')} {normalize_path(f.get('path', '/'))}"
            g.setdefault(key, []).append(f)
        return g

    l_groups = group(left_flows)
    r_groups = group(right_flows)

    all_keys = sorted(set(l_groups) | set(r_groups))
    left_only = sorted(set(l_groups) - set(r_groups))
    right_only = sorted(set(r_groups) - set(l_groups))
    shared = sorted(set(l_groups) & set(r_groups))

    ls = _short_label(left_label)
    rs = _short_label(right_label)

    lines: list[str] = []
    lines.append(f"# MITM comparison: {scenario_name}")
    lines.append("")
    lines.append(f"**{left_label}**: {left_summary.get('status', 'N/A')} — {len(left_flows)} flows")
    lines.append(f"**{right_label}**: {right_summary.get('status', 'N/A')} — {len(right_flows)} flows")
    lines.append("")

    # Endpoint matrix.
    lines.append("## Endpoint matrix")
    lines.append("")
    header = f"| method | normalized path | {ls} # | {rs} # | {ls} mean ms | {rs} mean ms | {ls} statuses | {rs} statuses |"
    sep = "|---|---|---|---|---|---|---|---|"
    lines.append(header)
    lines.append(sep)
    for key in all_keys:
        method, path = key.split(" ", 1)
        lo = l_groups.get(key, [])
        rr = r_groups.get(key, [])
        lc = len(lo)
        rc = len(rr)
        ld = round(statistics.mean([f.get("duration_ms", 0) or 0 for f in lo]), 1) if lo else "-"
        rd = round(statistics.mean([f.get("duration_ms", 0) or 0 for f in rr]), 1) if rr else "-"
        ls_ = ", ".join(sorted(str(f.get("status", "?")) for f in lo))
        rs_ = ", ".join(sorted(str(f.get("status", "?")) for f in rr))
        lines.append(f"| {method} | `{path}` | {lc} | {rc} | {ld} | {rd} | {ls_} | {rs_} |")
    lines.append("")

    # Missing endpoints — always emit the section.
    lines.append("## Missing endpoints")
    lines.append("")
    if left_only:
        lines.append(f"### {left_label} only")
        lines.append("")
        for key in left_only:
            lines.append(f"- `{key}`")
        lines.append("")
    else:
        lines.append(f"_No endpoints present only in {left_label}._")
        lines.append("")
    if right_only:
        lines.append(f"### {right_label} only")
        lines.append("")
        for key in right_only:
            lines.append(f"- `{key}`")
        lines.append("")
    else:
        lines.append(f"_No endpoints present only in {right_label}._")
        lines.append("")

    # Per-endpoint diffs.
    if shared:
        lines.append("## Per-endpoint comparison")
        lines.append("")
        for key in shared:
            lines.append(f"### `{key}`")
            lines.append("")
            lo = l_groups[key]
            rr = r_groups[key]

            ohk = header_keys(lo)
            rhk = header_keys(rr)
            if ohk != rhk:
                lines.append("**Header key differences:**")
                lines.append("")
                if ohk - rhk:
                    lines.append(f"- {left_label} only: `{ohk - rhk}`")
                if rhk - ohk:
                    lines.append(f"- {right_label} only: `{rhk - ohk}`")
                lines.append("")

            o_req = lo[0].get("request_body_json")
            r_req = rr[0].get("request_body_json")
            if o_req is not None or r_req is not None:
                lines.append("**Request body diff:**")
                lines.append("")
                if o_req != r_req:
                    diff = json_diff(o_req or {}, r_req or {}, left_label, right_label)
                    lines.append("```diff")
                    lines.append(diff.rstrip())
                    lines.append("```")
                else:
                    lines.append("_identical_")
                lines.append("")

            o_resp = lo[0].get("response_body_json")
            r_resp = rr[0].get("response_body_json")
            if o_resp is not None or r_resp is not None:
                lines.append("**Response body diff:**")
                lines.append("")
                if o_resp != r_resp:
                    diff = json_diff(o_resp or {}, r_resp or {}, left_label, right_label)
                    lines.append("```diff")
                    lines.append(diff.rstrip())
                    lines.append("```")
                else:
                    lines.append("_identical_")
                lines.append("")

            os_ = sorted(str(f.get("status", "?")) for f in lo)
            rs_ = sorted(str(f.get("status", "?")) for f in rr)
            lines.append(f"**Status codes:** {left_label}: [{', '.join(os_)}] | {right_label}: [{', '.join(rs_)}]")
            lines.append("")

            ods = [f.get("duration_ms", 0) or 0 for f in lo]
            rds = [f.get("duration_ms", 0) or 0 for f in rr]
            if ods and rds:
                try:
                    op50 = sorted(ods)[len(ods) // 2]
                    op95 = sorted(ods)[int(len(ods) * 0.95)]
                    rp50 = sorted(rds)[len(rds) // 2]
                    rp95 = sorted(rds)[int(len(rds) * 0.95)]
                    lines.append(f"**Timing (ms):** p50: {left_label} {op50:.1f} / {right_label} {rp50:.1f} | p95: {left_label} {op95:.1f} / {right_label} {rp95:.1f}")
                except (IndexError, statistics.StatisticsError):
                    pass
            lines.append("")
    else:
        lines.append("_No shared endpoints to compare._")
        lines.append("")

    text = "\n".join(lines)
    text = redact_report(text)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text)
    print(f"report written to {output_path}", flush=True)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Compare two MITM captures")
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--left-dir", default=None, help="Path to left (baseline) capture directory")
    ap.add_argument("--right-dir", default=None, help="Path to right (target) capture directory")
    ap.add_argument("--left-label", default="official", help="Label for the left backend")
    ap.add_argument("--right-label", default="runner-server", help="Label for the right backend")
    ap.add_argument("--output", required=True)
    # Legacy aliases for backward compatibility.
    ap.add_argument("--official-dir", dest="left_dir_compat", default=None, help=argparse.SUPPRESS)
    ap.add_argument("--runner-server-dir", dest="right_dir_compat", default=None, help=argparse.SUPPRESS)
    args = ap.parse_args()

    left_raw = args.left_dir_compat or args.left_dir
    right_raw = args.right_dir_compat or args.right_dir
    if not left_raw:
        ap.error("either --left-dir or --official-dir is required")
    if not right_raw:
        ap.error("either --right-dir or --runner-server-dir is required")
    left_dir = Path(left_raw)
    right_dir = Path(right_raw)
    output = Path(args.output)

    if not left_dir.exists():
        print(f"left capture dir not found: {left_dir}", file=sys.stderr)
        sys.exit(4)
    if not right_dir.exists():
        print(f"right capture dir not found: {right_dir}", file=sys.stderr)
        sys.exit(4)

    render_report(args.scenario, left_dir, right_dir, output, args.left_label, args.right_label)
