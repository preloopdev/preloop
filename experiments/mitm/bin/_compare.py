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

    # Strip the official runner's single-segment random base path prefix
    # (e.g. /abc123/_apis/... → /_apis/...). Must precede _apis and be a single
    # alphanumeric segment (never multi-segment like /runner/server/).
    path = re.sub(r"^/([a-zA-Z0-9]+)/_apis/", "/_apis/", path, count=1)
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
        for kv in qs.split("&"):
            if "=" in kv:
                k, v = kv.split("=", 1)
                if k in ("sessionId", "lastMessageId", "api-version", "taskInstanceId", "requestId", "agentId"):
                    v = "{volatile}"
                params.append((k, v))
            else:
                params.append((kv, ""))
        params.sort(key=lambda x: x[0])
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


def json_diff(ja: Any, jb: Any) -> str:
    a_lines = json.dumps(ja, indent=2, sort_keys=True, ensure_ascii=False).splitlines(keepends=True)
    b_lines = json.dumps(jb, indent=2, sort_keys=True, ensure_ascii=False).splitlines(keepends=True)
    return "".join(difflib.unified_diff(a_lines, b_lines, fromfile="official", tofile="runner-server"))


def header_keys(flows: list[dict]) -> set[str]:
    ignored = {"date", "server", "content-length", "x-request-id", "x-vss-e2eid", "x-msedge-ref"}
    keys = set()
    for f in flows:
        for hh in f.get("request_headers", []) + f.get("response_headers", []):
            name = hh[0].lower() if isinstance(hh, list) and hh else ""
            if name not in ignored:
                keys.add(name)
    return keys


def render_report(scenario_name: str, official_dir: Path, rs_dir: Path, output_path: Path):
    official_flows = load_flows(official_dir / "flows.jsonl")
    rs_flows = load_flows(rs_dir / "flows.jsonl")

    official_summary = {}
    rs_summary = {}
    if (official_dir / "summary.json").exists():
        official_summary = json.loads((official_dir / "summary.json").read_text())
    if (rs_dir / "summary.json").exists():
        rs_summary = json.loads((rs_dir / "summary.json").read_text())

    def group(flows: list[dict]) -> dict[str, list[dict]]:
        g: dict[str, list[dict]] = {}
        for f in flows:
            key = f"{f.get('method', '?')} {normalize_path(f.get('path', '/'))}"
            g.setdefault(key, []).append(f)
        return g

    o_groups = group(official_flows)
    r_groups = group(rs_flows)

    all_keys = sorted(set(o_groups) | set(r_groups))
    official_only = sorted(set(o_groups) - set(r_groups))
    rs_only = sorted(set(r_groups) - set(o_groups))
    shared = sorted(set(o_groups) & set(r_groups))

    lines: list[str] = []
    lines.append(f"# MITM comparison: {scenario_name}")
    lines.append("")
    lines.append(f"**Official backend**: {official_summary.get('status', 'N/A')} — {len(official_flows)} flows")
    lines.append(f"**Runner.server backend**: {rs_summary.get('status', 'N/A')} — {len(rs_flows)} flows")
    lines.append("")

    # Endpoint matrix.
    lines.append("## Endpoint matrix")
    lines.append("")
    header = "| method | normalized path | official # | rs # | official mean ms | rs mean ms | official statuses | rs statuses |"
    sep = "|---|---|---|---|---|---|---|---|"
    lines.append(header)
    lines.append(sep)
    for key in all_keys:
        method, path = key.split(" ", 1)
        oo = o_groups.get(key, [])
        rr = r_groups.get(key, [])
        oc = len(oo)
        rc = len(rr)
        od = round(statistics.mean([f.get("duration_ms", 0) or 0 for f in oo]), 1) if oo else "-"
        rd = round(statistics.mean([f.get("duration_ms", 0) or 0 for f in rr]), 1) if rr else "-"
        os_ = ", ".join(sorted(str(f.get("status", "?")) for f in oo))
        rs_ = ", ".join(sorted(str(f.get("status", "?")) for f in rr))
        lines.append(f"| {method} | `{path}` | {oc} | {rc} | {od} | {rd} | {os_} | {rs_} |")
    lines.append("")

    # Missing endpoints — always emit the section.
    lines.append("## Missing endpoints")
    lines.append("")
    if official_only:
        lines.append("### Official only")
        lines.append("")
        for key in official_only:
            lines.append(f"- `{key}`")
        lines.append("")
    else:
        lines.append("_No endpoints present only in official._")
        lines.append("")
    if rs_only:
        lines.append("### Runner.server only")
        lines.append("")
        for key in rs_only:
            lines.append(f"- `{key}`")
        lines.append("")
    else:
        lines.append("_No endpoints present only in runner.server._")
        lines.append("")

    # Per-endpoint diffs.
    if shared:
        lines.append("## Per-endpoint comparison")
        lines.append("")
        for key in shared:
            lines.append(f"### `{key}`")
            lines.append("")
            oo = o_groups[key]
            rr = r_groups[key]

            ohk = header_keys(oo)
            rhk = header_keys(rr)
            if ohk != rhk:
                lines.append("**Header key differences:**")
                lines.append("")
                if ohk - rhk:
                    lines.append(f"- Official only: `{ohk - rhk}`")
                if rhk - ohk:
                    lines.append(f"- Runner.server only: `{rhk - ohk}`")
                lines.append("")

            o_req = oo[0].get("request_body_json")
            r_req = rr[0].get("request_body_json")
            if o_req is not None or r_req is not None:
                lines.append("**Request body diff:**")
                lines.append("")
                if o_req != r_req:
                    diff = json_diff(o_req or {}, r_req or {})
                    lines.append("```diff")
                    lines.append(diff.rstrip())
                    lines.append("```")
                else:
                    lines.append("_identical_")
                lines.append("")

            o_resp = oo[0].get("response_body_json")
            r_resp = rr[0].get("response_body_json")
            if o_resp is not None or r_resp is not None:
                lines.append("**Response body diff:**")
                lines.append("")
                if o_resp != r_resp:
                    diff = json_diff(o_resp or {}, r_resp or {})
                    lines.append("```diff")
                    lines.append(diff.rstrip())
                    lines.append("```")
                else:
                    lines.append("_identical_")
                lines.append("")

            os_ = sorted(str(f.get("status", "?")) for f in oo)
            rs_ = sorted(str(f.get("status", "?")) for f in rr)
            lines.append(f"**Status codes:** official: [{', '.join(os_)}] | runner.server: [{', '.join(rs_)}]")
            lines.append("")

            ods = [f.get("duration_ms", 0) or 0 for f in oo]
            rds = [f.get("duration_ms", 0) or 0 for f in rr]
            if ods and rds:
                try:
                    op50 = sorted(ods)[len(ods) // 2]
                    op95 = sorted(ods)[int(len(ods) * 0.95)]
                    rp50 = sorted(rds)[len(rds) // 2]
                    rp95 = sorted(rds)[int(len(rds) * 0.95)]
                    lines.append(f"**Timing (ms):** p50: official {op50:.1f} / rs {rp50:.1f} | p95: official {op95:.1f} / rs {rp95:.1f}")
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--official-dir", required=True)
    ap.add_argument("--runner-server-dir", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    official_dir = Path(args.official_dir)
    rs_dir = Path(args.runner_server_dir)
    output = Path(args.output)

    if not official_dir.exists():
        print(f"official capture dir not found: {official_dir}", file=sys.stderr)
        sys.exit(4)
    if not rs_dir.exists():
        print(f"runner-server capture dir not found: {rs_dir}", file=sys.stderr)
        sys.exit(4)

    render_report(args.scenario, official_dir, rs_dir, output)
