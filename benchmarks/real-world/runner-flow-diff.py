#!/usr/bin/env python3
"""Strict-ish runner-vs-runner MITM flow comparison.

Compares two mitmproxy capture directories (flows.jsonl + summary.json) as runner
traffic against the same control plane.  It focuses on what the control plane and
the official runner contract observe: endpoint sequence, status codes, request
schemas, response schemas, selected request values after volatile redaction, and
body hashes for non-JSON payloads.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

VOLATILE_KEY_RE = re.compile(
    r"(^|_)(id|guid|token|session|signature|timestamp|time|date|url|uri|nonce|etag|sha|hash|expires|created|updated|started|completed|finish|worker|runner|agent|request|job|plan)(_|$)",
    re.IGNORECASE,
)
GUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
LONG_TOKEN_RE = re.compile(r"[A-Za-z0-9_\-]{24,}")
ISO_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?")
DIGIT_SEG_RE = re.compile(r"(?<=/)\d+(?=/|\?|$)")
QUERY_VOL_RE = re.compile(r"([?&][^=]*(?:id|token|signature|expires|time|date|session|request|plan|job|agent|name)[^=]*=)[^&]+", re.I)
RUN_ACTIONS_HOST_RE = re.compile(r"^run-actions-\d+-")
RESULTS_BLOB_HOST_RE = re.compile(r"^productionresultssa\d+\.blob\.core\.windows\.net$")

IGNORED_HOSTS = {
    "api.github.com",  # gh CLI dispatch/status if captured outside runner should be excluded by scripts, but guard anyway.
}


def load_json(path: Path) -> Any:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def load_flows(path: Path) -> list[dict[str, Any]]:
    flows_path = path / "flows.jsonl"
    if not flows_path.exists():
        raise SystemExit(f"missing {flows_path}")
    flows = []
    for line in flows_path.read_text().splitlines():
        if not line.strip():
            continue
        f = json.loads(line)
        if f.get("host") in IGNORED_HOSTS:
            continue
        flows.append(f)
    flows.sort(key=lambda f: f.get("flow_index") or 0)
    return flows


def norm_host(host: str) -> str:
    if RUN_ACTIONS_HOST_RE.match(host):
        return RUN_ACTIONS_HOST_RE.sub("run-actions-{n}-", host)
    if RESULTS_BLOB_HOST_RE.match(host):
        return "productionresultssa{n}.blob.core.windows.net"
    return host


def norm_path(host: str, path: str) -> str:
    # Signed Azure blob query strings are entirely volatile SAS material; the
    # semantically useful part is the log object path.
    if RESULTS_BLOB_HOST_RE.match(host) and "?" in path:
        path = path.split("?", 1)[0]
    # Official runner deletes /session; aksh currently deletes /session/{guid}.
    # Group them so the report can focus on request/response contract, not a
    # path spelling already covered by body/schema diffs.
    if host == "broker.actions.githubusercontent.com":
        path = re.sub(r"^/session/[0-9a-fA-F-]{36}$", "/session/{guid}", path)
    path = GUID_RE.sub("{guid}", path)
    path = DIGIT_SEG_RE.sub("/{n}", path)
    path = QUERY_VOL_RE.sub(lambda m: m.group(1) + "{volatile}", path)
    return path


def endpoint(flow: dict[str, Any]) -> str:
    host = flow.get("host", "?")
    return f"{flow.get('method','?')} {norm_host(host)}{norm_path(host, flow.get('path','/'))}"


def shape(v: Any) -> Any:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, list):
        unique = []
        for item in v:
            s = shape(item)
            if s not in unique:
                unique.append(s)
        return unique
    if isinstance(v, dict):
        return {k: shape(v[k]) for k in sorted(v)}
    return type(v).__name__


def scrub(v: Any, key: str = "") -> Any:
    if VOLATILE_KEY_RE.search(key):
        return "{volatile}"
    if isinstance(v, dict):
        return {k: scrub(val, k) for k, val in sorted(v.items())}
    if isinstance(v, list):
        return [scrub(x, key) for x in v]
    if isinstance(v, str):
        s = ISO_RE.sub("{time}", v)
        s = GUID_RE.sub("{guid}", s)
        s = LONG_TOKEN_RE.sub(lambda m: "{token}" if len(set(m.group(0))) > 8 else m.group(0), s)
        return s
    return v


def body_sig(flow: dict[str, Any], field: str) -> tuple[Any, Any, str]:
    js = flow.get(f"{field}_body_json")
    b64 = flow.get(f"{field}_body_b64") or ""
    if js is not None:
        return shape(js), scrub(js), "json"
    if b64:
        try:
            raw = base64.b64decode(b64)
        except Exception:
            raw = b64.encode()
        # For non-JSON body, shape is stable (bytes+sha256 structure), value has actuals.
        sig = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        return {"bytes": "number", "sha256": "string"}, sig, "binary"
    return None, None, "empty"


def compact_diff(left: Any, right: Any, max_chars: int = 2000) -> str:
    a = json.dumps(left, sort_keys=True, indent=2, ensure_ascii=False)
    b = json.dumps(right, sort_keys=True, indent=2, ensure_ascii=False)
    if a == b:
        return ""
    import difflib
    text = "\n".join(difflib.unified_diff(a.splitlines(), b.splitlines(), fromfile="official", tofile="aksh", lineterm=""))
    if len(text) > max_chars:
        return text[:max_chars] + "\n... truncated ..."
    return text


def compare(left_dir: Path, right_dir: Path, scenario: str, out: Path) -> int:
    left = load_flows(left_dir)
    right = load_flows(right_dir)
    left_sum = load_json(left_dir / "summary.json")
    right_sum = load_json(right_dir / "summary.json")

    issues: list[tuple[str, str]] = []
    lines: list[str] = []
    lines.append(f"# Runner flow diff: {scenario}")
    lines.append("")
    lines.append(f"- official capture: `{left_dir}`")
    lines.append(f"- aksh capture: `{right_dir}`")
    lines.append(f"- official summary: status={left_sum.get('status')} flows={len(left)}")
    lines.append(f"- aksh summary: status={right_sum.get('status')} flows={len(right)}")
    lines.append("")

    lseq = [endpoint(f) for f in left]
    rseq = [endpoint(f) for f in right]
    if lseq != rseq:
        issues.append(("endpoint-sequence", "normalized endpoint sequence differs"))
    lcnt, rcnt = Counter(lseq), Counter(rseq)
    lines.append("## Endpoint counts")
    lines.append("")
    lines.append("| endpoint | official | aksh |")
    lines.append("|---|---:|---:|")
    for ep in sorted(set(lcnt) | set(rcnt)):
        lc, rc = lcnt[ep], rcnt[ep]
        marker = "" if lc == rc else " ⚠"
        lines.append(f"| `{ep}` | {lc} | {rc}{marker} |")
    lines.append("")

    if lseq != rseq:
        lines.append("## Endpoint sequence diff")
        lines.append("")
        lines.append("```diff")
        diff = compact_diff(lseq, rseq, max_chars=6000)
        lines.append(diff)
        lines.append("```")
        lines.append("")

    pairs_by_ep: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for ep in sorted(set(lcnt) & set(rcnt)):
        lf = [f for f in left if endpoint(f) == ep]
        rf = [f for f in right if endpoint(f) == ep]
        for a, b in zip(lf, rf):
            pairs_by_ep[ep].append((a, b))

    lines.append("## Per-flow contract differences")
    lines.append("")
    any_detail = False
    for ep, pairs in pairs_by_ep.items():
        ep_lines: list[str] = []
        for idx, (a, b) in enumerate(pairs, start=1):
            local: list[str] = []
            if a.get("status") != b.get("status"):
                issues.append(("status", f"{ep} #{idx}: {a.get('status')} != {b.get('status')}"))
                local.append(f"- status: official={a.get('status')} aksh={b.get('status')}")
            for field in ("request", "response"):
                ashape, aval, akind = body_sig(a, field)
                bshape, bval, bkind = body_sig(b, field)
                if ashape != bshape:
                    issues.append((f"{field}-schema", f"{ep} #{idx}"))
                    local.append(f"- {field} schema differs")
                    local.append("```diff")
                    local.append(compact_diff(ashape, bshape))
                    local.append("```")
                elif akind == "json" and aval != bval:
                    # Value diffs after redaction are lower severity but still real; official may care.
                    issues.append((f"{field}-value", f"{ep} #{idx}"))
                    local.append(f"- {field} redacted value differs")
                    local.append("```diff")
                    local.append(compact_diff(aval, bval))
                    local.append("```")
                elif akind == "binary" and aval != bval:
                    issues.append((f"{field}-binary", f"{ep} #{idx}"))
                    local.append(f"- {field} binary body differs")
                    local.append("```diff")
                    local.append(compact_diff(aval, bval))
                    local.append("```")
            if local:
                ep_lines.append(f"#### occurrence {idx}")
                ep_lines.extend(local)
                ep_lines.append("")
        if ep_lines:
            any_detail = True
            lines.append(f"### `{ep}`")
            lines.append("")
            lines.extend(ep_lines)
    if not any_detail:
        lines.append("_No per-flow status/schema/redacted-value differences._")
        lines.append("")

    lines.append("## Verdict")
    lines.append("")
    if issues:
        lines.append(f"FAIL: {len(issues)} contract differences found.")
        lines.append("")
        counts = Counter(kind for kind, _ in issues)
        for kind, n in sorted(counts.items()):
            lines.append(f"- {kind}: {n}")
    else:
        lines.append("PASS: endpoint sequence, statuses, schemas, and redacted body values match.")
    lines.append("")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    print(out)
    return 1 if issues else 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", required=True)
    p.add_argument("--official-dir", required=True, type=Path)
    p.add_argument("--aksh-dir", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    args = p.parse_args()
    return compare(args.official_dir, args.aksh_dir, args.scenario, args.output)


if __name__ == "__main__":
    raise SystemExit(main())
