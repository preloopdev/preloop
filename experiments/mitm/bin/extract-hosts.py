#!/usr/bin/env python3
"""Extract flows for selected hosts from capture directories into a corpus.

Walks every `flows.jsonl` under the given roots, keeps records whose host
matches the allowlist (same syntax as `MITM_CAPTURE_HOSTS`: a leading dot
matches the domain and subdomains), and writes them to
`OUT/<scenario>/flows.jsonl`, where <scenario> is the capture directory's path
relative to its root. Bodies stay as recorded (already redacted at capture);
records are re-scrubbed with the current redaction rules as a second pass.

Usage:
  extract-hosts.py --hosts api.github.com,github.com,codeload.github.com \
      --out corpus/ .runner-watch/golden/v2.336.0
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "addons"))
from hosts import host_selected, parse_allowlist  # noqa: E402
from redact import redact_json  # noqa: E402

SKIP_PATHS = ("/_dns",)  # capture-harness DNS probes, not service traffic


def extract(roots: list[Path], hosts: list[str], out: Path) -> dict[str, int]:
    # A reused corpus directory must not keep scenarios the new filter drops:
    # reject anything but an empty/missing output so stale flows cannot sit in
    # the corpus while counts omit them.
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"refusing to extract into nonempty directory: {out}")

    counts: dict[str, int] = {}
    # scenario name -> the flows.jsonl that produced it; a second writer for
    # the same name means two roots collided and one would silently overwrite.
    sources: dict[str, Path] = {}
    for root in roots:
        for flows in sorted(root.rglob("flows.jsonl")):
            rel = flows.parent.relative_to(root).as_posix()
            # A flows.jsonl directly under the root has rel "."; fall back to
            # the directory name so multiple direct captures stay distinct.
            scenario = rel if rel != "." else root.name
            if scenario in sources and sources[scenario] != flows:
                raise SystemExit(
                    f"scenario name collision: {flows} and {sources[scenario]} "
                    f"both map to {scenario!r}; rename one capture directory"
                )
            sources[scenario] = flows
            kept = []
            for line in flows.read_text().splitlines():
                if not line.strip():
                    continue
                record = json.loads(line)
                if not host_selected(record.get("host", ""), hosts):
                    continue
                if record.get("path", "").split("?")[0] in SKIP_PATHS:
                    continue
                for key in ("request_body_json", "response_body_json"):
                    if record.get(key) is not None:
                        record[key] = redact_json(record[key])
                kept.append(record)
            if kept:
                target = out / scenario / "flows.jsonl"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in kept))
                counts[scenario] = len(kept)
    return counts


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hosts", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("roots", nargs="+", type=Path)
    args = ap.parse_args()
    hosts = parse_allowlist(args.hosts)
    counts = extract(args.roots, hosts, args.out)
    for scenario, n in counts.items():
        print(f"{n:5d}  {scenario}")
    print(f"{sum(counts.values())} flows from {len(counts)} captures -> {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
