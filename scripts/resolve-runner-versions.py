#!/usr/bin/env python3
"""Resolve baseline/target actions/runner versions for the runner-sync pipeline.

Prints `from=<baseline>` and `to=<target>` on stdout so a workflow can capture
them with sed/grep. The baseline defaults to the last reconciled version (the
pipeline's own state, falling back to the newest committed golden dir); the
target defaults to `runner_version` in versions.toml (the Renovate bump).

Usage:
  resolve-runner-versions.py [--from vX.Y.Z] [--to vX.Y.Z]
"""

import argparse
import json
import re
import sys
from pathlib import Path

VERSION_RE = re.compile(r"v?(\d+)\.(\d+)\.(\d+)")


def version_key(tag: str) -> tuple[int, int, int]:
    m = VERSION_RE.fullmatch(tag)
    return tuple(int(x) for x in m.groups()) if m else (0, 0, 0)


def last_reconciled() -> str:
    candidates: list[str] = []
    state = Path(".runner-watch/state.json")
    if state.exists():
        try:
            to = json.loads(state.read_text()).get("to", "")
            if to and VERSION_RE.fullmatch(to):
                candidates.append(to)
        except Exception:
            pass
    golden = Path(".runner-watch/golden")
    if golden.is_dir():
        for entry in golden.iterdir():
            if entry.is_dir() and VERSION_RE.fullmatch(entry.name):
                candidates.append(entry.name)
    if not candidates:
        return ""
    return max(candidates, key=version_key)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="from_", help="baseline tag (default: last reconciled)")
    parser.add_argument("--to", help="target tag (default: runner_version in versions.toml)")
    args = parser.parse_args()

    to = args.to
    if not to:
        text = Path("versions.toml").read_text()
        match = re.search(r'^runner_version\s*=\s*"([^"]+)"', text, re.M)
        to = match.group(1) if match else ""

    from_ = args.from_ or last_reconciled()

    if not to:
        print("resolve-runner-versions: no target version (runner_version missing?)", file=sys.stderr)
        return 1
    if not from_:
        print("resolve-runner-versions: no baseline version (state.json/goldens missing?)", file=sys.stderr)
        return 1

    print(f"from={from_}")
    print(f"to={to}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
