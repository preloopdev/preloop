#!/usr/bin/env python3
"""Promote the [Unreleased] changelog section into a dated release entry.

Usage: changelog-release.py VERSION   (VERSION as X.Y.Z or vX.Y.Z)

Rewrites CHANGELOG.md in place: the [Unreleased] heading becomes
"## [X.Y.Z] - <today>", keeping its body, and a fresh empty [Unreleased]
section is inserted above it. Fails if the version entry already exists or
the [Unreleased] section is missing/duplicated.
"""

import re
import sys
from datetime import date


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("usage: changelog-release.py VERSION")
    version = sys.argv[1].lstrip("v")
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        sys.exit(f"bad version {version!r}; expected X.Y.Z")

    text = open("CHANGELOG.md", encoding="utf-8").read()
    if re.search(rf"^## \[{re.escape(version)}\]", text, re.M):
        sys.exit(f"CHANGELOG.md already has an entry for [{version}]")
    if text.count("## [Unreleased]") != 1:
        sys.exit("CHANGELOG.md must have exactly one [Unreleased] section")

    marker = "## [Unreleased]\n"
    entry = f"## [{version}] - {date.today().isoformat()}\n\n"
    text = text.replace(marker + "\n", entry, 1)
    text = text.replace(entry, marker + "\n" + entry, 1)
    open("CHANGELOG.md", "w", encoding="utf-8").write(text)
    print(f"promoted [Unreleased] to [{version}] - {date.today().isoformat()}")


if __name__ == "__main__":
    main()
