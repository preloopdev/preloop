#!/usr/bin/env python3
"""Verify every SHA-pinned GitHub Action carries a truthful `# <ref>` comment.

Renovate's github-actions manager reads that trailing comment to decide what a
pinned SHA *is*. Two ways it goes wrong, both silent:

  * no comment at all -> Renovate skips the pin entirely and it is frozen
    forever, including for security releases.
  * a comment that no longer matches the SHA (someone hand-edited one without
    the other) -> Renovate computes its next update from the wrong baseline.

Renovate cannot detect either case; it trusts the comment. This does.

Rules, by comment kind:

  * version-like (`# v4.6.2`, `# 1.2.3`) -> the SHA must be exactly what that
    tag points at. Accepts either the commit SHA or the annotated tag object
    SHA, because both appear in the wild (`Swatinem/rust-cache@<tag-object>`).
  * moving ref (`# stable`, `# v2`, `# main`, a branch name) -> the SHA must be
    *reachable from* that ref. A digest-only pin legitimately lags the ref head
    until Renovate bumps it, so requiring equality would fail on every pin that
    is merely not-yet-updated. Requiring ancestry still catches a comment that
    names a ref the commit was never on.

Usage:
    verify-action-pins.py [paths...]      # defaults to .github/workflows
Env:
    GITHUB_TOKEN / GH_TOKEN  raises the API rate limit and is required for
                             private repos. Unauthenticated works for a small
                             number of public pins.
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

API = "https://api.github.com"

# `uses: owner/repo[/subpath]@<40-hex sha> # <ref>` — the ref comment is
# optional here on purpose so a missing one is reported rather than skipped.
USES = re.compile(
    r"""uses:\s*
        (?P<owner>[A-Za-z0-9._-]+)/(?P<repo>[A-Za-z0-9._-]+)
        (?P<subpath>/[^@\s]+)?
        @(?P<sha>[0-9a-f]{40})
        (?:\s*\#\s*(?P<ref>\S+))?""",
    re.VERBOSE,
)

VERSION_LIKE = re.compile(r"^v?\d+(\.\d+)*$")


def api(path: str) -> object | None:
    """GET a JSON endpoint; None on 404/422 (absent ref, unknown commit)."""
    req = urllib.request.Request(f"{API}{path}")
    req.add_header("Accept", "application/vnd.github+json")
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 422):
            return None
        raise


def tag_shas(repo: str, ref: str) -> set[str]:
    """Every SHA that legitimately identifies `ref`: the tag object and the
    commit it dereferences to."""
    obj = api(f"/repos/{repo}/git/ref/tags/{ref}")
    if not isinstance(obj, dict):
        return set()
    target = obj.get("object") or {}
    shas = {target.get("sha")}
    if target.get("type") == "tag":  # annotated: deref to the commit
        inner = api(f"/repos/{repo}/git/tags/{target['sha']}")
        if isinstance(inner, dict):
            shas.add((inner.get("object") or {}).get("sha"))
    return {s for s in shas if s}


def reachable(repo: str, ref: str, sha: str) -> bool:
    """Whether `sha` is `ref` or an ancestor of it."""
    if sha in tag_shas(repo, ref):
        return True
    cmp = api(f"/repos/{repo}/compare/{ref}...{sha}")
    if not isinstance(cmp, dict):
        return False
    # base...head: "identical" = same commit, "behind" = head is an ancestor of
    # the ref. "ahead"/"diverged" mean the commit is not on that ref.
    return cmp.get("status") in ("identical", "behind")


def main(argv: list[str]) -> int:
    roots = [Path(p) for p in argv[1:]] or [Path(".github/workflows")]
    files: list[Path] = []
    for root in roots:
        files.extend(sorted(root.rglob("*.yml")) if root.is_dir() else [root])

    failures: list[str] = []
    checked = 0

    for path in files:
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            m = USES.search(line)
            if not m:
                continue
            repo = f"{m['owner']}/{m['repo']}"
            sha, ref = m["sha"], m["ref"]
            where = f"{path}:{lineno} {repo}@{sha[:12]}"

            if not ref:
                failures.append(
                    f"{where}\n"
                    f"    bare SHA with no version comment -- Renovate skips this pin\n"
                    f"    entirely, so it will never be updated. Add the tag or branch:\n"
                    f"    `@{sha} # <tag-or-branch>`"
                )
                continue

            checked += 1
            if VERSION_LIKE.match(ref):
                valid = tag_shas(repo, ref)
                if not valid:
                    failures.append(f"{where}\n    comment names tag `{ref}`, which does not exist in {repo}")
                elif sha not in valid:
                    failures.append(
                        f"{where}\n"
                        f"    comment says `{ref}` but that tag points at "
                        f"{sorted(s[:12] for s in valid)}.\n"
                        f"    Renovate will compute its next update from the wrong baseline."
                    )
            elif not reachable(repo, ref, sha):
                failures.append(
                    f"{where}\n"
                    f"    comment says `{ref}` but the commit is not on that ref."
                )

    print(f"checked {checked} pinned action(s) across {len(files)} file(s)")
    if failures:
        print(f"\n{len(failures)} problem(s):\n", file=sys.stderr)
        for f in failures:
            print(f"  {f}\n", file=sys.stderr)
        return 1
    print("every pinned action has a truthful version comment")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
