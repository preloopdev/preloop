#!/usr/bin/env python3
"""Scrub live credentials from golden capture directories, in place.

Idempotent. Handles every artifact a capture can produce:

- ``flows.jsonl`` — base64 body fields are decoded, redacted, re-encoded
  (and their ``*_sha256`` fingerprints recomputed); JSON body fields and
  every other string field are redacted structurally.
- ``flow.*.bin`` — raw request/response body dumps are redacted as bytes.
- ``flows.mitm`` — raw mitmproxy stream, via an offline mitmdump pass with
  ``addons/scrub.py`` (requires mitmdump on PATH; skipped with a warning
  when it is missing so jsonl/bin scrubbing still completes).

Usage::

    scrub-goldens.py [path ...]       # default: .runner-watch/golden
    scrub-goldens.py --check          # exit 1 if anything would change
"""

import argparse
import base64
import glob
import hashlib
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "addons"))

import redact  # noqa: E402

B64_FIELDS = ("request_body_b64", "response_body_b64")
JSON_FIELDS = ("request_body_json", "response_body_json")
HEADER_FIELDS = ("request_headers", "response_headers")


def scrub_record(record: dict) -> bool:
    """Redact one flows.jsonl record; return True if it changed."""
    changed = False
    for field in B64_FIELDS:
        raw = record.get(field) or ""
        if not raw:
            continue
        try:
            decoded = base64.b64decode(raw)
        except Exception:
            continue
        scrubbed = redact.redact_bytes(decoded)
        if scrubbed == decoded:
            continue
        record[field] = base64.b64encode(scrubbed).decode()
        sha_field = field.replace("_b64", "_sha256")
        if sha_field in record:
            record[sha_field] = hashlib.sha256(scrubbed).hexdigest()
        changed = True
    for field in JSON_FIELDS:
        value = record.get(field)
        if value is None:
            continue
        scrubbed = redact.redact_json(value)
        if scrubbed != value:
            record[field] = scrubbed
            changed = True
    # Header lists ([name, value] pairs) can carry raw credentials in
    # captures produced outside addons/capture.py (benchmark harnesses).
    for field in HEADER_FIELDS:
        value = record.get(field)
        if not value:
            continue
        scrubbed = redact.redact_json(value)
        if scrubbed != value:
            record[field] = scrubbed
            changed = True
    for key, value in list(record.items()):
        if (
            key in B64_FIELDS
            or key in JSON_FIELDS
            or key in HEADER_FIELDS
            or not isinstance(value, str)
        ):
            continue
        scrubbed = redact.redact_str(value)
        if scrubbed != value:
            record[key] = scrubbed
            changed = True
    return changed


def scrub_jsonl(path: str, dry_run: bool = False) -> bool:
    changed = False
    with open(path, encoding="utf-8") as handle:
        lines = handle.read().splitlines()
    out = []
    for line in lines:
        if not line.strip():
            out.append(line)
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            scrubbed = redact.redact_str(line)
            if scrubbed != line:
                changed = True
            out.append(scrubbed)
            continue
        if scrub_record(record):
            changed = True
        out.append(json.dumps(record, ensure_ascii=False))
    if changed and not dry_run:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(out) + "\n")
    return changed


def scrub_bin(path: str, dry_run: bool = False) -> bool:
    data = open(path, "rb").read()
    scrubbed = redact.redact_bytes(data)
    if scrubbed == data:
        return False
    if dry_run:
        return True
    with open(path, "wb") as handle:
        handle.write(scrubbed)
    return True


def scrub_mitm(path: str) -> bool:
    """Rewrite a flows.mitm through an offline mitmdump scrub pass."""
    # Skip clean streams: rewriting through mitmdump is not byte-identical
    # even when nothing needs redacting, so only round-trip when there is
    # actually something to scrub.
    data = open(path, "rb").read()
    if not any(pattern.search(data) for pattern in redact._TOKEN_PATTERNS):
        return False
    addons = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "addons", "scrub.py")
    tmp = f"{path}.scrubbed"
    try:
        result = subprocess.run(
            ["mitmdump", "--quiet", "-r", path, "-w", tmp, "-s", addons],
            capture_output=True,
            timeout=300,
        )
    except FileNotFoundError:
        print(f"WARNING: mitmdump not found; cannot scrub {path}", file=sys.stderr)
        return False
    except subprocess.TimeoutExpired:
        print(f"WARNING: mitmdump timed out scrubbing {path}", file=sys.stderr)
        return False
    if result.returncode != 0 or not os.path.exists(tmp):
        print(
            f"WARNING: mitmdump failed scrubbing {path}: "
            f"{result.stderr.decode(errors='replace')[:500]}",
            file=sys.stderr,
        )
        if os.path.exists(tmp):
            os.unlink(tmp)
        return False
    os.replace(tmp, path)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", default=[".runner-watch/golden"])
    parser.add_argument(
        "--check",
        action="store_true",
        help="report what would change without modifying files; exit 1 if dirty",
    )
    args = parser.parse_args()

    roots = args.paths
    targets = []
    for root in roots:
        targets.extend(glob.glob(os.path.join(root, "**", "*flows.jsonl"), recursive=True))
        targets.extend(glob.glob(os.path.join(root, "**", "flow.*.bin"), recursive=True))
        targets.extend(glob.glob(os.path.join(root, "**", "flows.mitm"), recursive=True))

    dirty = 0
    for path in sorted(targets):
        if path.endswith(".jsonl"):
            changed = scrub_jsonl(path, dry_run=args.check)
        elif path.endswith("flows.mitm"):
            if args.check:
                continue
            changed = scrub_mitm(path)
        else:
            changed = scrub_bin(path, dry_run=args.check)
        if changed:
            dirty += 1
            print(f"scrubbed: {path}")
    print(f"scrubbed {dirty} file(s)")
    # Mutate mode always succeeds once the scrub ran (even with nothing to
    # do); --check reports dirtiness through the exit code instead.
    return 1 if (args.check and dirty) else 0


if __name__ == "__main__":
    sys.exit(main())
