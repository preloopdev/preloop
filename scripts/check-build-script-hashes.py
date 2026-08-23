#!/usr/bin/env python3
"""Verify the reviewed source hash of every Cargo build script.

Cargo does not currently provide a built-in build-script allowlist. This
checker resolves the locked dependency graph with `cargo metadata` (which does
not compile or execute build scripts), hashes every `custom-build` target, and
compares the result with the committed allowlist.

Use `--write` only when intentionally reviewing a dependency or build-script
change:

    python3 scripts/check-build-script-hashes.py --write
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
from typing import Any


ALLOWLIST_VERSION = 1


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def metadata(manifest: Path, cargo: str) -> dict[str, Any]:
    command = [
        cargo,
        "metadata",
        "--format-version",
        "1",
        "--locked",
        "--manifest-path",
        str(manifest),
    ]
    try:
        result = subprocess.run(
            command,
            cwd=manifest.parent,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as error:
        sys.stderr.write(error.stderr)
        raise SystemExit(error.returncode) from error
    return json.loads(result.stdout)


def current_scripts(manifest: Path, cargo: str) -> dict[str, dict[str, str]]:
    graph = metadata(manifest, cargo)
    scripts: dict[str, dict[str, str]] = {}

    for package in graph["packages"]:
        package_root = Path(package["manifest_path"]).parent
        source = package.get("source") or "workspace"
        for target in package["targets"]:
            if "custom-build" not in target["kind"]:
                continue

            script = Path(target["src_path"])
            try:
                relative_script = script.relative_to(package_root).as_posix()
            except ValueError as error:
                raise SystemExit(
                    f"build script is outside package root: {script}"
                ) from error
            if not script.is_file():
                raise SystemExit(
                    f"build script source is missing; run `cargo fetch --locked`: {script}"
                )

            key = "|".join(
                [
                    source,
                    package["name"],
                    package["version"],
                    target["name"],
                    relative_script,
                ]
            )
            if key in scripts:
                raise SystemExit(f"duplicate build-script identity: {key}")
            scripts[key] = {
                "package": package["name"],
                "version": package["version"],
                "source": source,
                "target": target["name"],
                "path": relative_script,
                "sha256": sha256(script),
            }
    return scripts


def load_allowlist(path: Path) -> dict[str, dict[str, str]]:
    try:
        with path.open("rb") as source:
            document = tomllib.load(source)
    except FileNotFoundError as error:
        raise SystemExit(f"missing build-script allowlist: {path}") from error

    if document.get("version") != ALLOWLIST_VERSION:
        raise SystemExit(
            f"{path}: expected version {ALLOWLIST_VERSION}, "
            f"got {document.get('version')!r}"
        )

    entries = document.get("build_script", [])
    if not isinstance(entries, list):
        raise SystemExit(f"{path}: build_script must be an array of tables")

    allowlist: dict[str, dict[str, str]] = {}
    required = {"package", "version", "source", "target", "path", "sha256"}
    for entry in entries:
        if not isinstance(entry, dict) or not required <= entry.keys():
            raise SystemExit(
                f"{path}: every build_script entry requires "
                f"{', '.join(sorted(required))}"
            )
        key = "|".join(
            [
                entry["source"],
                entry["package"],
                entry["version"],
                entry["target"],
                entry["path"],
            ]
        )
        if key in allowlist:
            raise SystemExit(f"{path}: duplicate build-script identity: {key}")
        allowlist[key] = {name: str(entry[name]) for name in required}
    return allowlist


def toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def write_allowlist(path: Path, scripts: dict[str, dict[str, str]]) -> None:
    lines = [
        "# Reviewed SHA-256 hashes of every Cargo custom-build target.",
        "#",
        "# Regenerate only after reviewing dependency/build-script changes:",
        "#   cargo fetch --locked",
        "#   python3 scripts/check-build-script-hashes.py --write",
        "version = 1",
        "",
    ]
    for key in sorted(scripts):
        entry = scripts[key]
        lines.append("[[build_script]]")
        for field in ("package", "version", "source", "target", "path", "sha256"):
            lines.append(f"{field} = {toml_string(entry[field])}")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as output:
        output.write("\n".join(lines))
        temporary = Path(output.name)
    os.replace(temporary, path)


def check(
    current: dict[str, dict[str, str]],
    allowed: dict[str, dict[str, str]],
) -> int:
    failures = 0
    for key in sorted(current):
        entry = current[key]
        reviewed = allowed.get(key)
        if reviewed is None:
            print(
                "UNREVIEWED build script:"
                f" {entry['package']} {entry['version']} {entry['path']}"
                f" sha256={entry['sha256']}",
                file=sys.stderr,
            )
            failures += 1
        elif reviewed["sha256"] != entry["sha256"]:
            print(
                "CHANGED build script:"
                f" {entry['package']} {entry['version']} {entry['path']}"
                f" expected={reviewed['sha256']}"
                f" actual={entry['sha256']}",
                file=sys.stderr,
            )
            failures += 1

    for key in sorted(set(allowed) - set(current)):
        entry = allowed[key]
        print(
            "STALE build-script allowlist entry:"
            f" {entry['package']} {entry['version']} {entry['path']}",
            file=sys.stderr,
        )
        failures += 1

    if failures:
        return 1
    print(f"build-script allowlist: {len(current)} entries verified")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--allowlist",
        type=Path,
        default=Path("supply-chain/build-scripts.toml"),
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=Path("Cargo.toml"),
    )
    parser.add_argument("--cargo", default="cargo")
    parser.add_argument(
        "--write",
        action="store_true",
        help="write the current hashes instead of checking them",
    )
    args = parser.parse_args()

    manifest = args.manifest_path.resolve()
    allowlist = args.allowlist.resolve()
    scripts = current_scripts(manifest, args.cargo)
    if args.write:
        write_allowlist(allowlist, scripts)
        print(f"wrote {len(scripts)} build-script hashes to {allowlist}")
        return 0
    return check(scripts, load_allowlist(allowlist))


if __name__ == "__main__":
    raise SystemExit(main())
