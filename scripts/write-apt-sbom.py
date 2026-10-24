#!/usr/bin/env python3
"""Convert the baked dpkg manifest into a CycloneDX SBOM.

The golden bake writes `<golden>.apt-manifest.txt` from `dpkg-query` after the
security-pocket update. Scanners (Grype, Trivy) need a real SBOM rather than a
tab-separated list, and the `syft:distro:*` properties are what let them match
Ubuntu advisories instead of guessing the release from purls.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from pathlib import Path
from urllib.parse import quote


def purl(distro: str, name: str, version: str, arch: str | None = None) -> str:
    value = f"pkg:deb/{distro}/{quote(name, safe='')}@{quote(version, safe='._+~-')}"
    if arch:
        value += f"?arch={quote(arch, safe='')}"
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--distro", default="ubuntu")
    parser.add_argument("--distro-version", default="24.04")
    args = parser.parse_args()

    raw = args.manifest.read_bytes()
    components = []
    for number, line in enumerate(raw.decode("utf-8").splitlines(), start=1):
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) != 3:
            raise SystemExit(
                f"{args.manifest}:{number}: expected package, version, architecture; "
                f"got {line!r}"
            )
        name, version, arch = fields
        ref = purl(args.distro, name, version, arch)
        components.append(
            {
                "type": "library",
                "bom-ref": ref,
                "name": name,
                "version": version,
                "purl": ref,
                "properties": [
                    {"name": "syft:package:type", "value": "deb"},
                    {"name": "syft:package:metadataType", "value": "dpkg"},
                ],
            }
        )

    distro_properties = [
        {"name": "syft:distro:id", "value": args.distro},
        {"name": "syft:distro:versionID", "value": args.distro_version},
    ]
    distro_ref = purl(args.distro, args.distro, args.distro_version)
    document = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": "urn:uuid:"
        + str(uuid.uuid5(uuid.NAMESPACE_URL, hashlib.sha256(raw).hexdigest())),
        "version": 1,
        "metadata": {
            "component": {
                "type": "operating-system",
                "bom-ref": distro_ref,
                "name": args.distro,
                "version": args.distro_version,
                "purl": distro_ref,
                "properties": distro_properties,
            },
            "properties": distro_properties,
        },
        "components": components,
    }

    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
