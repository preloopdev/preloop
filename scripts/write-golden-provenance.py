#!/usr/bin/env python3
"""Write the signed input/output manifest for one Preloop golden."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def file_record(path: Path) -> dict:
    return {
        "name": path.name,
        "sha256": sha256(path),
        "size": path.stat().st_size,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--golden", type=Path, required=True)
    parser.add_argument("--base-evidence", type=Path, required=True)
    parser.add_argument("--base-sbom", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    evidence = json.loads(args.base_evidence.read_text(encoding="utf-8"))
    sbom_evidence = evidence.get("sbom")
    if not isinstance(sbom_evidence, dict) or not sbom_evidence.get("digest"):
        fail(
            "base evidence has no SBOM record; refusing to bind an unrelated "
            "SBOM to the golden"
        )
    sbom_sha = sha256(args.base_sbom)
    if sbom_sha != sbom_evidence["digest"]:
        fail(
            f"base SBOM {args.base_sbom} sha256 {sbom_sha} does not match "
            f"the base evidence digest {sbom_evidence['digest']}"
        )
    platform = evidence["platform"]
    image = evidence["image"]
    predicate = {
        "schema_version": 1,
        "type": "https://preloop.dev/provenance/golden/v1",
        "subject": file_record(args.golden),
        "base_image": {
            "reference": image["reference"],
            "index_digest": image["index_digest"],
            "platform": f'{platform["os"]}/{platform["architecture"]}',
            "platform_manifest_digest": platform["manifest_digest"],
            "source": platform.get("annotations", {}).get(
                "org.opencontainers.image.source"
            ),
            "source_revision": platform.get("annotations", {}).get(
                "org.opencontainers.image.revision"
            ),
        },
        "base_evidence": file_record(args.base_evidence),
        "base_sbom": file_record(args.base_sbom),
        "workflow": {
            key: value
            for key, value in {
                "repository": os.environ.get("GITHUB_REPOSITORY"),
                "workflow_ref": os.environ.get("GITHUB_WORKFLOW_REF"),
                "commit": os.environ.get("GITHUB_SHA"),
                "ref": os.environ.get("GITHUB_REF"),
                "run_id": os.environ.get("GITHUB_RUN_ID"),
                "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
            }.items()
            if value
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(predicate, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(predicate, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
