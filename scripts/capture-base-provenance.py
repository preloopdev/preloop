#!/usr/bin/env python3
"""Capture the immutable OCI evidence used to build a Preloop golden.

The stock Ubuntu image is served from mirror.gcr.io, which is a Docker Hub
cache rather than a Google-built image. This helper records the digest-pinned
index, the selected platform manifest, and any OCI attestation manifests
attached to that platform. When the image exposes an SPDX attestation, its
SBOM blob is copied locally as well.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import NoReturn
from urllib.parse import quote


ACCEPT_MANIFEST = ", ".join(
    [
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    ]
)
USER_AGENT = "preloop-base-provenance/1"


def fail(message: str) -> NoReturn:
    raise SystemExit(message)


def digest_bytes(body: bytes) -> str:
    return "sha256:" + hashlib.sha256(body).hexdigest()


def parse_image(reference: str) -> tuple[str, str, str, str]:
    name, separator, digest = reference.rpartition("@")
    if not separator or not digest.startswith("sha256:"):
        fail(f"{reference!r} must be pinned with @sha256:<digest>")
    if len(digest.removeprefix("sha256:")) != 64 or any(
        character not in "0123456789abcdef"
        for character in digest.removeprefix("sha256:")
    ):
        fail(f"{reference!r} has an invalid sha256 digest")

    parts = name.split("/")
    if len(parts) == 1 or (
        "." not in parts[0] and ":" not in parts[0] and parts[0] != "localhost"
    ):
        registry = "docker.io"
        repository = "/".join(parts if len(parts) > 1 else ["library", parts[0]])
    else:
        registry = parts[0]
        repository = "/".join(parts[1:])
    if not repository:
        fail(f"{reference!r} has no repository path")
    repository_parts = repository.rsplit("/", 1)
    if ":" in repository_parts[-1]:
        repository_parts[-1] = repository_parts[-1].split(":", 1)[0]
        repository = "/".join(repository_parts)
    return registry, repository, digest, name


def manifest_url(registry: str, repository: str, reference: str) -> str:
    return (
        f"https://{registry}/v2/{quote(repository, safe='/')}/manifests/"
        f"{quote(reference, safe=':@')}"
    )


def blob_url(registry: str, repository: str, digest: str) -> str:
    return (
        f"https://{registry}/v2/{quote(repository, safe='/')}/blobs/"
        f"{quote(digest, safe=':')}"
    )


def referrers_url(registry: str, repository: str, subject_digest: str) -> str:
    return (
        f"https://{registry}/v2/{quote(repository, safe='/')}/referrers/"
        f"{quote(subject_digest, safe=':')}"
    )


def fetch(url: str, accept: str, allow_404: bool = False) -> bytes:
    auth_args: list[str] = []
    # GHCR requires a registry-scoped token (not the raw credential): exchange
    # it against the token endpoint. Try the configured credential first, then
    # the anonymous exchange — public packages serve anonymous tokens, and a
    # workflow token may legitimately lack access to a package linked to a
    # different repository.
    registry = url.split("/")[2]
    path = url.split("/v2/", 1)[1]
    for marker in ("/manifests/", "/blobs/", "/referrers/"):
        if marker in path:
            path = path.split(marker, 1)[0]
            break
    repo = path
    exchange_url = (
        f"https://{registry}/token?service={registry}"
        f"&scope=repository:{repo}:pull"
    )
    token = os.environ.get("GHCR_TOKEN") or os.environ.get("GITHUB_TOKEN")
    credentials = ([("x-access-token", token)] if token else []) + [None]
    registry_token = None
    for credential in credentials:
        cmd = [
            "curl",
            "--fail",
            "--silent",
            "--show-error",
            "--location",
        ]
        if credential:
            cmd += ["--user", f"{credential[0]}:{credential[1]}"]
        cmd += [exchange_url]
        try:
            exchange = subprocess.run(cmd, check=True, capture_output=True)
            registry_token = json.loads(exchange.stdout).get("token")
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            registry_token = None
        if registry_token:
            break
    auth_args = (
        ["--header", f"Authorization: Bearer {registry_token}"]
        if registry_token
        else []
    )
    try:
        result = subprocess.run(
            [
                "curl",
                "--fail",
                "--silent",
                "--show-error",
                "--location",
                "--retry",
                "4",
                "--retry-all-errors",
                "--connect-timeout",
                "15",
                "--max-time",
                "120",
                "--header",
                f"Accept: {accept}",
                *auth_args,
                "--user-agent",
                USER_AGENT,
                url,
            ],
            check=True,
            capture_output=True,
        )
    except FileNotFoundError:
        fail("curl is required to capture OCI base provenance")
    except subprocess.CalledProcessError as error:
        if allow_404 and b"404" in error.stderr:
            return b""
        detail = error.stderr.decode("utf-8", "replace").strip()
        fail(f"registry request failed for {url}: {detail}")
    return result.stdout


def fetch_manifest(registry: str, repository: str, digest: str) -> tuple[dict, str]:
    body = fetch(manifest_url(registry, repository, digest), ACCEPT_MANIFEST)
    actual = digest_bytes(body)
    if actual != digest:
        fail(f"registry returned {actual} for requested manifest {digest}")
    try:
        return json.loads(body), actual
    except json.JSONDecodeError as error:
        fail(f"registry returned invalid JSON for {digest}: {error}")


def descriptor_summary(descriptor: dict) -> dict:
    annotations = descriptor.get("annotations") or {}
    platform = descriptor.get("platform") or {}
    return {
        key: value
        for key, value in {
            "digest": descriptor.get("digest"),
            "media_type": descriptor.get("mediaType"),
            "size": descriptor.get("size"),
            "platform": platform or None,
            "annotations": annotations or None,
            "reference_type": annotations.get("vnd.docker.reference.type"),
            "reference_digest": annotations.get("vnd.docker.reference.digest"),
        }.items()
        if value is not None
    }


def select_platform_descriptor(index: dict, os_name: str, architecture: str) -> dict:
    for descriptor in index.get("manifests", []):
        platform = descriptor.get("platform") or {}
        annotations = descriptor.get("annotations") or {}
        if (
            platform.get("os") == os_name
            and platform.get("architecture") == architecture
            and annotations.get("vnd.docker.reference.type") != "attestation-manifest"
        ):
            return descriptor
    fail(f"no {os_name}/{architecture} platform manifest found in the base index")


def fetch_attestation(
    registry: str, repository: str, descriptor: dict
) -> tuple[dict, str]:
    digest = descriptor.get("digest")
    if not isinstance(digest, str):
        fail("attestation descriptor has no digest")
    return fetch_manifest(registry, repository, digest)


def is_attestation_descriptor(descriptor: dict, subject_digest: str) -> bool:
    annotations = descriptor.get("annotations") or {}
    return (
        annotations.get("vnd.docker.reference.type") == "attestation-manifest"
        and annotations.get("vnd.docker.reference.digest") == subject_digest
    )


def fetch_referrers(
    registry: str, repository: str, subject_digest: str
) -> list[dict]:
    body = fetch(
        referrers_url(registry, repository, subject_digest),
        ACCEPT_MANIFEST,
        allow_404=True,
    )
    if not body:
        # Registries return 404 for subjects without referrers (or without
        # referrers support); that means "none", not an error.
        return []
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as error:
        fail(f"referrers response for {subject_digest} is not valid JSON: {error}")
    manifests = payload.get("manifests", [])
    if not isinstance(manifests, list):
        fail(f"referrers response for {subject_digest} has no manifests list")
    return [
        descriptor
        for descriptor in manifests
        if is_attestation_descriptor(descriptor, subject_digest)
    ]


def fetch_cosign_attestation(
    registry: str, repository: str, subject_digest: str
) -> list[dict]:
    # cosign stores attestations in the tag schema (`sha256-<digest>.att`)
    # rather than the OCI referrers API; GHCR does not index those as
    # referrers, so discover the tag directly.
    tag = "sha256-" + subject_digest.removeprefix("sha256:") + ".att"
    body = fetch(
        manifest_url(registry, repository, tag), ACCEPT_MANIFEST, allow_404=True
    )
    if not body:
        return []
    try:
        return [json.loads(body)]
    except json.JSONDecodeError:
        return []


def copy_spdx_blob(
    registry: str,
    repository: str,
    layer: dict,
    output: Path,
) -> dict:
    digest = layer.get("digest")
    if not isinstance(digest, str):
        fail("SPDX layer has no digest")
    body = fetch(blob_url(registry, repository, digest), "application/json")
    actual = digest_bytes(body)
    if actual != digest:
        fail(f"registry returned {actual} for SPDX blob {digest}")
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as error:
        fail(f"SPDX blob {digest} is not valid JSON: {error}")
    # cosign wraps the statement in a DSSE envelope: the payload is the
    # base64-encoded in-toto statement.
    if isinstance(parsed, dict) and isinstance(parsed.get("payload"), str):
        try:
            payload = base64.b64decode(parsed["payload"])
        except (ValueError, TypeError) as error:
            fail(f"SPDX blob {digest} has an undecodable DSSE payload: {error}")
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as error:
            fail(f"SPDX blob {digest} has a non-JSON DSSE payload: {error}")
    # BuildKit attaches the SPDX document as the predicate of an in-toto
    # statement; consumers of the sidecar expect a bare SPDX JSON document,
    # so unwrap the predicate before publishing it.
    if isinstance(parsed, dict) and isinstance(parsed.get("predicate"), dict):
        statement_type = parsed.get("_type")
        if not isinstance(statement_type, str) or not statement_type.startswith(
            "https://in-toto.io/Statement/"
        ):
            fail(
                f"SPDX blob {digest} has a predicate but is not an in-toto "
                "statement; refusing to guess its structure"
            )
        parsed = parsed["predicate"]
    elif isinstance(parsed, dict) and isinstance(parsed.get("predicate"), str):
        # cosign embeds the SPDX document as a JSON string predicate.
        try:
            parsed = json.loads(parsed["predicate"])
        except json.JSONDecodeError as error:
            fail(f"SPDX blob {digest} has a non-JSON string predicate: {error}")
    body = json.dumps(parsed, indent=2, sort_keys=True).encode("utf-8")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(body)
    return {
        "path": output.name,
        # The digest of the published sidecar (post-unwrap); the registry
        # blob digest is kept as `source_digest` for the integrity check.
        "digest": digest_bytes(body),
        "source_digest": digest,
        "size": len(body),
        "media_type": layer.get("mediaType"),
        "predicate_type": (layer.get("annotations") or {}).get(
            "in-toto.io/predicate-type"
        )
        or (layer.get("annotations") or {}).get("predicateType"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--platform", required=True, help="for example linux/amd64")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sbom-output", type=Path, required=True)
    parser.add_argument("--require-sbom", action="store_true")
    parser.add_argument("--require-source-prefix")
    args = parser.parse_args()

    try:
        os_name, architecture = args.platform.split("/", 1)
    except ValueError:
        fail(f"invalid platform {args.platform!r}; expected OS/ARCH")
    if not os_name or not architecture:
        fail(f"invalid platform {args.platform!r}; expected OS/ARCH")

    registry, repository, index_digest, image_name = parse_image(args.image)
    index, index_hash = fetch_manifest(registry, repository, index_digest)
    platform_descriptor = select_platform_descriptor(index, os_name, architecture)
    platform_digest = platform_descriptor.get("digest")
    if not isinstance(platform_digest, str):
        fail("platform descriptor has no digest")
    platform_manifest, platform_hash = fetch_manifest(
        registry, repository, platform_digest
    )

    source_annotations = platform_descriptor.get("annotations") or {}
    # Composite descriptors often drop annotations; the platform manifest
    # itself carries them (org.opencontainers.image.source/revision/...).
    if not source_annotations:
        source_annotations = platform_manifest.get("annotations") or {}
    source = source_annotations.get("org.opencontainers.image.source")
    if args.require_source_prefix:
        prefix = args.require_source_prefix
        # Match the prefix itself or a path below it (`/` boundary) so a
        # lookalike URL sharing the prefix string is not accepted.
        matches = isinstance(source, str) and (
            source == prefix or source.startswith(prefix + "/")
        )
        if not matches:
            fail(
                "base source annotation does not match the required prefix: "
                f"{source!r} (expected {prefix!r})"
            )

    # Collect attestation manifests from every discovery mechanism: the OCI
    # referrers API, attestation descriptors listed in the index, and
    # cosign's tag-schema attestation (sha256-<digest>.att).
    attestation_manifests: list[tuple[dict, str | None]] = []
    seen: set[str] = set()
    for descriptor in [
        *fetch_referrers(registry, repository, platform_digest),
        *[
            descriptor
            for descriptor in index.get("manifests", [])
            if is_attestation_descriptor(descriptor, platform_digest)
        ],
    ]:
        digest = descriptor.get("digest")
        if not isinstance(digest, str) or digest in seen:
            continue
        seen.add(digest)
        manifest, manifest_hash = fetch_attestation(registry, repository, descriptor)
        attestation_manifests.append((manifest, manifest_hash))
    for manifest in fetch_cosign_attestation(registry, repository, platform_digest):
        key = digest_bytes(json.dumps(manifest, sort_keys=True).encode("utf-8"))
        if key in seen:
            continue
        seen.add(key)
        attestation_manifests.append((manifest, None))

    attestations = []
    sbom = None
    for manifest, manifest_hash in attestation_manifests:
        layers = []
        for layer in manifest.get("layers", []):
            annotations = layer.get("annotations") or {}
            # cosign uses a bare `predicateType` annotation; BuildKit and
            # GitHub's attestation API use the in-toto namespaced key.
            predicate_type = annotations.get("in-toto.io/predicate-type") or (
                annotations.get("predicateType")
            )
            summary = {
                key: value
                for key, value in {
                    "digest": layer.get("digest"),
                    "media_type": layer.get("mediaType"),
                    "size": layer.get("size"),
                    "predicate_type": predicate_type,
                    "annotations": annotations or None,
                }.items()
                if value is not None
            }
            layers.append(summary)
            if (
                sbom is None
                and predicate_type == "https://spdx.dev/Document"
            ):
                sbom = copy_spdx_blob(
                    registry,
                    repository,
                    layer,
                    args.sbom_output,
                )
        attestations.append(
            {
                "descriptor": descriptor_summary(
                    {
                        "mediaType": manifest.get("mediaType"),
                        "size": None,
                        "digest": manifest_hash,
                        "platform": None,
                        "annotations": manifest.get("annotations"),
                    }
                ),
                "manifest_digest": manifest_hash,
                "manifest_sha256": manifest_hash,
                "layers": layers,
            }
        )

    if args.require_sbom and sbom is None:
        fail(
            f"base {args.image} has no SPDX attestation for "
            f"{os_name}/{architecture}"
        )

    evidence = {
        "schema_version": 1,
        "image": {
            "reference": args.image,
            "registry": registry,
            "repository": repository,
            "name_without_digest": image_name,
            "index_digest": index_digest,
            "index_sha256": index_hash,
            "media_type": index.get("mediaType"),
        },
        "platform": {
            "os": os_name,
            "architecture": architecture,
            "descriptor": descriptor_summary(platform_descriptor),
            "manifest_digest": platform_digest,
            "manifest_sha256": platform_hash,
            "media_type": platform_manifest.get("mediaType"),
            "annotations": source_annotations,
        },
        "attestations": attestations,
        "sbom": sbom,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(evidence, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
