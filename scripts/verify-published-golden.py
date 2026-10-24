#!/usr/bin/env python3
"""Verify a golden package published to GHCR the way the engine consumes it.

`docs/build-goldens.md` requires the published packages to be public — the
engine's default OCI path intentionally pulls without credentials — and pins
the `<tag>-linux-amd64` platform manifest rather than the `<tag>` index. A push
that leaves the package private, or that leaves `latest` pointing at a
different pack, therefore breaks every consumer even though the push itself
succeeded.

This checker resolves the tags with an *anonymous* registry token and asserts
the runbook's invariants, so a publish job fails at publication time instead of
on the next engine that tries to pull:

1. `<tag>` resolves to an OCI index that lists the platform manifest;
2. `<tag>-linux-amd64` resolves to an OCI image manifest;
3. its artifact type is `application/vnd.smolmachines.smolmachine.v1`;
4. its single layer digest is the SHA-256 of the packed payload.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import NoReturn
from urllib.parse import quote


REGISTRY = "ghcr.io"
USER_AGENT = "preloop-verify-published-golden/1"
ACCEPT = ", ".join(
    [
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
    ]
)
ARTIFACT_TYPE = "application/vnd.smolmachines.smolmachine.v1"
PLATFORM_SUFFIX = "linux-amd64"
CHUNK = 8 * 1024 * 1024


def fail(message: str) -> NoReturn:
    raise SystemExit(message)


def curl(url: str, accept: str, token: str | None = None) -> tuple[int, bytes, str]:
    """GET `url` and return (status, body, curl stderr). No credentials unless
    `token` is given, so every request here is what an anonymous consumer sees."""
    command = [
        "curl",
        "--silent",
        "--show-error",
        "--location",
        "--connect-timeout",
        "15",
        "--max-time",
        "120",
        "--user-agent",
        USER_AGENT,
        "--header",
        f"Accept: {accept}",
        "--write-out",
        "\n%{http_code}",
    ]
    if token:
        command += ["--header", f"Authorization: Bearer {token}"]
    command.append(url)
    result = subprocess.run(command, capture_output=True)
    body, _, status = result.stdout.rpartition(b"\n")
    detail = result.stderr.decode(errors="replace").strip()
    try:
        code = int(status)
    except ValueError:
        return 0, b"", detail or f"curl exited {result.returncode}"
    if code == 0:
        detail = detail or f"curl exited {result.returncode}"
    return code, body, detail


def get_json(url: str, accept: str, token: str | None = None) -> tuple[int, dict]:
    code, body, detail = curl(url, accept, token)
    if code != 200:
        return code, {}
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as error:
        fail(f"{url} did not return JSON: {error}")
    if not isinstance(payload, dict):
        fail(f"{url} did not return a JSON object")
    return code, payload


def anonymous_pull_token(repository: str) -> str:
    """A token issued to nobody in particular: it only exists if the package is
    public, which is exactly the property the runbook depends on."""
    url = (
        f"https://{REGISTRY}/token?service={REGISTRY}"
        f"&scope=repository:{quote(repository, safe='/')}:pull"
    )
    code, payload = get_json(url, "application/json")
    token = payload.get("token")
    if code != 200 or not token:
        fail(
            f"{REGISTRY}/{repository} served no anonymous pull token (HTTP {code}): "
            "the package is not publicly pullable, so every engine that resolves "
            "the golden without credentials fails. Set the package visibility to "
            "public in the organization's package settings."
        )
    return str(token)


def get_manifest(repository: str, reference: str, token: str) -> tuple[dict, str]:
    url = (
        f"https://{REGISTRY}/v2/{quote(repository, safe='/')}/manifests/"
        f"{quote(reference, safe=':')}"
    )
    code, body, detail = curl(url, ACCEPT, token)
    if code != 200:
        fail(
            f"{REGISTRY}/{repository}:{reference} did not resolve anonymously "
            f"(HTTP {code}): {detail or 'no body'}"
        )
    try:
        manifest = json.loads(body)
    except json.JSONDecodeError as error:
        fail(f"{REGISTRY}/{repository}:{reference} did not return JSON: {error}")
    digest = "sha256:" + hashlib.sha256(body).hexdigest()
    return manifest, digest


def payload_digest(path: str) -> str:
    target = Path(path)
    if not target.is_file():
        fail(f"packed payload {path} is missing")
    digest = hashlib.sha256()
    with target.open("rb") as handle:
        while chunk := handle.read(CHUNK):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repository",
        required=True,
        help=f"GHCR repository as owner/name (the {REGISTRY}/ prefix is optional)",
    )
    parser.add_argument("--tag", required=True, help="published index tag to check")
    parser.add_argument(
        "--platform-tag",
        help=f"platform manifest tag (default <tag>-{PLATFORM_SUFFIX})",
    )
    parser.add_argument(
        "--payload",
        help="packed payload whose SHA-256 must be the manifest's layer digest",
    )
    args = parser.parse_args(argv[1:])

    repository = args.repository.removeprefix(f"{REGISTRY}/")
    platform_tag = args.platform_tag or f"{args.tag}-{PLATFORM_SUFFIX}"

    index, _ = get_manifest(repository, args.tag, anonymous_pull_token(repository))
    entries = index.get("manifests")
    if not isinstance(entries, list) or not entries:
        fail(
            f"{REGISTRY}/{repository}:{args.tag} is not an OCI index "
            f"(mediaType {index.get('mediaType')!r}); the engine selects the "
            f"platform manifest from the index"
        )

    platform, platform_digest = get_manifest(repository, platform_tag, anonymous_pull_token(repository))
    if platform.get("artifactType") != ARTIFACT_TYPE:
        fail(
            f"{REGISTRY}/{repository}:{platform_tag} has artifact type "
            f"{platform.get('artifactType')!r}, want {ARTIFACT_TYPE!r}"
        )
    layers = platform.get("layers")
    if not isinstance(layers, list) or len(layers) != 1:
        fail(
            f"{REGISTRY}/{repository}:{platform_tag} carries "
            f"{len(layers) if isinstance(layers, list) else 0} layers, want the one "
            "packed payload"
        )
    if not any(entry.get("digest") == platform_digest for entry in entries):
        fail(
            f"{REGISTRY}/{repository}:{args.tag} does not list the {platform_tag} "
            f"manifest ({platform_digest}); the index and the platform tag disagree"
        )
    if args.payload:
        local = payload_digest(args.payload)
        if layers[0].get("digest") != local:
            fail(
                f"{REGISTRY}/{repository}:{platform_tag} layer digest "
                f"{layers[0].get('digest')!r} does not match {args.payload} ({local})"
            )

    print(
        f"ok {REGISTRY}/{repository}:{args.tag} -> {platform_tag} "
        f"{platform_digest} ({ARTIFACT_TYPE})"
    )
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))
