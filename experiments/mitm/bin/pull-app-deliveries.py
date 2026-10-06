#!/usr/bin/env python3
"""Pull a GitHub App's webhook delivery history into flows.jsonl.

GitHub keeps every App webhook delivery it attempted for 3 days: the exact
request headers and payload it sent, and the receiver's response. That is the
ground truth for webhook fixtures, so no proxy is needed to record inbound
traffic. Each delivery becomes one `direction: "inbound"` record in the same
layout as the mitm capture addon (see ../FORMAT.md), with the GitHub delivery
metadata under `github_delivery`.

Usage:
  pull-app-deliveries.py --app-id 123 --pem app.pem --out DIR [--limit 500]
      [--api https://api.github.com] [--event push,pull_request]

The App JWT is minted locally (RS256) from the PEM; nothing is written
unredacted.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "addons"))
from redact import redact_bytes, redact_headers, redact_json  # noqa: E402

CAPTURE_FORMAT = 1


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def app_jwt(app_id: str, pem: bytes, now: int | None = None) -> str:
    """RS256 App JWT per GitHub's rules: iat 60s in the past, exp <= 10 min."""
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    now = int(time.time()) if now is None else now
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    claims = _b64url(json.dumps({"iat": now - 60, "exp": now + 540, "iss": str(app_id)}).encode())
    signing_input = f"{header}.{claims}".encode()
    key = serialization.load_pem_private_key(pem, password=None)
    signature = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return f"{header}.{claims}.{_b64url(signature)}"


def _get(url: str, token: str) -> tuple[object, dict[str, str]]:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "preloop-capture-kit",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp), {k.lower(): v for k, v in resp.headers.items()}


def _next_link(headers: dict[str, str]) -> str | None:
    for part in headers.get("link", "").split(","):
        if 'rel="next"' in part:
            return part.split(";")[0].strip().strip("<>")
    return None


def _headers(headers: dict | None) -> list[list[str]]:
    """GitHub returns delivery headers as a JSON object; redact_headers takes a
    mapping and returns the capture addon's `[[name, value], ...]` pairs."""
    return redact_headers({k: str(v) for k, v in (headers or {}).items()})


def _response_json(response: dict) -> object:
    """Parse the receiver's response payload as JSON when its content type says
    it is one; GitHub returns the payload as a string either way."""
    payload = response.get("payload")
    if not isinstance(payload, str) or not payload:
        return None
    headers = {str(k).lower(): str(v) for k, v in (response.get("headers") or {}).items()}
    if "json" not in headers.get("content-type", ""):
        return None
    try:
        return json.loads(payload)
    except (ValueError, TypeError):
        return None


def delivery_to_record(detail: dict, index: int) -> dict:
    """Map one `GET /app/hook/deliveries/{id}` body onto a flow record."""
    url = urllib.parse.urlsplit(detail.get("url") or "")
    request = detail.get("request") or {}
    response = detail.get("response") or {}
    # Redact before every serialized form is derived: the base64 fields must
    # carry the same scrubbed bytes as the JSON fields, or exports leak the
    # credentials the JSON view was cleaned of.
    payload = redact_json(request.get("payload"))
    body = (
        redact_bytes(json.dumps(payload, separators=(",", ":")).encode())
        if payload is not None
        else b""
    )
    resp_payload = response.get("payload")
    resp_bytes = (
        redact_bytes(resp_payload.encode()) if isinstance(resp_payload, str) else b""
    )
    resp_json = redact_json(_response_json(response))
    duration = detail.get("duration")
    return {
        "capture_format": CAPTURE_FORMAT,
        "direction": "inbound",
        "flow_index": index,
        "ts_request": detail.get("delivered_at"),
        "ts_response": None,
        "duration_ms": round(duration * 1000, 3) if isinstance(duration, (int, float)) else None,
        "method": "POST",
        "scheme": url.scheme or "https",
        "host": url.hostname or "",
        "path": (url.path or "/") + (f"?{url.query}" if url.query else ""),
        "request_headers": _headers(request.get("headers")),
        "request_body_b64": base64.b64encode(body).decode(),
        "request_body_json": payload,
        "status": detail.get("status_code"),
        "response_headers": _headers(response.get("headers")),
        "response_body_b64": base64.b64encode(resp_bytes).decode(),
        "response_body_json": resp_json,
        "github_delivery": {
            "id": detail.get("id"),
            "guid": detail.get("guid"),
            "event": detail.get("event"),
            "action": detail.get("action"),
            "redelivery": detail.get("redelivery"),
            "status": detail.get("status"),
            "delivered_at": detail.get("delivered_at"),
            "installation_id": detail.get("installation_id"),
            "repository_id": detail.get("repository_id"),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--app-id", required=True)
    ap.add_argument("--pem", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--api", default=os.environ.get("GITHUB_API_URL", "https://api.github.com"))
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--event", default="", help="comma-separated event filter")
    args = ap.parse_args()

    events = {e.strip() for e in args.event.split(",") if e.strip()}
    pem = args.pem.read_bytes()
    api = args.api.rstrip("/")
    args.out.mkdir(parents=True, exist_ok=True)
    out = args.out / "flows.jsonl"

    # A minted JWT lives ~9 minutes; pagination and per-delivery detail
    # requests in a long import outlive it, so re-mint 30s before expiry
    # instead of reusing the first token until it dies mid-import.
    token: str | None = None
    token_expires_at = 0.0

    def current_token() -> str:
        nonlocal token, token_expires_at
        if token is None or time.time() >= token_expires_at - 30:
            now = int(time.time())
            token = app_jwt(args.app_id, pem, now=now)
            token_expires_at = now + 540
        return token

    written = 0
    url: str | None = f"{api}/app/hook/deliveries?per_page=100"
    with out.open("a") as f:
        while url and written < args.limit:
            page, headers = _get(url, current_token())
            for summary in page:  # newest first
                if written >= args.limit:
                    break
                if events and summary.get("event") not in events:
                    continue
                detail, _ = _get(
                    f"{api}/app/hook/deliveries/{summary['id']}", current_token()
                )
                written += 1
                f.write(json.dumps(delivery_to_record(detail, written), ensure_ascii=False) + "\n")
            url = _next_link(headers)
    print(f"wrote {written} deliveries to {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
