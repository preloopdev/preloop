"""Shared token redaction for MITM captures and golden fixtures.

The capture pipeline records real runner traffic against the official GitHub
backend, so live credentials ride in Authorization headers, job-message
bodies, and registration responses. This module is the single place that
recognizes them: capture.py redacts bodies before they are written, the
flows.mitm scrubber and scrub-goldens.py use the same rules to strip tokens
from raw streams and committed goldens.

Redaction is deliberately targeted at GitHub credential shapes found in
runner traffic rather than generic high-entropy strings: over-broad rules
would mangle legitimate capture content (hashes, base64 log bodies, the
server's own masking regexes) and hurt replay fidelity. Generic
high-entropy scrubbing stays in the report path (runner-watch compare.rs),
which never feeds replay.
"""

import re

# Header names whose values are always redacted.
REDACT_HEADERS = {
    "authorization",
    "cookie",
    "set-cookie",
    "x-vss-session",
    "x-tfs-session",
    "x-vss-e2eid",
}
# Header names containing any of these substrings -> redact the value.
REDACT_SUBSTRINGS = ("token",)

REDACTED = "***REDACTED***"

# GitHub credential shapes seen in runner traffic:
#   ghp_/gho_/ghu_/ghs_/ghr_  classic PATs, OAuth, user, installation,
#                             and refresh tokens
#   github_pat_<22>_<59>      fine-grained PATs (never matches the server's
#                             own masking regex strings: those continue with
#                             `[0-9]`, not an alphanumeric run)
#   eyJ...                    three-segment base64url JWTs (registration,
#                             OIDC, and installation-token payloads)
_TOKEN_PATTERNS = (
    # The class includes dots so the whole `ghs_<installation>_<jwt>` shape
    # is consumed in one pass instead of leaving the JWT segments behind.
    re.compile(rb"gh[sopur]_[A-Za-z0-9_.\-]{8,}"),
    re.compile(rb"github_pat_[A-Za-z0-9_]{15,}"),
    # JWT: `eyJ` header plus one or more dot-separated base64url segments.
    re.compile(rb"eyJ[A-Za-z0-9_\-]{8,}(\.[A-Za-z0-9_\-]{8,})+"),
)


def redact_bytes(data: bytes) -> bytes:
    """Redact credential shapes from raw bytes (bodies, bin dumps)."""
    for pattern in _TOKEN_PATTERNS:
        data = pattern.sub(REDACTED.encode(), data)
    return data


def redact_str(text: str) -> str:
    """Redact credential shapes from a text string."""
    for pattern in _TOKEN_PATTERNS:
        text = pattern.sub(REDACTED.encode(), text.encode()).decode()
    return text


def redact_json(value):
    """Recursively redact string values that look like credentials.

    Keys are preserved: a `github_token` key is not secret, its value is.
    """
    if isinstance(value, dict):
        return {key: redact_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_json(item) for item in value]
    if isinstance(value, str):
        return redact_str(value)
    return value


def redact_headers(headers) -> list[list[str]]:
    """Map a mitmproxy header object to a redacted ``[name, value]`` list.

    Used by capture.py for the flows.jsonl record; the flows.mitm scrubber
    mutates the header object in place instead (see scrub.py).
    """
    out = []
    for name, value in headers.items():
        nl = name.lower()
        if nl in REDACT_HEADERS or any(s in nl for s in REDACT_SUBSTRINGS):
            out.append([name, REDACTED])
        else:
            out.append([name, value])
    return out


def redact_headers_in_place(headers) -> None:
    """Overwrite sensitive header values in a mitmproxy Headers object."""
    for name in list(headers.keys()):
        nl = name.lower()
        if nl in REDACT_HEADERS or any(s in nl for s in REDACT_SUBSTRINGS):
            headers[name] = REDACTED
