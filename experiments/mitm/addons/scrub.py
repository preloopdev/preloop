"""Offline mitmproxy addon that strips live credentials from a raw capture.

Run against a recorded stream without a listener::

    mitmdump --quiet -r flows.mitm -w flows.scrubbed.mitm -s scrub.py

Every flow's headers and bodies are redacted in place; mitmdump writes the
scrubbed flows to ``-w``. Bodies are redacted through the decoded ``content``
setter so gzip-encoded bodies are covered too, and mitmdump re-encodes with
the original content-encoding, keeping the stream replay-compatible
(``replay.sh`` answers the runner from these responses, and credentials are
opaque strings to it).

This is the commit boundary: ``record-golden.sh`` runs it over a capture
before copying it into ``.runner-watch/golden/``.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mitmproxy import http  # noqa: E402

import redact  # noqa: E402


def _scrub_message(message) -> None:
    if message is None:
        return
    redact.redact_headers_in_place(message.headers)
    if message.content:
        message.content = redact.redact_bytes(message.content)


def request(flow: http.HTTPFlow) -> None:
    _scrub_message(flow.request)


def response(flow: http.HTTPFlow) -> None:
    _scrub_message(flow.response)
