"""mitmproxy addon: write one JSON line per completed flow to $MITM_CAPTURE_DIR/flows.jsonl.
Large (>256KB) or non-JSON bodies are saved as bin files and omitted from JSONL base64."""

import base64
import hashlib
import json
import os
import sys
from pathlib import Path

from mitmproxy import http

REDACT_HEADERS = {
    "authorization", "cookie", "set-cookie",
    "x-vss-session", "x-tfs-session", "x-vss-e2eid",
}
REDACT_SUBSTRINGS = ("token",)


def _capture_dir() -> Path | None:
    d = os.environ.get("MITM_CAPTURE_DIR", "")
    if not d:
        return None
    p = Path(d)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _safe_b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _safe_json(data: bytes) -> object:
    try:
        return json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def _redact_headers(headers) -> list[list[str]]:
    out = []
    for name, value in headers.items():
        nl = name.lower()
        if nl in REDACT_HEADERS or any(s in nl for s in REDACT_SUBSTRINGS):
            out.append([name, "***REDACTED***"])
        else:
            out.append([name, value])
    return out


def _dump_flow(flow: http.HTTPFlow, index: int, cd: Path):
    request = flow.request
    response = flow.response

    content_type = request.headers.get("content-type", "")
    is_json = "json" in content_type

    req_body = request.get_content(strict=False) or b""
    req_b64 = _safe_b64(req_body)
    req_json = _safe_json(req_body) if is_json else None
    req_sha = hashlib.sha256(req_body).hexdigest()

    if response is not None:
        resp_content_type = response.headers.get("content-type", "")
        resp_is_json = "json" in resp_content_type
        resp_body = response.get_content(strict=False) or b""
        resp_b64 = _safe_b64(resp_body)
        resp_json = _safe_json(resp_body) if resp_is_json else None
        resp_sha = hashlib.sha256(resp_body).hexdigest()
        status = response.status_code
        resp_headers = _redact_headers(response.headers)
        ts_resp = response.timestamp_end or response.timestamp_start
    else:
        resp_b64 = ""
        resp_json = None
        resp_sha = ""
        status = None
        resp_headers = []
        ts_resp = None

    duration = None
    if response is not None and response.timestamp_end and request.timestamp_start:
        duration = (response.timestamp_end - request.timestamp_start) * 1000

    # Large/non-JSON bodies: save as bin files, omit base64 from JSONL
    req_large = len(req_body) > 256 * 1024 or (_safe_json(req_body) is None and req_body)
    resp_large = response and (len(resp_body) > 256 * 1024 or (_safe_json(resp_body) is None and resp_body))
    if req_large:
        (cd / f"flow.{index}.req.bin").write_bytes(req_body)
    if resp_large:
        (cd / f"flow.{index}.resp.bin").write_bytes(resp_body)

    record = {
        "flow_index": index,
        "ts_request": request.timestamp_start,
        "ts_response": ts_resp,
        "duration_ms": round(duration, 3) if duration is not None else None,
        "method": request.method,
        "scheme": request.scheme,
        "host": request.host,
        "path": request.path,
        "request_headers": _redact_headers(request.headers),
        "request_body_b64": "" if req_large else req_b64,
        "request_body_json": req_json,
        "request_body_sha256": req_sha,
        "status": status,
        "response_headers": resp_headers,
        "response_body_b64": "" if resp_large else resp_b64,
        "response_body_json": resp_json,
        "response_body_sha256": resp_sha,
    }

    with (cd / "flows.jsonl").open("a") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
        f.flush()


class Capture:
    counter: int = 0

    def request(self, flow: http.HTTPFlow):
        Capture.counter += 1
        flow.metadata["_capture_order"] = Capture.counter

    def response(self, flow: http.HTTPFlow):
        self._do_dump(flow)

    def error(self, flow: http.HTTPFlow):
        self._do_dump(flow)

    def _do_dump(self, flow: http.HTTPFlow):
        cd = _capture_dir()
        if cd is None:
            return
        index = flow.metadata.get("_capture_order", Capture.counter)
        try:
            _dump_flow(flow, index, cd)
        except Exception:
            print(f"[capture] failed to dump flow {index}", file=sys.stderr, flush=True)


addons = [Capture()]
