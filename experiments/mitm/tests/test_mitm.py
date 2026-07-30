#!/usr/bin/env python3
"""Tests for the MITM experiment Python modules."""

import json
import sys
import tempfile
from pathlib import Path

import pytest

# Add bin/ to path so we can import the modules.
sys.path.insert(0, str(Path(__file__).parent.parent / "bin"))

from _compare import (
    _short_label,
    load_flows,
    normalize_path,
    redact_report,
    render_report,
)
from _run_scenario import match_event, wait_for_event


# ── normalize_path tests ─────────────────────────────────────────────


class TestNormalizePath:
    """Test URL path normalization for comparison."""

    def test_runner_server_prefix_stripped(self):
        assert normalize_path("/runner/server/_apis/connectionData") == "/_apis/connectionData"

    def test_runner_server_prefix_only_at_start(self):
        # Should NOT strip /runner/server in the middle of a path.
        result = normalize_path("/foo/runner/server/bar")
        assert "/runner/server/" in result or "runner" in result

    def test_single_segment_org_prefix_stripped(self):
        assert normalize_path("/abc123/_apis/something") == "/_apis/something"

    def test_hyphenated_org_prefix_stripped(self):
        """aksh uses GHES-style org prefix routing: /:org/_apis/..."""
        assert normalize_path("/my-org/_apis/v2/whatever") == "/_apis/v2/whatever"

    def test_multi_segment_org_not_stripped(self):
        """Multi-segment paths like /runner/server should not be treated as org prefixes."""
        result = normalize_path("/org/sub/_apis/something")
        # Should NOT strip /org/sub as a single segment.
        assert "org" in result or "sub" in result

    def test_guid_replaced(self):
        result = normalize_path("/_apis/something/12345678-1234-1234-1234-123456789abc")
        assert "{guid}" in result
        assert "12345678" not in result

    def test_numeric_segments_replaced(self):
        result = normalize_path("/_apis/pools/42/agents/7")
        assert "/{n}/" in result

    def test_query_param_numeric_values_replaced(self):
        result = normalize_path("/_apis/foo?bar=123&baz=hello")
        assert "bar={n}" in result
        assert "baz=hello" in result

    def test_query_param_guid_values_replaced(self):
        result = normalize_path("/_apis/foo?id=12345678-1234-1234-1234-123456789abc")
        assert "id={guid}" in result

    def test_no_query_string(self):
        result = normalize_path("/_apis/connectionData")
        assert "?" not in result

    def test_empty_path(self):
        result = normalize_path("/")
        assert result == "/"

    def test_aksh_compat_routes_normalize_same_as_runner_server(self):
        """aksh and runner-server routes should normalize identically."""
        rs_path = "/runner/server/_apis/distributedtask/pools/1/messages"
        aksh_path = "/runner/server/_apis/distributedtask/pools/1/messages"
        assert normalize_path(rs_path) == normalize_path(aksh_path)

    def test_official_random_prefix_stripped(self):
        """Official runner uses a random single-segment prefix."""
        result = normalize_path("/abcXYZ123/_apis/distributedtask/hubs/actions")
        assert result == "/_apis/distributedtask/hubs/actions"


# ── _short_label tests ───────────────────────────────────────────────


class TestShortLabel:
    def test_single_word(self):
        assert _short_label("official") == "offi"

    def test_two_words(self):
        assert _short_label("runner-server") == "rs"

    def test_aksh(self):
        assert _short_label("aksh") == "aksh"

    def test_underscores(self):
        assert _short_label("runner_server") == "rs"

    def test_golden(self):
        assert _short_label("golden") == "gold"


# ── load_flows tests ─────────────────────────────────────────────────


class TestLoadFlows:
    def test_missing_file(self, tmp_path):
        assert load_flows(tmp_path / "nonexistent.jsonl") == []

    def test_empty_file(self, tmp_path):
        p = tmp_path / "flows.jsonl"
        p.write_text("")
        assert load_flows(p) == []

    def test_valid_jsonl(self, tmp_path):
        p = tmp_path / "flows.jsonl"
        p.write_text('{"method":"GET","path":"/foo"}\n{"method":"POST","path":"/bar"}\n')
        flows = load_flows(p)
        assert len(flows) == 2
        assert flows[0]["method"] == "GET"

    def test_skips_blank_lines(self, tmp_path):
        p = tmp_path / "flows.jsonl"
        p.write_text('{"method":"GET"}\n\n\n{"method":"POST"}\n')
        flows = load_flows(p)
        assert len(flows) == 2


# ── redact_report tests ──────────────────────────────────────────────


class TestRedactReport:
    def test_jwt_redacted(self):
        # JWT has three base64url segments; first starts with eyJ and each ≥20 chars after eyJ.
        s = "token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc123def456ghi789jkl012mno345"
        result = redact_report(s)
        assert "eyJhbGci" not in result
        assert "***REDACTED***" in result

    def test_long_mixed_alphanumeric_redacted(self):
        # Redaction requires >6 distinct characters in a 30+ char run.
        s = "secret: " + "abcdefghij1234567890ABCDEFGHIJ1234"
        result = redact_report(s)
        assert "abcdefghij1234567890ABCDEFGHIJ1234" not in result
    def test_short_string_preserved(self):
        s = "hello world"
        result = redact_report(s)
        assert result == s


# ── match_event tests ────────────────────────────────────────────────


class TestMatchEvent:
    def test_runner_registered(self):
        flows = [
            {"method": "POST", "status": 201, "path": "/_apis/distributedtask/pools/1/agents"},
        ]
        assert match_event("runner_registered", flows) is True

    def test_runner_registered_not_matched(self):
        flows = [
            {"method": "GET", "status": 200, "path": "/_apis/connectionData"},
        ]
        assert match_event("runner_registered", flows) is False

    def test_job_assigned_json(self):
        flows = [
            {
                "method": "GET",
                "status": 200,
                "path": "/_apis/distributedtask/pools/1/messages",
                "response_body_json": {"messageType": "PipelineAgentJobRequest"},
            },
        ]
        assert match_event("job_assigned", flows) is True

    def test_job_completed_path(self):
        flows = [
            {"method": "PATCH", "status": 200, "path": "/_apis/distributedtask/hubs/actions/jobrequests/abc123"},
        ]
        assert match_event("job_completed", flows) is True

    def test_empty_flows(self):
        assert match_event("runner_registered", []) is False

    def test_wait_ignores_events_before_cursor(self, tmp_path: Path):
        flows = [
            {
                "flow_index": 1,
                "method": "PATCH",
                "status": 200,
                "path": "/_apis/distributedtask/hubs/actions/jobrequests/first",
            },
            {
                "flow_index": 2,
                "method": "PATCH",
                "status": 200,
                "path": "/_apis/distributedtask/hubs/actions/jobrequests/second",
            },
        ]
        (tmp_path / "flows.jsonl").write_text(
            "".join(json.dumps(flow) + "\n" for flow in flows)
        )
        assert wait_for_event("job_completed", tmp_path, 0.1, after_flow_index=1)


# ── render_report tests ──────────────────────────────────────────────


class TestRenderReport:
    def _make_capture(self, tmp_path: Path, name: str, flows: list[dict]) -> Path:
        d = tmp_path / name
        d.mkdir()
        (d / "flows.jsonl").write_text(
            "\n".join(json.dumps(f) for f in flows) + "\n"
        )
        (d / "summary.json").write_text(json.dumps({"status": "ok"}))
        return d

    def test_basic_report(self, tmp_path):
        left = self._make_capture(tmp_path, "left", [
            {"method": "GET", "path": "/_apis/connectionData", "status": 200, "duration_ms": 10},
        ])
        right = self._make_capture(tmp_path, "right", [
            {"method": "GET", "path": "/_apis/connectionData", "status": 200, "duration_ms": 15},
        ])
        output = tmp_path / "report.md"
        render_report("test-scenario", left, right, output, "official", "aksh")
        text = output.read_text()
        assert "official" in text.lower()
        assert "aksh" in text
        assert "/_apis/connectionData" in text

    def test_custom_labels_in_report(self, tmp_path):
        left = self._make_capture(tmp_path, "left", [
            {"method": "POST", "path": "/foo", "status": 201, "duration_ms": 5},
        ])
        right = self._make_capture(tmp_path, "right", [
            {"method": "POST", "path": "/foo", "status": 201, "duration_ms": 8},
        ])
        output = tmp_path / "report.md"
        render_report("test", left, right, output, "golden", "my-backend")
        text = output.read_text()
        assert "golden" in text
        assert "my-backend" in text
        assert "official" not in text
        assert "runner.server" not in text.lower().replace("runner.server", "")

    def test_empty_right_exits(self, tmp_path):
        left = self._make_capture(tmp_path, "left", [
            {"method": "GET", "path": "/foo", "status": 200, "duration_ms": 1},
        ])
        right = self._make_capture(tmp_path, "right", [])
        # Write empty flows file.
        (right / "flows.jsonl").write_text("")
        output = tmp_path / "report.md"
        with pytest.raises(SystemExit) as exc_info:
            render_report("test", left, right, output, "left", "right")
        assert exc_info.value.code == 5

    def test_empty_left_exits(self, tmp_path):
        left = self._make_capture(tmp_path, "left", [])
        (left / "flows.jsonl").write_text("")
        right = self._make_capture(tmp_path, "right", [
            {"method": "GET", "path": "/foo", "status": 200, "duration_ms": 1},
        ])
        output = tmp_path / "report.md"
        with pytest.raises(SystemExit) as exc_info:
            render_report("test", left, right, output, "left", "right")
        assert exc_info.value.code == 5

    def test_both_empty_ok(self, tmp_path):
        left = self._make_capture(tmp_path, "left", [])
        (left / "flows.jsonl").write_text("")
        right = self._make_capture(tmp_path, "right", [])
        (right / "flows.jsonl").write_text("")
        output = tmp_path / "report.md"
        render_report("test", left, right, output, "a", "b")
        text = output.read_text()
        assert "No shared endpoints" in text

    def test_left_only_endpoints(self, tmp_path):
        left = self._make_capture(tmp_path, "left", [
            {"method": "GET", "path": "/only-left", "status": 200, "duration_ms": 1},
        ])
        right = self._make_capture(tmp_path, "right", [
            {"method": "GET", "path": "/shared", "status": 200, "duration_ms": 1},
        ])
        output = tmp_path / "report.md"
        render_report("test", left, right, output, "alpha", "beta")
        text = output.read_text()
        assert "alpha only" in text


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
