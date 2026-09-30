"""Tests for the reusable capture kit: host allowlists, corpus extraction,
and GitHub App delivery-history mapping."""

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "addons"))

from hosts import host_selected, parse_allowlist  # noqa: E402


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), ROOT / "bin" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


extract_hosts = _load("extract-hosts")
pull_deliveries = _load("pull-app-deliveries")


class TestHostAllowlist:
    def test_empty_allowlist_selects_everything(self):
        assert host_selected("broker.actions.githubusercontent.com", [])

    def test_exact_host_does_not_match_subdomains(self):
        allow = parse_allowlist("github.com")
        assert host_selected("GitHub.com", allow)
        assert not host_selected("api.github.com", allow)

    def test_leading_dot_matches_domain_and_subdomains_only(self):
        allow = parse_allowlist(" .github.com , codeload.github.com")
        assert host_selected("github.com", allow)
        assert host_selected("api.github.com", allow)
        assert not host_selected("notgithub.com", allow)
        assert not host_selected("github.com.evil.test", allow)


class TestExtractHosts:
    def test_keeps_selected_hosts_per_scenario_and_drops_dns_probes(self, tmp_path):
        scenario = tmp_path / "golden" / "10-checkout"
        scenario.mkdir(parents=True)
        records = [
            {"host": "api.github.com", "path": "/actions/runner-registration", "status": 200},
            {"host": "github.com", "path": "/_dns", "status": 200},
            {"host": "broker.actions.githubusercontent.com", "path": "/message", "status": 200},
            {"host": "codeload.github.com", "path": "/actions/checkout/tar.gz/abc", "status": 200},
        ]
        (scenario / "flows.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))

        counts = extract_hosts.extract(
            [tmp_path / "golden"],
            parse_allowlist("api.github.com,github.com,codeload.github.com"),
            tmp_path / "out",
        )

        assert counts == {"10-checkout": 2}
        kept = [json.loads(l) for l in (tmp_path / "out" / "10-checkout" / "flows.jsonl").read_text().splitlines()]
        assert [r["host"] for r in kept] == ["api.github.com", "codeload.github.com"]

    def test_scenarios_without_matches_write_nothing(self, tmp_path):
        scenario = tmp_path / "golden" / "01-idle"
        scenario.mkdir(parents=True)
        (scenario / "flows.jsonl").write_text(json.dumps({"host": "broker.actions.githubusercontent.com", "path": "/x"}) + "\n")
        assert extract_hosts.extract([tmp_path / "golden"], ["github.com"], tmp_path / "out") == {}
        assert not (tmp_path / "out").exists()


class TestDeliveryMapping:
    DETAIL = {
        "id": 42,
        "guid": "0b989ba4-242f-11e5-81e1-c7b6966d2516",
        "delivered_at": "2026-09-08T12:00:00Z",
        "redelivery": True,
        "duration": 10.01,
        "status": "timed out",
        "status_code": 504,
        "event": "push",
        "action": None,
        "installation_id": 7,
        "repository_id": 9,
        "url": "https://cpane.example/api/v1/github/webhooks?x=1",
        "request": {
            "headers": {
                "X-GitHub-Event": "push",
                "X-GitHub-Delivery": "0b989ba4-242f-11e5-81e1-c7b6966d2516",
                "X-Hub-Signature-256": "sha256=deadbeef",
            },
            "payload": {"ref": "refs/heads/main", "installation": {"id": 7}},
        },
        "response": {"headers": {"Content-Type": "text/plain"}, "payload": "upstream timed out"},
    }

    def test_delivery_becomes_inbound_flow_with_github_metadata(self):
        record = pull_deliveries.delivery_to_record(self.DETAIL, 3)
        assert record["capture_format"] == 1
        assert record["direction"] == "inbound"
        assert (record["method"], record["host"], record["path"]) == (
            "POST",
            "cpane.example",
            "/api/v1/github/webhooks?x=1",
        )
        assert record["status"] == 504
        assert record["duration_ms"] == 10010.0
        assert record["request_body_json"] == {"ref": "refs/heads/main", "installation": {"id": 7}}
        assert record["github_delivery"]["guid"] == "0b989ba4-242f-11e5-81e1-c7b6966d2516"
        assert record["github_delivery"]["redelivery"] is True
        names = [name.lower() for name, _ in record["request_headers"]]
        assert "x-github-delivery" in names

    def test_app_jwt_follows_github_claim_rules(self):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        token = pull_deliveries.app_jwt("123", pem, now=1_000_000)
        import base64

        header, claims, _ = token.split(".")
        pad = lambda s: s + "=" * (-len(s) % 4)  # noqa: E731
        assert json.loads(base64.urlsafe_b64decode(pad(header)))["alg"] == "RS256"
        body = json.loads(base64.urlsafe_b64decode(pad(claims)))
        assert body["iss"] == "123"
        assert body["iat"] < 1_000_000
        assert body["exp"] - body["iat"] <= 600
