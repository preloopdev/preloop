from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parents[1]


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


bench = load("virtualization_bench", ROOT / "bench.py")
common = load("virtualization_workloads", ROOT / "workloads" / "common.py")
base = load("virtualization_adapter_base", ROOT / "adapters" / "base.py")


class RuntimeTests(unittest.TestCase):
    def test_event_log_writes_real_json_lines(self):
        manifest = bench.load_manifest(ROOT / "campaign.example.toml")
        record = bench.event_record(
            manifest,
            phase="test",
            arm="smolvm",
            event_kind="complete",
            success=True,
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "events.jsonl"
            bench.EventLog(path).append(record)
            lines = path.read_text().splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0])["event_kind"], "complete")
            with self.assertRaises(ValueError):
                bench.EventLog(path).append(record)

    def test_adapter_writer_redacts_token_and_rejects_unknown_units(self):
        with tempfile.TemporaryDirectory() as td:
            writer = base.EventWriter(Path(td) / "events.jsonl")
            event = base.LifecycleEvent(
                manifest_sha256="m",
                campaign="c",
                phase="p",
                arm="a",
                repetition=0,
                event_kind="complete",
                success=True,
                diagnostic="bearer super-secret",
                units={"duration": "ms"},
            )
            row = writer.write(event)
            self.assertNotIn("super-secret", json.dumps(row))
            with self.assertRaises(base.AdapterError):
                base.validate_events(
                    [dict(row, event_kind="sample", units={"duration": "bogus"})]
                )

    def test_all_synthetic_workloads_are_bounded_and_deterministic(self):
        for kind in common.SUPPORTED:
            if kind == "network":
                continue
            if kind in {"checkout-build", "docker", "service-container"}:
                continue
            first = common.run(kind, 4)
            second = common.run(kind, 4)
            self.assertEqual(first, second)
            self.assertEqual(len(first["sha256"]), 64)

    def test_hypeman_uses_firecracker_api_without_body_token(self):
        from benchmarks.virtualization.adapters import hypeman
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            binary = root / "hypeman"
            config = root / "config.yaml"
            binary.write_bytes(b"hypeman-test")
            config.write_text("hypervisor: firecracker\n")
            calls = []

            def transport(method, path, payload):
                calls.append((method, path, payload))
                if method == "GET" and path.startswith("/instances?"):
                    return [{"name": "bench-c-1"}]
                return {"ok": True}

            adapter = hypeman.HypemanFirecrackerAdapter(
                {
                    "campaign": "c",
                    "base_url": "http://127.0.0.1:4973",
                    "token": "secret-token",
                    "hypervisor": "firecracker",
                    "binary_path": str(binary),
                    "binary_sha256": base.sha256_file(binary),
                    "config_path": str(config),
                    "config_sha256": base.sha256_file(config),
                    "image": "sha256:image",
                },
                transport=transport,
            )
            adapter.create({"vm_id": "bench-c-1", "resource_profile": {"memory_mib": 1024}})
            adapter.prepare_snapshot({"vm_id": "bench-c-1"})
            adapter.list_owned()
            self.assertEqual(calls[0][1], "/instances")
            self.assertNotIn("secret-token", json.dumps(calls))
            self.assertEqual(calls[0][2]["health_check"]["type"], "exec")
            self.assertEqual(
                calls[0][2]["health_check"]["exec"]["command"],
                ["/bin/sh", "-c", "true"],
            )
            self.assertTrue(any(path.endswith("/standby") for _, path, _ in calls))

    def test_smolvm_exec_uses_current_guest_agent_cli(self):
        from benchmarks.virtualization.adapters import smolvm

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            binary = root / "smolvm"
            config = root / "config.yaml"
            binary.write_bytes(b"smolvm-test")
            config.write_text("seccomp: enforce\n")
            calls = []

            def runner(argv):
                calls.append(argv)
                return {"returncode": 0, "stdout": "ready"}

            adapter = smolvm.SmolVMAdapter(
                {
                    "campaign": "c",
                    "binary": str(binary),
                    "binary_sha256": base.sha256_file(binary),
                    "config_path": str(config),
                    "config_sha256": base.sha256_file(config),
                    "data_dir": str(root / "data"),
                    "image": "sha256:image",
                },
                runner=runner,
            )
            adapter.exec({"vm_id": "bench-c-1"}, ["/bin/sh", "-c", "true"])
            self.assertEqual(
                calls[0],
                [str(binary), "machine", "exec", "--name", "bench-c-1",
                 "--", "/bin/sh", "-c", "true"],
            )
            self.assertNotIn("--user", calls[0])

    def test_collector_marks_missing_counters_and_large_gaps(self):
        from benchmarks.virtualization import collect

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "proc").mkdir()
            (root / "sys").mkdir()
            (root / "proc" / "stat").write_text("cpu 1 2 3 4\n")
            (root / "proc" / "meminfo").write_text("MemAvailable: 100 kB\n")
            (root / "proc" / "loadavg").write_text("0 0 0 1/1 1\n")
            (root / "proc" / "net").mkdir()
            (root / "proc" / "net" / "dev").write_text(
                "Inter-| Receive | Transmit\n"
                " face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n"
            )
            sample = collect.collect_host(root / "proc", root / "sys")
            self.assertEqual(sample["memory_bytes"]["MemAvailable"], 102400)
            self.assertEqual(sample["psi"]["cpu"]["status"], "unavailable")
            collector = collect.Collector(interval_s=0.001)
            collector.samples = [{"monotonic_ns": 0}, {"monotonic_ns": 4_000_000}]
            self.assertTrue(collector.coverage(2)["invalid_gap"])


if __name__ == "__main__":
    unittest.main()
