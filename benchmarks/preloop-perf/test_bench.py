#!/usr/bin/env python3
"""Regression tests for the benchmark's destructive-cleanup guard."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import subprocess
import sys
import time
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("bench.py")
SPEC = importlib.util.spec_from_file_location("preloop_perf_bench", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def completed(returncode: int, stdout: str) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["pgrep"], returncode=returncode, stdout=stdout, stderr="")


class EngineDetectionTests(unittest.TestCase):
    def test_engine_scan_matches_the_serve_spelling_and_not_only_the_hidden_alias(self) -> None:
        # `preloop serve` is what an operator's engine runs as, and a scan blind
        # to it reports an idle host while a real runner pool is up.
        for argv in (["preloop", "serve"], ["preloop", "engine"], ["preloop-server", "serve"]):
            with self.subTest(argv=argv):
                process = subprocess.Popen(
                    [sys.executable, "-c", "import time; time.sleep(30)", *argv]
                )
                try:
                    deadline = time.time() + 10
                    while time.time() < deadline and process.pid not in (MODULE.engine_pids() or []):
                        time.sleep(0.05)
                    self.assertIn(process.pid, MODULE.engine_pids() or [])
                finally:
                    process.kill()
                    process.wait(timeout=10)

    def test_pgrep_failure_is_reported_as_unknown_rather_than_as_an_idle_host(self) -> None:
        self.assertIsNone(MODULE.parse_pgrep_pids(completed(2, "")))
        self.assertIsNone(MODULE.parse_pgrep_pids(completed(0, "not-a-pid\n")))
        self.assertEqual(MODULE.parse_pgrep_pids(completed(1, "")), [])
        self.assertEqual(MODULE.parse_pgrep_pids(completed(0, "41\n42\n")), [41, 42])

    def test_engines_not_holding_the_benchmark_port_are_never_reclaimable(self) -> None:
        self.assertEqual(MODULE.classify_engines([41, 42], [42]), ([42], [41]))
        # No port listing available: every engine has to count as somebody else's.
        self.assertEqual(MODULE.classify_engines([41, 42], None), ([], [41, 42]))


class DestructiveCleanupGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calls: list[list[str]] = []
        self.addCleanup(setattr, MODULE, "engine_pids", MODULE.engine_pids)
        self.addCleanup(setattr, MODULE, "run", MODULE.run)
        MODULE.run = lambda cmd, **kwargs: self.calls.append(list(cmd))  # type: ignore[assignment]

    def refuses(self) -> None:
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            MODULE.delete_bench_machines()
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            MODULE.purge_orphan_vm_dirs()
        self.assertEqual(self.calls, [])

    def test_a_live_engine_stops_the_cleanup_before_any_vm_is_deleted(self) -> None:
        MODULE.engine_pids = lambda: [4242]  # type: ignore[assignment]
        self.refuses()

    def test_an_inconclusive_engine_scan_stops_the_cleanup_too(self) -> None:
        MODULE.engine_pids = lambda: None  # type: ignore[assignment]
        self.refuses()


if __name__ == "__main__":
    unittest.main()
