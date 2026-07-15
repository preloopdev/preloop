#!/usr/bin/env python3
"""Regression tests for strict concurrency capture comparison."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path
import tempfile


MODULE_PATH = Path(__file__).with_name("concurrency-log-compare.py")
SPEC = importlib.util.spec_from_file_location("concurrency_log_compare", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
SideCapture = MODULE.SideCapture
compare = MODULE.compare


class CompareStrictParityTests(unittest.TestCase):
    def capture(self, name: str, **overrides: object) -> SideCapture:
        values = {
            "name": name,
            "conclusion": "success",
            "jobs": {},
            "step_conclusions": {},
            "markers": set(),
            "step_logs": {},
            "raw_log": "",
        }
        values.update(overrides)
        return SideCapture(**values)

    def test_zero_jobs_does_not_match_one_failed_job(self) -> None:
        github = self.capture("github", conclusion="failure")
        aksh = self.capture(
            "aksh", conclusion="failure", jobs={"build": "failure"}
        )

        result = compare(github, aksh)

        self.assertFalse(result["ok"])
        self.assertTrue(any("job count" in issue for issue in result["issues"]))

    def test_missing_step_is_a_hard_failure(self) -> None:
        github = self.capture(
            "github", step_conclusions={"expected": "success"}
        )
        aksh = self.capture("aksh")

        result = compare(github, aksh)

        self.assertFalse(result["ok"])
        self.assertTrue(
            any("missing aksh step" in issue for issue in result["issues"])
        )

    def test_missing_cancel_annotation_is_a_hard_failure(self) -> None:
        github = self.capture(
            "github", conclusion="cancelled", markers={"CANCEL_ERROR"}
        )
        aksh = self.capture("aksh", conclusion="cancelled")

        result = compare(github, aksh)

        self.assertFalse(result["ok"])
        self.assertTrue(
            any("cancel error annotation" in issue for issue in result["issues"])
        )

    def test_matching_structure_and_markers_pass(self) -> None:
        github = self.capture(
            "github",
            jobs={"build": "success"},
            step_conclusions={"exercise": "success"},
            markers={"SCENARIO=one", "DONE=one"},
        )
        aksh = self.capture(
            "aksh",
            jobs={"build": "success"},
            step_conclusions={"exercise": "success"},
            markers={"SCENARIO=one", "DONE=one"},
        )

        result = compare(github, aksh)

        self.assertTrue(result["ok"], result["issues"])

    def test_manifest_covers_23_unique_scenarios(self) -> None:
        self.assertEqual(len(MODULE.CAPTURE_PAIRS), 23)
        names = [name for name, _, _ in MODULE.CAPTURE_PAIRS]
        self.assertEqual(len(names), len(set(names)))

    def test_native_run_job_map_is_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            capture_dir = Path(directory)
            (capture_dir / "summary.json").write_text(
                json.dumps({"status": "failure", "jobs": {"build": "failure"}})
            )
            (capture_dir / "run.log").write_text("")

            capture = MODULE.load_aksh_capture(capture_dir)

        self.assertEqual(capture.jobs, {"build": "failure"})
        self.assertEqual(capture.conclusion, "failure")


if __name__ == "__main__":
    unittest.main()
