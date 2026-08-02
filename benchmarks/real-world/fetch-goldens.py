#!/usr/bin/env python3
"""Fetch GitHub Actions golden logs for real-world conformance repos.

Cell A of the conformance matrix: official runner against GitHub.
For each repo we capture a recent successful run's job/step structure
(normalized) and the per-step log content, so aksh server + runner runs
(cells B/C) can be compared against it.

Usage:
  python3 fetch-goldens.py <repo> <run_id> <outdir>

The outdir receives:
  golden.json      normalized job/step structure (names, conclusions, order)
  logs/            per-step log files (normalized line endings only)
"""

import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path


def gh(args: list[str]) -> bytes:
    return subprocess.run(
        ["gh", "api", "/".join(args)], check=True, capture_output=True
    ).stdout


def normalize_log_text(text: str) -> str:
    """Strip volatile content: timestamps, ANSI codes, run ids, urls, tokens."""
    import re

    # ANSI escape sequences
    text = re.sub(r"\x1b\[[0-9;]*m", "", text)
    # Leading timestamps (2026-08-01T04:01:31.1234567Z)
    text = re.sub(r"(?m)^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z?\s*", "", text)
    # GitHub step markers: ##[group]/##[endgroup]
    # Keep these: they delimit steps and are semantically meaningful.
    return text


def main() -> None:
    repo, run_id, outdir = sys.argv[1], sys.argv[2], sys.argv[3]
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    run_meta = json.loads(gh(["repos", repo, "actions/runs", run_id]))
    jobs = json.loads(gh(["repos", repo, "actions/runs", run_id, "jobs?per_page=100"]))
    if jobs.get("total_count", 0) > len(jobs.get("jobs", [])):
        print("warning: more jobs than fetched; pagination not implemented")

    golden = {
        "repo": repo,
        "run_id": run_id,
        "workflow_name": run_meta.get("name"),
        "workflow_path": run_meta.get("path"),
        "event": run_meta.get("event"),
        "head_sha": run_meta.get("head_sha"),
        "conclusion": run_meta.get("conclusion"),
        "created_at": run_meta.get("created_at"),
        "jobs": [],
    }
    for job in jobs.get("jobs", []):
        golden["jobs"].append(
            {
                "name": job["name"],
                "conclusion": job.get("conclusion"),
                "status": job.get("status"),
                "steps": [
                    {
                        "name": step["name"],
                        "number": step["number"],
                        "conclusion": step.get("conclusion"),
                        "status": step.get("status"),
                    }
                    for step in job.get("steps", [])
                ],
            }
        )

    # Logs: download the run logs zip and split per step.
    logs_dir = out / "logs"
    logs_dir.mkdir(exist_ok=True)
    zip_bytes = gh(["repos", repo, "actions/runs", run_id, "logs"])
    zip_path = out / "run-logs.zip"
    zip_path.write_bytes(zip_bytes)
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            # GitHub log layout: <job-name>/<step-number>_<step-name>.txt
            if member.endswith(".txt"):
                raw = zf.read(member).decode("utf-8", errors="replace")
                safe = member.replace("/", "__")
                (logs_dir / safe).write_text(normalize_log_text(raw))

    (out / "golden.json").write_text(json.dumps(golden, indent=2))
    print(f"wrote {out}/golden.json with {len(golden['jobs'])} jobs")


if __name__ == "__main__":
    main()
