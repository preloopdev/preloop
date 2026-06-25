#!/usr/bin/env python3
"""Run a scenario against a MITM capture. Reads TOML, executes steps."""

import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib


RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def log(msg: str, level: str = "info"):
    prefix = {"ok": f"{GREEN}[OK]{RESET}", "wait": f"{YELLOW}[WAIT]{RESET}", "err": f"{RED}[ERR]{RESET}"}.get(level, "[INFO]")
    print(f"{prefix} {msg}", flush=True)


def load_flows(capture_dir: Path) -> list[dict]:
    flows = []
    jl = capture_dir / "flows.jsonl"
    if jl.exists():
        for line in jl.read_text().splitlines():
            if line.strip():
                flows.append(json.loads(line))
    return flows


def _b64_decode(s: str) -> str:
    """Decode base64 to string, returning empty on error."""
    try:
        return base64.b64decode(s).decode("utf-8", errors="replace")
    except Exception:
        return ""


def match_event(event: str, flows: list[dict]) -> bool:
    for f in flows:
        path = f.get("path", "")
        method = f.get("method", "")
        status = f.get("status", "")
        if event == "runner_registered":
            if "POST" in method and status in (200, 201) and "/_apis/distributedtask/pools/" in path and "/agents" in path:
                return True
        elif event == "job_assigned":
            resp = f.get("response_body_json")
            if resp and isinstance(resp, dict) and resp.get("messageType") == "PipelineAgentJobRequest":
                return True
            body = _b64_decode(f.get("response_body_b64", ""))
            if "PipelineAgentJobRequest" in body:
                return True
        elif event == "job_completed":
            if "/jobrequests/" in path:
                return True
            body = _b64_decode(f.get("request_body_b64", "")) + _b64_decode(f.get("response_body_b64", ""))
            if "JobCompleted" in body:
                return True
    return False


def wait_for_event(event: str, capture_dir: Path, timeout: int) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        flows = load_flows(capture_dir)
        if match_event(event, flows):
            return True
        time.sleep(2)
    return False


def submit_workflow_official(workflow_path: str) -> str | None:
    owner = os.environ["GITHUB_OWNER"]
    repo = os.environ["GITHUB_REPO"]
    ref = os.environ.get("GITHUB_REF", "main")
    basename = os.path.basename(workflow_path)
    log(f"submitting workflow {basename} to {owner}/{repo}@{ref}")
    subprocess.run(["gh", "workflow", "run", basename, "-R", f"{owner}/{repo}", "--ref", ref], check=True)
    time.sleep(3)
    result = subprocess.run(
        ["gh", "run", "list", "-R", f"{owner}/{repo}", "--workflow", basename, "--limit", "1", "--json", "databaseId", "--jq", ".[0].databaseId"],
        check=True, capture_output=True, text=True,
    )
    run_id = result.stdout.strip()
    log(f"run id: {run_id}", "ok")
    return run_id


def submit_workflow_runner_server(workflow_path: str, mitm_dir: Path) -> str | None:
    cache = mitm_dir / ".cache" / "runner.server"
    cli = cache / "src" / "Runner.Client"
    log(f"submitting workflow {workflow_path} to runner.server")
    wf_abs = str(Path(workflow_path).resolve())
    subprocess.run(
        ["dotnet", "run", "--project", str(cli), "--", "--workflow", wf_abs, "--event", "push", "--server", "http://127.0.0.1:5000"],
        cwd=str(cache), check=True,
    )
    time.sleep(2)
    req = urllib.request.urlopen("http://127.0.0.1:5000/runner/server/_apis/v1/Message/workflow/runs?owner=&repo=")
    data = json.loads(req.read())
    if isinstance(data, list) and data:
        run_id = str(data[0].get("id", ""))
        log(f"run id: {run_id}", "ok")
        return run_id
    return None


def cancel_workflow_official(run_id: str):
    owner = os.environ["GITHUB_OWNER"]
    repo = os.environ["GITHUB_REPO"]
    log(f"cancelling run {run_id}")
    subprocess.run(["gh", "run", "cancel", run_id, "-R", f"{owner}/{repo}"], check=True)


def cancel_workflow_runner_server(run_id: str):
    url = f"http://127.0.0.1:5000/runner/server/_apis/v1/Message/cancelWorkflow/{run_id}"
    log(f"cancelling run {run_id} via {url}")
    req = urllib.request.Request(url, method="POST", data=b"")
    urllib.request.urlopen(req)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", required=True, choices=["official", "runner-server"])
    parser.add_argument("--scenario", required=True, help="path to scenario.toml")
    parser.add_argument("--capture-dir", required=True)
    parser.add_argument("--mitm-dir", required=True)
    parser.add_argument("--run", default="true")
    args = parser.parse_args()

    if args.run != "true":
        log("dry-run only", "wait")
        return

    scenario_path = Path(args.scenario)
    if not scenario_path.exists():
        log(f"scenario not found: {scenario_path}", "err")
        sys.exit(1)

    with open(scenario_path, "rb") as f:
        cfg = tomllib.load(f)

    desc = cfg.get("description", args.scenario)
    duration = cfg.get("duration_seconds_max", 300)
    steps = cfg.get("steps", [])
    log(f"scenario: {desc}", "ok")

    capture_dir = Path(args.capture_dir)
    mitm_dir = Path(args.mitm_dir)
    last_run_id = None
    deadline = time.time() + duration

    for i, step in enumerate(steps):
        kind = step.get("kind", "")
        if time.time() > deadline:
            log(f"step {i}: deadline exceeded", "err")
            sys.exit(10)

        if kind == "wait_seconds":
            n = step.get("n", 0)
            log(f"step {i}: waiting {n}s")
            time.sleep(n)

        elif kind == "wait_for_event":
            event = step.get("event", "")
            timeout = step.get("timeout", 60)
            log(f"step {i}: waiting for event '{event}' (timeout {timeout}s)")
            ok = wait_for_event(event, capture_dir, timeout)
            if ok:
                log(f"step {i}: event '{event}' matched", "ok")
            else:
                log(f"step {i}: event '{event}' timed out", "err")
                sys.exit(10)

        elif kind == "submit_workflow":
            wf = step.get("path", "")
            if not wf:
                log(f"step {i}: missing path", "err")
                sys.exit(9)
            wf_path = scenario_path.parent / wf
            if args.backend == "official":
                last_run_id = submit_workflow_official(str(wf_path))
            else:
                last_run_id = submit_workflow_runner_server(str(wf_path), mitm_dir)

        elif kind == "cancel_workflow":
            if last_run_id is None:
                log(f"step {i}: no run id to cancel", "err")
                sys.exit(1)
            if args.backend == "official":
                cancel_workflow_official(last_run_id)
            else:
                cancel_workflow_runner_server(last_run_id)

        else:
            log(f"step {i}: unknown kind '{kind}'", "err")
            sys.exit(8)

    log("scenario complete", "ok")


if __name__ == "__main__":
    main()
