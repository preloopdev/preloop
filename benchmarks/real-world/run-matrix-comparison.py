#!/usr/bin/env python3
"""Concurrency Matrix Stress Testing and Log Comparison Harness.
Supports 4 combinations:
1. GitHub + Official Runner (github-official)
2. GitHub + aksh-runner (github-aksh)
3. aksh server + Official Runner (aksh-official)
4. aksh server + aksh-runner (aksh-aksh)

Also compares any two captured results directories.
"""

import os
import sys
import json
import time
import re
import argparse
import subprocess
import urllib.request
import urllib.error
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field

SCENARIO_WORKFLOWS = {
    "01-bare-string": "concurrency-01-bare-string.yml",
    "02-cancel-in-progress": "concurrency-02-cancel-in-progress.yml",
    "03-fifo-pending": "concurrency-03-fifo-pending.yml",
    "04-cancel-expr-true": "concurrency-04-cancel-expr-true.yml",
    "05-cancel-expr-false": "concurrency-05-cancel-expr-false.yml",
    "06-queue-max": "concurrency-06-queue-max.yml",
    "07a-case-Prod": "concurrency-07a-case-Prod.yml",
    "07b-case-prod": "concurrency-07b-case-prod.yml",
    "08-job-level": "concurrency-08-job-level.yml",
    "09-multi-job-hold": "concurrency-09-multi-job-hold.yml",
    "10-empty-group": "concurrency-10-empty-group.yml",
    "11-expr-group-ref": "concurrency-11-expr-group-ref.yml",
    "12-matrix-same-group": "concurrency-12-matrix-same-group.yml",
    "13-jobset-caller-only": "concurrency-13-jobset-caller-only.yml",
    "14-jobset-embedded-only": "concurrency-14-jobset-embedded-only.yml",
    "15-jobset-different-key": "concurrency-15-jobset-different-key.yml",
}

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?")
BOM = "\ufeff"

def strip_noise(line: str) -> str:
    line = line.replace(BOM, "")
    line = ANSI_RE.sub("", line)
    line = TS_RE.sub("", line)
    return line.strip()

# ─── GitHub API Helpers ──────────────────────────────────────────────────────

def run_cmd(args: list[str], env=None) -> str:
    r = subprocess.run(args, capture_output=True, text=True, check=True, env=env)
    return r.stdout.strip()

def get_existing_github_run_ids(workflow_file: str, repo: str) -> set[str]:
    try:
        out = run_cmd([
            "gh", "run", "list",
            "--repo", repo,
            "--workflow", workflow_file,
            "--limit", "30",
            "--json", "databaseId"
        ])
        runs = json.loads(out)
        return {str(r["databaseId"]) for r in runs}
    except Exception as e:
        print(f"Warning: failed to list runs for {workflow_file}: {e}")
        return set()

def trigger_github_workflow(workflow_file: str, repo: str, ref: str) -> None:
    print(f"Triggering {workflow_file} on GitHub...")
    run_cmd([
        "gh", "workflow", "run",
        "--repo", repo,
        workflow_file,
        "--ref", ref
    ])

def wait_for_new_github_run(workflow_file: str, repo: str, known_ids: set[str], timeout: int = 120) -> str:
    start_time = time.time()
    while time.time() - start_time < timeout:
        time.sleep(2)
        current_ids = get_existing_github_run_ids(workflow_file, repo)
        new_ids = current_ids - known_ids
        if new_ids:
            # Return the latest new run ID
            return sorted(list(new_ids))[-1]
    raise TimeoutError(f"New run for workflow {workflow_file} did not appear on GitHub within {timeout} seconds.")

# ─── aksh API Helpers ────────────────────────────────────────────────────────

def api_request(method: str, url: str, body: dict = None) -> dict:
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {
        "Authorization": "Bearer aksh-system-token",
        "Content-Type": "application/json"
    }
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            raw = r.read()
        if not raw:
            return {}
        return json.loads(raw)
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", "replace")
        raise RuntimeError(f"HTTP {e.code}: {err_body}")

# ─── Log Comparison Logic (Fidelity checks) ──────────────────────────────────

@dataclass
class SideCapture:
    name: str
    conclusion: str | None
    jobs: dict[str, str] = field(default_factory=dict)
    step_conclusions: dict[str, str] = field(default_factory=dict)
    markers: set[str] = field(default_factory=set)
    step_logs: dict[str, list[str]] = field(default_factory=dict)
    raw_log: str = ""

def parse_run_log(text: str) -> dict[str, list[str]]:
    """Parse log text of format 'job\\tstep\\tcontent' or plain lines."""
    steps = defaultdict(list)
    for raw in text.splitlines():
        parts = raw.split("\t", 2)
        if len(parts) == 3:
            _j, step, content = parts
            c = strip_noise(content)
        elif len(parts) == 2:
            step, content = parts
            c = strip_noise(content)
        else:
            step = "_"
            c = strip_noise(raw)
        if not c:
            continue
        # Strip common infra noise
        if any(x in c for x in ("Current runner version", "Prepare workflow directory", "Prepare all required actions", "Operating System", "Runner Image", "GITHUB_TOKEN Permissions", "shell: /usr/bin/bash")):
            continue
        steps[step].append(c)
    return dict(steps)

def extract_markers(lines: list[str]) -> set[str]:
    markers = set()
    for line in lines:
        if line.startswith("##[") and "error" in line.lower() and "cancel" in line.lower():
            markers.add("CANCEL_ERROR")
            continue
        if "SCENARIO=" in line and "echo" not in line:
            m = re.search(r"SCENARIO=([^\s]+)", line)
            if m: markers.add(f"SCENARIO={m.group(1)}")
        if re.search(r"\bDONE=([^\s]+)", line) and "echo" not in line:
            m = re.search(r"\bDONE=([^\s]+)", line)
            if m: markers.add(f"DONE={m.group(1)}")
        if "SHOULD_NOT_REACH" in line and "echo" not in line:
            if line.strip() == "SHOULD_NOT_REACH" or line.endswith("SHOULD_NOT_REACH"):
                markers.add("SHOULD_NOT_REACH_EXECUTED")
        if "The operation was canceled" in line or "operation was cancelled" in line.lower():
            markers.add("CANCEL_ERROR")
    return markers

def load_capture(dir_path: Path) -> SideCapture:
    summary = json.loads((dir_path / "summary.json").read_text(encoding="utf-8"))
    log = (dir_path / "run.log").read_text(encoding="utf-8", errors="replace") if (dir_path / "run.log").exists() else ""
    steps = parse_run_log(log)
    markers = set()
    for lines in steps.values():
        markers |= extract_markers(lines)
    markers |= extract_markers([strip_noise(l) for l in log.splitlines()])

    step_conc = {}
    jobs = {}
    for j in summary.get("jobs", []):
        name = j.get("name") or j.get("id") or ""
        conclusion = j.get("conclusion") or j.get("status") or ""
        jobs[name] = conclusion
        for s in j.get("steps", []):
            if s.get("name"):
                step_conc[s["name"]] = s.get("conclusion") or s.get("status") or ""

    return SideCapture(
        name=dir_path.name,
        conclusion=summary.get("conclusion") or summary.get("status"),
        jobs=jobs,
        step_conclusions=step_conc,
        markers=markers,
        step_logs=steps,
        raw_log=log
    )

def compare_scenarios(left: SideCapture, right: SideCapture) -> dict:
    issues = []
    notes = []

    def norm(c: str | None) -> str:
        if not c: return ""
        c = c.lower()
        if c in ("canceled",): return "cancelled"
        if c in ("succeeded",): return "success"
        if c in ("failed",): return "failure"
        return c

    if norm(left.conclusion) != norm(right.conclusion):
        issues.append(f"Run conclusion mismatch: {left.name}={left.conclusion} vs {right.name}={right.conclusion}")
    else:
        notes.append(f"Run conclusion match: {norm(left.conclusion)}")

    left_scenarios = {m for m in left.markers if m.startswith("SCENARIO=")}
    right_scenarios = {m for m in right.markers if m.startswith("SCENARIO=")}
    for sm in sorted(left_scenarios):
        if sm not in right_scenarios:
            issues.append(f"Scenario marker {sm} missing in {right.name}")
        else:
            notes.append(f"Scenario marker verified: {sm}")

    left_dones = {m for m in left.markers if m.startswith("DONE=")}
    right_dones = {m for m in right.markers if m.startswith("DONE=")}
    for dm in sorted(left_dones):
        if dm not in right_dones:
            if norm(left.conclusion) != "cancelled":
                issues.append(f"DONE marker {dm} missing in {right.name}")
        else:
            notes.append(f"DONE marker verified: {dm}")

    # Step matching
    left_user = {k: v for k, v in left.step_conclusions.items() if k not in ("Set up job", "Complete job")}
    right_user = {k: v for k, v in right.step_conclusions.items() if k not in ("Set up job", "Complete job")}
    for sname, sconc in left_user.items():
        matched_conc = None
        for rname, rconc in right_user.items():
            if sname == rname or sname in rname or rname in sname:
                matched_conc = rconc
                break
        if matched_conc:
            if norm(sconc) != norm(matched_conc):
                issues.append(f"Step '{sname}' conclusion mismatch: {left.name}={sconc} vs {right.name}={matched_conc}")
            else:
                notes.append(f"Step '{sname}' match: conclusion={norm(sconc)}")
        else:
            notes.append(f"No step matching '{sname}' found in {right.name}")

    return {
        "ok": len(issues) == 0,
        "issues": issues,
        "notes": notes
    }


# ─── Runner/Server Orchestration (Local) ──────────────────────────────────────

def start_aksh_server(port: int, state_dir: Path) -> subprocess.Popen:
    print(f"Starting aksh-runner-server serve on port {port}...")
    state_dir.mkdir(parents=True, exist_ok=True)
    # We want to run aksh-runner-server from target/release/aksh-runner-server
    server_bin = Path("target/release/aksh-runner-server")
    if not server_bin.exists():
        server_bin = Path("target/debug/aksh-runner-server")
    if not server_bin.exists():
        raise FileNotFoundError("Could not find aksh-runner-server binary.")
    
    env = os.environ.copy()
    env["AKSH_PUBLIC_URL"] = f"http://127.0.0.1:{port}"
    p = subprocess.Popen(
        [str(server_bin), "serve", "--listen", f"127.0.0.1:{port}", "--state-dir", str(state_dir)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env
    )
    # Wait for server ready
    start_time = time.time()
    while time.time() - start_time < 15:
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{port}/healthz")
            with urllib.request.urlopen(req) as response:
                if response.status == 200:
                    print("aksh-runner-server is ready.")
                    return p
        except Exception:
            time.sleep(0.5)
    p.terminate()
    raise TimeoutError("aksh-runner-server failed to start in time.")

def start_runner(runner_type: str, server_url: str, runner_dir: Path, state_dir: Path) -> subprocess.Popen:
    print(f"Configuring and starting {runner_type} runner...")
    if runner_type == "official":
        # Configure official runner
        # 1. Fetch token
        resp = api_request("POST", f"{server_url}/api/v3/repos/owner/repo/actions/runners/registration-token")
        token = resp["token"]
        # 2. Run config
        config_script = runner_dir / "config.sh"
        if not config_script.exists():
            raise FileNotFoundError(f"Could not find config.sh in {runner_dir}")
        # Clean old config
        for f in (".runner", ".credentials", ".credentials_rsaparams"):
            (runner_dir / f).unlink(missing_ok=True)
        subprocess.run([
            str(config_script), "--unattended",
            "--url", f"{server_url}/runner/server",
            "--token", token,
            "--name", f"official-{int(time.time())}",
            "--labels", "self-hosted,fidelity-test",
            "--work", "_work",
            "--replace"
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=runner_dir)
        
        # 3. Start runner
        run_script = runner_dir / "run.sh"
        p = subprocess.Popen([str(run_script)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=runner_dir)
        return p
    else:
        # Configure aksh-runner
        runner_bin = Path("target/release/aksh-runner")
        if not runner_bin.exists():
            runner_bin = Path("target/debug/aksh-runner")
        if not runner_bin.exists():
            raise FileNotFoundError("Could not find aksh-runner binary.")
        # config
        work_dir = state_dir / "runner-work"
        work_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run([
            str(runner_bin), "configure",
            "--url", server_url,
            "--token", "aksh-system-token",
            "--name", f"aksh-{int(time.time())}",
            "--labels", "self-hosted,fidelity-test",
            "--work", str(work_dir)
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # run
        p = subprocess.Popen([str(runner_bin), "run"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return p

# ─── Scenario Runner ──────────────────────────────────────────────────────────

def execute_local_scenario(scenario: str, server_url: str, state_dir: Path, out_dir: Path) -> dict:
    workflow_file = SCENARIO_WORKFLOWS[scenario]
    yaml_path = Path(".github/workflows") / workflow_file
    yaml_text = yaml_path.read_text(encoding="utf-8")
    
    reusable_map = {}
    if "jobset" in scenario:
        callee_path = Path(".github/workflows/reusable-callee.yml")
        reusable_map["./.github/workflows/reusable-callee.yml"] = callee_path.read_text(encoding="utf-8")
        # Also check local workflow directory path variant
        reusable_map[".github/workflows/reusable-callee.yml"] = callee_path.read_text(encoding="utf-8")

    def submit_one(name_suffix: str = ""):
        body = {
            "workflow_yaml": yaml_text,
            "event": "push",
            "repository": "owner/repo",
            "reusable_workflows": reusable_map
        }
        # Inject specific trigger values if needed
        if scenario == "07a-case-Prod":
            # Case Prod
            pass
        resp = api_request("POST", f"{server_url}/api/v1/runs", body)
        return resp["run_id"]

    print(f"\n--- Running Local Scenario: {scenario} ---")
    runs = []
    
    # Custom trigger sequences
    if scenario == "01-bare-string":
        runs.append(("01-bare-A", submit_one()))
        time.sleep(1.0)
        runs.append(("01-bare-B", submit_one()))
    elif scenario in ("02-cancel-in-progress", "04-cancel-expr-true"):
        runs.append((f"{scenario[:2]}-cancel-A", submit_one()))
        time.sleep(2.0)
        runs.append((f"{scenario[:2]}-cancel-B", submit_one()))
    elif scenario in ("03-fifo-pending", "05-cancel-expr-false"):
        runs.append((f"{scenario[:2]}-A", submit_one()))
        time.sleep(1.0)
        runs.append((f"{scenario[:2]}-B", submit_one()))
    elif scenario == "06-queue-max":
        runs.append(("06-queue-max-A", submit_one()))
        time.sleep(0.5)
        runs.append(("06-queue-max-B", submit_one()))
        time.sleep(0.5)
        runs.append(("06-queue-max-C", submit_one()))
    elif scenario in ("07a-case-Prod", "07b-case-prod"):
        # Trigger Case variants sequentially
        runs.append(("07-case-A", submit_one()))
    else:
        runs.append((scenario, submit_one()))

    results = {}
    for name, run_id in runs:
        print(f"Waiting for run {name} ({run_id}) to complete...")
        run_url = f"{server_url}/api/v1/runs/{run_id}"
        
        # Poll for completion
        conclusion = None
        for _ in range(120):
            time.sleep(1.0)
            run_obj = api_request("GET", run_url)
            status = run_obj.get("status")
            if status in ("success", "failure", "cancelled"):
                conclusion = status
                break
        
        if not conclusion:
            print(f"Warning: Run {run_id} timed out.")
            conclusion = "timeout"

        # Save capture files
        run_dir = out_dir / name
        run_dir.mkdir(parents=True, exist_ok=True)
        
        # Fetch run details
        run_obj = api_request("GET", run_url)
        (run_dir / "summary.json").write_text(json.dumps(run_obj, indent=2))
        (run_dir / "jobs.json").write_text(json.dumps({"jobs": run_obj.get("jobs", [])}, indent=2))
        (run_dir / "run.json").write_text(json.dumps({"run_id": run_id, "status": conclusion}, indent=2))
        
        # Fetch logs via GET /api/v1/runs/:run_id/logs
        log_text = ""
        try:
            req = urllib.request.Request(f"{server_url}/api/v1/runs/{run_id}/logs")
            req.add_header("Authorization", "Bearer aksh-system-token")
            with urllib.request.urlopen(req) as response:
                log_text = response.read().decode("utf-8", "replace")
        except Exception as e:
            print(f"Warning: failed to fetch REST logs for run {run_id}: {e}")
            # Fallback to local state results files
            log_lines = []
            results_dir = state_dir / "replay" / "results"
            if results_dir.exists():
                for step_file in sorted(results_dir.glob("**/step-*.txt")):
                    try:
                        step_name = step_file.stem
                        for line in step_file.read_text(errors="replace").splitlines():
                            log_lines.append(f"build\t{step_name}\t{line}")
                    except Exception:
                        pass
            log_text = "\n".join(log_lines) + "\n"
            
        (run_dir / "run.log").write_text(log_text, encoding="utf-8")
        results[name] = conclusion
        print(f"Run {name} finished with status: {conclusion}")

    return results

def execute_github_scenario(scenario: str, repo: str, token: str, ref: str, out_dir: Path) -> dict:
    workflow_file = SCENARIO_WORKFLOWS[scenario]
    
    def submit_one(name: str):
        known_ids = get_existing_github_run_ids(workflow_file, repo)
        trigger_github_workflow(workflow_file, repo, ref)
        print(f"Polling GitHub for new run of {workflow_file}...")
        run_id = wait_for_new_github_run(workflow_file, repo, known_ids)
        return run_id

    print(f"\n--- Running GitHub Scenario: {scenario} ---")
    runs = []
    
    # Custom trigger sequences
    if scenario == "01-bare-string":
        runs.append(("01-bare-A", submit_one("01-bare-A")))
        time.sleep(1.0)
        runs.append(("01-bare-B", submit_one("01-bare-B")))
    elif scenario in ("02-cancel-in-progress", "04-cancel-expr-true"):
        runs.append((f"{scenario[:2]}-cancel-A", submit_one(f"{scenario[:2]}-cancel-A")))
        time.sleep(2.0)
        runs.append((f"{scenario[:2]}-cancel-B", submit_one(f"{scenario[:2]}-cancel-B")))
    elif scenario in ("03-fifo-pending", "05-cancel-expr-false"):
        runs.append((f"{scenario[:2]}-A", submit_one(f"{scenario[:2]}-A")))
        time.sleep(1.0)
        runs.append((f"{scenario[:2]}-B", submit_one(f"{scenario[:2]}-B")))
    elif scenario == "06-queue-max":
        runs.append(("06-queue-max-A", submit_one("06-queue-max-A")))
        time.sleep(0.5)
        runs.append(("06-queue-max-B", submit_one("06-queue-max-B")))
        time.sleep(0.5)
        runs.append(("06-queue-max-C", submit_one("06-queue-max-C")))
    elif scenario in ("07a-case-Prod", "07b-case-prod"):
        runs.append(("07-case-A", submit_one("07-case-A")))
    else:
        runs.append((scenario, submit_one(scenario)))

    results = {}
    for name, run_id in runs:
        print(f"Waiting for GitHub run {name} ({run_id}) to complete...")
        
        # Poll for completion
        conclusion = None
        for _ in range(120):
            time.sleep(5.0)
            out = run_cmd([
                "gh", "run", "view",
                "--repo", repo,
                run_id,
                "--json", "status,conclusion,jobs"
            ])
            run_obj = json.loads(out)
            status = run_obj.get("status")
            if status == "completed":
                conclusion = run_obj.get("conclusion")
                break
        
        if not conclusion:
            print(f"Warning: GitHub run {run_id} timed out.")
            conclusion = "timeout"

        # Save capture files
        run_dir = out_dir / name
        run_dir.mkdir(parents=True, exist_ok=True)
        
        out = run_cmd([
            "gh", "run", "view",
            "--repo", repo,
            run_id,
            "--json", "status,conclusion,jobs,createdAt,updatedAt,url"
        ])
        run_obj = json.loads(out)
        (run_dir / "summary.json").write_text(json.dumps(run_obj, indent=2))
        (run_dir / "jobs.json").write_text(json.dumps({"jobs": run_obj.get("jobs", [])}, indent=2))
        (run_dir / "run.json").write_text(json.dumps({"run_id": run_id, "status": conclusion}, indent=2))
        
        # Fetch logs via gh run view --log
        log_text = run_cmd([
            "gh", "run", "view",
            "--repo", repo,
            run_id,
            "--log"
        ])
        (run_dir / "run.log").write_text(log_text, encoding="utf-8")
        results[name] = conclusion
        print(f"GitHub run {name} finished with status: {conclusion}")

    return results

# ─── Main Execution ───────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Concurrency Matrix Comparison Harness")
    ap.add_argument("--runner", choices=["official", "aksh"], default="aksh")
    ap.add_argument("--server", choices=["github", "aksh"], default="aksh")
    ap.add_argument("--repo", default="Bnjoroge1/aksh-concurrency-probe", help="GitHub repo for live runs")
    ap.add_argument("--ref", default="main", help="Git ref for GitHub workflow dispatch")
    ap.add_argument("--out-dir", type=Path, default=Path("benchmarks/real-world/results/matrix"))
    ap.add_argument("--compare-dirs", nargs=2, type=Path, default=None,
                    help="Compare two captured results directories and output comparison MD")
    ap.add_argument("--scenarios", default=None, help="Comma-separated scenarios to run (default: run all)")
    ap.add_argument("--port", type=int, default=9090, help="aksh-runner-server local port")
    ap.add_argument("--state-dir", type=Path, default=Path("/tmp/aksh-matrix-state"), help="Local aksh state dir")
    ap.add_argument("--runner-dir", type=Path, default=Path("/Users/bnjoroge/mitm-proxy/experiments/mitm/.cache/runner-official"),
                    help="Official runner directory")
    
    args = ap.parse_args()

    if args.compare_dirs:
        dir1, dir2 = args.compare_dirs
        print(f"\n=== Comparing Captured Result Folders: {dir1.name} vs {dir2.name} ===")
        
        results = []
        subdirs1 = {p.name for p in dir1.iterdir() if p.is_dir()}
        subdirs2 = {p.name for p in dir2.iterdir() if p.is_dir()}
        common = sorted(list(subdirs1 & subdirs2))
        
        total = len(common)
        passed = 0
        report_lines = [
            "# Concurrency Matrix Log Content Compare",
            f"- **Left Capture:** `{dir1}`",
            f"- **Right Capture:** `{dir2}`",
            f"- **Common Scenarios:** {total}",
            "",
            "| Scenario | Result | Notes / Mismatches |",
            "|---|---|---|",
        ]
        
        for name in common:
            left = load_capture(dir1 / name)
            right = load_capture(dir2 / name)
            cmp = compare_scenarios(left, right)
            if cmp["ok"]:
                passed += 1
                status = "✅ PASS"
                notes = "; ".join(cmp["notes"][:3])
            else:
                status = "❌ FAIL"
                notes = "; ".join(cmp["issues"])
            report_lines.append(f"| {name} | {status} | {notes} |")
            
        report_lines.append("")
        report_lines.append(f"### Score: **{passed}/{total}** scenarios match perfectly.")
        
        report_md = "\n".join(report_lines)
        print(report_md)
        (dir1 / f"COMPARE-WITH-{dir2.name}.md").write_text(report_md, encoding="utf-8")
        return 0 if passed == total else 1

    # Run mode
    scenarios_to_run = list(SCENARIO_WORKFLOWS.keys())
    if args.scenarios:
        scenarios_to_run = [s.strip() for s in args.scenarios.split(",") if s.strip() in SCENARIO_WORKFLOWS]

    env_name = f"{args.runner}-{args.server}"
    out_path = args.out_dir / env_name
    out_path.mkdir(parents=True, exist_ok=True)

    server_proc = None
    runner_proc = None
    
    try:
        if args.server == "aksh":
            # Start local server
            server_proc = start_aksh_server(args.port, args.state_dir)
            # Start local runner (aksh or official)
            server_url = f"http://127.0.0.1:{args.port}"
            runner_proc = start_runner(args.runner, server_url, args.runner_dir, args.state_dir)
            
            # Wait for runner to register
            time.sleep(3.0)

        results = {}
        for scenario in scenarios_to_run:
            if args.server == "aksh":
                res = execute_local_scenario(scenario, f"http://127.0.0.1:{args.port}", args.state_dir, out_path)
            else:
                token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN", "")
                res = execute_github_scenario(scenario, args.repo, token, args.ref, out_path)
            results.update(res)

        print("\n=== Concurrency Matrix Run Results ===")
        print(json.dumps(results, indent=2))
        (out_path / "matrix-results.json").write_text(json.dumps(results, indent=2))
        
    finally:
        # Clean up processes
        if runner_proc:
            print("Stopping runner...")
            runner_proc.terminate()
            runner_proc.wait()
        if server_proc:
            print("Stopping aksh-runner-server...")
            server_proc.terminate()
            server_proc.wait()
            
    return 0

if __name__ == "__main__":
    sys.exit(main())

