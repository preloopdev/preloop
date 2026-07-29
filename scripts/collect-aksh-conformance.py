#!/usr/bin/env python3
"""Run each scenario through aksh infrastructure and collect results as JSONL."""

import json, os, subprocess, time, tempfile
from pathlib import Path

SERVER_PORT = 9191
RESULTS_FILE = "benchmarks/compatibility/runner/behavior/conformance-aksh-new.jsonl"

scenarios = [
    ("101-dynamic-matrix-dataflow", "workflow_dispatch"),
    ("102-mask-and-secret-propagation", "workflow_dispatch"),
    ("103-composite-nested-post", "workflow_dispatch"),
    ("104-job-defaults-env-cascade", "workflow_dispatch"),
    ("105-concurrency-cancellation-group", "workflow_dispatch"),
    ("107-continue-on-error-status-funcs", "workflow_dispatch"),
    ("108-workflow-dispatch-inputs", "workflow_dispatch"),
    ("109-log-streaming-backpressure", "workflow_dispatch"),
    ("110-environment-deployment-url", "workflow_dispatch"),
    ("111-github-state-post-execution", "workflow_dispatch"),
    ("112-service-container-health-ports", "workflow_dispatch"),
    ("113-artifact-v4-multi-pattern", "workflow_dispatch"),
    ("114-step-timeout-graceful-kill", "workflow_dispatch"),
    ("115-cache-v2-restore-fallback", "workflow_dispatch"),
]

records = []

for name, event in scenarios:
    wf_path = Path(f"experiments/mitm/scenarios/{name}/{name}.yml")
    if not wf_path.exists():
        print(f"SKIP: {wf_path} not found")
        continue

    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    state_dir = Path(tempfile.mkdtemp(prefix=f"aksh-{name[:10]}-"))
    runner_root = Path(tempfile.mkdtemp(prefix=f"rr-{name[:10]}-"))

    try:
        # Kill any stale server on our port
        subprocess.run(["lsof", "-ti", f":{SERVER_PORT}"], capture_output=True, text=True)
        pids = subprocess.run(["lsof", "-ti", f":{SERVER_PORT}"], capture_output=True, text=True)
        if pids.stdout.strip():
            for pid in pids.stdout.strip().split('\n'):
                subprocess.run(["kill", "-9", pid], capture_output=True)
        time.sleep(2)

        # Start server
        print("  Starting server...", end=" ", flush=True)
        server = subprocess.Popen(
            ["target/release/preloop-server", "serve",
             "--listen", f"127.0.0.1:{SERVER_PORT}",
             "--state-dir", str(state_dir)],
            env={**os.environ,
                 "AKSH_PUBLIC_URL": f"http://127.0.0.1:{SERVER_PORT}",
                 "AKSH_SYSTEM_TOKEN": "aksh-system-token"},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        time.sleep(3)

        # Verify server is up
        for _ in range(30):
            r = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                                f"http://127.0.0.1:{SERVER_PORT}/"],
                               capture_output=True, text=True, timeout=5)
            if r.stdout.strip():
                print("OK")
                break
            time.sleep(1)
        else:
            print("TIMEOUT")
            continue

        # Configure runner
        print("  Configuring runner...", end=" ", flush=True)
        c = subprocess.run(
            ["target/release/preloop-runner", "--runner-root", str(runner_root),
             "configure", "--url", f"http://127.0.0.1:{SERVER_PORT}",
             "--token", "dummy-token", "--name", f"aksh-{name[:20]}",
             "--unattended", "--replace", "--ephemeral", "--no-externals"],
            capture_output=True, text=True, timeout=30
        )
        if c.returncode != 0:
            print(f"FAIL: {c.stderr[-150:]}")
            continue
        print("OK")

        # Submit workflow with correct event
        print("  Submitting...", end=" ", flush=True)
        s = subprocess.run(
            ["target/debug/aksh-runner-client", "--server",
             f"http://127.0.0.1:{SERVER_PORT}",
             "submit", "-W", str(wf_path),
             "--event", event, "--repository", "preloopdev/aksh",
             "--git-ref", "refs/heads/main"],
            capture_output=True, text=True, timeout=30
        )
        if s.returncode != 0:
            print(f"FAIL: {s.stderr[-150:]}")
            continue
        submit_data = json.loads(s.stdout)
        run_id = submit_data.get("run_id", "")
        print(f"OK run_id={run_id}")

        # Run runner
        print("  Running runner...", end=" ", flush=True)
        r = subprocess.run(
            ["target/release/preloop-runner", "--runner-root", str(runner_root),
             "run", "--once"],
            capture_output=True, text=True, timeout=120
        )
        print(f"exit={r.returncode}")

        # Collect results from API
        print("  Collecting results...", end=" ", flush=True)
        time.sleep(2)
        for _ in range(30):
            time.sleep(1)
            try:
                g = subprocess.run(
                    ["curl", "-s", "-H", "Authorization: Bearer aksh-system-token",
                     f"http://127.0.0.1:{SERVER_PORT}/api/v1/runs/{run_id}"],
                    capture_output=True, text=True, timeout=5
                )
                if g.stdout.strip():
                    run_data = json.loads(g.stdout)
                    status = run_data.get("status", "")
                    conclusion = run_data.get("conclusion") or status
                    if status in ("completed", "success", "failure", "cancelled", "skipped"):
                        print(f"conclusion={conclusion}")

                        record = {
                            "runner": "aksh",
                            "workflow": name,
                            "run_id": run_id,
                            "conclusion": conclusion,
                            "result": {"conclusion": conclusion, "jobs": []}
                        }
                        for jd in run_data.get("jobs_list", []):
                            record["result"]["jobs"].append({
                                "name": jd.get("name", ""),
                                "conclusion": jd.get("conclusion", ""),
                                "steps": []
                            })
                        records.append(record)
                        break
            except:
                pass
        else:
            print("TIMEOUT")

    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        subprocess.run(["kill", "-9", str(server.pid)], capture_output=True)
        subprocess.run(["rm", "-rf", str(state_dir), str(runner_root)], capture_output=True)

# Write results
with open(RESULTS_FILE, 'w') as f:
    for r in records:
        f.write(json.dumps(r) + '\n')
    print(f"\nDone: {len(records)} records written to {RESULTS_FILE}")
    for r in records:
        print(f"  {r['workflow']}: {r['conclusion']} ({len(r['result']['jobs'])} jobs)")
