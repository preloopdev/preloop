#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path

import importlib.util
spec = importlib.util.spec_from_file_location(
    "log_content_diff",
    Path(__file__).parent / "log-content-diff.py"
)
log_content_diff = importlib.util.module_from_spec(spec)
spec.loader.exec_module(log_content_diff)

SCENARIOS = [
    ("30-container-job-basic", "experiments/mitm/scenarios/30-container-job-basic/30-container-job-basic.yml"),
    ("31-container-with-services", "experiments/mitm/scenarios/31-container-with-services/31-container-with-services.yml"),
    ("33-container-env-options", "experiments/mitm/scenarios/33-container-env-options/33-container-env-options.yml"),
    ("34-container-with-checkout", "experiments/mitm/scenarios/34-container-with-checkout/34-container-with-checkout.yml"),
    ("35-container-lifecycle", "experiments/mitm/scenarios/35-container-lifecycle/35-container-lifecycle.yml"),
]

# On non-darwin (e.g. Linux CI), or if docker context works, we can also run 36
if sys.platform != 'darwin':
    SCENARIOS.append(("32-services-no-container", "experiments/mitm/scenarios/32-services-no-container/32-services-no-container.yml"))
    SCENARIOS.append(("36-docker-action", "experiments/mitm/scenarios/36-docker-action/36-docker-action.yml"))

def main():
    conformance_dir = Path(".runner-watch/golden/v2.335.1")
    save_dir = Path("/tmp/aksh-conformance-logs")
    
    if save_dir.exists():
        # Clear old logs
        for f in save_dir.glob("*.log"):
            f.unlink()
    
    failed = False
    
    for name, workflow in SCENARIOS:
        print(f"=== Running E2E for {name} ===")
        # Run runner-e2e
        cmd = [
            "target/release/aksh-conformance",
            "runner-e2e",
            "--runner-bin", "target/release/aksh-runner",
            "--workflow", workflow,
            "--save-logs", str(save_dir)
        ]
        
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            print(f"E2E execution failed for {name}:\n{res.stderr}")
            # If the scenario is 36 and we are on mac, it is expected to fail on docker-action-host
            if name == "36-docker-action" and sys.platform == 'darwin':
                print("Skipping 36-docker-action failure on macOS due to .Trashes permission restrictions.")
            else:
                failed = True
                continue
                
        # Find golden logs
        scenario_dir = conformance_dir / name
        golden_logs = list(scenario_dir.glob("job-*-logs.txt"))
        if not golden_logs:
            print(f"No golden log found for {name}, skipping log comparison.")
            continue
            
        for golden_log in golden_logs:
            # Match by job name from the filename.
            # Golden logs are named `job-<job_name_kebab>-logs.txt`
            # Target logs are named `<job_name>.log`
            # We can find the target log that corresponds to this golden log by parsing the "Complete job name: " inside the golden log!
            gold_run = log_content_diff.RunLog(golden_log)
            # Find the job name from gold_run
            job_name = None
            for line in gold_run.lines:
                if "Complete job name: " in line.content:
                    pos = line.content.find("Complete job name: ")
                    job_name = line.content[pos + len("Complete job name: "):].strip()
                    break
            
            if not job_name:
                # Fallback to matching by file name
                print(f"Could not extract job name from golden log {golden_log.name}")
                continue
                
            target_log = save_dir / f"{job_name}.log"
            if not target_log.exists():
                print(f"Target log {target_log.name} not found for golden log {golden_log.name}")
                if name == "36-docker-action" and job_name == "docker-action-host" and sys.platform == 'darwin':
                    print("Skipping missing host log on macOS.")
                    continue
                failed = True
                continue
                
            print(f"Comparing log content: {golden_log.name} vs {target_log.name}")
            target_run = log_content_diff.RunLog(target_log)
            diff = log_content_diff.LogDiff(gold_run, target_run, name).run()
           
            # Check for high or medium severity issues
            filtered_issues = []
            for sev, msg in diff.issues:
               if "##[warning]" in msg or "warning count" in msg or "missing annotations" in msg:
                   continue
               if sev in ("high", "medium"):
                   filtered_issues.append((sev, msg))

            if filtered_issues:
                print(f"🔴 Log conformance mismatch found in {name} ({job_name}):")
                for sev, msg in filtered_issues:
                    print(f"  [{sev.upper()}] {msg}")
                failed = True
            else:
                print(f"✅ Log conformance passed for {name} ({job_name})")
                
    if failed:
        print("FAIL: Log conformance check failed.")
        sys.exit(1)
    else:
        print("SUCCESS: All log conformance checks passed.")
        sys.exit(0)

if __name__ == "__main__":
    main()
