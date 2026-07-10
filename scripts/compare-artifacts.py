#!/usr/bin/env python3
"""Compare GitHub and aksh official-runner artifacts.

Both sides run the OFFICIAL runner — we're comparing the SERVER behavior.
- GitHub side: `jobs.json` from GH API (step names, conclusions), `steps.log` (output)
- aksh side: `status.json` (job conclusions), `_diag/Worker_*.log` (step names, results)

Compares:
  1. Job-level conclusions (per-job)
  2. Step names and order
  3. Step-level results (success/failure/skipped)

Note: Step output text comparison is one-sided — GitHub has `steps.log` but
aksh side only has the Worker diagnostic (which is internal runner tracing, not
user-visible stdout). Full output comparison requires the aksh server to expose
its stored Twirp log uploads.
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path

scenario = sys.argv[1] if len(sys.argv) > 1 else None
if not scenario:
    raise SystemExit("usage: compare-artifacts.py SCENARIO")
root = Path(__file__).resolve().parents[1] / "benchmarks/real-world/results/server-compare" / scenario

def load_json(path: Path):
    return json.loads(path.read_text()) if path.exists() else None

NORM = {"success": "succeeded", "failure": "failed", "cancelled": "cancelled",
        "skipped": "skipped"}

# ── Load data ───────────────────────────────────────────────────
gh_jobs_data = load_json(root / "github/jobs.json") or {}
aksh_status = load_json(root / "aksh-server/status.json") or {}
gh = gh_jobs_data.get("jobs", [])

print(f"{'='*60}")
print(f"  Scenario: {scenario}")
print(f"{'='*60}")

# ── 1. Job-level comparison ─────────────────────────────────────
print("\n── Job Conclusions ──")
gh_job_map = {j["name"]: j.get("conclusion", "") for j in gh}
aksh_job_map = aksh_status.get("jobs", {})

all_jobs = sorted(set(list(gh_job_map.keys()) + list(aksh_job_map.keys())))
job_match = True
for job_name in all_jobs:
    gh_c = gh_job_map.get(job_name, "MISSING")
    ak_c = aksh_job_map.get(job_name, "MISSING")
    gh_norm = NORM.get(gh_c, gh_c)
    ak_norm = NORM.get(ak_c, ak_c)
    match = "✅" if gh_norm == ak_norm else "❌"
    if gh_norm != ak_norm:
        job_match = False
    print(f"  {match} {job_name}: github={gh_c} aksh={ak_c}")

# ── 2. Step-level comparison ────────────────────────────────────
print("\n── Step Results (per job) ──")
# GitHub steps from jobs.json
gh_steps_by_job: dict[str, list[tuple[str, str]]] = {}
for job in gh:
    jname = job.get("name", "")
    steps = []
    for step in job.get("steps", []):
        name = step.get("name", "")
        if name not in {"Set up job", "Complete job"}:
            steps.append((name, step.get("conclusion", "")))
    gh_steps_by_job[jname] = steps

# aksh steps from Worker diagnostic
aksh_steps_all: list[tuple[str, str]] = []
worker_logs = sorted((root / "aksh-server/diag").glob("Worker_*.log"))
for wlog in worker_logs:
    text = wlog.read_text(errors="replace")
    chunks = text.split("Processing step: DisplayName='")[1:]
    for chunk in chunks:
        name, _, body = chunk.partition("'")
        # Limit search to this step's section — stop before next step or job finalization
        step_body = body.split("Processing step: DisplayName=", 1)[0]
        for boundary in ["Finalize job", "JobRunner] Job result", "complete_job"]:
            step_body = step_body.split(boundary, 1)[0]
        results = re.findall(
            r'"result": "(Succeeded|Failed|Skipped|succeeded|failed|skipped)"',
            step_body,
        )
        # Use first result (the step's own result), not last (could be telemetry)
        if results:
            aksh_steps_all.append((name, results[0].lower()))
        elif "Skipping step" in step_body or "condition evaluation" in step_body:
            aksh_steps_all.append((name, "skipped"))
        else:
            aksh_steps_all.append((name, "unknown"))

step_order_match = True
step_results_match = True

# Flatten GitHub steps for comparison
gh_steps_flat = [(n, c) for steps in gh_steps_by_job.values() for n, c in steps]
gh_names = [n for n, _ in gh_steps_flat]
ak_names = [n for n, _ in aksh_steps_all]

if not gh_steps_flat and not aksh_steps_all:
    print("  (no step data on either side)")
elif not gh_steps_flat:
    print("  (no GitHub step data)")
    for name, result in aksh_steps_all:
        print(f"     aksh: \"{name}\" → {result}")
elif not aksh_steps_all:
    print("  (no aksh step data)")
    for name, result in gh_steps_flat:
        print(f"     github: \"{name}\" → {result}")
else:
    if gh_names != ak_names:
        step_order_match = False
        print(f"  ⚠️  Step order/names differ")
        print(f"    GitHub ({len(gh_names)} steps): {gh_names}")
        print(f"    aksh   ({len(ak_names)} steps): {ak_names}")
    
    # Compare results for matching steps
    if len(gh_steps_flat) == len(aksh_steps_all):
        for (gn, gc), (an, ac) in zip(gh_steps_flat, aksh_steps_all):
            gh_norm = NORM.get(gc, gc)
            names_match = gn == an
            results_match = gh_norm == ac
            if names_match and results_match:
                print(f"  ✅ \"{gn}\": github={gc} aksh={ac}")
            elif names_match:
                step_results_match = False
                print(f"  ❌ \"{gn}\": github={gc} aksh={ac}")
            else:
                step_results_match = False
                step_order_match = False
                print(f"  ❌ \"{gn}\" vs \"{an}\": github={gc} aksh={ac}")
    else:
        step_results_match = False
        # Show what we have
        max_len = max(len(gh_steps_flat), len(aksh_steps_all))
        for i in range(max_len):
            g = gh_steps_flat[i] if i < len(gh_steps_flat) else ("—", "—")
            a = aksh_steps_all[i] if i < len(aksh_steps_all) else ("—", "—")
            print(f"  {'✅' if g[0]==a[0] and NORM.get(g[1],g[1])==a[1] else '❌'} [{i+1}] github=\"{g[0]}\"({g[1]}) aksh=\"{a[0]}\"({a[1]})")

# ── Summary ─────────────────────────────────────────────────────
print(f"\n── Summary ──")
print(f"  job_conclusion_match={job_match}")
print(f"  step_order_match={step_order_match}")
print(f"  step_results_match={step_results_match}")
print(f"  github_jobs={len(gh)}")
print(f"  aksh_jobs={len(aksh_job_map)}")
print(f"  github_steps={len(gh_steps_flat)}")
print(f"  aksh_steps={len(aksh_steps_all)}")

if job_match and step_order_match and step_results_match:
    print(f"  overall=✅ FULL MATCH")
elif job_match and step_results_match:
    print(f"  overall=⚠️  JOB+RESULT MATCH (step order differs)")  
elif job_match:
    print(f"  overall=⚠️  JOB MATCH (step-level differences)")
else:
    print(f"  overall=❌ MISMATCH")
