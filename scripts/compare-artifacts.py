#!/usr/bin/env python3
"""Compare GitHub and aksh official-runner artifacts.

Both sides run the OFFICIAL runner — we're comparing the SERVER behavior.
- GitHub side: `jobs.json` from GH API (step names, conclusions), `steps.log` (output)
- aksh side: `run.json` from aksh API (job_steps with step records), or fallback to
  `status.json` + Worker diag logs.  Step log blobs from replay dir.

Compares:
  1. Job-level conclusions (per-job)
  2. Step names, order, and results
  3. Step log content (when available on both sides)
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path

scenario = sys.argv[1] if len(sys.argv) > 1 else None
if not scenario:
    raise SystemExit("usage: compare-artifacts.py SCENARIO")
root = Path(__file__).resolve().parents[1] / "benchmarks/compatibility/server/behavior" / scenario

def load_json(path: Path):
    return json.loads(path.read_text()) if path.exists() else None

# Normalize GitHub string conclusions to match runner status codes.
NORM = {"success": "succeeded", "failure": "failed", "cancelled": "cancelled",
        "skipped": "skipped"}

# Map runner conclusion codes (from Twirp WorkflowStepsUpdate) to strings.
CONCLUSION_CODES = {2: "success", 3: "failure", 4: "cancelled", 7: "skipped", 8: "cancelled", 0: None}
STATUS_CODES = {0: "pending", 4: "in_progress", 6: "completed"}

ANSI_RE = re.compile(r"(?:\x1b|\^\[)\[[0-9;]*[A-Za-z]")
# GitHub log timestamp prefix: 2024-06-25T12:34:56.1234567Z
TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\s*")

def strip_log_line(line: str) -> str:
    """Strip ANSI escapes and leading timestamps for content comparison."""
    line = ANSI_RE.sub("", line).lstrip("\ufeff")
    line = TIMESTAMP_RE.sub("", line).removeprefix("##[group]").rstrip()
    return "" if line == "##[endgroup]" else line


# ── Load data ───────────────────────────────────────────────────
gh_jobs_data = load_json(root / "github/jobs.json") or {}
aksh_status = load_json(root / "aksh-server/status.json") or {}
aksh_run = load_json(root / "aksh-server/run.json")
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
gh_steps_by_job: dict[str, list[dict]] = {}
for job in gh:
    jname = job.get("name", "")
    steps = []
    for step in job.get("steps", []):
        name = step.get("name", "")
        if name not in {"Set up job", "Complete job"}:
            steps.append({
                "name": name,
                "conclusion": step.get("conclusion", ""),
                "number": step.get("number", 0),
                "status": step.get("status", ""),
            })
    gh_steps_by_job[jname] = steps

# aksh steps — prefer run.json (API response), fallback to Worker diag logs.
aksh_steps_by_job: dict[str, list[dict]] = {}
aksh_step_source = "none"

if aksh_run and aksh_run.get("job_steps"):
    aksh_step_source = "api"
    job_steps = aksh_run["job_steps"]
    # job_steps uses aksh job display IDs. Never align unrelated jobs by
    # iteration order: aksh BTreeMaps and GitHub API ordering differ.
    for job_key, steps_list in job_steps.items():
        job_name = job_key
        steps = []
        for step in steps_list:
            if step.get("name", "") in {"Set up job", "Complete job"}:
                continue
            conclusion_code = step.get("conclusion", 0)
            conclusion_str = CONCLUSION_CODES.get(conclusion_code)
            if conclusion_str is None and isinstance(conclusion_code, str):
                conclusion_str = conclusion_code
            steps.append({
                "name": step.get("name", ""),
                "conclusion": conclusion_str or "",
                "number": step.get("number", 0),
                "status": STATUS_CODES.get(step.get("status", 0), "unknown"),
                "external_id": step.get("external_id", ""),
            })
        aksh_steps_by_job[job_name] = steps
else:
    # Fallback: parse Worker diagnostic logs (legacy path).
    aksh_step_source = "diag"
    diag_dir = root / "aksh-server/diag"
    if diag_dir.exists():
        worker_logs = sorted(diag_dir.glob("Worker_*.log"))
        all_steps: list[dict] = []
        for wlog in worker_logs:
            text = wlog.read_text(errors="replace")
            chunks = text.split("Processing step: DisplayName='")[1:]
            for chunk in chunks:
                name, _, body = chunk.partition("'")
                step_body = body.split("Processing step: DisplayName=", 1)[0]
                for boundary in ["Finalize job", "JobRunner] Job result", "complete_job"]:
                    step_body = step_body.split(boundary, 1)[0]
                results = re.findall(
                    r'"result": "(Succeeded|Failed|Skipped|succeeded|failed|skipped)"',
                    step_body,
                )
                if results:
                    all_steps.append({"name": name, "conclusion": results[0].lower()})
                elif "Skipping step" in step_body or "condition evaluation" in step_body:
                    all_steps.append({"name": name, "conclusion": "skipped"})
                else:
                    all_steps.append({"name": name, "conclusion": "unknown"})
        # Single-job fallback: assign all steps to first GitHub job name.
        gh_job_names = list(gh_steps_by_job.keys())
        job_name = gh_job_names[0] if gh_job_names else "default"
        aksh_steps_by_job[job_name] = all_steps

print(f"  (aksh step source: {aksh_step_source})")

step_order_match = True
step_results_match = True

for job_name in sorted(set(list(gh_steps_by_job.keys()) + list(aksh_steps_by_job.keys()))):
    gh_steps = gh_steps_by_job.get(job_name, [])
    ak_steps = aksh_steps_by_job.get(job_name, [])
    print(f"\n  Job: {job_name}")

    if not gh_steps and not ak_steps:
        print("    (no step data on either side)")
        continue
    elif not gh_steps:
        print("    (no GitHub step data)")
        for s in ak_steps:
            print(f"      aksh: \"{s['name']}\" → {s['conclusion']}")
        continue
    elif not ak_steps:
        print("    (no aksh step data)")
        for s in gh_steps:
            print(f"      github: \"{s['name']}\" → {s['conclusion']}")
        continue

    gh_names = [s["name"] for s in gh_steps]
    ak_names = [s["name"] for s in ak_steps]

    if gh_names != ak_names:
        step_order_match = False
        print(f"    ⚠️  Step order/names differ")
        print(f"      GitHub ({len(gh_names)} steps): {gh_names}")
        print(f"      aksh   ({len(ak_names)} steps): {ak_names}")

    max_len = max(len(gh_steps), len(ak_steps))
    for i in range(max_len):
        if i < len(gh_steps) and i < len(ak_steps):
            gs, aks = gh_steps[i], ak_steps[i]
            gc = NORM.get(gs["conclusion"], gs["conclusion"])
            ac = NORM.get(aks["conclusion"], aks["conclusion"])
            names_ok = gs["name"] == aks["name"]
            results_ok = gc == ac
            if names_ok and results_ok:
                print(f"    ✅ \"{gs['name']}\": github={gs['conclusion']} aksh={aks['conclusion']}")
            elif names_ok:
                step_results_match = False
                print(f"    ❌ \"{gs['name']}\": github={gs['conclusion']} aksh={aks['conclusion']}")
            else:
                step_results_match = False
                step_order_match = False
                print(f"    ❌ \"{gs['name']}\" vs \"{aks['name']}\": github={gs['conclusion']} aksh={aks['conclusion']}")
        elif i < len(gh_steps):
            step_results_match = False
            print(f"    ❌ [{i+1}] github=\"{gh_steps[i]['name']}\"({gh_steps[i]['conclusion']}) aksh=MISSING")
        else:
            step_results_match = False
            print(f"    ❌ [{i+1}] github=MISSING aksh=\"{ak_steps[i]['name']}\"({ak_steps[i]['conclusion']})")

# ── 3. Log content comparison ───────────────────────────────────
print("\n── Log Content ──")

log_content_match = False
gh_step_log_count = 0
ak_step_log_count = 0
log_match_pct = None

# GitHub's `gh run view --log` format is: job<TAB>step<TAB>timestamped-line.
gh_logs_by_step: dict[tuple[str, str], list[str]] = {}
gh_steps_log_path = root / "github/steps.log"
if gh_steps_log_path.exists():
    for line in gh_steps_log_path.read_text(errors="replace").splitlines():
        fields = line.split("\t", 2)
        if len(fields) != 3:
            continue
        job_name, step_name, content = fields
        if step_name in {"Set up job", "Complete job"}:
            continue
        stripped = strip_log_line(content).removeprefix("##[group]")
        if stripped and stripped != "##[endgroup]":
            gh_logs_by_step.setdefault((job_name, step_name), []).append(stripped)
    gh_step_log_count = len(gh_logs_by_step)

# aksh step logs from replay dir, mapped through the Twirp external step UUID.
ak_logs_by_step: dict[tuple[str, str], list[str]] = {}
step_identity = {
    step.get("external_id", ""): (job_name, step.get("name", ""))
    for job_name, steps in aksh_steps_by_job.items()
    for step in steps
    if step.get("external_id")
}
replay_dir = root / "aksh-server/replay/results"
if not replay_dir.exists():
    replay_dir = root / "aksh-server/.aksh/replay/results"

if replay_dir.exists() and step_identity:
    for run_dir in replay_dir.iterdir():
        if not run_dir.is_dir():
            continue
        for job_dir in run_dir.iterdir():
            if not job_dir.is_dir():
                continue
            for log_file in sorted(job_dir.glob("step-*.txt")):
                step_id = log_file.stem.removeprefix("step-")
                identity = step_identity.get(step_id)
                if identity is None:
                    continue
                lines = [
                    stripped
                    for line in log_file.read_text(errors="replace").splitlines()
                    if (stripped := strip_log_line(line))
                ]
                ak_logs_by_step[identity] = lines
    ak_step_log_count = len(ak_logs_by_step)

# Compare logs for steps present on both sides.
total_lines = 0
matching_lines = 0
diff_samples: list[str] = []

all_log_steps = sorted(set(gh_logs_by_step) | set(ak_logs_by_step))
if all_log_steps:
    for job_name, step_name in all_log_steps:
        identity = (job_name, step_name)
        gh_lines = gh_logs_by_step.get(identity, [])
        ak_lines = ak_logs_by_step.get(identity, [])
        label = f'{job_name} / {step_name}'
        if not gh_lines:
            print(f"  Step \"{label}\": aksh={len(ak_lines)} lines, github=none")
            continue
        if not ak_lines:
            print(f"  Step \"{label}\": github={len(gh_lines)} lines, aksh=none")
            continue

        max_len = max(len(gh_lines), len(ak_lines))
        step_matching = 0
        step_diffs: list[str] = []
        for i in range(max_len):
            total_lines += 1
            gl = gh_lines[i] if i < len(gh_lines) else ""
            al = ak_lines[i] if i < len(ak_lines) else ""
            if gl == al:
                step_matching += 1
                matching_lines += 1
            elif len(step_diffs) < 3:
                step_diffs.extend([
                    f"        line {i+1}:",
                    f"          github: {gl[:120]}",
                    f"          aksh:   {al[:120]}",
                ])

        if step_matching == max_len:
            print(f"  ✅ Step \"{label}\": {max_len} lines match")
        else:
            pct = step_matching / max_len * 100 if max_len else 0
            print(f"  ❌ Step \"{label}\": {step_matching}/{max_len} lines match ({pct:.1f}%)")
            for difference in step_diffs:
                print(difference)

    if total_lines > 0:
        log_match_pct = matching_lines / total_lines * 100
        log_content_match = (
            matching_lines == total_lines
            and all(gh_logs_by_step.get(identity) and ak_logs_by_step.get(identity) for identity in all_log_steps)
        )
else:
    if not gh_logs_by_step and not ak_logs_by_step:
        print("  (no log data on either side)")
    elif not gh_logs_by_step:
        print("  (no GitHub log data)")
    else:
        print("  (no aksh log data)")

# ── Summary ─────────────────────────────────────────────────────
print(f"\n── Summary ──")
print(f"  job_conclusion_match={job_match}")
print(f"  step_order_match={step_order_match}")
print(f"  step_results_match={step_results_match}")
print(f"  log_content_match={log_content_match}")
if log_match_pct is not None:
    print(f"  log_match_pct={log_match_pct:.1f}")
print(f"  github_jobs={len(gh)}")
print(f"  aksh_jobs={len(aksh_job_map)}")
print(f"  github_steps={sum(len(s) for s in gh_steps_by_job.values())}")
print(f"  aksh_steps={sum(len(s) for s in aksh_steps_by_job.values())}")
print(f"  aksh_step_source={aksh_step_source}")
print(f"  github_step_logs={gh_step_log_count}")
print(f"  aksh_step_logs={ak_step_log_count}")

if job_match and step_order_match and step_results_match and log_content_match:
    print(f"  overall=✅ FULL MATCH")
elif job_match and step_order_match and step_results_match:
    print(f"  overall=⚠️  STEP MATCH (log content differs or unavailable)")
elif job_match and step_results_match:
    print(f"  overall=⚠️  JOB+RESULT MATCH (step order differs)")
elif job_match:
    print(f"  overall=⚠️  JOB MATCH (step-level differences)")
else:
    print(f"  overall=❌ MISMATCH")
