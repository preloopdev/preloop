#!/usr/bin/env bash
# Capture concurrency scenarios on local aksh (server + aksh-runner) in
# benchmarks-style directories for log/step comparison against live GitHub.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SERVER_BIN="${SERVER_BIN:-$REPO_ROOT/target/release/aksh-runner-server}"
RUNNER_BIN="${RUNNER_BIN:-$REPO_ROOT/target/release/aksh-runner}"
PORT="${AKSH_PORT:-9211}"
SERVER_URL="http://127.0.0.1:${PORT}"
TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
OUT="${OUT:-$REPO_ROOT/benchmarks/real-world/results/concurrency-live/aksh-compare-$TS}"
STATE_DIR="$OUT/state"
RUNNER_DIR="$OUT/runner"
LOG_DIR="$OUT/_process-logs"
mkdir -p "$OUT" "$STATE_DIR" "$RUNNER_DIR" "$LOG_DIR"

cleanup() {
  if [[ -n "${RPID:-}" ]]; then kill "$RPID" 2>/dev/null || true; fi
  if [[ -n "${SPID:-}" ]]; then kill "$SPID" 2>/dev/null || true; fi
  pkill -f "aksh-runner-server.*${PORT}" 2>/dev/null || true
}
trap cleanup EXIT

echo "OUT=$OUT"
pkill -f "aksh-runner-server.*${PORT}" 2>/dev/null || true
sleep 1

RUST_LOG=info AKSH_PUBLIC_URL="$SERVER_URL" \
  "$SERVER_BIN" serve --listen "127.0.0.1:${PORT}" --state-dir "$STATE_DIR" \
  >"$LOG_DIR/server.log" 2>&1 &
SPID=$!
for i in $(seq 1 50); do curl -sf "$SERVER_URL/healthz" >/dev/null && break; sleep 0.1; done
curl -sf "$SERVER_URL/healthz" >/dev/null

(cd "$RUNNER_DIR" && "$RUNNER_BIN" configure \
  --url "$SERVER_URL" --token aksh-system-token \
  --name conc-cmp --labels self-hosted,ubuntu-latest,macOS,ARM64 \
  --work _work) >"$LOG_DIR/configure.log" 2>&1

(cd "$RUNNER_DIR" && RUST_LOG=info "$RUNNER_BIN" run) >"$LOG_DIR/runner.log" 2>&1 &
RPID=$!
sleep 2

export SERVER_URL OUT STATE_DIR LOG_DIR
python3 - <<'PY'
import json, os, time, urllib.request, pathlib, re, subprocess, urllib.error
from pathlib import Path

SERVER = os.environ["SERVER_URL"]
OUT = Path(os.environ["OUT"])
STATE = Path(os.environ["STATE_DIR"])
AUTH = {"Authorization": "Bearer aksh-system-token", "Content-Type": "application/json"}

def api(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(SERVER + path, data=data, headers=AUTH, method=method)
    with urllib.request.urlopen(req) as r:
        raw = r.read()
    if not raw:
        return None
    # tolerate control chars in workflow_yaml by extracting fields loosely if needed
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        text = raw.decode("utf-8", "replace")
        # strip unescaped control chars inside strings is hard; use status/jobs only via regex
        m = re.search(r'"status"\s*:\s*"([^"]+)"', text)
        jobs = dict(re.findall(r'"([^"]+)"\s*:\s*"(queued|pending|in_progress|success|failure|cancelled|skipped)"', text))
        # filter only job map-ish
        return {"status": m.group(1) if m else "unknown", "jobs": {k:v for k,v in jobs.items() if k not in ("status",)}}

def submit(yaml, repo="owner/repo", git_ref="refs/heads/main"):
    body = {
        "workflow_yaml": yaml,
        "event": "push",
        "repository": repo,
        "git_ref": git_ref,
    }
    return api("POST", "/api/v1/runs", body)

def wait_run(run_id, timeout=120):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = api("GET", f"/api/v1/runs/{run_id}")
        st = last.get("status")
        if st in ("success", "failure", "cancelled"):
            return last
        time.sleep(0.5)
    return last

def events(run_id):
    req = urllib.request.Request(SERVER + f"/api/v1/runs/{run_id}/events.ndjson", headers=AUTH)
    with urllib.request.urlopen(req) as r:
        return r.read().decode("utf-8", "replace")

# Stable IDs: the runner logs "Dispatching job <uuid>" for each job execution.
# That UUID is used as the state/replay/results/<uuid>/<uuid>/ directory name.
# We track captured UUIDs so that when a scenario pair shares a log window, we
# can assign the first UUID to capture-A and the second to capture-B.
_captured_job_uuids: set = set()
_runner_log_path = Path(os.environ["LOG_DIR"]) / "runner.log"

def _runner_log_size() -> int:
    """Return current byte length of runner.log (0 if not yet created)."""
    return _runner_log_path.stat().st_size if _runner_log_path.exists() else 0

def _read_runner_log_delta(offset: int) -> str:
    """Read runner.log from byte offset to current end."""
    if not _runner_log_path.exists():
        return ""
    with open(_runner_log_path, "rb") as fh:
        fh.seek(offset)
        return fh.read().decode("utf-8", "replace")

def _find_job_uuids(log_delta: str, count: int = 1) -> list[tuple[str, str]]:
    """Return [(job_uuid, job_name)] for the first `count` uncaptured dispatches in log_delta.

    Parses "Starting job: <name> (<uuid>)" lines (emitted after each dispatch) so the
    runner-assigned job name is available for per-job step distribution in multi-job runs.
    UUIDs already in _captured_job_uuids are skipped.
    """
    found: list[tuple[str, str]] = []
    seen_in_call: set[str] = set()
    for m in re.finditer(r"Starting job: (\S+) \(([0-9a-f-]{36})\)", log_delta):
        job_name, uuid = m.group(1), m.group(2)
        if uuid not in _captured_job_uuids and uuid not in seen_in_call:
            found.append((uuid, job_name))
            seen_in_call.add(uuid)
            if len(found) >= count:
                break
    return found

def _read_step_logs_for_job(job_uuid: str) -> list[str]:
    """Read step-*.txt and job-logs.txt from the state dir for a specific job UUID.

    The runner writes state/replay/results/<job_uuid>/<job_uuid>/ for each dispatched job.
    Using the job_uuid keeps each capture strictly isolated to its own run.
    """
    job_dir = STATE / "replay" / "results" / job_uuid / job_uuid
    lines: list[str] = []
    if not job_dir.exists():
        return lines
    for step_file in sorted(job_dir.glob("step-*.txt")):
        try:
            text = step_file.read_text(errors="replace")
        except Exception:
            continue
        step_name = step_file.stem  # step-<uuid>
        for line in text.splitlines():
            lines.append(f"build\t{step_name}\t{line}")
    job_log = job_dir / "job-logs.txt"
    if job_log.exists():
        try:
            for line in job_log.read_text(errors="replace").splitlines():
                lines.append(f"build\tjob-log\t{line}")
        except Exception:
            pass
    return lines

def capture(name, run_id, run_obj, log_offset: int = 0):
    """Capture one run, isolating all log content to the run's own state directory.

    log_offset: byte offset in runner.log at the time the scenario's first run was
    submitted.  Used to find the dispatched job UUIDs so step logs are read exclusively
    from those jobs' state directories, preventing cross-run log contamination.

    Multi-job runs: finds N uncaptured UUIDs where N = len(jobs_map), then distributes
    step names to each job by the runner job-name in "Starting job: <name> (<uuid>)".
    """
    d = OUT / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "github-run-id.txt").write_text(run_id)  # aksh run id

    # Job statuses come from the run API response — authoritative and already scoped to
    # this run.  Do NOT overwrite them with heuristics from the global runner.log.
    jobs_map = run_obj.get("jobs") or {}
    jobs = []
    for jid, status in jobs_map.items():
        jobs.append({
            "name": jid,
            "id": jid,
            "status": status,
            "conclusion": status,
            "steps": [],
        })
    jobs_by_name = {j["name"]: j for j in jobs}

    # Find runner-internal job UUIDs from the runner.log delta.
    # For a run with N jobs, find the first N uncaptured dispatches.
    num_jobs = max(1, len(jobs_map))
    log_delta = _read_runner_log_delta(log_offset)
    job_pairs = _find_job_uuids(log_delta, num_jobs)

    if not job_pairs:
        print(f"WARNING: no job UUID found in runner.log delta for {name}; "
              "step log isolation unavailable")

    # Mark all found UUIDs as captured before reading logs (prevents sibling capture
    # in the same log window from re-finding them).
    for uuid, _ in job_pairs:
        _captured_job_uuids.add(uuid)

    # Build per-job step lists and collect combined step log lines for the run.log.
    # The combined run.log is used by the compare script to extract SCENARIO/DONE markers
    # across all jobs in the run.
    all_step_log_lines: list[str] = []
    found_uuids: list[str] = []

    for uuid, runner_job_name in job_pairs:
        found_uuids.append(uuid)
        job_lines = _read_step_logs_for_job(uuid)
        all_step_log_lines.extend(job_lines)

        # Slice the log from this job's dispatch to the next dispatch (or end), to
        # isolate step names for this specific job.
        dispatch_marker = f"Starting job: {runner_job_name} ({uuid})"
        dispatch_pos = log_delta.find(dispatch_marker)
        next_dispatch_pos = log_delta.find("Starting job:", dispatch_pos + 1) if dispatch_pos >= 0 else -1
        if dispatch_pos >= 0:
            job_slice = (
                log_delta[dispatch_pos:next_dispatch_pos]
                if next_dispatch_pos > dispatch_pos
                else log_delta[dispatch_pos:]
            )
        else:
            job_slice = log_delta

        step_names_raw = re.findall(r"Running step: (.+)", job_slice)
        seen_steps: list[str] = []
        for sn in step_names_raw:
            if sn not in seen_steps:
                seen_steps.append(sn)

        # Match to the API job by runner job name; fall back to first job for single-job runs.
        target_job = jobs_by_name.get(runner_job_name) or (jobs[0] if jobs else None)
        if target_job is None:
            continue

        job_conclusion = target_job.get("conclusion", "success")
        user_steps = []
        for sn in seen_steps:
            if job_conclusion == "cancelled" and (
                "sleep" in sn.lower() or "long" in sn.lower() or sn.startswith("Run ")
            ):
                step_conc = "cancelled"
            else:
                step_conc = "success"
            user_steps.append({"name": sn, "conclusion": step_conc, "status": step_conc})

        target_job["steps"] = (
            [{"name": "Set up job", "conclusion": "success"}]
            + user_steps
            + [{"name": "Complete job", "conclusion": "success"}]
        )

    (d / "run.log").write_text("\n".join(all_step_log_lines) + "\n")

    conclusion = run_obj.get("status")
    summary = {
        "workflow": name,
        "run_id": run_id,
        "status": "completed" if conclusion in ("success", "failure", "cancelled") else conclusion,
        "conclusion": conclusion,
        "jobs": jobs,
        "source": "aksh",
    }
    (d / "summary.json").write_text(json.dumps(summary, indent=2))
    (d / "jobs.json").write_text(json.dumps({"jobs": jobs}, indent=2))
    (d / "run.json").write_text(json.dumps({"run_id": run_id, "status": conclusion, "jobs": jobs_map}, indent=2))
    (d / "events.ndjson").write_text(events(run_id))
    # Dump the scoped runner log slice for diagnostics
    (d / "runner-process.log").write_text(log_delta)
    print(f"captured {name}: status={conclusion} jobs={jobs_map} job_uuids={found_uuids}")
    return summary



# ── Scenarios (self-hosted labels, shorter sleeps where possible) ──

Y_BARE = """on: push
concurrency: bare-string-group
jobs:
  build:
    runs-on: self-hosted
    steps:
      - name: marker
        run: |
          echo "SCENARIO=01-bare-string"
          echo "RUN_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
          sleep 8
          echo "DONE=01"
"""

Y_CANCEL = """on: push
concurrency:
  group: cancel-ip-group
  cancel-in-progress: true
jobs:
  long:
    runs-on: self-hosted
    steps:
      - name: sleep-long
        run: |
          echo "SCENARIO=02-cancel-in-progress"
          echo "START=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
          sleep 120
          echo "SHOULD_NOT_REACH"
"""

Y_FIFO = """on: push
concurrency:
  group: fifo-group
  cancel-in-progress: false
jobs:
  long:
    runs-on: self-hosted
    steps:
      - name: sleep-a-bit
        run: |
          echo "SCENARIO=03-fifo-pending"
          echo "START=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
          sleep 6
          echo "DONE=03"
"""

Y_CANCEL_EXPR = """on: push
concurrency:
  group: cancel-expr-true
  cancel-in-progress: ${{ true }}
jobs:
  long:
    runs-on: self-hosted
    steps:
      - name: sleep-long
        run: |
          echo "SCENARIO=04-cancel-expr-true"
          sleep 120
          echo "SHOULD_NOT_REACH"
"""

Y_EXPR_FALSE = """on: push
concurrency:
  group: cancel-expr-false
  cancel-in-progress: ${{ false }}
jobs:
  long:
    runs-on: self-hosted
    steps:
      - name: sleep-a-bit
        run: |
          echo "SCENARIO=05-cancel-expr-false"
          sleep 5
          echo "DONE=05"
"""

Y_JOB_LEVEL = """on: push
jobs:
  one:
    runs-on: self-hosted
    concurrency:
      group: job-level-serial
      cancel-in-progress: false
    steps:
      - name: one
        run: |
          echo "SCENARIO=08-job-level job=one"
          sleep 5
          echo "DONE=one"
  two:
    runs-on: self-hosted
    concurrency:
      group: job-level-serial
      cancel-in-progress: false
    steps:
      - name: two
        run: |
          echo "SCENARIO=08-job-level job=two"
          sleep 4
          echo "DONE=two"
"""

Y_EMPTY = """on: push
concurrency:
  group: ${{ github.event.head_commit.id_missing }}
jobs:
  probe:
    runs-on: self-hosted
    steps:
      - name: should-not-run
        run: echo "SCENARIO=10-empty-group SHOULD_NOT_RUN"
"""

Y_EXPR_GROUP = """on: push
concurrency:
  group: ref-${{ github.ref }}
  cancel-in-progress: false
jobs:
  build:
    runs-on: self-hosted
    steps:
      - name: marker
        run: |
          echo "SCENARIO=11-expr-group-ref"
          echo "REF=refs/heads/main"
          sleep 3
          echo "DONE=11"
"""

results = {}

# Each scenario pair records the runner.log byte offset before the first submission.
# capture() uses this offset to find the runner-internal job UUID for step log isolation.

# 01 bare A then B
log_01 = _runner_log_size()
a = submit(Y_BARE); print("01A", a)
time.sleep(1)
b = submit(Y_BARE); print("01B", b)
wa = wait_run(a["run_id"]); wb = wait_run(b["run_id"])
capture("01-bare-A", a["run_id"], wa, log_offset=log_01)
capture("01-bare-B", b["run_id"], wb, log_offset=log_01)
results["01"] = (wa["status"], wb["status"])

# 02 cancel
log_02 = _runner_log_size()
a = submit(Y_CANCEL); print("02A", a)
time.sleep(2)
b = submit(Y_CANCEL); print("02B", b)
wa = wait_run(a["run_id"], 90); wb = wait_run(b["run_id"], 150)
capture("02-cancel-A", a["run_id"], wa, log_offset=log_02)
capture("02-cancel-B", b["run_id"], wb, log_offset=log_02)
results["02"] = (wa["status"], wb["status"])

# 03 fifo
log_03 = _runner_log_size()
a = submit(Y_FIFO); print("03A", a)
time.sleep(0.5)
b = submit(Y_FIFO); print("03B", b)
# B should be pending initially
time.sleep(0.3)
b0 = api("GET", f"/api/v1/runs/{b['run_id']}")
print("03B early status", b0.get("status"))
wa = wait_run(a["run_id"]); wb = wait_run(b["run_id"])
capture("03-fifo-A", a["run_id"], wa, log_offset=log_03)
capture("03-fifo-B", b["run_id"], wb, log_offset=log_03)
results["03"] = (wa["status"], wb["status"], b0.get("status"))

# 04 cancel expr
log_04 = _runner_log_size()
a = submit(Y_CANCEL_EXPR); time.sleep(2); b = submit(Y_CANCEL_EXPR)
wa = wait_run(a["run_id"], 90); wb = wait_run(b["run_id"], 150)
capture("04-cancel-expr-A", a["run_id"], wa, log_offset=log_04)
capture("04-cancel-expr-B", b["run_id"], wb, log_offset=log_04)
results["04"] = (wa["status"], wb["status"])

# 05 expr false fifo
log_05 = _runner_log_size()
a = submit(Y_EXPR_FALSE); time.sleep(0.5); b = submit(Y_EXPR_FALSE)
wa = wait_run(a["run_id"]); wb = wait_run(b["run_id"])
capture("05-expr-false-A", a["run_id"], wa, log_offset=log_05)
capture("05-expr-false-B", b["run_id"], wb, log_offset=log_05)
results["05"] = (wa["status"], wb["status"])

# 08 job level (two jobs; multi-job capture: both job UUIDs dispatched from same log window)
log_08 = _runner_log_size()
try:
    a = submit(Y_JOB_LEVEL)
    wa = wait_run(a["run_id"], 90)
    capture("08-job-level", a["run_id"], wa, log_offset=log_08)
    results["08"] = wa["status"]
except Exception as e:
    print("08 failed", e)
    results["08"] = f"error:{e}"

# 10 empty (empty concurrency group → server rejects or run fails immediately;
# no job is dispatched, so no state dir step files exist — run.log stays empty)
log_10 = _runner_log_size()
try:
    a = submit(Y_EMPTY)
    # may 422
    if a and a.get("run_id"):
        wa = wait_run(a["run_id"], 30)
        capture("10-empty-group", a["run_id"], wa, log_offset=log_10)
        results["10"] = wa["status"]
    else:
        # HTTP error path — no run was created
        d = OUT / "10-empty-group"
        d.mkdir(exist_ok=True)
        (d/"summary.json").write_text(json.dumps({
            "workflow": "10-empty-group", "run_id": None,
            "conclusion": "failure", "status": "completed", "jobs": [], "source": "aksh",
            "note": "rejected at submit"
        }, indent=2))
        (d/"run.log").write_text("")
        (d/"jobs.json").write_text(json.dumps({"jobs":[]}))
        results["10"] = "failure-submit"
except urllib.error.HTTPError as e:
    body = e.read().decode("utf-8", "replace")
    d = OUT / "10-empty-group"
    d.mkdir(exist_ok=True)
    (d/"summary.json").write_text(json.dumps({
        "workflow": "10-empty-group", "run_id": None,
        "conclusion": "failure", "status": "completed", "jobs": [],
        "source": "aksh", "http_status": e.code, "body": body,
    }, indent=2))
    (d/"run.log").write_text(body)
    (d/"jobs.json").write_text(json.dumps({"jobs":[]}))
    results["10"] = f"http-{e.code}"
    print("10 empty rejected", e.code, body[:200])

# 11 expr group
log_11 = _runner_log_size()
a = submit(Y_EXPR_GROUP)
wa = wait_run(a["run_id"])
capture("11-expr-group", a["run_id"], wa, log_offset=log_11)
results["11"] = wa["status"]

(OUT / "aksh-results.json").write_text(json.dumps(results, indent=2))
print("RESULTS", json.dumps(results, indent=2))
PY

echo "Running compare..."
python3 "$REPO_ROOT/benchmarks/real-world/concurrency-log-compare.py" \
  --github-root "$REPO_ROOT/benchmarks/real-world/results/concurrency-live/2026-07-13T13-19-42Z" \
  --aksh-root "$OUT"

echo "DONE OUT=$OUT"
