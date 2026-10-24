#!/usr/bin/env python3
"""Round-3 §10 repro against a patched preloop server (port 19110).

Redo of the pentest sequence:
  1. submit a workflow, read the queued job's agent_job_id from the DB
  2. mint a job runtime JWT locally (PRELOOP_HMAC_KEY is known for the repro)
  3. call GetJobDiagLogsSignedBlobURL 33 times (32-cap map, evict-oldest)
  4. PUT with the evicted token -> expect 404 (pre-fix: 201); the other 32
     tokens' PUTs still succeed. NOTE: which index is evicted depends on
     second-granularity `created_unix` ties (BTreeMap/jti order), so the
     script probes all 33 and asserts exactly one is dead.
  5. reaper demo: an aged orphan staging dir is swept, a fresh one is kept.

Env: PRELOOP_HOME (state dir), PRELOOP_SYSTEM_TOKEN, PRELOOP_HMAC_KEY (hex).
"""
import base64
import hashlib
import hmac
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.request
import urllib.error
import uuid

BASE = "http://127.0.0.1:19110"
SYS_TOKEN = os.environ["PRELOOP_SYSTEM_TOKEN"]
HMAC_KEY = bytes.fromhex(os.environ["PRELOOP_HMAC_KEY"])
STATE_DIR = os.environ["PRELOOP_HOME"]


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def mint_job_jwt(job_id: str, plan_id: str) -> str:
    now = int(time.time())
    claims = {
        "sub": f"preloop-job-{job_id}",
        "scp": f"Actions.Results:{plan_id}:{job_id}",
        "iss": "https://preloop.local",
        "iat": now,
        "nbf": now,
        "exp": now + 2999,
        "jti": str(uuid.uuid4()),
    }
    header = b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = b64url(json.dumps(claims).encode())
    sig = b64url(hmac.new(HMAC_KEY, f"{header}.{payload}".encode(), hashlib.sha256).digest())
    return f"{header}.{payload}.{sig}"


def api(method, path, token, body=None):
    req = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as r:
        return r.status, r.read()


def put_bearerless(url, data: bytes):
    req = urllib.request.Request(url, data=data, method="PUT")
    try:
        with urllib.request.urlopen(req) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


# 1. Submit a workflow; the job stays queued (no runner), hence live.
status, raw = api(
    "POST",
    "/api/v1/runs",
    SYS_TOKEN,
    {
        "workflow_yaml": "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n",
        "event": "push",
        "repository": "owner/repo",
    },
)
assert status == 200, (status, raw[:200])
run_id = json.loads(raw)["run_id"]
print(f"submitted run {run_id}")

# 2. Read the queued job's agent_job_id straight from the control DB.
db = sqlite3.connect(os.path.join(STATE_DIR, "state", "preloop.db"))
row = db.execute(
    "SELECT agent_job_id FROM job_requests WHERE run_id = ? AND result IS NULL LIMIT 1",
    (run_id,),
).fetchone()
assert row, "no live job request found"
job_id = row[0]
print(f"live job agent_job_id={job_id}")

# 3. Mint 33 diag upload URLs through the real Twirp endpoint.
plan_id = "repro-plan"
job_token = mint_job_jwt(job_id, plan_id)
urls = []
for _ in range(33):
    status, raw = api(
        "POST",
        "/twirp/results.services.receiver.Receiver/GetJobDiagLogsSignedBlobURL",
        job_token,
        {"workflow_run_backend_id": plan_id, "workflow_job_run_backend_id": job_id},
    )
    assert status == 200, (status, raw[:200])
    urls.append(json.loads(raw)["diag_logs_url"])
print(f"minted {len(urls)} diag upload URLs against the 32-cap map")

# 4. Exactly one token must be dead (evicted) -> 404; the other 32 -> 201.
results = [put_bearerless(url, b"x" * 1024) for url in urls]
dead = [i for i, st in enumerate(results) if st != 201]
live = [i for i, st in enumerate(results) if st == 201]
print(f"PUT statuses: {len(live)}x 201, dead indexes {dead} "
      f"({[results[i] for i in dead]})")
assert len(dead) == 1, f"expected exactly one evicted token, got {dead}"
assert results[dead[0]] == 404, "evicted token must be rejected with 404 (pre-fix: 201)"
print(f"OK: evicted token urls[{dead[0]}] -> 404; other 32 -> 201")

# 5. Reaper demo: an aged orphan staging dir is swept, a fresh one is kept.
diag_root = os.path.join(STATE_DIR, "state", "blobs", "diag")
os.makedirs(diag_root, exist_ok=True)
aged = os.path.join(diag_root, "orphan-aged")
fresh = os.path.join(diag_root, "orphan-fresh")
for d in (aged, fresh):
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "data"), "wb") as f:
        f.write(b"staged")
subprocess.run(["touch", "-d", "25 hours ago", aged, os.path.join(aged, "data")],
               check=True)
print("seeded aged (25h) + fresh staging dirs; waiting for the 10s reaper tick...")
deadline = time.time() + 45
while os.path.exists(aged) and time.time() < deadline:
    time.sleep(2)
aged_gone = not os.path.exists(aged)
fresh_kept = os.path.exists(fresh)
print(f"aged dir swept: {aged_gone} (expect True)")
print(f"fresh dir kept: {fresh_kept} (expect True)")
assert aged_gone and fresh_kept

print("\nALL REPRO CHECKS PASSED")
