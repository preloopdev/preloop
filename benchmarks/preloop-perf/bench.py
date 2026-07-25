#!/usr/bin/env python3
"""Preloop end-to-end performance benchmark.

Measures what a developer actually feels when running `preloop run` against a
warm local engine, and compares it against the same shell work executed
directly on the host.

Design constraints
------------------
* Deterministic: the benchmark workspace is generated from a fixed recipe, the
  workflow does fixed-size work, and no step touches the network.
* Honest: the timed path is the shipped one (release binaries, real VM, real
  runner protocol). Nothing is stubbed for the benchmark's benefit.
* Isolated: the harness runs its own engine on its own port with its own
  ``PRELOOP_HOME`` so it cannot inherit or corrupt a developer's state.

Network access is confined to *setup*: SmolVM pulls the digest-pinned base
image inside the freshly created golden VM and the orchestrator apt-installs
the guest toolchain into it. Both happen before any timed region, and no
measured run touches the network.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

# ── configuration ────────────────────────────────────────────────────────────

REPO = Path(__file__).resolve().parents[2]
CACHE = Path(os.environ.get("PRELOOP_BENCH_CACHE", Path.home() / ".cache/preloop-perf"))
HOME = CACHE / "home"
WS = CACHE / "ws"
ENGINE_LOG = CACHE / "engine.log"

# Bump when the fixture recipe changes so cached workspaces are rebuilt.
FIXTURE_VERSION = 1
FIXTURE_DIRS = 12
FIXTURE_FILES_PER_DIR = 25

LISTEN = os.environ.get("PRELOOP_BENCH_LISTEN", "127.0.0.1:19090")
BASE_URL = f"http://{LISTEN}"
TOKEN = "0" * 64
POOL_SIZE = 2

WARMUP_RUNS = 2
CLI_RUNS = int(os.environ.get("PRELOOP_BENCH_CLI_RUNS", "9"))
API_RUNS = int(os.environ.get("PRELOOP_BENCH_API_RUNS", "9"))
HOST_RUNS = int(os.environ.get("PRELOOP_BENCH_HOST_RUNS", "5"))

RUNNER_TARGET = "aarch64-unknown-linux-gnu"
RUNNER_BUNDLE = REPO / "target" / RUNNER_TARGET / "release"
CLI_BIN = REPO / "target/release/preloop"

# Ubuntu 24.04, pinned by digest so the guest toolchain cannot drift mid-session.
# Served from ECR Public rather than Docker Hub: SmolVM re-pulls the image inside
# every freshly created VM, and Docker Hub's anonymous rate limit turns that into
# a hard failure after a few dozen boots.
BASE_IMAGE = os.environ.get(
    "PRELOOP_BENCH_BASE_IMAGE",
    "public.ecr.aws/ubuntu/ubuntu@sha256:"
    "22a8228e1e48cbe7e0e0f2056e752ffb8a35950cda150a4e5e16417200bec648",
)

# Fixed-size guest work. Kept identical in the workflow and the host baseline.
FSGEN_FILES = 1500
NODE_ITERATIONS = 3_000_000

GIT_ENV = {
    "GIT_AUTHOR_NAME": "preloop-bench",
    "GIT_AUTHOR_EMAIL": "bench@preloop.local",
    "GIT_COMMITTER_NAME": "preloop-bench",
    "GIT_COMMITTER_EMAIL": "bench@preloop.local",
    "GIT_AUTHOR_DATE": "2020-01-01T00:00:00Z",
    "GIT_COMMITTER_DATE": "2020-01-01T00:00:00Z",
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_CONFIG_SYSTEM": "/dev/null",
}

WORKFLOW = f"""name: bench
on: push

jobs:
  bench:
    runs-on: self-hosted
    steps:
      - name: noop
        run: 'true'
      - name: fsgen
        run: |
          set -eu
          out="$RUNNER_TEMP/fsgen"
          rm -rf "$out"; mkdir -p "$out"
          i=0
          while [ $i -lt {FSGEN_FILES} ]; do
            printf '%s\\n' "line-$i-{'a' * 60}" > "$out/f$i.txt"
            i=$((i+1))
          done
          sync
      - name: fsread
        run: |
          set -eu
          find "$RUNNER_TEMP/fsgen" -type f -print0 | xargs -0 cat | sha256sum
      - name: node
        run: node -e 'let h=0;for(let i=0;i<{NODE_ITERATIONS};i++){{h=(h*31+i)>>>0}}console.log(h)'
"""

HOST_BASELINE = f"""#!/usr/bin/env bash
# Host-native equivalent of benchmarks/preloop-perf workflow `bench.yml`.
set -euo pipefail
RUNNER_TEMP="$(mktemp -d)"
trap 'rm -rf "$RUNNER_TEMP"' EXIT

sha256() {{ if command -v sha256sum >/dev/null; then sha256sum; else shasum -a 256; fi; }}

# step: noop
true

# step: fsgen
out="$RUNNER_TEMP/fsgen"
rm -rf "$out"; mkdir -p "$out"
i=0
while [ $i -lt {FSGEN_FILES} ]; do
  printf '%s\\n' "line-$i-{'a' * 60}" > "$out/f$i.txt"
  i=$((i+1))
done
sync

# step: fsread
find "$RUNNER_TEMP/fsgen" -type f -print0 | xargs -0 cat | sha256 >/dev/null

# step: node
node -e 'let h=0;for(let i=0;i<{NODE_ITERATIONS};i++){{h=(h*31+i)>>>0}}console.log(h)' >/dev/null
"""


def log(message: str) -> None:
    print(f"[bench] {message}", file=sys.stderr, flush=True)


def die(message: str) -> "NoReturn":  # type: ignore[name-defined]
    print(f"ERROR: {message}", file=sys.stderr, flush=True)
    sys.exit(1)


def run(cmd, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


# ── build ────────────────────────────────────────────────────────────────────


def ensure_build() -> None:
    """Build the release host CLI/engine and the Linux guest runner."""
    log("building release binaries")
    steps = [
        (
            ["cargo", "zigbuild", "--release", "-p", "aksh-runner", "--target", RUNNER_TARGET],
            "guest runner",
        ),
        (["cargo", "build", "--release", "-p", "preloop-cli"], "host CLI/engine"),
    ]
    for cmd, label in steps:
        result = run(cmd, cwd=REPO)
        if result.returncode != 0:
            die(f"failed to build {label}:\n{result.stdout}\n{result.stderr}")
    if not CLI_BIN.is_file():
        die(f"missing {CLI_BIN}")
    if not (RUNNER_BUNDLE / "preloop-runner").is_file():
        die(f"missing {RUNNER_BUNDLE / 'preloop-runner'}")


# ── fixture ──────────────────────────────────────────────────────────────────


def fixture_stamp() -> str:
    payload = json.dumps(
        {
            "version": FIXTURE_VERSION,
            "dirs": FIXTURE_DIRS,
            "files": FIXTURE_FILES_PER_DIR,
            "workflow": WORKFLOW,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def ensure_fixture() -> None:
    """Materialise a deterministic Git workspace used as the run's repository."""
    stamp = fixture_stamp()
    marker = WS / ".bench-stamp"
    if marker.is_file() and marker.read_text().strip() == stamp:
        # Reset any residue from a previous run so the snapshot input is fixed.
        result = run(["git", "status", "--porcelain"], cwd=WS)
        if result.returncode == 0 and not result.stdout.strip():
            return
        run(["git", "reset", "--hard", "--quiet"], cwd=WS)
        run(["git", "clean", "-qfdx", "-e", ".bench-stamp"], cwd=WS)
        return

    log("generating benchmark workspace fixture")
    if WS.exists():
        shutil.rmtree(WS)
    WS.mkdir(parents=True)
    for d in range(FIXTURE_DIRS):
        directory = WS / "src" / f"mod{d:02d}"
        directory.mkdir(parents=True)
        for f in range(FIXTURE_FILES_PER_DIR):
            digest = hashlib.sha256(f"{d}:{f}".encode()).hexdigest()
            body = "\n".join(digest[i : i + 16] for i in range(0, 64, 16)) * 20
            (directory / f"file{f:02d}.txt").write_text(body + "\n")
    (WS / "README.md").write_text("preloop performance benchmark fixture\n")
    workflows = WS / ".github/workflows"
    workflows.mkdir(parents=True)
    (workflows / "bench.yml").write_text(WORKFLOW)

    env = {**os.environ, **GIT_ENV}
    for cmd in (
        ["git", "init", "--quiet", "-b", "main"],
        ["git", "add", "--all"],
        ["git", "commit", "--quiet", "-m", "benchmark fixture"],
    ):
        result = run(cmd, cwd=WS, env=env)
        if result.returncode != 0:
            die(f"fixture git step {cmd[1]} failed: {result.stderr}")
    marker.write_text(stamp + "\n")


def ensure_host_baseline() -> Path:
    path = CACHE / "host-baseline.sh"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(HOST_BASELINE)
    path.chmod(0o755)
    return path


# ── engine lifecycle ─────────────────────────────────────────────────────────


def port_free() -> bool:
    host, port = LISTEN.split(":")
    with socket.socket() as probe:
        return probe.connect_ex((host, int(port))) != 0


def foreign_engine_pids() -> list[int]:
    # BSD pgrep has no `\s`; match the literal argv fragment instead.
    result = run(["pgrep", "-f", "preloop engine"])
    pids = []
    for line in result.stdout.split():
        try:
            pid = int(line)
        except ValueError:
            continue
        if pid != os.getpid():
            pids.append(pid)
    return pids


def stop_foreign_engines() -> None:
    """Any other engine shares the `preloop-runner-*` VM namespace with us."""
    pids = foreign_engine_pids()
    if not pids:
        return
    log(f"stopping conflicting preloop engine(s): {pids}")
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.time() + 30
    while time.time() < deadline and foreign_engine_pids():
        time.sleep(0.2)
    for pid in foreign_engine_pids():
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    time.sleep(1.0)


def delete_bench_machines() -> None:
    result = run(["smolvm", "machine", "ls", "--json"])
    if result.returncode != 0:
        return
    try:
        machines = json.loads(result.stdout)
    except json.JSONDecodeError:
        return
    names = [m.get("name", "") for m in machines if m.get("name", "").startswith("preloop-runner-")]
    # A golden VM is the copy-on-write fork base of its clones; SmolVM refuses
    # to delete or restart it while any clone is alive, so clones go first.
    names.sort(key=lambda name: name.endswith("-golden"))
    for name in names:
        run(["smolvm", "machine", "delete", "--name", name])
    if names:
        time.sleep(1.0)
    purge_orphan_vm_dirs()


def smolvm_vm_dirs() -> list[Path]:
    roots = [
        Path.home() / "Library/Caches/smolvm/vms",
        Path.home() / ".cache/smolvm/vms",
    ]
    for root in roots:
        if root.is_dir():
            return [entry for entry in root.iterdir() if entry.is_dir()]
    return []


def purge_orphan_vm_dirs() -> None:
    """Remove VM state a crashed run left behind.

    SmolVM keeps its machine registry in SQLite but resolves `machine status`
    from the on-disk VM directory. A killed engine can leave a directory (and a
    live `_boot-vm` process) whose registry row is gone, and every later
    provision then fails with a status/delete disagreement. Nothing but this
    harness owns `preloop-runner-*`, so orphans are always safe to reap.
    """
    reap_orphan_vm_processes()
    for directory in smolvm_vm_dirs():
        marker = directory / "name"
        if not marker.is_file():
            continue
        if not marker.read_text().strip().startswith("preloop-runner-"):
            continue
        pid_file = directory / "agent.pid"
        if pid_file.is_file():
            fields = pid_file.read_text().split()
            if fields and fields[0].isdigit():
                try:
                    os.kill(int(fields[0]), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
        shutil.rmtree(directory, ignore_errors=True)


BOOT_VM_CONFIG = re.compile(r"(/\S+/vms/[0-9a-f]+)/boot-config\.json")


def reap_orphan_vm_processes() -> None:
    """Kill hypervisor processes for benchmark VMs SmolVM no longer tracks.

    A `preloop-runner-*` clone whose registry row is gone keeps its libkrun
    process alive forever; enough of them starve later boots. Only benchmark
    VMs and directory-less strays are reaped — every other VM on the host is
    left untouched.
    """
    # -ww disables the terminal-width truncation that would hide the config path.
    listing = run(["ps", "-Awwo", "pid=,command="])
    reaped = 0
    for line in listing.stdout.splitlines():
        if "_boot-vm" not in line:
            continue
        fields = line.split(maxsplit=1)
        if len(fields) != 2 or not fields[0].isdigit():
            continue
        match = BOOT_VM_CONFIG.search(fields[1])
        if not match:
            continue
        directory = Path(match.group(1))
        marker = directory / "name"
        stray = not directory.is_dir()
        ours = marker.is_file() and marker.read_text().strip().startswith("preloop-runner-")
        if not (stray or ours):
            continue
        try:
            os.kill(int(fields[0]), signal.SIGKILL)
            reaped += 1
        except (ProcessLookupError, PermissionError):
            pass
    if reaped:
        log(f"reaped {reaped} orphaned benchmark VM process(es)")
        time.sleep(1.0)


def start_engine() -> subprocess.Popen:
    HOME.mkdir(parents=True, exist_ok=True)
    (HOME / "state").mkdir(parents=True, exist_ok=True)
    (HOME / "engine.token").write_text(TOKEN)
    (HOME / "engine.token").chmod(0o600)
    seed_action_cache()

    env = {
        **os.environ,
        "PRELOOP_HOME": str(HOME),
        "PRELOOP_LISTEN": LISTEN,
        "PRELOOP_RUNNER_BUNDLE": str(RUNNER_BUNDLE),
        "PRELOOP_RUNNER_POOL_SIZE": str(POOL_SIZE),
        "PRELOOP_RUNNER_BASE_IMAGE": BASE_IMAGE,
        "AKSH_SYSTEM_TOKEN": TOKEN,
        "RUST_LOG": "info",
    }
    env.pop("AKSH_URL", None)
    ENGINE_LOG.parent.mkdir(parents=True, exist_ok=True)
    handle = ENGINE_LOG.open("w")
    log(f"starting engine on {LISTEN}")
    process = subprocess.Popen(
        [str(CLI_BIN), "engine"],
        cwd=str(REPO),
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=handle,
        stderr=handle,
    )
    return process


def seed_action_cache() -> None:
    """Pre-populate the server action cache so no run reaches api.github.com."""
    source = REPO / ".aksh/actions"
    if not source.is_dir():
        return
    destination = HOME / "state/actions"
    for tarball in source.rglob("action.tar.gz"):
        target = destination / tarball.relative_to(source)
        if target.is_file():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(tarball, target)


READY_MARKER = re.compile(r"ephemeral runner ready")


def ready_count() -> int:
    if not ENGINE_LOG.is_file():
        return 0
    return len(READY_MARKER.findall(ENGINE_LOG.read_text(errors="replace")))


def wait_for_pool(process: subprocess.Popen, timeout: float = 300.0) -> float:
    """Block until the engine serves HTTP and every pool slot is registered."""
    started = time.time()
    deadline = started + timeout
    while time.time() < deadline:
        if process.poll() is not None:
            die(f"engine exited early ({process.returncode}); log:\n{ENGINE_LOG.read_text()[-4000:]}")
        if ready_count() >= POOL_SIZE:
            return (time.time() - started) * 1000.0
        time.sleep(0.1)
    die(f"pool not ready within {timeout}s; log:\n{ENGINE_LOG.read_text()[-4000:]}")


class Pool:
    """Gate every measured run on a fully replenished pool.

    Each finished job burns its ephemeral VM and the slot re-forks a fresh one.
    Without this gate a run either finds a warm slot or waits ~1.5 s for one,
    which makes the wall-clock distribution bimodal and the median unstable.
    A developer loop has think time between runs, so "pool warm" is both the
    representative state and the low-variance one. Replenishment latency is
    reported separately as `slot_ready_ms`.
    """

    def __init__(self, expected: int) -> None:
        self.expected = expected

    def await_warm(self, timeout: float = 120.0) -> float:
        """Wait for a fully replenished pool; return how long that took."""
        started = time.perf_counter()
        deadline = time.time() + timeout
        while time.time() < deadline:
            if ready_count() >= self.expected:
                return (time.perf_counter() - started) * 1000.0
            time.sleep(0.02)
        die(f"pool did not replenish to {self.expected} ready runners within {timeout}s")

    def job_started(self) -> None:
        self.expected += 1


def stop_engine(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=30)
    delete_bench_machines()


# ── measurement ──────────────────────────────────────────────────────────────


def cli_env() -> dict:
    env = {
        **os.environ,
        "PRELOOP_HOME": str(HOME),
        "AKSH_TOKEN": TOKEN,
    }
    env.pop("AKSH_URL", None)
    env.pop("RUST_LOG", None)
    return env


def cli_run() -> float:
    """Wall-clock milliseconds for one `preloop run`, as a developer sees it."""
    started = time.perf_counter()
    result = run([str(CLI_BIN), "run", "-f", "bench.yml"], cwd=WS, env=cli_env())
    elapsed = (time.perf_counter() - started) * 1000.0
    if result.returncode != 0:
        die(f"`preloop run` failed ({result.returncode}):\n{result.stdout}\n{result.stderr}")
    return elapsed


def api(path: str, body: dict | None = None, extra_headers: dict | None = None):
    headers = {"authorization": f"Bearer {TOKEN}"}
    data = None
    if body is not None:
        headers["content-type"] = "application/json"
        data = json.dumps(body).encode()
    headers.update(extra_headers or {})
    request = urllib.request.Request(BASE_URL + path, data=data, headers=headers)
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = response.read()
    return payload


TERMINAL = {"success", "failure", "cancelled", "skipped", "timedout"}
LOG_TIMESTAMP = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)", re.MULTILINE)


def api_run() -> dict:
    """One instrumented run, split into submit / dispatch / in-VM phases."""
    submission = {
        "workflow_yaml": WORKFLOW,
        "event": "push",
        "repository": "local/preloop-perf",
        "git_ref": "refs/heads/main",
        "workflow_path": ".github/workflows/bench.yml",
    }
    workspace_header = base64.urlsafe_b64encode(str(WS).encode()).decode().rstrip("=")

    t0 = time.perf_counter()
    accepted = json.loads(
        api("/api/v1/runs", submission, {"x-preloop-local-workspace": workspace_header})
    )
    t_submitted = time.perf_counter()
    run_id = accepted["run_id"]

    status = None
    deadline = time.time() + 300
    while time.time() < deadline:
        state = json.loads(api(f"/api/v1/runs/{run_id}"))
        status = str(state.get("status") or "").lower()
        if status in TERMINAL:
            break
        time.sleep(0.005)
    t_done = time.perf_counter()
    if status != "success":
        die(f"instrumented run {run_id} ended with status {status!r}")

    logs = api(f"/api/v1/runs/{run_id}/logs").decode(errors="replace")
    stamps = LOG_TIMESTAMP.findall(logs)
    if len(stamps) < 2:
        die(f"run {run_id} produced no parseable job log timestamps")
    job_ms = (iso_ms(stamps[-1]) - iso_ms(stamps[0])) * 1000.0

    submit_ms = (t_submitted - t0) * 1000.0
    total_ms = (t_done - t0) * 1000.0
    return {
        "submit_ms": submit_ms,
        "total_ms": total_ms,
        "job_ms": job_ms,
        "dispatch_ms": max(total_ms - submit_ms - job_ms, 0.0),
    }


def iso_ms(stamp: str) -> float:
    return datetime.strptime(stamp.rstrip("Z"), "%Y-%m-%dT%H:%M:%S.%f").timestamp()


def host_run(script: Path) -> float:
    started = time.perf_counter()
    result = run(["bash", str(script)])
    elapsed = (time.perf_counter() - started) * 1000.0
    if result.returncode != 0:
        die(f"host baseline failed:\n{result.stdout}\n{result.stderr}")
    return elapsed


# ── entrypoint ───────────────────────────────────────────────────────────────


def metric(name: str, value: float, digits: int = 1) -> None:
    print(f"METRIC {name}={round(value, digits)}")


def trimmed_mean(samples) -> float:
    """Mean without the single best and worst sample.

    The CLI polls run events on a fixed 250 ms cadence, so raw latencies land on
    a coarse grid and a plain median snaps to whichever mode happens to win.
    Trimming one sample from each end keeps the estimator smooth enough to
    resolve sub-quantum improvements while staying robust to a stray outlier.
    """
    ordered = sorted(samples)
    core = ordered[1:-1] if len(ordered) > 3 else ordered
    return statistics.fmean(core)


def main() -> int:
    if shutil.which("smolvm") is None:
        die("smolvm not found on PATH")
    if shutil.which("node") is None:
        die("node not found on PATH (needed for the host baseline)")

    ensure_build()
    ensure_fixture()
    host_script = ensure_host_baseline()

    stop_foreign_engines()
    delete_bench_machines()
    if not port_free():
        die(f"{LISTEN} is occupied; set PRELOOP_BENCH_LISTEN to a free port")

    engine = start_engine()
    try:
        pool_boot_ms = wait_for_pool(engine)
        log(f"pool ready in {pool_boot_ms:.0f} ms")

        pool = Pool(POOL_SIZE)
        replenish_waits: list[float] = []

        def measured(action, record_wait: bool = False):
            waited = pool.await_warm()
            if record_wait:
                replenish_waits.append(waited)
            pool.job_started()
            return action()

        for _ in range(WARMUP_RUNS):
            measured(cli_run)

        cli_samples = [measured(cli_run, record_wait=True) for _ in range(CLI_RUNS)]
        api_samples = [measured(api_run) for _ in range(API_RUNS)]
        host_samples = [host_run(host_script) for _ in range(HOST_RUNS)]
    finally:
        stop_engine(engine)

    e2e = trimmed_mean(cli_samples)
    host = trimmed_mean(host_samples)

    metric("e2e_ms", e2e)
    metric("e2e_min_ms", min(cli_samples))
    metric("host_ms", host)
    metric("overhead_ms", e2e - host)
    metric("overhead_ratio", e2e / host, 3)
    metric("submit_ms", trimmed_mean(s["submit_ms"] for s in api_samples))
    metric("api_total_ms", trimmed_mean(s["total_ms"] for s in api_samples))
    metric("job_ms", trimmed_mean(s["job_ms"] for s in api_samples))
    metric("dispatch_ms", trimmed_mean(s["dispatch_ms"] for s in api_samples))
    metric("pool_boot_ms", pool_boot_ms)
    metric("replenish_wait_ms", statistics.median(replenish_waits))

    spread = max(cli_samples) - min(cli_samples)
    print(f"ASI cli_runs={len(cli_samples)}")
    print(f"ASI cli_spread_ms={round(spread, 1)}")
    print(f"ASI cli_samples={[round(s) for s in cli_samples]}")
    print(f"ASI api_samples={[round(s['total_ms']) for s in api_samples]}")
    print(f"ASI host_samples={[round(s) for s in host_samples]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
