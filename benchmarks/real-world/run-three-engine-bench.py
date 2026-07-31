#!/usr/bin/env python3
"""Run repeatable act/agent-ci/preloop workload benchmarks.

The harness keeps each workflow run isolated in a fresh source workspace, records
engine stdout/stderr and sampled host/container/VM resources, and emits one JSON
record per attempt. It intentionally does not impose a short step timeout: the
workflow's own timeout-minutes is the upper bound.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = ROOT / "benchmarks/real-world/results/three-engine"
SAMPLE_SECONDS = 0.25


@dataclass(frozen=True)
class Workload:
    name: str
    repo: str | None
    branch: str | None
    workflow: str


WORKLOADS = {
    "preloop": Workload("preloop", None, None, "preloop-bench.yml"),
    "agent-ci": Workload("agent-ci", "https://github.com/redwoodjs/agent-ci.git", "main", "agent-ci-bench.yml"),
    "act": Workload("act", "https://github.com/nektos/act.git", "master", "act-bench.yml"),
    "redis-py": Workload("redis-py", "https://github.com/redis/redis-py.git", "master", "redis-py-bench.yml"),
    "docker-compose": Workload("docker-compose", "https://github.com/docker/compose.git", "main", "docker-compose-bench.yml"),
}


@dataclass
class Sample:
    elapsed_s: float
    pids: int
    rss_bytes: int
    cpu_percent: float
    docker_rss_bytes: int
    docker_cpu_percent: float
    vm_rss_bytes: int
    vm_cpu_percent: float


class Sampler:
    def __init__(self, root_pid: int, engine: str, home: Path | None, output: Path):
        self.root_pid = root_pid
        self.engine = engine
        self.home = home
        self.output = output
        self.started = time.monotonic()
        self.samples: list[Sample] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="benchmark-sampler", daemon=True)
        self._baseline_docker = self._docker_ids()
        self._baseline_vms = self._vm_names()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=3)

    def _run(self) -> None:
        while not self._stop.is_set():
            self.samples.append(self._sample())
            self._stop.wait(SAMPLE_SECONDS)
        self.samples.append(self._sample())

    def _sample(self) -> Sample:
        pids = self._descendants(self.root_pid)
        if self.home:
            pid_file = self.home / "preloop.pid"
            try:
                pids |= self._descendants(int(pid_file.read_text().strip()))
            except (OSError, ValueError):
                pass
        host = self._ps(pids)
        docker_ids = self._docker_ids() - self._baseline_docker
        docker = self._docker_stats(docker_ids)
        vm_names = self._vm_names() - self._baseline_vms
        vm = self._vm_stats(vm_names)
        return Sample(
            elapsed_s=time.monotonic() - self.started,
            pids=len(pids),
            rss_bytes=host["rss"],
            cpu_percent=host["cpu"],
            docker_rss_bytes=docker["rss"],
            docker_cpu_percent=docker["cpu"],
            vm_rss_bytes=vm["rss"],
            vm_cpu_percent=vm["cpu"],
        )

    @staticmethod
    def _descendants(root: int) -> set[int]:
        if root <= 0:
            return set()
        try:
            rows = subprocess.check_output(["ps", "-axo", "pid=,ppid="], text=True).splitlines()
        except (OSError, subprocess.SubprocessError):
            return {root}
        children: dict[int, list[int]] = {}
        for row in rows:
            parts = row.split()
            if len(parts) == 2:
                children.setdefault(int(parts[1]), []).append(int(parts[0]))
        found = {root}
        pending = [root]
        while pending:
            parent = pending.pop()
            for child in children.get(parent, []):
                if child not in found:
                    found.add(child)
                    pending.append(child)
        return found

    @staticmethod
    def _ps(pids: set[int]) -> dict[str, float | int]:
        if not pids:
            return {"rss": 0, "cpu": 0.0}
        try:
            text = subprocess.check_output(["ps", "-o", "pid=,rss=,%cpu=", "-p", ",".join(map(str, pids))], text=True)
        except (OSError, subprocess.SubprocessError):
            return {"rss": 0, "cpu": 0.0}
        rss = 0
        cpu = 0.0
        for row in text.splitlines():
            parts = row.split()
            if len(parts) == 3:
                rss += int(float(parts[1])) * 1024
                cpu += float(parts[2])
        return {"rss": rss, "cpu": cpu}

    @staticmethod
    def _docker_ids() -> set[str]:
        try:
            return set(subprocess.check_output(["docker", "ps", "-aq"], text=True, stderr=subprocess.DEVNULL).split())
        except (OSError, subprocess.SubprocessError):
            return set()

    @staticmethod
    def _docker_stats(ids: set[str]) -> dict[str, float | int]:
        if not ids:
            return {"rss": 0, "cpu": 0.0}
        try:
            text = subprocess.check_output(
                ["docker", "stats", "--no-stream", "--format", "{{.ID}}\t{{.CPUPerc}}\t{{.MemUsage}}", *ids],
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except (OSError, subprocess.SubprocessError):
            return {"rss": 0, "cpu": 0.0}
        rss = 0
        cpu = 0.0
        for row in text.splitlines():
            parts = row.split("\t")
            if len(parts) != 3:
                continue
            cpu += float(parts[1].rstrip("%") or 0)
            match = re.match(r"([0-9.]+)\s*([KMG]i?B)", parts[2])
            if match:
                value = float(match.group(1))
                factor = {"KB": 1000, "KiB": 1024, "MB": 1000**2, "MiB": 1024**2, "GB": 1000**3, "GiB": 1024**3}[match.group(2)]
                rss += int(value * factor)
        return {"rss": rss, "cpu": cpu}

    @staticmethod
    def _vm_names() -> set[str]:
        try:
            rows = json.loads(subprocess.check_output(["smolvm", "machine", "list", "--json"], text=True, stderr=subprocess.DEVNULL))
            return {str(row["name"]) for row in rows}
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError, KeyError, TypeError):
            return set()

    @staticmethod
    def _vm_stats(names: set[str]) -> dict[str, float | int]:
        if not names:
            return {"rss": 0, "cpu": 0.0}
        try:
            rows = json.loads(subprocess.check_output(["smolvm", "machine", "list", "--json"], text=True, stderr=subprocess.DEVNULL))
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            return {"rss": 0, "cpu": 0.0}
        pids = {int(row["pid"]) for row in rows if row.get("name") in names and row.get("pid")}
        return Sampler._ps(pids)


def run_checked(args: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, env=env, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def repo_commit(repo: Path) -> str:
    return run_checked(["git", "rev-parse", "HEAD"], cwd=repo).stdout.strip()


def ensure_repo(workload: Workload, cache_root: Path) -> Path:
    if workload.repo is None:
        return ROOT
    target = cache_root / workload.name
    if not (target / ".git").exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        run_checked(["git", "clone", "--depth", "1", "--branch", workload.branch or "main", workload.repo, str(target)])
    return target


def materialize(source: Path, workflow: Path, destination: Path, keep_git: bool) -> None:
    if keep_git:
        run_checked(["git", "clone", "--local", "--no-hardlinks", str(source), str(destination)])
        run_checked(["git", "checkout", "--detach", repo_commit(source)], cwd=destination)
    else:
        destination.mkdir(parents=True)
        archive = destination.parent / "source.tar"
        run_checked(["git", "archive", "--format=tar", "-o", str(archive), "HEAD"], cwd=source)
        with tarfile.open(archive) as stream:
            stream.extractall(destination, filter="fully_trusted")
        archive.unlink()
    workflow_dir = destination / ".github" / "workflows"
    workflow_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(workflow, workflow_dir / workflow.name)


def stop_preloop(home: Path) -> None:
    try:
        pid = int((home / "preloop.pid").read_text().strip())
    except (OSError, ValueError):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.2)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def capture_preloop_logs(home: Path, run_dir: Path) -> None:
    server_log = home / "engine.log"
    if server_log.exists():
        shutil.copy2(server_log, run_dir / "preloop-server.log")
    replay_root = home / "state" / "replay" / "results"
    if not replay_root.exists():
        return
    destination = run_dir / "preloop-job-logs"
    for path in replay_root.rglob("job-logs.txt"):
        relative = path.relative_to(replay_root)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def preloop_listen_port(env: dict[str, str]) -> int:
    return int(env["PRELOOP_LISTEN"].rsplit(":", 1)[1])


def preloop_process_alive(home: Path) -> bool:
    try:
        pid = int((home / "preloop.pid").read_text().strip())
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def wait_for_preloop_health(env: dict[str, str], timeout_s: float = 30.0) -> None:
    url = f"http://127.0.0.1:{preloop_listen_port(env)}/healthz"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=0.5):
                return
        except (OSError, urllib.error.URLError):
            time.sleep(0.1)
    raise RuntimeError(f"preloop engine did not become ready at {url}")


def start_preloop_engine(home: Path, env: dict[str, str]) -> bool:
    if preloop_process_alive(home):
        try:
            wait_for_preloop_health(env, timeout_s=2.0)
            return False
        except RuntimeError:
            stop_preloop(home)
    home.mkdir(parents=True, exist_ok=True)
    log = (home / "engine.log").open("a")
    process = subprocess.Popen(
        [str(ROOT / "target/debug/preloop"), "engine"],
        cwd=ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=log,
        stderr=log,
    )
    (home / "preloop.pid").write_text(str(process.pid))
    try:
        wait_for_preloop_health(env)
    except Exception:
        process.kill()
        process.wait()
        raise
    return True


def install_rust_in_preloop_golden(env: dict[str, str]) -> float:
    started = time.monotonic()
    golden = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            rows = json.loads(
                subprocess.check_output(
                    ["smolvm", "machine", "list", "--json"],
                    text=True,
                    stderr=subprocess.DEVNULL,
                )
            )
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            rows = []
        for row in rows:
            name = str(row.get("name", ""))
            if name == "preloop-runner-golden":
                golden = name
                break
        if golden:
            break
        time.sleep(0.2)
    if golden is None:
        raise RuntimeError("preloop runner golden did not appear")
    script = (
        "export RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo; "
        "export PATH=/opt/cargo/bin:$PATH; "
        "if ! /opt/cargo/bin/rustc --version >/dev/null 2>&1; then "
        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | "
        "sh -s -- -y --profile minimal --default-toolchain 1.89.0; "
        "fi; "
        "/opt/cargo/bin/rustup component add rustfmt clippy; "
        "for tool in cargo cargo-clippy cargo-fmt rustc rustfmt rustup; do "
        "ln -sf /opt/cargo/bin/$tool /usr/local/bin/$tool; "
        "done; "
        "sync"
    )
    result = None
    for _ in range(30):
        result = subprocess.run(
            ["smolvm", "machine", "exec", "--name", golden, "sh", "-lc", script],
            env=env,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if result.returncode == 0:
            break
        if "not running" in result.stdout:
            subprocess.run(
                ["smolvm", "machine", "start", "--name", golden],
                env=env,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        time.sleep(1)
    if result is None or result.returncode != 0:
        output = result.stdout if result is not None else "no exec result"
        raise RuntimeError(f"Rust golden setup failed: {output[-2000:]}")
    return time.monotonic() - started


def preloop_home_root(cache_root: Path) -> Path:
    suffix = hashlib.sha256(str(cache_root).encode()).hexdigest()[:10]
    return Path("/tmp") / f"preloop-bench-{suffix}"


def engine_command(engine: str, workflow: Path, repo: Path, cold: bool) -> list[str]:
    if engine == "act":
        return [
            "act",
            "workflow_dispatch",
            "--workflows",
            str(workflow),
            "--directory",
            str(repo),
            "--bind",
            "--platform",
            "ubuntu-latest=catthehacker/ubuntu:act-latest",
            "--container-architecture",
            "linux/arm64",
            f"--pull={'true' if cold else 'false'}",
            "--rm",
            "--json",
        ]
    if engine == "agent-ci":
        return ["agent-ci", "run", "--workflow", str(workflow), "--json", "--quiet"]
    if engine == "preloop":
        return [str(ROOT / "target/debug/preloop"), "run", "--file", str(workflow), "--no-debug"]
    raise ValueError(engine)


def parse_result(engine: str, returncode: int, stdout: str, stderr: str, timed_out: bool) -> str:
    if timed_out:
        return "timeout"
    if returncode == 0:
        return "success"
    if engine == "preloop" and "Run:" in stdout and "Success" in stdout:
        return "success"
    return "failure"


def extract_steps(engine: str, stdout: str) -> list[dict[str, Any]]:
    steps: dict[str, dict[str, Any]] = {}
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if engine == "agent-ci":
            if event.get("event") == "step.start":
                steps[event["step"]] = {
                    "name": event["step"],
                    "index": event.get("index"),
                    "started": event.get("ts"),
                }
            elif event.get("event") == "step.finish":
                step = steps.setdefault(event["step"], {"name": event["step"]})
                step.update({
                    "status": event.get("status"),
                    "duration_ms": event.get("durationMs"),
                    "finished": event.get("ts"),
                })
        elif engine == "act" and event.get("stepResult") in {"success", "failure"}:
            name = event.get("step")
            if name:
                step = steps.setdefault(name, {"name": name})
                step.update({
                    "status": event.get("stepResult"),
                    "duration_ms": event.get("executionTime", 0) / 1_000_000,
                    "finished": event.get("time"),
                })
    return list(steps.values())


def run_one(engine: str, workload: Workload, source: Path, workflow: Path, output: Path, rep: int, cold: bool, cache_root: Path) -> dict[str, Any]:
    cache_mode = "cold" if cold else "warm"
    run_dir = output / engine / workload.name / f"run-{rep:02d}-{cache_mode}"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    repo = run_dir / "workspace"
    log_path = run_dir / "engine.log"
    run_dir.mkdir(parents=True)
    materialize(source, workflow, repo, keep_git=workload.name == "act")
    home = None
    env = os.environ.copy()
    env.update({"CI": "true", "GITHUB_ACTIONS": "true", "BENCHMARK_ENGINE": engine})
    if engine == "agent-ci":
        home = cache_root / "homes" / engine / workload.name / ("warm" if not cold else f"cold-{rep}")
        home.mkdir(parents=True, exist_ok=True)
        env["HOME"] = str(home)
        env["AI_AGENT"] = "1"
        env["AGENT_CI_JSON"] = "1"
    elif engine == "act":
        home = cache_root / "homes" / engine / workload.name / ("warm" if not cold else f"cold-{rep}")
        home.mkdir(parents=True, exist_ok=True)
        env["HOME"] = str(home)
    preparation_s = 0.0
    if engine == "preloop":
        home = preloop_home_root(cache_root) / workload.name / ("warm" if not cold else f"cold-{rep}")
        home.mkdir(parents=True, exist_ok=True)
        env["PRELOOP_HOME"] = str(home)
        port_key = f"{cache_root}:{workload.name}".encode()
        port = 19000 + int.from_bytes(hashlib.sha256(port_key).digest()[:2], "big") % 500
        env["PRELOOP_LISTEN"] = f"127.0.0.1:{port}"
        env["PRELOOP_RUNNER_BASE_IMAGE"] = "ubuntu:24.04"
        env["AKSH_SYSTEM_TOKEN"] = hashlib.sha256(str(home).encode()).hexdigest()
        env.pop("AKSH_URL", None)
        env["PRELOOP_RUNNER_POOL_SIZE"] = "1"
        env["RUST_LOG"] = "info"
        try:
            if start_preloop_engine(home, env):
                preparation_s += install_rust_in_preloop_golden(env)
        except Exception:
            stop_preloop(home)
            raise
    command = engine_command(engine, repo / ".github/workflows" / workflow.name, repo, cold)
    started = time.monotonic()
    proc = subprocess.Popen(command, cwd=repo, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    sampler = Sampler(proc.pid, engine, home, run_dir)
    sampler.start()
    timed_out = False
    try:
        stdout, _ = proc.communicate(timeout=3600)
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        proc.kill()
        stdout, _ = proc.communicate()
        stdout = (exc.output or "") + stdout
    elapsed = time.monotonic() - started
    sampler.stop()
    log_path.write_text(stdout)
    if engine == "preloop":
        capture_preloop_logs(home, run_dir)
    samples = [sample.__dict__ for sample in sampler.samples]
    (run_dir / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
    if engine == "preloop" and cold:
        stop_preloop(home)
        shutil.rmtree(home, ignore_errors=True)
    result = parse_result(engine, proc.returncode, stdout, "", timed_out)
    peak = max(sampler.samples, key=lambda sample: sample.rss_bytes + sample.docker_rss_bytes + sample.vm_rss_bytes, default=Sample(0, 0, 0, 0, 0, 0, 0, 0))
    record = {
        "engine": engine,
        "workload": workload.name,
        "rep": rep,
        "cache_mode": "cold" if cold else "warm",
        "status": result,
        "returncode": proc.returncode,
        "elapsed_s": elapsed,
        "preparation_s": preparation_s,
        "source_commit": repo_commit(source),
        "command": command,
        "peak_host_rss_bytes": max((s.rss_bytes for s in sampler.samples), default=0),
        "peak_host_cpu_percent": max((s.cpu_percent for s in sampler.samples), default=0),
        "peak_docker_rss_bytes": max((s.docker_rss_bytes for s in sampler.samples), default=0),
        "peak_docker_cpu_percent": max((s.docker_cpu_percent for s in sampler.samples), default=0),
        "peak_vm_rss_bytes": max((s.vm_rss_bytes for s in sampler.samples), default=0),
        "peak_vm_cpu_percent": max((s.vm_cpu_percent for s in sampler.samples), default=0),
        "sample_count": len(samples),
        "steps": extract_steps(engine, stdout),
        "log": str(log_path),
    }
    (run_dir / "result.json").write_text(json.dumps(record, indent=2) + "\n")
    shutil.rmtree(repo, ignore_errors=True)
    print(json.dumps(record), flush=True)
    return record


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["act", "agent-ci", "preloop"], action="append")
    parser.add_argument("--workload", choices=sorted(WORKLOADS), action="append")
    parser.add_argument("--runs", type=int, default=4)
    parser.add_argument("--cold-runs", default="1,3")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--cache-root", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.runs < 4:
        parser.error("--runs must be at least 4")
    engines = args.engine or ["act", "agent-ci", "preloop"]
    workload_names = args.workload or list(WORKLOADS)
    cold_runs = {int(value) for value in args.cold_runs.split(",") if value}
    output = args.output.resolve()
    cache_root = (args.cache_root or output / "cache").resolve()
    output.mkdir(parents=True, exist_ok=True)
    cache_root.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, Any]] = []
    sources: dict[str, Path] = {}
    for name in workload_names:
        workload = WORKLOADS[name]
        source = ensure_repo(workload, cache_root / "repos")
        sources[name] = source
        manifest.append({"workload": name, "source": str(source), "commit": repo_commit(source), "workflow": workload.workflow})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    plan = [{"engine": engine, "workload": name, "rep": rep, "cache_mode": "cold" if rep in cold_runs else "warm"} for engine in engines for name in workload_names for rep in range(1, args.runs + 1)]
    if args.dry_run:
        print(json.dumps(plan, indent=2))
        return 0
    workflow_root = ROOT / "benchmarks/real-world"
    results = []
    for engine in engines:
        for name in workload_names:
            workload = WORKLOADS[name]
            for rep in range(1, args.runs + 1):
                results.append(run_one(engine, workload, sources[name], workflow_root / workload.workflow, output, rep, rep in cold_runs, cache_root))
    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    if "preloop" in engines:
        for name in workload_names:
            warm_home = preloop_home_root(cache_root) / name / "warm"
            stop_preloop(warm_home)
            shutil.rmtree(warm_home, ignore_errors=True)
    return 0 if all(result["status"] == "success" for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
