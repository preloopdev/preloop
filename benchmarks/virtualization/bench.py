#!/usr/bin/env python3
"""Fail-closed, standard-library benchmark campaign harness."""
from __future__ import annotations
import argparse, hashlib, importlib, ipaddress, json, os, platform, shutil, socket, subprocess, sys, time, urllib.request, re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
try:
    import tomllib
except ImportError:
    tomllib = None
SCHEMA_VERSION=1
REQUIRED_COMMANDS=("validate","preflight","smoke","run","assert-clean","cleanup","verify-supply","adapter-contract","collector-overhead")
PRODUCTION_PORTS={9090}; PRODUCTION_PATHS=("/var/lib/preloop/state","/run/preloop","/etc/preloop")
HEX64=re.compile(r"^sha256:[0-9a-f]{64}$"); HEX40=re.compile(r"^[0-9a-f]{40}$")
TOKEN_VALUE=re.compile(r"(?i)(bearer\s+|token[=:]\s*|password[=:]\s*|secret[=:]\s*)[^\s,;]+")

def _placeholder_digest(value: str) -> bool:
    body = value.removeprefix("sha256:")
    return len(body) == 64 and len(set(body)) == 1

def _redact_value(value):
    if isinstance(value, str):
        return TOKEN_VALUE.sub(lambda match: match.group(1) + "[REDACTED]", value)
    if isinstance(value, dict):
        return {str(key): _redact_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    return value

def load_manifest(path: str|Path)->dict[str,Any]:
    if tomllib is None: raise ValueError("STOP: Python 3.11+ tomllib is required")
    try:
        with open(path,"rb") as f: value=tomllib.load(f)
    except (OSError,ValueError) as e: raise ValueError(f"STOP: cannot load manifest: {e}") from e
    if not isinstance(value,dict): raise ValueError("STOP: manifest root must be a TOML table")
    return value

def _canonical(v:Any)->Any:
    if isinstance(v,dict): return {str(k):_canonical(v[k]) for k in sorted(v)}
    if isinstance(v,list): return [_canonical(x) for x in v]
    return v

def canonical_manifest_bytes(manifest:dict[str,Any])->bytes:
    return (json.dumps(_canonical(manifest),sort_keys=True,separators=(",",":"),ensure_ascii=True)+"\n").encode()

def canonical_manifest_hash(manifest:dict[str,Any])->str:
    return hashlib.sha256(canonical_manifest_bytes(manifest)).hexdigest()

def _walk(v:Any,p=""):
    if isinstance(v,dict):
        for k,x in v.items(): yield from _walk(x,f"{p}.{k}" if p else str(k))
    elif isinstance(v,list):
        for i,x in enumerate(v): yield from _walk(x,f"{p}[{i}]")
    else: yield p,v

def validate_manifest(m:dict[str,Any])->list[str]:
    e=[]; req=("campaign_id","created_utc","operator","repository_commit","target_hostname","versions","isolation","resources","benchmark","slo")
    e += [f"missing required field: {k}" for k in req if k not in m]
    v=m.get("versions",{}); v=v if isinstance(v,dict) else {}; e += [] if isinstance(m.get("versions",{}),dict) else ["versions must be a table"]
    for k,x in {"smolvm":"1.8.2","hypeman_commit":"95db3fb917fa3caaf4cdd4655051ed7bdec975e2","firecracker":"1.14.2","runner":"2.336.0"}.items():
        if v.get(k)!=x: e.append(f"{k} must be pinned exactly to {x}")
    for k in ("guest_image_digest","workload_revision"):
        if not HEX64.fullmatch(str(v.get(k,""))): e.append(f"{k} must be an immutable sha256 digest")
    if not HEX40.fullmatch(str(m.get("repository_commit",""))): e.append("repository_commit must be a 40-character commit SHA")
    i=m.get("isolation",{}); i=i if isinstance(i,dict) else {}; e += [] if isinstance(m.get("isolation",{}),dict) else ["isolation must be a table"]
    ports=i.get("benchmark_ports",[])
    if not isinstance(ports,list) or not ports: e.append("benchmark_ports must be a non-empty list")
    elif any(not isinstance(p,int) or p in PRODUCTION_PORTS or not 1024<=p<=65535 for p in ports): e.append("benchmark ports include production/invalid port")
    for k in ("state_dir","runtime_dir","vm_prefix","bridge","cidr","cgroup_slice"):
        if not isinstance(i.get(k),str) or not i[k]: e.append(f"missing isolation.{k}")
    for k in ("state_dir","runtime_dir"):
        if any(i.get(k,"")==x or i.get(k,"").startswith(x+"/") for x in PRODUCTION_PATHS): e.append(f"isolation.{k} overlaps production path")
    if not str(i.get("vm_prefix","")).startswith("preloop-bench-"): e.append("vm_prefix is not benchmark-owned")
    try:
        n=ipaddress.ip_network(str(i.get("cidr")),strict=True)
        if n.overlaps(ipaddress.ip_network("10.0.0.0/8")) or n.overlaps(ipaddress.ip_network("192.168.0.0/16")): e.append("cidr collides with reserved/production network")
    except ValueError: e.append("isolation.cidr must be a valid network")
    if i.get("production_service_policy") not in ("must_remain_running","approved_maintenance_stop"): e.append("invalid production_service_policy")
    r=m.get("resources",{}); total=0
    if not isinstance(r,dict) or not r: e.append("resources must be a non-empty table")
    else:
        for k,p in r.items():
            if not isinstance(p,dict) or not isinstance(p.get("memory_mib"),int): e.append(f"resources.{k}.memory_mib missing"); continue
            total=max(total,p["memory_mib"])
    lim=m.get("limits",{}); ceiling=lim.get("memory_ceiling_gib") if isinstance(lim,dict) else None
    if ceiling!=18: e.append("memory_ceiling_gib must be exactly 18")
    if total>18*1024: e.append("resource memory matrix exceeds 18 GiB ceiling")
    b=m.get("benchmark",{}); 
    if not isinstance(b,dict) or not isinstance(b.get("repetitions"),int) or b.get("repetitions",0)<1: e.append("benchmark repetitions required")
    if isinstance(b,dict) and isinstance(b.get("concurrency"),dict):
        matrix_total=0
        for name,count in b["concurrency"].items():
            if name not in r or not isinstance(count,int) or count < 0:
                e.append(f"invalid benchmark.concurrency.{name}")
            else:
                matrix_total += r[name].get("memory_mib",0) * count
        if matrix_total > 18*1024: e.append("concurrency memory matrix exceeds 18 GiB ceiling")
    if not isinstance(m.get("slo"),dict) or not m["slo"]: e.append("SLOs are required")
    for k,x in _walk(m):
        if re.search(r"(secret|token|password|credential|private_key)",k,re.I): e.append(f"secret-like field/value forbidden: {k}")
        if isinstance(x,str) and k.endswith(("ref","tag","branch")) and re.search(r"\b(main|master|latest|v?\d+\.\d+\.\d+)\b",x): e.append(f"floating reference forbidden: {k}")
    return e

def event_record(m,**kw):
    return {
        "schema_version": SCHEMA_VERSION,
        "manifest_sha256": canonical_manifest_hash(m),
        "campaign": m.get("campaign_id", ""),
        "phase": kw.get("phase", ""),
        "arm": kw.get("arm", ""),
        "repetition": kw.get("repetition", 0),
        "workload": kw.get("workload", ""),
        "resource_profile": kw.get("resource_profile", ""),
        "cache_class": kw.get("cache_class", ""),
        "warmup": kw.get("warmup", False),
        "monotonic_ns": time.monotonic_ns(),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "event_kind": kw.get("event_kind", ""),
        "success": kw.get("success", False),
        "failure_class": kw.get("failure_class", ""),
        "diagnostic": _redact_value(kw.get("diagnostic", "")),
        "values": _redact_value(kw.get("values", {})),
        "units": kw.get("units", {}),
        "candidate_hashes": _redact_value(kw.get("candidate_hashes", {})),
    }

class EventLog:
    """Append-only JSONL sink; never rewrites or truncates an evidence file."""
    def __init__(self, path: str|Path):
        self.path=Path(path)
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self._last = -1
        self._terminal = set()
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if not line:
                    continue
                prior = json.loads(line)
                self._last = max(self._last, int(prior["monotonic_ns"]))
                if prior.get("event_kind") in {"complete", "failed", "timeout", "deleted"}:
                    self._terminal.add((str(prior["phase"]), int(prior["repetition"]),
                                        str(prior["arm"])))
    def append(self, record:dict[str,Any])->None:
        required = ("schema_version", "manifest_sha256", "campaign", "phase",
                    "arm", "repetition", "monotonic_ns", "timestamp_utc",
                    "event_kind", "success", "failure_class", "diagnostic",
                    "values", "units")
        missing = [key for key in required if key not in record]
        if missing:
            raise ValueError("event missing " + ",".join(missing))
        record = _redact_value(record)
        monotonic = int(record["monotonic_ns"])
        if monotonic < self._last:
            raise ValueError("event monotonic time decreased")
        key = (str(record["phase"]), int(record["repetition"]), str(record["arm"]))
        if record["event_kind"] in {"complete", "failed", "timeout", "deleted"}:
            if key in self._terminal:
                raise ValueError("duplicate terminal event")
            self._terminal.add(key)
        self._last = monotonic
        with self.path.open("a",encoding="utf-8") as f:
            f.write(json.dumps(record,sort_keys=True,separators=(",",":"))+"\n")

def _read_meminfo():
    p=Path("/proc/meminfo")
    if not p.exists(): return None
    out={}
    for line in p.read_text().splitlines():
        key, _, value=line.partition(":")
        try: out[key]=int(value.strip().split()[0])*1024
        except (ValueError, IndexError): pass
    return out


def _inventory(m: dict[str, Any]) -> dict[str, Any]:
    inventory: dict[str, Any] = {
        "campaign": m["campaign_id"],
        "manifest_sha256": canonical_manifest_hash(m),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "cpu_count": os.cpu_count(),
    }
    mem = _read_meminfo()
    if mem is not None:
        inventory["memory"] = {
            key: mem.get(key)
            for key in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree")
            if key in mem
        }
    vmstat = Path("/proc/vmstat")
    if vmstat.exists():
        inventory["vmstat"] = {
            key: int(value)
            for line in vmstat.read_text().splitlines()
            for key, value in [line.split(maxsplit=1)]
            if key in {"pswpin", "pswpout"} and value.isdigit()
        }
    inventory["psi"] = {}
    for resource in ("cpu", "memory", "io"):
        path = Path("/proc/pressure") / resource
        if path.exists():
            inventory["psi"][resource] = path.read_text()
    try:
        usage = shutil.disk_usage("/")
        inventory["root_free_bytes"] = usage.free
    except OSError:
        inventory["root_free_bytes"] = None
    return inventory

def _swap_counters() -> dict[str, int]:
    path = Path("/proc/vmstat")
    if not path.exists():
        return {}
    counters: dict[str, int] = {}
    for line in path.read_text().splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] in {"pswpin", "pswpout"} and fields[1].isdigit():
            counters[fields[0]] = int(fields[1])
    return counters

def _cpu_ticks() -> tuple[int, int] | None:
    try:
        fields = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
        values = [int(value) for value in fields]
        return sum(values), values[3]
    except (OSError, IndexError, ValueError):
        return None

def _cpu_idle_percent(before, after) -> float | None:
    if before is None or after is None:
        return None
    total = after[0] - before[0]
    idle = after[1] - before[1]
    return 100.0 * idle / total if total > 0 else None


def _preflight_checks(m, endpoint_required=True, swap_baseline=None, cpu_baseline=None):
    failures=[]
    mem=_read_meminfo()
    if mem is None: failures.append("/proc/meminfo unavailable; host inventory is not evidence")
    elif mem.get("MemAvailable",0) < 18*1024**3: failures.append("available memory below 18 GiB")
    current_swap = _swap_counters()
    if swap_baseline is not None and any(
        current_swap.get(key, 0) > swap_baseline.get(key, 0)
        for key in ("pswpin", "pswpout")
    ):
        failures.append("swap I/O increased during observation")
    cpu_idle = _cpu_idle_percent(cpu_baseline, _cpu_ticks())
    if cpu_baseline is not None and cpu_idle is None:
        failures.append("CPU idle percentage unavailable")
    elif cpu_idle is not None and cpu_idle <= 90.0:
        failures.append(f"CPU idle is {cpu_idle:.2f}% (required >90%)")
    for resource in ("cpu", "memory", "io"):
        pressure=Path("/proc/pressure")/resource
        if pressure.exists():
            for line in pressure.read_text().splitlines():
                if line.startswith("some "):
                    match=re.search(r"avg10=([0-9.]+)",line)
                    if match and float(match.group(1)) >= 1.0: failures.append(f"{resource} PSI avg10 is at least 1%")
                    break
    if endpoint_required:
        pre=m.get("preflight",{})
        endpoint=pre.get("queue_endpoint") if isinstance(pre,dict) else None
        if not endpoint: failures.append("queue_endpoint is required for live preflight")
        else:
            try:
                request = urllib.request.Request(endpoint)
                token = os.environ.get("PRELOOP_BENCH_QUEUE_TOKEN")
                if token:
                    request.add_header("Authorization", f"Bearer {token}")
                with urllib.request.urlopen(request,timeout=5) as r:
                    if r.status >= 400: failures.append(f"queue endpoint returned HTTP {r.status}")
                    payload = json.loads(r.read(1024 * 1024))
                    if isinstance(payload, list):
                        active = [
                            row.get("status")
                            for row in payload
                            if isinstance(row, dict)
                            and row.get("status") in {"queued", "pending", "in_progress"}
                        ]
                        if active:
                            failures.append(f"production queue is not idle ({len(active)} active runs)")
            except Exception as exc: failures.append(f"queue endpoint unavailable: {exc}")
    for port in m.get("isolation", {}).get("benchmark_ports", []):
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(("127.0.0.1", int(port)))
        except OSError:
            failures.append(f"benchmark port {port} is already in use")
        finally:
            probe.close()
    try:
        if shutil.disk_usage("/").free < 150 * 1024**3:
            failures.append("root filesystem free space is below 150 GiB")
    except OSError:
        failures.append("root filesystem free space unavailable")
    return failures

def _dispatch(cmd,args,m):
    return _campaign_command(cmd, args, m)


def _campaign_paths(m: dict[str, Any]) -> tuple[Path, Path]:
    """Return campaign state paths after proving they are campaign-owned."""
    isolation = m.get("isolation", {})
    campaign = str(m.get("campaign_id", ""))
    state = Path(str(isolation.get("state_dir", "")))
    runtime = Path(str(isolation.get("runtime_dir", "")))
    if not campaign or not campaign.replace("-", "").isalnum():
        raise ValueError("STOP: invalid campaign id")
    if not state.is_absolute() or not runtime.is_absolute():
        raise ValueError("STOP: campaign paths must be absolute")
    expected = Path("/var/lib/preloop-bench") / campaign
    expected_runtime = Path("/run/preloop-bench") / campaign
    if state != expected or runtime != expected_runtime:
        raise ValueError("STOP: campaign paths must be exact standard locations")
    return state, runtime


def _candidate_spec(m: dict[str, Any], arm: str) -> dict[str, Any]:
    candidates = m.get("candidates", {})
    if not isinstance(candidates, dict) or not isinstance(candidates.get(arm), dict):
        raise ValueError(
            f"STOP: manifest has no installed {arm} candidate; complete the "
            "pinned installation step before live benchmarking"
        )
    spec = dict(candidates[arm])
    spec["campaign"] = str(m["campaign_id"])
    spec["data_dir"] = str(spec.get("data_dir") or _campaign_paths(m)[0] / arm)
    if arm == "hypeman-fc":
        spec["token"] = os.environ.get("PRELOOP_BENCH_HYPEMAN_TOKEN", "")
    return spec


def _write_result(m: dict[str, Any], record: dict[str, Any]) -> Path:
    state, _ = _campaign_paths(m)
    result = state / "results" / f"{m['campaign_id']}.jsonl"
    EventLog(result).append(record)
    return result


def _make_runtime_adapter(m: dict[str, Any], arm: str):
    try:
        from benchmarks.virtualization.adapters import HypemanFirecrackerAdapter, SmolVMAdapter
    except ModuleNotFoundError:
        from adapters import HypemanFirecrackerAdapter, SmolVMAdapter
    spec = _candidate_spec(m, arm)
    if arm == "smolvm":
        def runner(argv):
            env = dict(os.environ)
            env.update({
                "SMOLVM_DATA_DIR": str(spec["data_dir"]),
                "SMOLVM_SECCOMP": "enforce",
                "SMOLVM_LANDLOCK": "enforce",
            })
            completed = subprocess.run(argv, env=env, capture_output=True, text=True, check=False)
            if completed.returncode:
                raise RuntimeError(completed.stderr[-1000:] or "SmolVM command failed")
            return {"returncode": 0, "stdout": completed.stdout}
        return SmolVMAdapter(spec, runner=runner), spec
    if arm == "hypeman-fc":
        def transport(method, path, payload, headers=None):
            url = str(spec["base_url"]).rstrip("/") + path
            body = None if method == "GET" or not payload else json.dumps(payload).encode()
            request = urllib.request.Request(
                url, data=body, method=method,
                headers={"Content-Type": "application/json", **(headers or {})},
            )
            with urllib.request.urlopen(request, timeout=120) as response:
                raw = response.read()
                if not raw:
                    return {"status": response.status}
                return json.loads(raw)
        return HypemanFirecrackerAdapter(spec, transport=transport), spec
    raise ValueError(f"STOP: unsupported arm {arm}")


def _verify_supply(m: dict[str, Any]) -> int:
    versions = m["versions"]
    for key in ("guest_image_digest", "workload_revision"):
        if not HEX64.fullmatch(str(versions[key])):
            print(f"STOP: {key} is not an immutable digest", file=sys.stderr)
            return 2
        if _placeholder_digest(str(versions[key])):
            print(f"STOP: {key} is still a template placeholder", file=sys.stderr)
            return 2
    supply = m.get("supply_chain", {})
    if not isinstance(supply, dict):
        print("STOP: supply_chain must be a table", file=sys.stderr)
        return 2
    expected_workloads = {
        "noop", "cpu", "memory", "disk-seq", "disk-rand", "metadata",
        "network", "checkout-build", "docker", "service-container",
    }
    declared_workloads = set(supply.get("workloads", []))
    if declared_workloads != expected_workloads:
        print("STOP: supply_chain.workloads does not cover the frozen workload catalog",
              file=sys.stderr)
        return 2
    for raw in supply.get("files", []):
        path = Path(str(raw))
        if not path.is_file():
            print(f"STOP: supply-chain input missing: {path}", file=sys.stderr)
            return 2
        if path.name == "catalog.toml":
            try:
                catalog = tomllib.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                print(f"STOP: cannot parse workload catalog: {exc}", file=sys.stderr)
                return 2
            expected_repositories = {
                "ripgrep": (
                    "BurntSushi/ripgrep",
                    "f9c05a949d1a0dc8e16dee28ca9605d38611faeb",
                    ".github/workflows/ci.yml",
                    "rustfmt",
                ),
                "flask": (
                    "pallets/flask",
                    "36e4a824f340fdee7ed50937ba8e7f6bc7d17f81",
                    ".github/workflows/pre-commit.yaml",
                    "main",
                ),
                "vite": (
                    "vitejs/vite",
                    "3ac77d9dd742968961af38a5a91ed6b061ceda7d",
                    ".github/workflows/ci.yml",
                    "lint",
                ),
                "chi": (
                    "go-chi/chi",
                    "8b258c7bb28f97a5f2a856ff7ef962578fec9215",
                    ".github/workflows/ci.yml",
                    "test",
                ),
                "testcontainers_go": (
                    "testcontainers/testcontainers-go",
                    "ea854ecb16425b6e77bc19e95080213fb69a6ac9",
                    ".github/workflows/ci.yml",
                    "detect-modules",
                ),
            }
            actual = catalog.get("real_workflows")
            if not isinstance(actual, dict) or set(actual) != set(expected_repositories):
                print("STOP: catalog does not contain exactly the frozen real workflows",
                      file=sys.stderr)
                return 2
            for name, expected in expected_repositories.items():
                row = actual.get(name)
                if not isinstance(row, dict) or tuple(row.get(key) for key in
                                                     ("repository", "commit", "workflow", "job")) != expected:
                    print(f"STOP: real workflow pin mismatch: {name}", file=sys.stderr)
                    return 2
        if path.name == "image-manifest.toml":
            try:
                image = tomllib.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                print(f"STOP: cannot parse image manifest: {exc}", file=sys.stderr)
                return 2
            required = ("base", "runner_version", "node", "python", "go",
                        "rust", "fio", "iperf3", "docker", "user", "uid", "gid")
            missing = [key for key in required if key not in image]
            if missing:
                print("STOP: image manifest missing " + ",".join(missing), file=sys.stderr)
                return 2
            if any(isinstance(value, str) and "pinned-by-campaign" in value
                   for value in image.values()):
                print("STOP: image manifest still contains campaign placeholders",
                      file=sys.stderr)
                return 2
    print(json.dumps({"guest_image_digest": versions["guest_image_digest"],
                      "workload_revision": versions["workload_revision"],
                      "real_workflows": 5,
                      "verified_files": len(supply.get("files", []))},
                     sort_keys=True))
    print("SUPPLY PASS")
    return 0


def _adapter_contract(m: dict[str, Any], arm: str) -> int:
    try:
        from benchmarks.virtualization.adapters import HypemanFirecrackerAdapter, SmolVMAdapter
    except ModuleNotFoundError:
        from adapters import HypemanFirecrackerAdapter, SmolVMAdapter

    spec = _candidate_spec(m, arm)
    if not os.environ.get("PRELOOP_BENCH_APPROVED"):
        raise ValueError("STOP: adapter-contract requires PRELOOP_BENCH_APPROVED")

    if arm == "smolvm":
        def runner(argv):
            env = dict(os.environ)
            env.update({
                "SMOLVM_DATA_DIR": str(spec["data_dir"]),
                "SMOLVM_SECCOMP": "enforce",
                "SMOLVM_LANDLOCK": "enforce",
            })
            completed = subprocess.run(argv, env=env, capture_output=True, text=True, check=False)
            if completed.returncode:
                raise RuntimeError(completed.stderr[-1000:] or "SmolVM command failed")
            return {"returncode": 0, "stdout": completed.stdout}
        adapter = SmolVMAdapter(spec, runner=runner)
    elif arm == "hypeman-fc":
        def transport(method, path, payload, headers=None):
            url = str(spec["base_url"]).rstrip("/") + path
            body = None if method == "GET" or not payload else json.dumps(payload).encode()
            request = urllib.request.Request(
                url, data=body, method=method,
                headers={"Content-Type": "application/json", **(headers or {})},
            )
            with urllib.request.urlopen(request, timeout=120) as response:
                raw = response.read()
                if not raw:
                    return {"status": response.status}
                return json.loads(raw)
        adapter = HypemanFirecrackerAdapter(spec, transport=transport)
    else:
        raise ValueError(f"STOP: unsupported arm {arm}")

    vm = f"bench-{m['campaign_id']}-adapter-contract"
    values = {
        "vm_id": vm,
        "image": spec.get("image") or m.get("versions", {}).get("guest_image_digest"),
        "resource_profile": m["resources"]["tiny"],
    }
    clone = f"{vm}-clone"
    try:
        adapter.prepare_image(values)
        adapter.create(values)
        if arm == "smolvm":
            adapter.start(values)
        adapter.wait_guest_ready(values)
        adapter.exec(values, ["/bin/sh", "-c", "true"])
        adapter.prepare_snapshot(values)
        adapter.clone_or_restore({**values, "fork_name": clone, "clone_id": clone})
        adapter.stop({"vm_id": clone})
        adapter.delete({"vm_id": clone})
        adapter.stop(values)
        adapter.delete(values)
    except Exception as exc:
        # Best-effort cleanup is restricted to the two IDs generated above.
        for item in (clone, vm):
            try:
                adapter.delete({"vm_id": item})
            except Exception:
                pass
        raise ValueError(f"{arm} adapter contract failed: {type(exc).__name__}: {exc}") from exc
    print(json.dumps({"arm": arm, "vm_id": vm, "operations": [
        "prepare_image", "create", "start", "wait_guest_ready", "exec",
        "prepare_snapshot", "clone_or_restore", "stop", "delete"
    ]}, sort_keys=True))
    print("ADAPTER CONTRACT PASS")
    return 0


def _collector_overhead(m: dict[str, Any]) -> int:
    try:
        from benchmarks.virtualization.collect import measure_overhead
    except ModuleNotFoundError:
        from collect import measure_overhead

    result = measure_overhead(100)
    print(json.dumps(result, sort_keys=True))
    print("COLLECTOR OVERHEAD MEASURED")
    return 0


def _assert_clean(m: dict[str, Any]) -> int:
    state, runtime = _campaign_paths(m)
    present = [str(path) for path in (state, runtime) if path.exists()]
    if present:
        print("DIRTY: " + ", ".join(present), file=sys.stderr)
        return 2
    print("CLEAN")
    return 0


def _cleanup(m: dict[str, Any], args) -> int:
    if not args.yes:
        print("STOP: cleanup requires explicit --yes operator gate", file=sys.stderr)
        return 2
    if args.manifest_sha256 != canonical_manifest_hash(m):
        print("STOP: manifest hash mismatch", file=sys.stderr)
        return 2
    if not m.get("isolation", {}).get("allow_destructive_bench_state_cleanup"):
        print("STOP: manifest does not authorize campaign cleanup", file=sys.stderr)
        return 2
    state, runtime = _campaign_paths(m)
    resources = [path for path in (state, runtime) if path.exists()]
    print("CLEANUP RESOURCES:")
    for path in resources:
        print(f"  {path}")
    if args.dry_run:
        print("CLEANUP PLAN ONLY")
        return 0
    for path in resources:
        shutil.rmtree(path)
    return _assert_clean(m)


def _run_campaign(m: dict[str, Any], args) -> int:
    if not args.phase:
        print("STOP: --phase is required for run", file=sys.stderr)
        return 2
    if not os.environ.get("PRELOOP_BENCH_APPROVED"):
        print("STOP: set PRELOOP_BENCH_APPROVED only after the operator-approved "
              "maintenance/quiescence gate", file=sys.stderr)
        return 2
    try:
        state, _ = _campaign_paths(m)
        arm = args.arm or "smolvm"
        adapter, spec = _make_runtime_adapter(m, arm)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.phase not in {"lifecycle", "isolation"}:
        print("STOP: official-runner/control-plane transport is not configured for "
              f"phase {args.phase}; no benchmark samples were recorded", file=sys.stderr)
        return 2
    repetitions = int(m["benchmark"].get("repetitions", 1))
    if args.phase == "isolation":
        repetitions = min(repetitions, 1)
    all_success = True
    for repetition in range(repetitions):
        vm = f"bench-{m['campaign_id']}-{args.phase}-{repetition}"
        values = {
            "vm_id": vm,
            "image": spec.get("image") or m["versions"]["guest_image_digest"],
            "resource_profile": m["resources"]["tiny"],
        }
        started = time.monotonic_ns()
        success = False
        failure = ""
        diagnostic = ""
        try:
            adapter.prepare_image(values)
            adapter.create(values)
            if arm == "smolvm":
                adapter.start(values)
            adapter.wait_guest_ready(values)
            adapter.exec(values, ["/bin/sh", "-c", "true"])
            success = True
        except Exception as exc:
            all_success = False
            failure = "adapter_failure"
            diagnostic = f"{type(exc).__name__}: {exc}"
        finally:
            for operation in ("stop", "delete"):
                try:
                    getattr(adapter, operation)(values)
                except Exception as exc:
                    all_success = False
                    if not diagnostic:
                        failure = "cleanup_failure"
                        diagnostic = f"{type(exc).__name__}: {exc}"
        duration_ms = (time.monotonic_ns() - started) / 1_000_000
        values_out = {"duration_ms": duration_ms}
        if args.phase == "isolation":
            # The integration posture is evidence, not an assumption about
            # Firecracker itself. Hypeman's pinned launcher is direct
            # firecracker invocation unless a jailer-equivalent is explicitly
            # configured and observed.
            jailer = bool(spec.get("jailer_equivalent"))
            posture = "multi_tenant_eligible" if jailer else "trusted_single_tenant_only"
            values_out.update({
                "isolation_posture": posture,
                "jailer_equivalent": jailer,
                "seccomp": spec.get("seccomp", "unverified"),
                "landlock": spec.get("landlock", "unverified"),
            })
        record = event_record(
            m,
            phase=args.phase,
            arm=arm,
            repetition=repetition,
            event_kind=(
                "isolation_gate" if args.phase == "isolation" and success
                else "failed" if not success
                else "complete"
            ),
            success=success,
            failure_class=failure,
            diagnostic=diagnostic,
            values=values_out,
            units={"duration_ms": "ms"},
            resource_profile="tiny",
            cache_class="image-warm-boot-cold",
            candidate_hashes={
                "binary_sha256": spec.get("binary_sha256", ""),
                "config_sha256": spec.get("config_sha256", ""),
            },
        )
        _write_result(m, record)
    print(f"RESULTS {state / 'results' / (m['campaign_id'] + '.jsonl')}")
    if not all_success:
        print("STOP: one or more lifecycle samples failed", file=sys.stderr)
        return 2
    print("CAMPAIGN PHASE PASS")
    return 0


def _campaign_command(cmd: str, args, m: dict[str, Any]) -> int:
    try:
        if cmd == "verify-supply":
            return _verify_supply(m)
        if cmd == "adapter-contract":
            return _adapter_contract(m, args.arm)
        if cmd == "collector-overhead":
            return _collector_overhead(m)
        if cmd == "assert-clean":
            return _assert_clean(m)
        if cmd == "cleanup":
            return _cleanup(m, args)
        if cmd == "smoke":
            args.phase = "smoke"
            return _run_campaign(m, args)
        if cmd == "run":
            return _run_campaign(m, args)
        print(f"STOP: unsupported campaign command {cmd}", file=sys.stderr)
        return 2
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"STOP: {exc}", file=sys.stderr)
        return 2

def main(argv=None):
    p=argparse.ArgumentParser(description="safe virtualization benchmark harness"); p.add_argument("command",choices=REQUIRED_COMMANDS); p.add_argument("--manifest",required=True); p.add_argument("--dry-run",action="store_true"); p.add_argument("--fake",action="store_true"); p.add_argument("--observe",type=int,default=0); p.add_argument("--arm",default=""); p.add_argument("--phase",default=""); p.add_argument("--manifest-sha256",default=""); p.add_argument("--yes",action="store_true"); a=p.parse_args(argv)
    try: m=load_manifest(a.manifest)
    except ValueError as x: print(x,file=sys.stderr); return 2
    if a.command=="validate":
        e=validate_manifest(m)
        if e:
            for x in e: print("INVALID: "+x,file=sys.stderr)
            return 2
        print("VALID"); return 0
    if a.command=="cleanup":
        return _dispatch("cleanup", a, m)
    if a.command=="preflight":
        if validate_manifest(m): print("STOP: invalid manifest",file=sys.stderr); return 2
        if a.fake or a.dry_run: print("PREFLIGHT PASS (fake)"); return 0
        swap_baseline = _swap_counters()
        cpu_baseline = _cpu_ticks()
        time.sleep(0.1)
        failures=_preflight_checks(m, swap_baseline=swap_baseline, cpu_baseline=cpu_baseline)
        inventory = _inventory(m)
        try:
            state, _ = _campaign_paths(m)
            evidence = state / "preflight" / "inventory.json"
            evidence.parent.mkdir(parents=True, exist_ok=True)
            evidence.write_text(
                json.dumps(inventory, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            os.chmod(evidence, 0o600)
        except (OSError, ValueError) as exc:
            failures.append(f"cannot persist preflight inventory: {exc}")
        print(json.dumps(inventory, sort_keys=True))
        if failures:
            for x in failures: print("STOP: "+x,file=sys.stderr)
            return 2
        observe=max(0,a.observe)
        for _ in range(observe):
            time.sleep(1)
            cpu_baseline = _cpu_ticks()
            time.sleep(0.1)
            failures=_preflight_checks(m, swap_baseline=swap_baseline, cpu_baseline=cpu_baseline)
            if failures:
                for x in failures: print("STOP: "+x,file=sys.stderr)
                return 2
        print("PREFLIGHT PASS"); return 0
    if a.fake or a.dry_run:
        print(json.dumps(event_record(m,phase=a.phase or a.command,event_kind="dry-run",diagnostic="not executed; no live success claim"),sort_keys=True)); return 0
    return _dispatch(a.command,a,m)
if __name__=="__main__": raise SystemExit(main())
