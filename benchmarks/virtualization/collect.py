"""Best-effort host/process/cgroup collectors with explicit unavailable values."""
from __future__ import annotations
import os, socket, time
from pathlib import Path
from typing import Any, Mapping
try:
    from benchmarks.virtualization.adapters.base import redact, EventWriter, LifecycleEvent, validate_events
except ModuleNotFoundError:
    from adapters.base import redact, EventWriter, LifecycleEvent, validate_events

__all__ = [
    "EventWriter", "LifecycleEvent", "validate_events", "redact",
    "collect_host", "collect_process", "collect_cgroup", "Collector",
    "measure_overhead", "unavailable",
]

UNAVAILABLE = {"status":"unavailable"}
def unavailable(reason: str) -> dict[str,str]: return {"status":"unavailable","reason":reason}
def _read(path: str | os.PathLike[str]) -> str | None:
    try: return Path(path).read_text().strip()
    except (OSError, ValueError): return None

def _key_value_lines(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    result = {}
    for line in raw.splitlines():
        key, sep, value = line.partition(":")
        if sep:
            result[key.strip()] = value.strip()
    return result

def _numeric_meminfo(raw: str | None) -> dict[str, int]:
    result = {}
    for key, value in _key_value_lines(raw).items():
        fields = value.split()
        if fields and fields[0].isdigit():
            multiplier = 1024 if len(fields) > 1 and fields[1].lower() == "kb" else 1
            result[key] = int(fields[0]) * multiplier
    return result

def _psi(sys_root: str | os.PathLike[str]) -> dict[str, Any]:
    result = {}
    for resource in ("cpu", "memory", "io"):
        raw = _read(Path(sys_root) / "pressure" / resource)
        if raw is None:
            result[resource] = unavailable("PSI unavailable")
            continue
        parsed = {}
        for line in raw.splitlines():
            fields = line.split()
            if not fields:
                continue
            values = {}
            for field in fields[1:]:
                key, _, value = field.partition("=")
                try: values[key] = float(value)
                except ValueError: continue
            parsed[fields[0]] = values
        result[resource] = parsed
    return result

def collect_host(proc_root: str = "/proc", sys_root: str = "/sys") -> dict[str,Any]:
    out={"timestamp_ns":time.monotonic_ns(), "hostname": socket.gethostname()}
    stat=_read(Path(proc_root)/"stat")
    if stat:
        line=next((x for x in stat.splitlines() if x.startswith("cpu ")),"")
        vals=line.split()[1:]
        try: out["cpu_ticks"]={"user":int(vals[0]),"system":int(vals[2]),"idle":int(vals[3])}
        except (IndexError,ValueError): out["cpu_ticks"]=unavailable("invalid /proc/stat")
    else: out["cpu_ticks"]=unavailable("/proc/stat unavailable")
    out["memory_bytes"]=_numeric_meminfo(_read(Path(proc_root)/"meminfo")) or unavailable(
        "/proc/meminfo unavailable")
    load=_read(Path(proc_root)/"loadavg")
    out["loadavg"]=load.split()[:3] if load else unavailable("/proc/loadavg unavailable")
    out["psi"]=_psi(sys_root)
    net=_read(Path(proc_root)/"net/dev")
    if net:
        interfaces={}
        for line in net.splitlines()[2:]:
            name, _, values=line.partition(":")
            fields=values.split()
            if len(fields) >= 10:
                interfaces[name.strip()]={"rx_bytes":int(fields[0]),"rx_packets":int(fields[1]),
                                          "tx_bytes":int(fields[8]),"tx_packets":int(fields[9])}
        out["network"]=interfaces
    else:
        out["network"]=unavailable("/proc/net/dev unavailable")
    return redact(out)

def collect_cgroup(path: str | os.PathLike[str]) -> dict[str,Any]:
    root=Path(path); out={}
    for name in ("memory.current","memory.peak","memory.events","memory.max",
                 "cpu.stat","cpu.max","io.stat","io.max","pids.current","pids.max","cgroup.procs"):
        raw=_read(root/name)
        if raw is None: out[name]=unavailable("cgroup file unavailable")
        elif name=="memory.events": out[name]={p[0]:int(p[1]) for p in (line.split() for line in raw.splitlines()) if len(p)==2 and p[1].isdigit()}
        elif name in ("memory.current","memory.peak","pids.current"): out[name]=int(raw) if raw.isdigit() else unavailable("invalid counter")
        else: out[name]=raw
    return out

def collect_process(pid: int, proc_root: str = "/proc") -> dict[str,Any]:
    root=Path(proc_root)/str(pid); out={"pid":pid}
    status=_read(root/"status")
    if status:
        for line in status.splitlines():
            if line.startswith(("VmRSS:","VmSize:","VmPeak:","Threads:","voluntary_ctxt_switches:","nonvoluntary_ctxt_switches:")):
                p=line.split(); out[p[0].rstrip(":")]=int(p[1]) if len(p)>1 and p[1].isdigit() else unavailable("invalid status")
    else: out["status"]=unavailable("process unavailable")
    smaps=_read(root/"smaps_rollup"); out["smaps_rollup"]=smaps if smaps is not None else unavailable("smaps unavailable")
    out["cmdline"]=(_read(root/"cmdline") or "").replace("\x00", " ") or unavailable("cmdline unavailable")
    out["cgroup"]=_read(root/"cgroup") or unavailable("cgroup unavailable")
    out["namespaces"]={}
    try:
        for entry in (root/"ns").iterdir():
            try: out["namespaces"][entry.name]=os.readlink(entry)
            except OSError: pass
    except OSError:
        out["namespaces"]=unavailable("namespace links unavailable")
    try: out["fd_count"]=len(list((root/"fd").iterdir()))
    except OSError: out["fd_count"]=unavailable("fd unavailable")
    return out

class Collector:
    def __init__(self, interval_s: float=1.0): self.interval_s=interval_s; self.samples=[]
    def sample(self, pid: int|None=None, cgroup: str|None=None) -> dict[str,Any]:
        row={"monotonic_ns":time.monotonic_ns(),"host":collect_host()}
        if pid is not None: row["process"]=collect_process(pid)
        if cgroup is not None: row["cgroup"]=collect_cgroup(cgroup)
        self.samples.append(row); return row
    def coverage(self, expected_intervals: int|None=None) -> dict[str,Any]:
        times=[int(x["monotonic_ns"]) for x in self.samples]; gaps=[b-a for a,b in zip(times,times[1:])]
        expected=expected_intervals if expected_intervals is not None else len(times)
        target_ns=int(self.interval_s*1_000_000_000*3)
        return {"expected_intervals":expected,"actual_intervals":len(times),
                "largest_gap_ns":max(gaps,default=0),
                "clock_skew_ns":(times[-1]-times[0]) if len(times)>1 else 0,
                "coverage":len(times)/expected if expected else 0,
                "invalid_gap":max(gaps,default=0) > target_ns}

def measure_overhead(iterations: int=10) -> dict[str,float]:
    start=time.process_time_ns()
    for _ in range(max(1,iterations)): collect_host()
    elapsed=time.process_time_ns()-start
    return {"collector_cpu_ns":float(elapsed),"iterations":float(max(1,iterations))}
