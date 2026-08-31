from __future__ import annotations

import hashlib, json, os, re, time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

TOKEN = re.compile(r"(?i)(bearer\s+|token[=:]\s*|password[=:]\s*|secret[=:]\s*)[^\s,;]+")
UNITS = {"ns", "us", "ms", "s", "bytes", "count", "percent", "bytes_per_second", "unknown"}
TERMINAL = {"complete", "failed", "timeout", "deleted"}

class AdapterError(RuntimeError): pass
class Adapter(Protocol):
    def prepare_image(self, values: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def create(self, values: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def start(self, values: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def wait_guest_ready(self, values: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def exec(self, values: Mapping[str, Any], command: list[str]) -> Mapping[str, Any]: ...
    def prepare_snapshot(self, values: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def clone_or_restore(self, values: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def stop(self, values: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def delete(self, values: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def list_owned(self) -> list[str]: ...
    def collect_runtime_stats(self, values: Mapping[str, Any]) -> Mapping[str, Any]: ...

@dataclass(frozen=True)
class LifecycleEvent:
    manifest_sha256: str
    campaign: str
    phase: str
    arm: str
    repetition: int
    event_kind: str
    success: bool
    failure_class: str | None = None
    diagnostic: str = ""
    values: Mapping[str, Any] = field(default_factory=dict)
    units: Mapping[str, str] = field(default_factory=dict)
    monotonic_ns: int = field(default_factory=time.monotonic_ns)
    timestamp_utc: str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    schema_version: int = 1
    candidate_binary_sha256: str = ""
    candidate_config_sha256: str = ""

    def as_dict(self) -> dict[str, Any]:
        d = self.__dict__.copy()
        d["values"] = dict(self.values); d["units"] = dict(self.units)
        d["diagnostic"] = redact(self.diagnostic)
        return d

def redact(value: Any) -> Any:
    if isinstance(value, str): return TOKEN.sub(lambda m: m.group(1) + "[REDACTED]", value)
    if isinstance(value, Mapping): return {str(k): redact(v) for k,v in value.items()}
    if isinstance(value, list): return [redact(v) for v in value]
    return value

class EventWriter:
    """Append-only JSONL writer. Terminal records are fsync'd and immutable."""
    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path); self._last = -1; self._terminal: set[tuple[str,int,str]] = set()
        self.path.parent.mkdir(parents=True, exist_ok=True)
    def write(self, event: LifecycleEvent | Mapping[str, Any]) -> dict[str, Any]:
        d = redact(event.as_dict() if isinstance(event, LifecycleEvent) else dict(event))
        required = ("schema_version","manifest_sha256","campaign","phase","arm","repetition","monotonic_ns","timestamp_utc","event_kind","success","failure_class","diagnostic","values","units")
        missing = [x for x in required if x not in d]
        if missing: raise AdapterError("event missing " + ",".join(missing))
        n = int(d["monotonic_ns"]); key = (str(d["phase"]), int(d["repetition"]), str(d["arm"]))
        if n < self._last: raise AdapterError("event monotonic time decreased")
        if str(d["event_kind"]) in TERMINAL and key in self._terminal: raise AdapterError("duplicate terminal event")
        if str(d["event_kind"]) in TERMINAL: self._terminal.add(key)
        self._last = n
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(d, sort_keys=True, separators=(",", ":")) + "\n"); f.flush()
            if str(d["event_kind"]) in TERMINAL: os.fsync(f.fileno())
        return d

def validate_events(source: str | os.PathLike[str] | list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = [json.loads(x) for x in Path(source).read_text().splitlines() if x.strip()] if not isinstance(source, list) else [dict(x) for x in source]
    last = -1; terminals: set[tuple[str,int,str]] = set(); campaigns=set(); manifests=set()
    for d in rows:
        for k in ("schema_version","manifest_sha256","campaign","phase","arm","repetition","monotonic_ns","timestamp_utc","event_kind","success","failure_class","diagnostic","values","units"): 
            if k not in d: raise AdapterError("event missing " + k)
        if int(d["monotonic_ns"]) < last: raise AdapterError("event monotonic time decreased")
        last = int(d["monotonic_ns"])
        if not d.get("event_kind") or not d.get("phase") or not d.get("arm"):
            raise AdapterError("event phase, arm, and event_kind are required")
        campaigns.add(str(d["campaign"])); manifests.add(str(d["manifest_sha256"]))
        for u in d.get("units", {}).values():
            if u not in UNITS: raise AdapterError("unknown unit " + str(u))
        if str(d["event_kind"]) in TERMINAL:
            key=(str(d["phase"]),int(d["repetition"]),str(d["arm"]))
            if key in terminals: raise AdapterError("duplicate terminal event")
            terminals.add(key)
        if "[REDACTED]" not in json.dumps(d) and TOKEN.search(json.dumps(d)): raise AdapterError("secret in event")
    if len(campaigns) > 1 or len(manifests) > 1:
        raise AdapterError("event stream mixes campaigns or manifests")
    return rows

def sha256_file(path: str | os.PathLike[str]) -> str:
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for chunk in iter(lambda:f.read(1024*1024), b""): h.update(chunk)
    return h.hexdigest()

def require_hash(path: str, expected: str) -> str:
    actual=sha256_file(path)
    if actual.lower() != expected.lower(): raise AdapterError(f"hash mismatch for {path}")
    return actual
