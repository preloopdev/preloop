from __future__ import annotations
import os
import subprocess
from typing import Any, Callable, Mapping
from .base import AdapterError, require_hash

class HypemanFirecrackerAdapter:
    arm="hypeman-fc"
    def __init__(self, manifest: Mapping[str, Any], transport: Callable[[str,str,Mapping[str,Any]], Any] | None = None):
        self.manifest=manifest; self.transport=transport; self.owned=set()
        self.base_url=str(manifest.get("base_url", "")); self.token=str(manifest.get("token", ""))
        if not self.base_url.startswith("http://127.0.0.1:") and not self.base_url.startswith("http://localhost:"):
            raise AdapterError("Hypeman endpoint must be authenticated loopback")
        if not self.token: raise AdapterError("Hypeman authentication token required")
        if manifest.get("hypervisor") != "firecracker": raise AdapterError("hypervisor must be firecracker")
        for key in ("binary_sha256","config_sha256"):
            if not manifest.get(key): raise AdapterError("exact "+key+" required")
        if manifest.get("binary_path"): require_hash(str(manifest["binary_path"]),str(manifest["binary_sha256"]))
        if manifest.get("config_path"): require_hash(str(manifest["config_path"]),str(manifest["config_sha256"]))
    def _call(self,method,path,payload):
        if self.transport is None: raise AdapterError("live Hypeman transport not configured")
        request = dict(payload)
        if method == "POST":
            request.setdefault("hypervisor", "firecracker")
        try:
            return self.transport(method, path, request, {"Authorization": f"Bearer {self.token}"})
        except TypeError:
            # Keep the test transport deliberately small while ensuring the
            # real transport receives the token as a header, never JSON data.
            return self.transport(method, path, request)
    def _vm(self,v):
        vm=str(v.get("vm_id", "")); campaign=str(self.manifest.get("campaign", ""))
        if not campaign or not vm.startswith("bench-"+campaign+"-"): raise AdapterError("instance id is not campaign-isolated")
        self.owned.add(vm); return vm
    def create(self,v):
        vm = self._vm(v)
        profile = v.get("resource_profile", {})
        return self._call("POST", "/instances", {
            "name": vm,
            "image": v.get("image") or self.manifest.get("image"),
            "size": f"{int(profile.get('memory_mib', 1024))}MB",
            "overlay_size": f"{int(profile.get('writable_disk_gib', 10))}GB",
            "vcpus": int(profile.get("vcpus", 1)),
            "hypervisor": "firecracker",
            "tags": {"campaign": self.manifest["campaign"]},
            "skip_guest_agent": False,
            "health_check": {
                "type": "exec",
                "interval": "5s",
                "timeout": "5s",
                "start_period": "30s",
                "failure_threshold": 3,
                "success_threshold": 1,
                "exec": {"command": ["/bin/sh", "-c", "true"]},
            },
        })
    def prepare_image(self,v): return {"image": v.get("image") or self.manifest.get("image")}
    def start(self,v):
        vm=self._vm(v); return self._call("POST", f"/instances/{vm}/start", {})
    def wait_guest_ready(self,v):
        vm=self._vm(v)
        result = self._call("GET", f"/instances/{vm}/wait?state=Running&timeout={v.get('timeout','60s')}", {})
        status = self._call("GET", f"/instances/{vm}", {})
        health = status.get("health_status", {}) if isinstance(status, Mapping) else {}
        if health.get("status") != "healthy":
            raise AdapterError(f"Hypeman guest agent is not healthy: {health}")
        return result
    def exec(self,v,command):
        if not command or any("\x00" in x for x in command): raise AdapterError("invalid guest command")
        cli = self.manifest.get("cli_path")
        if not cli:
            raise AdapterError("Hypeman exec requires a pinned cli_path")
        env = os.environ.copy()
        env["HYPEMAN_BASE_URL"] = self.base_url
        env["HYPEMAN_API_KEY"] = self.token
        result = subprocess.run([str(cli), "exec", self._vm(v), *command],
                                capture_output=True, text=True, check=False, env=env)
        return {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
    def prepare_snapshot(self,v):
        vm=self._vm(v); return self._call("POST", f"/instances/{vm}/standby", {})
    def clone_or_restore(self,v):
        vm=self._vm(v)
        if v.get("fork_name"):
            return self._call("POST", f"/instances/{vm}/fork", {
                "name": str(v["fork_name"]),
                "from_running": bool(v.get("from_running", False)),
            })
        return self._call("POST", f"/instances/{vm}/restore", {})
    def stop(self,v):
        vm=self._vm(v); return self._call("POST", f"/instances/{vm}/stop", {})
    def delete(self,v):
        vm=self._vm(v); return self._call("DELETE", f"/instances/{vm}", {})
    def list_owned(self):
        if self.transport is None: raise AdapterError("live Hypeman transport not configured")
        rows=self._call("GET", "/instances?tags[campaign]=" + self.manifest["campaign"], {})
        ids=rows if isinstance(rows,list) else rows.get("ids",[])
        return [x.get("name", x) if isinstance(x, dict) else x for x in ids
                if str(x.get("name", x) if isinstance(x, dict) else x).startswith("bench-"+self.manifest["campaign"]+"-")]
    def collect_runtime_stats(self,v):
        vm=self._vm(v); return self._call("GET", f"/instances/{vm}/stats", {})
