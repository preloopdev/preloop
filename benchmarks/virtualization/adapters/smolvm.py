from __future__ import annotations
from pathlib import Path
from typing import Any, Callable, Mapping
from .base import AdapterError, require_hash

class SmolVMAdapter:
    arm = "smolvm"
    def __init__(self, manifest: Mapping[str, Any], runner: Callable[[list[str]], Any] | None = None):
        self.manifest = manifest; self.runner = runner; self.owned: set[str] = set()
        self.binary = str(manifest.get("binary", "")); self.data_dir = Path(str(manifest.get("data_dir", "")))
        if not self.binary or not self.data_dir.is_absolute(): raise AdapterError("SmolVM requires absolute binary and data_dir")
        expected = manifest.get("binary_sha256"); config = manifest.get("config_sha256")
        if not expected or not config: raise AdapterError("exact binary_sha256 and config_sha256 required")
        require_hash(self.binary, str(expected)); self.config_sha256 = str(config)
        if manifest.get("config_path"):
            require_hash(str(manifest["config_path"]), self.config_sha256)
    def _call(self, args: list[str]) -> Mapping[str, Any]:
        if self.runner is None: raise AdapterError("live SmolVM runner not configured")
        return self.runner(args)
    def _id(self, values):
        vm=str(values.get("vm_id", ""))
        campaign=str(self.manifest.get("campaign", ""));
        if not campaign or not vm or not vm.startswith("bench-"+campaign+"-"): raise AdapterError("VM id is not campaign-isolated")
        self.owned.add(vm); return vm
    def prepare_image(self,v): return {"image": v.get("image") or self.manifest.get("image")}
    def create(self,v):
        profile = v.get("resource_profile", {})
        extra = ["--name", self._id(v), "--image", str(v.get("image") or self.manifest.get("image")),
                 "--cpus", str(profile.get("vcpus", 1)),
                 "--mem", str(profile.get("memory_mib", 1024)),
                 "--storage", str(profile.get("writable_disk_gib", 10))]
        return self._call([self.binary, "machine", "create", *extra])
    def start(self,v):
        extra = ["machine", "start", "--name", self._id(v)]
        if v.get("forkable"): extra.append("--forkable")
        return self._call([self.binary, *extra])
    def wait_guest_ready(self,v):
        # SmolVM has no separate guest-agent endpoint. Status plus a bounded
        # no-op exec is the readiness boundary used by the provider.
        return self.exec(v, ["/bin/sh", "-c", "true"])
    def exec(self,v,command):
        if not command or any("\x00" in x for x in command): raise AdapterError("invalid guest command")
        # SmolVM 1.8.x executes through the guest agent and no longer accepts
        # a host-side `--user` option.  The agent starts commands as root for
        # the benchmark image; passing `--user root` is parsed as a guest
        # command and makes every readiness probe fail.
        return self._call([self.binary, "machine", "exec", "--name", self._id(v),
                           "--", *command])
    def prepare_snapshot(self,v):
        output = str(v.get("snapshot_path") or (self.data_dir / self.manifest["campaign"] / f"{self._id(v)}.smolmachine"))
        return self._call([self.binary, "machine", "pack", "--name", self._id(v), "--output", output])
    def clone_or_restore(self,v):
        golden = str(v.get("golden_id") or self._id(v))
        clone = str(v.get("clone_id") or v.get("fork_name") or self._id(v))
        if not clone.startswith("bench-"+self.manifest["campaign"]+"-"):
            raise AdapterError("clone id is not campaign-isolated")
        return self._call([self.binary, "machine", "fork", "--golden", golden, "--name", clone])
    def stop(self,v): return self._call([self.binary, "machine", "stop", "--name", self._id(v)])
    def delete(self,v): return self._call([self.binary, "machine", "delete", "--name", self._id(v), "-f"])
    def list_owned(self):
        if self.runner is None: raise AdapterError("live SmolVM runner not configured")
        result=self.runner([self.binary,"machine","ls","--json"])
        ids=result if isinstance(result,list) else result.get("ids",[])
        return [x for x in ids if str(x).startswith("bench-"+self.manifest["campaign"]+"-")]
    def collect_runtime_stats(self,v):
        return self._call([self.binary, "machine", "status", "--name", self._id(v), "--json"])
