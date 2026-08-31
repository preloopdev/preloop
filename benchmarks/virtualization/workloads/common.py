from __future__ import annotations
import hashlib
import os
import random
import shutil
import subprocess
import tempfile
import urllib.request

SUPPORTED = (
    "noop",
    "cpu",
    "memory",
    "disk-seq",
    "disk-rand",
    "metadata",
    "network",
    "checkout-build",
    "docker",
    "service-container",
)

def checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def run(kind: str = "noop", iterations: int = 1000) -> dict[str, object]:
    if kind not in SUPPORTED:
        raise ValueError(f"unknown workload: {kind}")
    if iterations < 0 or iterations > 100_000:
        raise ValueError("iterations out of bounded range")
    if kind == "noop":
        payload = b"preloop-virtualization-noop\n"
    elif kind == "cpu":
        value = 0
        for i in range(iterations): value = (value * 33 + i) & 0xFFFFFFFF
        payload = value.to_bytes(4, "big")
    elif kind == "memory":
        size = min(max(iterations, 1), 16 * 1024) * 1024
        block = bytearray(size)
        for offset in range(0, size, 4096):
            block[offset] = (offset // 4096) & 0xFF
        payload = bytes(block[::4096])
    elif kind == "disk-seq":
        block = (b"preloop-sequential-block\n" * 256)[:4096]
        with tempfile.TemporaryDirectory(prefix="preloop-bench-disk-") as td:
            path = os.path.join(td, "seq.bin")
            with open(path, "wb") as stream:
                for _ in range(max(iterations, 1)):
                    stream.write(block)
                stream.flush()
                os.fsync(stream.fileno())
            with open(path, "rb") as stream:
                payload = hashlib.sha256(stream.read()).digest()
    elif kind == "disk-rand":
        block = (b"preloop-random-block\n" * 256)[:4096]
        with tempfile.TemporaryDirectory(prefix="preloop-bench-rand-") as td:
            path = os.path.join(td, "rand.bin")
            with open(path, "wb") as stream:
                stream.truncate(max(iterations, 1) * len(block))
            rng = random.Random(0)
            digest = hashlib.sha256()
            with open(path, "r+b") as stream:
                for _ in range(max(iterations, 1)):
                    stream.seek(rng.randrange(max(iterations, 1)) * len(block))
                    stream.write(block)
                    digest.update(block)
                stream.flush()
                os.fsync(stream.fileno())
            payload = digest.digest()
    elif kind == "metadata":
        payload = b"".join(f"file-{i:06d}\n".encode() for i in range(iterations))
    elif kind == "network":
        endpoint = os.environ.get("PRELOOP_BENCH_HTTP_ENDPOINT")
        if not endpoint:
            raise RuntimeError("PRELOOP_BENCH_HTTP_ENDPOINT is required for network workload")
        with urllib.request.urlopen(endpoint, timeout=10) as response:
            payload = response.read(16 * 1024 * 1024 + 1)
        if len(payload) > 16 * 1024 * 1024:
            raise RuntimeError("network payload exceeds bounded workload size")
    elif kind == "checkout-build":
        workspace = os.environ.get("PRELOOP_BENCH_WORKSPACE")
        if not workspace or not os.path.isdir(workspace):
            raise RuntimeError("PRELOOP_BENCH_WORKSPACE is required for checkout-build")
        completed = subprocess.run(
            ["git", "-C", workspace, "status", "--porcelain"],
            check=False,
            capture_output=True,
        )
        if completed.returncode:
            raise RuntimeError("git workspace check failed")
        payload = completed.stdout
    elif kind in ("docker", "service-container"):
        if shutil.which("docker") is None:
            raise RuntimeError("docker is required for container workload")
        completed = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode:
            raise RuntimeError("docker daemon is unavailable")
        payload = completed.stdout.encode()
    return {"workload": kind, "iterations": iterations, "bytes": len(payload), "sha256": checksum(payload)}
