# Container-engine bootstrap for a guest that hosts a workload. Rendered by
# `docker_start_command` (crates/preloop-orchestrator/src/lib.rs), which
# substitutes the placeholders below with the values build.rs compiles in from
# official-image.toml, then hands the result to `sh -c` — inline when the exec
# channel is root, base64-decoded into `sudo -n sh` otherwise.
#
# A newline separates statements exactly like a `;`, so every statement sits
# on its own line: a `#` comment then comments only itself out, which the
# Rust string-continuation form this file replaced could not promise.
@@GUEST_STACK_ULIMIT@@
@@GUEST_NOFILE_ULIMIT@@
command -v dockerd >/dev/null 2>&1 || exit 0
raise_engine_chain() {
  raised=0
  failed=0
  for pid in $(cat /var/run/docker.pid 2>/dev/null) $(pgrep -x dockerd 2>/dev/null) $(pgrep -x containerd 2>/dev/null); do
    stack=
    nofile=
    while read -r word1 word2 word3 value _rest; do
      [ "$word1/$word2/$word3" = "Max/stack/size" ] && stack=$value
      [ "$word1/$word2/$word3" = "Max/open/files" ] && nofile=$value
    done 2>/dev/null < "/proc/$pid/limits"
    case "$stack" in ''|*[!0-9]*) stack= ;; esac
    case "$nofile" in ''|*[!0-9]*) nofile= ;; esac
    if [ -n "$stack" ] && [ "$stack" -lt @@GOLDEN_STACK_SOFT_BYTES@@ ]; then
      raised=1
      prlimit --pid "$pid" --stack=@@GOLDEN_STACK_PRLIMIT@@ 2>/dev/null || failed=1
    fi
    if [ -n "$nofile" ] && [ "$nofile" -lt @@GOLDEN_RLIMIT_NOFILE_SOFT@@ ]; then
      raised=1
      prlimit --pid "$pid" --nofile=@@GOLDEN_NOFILE_PRLIMIT@@ 2>/dev/null || failed=1
    fi
  done
  if [ "$failed" -ne 0 ]; then
    echo 'could not raise an inherited dockerd/containerd to the hosted process limits' >&2
    return 1
  fi
  [ "$raised" -eq 1 ] && echo 'raised the inherited engine chain to the hosted process limits'
  return 0
}
docker info >/dev/null 2>&1 && { raise_engine_chain || exit 1; exit 0; }
rm -f /var/run/docker.pid
mkdir -p @@DOCKER_DATA_ROOT@@
modprobe overlay >/dev/null 2>&1 || true
modprobe fuse >/dev/null 2>&1 || true
if grep -q fuse /proc/filesystems; then
  [ -e /dev/fuse ] || mknod /dev/fuse c 10 229
fi
mkdir -p /tmp/.preloop-ovprobe
if mount -t overlay overlay -o lowerdir=/tmp:/usr /tmp/.preloop-ovprobe 2>/dev/null; then
  umount /tmp/.preloop-ovprobe 2>/dev/null || true
  DRIVER=
else
  [ -e /dev/fuse ] || DRIVER=vfs
fi
rmdir /tmp/.preloop-ovprobe 2>/dev/null || true
if [ -n "$DRIVER" ]; then
  printf '{"data-root":"@@DOCKER_DATA_ROOT@@","storage-driver":"%s"}\n' "$DRIVER" > /etc/docker/daemon.json
fi
start_dockerd() {
  rm -f /var/run/docker.pid
  dockerd >/var/log/dockerd.log 2>&1 &
  DOCKERD_PID=$!
  ready=0
  for _ in $(seq 1 50); do
    docker info >/dev/null 2>&1 && { ready=1; break; }
    sleep 0.2
  done
  if [ "$ready" -eq 0 ]; then
    kill "$DOCKERD_PID" 2>/dev/null || true
    for _ in $(seq 1 25); do
      kill -0 "$DOCKERD_PID" 2>/dev/null || break
      sleep 0.2
    done
    return 1
  fi
  return 0
}
if start_dockerd; then raise_engine_chain || exit 1; exit 0; fi
rm -rf @@DOCKER_DATA_ROOT@@/*
if start_dockerd; then raise_engine_chain || exit 1; exit 0; fi
echo 'dockerd failed to start after data-root reset' >&2
exit 1
