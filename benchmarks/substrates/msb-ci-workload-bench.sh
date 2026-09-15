#!/usr/bin/env bash
# msb-ci-workload-bench.sh — CI-representative filesystem workloads on
# MicroSandbox, mirroring ci-workload-bench.sh.
#
# The in-guest WORKLOAD is byte-identical to the aenv/smolvm harness except
# for BASE: /work/bench (a directory on msb's overlay-backed root) instead
# of /var/lib/preloop-runner/_work/bench. Both are "the job workspace on
# disk-backed storage, not /tmp"; the workload records `stat -fc %T` per run
# so the filesystem is explicit in the samples. msb mounts tmpfs on /tmp
# (like SmolVM), which is why BASE avoids it here too.
#
# Fresh arm: `msb create`. Clone arm: stop (required) + `msb snapshot
# create` + `msb run --from-snapshot`. Unlike a smolvm fork, an msb snapshot
# is a full stopped-disk image, so post-boot writes (the apt git install)
# ARE inherited — PREP still runs in the clone untimed as a no-op guard.
#
# Usage: msb-ci-workload-bench.sh <reps> <out.jsonl>
set -uo pipefail
REPS="${1:-3}"
OUT="${2:-/tmp/msb-ci-workload.jsonl}"
CPUS=4
MEM="4G"
IMAGE="ubuntu:24.04"
: > "$OUT"
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
export PATH="$HOME/.local/bin:$PATH"

# The in-guest workload. Emits `op=seconds` lines; the driver tags them with
# substrate and VM kind. Uses only coreutils, tar and git so it runs on a stock
# ubuntu:24.04 rootfs in any substrate.
read -r -d '' WORKLOAD <<'GUEST'
set -u
BASE=/work/bench
ms() { date +%s%3N; }
emit() { printf '%s=%s.%03d\n' "$1" $(( $2/1000 )) $(( $2%1000 )); }
rm -rf "$BASE"; mkdir -p "$BASE"; cd "$BASE"
echo "fs=$(stat -fc %T .)"

# A source tree shaped like a real repo: nested dirs, many small text files,
# a few large ones. Built untimed so every later op sees identical input.
mkdir -p src
i=0
while [ $i -lt 40 ]; do
  mkdir -p "src/pkg$i"
  j=0
  while [ $j -lt 125 ]; do
    printf 'module pkg%s file %s\nexport const value = %s;\n' "$i" "$j" "$((i*j))" > "src/pkg$i/f$j.ts"
    j=$((j+1))
  done
  i=$((i+1))
done
dd if=/dev/urandom of=src/blob.bin bs=1M count=64 2>/dev/null
FILES=$(find src -type f | wc -l)
echo "tree_files=$FILES"

# 1. bulk create: extract a 5 000-file tree (the `actions/checkout` /
#    node_modules unpack shape).
tar -cf tree.tar src
t=$(ms); mkdir -p unpacked && tar -C unpacked -xf tree.tar; e=$(ms); emit untar_5k $((e-t))

# 2. per-file create through a shell loop.
t=$(ms); mkdir -p loop && i=0; while [ $i -lt 5000 ]; do echo "$i" > "loop/f$i"; i=$((i+1)); done; sync; e=$(ms)
emit loop_create_5k $((e-t))

# 3. per-file create with an fsync each (worst case: package managers and
#    compilers that flush, and git object writes).
t=$(ms); mkdir -p fsynced && i=0; while [ $i -lt 500 ]; do dd if=/dev/zero of="fsynced/f$i" bs=4k count=1 conv=fsync 2>/dev/null; i=$((i+1)); done; e=$(ms)
emit fsync_create_500 $((e-t))

# 4. tree copy (cache restore / workspace duplication).
t=$(ms); cp -a src copied; e=$(ms); emit copy_tree $((e-t))

# 5. metadata walk (build systems stat-ing every source file).
t=$(ms); find src -type f -exec stat -c %s {} + > /dev/null; e=$(ms); emit stat_walk $((e-t))
t=$(ms); du -s src > /dev/null; e=$(ms); emit du_tree $((e-t))

# 6. tree delete (post-job cleanup, `rm -rf node_modules`).
t=$(ms); rm -rf copied; e=$(ms); emit rm_tree $((e-t))

# 7. git: init + add + commit is the most fsync-heavy thing CI does routinely.
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
git init -q repo 2>/dev/null
cp -a src repo/src
cd repo
git config user.email b@l; git config user.name b; git config commit.gpgsign false
t=$(ms); git add -A; e=$(ms); emit git_add $((e-t))
t=$(ms); git commit -qm one; e=$(ms); emit git_commit $((e-t))
t=$(ms); git status --porcelain > /dev/null; e=$(ms); emit git_status_clean $((e-t))
find src -name 'f1.ts' -exec sh -c 'echo "// touched" >> "$1"' _ {} \; 2>/dev/null
t=$(ms); git status --porcelain > /dev/null; e=$(ms); emit git_status_dirty $((e-t))
t=$(ms); git clone -q --no-hardlinks . ../clone; e=$(ms); emit git_clone_local $((e-t))
cd "$BASE"

# 8. archive + compress (artifact upload / cache save shape).
t=$(ms); tar -czf tree.tgz src; e=$(ms); emit targz_create $((e-t))
t=$(ms); mkdir -p gz && tar -C gz -xzf tree.tgz; e=$(ms); emit targz_extract $((e-t))

# 9. bulk streams for reference.
t=$(ms); dd if=/dev/zero of=big bs=1M count=1024 conv=fsync 2>/dev/null; e=$(ms); emit write_1g_fsync $((e-t))
t=$(ms); dd if=big of=/dev/null bs=1M 2>/dev/null; e=$(ms); emit read_1g $((e-t))
t=$(ms); i=0; while [ $i -lt 200 ]; do dd if=/dev/zero of="chunk$i" bs=1M count=1 2>/dev/null; i=$((i+1)); done; sync; e=$(ms)
emit write_200x1m $((e-t))

cd /; rm -rf "$BASE"
echo done
GUEST

emit_samples() { # vmkind rep <<< output
  local vmkind="$1" rep="$2" line op seconds
  while IFS= read -r line; do
    case "$line" in
      *=*)
        op="${line%%=*}"; seconds="${line#*=}"
        case "$op" in fs|tree_files) continue ;; esac
        printf '{"substrate":"%s","vm":"%s","op":"%s","seconds":%s,"rep":%s}\n' \
          "msb" "$vmkind" "$op" "$seconds" "$rep" >> "$OUT"
        ;;
    esac
  done
}

PREP='export DEBIAN_FRONTEND=noninteractive; command -v git >/dev/null || (apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq git >/dev/null 2>&1)'

wait_ready() { # name timeout_secs
  local name="$1" deadline=$(($(date +%s) + $2))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    msb exec "$name" -- /bin/true >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  return 1
}

log "msb: warming image cache"
msb pull "$IMAGE" >/dev/null 2>&1

for rep in $(seq 1 "$REPS"); do
  log "msb rep $rep/$REPS: fresh sandbox"
  m="ciw-$rep-$$" snap="ciw-snap-$rep-$$"
  msb create "$IMAGE" -n "$m" -c $CPUS -m $MEM >/dev/null 2>&1 || continue
  wait_ready "$m" 180 || { msb remove "$m" >/dev/null 2>&1; continue; }
  msb exec "$m" -- /bin/sh -c "$PREP" >/dev/null 2>&1
  msb exec "$m" -- /bin/sh -c "$WORKLOAD" 2>/dev/null | emit_samples cold "$rep"

  log "msb rep $rep/$REPS: clone from snapshot"
  if msb stop "$m" >/dev/null 2>&1 && msb snapshot create --from "$m" "$snap" >/dev/null 2>&1; then
    c="ciwc-$rep-$$"
    if msb run --from-snapshot "$snap" -n "$c" -c $CPUS -m $MEM -d -- /bin/sh -c 'sleep infinity' >/dev/null 2>&1 && wait_ready "$c" 120; then
      msb exec "$c" -- /bin/sh -c "$PREP" >/dev/null 2>&1
      msb exec "$c" -- /bin/sh -c "$WORKLOAD" 2>/dev/null | emit_samples clone "$rep"
      msb stop "$c" >/dev/null 2>&1; msb remove "$c" >/dev/null 2>&1
    fi
    msb snapshot remove "$snap" >/dev/null 2>&1
  fi
  msb stop "$m" >/dev/null 2>&1; msb remove "$m" >/dev/null 2>&1
done
log "complete: $OUT ($(wc -l < "$OUT") samples)"
