#!/usr/bin/env bash
# ci-workload-bench.sh — CI-representative filesystem workloads, per substrate,
# measured inside the guest with preloop taken out of the picture.
#
# Why this exists: the primitive micro-benchmark disagreed with itself on small
# writes (a shell loop creating 20 000 files favoured SmolVM 3×, while `tar`
# extracting the same 20 000 files favoured AgentENV 8×). Neither is a CI
# workload. These are: git checkout, git add/commit/status, tree copy, tree
# delete, metadata walks, archive handling, and per-file fsync — the operations
# `actions/checkout`, `npm ci`, `cargo build`, and cache/artifact steps actually
# perform.
#
# Fairness rules:
#   * identical guest resources (4 vCPU / 4 GiB) and the same base image;
#   * every workload runs under $BASE, which the caller points at the path
#     preloop gives a job — NOT /tmp, which is tmpfs on SmolVM and a block
#     device on AgentENV (measuring RAM against a disk);
#   * each substrate is measured twice: in a fresh VM, and in a clone/fork,
#     because a job VM is always a clone and copy-on-write is not free;
#   * `git` is installed before timing starts, from the same mirror;
#   * one substrate at a time, host otherwise idle.
#
# Usage: ci-workload-bench.sh <reps> <out.jsonl> [aenv|smolvm|both]
set -uo pipefail
REPS="${1:-3}"
OUT="${2:-/tmp/ci-workload.jsonl}"
WHICH="${3:-both}"
CPUS=4
MEM=4096
: > "$OUT"
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# The in-guest workload. Emits `op=seconds` lines; the driver tags them with
# substrate and VM kind. Uses only coreutils, tar and git so it runs on a stock
# ubuntu:24.04 rootfs in either substrate.
read -r -d '' WORKLOAD <<'GUEST'
set -u
BASE=/var/lib/preloop-runner/_work/bench
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

# 2. per-file create through a shell loop (the shape that favoured SmolVM).
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

emit_samples() { # substrate vmkind rep <<< output
  local substrate="$1" vmkind="$2" rep="$3" line op seconds
  while IFS= read -r line; do
    case "$line" in
      *=*)
        op="${line%%=*}"; seconds="${line#*=}"
        case "$op" in fs|tree_files) continue ;; esac
        printf '{"substrate":"%s","vm":"%s","op":"%s","seconds":%s,"rep":%s}\n' \
          "$substrate" "$vmkind" "$op" "$seconds" "$rep" >> "$OUT"
        ;;
    esac
  done
}

PREP='export DEBIAN_FRONTEND=noninteractive; command -v git >/dev/null || (apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq git >/dev/null 2>&1); mkdir -p /var/lib/preloop-runner/_work'

run_aenv() {
  for rep in $(seq 1 "$REPS"); do
    log "aenv rep $rep/$REPS: cold sandbox"
    local id snap clone
    id=$(aenv start --cold docker.io/library/ubuntu:24.04 --cpu $CPUS --memory $MEM --timeout 3600 -d) || continue
    aenv exec "$id" -- /bin/sh -c "$PREP" >/dev/null 2>&1
    aenv exec "$id" -- /bin/sh -c "$WORKLOAD" 2>/dev/null | emit_samples aenv cold "$rep"

    log "aenv rep $rep/$REPS: clone of a snapshot"
    snap="ciw-$rep-$$"
    aenv snapshot create "$id" --name "$snap" >/dev/null 2>&1
    clone=$(aenv start "$snap" --timeout 3600 -d)
    if [ -n "$clone" ]; then
      aenv exec "$clone" -- /bin/sh -c "$WORKLOAD" 2>/dev/null | emit_samples aenv clone "$rep"
      aenv delete "$clone" >/dev/null 2>&1
    fi
    aenv delete "$id" >/dev/null 2>&1
  done
}

run_smolvm() {
  for rep in $(seq 1 "$REPS"); do
    log "smolvm rep $rep/$REPS: fresh machine"
    local m="ciw-$rep-$$" c="ciwc-$rep-$$"
    smolvm machine create --name "$m" --image ubuntu:24.04 --cpus $CPUS --mem $MEM --storage 20 --net \
      -- /bin/sh -c 'sleep infinity' >/dev/null 2>&1
    smolvm machine start --name "$m" >/dev/null 2>&1 || continue
    smolvm machine exec --name "$m" -- /bin/sh -c "$PREP" >/dev/null 2>&1
    smolvm machine exec --name "$m" -- /bin/sh -c "$WORKLOAD" 2>/dev/null | emit_samples smolvm cold "$rep"

    log "smolvm rep $rep/$REPS: fork of a forkable golden"
    smolvm machine stop --name "$m" >/dev/null 2>&1
    smolvm machine start --name "$m" --forkable >/dev/null 2>&1
    if smolvm machine fork --golden "$m" --name "$c" >/dev/null 2>&1; then
      # A SmolVM fork does not inherit post-boot writes, so git must be
      # reinstalled in the clone — untimed, exactly as the pool does it.
      smolvm machine exec --name "$c" -- /bin/sh -c "$PREP" >/dev/null 2>&1
      smolvm machine exec --name "$c" -- /bin/sh -c "$WORKLOAD" 2>/dev/null | emit_samples smolvm clone "$rep"
      smolvm machine delete --name "$c" -f >/dev/null 2>&1
    fi
    smolvm machine stop --name "$m" >/dev/null 2>&1
    smolvm machine delete --name "$m" -f >/dev/null 2>&1
  done
}

case "$WHICH" in
  aenv) run_aenv ;;
  smolvm) run_smolvm ;;
  *) run_aenv; run_smolvm ;;
esac
log "complete: $OUT ($(wc -l < "$OUT") samples)"
