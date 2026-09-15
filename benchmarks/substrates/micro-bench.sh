#!/usr/bin/env bash
# micro-bench.sh — substrate primitive comparison: AgentENV (aenv) vs SmolVM.
#
# Measures the VM operations preloop's runner pool actually performs, with
# identical guest resources on both substrates and the image pre-pulled on
# both, so no run pays a first-touch registry cost. Every timing is wall clock
# around a single CLI invocation, repeated N times; raw samples are emitted as
# JSON lines for later analysis.
#
# Usage: micro-bench.sh <reps> <out.jsonl>
set -uo pipefail
REPS="${1:-10}"
OUT="${2:-/tmp/micro-bench.jsonl}"
CPUS=4
MEM=4096
IMAGE_AENV="docker.io/library/ubuntu:24.04"
IMAGE_SMOL="ubuntu:24.04"
: > "$OUT"

now() { date +%s.%N; }
emit() { # substrate op seconds ok extra
  printf '{"substrate":"%s","op":"%s","seconds":%s,"ok":%s,"rep":%s%s}\n' \
    "$1" "$2" "$3" "$4" "$5" "${6:-}" >> "$OUT"
}
dur() { echo "$2-$1" | bc; }

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# ── AgentENV ────────────────────────────────────────────────────────────────
aenv_bench() {
  log "aenv: warming image cache"
  local warm; warm=$(aenv start --cold "$IMAGE_AENV" --cpu $CPUS --memory $MEM --timeout 300 -d 2>/dev/null)
  [ -n "$warm" ] && aenv delete "$warm" >/dev/null 2>&1

  for r in $(seq 1 "$REPS"); do
    log "aenv rep $r/$REPS"
    # cold boot from OCI image
    local s e id
    s=$(now); id=$(aenv start --cold "$IMAGE_AENV" --cpu $CPUS --memory $MEM --timeout 1800 -d 2>/dev/null); e=$(now)
    if [ -z "$id" ]; then emit aenv cold_boot 0 false "$r"; continue; fi
    emit aenv cold_boot "$(dur "$s" "$e")" true "$r"

    # first exec after boot (guest agent readiness included)
    s=$(now); aenv exec "$id" -- /bin/true >/dev/null 2>&1; local rc=$?; e=$(now)
    emit aenv first_exec "$(dur "$s" "$e")" "$([ $rc = 0 ] && echo true || echo false)" "$r"

    # steady-state exec round trip
    local t0 t1
    t0=$(now); for i in $(seq 1 10); do aenv exec "$id" -- /bin/true >/dev/null 2>&1; done; t1=$(now)
    emit aenv exec_x10 "$(dur "$t0" "$t1")" true "$r"

    # snapshot (fork base preparation)
    local snap="mb-$r-$$"
    s=$(now); aenv snapshot create "$id" --name "$snap" >/dev/null 2>&1; rc=$?; e=$(now)
    emit aenv snapshot "$(dur "$s" "$e")" "$([ $rc = 0 ] && echo true || echo false)" "$r"

    # fork: start a clone from the snapshot, then first exec in the clone
    local c
    s=$(now); c=$(aenv start "$snap" --timeout 1800 -d 2>/dev/null); e=$(now)
    emit aenv fork "$(dur "$s" "$e")" "$([ -n "$c" ] && echo true || echo false)" "$r"
    if [ -n "$c" ]; then
      s=$(now); aenv exec "$c" -- /bin/true >/dev/null 2>&1; rc=$?; e=$(now)
      emit aenv fork_first_exec "$(dur "$s" "$e")" "$([ $rc = 0 ] && echo true || echo false)" "$r"
      # 8 concurrent forks: what a pool refill looks like
      s=$(now)
      local kids=()
      for i in $(seq 1 8); do aenv start "$snap" --timeout 900 -d > "/tmp/mbfork.$i" 2>/dev/null & kids+=($!); done
      for p in "${kids[@]}"; do wait "$p"; done
      e=$(now)
      emit aenv fork_x8_concurrent "$(dur "$s" "$e")" true "$r"
      for i in $(seq 1 8); do
        local f; f=$(cat "/tmp/mbfork.$i" 2>/dev/null | tr -d '[:space:]')
        [ -n "$f" ] && aenv delete "$f" >/dev/null 2>&1
        rm -f "/tmp/mbfork.$i"
      done
      s=$(now); aenv delete "$c" >/dev/null 2>&1; e=$(now)
      emit aenv delete_clone "$(dur "$s" "$e")" true "$r"
    fi

    # pause / resume
    s=$(now); aenv pause "$id" >/dev/null 2>&1; e=$(now)
    emit aenv pause "$(dur "$s" "$e")" true "$r"
    s=$(now); aenv resume "$id" --timeout 3600 >/dev/null 2>&1; e=$(now)
    emit aenv resume "$(dur "$s" "$e")" true "$r"
    s=$(now); aenv exec "$id" -- /bin/true >/dev/null 2>&1; e=$(now)
    emit aenv exec_after_resume "$(dur "$s" "$e")" true "$r"

    # guest work: CPU, disk write, tarball unpack (proxy for checkout/build IO)
    s=$(now); aenv exec "$id" -- /bin/sh -c 'i=0; while [ $i -lt 3000000 ]; do i=$((i+1)); done' >/dev/null 2>&1; e=$(now)
    emit aenv guest_cpu_loop "$(dur "$s" "$e")" true "$r"
    s=$(now); aenv exec "$id" -- /bin/sh -c 'dd if=/dev/zero of=/tmp/blk bs=1M count=512 conv=fsync 2>&1 | tail -1' >/dev/null 2>&1; e=$(now)
    emit aenv guest_dd_512m "$(dur "$s" "$e")" true "$r"
    s=$(now); aenv exec "$id" -- /bin/sh -c 'mkdir -p /tmp/many && cd /tmp/many && for i in $(seq 1 2000); do echo x > f$i; done && sync' >/dev/null 2>&1; e=$(now)
    emit aenv guest_2000_files "$(dur "$s" "$e")" true "$r"

    s=$(now); aenv delete "$id" >/dev/null 2>&1; e=$(now)
    emit aenv delete "$(dur "$s" "$e")" true "$r"
  done
}

# ── SmolVM ──────────────────────────────────────────────────────────────────
smol_bench() {
  log "smolvm: warming image cache"
  smolvm machine create --name mb-warm --image "$IMAGE_SMOL" --cpus $CPUS --mem $MEM --storage 20 --net \
    -- /bin/sh -c 'sleep infinity' >/dev/null 2>&1
  smolvm machine start --name mb-warm >/dev/null 2>&1
  smolvm machine exec --name mb-warm -- /bin/true >/dev/null 2>&1
  smolvm machine stop --name mb-warm >/dev/null 2>&1
  smolvm machine delete --name mb-warm -f >/dev/null 2>&1

  for r in $(seq 1 "$REPS"); do
    log "smolvm rep $r/$REPS"
    local m="mb-$r-$$" s e rc
    # create + start = the comparable "cold boot"
    s=$(now)
    smolvm machine create --name "$m" --image "$IMAGE_SMOL" --cpus $CPUS --mem $MEM --storage 20 --net \
      -- /bin/sh -c 'sleep infinity' >/dev/null 2>&1 && \
      smolvm machine start --name "$m" >/dev/null 2>&1
    rc=$?; e=$(now)
    if [ $rc != 0 ]; then emit smolvm cold_boot 0 false "$r"; smolvm machine delete --name "$m" -f >/dev/null 2>&1; continue; fi
    emit smolvm cold_boot "$(dur "$s" "$e")" true "$r"

    s=$(now); smolvm machine exec --name "$m" -- /bin/true >/dev/null 2>&1; rc=$?; e=$(now)
    emit smolvm first_exec "$(dur "$s" "$e")" "$([ $rc = 0 ] && echo true || echo false)" "$r"

    local t0 t1
    t0=$(now); for i in $(seq 1 10); do smolvm machine exec --name "$m" -- /bin/true >/dev/null 2>&1; done; t1=$(now)
    emit smolvm exec_x10 "$(dur "$t0" "$t1")" true "$r"

    # fork base: smolvm needs the machine restarted in forkable mode
    s=$(now)
    smolvm machine stop --name "$m" >/dev/null 2>&1
    smolvm machine start --name "$m" --forkable >/dev/null 2>&1
    rc=$?; e=$(now)
    emit smolvm snapshot "$(dur "$s" "$e")" "$([ $rc = 0 ] && echo true || echo false)" "$r"

    local c="mbc-$r-$$"
    s=$(now); smolvm machine fork --golden "$m" --name "$c" >/dev/null 2>&1; rc=$?; e=$(now)
    emit smolvm fork "$(dur "$s" "$e")" "$([ $rc = 0 ] && echo true || echo false)" "$r"
    if [ $rc = 0 ]; then
      s=$(now); smolvm machine exec --name "$c" -- /bin/true >/dev/null 2>&1; rc=$?; e=$(now)
      emit smolvm fork_first_exec "$(dur "$s" "$e")" "$([ $rc = 0 ] && echo true || echo false)" "$r"
      # 8 concurrent forks from the same golden
      s=$(now)
      local kids=()
      for i in $(seq 1 8); do smolvm machine fork --golden "$m" --name "mbf$i-$r-$$" >/dev/null 2>&1 & kids+=($!); done
      for p in "${kids[@]}"; do wait "$p"; done
      e=$(now)
      emit smolvm fork_x8_concurrent "$(dur "$s" "$e")" true "$r"
      for i in $(seq 1 8); do smolvm machine delete --name "mbf$i-$r-$$" -f >/dev/null 2>&1; done
      s=$(now); smolvm machine delete --name "$c" -f >/dev/null 2>&1; e=$(now)
      emit smolvm delete_clone "$(dur "$s" "$e")" true "$r"
    fi

    # pause/resume equivalent: stop + start (smolvm has no live pause)
    s=$(now); smolvm machine stop --name "$m" >/dev/null 2>&1; e=$(now)
    emit smolvm pause "$(dur "$s" "$e")" true "$r"
    s=$(now); smolvm machine start --name "$m" >/dev/null 2>&1; e=$(now)
    emit smolvm resume "$(dur "$s" "$e")" true "$r"
    s=$(now); smolvm machine exec --name "$m" -- /bin/true >/dev/null 2>&1; e=$(now)
    emit smolvm exec_after_resume "$(dur "$s" "$e")" true "$r"

    s=$(now); smolvm machine exec --name "$m" -- /bin/sh -c 'i=0; while [ $i -lt 3000000 ]; do i=$((i+1)); done' >/dev/null 2>&1; e=$(now)
    emit smolvm guest_cpu_loop "$(dur "$s" "$e")" true "$r"
    s=$(now); smolvm machine exec --name "$m" -- /bin/sh -c 'dd if=/dev/zero of=/tmp/blk bs=1M count=512 conv=fsync 2>&1 | tail -1' >/dev/null 2>&1; e=$(now)
    emit smolvm guest_dd_512m "$(dur "$s" "$e")" true "$r"
    s=$(now); smolvm machine exec --name "$m" -- /bin/sh -c 'mkdir -p /tmp/many && cd /tmp/many && for i in $(seq 1 2000); do echo x > f$i; done && sync' >/dev/null 2>&1; e=$(now)
    emit smolvm guest_2000_files "$(dur "$s" "$e")" true "$r"

    s=$(now); smolvm machine delete --name "$m" -f >/dev/null 2>&1; e=$(now)
    emit smolvm delete "$(dur "$s" "$e")" true "$r"
  done
}

case "${3:-both}" in
  aenv) aenv_bench ;;
  smolvm) smol_bench ;;
  *) aenv_bench; smol_bench ;;
esac
log "micro-bench complete: $OUT ($(wc -l < "$OUT") samples)"
