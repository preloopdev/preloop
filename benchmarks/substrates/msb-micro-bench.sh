#!/usr/bin/env bash
# msb-micro-bench.sh — substrate primitives for MicroSandbox (msb), mirroring
# micro-bench.sh (AgentENV vs SmolVM) op-for-op where the CLI allows it.
#
# Mapping notes (all verified against msb v0.6.18 on cpane):
# - cold_boot: `msb create` boots in the background, so cold_boot = create +
#   poll-exec until the guest answers (bounded). first_exec is then a single
#   warm exec, matching the other substrates' semantics.
# - snapshot (fork-base prep): `msb snapshot create` requires a STOPPED
#   sandbox, so snapshot = stop + snapshot create. Compare against smolvm's
#   stop + start --forkable, not against aenv's live snapshot.
# - fork: `msb run --from-snapshot <snap> -d` (returns fast; readiness lands
#   in fork_first_exec, same split as the aenv harness).
# - pause/resume: msb has NO live pause; measured as stop/start and labeled
#   as such in analysis. Same mapping the smolvm harness uses.
# - Filesystem fairness: msb mounts tmpfs on /tmp (like SmolVM) and an
#   overlay root. Disk ops run on BOTH /tmp and /disk (overlay-backed) with
#   separate op names; the /tmp numbers compare against smolvm /tmp, the
#   /disk numbers against aenv's ext4-backed /tmp.
#
# Usage: msb-micro-bench.sh <reps> <out.jsonl>
set -uo pipefail

REPS="${1:-10}"
OUT="${2:-/tmp/msb-micro-bench.jsonl}"
CPUS=4
MEM="4G"
IMAGE="ubuntu:24.04"
: > "$OUT"

now() { date +%s.%N; }
emit() { # op seconds ok rep
  printf '{"substrate":"%s","op":"%s","seconds":%s,"ok":%s,"rep":%s%s}\n' \
    "msb" "$1" "$2" "$3" "$4" "${5:-}" >> "$OUT"
}
dur() { echo "$2-$1" | bc; }
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
export PATH="$HOME/.local/bin:$PATH"

log "msb: warming image cache"
msb pull "$IMAGE" >/dev/null 2>&1
msb create "$IMAGE" -n msb-warm -c $CPUS -m $MEM >/dev/null 2>&1
for i in $(seq 1 60); do msb exec msb-warm -- /bin/true >/dev/null 2>&1 && break; sleep 1; done
msb stop msb-warm >/dev/null 2>&1
msb remove msb-warm >/dev/null 2>&1

wait_ready() { # name timeout_secs
  local name="$1" deadline=$(($(date +%s) + $2))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    msb exec "$name" -- /bin/true >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  return 1
}

for r in $(seq 1 "$REPS"); do
  log "msb rep $r/$REPS"
  m="msb-$r-$$"
  # cold boot: create + time-to-ready-guest
  s=$(now)
  msb create "$IMAGE" -n "$m" -c $CPUS -m $MEM >/dev/null 2>&1 && wait_ready "$m" 180
  rc=$?; e=$(now)
  if [ $rc != 0 ]; then emit cold_boot 0 false "$r"; msb remove "$m" >/dev/null 2>&1; continue; fi
  emit cold_boot "$(dur "$s" "$e")" true "$r"

  s=$(now); msb exec "$m" -- /bin/true >/dev/null 2>&1; rc=$?; e=$(now)
  if [ $rc = 0 ]; then emit first_exec "$(dur "$s" "$e")" true "$r"; else emit first_exec 0 false "$r"; fi

  t0=$(now); for i in $(seq 1 10); do msb exec "$m" -- /bin/true >/dev/null 2>&1; done; t1=$(now)
  emit exec_x10 "$(dur "$t0" "$t1")" true "$r"

  # fork-base prep: stop (required) + snapshot
  snap="msb-snap-$r-$$"
  s=$(now)
  msb stop "$m" >/dev/null 2>&1 && msb snapshot create --from "$m" "$snap" >/dev/null 2>&1
  rc=$?; e=$(now)
  if [ $rc = 0 ]; then emit snapshot "$(dur "$s" "$e")" true "$r"; else emit snapshot 0 false "$r"; fi

  # fork one clone from the snapshot
  c="msbc-$r-$$"
  s=$(now); msb run --from-snapshot "$snap" -n "$c" -c $CPUS -m $MEM -d -- /bin/sh -c 'sleep infinity' >/dev/null 2>&1; rc=$?; e=$(now)
  if [ $rc = 0 ]; then emit fork "$(dur "$s" "$e")" true "$r"; else emit fork 0 false "$r"; fi
  if [ $rc = 0 ]; then
    s=$(now); wait_ready "$c" 120; rc=$?; e=$(now)
    if [ $rc = 0 ]; then emit fork_first_exec "$(dur "$s" "$e")" true "$r"; else emit fork_first_exec 0 false "$r"; fi
    # 8 concurrent forks from the same snapshot
    s=$(now)
    kids=()
    for i in $(seq 1 8); do msb run --from-snapshot "$snap" -n "msbf$i-$r-$$" -c $CPUS -m $MEM -d -- /bin/sh -c 'sleep infinity' >/dev/null 2>&1 & kids+=($!); done
    for p in "${kids[@]}"; do wait "$p"; done
    e=$(now)
    emit fork_x8_concurrent "$(dur "$s" "$e")" true "$r"
    for i in $(seq 1 8); do msb stop "msbf$i-$r-$$" >/dev/null 2>&1; msb remove "msbf$i-$r-$$" >/dev/null 2>&1; done
    s=$(now); msb stop "$c" >/dev/null 2>&1; msb remove "$c" >/dev/null 2>&1; e=$(now)
    emit delete_clone "$(dur "$s" "$e")" true "$r"
  fi
  msb snapshot remove "$snap" >/dev/null 2>&1

  # stop/start in place of pause/resume (no live pause on msb). $m is stopped
  # after the snapshot above, so start it untimed before timing the stop.
  msb start "$m" >/dev/null 2>&1 && wait_ready "$m" 120
  s=$(now); msb stop "$m" >/dev/null 2>&1; e=$(now)
  emit pause "$(dur "$s" "$e")" true "$r"
  s=$(now); msb start "$m" >/dev/null 2>&1 && wait_ready "$m" 120; rc=$?; e=$(now)
  if [ $rc = 0 ]; then emit resume "$(dur "$s" "$e")" true "$r"; else emit resume 0 false "$r"; fi
  s=$(now); msb exec "$m" -- /bin/true >/dev/null 2>&1; e=$(now)
  emit exec_after_resume "$(dur "$s" "$e")" true "$r"

  s=$(now); msb exec "$m" -- /bin/sh -c 'i=0; while [ $i -lt 3000000 ]; do i=$((i+1)); done' >/dev/null 2>&1; e=$(now)
  emit guest_cpu_loop "$(dur "$s" "$e")" true "$r"
  s=$(now); msb exec "$m" -- /bin/sh -c 'dd if=/dev/zero of=/tmp/blk bs=1M count=512 conv=fsync 2>&1 | tail -1' >/dev/null 2>&1; e=$(now)
  emit guest_dd_512m "$(dur "$s" "$e")" true "$r"
  s=$(now); msb exec "$m" -- /bin/sh -c 'mkdir -p /tmp/many && cd /tmp/many && for i in $(seq 1 2000); do echo x > f$i; done && sync' >/dev/null 2>&1; e=$(now)
  emit guest_2000_files "$(dur "$s" "$e")" true "$r"
  # same ops on the overlay-backed root (disk, not tmpfs)
  s=$(now); msb exec "$m" -- /bin/sh -c 'mkdir -p /disk && dd if=/dev/zero of=/disk/blk bs=1M count=512 conv=fsync 2>&1 | tail -1' >/dev/null 2>&1; e=$(now)
  emit guest_dd_512m_disk "$(dur "$s" "$e")" true "$r"
  s=$(now); msb exec "$m" -- /bin/sh -c 'mkdir -p /disk/many && cd /disk/many && for i in $(seq 1 2000); do echo x > f$i; done && sync' >/dev/null 2>&1; e=$(now)
  emit guest_2000_files_disk "$(dur "$s" "$e")" true "$r"

  s=$(now); msb stop "$m" >/dev/null 2>&1; msb remove "$m" >/dev/null 2>&1; e=$(now)
  emit delete "$(dur "$s" "$e")" true "$r"
done
log "msb-micro-bench complete: $OUT ($(wc -l < "$OUT") samples)"
