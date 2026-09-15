#!/usr/bin/env bash
# Does an AgentENV clone (booted from a snapshot) write as fast as a cold
# sandbox? The e2e IO workflow cost ~120 s inside a fork but only ~2.6 s in a
# cold sandbox, so the snapshot's copy-on-write path is the suspect.
set -uo pipefail
W='
set -e
ms() { date +%s%3N; }
for base in /tmp /var/lib/preloop-runner/_work; do
  mkdir -p "$base"
  printf -- "--- %s (%s)
" "$base" "$(stat -fc %T "$base")"
  t0=$(ms); dd if=/dev/zero of=$base/seq bs=1M count=1024 conv=fsync 2>/dev/null; t1=$(ms)
  dd if=$base/seq of=/dev/null bs=1M 2>/dev/null; t2=$(ms)
  rm -rf $base/many $base/many2; mkdir -p $base/many; cd $base/many
  i=1; while [ $i -le 20000 ]; do echo $i > f$i; i=$((i+1)); done; sync; t3=$(ms)
  tar -C $base/many -cf $base/many.tar . && rm -rf $base/many && mkdir -p $base/many2 && tar -C $base/many2 -xf $base/many.tar; t4=$(ms)
  cd /; rm -rf $base/many2 $base/many.tar $base/seq
  printf "write_1g_fsync=%s.%03d read_1g=%s.%03d create_20k=%s.%03d tar_roundtrip=%s.%03d
" \
    $(( (t1-t0)/1000 )) $(( (t1-t0)%1000 )) $(( (t2-t1)/1000 )) $(( (t2-t1)%1000 )) \
    $(( (t3-t2)/1000 )) $(( (t3-t2)%1000 )) $(( (t4-t3)/1000 )) $(( (t4-t3)%1000 ))
done
free -m | sed -n 2p
'

echo "=== AgentENV cold sandbox ==="
ID=$(aenv start --cold docker.io/library/ubuntu:24.04 --cpu 4 --memory 4096 --timeout 1800 -d)
aenv exec "$ID" -- /bin/bash -c "$W"

echo "=== AgentENV clone of a snapshot of that sandbox ==="
SNAP="io-clone-$$"
aenv snapshot create "$ID" --name "$SNAP" >/dev/null
CLONE=$(aenv start "$SNAP" --timeout 1800 -d)
aenv exec "$CLONE" -- /bin/bash -c "$W"

echo "=== SmolVM fork of a forkable golden ==="
smolvm machine create --name io-golden --image ubuntu:24.04 --cpus 4 --mem 4096 --storage 20 --net -- /bin/sh -c 'sleep infinity' >/dev/null 2>&1
smolvm machine start --name io-golden --forkable >/dev/null 2>&1
smolvm machine fork --golden io-golden --name io-clone >/dev/null 2>&1
smolvm machine exec --name io-clone -- /bin/bash -c "$W"

aenv delete "$CLONE" >/dev/null 2>&1; aenv delete "$ID" >/dev/null 2>&1
smolvm machine delete --name io-clone -f >/dev/null 2>&1
smolvm machine stop --name io-golden >/dev/null 2>&1
smolvm machine delete --name io-golden -f >/dev/null 2>&1
