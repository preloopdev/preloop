#!/usr/bin/env bash
# Attribute the IO gap per filesystem, outside preloop, identical resources.
#
# The first run of this experiment was misleading: SmolVM mounts a tmpfs on
# /tmp, so a workflow writing there measures RAM on SmolVM and a real block
# device on AgentENV. Every path is therefore measured explicitly, including
# the one preloop actually gives a job (/var/lib/preloop-runner/_work).
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
echo "=== AgentENV ==="
ID=$(aenv start --cold docker.io/library/ubuntu:24.04 --cpu 4 --memory 4096 --timeout 1800 -d)
aenv exec "$ID" -- /bin/bash -c "$W"
aenv delete "$ID" >/dev/null

echo "=== SmolVM ==="
smolvm machine create --name io-attrib --image ubuntu:24.04 --cpus 4 --mem 4096 --storage 20 --net -- /bin/sh -c 'sleep infinity' >/dev/null 2>&1
smolvm machine start --name io-attrib >/dev/null 2>&1
smolvm machine exec --name io-attrib -- /bin/bash -c "$W"
smolvm machine stop --name io-attrib >/dev/null 2>&1
smolvm machine delete --name io-attrib -f >/dev/null 2>&1
