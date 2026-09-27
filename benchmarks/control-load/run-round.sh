#!/usr/bin/env bash
# One load/chaos round against a fresh Postgres database.
#
# Required: PG_ADMIN_URL (server admin URL, e.g. the smolvm Postgres)
# Tunables (env): LABEL NODES RUNS_PER_SEC DURATION DRAIN RUNNERS BURSTS
#   WEBHOOK_FRACTION JOB_MEDIAN_MS FAULTS (proxy faults, space separated)
#   KILL_NODE_AT (seconds; kill -9 node 0 then restart it 10s later)
#   PROFILE (release|debug)
set -euo pipefail

: "${PG_ADMIN_URL:?set PG_ADMIN_URL}"
LABEL=${LABEL:-round}
NODES=${NODES:-2}
RUNS_PER_SEC=${RUNS_PER_SEC:-5}
DURATION=${DURATION:-120}
DRAIN=${DRAIN:-300}
RUNNERS=${RUNNERS:-200}
BURSTS=${BURSTS:-}
WEBHOOK_FRACTION=${WEBHOOK_FRACTION:-0.5}
JOB_MEDIAN_MS=${JOB_MEDIAN_MS:-4000}
FAULTS=${FAULTS:-}
KILL_NODE_AT=${KILL_NODE_AT:-}
PROFILE=${PROFILE:-release}
BASE_PORT=${BASE_PORT:-18080}

root=$(cd "$(dirname "$0")/../.." && pwd)
out="$root/load-results/$LABEL"
work=$(mktemp -d /tmp/preloop-load.XXXX)
mkdir -p "$out"
psql=${PSQL:-psql}

cargo build --manifest-path "$root/Cargo.toml" --profile "$PROFILE" \
  -p preloop-runner-server --bin preloop-server >&2
cargo build --manifest-path "$root/Cargo.toml" --profile "$PROFILE" -p preloop-control-load >&2
bindir="$root/target/$PROFILE"
[ "$PROFILE" = dev ] && bindir="$root/target/debug"

db="preloop_load_$(date +%s)"
"$psql" "$PG_ADMIN_URL" -qc "CREATE DATABASE $db"
db_url="${PG_ADMIN_URL%/*}/$db"
engine_db_url="$db_url"

pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

if [ -n "$FAULTS" ]; then
  upstream=$(echo "$PG_ADMIN_URL" | sed -E 's#.*@([^/]+)/.*#\1#')
  proxy_args=()
  for f in $FAULTS; do proxy_args+=(--fault "$f"); done
  "$bindir/preloop-control-load" proxy --listen 127.0.0.1:55499 --upstream "$upstream" \
    ${proxy_args[@]+"${proxy_args[@]}"} >"$out/proxy.log" 2>&1 &
  pids+=($!)
  engine_db_url=$(echo "$db_url" | sed -E 's#@[^/]+/#@127.0.0.1:55499/#')
  sleep 1
fi

sha=$("$bindir/preloop-control-load" prepare-workspace --dir "$work/ws")
# Never read the operator's real ~/.preloop/config.toml (GitHub App, secrets).
: >"$work/config.toml"
# One cluster key for every node (production: from the secret manager).
cluster_key=$(openssl rand -hex 32)

start_node() {
  local i=$1
  env $(env | grep -o '^PRELOOP_GITHUB[A-Z_]*' | sed 's/^/-u /') \
  PRELOOP_CONFIG="$work/config.toml" \
  PRELOOP_HMAC_KEY="$cluster_key" \
  PRELOOP_STORE_URL="$engine_db_url" \
  PRELOOP_SYSTEM_TOKEN=preloop-system-token \
  PRELOOP_WEBHOOK_SECRET=load-webhook-secret \
  PRELOOP_LOCAL_WORKSPACE="$work/ws" \
  PRELOOP_CREDENTIAL_STORE=file \
  PRELOOP_RUNNER_URL="http://127.0.0.1:$((BASE_PORT + i))" \
  RUST_LOG=${RUST_LOG:-warn} \
    "$bindir/preloop-server" serve --listen "127.0.0.1:$((BASE_PORT + i))" \
      --state-dir "$work/node$i" >>"$out/node$i.log" 2>&1 &
  echo $!
}

servers=()
node_pids=()
for i in $(seq 0 $((NODES - 1))); do
  node_pids[i]=$(start_node "$i")
  pids+=("${node_pids[i]}")
  servers+=("http://127.0.0.1:$((BASE_PORT + i))")
done
for s in "${servers[@]}"; do
  for _ in $(seq 1 60); do curl -fsS "$s/readyz" >/dev/null 2>&1 && break; sleep 1; done
done

if [ -n "$KILL_NODE_AT" ]; then
  (
    sleep "$KILL_NODE_AT"
    echo "[chaos] kill -9 node0 at ${KILL_NODE_AT}s" >&2
    kill -9 "${node_pids[0]}" 2>/dev/null || true
    sleep 10
    echo "[chaos] restarting node0" >&2
    start_node 0 >/dev/null
  ) &
  pids+=($!)
fi

burst_args=()
for b in $BURSTS; do burst_args+=(--burst "$b"); done
servers_csv=$(IFS=,; echo "${servers[*]}")
"$bindir/preloop-control-load" run \
  --servers "$servers_csv" --label "$LABEL" --out "$root/load-results" \
  --runs-per-sec "$RUNS_PER_SEC" --duration-secs "$DURATION" --drain-secs "$DRAIN" \
  --runners "$RUNNERS" --webhook-sha "$sha" --webhook-fraction "$WEBHOOK_FRACTION" \
  --job-median-ms "$JOB_MEDIAN_MS" --pg-url "$db_url" ${burst_args[@]+"${burst_args[@]}"} \
  2>"$out/harness.log" | tee "$out/stdout.txt"

# Per-node transaction phase timing, captured before the nodes stop.
for i in "${!servers[@]}"; do
  curl -fsS -H "Authorization: Bearer preloop-system-token" \
    "${servers[i]}/api/v1/debug/txn-stats" >"$out/txn-stats-node$i.json" 2>/dev/null || true
done

# Post-round integrity checks straight from the database.
"$psql" "$db_url" -At >"$out/integrity.txt" <<'SQL'
SET search_path TO control;
SELECT 'runs', count(*) FROM runs;
SELECT 'jobs_by_status', status, count(*) FROM jobs GROUP BY status ORDER BY 2;
SELECT 'inflight_requests', count(*) FROM job_requests WHERE result IS NULL;
SELECT 'duplicate_inflight', count(*) FROM (SELECT run_id, job_id FROM job_requests WHERE result IS NULL GROUP BY 1,2 HAVING count(*) > 1) d;
SELECT 'duplicate_runners', count(*) FROM (SELECT name FROM runners GROUP BY name HAVING count(*) > 1) d;
SELECT 'webhook_states', state, count(*) FROM webhook_deliveries GROUP BY state;
SELECT 'db_size_mb', pg_database_size(current_database()) / 1048576;
SQL
cat "$out/integrity.txt"
echo "results: $out"
