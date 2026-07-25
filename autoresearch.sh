#!/usr/bin/env bash
# Preloop end-to-end performance benchmark.
#
# Boots an isolated Preloop engine (own port, own PRELOOP_HOME, own SmolVM
# runner pool), runs a fixed offline workflow through it, and compares the
# wall-clock cost against the identical shell work executed on the host.
#
# The primary workload is a 4-shard matrix: independent jobs are how real CI
# workflows are shaped, and a fixed-size runner pool is where preloop stops
# keeping up with the host.
#
# Primary metric:
#   e2e_ms          trimmed-mean wall clock of `preloop run` on the matrix
#
# Secondary metrics:
#   e2e_min_ms      best observed matrix run
#   host_ms         host-native equivalent, shards run concurrently
#   overhead_ms     e2e_ms - host_ms (the gap we are closing)
#   overhead_ratio  e2e_ms / host_ms
#   single_ms       same work as one job, so the one-job path cannot regress
#   host_single_ms  host-native single shard
#   single_ratio    single_ms / host_single_ms
#   submit_ms       POST /api/v1/runs, i.e. workspace snapshot cost
#   api_total_ms    submit -> terminal status, measured without the CLI
#   job_ms          first-to-last job log timestamp across all shards
#   dispatch_ms     api_total_ms - submit_ms - job_ms (queue, claim, report)
#   pool_boot_ms    engine start -> warm pool settled
#   warm_runners    how many runners the engine chose to keep warm
#   replenish_wait_ms  wait for the pool to refill between measured runs
#
# See benchmarks/preloop-perf/bench.py for the workload definition.

set -euo pipefail

cd "$(dirname "$0")"
exec python3 benchmarks/preloop-perf/bench.py "$@"
