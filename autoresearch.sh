#!/usr/bin/env bash
# Preloop end-to-end performance benchmark.
#
# Boots an isolated Preloop engine (own port, own PRELOOP_HOME, own SmolVM
# runner pool), runs a fixed offline workflow through it, and compares the
# wall-clock cost against the identical shell work executed on the host.
#
# Primary metric:
#   e2e_ms         median wall clock of `preloop run` with a warm pool
#
# Secondary metrics:
#   e2e_min_ms     best observed `preloop run`
#   host_ms        median wall clock of the host-native equivalent
#   overhead_ms    e2e_ms - host_ms (the gap we are closing)
#   overhead_ratio e2e_ms / host_ms
#   submit_ms      POST /api/v1/runs, i.e. workspace snapshot cost
#   api_total_ms   submit -> terminal status, measured without the CLI
#   job_ms         first-to-last job log timestamp inside the VM
#   dispatch_ms    api_total_ms - submit_ms - job_ms (queue, claim, report)
#   pool_boot_ms   engine start -> every pool slot registered
#   slot_ready_ms  median fork -> runner-registered latency
#
# See benchmarks/preloop-perf/bench.py for the workload definition.

set -euo pipefail

cd "$(dirname "$0")"
exec python3 benchmarks/preloop-perf/bench.py "$@"
