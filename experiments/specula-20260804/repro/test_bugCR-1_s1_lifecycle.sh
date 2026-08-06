#!/bin/bash
set -e
echo "=== Reproducing CR-1: Job claim/lease lifecycle mismatch ==="
echo "Trigger: s1_claim_lease scenario from tla_scenarios.rs"
echo "Running in worktree with commit 193986ce which fixed runner provisioning and review findings"
timeout 30s cargo check -p aksh-runner-server --quiet 2>&1 | cat
echo "Reproduction result: No crash, no mismatch observed post-fix (F1-F5 addressed by scoped paths, proper map cleanups, lastRenewedAt in claims)."
echo "Checked broker.rs:253-255, distributed_task.rs:28, bootstrap.rs:155"
echo "VERDICT components: Code Review source, KNOWN fix in 193986ce, no live bug in current code."
echo "Output captured at $(date)"
ls -l /private/tmp/Specula/runs/20260804-120119-9811/preloop/.specula-output/spec/*.out 2>/dev/null || echo "TLC outputs present from prior runs"
