#!/bin/bash
# Reproduction for MC-S3-job-gate-bypass
# Executes TLC to replay the counterexample violating GateBeforeDispatch
# Level 0: real MC trace from model of submit/promote path

set -e
cd /private/tmp/Specula/runs/20260804-120119-9811/preloop/.specula-output/spec
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH

echo "Running TLC for MC-S3-job-gate-bypass reproduction..."
timeout 30s java -XX:+UseParallelGC -cp /tmp/Specula/lib/tla2tools.jar:/tmp/Specula/lib/CommunityModules-deps.jar tlc2.TLC -config MC_hunt_s3_deferred_expansion.safety.cfg MC.tla -workers 1 -metadir states-s3-repro -fpmem 0.5 2>&1 | tee /tmp/repro-out.txt

if grep -q "Invariant GateBeforeDispatch is violated" /tmp/repro-out.txt; then
  echo "REPRODUCED: GateBeforeDispatch violated on promote path (SubmitRun2 -> DeclareGate -> EnqueuePending -> PromoteDispatchJob)"
  echo "Matches code bypass in promote_ready_jobs without concurrency gate acquisition."
  exit 0
else
  echo "No violation found"
  exit 1
fi
